//! Layer 3 — std concurrency stress tests (real threads).
//!
//! One producer thread and up to N consumer threads hammering the buffer.
//! Asserts:
//!   * Every consumer observes EVERY item, in order, no gaps (fan-out, no loss).
//!   * No torn reads: each payload carries a checksum.
//!   * The producer retries on full (never drops an item).
//!
//! Also includes two dedicated tests for the BLOCKING `push`/`pop` methods,
//! structured as "spawn the blocking call + guarantee progress from another
//! thread + join" so they can never hang a correct implementation.
//!
//! IMPORTANT: run these on aarch64 (ARM) as well as x86. x86's TSO hides missing
//! Acquire/Release; ARM exposes them. See README.
//!
//! Iteration counts scale up with `--features heavy-stress`.
//! Skipped under `--features loom`.
#![cfg(not(feature = "loom"))]

use spmc_ring::ring_buffer::spmc_ring_buffer::{SpmcRingBufferConsumer, SpmcRingBuffer};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

/// A payload with a self-consistency checksum so a torn/half-written value is
/// detectable on read. `check` must always equal `derive(seq)`.
#[derive(Clone, Copy, Debug)]
struct Payload {
    seq: u64,
    check: u64,
}

impl Payload {
    fn new(seq: u64) -> Self {
        Payload { seq, check: Self::derive(seq) }
    }
    fn derive(seq: u64) -> u64 {
        seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ seq.rotate_left(32)
    }
    fn verify(&self) {
        assert_eq!(
            self.check,
            Self::derive(self.seq),
            "TORN READ: seq={} check={} (expected {})",
            self.seq, self.check, Self::derive(self.seq),
        );
    }
}

const ITEMS: u64 = if cfg!(feature = "heavy-stress") { 5_000_000 } else { 200_000 };

// Standard stress config: CAP = 1024, N = 4 (both compile-time constants).
const CAP: usize = 1024;
const N: usize = 4;

fn run_stress(num_consumers: usize) {
    assert!((1..=N).contains(&num_consumers));

    let rb = SpmcRingBuffer::<Payload, CAP, N>::new();
    let mut producer = rb.get_new_producer().unwrap();
    let consumers: Vec<_> = (0..num_consumers).map(|_| rb.get_new_consumer().unwrap()).collect();

    let done = std::sync::Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = consumers
        .into_iter()
        .map(|c| {
            let done = done.clone();
            thread::spawn(move || {
                let mut expected: u64 = 0;
                loop {
                    match c.try_pop() {
                        Some(p) => {
                            p.verify();
                            assert_eq!(
                                p.seq, expected,
                                "consumer {} out-of-order/gap: got {} expected {}",
                                c.id(), p.seq, expected
                            );
                            expected += 1;
                            if expected == ITEMS {
                                break;
                            }
                        }
                        None => {
                            if done.load(Ordering::Acquire) && expected == ITEMS {
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    }
                }
                assert_eq!(expected, ITEMS, "consumer {} missed items", c.id());
            })
        })
        .collect();

    // Producer: push every item, retrying on full so nothing is dropped.
    let mut item = 0u64;
    while item < ITEMS {
        match producer.try_push(Payload::new(item)) {
            Ok(()) => item += 1,
            Err(_) => std::hint::spin_loop(),
        }
    }
    done.store(true, Ordering::Release);

    for h in handles {
        h.join().expect("consumer thread panicked (see assertion above)");
    }
}

#[test]
fn stress_one_consumer() {
    run_stress(1);
}

#[test]
fn stress_two_consumers() {
    run_stress(2);
}

#[test]
fn stress_four_consumers_full_fanout() {
    run_stress(4);
}

/// Small capacity maximises full/empty boundary crossings and forces the lazy
/// slow-path refresh to run constantly.
#[test]
fn stress_tiny_capacity_high_contention() {
    let rb = SpmcRingBuffer::<Payload, 4, 4>::new();
    let mut producer = rb.get_new_producer().unwrap();
    let consumers: Vec<_> = (0..4).map(|_| rb.get_new_consumer().unwrap()).collect();
    let items: u64 = 50_000;
    let done = std::sync::Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = consumers
        .into_iter()
        .map(|c| {
            let done = done.clone();
            thread::spawn(move || {
                let mut expected = 0u64;
                loop {
                    match c.try_pop() {
                        Some(p) => {
                            p.verify();
                            assert_eq!(p.seq, expected, "consumer {} gap/order", c.id());
                            expected += 1;
                            if expected == items {
                                break;
                            }
                        }
                        None => {
                            if done.load(Ordering::Acquire) && expected == items {
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    }
                }
                assert_eq!(expected, items);
            })
        })
        .collect();

    let mut i = 0u64;
    while i < items {
        match producer.try_push(Payload::new(i)) {
            Ok(()) => i += 1,
            Err(_) => std::hint::spin_loop(),
        }
    }
    done.store(true, Ordering::Release);
    for h in handles {
        h.join().unwrap();
    }
}

/// Uneven consumer speeds: one consumer deliberately dawdles. The producer must
/// stall (full + retry) rather than overwrite; the slow consumer still receives
/// the entire stream.
#[test]
fn stress_uneven_consumer_speeds() {
    let rb = SpmcRingBuffer::<Payload, 64, 4>::new();
    let mut producer = rb.get_new_producer().unwrap();
    let fast = rb.get_new_consumer().unwrap();
    let slow = rb.get_new_consumer().unwrap();
    let items: u64 = 50_000;
    let done = std::sync::Arc::new(AtomicBool::new(false));

    let spawn_consumer = |c: SpmcRingBufferConsumer<Payload, 64>,
                          done: std::sync::Arc<AtomicBool>,
                          dawdle: bool| {
        thread::spawn(move || {
            let mut expected = 0u64;
            let mut tick = 0u64;
            loop {
                match c.try_pop() {
                    Some(p) => {
                        p.verify();
                        assert_eq!(p.seq, expected, "consumer {} gap/order", c.id());
                        expected += 1;
                        if dawdle {
                            tick += 1;
                            if tick % 8 == 0 {
                                thread::yield_now();
                            }
                        }
                        if expected == items {
                            break;
                        }
                    }
                    None => {
                        if done.load(Ordering::Acquire) && expected == items {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            }
            assert_eq!(expected, items, "consumer {} missed items", c.id());
        })
    };

    let hf = spawn_consumer(fast, done.clone(), false);
    let hs = spawn_consumer(slow, done.clone(), true);

    let mut i = 0u64;
    while i < items {
        match producer.try_push(Payload::new(i)) {
            Ok(()) => i += 1,
            Err(_) => std::hint::spin_loop(),
        }
    }
    done.store(true, Ordering::Release);
    hf.join().unwrap();
    hs.join().unwrap();
}

// ---------------------------------------------------------------------------
// Blocking API: spawn the blocking call + guarantee progress + join.
//
// These test the spin-forever `push`/`pop`. Safety comes from structure: the
// blocking call runs on a spawned thread, and the MAIN thread unconditionally
// creates the unblock condition, then join() proves the call terminated. A
// correct impl always unblocks; only a bug (e.g. missing Acquire) could hang,
// which is a real failure the CI per-test timeout will surface.
// ---------------------------------------------------------------------------
#[test]
fn blocking_pop_unblocks_when_producer_pushes() {
    let rb = SpmcRingBuffer::<u64, 8, 4>::new();
    let mut producer = rb.get_new_producer().unwrap();
    let consumer = rb.get_new_consumer().unwrap();

    // Consumer thread calls the BLOCKING pop() — spins until main pushes.
    let handle = thread::spawn(move || consumer.pop());

    // Main is the guarantee: it definitely publishes an item.
    producer.push(42);

    // join() proves the blocking pop actually returned (and returned 42).
    let got = handle.join().unwrap();
    assert_eq!(got, 42);
}

#[test]
fn blocking_push_unblocks_when_consumer_frees_a_slot() {
    let rb = SpmcRingBuffer::<u64, 8, 4>::new();
    let mut producer = rb.get_new_producer().unwrap();
    let consumer = rb.get_new_consumer().unwrap();

    // Fill the buffer (CAP = 8).
    for i in 0..8u64 {
        producer.try_push(i).unwrap();
    }

    // Spawn the BLOCKING push(999) — it spins because the buffer is full.
    let handle = thread::spawn(move || {
        producer.push(999);
        producer // hand the producer back so we can join cleanly
    });

    // Main guarantees progress: draining frees slots so the spinning push can
    // complete. Blocking pop() here is safe — items 0..8 are definitely present.
    let first = consumer.pop();
    assert_eq!(first, 0);
    // Drain the rest so the producer's slow-path refresh definitely observes
    // enough free space to place 999.
    for expected in 1..8u64 {
        assert_eq!(consumer.pop(), expected);
    }

    let _producer = handle.join().unwrap(); // proves push(999) unblocked
    // The 999 is now somewhere in the buffer; drain until we see it.
    let mut saw_999 = false;
    for _ in 0..16 {
        if let Some(v) = consumer.try_pop() {
            if v == 999 {
                saw_999 = true;
                break;
            }
        }
    }
    assert!(saw_999, "the unblocked push should have placed item 999");
}
