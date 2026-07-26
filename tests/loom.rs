//! Layer 5 — Loom exhaustive interleaving / memory-model checking.
//!
//! Loom simulates the C11 memory model and EXHAUSTIVELY explores every legal
//! thread interleaving AND every legal reordering permitted by your chosen
//! atomic orderings, for a small scenario. It finds a missing `Acquire` that a
//! billion-iteration stress run on x86 never would — because x86's TSO silently
//! "fixes" it, but loom (and real ARM) will not.
//!
//! These models use `try_push`/`try_pop` (non-blocking) so a bad interleaving
//! surfaces as a failed assertion, never a hang. The reference impl routes all
//! atomics/cells through its `sync` shim and splits slot access in
//! `Inner::read_slot`/`write_slot` so the loom closure-cell API is used under
//! `--features loom` (see README "loom" section).
//!
//! Golden rules: keep models TINY (CAP 2, 1 producer + 1-2 consumers, 1-2 ops).
//!
//! Only compiles under `--features loom`:
//!     cargo test --features loom --test loom --release
#![cfg(feature = "loom")]

use loom::thread;
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;

/// CONTRACT 1 — Producer -> consumer visibility (release/acquire).
/// The consumer may observe `None` (looked before publish) or `Some(exact
/// value)` — never a torn/garbage value or one never published. A violation
/// proves a missing Acquire on the consumer's `write` load or a missing Release
/// on the producer's store.
#[test]
fn loom_producer_to_consumer_release_acquire() {
    loom::model(|| {
        let rb = SpmcRingBuffer::<u64, 2, 2>::new();
        let mut producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

        let prod = thread::spawn(move || {
            let _ = producer.try_push(0xABCD_u64);
        });
        let cons = thread::spawn(move || match consumer.try_pop() {
            None => {}
            Some(v) => assert_eq!(v, 0xABCD, "consumer saw a value never published / torn"),
        });

        prod.join().unwrap();
        cons.join().unwrap();
    });
}

/// CONTRACT 2 — No loss / no duplication for a single consumer across two
/// pushes and two pops. Whatever it pops must be a prefix of [10, 11], in order.
#[test]
fn loom_no_loss_no_duplication_single_consumer() {
    loom::model(|| {
        let rb = SpmcRingBuffer::<u64, 2, 1>::new();
        let mut producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

        let prod = thread::spawn(move || {
            // Both fit in CAP=2, so both succeed regardless of scheduling.
            assert!(producer.try_push(10).is_ok());
            assert!(producer.try_push(11).is_ok());
        });
        let cons = thread::spawn(move || {
            let mut expected = 10u64;
            for _ in 0..2 {
                if let Some(v) = consumer.try_pop() {
                    assert_eq!(v, expected, "out-of-order / gap / duplicate");
                    expected += 1;
                }
            }
        });

        prod.join().unwrap();
        cons.join().unwrap();
    });
}

/// CONTRACT 3 — Fan-out under concurrency: two consumers each independently
/// observe the single produced item (or None if they raced ahead of publish).
#[test]
fn loom_fanout_two_consumers() {
    loom::model(|| {
        let rb = SpmcRingBuffer::<u64, 2, 2>::new();
        let mut producer = rb.get_new_producer().unwrap();
        let c1 = rb.get_new_consumer().unwrap();
        let c2 = rb.get_new_consumer().unwrap();

        let prod = thread::spawn(move || {
            let _ = producer.try_push(7u64);
        });
        let h1 = thread::spawn(move || match c1.try_pop() {
            None => {}
            Some(v) => assert_eq!(v, 7),
        });
        let h2 = thread::spawn(move || match c2.try_pop() {
            None => {}
            Some(v) => assert_eq!(v, 7),
        });

        prod.join().unwrap();
        h1.join().unwrap();
        h2.join().unwrap();
    });
}

/// CONTRACT 4 — The SAFETY invariant under concurrency: the producer's cached
/// view of consumer progress is never AHEAD of the true value. A consumer pops
/// (releasing its read index) concurrently with a producer whose try_push slow
/// path refreshes the cache. The cache must stay <= the true min under every
/// interleaving. (We pre-fill single-threaded to keep the state space tiny; the
/// concurrent phase is one pop vs one full-path try_push.)
#[test]
fn loom_producer_cache_never_ahead_of_reality() {
    loom::model(|| {
        let rb = SpmcRingBuffer::<u64, 2, 2>::new();
        let mut producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

        // Fill so the next try_push takes the slow (refresh) path.
        assert!(producer.try_push(1u64).is_ok());
        assert!(producer.try_push(2u64).is_ok());

        let consumer_thread = thread::spawn(move || {
            let _ = consumer.try_pop(); // release-stores the advanced read index
        });

        let producer_thread = thread::spawn(move || {
            // This try_push is full -> hits the slow path -> refreshes cache
            // with Acquire loads. Whether it succeeds or fails, the cache must
            // never exceed the true min.
            let _ = producer.try_push(3u64);
            assert!(
                producer.cached_min_consumer_index() <= producer.true_min_consumer_index(),
                "SAFETY VIOLATION under interleaving: cache {} > true {}",
                producer.cached_min_consumer_index(),
                producer.true_min_consumer_index(),
            );
        });

        consumer_thread.join().unwrap();
        producer_thread.join().unwrap();
    });
}

/// CONTRACT 5 — Fail-on-full never overwrites. CAP=1: the first item occupies
/// the only slot; a concurrent second push may succeed (consumer freed the slot
/// and the producer's refresh saw it) or fail — but must never clobber the
/// unconsumed first item. The consumer detects a clobber by asserting it only
/// ever sees values in order.
#[test]
fn loom_fail_on_full_no_overwrite() {
    loom::model(|| {
        let rb = SpmcRingBuffer::<u64, 1, 1>::new();
        let mut producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

        assert!(producer.try_push(100u64).is_ok());

        let prod = thread::spawn(move || {
            // May fail (slot occupied) or succeed (consumer freed it). Never an
            // overwrite of the unconsumed 100.
            let _ = producer.try_push(101u64);
        });
        let cons = thread::spawn(move || {
            let mut expected = 100u64;
            for _ in 0..2 {
                if let Some(v) = consumer.try_pop() {
                    assert_eq!(v, expected, "OVERWRITE: unconsumed value was clobbered");
                    expected += 1;
                }
            }
        });

        prod.join().unwrap();
        cons.join().unwrap();
    });
}
