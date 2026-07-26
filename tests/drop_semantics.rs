//! Layer 5b — Resource / Drop correctness (std, single-threaded but exact).
//!
//! A ring buffer that clones values out to N consumers and reuses slots is a
//! classic place to leak or double-drop. These tests use a payload that counts
//! live instances, so we can assert nothing is leaked and nothing is dropped
//! twice across fills, wrap-arounds, and teardown.
//!
//! NOTE: with fan-out + Clone semantics, each consumer's `pop` produces its own
//! clone; those clones are owned by the caller and dropped there. What this
//! test pins down is that the *slots themselves* never leak or double-free the
//! stored value as they are overwritten and as the buffer is dropped.
//!
//! Skipped under `--features loom`.
#![cfg(not(feature = "loom"))]

use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;
use std::sync::atomic::{AtomicI64, Ordering};

static LIVE: AtomicI64 = AtomicI64::new(0);

struct Tracked {
    _v: u64,
}

impl Tracked {
    fn new(v: u64) -> Self {
        LIVE.fetch_add(1, Ordering::SeqCst);
        println!("Tracked Number Added");
        Tracked { _v: v }
    }
}

impl Clone for Tracked {
    // Cloning creates a new live instance (mirrors the producer/consumer
    // copying a value out of a slot). Every clone must be balanced by a drop.
    fn clone(&self) -> Self {
        LIVE.fetch_add(1, Ordering::SeqCst);
        println!("Tracked Number Added from Clone");
        Tracked { _v: self._v }
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        let prev = LIVE.fetch_sub(1, Ordering::SeqCst);
        println!("Tracked Number Subtracted From: {prev:?}");
        assert!(prev > 0, "DOUBLE DROP: live count went negative");
    }
}

/// Run a body, then assert the global live count returned to its starting
/// value (no leak, no double-drop).
fn assert_balanced(body: impl FnOnce()) {
    let start = LIVE.load(Ordering::SeqCst);
    body();
    let end = LIVE.load(Ordering::SeqCst);
    assert_eq!(
        start,
        end,
        "resource imbalance: {} tracked instances leaked (>0) or over-dropped (<0)",
        end - start
    );
}

#[test]
fn drop_semantics_all_sections_sequential() {
    // All sections share the global LIVE counter, so they MUST run in one
    // thread. Keeping them in a single #[test] guarantees that regardless of
    // cargo's default parallel test harness.
    no_leak_on_simple_fill_and_drain();
    no_leak_across_wrap_around();
    no_leak_when_buffer_dropped_while_non_empty();
    no_leak_with_multiple_consumers();

    // Everything balanced back to zero across all sections.
    assert_eq!(
        LIVE.load(Ordering::SeqCst),
        0,
        "global tracked-instance leak across sections"
    );
}

fn no_leak_on_simple_fill_and_drain() {
    assert_balanced(|| {
        let rb = SpmcRingBuffer::<Tracked, 4, 4>::new();
        let p = rb.get_new_producer().unwrap();
        let c = rb.get_new_consumer().unwrap();

        for i in 0..4u64 {
            // Tracked is not Debug, so avoid Result::unwrap (needs E: Debug).
            assert!(p.try_push(Tracked::new(i)).is_ok());
        }
        // Drain: each pop hands us an owned clone; dropping it here balances.
        for _ in 0..4 {
            let popped = c.try_pop().expect("value present");
            drop(popped);
        }
        println!("First Done");
        drop(p);
        println!("Producer Dropped");
        drop(c);
        println!("Consumer Dropped");
        drop(rb);
        println!("Buffer Dropped");
        // Buffer still holds its slot copies until it is dropped at scope end.
    });
}

fn no_leak_across_wrap_around() {
    assert_balanced(|| {
        let rb = SpmcRingBuffer::<Tracked, 4, 4>::new();
        let p = rb.get_new_producer().unwrap();
        let c = rb.get_new_consumer().unwrap();

        for i in 0..100u64 {
            assert!(p.try_push(Tracked::new(i)).is_ok());
            let got = c.try_pop().expect("value present");
            drop(got);
        }
    });
}

fn no_leak_when_buffer_dropped_while_non_empty() {
    // Values still resident in slots must be dropped exactly once when the
    // buffer itself is dropped.
    assert_balanced(|| {
        let rb = SpmcRingBuffer::<Tracked, 8, 4>::new();
        let p = rb.get_new_producer().unwrap();
        let _c = rb.get_new_consumer().unwrap();
        for i in 0..8u64 {
            assert!(p.try_push(Tracked::new(i)).is_ok());
        }
        // Drop everything without consuming: producer, consumer, buffer.
    });
}

fn no_leak_with_multiple_consumers() {
    assert_balanced(|| {
        let rb = SpmcRingBuffer::<Tracked, 8, 4>::new();
        let mut p = rb.get_new_producer().unwrap();
        let c1 = rb.get_new_consumer().unwrap();
        let c2 = rb.get_new_consumer().unwrap();

        for i in 0..8u64 {
            assert!(p.try_push(Tracked::new(i)).is_ok());
        }
        // Each consumer clones out its own copies; balanced as we drop them.
        while let Some(v) = c1.try_pop() {
            drop(v);
        }
        while let Some(v) = c2.try_pop() {
            drop(v);
        }
    });
}
