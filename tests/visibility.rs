//! Layer 2 — Asymmetric visibility contract (the heart of the design).
//!
//! Two directions, two very different guarantees:
//!
//!   * Producer -> consumers : IMMEDIATE. After `try_push` returns Ok, any
//!     consumer that calls `try_pop` can see the item.
//!   * Consumers -> producer : LAZY. The producer keeps a cached snapshot of
//!     the slowest consumer and only refreshes it ON THE `try_push` SLOW PATH
//!     (i.e. when the cache says "full"). There is NO standalone refresh
//!     operation — the refresh is observed by driving `try_push` into its full
//!     path, exactly as production does.
//!
//! The critical *safety* invariant threaded through every test:
//!
//!     cached_min_consumer_index() <= true_min_consumer_index()
//!
//! i.e. the producer may lag reality (stale-behind, fine) but must NEVER read
//! ahead of it (stale-ahead, catastrophic — it would reclaim a live slot).
//!
//! Skipped under `--features loom`.
#![cfg(not(feature = "loom"))]

use spmc_ring::ring_buffer::spmc_ring_buffer::{SpmcRingBufferProducer, SpmcRingBuffer};
use std::sync::Arc;

/// Assert the producer's cache is a conservative lower bound of reality.
fn assert_never_stale_ahead<const CAP: usize, const N: usize>(
    p: &SpmcRingBufferProducer<u64, CAP, N>,
) {
    assert!(
        p.cached_min_consumer_index() <= p.true_min_consumer_index(),
        "SAFETY VIOLATION: producer cache {} is AHEAD of true consumer min {} \
         (would let it overwrite a live slot)",
        p.cached_min_consumer_index(),
        p.true_min_consumer_index(),
    );
}

// ---------------------------------------------------------------------------
// Direction 1: producer -> consumer is immediate.
// ---------------------------------------------------------------------------
#[test]
fn produce_is_immediately_visible_to_all_consumers() {
    let rb = Arc::new(SpmcRingBuffer::<u64, 8, 4>::new());
    let mut p = rb.get_new_producer().unwrap();
    let c1 = rb.get_new_consumer().unwrap();
    let c2 = rb.get_new_consumer().unwrap();

    p.try_push(1000).unwrap();
    // No delay: both consumers see it right away.
    assert_eq!(c1.try_pop(), Some(1000));
    assert_eq!(c2.try_pop(), Some(1000));
}

// ---------------------------------------------------------------------------
// Direction 2: consumer -> producer is lazy, refreshed only via try_push.
// ---------------------------------------------------------------------------
#[test]
fn consumer_progress_is_invisible_until_a_full_try_push_requeries() {
    let rb = Arc::new(SpmcRingBuffer::<u64, 4, 4>::new());
    let mut p = rb.get_new_producer().unwrap();
    let c = rb.get_new_consumer().unwrap();

    // Fill the buffer. The final push that fills it may or may not have taken
    // the slow path; regardless, capture the current cached value.
    for i in 0..4u64 {
        p.try_push(i).unwrap();
    }
    let cached_before = p.cached_min_consumer_index();

    // Consumer advances. This does NOT touch the producer's cache.
    assert_eq!(c.try_pop(), Some(0));

    // Lazy: the producer's cached view has not moved (no full-check happened).
    assert_eq!(
        p.cached_min_consumer_index(),
        cached_before,
        "consumer progress must be invisible to the producer until it re-queries"
    );
    // Ground truth HAS moved, proving the cache is stale.
    assert!(
        p.true_min_consumer_index() > cached_before,
        "true consumer position should have advanced"
    );
    assert_never_stale_ahead(&p);

    // The refresh happens as a SIDE EFFECT of a try_push that hits the full
    // path (the freed slot is discovered, so this push SUCCEEDS).
    assert_eq!(p.try_push(4), Ok(()), "freed slot should be found on slow path");
    assert!(
        p.cached_min_consumer_index() > cached_before,
        "after the full-path re-query the producer must see the consumer's progress"
    );
    assert_never_stale_ahead(&p);
}

#[test]
fn producer_may_miss_updates_but_try_push_slow_path_recovers() {
    // The producer can be arbitrarily stale, but a try_push that would fail
    // against the stale cache re-queries and then succeeds if space truly
    // exists. This models "only when it queries again does it know".
    let rb = Arc::new(SpmcRingBuffer::<u64, 2, 4>::new());
    let mut p = rb.get_new_producer().unwrap();
    let c = rb.get_new_consumer().unwrap();

    p.try_push(1).unwrap();
    p.try_push(2).unwrap();

    // Consumer frees both slots, but the producer's cache is stale.
    assert_eq!(c.try_pop(), Some(1));
    assert_eq!(c.try_pop(), Some(2));

    // try_push hits the full fast-path, refreshes, discovers free space, and
    // succeeds — the "misses updates until it re-queries" behaviour.
    assert_eq!(p.try_push(3), Ok(()));
    assert_never_stale_ahead(&p);
}

// ---------------------------------------------------------------------------
// The safety invariant under a fuzzed op schedule.
// ---------------------------------------------------------------------------
#[test]
fn stale_behind_never_ahead_under_random_ops() {
    // Deterministic xorshift schedule (no rng dep) interleaving push / pop,
    // continuously asserting the cache is never ahead of reality. Note: there
    // is no explicit "refresh" op — refresh only happens inside try_push.
    let rb = Arc::new(SpmcRingBuffer::<u64, 4, 4>::new());
    let mut p = rb.get_new_producer().unwrap();
    let c = rb.get_new_consumer().unwrap();

    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };

    let mut produced = 0u64;
    for _ in 0..50_000 {
        if next() % 2 == 0 {
            let _ = p.try_push(produced); // may fail when full; fine
            produced = produced.wrapping_add(1);
        } else {
            let _ = c.try_pop();
        }
        assert_never_stale_ahead(&p);
    }
}

// ---------------------------------------------------------------------------
// The "consumers must not disappear" constraint expressed as backpressure.
// ---------------------------------------------------------------------------
#[test]
fn dead_consumer_permanently_stalls_the_producer() {
    // A registered-but-idle consumer must permanently block the producer once
    // the buffer fills — this is the intended stall, not a bug. We use try_push
    // (never the blocking push, which would hang by design here).
    let rb = Arc::new(SpmcRingBuffer::<u64, 4, 4>::new());
    let mut p = rb.get_new_producer().unwrap();
    let alive = rb.get_new_consumer().unwrap();
    let _dead = rb.get_new_consumer().unwrap(); // registered, never consumes

    for i in 0..4u64 {
        p.try_push(i).unwrap();
    }
    for _ in 0..100 {
        while alive.try_pop().is_some() {}
        // The dead consumer pins index 0, so every further push fails, and each
        // failing push has already re-queried on its slow path.
        assert_eq!(
            p.try_push(999),
            Err(999),
            "dead consumer must keep the stream stalled forever"
        );
        assert_never_stale_ahead(&p);
    }
}

#[test]
fn stream_resumes_only_when_all_consumers_advance() {
    let rb = Arc::new(SpmcRingBuffer::<u64, 2, 4>::new());
    let mut p = rb.get_new_producer().unwrap();
    let c1 = rb.get_new_consumer().unwrap();
    let c2 = rb.get_new_consumer().unwrap();

    p.try_push(0).unwrap();
    p.try_push(1).unwrap();
    assert_eq!(p.try_push(2), Err(2));

    // Only c1 advances -> c2 still pins index 0 -> still full.
    assert_eq!(c1.try_pop(), Some(0));
    assert_eq!(
        p.try_push(2),
        Err(2),
        "one consumer advancing is not enough; the slowest still gates"
    );

    // Now c2 advances too -> min moves -> one slot frees.
    assert_eq!(c2.try_pop(), Some(0));
    assert_eq!(p.try_push(2), Ok(()), "all consumers past index 0 -> slot freed");
    assert_never_stale_ahead(&p);
}
