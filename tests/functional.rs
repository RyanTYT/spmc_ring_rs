//! Layer 1 — Sequential functional tests.
//!
//! No threads. These pin down the pure index/mask logic and the fail-on-full +
//! every-item-in-order + fan-out semantics. Fast and fully deterministic — run
//! these first, they catch most off-by-one bugs.
//!
//! Standard config for the suite: CAP = 8, N = 4 (CAP must be a power of two).
//!
//! Skipped entirely under `--features loom` (loom is for the concurrency model).
#![cfg(not(feature = "loom"))]

use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;

// Convenience alias so call sites stay short.
type Rb = SpmcRingBuffer<u64, 8, 4>;

#[test]
fn empty_buffer_pops_none() {
    let rb = Rb::new();
    let c = rb.get_new_consumer().unwrap();
    assert_eq!(c.try_pop(), None);
}

#[test]
fn single_push_single_pop() {
    let rb = Rb::new();
    let mut p = rb.get_new_producer().unwrap();
    let c = rb.get_new_consumer().unwrap();

    p.try_push(42).unwrap();
    assert_eq!(c.try_pop(), Some(42));
    assert_eq!(c.try_pop(), None, "buffer should be empty again");
}

#[test]
fn fifo_order_preserved() {
    let rb = Rb::new();
    let mut p = rb.get_new_producer().unwrap();
    let c = rb.get_new_consumer().unwrap();

    for i in 0..8u64 {
        p.try_push(i).unwrap();
    }
    for i in 0..8u64 {
        assert_eq!(c.try_pop(), Some(i));
    }
    assert_eq!(c.try_pop(), None);
}

#[test]
fn try_push_fails_when_full_and_returns_item() {
    // CAP=8, one consumer that never consumes -> 9th push must fail with the
    // item handed back.
    let rb = Rb::new();
    let mut p = rb.get_new_producer().unwrap();
    let _c = rb.get_new_consumer().unwrap(); // registered but idle

    for i in 0..8u64 {
        assert_eq!(p.try_push(i), Ok(()), "push {i} within capacity should succeed");
    }
    // Now full: the item must come back unchanged.
    assert_eq!(p.try_push(99), Err(99), "full push must return the item");
}

#[test]
fn try_push_succeeds_again_after_consumer_frees_a_slot() {
    // Use CAP=2 here to make "full" cheap to reach.
    let rb = SpmcRingBuffer::<u64, 2, 4>::new();
    let mut p = rb.get_new_producer().unwrap();
    let c = rb.get_new_consumer().unwrap();

    p.try_push(1).unwrap();
    p.try_push(2).unwrap();
    assert_eq!(p.try_push(3), Err(3), "should be full");

    // Consumer frees exactly one slot.
    assert_eq!(c.try_pop(), Some(1));
    // Producer must re-query (lazy) to notice; try_push handles it on its slow
    // path.
    assert_eq!(p.try_push(3), Ok(()), "one slot freed -> one push succeeds");
    assert_eq!(p.try_push(4), Err(4), "still full after one");
}

#[test]
fn wrap_around_many_times() {
    // Interleave push/pop across the CAP boundary to exercise the mask.
    let rb = Rb::new();
    let mut p = rb.get_new_producer().unwrap();
    let c = rb.get_new_consumer().unwrap();

    for i in 0..10_000u64 {
        p.try_push(i).unwrap();
        assert_eq!(c.try_pop(), Some(i), "value survived wrap-around at {i}");
    }
    assert_eq!(c.try_pop(), None);
}

#[test]
fn fan_out_every_consumer_sees_every_item_independently() {
    let rb = Rb::new();
    let mut p = rb.get_new_producer().unwrap();
    let c1 = rb.get_new_consumer().unwrap();
    let c2 = rb.get_new_consumer().unwrap();
    let c3 = rb.get_new_consumer().unwrap();

    for i in 0..8u64 {
        p.try_push(i).unwrap();
    }

    // Consumers advance at different rates but each must see the full stream.
    assert_eq!(c1.try_pop(), Some(0));
    assert_eq!(c2.try_pop(), Some(0));
    assert_eq!(c1.try_pop(), Some(1));
    assert_eq!(c3.try_pop(), Some(0)); // c3 still starts at 0

    let drain = |c: &spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBufferConsumer<u64, 8>, start: u64| {
        let mut expected = start;
        while let Some(v) = c.try_pop() {
            assert_eq!(v, expected, "consumer saw out-of-order / gap");
            expected += 1;
        }
        expected
    };
    assert_eq!(drain(&c1, 2), 8);
    assert_eq!(drain(&c2, 1), 8);
    assert_eq!(drain(&c3, 1), 8);
}

#[test]
fn slowest_consumer_gates_the_buffer() {
    // Free space is bounded by the *slower* of the two consumers.
    let rb = SpmcRingBuffer::<u64, 4, 4>::new();
    let mut p = rb.get_new_producer().unwrap();
    let fast = rb.get_new_consumer().unwrap();
    let _slow = rb.get_new_consumer().unwrap(); // never consumes

    for i in 0..4u64 {
        p.try_push(i).unwrap();
    }
    while fast.try_pop().is_some() {} // fast drains everything it can
    // The slow consumer still pins index 0, so the buffer is full.
    assert_eq!(
        p.try_push(100),
        Err(100),
        "slow consumer must keep the buffer full even though fast drained"
    );
}

#[test]
fn capacity_one_degenerate() {
    // CAP=1 is a valid power of two.
    let rb = SpmcRingBuffer::<u64, 1, 4>::new();
    let mut p = rb.get_new_producer().unwrap();
    let c = rb.get_new_consumer().unwrap();

    p.try_push(7).unwrap();
    assert_eq!(p.try_push(8), Err(8));
    assert_eq!(c.try_pop(), Some(7));
    p.try_push(8).unwrap();
    assert_eq!(c.try_pop(), Some(8));
}

#[test]
fn registering_more_than_n_consumers_returns_none() {
    // N = 2 here; the 3rd consumer request must return None (not panic).
    let rb = SpmcRingBuffer::<u64, 8, 2>::new();
    assert!(rb.get_new_consumer().is_some(), "1st consumer");
    assert!(rb.get_new_consumer().is_some(), "2nd consumer");
    assert!(rb.get_new_consumer().is_none(), "3rd consumer must be None (> N=2)");
}

#[test]
fn only_one_producer_is_handed_out() {
    // Single producer: first call Some, second call None.
    let rb = SpmcRingBuffer::<u64, 8, 4>::new();
    let p1 = rb.get_new_producer();
    assert!(p1.is_some(), "first producer must be Some");
    assert!(rb.get_new_producer().is_none(), "second producer must be None");
}
