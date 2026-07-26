#[cfg(test)]
mod multi_consumer_tests {
    use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // NOTE: these tests only exercise the multi-consumer path because this
    // crate compiles NUM_CONSUMERS as 4 under `#[cfg(test)]` (see top of
    // lib.rs). With NUM_CONSUMERS hardcoded to 1 in production, none of
    // this code is currently reachable outside of tests - worth deciding
    // whether that's intentional or a work-in-progress.

    #[test]
    fn can_create_exactly_num_consumers_consumers() {
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let mut consumers = Vec::new();
        for i in 0..NUM_CONSUMERS {
            consumers.push(rb.get_new_consumer().unwrap_or_else(|| {
                panic!("consumer {i} of {NUM_CONSUMERS} should have been created")
            }));
        }
        assert!(
            rb.get_new_consumer().is_none(),
            "should not be able to exceed NUM_CONSUMERS consumers"
        );
    }

    // #[test]
    // fn each_consumer_gets_an_independent_head() {
    //     // Sequentially prove the per-consumer head slots don't alias:
    //     // draining one consumer fully must not affect what another
    //     // consumer, still behind, is able to read.
    //     let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
    //     let producer = rb.get_new_producer().unwrap();
    //     let c0 = rb.get_new_consumer().unwrap();
    //     let c1 = rb.get_new_consumer().unwrap();
    //
    //     for i in 0..4 {
    //         assert!(producer.push(i));
    //     }
    //
    //     // Drain c0 completely.
    //     for i in 0..4 {
    //         assert_eq!(c0.consume(), Some(i));
    //     }
    //     assert_eq!(c0.consume(), None);
    //
    //     // c1 hasn't consumed anything yet - it must still see the full
    //     // sequence from the start, untouched by c0's progress.
    //     for i in 0..4 {
    //         assert_eq!(c1.consume(), Some(i));
    //     }
    //     assert_eq!(c1.consume(), None);
    // }

    #[test]
    fn slowest_consumer_gates_producer_even_if_others_are_drained() {
        // This is the important invariant in `push`: it takes the MIN head
        // across all consumers. A fast consumer draining everything must
        // NOT let the producer race ahead and overwrite data the slow
        // consumer hasn't read yet.
        //
        // IMPORTANT: every one of the NUM_CONSUMERS head slots must be
        // claimed here. If a slot is left unclaimed, its head sits at 0
        // forever and permanently gates the producer once the ring fills
        // once - see `bug_repro::unclaimed_consumer_slots_permanently_block_producer`
        // below, which isolates that as a separate finding.
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let producer = rb.get_new_producer().unwrap();
        let fast = rb.get_new_consumer().unwrap();
        let slow = rb.get_new_consumer().unwrap();
        let others: Vec<_> = (2..NUM_CONSUMERS)
            .map(|_| rb.get_new_consumer().unwrap())
            .collect();

        // Fill the ring completely (capacity 8).
        for i in 0..SPMC_RING_SIZE {
            assert!(producer.push(i as i32));
        }

        // Fast consumer, and every "other" consumer, drain everything.
        for i in 0..SPMC_RING_SIZE {
            assert_eq!(fast.consume(), Some(i as i32));
        }
        for other in &others {
            for i in 0..SPMC_RING_SIZE {
                assert_eq!(other.consume(), Some(i as i32));
            }
        }

        // Slow consumer hasn't read anything - producer must still be
        // blocked, because it's gated by `slow`, not by the others.
        assert!(
            !producer.push(999),
            "producer must stay blocked by the slowest consumer, not unblock just because other consumers drained"
        );

        // Once slow catches up partially, exactly that much room opens up.
        assert_eq!(slow.consume(), Some(0));
        assert!(
            producer.push(999),
            "one slot should now be free after `slow` consumed one item"
        );
        assert!(
            !producer.push(1000),
            "still gated by `slow`'s remaining backlog"
        );
    }

    #[test]
    fn all_consumers_receive_identical_broadcast_stream_concurrently() {
        // Single producer thread + one thread per consumer, all running
        // concurrently. Every consumer must observe the exact same
        // in-order sequence, independent of the others' pace.
        let rb = Arc::new(SpmcRingBuffer::<u64>::new());
        let producer = rb.get_new_producer().unwrap();
        let consumers: Vec<_> = (0..NUM_CONSUMERS)
            .map(|_| rb.get_new_consumer().unwrap())
            .collect();

        const N: u64 = 200_000;

        let producer_handle = thread::spawn(move || {
            let mut i = 0u64;
            while i < N {
                if producer.push(i) {
                    i += 1;
                } else {
                    thread::yield_now();
                }
            }
        });

        let consumer_handles: Vec<_> = consumers
            .into_iter()
            .enumerate()
            .map(|(idx, consumer)| {
                thread::spawn(move || {
                    let mut expected = 0u64;
                    while expected < N {
                        if let Some(val) = consumer.consume() {
                            assert_eq!(
                                val, expected,
                                "consumer {idx} got item out of order or lost/duplicated an item"
                            );
                            expected += 1;
                        } else {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        producer_handle.join().unwrap();
        for h in consumer_handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn mismatched_consumer_speeds_still_preserve_correctness() {
        // One consumer reads as fast as possible; others are artificially
        // slowed down. This forces the producer to repeatedly hit the
        // full condition mid-run, which is exactly where a bug in the
        // slowest-head calculation would show up as corrupted or skipped
        // data for the faster consumers.
        #[derive(Copy, Clone, Debug, PartialEq)]
        struct Tagged {
            seq: u64,
            checksum: u64, // seq * 7 + 3
        }

        let rb = Arc::new(SpmcRingBuffer::<Tagged>::new());
        let producer = rb.get_new_producer().unwrap();
        let consumers: Vec<_> = (0..NUM_CONSUMERS)
            .map(|_| rb.get_new_consumer().unwrap())
            .collect();

        const N: u64 = 50_000;

        let producer_handle = thread::spawn(move || {
            let mut i = 0u64;
            while i < N {
                let item = Tagged {
                    seq: i,
                    checksum: i * 7 + 3,
                };
                if producer.push(item) {
                    i += 1;
                } else {
                    thread::yield_now();
                }
            }
        });

        let consumer_handles: Vec<_> = consumers
            .into_iter()
            .enumerate()
            .map(|(idx, consumer)| {
                thread::spawn(move || {
                    let mut expected = 0u64;
                    let mut iterations = 0u64;
                    while expected < N {
                        // All but consumer 0 get an occasional artificial
                        // stall to desynchronize consumer progress.
                        iterations += 1;
                        if idx != 0 && iterations % 500 == 0 {
                            thread::sleep(Duration::from_micros(200));
                        }
                        if let Some(item) = consumer.consume() {
                            assert_eq!(item.seq, expected, "consumer {idx} out of order");
                            assert_eq!(
                                item.checksum,
                                item.seq * 7 + 3,
                                "consumer {idx} observed a torn/corrupted read"
                            );
                            expected += 1;
                        } else {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        producer_handle.join().unwrap();
        for h in consumer_handles {
            h.join().unwrap();
        }
    }
}
