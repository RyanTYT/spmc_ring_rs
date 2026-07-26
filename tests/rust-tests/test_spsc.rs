#[cfg(test)]
mod spmc_ring_buffer_tests {
    use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // ---------- Basic single-threaded correctness ----------

    #[test]
    fn new_buffer_is_empty() {
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let consumer = rb.get_new_consumer().expect("should get consumer");
        assert_eq!(consumer.consume(), None);
    }

    #[test]
    fn single_push_then_consume_returns_same_value() {
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let producer = rb.get_new_producer().expect("should get producer");
        let consumer = rb.get_new_consumer().expect("should get consumer");

        assert!(producer.push(42));
        assert_eq!(consumer.consume(), Some(42));
        assert_eq!(consumer.consume(), None);
    }

    #[test]
    fn fifo_order_is_preserved() {
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

        for i in 0..8 {
            assert!(producer.push(i));
        }
        for i in 0..8 {
            assert_eq!(consumer.consume(), Some(i));
        }
        assert_eq!(consumer.consume(), None);
    }

    // ---------- Boundary conditions ----------

    #[test]
    fn push_fails_when_buffer_is_full() {
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let producer = rb.get_new_producer().unwrap();
        let _consumer = rb.get_new_consumer().unwrap();

        for i in 0..8 {
            assert!(
                producer.push(i),
                "push {i} should succeed while buffer has room"
            );
        }
        // Ring capacity is SPMC_RING_SIZE (8) - the 9th push should fail
        assert!(!producer.push(999), "push should fail once buffer is full");
    }

    #[test]
    fn push_succeeds_again_after_consumer_drains_one_slot() {
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

        for i in 0..8 {
            assert!(producer.push(i));
        }
        assert!(!producer.push(999));

        assert_eq!(consumer.consume(), Some(0));
        assert!(
            producer.push(999),
            "space freed by consume should allow another push"
        );
    }

    #[test]
    fn wraparound_indices_do_not_corrupt_data() {
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

        // Push/consume past the ring size many times to exercise the
        // `& (SPMC_RING_SIZE - 1)` masking logic in both push and consume.
        for round in 0..100 {
            for i in 0..8 {
                let value = round * 8 + i;
                assert!(producer.push(value));
            }
            for i in 0..8 {
                let value = round * 8 + i;
                assert_eq!(consumer.consume(), Some(value));
            }
        }
        assert_eq!(consumer.consume(), None);
    }

    // ---------- Singleton / capacity enforcement ----------

    #[test]
    fn only_one_producer_can_ever_be_created() {
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let _p1 = rb
            .get_new_producer()
            .expect("first producer should succeed");
        assert!(
            rb.get_new_producer().is_none(),
            "second producer must be rejected"
        );
    }

    #[test]
    fn consumer_count_is_capped_at_num_consumers() {
        // NUM_CONSUMERS is currently hardcoded to 1 - this test is a canary:
        // it will need updating (and so will `push`'s slowest-head loop)
        // if that constant is ever raised.
        let rb: SpmcRingBuffer<i32> = SpmcRingBuffer::new();
        let _c1 = rb
            .get_new_consumer()
            .expect("first consumer should succeed");
        assert!(
            rb.get_new_consumer().is_none(),
            "consumer count should be capped at NUM_CONSUMERS"
        );
    }

    // ---------- Concurrency / stress ----------

    #[test]
    fn concurrent_push_and_consume_preserve_all_items_and_order() {
        let rb = Arc::new(SpmcRingBuffer::<u64>::new());
        let producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

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

        let consumer_handle = thread::spawn(move || {
            let mut expected = 0u64;
            while expected < N {
                if let Some(val) = consumer.consume() {
                    assert_eq!(
                        val, expected,
                        "items must arrive in FIFO order without loss or duplication"
                    );
                    expected += 1;
                } else {
                    thread::yield_now();
                }
            }
        });

        producer_handle.join().unwrap();
        consumer_handle.join().unwrap();
    }

    #[test]
    fn no_torn_reads_under_contention_with_composite_values() {
        // Struct with a redundant checksum field. If the producer's fullness
        // check ever lets it overwrite a slot the consumer hasn't finished
        // reading (e.g. due to the Relaxed load of the consumer's head),
        // this should surface as a checksum mismatch instead of silently
        // passing like a plain integer test might.
        #[derive(Copy, Clone, Debug, PartialEq)]
        struct Tagged {
            seq: u64,
            checksum: u64, // always seq * 3 + 1
        }

        let rb = Arc::new(SpmcRingBuffer::<Tagged>::new());
        let producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

        const N: u64 = 200_000;

        let producer_handle = thread::spawn(move || {
            let mut i = 0u64;
            while i < N {
                let item = Tagged {
                    seq: i,
                    checksum: i * 3 + 1,
                };
                if producer.push(item) {
                    i += 1;
                } else {
                    thread::yield_now();
                }
            }
        });

        let consumer_handle = thread::spawn(move || {
            let mut expected = 0u64;
            while expected < N {
                if let Some(item) = consumer.consume() {
                    assert_eq!(item.seq, expected);
                    assert_eq!(item.checksum, item.seq * 3 + 1, "torn read/write detected");
                    expected += 1;
                } else {
                    thread::yield_now();
                }
            }
        });

        producer_handle.join().unwrap();
        consumer_handle.join().unwrap();
    }

    #[test]
    fn producer_recovers_when_consumer_lags_then_catches_up() {
        // Sanity check that a temporarily slow consumer doesn't cause lost
        // items or a permanent false-full state once it catches up.
        let rb = Arc::new(SpmcRingBuffer::<u32>::new());
        let producer = rb.get_new_producer().unwrap();
        let consumer = rb.get_new_consumer().unwrap();

        const N: u32 = 5_000;

        let producer_handle = thread::spawn(move || {
            let mut i = 0u32;
            while i < N {
                if producer.push(i) {
                    i += 1;
                } else {
                    thread::yield_now();
                }
            }
        });

        // Let the producer run ahead and hit the full condition first.
        thread::sleep(Duration::from_millis(5));

        let consumer_handle = thread::spawn(move || {
            let mut expected = 0u32;
            while expected < N {
                if let Some(v) = consumer.consume() {
                    assert_eq!(v, expected);
                    expected += 1;
                } else {
                    thread::yield_now();
                }
            }
        });

        producer_handle.join().unwrap();
        consumer_handle.join().unwrap();
    }
}
