//! Layer 4 — Property-based model checking (single-threaded, deterministic).
//!
//! We drive the buffer with random sequences of operations and compare its
//! observable behaviour against a trivially-correct reference model:
//!
//!   * model write index      = number of successful pushes
//!   * model per-consumer view = a queue of everything produced but not yet
//!     popped BY THAT CONSUMER (fan-out, no loss, FIFO)
//!   * a push succeeds iff (write - slowest_read) < CAP  (fail-on-full)
//!
//! There is NO standalone "refresh" op — the lazy cache is only refreshed on
//! the try_push slow path, so the op set is just Push / Pop(id). The safety
//! invariant `cached <= true` is checked after every op.
//!
//! proptest SHRINKS any failure to a minimal reproducer.
//!
//! Note on const generics: proptest picks values at runtime, but CAP/N are
//! compile-time. We therefore enumerate a fixed set of (CAP, N) configs and let
//! proptest fuzz the op sequence within each. CAP values are powers of two.
//!
//! Skipped under `--features loom`.
#![cfg(not(feature = "loom"))]

use proptest::prelude::*;
use std::collections::VecDeque;
use std::sync::Arc;
use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;

#[derive(Clone, Debug)]
enum Op {
    Push,
    Pop(usize), // consumer index (already reduced mod N by the caller)
}

/// The reference model: obvious, sequential, correct-by-inspection.
struct Model {
    cap: usize,
    write: u64,
    pending: Vec<VecDeque<u64>>, // produced-but-unconsumed per consumer
    read: Vec<u64>,
}

impl Model {
    fn new(cap: usize, consumers: usize) -> Self {
        Model {
            cap,
            write: 0,
            pending: vec![VecDeque::new(); consumers],
            read: vec![0; consumers],
        }
    }
    fn slowest_read(&self) -> u64 {
        self.read.iter().copied().min().unwrap_or(self.write)
    }
    fn live_items(&self) -> u64 {
        self.write - self.slowest_read()
    }
    fn try_push(&mut self) -> Option<u64> {
        if (self.live_items() as usize) < self.cap {
            let v = self.write;
            self.write += 1;
            for q in &mut self.pending {
                q.push_back(v);
            }
            Some(v)
        } else {
            None
        }
    }
    fn pop(&mut self, c: usize) -> Option<u64> {
        if let Some(v) = self.pending[c].pop_front() {
            self.read[c] += 1;
            Some(v)
        } else {
            None
        }
    }
}

/// Drive both the model and the real impl through the same op sequence.
/// `run_one::<CAP, N>` is monomorphized per config we test.
fn run_one<const CAP: usize, const N: usize>(ops: &[Op]) {
    let rb = Arc::new(SpmcRingBuffer::<u64, CAP, N>::new());
    let mut producer = rb.get_new_producer().unwrap();
    let consumers: Vec<_> = (0..N).map(|_| rb.get_new_consumer().unwrap()).collect();
    let mut model = Model::new(CAP, N);

    let mut next_value = 0u64;

    for op in ops {
        match *op {
            Op::Push => {
                let model_ok = model.try_push();
                let real = producer.try_push(next_value);
                match (model_ok, real) {
                    (Some(expected_v), Ok(())) => {
                        assert_eq!(expected_v, next_value, "value numbering diverged");
                        next_value += 1;
                    }
                    (None, Err(v)) => {
                        assert_eq!(v, next_value, "full try_push must return the same item");
                    }
                    (Some(_), Err(_)) => panic!(
                        "impl rejected a push the model accepted (spurious full). live={} cap={}",
                        model.live_items(), CAP
                    ),
                    (None, Ok(())) => panic!(
                        "impl ACCEPTED a push the model rejected (OVERWRITE!). live={} cap={}",
                        model.live_items(), CAP
                    ),
                }
            }
            Op::Pop(c) => {
                let expected = model.pop(c);
                let got = consumers[c].try_pop();
                assert_eq!(got, expected, "consumer {c} pop mismatch: model={expected:?} impl={got:?}");
            }
        }

        // Safety invariant after every op: cache never ahead of reality.
        assert!(
            producer.cached_min_consumer_index() as u64 <= producer.true_min_consumer_index() as u64,
            "SAFETY: producer cache {} ahead of true {}",
            producer.cached_min_consumer_index(),
            producer.true_min_consumer_index(),
        );
    }
}

fn op_strategy(num_consumers: usize) -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => Just(Op::Push),
        4 => (0..num_consumers).prop_map(Op::Pop),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    // Config A: CAP=8, N=4 (the suite standard).
    #[test]
    fn model_matches_impl_cap8_n4(ops in prop::collection::vec(op_strategy(4), 0..300)) {
        run_one::<8, 4>(&ops);
    }

    // Config B: tiny CAP to stress the full/empty boundary + slow path.
    #[test]
    fn model_matches_impl_cap2_n4(ops in prop::collection::vec(op_strategy(4), 0..300)) {
        run_one::<2, 4>(&ops);
    }

    // Config C: single consumer, small buffer.
    #[test]
    fn model_matches_impl_cap4_n1(ops in prop::collection::vec(op_strategy(1), 0..300)) {
        run_one::<4, 1>(&ops);
    }

    // Config D: CAP=1 degenerate.
    #[test]
    fn model_matches_impl_cap1_n2(ops in prop::collection::vec(op_strategy(2), 0..300)) {
        run_one::<1, 2>(&ops);
    }
}

// Hand-written regressions (always run, fast).
#[test]
fn regression_fill_then_partial_drain() {
    let ops = vec![
        Op::Push, Op::Push, Op::Push, Op::Push, // fill toward CAP
        Op::Push,                               // may be full
        Op::Pop(0), Op::Pop(1),                 // only some consumers advance
        Op::Push,                               // gated by slowest consumer
    ];
    run_one::<4, 2>(&ops);
}

#[test]
fn regression_interleaved_wrap() {
    let mut ops = Vec::new();
    for _ in 0..50 {
        ops.push(Op::Push);
        ops.push(Op::Pop(0));
        ops.push(Op::Pop(1));
    }
    run_one::<2, 2>(&ops);
}
