//! Worker functions — the producer/consumer hot loops.
//!
//! Each worker runs the paced hot loop (push or pop) for one trial's measured
//! window. The worker syncs the warmup→measure transition via a [`Barrier`]
//! and stops when the coordinator sets the stop flag. Per-thread stats are
//! pre-allocated and padded (see [`super::bencher::Padded`]).
//!
//! # Pacing
//!
//! `target_attempts` is `Some(rate)` to pace, or `None` for flat-out (Max).
//! Pacing uses spin (`spin_loop`), not `sleep`, because `sleep` overshoots by
//! 100µs–1ms and makes precise ratios impossible.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Barrier;
use std::time::{Duration, Instant};

use crate::bench::backend::{RbConsumer, RbProducer};
use crate::bench::bencher::{PopStats, PushStats};
use crate::bench::scenario::Payload;

/// Worker-visible phase constants (driven by the coordinator).
pub const PHASE_WARMUP: u8 = 0;
pub const PHASE_MEASURE: u8 = 1;

/// Spin until `Instant::now() >= deadline`. Uses `spin_loop` (not `sleep`)
/// because `sleep` overshoots by 100µs–1ms, which would make precise ratios
/// impossible. Burns CPU; mitigated by the `idle_gap` between trials.
#[inline]
fn spin_until(deadline: Instant) {
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

/// Producer worker: push flat-out or paced at `target_attempts` ops/sec.
/// `stats` is pre-allocated and padded; the worker mutates it in place.
pub(crate) fn producer_work<P: Payload, Prod: RbProducer<P>>(
    producer: &mut Prod,
    stats: &mut PushStats,
    phase: &AtomicU8,
    stop: &AtomicBool,
    measure_barrier: &Barrier,
    _window: Duration,
    target_attempts: Option<f64>,
) {
    let period = target_attempts.map(|r| Duration::from_secs_f64(1.0 / r));

    // Continuous sequence number across warmup AND measure — do NOT reset at
    // the barrier. Only the STATS counters reset (at the barrier, below).
    // This keeps the producer's published stream consistent with what
    // consumers expect (each consumer independently tracks the same stream).
    let mut seq: u64 = 0;

    // WARMUP: push flat-out, advancing seq, but don't record stats. Fills
    // instruction/data caches; the stats counters are reset at the barrier.
    while phase.load(Ordering::Relaxed) == PHASE_WARMUP {
        if producer.try_push(P::from_seq(seq)).is_ok() {
            seq = seq.wrapping_add(1);
        }
    }

    // MEASURE: all threads released simultaneously by the barrier.
    measure_barrier.wait();
    let now = Instant::now();
    stats.reset(now); // reset COUNTERS only; seq continues from warmup
    let mut next_deadline = now;
    if let Some(p) = period {
        next_deadline = now + p;
    }

    let mut prev_fail: Option<Instant> = None;
    loop {
        if stop.load(Ordering::Relaxed) {
            stats.seal(Instant::now());
            break;
        }
        match producer.try_push(P::from_seq(seq)) {
            Ok(()) => {
                stats.record_ok();
                prev_fail = None;
                seq = seq.wrapping_add(1);
            }
            Err(_v) => {
                let now = Instant::now();
                stats.record_fail(prev_fail, now);
                prev_fail = Some(now);
            }
        }
        if let Some(p) = period {
            next_deadline += p;
            spin_until(next_deadline);
        }
    }
}

/// Consumer worker: pop flat-out or paced at `target_attempts` ops/sec.
/// Verifies every popped value against the in-order sequence (fan-out: each
/// consumer independently sees every item, FIFO, no gaps). Verification cost
/// (~1ns) is paid equally by both backends, so the comparison stays fair.
pub(crate) fn consumer_work<P: Payload, Cons: RbConsumer<P>>(
    consumer: &Cons,
    stats: &mut PopStats,
    phase: &AtomicU8,
    stop: &AtomicBool,
    measure_barrier: &Barrier,
    _window: Duration,
    target_attempts: Option<f64>,
) {
    let period = target_attempts.map(|r| Duration::from_secs_f64(1.0 / r));

    // Continuous expected-sequence across warmup AND measure — do NOT reset at
    // the barrier. Only the STATS counters reset (at the barrier, below).
    let mut expected: u64 = 0;

    // WARMUP: pop and advance expected, verifying order, but don't record
    // stats. The producer publishes a continuous stream; each consumer's
    // cursor independently advances through it.
    while phase.load(Ordering::Relaxed) == PHASE_WARMUP {
        if let Some(v) = consumer.try_pop() {
            v.verify(expected);
            expected = expected.wrapping_add(1);
        }
    }

    measure_barrier.wait();
    let now = Instant::now();
    stats.reset(now); // reset COUNTERS only; expected continues from warmup
    let mut next_deadline = now;
    if let Some(p) = period {
        next_deadline = now + p;
    }

    let mut prev_fail: Option<Instant> = None;
    loop {
        if stop.load(Ordering::Relaxed) {
            stats.seal(Instant::now());
            break;
        }
        match consumer.try_pop() {
            Some(v) => {
                v.verify(expected);
                stats.record_ok();
                prev_fail = None;
                expected = expected.wrapping_add(1);
            }
            None => {
                let now = Instant::now();
                stats.record_fail(prev_fail, now);
                prev_fail = Some(now);
            }
        }
        if let Some(p) = period {
            next_deadline += p;
            spin_until(next_deadline);
        }
    }
}
