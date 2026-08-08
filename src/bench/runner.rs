//! Runner — the main entry point: spawns 1 producer + N consumers inside a
//! `std::thread::scope`, runs `trials` measured windows with warmup, and
//! aggregates results into [`RawRow`] / [`SummaryRow`].
//!
//! # Thread model
//!
//! All threads live inside one [`std::thread::scope`]. Worker threads (1
//! producer + N consumers) run the hot loop; the coordinator (the scope
//! caller) drives phase transitions:
//!
//!   1. **Warmup** — workers run flat-out, counters ignored. Duration:
//!      `BenchConfig::warmup`.
//!   2. **Measure** — a `Barrier` (size = workers + 1) releases all threads
//!      simultaneously; each worker resets its local counters and captures
//!      `window_start`.
//!   3. **Stop** — after `BenchConfig::duration`, the coordinator sets an
//!      `AtomicBool`; each worker notices it on its next op, seals its stats,
//!      and returns.
//!
//! Per-thread measurement state is pre-allocated in arrays of `Padded<…>` so
//! adjacent threads' stats don't false-share.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Barrier;
use std::thread;
use std::time::Duration;

use crate::bench::backend::RingBuffer;
use crate::bench::bencher::{PopStats, PushStats, Padded, BATCH_SIZE};
use crate::bench::scenario::{Payload, Ratio, RawRow, Role, SummaryRow, BenchConfig};
use crate::bench::worker::{consumer_work, producer_work, PHASE_MEASURE, PHASE_WARMUP};

/// Conservative fixed base attempt rate (attempts/sec). Below the per-op cost
/// of either backend on modern hardware, so the slow side paces (spins) rather
/// than being limited by the buffer, and the fast side's `k × base` is
/// achievable up to `k` ≈ 5–10 before it saturates.
const BASE_RATE: f64 = 2_000_000.0;

/// Translate a [`Ratio`] to per-thread target attempt rates
/// (producer_rate, per_consumer_rate). `None` means flat-out (Max).
fn ratio_to_targets(ratio: Ratio, n: usize) -> (Option<f64>, Option<f64>) {
    let base = BASE_RATE;
    match ratio {
        Ratio::Max => (None, None),
        Ratio::Balanced => (Some(base * n as f64), Some(base)),
        Ratio::Prod(k) => (Some(base * n as f64 * k as f64), Some(base)),
        Ratio::Cons(k) => (Some(base * n as f64), Some(base * k as f64)),
    }
}

/// Pre-allocate stats buffers sized for the scenario.
fn est_batches(duration: Duration) -> usize {
    // At up to 20 M ops/s with 512-op batches, ~40k batches/sec.
    ((duration.as_secs_f64() * 20_000_000.0 / BATCH_SIZE as f64) as usize + 1024).max(256)
}

/// Run one trial: spawn threads, warm up, measure, collect stats.
fn run_trial<P: Payload, RB: RingBuffer<P, CAP, N>, const CAP: usize, const N: usize>(
    cfg: &BenchConfig,
) -> (PushStats, Vec<PopStats>) {
    assert_eq!(
        cfg.num_consumers, N,
        "BenchConfig.num_consumers ({}) must equal the scenario's N ({})",
        cfg.num_consumers, N,
    );
    assert_eq!(
        cfg.capacity, CAP,
        "BenchConfig.capacity ({}) must equal the scenario's CAP ({})",
        cfg.capacity, CAP,
    );

    let (prod_target, cons_target) = ratio_to_targets(cfg.ratio, N);
    let n_workers = cfg.workers();
    let measure_barrier = Barrier::new(n_workers);
    let phase = AtomicU8::new(PHASE_WARMUP);
    let stop = AtomicBool::new(false);

    let rb = RB::new();
    let mut producer = rb.get_new_producer().expect("producer slot available");
    let consumers: Vec<_> = (0..N)
        .map(|_| rb.get_new_consumer().expect("consumer slot available"))
        .collect();
    drop(rb); // Arc keeps inner alive; rb itself is no longer needed.

    let est = est_batches(cfg.duration);
    let mut push_stats = Padded::new(PushStats::new(BATCH_SIZE, est, est));
    let mut pop_stats: Vec<Padded<PopStats>> = (0..N)
        .map(|_| Padded::new(PopStats::new(BATCH_SIZE, est, est)))
        .collect();

    let warmup = cfg.warmup;
    let duration = cfg.duration;

    // All closures share &refs to phase/stop/barrier (they're Sync). Set up
    // the refs once; closures capture them by move (the refs, not the owned
    // atomics).
    let phase_ref = &phase;
    let stop_ref = &stop;
    let barrier_ref = &measure_barrier;

    thread::scope(|s| {
        // Coordinator: sleep warmup, advance to MEASURE, sleep duration, set stop.
        s.spawn(move || {
            thread::sleep(warmup);
            phase_ref.store(PHASE_MEASURE, Ordering::Relaxed);
            thread::sleep(duration);
            stop_ref.store(true, Ordering::Relaxed);
        });

        // Producer thread: move `producer`, borrow `push_stats` mutably.
        s.spawn(|| {
            producer_work(
                &mut producer,
                push_stats.get_mut(),
                phase_ref,
                stop_ref,
                barrier_ref,
                duration,
                prod_target,
            );
        });

        // Consumer threads: disjoint &mut refs via iter_mut(), borrow consumers.
        for (i, pop_padded) in pop_stats.iter_mut().enumerate() {
            let cons = &consumers[i];
            s.spawn(move || {
                consumer_work(
                    cons,
                    pop_padded.get_mut(),
                    phase_ref,
                    stop_ref,
                    barrier_ref,
                    duration,
                    cons_target,
                );
            });
        }
    });

    // Collect stats (move out of Padded).
    let push = push_stats.into_inner();
    let pops: Vec<PopStats> = pop_stats.into_iter().map(Padded::into_inner).collect();
    (push, pops)
}

/// The main entry point: run `cfg.trials` trials and aggregate.
///
/// Returns `(raw_rows, summary_rows)` for the CSV writers. `raw_rows` has one
/// entry per trial × thread; `summary_rows` has one entry per role
/// (producer, consumer) aggregated across trials.
pub fn run_scenario<P: Payload, RB: RingBuffer<P, CAP, N>, const CAP: usize, const N: usize>(
    cfg: &BenchConfig,
) -> (Vec<RawRow>, Vec<SummaryRow>) {
    let (prod_target, cons_target) = ratio_to_targets(cfg.ratio, N);
    let ratio_label = cfg.ratio.label();
    let est = est_batches(cfg.duration);

    let mut raw_rows = Vec::new();
    // Per-trial aggregated scalars (for summary aggregation across trials).
    let mut prod_tputs = Vec::new();
    let mut prod_fail_rates = Vec::new();
    let mut prod_lat_p50 = Vec::new();
    let mut prod_lat_p99 = Vec::new();
    let mut prod_fail_lat_p50 = Vec::new();
    let mut prod_fail_lat_p99 = Vec::new();

    let mut cons_tputs_total = Vec::new();
    let mut cons_fail_rates = Vec::new();
    let mut cons_lat_p50 = Vec::new();
    let mut cons_lat_p99 = Vec::new();
    let mut cons_fail_lat_p50 = Vec::new();
    let mut cons_fail_lat_p99 = Vec::new();

    for trial in 0..cfg.trials {
        let (mut push, mut pops) = run_trial::<P, RB, CAP, N>(cfg);

        // Producer raw row.
        let push_tput = push.throughput();
        let push_fail_rate = push.fail_rate();
        let push_p50 = push.lat_p50();
        let push_p99 = push.lat_p99();
        let push_max = push.lat_max();
        let push_fail_p50 = push.fail_lat_p50();
        let push_fail_p99 = push.fail_lat_p99();

        raw_rows.push(RawRow {
            scenario: cfg.name.clone(),
            backend: cfg.backend.clone(),
            trial,
            role: Role::Producer,
            thread_id: 0,
            capacity: CAP,
            num_consumers: N,
            ratio: ratio_label.clone(),
            ok_ops: push.ok_ops,
            fail_ops: push.fail_ops,
            window_ns: push.window_ns(),
            throughput_ops_s: push_tput,
            fail_rate_pct: push_fail_rate * 100.0,
            lat_p50_ns: push_p50,
            lat_p99_ns: push_p99,
            lat_max_ns: push_max,
            fail_lat_p50_ns: push_fail_p50,
            fail_lat_p99_ns: push_fail_p99,
            payload_bytes: P::BYTES,
        });

        prod_tputs.push(push_tput);
        prod_fail_rates.push(push_fail_rate);
        prod_lat_p50.push(push_p50);
        prod_lat_p99.push(push_p99);
        prod_fail_lat_p50.push(push_fail_p50);
        prod_fail_lat_p99.push(push_fail_p99);

        // Consumer raw rows + aggregation.
        let mut cons_total_tput = 0.0f64;
        let mut cons_total_ok = 0u64;
        let mut cons_total_fail = 0u64;
        let mut cons_lat_p50_vals: Vec<f64> = Vec::new();
        let mut cons_lat_p99_vals: Vec<f64> = Vec::new();
        let mut cons_fail_lat_p50_vals: Vec<f64> = Vec::new();
        let mut cons_fail_lat_p99_vals: Vec<f64> = Vec::new();

        for (i, pop) in pops.iter_mut().enumerate() {
            let tput = pop.throughput();
            let fr = pop.fail_rate();
            let p50 = pop.lat_p50();
            let p99 = pop.lat_p99();
            let flp50 = pop.fail_lat_p50();
            let flp99 = pop.fail_lat_p99();

            cons_total_tput += tput;
            cons_total_ok += pop.ok_ops;
            cons_total_fail += pop.fail_ops;
            if !p50.is_nan() { cons_lat_p50_vals.push(p50); }
            if !p99.is_nan() { cons_lat_p99_vals.push(p99); }
            if !flp50.is_nan() { cons_fail_lat_p50_vals.push(flp50); }
            if !flp99.is_nan() { cons_fail_lat_p99_vals.push(flp99); }

            raw_rows.push(RawRow {
                scenario: cfg.name.clone(),
                backend: cfg.backend.clone(),
                trial,
                role: Role::Consumer,
                thread_id: i,
                capacity: CAP,
                num_consumers: N,
                ratio: ratio_label.clone(),
                ok_ops: pop.ok_ops,
                fail_ops: pop.fail_ops,
                window_ns: pop.window_ns(),
                throughput_ops_s: tput,
                fail_rate_pct: fr * 100.0,
                lat_p50_ns: p50,
                lat_p99_ns: p99,
                lat_max_ns: pop.lat_max(),
                fail_lat_p50_ns: flp50,
                fail_lat_p99_ns: flp99,
                payload_bytes: P::BYTES,
            });
        }

        let cons_fail_rate = if cons_total_ok + cons_total_fail > 0 {
            cons_total_fail as f64 / (cons_total_ok + cons_total_fail) as f64
        } else {
            0.0
        };
        let cons_lat_p50_mean = mean(&cons_lat_p50_vals);
        let cons_lat_p99_mean = mean(&cons_lat_p99_vals);
        let cons_fail_lat_p50_mean = mean(&cons_fail_lat_p50_vals);
        let cons_fail_lat_p99_mean = mean(&cons_fail_lat_p99_vals);

        cons_tputs_total.push(cons_total_tput);
        cons_fail_rates.push(cons_fail_rate);
        cons_lat_p50.push(cons_lat_p50_mean);
        cons_lat_p99.push(cons_lat_p99_mean);
        cons_fail_lat_p50.push(cons_fail_lat_p50_mean);
        cons_fail_lat_p99.push(cons_fail_lat_p99_mean);

        // Inter-trial cooldown (mitigates thermal taint from spin-pacing).
        if trial + 1 < cfg.trials {
            thread::sleep(cfg.idle_gap);
        }
    }

    // Build summary rows.
    let prod_summary = SummaryRow {
        scenario: cfg.name.clone(),
        backend: cfg.backend.clone(),
        role: Role::Producer,
        capacity: CAP,
        num_consumers: N,
        ratio: ratio_label.clone(),
        payload_bytes: P::BYTES,
        trials: cfg.trials,
        tput_mean: mean(&prod_tputs),
        tput_median: median(&prod_tputs),
        tput_min: prod_tputs.iter().copied().fold(f64::INFINITY, f64::min),
        tput_max: prod_tputs.iter().copied().fold(0.0, f64::max),
        fail_rate_mean: mean(&prod_fail_rates),
        lat_p50_mean: mean(&prod_lat_p50),
        lat_p50_median: median(&prod_lat_p50),
        lat_p99_mean: mean(&prod_lat_p99),
        lat_p99_median: median(&prod_lat_p99),
        fail_lat_p50_mean: mean(&prod_fail_lat_p50),
        fail_lat_p50_median: median(&prod_fail_lat_p50),
        fail_lat_p99_mean: mean(&prod_fail_lat_p99),
        fail_lat_p99_median: median(&prod_fail_lat_p99),
    };

    let cons_summary = SummaryRow {
        scenario: cfg.name.clone(),
        backend: cfg.backend.clone(),
        role: Role::Consumer,
        capacity: CAP,
        num_consumers: N,
        ratio: ratio_label,
        payload_bytes: P::BYTES,
        trials: cfg.trials,
        tput_mean: mean(&cons_tputs_total),
        tput_median: median(&cons_tputs_total),
        tput_min: cons_tputs_total.iter().copied().fold(f64::INFINITY, f64::min),
        tput_max: cons_tputs_total.iter().copied().fold(0.0, f64::max),
        fail_rate_mean: mean(&cons_fail_rates),
        lat_p50_mean: mean(&cons_lat_p50),
        lat_p50_median: median(&cons_lat_p50),
        lat_p99_mean: mean(&cons_lat_p99),
        lat_p99_median: median(&cons_lat_p99),
        fail_lat_p50_mean: mean(&cons_fail_lat_p50),
        fail_lat_p50_median: median(&cons_fail_lat_p50),
        fail_lat_p99_mean: mean(&cons_fail_lat_p99),
        fail_lat_p99_median: median(&cons_fail_lat_p99),
    };

    let _ = (prod_target, cons_target, est); // suppress unused warnings
    (raw_rows, vec![prod_summary, cons_summary])
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        f64::NAN
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

fn median(v: &[f64]) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    let mut sorted: Vec<f64> = v.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}
