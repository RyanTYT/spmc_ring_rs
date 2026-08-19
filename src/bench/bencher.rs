//! Timing engine — the measurement primitives used by the scenario runner.
//!
//! # What this module provides
//!
//!   * [`Padded`] — a `#[repr(C, align(128))]` wrapper that forces each
//!     per-thread stats struct onto its own pair of cache lines, so the
//!     measurement state of one thread cannot be falsely shared with another's
//!     (or with the buffer's own atomics). 128 (not 64) defeats adjacent-line
//!     hardware prefetchers.
//!   * [`PushStats`] / [`PopStats`] — the per-thread measurement state. Local
//!     `u64` counters (NO shared atomics in the hot loop — counters are read
//!     only after the thread joins). Pre-allocated latency buffers (no
//!     allocation in the hot loop).
//!   * [`percentile_f64`] / [`percentile_u64`] — nearest-rank percentiles over
//!     a sort-in-place slice.
//!
//! # Measurement methodology (anti-taint)
//!
//! The fast path of a lock-free `try_push`/`try_pop` is ~5-20 ns; a single
//! `Instant::now()` costs ~20-40 ns, so timing every op would *double* the
//! measured latency AND slow the thread (which changes the contention pattern
//! under test). We therefore split timing by path:
//!
//!   * **Fast path (success): batched.** One `Instant::now()` per `BATCH`
//!     consecutive successes, recording `batch_ns / BATCH` as a single
//!     batch-mean. Overhead ≈ 40 ns / 512 ops ≈ 0.08 ns/op — negligible. We
//!     lose per-op percentile granularity but gain honesty; the distribution
//!     of batch-means gives a *p50/p99 of typical batch latency* (labelled as
//!     such in the CSV and the report).
//!   * **Slow path (failure): per-op, retroactively.** A fail means the buffer
//!     is full (push) or empty (pop) and the op took the slow path (Acquire
//!     scan + retry for the lock-free impl; mutex wait for the locked impl).
//!     Those ops are µs-scale, so ~40 ns of `Instant` overhead is <1% and
//!     per-op timing is honest. To avoid *any* `Instant` on the fast path, the
//!     fail latency is recorded as the **inter-fail interval**: on each fail
//!     we take `Instant::now()`; if the *previous* op was also a fail, the
//!     interval since it is recorded as one fail-latency sample. The fast path
//!     (consecutive successes) touches no `Instant` at all.
//!
//! See the methodology discussion in the plan (and `report/README.md`) for the
//! full rationale.

use std::sync::Arc;
use std::time::Instant;

/// Batch size for fast-path batched latency. 512 keeps per-op Instant overhead
/// ≈ 0.08 ns while giving enough batch-means per second (at 10 M ops/s and
/// 512-op batches, ~20 k batch-means/s) for stable p50/p99.
pub const BATCH_SIZE: u64 = 512;

// ---------------------------------------------------------------------------
// Padded — cache-line isolation for per-thread state.
// ---------------------------------------------------------------------------

/// Wrap `T` so it occupies its own 128-byte block, preventing false sharing
/// between the measurement state of adjacent threads (and between that state
/// and the ring buffer's own hot atomics). Every per-thread stats struct is
/// stored in a `Padded`.
#[repr(C, align(128))]
pub struct Padded<T>(pub T);

impl<T> Padded<T> {
    #[inline]
    pub fn new(v: T) -> Self {
        Self(v)
    }
    #[inline]
    pub fn get(&self) -> &T {
        &self.0
    }
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Percentile helpers (nearest-rank, sort-in-place).
// ---------------------------------------------------------------------------

/// Nearest-rank percentile over `vals` (sorted in place). `pct` in [0,100].
/// Returns `f64::NAN` for an empty slice. Nearest-rank (not interpolated) is
/// chosen for readability: sample #ceil(pct/100 * n) is the percentile value,
/// directly traceable to the data.
pub fn percentile_f64(vals: &mut [f64], pct: f64) -> f64 {
    if vals.is_empty() {
        return f64::NAN;
    }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    nearest_rank_index(vals.len(), pct)
        .map(|i| vals[i])
        .unwrap_or(f64::NAN)
}

/// Same as [`percentile_f64`] but for `u64` latency samples (slow path).
pub fn percentile_u64(vals: &mut [u64], pct: f64) -> f64 {
    if vals.is_empty() {
        return f64::NAN;
    }
    vals.sort_unstable();
    nearest_rank_index(vals.len(), pct)
        .map(|i| vals[i] as f64)
        .unwrap_or(f64::NAN)
}

fn nearest_rank_index(n: usize, pct: f64) -> Option<usize> {
    if n == 0 {
        return None;
    }
    let p = pct.clamp(0.0, 100.0);
    // nearest-rank: rank = ceil(p/100 * n), 1-based; index = rank-1.
    let rank = ((p / 100.0) * n as f64).ceil() as usize;
    let rank = rank.clamp(1, n);
    Some(rank - 1)
}

// ---------------------------------------------------------------------------
// PushStats — producer-side per-thread measurement state.
// ---------------------------------------------------------------------------

/// Per-thread measurement state for a producer thread. Owned by one thread;
/// the hot loop mutates it via `record_*`; the runner reads it after join.
///
/// **No shared atomics** are touched in the hot loop — `ok_ops`/`fail_ops` are
/// plain `u64`. The latency buffers are pre-allocated in [`PushStats::new`] so
/// the hot loop only does `vec[i] = x` (no realloc).
pub struct PushStats {
    /// Successful (fast-path) push count over the measured window.
    pub ok_ops: u64,
    /// Failed (`Err`, full-buffer / slow-path) push count over the window.
    pub fail_ops: u64,
    /// Fast-path batch-mean ns/op (one entry per `BATCH_SIZE` consecutive
    /// successes). p50/p99 of THIS is the reported "push latency p50/p99"
    /// (batched, not per-op — see module docs).
    pub lat_batches: Vec<f64>,
    /// Slow-path per-op latency samples (ns), recorded as the inter-fail
    /// interval. Honest per-op timing because fail ops are µs-scale.
    pub fail_lat_samples: Vec<u64>,
    /// Window bounds (captured per-thread at start / when the stop flag is
    /// observed). Per-thread window handles the case where threads notice the
    /// stop flag at slightly different instants.
    pub window_start: Instant,
    pub window_end: Instant,

    // --- batch timing state (not for output) ---
    batch_start: Instant,
    batch_count: u64,
    batch_size: u64,
}

impl PushStats {
    /// Pre-allocate buffers for a window of `duration_secs` at an estimated
    /// `peak_ops_per_sec` (over-allocation is fine; the hot loop never
    /// reallocates). `cap_samples` bounds the slow-path sample buffer.
    pub fn new(batch_size: u64, est_batches: usize, cap_samples: usize) -> Self {
        Self {
            ok_ops: 0,
            fail_ops: 0,
            lat_batches: Vec::with_capacity(est_batches),
            fail_lat_samples: Vec::with_capacity(cap_samples),
            window_start: Instant::now(),
            window_end: Instant::now(),
            batch_start: Instant::now(),
            batch_count: 0,
            batch_size,
        }
    }

    /// Reset all counters/buffers and capture a fresh `window_start`. Called
    /// at the warmup→measure boundary so warmup work is excluded.
    pub fn reset(&mut self, now: Instant) {
        self.ok_ops = 0;
        self.fail_ops = 0;
        self.lat_batches.clear();
        self.fail_lat_samples.clear();
        self.batch_start = now;
        self.batch_count = 0;
        self.window_start = now;
        self.window_end = now;
    }

    /// Record a successful (fast-path) push. Manages the batch timer: every
    /// `batch_size` consecutive successes, seal one batch-mean into
    /// [`PushStats::lat_batches`] and reset. No `Instant::now()` per op — only
    /// one per batch.
    #[inline]
    pub fn record_ok(&mut self) {
        self.ok_ops += 1;
        self.batch_count += 1;
        if self.batch_count >= self.batch_size {
            let ns = self.batch_start.elapsed().as_nanos() as f64;
            self.lat_batches.push(ns / self.batch_count as f64);
            self.batch_count = 0;
            self.batch_start = Instant::now();
        }
    }

    /// Record a failed (slow-path) push. `prev_fail` is the `Instant` captured
    /// at the *previous* fail (if any); `now` is the current fail's timestamp.
    /// The inter-fail interval is recorded as one slow-path latency sample.
    /// Also flushes any partial fast-path batch so its (smaller) batch-mean
    /// stays accurate.
    #[inline]
    pub fn record_fail(&mut self, prev_fail: Option<Instant>, now: Instant) {
        self.fail_ops += 1;
        // Flush a partial fast-path batch (the successes since the last batch
        // seal) so its mean isn't diluted by the fail that interrupted it.
        if self.batch_count > 0 {
            let ns = self.batch_start.elapsed().as_nanos() as f64;
            self.lat_batches.push(ns / self.batch_count as f64);
            self.batch_count = 0;
            self.batch_start = now;
        }
        if let Some(t0) = prev_fail {
            self.fail_lat_samples
                .push(now.duration_since(t0).as_nanos() as u64);
        }
    }

    /// Seal the window: flush any trailing partial batch and record the
    /// per-thread stop instant.
    pub fn seal(&mut self, now: Instant) {
        if self.batch_count > 0 {
            let ns = self.batch_start.elapsed().as_nanos() as f64;
            self.lat_batches.push(ns / self.batch_count as f64);
            self.batch_count = 0;
        }
        self.window_end = now;
    }

    /// Measured window length in nanoseconds (per-thread).
    pub fn window_ns(&self) -> f64 {
        self.window_end.duration_since(self.window_start).as_nanos() as f64
    }

    /// Achieved push throughput in ops/sec (successes only).
    pub fn throughput(&self) -> f64 {
        let ns = self.window_ns();
        if ns <= 0.0 {
            0.0
        } else {
            self.ok_ops as f64 * 1e9 / ns
        }
    }

    /// Fraction of `try_push` calls that returned `Err` (0.0–1.0).
    pub fn fail_rate(&self) -> f64 {
        let total = self.ok_ops + self.fail_ops;
        if total == 0 {
            0.0
        } else {
            self.fail_ops as f64 / total as f64
        }
    }

    /// p50 of fast-path batch-mean latency (ns/op). NaN if no batches.
    pub fn lat_p50(&mut self) -> f64 {
        percentile_f64(&mut self.lat_batches, 50.0)
    }

    /// p99 of fast-path batch-mean latency (ns/op). NaN if no batches.
    pub fn lat_p99(&mut self) -> f64 {
        percentile_f64(&mut self.lat_batches, 99.0)
    }

    /// Max fast-path batch-mean latency (ns/op). NaN if no batches.
    pub fn lat_max(&self) -> f64 {
        self.lat_batches
            .iter()
            .copied()
            .fold(f64::NAN, f64::max)
    }

    /// p50 of slow-path (inter-fail) latency (ns). NaN if no fails.
    pub fn fail_lat_p50(&mut self) -> f64 {
        percentile_u64(&mut self.fail_lat_samples, 50.0)
    }

    /// p99 of slow-path (inter-fail) latency (ns). NaN if no fails.
    pub fn fail_lat_p99(&mut self) -> f64 {
        percentile_u64(&mut self.fail_lat_samples, 99.0)
    }
}

// ---------------------------------------------------------------------------
// PopStats — consumer-side per-thread measurement state. Symmetric to PushStats.
// ---------------------------------------------------------------------------

/// Per-thread measurement state for a consumer thread. Symmetric to
/// [`PushStats`]: `ok_ops` = successful pops, `fail_ops` = `None` returns
/// (empty-buffer / slow path).
pub struct PopStats {
    pub ok_ops: u64,
    pub fail_ops: u64,
    pub lat_batches: Vec<f64>,
    pub fail_lat_samples: Vec<u64>,
    pub window_start: Instant,
    pub window_end: Instant,
    batch_start: Instant,
    batch_count: u64,
    batch_size: u64,
}

impl PopStats {
    pub fn new(batch_size: u64, est_batches: usize, cap_samples: usize) -> Self {
        Self {
            ok_ops: 0,
            fail_ops: 0,
            lat_batches: Vec::with_capacity(est_batches),
            fail_lat_samples: Vec::with_capacity(cap_samples),
            window_start: Instant::now(),
            window_end: Instant::now(),
            batch_start: Instant::now(),
            batch_count: 0,
            batch_size,
        }
    }

    pub fn reset(&mut self, now: Instant) {
        self.ok_ops = 0;
        self.fail_ops = 0;
        self.lat_batches.clear();
        self.fail_lat_samples.clear();
        self.batch_start = now;
        self.batch_count = 0;
        self.window_start = now;
        self.window_end = now;
    }

    #[inline]
    pub fn record_ok(&mut self) {
        self.ok_ops += 1;
        self.batch_count += 1;
        if self.batch_count >= self.batch_size {
            let ns = self.batch_start.elapsed().as_nanos() as f64;
            self.lat_batches.push(ns / self.batch_size as f64);
            self.batch_count = 0;
            self.batch_start = Instant::now();
        }
    }

    #[inline]
    pub fn record_fail(&mut self, prev_fail: Option<Instant>, now: Instant) {
        self.fail_ops += 1;
        if self.batch_count > 0 {
            let ns = self.batch_start.elapsed().as_nanos() as f64;
            self.lat_batches.push(ns / self.batch_count as f64);
            self.batch_count = 0;
            self.batch_start = now;
        }
        if let Some(t0) = prev_fail {
            self.fail_lat_samples
                .push(now.duration_since(t0).as_nanos() as u64);
        }
    }

    pub fn seal(&mut self, now: Instant) {
        if self.batch_count > 0 {
            let ns = self.batch_start.elapsed().as_nanos() as f64;
            self.lat_batches.push(ns / self.batch_count as f64);
            self.batch_count = 0;
        }
        self.window_end = now;
    }

    pub fn window_ns(&self) -> f64 {
        self.window_end.duration_since(self.window_start).as_nanos() as f64
    }

    pub fn throughput(&self) -> f64 {
        let ns = self.window_ns();
        if ns <= 0.0 {
            0.0
        } else {
            self.ok_ops as f64 * 1e9 / ns
        }
    }

    pub fn fail_rate(&self) -> f64 {
        let total = self.ok_ops + self.fail_ops;
        if total == 0 {
            0.0
        } else {
            self.fail_ops as f64 / total as f64
        }
    }

    pub fn lat_p50(&mut self) -> f64 {
        percentile_f64(&mut self.lat_batches, 50.0)
    }
    pub fn lat_p99(&mut self) -> f64 {
        percentile_f64(&mut self.lat_batches, 99.0)
    }
    pub fn lat_max(&self) -> f64 {
        self.lat_batches.iter().copied().fold(f64::NAN, f64::max)
    }
    pub fn fail_lat_p50(&mut self) -> f64 {
        percentile_u64(&mut self.fail_lat_samples, 50.0)
    }
    pub fn fail_lat_p99(&mut self) -> f64 {
        percentile_u64(&mut self.fail_lat_samples, 99.0)
    }
}

// ---------------------------------------------------------------------------
// Phase-3 validation: a single-thread window that exercises both fast and
// slow paths and asserts the stats are sane. Run with:
//   cargo test --features bench --lib bencher
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;
    use std::time::Duration;

    /// Push flat-out for 30 ms into a CAP=4 buffer with a registered-but-idle
    /// consumer (so the buffer fills immediately, exercising both the fast
    /// path and the slow/fail path). Asserts the stats come out sane: ops
    /// counted, throughput positive, both batch and fail-latency buffers
    /// populated.
    #[test]
    fn push_stats_records_fast_and_slow_path() {
        let rb = Arc::new(SpmcRingBuffer::<u64, 4, 1>::new());
        let _idle = rb.get_new_consumer().unwrap(); // registered, never consumes → fills
        let mut producer = rb.get_new_producer().unwrap();

        let window = Duration::from_millis(30);
        let deadline = Instant::now() + window;

        let mut stats = PushStats::new(BATCH_SIZE, 4_096, 4_096);
        stats.reset(Instant::now());

        let mut item = 0u64;
        let mut prev_fail: Option<Instant> = None;
        loop {
            if Instant::now() >= deadline {
                stats.seal(Instant::now());
                break;
            }
            match producer.try_push(item) {
                Ok(()) => {
                    stats.record_ok();
                    prev_fail = None;
                    item = item.wrapping_add(1);
                }
                Err(v) => {
                    let now = Instant::now();
                    stats.record_fail(prev_fail, now);
                    prev_fail = Some(now);
                    item = v;
                }
            }
        }

        // Sanity: pushed *something* successfully in 30 ms.
        assert!(stats.ok_ops > 0, "expected some successful pushes");
        // Sanity: the buffer is tiny and the consumer is idle, so we MUST have
        // hit the slow path (fail_ops > 0) and recorded inter-fail samples.
        assert!(stats.fail_ops > 0, "expected slow-path fails (buffer fills)");
        assert!(
            !stats.fail_lat_samples.is_empty(),
            "fail-latency samples should be recorded on the slow path"
        );
        // Throughput positive and bounded by plausibility.
        let tput = stats.throughput();
        assert!(tput > 0.0, "throughput must be positive, got {tput}");
        assert!(tput < 1e12, "throughput implausibly high: {tput}");
        // Batch-means present (fast path ran).
        assert!(!stats.lat_batches.is_empty(), "fast-path batches should exist");
        // p50/p99 of batch-means and fail latency must be positive numbers.
        assert!(stats.lat_p50() > 0.0, "p50 batch latency positive");
        assert!(stats.lat_p99() >= stats.lat_p50(), "p99 >= p50 (latency)");
        assert!(stats.fail_lat_p50() > 0.0, "p50 fail latency positive");
        assert!(stats.fail_lat_p99() >= stats.fail_lat_p50(), "p99 >= p50 (fail)");
        // fail_rate in [0,1].
        let fr = stats.fail_rate();
        assert!((0.0..=1.0).contains(&fr), "fail_rate in [0,1], got {fr}");
        // Window length roughly 30 ms (allow scheduler slack).
        let win_ms = stats.window_ns() / 1e6;
        assert!(win_ms >= 25.0 && win_ms <= 100.0, "window ~30ms, got {win_ms}ms");
    }

    /// Nearest-rank percentile correctness on a known dataset.
    #[test]
    fn percentile_nearest_rank_known() {
        let mut v: Vec<u64> = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        // n=10. p50 → rank ceil(0.5*10)=5 → index 4 → 50. p100 → index 9 → 100.
        assert_eq!(percentile_u64(&mut v, 50.0), 50.0);
        assert_eq!(percentile_u64(&mut v, 100.0), 100.0);
        assert_eq!(percentile_u64(&mut v, 0.0), 10.0);
        // Empty → NaN.
        assert!(percentile_u64(&mut [], 50.0).is_nan());
    }

    /// `Padded` is 128-aligned and at least 128 bytes.
    #[test]
    fn padded_alignment() {
        let p = Padded::new(0u64);
        let addr = &p as *const _ as usize;
        assert_eq!(addr % 128, 0, "Padded must be 128-aligned");
    }
}
