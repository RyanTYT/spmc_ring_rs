//! Scenario types — the configuration and result-row shapes used by the
//! runner. The thread orchestration, pacing, and aggregation live in
//! [`super::runner`] and [`super::worker`]; the CSV writers live in
//! [`super::csv`].

use std::time::Duration;

// ===========================================================================
// Rate-ratio sweep axis.
// ===========================================================================

/// The producer : consumer rate ratio for a scenario.
///
/// Semantics for SPMC fan-out (where aggregate pop rate = push rate in steady
/// state): the ratio is over the **per-consumer** target attempt rate, and
/// "fast" means the side that attempts more often. The fast side is paced to
/// `k × base` attempts/sec; the slow side attempts at `base`. The fast side's
/// excess attempts become `try_push` → `Err` (full) or `try_pop` → `None`
/// (empty), depending on which side is fast.
///
/// * [`Ratio::Max`] — both sides flat-out (true ceiling). No pacing.
/// * [`Ratio::Balanced`] — producer attempts at `N × base`, each consumer at
///   `base`; in steady state consumers keep up (fan-out). Baseline fast path.
/// * [`Ratio::Prod(k)`] — producer attempts at `k × base`, consumers at
///   `base`. As `k` grows, the producer outpaces the aggregate consumer rate;
///   the buffer fills and `try_push` starts returning `Err`.
/// * [`Ratio::Cons(k)`] — consumers attempt at `k × base` each, producer at
///   `base`. As `k` grows, consumers outpace the producer; the buffer empties
///   and `try_pop` returns `None`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Ratio {
    Max,
    Balanced,
    Prod(u32),
    Cons(u32),
}

impl Ratio {
    /// Short stable label for CSV / chart axes.
    pub fn label(self) -> String {
        match self {
            Ratio::Max => "max".into(),
            Ratio::Balanced => "balanced".into(),
            Ratio::Prod(k) => format!("prod_{k}x"),
            Ratio::Cons(k) => format!("cons_{k}x"),
        }
    }
}

/// Worker role.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Producer,
    Consumer,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Role::Producer => "producer",
            Role::Consumer => "consumer",
        }
    }
}

// ===========================================================================
// Scenario configuration.
// ===========================================================================

/// Full per-scenario configuration. `T` is the item type; `CAP` and `N` are
/// const generics (so each scenario is monomorphised to a concrete config).
#[derive(Clone, Debug)]
pub struct BenchConfig {
    /// Scenario name (CSV `scenario` column).
    pub name: String,
    /// Backend tag (`"spmc"` or `"sync"`).
    pub backend: String,
    /// Number of consumers (must equal the `N` the scenario is monomorphised
    /// for — the runner asserts this).
    pub num_consumers: usize,
    /// Capacity (must equal the `CAP` the scenario is monomorphised for).
    pub capacity: usize,
    /// Producer : consumer rate ratio.
    pub ratio: Ratio,
    /// Number of measured trials (results aggregated across them).
    pub trials: usize,
    /// Per-trial measured-window duration.
    pub duration: Duration,
    /// Warmup duration (discarded; runs before every trial).
    pub warmup: Duration,
    /// Idle gap between trials (lets the CPU cool — mitigates thermal taint
    /// from spin-pacing).
    pub idle_gap: Duration,
}

impl BenchConfig {
    /// Total number of worker threads (1 producer + N consumers).
    pub fn workers(&self) -> usize {
        1 + self.num_consumers
    }
}

// ===========================================================================
// CSV row shapes.
// ===========================================================================

/// One row in the raw CSV: per scenario × backend × trial × thread. Full
/// audit — what each individual thread measured.
#[derive(Clone, Debug)]
pub struct RawRow {
    pub scenario: String,
    pub backend: String,
    pub trial: usize,
    pub role: Role,
    pub thread_id: usize,
    pub capacity: usize,
    pub num_consumers: usize,
    pub ratio: String,
    pub ok_ops: u64,
    pub fail_ops: u64,
    pub window_ns: f64,
    pub throughput_ops_s: f64,
    pub fail_rate_pct: f64,
    pub lat_p50_ns: f64,
    pub lat_p99_ns: f64,
    pub lat_max_ns: f64,
    pub fail_lat_p50_ns: f64,
    pub fail_lat_p99_ns: f64,
    pub payload_bytes: usize,
}

/// One row in the summary CSV: per scenario × backend × role, aggregated
/// across trials. Throughput is summed across threads of that role (producer =
/// total push throughput; consumers = total pop throughput). Latency is the
/// per-thread p50/p99 **averaged** across threads of that role, then
/// mean/median across trials.
#[derive(Clone, Debug)]
pub struct SummaryRow {
    pub scenario: String,
    pub backend: String,
    pub role: Role,
    pub capacity: usize,
    pub num_consumers: usize,
    pub ratio: String,
    pub payload_bytes: usize,
    pub trials: usize,
    // Aggregation across trials.
    pub tput_mean: f64,
    pub tput_median: f64,
    pub tput_min: f64,
    pub tput_max: f64,
    pub fail_rate_mean: f64,
    pub lat_p50_mean: f64,
    pub lat_p50_median: f64,
    pub lat_p99_mean: f64,
    pub lat_p99_median: f64,
    pub fail_lat_p50_mean: f64,
    pub fail_lat_p50_median: f64,
    pub fail_lat_p99_mean: f64,
    pub fail_lat_p99_median: f64,
}

// ===========================================================================
// Payload trait — generator + verifier for the item type.
// ===========================================================================

/// A payload type the harness can generate and verify. Letting the runner be
/// generic over `P` (not `T` directly) keeps generation/verification in one
/// place and makes the payload-size sweep (Group D) a one-line switch.
pub trait Payload: Clone + Send + Sync + 'static {
    /// Size of one value in bytes (for the CSV `payload_bytes` column).
    const BYTES: usize;
    /// Build a payload from a sequence number.
    fn from_seq(seq: u64) -> Self;
    /// Verify this payload against an expected sequence number.
    fn verify(&self, seq: u64);
}

impl Payload for u64 {
    const BYTES: usize = 8;
    fn from_seq(seq: u64) -> Self {
        seq
    }
    fn verify(&self, seq: u64) {
        assert_eq!(*self, seq, "u64 payload mismatch: got {self} expected {seq}");
    }
}

impl Payload for [u64; 8] {
    const BYTES: usize = 64;
    fn from_seq(seq: u64) -> Self {
        let mut v = [0u64; 8];
        for (i, slot) in v.iter_mut().enumerate() {
            *slot = seq.wrapping_add(i as u64);
        }
        v
    }
    fn verify(&self, seq: u64) {
        for (i, lane) in self.iter().enumerate() {
            assert_eq!(
                *lane, seq.wrapping_add(i as u64),
                "[u64;8] payload mismatch: lane {i} got {lane} expected {}",
                seq.wrapping_add(i as u64),
            );
        }
    }
}
