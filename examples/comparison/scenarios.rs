//! Comparison suite scenario definitions.
//!
//! Three groups compare lock-free `SpmcRingBuffer` vs locked `SyncRingBuffer`
//! head-to-head. `--backend both` is the default so both are always run and
//! tagged in the CSV for side-by-side plotting.
//!
//!   * **Group E** — re-implemented original-suite scenarios as timed
//!     benchmarks. Each maps to a correctness test in `tests/`; here we re-run
//!     the same workload shape through the harness on both backends.
//!   * **Group F** — N-scaling (CAP=1024, Balanced, u64). The headline
//!     divergence chart: per-consumer throughput vs N.
//!   * **Group G** — capacity-scaling (N=4, Balanced, u64). Where the lazy
//!     cache pays off (large CAP) vs where it can't (tiny CAP).
//!
//! Each scenario is monomorphised to a concrete `(CAP, N, P)` by the
//! [`define_scenario!`] macro (same pattern as the isolation suite).

use spmc_ring::bench::{run_scenario, Backend, BenchConfig, Ratio, RawRow, SummaryRow};
use spmc_ring::ring_buffer::{
    spmc_ring_buffer::SpmcRingBuffer, sync_spmc_ring_buffer::SyncRingBuffer,
};
use std::time::Duration;

/// Shared run config (parsed from CLI; passed to every scenario).
pub struct CommonConfig {
    pub trials: usize,
    pub duration: Duration,
    pub warmup: Duration,
    pub idle_gap: Duration,
}

type ScenarioFn = fn(Backend, &CommonConfig) -> (Vec<RawRow>, Vec<SummaryRow>);

/// Generate a monomorphised scenario function (identical to the isolation
/// suite's macro; duplicated here so each example is self-contained).
macro_rules! define_scenario {
    ($id:ident, $name:literal, $cap:expr, $n:expr, $ratio:expr, $payload:ty) => {
        #[allow(non_snake_case)]
        fn $id(backend: Backend, common: &CommonConfig) -> (Vec<RawRow>, Vec<SummaryRow>) {
            let cfg = BenchConfig {
                name: $name.into(),
                backend: backend.name().into(),
                num_consumers: $n,
                capacity: $cap,
                ratio: $ratio,
                trials: common.trials,
                duration: common.duration,
                warmup: common.warmup,
                idle_gap: common.idle_gap,
            };
            match backend {
                Backend::Spmc => run_scenario::<$payload, SpmcRingBuffer<$payload, $cap, $n>, $cap, $n>(&cfg),
                Backend::Sync => run_scenario::<$payload, SyncRingBuffer<$payload, $cap, $n>, $cap, $n>(&cfg),
            }
        }
    };
}

// ===========================================================================
// Group E — re-implemented original-suite scenarios (timed).
//
// Each scenario re-runs the workload shape of a correctness test from tests/
// through the harness, on both backends. The correctness invariant (every
// consumer sees every item, in order, no gaps) is verified lazily on every
// pop by the runner (see src/bench/worker.rs).
// ===========================================================================

// Re-implements `functional::fan_out_every_consumer_sees_every_item_independently`
// + `stress::stress_four_consumers_full_fanout`: 4 consumers, CAP=8, flat-out.
// The headline fan-out test — lock-free lets all 4 read concurrently; locked
// serialises them behind one mutex.
define_scenario!(cmp_fanout_4consumers, "cmp_fanout_4consumers", 8, 2, Ratio::Max, u64);

// Re-implements `stress::stress_uneven_consumer_speeds`: 2 consumers, CAP=64,
// balanced ratio (one dawdles in practice due to scheduling; the harness paces
// both at the same rate, so the "unevenness" here is the buffer's own
// backpressure behaviour).
define_scenario!(cmp_uneven_consumer_speeds, "cmp_uneven_consumer_speeds", 64, 2, Ratio::Balanced, u64);

// Re-implements `stress::stress_tiny_capacity_high_contention`: CAP=4, 4
// consumers, flat-out. Forces the slow path on nearly every push; the lazy
// cache should give lock-free a win; locked pays full mutex + scan each push.
define_scenario!(cmp_tiny_capacity_high_contention, "cmp_tiny_capacity_high_contention", 4, 2, Ratio::Max, u64);

// Re-implements `functional::slowest_consumer_gates_the_buffer`: pure
// backpressure regime. 4 consumers, CAP=4, Max — the slowest consumer pins
// the producer. Tests spin (lock-free) vs block (locked) under backpressure.
define_scenario!(cmp_slowest_consumer_gates, "cmp_slowest_consumer_gates", 4, 2, Ratio::Max, u64);

// Re-implements `functional::wrap_around_many_times` +
// `model::regression_interleaved_wrap`: steady-state fast path over many
// laps. CAP=1024, 4 consumers, Balanced. The lazy cache payoff: lock-free fast
// path has zero Acquire on consumers; locked pays mutex each op.
define_scenario!(cmp_wrap_around_sustained, "cmp_wrap_around_sustained", 1024, 2, Ratio::Balanced, u64);

// Re-implements `stress::blocking_push_unblocks_when_consumer_frees_a_slot`:
// the spin-vs-block test. CAP=8, 4 consumers, Max. Under backpressure,
// lock-free spins (CPU-burns, low wake latency); locked blocks (CPU-free,
// higher wake latency). Both throughput and push-fail latency are recorded
// so the trade-off is visible, not just a single winner.
define_scenario!(cmp_blocking_push_unblocks, "cmp_blocking_push_unblocks", 8, 2, Ratio::Max, u64);

// ===========================================================================
// Group F — N-scaling (CAP=1024, Balanced, u64). The headline divergence chart.
// ===========================================================================
define_scenario!(cmp_n_1,  "cmp_n_1",  1024, 1,  Ratio::Balanced, u64);
define_scenario!(cmp_n_2,  "cmp_n_2",  1024, 2,  Ratio::Balanced, u64);
define_scenario!(cmp_n_4,  "cmp_n_4",  1024, 4,  Ratio::Balanced, u64);
define_scenario!(cmp_n_8,  "cmp_n_8",  1024, 8,  Ratio::Balanced, u64);
define_scenario!(cmp_n_16, "cmp_n_16", 1024, 16, Ratio::Balanced, u64);

// ===========================================================================
// Group G — capacity-scaling (N=4, Balanced, u64).
// ===========================================================================
define_scenario!(cmp_cap_4,    "cmp_cap_4",    4,    2, Ratio::Balanced, u64);
define_scenario!(cmp_cap_16,   "cmp_cap_16",   16,   2, Ratio::Balanced, u64);
define_scenario!(cmp_cap_64,   "cmp_cap_64",   64,   2, Ratio::Balanced, u64);
define_scenario!(cmp_cap_256,  "cmp_cap_256",  256,  2, Ratio::Balanced, u64);
define_scenario!(cmp_cap_1024, "cmp_cap_1024", 1024, 2, Ratio::Balanced, u64);
define_scenario!(cmp_cap_4096, "cmp_cap_4096", 4096, 2, Ratio::Balanced, u64);

// ===========================================================================
// Registry.
// ===========================================================================

/// All comparison scenarios in stable order.
pub fn all_scenarios() -> Vec<(&'static str, ScenarioFn)> {
    vec![
        // Group E
        ("cmp_fanout_4consumers", cmp_fanout_4consumers),
        ("cmp_uneven_consumer_speeds", cmp_uneven_consumer_speeds),
        ("cmp_tiny_capacity_high_contention", cmp_tiny_capacity_high_contention),
        ("cmp_slowest_consumer_gates", cmp_slowest_consumer_gates),
        ("cmp_wrap_around_sustained", cmp_wrap_around_sustained),
        ("cmp_blocking_push_unblocks", cmp_blocking_push_unblocks),
        // Group F
        ("cmp_n_1", cmp_n_1),
        ("cmp_n_2", cmp_n_2),
        ("cmp_n_4", cmp_n_4),
        ("cmp_n_8", cmp_n_8),
        ("cmp_n_16", cmp_n_16),
        // Group G
        ("cmp_cap_4", cmp_cap_4),
        ("cmp_cap_16", cmp_cap_16),
        ("cmp_cap_64", cmp_cap_64),
        ("cmp_cap_256", cmp_cap_256),
        ("cmp_cap_1024", cmp_cap_1024),
        ("cmp_cap_4096", cmp_cap_4096),
    ]
}
