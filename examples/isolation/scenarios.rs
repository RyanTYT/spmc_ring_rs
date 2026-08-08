//! Isolation suite scenario definitions.
//!
//! Four groups characterise one backend (default `spmc`) across varying inputs:
//!
//!   * **Group A** — rate-ratio sweep (CAP=1024, N=4, u64). Varies `Ratio`.
//!   * **Group B** — capacity sweep (N=4, Max, u64). Varies `CAP`.
//!   * **Group C** — consumer-count sweep (CAP=1024, Balanced, u64). Varies `N`.
//!   * **Group D** — payload size (CAP=1024, N=4, Balanced). u64 vs [u64; 8].
//!
//! Each scenario is monomorphised to a concrete `(CAP, N, P)` by the
//! [`define_scenario!`] macro, which generates a function that dispatches to
//! `run_scenario` with the right type params for the selected backend.

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

/// A scenario function: runs one scenario for one backend, returns rows.
type ScenarioFn = fn(Backend, &CommonConfig) -> (Vec<RawRow>, Vec<SummaryRow>);

/// Generate a monomorphised scenario function.
///
/// Parameters: `$id` (function name), `$name` (CSV scenario column), `$cap`,
/// `$n`, `$ratio:expr`, `$payload:ty`.
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
// Group A — rate-ratio sweep (CAP=1024, N=4, u64).
// ===========================================================================
define_scenario!(rate_max,        "rate_max",        1024, 4, Ratio::Max,       u64);
define_scenario!(rate_balanced,   "rate_balanced",   1024, 4, Ratio::Balanced, u64);
define_scenario!(rate_prod_2x,    "rate_prod_2x",    1024, 4, Ratio::Prod(2), u64);
define_scenario!(rate_prod_4x,    "rate_prod_4x",    1024, 4, Ratio::Prod(4), u64);
define_scenario!(rate_prod_8x,    "rate_prod_8x",    1024, 4, Ratio::Prod(8), u64);
define_scenario!(rate_prod_16x,   "rate_prod_16x",   1024, 4, Ratio::Prod(16), u64);
define_scenario!(rate_cons_2x,    "rate_cons_2x",    1024, 4, Ratio::Cons(2), u64);
define_scenario!(rate_cons_4x,    "rate_cons_4x",    1024, 4, Ratio::Cons(4), u64);

// ===========================================================================
// Group B — capacity sweep (N=4, Max, u64). Varies CAP.
// ===========================================================================
define_scenario!(cap_1,     "cap_1",     1,     4, Ratio::Max, u64);
define_scenario!(cap_2,     "cap_2",     2,     4, Ratio::Max, u64);
define_scenario!(cap_4,     "cap_4",     4,     4, Ratio::Max, u64);
define_scenario!(cap_16,    "cap_16",    16,    4, Ratio::Max, u64);
define_scenario!(cap_64,    "cap_64",    64,    4, Ratio::Max, u64);
define_scenario!(cap_256,   "cap_256",   256,   4, Ratio::Max, u64);
define_scenario!(cap_1024,  "cap_1024",  1024,  4, Ratio::Max, u64);
define_scenario!(cap_4096,  "cap_4096",  4096,  4, Ratio::Max, u64);

// ===========================================================================
// Group C — consumer-count sweep (CAP=1024, Balanced, u64). Varies N.
// ===========================================================================
define_scenario!(n_1,  "n_1",  1024, 1,  Ratio::Balanced, u64);
define_scenario!(n_2,  "n_2",  1024, 2,  Ratio::Balanced, u64);
define_scenario!(n_4,  "n_4",  1024, 4,  Ratio::Balanced, u64);
define_scenario!(n_8,  "n_8",  1024, 8,  Ratio::Balanced, u64);
define_scenario!(n_16, "n_16", 1024, 16, Ratio::Balanced, u64);

// ===========================================================================
// Group D — payload size (CAP=1024, N=4, Balanced). u64 vs [u64; 8].
// ===========================================================================
define_scenario!(payload_u64,    "payload_u64",    1024, 4, Ratio::Balanced, u64);
define_scenario!(payload_u64x8,  "payload_u64x8",  1024, 4, Ratio::Balanced, [u64; 8]);

// ===========================================================================
// Registry — all scenarios in stable order.
// ===========================================================================

/// All isolation scenarios as `(name, function)` pairs, in group order.
pub fn all_scenarios() -> Vec<(&'static str, ScenarioFn)> {
    vec![
        // Group A
        ("rate_max", rate_max),
        ("rate_balanced", rate_balanced),
        ("rate_prod_2x", rate_prod_2x),
        ("rate_prod_4x", rate_prod_4x),
        ("rate_prod_8x", rate_prod_8x),
        ("rate_prod_16x", rate_prod_16x),
        ("rate_cons_2x", rate_cons_2x),
        ("rate_cons_4x", rate_cons_4x),
        // Group B
        ("cap_1", cap_1),
        ("cap_2", cap_2),
        ("cap_4", cap_4),
        ("cap_16", cap_16),
        ("cap_64", cap_64),
        ("cap_256", cap_256),
        ("cap_1024", cap_1024),
        ("cap_4096", cap_4096),
        // Group C
        ("n_1", n_1),
        ("n_2", n_2),
        ("n_4", n_4),
        ("n_8", n_8),
        ("n_16", n_16),
        // Group D
        ("payload_u64", payload_u64),
        ("payload_u64x8", payload_u64x8),
    ]
}
