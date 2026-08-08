//! Benchmark harness — drives either ring-buffer backend through a single
//! backend-agnostic trait.
//!
//! Gated behind the `bench` feature. Production builds
//! (`--no-default-features`) never compile this module, so no harness code is
//! linked into shipped binaries.
//!
//! # Module layout
//!
//!   * [`backend`] — the `RbProducer` / `RbConsumer` / `RingBuffer` traits +
//!     blanket impls for both backends + `Backend` selector.
//!   * [`bencher`] — timing primitives: `Padded` (cache-line isolation),
//!     `PushStats` / `PopStats` (per-thread measurement state), percentile
//!     helpers.
//!   * [`scenario`] — scenario types: `Ratio`, `Role`, `BenchConfig`,
//!     `RawRow`, `SummaryRow`, `Payload` trait + impls.
//!   * [`worker`] — the producer/consumer hot loops (batched fast-path + per-op
//!     slow-path timing, spin-pacing).
//!   * [`runner`] — `run_scenario`: thread orchestration, multi-trial
//!     aggregation (mean/median/min/max across trials).
//!   * [`csv`] — raw + summary CSV writers.
//!
//! Phases 5–6 build the isolation and comparison scenario sets + CLIs on top
//! of `run_scenario`.

pub mod backend;
pub mod bencher;
pub mod csv;
pub mod runner;
pub mod scenario;
pub mod worker;

pub use backend::{Backend, RbConsumer, RbProducer, RingBuffer};
pub use bencher::{PopStats, PushStats, Padded, BATCH_SIZE};
pub use csv::{write_raw_csv, write_summary_csv};
pub use runner::run_scenario;
pub use scenario::{BenchConfig, Payload, Ratio, RawRow, Role, SummaryRow};
