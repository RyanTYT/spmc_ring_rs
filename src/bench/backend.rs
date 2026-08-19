//! Backend-agnostic ring-buffer trait + blanket impls for both backends.
//!
//! The benchmark harness drives a ring buffer through [`RingBuffer`] without
//! knowing whether the underlying storage is lock-free
//! ([`SpmcRingBuffer`](crate::ring_buffer::spmc_ring_buffer::SpmcRingBuffer))
//! or mutex-guarded
//! ([`SyncRingBuffer`](crate::ring_buffer::sync_spmc_ring_buffer::SyncRingBuffer)).
//! Each backend supplies a blanket impl; the scenarios are written once
//! against the trait and the CLI selects which backend(s) to run (phase 5).
//!
//! Only the *performance* surface is abstracted: `new`, `get_new_producer`,
//! `get_new_consumer`, `try_push`/`push`, `try_pop`/`pop`. The SPMC
//! inspection hooks (`cached_min_consumer_index`, `true_min_consumer_index`,
//! `id`, …) are deliberately NOT in the trait — they're correctness-only and
//! SPMC-specific; a backend without hooks (e.g. `SyncRingBuffer`) still runs
//! through the harness, and the comparison suite re-implements its scenarios
//! against the trait.
//!
//! # Why associated types (not a single `RB: RingBuffer<…>` parameter)
//!
//! `CAP` and `N` are *const generics*, so a scenario that varies them cannot
//! hold "the backend type" as one `RB` type parameter (you can't abstract over
//! different const-generic values in one type). The trait instead fixes
//! `<T, CAP, N>` and exposes `Producer`/`Consumer` as associated types; each
//! concrete `(CAP, N)` configuration is its own monomorphised impl. The
//! scenario runner (phase 4) enumerates concrete configs with a small
//! `scenario!` macro so each is a readable one-liner.

use crate::ring_buffer::{spmc_ring_buffer, sync_spmc_ring_buffer};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Traits.
// ---------------------------------------------------------------------------

/// Producer side of a ring buffer: push one item, non-blocking or spinning.
pub trait RbProducer<T> {
    /// Non-blocking push. `Ok(())` on success; `Err(item)` hands the item back
    /// when the buffer is full (fail-on-full, never overwrites).
    fn try_push(&mut self, item: T) -> Result<(), T>;
    /// Blocking push: spin on `try_push` until it succeeds.
    fn push(&mut self, item: T);
}

/// Consumer side of a ring buffer: pop one item (for *this* consumer), with
/// fan-out semantics (each consumer independently sees every item, FIFO).
pub trait RbConsumer<T> {
    /// Non-blocking pop. `None` if this consumer has caught up to the producer.
    fn try_pop(&self) -> Option<T>;
    /// Blocking pop: spin on `try_pop` until an item is available.
    fn pop(&self) -> T;
}

/// A ring-buffer backend, sized with const generics `<T, CAP, N>`.
///
/// `CAP` must be a power of two (each backend enforces this at compile time).
/// The associated `Producer`/`Consumer` types must be `Send` (movable to a
/// benchmark thread); `Consumer` must also be `Sync` (it may be shared across
/// the harness's bookkeeping).
pub trait RingBuffer<T, const CAP: usize, const N: usize>: Sized + Send + Sync {
    type Producer: RbProducer<T> + Send;
    type Consumer: RbConsumer<T> + Send + Sync;

    /// Construct a ring buffer of capacity `CAP` for up to `N` consumers.
    fn new() -> Self;
    /// Obtain the (single) producer handle. Returns `None` if a producer has
    /// already been issued (single-producer invariant).
    ///
    /// Takes `&Arc<Self>` so backends whose producer/consumer handles keep the
    /// buffer's allocation alive via a cloned `Arc` (e.g. `SpmcRingBuffer`)
    /// can capture that stake. Backends that don't need it (e.g.
    /// `SyncRingBuffer`, whose internal storage is already `Arc`-shared) just
    /// deref the `Arc` and call their `&self`-receiver inherent method.
    fn get_new_producer(self: &Arc<Self>) -> Option<Self::Producer>;
    /// Register a new consumer. Returns `None` if all `N` slots are taken.
    /// See `get_new_producer` for why the receiver is `&Arc<Self>`.
    fn get_new_consumer(self: &Arc<Self>) -> Option<Self::Consumer>;
}

// ---------------------------------------------------------------------------
// Blanket impl: lock-free SPMC.
//
// `T: Clone + Send` — Clone for fan-out pop; Send for cross-thread producer/
// consumer handles. (The underlying `SpmcRingBuffer`/`Producer`/`Consumer`
// are Send+Sync when `T: Send` via their explicit `unsafe impl`s.)
//
// Inside each trait method we call the INHERENT method of the same name via
// method-call syntax; Rust resolves to the inherent method (inherent methods
// shadow trait methods), so there is no recursion through the trait.
// ---------------------------------------------------------------------------
impl<T: Clone + Send, const CAP: usize, const N: usize> RingBuffer<T, CAP, N>
    for spmc_ring_buffer::SpmcRingBuffer<T, CAP, N>
{
    type Producer = spmc_ring_buffer::SpmcRingBufferProducer<T, CAP, N>;
    type Consumer = spmc_ring_buffer::SpmcRingBufferConsumer<T, CAP, N>;

    fn new() -> Self {
        spmc_ring_buffer::SpmcRingBuffer::new()
    }
    fn get_new_producer(self: &Arc<Self>) -> Option<Self::Producer> {
        spmc_ring_buffer::SpmcRingBuffer::get_new_producer(self) // inherent
    }
    fn get_new_consumer(self: &Arc<Self>) -> Option<Self::Consumer> {
        spmc_ring_buffer::SpmcRingBuffer::get_new_consumer(self) // inherent
    }
}

impl<T: Clone, const CAP: usize, const N: usize> RbProducer<T>
    for spmc_ring_buffer::SpmcRingBufferProducer<T, CAP, N>
{
    fn try_push(&mut self, item: T) -> Result<(), T> {
        spmc_ring_buffer::SpmcRingBufferProducer::try_push(&self, item)
    }
    fn push(&mut self, item: T) {
        spmc_ring_buffer::SpmcRingBufferProducer::push(&self, item)
    }
}

impl<T: Clone, const CAP: usize, const N: usize> RbConsumer<T>
    for spmc_ring_buffer::SpmcRingBufferConsumer<T, CAP, N>
{
    fn try_pop(&self) -> Option<T> {
        self.try_pop() // inherent
    }
    fn pop(&self) -> T {
        self.pop() // inherent
    }
}

// ---------------------------------------------------------------------------
// Blanket impl: locked SPMC.
//
// `SyncRingBuffer`/`SyncProducer`/`SyncConsumer` derive Send+Sync
// automatically (they hold `Arc<Mutex<Inner>>`; `Inner: Send` when `T: Send`),
// so no explicit `unsafe impl` is needed here.
// ---------------------------------------------------------------------------
impl<T: Clone + Send + Sync, const CAP: usize, const N: usize> RingBuffer<T, CAP, N>
    for sync_spmc_ring_buffer::SyncRingBuffer<T, CAP, N>
{
    type Producer = sync_spmc_ring_buffer::SyncRingBufferProducer<T, CAP, N>;
    type Consumer = sync_spmc_ring_buffer::SyncRingBufferConsumer<T, CAP>;

    fn new() -> Self {
        sync_spmc_ring_buffer::SyncRingBuffer::new()
    }
    fn get_new_producer(self: &Arc<Self>) -> Option<Self::Producer> {
        // SyncRingBuffer's inherent get_new_producer takes &self; deref the
        // Arc to call it. The SyncRingBuffer's own internal storage is already
        // Arc-shared, so no extra keep-alive stake is needed here.
        (**self).get_new_producer()
    }
    fn get_new_consumer(self: &Arc<Self>) -> Option<Self::Consumer> {
        (**self).get_new_consumer()
    }
}

impl<T: Clone, const CAP: usize, const N: usize> RbProducer<T>
    for sync_spmc_ring_buffer::SyncRingBufferProducer<T, CAP, N>
{
    fn try_push(&mut self, item: T) -> Result<(), T> {
        sync_spmc_ring_buffer::SyncRingBufferProducer::try_push(&self, item)
    }
    fn push(&mut self, item: T) {
        sync_spmc_ring_buffer::SyncRingBufferProducer::push(&self, item)
    }
}

impl<T: Clone, const CAP: usize> RbConsumer<T>
    for sync_spmc_ring_buffer::SyncRingBufferConsumer<T, CAP>
{
    fn try_pop(&self) -> Option<T> {
        self.try_pop() // inherent
    }
    fn pop(&self) -> T {
        self.pop() // inherent
    }
}

// ---------------------------------------------------------------------------
// Backend selector (used by the CLI in phase 5 to pick which backend(s) run).
// ---------------------------------------------------------------------------

/// Which ring-buffer backend to benchmark.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Backend {
    /// Lock-free SPMC (`SpmcRingBuffer`).
    Spmc,
    /// Mutex-guarded (`SyncRingBuffer`).
    Sync,
}

impl Backend {
    /// Short stable name used as the `backend` column in the CSV output and
    /// accepted by `--backend` on the CLI.
    pub fn name(self) -> &'static str {
        match self {
            Backend::Spmc => "spmc",
            Backend::Sync => "sync",
        }
    }

    /// All backends, in a stable order (used for `--backend both`).
    pub fn all() -> &'static [Backend] {
        &[Backend::Spmc, Backend::Sync]
    }
}

impl std::str::FromStr for Backend {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "spmc" => Ok(Backend::Spmc),
            "sync" => Ok(Backend::Sync),
            other => Err(format!(
                "unknown backend `{other}` (expected `spmc`, `sync`, or `both`)"
            )),
        }
    }
}
