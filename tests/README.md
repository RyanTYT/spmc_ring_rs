# SPMC Ring Buffer — Test Suite

A compartmentalised test suite for a **Single-Producer / Multi-Consumer** ring
buffer, sized with **const generics** `SpmcRingBuffer<T, CAP, N>`.

| Property | Behaviour |
|---|---|
| Sizing | `CAP` (capacity, **power of two**, compile-time checked) and `N` (consumers) are const generics — the compiler specializes each config |
| Producers / consumers | 1 producer, `N` consumers |
| Fan-out | every consumer independently sees **every** item, in order, **no gaps** |
| Full policy | `try_push` returns **`Err(item)`** (hands the item back) — never overwrites; `push` spins until success |
| Producer → consumer | **immediate** visibility (release/acquire) |
| Consumer → producer | **lazy** — producer caches a snapshot, refreshed only on the `try_push` slow path (the full-check) |
| Safety invariant | producer's cached consumer position is a **conservative lower bound**: may lag reality (stale-behind) but must **never** read ahead (stale-ahead) |
| Dead consumers | a registered consumer that stops consuming **permanently stalls** the producer (by design); `push`/`pop` spin forever |

> The buffer in `src/lib.rs` is a **reference implementation**. Swap its body
> for your real data structure, keep the same public API + inspection hooks +
> the `sync`/slot-access split, and the whole suite runs against yours.

## API under test

```rust
let rb = SpmcRingBuffer::<u64, 8, 4>::new();     // CAP=8 (pow2), N=4
let mut producer = rb.get_new_producer().unwrap();  // Some on 1st call, None after
let c1 = rb.get_new_consumer().unwrap();          // Some while < N, None beyond
let c2 = rb.get_new_consumer().unwrap();

producer.try_push(item)?;   // Result<(), T> — Err(item) when full
producer.push(item);        // () — spins until success

let x = c1.try_pop();       // Option<T> — None when caught up
let y = c1.pop();           // T — spins until an item is available
```

Inspection hooks (Layers 2/4/5), gated behind the `test-hooks` feature:

```rust
producer.cached_min_consumer_index()  // real, PLAIN read (producer-owned, no ordering)
producer.true_min_consumer_index()    // `test-hooks`, Acquire loads — ground truth
consumer.id()                         // `test-hooks`
```

There is **no** standalone `refresh` method — the lazy cache is refreshed only
inside `try_push`'s slow path, and tests observe it by driving `try_push` into
the full path.

## Feature flags

| Feature | Default | Purpose |
|---|---|---|
| `test-hooks` | **ON** | Exposes the inspection hooks to the integration tests under `tests/`. Those are separate crates linking the lib **without** `cfg(test)`, so `cfg(test)` can't reach them — a feature is the correct gate. Production consumers build with `--no-default-features`. |
| `loom` | off | Swaps std atomics/cells for loom's instrumented ones. Enable with `--features loom`. Preferred over the older `--cfg loom` RUSTFLAGS trick (a Cargo feature is first-class and gates the optional `loom` dependency directly). |
| `heavy-stress` | off | Million-iteration stress runs. |

## Layout — one file per layer

| File | Layer | Compiles when | What it proves |
|---|---|---|---|
| `src/lib.rs` | — | always | reference impl + `sync` shim + slot-access split |
| `tests/functional.rs` | 1 | `not(loom)` | sequential logic: empty/full/wrap/fan-out/fail-on-full |
| `tests/visibility.rs` | 2 | `not(loom)` | **asymmetric visibility** + stale-behind-never-ahead + backpressure |
| `tests/stress.rs` | 3 | `not(loom)` | 1P/NC real threads, torn-read checksum, no-loss + blocking push/pop |
| `tests/model.rs` | 4 | `not(loom)` | proptest vs a reference model, with shrinking, across configs |
| `tests/drop_semantics.rs` | 5b | `not(loom)` | no leak / no double-drop across fills, wrap, teardown |
| `tests/loom.rs` | 5 | **`loom`** | **exhaustive** interleaving/memory-model checks |

Every `not(loom)` file starts with `#![cfg(not(feature = "loom"))]` and
`loom.rs` starts with `#![cfg(feature = "loom")]`. This guarantees each file
either compiles-and-runs or is **skipped as an empty crate** in a given build —
never a compile error. So `cargo test` runs everything except loom, and
`cargo test --features loom` runs only loom. They never clash.

---

# The commands — comprehensive testing

Copy-paste this block. Each configuration is isolated; nothing breaks across
them because of the `cfg(feature = "loom")` gating described above.

```bash
# ---------------------------------------------------------------------------
# 1. NORMAL — the fast suite (functional, visibility, stress, model, drop).
#    loom.rs is skipped (empty crate). test-hooks is ON by default.
# ---------------------------------------------------------------------------
cargo test

# Heavy stress variant (millions of iterations; still no loom):
cargo test --features heavy-stress --release

# Widen the property tests:
PROPTEST_CASES=20000 cargo test --test model

# ---------------------------------------------------------------------------
# 2. LOOM — exhaustive interleaving / memory-ordering checks.
#    ONLY loom.rs compiles; the 5 not(loom) files are skipped (empty crates).
#    Always --release (loom is a heavy interpreter).
# ---------------------------------------------------------------------------
cargo test --features loom --test loom --release

# Bound the search if a model is too large:
LOOM_MAX_PREEMPTIONS=3 cargo test --features loom --test loom --release

# Debug one failing schedule:
LOOM_LOG=trace LOOM_CHECKPOINT_FILE=loom.json \
  cargo tesm --features loom --test loom loom_producer_to_consumer_release_acquire --release

# ---------------------------------------------------------------------------
# 3. MIRI — Undefined-Behavior / aliasing checker (validates the raw pointer
#    arithmetic). Runs the NORMAL build (no loom). Small deterministic tests
#    only — NOT stress. Run BOTH borrow models.
# ---------------------------------------------------------------------------
rustup +nightly component add miri

# Stacked Borrows (default):
cargo +nightly miri test --test functional --test visibility --test drop_semantics
PROPTEST_CASES=32 cargo +nightly miri test --test model

# Tree Borrows (second aliasing model — run both):
MIRIFLAGS="-Zmiri-tree-borrows" \
  cargo +nightly miri test --test functional --test visibility --test drop_semantics

# ---------------------------------------------------------------------------
# 4. ARM — rerun stress + loom on aarch64 (x86 TSO hides missing fences).
# ---------------------------------------------------------------------------
cargo test --features heavy-stress --release          # on an aarch64 host
cargo test --features loom --test loom --release      # on an aarch64 host

# ---------------------------------------------------------------------------
# 5. ThreadSanitizer (optional) — data races on the stress layer.
# ---------------------------------------------------------------------------
RUSTFLAGS="-Zsanitizer=thread" \
  cargo +nightly test --target aarch64-apple-darwin --test stress --release
```

### Which tests run in which configuration (nothing breaks)

| Command | functional | visibility | stress | model | drop_semantics | loom |
|---|:--:|:--:|:--:|:--:|:--:|:--:|
| `cargo test` | ✅ run | ✅ run | ✅ run | ✅ run | ✅ run | ⚪ skipped |
| `cargo test --features loom --test loom` | ⚪ skipped | ⚪ skipped | ⚪ skipped | ⚪ skipped | ⚪ skipped | ✅ run |
| `cargo +nightly miri test --test functional …` | ✅ run | ✅ run | ⛔ don't | ✅ (few cases) | ✅ run | ⚪ skipped |

- ✅ run · ⚪ skipped (empty crate, no compile error) · ⛔ excluded (too slow under Miri)
- **Never** run Miri on `stress` (50–500× slowdown) or on `loom` (loom is its own build).
- **Never** pass `--features loom` to the normal suite — it would skip all 5 std tests.

---

# loom — configuring the SPMC code

loom can only see atomics/cells that come from `loom::*`, and its `UnsafeCell`
uses a **closure API** (`.with` / `.with_mut`) — it must instrument each access.
Three pieces make the buffer loom-ready (all in `src/lib.rs`; mirror them into
your real code):

**1. `Cargo.toml` — loom as an optional dep gated by the `loom` feature:**

```toml
[dependencies]
loom = { version = "0.7", optional = true }

[features]
loom = ["dep:loom"]
```

**2. A `sync` shim — swap std ↔ loom types on the feature:**

```rust
mod sync {
    #[cfg(feature = "loom")]      pub(crate) use loom::cell::UnsafeCell;
    #[cfg(feature = "loom")]      pub(crate) use loom::sync::atomic::{AtomicUsize, Ordering};
    #[cfg(not(feature = "loom"))] pub(crate) use std::cell::UnsafeCell;
    #[cfg(not(feature = "loom"))] pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};
}
```

**3. Split ONLY the slot access.** Storage is `[UnsafeCell<Option<T>>; CAP]`
(per-slot cells) in both builds — that per-slot layout is what makes it
loom-checkable with no storage fallback. The only divergence is how a slot is
touched: production uses raw pointer arithmetic; loom uses the closure API:

```rust
#[inline]
fn write_slot(&self, index: usize, value: Option<T>) {
    let idx = index & (CAP - 1);
    #[cfg(not(feature = "loom"))]
    unsafe {
        let base = self.slots.as_ptr() as *mut UnsafeCell<Option<T>>;
        *(*base.add(idx)).get() = value;               // production: pointer arithmetic
    }
    #[cfg(feature = "loom")]
    self.slots[idx].with_mut(|p| unsafe { *p = value });  // loom: instrumented
}
```

All `Release`/`Acquire` ordering is written once (never `cfg`-split) — loom
verifies the ordering, which is layout-independent.

---

# Miri — configuring the SPMC code

Unlike loom, Miri needs **no code changes** — it runs the normal (non-loom)
build, exercising exactly the production `[UnsafeCell<Option<T>>; CAP]` +
pointer-arithmetic path.

**What it does.** Miri interprets your MIR in a VM that tracks every allocation,
pointer provenance, and borrow stack. It runs your **existing tests unchanged**
and **detects Undefined Behavior**: OOB, use-after-free, invalid pointer
arithmetic, uninitialized reads, and — most relevant here — **aliasing
violations** under Stacked/Tree Borrows. It is the only tool that *validates the
raw pointer arithmetic itself*; a bug there can pass `cargo test` on x86 yet be
UB.

**Two caveats:**
1. Miri explores **one schedule** per run — it is **not** a substitute for loom.
2. Miri is **50–500× slower** than native — run it on the **small deterministic**
   tests, never the million-iteration stress runs.

**Maximise its value:** run **both** aliasing models (Stacked Borrows default,
and `MIRIFLAGS="-Zmiri-tree-borrows"`), as shown in the command block above.

**Miri-safe targets:** `functional`, `visibility`, `model` (small
`PROPTEST_CASES`), `drop_semantics`.
**Do NOT run under Miri:** `stress` (too slow), `loom` (its own build).

---

## Plugging in your implementation

Keep the public surface identical:

- `SpmcRingBuffer::<T, CAP, N>::new()`  (CAP must be a power of two — enforced by a `const` assertion)
- `get_new_producer() -> Option<Producer<T, CAP, N>>`  (Some once; None if a producer already exists)
- `get_new_consumer() -> Option<Consumer<T, CAP, N>>`  (Some while < N consumers; None beyond)
- `Producer::try_push(&mut self, T) -> Result<(), T>` and `Producer::push(&mut self, T)`
- `Consumer::try_pop(&self) -> Option<T>` and `Consumer::pop(&self) -> T`  (`T: Clone` for fan-out)

And the inspection hooks (behind the `test-hooks` feature):

- `Producer::cached_min_consumer_index(&self) -> usize`  (real, plain read — NOT gated)
- `Producer::true_min_consumer_index(&self) -> usize`  (`test-hooks`, Acquire)
- `Consumer::id(&self) -> usize`  (`test-hooks`)

Route all shared state through the `sync` module and confine the loom/production
divergence to `write_slot`/`read_slot`. Build production with
`--no-default-features` so the `test-hooks` methods don't exist in your shipped code.
```
