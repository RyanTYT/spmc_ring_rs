# Isolation performance suite

Characterises a **single** ring-buffer backend across varying inputs. Default
backend is `spmc` (lock-free); pass `--backend both` to also collect `sync`
numbers for inline comparison (the comparison suite is the dedicated head-to-
head, but `--backend both` here lets you see both on the same scenarios).

## Scenarios

Four groups, 23 scenarios total. Each is monomorphised to a concrete
`(CAP, N, payload)` by `define_scenario!` in `scenarios.rs`.

### Group A — rate-ratio sweep (CAP=1024, N=4, u64)

Varies the producer : consumer attempt-rate ratio. The core of the suite.

| Scenario | Ratio | What it stresses |
|---|---|---|
| `rate_max` | Max | Both flat-out; the ceiling |
| `rate_balanced` | Balanced | Steady-state fast path (baseline) |
| `rate_prod_2x` … `rate_prod_16x` | Prod(k) | Fast producer → buffer fills → `try_push` slow path (lazy cache refresh, Acquire scan, retry) |
| `rate_cons_2x`, `rate_cons_4x` | Cons(k) | Fast consumers → buffer empties → `try_pop` None-path spin |

**Expected story:** as `k` grows for `Prod(k)`, push throughput plateaus (the
excess attempts become `Err` returns) while push-fail rate rises; the lazy
cache means the lock-free producer's fast path stays cheap until the buffer
fills. For `Cons(k)`, pop throughput plateaus and pop-fail (None) rate rises.

### Group B — capacity sweep (N=4, Max, u64)

Varies `CAP ∈ {1, 2, 4, 16, 64, 256, 1024, 4096}`. Tiny CAP forces the slow
path on nearly every push (the lazy cache refreshes constantly); large CAP
keeps the fast path dominant (the lazy cache pays off — zero Acquire loads on
consumers in steady state).

### Group C — consumer-count sweep (CAP=1024, Balanced, u64)

Varies `N ∈ {1, 2, 4, 8, 16}`. Per-consumer throughput should stay ~flat as N
grows (independent cursors, concurrent slot reads) — the **fan-out parallelism
proof**.

### Group D — payload size (CAP=1024, N=4, Balanced)

| Scenario | Payload | Bytes |
|---|---|---|
| `payload_u64` | `u64` | 8 |
| `payload_u64x8` | `[u64; 8]` | 64 |

Factors buffer-mechanics cost (8-byte) from clone/copy cost (64-byte).

## Usage

```bash
# Full suite, spmc only (default), 5 trials × 1s each:
cargo run --release --features bench --example isolation

# Both backends (for inline comparison):
cargo run --release --features bench --example isolation -- --backend both

# A subset (substring match on scenario name):
cargo run --release --features bench --example isolation -- --scenarios rate_

# Faster smoke run:
cargo run --release --features bench --example isolation -- \
    --trials 2 --duration 0.2 --warmup 50 --idle-gap 30
```

### CLI flags

| Flag | Default | Purpose |
|---|---|---|
| `--backend spmc\|sync\|both` | `spmc` | Which backend(s) to benchmark |
| `--trials N` | `5` | Measured trials per scenario (aggregated mean/median) |
| `--duration SECS` | `1.0` | Per-trial measured-window duration (seconds) |
| `--warmup MS` | `300` | Warmup duration before each trial (milliseconds) |
| `--idle-gap MS` | `150` | Cooldown between trials (mitigates thermal taint from spin-pacing) |
| `--out PATH` | `results/isolation.raw.csv` | Raw CSV (one row per trial × thread) |
| `--summary PATH` | `results/isolation.summary.csv` | Summary CSV (one row per role, aggregated across trials) |
| `--scenarios SUBSTR` | (all) | Run only scenarios whose name contains the substring |
| `--no-pin` | (no-op) | Accepted for forward compatibility; pinning is opt-in via the `bench-pin` feature (deferred) |

## Output

Two CSV files (see `report/README.md` for the full schema):

* **Raw** — one row per scenario × backend × trial × thread. Full audit.
* **Summary** — one row per scenario × backend × role, with `tput_mean`,
  `tput_median`, `tput_min`, `tput_max`, `fail_rate_mean`, and latency
  `p50`/`p99` mean + median across trials.

See the top-level `report/README.md` for the measurement methodology (batched
fast-path timing, per-op slow-path timing, padded per-thread state, spin-
pacing, and what the percentiles actually mean).
