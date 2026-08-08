# Report — measurement methodology & caveats

This document explains **what the numbers in the charts and CSVs actually
mean**, and the measurement-taint mitigations the harness applies. Read this
before drawing conclusions from the report.

## Quick start

```bash
# Full run (defaults: 5 trials × 1s each, both backends for comparison):
./report/run_all.sh

# Quick smoke run:
TRIALS=2 DURATION=0.2 ./report/run_all.sh

# Just regenerate the report from existing CSVs:
python3 report/generate_report.py
```

Outputs land in `results/`:
* `*.raw.csv` — one row per scenario × backend × trial × thread (full audit)
* `*.summary.csv` — one row per scenario × backend × role (aggregated)
* `charts/*.png` — the charts
* `summary.md` — a text digest of the numbers behind the charts

## Measurement methodology

### 1. Throughput is the headline (zero per-op timing)

Throughput = `ok_ops / window_seconds`, where `ok_ops` is a **local `u64`
counter** incremented in the hot loop (no shared atomic — no cache-line
bouncing), and `window_seconds` is the per-thread measured window
(`window_end − window_start`, captured per-thread at the stop flag). **No
`Instant::now()` is called per op.** This is the trustworthy number; latency
is secondary.

### 2. Fast-path latency is batched (not per-op)

The fast path of a lock-free `try_push`/`try_pop` is ~5-20 ns; a single
`Instant::now()` costs ~20-40 ns, so timing every op would **double** the
measured latency and slow the thread (changing the contention pattern under
test). Instead, the harness times a **batch** of 512 consecutive successes
with one `Instant::now()` at each end and records `batch_ns / 512` as a single
batch-mean. Overhead ≈ 50 ns per 512 ops ≈ 0.1 ns/op for the batch-seal
`Instant` calls — negligible.

**What the percentiles mean:** `lat_p50` / `lat_p99` are the p50/p99 of
**batch-means**, not per-op latencies. A batch-mean is the average latency of
512 consecutive fast-path ops; its p50 is the "typical batch" and its p99 is
the "worst 1% of batches." This is honest about what's measurable with
`Instant` and is directly traceable to the data. **Do not interpret `lat_p99`
as a per-op p99** — that would require ~20-40 ns of `Instant` overhead per op,
which would taint the very thing being measured.

### 2a. Total fast-path measurement overhead per op

The batch-seal `Instant` calls (above) are ~0.1 ns/op amortized, but they are
NOT the only per-op cost. The full fast-path overhead per op is:

| Component | Cost | Avoidable? |
|---|---|---|
| `ok_ops += 1` counter write (L1, padded) | ~1–2 ns | No — this IS the throughput measurement |
| `stop.load(Relaxed)` (window-end check) | ~0.3–1 ns | Technically yes (every 64 ops), but window-overshoot tradeoff isn't worth it |
| `batch_count += 1` + predicted branch | ~0.3 ns | No — needed for batch sealing |
| Batch-seal `elapsed()` + `Instant::now()` | ~52 ns / 512 ops | Amortized ~0.1 ns/op |
| **Total fast-path overhead** | **~2–3 ns/op** | |

For a 10 ns lock-free fast-path op, that's ~20–30% overhead. For a 30 ns
locked fast-path op, ~7–10%. This overhead is **equal for both backends**
(same harness code path), so the *relative* comparison is fair; the
*absolute* throughput numbers are ~10-25% lower than an unmeasured run would
show. There is no way to measure throughput without counting ops — the
counter write is irreducible.

### 3. Slow-path latency is per-op (honest because ops are µs-scale)

When `try_push` returns `Err` (full) or `try_pop` returns `None` (empty), the
op took the slow path (Acquire scan + retry for the lock-free impl; mutex wait
for the locked impl). Those ops are µs-scale, so ~40 ns of `Instant` overhead
is <1% and per-op timing is legitimate. To avoid *any* `Instant` on the fast
path, the slow-path latency is recorded as the **inter-fail interval**: on
each fail we take `Instant::now()`; if the previous op was also a fail, the
interval since it is recorded as one slow-path sample. The fast path
(consecutive successes) touches no `Instant` at all.

`fail_lat_p50` / `fail_lat_p99` are the p50/p99 of these inter-fail intervals
— **per-op, honest** — and are the metric that shows the lock-free vs locked
divergence under backpressure (spin vs block).

### 4. Padded per-thread state (no false sharing)

Each thread's measurement state (`PushStats` / `PopStats`) is wrapped in
`Padded<T>` (`#[repr(C, align(128))]`), so adjacent threads' stats don't share
a cache line. 128 bytes (not 64) defeats adjacent-line hardware prefetchers.
Without this, false sharing between measurement state alone can flip a
lock-free-vs-locked comparison.

### 5. Barrier-synchronised start, per-thread stop

All worker threads wait on a `Barrier` at the warmup→measure boundary, then
reset their local counters and capture `window_start` simultaneously. The
coordinator sets a stop `AtomicBool` after `duration`; each thread notices it
on its next op and captures its own `window_end`. Per-thread windows handle
the case where threads notice the stop flag at slightly different instants.

### 6. Warmup is discarded; sequence numbers are continuous

Each trial runs a warmup phase (counters ignored, `--warmup` ms) to fill
instruction/data caches and stabilise branch predictors. The **sequence
numbers are continuous across warmup and measure** (the producer's `seq` and
each consumer's `expected` do NOT reset at the barrier — only the stats
counters do). This keeps the published stream consistent with what consumers
expect, so the per-pop correctness verification works throughout.

### 7. Multiple trials + mean **and** median

Each scenario runs `--trials` (default 5) measured windows. The summary CSV
reports **both**:
* `tput_mean` — sensitive to tails (shows OS-preemption impact)
* `tput_median` — resistant to OS-preemption outliers (the headline)
* `tput_min` / `tput_max` — visible spread

Latency p50/p99 are also reported as both mean and median across trials. The
report script plots the **median** (the headline); the mean is in the CSV for
those who want to see tail sensitivity.

### 8. Spin-pacing (not sleep) + idle gap

Rate-ratio scenarios pace the fast side to `k × base` attempts/sec using
`spin_loop` (not `thread::sleep`, which overshoots by 100µs–1ms and makes
precise ratios impossible). Spin-pacing burns CPU; the `--idle-gap` (default
150 ms) between trials lets the CPU cool, mitigating thermal throttling.

## The rate-ratio model for SPMC fan-out

In SPMC fan-out, every consumer pops the *same* stream the producer pushes, so
aggregate consumer rate = producer rate in steady state. The meaningful
imbalance axis is the **per-consumer** target attempt rate vs the producer's:

| `Ratio` | Producer target | Each consumer target | What happens |
|---|---|---|---|
| `Max` | flat-out | flat-out | The ceiling; no pacing |
| `Balanced` | `N × base` | `base` | Steady-state fast path (consumers keep up) |
| `Prod(k)` | `k × N × base` | `base` | Producer outpaces → buffer fills → `Err` |
| `Cons(k)` | `N × base` | `k × base` | Consumers outpace → buffer empties → `None` |

`base` is a conservative fixed 2 M attempts/sec (see `src/bench/runner.rs`
`BASE_RATE`). This is below the per-op cost of either backend on modern
hardware, so the slow side paces (spins) rather than being limited by the
buffer, and the fast side's `k × base` is achievable up to `k` ≈ 5–10 before
it saturates.

### Known limitation: high N under `Balanced`

`Ratio::Balanced` paces the producer at `N × base` attempts/sec (one producer
thread serving N consumers). At high N (e.g. N=16), this is 32 M attempts/sec
on one thread — beyond what one thread can sustain. The producer saturates
and the fail rate rises. This is an **honest characteristic of SPMC** (one
producer must serve all N consumers) and a known limitation of a fixed
`base`. A per-host calibration run could replace `BASE_RATE`; the fixed value
keeps results comparable across runs. **When reading the N-scaling charts,
note that `Balanced` at high N stresses the producer more than the consumers.**

## What the CSV columns mean

### Raw CSV (one row per trial × thread)

| Column | Meaning |
|---|---|
| `scenario`, `backend`, `trial`, `role`, `thread_id` | identifiers |
| `capacity`, `num_consumers`, `ratio`, `payload_bytes` | config |
| `ok_ops`, `fail_ops` | raw counts (fast-path successes / slow-path fails) |
| `window_ns` | per-thread measured window (ns) |
| `throughput_ops_s` | `ok_ops / window_ns × 1e9` |
| `fail_rate_pct` | `fail_ops / (ok_ops + fail_ops) × 100` |
| `lat_p50_ns`, `lat_p99_ns`, `lat_max_ns` | fast-path **batched-mean** percentiles (see §2) |
| `fail_lat_p50_ns`, `fail_lat_p99_ns` | slow-path **per-op** percentiles (see §3) |

### Summary CSV (one row per scenario × backend × role)

Same identifiers + config, plus `trials`, then the across-trial aggregations:
`tput_mean`, `tput_median`, `tput_min`, `tput_max`, `fail_rate_mean`, and the
latency `p50`/`p99` mean **and** median across trials.

## Chart inventory

### Isolation (per backend)
1. `iso_rate_ratio_tput_<be>.png` — throughput vs rate-ratio (push + total pop + fail-rate bars)
2. `iso_rate_ratio_lat_<be>.png` — fast-path latency vs rate-ratio (log-y)
3. `iso_capacity_tput_<be>.png` — throughput vs capacity (log-x)
4. `iso_n_scaling_<be>.png` — per-consumer throughput vs N (fan-out parallelism proof)
5. `iso_payload_<be>.png` — payload size comparison (8B vs 64B)

### Comparison (both backends)
6. `cmp_scenarios_tput.png` — per-scenario push + pop throughput (grouped bar, spmc vs sync)
7. `cmp_n_scaling.png` — per-consumer + total pop throughput vs N (the headline divergence)
8. `cmp_capacity_tput.png` — push throughput vs capacity (both backends, log-x)
9. `cmp_matrix.png` — lock-free / locked ratio heatmap per scenario × metric

## Caveats (read before trusting any number)

* **macOS advisory pinning.** Core-affinity pinning is opt-in (`bench-pin`
  feature, deferred) and advisory-only on macOS. The harness does not pin by
  default; numbers are *more comparable* than absolute. On Apple Silicon, be
  wary of P-core vs E-core asymmetry if you enable pinning.
* **Thermal throttling.** Spin-pacing burns CPU. For 1s windows with 150 ms
  idle gaps this is small, but a long full-suite run can thermally throttle a
  laptop, dropping clock speed and tainting later trials. The idle gap is the
  mitigation; if you shorten it, watch for declining throughput across trials.
* **`Instant` overhead.** ~20-40 ns per call on modern hardware. The harness
  avoids it on the fast path (batched — ~0.1 ns/op amortized) but the total
  fast-path measurement overhead is ~2-3 ns/op (counter writes + stop-check +
  amortized batch seal). See report methodology §2a. Slow-path uses `Instant`
  per fail (legitimate at µs scale).
* **`lat_p99` is batched, not per-op.** See §2. Do not compare it to per-op p99
  numbers from other harnesses that time every op (those numbers are tainted
  by the clock).
* **The locked backend is a stand-in.** `SyncRingBuffer` is a straightforward
  `Mutex`-guarded impl. Swap in your real locked impl (keeping the same
  signatures) and the numbers update. The *relative* comparison is the point,
  not the absolute locked numbers.
* **Single host, single run.** Numbers are from one machine. Use the relative
  comparison (spmc vs sync) as the signal; absolute throughput varies by host.
