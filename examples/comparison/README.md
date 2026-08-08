# Comparison performance suite

Head-to-head comparison of lock-free `SpmcRingBuffer` vs locked
`SyncRingBuffer`. `--backend both` is the default so both are always run and
tagged `spmc` / `sync` in the CSV for side-by-side plotting.

## Scenarios

Three groups, 17 scenarios total.

### Group E — re-implemented original-suite scenarios (timed)

Each scenario re-runs the workload shape of a correctness test from `tests/`
through the harness, on **both** backends. The correctness invariant (every
consumer sees every item, in order, no gaps) is verified lazily on every pop
by the runner (see `src/bench/worker.rs`).

| Scenario | Re-implements | Why it's a comparison test |
|---|---|---|
| `cmp_fanout_4consumers` | `functional::fan_out_…` + `stress::stress_four_consumers_full_fanout` | **The headline.** Lock-free: N concurrent readers. Locked: N readers serialised behind one mutex. Per-consumer throughput should diverge hardest here. |
| `cmp_uneven_consumer_speeds` | `stress::stress_uneven_consumer_speeds` | One dawdling consumer pins the slowest-read; tests slow-path + min-scan behaviour. |
| `cmp_tiny_capacity_high_contention` | `stress::stress_tiny_capacity_high_contention` (CAP=4) | Forces slow path on nearly every push. Lazy cache should give lock-free a win. |
| `cmp_slowest_consumer_gates` | `functional::slowest_consumer_gates_the_buffer` | Pure backpressure. Tests spin (lock-free) vs block (locked). |
| `cmp_wrap_around_sustained` | `functional::wrap_around_many_times` + `model::regression_interleaved_wrap` | Steady-state fast path over many laps. The lazy-cache payoff: lock-free fast path has zero Acquire on consumers. |
| `cmp_blocking_push_unblocks` | `stress::blocking_push_unblocks_when_consumer_frees_a_slot` | The spin-vs-block test. **Both throughput and push-fail latency are recorded** so the trade-off (lock-free wins latency, possibly loses CPU-efficiency) is visible. |

### Group F — N-scaling (CAP=1024, Balanced, u64)

`N ∈ {1, 2, 4, 8, 16}`. The headline divergence chart: per-consumer throughput
vs N for both backends on shared axes. Lock-free should be ~flat (independent
cursors, concurrent reads); locked should drop as ~1/N (mutex serialisation).

> **Note on the pacing model at high N:** `Ratio::Balanced` paces the producer
> at `N × base` attempts/sec (one producer thread serving N consumers). At
> high N this exceeds what one thread can sustain, so the producer saturates
> and the fail rate rises. This is an honest characteristic of SPMC (one
> producer must serve all N consumers) and a known limitation of a fixed
> `base` rate — see `report/README.md` for the pacing model and its caveats.

### Group G — capacity-scaling (N=4, Balanced, u64)

`CAP ∈ {4, 16, 64, 256, 1024, 4096}`. Where the lazy cache pays off (large
CAP, fast path dominant) vs where it can't (tiny CAP, slow path on nearly
every push). The crossover is the interesting point.

## Usage

```bash
# Full suite, both backends (default), 5 trials × 1s each:
cargo run --release --features bench --example comparison

# One backend only:
cargo run --release --features bench --example comparison -- --backend spmc

# A subset (substring match on scenario name):
cargo run --release --features bench --example comparison -- --scenarios cmp_n_

# Faster smoke run:
cargo run --release --features bench --example comparison -- \
    --trials 2 --duration 0.2 --warmup 50 --idle-gap 30
```

### CLI flags

Same as the isolation suite, except `--backend` defaults to `both` and the
output paths default to `results/comparison.*.csv`. See
`examples/isolation/README.md` for the full flag reference.

## Output

Two CSV files (see `report/README.md` for the schema):

* **Raw** — one row per scenario × backend × trial × thread.
* **Summary** — one row per scenario × backend × role, aggregated across
  trials (mean/median/min/max throughput, mean/median latency).

The report script (`report/generate_report.py`) reads the summary CSV and
plots both backends on shared axes for the comparison charts.
