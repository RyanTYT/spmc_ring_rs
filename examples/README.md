# Example binaries — performance suites

Two example binaries, both gated behind the `bench` feature:

| Example | Default backend | Purpose |
|---|---|---|
| `isolation` | `spmc` | Characterise a single backend across varying inputs (rate-ratio, capacity, N, payload) |
| `comparison` | `both` | Head-to-head lock-free vs locked, including re-implemented original-suite scenarios |

Both produce a raw CSV (per trial × thread) and a summary CSV (per role,
aggregated across trials with mean/median/min/max). The report script
(`report/generate_report.py`) reads the summary CSVs and plots the charts.

## Quick start

```bash
# Full run (both suites + report):
./report/run_all.sh

# Just one suite:
cargo run --release --features bench --example isolation -- --backend both
cargo run --release --features bench --example comparison

# A subset (substring match on scenario name):
cargo run --release --features bench --example isolation -- --scenarios rate_
cargo run --release --features bench --example comparison -- --scenarios cmp_n_
```

See each suite's README for its scenario list and CLI flags:
* [`isolation/README.md`](isolation/README.md) — Groups A–D (23 scenarios)
* [`comparison/README.md`](comparison/README.md) — Groups E–G (17 scenarios)

## Shared CLI flags

| Flag | isolation default | comparison default | Purpose |
|---|---|---|---|
| `--backend` | `spmc` | `both` | Which backend(s) to benchmark |
| `--trials` | `5` | `5` | Measured trials per scenario |
| `--duration` | `1.0` | `1.0` | Per-trial window (seconds) |
| `--warmup` | `300` | `300` | Warmup before each trial (ms) |
| `--idle-gap` | `150` | `150` | Cooldown between trials (ms) |
| `--out` | `results/isolation.raw.csv` | `results/comparison.raw.csv` | Raw CSV path |
| `--summary` | `results/isolation.summary.csv` | `results/comparison.summary.csv` | Summary CSV path |
| `--scenarios` | (all) | (all) | Substring filter on scenario name |
| `--no-pin` | (no-op) | (no-op) | Accepted for forward compatibility |

## Measurement methodology

See [`report/README.md`](../report/README.md) for the full methodology:
batched fast-path timing, per-op slow-path timing, padded per-thread state,
spin-pacing, and what the percentiles actually mean.
