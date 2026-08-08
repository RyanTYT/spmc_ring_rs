#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_all.sh — build + run both benchmark binaries + generate the report.
#
# Idempotent: overwrites results/ each run. Edit the variables below to tune
# the run (trials, duration, etc.), or pass them through as env vars.
#
# Usage:
#   ./report/run_all.sh                 # full run (defaults below)
#   TRIALS=3 DURATION=0.5 ./report/run_all.sh   # quick run
# ---------------------------------------------------------------------------
set -euo pipefail

# --- config (overridable via env) -----------------------------------------
TRIALS="${TRIALS:-5}"
DURATION="${DURATION:-1.0}"
WARMUP="${WARMUP:-300}"
IDLE_GAP="${IDLE_GAP:-150}"
BACKEND="${BACKEND:-both}"          # isolation backend (comparison always uses both)

# --- paths ----------------------------------------------------------------
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/results"
CHARTS="$RESULTS/charts"

# Cargo env (so subshells find cargo if not on PATH)
export PATH="$HOME/.cargo/bin:$PATH"

cd "$ROOT"

echo "=== 1/4 Build (release, bench feature) ==="
cargo build --release --features bench --examples

echo "=== 2/4 Isolation suite (backend=$BACKEND, trials=$TRIALS, duration=${DURATION}s) ==="
cargo run --release --features bench --example isolation -- \
    --backend "$BACKEND" \
    --trials "$TRIALS" --duration "$DURATION" \
    --warmup "$WARMUP" --idle-gap "$IDLE_GAP" \
    --out "$RESULTS/isolation.raw.csv" \
    --summary "$RESULTS/isolation.summary.csv"

echo "=== 3/4 Comparison suite (both backends, trials=$TRIALS, duration=${DURATION}s) ==="
cargo run --release --features bench --example comparison -- \
    --trials "$TRIALS" --duration "$DURATION" \
    --warmup "$WARMUP" --idle-gap "$IDLE_GAP" \
    --out "$RESULTS/comparison.raw.csv" \
    --summary "$RESULTS/comparison.summary.csv"

echo "=== 4/4 Report ==="
python3 "$ROOT/report/generate_report.py" \
    --results-dir "$RESULTS" \
    --charts-dir "$CHARTS"

echo
echo "=== Done ==="
echo "  raw CSVs      → $RESULTS/*.raw.csv"
echo "  summary CSVs  → $RESULTS/*.summary.csv"
echo "  charts        → $CHARTS/*.png"
echo "  summary       → $RESULTS/summary.md"
