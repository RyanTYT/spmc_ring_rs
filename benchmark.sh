#!/usr/bin/env bash
#
# bench_matrix.sh — interactive build/run/profile harness for ring_bench.
#
# 1. Builds ring_bench (release) once.
# 2. Asks which capacities / consumer counts / item sizes / item counts to
#    exercise, then runs the cartesian product of your selections.
# 3. Each run is measured with `perf stat` (falls back to /usr/bin/time -v,
#    then to a plain wall-clock `time` if neither perf nor GNU time is
#    available).
# 4. Optionally runs one extra --check correctness pass per combination
#    (not perf-measured — correctness and profiling passes are kept
#    separate on purpose).
# 5. Prints a summary table at the end and saves everything (raw perf/
#    program logs + a TSV summary) under bench_results/<timestamp>/.
#
# Run from the crate root (wherever Cargo.toml lives).
#
# CARGO_TARGET_KIND below controls whether ring_bench is built via
# `cargo build --bin ring_bench` or `cargo build --example ring_bench`.
# It defaults to "example" because clap/crossterm/ratatui/thread-priority
# are typically kept as [dev-dependencies] for a bench-only harness like
# this — and dev-dependencies are only linked into test, bench, and
# EXAMPLE targets, never into a plain [[bin]] target. If you promote those
# crates to real [dependencies] and keep ring_bench.rs under src/bin/,
# switch this to "bin".

set -uo pipefail

BIN_NAME="benchmark"
CARGO_TARGET_KIND="example"   # "example" (dev-dependencies OK) or "bin" (needs [dependencies])
SUPPORTED_CAPACITIES=(1024 4096 16384 65536 262144)
SUPPORTED_CONSUMERS=(1 2 4 8)
DEFAULT_NUM_ITEMS=1000000
DEFAULT_ITEM_SIZE=64
DEFAULT_PERF_EVENTS="task-clock,cycles,instructions,cache-references,cache-misses,branch-instructions,branch-misses,context-switches"

case "$CARGO_TARGET_KIND" in
    example)
        CARGO_TARGET_FLAG="--example"
        BIN_PATH="target/release/examples/$BIN_NAME"
        ;;
    bin)
        CARGO_TARGET_FLAG="--bin"
        BIN_PATH="target/release/$BIN_NAME"
        ;;
    *)
        echo "error: CARGO_TARGET_KIND must be 'example' or 'bin' (got '$CARGO_TARGET_KIND')" >&2
        exit 1
        ;;
esac

# ---------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------

die() {
    echo "error: $*" >&2
    exit 1
}

info() {
    echo ">> $*"
}

# select_multi OUT_ARRAY_NAME "prompt" opt1 opt2 opt3 ...
# Prints a numbered list, reads a selection ("all", or space/comma
# separated indices, e.g. "1 3 4"), writes the chosen values into the
# array named by OUT_ARRAY_NAME.
#
# Deliberately avoids `local -n` namerefs (bash 4.3+ only) so this still
# works under macOS's default /bin/bash, which is stuck on 3.2 for
# licensing reasons. Assignment into the caller's array is done via `eval`
# with each value safely quoted through `printf %q` instead.
select_multi() {
    local out_name="$1"
    local prompt="$2"
    shift 2
    local options=("$@")

    echo
    echo "$prompt"
    local i
    for i in "${!options[@]}"; do
        printf "  [%d] %s\n" "$((i + 1))" "${options[$i]}"
    done
    echo "  Enter numbers separated by spaces, or 'all'."

    local reply
    read -r -p "> " reply
    reply="${reply//,/ }"

    local selected=()
    if [[ -z "$reply" || "$reply" == "all" || "$reply" == "a" ]]; then
        selected=("${options[@]}")
    else
        local idx
        for idx in $reply; do
            if [[ "$idx" =~ ^[0-9]+$ ]] && (( idx >= 1 && idx <= ${#options[@]} )); then
                selected+=("${options[$((idx - 1))]}")
            else
                echo "  (ignoring invalid selection: $idx)" >&2
            fi
        done
        if [[ ${#selected[@]} -eq 0 ]]; then
            echo "  No valid selections — using all options." >&2
            selected=("${options[@]}")
        fi
    fi

    local assign="$out_name=("
    local val
    for val in "${selected[@]}"; do
        assign="$assign $(printf '%q' "$val")"
    done
    assign="$assign)"
    eval "$assign"
}

ask_default() {
    # ask_default "prompt" default_value -> echoes chosen value
    local prompt="$1" default_value="$2" reply
    read -r -p "$prompt [$default_value]: " reply
    echo "${reply:-$default_value}"
}

ask_yes_no() {
    # ask_yes_no "prompt" default(y/n) -> returns 0 for yes, 1 for no
    local prompt="$1" default="${2:-n}" reply
    local hint="y/N"
    [[ "$default" == "y" ]] && hint="Y/n"
    read -r -p "$prompt [$hint]: " reply
    reply="${reply:-$default}"
    [[ "$reply" =~ ^[Yy] ]]
}

# ---------------------------------------------------------------------
# 0. sanity checks
# ---------------------------------------------------------------------

command -v cargo >/dev/null 2>&1 || die "cargo not found on PATH"
[[ -f Cargo.toml ]] || die "no Cargo.toml here — run this from your crate root"

# ---------------------------------------------------------------------
# Measurement backend detection.
#
# `perf stat` is Linux-only — there's no direct equivalent on macOS. In
# order of preference this script uses:
#   1. perf              (Linux)            — cycles/instructions/IPC/cache/branch stats.
#   2. hyperfine          (any OS, optional) — `brew install hyperfine` /
#                          `cargo install hyperfine`. Runs the command
#                          several times and reports statistically robust
#                          wall-clock timing (mean/stddev/min/max). No
#                          hardware counters.
#   3. /usr/bin/time -l   (macOS, built in)  — wall/user/sys time, max RSS,
#                          page faults, context switches. On Apple Silicon
#                          it *sometimes* also reports "instructions
#                          retired" / "cycles elapsed" (varies by macOS
#                          version — Intel Macs generally don't have this).
#                          Picked up automatically when present, N/A
#                          otherwise.
#   4. /usr/bin/time -v   (Linux, GNU coreutils, no perf installed)
#   5. plain wall-clock timing as a last resort.
#
# hyperfine and time -l/-v are complementary, not exclusive — when both
# are available this script uses hyperfine for the timing numbers and a
# separate time -l/-v pass for RSS/cycles/instructions.
#
# If you specifically need perf-style hardware counters (cycles,
# instructions, cache misses, IPC) on macOS and time -l isn't giving them
# to you, your options outside this script are:
#   - Xcode Instruments' "CPU Counters" template, e.g.
#       xctrace record --template "CPU Counters" --launch -- \
#           ./target/release/examples/ring_bench --capacity 4096 ...
#     inspected in Instruments.app or via `xctrace export`.
#   - poop (https://github.com/andrewrk/poop) — cross-platform perf-stat-
#     alike that reads hardware counters via macOS's private kperf API.
#     Needs `sudo` and can be finicky depending on SIP/entitlements.
# ---------------------------------------------------------------------

OS_NAME=$(uname -s 2>/dev/null || echo unknown)
HAVE_PERF=0
HAVE_HYPERFINE=0
HAVE_TIME_L=0   # macOS/BSD `time -l`
HAVE_TIME_V=0   # GNU `time -v`
HYPERFINE_RUNS=5

if command -v perf >/dev/null 2>&1; then
    HAVE_PERF=1
fi

if command -v hyperfine >/dev/null 2>&1; then
    HAVE_HYPERFINE=1
fi

if [[ "$HAVE_PERF" -eq 0 ]]; then
    if [[ "$OS_NAME" == "Darwin" ]]; then
        if [[ -x /usr/bin/time ]]; then
            HAVE_TIME_L=1
        fi
        if [[ "$HAVE_HYPERFINE" -eq 0 ]]; then
            echo "note: no 'perf' on macOS (expected) and no 'hyperfine' found." >&2
            echo "      Falling back to /usr/bin/time -l only. For statistically robust" >&2
            echo "      timing, consider: brew install hyperfine" >&2
        fi
    else
        if /usr/bin/time -v true >/dev/null 2>&1; then
            HAVE_TIME_V=1
        fi
        if [[ "$HAVE_HYPERFINE" -eq 0 && "$HAVE_TIME_V" -eq 0 ]]; then
            echo "note: neither 'perf' nor '/usr/bin/time -v' nor 'hyperfine' found —" >&2
            echo "      falling back to plain wall-clock timing." >&2
        fi
    fi
fi

# hires_now: portable-ish high resolution timestamp for the last-resort
# fallback timer. `date +%s.%N` only works with GNU date — BSD date (the
# default on macOS) silently prints a literal "N" instead of nanoseconds,
# so we detect that and fall back to perl's Time::HiRes (bundled with
# macOS) instead.
hires_now() {
    local t
    t=$(date +%s.%N 2>/dev/null)
    if [[ -z "$t" || "$t" == *N* ]]; then
        if command -v perl >/dev/null 2>&1; then
            perl -MTime::HiRes=time -e 'printf "%.9f\n", time'
        else
            date +%s
        fi
    else
        echo "$t"
    fi
}

# ---------------------------------------------------------------------
# 1. build
# ---------------------------------------------------------------------

info "Building $BIN_NAME (release, $CARGO_TARGET_KIND target)..."
cargo build --release "$CARGO_TARGET_FLAG" "$BIN_NAME" || die "cargo build failed"

[[ -x "$BIN_PATH" ]] || die "expected binary not found at $BIN_PATH"

# ---------------------------------------------------------------------
# 2. interactive selection
# ---------------------------------------------------------------------

echo
echo "=== ring_bench matrix runner ==="

select_multi CHOSEN_CAPACITIES "Ring buffer capacities to test:" "${SUPPORTED_CAPACITIES[@]}"
select_multi CHOSEN_CONSUMERS "Consumer thread counts to test:" "${SUPPORTED_CONSUMERS[@]}"

NUM_ITEMS=$(ask_default "Number of items to push per run" "$DEFAULT_NUM_ITEMS")
ITEM_SIZE=$(ask_default "Item payload size in bytes" "$DEFAULT_ITEM_SIZE")

RUN_CHECK=0
if ask_yes_no "Also run a --check correctness pass for each combination? (not perf-measured)" "n"; then
    RUN_CHECK=1
fi

PERF_EVENTS="$DEFAULT_PERF_EVENTS"
if [[ "$HAVE_PERF" -eq 1 ]]; then
    PERF_EVENTS=$(ask_default "perf events to record (comma separated)" "$DEFAULT_PERF_EVENTS")
fi

TOTAL_RUNS=$(( ${#CHOSEN_CAPACITIES[@]} * ${#CHOSEN_CONSUMERS[@]} ))
echo
echo "About to run $TOTAL_RUNS combination(s):"
echo "  capacities:      ${CHOSEN_CAPACITIES[*]}"
echo "  consumer counts: ${CHOSEN_CONSUMERS[*]}"
echo "  num_items:       $NUM_ITEMS"
echo "  item_size:       $ITEM_SIZE"
echo "  check pass:      $([[ $RUN_CHECK -eq 1 ]] && echo yes || echo no)"
ask_yes_no "Proceed?" "y" || die "aborted by user"

# ---------------------------------------------------------------------
# 3. run
# ---------------------------------------------------------------------

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="bench_results/$TIMESTAMP"
mkdir -p "$RESULTS_DIR"
SUMMARY_TSV="$RESULTS_DIR/summary.tsv"

# printf "capacity\tconsumers\tnum_items\titem_size\tproducer_items_per_sec\tavg_consumer_items_per_sec\telapsed_sec\tcycles\tinstructions\tipc\tcache_miss_pct\tbranch_miss_pct\tcheck\n" \
#     > "$SUMMARY_TSV"
printf "capacity\tconsumers\tnum_items\titem_size\tproducer_items_per_sec\tavg_consumer_items_per_sec\telapsed_sec\tcycles\tinstructions\tipc\tcache_miss_pct\tbranch_miss_pct\tmax_rss_kb\tcheck\n" \
    > "$SUMMARY_TSV"

# extract_num "pattern" file -> first number matching pattern (perf's
# thousands separators stripped), or "N/A"
extract_num() {
    local pattern="$1" file="$2" line
    line=$(grep -m1 -E "$pattern" "$file" 2>/dev/null) || true
    [[ -z "$line" ]] && { echo "N/A"; return; }
    echo "$line" | grep -oE '[0-9][0-9,.]*' | head -1 | tr -d ','
}

extract_pct() {
    # pulls the "X.XX% of all Y" style trailing percentage on a perf line
    local pattern="$1" file="$2" line
    line=$(grep -m1 -E "$pattern" "$file" 2>/dev/null) || true
    [[ -z "$line" ]] && { echo "N/A"; return; }
    echo "$line" | grep -oE '[0-9]+\.[0-9]+ ?%' | head -1 | tr -d ' %'
}

# extract_time_field "pattern" file -> total seconds parsed out of GNU/BSD
# time's "[h:]mm:ss[.ss]" or plain-seconds elapsed-time formats.
extract_time_field() {
    local pattern="$1" file="$2" line val
    line=$(grep -m1 -E "$pattern" "$file" 2>/dev/null) || true
    [[ -z "$line" ]] && { echo "N/A"; return; }
    val=$(echo "$line" | grep -oE '[0-9]+(:[0-9.]+)+|[0-9]+\.[0-9]+')
    [[ -z "$val" ]] && { echo "N/A"; return; }
    awk -F: -v v="$val" 'BEGIN {
        n = split(v, f, ":")
        if (n == 3) printf "%.3f", f[1]*3600 + f[2]*60 + f[3]
        else if (n == 2) printf "%.3f", f[1]*60 + f[2]
        else printf "%.3f", f[1]
    }'
}

run_one() {
    local cap="$1" cons="$2"
    local tag="cap${cap}_cons${cons}"
    local out_log="$RESULTS_DIR/${tag}.stdout.log"
    local perf_log="$RESULTS_DIR/${tag}.perf.log"
    local check_log="$RESULTS_DIR/${tag}.check.log"

    info "Running capacity=$cap num_consumers=$cons num_items=$NUM_ITEMS item_size=$ITEM_SIZE ..."

    local elapsed="N/A" cycles="N/A" instructions="N/A" ipc="N/A" cmiss="N/A" bmiss="N/A" maxrss="N/A"
    local cmd_args=(--capacity "$cap" --num-consumers "$cons" --num-items "$NUM_ITEMS" --item-size "$ITEM_SIZE")

    if [[ "$HAVE_PERF" -eq 1 ]]; then
        perf stat -e "$PERF_EVENTS" -o "$perf_log" -- \
            "$BIN_PATH" "${cmd_args[@]}" \
            > "$out_log" 2>&1
        elapsed=$(extract_num 'seconds time elapsed' "$perf_log")
        cycles=$(extract_num '^ *[0-9,]+ +cycles' "$perf_log")
        instructions=$(extract_num '^ *[0-9,]+ +instructions' "$perf_log")
        ipc=$(grep -m1 'instructions' "$perf_log" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+ +insn per cycle' | grep -oE '^[0-9]+\.[0-9]+')
        [[ -z "$ipc" ]] && ipc="N/A"
        cmiss=$(extract_pct 'cache-misses' "$perf_log")
        bmiss=$(extract_pct 'branch-misses' "$perf_log")
    else
        # No perf (macOS, or Linux without it installed). hyperfine and
        # time -l/-v are complementary: hyperfine gives statistically
        # robust wall-clock timing over several runs, time -l/-v gives a
        # single run's OS-level resource stats.
        : > "$perf_log"

        if [[ "$HAVE_HYPERFINE" -eq 1 ]]; then
            local hf_json="$RESULTS_DIR/${tag}.hyperfine.json"
            hyperfine --warmup 1 --min-runs "$HYPERFINE_RUNS" --max-runs "$HYPERFINE_RUNS" \
                --export-json "$hf_json" \
                -- "$BIN_PATH ${cmd_args[*]}" \
                >> "$perf_log" 2>&1
            elapsed=$(grep -o '"mean": *[0-9.]*' "$hf_json" 2>/dev/null | head -1 | grep -oE '[0-9.]+')
            [[ -z "$elapsed" ]] && elapsed="N/A"
            # one representative run to capture program stdout for throughput parsing
            "$BIN_PATH" "${cmd_args[@]}" > "$out_log" 2>/dev/null
        fi

        if [[ "$OS_NAME" == "Darwin" && "$HAVE_TIME_L" -eq 1 ]]; then
            local time_log="$RESULTS_DIR/${tag}.time.log"
            /usr/bin/time -l "$BIN_PATH" "${cmd_args[@]}" > "$out_log" 2>"$time_log"
            [[ "$elapsed" == "N/A" ]] && elapsed=$(extract_time_field ' real' "$time_log")
            maxrss=$(extract_num 'maximum resident set size' "$time_log")
            instructions=$(extract_num 'instructions retired' "$time_log")
            cycles=$(extract_num 'cycles elapsed' "$time_log")
            cat "$time_log" >> "$perf_log"
        elif [[ "$HAVE_TIME_V" -eq 1 ]]; then
            local time_log="$RESULTS_DIR/${tag}.time.log"
            /usr/bin/time -v "$BIN_PATH" "${cmd_args[@]}" > "$out_log" 2>"$time_log"
            [[ "$elapsed" == "N/A" ]] && elapsed=$(extract_time_field 'Elapsed \(wall clock\) time' "$time_log")
            maxrss=$(extract_num 'Maximum resident set size' "$time_log")
            cat "$time_log" >> "$perf_log"
        elif [[ "$elapsed" == "N/A" ]]; then
            local start end
            start=$(hires_now)
            "$BIN_PATH" "${cmd_args[@]}" > "$out_log" 2>>"$perf_log"
            end=$(hires_now)
            elapsed=$(awk -v s="$start" -v e="$end" 'BEGIN { printf "%.3f", e - s }')
        fi
    fi

    local producer_tp avg_consumer_tp
    producer_tp=$(grep -m1 '^producer:' "$out_log" | grep -oE '\([0-9]+ items/sec\)' | grep -oE '[0-9]+')
    [[ -z "$producer_tp" ]] && producer_tp="N/A"
    avg_consumer_tp=$(grep '^consumer\[' "$out_log" \
        | grep -oE '[0-9]+ items/sec' | grep -oE '^[0-9]+' \
        | awk '{ sum += $1; n += 1 } END { if (n > 0) printf "%.0f", sum / n; else print "N/A" }')
    [[ -z "$avg_consumer_tp" ]] && avg_consumer_tp="N/A"

    local check_result="skipped"
    if [[ "$RUN_CHECK" -eq 1 ]]; then
        if "$BIN_PATH" "${cmd_args[@]}" --check \
               > "$check_log" 2>&1; then
            check_result="PASS"
        else
            check_result="FAIL"
        fi
    fi

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "$cap" "$cons" "$NUM_ITEMS" "$ITEM_SIZE" \
        "$producer_tp" "$avg_consumer_tp" "$elapsed" \
        "$cycles" "$instructions" "$ipc" "$cmiss" "$bmiss" "$maxrss" "$check_result" \
        >> "$SUMMARY_TSV"
}

for cap in "${CHOSEN_CAPACITIES[@]}"; do
    for cons in "${CHOSEN_CONSUMERS[@]}"; do
        run_one "$cap" "$cons"
    done
done

# ---------------------------------------------------------------------
# 4. summary
# ---------------------------------------------------------------------

echo
echo "=== Results ==="

# Formats TSV columns into vertical key-value blocks per test run
awk -F'\t' '
NR == 1 {
    for (i = 1; i <= NF; i++) headers[i] = $i
    next
}
{
    print "----------------------------------------"
    printf "Run #%d: cap=%s, cons=%s\n", NR-1, $1, $2
    print "----------------------------------------"
    for (i = 1; i <= NF; i++) {
        printf "  %-28s : %s\n", headers[i], $i
    }
    print ""
}' "$SUMMARY_TSV"

echo "Raw logs and summary.tsv saved under: $RESULTS_DIR"
if [[ "$RUN_CHECK" -eq 1 ]] && grep -q "FAIL" "$SUMMARY_TSV"; then
    echo "WARNING: at least one --check pass FAILED — see ${RESULTS_DIR}/*.check.log" >&2
    exit 2
fi

# echo
# echo "=== Results ==="
# if command -v column >/dev/null 2>&1; then
#     column -t -s $'\t' "$SUMMARY_TSV"
# else
#     cat "$SUMMARY_TSV"
# fi

echo
echo "Raw logs and summary.tsv saved under: $RESULTS_DIR"
if [[ "$RUN_CHECK" -eq 1 ]] && grep -q "FAIL" "$SUMMARY_TSV"; then
    echo "WARNING: at least one --check pass FAILED — see ${RESULTS_DIR}/*.check.log" >&2
    exit 2
fi
