#!/usr/bin/env python3
"""Generate the SPMC ring-buffer performance report.

Reads the summary CSVs produced by the isolation and comparison example
binaries, writes charts (PNG) + a markdown summary.

Usage:
    python3 report/generate_report.py [--results-dir DIR] [--charts-dir DIR]

Inputs (defaults under ./results/):
    isolation.summary.csv
    comparison.summary.csv

Outputs:
    charts/*.png          (one file per chart)
    summary.md            (a text digest of the numbers behind the charts)

Pure matplotlib — no pandas/seaborn. Run as:
    pip install --user --break-system-packages matplotlib
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # headless (no display)
import matplotlib.pyplot as plt


# ---------------------------------------------------------------------------
# CSV reading.
# ---------------------------------------------------------------------------

# Summary CSV columns (must match src/bench/csv.rs SUMMARY_HEADER).
SUMMARY_COLUMNS = [
    "scenario", "backend", "role", "capacity", "num_consumers", "ratio",
    "payload_bytes", "trials", "tput_mean", "tput_median", "tput_min",
    "tput_max", "fail_rate_mean", "lat_p50_mean", "lat_p50_median",
    "lat_p99_mean", "lat_p99_median", "fail_lat_p50_mean",
    "fail_lat_p50_median", "fail_lat_p99_mean", "fail_lat_p99_median",
]


def read_summary(path: Path) -> list[dict]:
    """Read a summary CSV into a list of row dicts. Empty cells (NaN) -> None."""
    if not path.exists():
        print(f"  (missing: {path} — skipping)", file=sys.stderr)
        return []
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            row = {}
            for k in SUMMARY_COLUMNS:
                v = r.get(k, "")
                row[k] = _parse_num(v) if k not in (
                    "scenario", "backend", "role", "ratio") else v
            rows.append(row)
    return rows


def _parse_num(s: str):
    s = s.strip()
    if s == "":
        return None
    try:
        x = float(s)
        return x if x == x else None  # NaN check
    except ValueError:
        return None


def group(rows, key_fn):
    """Group rows by key_fn, preserving first-seen order."""
    g = defaultdict(list)
    for r in rows:
        g[key_fn(r)].append(r)
    return dict(g)


# ---------------------------------------------------------------------------
# Chart helpers.
# ---------------------------------------------------------------------------

BACKEND_COLORS = {"spmc": "#2E86C1", "sync": "#E74C3C"}
BACKEND_LABELS = {"spmc": "lock-free (SpmcRingBuffer)", "sync": "locked (SyncRingBuffer)"}


def _series(rows, x_key, y_key, backend):
    """Extract (x, y) pairs for a backend, sorted by x."""
    pts = []
    for r in rows:
        if r["backend"] != backend:
            continue
        x = r.get(x_key)
        y = r.get(y_key)
        if x is not None and y is not None:
            pts.append((x, y))
    pts.sort()
    return [p[0] for p in pts], [p[1] for p in pts]


def _save(fig, charts_dir: Path, name: str):
    out = charts_dir / f"{name}.png"
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    plt.close(fig)
    print(f"  + {out.name}")


def _fmt_tput(v):
    if v is None:
        return "—"
    if v >= 1e6:
        return f"{v/1e6:.1f}M"
    if v >= 1e3:
        return f"{v/1e3:.1f}K"
    return f"{v:.0f}"


def _fmt_ns(v):
    if v is None:
        return "—"
    if v >= 1e3:
        return f"{v/1e3:.1f}µs"
    return f"{v:.0f}ns"


# ---------------------------------------------------------------------------
# Isolation charts (Group A–D).
# ---------------------------------------------------------------------------

def chart_isolation_rate_ratio(iso_rows, charts_dir):
    """A: throughput vs rate-ratio (line), push + total pop, one backend."""
    rate_scenarios = [
        "rate_max", "rate_balanced", "rate_prod_2x", "rate_prod_4x",
        "rate_prod_8x", "rate_prod_16x", "rate_cons_2x", "rate_cons_4x",
    ]
    by_scn = group(iso_rows, lambda r: r["scenario"])
    backends = sorted({r["backend"] for r in iso_rows})
    for be in backends:
        xs, push_tput, pop_tput, fail_rates = [], [], [], []
        for scn in rate_scenarios:
            rs = by_scn.get(scn, [])
            prod = next((r for r in rs if r["backend"] == be and r["role"] == "producer"), None)
            cons = next((r for r in rs if r["backend"] == be and r["role"] == "consumer"), None)
            if prod is None:
                continue
            xs.append(scn.replace("rate_", ""))
            push_tput.append(prod.get("tput_median") or 0)
            pop_tput.append(cons.get("tput_median") or 0 if cons else 0)
            fail_rates.append(prod.get("fail_rate_mean") or 0)
        if not xs:
            continue
        fig, ax1 = plt.subplots(figsize=(10, 5))
        ax1.plot(xs, push_tput, "o-", color=BACKEND_COLORS[be], label="push throughput")
        ax1.plot(xs, pop_tput, "s--", color="#27AE60", label="total pop throughput")
        ax1.set_xlabel("rate ratio")
        ax1.set_ylabel("throughput (ops/s)")
        ax1.set_title(f"Isolation — throughput vs rate-ratio [{BACKEND_LABELS[be]}]\n(CAP=1024, N=4, u64; median across trials)")
        ax1.legend(loc="upper left")
        ax1.grid(True, alpha=0.3)
        ax2 = ax1.twinx()
        ax2.bar(xs, [fr * 100 for fr in fail_rates], alpha=0.2, color="#8E44AD", label="push fail rate %")
        ax2.set_ylabel("push fail rate (%)")
        ax2.legend(loc="upper right")
        plt.xticks(rotation=30, ha="right")
        _save(fig, charts_dir, f"iso_rate_ratio_tput_{be}")


def chart_isolation_rate_ratio_latency(iso_rows, charts_dir):
    """A: fast-path latency vs rate-ratio (push/pop p50/p99)."""
    rate_scenarios = [
        "rate_max", "rate_balanced", "rate_prod_2x", "rate_prod_4x",
        "rate_prod_8x", "rate_prod_16x", "rate_cons_2x", "rate_cons_4x",
    ]
    by_scn = group(iso_rows, lambda r: r["scenario"])
    backends = sorted({r["backend"] for r in iso_rows})
    for be in backends:
        xs, push_p50, push_p99, pop_p50, pop_p99 = [], [], [], [], []
        for scn in rate_scenarios:
            rs = by_scn.get(scn, [])
            prod = next((r for r in rs if r["backend"] == be and r["role"] == "producer"), None)
            cons = next((r for r in rs if r["backend"] == be and r["role"] == "consumer"), None)
            if prod is None:
                continue
            xs.append(scn.replace("rate_", ""))
            push_p50.append(prod.get("lat_p50_median") or 0)
            push_p99.append(prod.get("lat_p99_median") or 0)
            pop_p50.append(cons.get("lat_p50_median") or 0 if cons else 0)
            pop_p99.append(cons.get("lat_p99_median") or 0 if cons else 0)
        if not xs:
            continue
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(xs, push_p50, "o-", color=BACKEND_COLORS[be], label="push p50")
        ax.plot(xs, push_p99, "o--", color=BACKEND_COLORS[be], alpha=0.5, label="push p99")
        ax.plot(xs, pop_p50, "s-", color="#27AE60", label="pop p50")
        ax.plot(xs, pop_p99, "s--", color="#27AE60", alpha=0.5, label="pop p99")
        ax.set_xlabel("rate ratio")
        ax.set_ylabel("latency (ns/op, batched-mean)")
        ax.set_yscale("log")
        ax.set_title(f"Isolation — fast-path latency vs rate-ratio [{BACKEND_LABELS[be]}]\n(batched-mean p50/p99; see report/README.md for methodology)")
        ax.legend()
        ax.grid(True, alpha=0.3, which="both")
        plt.xticks(rotation=30, ha="right")
        _save(fig, charts_dir, f"iso_rate_ratio_lat_{be}")


def chart_isolation_capacity(iso_rows, charts_dir):
    """B: throughput vs capacity (log-x)."""
    cap_scenarios = [
        ("cap_1", 1), ("cap_2", 2), ("cap_4", 4), ("cap_16", 16),
        ("cap_64", 64), ("cap_256", 256), ("cap_1024", 1024), ("cap_4096", 4096),
    ]
    by_scn = group(iso_rows, lambda r: r["scenario"])
    backends = sorted({r["backend"] for r in iso_rows})
    for be in backends:
        xs, push_t, pop_t = [], [], []
        for scn, cap in cap_scenarios:
            rs = by_scn.get(scn, [])
            prod = next((r for r in rs if r["backend"] == be and r["role"] == "producer"), None)
            cons = next((r for r in rs if r["backend"] == be and r["role"] == "consumer"), None)
            if prod is None:
                continue
            xs.append(cap)
            push_t.append(prod.get("tput_median") or 0)
            pop_t.append(cons.get("tput_median") or 0 if cons else 0)
        if not xs:
            continue
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.plot(xs, push_t, "o-", color=BACKEND_COLORS[be], label="push throughput")
        ax.plot(xs, pop_t, "s--", color="#27AE60", label="total pop throughput")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("capacity (CAP, log scale)")
        ax.set_ylabel("throughput (ops/s, median)")
        ax.set_title(f"Isolation — throughput vs capacity [{BACKEND_LABELS[be]}]\n(N=4, Max ratio, u64)")
        ax.legend()
        ax.grid(True, alpha=0.3, which="both")
        _save(fig, charts_dir, f"iso_capacity_tput_{be}")


def chart_isolation_n_scaling(iso_rows, charts_dir):
    """C: per-consumer throughput vs N."""
    n_scenarios = [("n_1", 1), ("n_2", 2), ("n_4", 4), ("n_8", 8), ("n_16", 16)]
    by_scn = group(iso_rows, lambda r: r["scenario"])
    backends = sorted({r["backend"] for r in iso_rows})
    for be in backends:
        xs, per_cons_tput, total_cons_tput = [], [], []
        for scn, n in n_scenarios:
            rs = by_scn.get(scn, [])
            cons = next((r for r in rs if r["backend"] == be and r["role"] == "consumer"), None)
            if cons is None:
                continue
            total = cons.get("tput_median") or 0
            xs.append(n)
            per_cons_tput.append(total / n if n else 0)
            total_cons_tput.append(total)
        if not xs:
            continue
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.plot(xs, per_cons_tput, "o-", color=BACKEND_COLORS[be], label="per-consumer throughput")
        ax.set_xlabel("number of consumers (N)")
        ax.set_ylabel("per-consumer throughput (ops/s, median)")
        ax.set_title(f"Isolation — per-consumer throughput vs N [{BACKEND_LABELS[be]}]\n(CAP=1024, Balanced, u64; fan-out parallelism proof)")
        ax.legend()
        ax.grid(True, alpha=0.3)
        _save(fig, charts_dir, f"iso_n_scaling_{be}")


def chart_isolation_payload(iso_rows, charts_dir):
    """D: payload size comparison (grouped bar)."""
    by_scn = group(iso_rows, lambda r: r["scenario"])
    backends = sorted({r["backend"] for r in iso_rows})
    for be in backends:
        labels, push_t, pop_t = [], [], []
        for scn, lbl in [("payload_u64", "u64 (8B)"), ("payload_u64x8", "[u64;8] (64B)")]:
            rs = by_scn.get(scn, [])
            prod = next((r for r in rs if r["backend"] == be and r["role"] == "producer"), None)
            cons = next((r for r in rs if r["backend"] == be and r["role"] == "consumer"), None)
            if prod is None:
                continue
            labels.append(lbl)
            push_t.append(prod.get("tput_median") or 0)
            pop_t.append(cons.get("tput_median") or 0 if cons else 0)
        if not labels:
            continue
        x = range(len(labels))
        fig, ax = plt.subplots(figsize=(7, 5))
        ax.bar([i - 0.2 for i in x], push_t, 0.4, color=BACKEND_COLORS[be], label="push")
        ax.bar([i + 0.2 for i in x], pop_t, 0.4, color="#27AE60", label="total pop")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels)
        ax.set_ylabel("throughput (ops/s, median)")
        ax.set_title(f"Isolation — payload size [{BACKEND_LABELS[be]}]\n(CAP=1024, N=4, Balanced)")
        ax.legend()
        ax.grid(True, alpha=0.3, axis="y")
        _save(fig, charts_dir, f"iso_payload_{be}")


# ---------------------------------------------------------------------------
# Comparison charts (Group E–G).
# ---------------------------------------------------------------------------

def chart_comparison_scenarios(cmp_rows, charts_dir):
    """E: per-scenario throughput (grouped bar, spmc vs sync)."""
    e_scenarios = [
        "cmp_fanout_4consumers", "cmp_uneven_consumer_speeds",
        "cmp_tiny_capacity_high_contention", "cmp_slowest_consumer_gates",
        "cmp_wrap_around_sustained", "cmp_blocking_push_unblocks",
    ]
    by_scn = group(cmp_rows, lambda r: r["scenario"])
    labels, spmc_push, sync_push, spmc_pop, sync_pop = [], [], [], [], []
    for scn in e_scenarios:
        rs = by_scn.get(scn, [])
        sp = next((r for r in rs if r["backend"] == "spmc" and r["role"] == "producer"), None)
        sy = next((r for r in rs if r["backend"] == "sync" and r["role"] == "producer"), None)
        spc = next((r for r in rs if r["backend"] == "spmc" and r["role"] == "consumer"), None)
        syc = next((r for r in rs if r["backend"] == "sync" and r["role"] == "consumer"), None)
        if sp is None or sy is None:
            continue
        labels.append(scn.replace("cmp_", ""))
        spmc_push.append(sp.get("tput_median") or 0)
        sync_push.append(sy.get("tput_median") or 0)
        spmc_pop.append(spc.get("tput_median") or 0 if spc else 0)
        sync_pop.append(syc.get("tput_median") or 0 if syc else 0)
    if not labels:
        return
    x = range(len(labels))
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    w = 0.35
    ax1.bar([i - w/2 for i in x], spmc_push, w, color=BACKEND_COLORS["spmc"], label="lock-free")
    ax1.bar([i + w/2 for i in x], sync_push, w, color=BACKEND_COLORS["sync"], label="locked")
    ax1.set_xticks(list(x)); ax1.set_xticklabels(labels, rotation=25, ha="right")
    ax1.set_ylabel("push throughput (ops/s, median)")
    ax1.set_title("Comparison — push throughput")
    ax1.legend(); ax1.grid(True, alpha=0.3, axis="y")
    ax2.bar([i - w/2 for i in x], spmc_pop, w, color=BACKEND_COLORS["spmc"], label="lock-free")
    ax2.bar([i + w/2 for i in x], sync_pop, w, color=BACKEND_COLORS["sync"], label="locked")
    ax2.set_xticks(list(x)); ax2.set_xticklabels(labels, rotation=25, ha="right")
    ax2.set_ylabel("total pop throughput (ops/s, median)")
    ax2.set_title("Comparison — total pop throughput")
    ax2.legend(); ax2.grid(True, alpha=0.3, axis="y")
    _save(fig, charts_dir, "cmp_scenarios_tput")


def chart_comparison_n_scaling(cmp_rows, charts_dir):
    """F: per-consumer throughput vs N — both backends (the headline)."""
    n_scenarios = [("cmp_n_1", 1), ("cmp_n_2", 2), ("cmp_n_4", 4), ("cmp_n_8", 8), ("cmp_n_16", 16)]
    by_scn = group(cmp_rows, lambda r: r["scenario"])
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    for be, color in BACKEND_COLORS.items():
        xs, per_cons, total = [], [], []
        for scn, n in n_scenarios:
            rs = by_scn.get(scn, [])
            cons = next((r for r in rs if r["backend"] == be and r["role"] == "consumer"), None)
            if cons is None:
                continue
            total_t = cons.get("tput_median") or 0
            xs.append(n)
            per_cons.append(total_t / n if n else 0)
            total.append(total_t)
        if not xs:
            continue
        ax1.plot(xs, per_cons, "o-", color=color, label=BACKEND_LABELS[be])
        ax2.plot(xs, total, "s-", color=color, label=BACKEND_LABELS[be])
    ax1.set_xlabel("number of consumers (N)")
    ax1.set_ylabel("per-consumer throughput (ops/s, median)")
    ax1.set_title("Comparison — per-consumer throughput vs N\n(CAP=1024, Balanced, u64; the headline divergence)")
    ax1.legend(); ax1.grid(True, alpha=0.3)
    ax2.set_xlabel("number of consumers (N)")
    ax2.set_ylabel("total pop throughput (ops/s, median)")
    ax2.set_title("Comparison — total pop throughput vs N\n(lock-free should rise with N; locked should plateau)")
    ax2.legend(); ax2.grid(True, alpha=0.3)
    _save(fig, charts_dir, "cmp_n_scaling")


def chart_comparison_capacity(cmp_rows, charts_dir):
    """G: throughput vs capacity — both backends."""
    cap_scenarios = [
        ("cmp_cap_4", 4), ("cmp_cap_16", 16), ("cmp_cap_64", 64),
        ("cmp_cap_256", 256), ("cmp_cap_1024", 1024), ("cmp_cap_4096", 4096),
    ]
    by_scn = group(cmp_rows, lambda r: r["scenario"])
    fig, ax = plt.subplots(figsize=(9, 5))
    for be, color in BACKEND_COLORS.items():
        xs, tputs = [], []
        for scn, cap in cap_scenarios:
            rs = by_scn.get(scn, [])
            prod = next((r for r in rs if r["backend"] == be and r["role"] == "producer"), None)
            if prod is None:
                continue
            xs.append(cap)
            tputs.append(prod.get("tput_median") or 0)
        if not xs:
            continue
        ax.plot(xs, tputs, "o-", color=color, label=BACKEND_LABELS[be])
    ax.set_xscale("log", base=2)
    ax.set_xlabel("capacity (CAP, log scale)")
    ax.set_ylabel("push throughput (ops/s, median)")
    ax.set_title("Comparison — push throughput vs capacity\n(N=4, Balanced, u64)")
    ax.legend(); ax.grid(True, alpha=0.3, which="both")
    _save(fig, charts_dir, "cmp_capacity_tput")


def chart_comparison_matrix(cmp_rows, charts_dir):
    """Headline comparison matrix: spmc/sync ratio per scenario × metric."""
    e_scenarios = [
        "cmp_fanout_4consumers", "cmp_uneven_consumer_speeds",
        "cmp_tiny_capacity_high_contention", "cmp_slowest_consumer_gates",
        "cmp_wrap_around_sustained", "cmp_blocking_push_unblocks",
    ]
    by_scn = group(cmp_rows, lambda r: r["scenario"])
    metrics = ["push tput", "pop tput", "push p50 lat", "push p99 lat"]
    matrix = []
    row_labels = []
    for scn in e_scenarios:
        rs = by_scn.get(scn, [])
        sp = next((r for r in rs if r["backend"] == "spmc" and r["role"] == "producer"), None)
        sy = next((r for r in rs if r["backend"] == "sync" and r["role"] == "producer"), None)
        spc = next((r for r in rs if r["backend"] == "spmc" and r["role"] == "consumer"), None)
        syc = next((r for r in rs if r["backend"] == "sync" and r["role"] == "consumer"), None)
        if sp is None or sy is None:
            continue
        def ratio(a, b):
            if a is None or b is None or b == 0:
                return float("nan")
            return a / b
        row_labels.append(scn.replace("cmp_", ""))
        matrix.append([
            ratio(sp.get("tput_median"), sy.get("tput_median")),
            ratio(spc.get("tput_median") if spc else None, syc.get("tput_median") if syc else None),
            ratio(sy.get("lat_p50_median"), sp.get("lat_p50_median")),  # latency: sync/spmc (>1 = lock-free faster)
            ratio(sy.get("lat_p99_median"), sp.get("lat_p99_median")),
        ])
    if not row_labels:
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    import numpy as np
    data = np.array([[v if v == v else 0 for v in row] for row in matrix])
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=0, vmax=max(3.0, float(np.nanmax(data))))
    ax.set_xticks(range(len(metrics))); ax.set_xticklabels(metrics, rotation=20)
    ax.set_yticks(range(len(row_labels))); ax.set_yticklabels(row_labels)
    for i in range(len(row_labels)):
        for j in range(len(metrics)):
            v = matrix[i][j]
            txt = f"{v:.1f}×" if v == v and v != 0 else "—"
            ax.text(j, i, txt, ha="center", va="center", fontsize=9,
                    color="black" if (v == v and 0.5 < v < 5) else "white")
    ax.set_title("Comparison — lock-free / locked ratio\n(>1 = lock-free wins; latency rows are sync/spmc so >1 = lock-free faster)")
    fig.colorbar(im, ax=ax, label="ratio (×)")
    _save(fig, charts_dir, "cmp_matrix")


# ---------------------------------------------------------------------------
# Summary markdown.
# ---------------------------------------------------------------------------

def write_summary_md(iso_rows, cmp_rows, out: Path):
    with open(out, "w") as f:
        f.write("# SPMC Ring Buffer — Performance Report\n\n")
        f.write(f"Generated by `report/generate_report.py`. Charts in `results/charts/`.\n\n")
        f.write("## Run config\n\n")
        f.write("- Backend: see CSV `backend` column (`spmc` = lock-free, `sync` = locked)\n")
        f.write("- Aggregation: `mean` + `median` across trials (median is the headline; mean shows tail sensitivity)\n")
        f.write("- Latency: batched-mean p50/p99 on the fast path; per-op on the slow path (see `report/README.md`)\n\n")
        if iso_rows:
            f.write("## Isolation suite — digest\n\n")
            f.write("| scenario | backend | role | tput median | fail rate | lat p50 | lat p99 |\n")
            f.write("|---|---|---|---|---|---|---|\n")
            for r in sorted(iso_rows, key=lambda r: (r["scenario"], r["backend"], r["role"])):
                f.write(f"| {r['scenario']} | {r['backend']} | {r['role']} | "
                        f"{_fmt_tput(r.get('tput_median'))} | "
                        f"{(r.get('fail_rate_mean') or 0)*100:.1f}% | "
                        f"{_fmt_ns(r.get('lat_p50_median'))} | "
                        f"{_fmt_ns(r.get('lat_p99_median'))} |\n")
            f.write("\n")
        if cmp_rows:
            f.write("## Comparison suite — digest\n\n")
            f.write("| scenario | backend | role | tput median | fail rate | lat p50 | lat p99 |\n")
            f.write("|---|---|---|---|---|---|---|\n")
            for r in sorted(cmp_rows, key=lambda r: (r["scenario"], r["backend"], r["role"])):
                f.write(f"| {r['scenario']} | {r['backend']} | {r['role']} | "
                        f"{_fmt_tput(r.get('tput_median'))} | "
                        f"{(r.get('fail_rate_mean') or 0)*100:.1f}% | "
                        f"{_fmt_ns(r.get('lat_p50_median'))} | "
                        f"{_fmt_ns(r.get('lat_p99_median'))} |\n")
    print(f"  + {out.name}")


# ---------------------------------------------------------------------------
# Main.
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", default="results")
    ap.add_argument("--charts-dir", default=None)
    args = ap.parse_args()

    results = Path(args.results_dir)
    charts = Path(args.charts_dir) if args.charts_dir else results / "charts"
    charts.mkdir(parents=True, exist_ok=True)

    print("Reading CSVs...")
    iso = read_summary(results / "isolation.summary.csv")
    cmp = read_summary(results / "comparison.summary.csv")
    print(f"  isolation rows: {len(iso)}")
    print(f"  comparison rows: {len(cmp)}")

    if not iso and not cmp:
        print("No data. Run the isolation and/or comparison binaries first.", file=sys.stderr)
        return 1

    print("Generating charts...")
    if iso:
        chart_isolation_rate_ratio(iso, charts)
        chart_isolation_rate_ratio_latency(iso, charts)
        chart_isolation_capacity(iso, charts)
        chart_isolation_n_scaling(iso, charts)
        chart_isolation_payload(iso, charts)
    if cmp:
        chart_comparison_scenarios(cmp, charts)
        chart_comparison_n_scaling(cmp, charts)
        chart_comparison_capacity(cmp, charts)
        chart_comparison_matrix(cmp, charts)

    print("Writing summary.md...")
    write_summary_md(iso, cmp, results / "summary.md")

    print(f"\nDone. Charts → {charts}, summary → {results/'summary.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
