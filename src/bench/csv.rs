//! CSV writers — emit raw and summary CSV from the runner's output.
//!
//! Two tiers:
//!   * **Raw** — one row per scenario × backend × trial × thread (full audit).
//!   * **Summary** — one row per scenario × backend × role, aggregated across
//!     trials with mean/median/min/max.
//!
//! Both writers append (with a header only on a fresh file) so multiple
//! scenario runs can be concatenated into one CSV.

use std::fs::OpenOptions;
use std::io::{Result, Write};
use std::path::Path;

use crate::bench::scenario::{RawRow, SummaryRow};

/// Write raw rows to `path`, appending to an existing file (header only on
/// fresh file). Returns the number of data rows written.
pub fn write_raw_csv(rows: &[RawRow], path: &Path) -> Result<usize> {
    let fresh = !path.exists();
    let mut f = OpenOptions::new().create(true, ).append(true).open(path)?;
    if fresh {
        writeln!(f, "{}", RAW_HEADER)?;
    }
    for r in rows {
        writeln!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            r.scenario, r.backend, r.trial, r.role.name(), r.thread_id,
            r.capacity, r.num_consumers, r.ratio,
            r.ok_ops, r.fail_ops, fmt_f(r.window_ns), fmt_f(r.throughput_ops_s),
            fmt_f(r.fail_rate_pct),
            fmt_f(r.lat_p50_ns), fmt_f(r.lat_p99_ns), fmt_f(r.lat_max_ns),
            fmt_f(r.fail_lat_p50_ns), fmt_f(r.fail_lat_p99_ns),
            r.payload_bytes,
        )?;
    }
    Ok(rows.len())
}

/// Write summary rows to `path` (append + header-on-fresh).
pub fn write_summary_csv(rows: &[SummaryRow], path: &Path) -> Result<usize> {
    let fresh = !path.exists();
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    if fresh {
        writeln!(f, "{}", SUMMARY_HEADER)?;
    }
    for r in rows {
        writeln!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            r.scenario, r.backend, r.role.name(),
            r.capacity, r.num_consumers, r.ratio, r.payload_bytes, r.trials,
            fmt_f(r.tput_mean), fmt_f(r.tput_median), fmt_f(r.tput_min), fmt_f(r.tput_max),
            fmt_f(r.fail_rate_mean),
            fmt_f(r.lat_p50_mean), fmt_f(r.lat_p50_median),
            fmt_f(r.lat_p99_mean), fmt_f(r.lat_p99_median),
            fmt_f(r.fail_lat_p50_mean), fmt_f(r.fail_lat_p50_median),
            fmt_f(r.fail_lat_p99_mean), fmt_f(r.fail_lat_p99_median),
        )?;
    }
    Ok(rows.len())
}

/// Format an f64 for CSV; NaN → empty string (so cells stay parseable).
fn fmt_f(v: f64) -> String {
    if v.is_nan() || v.is_infinite() {
        String::new()
    } else {
        format!("{v}")
    }
}

const RAW_HEADER: &str = "scenario,backend,trial,role,thread_id,capacity,num_consumers,ratio,ok_ops,fail_ops,window_ns,throughput_ops_s,fail_rate_pct,lat_p50_ns,lat_p99_ns,lat_max_ns,fail_lat_p50_ns,fail_lat_p99_ns,payload_bytes";

const SUMMARY_HEADER: &str = "scenario,backend,role,capacity,num_consumers,ratio,payload_bytes,trials,tput_mean,tput_median,tput_min,tput_max,fail_rate_mean,lat_p50_mean,lat_p50_median,lat_p99_mean,lat_p99_median,fail_lat_p50_mean,fail_lat_p50_median,fail_lat_p99_mean,fail_lat_p99_median";
