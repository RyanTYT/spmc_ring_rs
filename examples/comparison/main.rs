//! Comparison suite entry point — runs scenarios for both backends and writes
//! raw + summary CSV tagged `spmc` / `sync` for side-by-side plotting.
//!
//! # Usage
//!
//! ```text
//! cargo run --release --features bench --example comparison -- [OPTIONS]
//! ```
//!
//! # Options
//!
//! Same as the isolation suite, except `--backend` defaults to `both` and the
//! output paths default to `results/comparison.*.csv`. See
//! `examples/isolation/README.md` for the full flag reference.

mod scenarios;

use scenarios::{all_scenarios, CommonConfig};
use spmc_ring::bench::{write_raw_csv, write_summary_csv, Backend};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

struct Cli {
    backends: Vec<Backend>,
    common: CommonConfig,
    out: PathBuf,
    summary: PathBuf,
    scenarios: Option<String>,
}

fn parse_cli() -> Result<Cli, String> {
    let mut backends: Vec<Backend> = Backend::all().to_vec(); // default: both
    let mut trials = 5usize;
    let mut duration = 1.0f64;
    let mut warmup = 300u64;
    let mut idle_gap = 150u64;
    let mut out = PathBuf::from("results/comparison.raw.csv");
    let mut summary = PathBuf::from("results/comparison.summary.csv");
    let mut scenarios: Option<String> = None;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--backend" => {
                i += 1;
                let v = args.get(i).ok_or("--backend needs a value")?;
                backends = match v.to_ascii_lowercase().as_str() {
                    "spmc" => vec![Backend::Spmc],
                    "sync" => vec![Backend::Sync],
                    "both" => Backend::all().to_vec(),
                    other => return Err(format!("--backend `{other}` (expected spmc|sync|both)")),
                };
            }
            "--trials" => {
                i += 1;
                trials = args.get(i).ok_or("--trials needs a value")?
                    .parse().map_err(|e: std::num::ParseIntError| format!("--trials: {e}"))?;
            }
            "--duration" => {
                i += 1;
                duration = args.get(i).ok_or("--duration needs a value")?
                    .parse().map_err(|e: std::num::ParseFloatError| format!("--duration: {e}"))?;
            }
            "--warmup" => {
                i += 1;
                warmup = args.get(i).ok_or("--warmup needs a value")?
                    .parse().map_err(|e: std::num::ParseIntError| format!("--warmup: {e}"))?;
            }
            "--idle-gap" => {
                i += 1;
                idle_gap = args.get(i).ok_or("--idle-gap needs a value")?
                    .parse().map_err(|e: std::num::ParseIntError| format!("--idle-gap: {e}"))?;
            }
            "--out" => {
                i += 1;
                out = PathBuf::from(args.get(i).ok_or("--out needs a value")?);
            }
            "--summary" => {
                i += 1;
                summary = PathBuf::from(args.get(i).ok_or("--summary needs a value")?);
            }
            "--scenarios" => {
                i += 1;
                scenarios = Some(args.get(i).ok_or("--scenarios needs a value")?.clone());
            }
            "--no-pin" => { /* no-op; pinning is opt-in (bench-pin, deferred) */ }
            other => return Err(format!("unknown argument `{other}`")),
        }
        i += 1;
    }

    if backends.is_empty() {
        return Err("no backend selected".into());
    }
    if trials == 0 {
        return Err("--trials must be >= 1".into());
    }
    if duration <= 0.0 {
        return Err("--duration must be > 0".into());
    }

    Ok(Cli {
        backends,
        common: CommonConfig {
            trials,
            duration: Duration::from_secs_f64(duration),
            warmup: Duration::from_millis(warmup),
            idle_gap: Duration::from_millis(idle_gap),
        },
        out,
        summary,
        scenarios,
    })
}

fn matches(filter: &Option<String>, name: &str) -> bool {
    filter.as_ref().map(|f| name.contains(f.as_str())).unwrap_or(true)
}

fn main() -> ExitCode {
    let cli = match parse_cli() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("comparison: {e}");
            eprintln!("usage: comparison [--backend spmc|sync|both] [--trials N] [--duration SECS] [--warmup MS] [--idle-gap MS] [--out PATH] [--summary PATH] [--scenarios SUBSTR] [--no-pin]");
            return ExitCode::from(2);
        }
    };

    if let Some(parent) = cli.out.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Some(parent) = cli.summary.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(&cli.out);
    let _ = std::fs::remove_file(&cli.summary);

    let scenarios = all_scenarios();
    let total = scenarios.iter().filter(|(n, _)| matches(&cli.scenarios, n)).count() * cli.backends.len();
    let mut done = 0;

    for (name, run) in &scenarios {
        if !matches(&cli.scenarios, name) {
            continue;
        }
        for backend in &cli.backends {
            print!("[{done}/{total}] {name} ({}) ... ", backend.name());
            let (raw, summary) = run(*backend, &cli.common);
            if let Err(e) = write_raw_csv(&raw, &cli.out) {
                eprintln!("raw CSV write failed: {e}");
                return ExitCode::from(1);
            }
            if let Err(e) = write_summary_csv(&summary, &cli.summary) {
                eprintln!("summary CSV write failed: {e}");
                return ExitCode::from(1);
            }
            let prod = summary.iter().find(|s| s.role.name() == "producer")
                .expect("summary has a producer row");
            let cons = summary.iter().find(|s| s.role.name() == "consumer")
                .expect("summary has a consumer row");
            println!(
                "push {:.0} ops/s (fail {:.1}%, p50 {:.1}ns) | pop {:.0} ops/s (fail {:.1}%, p50 {:.1}ns)",
                prod.tput_median, prod.fail_rate_mean * 100.0, prod.lat_p50_median,
                cons.tput_median, cons.fail_rate_mean * 100.0, cons.lat_p50_median,
            );
            done += 1;
        }
    }

    println!("\ncomparison: {done} scenario-backend runs complete");
    println!("  raw    → {}", cli.out.display());
    println!("  summary→ {}", cli.summary.display());
    ExitCode::SUCCESS
}
