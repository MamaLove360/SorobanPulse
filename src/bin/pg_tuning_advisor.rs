//! PostgreSQL configuration tuning advisor CLI (Issue #824).
//!
//! Prints a `postgresql.conf` snippet recommended for the given host profile,
//! using the PGTune-style heuristics in `soroban_pulse::db_config_advisor`.
//!
//! Usage:
//!   cargo run --bin pg_tuning_advisor -- --memory-mb 8192 --cpu-count 4 \
//!       --max-connections 100 --ssd
// Issue #1246: a panic in production is a silent availability incident.
// Test code is exempt via `allow-{unwrap,expect}-in-tests` in .clippy.toml.
#![deny(clippy::unwrap_used, clippy::expect_used)]

use soroban_pulse::db_config_advisor::{recommend_postgres_config, PgTuningInput};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return;
    }

    let total_memory_mb = flag_value(&args, "--memory-mb")
        .and_then(|v| v.parse().ok())
        .unwrap_or(8192);
    let cpu_count = flag_value(&args, "--cpu-count")
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let max_connections = flag_value(&args, "--max-connections")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let storage_is_ssd = !args.iter().any(|a| a == "--hdd");

    let input = PgTuningInput {
        total_memory_mb,
        cpu_count,
        max_connections,
        storage_is_ssd,
    };

    match recommend_postgres_config(&input) {
        Ok(report) => print!("{}", report.render_config_snippet()),
        Err(e) => {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
    }
}

fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
}

fn print_usage() {
    eprintln!(
        "Usage: pg_tuning_advisor [OPTIONS]

Prints a recommended postgresql.conf snippet for the given host profile.

Options:
  --memory-mb <n>        Total host RAM in MB (default: 8192)
  --cpu-count <n>        Number of CPU cores (default: 4)
  --max-connections <n>  Expected max concurrent connections (default: 100)
  --ssd                  Assume SSD/NVMe storage (default)
  --hdd                  Assume spinning disk storage
  -h, --help             Show this message

Example:
  cargo run --bin pg_tuning_advisor -- --memory-mb 16384 --cpu-count 8 --max-connections 200"
    );
}
