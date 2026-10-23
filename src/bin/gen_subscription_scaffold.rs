//! Subscription scaffolding generator CLI.
//!
//! Generates a complete set of Rust source files, SQL migration, and tests
//! for a new named subscription + delivery channel.
//!
//! Usage:
//!   gen_subscription_scaffold <NAME> [OPTIONS]
//!
//! Options:
//!   --output-dir <path>      Destination directory (default: current directory)
//!   --channel-type <type>    webhook | email | sms  (default: webhook)
//!   --with-filter            Also generate content filter configuration module
//!   --with-tests             Also generate a test scaffold module
//!   --dry-run                Print file contents to stdout; do not write files
//!
//! Examples:
//!   cargo run --bin gen_subscription_scaffold -- token-transfer
//!   cargo run --bin gen_subscription_scaffold -- payment --channel-type email --with-tests
//!   cargo run --bin gen_subscription_scaffold -- nft-sale --with-filter --with-tests --dry-run
// Issue #1246: a panic in production is a silent availability incident.
// Test code is exempt via `allow-{unwrap,expect}-in-tests` in .clippy.toml.
#![deny(clippy::unwrap_used, clippy::expect_used)]

use soroban_pulse::codegen::{self, ChannelType, GeneratedFile, ScaffoldConfig};
use std::{path::PathBuf, process};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        process::exit(0);
    }

    match run(&args) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("error: {e}");
            process::exit(1);
        }
    }
}

fn run(args: &[String]) -> Result<(), String> {
    let config = parse_args(args)?;
    let output_dir = parse_output_dir(args).unwrap_or_else(|| PathBuf::from("."));
    let dry_run = args.iter().any(|a| a == "--dry-run");

    let files = codegen::generate_all(&config);

    if dry_run {
        print_dry_run(&files);
    } else {
        codegen::write_files(&files, &output_dir)
            .map_err(|e| format!("failed to write files: {e}"))?;
        print_summary(&config, &files, &output_dir);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

fn parse_args(args: &[String]) -> Result<ScaffoldConfig, String> {
    let name = args
        .first()
        .filter(|a| !a.starts_with('-'))
        .ok_or("NAME is required as the first argument")?
        .clone();

    if name.is_empty() || name == "--help" {
        return Err("NAME must be a non-empty string".into());
    }

    let channel_type = flag_value(args, "--channel-type")
        .map(|s| ChannelType::parse(&s))
        .transpose()?
        .unwrap_or(ChannelType::Webhook);

    let with_filter = args.iter().any(|a| a == "--with-filter");
    let with_tests = args.iter().any(|a| a == "--with-tests");

    Ok(ScaffoldConfig::new(&name, channel_type, with_filter, with_tests))
}

fn parse_output_dir(args: &[String]) -> Option<PathBuf> {
    flag_value(args, "--output-dir").map(PathBuf::from)
}

/// Return the value that follows `flag` in `args`, if present.
fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|w| w[0] == flag)
        .map(|w| w[1].clone())
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

fn print_dry_run(files: &[GeneratedFile]) {
    for file in files {
        println!("=== {} ===", file.relative_path);
        println!("{}", file.content);
        println!();
    }
}

fn print_summary(config: &ScaffoldConfig, files: &[GeneratedFile], output_dir: &PathBuf) {
    println!(
        "Generated {} file(s) for '{}' ({}) in '{}':",
        files.len(),
        config.name,
        config.channel_type.as_str(),
        output_dir.display()
    );
    for file in files {
        println!("  {}", file.relative_path);
    }
    println!();
    println!("Next steps:");
    println!(
        "  1. Add `pub mod {snake};` to src/lib.rs",
        snake = config.snake_name
    );

    let handler_file = format!("src/{}_subscriptions.rs", config.snake_name);
    println!("  2. Register routes from {handler_file} in src/routes.rs");

    // Migration hint
    let migration_file = files
        .iter()
        .find(|f| f.relative_path.ends_with(".sql"))
        .map(|f| f.relative_path.as_str())
        .unwrap_or("migrations/<timestamp>_add_*.sql");
    println!("  3. Run: sqlx migrate run   # applies {migration_file}");

    if config.with_filter {
        println!(
            "  4. Wire {snake}_filter_config::evaluate_all() into the delivery worker",
            snake = config.snake_name
        );
    }
    if config.with_tests {
        println!(
            "  {}. Run: cargo test {snake}",
            if config.with_filter { "5" } else { "4" },
            snake = config.snake_name
        );
    }
}

fn print_usage() {
    eprintln!(
        "Usage: gen_subscription_scaffold <NAME> [OPTIONS]

Arguments:
  NAME                     Subscription name, e.g. token-transfer or payment

Options:
  --output-dir <path>      Write files here (default: current directory)
  --channel-type <type>    webhook | email | sms  (default: webhook)
  --with-filter            Generate content filter configuration module
  --with-tests             Generate test scaffold module
  --dry-run                Print generated files to stdout without writing
  -h, --help               Show this message

Examples:
  cargo run --bin gen_subscription_scaffold -- token-transfer
  cargo run --bin gen_subscription_scaffold -- payment --channel-type email --with-tests
  cargo run --bin gen_subscription_scaffold -- nft-sale --with-filter --with-tests --dry-run
"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_name_only() {
        let cfg = parse_args(&args(&["transfer"])).unwrap();
        assert_eq!(cfg.name, "transfer");
        assert_eq!(cfg.channel_type, ChannelType::Webhook);
        assert!(!cfg.with_filter);
        assert!(!cfg.with_tests);
    }

    #[test]
    fn parses_channel_type_email() {
        let cfg = parse_args(&args(&["payment", "--channel-type", "email"])).unwrap();
        assert_eq!(cfg.channel_type, ChannelType::Email);
    }

    #[test]
    fn parses_flags() {
        let cfg =
            parse_args(&args(&["nft", "--with-filter", "--with-tests"])).unwrap();
        assert!(cfg.with_filter);
        assert!(cfg.with_tests);
    }

    #[test]
    fn errors_without_name() {
        assert!(parse_args(&args(&[])).is_err());
    }

    #[test]
    fn errors_on_unknown_channel_type() {
        let result = parse_args(&args(&["transfer", "--channel-type", "fax"]));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("fax"));
    }

    #[test]
    fn flag_value_finds_next_arg() {
        let a = args(&["name", "--output-dir", "/tmp/out", "--channel-type", "sms"]);
        assert_eq!(flag_value(&a, "--output-dir"), Some("/tmp/out".into()));
        assert_eq!(flag_value(&a, "--channel-type"), Some("sms".into()));
        assert_eq!(flag_value(&a, "--missing"), None);
    }

    #[test]
    fn parse_output_dir_returns_none_when_absent() {
        let a = args(&["name"]);
        assert!(parse_output_dir(&a).is_none());
    }

    #[test]
    fn parse_output_dir_returns_path() {
        let a = args(&["name", "--output-dir", "/some/path"]);
        assert_eq!(
            parse_output_dir(&a),
            Some(PathBuf::from("/some/path"))
        );
    }
}
