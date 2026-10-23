mod client;
mod config;
mod exporter;
mod formatter;
mod query;
mod replay;
mod subscription;
mod tail;
mod webhook_test;

use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use colored::Colorize;
use std::path::PathBuf;

use client::ApiClient;
use config::Config;
use exporter::{export_events, ExportFormat};
use formatter::{print_contracts, print_events, print_stats, Format};
use query::{
    Contract, ContractQuery, EventQuery, EventStats, EventsResponse,
};

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

/// `spulse --version` output: crate version plus the git commit it was built from.
const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), " (", env!("SPULSE_GIT_SHA"), ")");

/// spulse — query and analyze Soroban Pulse events from the command line.
#[derive(Parser)]
#[command(name = "spulse", version = VERSION, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Soroban Pulse base URL (overrides config)
    #[arg(long, env = "SPULSE_BASE_URL", global = true)]
    base_url: Option<String>,

    /// API key (overrides config)
    #[arg(long, env = "SPULSE_API_KEY", global = true)]
    api_key: Option<String>,

    /// Output format: json | csv | table
    #[arg(long, short = 'f', global = true)]
    format: Option<Format>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Query Soroban events
    Events {
        /// Filter: starting ledger
        #[arg(long, short = 's')]
        from_ledger: Option<i64>,
        /// Filter: ending ledger
        #[arg(long, short = 'e')]
        to_ledger: Option<i64>,
        /// Filter: contract ID
        #[arg(long, short = 'c')]
        contract: Option<String>,
        /// Filter: event type (e.g. contract, system)
        #[arg(long, short = 't')]
        event_type: Option<String>,
        /// Filter: transaction hash
        #[arg(long)]
        tx: Option<String>,
        /// Results per page
        #[arg(long, short = 'l', default_value_t = 25)]
        limit: u32,
        /// Page number
        #[arg(long, short = 'p', default_value_t = 1)]
        page: u32,
        /// Sort direction: asc | desc
        #[arg(long, default_value = "desc")]
        sort: String,
        /// Sort field: ledger | created_at
        #[arg(long, default_value = "ledger")]
        sort_by: String,
        /// Write output to a file
        #[arg(long, short = 'o')]
        output: Option<PathBuf>,
        /// Fetch all pages automatically (for --output)
        #[arg(long)]
        all: bool,
        /// Max records when using --all (safety limit)
        #[arg(long, default_value_t = 10_000)]
        max: u64,
        /// Export file format when using --output
        #[arg(long, default_value = "json")]
        export_format: ExportFormat,
    },

    /// List or search indexed contracts
    Contracts {
        /// Search query
        #[arg(long, short = 's')]
        search: Option<String>,
        /// Results per page
        #[arg(long, short = 'l', default_value_t = 25)]
        limit: u32,
        /// Page number
        #[arg(long, short = 'p', default_value_t = 1)]
        page: u32,
    },

    /// Show event statistics
    Stats {
        /// Contract ID to scope stats to
        #[arg(long, short = 'c')]
        contract: Option<String>,
    },

    /// Export events to a file (auto-paginates)
    Export {
        /// Output file path (required)
        #[arg(long, short = 'o', required = true)]
        output: PathBuf,
        /// File format: json | jsonl | csv
        #[arg(long, short = 'F', default_value = "json")]
        format: ExportFormat,
        /// Filter: starting ledger
        #[arg(long)]
        from_ledger: Option<i64>,
        /// Filter: ending ledger
        #[arg(long)]
        to_ledger: Option<i64>,
        /// Filter: contract ID
        #[arg(long, short = 'c')]
        contract: Option<String>,
        /// Filter: event type
        #[arg(long, short = 't')]
        event_type: Option<String>,
        /// Maximum number of records to export
        #[arg(long, default_value_t = 100_000)]
        max: u64,
    },

    /// Manage CLI configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },

    /// Manage event subscriptions
    Subscriptions {
        #[command(subcommand)]
        action: SubscriptionAction,
    },

    /// Print a shell completion script (bash, zsh, fish, powershell, elvish)
    Completions {
        /// Shell to generate completions for
        shell: clap_complete::Shell,
    },

    /// Stream live events as NDJSON with color-coded output (one JSON object per line)
    Tail {
        /// Contract ID to stream events for
        #[arg(long, short = 'c')]
        contract: String,
        /// Filter: event type (contract, diagnostic, system)
        #[arg(long = "type", short = 't')]
        event_type: Option<String>,
        /// Disable color output
        #[arg(long)]
        no_color: bool,
    },

    /// Trigger a ledger replay and monitor progress
    Replay {
        /// Starting ledger sequence
        #[arg(long)]
        from_ledger: i64,
        /// Ending ledger sequence (optional)
        #[arg(long)]
        to_ledger: Option<i64>,
    },

    /// Send a synthetic test event to a webhook URL and report the result
    WebhookTest {
        /// Callback URL to POST the test payload to
        url: String,
        /// Contract ID to embed in the sample event
        #[arg(long, default_value = "CTESTCONTRACTID0000000000000000000000000000000000000")]
        contract: String,
        /// Seconds to wait for a response before timing out
        #[arg(long, default_value_t = 10)]
        timeout: u64,
    },
}

#[derive(Subcommand)]
enum SubscriptionAction {
    /// Create a new subscription
    Create {
        /// URL events will be delivered to
        #[arg(long)]
        callback_url: String,
        /// Ledger sequence to start streaming from
        #[arg(long)]
        from_ledger: i64,
        /// Delivery mode (e.g. "immediate", "batch")
        #[arg(long)]
        subscription_type: Option<String>,
        /// Events per batch (batch mode only)
        #[arg(long)]
        batch_size: Option<i32>,
        /// Max time to wait before flushing a partial batch, in ms
        #[arg(long)]
        batch_timeout_ms: Option<i32>,
    },
    /// Fetch a subscription by id
    Get { id: String },
    /// Cancel and delete a subscription
    Delete { id: String },
    /// Acknowledge delivery up to a ledger, advancing the subscription cursor
    Ack {
        id: String,
        #[arg(long)]
        ledger: i64,
    },
    /// Pause delivery without deleting the subscription
    Pause {
        id: String,
        /// Pause duration in seconds (omit to pause indefinitely)
        #[arg(long)]
        seconds: Option<i64>,
        #[arg(long)]
        reason: Option<String>,
    },
    /// Resume a paused subscription
    Resume {
        id: String,
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Set a config value
    Set {
        /// Config key (base_url, api_key, admin_api_key, default_format, default_limit, timeout_secs)
        key: String,
        /// Value to set
        value: String,
    },
    /// Get a config value
    Get {
        key: String,
    },
    /// Show all config values
    Show,
    /// Print the config file path
    Path,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    if let Err(e) = run() {
        eprintln!("{} {e:#}", "error:".red().bold());
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    // Completions need no config, so a broken config file cannot break them.
    if let Commands::Completions { shell } = cli.command {
        clap_complete::generate(shell, &mut Cli::command(), "spulse", &mut std::io::stdout());
        return Ok(());
    }

    // Load config and apply CLI overrides
    let mut cfg = Config::load()?;
    if let Some(url) = cli.base_url    { cfg.base_url = url; }
    if let Some(key) = cli.api_key     { cfg.api_key = key; }
    let format = cli.format.unwrap_or_else(|| {
        match cfg.default_format.as_str() {
            "json"  => Format::Json,
            "csv"   => Format::Csv,
            _       => Format::Table,
        }
    });

    match cli.command {
        Commands::Events {
            from_ledger, to_ledger, contract, event_type, tx,
            limit, page, sort, sort_by, output, all, max, export_format,
        } => {
            let query = EventQuery {
                from_ledger,
                to_ledger,
                contract_id: contract.clone(),
                event_type: event_type.clone(),
                tx_hash: tx,
                limit,
                page,
                sort,
                sort_by,
            };

            if let Some(path) = output {
                // Export mode: paginate automatically
                let client = ApiClient::new(&cfg)?;
                let n = export_events(
                    &client,
                    query,
                    export_format,
                    &path,
                    if all { Some(max) } else { Some(limit as u64) },
                )?;
                println!("{} {} record(s) → {}", "Exported".green().bold(), n, path.display());
            } else {
                let client = ApiClient::new(&cfg)?;
                let owned = query.to_params();
                let params: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                let resp: EventsResponse = client.get(&query.path(), &params)?;
                print_events(&resp.into_events(), format)?;
            }
        }

        Commands::Contracts { search, limit, page } => {
            let q = ContractQuery { search, limit, page };
            let client = ApiClient::new(&cfg)?;
            let owned = q.to_params();
            let params: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

            // Try to parse as a list of Contract objects
            let contracts: Vec<Contract> = client.get(q.path(), &params).unwrap_or_default();
            print_contracts(&contracts, format)?;
        }

        Commands::Stats { contract } => {
            let client = ApiClient::new(&cfg)?;
            let path = if let Some(ref id) = contract {
                format!("/v1/contracts/{id}/summary")
            } else {
                "/v1/events/stats".into()
            };
            let stats: EventStats = client.get(&path, &[])?;
            print_stats(&stats, format)?;
        }

        Commands::Export { output, format: efmt, from_ledger, to_ledger, contract, event_type, max } => {
            let query = EventQuery {
                from_ledger,
                to_ledger,
                contract_id: contract,
                event_type,
                limit: 100,
                page: 1,
                sort: "asc".into(),
                sort_by: "ledger".into(),
                ..Default::default()
            };
            let client = ApiClient::new(&cfg)?;
            let n = export_events(&client, query, efmt, &output, Some(max))?;
            println!("{} {} record(s) → {}", "Exported".green().bold(), n, output.display());
        }

        Commands::Completions { .. } => unreachable!("handled before loading config"),

        Commands::Config { action } => handle_config(action)?,

        Commands::Subscriptions { action } => handle_subscription(&cfg, action)?,

        Commands::Tail { contract, event_type, no_color } => {
            let client = ApiClient::new(&cfg)?;
            tail::run(&client, &contract, event_type.as_deref(), no_color)?;
        }

        Commands::Replay { from_ledger, to_ledger } => {
            let client = ApiClient::new(&cfg)?;
            replay::run(&client, from_ledger, to_ledger)?;
        }

        Commands::WebhookTest { url, contract, timeout } => {
            let result = webhook_test::send(&url, &contract, timeout)?;
            webhook_test::print_result(&url, &result);
            if !(200..300).contains(&result.status) {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Subscriptions subcommand handler
// ---------------------------------------------------------------------------

fn handle_subscription(cfg: &Config, action: SubscriptionAction) -> Result<()> {
    let client = ApiClient::new(cfg)?;
    match action {
        SubscriptionAction::Create { callback_url, from_ledger, subscription_type, batch_size, batch_timeout_ms } => {
            let sub = subscription::create(&client, callback_url, from_ledger, subscription_type, batch_size, batch_timeout_ms)?;
            println!("{} {}", "Created subscription".green().bold(), sub.id);
            println!("{}", serde_json::to_string_pretty(&sub)?);
        }
        SubscriptionAction::Get { id } => {
            let sub = subscription::get(&client, &id)?;
            println!("{}", serde_json::to_string_pretty(&sub)?);
        }
        SubscriptionAction::Delete { id } => {
            subscription::delete(&client, &id)?;
            println!("{} {}", "Deleted subscription".green().bold(), id);
        }
        SubscriptionAction::Ack { id, ledger } => {
            let resp = subscription::ack(&client, &id, ledger)?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
        SubscriptionAction::Pause { id, seconds, reason } => {
            let resp = subscription::pause(&client, &id, seconds, reason)?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
        SubscriptionAction::Resume { id, reason } => {
            let resp = subscription::resume(&client, &id, reason)?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Config subcommand handler
// ---------------------------------------------------------------------------

fn handle_config(action: ConfigAction) -> Result<()> {
    match action {
        ConfigAction::Set { key, value } => {
            let mut cfg = Config::load()?;
            cfg.set(&key, &value)?;
            cfg.save()?;
            println!("{} {}={}", "Set".green().bold(), key, value);
        }
        ConfigAction::Get { key } => {
            let cfg = Config::load()?;
            println!("{}", cfg.get(&key)?);
        }
        ConfigAction::Show => {
            let cfg = Config::load()?;
            println!("{:<16} {}", "base_url",       cfg.base_url);
            println!("{:<16} {}", "api_key",        mask(&cfg.api_key));
            println!("{:<16} {}", "admin_api_key",  mask(&cfg.admin_api_key));
            println!("{:<16} {}", "default_format", cfg.default_format);
            println!("{:<16} {}", "default_limit",  cfg.default_limit);
            println!("{:<16} {}s", "timeout",       cfg.timeout_secs);
        }
        ConfigAction::Path => {
            println!("{}", Config::path()?.display());
        }
    }
    Ok(())
}

fn mask(s: &str) -> String {
    if s.is_empty() { return "(not set)".dimmed().to_string(); }
    if s.len() <= 4 { return "****".into(); }
    format!("{}****", &s[..4])
}
