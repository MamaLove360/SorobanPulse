//! Issue #1148: Seed-data generator for local development and demos.
//!
//! Inserts realistic Soroban contract events, subscriptions, notification
//! channels, and contract labels into the database so that the dashboard
//! and API show a populated state immediately after `make seed`.
//!
//! # Usage
//!
//! ```text
//! cargo run --bin seed -- --events 500
//! cargo run --bin seed -- --events 100000   # performance-testing volume
//! ```
//!
//! All flags are optional; defaults are listed in [`SeedArgs`].
//!
//! # Environment
//!
//! Reads `DATABASE_URL` from the environment (or a `.env` file in the cwd).
//! The target database must already have migrations applied.
// Issue #1246: a panic in production is a silent availability incident.
// Test code is exempt via `allow-{unwrap,expect}-in-tests` in .clippy.toml.
#![deny(clippy::unwrap_used, clippy::expect_used)]

// Precision loss in timestamp/ledger arithmetic is acceptable for seed data.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    // Manual iterator advancement is necessary for flag value parsing.
    clippy::while_let_on_iterator
)]

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rand::prelude::*;
use serde_json::json;
use std::time::Instant;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// CLI args (hand-rolled — avoids adding clap as a required dep)
// ---------------------------------------------------------------------------

struct SeedArgs {
    /// Total number of events to insert.
    events: usize,
    /// DATABASE_URL override (falls back to env var / .env).
    database_url: Option<String>,
}

impl SeedArgs {
    fn parse() -> Result<Self> {
        let raw: Vec<String> = std::env::args().skip(1).collect();
        let mut iter = raw.iter();
        let mut events: usize = 500;
        let mut database_url: Option<String> = None;

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--events" | "-e" => {
                    let val = iter.next().context("--events requires a value")?;
                    events = val
                        .parse::<usize>()
                        .context("--events must be a positive integer")?;
                }
                "--database-url" | "--database_url" => {
                    database_url =
                        Some(iter.next().context("--database-url requires a value")?.clone());
                }
                "--help" | "-h" => {
                    eprintln!(
                        "Usage: seed [--events <N>] [--database-url <URL>]\n\
                         \n\
                         Options:\n\
                           --events, -e          Total events to insert (default: 500)\n\
                           --database-url        Override DATABASE_URL env var\n\
                           --help, -h            Show this message"
                    );
                    std::process::exit(0);
                }
                other => {
                    anyhow::bail!("Unknown argument: {other}. Run with --help for usage.");
                }
            }
        }

        Ok(Self {
            events,
            database_url,
        })
    }
}

// ---------------------------------------------------------------------------
// Constants / fixtures
// ---------------------------------------------------------------------------

/// Realistic-looking Soroban contract IDs (56-char Strkey C… addresses).
const CONTRACTS: &[(&str, &str, &str)] = &[
    (
        "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM",
        "Stellar DEX Aggregator",
        "SEP-41 token swap router",
    ),
    (
        "CBIFLQJBGB4E4RHXQXYMCYTHNVMKJXOCRQYAJJPWMF6TWRP3VDLKKGZ",
        "USDC Token",
        "Circle USDC on Soroban",
    ),
    (
        "CBDIZQUQVLZIPBKDYQ4KLNYBKPKZCTPQZ4BNK4ZKXGMJCBRWZPFRLVX",
        "XLM Wrapped",
        "Wrapped native XLM SAC",
    ),
    (
        "CCWAMYJME4AALYIPCN4LHFGEGN7COYKUYSQWPIYPXHJLNFGQZK4TM4Y",
        "Yield Aggregator",
        "Auto-compounding yield vault",
    ),
    (
        "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYC",
        "Governance Token",
        "On-chain DAO voting contract",
    ),
    (
        "CEQPFWTCXCSMIJZPBDL6NI56LPIBDCCYQQMFMDGBMRCLBQDLPQG5CPM",
        "NFT Marketplace",
        "Soroban non-fungible token market",
    ),
    (
        "CFRQXFMTUUPUIZLXUDLYGIWH47HPICJXEFXLZ7RSTBQAJUMJZPXV3XM",
        "Liquidity Pool",
        "Constant-product AMM liquidity pool",
    ),
    (
        "CGZFIDWSQ4SF7SGQKMF6BF4PSWWKQCSFAQKNHVHYORFLZAPN5Q7T3XE",
        "Lending Protocol",
        "Over-collateralised lending and borrowing",
    ),
];

/// Event types weighted toward "contract" (more realistic traffic distribution).
const EVENT_TYPES: &[&str] = &["contract", "contract", "contract", "diagnostic", "system"];

/// SEP-41 and other realistic event topics.
///
/// Returns owned `Value`s — `serde_json::Value` is not `Copy` so this cannot
/// be a `const`.
fn topics() -> Vec<(&'static str, serde_json::Value)> {
    vec![
        (
            "transfer",
            json!({
                "from":   "GDEV1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB",
                "to":     "GDEV2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB",
                "amount": "10000000"
            }),
        ),
        (
            "mint",
            json!({
                "to":     "GDEV1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB",
                "amount": "50000000"
            }),
        ),
        (
            "burn",
            json!({
                "from":   "GDEV2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB",
                "amount": "25000000"
            }),
        ),
        (
            "swap",
            json!({
                "token_in":   "XLM",
                "token_out":  "USDC",
                "amount_in":  "100000000",
                "amount_out": "9987234"
            }),
        ),
        (
            "approve",
            json!({
                "owner":             "GDEV1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB",
                "spender":           "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM",
                "amount":            "999999999",
                "expiration_ledger": 5_000_000
            }),
        ),
        (
            "set_admin",
            json!({
                "new_admin": "GDEV3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB"
            }),
        ),
        (
            "liquidity_added",
            json!({
                "amount_a":      "10000000",
                "amount_b":      "9850000",
                "shares_minted": "9924950"
            }),
        ),
        (
            "liquidity_removed",
            json!({
                "amount_a":     "5000000",
                "amount_b":     "4925000",
                "shares_burned":"4962475"
            }),
        ),
        (
            "vote_cast",
            json!({
                "proposal_id": 42,
                "voter":       "GDEV1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB",
                "weight":      "10000000",
                "support":     true
            }),
        ),
        (
            "log",
            json!({
                "level":   "info",
                "message": "contract execution completed successfully"
            }),
        ),
    ]
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env if present (best-effort; ignore error if file not found).
    let _ = dotenv::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("seed=info,warn")),
        )
        .init();

    let args = SeedArgs::parse()?;

    let database_url = args
        .database_url
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .context(
            "DATABASE_URL is not set. \
             Set it in your environment, a .env file, or pass --database-url.",
        )?;

    tracing::info!(events = args.events, "Starting seed run");
    let t0 = Instant::now();

    // Build a small pool — the seed binary doesn't need many connections.
    let settings = soroban_pulse::db_pool::DbPoolSettings {
        max_connections: 5,
        min_connections: 1,
        ..soroban_pulse::db_pool::DbPoolSettings::default()
    };
    let pool = soroban_pulse::db_pool::connect(&database_url, &settings)
        .await
        .context("Failed to connect to the database")?;

    // Run all seeding steps.
    seed_notification_channels(&pool).await?;
    seed_subscriptions(&pool).await?;
    seed_contract_metadata(&pool).await?;
    let inserted = seed_events(&pool, args.events).await?;

    let elapsed = t0.elapsed();
    tracing::info!(
        events_inserted = inserted,
        elapsed_ms = elapsed.as_millis(),
        "Seed run complete"
    );

    println!(
        "✓ Seed complete — {inserted} events inserted in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Seeders
// ---------------------------------------------------------------------------

/// Insert sample notification channels (webhook, email, SMS).
async fn seed_notification_channels(pool: &sqlx::PgPool) -> Result<()> {
    let channels = vec![
        (
            "dev-webhook",
            "webhook",
            json!({"url": "http://localhost:9001/webhook", "secret": "dev-secret-do-not-use"}),
            json!({"max_attempts": 3, "initial_backoff_ms": 1000,
                   "backoff_multiplier": 2.0, "max_backoff_ms": 60000}),
        ),
        (
            "dev-email",
            "email",
            json!({"to": "dev@example.com", "from": "noreply@sorobanpulse.dev",
                   "smtp_host": "localhost", "smtp_port": 1025}),
            json!({"max_attempts": 5, "initial_backoff_ms": 2000,
                   "backoff_multiplier": 1.5, "max_backoff_ms": 120000}),
        ),
        (
            "dev-sms",
            "sms",
            json!({"to": "+15550001234", "provider": "twilio",
                   "account_sid": "dev_sid", "auth_token": "dev_token"}),
            json!({"max_attempts": 2, "initial_backoff_ms": 5000,
                   "backoff_multiplier": 2.0, "max_backoff_ms": 30000}),
        ),
    ];

    let mut inserted = 0u32;
    for (name, channel_type, config, retry_policy) in &channels {
        let rows = sqlx::query(
            "INSERT INTO notification_channels (id, name, channel_type, config, retry_policy)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (name) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(name)
        .bind(channel_type)
        .bind(config)
        .bind(retry_policy)
        .execute(pool)
        .await
        .context("Failed to insert notification channel")?
        .rows_affected();

        inserted += rows as u32;
    }

    tracing::info!(inserted, "Notification channels seeded");
    Ok(())
}

/// Insert sample subscriptions.
async fn seed_subscriptions(pool: &sqlx::PgPool) -> Result<()> {
    let subscriptions: &[(&str, i64)] = &[
        ("http://localhost:9001/webhook", 1_000_000),
        ("http://localhost:9002/events", 1_000_000),
        ("http://dev-consumer.internal/callback", 999_000),
    ];

    let mut inserted = 0u32;
    for (callback_url, from_ledger) in subscriptions {
        let rows = sqlx::query(
            "INSERT INTO subscriptions (id, callback_url, from_ledger, acked_ledger, status)
             VALUES ($1, $2, $3, 0, 'active')
             ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(callback_url)
        .bind(from_ledger)
        .execute(pool)
        .await
        .context("Failed to insert subscription")?
        .rows_affected();

        inserted += rows as u32;
    }

    tracing::info!(inserted, "Subscriptions seeded");
    Ok(())
}

/// Insert contract labels into `contract_metadata`.
async fn seed_contract_metadata(pool: &sqlx::PgPool) -> Result<()> {
    let mut inserted = 0u32;
    for (contract_id, name, description) in CONTRACTS {
        let tags: Vec<&str> =
            if description.contains("SEP-41") || description.contains("token") {
                vec!["token", "sep-41"]
            } else if description.contains("swap")
                || description.contains("AMM")
                || description.contains("liquidity")
            {
                vec!["defi", "amm", "dex"]
            } else if description.contains("lending") {
                vec!["defi", "lending"]
            } else if description.contains("NFT") {
                vec!["nft", "marketplace"]
            } else if description.contains("DAO") || description.contains("voting") {
                vec!["governance", "dao"]
            } else {
                vec!["soroban"]
            };

        let rows = sqlx::query(
            "INSERT INTO contract_metadata
               (contract_id, name, description, verified, tags)
             VALUES ($1, $2, $3, true, $4)
             ON CONFLICT (contract_id) DO NOTHING",
        )
        .bind(contract_id)
        .bind(name)
        .bind(description)
        .bind(&tags)
        .execute(pool)
        .await
        .context("Failed to insert contract metadata")?
        .rows_affected();

        inserted += rows as u32;
    }

    tracing::info!(inserted, "Contract metadata entries seeded");
    Ok(())
}

/// Insert `total` events spread across all sample contracts, topics, and a
/// 30-day timestamp range.
///
/// Inserts in batches of 500 to keep memory low even for 100 000+ events.
async fn seed_events(pool: &sqlx::PgPool, total: usize) -> Result<usize> {
    let mut rng = SmallRng::seed_from_u64(1148); // deterministic for reproducibility

    // Build the topics table once (not a const because Value isn't Copy).
    let topic_table = topics();

    // 30 days of history, ending now.
    let end: DateTime<Utc> = Utc::now();
    let start: DateTime<Utc> = end - Duration::days(30);
    let window_secs = (end - start).num_seconds();

    // Starting ledger corresponding to ~30 days ago at ~5 s / ledger.
    let base_ledger: i64 = 1_000_000;
    let ledgers_per_second: f64 = 0.2; // 1 ledger every 5 s

    const BATCH: usize = 500;
    let mut total_inserted: usize = 0;

    for chunk_start in (0..total).step_by(BATCH) {
        let chunk_end = (chunk_start + BATCH).min(total);
        let mut tx = pool.begin().await.context("Failed to start transaction")?;

        for i in chunk_start..chunk_end {
            // Spread events linearly over the 30-day window with small jitter.
            let progress = i as f64 / total.max(1) as f64;
            let jitter_secs = rng.gen_range(-120_i64..=120_i64);
            let offset_secs = (progress * window_secs as f64) as i64 + jitter_secs;
            let offset_secs = offset_secs.max(0);
            let ts = start + Duration::seconds(offset_secs);
            let ledger = base_ledger + (offset_secs as f64 * ledgers_per_second) as i64;

            let (contract_id, _, _) = CONTRACTS
                .choose(&mut rng)
                .context("CONTRACTS fixture table is empty")?;
            let event_type = EVENT_TYPES
                .choose(&mut rng)
                .context("EVENT_TYPES fixture table is empty")?;
            let (topic, value) = topic_table
                .choose(&mut rng)
                .context("topic fixture table is empty")?;

            // Generate a pseudo-random 64-character hex tx hash.
            let tx_hash = format!("{:016x}{:016x}{:016x}{:016x}",
                rng.gen::<u64>(), rng.gen::<u64>(),
                rng.gen::<u64>(), rng.gen::<u64>());

            // event_data follows the shape expected by the API:
            //   { "topic": ["<symbol>"], "value": { … } }
            let event_data = json!({
                "topic": [topic],
                "value": value,
            });

            sqlx::query(
                "INSERT INTO events
                   (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (tx_hash, contract_id, event_type) DO NOTHING",
            )
            .bind(Uuid::new_v4())
            .bind(contract_id)
            .bind(*event_type)
            .bind(&tx_hash)
            .bind(ledger)
            .bind(ts)
            .bind(&event_data)
            .execute(&mut *tx)
            .await
            .context("Failed to insert event")?;

            total_inserted += 1;
        }

        tx.commit().await.context("Failed to commit batch")?;

        // Progress log every 10 000 events (approximate).
        if chunk_end > 0 && (chunk_end % 10_000) < BATCH {
            tracing::info!(inserted = total_inserted, total, "Event seeding progress");
        }
    }

    tracing::info!(total_inserted, "Events seeded");
    Ok(total_inserted)
}
