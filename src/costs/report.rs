/// Cost report generation.
///
/// Builds human-readable and JSON-serialisable cost reports, and
/// provides a background task that records periodic cost snapshots.
use super::{
    calculator::CostCalculator,
    compute::{available_vcpus, current_cpu_utilization, current_memory_bytes, ComputeUsage},
    forecast,
    models::{CostForecast, CostReport},
};
use chrono::{Duration, Utc};
use sqlx::PgPool;
use std::time::Duration as StdDuration;
use tracing::{info, warn};

/// Generate a cost report for the last `hours` hours.
pub fn generate_report(calculator: &CostCalculator, hours: i64) -> CostReport {
    let period_start = Utc::now() - Duration::hours(hours);
    calculator.report(period_start)
}

/// Generate a cost forecast for the next `forecast_days` days.
///
/// Per spec Requirement 6, the linear-regression model in [`forecast`] is fed
/// with the last 14 days of recorded entries. If fewer than 7 days of history
/// are available the forecast is rejected as "insufficient data" and `None`
/// is returned.
pub fn generate_forecast(calculator: &CostCalculator, forecast_days: u32) -> Option<CostForecast> {
    // Pull the last 14 days of entries for regression input.
    let report = calculator.report(Utc::now() - Duration::days(14));
    let entries: Vec<_> = report.breakdown.details.values().cloned().collect();

    // Require at least 7 days of history before forecasting.
    let oldest = entries.iter().map(|e| e.timestamp).min();
    let has_enough_history = match oldest {
        Some(ts) => Utc::now().signed_duration_since(ts) >= Duration::days(7),
        None => false,
    };
    if !has_enough_history {
        warn!(
            entry_count = entries.len(),
            "Insufficient cost history for forecast (need >= 7 days)"
        );
        return None;
    }

    forecast::forecast(&entries, forecast_days)
}

/// Spawn a background Tokio task that records cost snapshots every `interval`.
///
/// The task records one database snapshot and one compute snapshot per tick,
/// then prunes entries older than 30 days to bound memory usage.
///
/// The task exits cleanly when `shutdown_rx` fires.
pub fn spawn_collector(
    calculator: CostCalculator,
    pool: PgPool,
    interval: StdDuration,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    collect_snapshot(&calculator, &pool).await;
                    // Retain 30 days of history.
                    calculator.prune_older_than(24 * 30);
                }
                _ = shutdown_rx.changed() => {
                    info!("Cost collector shutting down");
                    break;
                }
            }
        }
    });
}

/// Collect one cost snapshot from live system state and database pool stats.
async fn collect_snapshot(calculator: &CostCalculator, pool: &PgPool) {
    // --- database snapshot ---
    let db_usage = collect_db_usage(pool).await;
    // Each snapshot covers 1 minute (1/60 of an hour).
    calculator.record_database(&db_usage, 1.0 / 60.0);

    // --- compute snapshot ---
    let cpu_util = current_cpu_utilization().unwrap_or(0.0);
    let memory_bytes = current_memory_bytes().unwrap_or(0);
    let vcpus = available_vcpus();

    let compute_usage = ComputeUsage {
        vcpu_count: vcpus,
        cpu_utilization: cpu_util,
        memory_bytes,
        request_count: 0, // updated separately via RequestCounter
        worker_threads: vcpus,
    };
    calculator.record_compute(&compute_usage, 1.0 / 60.0);

    info!(
        db_connections = db_usage.active_connections,
        cpu_utilization = cpu_util,
        vcpus,
        "Cost snapshot recorded"
    );
}

async fn collect_db_usage(pool: &PgPool) -> super::database::DatabaseUsage {
    let active_connections = u32::try_from(pool.size()).unwrap_or(0);
    let max_connections = u32::try_from(pool.options().get_max_connections()).unwrap_or(0);

    // Attempt to query pg_stat_database for real I/O stats.
    let (query_count, data_transfer_bytes) =
        match query_pg_stats(pool).await {
            Ok(stats) => stats,
            Err(e) => {
                warn!(error = %e, "Failed to query pg_stat_database; using zeros");
                (0, 0)
            }
        };

    super::database::DatabaseUsage {
        active_connections,
        max_connections,
        pool_size: active_connections,
        pool_idle: u32::try_from(pool.num_idle()).unwrap_or(0),
        query_count,
        data_transfer_bytes,
        storage_bytes: 0,
        iops: query_count, // approximate: 1 IOPS per query
    }
}

/// Query cumulative stats from `pg_stat_database` for the current database.
async fn query_pg_stats(pool: &PgPool) -> Result<(u64, u64), sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT
            COALESCE(xact_commit + xact_rollback, 0) AS transactions,
            COALESCE(blks_read + blks_hit, 0)        AS block_accesses
        FROM pg_stat_database
        WHERE datname = current_database()
        "#
    )
    .fetch_one(pool)
    .await?;

    let txns = u64::try_from(row.transactions.unwrap_or(0)).unwrap_or(0);
    // Each block access ≈ 8 KB.
    let bytes = u64::try_from(row.block_accesses.unwrap_or(0)).unwrap_or(0) * 8192;

    Ok((txns, bytes))
}
