//! Per-contract resource and fee statistics — Issue #1257.
//!
//! The indexer's resource sync (`crate::resources`, enabled with
//! `RESOURCE_STATS_ENABLED=true`) fills `events.resource_fee`, `cpu_insns` and
//! `mem_bytes` from each transaction's meta. `mv_contract_resource_stats`
//! aggregates them per contract (one row per transaction, not per event) and
//! is refreshed hourly by [`spawn_refresh`], which also publishes the top
//! contracts as Prometheus gauges for the Grafana dashboard.
//!
//! `GET /v1/contracts/{contract_id}/stats` serves the aggregates.

use std::time::Duration;

use axum::{
    extract::{Path, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::watch;
use utoipa::ToSchema;

use crate::{error::AppError, handlers::validate_contract_id, routes::AppState};

/// Materialized view holding the per-contract aggregates.
pub const VIEW: &str = "mv_contract_resource_stats";

/// How often the view is refreshed.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(3600);

/// Number of contracts (by total resource fee) exported as Prometheus gauges.
const TOP_CONTRACTS_EXPORTED: i64 = 20;

/// Aggregates of one resource across a contract's transactions. Fields are
/// `null` when no transaction reported that resource.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct ResourceAggregate {
    pub total: Option<i64>,
    pub avg: Option<f64>,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub max: Option<i64>,
}

/// Response body for `GET /v1/contracts/{contract_id}/stats`.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
pub struct ContractResourceStats {
    pub contract_id: String,
    /// Transactions with resource data that emitted events for this contract.
    pub tx_count: i64,
    pub first_ledger: i64,
    pub last_ledger: i64,
    /// Charged Soroban resource fee per transaction, in stroops.
    pub resource_fee: ResourceAggregate,
    /// CPU instructions per transaction.
    pub cpu_insns: ResourceAggregate,
    /// Memory bytes per transaction (requires RPC diagnostic events).
    pub mem_bytes: ResourceAggregate,
    /// When the aggregates were last refreshed (hourly).
    pub refreshed_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct StatsRow {
    contract_id: String,
    tx_count: i64,
    first_ledger: i64,
    last_ledger: i64,
    total_resource_fee: Option<i64>,
    avg_resource_fee: Option<f64>,
    p50_resource_fee: Option<f64>,
    p95_resource_fee: Option<f64>,
    max_resource_fee: Option<i64>,
    total_cpu_insns: Option<i64>,
    avg_cpu_insns: Option<f64>,
    p50_cpu_insns: Option<f64>,
    p95_cpu_insns: Option<f64>,
    max_cpu_insns: Option<i64>,
    total_mem_bytes: Option<i64>,
    avg_mem_bytes: Option<f64>,
    p50_mem_bytes: Option<f64>,
    p95_mem_bytes: Option<f64>,
    max_mem_bytes: Option<i64>,
    refreshed_at: DateTime<Utc>,
}

impl From<StatsRow> for ContractResourceStats {
    fn from(r: StatsRow) -> Self {
        Self {
            contract_id: r.contract_id,
            tx_count: r.tx_count,
            first_ledger: r.first_ledger,
            last_ledger: r.last_ledger,
            resource_fee: ResourceAggregate {
                total: r.total_resource_fee,
                avg: r.avg_resource_fee,
                p50: r.p50_resource_fee,
                p95: r.p95_resource_fee,
                max: r.max_resource_fee,
            },
            cpu_insns: ResourceAggregate {
                total: r.total_cpu_insns,
                avg: r.avg_cpu_insns,
                p50: r.p50_cpu_insns,
                p95: r.p95_cpu_insns,
                max: r.max_cpu_insns,
            },
            mem_bytes: ResourceAggregate {
                total: r.total_mem_bytes,
                avg: r.avg_mem_bytes,
                p50: r.p50_mem_bytes,
                p95: r.p95_mem_bytes,
                max: r.max_mem_bytes,
            },
            refreshed_at: r.refreshed_at,
        }
    }
}

/// Aggregated resource usage and fees for a contract.
#[utoipa::path(
    get,
    path = "/v1/contracts/{contract_id}/stats",
    tag = "events",
    params(("contract_id" = String, Path, description = "Stellar contract ID")),
    responses(
        (status = 200, description = "Aggregated resource usage, refreshed hourly", body = ContractResourceStats),
        (status = 400, description = "Invalid contract_id"),
        (status = 404, description = "No resource data recorded for this contract"),
    )
)]
pub async fn get_contract_resource_stats(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<ContractResourceStats>, AppError> {
    validate_contract_id(&contract_id)?;
    let row: Option<StatsRow> = sqlx::query_as(
        "SELECT contract_id, tx_count, first_ledger, last_ledger,
                total_resource_fee, avg_resource_fee, p50_resource_fee, p95_resource_fee, max_resource_fee,
                total_cpu_insns, avg_cpu_insns, p50_cpu_insns, p95_cpu_insns, max_cpu_insns,
                total_mem_bytes, avg_mem_bytes, p50_mem_bytes, p95_mem_bytes, max_mem_bytes,
                refreshed_at
         FROM mv_contract_resource_stats
         WHERE contract_id = $1",
    )
    .bind(&contract_id)
    .fetch_optional(&state.pool)
    .await?;
    row.map(|r| Json(r.into())).ok_or(AppError::NotFound)
}

/// Publish the top contracts by total resource fee as Prometheus gauges.
async fn update_metrics(pool: &PgPool) {
    let rows: Vec<(String, Option<i64>, Option<f64>, Option<f64>)> = match sqlx::query_as(
        "SELECT contract_id, total_resource_fee, avg_cpu_insns, avg_mem_bytes
         FROM mv_contract_resource_stats
         ORDER BY total_resource_fee DESC NULLS LAST
         LIMIT $1",
    )
    .bind(TOP_CONTRACTS_EXPORTED)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to read contract resource stats for metrics");
            return;
        }
    };
    for (contract_id, total_fee, avg_cpu, avg_mem) in rows {
        crate::metrics::update_contract_resource_stats(&contract_id, total_fee, avg_cpu, avg_mem);
    }
}

/// Refresh [`VIEW`] every [`REFRESH_INTERVAL`] (and once at startup) until shutdown.
pub fn spawn_refresh(pool: PgPool, mut shutdown: watch::Receiver<bool>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(REFRESH_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    crate::stats_refresh::refresh_one(&pool, VIEW).await;
                    update_metrics(&pool).await;
                }
                res = shutdown.changed() => {
                    if res.is_err() || *shutdown.borrow() {
                        tracing::info!("Contract resource stats refresh task shutting down");
                        break;
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> StatsRow {
        StatsRow {
            contract_id: "C".repeat(56),
            tx_count: 2,
            first_ledger: 10,
            last_ledger: 12,
            total_resource_fee: Some(4000),
            avg_resource_fee: Some(2000.0),
            p50_resource_fee: Some(2000.0),
            p95_resource_fee: Some(2900.0),
            max_resource_fee: Some(3000),
            total_cpu_insns: Some(2000),
            avg_cpu_insns: Some(1000.0),
            p50_cpu_insns: Some(1000.0),
            p95_cpu_insns: Some(1450.0),
            max_cpu_insns: Some(1500),
            total_mem_bytes: None,
            avg_mem_bytes: None,
            p50_mem_bytes: None,
            p95_mem_bytes: None,
            max_mem_bytes: None,
            refreshed_at: DateTime::from_timestamp(1_760_000_000, 0).unwrap(),
        }
    }

    #[test]
    fn row_maps_to_nested_response() {
        let stats = ContractResourceStats::from(row());
        assert_eq!(stats.tx_count, 2);
        assert_eq!(stats.resource_fee.total, Some(4000));
        assert_eq!(stats.resource_fee.p95, Some(2900.0));
        assert_eq!(stats.cpu_insns.max, Some(1500));
        assert_eq!(stats.mem_bytes.total, None);
    }

    #[test]
    fn response_json_shape() {
        let v = serde_json::to_value(ContractResourceStats::from(row())).unwrap();
        assert_eq!(v["resource_fee"]["avg"], 2000.0);
        assert!(v["mem_bytes"]["p50"].is_null());
        assert_eq!(v["first_ledger"], 10);
        assert!(v["refreshed_at"].is_string());
    }
}
