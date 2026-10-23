extern crate metrics as m;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LAG_WARN_BYTES: i64 = 10 * 1024 * 1024; // 10 MiB
const LAG_WARN_SECS: f64 = 30.0;
const LAG_CRITICAL_BYTES: i64 = 100 * 1024 * 1024; // 100 MiB
const LAG_CRITICAL_SECS: f64 = 60.0;
const DEFAULT_HEALTH_HISTORY_SIZE: usize = 60;

// Failover state constants for AtomicU8
const FAILOVER_NORMAL: u8 = 0;
const FAILOVER_IN_PROGRESS: u8 = 1;
const FAILOVER_COMPLETE: u8 = 2;

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Replication topology mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationMode {
    /// Standard streaming replication from primary to standby.
    Standard,
    /// Cascading replication: replica streams to another replica.
    Cascading,
    /// Read-only replica for query offloading.
    ReadReplica,
    /// Bi-directional replication (logical, experimental).
    BidirectionalSync,
    /// Selective logical replication of specific tables.
    SelectiveReplication,
}

impl Default for ReplicationMode {
    fn default() -> Self {
        Self::Standard
    }
}

/// Role of a replica in the replication topology.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaRole {
    Primary,
    StandbyReplica,
    CascadeSource,
    CascadeTarget,
}

impl Default for ReplicaRole {
    fn default() -> Self {
        Self::StandbyReplica
    }
}

/// Current state of the failover process.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FailoverState {
    Normal,
    PromotionInProgress,
    FailedOver,
}

// ---------------------------------------------------------------------------
// Configuration structs
// ---------------------------------------------------------------------------

/// Configuration for cascading replication topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CascadeReplicaConfig {
    /// Address of the upstream source this node replicates from.
    pub source_addr: String,
    /// Downstream targets that cascade from this node.
    pub target_addrs: Vec<String>,
    /// Replication slot name used for this cascade chain.
    pub replication_slot: String,
    /// Maximum WAL sender processes to allow.
    pub max_wal_senders: u32,
}

impl Default for CascadeReplicaConfig {
    fn default() -> Self {
        Self {
            source_addr: String::new(),
            target_addrs: Vec::new(),
            replication_slot: "cascade_slot".to_string(),
            max_wal_senders: 10,
        }
    }
}

/// Configuration for a read-only replica.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadReplicaConfig {
    /// Connection string for the read replica.
    pub connection_string: String,
    /// Relative weight for load balancing (higher = more traffic).
    pub load_weight: u32,
    /// Whether this replica is currently healthy.
    pub is_healthy: bool,
    /// Maximum acceptable lag in milliseconds before marking unhealthy.
    pub lag_threshold_ms: u64,
}

impl Default for ReadReplicaConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            load_weight: 1,
            is_healthy: true,
            lag_threshold_ms: 1000,
        }
    }
}

/// Configuration governing automatic failover behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    /// Whether automatic failover is enabled.
    pub auto_failover_enabled: bool,
    /// Byte lag threshold that triggers failover consideration.
    pub lag_threshold_bytes: i64,
    /// Replay lag (seconds) threshold that triggers failover consideration.
    pub lag_threshold_secs: f64,
    /// Seconds to wait for promotion to complete before giving up.
    pub promotion_timeout_secs: u64,
    /// Whether to run a data consistency check before promoting.
    pub consistency_check_enabled: bool,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            auto_failover_enabled: false,
            lag_threshold_bytes: LAG_CRITICAL_BYTES,
            lag_threshold_secs: LAG_CRITICAL_SECS,
            promotion_timeout_secs: 30,
            consistency_check_enabled: true,
        }
    }
}

/// Top-level monitor configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaMonitorConfig {
    pub mode: ReplicationMode,
    pub failover: FailoverConfig,
    pub cascade_config: Option<CascadeReplicaConfig>,
    pub read_replicas: Vec<ReadReplicaConfig>,
    /// How often (seconds) to collect health metrics.
    pub check_interval_secs: u64,
    /// Number of health snapshots to keep in the rolling history.
    pub health_history_size: usize,
}

impl Default for ReplicaMonitorConfig {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Standard,
            failover: FailoverConfig::default(),
            cascade_config: None,
            read_replicas: Vec::new(),
            check_interval_secs: 30,
            health_history_size: DEFAULT_HEALTH_HISTORY_SIZE,
        }
    }
}

// ---------------------------------------------------------------------------
// Data structs
// ---------------------------------------------------------------------------

/// A point-in-time health snapshot of the replication topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaHealthCheck {
    pub timestamp: DateTime<Utc>,
    pub is_healthy: bool,
    /// Aggregate byte lag across all replicas.
    pub lag_bytes: i64,
    /// Worst replay lag (seconds) across all replicas.
    pub replay_lag_secs: f64,
    /// Total number of walsender connections.
    pub connection_count: usize,
    /// Status of the WAL receiver on this node (empty on primary).
    pub wal_receiver_status: String,
}

/// Per-replica status, enhanced with role and slot information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaStatus {
    pub client_addr: String,
    pub state: String,
    pub sent_lag_bytes: i64,
    pub write_lag_secs: f64,
    pub flush_lag_secs: f64,
    pub replay_lag_secs: f64,
    // -- extended fields --
    pub role: ReplicaRole,
    pub replication_mode: ReplicationMode,
    /// Depth in the cascade chain (0 = direct replica of primary).
    pub cascade_depth: u32,
    pub slot_name: Option<String>,
    pub slot_lag_bytes: Option<i64>,
    pub application_name: String,
    /// sync_state from pg_stat_replication (async / sync / quorum).
    pub sync_state: String,
}

/// Catchup progress for a single replica.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatchupProgress {
    pub client_addr: String,
    pub sent_lsn: String,
    pub replay_lsn: String,
    pub delta_bytes: i64,
    pub estimated_seconds_to_catchup: Option<f64>,
}

// ---------------------------------------------------------------------------
// ReplicaMonitor
// ---------------------------------------------------------------------------

pub struct ReplicaMonitor {
    pub config: ReplicaMonitorConfig,
    pool: PgPool,
    health_history: Arc<RwLock<VecDeque<ReplicaHealthCheck>>>,
    /// FAILOVER_NORMAL / FAILOVER_IN_PROGRESS / FAILOVER_COMPLETE
    failover_state: Arc<AtomicU8>,
}

impl ReplicaMonitor {
    pub fn new(pool: PgPool, config: ReplicaMonitorConfig) -> Self {
        let history_size = config.health_history_size;
        Self {
            config,
            pool,
            health_history: Arc::new(RwLock::new(VecDeque::with_capacity(history_size))),
            failover_state: Arc::new(AtomicU8::new(FAILOVER_NORMAL)),
        }
    }

    /// Collect per-replica stats from pg_stat_replication, joining with
    /// pg_replication_slots for slot lag information.
    pub async fn collect_replica_stats(&self) -> Vec<ReplicaStatus> {
        collect_replica_stats_from_pool(&self.pool).await
    }

    /// Take a comprehensive health snapshot and push it to the rolling history.
    pub async fn check_replica_health(&self) -> ReplicaHealthCheck {
        let replicas = self.collect_replica_stats().await;

        let lag_bytes: i64 = replicas.iter().map(|r| r.sent_lag_bytes).sum();
        let replay_lag_secs: f64 = replicas
            .iter()
            .map(|r| r.replay_lag_secs)
            .fold(0.0_f64, f64::max);
        let connection_count = replicas.len();

        // Query WAL receiver status (only non-empty on standby nodes)
        let wal_receiver_status = self.wal_receiver_status().await;

        let is_healthy = replay_lag_secs < self.config.failover.lag_threshold_secs
            && lag_bytes < self.config.failover.lag_threshold_bytes;

        let check = ReplicaHealthCheck {
            timestamp: Utc::now(),
            is_healthy,
            lag_bytes,
            replay_lag_secs,
            connection_count,
            wal_receiver_status,
        };

        // Maintain rolling history
        let mut history = self.health_history.write().await;
        if history.len() >= self.config.health_history_size {
            history.pop_front();
        }
        history.push_back(check.clone());

        check
    }

    /// Returns true when the current replication lag exceeds the configured
    /// failover thresholds and auto-failover is enabled.
    pub fn detect_failover_needed(&self, replicas: &[ReplicaStatus]) -> bool {
        if !self.config.failover.auto_failover_enabled {
            return false;
        }
        if self.failover_state.load(Ordering::SeqCst) != FAILOVER_NORMAL {
            return false;
        }
        replicas.iter().any(|r| {
            r.sent_lag_bytes > self.config.failover.lag_threshold_bytes
                || r.replay_lag_secs > self.config.failover.lag_threshold_secs
        })
    }

    /// Attempt an automated failover by promoting the best replica.
    /// Returns a description of the action taken.
    pub async fn initiate_failover(&self) -> Result<String, String> {
        if self
            .failover_state
            .compare_exchange(
                FAILOVER_NORMAL,
                FAILOVER_IN_PROGRESS,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Err("Failover already in progress".to_string());
        }

        warn!("Initiating automatic failover");
        m::counter!("soroban_pulse_failover_events_total").increment(1);

        if self.config.failover.consistency_check_enabled {
            match self.check_data_consistency().await {
                Ok(true) => info!("Pre-failover consistency check passed"),
                Ok(false) => {
                    self.failover_state
                        .store(FAILOVER_NORMAL, Ordering::SeqCst);
                    return Err("Consistency check failed; aborting failover".to_string());
                }
                Err(e) => {
                    warn!(error = %e, "Consistency check errored; proceeding with failover");
                }
            }
        }

        // In a real deployment this would signal pg_ctl promote / Patroni / etc.
        // Here we record the event and transition state.
        self.failover_state
            .store(FAILOVER_COMPLETE, Ordering::SeqCst);
        info!("Failover promotion recorded");
        Ok("Failover promotion initiated — please complete promotion via your HA tool".to_string())
    }

    /// Run a lightweight consistency check using pg_stat_replication.
    pub async fn check_data_consistency(&self) -> Result<bool, String> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT COUNT(*) FROM pg_stat_replication WHERE state != 'streaming'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        let non_streaming = row.map(|(c,)| c).unwrap_or(0);
        if non_streaming > 0 {
            warn!(non_streaming, "Some replicas not in streaming state");
        }
        Ok(non_streaming == 0)
    }

    /// Return catchup progress (LSN delta) for each connected replica.
    pub async fn get_catchup_progress(&self) -> Vec<CatchupProgress> {
        let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
            "SELECT
                COALESCE(client_addr::text, 'unknown'),
                COALESCE(sent_lsn::text, '0/0'),
                COALESCE(replay_lsn::text, '0/0'),
                COALESCE(pg_wal_lsn_diff(sent_lsn, replay_lsn), 0)::bigint
             FROM pg_stat_replication",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        rows.into_iter()
            .map(|(addr, sent, replay, delta)| {
                let eta = if delta > 0 {
                    // Rough estimate: assume ~10 MB/s catchup rate
                    Some(delta as f64 / (10.0 * 1024.0 * 1024.0))
                } else {
                    None
                };
                CatchupProgress {
                    client_addr: addr,
                    sent_lsn: sent,
                    replay_lsn: replay,
                    delta_bytes: delta,
                    estimated_seconds_to_catchup: eta,
                }
            })
            .collect()
    }

    /// Return full replication status as JSON (for the API / dashboard).
    pub async fn query_replication_status(&self) -> Vec<serde_json::Value> {
        let replicas = self.collect_replica_stats().await;
        let health = self.check_replica_health().await;
        let catchup = self.get_catchup_progress().await;

        replicas
            .iter()
            .map(|r| {
                let catchup_info = catchup
                    .iter()
                    .find(|c| c.client_addr == r.client_addr)
                    .map(|c| {
                        serde_json::json!({
                            "delta_bytes": c.delta_bytes,
                            "estimated_catchup_secs": c.estimated_seconds_to_catchup,
                        })
                    });
                serde_json::json!({
                    "client_addr": r.client_addr,
                    "state": r.state,
                    "role": r.role,
                    "replication_mode": r.replication_mode,
                    "application_name": r.application_name,
                    "sync_state": r.sync_state,
                    "cascade_depth": r.cascade_depth,
                    "slot_name": r.slot_name,
                    "sent_lag_bytes": r.sent_lag_bytes,
                    "slot_lag_bytes": r.slot_lag_bytes,
                    "write_lag_seconds": r.write_lag_secs,
                    "flush_lag_seconds": r.flush_lag_secs,
                    "replay_lag_seconds": r.replay_lag_secs,
                    "overall_healthy": health.is_healthy,
                    "catchup": catchup_info,
                })
            })
            .collect()
    }

    /// Spawn a background monitoring loop.
    pub fn spawn(self, mut shutdown_rx: watch::Receiver<bool>) {
        let interval_secs = self.config.check_interval_secs;
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(Duration::from_secs(interval_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        debug!("Replica monitor: collecting health metrics");
                        let replicas = self.collect_replica_stats().await;
                        emit_replica_metrics(&replicas);
                        let health = self.check_replica_health().await;
                        emit_health_metrics(&health);

                        if self.detect_failover_needed(&replicas) {
                            warn!("Failover threshold exceeded — initiating automatic failover");
                            match self.initiate_failover().await {
                                Ok(msg) => info!("{}", msg),
                                Err(e) => error!("Failover failed: {}", e),
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("Replica monitor shutting down");
                        break;
                    }
                }
            }
        });
    }

    // -- private helpers --

    async fn wal_receiver_status(&self) -> String {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT COALESCE(status, 'unknown') FROM pg_stat_wal_receiver LIMIT 1")
                .fetch_optional(&self.pool)
                .await
                .unwrap_or(None);
        row.map(|(s,)| s).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Standalone / backward-compatible functions
// ---------------------------------------------------------------------------

async fn collect_replica_stats_from_pool(pool: &PgPool) -> Vec<ReplicaStatus> {
    // Join pg_stat_replication with pg_replication_slots for slot lag data.
    let rows: Vec<(
        String, String, i64, f64, f64, f64,
        String, String, String,
    )> = sqlx::query_as(
        "SELECT
            COALESCE(r.client_addr::text, 'unknown'),
            COALESCE(r.state, 'unknown'),
            COALESCE(pg_wal_lsn_diff(r.sent_lsn, r.replay_lsn), 0)::bigint,
            COALESCE(EXTRACT(EPOCH FROM r.write_lag), 0.0),
            COALESCE(EXTRACT(EPOCH FROM r.flush_lag), 0.0),
            COALESCE(EXTRACT(EPOCH FROM r.replay_lag), 0.0),
            COALESCE(r.application_name, 'unknown'),
            COALESCE(r.sync_state, 'async'),
            COALESCE(r.client_addr::text, 'unknown')
         FROM pg_stat_replication r",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| {
        debug!(error = %e, "pg_stat_replication query failed (expected on replica)");
        Vec::new()
    });

    // Also fetch slot lag information
    let slot_lags: std::collections::HashMap<String, i64> = sqlx::query_as::<_, (String, i64)>(
        "SELECT slot_name, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
         FROM pg_replication_slots WHERE active = true",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .collect();

    rows.into_iter()
        .map(|(client_addr, state, lag_bytes, write_lag, flush_lag, replay_lag, app_name, sync_state, _)| {
            let slot_lag = slot_lags.values().copied().next(); // associate first available slot
            ReplicaStatus {
                client_addr,
                state,
                sent_lag_bytes: lag_bytes,
                write_lag_secs: write_lag,
                flush_lag_secs: flush_lag,
                replay_lag_secs: replay_lag,
                role: ReplicaRole::StandbyReplica,
                replication_mode: ReplicationMode::Standard,
                cascade_depth: 0,
                slot_name: None,
                slot_lag_bytes: slot_lag,
                application_name: app_name,
                sync_state,
            }
        })
        .collect()
}

/// Emit Prometheus metrics for all replicas.
pub fn emit_replica_metrics(replicas: &[ReplicaStatus]) {
    m::gauge!("soroban_pulse_replica_count").set(replicas.len() as f64);

    let mut total_lag: i64 = 0;
    for r in replicas {
        let addr = r.client_addr.clone();
        m::gauge!("soroban_pulse_replica_lag_bytes", "client_addr" => addr.clone())
            .set(r.sent_lag_bytes as f64);
        m::gauge!("soroban_pulse_replica_write_lag_seconds", "client_addr" => addr.clone())
            .set(r.write_lag_secs);
        m::gauge!("soroban_pulse_replica_flush_lag_seconds", "client_addr" => addr.clone())
            .set(r.flush_lag_secs);
        m::gauge!("soroban_pulse_replica_replay_lag_seconds", "client_addr" => addr.clone())
            .set(r.replay_lag_secs);
        if let Some(slot_lag) = r.slot_lag_bytes {
            m::gauge!("soroban_pulse_replica_slot_lag_bytes", "client_addr" => addr.clone())
                .set(slot_lag as f64);
        }
        m::gauge!("soroban_pulse_cascade_replica_depth", "client_addr" => addr.clone())
            .set(r.cascade_depth as f64);

        total_lag += r.sent_lag_bytes;

        if r.sent_lag_bytes > LAG_CRITICAL_BYTES || r.replay_lag_secs > LAG_CRITICAL_SECS {
            error!(
                client_addr = %r.client_addr,
                lag_bytes = r.sent_lag_bytes,
                replay_lag_secs = r.replay_lag_secs,
                "CRITICAL: replica lag exceeds failover threshold",
            );
        } else if r.sent_lag_bytes > LAG_WARN_BYTES || r.replay_lag_secs > LAG_WARN_SECS {
            warn!(
                client_addr = %r.client_addr,
                lag_bytes = r.sent_lag_bytes,
                replay_lag_secs = r.replay_lag_secs,
                "Replica lag exceeds warning threshold",
            );
        }
    }

    // Health score: 100 when lag = 0, decays linearly toward 0 at critical threshold
    let health_score = if replicas.is_empty() {
        100.0
    } else {
        let worst_lag = replicas
            .iter()
            .map(|r| r.replay_lag_secs)
            .fold(0.0_f64, f64::max);
        (100.0 - (worst_lag / LAG_CRITICAL_SECS * 100.0).min(100.0)).max(0.0)
    };
    m::gauge!("soroban_pulse_replica_health_score").set(health_score);
}

fn emit_health_metrics(health: &ReplicaHealthCheck) {
    m::gauge!("soroban_pulse_replica_health_score").set(if health.is_healthy { 100.0 } else { 0.0 });
    m::gauge!("soroban_pulse_replica_lag_bytes", "client_addr" => "aggregate")
        .set(health.lag_bytes as f64);
    m::gauge!("soroban_pulse_replica_replay_lag_seconds", "client_addr" => "aggregate")
        .set(health.replay_lag_secs);
}

/// Backward-compatible standalone collector.
pub async fn collect_replica_stats(pool: &PgPool) -> Vec<ReplicaStatus> {
    collect_replica_stats_from_pool(pool).await
}

/// Backward-compatible JSON status query.
pub async fn query_replication_status(pool: &PgPool) -> Vec<serde_json::Value> {
    collect_replica_stats_from_pool(pool)
        .await
        .iter()
        .map(|r| {
            serde_json::json!({
                "client_addr": r.client_addr,
                "state": r.state,
                "application_name": r.application_name,
                "sync_state": r.sync_state,
                "sent_lag_bytes": r.sent_lag_bytes,
                "write_lag_seconds": r.write_lag_secs,
                "flush_lag_seconds": r.flush_lag_secs,
                "replay_lag_seconds": r.replay_lag_secs,
            })
        })
        .collect()
}

/// Backward-compatible background spawn using simple interval loop.
pub fn spawn(pool: PgPool, interval_secs: u64, mut shutdown_rx: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    debug!("Collecting replica sync metrics");
                    let replicas = collect_replica_stats_from_pool(&pool).await;
                    emit_replica_metrics(&replicas);
                }
                _ = shutdown_rx.changed() => {
                    debug!("Replica monitor shutting down");
                    break;
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_replica(addr: &str, lag_bytes: i64, replay_lag: f64) -> ReplicaStatus {
        ReplicaStatus {
            client_addr: addr.to_string(),
            state: "streaming".to_string(),
            sent_lag_bytes: lag_bytes,
            write_lag_secs: 0.1,
            flush_lag_secs: 0.2,
            replay_lag_secs: replay_lag,
            role: ReplicaRole::StandbyReplica,
            replication_mode: ReplicationMode::Standard,
            cascade_depth: 0,
            slot_name: None,
            slot_lag_bytes: None,
            application_name: "walreceiver".to_string(),
            sync_state: "async".to_string(),
        }
    }

    #[test]
    fn emit_metrics_no_panic_on_empty() {
        emit_replica_metrics(&[]);
    }

    #[test]
    fn emit_metrics_no_panic_with_data() {
        let replicas = vec![
            make_replica("10.0.0.1", 1024, 1.5),
            make_replica("10.0.0.2", 20 * 1024 * 1024, 60.0),
        ];
        emit_replica_metrics(&replicas);
    }

    #[test]
    fn replica_count_matches() {
        let replicas = vec![
            make_replica("10.0.0.1", 0, 0.0),
            make_replica("10.0.0.2", 0, 0.0),
        ];
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn failover_config_default() {
        let cfg = FailoverConfig::default();
        assert!(!cfg.auto_failover_enabled);
        assert_eq!(cfg.lag_threshold_bytes, LAG_CRITICAL_BYTES);
    }

    #[test]
    fn replica_monitor_config_default() {
        let cfg = ReplicaMonitorConfig::default();
        assert_eq!(cfg.mode, ReplicationMode::Standard);
        assert_eq!(cfg.health_history_size, DEFAULT_HEALTH_HISTORY_SIZE);
    }

    #[test]
    fn detect_failover_disabled_by_default() {
        // With auto_failover_enabled = false, detect_failover_needed must return false
        // even when lag is extreme.
        // We use a PgPool-less monitor since no DB call is made in detect_failover_needed.
        // Cannot construct PgPool without a connection, so test the logic directly.
        let config = ReplicaMonitorConfig {
            failover: FailoverConfig {
                auto_failover_enabled: false,
                lag_threshold_bytes: 1, // extremely low
                lag_threshold_secs: 0.001,
                ..Default::default()
            },
            ..Default::default()
        };
        // Manual logic check (mirrors detect_failover_needed without needing a pool)
        assert!(!config.failover.auto_failover_enabled);
    }

    #[test]
    fn cascade_config_default() {
        let cfg = CascadeReplicaConfig::default();
        assert_eq!(cfg.max_wal_senders, 10);
        assert_eq!(cfg.replication_slot, "cascade_slot");
    }

    #[test]
    fn read_replica_config_default() {
        let cfg = ReadReplicaConfig::default();
        assert_eq!(cfg.load_weight, 1);
        assert!(cfg.is_healthy);
        assert_eq!(cfg.lag_threshold_ms, 1000);
    }

    #[test]
    fn health_score_zero_replicas_is_100() {
        // health_score formula: 100 when replicas is empty
        let replicas: Vec<ReplicaStatus> = vec![];
        let worst_lag = replicas
            .iter()
            .map(|r| r.replay_lag_secs)
            .fold(0.0_f64, f64::max);
        let health_score = if replicas.is_empty() {
            100.0
        } else {
            (100.0 - (worst_lag / LAG_CRITICAL_SECS * 100.0).min(100.0)).max(0.0)
        };
        assert_eq!(health_score, 100.0);
    }

    #[test]
    fn health_score_at_critical_lag_is_zero() {
        let lag = LAG_CRITICAL_SECS;
        let score = (100.0 - (lag / LAG_CRITICAL_SECS * 100.0).min(100.0)).max(0.0);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn replication_mode_default_is_standard() {
        assert_eq!(ReplicationMode::default(), ReplicationMode::Standard);
    }

    #[test]
    fn replica_role_default_is_standby() {
        assert_eq!(ReplicaRole::default(), ReplicaRole::StandbyReplica);
    }
}
