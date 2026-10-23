//! Incident correlation engine.
//!
//! Correlates anomaly alerts, RPC errors, and indexer lag signals into a single
//! incident when they occur within a configurable correlation window (default
//! 5 minutes). Signals are deduplicated so that multiple correlated events in
//! the same window produce exactly one incident.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Default correlation window: 5 minutes.
pub const DEFAULT_CORRELATION_WINDOW_SECS: u64 = 300;

/// Environment variable used to override the correlation window.
pub const CORRELATION_WINDOW_ENV: &str = "CORRELATION_WINDOW_SECS";

/// The kind of signal that can be correlated into an incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    /// An anomaly detection alert.
    Anomaly,
    /// An RPC error observed by the backend.
    RpcError,
    /// Indexer lag exceeding its threshold.
    IndexerLag,
}

impl SignalKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            SignalKind::Anomaly => "anomaly",
            SignalKind::RpcError => "rpc_error",
            SignalKind::IndexerLag => "indexer_lag",
        }
    }
}

/// Severity assigned to an incident based on the aggregated signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

impl Severity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Severity::Low => "low",
            Severity::Medium => "medium",
            Severity::High => "high",
            Severity::Critical => "critical",
        }
    }
}

/// A single signal that may be correlated into an incident.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub kind: SignalKind,
    /// Short human-readable summary of the signal.
    pub summary: String,
    /// Optional structured detail (metric name, error code, lag seconds, ...).
    pub detail: Option<String>,
    /// Unix timestamp (seconds) at which the signal was observed.
    pub observed_at: u64,
}

impl Signal {
    pub fn new(kind: SignalKind, summary: impl Into<String>) -> Self {
        Self {
            kind,
            summary: summary.into(),
            detail: None,
            observed_at: now_unix_secs(),
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn at(mut self, observed_at: u64) -> Self {
        self.observed_at = observed_at;
        self
    }
}

/// An incident produced by correlating one or more signals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incident {
    pub id: i64,
    pub severity: Severity,
    pub status: IncidentStatus,
    pub summary: String,
    /// Unix timestamp (seconds) of the first correlated signal.
    pub started_at: u64,
    /// Unix timestamp (seconds) of the most recent correlated signal.
    pub last_seen_at: u64,
    /// Number of signals correlated into this incident.
    pub signal_count: i64,
    /// Aggregated timeline of signals for root-cause analysis.
    pub timeline: Vec<Signal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentStatus {
    Open,
    Resolved,
}

impl IncidentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            IncidentStatus::Open => "open",
            IncidentStatus::Resolved => "resolved",
        }
    }
}

/// In-memory grouping of signals that have not yet been persisted.
#[derive(Debug, Clone)]
struct PendingIncident {
    signals: Vec<Signal>,
    first_seen: u64,
    last_seen: u64,
}

impl PendingIncident {
    fn new(signal: Signal) -> Self {
        let ts = signal.observed_at;
        Self {
            signals: vec![signal],
            first_seen: ts,
            last_seen: ts,
        }
    }

    fn add(&mut self, signal: Signal) {
        self.last_seen = self.last_seen.max(signal.observed_at);
        self.first_seen = self.first_seen.min(signal.observed_at);
        self.signals.push(signal);
    }

    fn within_window(&self, ts: u64, window_secs: u64) -> bool {
        // A signal belongs to this incident if it falls within the window
        // relative to the most recent signal seen.
        let delta = ts.abs_diff(self.last_seen);
        delta <= window_secs
    }

    fn severity(&self) -> Severity {
        let mut has_anomaly = false;
        let mut has_rpc = false;
        let mut has_lag = false;
        for s in &self.signals {
            match s.kind {
                SignalKind::Anomaly => has_anomaly = true,
                SignalKind::RpcError => has_rpc = true,
                SignalKind::IndexerLag => has_lag = true,
            }
        }
        let distinct = [has_anomaly, has_rpc, has_lag]
            .iter()
            .filter(|b| **b)
            .count();
        match distinct {
            0 => Severity::Low,
            1 => Severity::Medium,
            2 => Severity::High,
            _ => Severity::Critical,
        }
    }

    fn summary(&self) -> String {
        let mut kinds: Vec<&str> = self.signals.iter().map(|s| s.kind.as_str()).collect();
        kinds.sort_unstable();
        kinds.dedup();
        format!(
            "Correlated incident ({} signals: {})",
            self.signals.len(),
            kinds.join(", ")
        )
    }
}

/// Prometheus-style metrics emitted by the correlation engine.
#[derive(Debug, Default)]
pub struct CorrelationMetrics {
    pub incidents_created: u64,
    pub incidents_resolved: u64,
    pub open_incidents: i64,
    pub correlation_duration_secs: Vec<f64>,
    pub detector_errors: u64,
}

impl CorrelationMetrics {
    fn record_created(&mut self, duration_secs: f64) {
        self.incidents_created += 1;
        self.open_incidents += 1;
        self.correlation_duration_secs.push(duration_secs);
    }

    fn record_resolved(&mut self) {
        self.incidents_resolved += 1;
        if self.open_incidents > 0 {
            self.open_incidents -= 1;
        }
    }

    fn record_error(&mut self) {
        self.detector_errors += 1;
    }
}

/// The correlation engine.
///
/// Signals are buffered in memory and grouped by the correlation window. When
/// a group is flushed (either because the window elapsed or because an explicit
/// flush is requested), it is persisted as a single incident.
pub struct IncidentCorrelationEngine {
    window: Duration,
    pool: Option<PgPool>,
    pending: Mutex<Vec<PendingIncident>>,
    metrics: Mutex<CorrelationMetrics>,
}

impl IncidentCorrelationEngine {
    /// Build an engine from the environment, falling back to the default window.
    pub fn from_env(pool: Option<PgPool>) -> Arc<Self> {
        let window_secs = std::env::var(CORRELATION_WINDOW_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_CORRELATION_WINDOW_SECS);
        Arc::new(Self::new(Duration::from_secs(window_secs), pool))
    }

    pub fn new(window: Duration, pool: Option<PgPool>) -> Self {
        Self {
            window,
            pool,
            pending: Mutex::new(Vec::new()),
            metrics: Mutex::new(CorrelationMetrics::default()),
        }
    }

    pub fn window_secs(&self) -> u64 {
        self.window.as_secs()
    }

    /// Ingest a signal. Returns the incident id if a group was flushed and
    /// persisted as a result of this signal.
    pub async fn ingest(&self, signal: Signal) -> Option<i64> {
        let window_secs = self.window.as_secs();
        let mut pending = self.pending.lock().await;

        // Try to find an existing group within the window.
        let mut matched: Option<usize> = None;
        for (idx, group) in pending.iter().enumerate() {
            if group.within_window(signal.observed_at, window_secs) {
                matched = Some(idx);
                break;
            }
        }

        match matched {
            Some(idx) => {
                pending[idx].add(signal);
                debug!("signal correlated into existing pending incident group");
                None
            }
            None => {
                pending.push(PendingIncident::new(signal));
                debug!("signal started a new pending incident group");
                None
            }
        }
    }

    /// Flush all pending groups whose window has elapsed and persist them.
    /// Returns the ids of incidents created.
    pub async fn flush_expired(&self) -> Vec<i64> {
        let now = now_unix_secs();
        let window_secs = self.window.as_secs();
        let mut pending = self.pending.lock().await;

        let mut ready = Vec::new();
        let mut remaining = Vec::new();
        for group in pending.drain(..) {
            if now.saturating_sub(group.last_seen) >= window_secs {
                ready.push(group);
            } else {
                remaining.push(group);
            }
        }
        *pending = remaining;
        drop(pending);

        let mut created = Vec::new();
        for group in ready {
            match self.persist(group).await {
                Ok(id) => created.push(id),
                Err(e) => {
                    error!(error = %e, "failed to persist incident");
                    self.metrics.lock().await.record_error();
                }
            }
        }
        created
    }

    /// Force-flush all pending groups regardless of window. Useful for tests
    /// and graceful shutdown.
    pub async fn flush_all(&self) -> Vec<i64> {
        let mut pending = self.pending.lock().await;
        let groups: Vec<PendingIncident> = pending.drain(..).collect();
        drop(pending);

        let mut created = Vec::new();
        for group in groups {
            match self.persist(group).await {
                Ok(id) => created.push(id),
                Err(e) => {
                    error!(error = %e, "failed to persist incident");
                    self.metrics.lock().await.record_error();
                }
            }
        }
        created
    }

    async fn persist(&self, group: PendingIncident) -> Result<i64, sqlx::Error> {
        let severity = group.severity();
        let summary = group.summary();
        let signal_count = group.signals.len() as i64;
        let timeline_json = serde_json::to_value(&group.signals).unwrap_or(serde_json::Value::Null);
        let duration_secs = group.last_seen.saturating_sub(group.first_seen) as f64;

        let id = if let Some(pool) = &self.pool {
            let row: (i64,) = sqlx::query_as(
                r#"
                INSERT INTO incidents
                    (severity, status, summary, started_at, last_seen_at, signal_count, timeline)
                VALUES ($1, $2, $3, to_timestamp($4), to_timestamp($5), $6, $7)
                RETURNING id
                "#,
            )
            .bind(severity.as_str())
            .bind(IncidentStatus::Open.as_str())
            .bind(&summary)
            .bind(group.first_seen as f64)
            .bind(group.last_seen as f64)
            .bind(signal_count)
            .bind(timeline_json)
            .fetch_one(pool)
            .await?;
            row.0
        } else {
            // No database configured: use a synthetic id for in-memory mode.
            now_unix_secs() as i64
        };

        self.metrics.lock().await.record_created(duration_secs);
        info!(
            incident_id = id,
            severity = severity.as_str(),
            signal_count,
            "incident created"
        );
        Ok(id)
    }

    /// Mark an incident as resolved.
    pub async fn resolve(&self, incident_id: i64) -> Result<(), sqlx::Error> {
        if let Some(pool) = &self.pool {
            sqlx::query(
                r#"
                UPDATE incidents
                SET status = $1, resolved_at = NOW()
                WHERE id = $2
                "#,
            )
            .bind(IncidentStatus::Resolved.as_str())
            .bind(incident_id)
            .execute(pool)
            .await?;
        }
        self.metrics.lock().await.record_resolved();
        info!(incident_id, "incident resolved");
        Ok(())
    }

    /// List incidents, most recent first.
    pub async fn list(&self, limit: i64) -> Result<Vec<Incident>, sqlx::Error> {
        let pool = match &self.pool {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };

        let rows: Vec<IncidentRow> = sqlx::query_as(
            r#"
            SELECT id, severity, status, summary,
                   EXTRACT(EPOCH FROM started_at)::bigint AS started_at,
                   EXTRACT(EPOCH FROM last_seen_at)::bigint AS last_seen_at,
                   signal_count, timeline
            FROM incidents
            ORDER BY last_seen_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(pool)
        .await?;

        Ok(rows.into_iter().map(Incident::from).collect())
    }

    /// Snapshot of the current metrics.
    pub async fn metrics_snapshot(&self) -> CorrelationMetrics {
        let m = self.metrics.lock().await;
        CorrelationMetrics {
            incidents_created: m.incidents_created,
            incidents_resolved: m.incidents_resolved,
            open_incidents: m.open_incidents,
            correlation_duration_secs: m.correlation_duration_secs.clone(),
            detector_errors: m.detector_errors,
        }
    }

    /// Record a detector error (e.g. a failed anomaly evaluation).
    pub async fn record_detector_error(&self) {
        self.metrics.lock().await.record_error();
    }
}

#[derive(sqlx::FromRow)]
struct IncidentRow {
    id: i64,
    severity: String,
    status: String,
    summary: String,
    started_at: i64,
    last_seen_at: i64,
    signal_count: i64,
    timeline: serde_json::Value,
}

impl From<IncidentRow> for Incident {
    fn from(row: IncidentRow) -> Self {
        let severity = match row.severity.as_str() {
            "critical" => Severity::Critical,
            "high" => Severity::High,
            "medium" => Severity::Medium,
            _ => Severity::Low,
        };
        let status = match row.status.as_str() {
            "resolved" => IncidentStatus::Resolved,
            _ => IncidentStatus::Open,
        };
        let timeline: Vec<Signal> = serde_json::from_value(row.timeline).unwrap_or_default();
        Incident {
            id: row.id,
            severity,
            status,
            summary: row.summary,
            started_at: row.started_at as u64,
            last_seen_at: row.last_seen_at as u64,
            signal_count: row.signal_count,
            timeline,
        }
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Convenience helper: correlate a batch of signals and return the number of
/// incidents that would be produced. Used by tests and dry-run tooling.
pub fn correlate_batch(window_secs: u64, signals: &[Signal]) -> usize {
    let mut groups: Vec<PendingIncident> = Vec::new();
    for signal in signals {
        let mut matched = false;
        for group in groups.iter_mut() {
            if group.within_window(signal.observed_at, window_secs) {
                group.add(signal.clone());
                matched = true;
                break;
            }
        }
        if !matched {
            groups.push(PendingIncident::new(signal.clone()));
        }
    }
    groups.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anomaly(ts: u64) -> Signal {
        Signal::new(SignalKind::Anomaly, "tx volume spike").at(ts)
    }

    fn rpc(ts: u64) -> Signal {
        Signal::new(SignalKind::RpcError, "rpc error").at(ts)
    }

    fn lag(ts: u64) -> Signal {
        Signal::new(SignalKind::IndexerLag, "indexer lag").at(ts)
    }

    #[test]
    fn three_correlated_events_produce_one_incident() {
        let signals = vec![anomaly(1000), rpc(1010), lag(1020)];
        assert_eq!(correlate_batch(300, &signals), 1);
    }

    #[test]
    fn events_outside_window_produce_separate_incidents() {
        let signals = vec![anomaly(1000), rpc(2000), lag(3000)];
        assert_eq!(correlate_batch(300, &signals), 3);
    }

    #[test]
    fn events_at_window_boundary_correlate() {
        let signals = vec![anomaly(1000), rpc(1300)];
        assert_eq!(correlate_batch(300, &signals), 1);
    }

    #[test]
    fn events_just_outside_window_do_not_correlate() {
        let signals = vec![anomaly(1000), rpc(1301)];
        assert_eq!(correlate_batch(300, &signals), 2);
    }

    #[test]
    fn severity_scales_with_distinct_signal_kinds() {
        let mut group = PendingIncident::new(anomaly(1000));
        assert_eq!(group.severity(), Severity::Medium);
        group.add(rpc(1010));
        assert_eq!(group.severity(), Severity::High);
        group.add(lag(1020));
        assert_eq!(group.severity(), Severity::Critical);
    }

    #[test]
    fn summary_lists_distinct_kinds() {
        let mut group = PendingIncident::new(anomaly(1000));
        group.add(rpc(1010));
        group.add(lag(1020));
        let summary = group.summary();
        assert!(summary.contains("anomaly"));
        assert!(summary.contains("rpc_error"));
        assert!(summary.contains("indexer_lag"));
    }

    #[tokio::test]
    async fn engine_deduplicates_within_window() {
        let engine = IncidentCorrelationEngine::new(Duration::from_secs(300), None);
        engine.ingest(anomaly(1000)).await;
        engine.ingest(rpc(1010)).await;
        engine.ingest(lag(1020)).await;

        let pending = engine.pending.lock().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].signals.len(), 3);
        drop(pending);

        let created = engine.flush_all().await;
        assert_eq!(created.len(), 1);

        let metrics = engine.metrics_snapshot().await;
        assert_eq!(metrics.incidents_created, 1);
        assert_eq!(metrics.open_incidents, 1);
    }

    #[tokio::test]
    async fn engine_separates_signals_outside_window() {
        let engine = IncidentCorrelationEngine::new(Duration::from_secs(300), None);
        engine.ingest(anomaly(1000)).await;
        engine.ingest(rpc(2000)).await;
        engine.ingest(lag(3000)).await;

        let pending = engine.pending.lock().await;
        assert_eq!(pending.len(), 3);
        drop(pending);

        let created = engine.flush_all().await;
        assert_eq!(created.len(), 3);
    }

    #[tokio::test]
    async fn flush_expired_only_flushes_old_groups() {
        let engine = IncidentCorrelationEngine::new(Duration::from_secs(1), None);
        let now = now_unix_secs();
        engine.ingest(anomaly(now.saturating_sub(100))).await;
        engine.ingest(rpc(now)).await;

        let created = engine.flush_expired().await;
        assert_eq!(created.len(), 1);

        let pending = engine.pending.lock().await;
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn resolve_updates_metrics() {
        let engine = IncidentCorrelationEngine::new(Duration::from_secs(300), None);
        engine.ingest(anomaly(1000)).await;
        let created = engine.flush_all().await;
        assert_eq!(created.len(), 1);

        engine.resolve(created[0]).await.unwrap();
        let metrics = engine.metrics_snapshot().await;
        assert_eq!(metrics.incidents_resolved, 1);
        assert_eq!(metrics.open_incidents, 0);
    }

    #[tokio::test]
    async fn detector_errors_are_counted() {
        let engine = IncidentCorrelationEngine::new(Duration::from_secs(300), None);
        engine.record_detector_error().await;
        let metrics = engine.metrics_snapshot().await;
        assert_eq!(metrics.detector_errors, 1);
    }

    #[test]
    fn window_from_env_defaults_when_unset() {
        // Ensure the default is used when the env var is absent.
        std::env::remove_var(CORRELATION_WINDOW_ENV);
        let engine = IncidentCorrelationEngine::from_env(None);
        assert_eq!(engine.window_secs(), DEFAULT_CORRELATION_WINDOW_SECS);
    }
}