//! Derived metrics DSL (#1262).
//!
//! This module implements a small DSL for computing custom metrics from
//! indexed events and base metrics. Metric definitions are parsed from a
//! YAML/TOML-like configuration, evaluated on a configured schedule by a
//! background job, published through the Prometheus `/metrics` endpoint, and
//! persisted as snapshots in the `derived_metric_snapshots` table.
//!
//! The DSL supports the following aggregation functions:
//! - `rate`: per-second rate of a counter over a window
//! - `delta`: absolute change of a gauge over a window
//! - `ratio`: ratio of two series
//! - `moving_average`: average of a series over a window
//! - `percentile`: configurable percentile of a series over a window
//!
//! See `.kiro/specs/derived-metrics-framework/requirements.md` for the full
//! specification.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::metrics;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur while parsing or evaluating derived metrics.
#[derive(Debug)]
pub enum DerivedMetricError {
    /// The configuration could not be parsed.
    Parse(String),
    /// The metric definition is semantically invalid.
    Invalid(String),
    /// Evaluation of the metric failed.
    Evaluation(String),
    /// Persistence of a snapshot failed.
    Storage(String),
}

impl fmt::Display for DerivedMetricError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DerivedMetricError::Parse(msg) => write!(f, "parse error: {msg}"),
            DerivedMetricError::Invalid(msg) => write!(f, "invalid metric: {msg}"),
            DerivedMetricError::Evaluation(msg) => write!(f, "evaluation error: {msg}"),
            DerivedMetricError::Storage(msg) => write!(f, "storage error: {msg}"),
        }
    }
}

impl std::error::Error for DerivedMetricError {}

impl From<sqlx::Error> for DerivedMetricError {
    fn from(err: sqlx::Error) -> Self {
        DerivedMetricError::Storage(err.to_string())
    }
}

// ---------------------------------------------------------------------------
// DSL types
// ---------------------------------------------------------------------------

/// The aggregation function applied by a derived metric.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    /// Per-second rate of a monotonically increasing counter over a window.
    Rate,
    /// Absolute change of a gauge over a window.
    Delta,
    /// Ratio of two series (`numerator` / `denominator`).
    Ratio,
    /// Average of a series over a window.
    MovingAverage,
    /// Configurable percentile of a series over a window.
    Percentile { percentile: f64 },
}

impl Aggregation {
    /// Parse an aggregation from its string representation.
    ///
    /// Accepts `rate`, `delta`, `ratio`, `moving_average`/`moving-average`,
    /// and `percentile` (optionally with a `percentile` parameter).
    pub fn parse(name: &str, percentile: Option<f64>) -> Result<Self, DerivedMetricError> {
        match name.trim().to_ascii_lowercase().as_str() {
            "rate" => Ok(Aggregation::Rate),
            "delta" => Ok(Aggregation::Delta),
            "ratio" => Ok(Aggregation::Ratio),
            "moving_average" | "moving-average" | "movingaverage" => {
                Ok(Aggregation::MovingAverage)
            }
            "percentile" => {
                let p = percentile.ok_or_else(|| {
                    DerivedMetricError::Invalid(
                        "percentile aggregation requires a `percentile` value".to_string(),
                    )
                })?;
                if !(0.0..=100.0).contains(&p) {
                    return Err(DerivedMetricError::Invalid(format!(
                        "percentile must be between 0 and 100, got {p}"
                    )));
                }
                Ok(Aggregation::Percentile { percentile: p })
            }
            other => Err(DerivedMetricError::Invalid(format!(
                "unknown aggregation function `{other}`"
            ))),
        }
    }

    /// Human-readable name of the aggregation.
    pub fn name(&self) -> &'static str {
        match self {
            Aggregation::Rate => "rate",
            Aggregation::Delta => "delta",
            Aggregation::Ratio => "ratio",
            Aggregation::MovingAverage => "moving_average",
            Aggregation::Percentile { .. } => "percentile",
        }
    }
}

/// A single derived metric definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DerivedMetric {
    /// Unique metric name (used as the Prometheus metric label).
    pub name: String,
    /// Optional human-readable description.
    #[serde(default)]
    pub description: Option<String>,
    /// The aggregation function to apply.
    pub aggregation: Aggregation,
    /// The source series (event type or base metric name).
    pub source: String,
    /// Optional secondary source, required for `ratio`.
    #[serde(default)]
    pub denominator: Option<String>,
    /// Evaluation window in seconds.
    #[serde(default = "default_window_seconds")]
    pub window_seconds: u64,
    /// Evaluation interval in seconds.
    #[serde(default = "default_interval_seconds")]
    pub interval_seconds: u64,
    /// Optional label filters applied to the source series.
    #[serde(default)]
    pub filters: HashMap<String, String>,
}

fn default_window_seconds() -> u64 {
    3600
}

fn default_interval_seconds() -> u64 {
    60
}

impl DerivedMetric {
    /// Validate the metric definition.
    pub fn validate(&self) -> Result<(), DerivedMetricError> {
        if self.name.trim().is_empty() {
            return Err(DerivedMetricError::Invalid(
                "metric name must not be empty".to_string(),
            ));
        }
        if !self
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
        {
            return Err(DerivedMetricError::Invalid(format!(
                "metric name `{}` contains invalid characters",
                self.name
            )));
        }
        if self.source.trim().is_empty() {
            return Err(DerivedMetricError::Invalid(format!(
                "metric `{}` has an empty source",
                self.name
            )));
        }
        if self.aggregation == Aggregation::Ratio && self.denominator.is_none() {
            return Err(DerivedMetricError::Invalid(format!(
                "ratio metric `{}` requires a denominator",
                self.name
            )));
        }
        if self.window_seconds == 0 {
            return Err(DerivedMetricError::Invalid(format!(
                "metric `{}` has a zero window",
                self.name
            )));
        }
        if self.interval_seconds == 0 {
            return Err(DerivedMetricError::Invalid(format!(
                "metric `{}` has a zero interval",
                self.name
            )));
        }
        Ok(())
    }

    /// The evaluation interval as a `Duration`.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_seconds)
    }

    /// The evaluation window as a `Duration`.
    pub fn window(&self) -> Duration {
        Duration::from_secs(self.window_seconds)
    }
}

// ---------------------------------------------------------------------------
// Configuration parsing
// ---------------------------------------------------------------------------

/// The top-level derived metrics configuration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DerivedMetricsConfig {
    /// The list of metric definitions.
    #[serde(default)]
    pub metrics: Vec<DerivedMetric>,
}

impl DerivedMetricsConfig {
    /// Parse a configuration from a YAML string.
    ///
    /// The expected shape is:
    ///
    /// ```yaml
    /// metrics:
    ///   - name: transfer_volume_1h
    ///     aggregation: rate
    ///     source: transfer
    ///     window_seconds: 3600
    ///     interval_seconds: 60
    /// ```
    pub fn from_yaml(input: &str) -> Result<Self, DerivedMetricError> {
        let config: DerivedMetricsConfig = serde_yaml::from_str(input)
            .map_err(|e| DerivedMetricError::Parse(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Parse a configuration from a TOML string.
    ///
    /// The expected shape is:
    ///
    /// ```toml
    /// [[metrics]]
    /// name = "transfer_volume_1h"
    /// aggregation = "rate"
    /// source = "transfer"
    /// window_seconds = 3600
    /// interval_seconds = 60
    /// ```
    pub fn from_toml(input: &str) -> Result<Self, DerivedMetricError> {
        let config: DerivedMetricsConfig = toml::from_str(input)
            .map_err(|e| DerivedMetricError::Parse(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Validate every metric in the configuration.
    pub fn validate(&self) -> Result<(), DerivedMetricError> {
        let mut seen = std::collections::HashSet::new();
        for metric in &self.metrics {
            metric.validate()?;
            if !seen.insert(metric.name.clone()) {
                return Err(DerivedMetricError::Invalid(format!(
                    "duplicate metric name `{}`",
                    metric.name
                )));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// A single data point in a source series.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DataPoint {
    /// Timestamp in seconds since the Unix epoch.
    pub timestamp: i64,
    /// The value at that timestamp.
    pub value: f64,
}

/// A source of data for evaluating derived metrics.
///
/// Implementations may read from the indexed events table, from base metrics,
/// or from any other backing store.
#[async_trait::async_trait]
pub trait MetricSource: Send + Sync {
    /// Fetch the series identified by `source` within the given window.
    ///
    /// `filters` contains optional label filters that must be applied.
    async fn fetch(
        &self,
        source: &str,
        filters: &HashMap<String, String>,
        window: Duration,
    ) -> Result<Vec<DataPoint>, DerivedMetricError>;
}

/// A `MetricSource` backed by the `derived_metric_events` table.
///
/// Each row represents an indexed event with a `source`, a `value`, and a
/// `labels` JSON object used for filtering.
pub struct PostgresMetricSource {
    pool: PgPool,
}

impl PostgresMetricSource {
    /// Create a new source backed by the given pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl MetricSource for PostgresMetricSource {
    async fn fetch(
        &self,
        source: &str,
        filters: &HashMap<String, String>,
        window: Duration,
    ) -> Result<Vec<DataPoint>, DerivedMetricError> {
        let since = now_epoch_seconds() - window.as_secs() as i64;
        let rows: Vec<(i64, f64)> = sqlx::query_as(
            r#"
            SELECT EXTRACT(EPOCH FROM occurred_at)::bigint AS ts, value
            FROM derived_metric_events
            WHERE source = $1
              AND occurred_at >= to_timestamp($2)
              AND ($3::jsonb IS NULL OR labels @> $3::jsonb)
            ORDER BY occurred_at ASC
            "#,
        )
        .bind(source)
        .bind(since)
        .bind(serde_json::to_value(filters).ok())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DerivedMetricError::Evaluation(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|(timestamp, value)| DataPoint { timestamp, value })
            .collect())
    }
}

/// Compute the value of a derived metric from its source series.
///
/// This is the pure evaluator: it takes the already-fetched series and returns
/// the aggregated value. It is exposed separately so it can be unit tested
/// without a database.
pub fn evaluate(
    metric: &DerivedMetric,
    series: &[DataPoint],
    denominator_series: &[DataPoint],
) -> Result<f64, DerivedMetricError> {
    let (Some(first), Some(last)) = (series.first(), series.last()) else {
        return Err(DerivedMetricError::Evaluation(format!(
            "no data points for metric `{}`",
            metric.name
        )));
    };

    let value = match &metric.aggregation {
        Aggregation::Rate => {
            let elapsed = (last.timestamp - first.timestamp) as f64;
            if elapsed <= 0.0 {
                return Err(DerivedMetricError::Evaluation(format!(
                    "non-positive elapsed time for rate metric `{}`",
                    metric.name
                )));
            }
            (last.value - first.value) / elapsed
        }
        Aggregation::Delta => {
            last.value - first.value
        }
        Aggregation::Ratio => {
            if denominator_series.is_empty() {
                return Err(DerivedMetricError::Evaluation(format!(
                    "no denominator data for ratio metric `{}`",
                    metric.name
                )));
            }
            let numerator: f64 = series.iter().map(|p| p.value).sum();
            let denominator: f64 = denominator_series.iter().map(|p| p.value).sum();
            if denominator == 0.0 {
                return Err(DerivedMetricError::Evaluation(format!(
                    "zero denominator for ratio metric `{}`",
                    metric.name
                )));
            }
            numerator / denominator
        }
        Aggregation::MovingAverage => {
            let sum: f64 = series.iter().map(|p| p.value).sum();
            sum / series.len() as f64
        }
        Aggregation::Percentile { percentile } => {
            let mut values: Vec<f64> = series.iter().map(|p| p.value).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            percentile_of(&values, *percentile)
        }
    };

    if value.is_nan() || value.is_infinite() {
        return Err(DerivedMetricError::Evaluation(format!(
            "metric `{}` produced a non-finite value",
            metric.name
        )));
    }

    Ok(value)
}

/// Compute the `p`-th percentile (0..=100) of a sorted slice using linear
/// interpolation between closest ranks.
fn percentile_of(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = (p / 100.0) * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = rank - lower as f64;
        sorted[lower] * (1.0 - weight) + sorted[upper] * weight
    }
}

// ---------------------------------------------------------------------------
// Snapshot persistence
// ---------------------------------------------------------------------------

/// A persisted snapshot of a derived metric value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DerivedMetricSnapshot {
    /// The metric name.
    pub metric_name: String,
    /// The computed value.
    pub value: f64,
    /// When the snapshot was taken (seconds since the Unix epoch).
    pub recorded_at: i64,
    /// The aggregation used.
    pub aggregation: String,
    /// The source series.
    pub source: String,
}

/// Persist a snapshot to the `derived_metric_snapshots` table.
pub async fn store_snapshot(
    pool: &PgPool,
    snapshot: &DerivedMetricSnapshot,
) -> Result<(), DerivedMetricError> {
    sqlx::query(
        r#"
        INSERT INTO derived_metric_snapshots
            (metric_name, value, recorded_at, aggregation, source)
        VALUES ($1, $2, to_timestamp($3), $4, $5)
        "#,
    )
    .bind(&snapshot.metric_name)
    .bind(snapshot.value)
    .bind(snapshot.recorded_at)
    .bind(&snapshot.aggregation)
    .bind(&snapshot.source)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Evaluator / runner
// ---------------------------------------------------------------------------

/// The derived metrics evaluator.
///
/// Holds the parsed metric definitions and the source used to fetch data.
pub struct DerivedMetricsEvaluator {
    metrics: Vec<DerivedMetric>,
    source: Arc<dyn MetricSource>,
    pool: Option<PgPool>,
}

impl DerivedMetricsEvaluator {
    /// Create a new evaluator.
    pub fn new(
        config: DerivedMetricsConfig,
        source: Arc<dyn MetricSource>,
        pool: Option<PgPool>,
    ) -> Result<Self, DerivedMetricError> {
        config.validate()?;
        Ok(Self {
            metrics: config.metrics,
            source,
            pool,
        })
    }

    /// The configured metrics.
    pub fn metrics(&self) -> &[DerivedMetric] {
        &self.metrics
    }

    /// Evaluate a single metric and publish its value.
    ///
    /// Returns the computed value.
    pub async fn evaluate_metric(
        &self,
        metric: &DerivedMetric,
    ) -> Result<f64, DerivedMetricError> {
        let start = Instant::now();

        let series = self
            .source
            .fetch(&metric.source, &metric.filters, metric.window())
            .await?;

        let denominator_series = if let Some(denominator) = &metric.denominator {
            self.source
                .fetch(denominator, &metric.filters, metric.window())
                .await?
        } else {
            Vec::new()
        };

        let value = evaluate(metric, &series, &denominator_series)?;

        // Publish to Prometheus.
        metrics::record_derived_metric(&metric.name, metric.aggregation.name(), value);
        metrics::observe_derived_metric_evaluation_duration(
            metric.aggregation.name(),
            start.elapsed().as_secs_f64(),
        );

        // Persist a snapshot if a pool is configured.
        if let Some(pool) = &self.pool {
            let snapshot = DerivedMetricSnapshot {
                metric_name: metric.name.clone(),
                value,
                recorded_at: now_epoch_seconds(),
                aggregation: metric.aggregation.name().to_string(),
                source: metric.source.clone(),
            };
            if let Err(err) = store_snapshot(pool, &snapshot).await {
                metrics::inc_derived_metric_error(metric.aggregation.name());
                warn!(metric = %metric.name, error = %err, "failed to store derived metric snapshot");
            }
        }

        debug!(metric = %metric.name, value, "evaluated derived metric");
        Ok(value)
    }

    /// Evaluate every configured metric once.
    pub async fn evaluate_all(&self) -> Vec<(String, Result<f64, DerivedMetricError>)> {
        let mut results = Vec::with_capacity(self.metrics.len());
        for metric in &self.metrics {
            let result = self.evaluate_metric(metric).await;
            if result.is_err() {
                metrics::inc_derived_metric_error(metric.aggregation.name());
            }
            results.push((metric.name.clone(), result));
        }
        results
    }
}

// ---------------------------------------------------------------------------
// Background job runner
// ---------------------------------------------------------------------------

/// A handle to the running derived metrics background job.
pub struct DerivedMetricsRunner {
    handle: JoinHandle<()>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl DerivedMetricsRunner {
    /// Spawn the background job.
    ///
    /// Each metric is evaluated on its own configured interval. The runner
    /// loops until [`DerivedMetricsRunner::shutdown`] is called.
    pub fn spawn(evaluator: Arc<DerivedMetricsEvaluator>) -> Self {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let shutdown_clone = shutdown.clone();

        let handle = tokio::spawn(async move {
            info!(
                metrics = evaluator.metrics().len(),
                "starting derived metrics background job"
            );

            // Track the next scheduled run for each metric.
            let mut next_runs: Vec<Instant> = evaluator
                .metrics()
                .iter()
                .map(|m| Instant::now() + m.interval())
                .collect();

            loop {
                let now = Instant::now();
                let mut next_wakeup = now + Duration::from_secs(60);

                for (idx, metric) in evaluator.metrics().iter().enumerate() {
                    if next_runs[idx] <= now {
                        if let Err(err) = evaluator.evaluate_metric(metric).await {
                            metrics::inc_derived_metric_error(metric.aggregation.name());
                            error!(
                                metric = %metric.name,
                                error = %err,
                                "derived metric evaluation failed"
                            );
                        }
                        next_runs[idx] = Instant::now() + metric.interval();
                    }
                    if next_runs[idx] < next_wakeup {
                        next_wakeup = next_runs[idx];
                    }
                }

                let sleep_for = next_wakeup.saturating_duration_since(Instant::now());
                tokio::select! {
                    _ = tokio::time::sleep(sleep_for) => {}
                    _ = shutdown_clone.notified() => {
                        info!("derived metrics background job shutting down");
                        break;
                    }
                }
            }
        });

        Self { handle, shutdown }
    }

    /// Signal the background job to shut down and wait for it to finish.
    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
        if let Err(err) = self.handle.await {
            warn!(error = %err, "derived metrics background job panicked");
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// In-memory registry (used by tests and simple deployments)
// ---------------------------------------------------------------------------

/// A simple in-memory metric source, useful for tests.
#[derive(Default)]
pub struct InMemoryMetricSource {
    series: RwLock<HashMap<String, Vec<DataPoint>>>,
}

impl InMemoryMetricSource {
    /// Create an empty source.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a series under the given source name.
    pub async fn insert(&self, source: &str, points: Vec<DataPoint>) {
        self.series.write().await.insert(source.to_string(), points);
    }
}

#[async_trait::async_trait]
impl MetricSource for InMemoryMetricSource {
    async fn fetch(
        &self,
        source: &str,
        _filters: &HashMap<String, String>,
        window: Duration,
    ) -> Result<Vec<DataPoint>, DerivedMetricError> {
        let cutoff = now_epoch_seconds() - window.as_secs() as i64;
        let guard = self.series.read().await;
        Ok(guard
            .get(source)
            .map(|points| {
                points
                    .iter()
                    .copied()
                    .filter(|p| p.timestamp >= cutoff)
                    .collect()
            })
            .unwrap_or_default())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metric() -> DerivedMetric {
        DerivedMetric {
            name: "transfer_volume_1h".to_string(),
            description: Some("Rolling transfer volume".to_string()),
            aggregation: Aggregation::Rate,
            source: "transfer".to_string(),
            denominator: None,
            window_seconds: 3600,
            interval_seconds: 60,
            filters: HashMap::new(),
        }
    }

    // ---- Parser tests -----------------------------------------------------

    #[test]
    fn parses_yaml_config() {
        let yaml = r#"
metrics:
  - name: transfer_volume_1h
    description: Rolling transfer volume
    aggregation: rate
    source: transfer
    window_seconds: 3600
    interval_seconds: 60
  - name: unique_addresses_daily
    aggregation: moving_average
    source: unique_address
    window_seconds: 86400
    interval_seconds: 300
"#;
        let config = DerivedMetricsConfig::from_yaml(yaml).expect("valid yaml");
        assert_eq!(config.metrics.len(), 2);
        assert_eq!(config.metrics[0].name, "transfer_volume_1h");
        assert_eq!(config.metrics[0].aggregation, Aggregation::Rate);
        assert_eq!(config.metrics[1].aggregation, Aggregation::MovingAverage);
    }

    #[test]
    fn parses_toml_config() {
        let toml = r#"
[[metrics]]
name = "transfer_volume_1h"
aggregation = "rate"
source = "transfer"
window_seconds = 3600
interval_seconds = 60

[[metrics]]
name = "transfer_ratio"
aggregation = "ratio"
source = "transfer"
denominator = "transfer_total"
window_seconds = 3600
interval_seconds = 60
"#;
        let config = DerivedMetricsConfig::from_toml(toml).expect("valid toml");
        assert_eq!(config.metrics.len(), 2);
        assert_eq!(config.metrics[1].aggregation, Aggregation::Ratio);
        assert_eq!(
            config.metrics[1].denominator.as_deref(),
            Some("transfer_total")
        );
    }

    #[test]
    fn rejects_ratio_without_denominator() {
        let yaml = r#"
metrics:
  - name: bad_ratio
    aggregation: ratio
    source: transfer
"#;
        let err = DerivedMetricsConfig::from_yaml(yaml).unwrap_err();
        assert!(matches!(err, DerivedMetricError::Invalid(_)));
    }

    #[test]
    fn rejects_duplicate_names() {
        let yaml = r#"
metrics:
  - name: dup
    aggregation: rate
    source: a
  - name: dup
    aggregation: delta
    source: b
"#;
        let err = DerivedMetricsConfig::from_yaml(yaml).unwrap_err();
        assert!(matches!(err, DerivedMetricError::Invalid(_)));
    }

    #[test]
    fn rejects_unknown_aggregation() {
        let err = Aggregation::parse("bogus", None).unwrap_err();
        assert!(matches!(err, DerivedMetricError::Invalid(_)));
    }

    #[test]
    fn parses_percentile_aggregation() {
        let agg = Aggregation::parse("percentile", Some(95.0)).unwrap();
        assert_eq!(agg, Aggregation::Percentile { percentile: 95.0 });
    }

    #[test]
    fn rejects_percentile_out_of_range() {
        let err = Aggregation::parse("percentile", Some(150.0)).unwrap_err();
        assert!(matches!(err, DerivedMetricError::Invalid(_)));
    }

    #[test]
    fn rejects_invalid_metric_name() {
        let mut metric = sample_metric();
        metric.name = "bad name!".to_string();
        assert!(metric.validate().is_err());
    }

    // ---- Evaluator tests --------------------------------------------------

    fn points(values: &[(i64, f64)]) -> Vec<DataPoint> {
        values
            .iter()
            .map(|(t, v)| DataPoint {
                timestamp: *t,
                value: *v,
            })
            .collect()
    }

    #[test]
    fn evaluates_rate() {
        let metric = sample_metric();
        let series = points(&[(0, 100.0), (60, 160.0)]);
        let value = evaluate(&metric, &series, &[]).unwrap();
        assert!((value - 1.0).abs() < 1e-9);
    }

    #[test]
    fn evaluates_delta() {
        let mut metric = sample_metric();
        metric.aggregation = Aggregation::Delta;
        let series = points(&[(0, 100.0), (60, 160.0)]);
        let value = evaluate(&metric, &series, &[]).unwrap();
        assert!((value - 60.0).abs() < 1e-9);
    }

    #[test]
    fn evaluates_ratio() {
        let mut metric = sample_metric();
        metric.aggregation = Aggregation::Ratio;
        metric.denominator = Some("total".to_string());
        let numerator = points(&[(0, 10.0), (60, 20.0)]);
        let denominator = points(&[(0, 100.0), (60, 100.0)]);
        let value = evaluate(&metric, &numerator, &denominator).unwrap();
        assert!((value - 0.15).abs() < 1e-9);
    }

    #[test]
    fn evaluates_moving_average() {
        let mut metric = sample_metric();
        metric.aggregation = Aggregation::MovingAverage;
        let series = points(&[(0, 10.0), (60, 20.0), (120, 30.0)]);
        let value = evaluate(&metric, &series, &[]).unwrap();
        assert!((value - 20.0).abs() < 1e-9);
    }

    #[test]
    fn evaluates_percentile() {
        let mut metric = sample_metric();
        metric.aggregation = Aggregation::Percentile { percentile: 50.0 };
        let series = points(&[(0, 10.0), (60, 20.0), (120, 30.0)]);
        let value = evaluate(&metric, &series, &[]).unwrap();
        assert!((value - 20.0).abs() < 1e-9);
    }

    #[test]
    fn percentile_interpolates() {
        let sorted = [10.0, 20.0, 30.0, 40.0];
        let p90 = percentile_of(&sorted, 90.0);
        assert!((p90 - 37.0).abs() < 1e-9);
    }

    #[test]
    fn evaluate_errors_on_empty_series() {
        let metric = sample_metric();
        let err = evaluate(&metric, &[], &[]).unwrap_err();
        assert!(matches!(err, DerivedMetricError::Evaluation(_)));
    }

    #[test]
    fn evaluate_errors_on_zero_denominator() {
        let mut metric = sample_metric();
        metric.aggregation = Aggregation::Ratio;
        metric.denominator = Some("total".to_string());
        let numerator = points(&[(0, 10.0)]);
        let denominator = points(&[(0, 0.0)]);
        let err = evaluate(&metric, &numerator, &denominator).unwrap_err();
        assert!(matches!(err, DerivedMetricError::Evaluation(_)));
    }

    #[tokio::test]
    async fn evaluator_uses_in_memory_source() {
        let source = Arc::new(InMemoryMetricSource::new());
        let now = now_epoch_seconds();
        source
            .insert("transfer", points(&[(now - 60, 100.0), (now, 160.0)]))
            .await;

        let config = DerivedMetricsConfig {
            metrics: vec![sample_metric()],
        };
        let evaluator = DerivedMetricsEvaluator::new(config, source, None).unwrap();
        let results = evaluator.evaluate_all().await;
        assert_eq!(results.len(), 1);
        let (name, result) = &results[0];
        assert_eq!(name, "transfer_volume_1h");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn evaluator_reports_errors_for_missing_source() {
        let source = Arc::new(InMemoryMetricSource::new());
        let config = DerivedMetricsConfig {
            metrics: vec![sample_metric()],
        };
        let evaluator = DerivedMetricsEvaluator::new(config, source, None).unwrap();
        let results = evaluator.evaluate_all().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1.is_err());
    }
}