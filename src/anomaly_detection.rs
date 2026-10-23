use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::VecDeque;
use uuid::Uuid;
use tracing::{info, error, warn};

use crate::incident_correlation::{self, CorrelationSignal, SignalKind};

/// Anomaly detection method
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DetectionMethod {
    #[serde(rename = "zscore")]
    ZScore,
    #[serde(rename = "iqr")]
    IQR,
    #[serde(rename = "mad")]
    MAD, // Median Absolute Deviation
}

/// Baseline statistics for a metric
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct BaselineStatistics {
    pub id: Uuid,
    pub subscription_id: Uuid,
    pub metric_name: String,
    pub mean: f64,
    pub std_dev: f64,
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub q1: f64,
    pub q3: f64,
    pub mad: f64,                      // Median Absolute Deviation
    pub sample_count: i64,
    pub training_window_days: i32,
    pub last_updated: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// Anomaly detection alert
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AnomalyAlert {
    pub id: Uuid,
    pub subscription_id: Uuid,
    pub event_id: Uuid,
    pub metric_name: String,
    pub metric_value: f64,
    pub expected_range: (f64, f64), // Will be stored as JSON
    pub detection_method: String,
    pub anomaly_score: f64,           // zscore or IQR distance
    pub severity: String,             // low, medium, high, critical
    pub alerting_enabled: bool,
    pub acknowledged: bool,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Request to create anomaly detection configuration
#[derive(Debug, Deserialize)]
pub struct CreateAnomalyDetectionRequest {
    pub metric_name: String,
    pub detection_method: DetectionMethod,
    pub z_score_threshold: Option<f64>, // Number of standard deviations (default: 3.0)
    pub iqr_multiplier: Option<f64>,    // Multiplier for IQR (default: 1.5)
    pub training_window_days: Option<i32>, // Days of history for baseline (default: 30)
    pub alerting_enabled: Option<bool>,
}

/// Response for anomaly alert
#[derive(Debug, Serialize)]
pub struct AnomalyAlertResponse {
    pub alert_id: Uuid,
    pub metric_name: String,
    pub metric_value: f64,
    pub anomaly_score: f64,
    pub severity: String,
    pub message: String,
}

/// Request to acknowledge an anomaly
#[derive(Debug, Deserialize)]
pub struct AcknowledgeAnomalyRequest {
    pub notes: Option<String>,
}

/// Calculate baseline statistics for a metric
pub async fn calculate_baseline(
    pool: &PgPool,
    subscription_id: Uuid,
    metric_name: &str,
    training_window_days: i32,
) -> Result<BaselineStatistics, String> {
    let cutoff = Utc::now() - Duration::days(training_window_days as i64);

    // Fetch historical metric values
    let values = sqlx::query_scalar::<_, Option<f64>>(
        "SELECT metric_value FROM metric_history
         WHERE subscription_id = $1 AND metric_name = $2 AND timestamp >= $3
         ORDER BY timestamp"
    )
    .bind(subscription_id)
    .bind(metric_name)
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to fetch metric history: {}", e))?;

    let values: Vec<f64> = values.into_iter().filter_map(|v| v).collect();

    if values.is_empty() {
        return Err(format!(
            "No data available for metric: {} in the last {} days",
            metric_name, training_window_days
        ));
    }

    // Calculate statistics
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|x| (x - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    let std_dev = variance.sqrt();

    let mut sorted_values = values.clone();
    sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let min = sorted_values[0];
    let max = sorted_values[sorted_values.len() - 1];
    let median = calculate_percentile(&sorted_values, 0.5);
    let q1 = calculate_percentile(&sorted_values, 0.25);
    let q3 = calculate_percentile(&sorted_values, 0.75);

    // Calculate MAD (Median Absolute Deviation)
    let deviations: Vec<f64> = sorted_values
        .iter()
        .map(|x| (x - median).abs())
        .collect();
    let mad = calculate_percentile(&deviations, 0.5);

    let baseline_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO baseline_statistics (id, subscription_id, metric_name, mean, std_dev, min, max, median, q1, q3, mad, sample_count, training_window_days, last_updated, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"
    )
    .bind(baseline_id)
    .bind(subscription_id)
    .bind(metric_name)
    .bind(mean)
    .bind(std_dev)
    .bind(min)
    .bind(max)
    .bind(median)
    .bind(q1)
    .bind(q3)
    .bind(mad)
    .bind(values.len() as i64)
    .bind(training_window_days)
    .bind(Utc::now())
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| format!("Failed to store baseline statistics: {}", e))?;

    info!(
        subscription_id = %subscription_id,
        metric_name = %metric_name,
        mean = mean,
        std_dev = std_dev,
        "Baseline statistics calculated"
    );

    Ok(BaselineStatistics {
        id: baseline_id,
        subscription_id,
        metric_name: metric_name.to_string(),
        mean,
        std_dev,
        min,
        max,
        median,
        q1,
        q3,
        mad,
        sample_count: values.len() as i64,
        training_window_days,
        last_updated: Utc::now(),
        created_at: Utc::now(),
    })
}

/// Detect anomalies using Z-score method
pub fn detect_zscore_anomaly(
    baseline: &BaselineStatistics,
    value: f64,
    threshold: f64,
) -> Option<f64> {
    if baseline.std_dev == 0.0 {
        return None;
    }

    let zscore = (value - baseline.mean).abs() / baseline.std_dev;

    if zscore > threshold {
        Some(zscore)
    } else {
        None
    }
}

/// Detect anomalies using IQR method
pub fn detect_iqr_anomaly(
    baseline: &BaselineStatistics,
    value: f64,
    multiplier: f64,
) -> Option<f64> {
    let iqr = baseline.q3 - baseline.q1;
    let lower_bound = baseline.q1 - multiplier * iqr;
    let upper_bound = baseline.q3 + multiplier * iqr;

    if value < lower_bound {
        Some((lower_bound - value).abs())
    } else if value > upper_bound {
        Some((value - upper_bound).abs())
    } else {
        None
    }
}

/// Detect anomalies using MAD (Median Absolute Deviation) method
pub fn detect_mad_anomaly(
    baseline: &BaselineStatistics,
    value: f64,
    threshold: f64,
) -> Option<f64> {
    if baseline.mad == 0.0 {
        return None;
    }

    let modified_zscore = (value - baseline.median).abs() / (1.4826 * baseline.mad);

    if modified_zscore > threshold {
        Some(modified_zscore)
    } else {
        None
    }
}

/// Record an anomaly alert
pub async fn record_anomaly_alert(
    pool: &PgPool,
    subscription_id: Uuid,
    event_id: Uuid,
    metric_name: String,
    metric_value: f64,
    expected_range: (f64, f64),
    detection_method: DetectionMethod,
    anomaly_score: f64,
    alerting_enabled: bool,
) -> Result<AnomalyAlertResponse, String> {
    let alert_id = Uuid::new_v4();

    // Determine severity based on anomaly score
    let severity = if anomaly_score > 5.0 {
        "critical"
    } else if anomaly_score > 3.0 {
        "high"
    } else if anomaly_score > 2.0 {
        "medium"
    } else {
        "low"
    };

    let method_str = match detection_method {
        DetectionMethod::ZScore => "zscore",
        DetectionMethod::IQR => "iqr",
        DetectionMethod::MAD => "mad",
    };

    sqlx::query(
        "INSERT INTO anomaly_alerts (id, subscription_id, event_id, metric_name, metric_value, expected_range, detection_method, anomaly_score, severity, alerting_enabled, acknowledged, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
    )
    .bind(alert_id)
    .bind(subscription_id)
    .bind(event_id)
    .bind(&metric_name)
    .bind(metric_value)
    .bind(format!("[{}, {}]", expected_range.0, expected_range.1))
    .bind(method_str)
    .bind(anomaly_score)
    .bind(severity)
    .bind(alerting_enabled)
    .bind(false)
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| format!("Failed to record anomaly alert: {}", e))?;

    info!(
        alert_id = %alert_id,
        subscription_id = %subscription_id,
        metric_name = %metric_name,
        anomaly_score = anomaly_score,
        severity = severity,
        "Anomaly detected and alerted"
    );

    // Feed the anomaly into the incident correlation engine. Failure to
    // correlate must not fail alert recording, so errors are only logged.
    let signal = CorrelationSignal {
        subscription_id,
        kind: SignalKind::Anomaly,
        source: metric_name.clone(),
        severity: severity.to_string(),
        observed_at: Utc::now(),
        details: serde_json::json!({
            "alert_id": alert_id,
            "event_id": event_id,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "anomaly_score": anomaly_score,
            "detection_method": method_str,
        }),
    };

    if let Err(e) = incident_correlation::ingest_signal(pool, signal).await {
        error!(
            alert_id = %alert_id,
            subscription_id = %subscription_id,
            error = %e,
            "Failed to correlate anomaly signal into incidents"
        );
    }

    Ok(AnomalyAlertResponse {
        alert_id,
        metric_name,
        metric_value,
        anomaly_score,
        severity: severity.to_string(),
        message: format!(
            "Anomaly detected: {} = {}, expected range: {:.2} - {:.2}",
            metric_name, metric_value, expected_range.0, expected_range.1
        ),
    })
}

/// Get anomaly alerts for a subscription
pub async fn get_anomaly_alerts(
    pool: &PgPool,
    subscription_id: Uuid,
    limit: i64,
) -> Result<Vec<AnomalyAlert>, String> {
    // Note: This is a simplified version since we stored expected_range as string
    sqlx::query(
        "SELECT id, subscription_id, event_id, metric_name, metric_value, detection_method, anomaly_score, severity, alerting_enabled, acknowledged, acknowledged_at, created_at
         FROM anomaly_alerts
         WHERE subscription_id = $1 AND acknowledged = false
         ORDER BY created_at DESC
         LIMIT $2"
    )
    .bind(subscription_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to fetch anomaly alerts: {}", e))
    .map(|rows| {
        rows.into_iter()
            .map(|row| {
                let (id, sub_id, event_id, metric, value, method, score, severity, alert_en, ack, ack_at, created) =
                    row.into();
                AnomalyAlert {
                    id,
                    subscription_id: sub_id,
                    event_id,
                    metric_name: metric,
                    metric_value: value,
                    expected_range: (0.0, 0.0), // Simplified
                    detection_method: method,
                    anomaly_score: score,
                    severity,
                    alerting_enabled: alert_en,
                    acknowledged: ack,
                    acknowledged_at: ack_at,
                    created_at: created,
                }
            })
            .collect()
    })
}

/// Calculate percentile value
fn calculate_percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }

    let index = ((percentile * (sorted_values.len() - 1) as f64) as usize).min(sorted_values.len() - 1);
    sorted_values[index]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zscore_anomaly_detection() {
        let baseline = BaselineStatistics {
            id: Uuid::new_v4(),
            subscription_id: Uuid::new_v4(),
            metric_name: "test".to_string(),
            mean: 100.0,
            std_dev: 10.0,
            min: 80.0,
            max: 120.0,
            median: 100.0,
            q1: 95.0,
            q3: 105.0,
            mad: 5.0,
            sample_count: 100,
            training_window_days: 30,
            last_updated: Utc::now(),
            created_at: Utc::now(),
        };

        // Value within 3 std devs
        assert!(detect_zscore_anomaly(&baseline, 130.0, 3.0).is_none());

        // Value beyond 3 std devs
        assert!(detect_zscore_anomaly(&baseline, 140.0, 3.0).is_some());
    }

    #[test]
    fn test_iqr_anomaly_detection() {
        let baseline = BaselineStatistics {
            id: Uuid::new_v4(),
            subscription_id: Uuid::new_v4(),
            metric_name: "test".to_string(),
            mean: 100.0,
            std_dev: 10.0,
            min: 80.0,
            max: 120.0,
            median: 100.0,
            q1: 90.0,
            q3: 110.0,
            mad: 5.0,
            sample_count: 100,
            training_window_days: 30,
            last_updated: Utc::now(),
            created_at: Utc::now(),
        };

        // IQR = 20, bounds = [70, 130] with multiplier 1.5
        assert!(detect_iqr_anomaly(&baseline, 150.0, 1.5).is_some());
        assert!(detect_iqr_anomaly(&baseline, 100.0, 1.5).is_none());
    }

    #[test]
    fn test_percentile_calculation() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(calculate_percentile(&values, 0.5), 3.0); // median
        assert_eq!(calculate_percentile(&values, 0.25), 2.0); // Q1
        assert_eq!(calculate_percentile(&values, 0.75), 4.0); // Q3
    }

    #[test]
    fn test_percentile_empty_and_single() {
        assert_eq!(calculate_percentile(&[], 0.5), 0.0);
        assert_eq!(calculate_percentile(&[42.0], 0.5), 42.0);
        assert_eq!(calculate_percentile(&[42.0], 0.99), 42.0);
    }

    /// Build a baseline from raw samples using the same statistics as
    /// `calculate_baseline`, without touching the database.
    fn baseline_from_samples(samples: &[f64]) -> BaselineStatistics {
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let variance = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
            / samples.len() as f64;
        let std_dev = variance.sqrt();

        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let median = calculate_percentile(&sorted, 0.5);
        let deviations: Vec<f64> = sorted.iter().map(|x| (x - median).abs()).collect();

        BaselineStatistics {
            id: Uuid::new_v4(),
            subscription_id: Uuid::new_v4(),
            metric_name: "test".to_string(),
            mean,
            std_dev,
            min: sorted[0],
            max: sorted[sorted.len() - 1],
            median,
            q1: calculate_percentile(&sorted, 0.25),
            q3: calculate_percentile(&sorted, 0.75),
            mad: calculate_percentile(&deviations, 0.5),
            sample_count: samples.len() as i64,
            training_window_days: 30,
            last_updated: Utc::now(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn test_baseline_training_statistics() {
        let samples = [10.0, 12.0, 14.0, 16.0, 18.0];
        let baseline = baseline_from_samples(&samples);

        assert!((baseline.mean - 14.0).abs() < 1e-9);
        assert!((baseline.std_dev - 8.0_f64.sqrt()).abs() < 1e-9);
        assert_eq!(baseline.min, 10.0);
        assert_eq!(baseline.max, 18.0);
        assert_eq!(baseline.median, 14.0);
        assert_eq!(baseline.q1, 12.0);
        assert_eq!(baseline.q3, 16.0);
        assert_eq!(baseline.mad, 2.0);
        assert_eq!(baseline.sample_count, 5);
    }

    #[test]
    fn test_baseline_constant_samples_have_zero_spread() {
        let baseline = baseline_from_samples(&[5.0; 20]);
        assert_eq!(baseline.mean, 5.0);
        assert_eq!(baseline.std_dev, 0.0);
        assert_eq!(baseline.mad, 0.0);
        assert_eq!(baseline.min, 5.0);
        assert_eq!(baseline.max, 5.0);
    }

    #[test]
    fn test_zscore_spike_and_drop_detection() {
        let baseline = baseline_from_samples(&[100.0, 102.0, 98.0, 101.0, 99.0]);

        // Spike above the upper threshold.
        assert!(detect_zscore_anomaly(&baseline, 200.0, 3.0).is_some());
        // Drop below the lower threshold.
        assert!(detect_zscore_anomaly(&baseline, 0.0, 3.0).is_some());
        // Normal value.
        assert!(detect_zscore_anomaly(&baseline, 100.0, 3.0).is_none());
    }

    #[test]
    fn test_iqr_spike_and_drop_detection() {
        let baseline = baseline_from_samples(&[10.0, 20.0, 30.0, 40.0, 50.0]);

        // Spike above upper bound.
        assert!(detect_iqr_anomaly(&baseline, 1000.0, 1.5).is_some());
        // Drop below lower bound.
        assert!(detect_iqr_anomaly(&baseline, -1000.0, 1.5).is_some());
        // Within bounds.
        assert!(detect_iqr_anomaly(&baseline, 30.0, 1.5).is_none());
    }

    #[test]
    fn test_mad_spike_and_drop_detection() {
        let baseline = baseline_from_samples(&[10.0, 11.0, 12.0, 13.0, 14.0]);

        assert!(detect_mad_anomaly(&baseline, 100.0, 3.5).is_some());
        assert!(detect_mad_anomaly(&baseline, -100.0, 3.5).is_some());
        assert!(detect_mad_anomaly(&baseline, 12.0, 3.5).is_none());
    }

    #[test]
    fn test_zero_spread_never_panics_or_alerts() {
        let baseline = baseline_from_samples(&[7.0; 10]);
        assert!(detect_zscore_anomaly(&baseline, 1000.0, 3.0).is_none());
        assert!(detect_mad_anomaly(&baseline, 1000.0, 3.5).is_none());
        // IQR is zero, so bounds collapse to the median; a far value is still flagged.
        assert!(detect_iqr_anomaly(&baseline, 1000.0, 1.5).is_some());
        assert!(detect_iqr_anomaly(&baseline, 7.0, 1.5).is_none());
    }

    #[test]
    fn test_threshold_configuration_changes_outcome() {
        let baseline = baseline_from_samples(&[100.0, 102.0, 98.0, 101.0, 99.0]);

        // 150 is ~25 std devs out: flagged at any sane threshold.
        assert!(detect_zscore_anomaly(&baseline, 150.0, 3.0).is_some());
        // A very high threshold suppresses the alert.
        assert!(detect_zscore_anomaly(&baseline, 150.0, 1000.0).is_none());

        // IQR multiplier controls sensitivity.
        let iqr_baseline = baseline_from_samples(&[10.0, 20.0, 30.0, 40.0, 50.0]);
        assert!(detect_iqr_anomaly(&iqr_baseline, 100.0, 1.5).is_some());
        assert!(detect_iqr_anomaly(&iqr_baseline, 100.0, 100.0).is_none());
    }

    /// Minimal cooldown tracker mirroring the alert de-duplication behaviour.
    struct Cooldown {
        window: Duration,
        last: Option<DateTime<Utc>>,
    }

    impl Cooldown {
        fn new(window: Duration) -> Self {
            Self { window, last: None }
        }

        fn should_alert(&mut self, now: DateTime<Utc>) -> bool {
            match self.last {
                Some(prev) if now - prev < self.window => false,
                _ => {
                    self.last = Some(now);
                    true
                }
            }
        }
    }

    #[test]
    fn test_cooldown_suppresses_duplicate_alerts() {
        let mut cooldown = Cooldown::new(Duration::minutes(5));
        let start = Utc::now();

        assert!(cooldown.should_alert(start));
        assert!(!cooldown.should_alert(start + Duration::minutes(1)));
        assert!(!cooldown.should_alert(start + Duration::minutes(4)));
        // After the window elapses, alerting resumes.
        assert!(cooldown.should_alert(start + Duration::minutes(6)));
        assert!(!cooldown.should_alert(start + Duration::minutes(7)));
    }

    #[test]
    fn test_identical_rates_never_alert_property() {
        // Deterministic sweep over many constant-rate sequences.
        for rate in 0..500 {
            let value = rate as f64;
            let samples = vec![value; 30];
            let baseline = baseline_from_samples(&samples);

            assert!(detect_zscore_anomaly(&baseline, value, 3.0).is_none());
            assert!(detect_mad_anomaly(&baseline, value, 3.5).is_none());
            assert!(detect_iqr_anomaly(&baseline, value, 1.5).is_none());
        }
    }

    #[test]
    fn test_identical_rates_with_noise_never_alert_property() {
        // Constant rate with tiny floating-point jitter must not alert either.
        for rate in 0..200 {
            let base = rate as f64;
            let samples: Vec<f64> = (0..30).map(|i| base + (i as f64) * 1e-9).collect();
            let baseline = baseline_from_samples(&samples);
            assert!(detect_zscore_anomaly(&baseline, base, 3.0).is_none());
        }
    }
}

// === Seasonal baselines

/// Per-bucket seasonal baseline (hour-of-day or day-of-week).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeasonalBaseline {
    pub bucket: u32,
    pub mean: f64,
    pub std_dev: f64,
    pub sample_count: usize,
}

/// Seasonal bucketing granularity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SeasonalPeriod {
    #[serde(rename = "hour_of_day")]
    HourOfDay,
    #[serde(rename = "day_of_week")]
    DayOfWeek,
}

impl SeasonalPeriod {
    pub fn bucket_of(&self, ts: DateTime<Utc>) -> u32 {
        use chrono::{Datelike, Timelike};
        match self {
            SeasonalPeriod::HourOfDay => ts.hour(),
            SeasonalPeriod::DayOfWeek => ts.weekday().num_days_from_monday(),
        }
    }

    pub fn bucket_count(&self) -> usize {
        match self {
            SeasonalPeriod::HourOfDay => 24,
            SeasonalPeriod::DayOfWeek => 7,
        }
    }
}

/// Learn per-bucket baselines so recurring daily/weekly shape is not flagged as anomalous.
pub fn learn_seasonal_baselines(
    samples: &[(DateTime<Utc>, f64)],
    period: SeasonalPeriod,
) -> Vec<SeasonalBaseline> {
    let mut buckets: Vec<Vec<f64>> = vec![Vec::new(); period.bucket_count()];
    for (ts, value) in samples {
        buckets[period.bucket_of(*ts) as usize].push(*value);
    }

    buckets
        .into_iter()
        .enumerate()
        .filter(|(_, values)| !values.is_empty())
        .map(|(bucket, values)| {
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            SeasonalBaseline {
                bucket: bucket as u32,
                mean,
                std_dev: variance.sqrt(),
                sample_count: values.len(),
            }
        })
        .collect()
}

/// Z-score of a value against its seasonal bucket rather than the global baseline.
pub fn detect_seasonal_anomaly(
    baselines: &[SeasonalBaseline],
    period: SeasonalPeriod,
    ts: DateTime<Utc>,
    value: f64,
    threshold: f64,
) -> Option<f64> {
    let bucket = period.bucket_of(ts);
    let baseline = baselines.iter().find(|b| b.bucket == bucket)?;
    if baseline.std_dev == 0.0 {
        return None;
    }
    let score = (value - baseline.mean).abs() / baseline.std_dev;
    if score > threshold {
        Some(score)
    } else {
        None
    }
}

// === Time-series forecasting

/// Holt double exponential smoothing state (level + trend).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HoltForecaster {
    pub alpha: f64,
    pub beta: f64,
    pub level: f64,
    pub trend: f64,
    /// Smoothed absolute forecast error, used to build prediction intervals.
    pub mean_abs_error: f64,
    pub observations: usize,
}

impl HoltForecaster {
    /// `alpha`/`beta` are clamped because values outside (0, 1] make the recursion diverge.
    pub fn new(alpha: f64, beta: f64, initial_value: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.01, 1.0),
            beta: beta.clamp(0.0, 1.0),
            level: initial_value,
            trend: 0.0,
            mean_abs_error: 0.0,
            observations: 1,
        }
    }

    /// Fit from a series, returning None when there is nothing to seed the level with.
    pub fn fit(values: &[f64], alpha: f64, beta: f64) -> Option<Self> {
        let first = *values.first()?;
        let mut f = HoltForecaster::new(alpha, beta, first);
        for value in &values[1..] {
            f.observe(*value);
        }
        Some(f)
    }

    /// Forecast `steps` ahead from the current level and trend.
    pub fn forecast(&self, steps: usize) -> f64 {
        self.level + self.trend * steps as f64
    }

    /// Prediction interval around the one-step forecast, scaled by smoothed error.
    pub fn prediction_interval(&self, steps: usize, k: f64) -> (f64, f64) {
        let point = self.forecast(steps);
        let margin = k * self.mean_abs_error.max(f64::EPSILON) * 1.25;
        (point - margin, point + margin)
    }

    /// Feed the next observation, updating level, trend and smoothed error.
    pub fn observe(&mut self, value: f64) -> f64 {
        let predicted = self.forecast(1);
        let error = value - predicted;

        let prev_level = self.level;
        self.level = self.alpha * value + (1.0 - self.alpha) * (self.level + self.trend);
        self.trend = self.beta * (self.level - prev_level) + (1.0 - self.beta) * self.trend;

        let n = self.observations as f64;
        self.mean_abs_error = (self.mean_abs_error * n + error.abs()) / (n + 1.0);
        self.observations += 1;

        error
    }

    /// True when the observation falls outside the prediction interval.
    pub fn is_anomalous(&self, value: f64, k: f64) -> bool {
        if self.observations < 3 {
            return false;
        }
        let (lower, upper) = self.prediction_interval(1, k);
        value < lower || value > upper
    }
}

/// A detected change in the slope of a series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendBreak {
    pub index: usize,
    pub slope_before: f64,
    pub slope_after: f64,
    pub magnitude: f64,
}

/// Detect trend breaks by comparing the linear slope of adjacent windows.
pub fn detect_trend_breaks(values: &[f64], window: usize, min_magnitude: f64) -> Vec<TrendBreak> {
    if window < 2 || values.len() < window * 2 {
        return Vec::new();
    }

    let mut breaks = Vec::new();
    for index in window..=(values.len() - window) {
        let slope_before = linear_slope(&values[index - window..index]);
        let slope_after = linear_slope(&values[index..index + window]);
        let magnitude = (slope_after - slope_before).abs();
        if magnitude >= min_magnitude {
            breaks.push(TrendBreak {
                index,
                slope_before,
                slope_after,
                magnitude,
            });
        }
    }
    breaks
}

/// Least-squares slope of a series against its index.
pub fn linear_slope(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 2 {
        return 0.0;
    }
    let mean_x = (n - 1) as f64 / 2.0;
    let mean_y = values.iter().sum::<f64>() / n as f64;
    let mut numerator = 0.0;
    let mut denominator = 0.0;
    for (i, y) in values.iter().enumerate() {
        let dx = i as f64 - mean_x;
        numerator += dx * (y - mean_y);
        denominator += dx * dx;
    }
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

// === Correlation and root cause

/// Pearson correlation between two equal-length series.
pub fn pearson_correlation(a: &[f64], b: &[f64]) -> Option<f64> {
    if a.len() != b.len() || a.len() < 2 {
        return None;
    }
    let n = a.len() as f64;
    let mean_a = a.iter().sum::<f64>() / n;
    let mean_b = b.iter().sum::<f64>() / n;

    let mut cov = 0.0;
    let mut var_a = 0.0;
    let mut var_b = 0.0;
    for i in 0..a.len() {
        let da = a[i] - mean_a;
        let db = b[i] - mean_b;
        cov += da * db;
        var_a += da * da;
        var_b += db * db;
    }

    if var_a == 0.0 || var_b == 0.0 {
        return None;
    }
    Some(cov / (var_a.sqrt() * var_b.sqrt()))
}

/// A correlated metric pair, optionally directed by lead time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricCorrelation {
    pub source_metric: String,
    pub target_metric: String,
    pub coefficient: f64,
    /// Samples the source leads the target by; 0 means simultaneous.
    pub lag: usize,
}

/// Correlate one metric against candidates, testing lags up to `max_lag`.
///
/// The best positive lag is reported as a candidate causal edge: a metric that
/// moves first is more likely to be the root cause than one that moves with it.
pub fn correlate_metrics(
    source: (&str, &[f64]),
    candidates: &[(String, Vec<f64>)],
    max_lag: usize,
    min_coefficient: f64,
) -> Vec<MetricCorrelation> {
    let (source_metric, source_values) = source;
    let mut results = Vec::new();

    for (target_metric, target_values) in candidates {
        let mut best: Option<(f64, usize)> = None;
        for lag in 0..=max_lag {
            if source_values.len() <= lag || target_values.len() <= lag {
                break;
            }
            let len = (source_values.len() - lag).min(target_values.len() - lag);
            if len < 2 {
                break;
            }
            let a = &source_values[..len];
            let b = &target_values[lag..lag + len];
            if let Some(coefficient) = pearson_correlation(a, b) {
                if best.map_or(true, |(c, _)| coefficient.abs() > c.abs()) {
                    best = Some((coefficient, lag));
                }
            }
        }

        if let Some((coefficient, lag)) = best {
            if coefficient.abs() >= min_coefficient {
                results.push(MetricCorrelation {
                    source_metric: source_metric.to_string(),
                    target_metric: target_metric.clone(),
                    coefficient,
                    lag,
                });
            }
        }
    }

    results.sort_by(|a, b| {
        b.coefficient
            .abs()
            .partial_cmp(&a.coefficient.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results
}

/// Node in a causal graph built from correlated, lagged metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalEdge {
    pub cause: String,
    pub effect: String,
    pub confidence: f64,
}

/// Build causal edges from correlations: only lagged pairs are treated as directional.
pub fn build_causal_graph(correlations: &[MetricCorrelation]) -> Vec<CausalEdge> {
    correlations
        .iter()
        .filter(|c| c.lag > 0)
        .map(|c| CausalEdge {
            cause: c.source_metric.clone(),
            effect: c.target_metric.clone(),
            confidence: c.coefficient.abs(),
        })
        .collect()
}

/// Root cause summary for an anomaly, linking the strongest upstream metric.
#[derive(Debug, Clone, Serialize)]
pub struct RootCauseHypothesis {
    pub anomalous_metric: String,
    pub likely_cause: Option<String>,
    pub confidence: f64,
    pub supporting_edges: Vec<CausalEdge>,
}

/// Pick the highest-confidence cause pointing at the anomalous metric.
pub fn infer_root_cause(anomalous_metric: &str, edges: &[CausalEdge]) -> RootCauseHypothesis {
    let supporting: Vec<CausalEdge> = edges
        .iter()
        .filter(|e| e.effect == anomalous_metric)
        .cloned()
        .collect();

    let best = supporting
        .iter()
        .max_by(|a, b| {
            a.confidence
                .partial_cmp(&b.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned();

    RootCauseHypothesis {
        anomalous_metric: anomalous_metric.to_string(),
        likely_cause: best.as_ref().map(|e| e.cause.clone()),
        confidence: best.as_ref().map_or(0.0, |e| e.confidence),
        supporting_edges: supporting,
    }
}

// === Alert tuning

/// Operator feedback on a fired alert.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlertFeedback {
    #[serde(rename = "true_positive")]
    TruePositive,
    #[serde(rename = "false_positive")]
    FalsePositive,
    /// A real anomaly that never fired an alert.
    #[serde(rename = "missed")]
    Missed,
}

/// Rolling alert accuracy statistics used to auto-tune thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertTuner {
    pub metric_name: String,
    pub threshold: f64,
    pub min_threshold: f64,
    pub max_threshold: f64,
    /// Target share of alerts that are false positives (issue target: < 0.05).
    pub target_false_positive_rate: f64,
    history: VecDeque<AlertFeedback>,
    window: usize,
}

impl AlertTuner {
    pub fn new(metric_name: impl Into<String>, threshold: f64) -> Self {
        Self {
            metric_name: metric_name.into(),
            threshold,
            min_threshold: 1.5,
            max_threshold: 8.0,
            target_false_positive_rate: 0.05,
            history: VecDeque::new(),
            window: 100,
        }
    }

    /// Record feedback, evicting the oldest entry once the window is full.
    pub fn record_feedback(&mut self, feedback: AlertFeedback) {
        if self.history.len() == self.window {
            self.history.pop_front();
        }
        self.history.push_back(feedback);
    }

    pub fn sample_count(&self) -> usize {
        self.history.len()
    }

    fn count(&self, kind: AlertFeedback) -> usize {
        self.history.iter().filter(|f| **f == kind).count()
    }

    /// Share of fired alerts that were false positives.
    pub fn false_positive_rate(&self) -> f64 {
        let fired = self.count(AlertFeedback::TruePositive) + self.count(AlertFeedback::FalsePositive);
        if fired == 0 {
            return 0.0;
        }
        self.count(AlertFeedback::FalsePositive) as f64 / fired as f64
    }

    /// Share of all real anomalies that were correctly alerted on.
    pub fn accuracy(&self) -> f64 {
        let total = self.history.len();
        if total == 0 {
            return 0.0;
        }
        self.count(AlertFeedback::TruePositive) as f64 / total as f64
    }

    /// Raise the threshold when noisy, lower it when anomalies are being missed.
    ///
    /// Returns the updated threshold. Tuning is skipped below 10 samples so a
    /// couple of early mistakes cannot swing the threshold to a bound.
    pub fn tune(&mut self) -> f64 {
        if self.history.len() < 10 {
            return self.threshold;
        }

        let fpr = self.false_positive_rate();
        let missed = self.count(AlertFeedback::Missed) as f64 / self.history.len() as f64;

        if fpr > self.target_false_positive_rate {
            self.threshold *= 1.0 + (fpr - self.target_false_positive_rate).min(0.5);
        } else if missed > 0.05 {
            self.threshold *= 1.0 - missed.min(0.25);
        }

        self.threshold = self.threshold.clamp(self.min_threshold, self.max_threshold);
        self.threshold
    }
}

/// Persist operator feedback so thresholds can be re-tuned from stored history.
pub async fn record_alert_feedback(
    pool: &PgPool,
    alert_id: Uuid,
    feedback: AlertFeedback,
    notes: Option<&str>,
) -> Result<(), String> {
    let label = match feedback {
        AlertFeedback::TruePositive => "true_positive",
        AlertFeedback::FalsePositive => "false_positive",
        AlertFeedback::Missed => "missed",
    };

    sqlx::query(
        "INSERT INTO anomaly_alert_feedback (id, alert_id, feedback, notes, created_at)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(alert_id)
    .bind(label)
    .bind(notes)
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| {
        error!(alert_id = %alert_id, error = %e, "Failed to record alert feedback");
        format!("Failed to record alert feedback: {}", e)
    })?;

    Ok(())
}

/// Load stored feedback for a metric and return a tuned threshold.
pub async fn tune_threshold_from_history(
    pool: &PgPool,
    subscription_id: Uuid,
    metric_name: &str,
    current_threshold: f64,
) -> Result<f64, String> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT f.feedback FROM anomaly_alert_feedback f
         JOIN anomaly_alerts a ON a.id = f.alert_id
         WHERE a.subscription_id = $1 AND a.metric_name = $2
         ORDER BY f.created_at DESC
         LIMIT 100",
    )
    .bind(subscription_id)
    .bind(metric_name)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to load alert feedback: {}", e))?;

    let mut tuner = AlertTuner::new(metric_name, current_threshold);
    for row in rows.into_iter().rev() {
        match row.as_str() {
            "true_positive" => tuner.record_feedback(AlertFeedback::TruePositive),
            "false_positive" => tuner.record_feedback(AlertFeedback::FalsePositive),
            "missed" => tuner.record_feedback(AlertFeedback::Missed),
            other => warn!(feedback = %other, "Unknown alert feedback label, ignoring"),
        }
    }

    let tuned = tuner.tune();
    info!(
        subscription_id = %subscription_id,
        metric_name = %metric_name,
        previous = current_threshold,
        tuned = tuned,
        false_positive_rate = tuner.false_positive_rate(),
        "Auto-tuned anomaly threshold"
    );
    Ok(tuned)
}
