// Issue #932: Event time series analysis
//
// Provides time-bucketed event counts, trend detection, seasonality detection,
// anomaly detection and point forecasting for indexed Soroban events.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{info, warn};

// ──────────────────────────────────────────────────────────────────────────────
// Public types
// ──────────────────────────────────────────────────────────────────────────────

/// The granularity at which events are bucketed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeSeriesGranularity {
    Hourly,
    Daily,
    Weekly,
    Monthly,
}

impl TimeSeriesGranularity {
    /// Return the PostgreSQL `date_trunc` truncation unit for this granularity.
    pub fn pg_trunc_unit(&self) -> &'static str {
        match self {
            Self::Hourly => "hour",
            Self::Daily => "day",
            Self::Weekly => "week",
            Self::Monthly => "month",
        }
    }

    /// Approximate duration of one bucket (used for forecasting / seasonality).
    pub fn bucket_duration(&self) -> Duration {
        match self {
            Self::Hourly => Duration::hours(1),
            Self::Daily => Duration::days(1),
            Self::Weekly => Duration::weeks(1),
            // Approximate — real months vary; 30 days is close enough for trend math.
            Self::Monthly => Duration::days(30),
        }
    }

    /// Human-readable label returned in API responses.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Hourly => "hourly",
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
        }
    }
}

/// A single data point in a time series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    /// The start of the time bucket.
    pub timestamp: DateTime<Utc>,
    /// Number of events in this bucket.
    pub count: i64,
    /// The contract ID this point belongs to, or `None` for an all-contracts rollup.
    pub contract_id: Option<String>,
}

/// A complete time series with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesSeries {
    pub points: Vec<TimeSeriesPoint>,
    /// Human-readable granularity label (e.g. `"hourly"`).
    pub granularity: String,
    /// Sum of all counts across all points.
    pub total: i64,
}

/// Direction of a detected trend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrendDirection {
    Increasing,
    Decreasing,
    Stable,
}

/// Result of a linear-regression trend analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendAnalysis {
    pub direction: TrendDirection,
    /// Slope of the least-squares regression line (events per bucket).
    pub slope: f64,
    /// R² coefficient of determination [0, 1]; higher means a better fit.
    pub confidence: f64,
    /// Number of data points used.
    pub data_points: usize,
}

/// Result of a seasonality test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeasonalityResult {
    /// Whether a repeating pattern was detected.
    pub has_seasonality: bool,
    /// Estimated period in *hours* if seasonality was found.
    pub period_hours: Option<f64>,
    /// Normalised strength of the seasonal signal [0, 1].
    pub strength: f64,
}

/// A single anomalous data point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyPoint {
    pub timestamp: DateTime<Utc>,
    /// Actual event count.
    pub value: i64,
    /// The expected (mean) value at the time of detection.
    pub expected: f64,
    /// How many standard deviations away from the mean this point is.
    pub deviation: f64,
}

/// Collection of anomalies detected in a time series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesAnomalies {
    pub anomalies: Vec<AnomalyPoint>,
    /// The threshold multiplier (in standard deviations) used for detection.
    pub threshold_multiplier: f64,
}

/// Query parameters for fetching a time series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesQuery {
    /// Optional contract ID to filter by; `None` returns an all-contracts rollup.
    pub contract_id: Option<String>,
    /// Start of the query range (inclusive).
    pub from: Option<DateTime<Utc>>,
    /// End of the query range (inclusive).
    pub to: Option<DateTime<Utc>>,
    /// Bucketing granularity.
    pub granularity: TimeSeriesGranularity,
}

// ──────────────────────────────────────────────────────────────────────────────
// Database query
// ──────────────────────────────────────────────────────────────────────────────

/// Fetch a time series from the `events` table, bucketed by the requested
/// granularity.
///
/// The query uses `date_trunc` to align timestamps and `COUNT(*)` for counts.
/// Results are ordered by bucket ascending.
pub async fn get_time_series(
    pool: &PgPool,
    query: TimeSeriesQuery,
) -> Result<TimeSeriesSeries, String> {
    let trunc_unit = query.granularity.pg_trunc_unit();

    // Build WHERE clauses dynamically so we don't bind superfluous parameters.
    let mut conditions: Vec<String> = Vec::new();
    if query.contract_id.is_some() {
        conditions.push("contract_id = $2".to_string());
    }

    // The parameter index for time bounds depends on whether contract_id is set.
    let mut next_param = if query.contract_id.is_some() { 3_usize } else { 2_usize };

    if query.from.is_some() {
        conditions.push(format!("timestamp >= ${}", next_param));
        next_param += 1;
    }
    if query.to.is_some() {
        conditions.push(format!("timestamp <= ${}", next_param));
        next_param += 1;
    }

    let _ = next_param; // used purely for numbering

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    // $1 is always the trunc unit (cast to text inside the SQL).
    let sql = format!(
        "SELECT date_trunc($1, timestamp) AS bucket, COUNT(*) AS count \
         FROM events \
         {} \
         GROUP BY bucket \
         ORDER BY bucket ASC",
        where_clause
    );

    // Bind parameters in declaration order.
    let mut q = sqlx::query_as::<_, (DateTime<Utc>, i64)>(&sql).bind(trunc_unit);

    if let Some(ref cid) = query.contract_id {
        q = q.bind(cid);
    }
    if let Some(from) = query.from {
        q = q.bind(from);
    }
    if let Some(to) = query.to {
        q = q.bind(to);
    }

    let rows: Vec<(DateTime<Utc>, i64)> = q
        .fetch_all(pool)
        .await
        .map_err(|e| format!("Failed to fetch time series: {}", e))?;

    let contract_label = query.contract_id.clone();
    let points: Vec<TimeSeriesPoint> = rows
        .into_iter()
        .map(|(bucket, count)| TimeSeriesPoint {
            timestamp: bucket,
            count,
            contract_id: contract_label.clone(),
        })
        .collect();

    let total = points.iter().map(|p| p.count).sum();

    info!(
        granularity = query.granularity.label(),
        data_points = points.len(),
        total,
        "Time series fetched"
    );

    Ok(TimeSeriesSeries {
        points,
        granularity: query.granularity.label().to_string(),
        total,
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// Trend detection (simple linear regression)
// ──────────────────────────────────────────────────────────────────────────────

/// Analyse the trend in a time series using ordinary least-squares regression.
///
/// Points are indexed 0 … N-1 on the x-axis (each index represents one bucket).
/// Returns [`TrendAnalysis`] with slope, R² confidence and direction.
///
/// - Slope >  0.05  → [`TrendDirection::Increasing`]
/// - Slope < -0.05  → [`TrendDirection::Decreasing`]
/// - Otherwise      → [`TrendDirection::Stable`]
pub fn detect_trend(series: &TimeSeriesSeries) -> TrendAnalysis {
    let n = series.points.len();
    if n < 2 {
        return TrendAnalysis {
            direction: TrendDirection::Stable,
            slope: 0.0,
            confidence: 0.0,
            data_points: n,
        };
    }

    let xs: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let ys: Vec<f64> = series.points.iter().map(|p| p.count as f64).collect();

    let (slope, intercept) = linear_regression(&xs, &ys);
    let r_squared = r_squared(&xs, &ys, slope, intercept);

    let direction = if slope > 0.05 {
        TrendDirection::Increasing
    } else if slope < -0.05 {
        TrendDirection::Decreasing
    } else {
        TrendDirection::Stable
    };

    TrendAnalysis {
        direction,
        slope,
        confidence: r_squared,
        data_points: n,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Seasonality detection
// ──────────────────────────────────────────────────────────────────────────────

/// Detect seasonality in a time series by comparing each point to the point
/// that is one candidate period earlier.
///
/// The candidate period is chosen based on granularity:
/// - Hourly  → 24 buckets (daily period)
/// - Daily   → 7 buckets (weekly period)
/// - Weekly  → 4 buckets (monthly period)
/// - Monthly → 12 buckets (annual period)
///
/// Strength is computed as 1 - (mean absolute deviation between corresponding
/// pairs) / (mean of all values).  A value above 0.7 is treated as seasonal.
pub fn detect_seasonality(series: &TimeSeriesSeries) -> SeasonalityResult {
    let n = series.points.len();

    // Determine the candidate period in buckets.
    let period_buckets: usize = match series.granularity.as_str() {
        "hourly" => 24,
        "daily" => 7,
        "weekly" => 4,
        "monthly" => 12,
        _ => 7,
    };

    // Need at least two full periods.
    if n < period_buckets * 2 {
        return SeasonalityResult {
            has_seasonality: false,
            period_hours: None,
            strength: 0.0,
        };
    }

    let values: Vec<f64> = series.points.iter().map(|p| p.count as f64).collect();
    let mean: f64 = values.iter().sum::<f64>() / n as f64;

    if mean == 0.0 {
        return SeasonalityResult {
            has_seasonality: false,
            period_hours: None,
            strength: 0.0,
        };
    }

    // Mean absolute deviation between values one period apart.
    let mut deviations = 0.0_f64;
    let mut pairs = 0_usize;

    for i in period_buckets..n {
        deviations += (values[i] - values[i - period_buckets]).abs();
        pairs += 1;
    }

    let mad = if pairs > 0 { deviations / pairs as f64 } else { 0.0 };
    let strength = (1.0 - mad / mean).max(0.0).min(1.0);
    let has_seasonality = strength >= 0.7;

    // Convert the period from buckets to hours.
    let bucket_hours = match series.granularity.as_str() {
        "hourly" => 1.0_f64,
        "daily" => 24.0,
        "weekly" => 168.0,
        "monthly" => 720.0,
        _ => 24.0,
    };
    let period_hours = if has_seasonality {
        Some(period_buckets as f64 * bucket_hours)
    } else {
        None
    };

    SeasonalityResult {
        has_seasonality,
        period_hours,
        strength,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Anomaly detection (mean ± k·σ)
// ──────────────────────────────────────────────────────────────────────────────

/// Detect anomalous points in a time series using a simple mean ± k·σ method.
///
/// A point is anomalous when `|value - mean| > threshold_multiplier × stddev`.
///
/// If there are fewer than 3 points (not enough to compute meaningful stats),
/// no anomalies are reported.
pub fn detect_anomalies(
    series: &TimeSeriesSeries,
    threshold_multiplier: f64,
) -> TimeSeriesAnomalies {
    let n = series.points.len();
    if n < 3 {
        return TimeSeriesAnomalies {
            anomalies: vec![],
            threshold_multiplier,
        };
    }

    let values: Vec<f64> = series.points.iter().map(|p| p.count as f64).collect();
    let mean: f64 = values.iter().sum::<f64>() / n as f64;
    let variance: f64 = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64;
    let stddev = variance.sqrt();

    if stddev == 0.0 {
        // All values identical — nothing is anomalous.
        return TimeSeriesAnomalies {
            anomalies: vec![],
            threshold_multiplier,
        };
    }

    let threshold = threshold_multiplier * stddev;
    let anomalies: Vec<AnomalyPoint> = series
        .points
        .iter()
        .filter_map(|p| {
            let diff = (p.count as f64 - mean).abs();
            if diff > threshold {
                Some(AnomalyPoint {
                    timestamp: p.timestamp,
                    value: p.count,
                    expected: mean,
                    deviation: diff / stddev,
                })
            } else {
                None
            }
        })
        .collect();

    if !anomalies.is_empty() {
        warn!(
            anomaly_count = anomalies.len(),
            threshold_multiplier,
            "Anomalies detected in time series"
        );
    }

    TimeSeriesAnomalies {
        anomalies,
        threshold_multiplier,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Forecasting (linear extrapolation)
// ──────────────────────────────────────────────────────────────────────────────

/// Forecast `count` future data points by extrapolating the trend of `series`.
///
/// Uses the same linear regression as [`detect_trend`].  The first forecasted
/// point starts immediately after the last observed bucket.  Counts are floored
/// at 0 (events cannot be negative).
pub fn forecast_next_points(series: &TimeSeriesSeries, count: usize) -> Vec<TimeSeriesPoint> {
    if series.points.is_empty() || count == 0 {
        return vec![];
    }

    let n = series.points.len();
    let xs: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let ys: Vec<f64> = series.points.iter().map(|p| p.count as f64).collect();

    let (slope, intercept) = linear_regression(&xs, &ys);

    // Determine the bucket duration so we can advance timestamps correctly.
    let bucket_dur = granularity_to_duration(&series.granularity);

    let Some(last_point) = series.points.last() else {
        return vec![];
    };
    let last_ts = last_point.timestamp;
    let last_x = (n - 1) as f64;

    (1..=count)
        .map(|i| {
            let x = last_x + i as f64;
            let predicted = (slope * x + intercept).max(0.0);
            let timestamp = last_ts + bucket_dur * (i as i32);
            TimeSeriesPoint {
                timestamp,
                count: predicted.round() as i64,
                contract_id: last_point.contract_id.clone(),
            }
        })
        .collect()
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Ordinary least-squares regression.  Returns `(slope, intercept)`.
fn linear_regression(xs: &[f64], ys: &[f64]) -> (f64, f64) {
    debug_assert_eq!(xs.len(), ys.len());
    let n = xs.len() as f64;
    let sum_x: f64 = xs.iter().sum();
    let sum_y: f64 = ys.iter().sum();
    let sum_xx: f64 = xs.iter().map(|x| x * x).sum();
    let sum_xy: f64 = xs.iter().zip(ys.iter()).map(|(x, y)| x * y).sum();

    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() < f64::EPSILON {
        return (0.0, sum_y / n);
    }

    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;
    (slope, intercept)
}

/// Compute R² (coefficient of determination) for a regression line.
fn r_squared(xs: &[f64], ys: &[f64], slope: f64, intercept: f64) -> f64 {
    let mean_y = ys.iter().sum::<f64>() / ys.len() as f64;
    let ss_tot: f64 = ys.iter().map(|y| (y - mean_y).powi(2)).sum();
    if ss_tot < f64::EPSILON {
        return 1.0; // Perfect fit when all Y values are the same.
    }
    let ss_res: f64 = xs
        .iter()
        .zip(ys.iter())
        .map(|(x, y)| {
            let predicted = slope * x + intercept;
            (y - predicted).powi(2)
        })
        .sum();
    (1.0 - ss_res / ss_tot).max(0.0).min(1.0)
}

/// Convert a granularity label string back to a [`Duration`].
fn granularity_to_duration(granularity: &str) -> Duration {
    match granularity {
        "hourly" => Duration::hours(1),
        "daily" => Duration::days(1),
        "weekly" => Duration::weeks(1),
        "monthly" => Duration::days(30),
        _ => Duration::days(1),
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Unit tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_series(counts: &[i64], granularity: &str) -> TimeSeriesSeries {
        let base = Utc::now();
        let dur = granularity_to_duration(granularity);
        let points = counts
            .iter()
            .enumerate()
            .map(|(i, &count)| TimeSeriesPoint {
                timestamp: base + dur * (i as i32),
                count,
                contract_id: None,
            })
            .collect();
        let total = counts.iter().sum();
        TimeSeriesSeries {
            points,
            granularity: granularity.to_string(),
            total,
        }
    }

    // ── TimeSeriesGranularity ────────────────────────────────────────────────

    #[test]
    fn test_granularity_pg_trunc_units() {
        assert_eq!(TimeSeriesGranularity::Hourly.pg_trunc_unit(), "hour");
        assert_eq!(TimeSeriesGranularity::Daily.pg_trunc_unit(), "day");
        assert_eq!(TimeSeriesGranularity::Weekly.pg_trunc_unit(), "week");
        assert_eq!(TimeSeriesGranularity::Monthly.pg_trunc_unit(), "month");
    }

    #[test]
    fn test_granularity_labels() {
        assert_eq!(TimeSeriesGranularity::Hourly.label(), "hourly");
        assert_eq!(TimeSeriesGranularity::Daily.label(), "daily");
        assert_eq!(TimeSeriesGranularity::Weekly.label(), "weekly");
        assert_eq!(TimeSeriesGranularity::Monthly.label(), "monthly");
    }

    #[test]
    fn test_granularity_serde_roundtrip() {
        for g in [
            TimeSeriesGranularity::Hourly,
            TimeSeriesGranularity::Daily,
            TimeSeriesGranularity::Weekly,
            TimeSeriesGranularity::Monthly,
        ] {
            let json = serde_json::to_string(&g).unwrap();
            let back: TimeSeriesGranularity = serde_json::from_str(&json).unwrap();
            assert_eq!(g, back);
        }
    }

    // ── detect_trend ────────────────────────────────────────────────────────

    #[test]
    fn test_trend_increasing() {
        let series = make_series(&[1, 3, 5, 7, 9, 11, 13], "daily");
        let trend = detect_trend(&series);
        assert_eq!(trend.direction, TrendDirection::Increasing);
        assert!(trend.slope > 0.0);
        assert!(trend.confidence > 0.95, "R² should be near 1 for perfect linear data");
        assert_eq!(trend.data_points, 7);
    }

    #[test]
    fn test_trend_decreasing() {
        let series = make_series(&[10, 8, 6, 4, 2, 0], "daily");
        let trend = detect_trend(&series);
        assert_eq!(trend.direction, TrendDirection::Decreasing);
        assert!(trend.slope < 0.0);
    }

    #[test]
    fn test_trend_stable() {
        let series = make_series(&[5, 5, 5, 5, 5, 5], "daily");
        let trend = detect_trend(&series);
        assert_eq!(trend.direction, TrendDirection::Stable);
        assert!((trend.slope).abs() < 1e-9);
    }

    #[test]
    fn test_trend_single_point() {
        let series = make_series(&[42], "daily");
        let trend = detect_trend(&series);
        assert_eq!(trend.direction, TrendDirection::Stable);
        assert_eq!(trend.data_points, 1);
    }

    #[test]
    fn test_trend_empty_series() {
        let series = make_series(&[], "daily");
        let trend = detect_trend(&series);
        assert_eq!(trend.direction, TrendDirection::Stable);
        assert_eq!(trend.data_points, 0);
    }

    // ── detect_seasonality ──────────────────────────────────────────────────

    #[test]
    fn test_seasonality_not_enough_data() {
        // Fewer than two full periods.
        let series = make_series(&[1, 2, 3], "hourly");
        let result = detect_seasonality(&series);
        assert!(!result.has_seasonality);
        assert!(result.period_hours.is_none());
    }

    #[test]
    fn test_seasonality_detected_daily_pattern() {
        // Repeat a 24-bucket pattern twice: strong daily seasonality.
        let pattern: Vec<i64> = (0..24).map(|i| if i % 4 == 0 { 10 } else { 1 }).collect();
        let doubled: Vec<i64> = pattern.iter().chain(pattern.iter()).copied().collect();
        let series = make_series(&doubled, "hourly");
        let result = detect_seasonality(&series);
        // With an almost-perfect pattern we expect high strength.
        assert!(
            result.strength > 0.5,
            "Expected strength > 0.5, got {}",
            result.strength
        );
    }

    #[test]
    fn test_seasonality_all_zeros() {
        let series = make_series(&vec![0; 48], "hourly");
        let result = detect_seasonality(&series);
        assert!(!result.has_seasonality, "All-zero series cannot be seasonal");
    }

    // ── detect_anomalies ────────────────────────────────────────────────────

    #[test]
    fn test_no_anomalies_uniform() {
        let series = make_series(&[5, 5, 5, 5, 5, 5, 5, 5, 5, 5], "daily");
        let result = detect_anomalies(&series, 2.0);
        assert!(result.anomalies.is_empty(), "Uniform series has no anomalies");
    }

    #[test]
    fn test_anomaly_spike() {
        // One large spike in an otherwise quiet series.
        let series = make_series(&[2, 2, 2, 2, 100, 2, 2, 2, 2, 2], "daily");
        let result = detect_anomalies(&series, 2.0);
        assert!(!result.anomalies.is_empty(), "Spike should be flagged as anomalous");
        let spike = &result.anomalies[0];
        assert_eq!(spike.value, 100);
        assert!(spike.deviation > 2.0);
    }

    #[test]
    fn test_anomaly_threshold_respected() {
        let series = make_series(&[1, 1, 1, 1, 4, 1, 1, 1, 1, 1], "daily");
        // With a very high threshold nothing should trigger.
        let loose = detect_anomalies(&series, 10.0);
        assert!(loose.anomalies.is_empty());
        // With a tight threshold the small spike should trigger.
        let tight = detect_anomalies(&series, 1.0);
        assert!(!tight.anomalies.is_empty());
    }

    #[test]
    fn test_anomaly_too_few_points() {
        let series = make_series(&[1, 1000], "daily");
        let result = detect_anomalies(&series, 2.0);
        assert!(result.anomalies.is_empty(), "Need >= 3 points for anomaly detection");
    }

    // ── forecast_next_points ────────────────────────────────────────────────

    #[test]
    fn test_forecast_count() {
        let series = make_series(&[1, 2, 3, 4, 5], "daily");
        let forecast = forecast_next_points(&series, 3);
        assert_eq!(forecast.len(), 3);
    }

    #[test]
    fn test_forecast_empty_series() {
        let series = make_series(&[], "daily");
        let forecast = forecast_next_points(&series, 5);
        assert!(forecast.is_empty());
    }

    #[test]
    fn test_forecast_zero_count() {
        let series = make_series(&[1, 2, 3], "daily");
        let forecast = forecast_next_points(&series, 0);
        assert!(forecast.is_empty());
    }

    #[test]
    fn test_forecast_increasing_trend() {
        // Perfect linear series: each forecasted count should be higher.
        let series = make_series(&[2, 4, 6, 8, 10], "daily");
        let forecast = forecast_next_points(&series, 3);
        assert_eq!(forecast.len(), 3);
        // Slope = 2, so next values should be around 12, 14, 16.
        assert!(forecast[0].count >= 11, "First forecast should be > 10");
        assert!(forecast[2].count > forecast[0].count, "Forecast should keep rising");
    }

    #[test]
    fn test_forecast_non_negative() {
        // Steeply declining series — forecasted values must not go negative.
        let series = make_series(&[100, 50, 10, 5, 1], "daily");
        let forecast = forecast_next_points(&series, 5);
        for point in forecast {
            assert!(point.count >= 0, "Forecasted count must be non-negative");
        }
    }

    #[test]
    fn test_forecast_timestamps_advance() {
        let series = make_series(&[1, 2, 3], "hourly");
        let forecast = forecast_next_points(&series, 3);
        let last_observed = series.points.last().unwrap().timestamp;
        for (i, point) in forecast.iter().enumerate() {
            let expected_ts = last_observed + Duration::hours((i as i64) + 1);
            assert_eq!(
                point.timestamp, expected_ts,
                "Forecasted timestamp should advance by one hour"
            );
        }
    }

    // ── linear_regression helper ─────────────────────────────────────────────

    #[test]
    fn test_linear_regression_perfect_fit() {
        let xs: Vec<f64> = vec![0.0, 1.0, 2.0, 3.0, 4.0];
        let ys: Vec<f64> = vec![1.0, 3.0, 5.0, 7.0, 9.0]; // y = 2x + 1
        let (slope, intercept) = linear_regression(&xs, &ys);
        assert!((slope - 2.0).abs() < 1e-9, "slope should be 2.0");
        assert!((intercept - 1.0).abs() < 1e-9, "intercept should be 1.0");
    }

    #[test]
    fn test_r_squared_perfect() {
        let xs: Vec<f64> = vec![0.0, 1.0, 2.0, 3.0];
        let ys: Vec<f64> = vec![0.0, 1.0, 2.0, 3.0]; // y = x
        let (s, i) = linear_regression(&xs, &ys);
        let r2 = r_squared(&xs, &ys, s, i);
        assert!((r2 - 1.0).abs() < 1e-9, "R² should be 1.0 for perfect linear data");
    }

    // ── TimeSeriesPoint / TimeSeriesSeries serde ────────────────────────────

    #[test]
    fn test_time_series_point_serde() {
        let point = TimeSeriesPoint {
            timestamp: Utc::now(),
            count: 42,
            contract_id: Some("CABC123".to_string()),
        };
        let json = serde_json::to_string(&point).unwrap();
        let back: TimeSeriesPoint = serde_json::from_str(&json).unwrap();
        assert_eq!(back.count, 42);
        assert_eq!(back.contract_id.as_deref(), Some("CABC123"));
    }

    #[test]
    fn test_trend_analysis_serde() {
        let ta = TrendAnalysis {
            direction: TrendDirection::Increasing,
            slope: 1.5,
            confidence: 0.92,
            data_points: 30,
        };
        let json = serde_json::to_string(&ta).unwrap();
        let back: TrendAnalysis = serde_json::from_str(&json).unwrap();
        assert_eq!(back.direction, TrendDirection::Increasing);
        assert!((back.slope - 1.5).abs() < 1e-9);
    }
}
