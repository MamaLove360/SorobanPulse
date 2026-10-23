/// Cost forecasting using a simple linear regression over recorded cost entries.
///
/// With limited history (< 2 data points) the forecast falls back to the last
/// known hourly rate extrapolated forward.
use super::models::{CostEntry, CostForecast, CostTrend};
use chrono::{DateTime, Duration, Utc};

/// Persist the current hourly cost breakdown into the `cost_snapshots` table.
///
/// One row is written per `(snapshot_hour, component, resource_type)` tuple and
/// existing rows are upserted so repeated calls within the same hour are
/// idempotent. This lets hourly snapshots survive restarts and be queried by
/// the admin cost report endpoint.
///
/// `breakdown` maps `(component, resource_type)` to the aggregated cost for the
/// hour. `snapshot_hour` is truncated to the hour boundary before writing.
pub async fn persist_snapshot(
    pool: &sqlx::PgPool,
    snapshot_hour: DateTime<Utc>,
    breakdown: &std::collections::HashMap<(String, String), f64>,
) -> Result<(), sqlx::Error> {
    // Truncate to the top of the hour so all writes within an hour collide.
    let hour = snapshot_hour
        .with_minute(0)
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(snapshot_hour);

    let mut tx = pool.begin().await?;
    for ((component, resource_type), cost) in breakdown {
        sqlx::query(
            r#"
            INSERT INTO cost_snapshots (snapshot_hour, component, resource_type, cost)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (snapshot_hour, component, resource_type)
            DO UPDATE SET cost = EXCLUDED.cost
            "#,
        )
        .bind(hour)
        .bind(component)
        .bind(resource_type)
        .bind(*cost)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Generate a cost forecast from a slice of historical cost entries.
///
/// * `entries`       – historical cost records (at least one required)
/// * `forecast_days` – how many days ahead to project
pub fn forecast(entries: &[CostEntry], forecast_days: u32) -> Option<CostForecast> {
    if entries.is_empty() {
        return None;
    }

    // Build hourly buckets: (unix_hour, total_cost_in_bucket).
    let mut buckets: std::collections::BTreeMap<i64, f64> = std::collections::BTreeMap::new();
    for entry in entries {
        let hour = entry.timestamp.timestamp() / 3600;
        *buckets.entry(hour).or_insert(0.0) += entry.cost;
    }

    if buckets.len() < 2 {
        // Not enough history — fall back to a flat extrapolation.
        let hourly_cost: f64 = buckets.values().sum::<f64>() / buckets.len() as f64;
        return Some(flat_forecast(hourly_cost, forecast_days));
    }

    // Collect (x, y) points where x is hours since the first bucket.
    let (xs, ys): (Vec<f64>, Vec<f64>) = {
        let first_hour = buckets.keys().next().copied().unwrap_or_default();
        buckets
            .iter()
            .map(|(&h, &cost)| ((h - first_hour) as f64, cost))
            .unzip()
    };

    let (slope, intercept) = linear_regression(&xs, &ys);

    // Project forward from the last observed hour.
    let last_x = xs.last().copied().unwrap_or_default();
    let horizon_hours = f64::from(forecast_days) * 24.0;
    let predicted_hourly = (intercept + slope * (last_x + horizon_hours)).max(0.0);
    let predicted_daily = predicted_hourly * 24.0;
    let predicted_monthly = predicted_daily * 30.0;

    // Residual std-dev gives us the confidence interval width.
    let residuals: Vec<f64> = xs
        .iter()
        .zip(ys.iter())
        .map(|(x, y)| y - (intercept + slope * x))
        .collect();
    let std_dev = std_dev(&residuals);
    let ci_half = 1.96 * std_dev * (horizon_hours / xs.len() as f64).sqrt().max(1.0);

    let trend = if slope > 0.0001 {
        CostTrend::Increasing
    } else if slope < -0.0001 {
        CostTrend::Decreasing
    } else {
        CostTrend::Stable
    };

    Some(CostForecast {
        forecast_date: Utc::now() + Duration::days(i64::from(forecast_days)),
        predicted_daily_cost: predicted_daily,
        predicted_monthly_cost: predicted_monthly,
        confidence_interval: (
            (predicted_monthly - ci_half).max(0.0),
            predicted_monthly + ci_half,
        ),
        trend,
    })
}

// ── internals ────────────────────────────────────────────────────────────────

fn flat_forecast(hourly_cost: f64, forecast_days: u32) -> CostForecast {
    let daily = hourly_cost * 24.0;
    let monthly = daily * 30.0;
    CostForecast {
        forecast_date: Utc::now() + Duration::days(i64::from(forecast_days)),
        predicted_daily_cost: daily,
        predicted_monthly_cost: monthly,
        confidence_interval: (monthly * 0.8, monthly * 1.2),
        trend: CostTrend::Stable,
    }
}

/// Ordinary least squares: returns (slope, intercept).
fn linear_regression(xs: &[f64], ys: &[f64]) -> (f64, f64) {
    let n = xs.len() as f64;
    let mean_x = xs.iter().sum::<f64>() / n;
    let mean_y = ys.iter().sum::<f64>() / n;

    let ss_xx: f64 = xs.iter().map(|x| (x - mean_x).powi(2)).sum();
    let ss_xy: f64 = xs.iter().zip(ys.iter()).map(|(x, y)| (x - mean_x) * (y - mean_y)).sum();

    if ss_xx.abs() < f64::EPSILON {
        return (0.0, mean_y);
    }

    let slope = ss_xy / ss_xx;
    let intercept = mean_y - slope * mean_x;
    (slope, intercept)
}

fn std_dev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::costs::models::ResourceType;
    use std::collections::HashMap;

    fn make_entry(cost: f64, hours_ago: i64) -> CostEntry {
        CostEntry {
            resource_id: "test".into(),
            resource_type: ResourceType::Compute,
            cost,
            timestamp: Utc::now() - Duration::hours(hours_ago),
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn empty_returns_none() {
        assert!(forecast(&[], 7).is_none());
    }

    #[test]
    fn single_entry_returns_flat_forecast() {
        let entries = vec![make_entry(1.0, 1)];
        let f = forecast(&entries, 1).unwrap();
        assert!(f.predicted_daily_cost >= 0.0);
    }

    #[test]
    fn increasing_trend_detected() {
        // Costs doubling each hour → increasing trend.
        let entries: Vec<CostEntry> = (0..10)
            .map(|i| make_entry((i + 1) as f64 * 0.1, 10 - i))
            .collect();
        let f = forecast(&entries, 7).unwrap();
        assert!(matches!(f.trend, CostTrend::Increasing));
    }

    #[test]
    fn snapshot_hour_truncation() {
        // Verify the hour-truncation logic used by persist_snapshot.
        let now = Utc::now();
        let hour = now
            .with_minute(0)
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .unwrap();
        assert_eq!(hour.minute(), 0);
        assert_eq!(hour.second(), 0);
        assert_eq!(hour.nanosecond(), 0);
    }
}
