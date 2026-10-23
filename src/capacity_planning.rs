// Capacity Planning Automation (Issue #589)
//
// This module predicts resource requirements and alerts on capacity thresholds.
// It analyzes growth trends, creates forecasts, and generates recommendations.

use std::collections::HashMap;

/// Capacity metric representing resource usage over time
#[derive(Clone, Debug)]
pub struct CapacityMetric {
    pub timestamp: std::time::SystemTime,
    pub value: f64,
}

/// Growth trend analysis for capacity planning
#[derive(Clone, Debug)]
pub struct GrowthTrend {
    pub metric_name: String,
    pub current_value: f64,
    pub previous_value: f64,
    pub growth_rate_percent: f64,
    pub trend_direction: TrendDirection,
    pub forecast_7days: f64,
    pub forecast_30days: f64,
}

/// Direction of trend
#[derive(Clone, Debug, PartialEq)]
pub enum TrendDirection {
    Increasing,
    Stable,
    Decreasing,
}

/// Resource recommendation
#[derive(Clone, Debug)]
pub struct ResourceRecommendation {
    pub resource: String,
    pub current_capacity: f64,
    pub recommended_capacity: f64,
    pub urgency: CapacityUrgency,
    pub reasoning: String,
}

/// Urgency level for capacity recommendations
#[derive(Clone, Debug, PartialEq)]
pub enum CapacityUrgency {
    Critical,  // <7 days
    High,      // 7-14 days
    Medium,    // 14-30 days
    Low,       // >30 days
}

/// Capacity alert for notification
#[derive(Clone, Debug)]
pub struct CapacityAlert {
    pub alert_type: String,
    pub resource: String,
    pub current_value: f64,
    pub threshold: f64,
    pub message: String,
    pub severity: AlertSeverity,
}

/// Alert severity level
#[derive(Clone, Debug, PartialEq)]
pub enum AlertSeverity {
    Critical,
    Warning,
    Info,
}

/// Capacity forecast data
#[derive(Clone, Debug)]
pub struct CapacityForecast {
    pub metric: String,
    pub current_value: f64,
    pub forecast_data: Vec<(i32, f64)>, // (days_ahead, predicted_value)
}

/// Capacity planning engine
pub struct CapacityPlanner {
    metrics_history: HashMap<String, Vec<CapacityMetric>>,
    capacity_thresholds: HashMap<String, f64>,
    alert_thresholds: HashMap<String, f64>,
}

impl Default for CapacityPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl CapacityPlanner {
    pub fn new() -> Self {
        let mut planner = Self {
            metrics_history: HashMap::new(),
            capacity_thresholds: HashMap::new(),
            alert_thresholds: HashMap::new(),
        };
        planner.initialize_thresholds();
        planner
    }

    fn initialize_thresholds(&mut self) {
        // Database connections: 100 max
        self.capacity_thresholds.insert("db_connections".to_string(), 100.0);
        self.alert_thresholds.insert("db_connections".to_string(), 80.0);

        // Memory: 8GB
        self.capacity_thresholds.insert("memory_gb".to_string(), 8.0);
        self.alert_thresholds.insert("memory_gb".to_string(), 6.4);

        // Storage: 1TB
        self.capacity_thresholds.insert("storage_gb".to_string(), 1024.0);
        self.alert_thresholds.insert("storage_gb".to_string(), 819.2);

        // Event processing rate: 10k events/sec
        self.capacity_thresholds.insert("events_per_sec".to_string(), 10000.0);
        self.alert_thresholds.insert("events_per_sec".to_string(), 8000.0);

        // API concurrency: 1000 connections
        self.capacity_thresholds.insert("api_connections".to_string(), 1000.0);
        self.alert_thresholds.insert("api_connections".to_string(), 750.0);
    }

    /// Record a metric value
    pub fn record_metric(&mut self, metric_name: String, value: f64) {
        self.metrics_history
            .entry(metric_name)
            .or_default()
            .push(CapacityMetric {
                timestamp: std::time::SystemTime::now(),
                value,
            });
    }

    /// Analyze growth trend for a metric
    pub fn analyze_growth_trend(&self, metric_name: &str) -> Option<GrowthTrend> {
        let history = self.metrics_history.get(metric_name)?;
        if history.len() < 2 {
            return None;
        }

        let current_value = history.last()?.value;
        let previous_value = history[history.len().saturating_sub(2)].value;

        let growth_rate_percent = if previous_value == 0.0 {
            if current_value > 0.0 { 100.0 } else { 0.0 }
        } else {
            ((current_value - previous_value) / previous_value) * 100.0
        };

        let trend_direction = if growth_rate_percent > 5.0 {
            TrendDirection::Increasing
        } else if growth_rate_percent < -5.0 {
            TrendDirection::Decreasing
        } else {
            TrendDirection::Stable
        };

        let forecast_7days = self.forecast_value(metric_name, 7);
        let forecast_30days = self.forecast_value(metric_name, 30);

        Some(GrowthTrend {
            metric_name: metric_name.to_string(),
            current_value,
            previous_value,
            growth_rate_percent,
            trend_direction,
            forecast_7days,
            forecast_30days,
        })
    }

    /// Forecast resource value at days_ahead
    pub fn forecast_value(&self, metric_name: &str, days_ahead: i32) -> f64 {
        let (previous, current) = match self.metrics_history.get(metric_name).map(|h| h.as_slice()) {
            Some([.., previous, current]) => (previous.value, current.value),
            _ => return 0.0,
        };
        let daily_change = current - previous;

        current + (daily_change * days_ahead as f64)
    }

    /// Generate resource recommendation based on trends
    pub fn generate_recommendation(&self, metric_name: &str) -> Option<ResourceRecommendation> {
        let trend = self.analyze_growth_trend(metric_name)?;
        let capacity = self.capacity_thresholds.get(metric_name)?;

        if trend.trend_direction == TrendDirection::Decreasing {
            return None; // No recommendation needed if usage is decreasing
        }

        let forecast_30 = trend.forecast_30days;
        let remaining_capacity = capacity - trend.current_value;
        let recommended_capacity = if forecast_30 > *capacity {
            forecast_30 * 1.2 // Add 20% buffer
        } else {
            *capacity
        };

        let urgency = if remaining_capacity <= capacity * 0.1 {
            CapacityUrgency::Critical
        } else if forecast_30 > *capacity * 0.8 {
            CapacityUrgency::High
        } else if forecast_30 > *capacity * 0.5 {
            CapacityUrgency::Medium
        } else {
            CapacityUrgency::Low
        };

        let reasoning = format!(
            "Growth trend: {:.1}% per period. 30-day forecast: {:.0} (capacity: {:.0})",
            trend.growth_rate_percent, forecast_30, capacity
        );

        Some(ResourceRecommendation {
            resource: metric_name.to_string(),
            current_capacity: trend.current_value,
            recommended_capacity,
            urgency,
            reasoning,
        })
    }

    /// Check if metric exceeds alert threshold
    pub fn check_alert_threshold(&self, metric_name: &str) -> Option<CapacityAlert> {
        let history = self.metrics_history.get(metric_name)?;
        let current_value = history.last()?.value;

        let alert_threshold = self.alert_thresholds.get(metric_name)?;
        let capacity_threshold = self.capacity_thresholds.get(metric_name)?;

        if current_value > *alert_threshold {
            let severity = if current_value > *capacity_threshold {
                AlertSeverity::Critical
            } else if current_value > alert_threshold * 1.1 {
                AlertSeverity::Warning
            } else {
                AlertSeverity::Info
            };

            let percent_of_capacity = (current_value / capacity_threshold) * 100.0;
            let message = format!(
                "{}: {} at {:.1}% of capacity ({:.0}/{:.0})",
                metric_name, severity_to_string(&severity), percent_of_capacity, current_value, capacity_threshold
            );

            return Some(CapacityAlert {
                alert_type: "CAPACITY_THRESHOLD".to_string(),
                resource: metric_name.to_string(),
                current_value,
                threshold: *alert_threshold,
                message,
                severity,
            });
        }

        None
    }

    /// Generate capacity forecast for dashboard
    pub fn generate_forecast(&self, metric_name: &str, days: i32) -> Option<CapacityForecast> {
        let history = self.metrics_history.get(metric_name)?;
        let current_value = history.last()?.value;

        let mut forecast_data = Vec::new();
        for day in 1..=days {
            let predicted_value = self.forecast_value(metric_name, day);
            forecast_data.push((day, predicted_value));
        }

        Some(CapacityForecast {
            metric: metric_name.to_string(),
            current_value,
            forecast_data,
        })
    }

    /// Get all metrics with recommendations
    pub fn get_recommendations_summary(&self) -> Vec<ResourceRecommendation> {
        let mut recommendations = Vec::new();

        for metric_name in self.metrics_history.keys() {
            if let Some(rec) = self.generate_recommendation(metric_name) {
                if rec.urgency != CapacityUrgency::Low {
                    recommendations.push(rec);
                }
            }
        }

        recommendations.sort_by(|a, b| {
            let urgency_cmp = match (&a.urgency, &b.urgency) {
                (CapacityUrgency::Critical, CapacityUrgency::Critical) => std::cmp::Ordering::Equal,
                (CapacityUrgency::Critical, _) => std::cmp::Ordering::Less,
                (_, CapacityUrgency::Critical) => std::cmp::Ordering::Greater,
                (CapacityUrgency::High, CapacityUrgency::High) => std::cmp::Ordering::Equal,
                (CapacityUrgency::High, _) => std::cmp::Ordering::Less,
                (_, CapacityUrgency::High) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            };
            urgency_cmp
        });

        recommendations
    }

    /// Get all active capacity alerts
    pub fn get_active_alerts(&self) -> Vec<CapacityAlert> {
        let mut alerts = Vec::new();

        for metric_name in self.metrics_history.keys() {
            if let Some(alert) = self.check_alert_threshold(metric_name) {
                alerts.push(alert);
            }
        }

        alerts
    }
}

fn severity_to_string(severity: &AlertSeverity) -> &str {
    match severity {
        AlertSeverity::Critical => "CRITICAL",
        AlertSeverity::Warning => "WARNING",
        AlertSeverity::Info => "INFO",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_metric() {
        let mut planner = CapacityPlanner::new();
        planner.record_metric("test_metric".to_string(), 50.0);

        assert_eq!(planner.metrics_history.len(), 1);
        assert_eq!(planner.metrics_history["test_metric"][0].value, 50.0);
    }

    #[test]
    fn test_analyze_growth_trend_increasing() {
        let mut planner = CapacityPlanner::new();
        planner.record_metric("db_connections".to_string(), 50.0);
        planner.record_metric("db_connections".to_string(), 75.0);

        let trend = planner.analyze_growth_trend("db_connections").unwrap();
        assert_eq!(trend.trend_direction, TrendDirection::Increasing);
    }

    #[test]
    fn test_forecast_capacity() {
        let mut planner = CapacityPlanner::new();
        planner.record_metric("memory_gb".to_string(), 4.0);
        planner.record_metric("memory_gb".to_string(), 5.0);

        let forecast = planner.forecast_value("memory_gb", 7);
        assert!(forecast > 5.0); // Should predict higher usage
    }

    #[test]
    fn test_generate_recommendation() {
        let mut planner = CapacityPlanner::new();
        planner.record_metric("storage_gb".to_string(), 100.0);
        planner.record_metric("storage_gb".to_string(), 200.0);

        let rec = planner.generate_recommendation("storage_gb");
        assert!(rec.is_some());
    }

    #[test]
    fn test_check_alert_threshold() {
        let mut planner = CapacityPlanner::new();
        planner.record_metric("api_connections".to_string(), 100.0);

        let alert = planner.check_alert_threshold("api_connections");
        assert!(alert.is_none()); // Should not alert if below threshold
    }

    #[test]
    fn test_check_alert_threshold_exceeded() {
        let mut planner = CapacityPlanner::new();
        planner.record_metric("api_connections".to_string(), 800.0);

        let alert = planner.check_alert_threshold("api_connections");
        assert!(alert.is_some());
        assert_eq!(alert.unwrap().severity, AlertSeverity::Warning);
    }

    #[test]
    fn test_generate_forecast() {
        let mut planner = CapacityPlanner::new();
        planner.record_metric("events_per_sec".to_string(), 1000.0);
        planner.record_metric("events_per_sec".to_string(), 1500.0);

        let forecast = planner.generate_forecast("events_per_sec", 7);
        assert!(forecast.is_some());
        assert_eq!(forecast.unwrap().forecast_data.len(), 7);
    }
}
