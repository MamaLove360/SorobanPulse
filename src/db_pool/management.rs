//! Issue #692: Connection pool configuration and management
//!
//! Provides REST endpoints for dynamically querying and configuring database connection pool settings.
//! Includes pool statistics monitoring and automated tuning recommendations.

use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::info;

use super::monitor::PoolStats;
use crate::error::ApiError;
use crate::models::{
    PoolConfig, PoolConfigRequest, PoolConfigResponse, PoolStatistics, PoolTuningGuide,
};

/// Get current pool configuration and statistics
pub async fn get_pool_config(
    pool: &PgPool,
    stats: Arc<PoolStats>,
    max_connections: u32,
) -> Result<PoolStatistics, ApiError> {
    let size = pool.size();
    let idle = pool.num_idle();
    let active = size.saturating_sub(idle as u32);
    let utilization = if max_connections > 0 {
        active as f64 / max_connections as f64
    } else {
        0.0
    };

    Ok(PoolStatistics {
        pool_size: size,
        idle_connections: idle as u32,
        active_connections: active,
        max_connections,
        utilization,
        peak_utilization: stats.peak_utilization(),
        exhaustion_events: stats.exhaustion_events(),
        avg_acquire_latency_ms: 0.0, // This would be tracked separately
        snapshot_timestamp: Utc::now(),
    })
}

/// Validate pool configuration parameters
pub fn validate_pool_config(config: &PoolConfigRequest) -> Result<(), ApiError> {
    if let Some(max) = config.max_connections {
        if max < 1 || max > 1000 {
            return Err(ApiError::BadRequest(
                "max_connections must be between 1 and 1000".to_string(),
            ));
        }
    }

    if let Some(min) = config.min_connections {
        if min < 1 || min > 1000 {
            return Err(ApiError::BadRequest(
                "min_connections must be between 1 and 1000".to_string(),
            ));
        }
    }

    if let Some(timeout) = config.connection_timeout_secs {
        if timeout < 1 || timeout > 3600 {
            return Err(ApiError::BadRequest(
                "connection_timeout_secs must be between 1 and 3600".to_string(),
            ));
        }
    }

    if let Some(idle) = config.idle_timeout_secs {
        if idle < 1 || idle > 86400 {
            return Err(ApiError::BadRequest(
                "idle_timeout_secs must be between 1 and 86400".to_string(),
            ));
        }
    }

    if let Some(threshold) = config.exhaustion_threshold {
        if threshold <= 0.0 || threshold > 1.0 {
            return Err(ApiError::BadRequest(
                "exhaustion_threshold must be between 0.0 and 1.0".to_string(),
            ));
        }
    }

    if let Some(interval) = config.sample_interval_secs {
        if interval < 1 || interval > 3600 {
            return Err(ApiError::BadRequest(
                "sample_interval_secs must be between 1 and 3600".to_string(),
            ));
        }
    }

    // Validate that min is not greater than max if both are provided
    if let (Some(min), Some(max)) = (config.min_connections, config.max_connections) {
        if min > max {
            return Err(ApiError::BadRequest(
                "min_connections cannot be greater than max_connections".to_string(),
            ));
        }
    }

    Ok(())
}

/// Generate tuning recommendations based on pool statistics
pub fn generate_tuning_guide(
    stats: &PoolStatistics,
    current_config: &PoolConfig,
) -> PoolTuningGuide {
    let mut recommendations = Vec::new();
    let mut reasoning = String::new();

    // Recommendation 1: High utilization
    if stats.peak_utilization > 0.8 {
        recommendations.push(format!(
            "Increase max_connections from {} to {}",
            current_config.max_connections,
            (current_config.max_connections as f64 * 1.5) as u32
        ));
        reasoning.push_str("Peak utilization exceeds 80%, indicating potential connection exhaustion. ");
    }

    // Recommendation 2: Low utilization
    if stats.peak_utilization < 0.3 && current_config.min_connections > 2 {
        recommendations.push(format!(
            "Reduce min_connections from {} to {}",
            current_config.min_connections,
            (current_config.min_connections / 2).max(1)
        ));
        reasoning.push_str("Peak utilization is below 30%, suggesting over-provisioning. ");
    }

    // Recommendation 3: No exhaustion events
    if stats.exhaustion_events == 0 && stats.peak_utilization < 0.5 {
        recommendations.push("Current configuration is well-sized for your workload.".to_string());
        reasoning.push_str("No exhaustion events detected and peak utilization is healthy. ");
    }

    // Recommendation 4: Frequent exhaustion
    if stats.exhaustion_events > 10 {
        recommendations.push(format!(
            "Increase max_connections to at least {}",
            (current_config.max_connections as f64 * 1.5) as u32
        ));
        recommendations.push("Consider increasing connection timeout for high-latency operations.".to_string());
        reasoning.push_str("Frequent exhaustion events indicate undersized pool. ");
    }

    if recommendations.is_empty() {
        recommendations.push("No immediate tuning needed.".to_string());
        reasoning = "Pool is operating within healthy parameters.".to_string();
    }

    PoolTuningGuide {
        current_config: current_config.clone(),
        recommendations,
        reasoning,
    }
}

/// Check pool health and return status
pub async fn check_pool_health(
    pool: &PgPool,
    stats: Arc<PoolStats>,
    max_connections: u32,
    exhaustion_threshold: f64,
) -> Result<serde_json::Value, ApiError> {
    let pool_stats = get_pool_config(pool, stats, max_connections).await?;

    let is_healthy = pool_stats.utilization < exhaustion_threshold && pool_stats.active_connections > 0;

    Ok(json!({
        "healthy": is_healthy,
        "utilization_percent": (pool_stats.utilization * 100.0).round() as u32,
        "active_connections": pool_stats.active_connections,
        "max_connections": pool_stats.max_connections,
        "exhaustion_events": pool_stats.exhaustion_events,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_pool_config_valid() {
        let config = PoolConfigRequest {
            max_connections: Some(50),
            min_connections: Some(5),
            connection_timeout_secs: Some(30),
            idle_timeout_secs: Some(300),
            exhaustion_threshold: Some(0.85),
            sample_interval_secs: Some(15),
        };
        assert!(validate_pool_config(&config).is_ok());
    }

    #[test]
    fn validate_pool_config_invalid_max() {
        let config = PoolConfigRequest {
            max_connections: Some(2000),
            min_connections: None,
            connection_timeout_secs: None,
            idle_timeout_secs: None,
            exhaustion_threshold: None,
            sample_interval_secs: None,
        };
        assert!(validate_pool_config(&config).is_err());
    }

    #[test]
    fn validate_pool_config_min_gt_max() {
        let config = PoolConfigRequest {
            max_connections: Some(10),
            min_connections: Some(20),
            connection_timeout_secs: None,
            idle_timeout_secs: None,
            exhaustion_threshold: None,
            sample_interval_secs: None,
        };
        assert!(validate_pool_config(&config).is_err());
    }

    #[test]
    fn generate_tuning_guide_high_utilization() {
        let stats = PoolStatistics {
            pool_size: 50,
            idle_connections: 5,
            active_connections: 45,
            max_connections: 50,
            utilization: 0.9,
            peak_utilization: 0.95,
            exhaustion_events: 5,
            avg_acquire_latency_ms: 50.0,
            snapshot_timestamp: Utc::now(),
        };

        let config = PoolConfig {
            max_connections: 50,
            min_connections: 5,
            connection_timeout_secs: 30,
            idle_timeout_secs: 300,
            exhaustion_threshold: 0.9,
            sample_interval_secs: 15,
        };

        let guide = generate_tuning_guide(&stats, &config);
        assert!(!guide.recommendations.is_empty());
        assert!(guide.reasoning.contains("Peak utilization"));
    }
}
