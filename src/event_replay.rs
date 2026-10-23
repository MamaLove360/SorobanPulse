use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;
use tracing::{info, error, warn};

/// Replays estimated to touch more events than this require explicit
/// operator approval before they are allowed to proceed.
pub const LARGE_REPLAY_THRESHOLD: i64 = 100_000;

/// Dry-run cost estimate for a prospective replay, returned before any
/// replay is actually initiated.
#[derive(Debug, Serialize)]
pub struct ReplayCostEstimate {
    pub start_ledger: i64,
    pub end_ledger: i64,
    pub estimated_events: i64,
    pub requires_approval: bool,
}

/// Estimate the number of events a replay over `[start_ledger, end_ledger]`
/// would touch, without creating a replay record. Use this to dry-run a
/// replay before committing to it.
pub async fn estimate_replay_cost(
    pool: &PgPool,
    start_ledger: i64,
    end_ledger: i64,
) -> Result<ReplayCostEstimate, String> {
    let estimated_events = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM events WHERE ledger >= $1 AND ledger <= $2",
    )
    .bind(start_ledger)
    .bind(end_ledger)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to estimate replay cost: {}", e))?;

    Ok(ReplayCostEstimate {
        start_ledger,
        end_ledger,
        estimated_events,
        requires_approval: estimated_events > LARGE_REPLAY_THRESHOLD,
    })
}

/// Replay configuration for replaying events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Maximum age in days for replay (prevent replaying very old events)
    pub max_age_days: i32,
    /// Maximum events to replay in a single request
    pub max_events_per_request: i32,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            max_age_days: 90,
            max_events_per_request: 1000,
        }
    }
}

/// Request to replay events from a specific ledger
#[derive(Debug, Deserialize, Serialize)]
pub struct ReplayFromLedgerRequest {
    pub subscription_id: Uuid,
    pub ledger: i64,
    pub limit: Option<i32>,
}

/// Request to replay events from a specific timestamp
#[derive(Debug, Deserialize, Serialize)]
pub struct ReplayFromTimestampRequest {
    pub subscription_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub limit: Option<i32>,
}

/// Replay status for tracking replay operations
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ReplayStatus {
    pub id: Uuid,
    pub subscription_id: Uuid,
    pub start_ledger: i64,
    pub end_ledger: i64,
    pub total_events: i64,
    pub delivered_events: i64,
    pub failed_events: i64,
    pub status: String, // pending, in_progress, completed, failed
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Response for replay operations
#[derive(Debug, Serialize)]
pub struct ReplayResponse {
    pub replay_id: Uuid,
    pub subscription_id: Uuid,
    pub total_events: i64,
    pub status: String,
    pub message: String,
}

/// Replay a set of events from a specific ledger
pub async fn replay_from_ledger(
    pool: &PgPool,
    subscription_id: Uuid,
    start_ledger: i64,
    limit: Option<i32>,
    config: &ReplayConfig,
    approved: bool,
) -> Result<ReplayResponse, String> {
    let limit = limit.unwrap_or(config.max_events_per_request);
    let limit = limit.min(config.max_events_per_request);

    // Validate subscription exists
    let subscription = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM subscriptions WHERE id = $1)"
    )
    .bind(subscription_id)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to validate subscription: {}", e))?;

    if !subscription {
        return Err(format!("Subscription not found: {}", subscription_id));
    }

    // Get the current max ledger to determine end_ledger
    let current_ledger = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(ledger) FROM events"
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to get current ledger: {}", e))?
    .unwrap_or(start_ledger);

    let end_ledger = current_ledger;

    // Count events in the range
    let event_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM events WHERE ledger >= $1 AND ledger <= $2"
    )
    .bind(start_ledger)
    .bind(end_ledger)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to count events: {}", e))?;

    if event_count > LARGE_REPLAY_THRESHOLD && !approved {
        warn!(
            subscription_id = %subscription_id,
            event_count = event_count,
            "Rejected large replay pending approval"
        );
        return Err(format!(
            "Replay would touch {} events, exceeding the {} event safety threshold; call estimate_replay_cost to confirm scope, then retry with approved=true",
            event_count, LARGE_REPLAY_THRESHOLD
        ));
    }

    // Create replay status record
    let replay_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO replay_status (id, subscription_id, start_ledger, end_ledger, total_events, delivered_events, failed_events, status, started_at, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
    )
    .bind(replay_id)
    .bind(subscription_id)
    .bind(start_ledger)
    .bind(end_ledger)
    .bind(event_count)
    .bind(0i64)
    .bind(0i64)
    .bind("pending")
    .bind(Utc::now())
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| format!("Failed to create replay status: {}", e))?;

    info!(
        replay_id = %replay_id,
        subscription_id = %subscription_id,
        start_ledger = start_ledger,
        end_ledger = end_ledger,
        event_count = event_count,
        "Initiated event replay from ledger"
    );

    Ok(ReplayResponse {
        replay_id,
        subscription_id,
        total_events: event_count,
        status: "pending".to_string(),
        message: format!(
            "Replay started with {} events from ledger {} to {}",
            event_count, start_ledger, end_ledger
        ),
    })
}

/// Replay events from a specific timestamp
pub async fn replay_from_timestamp(
    pool: &PgPool,
    subscription_id: Uuid,
    timestamp: DateTime<Utc>,
    limit: Option<i32>,
    config: &ReplayConfig,
    approved: bool,
) -> Result<ReplayResponse, String> {
    let limit = limit.unwrap_or(config.max_events_per_request);
    let limit = limit.min(config.max_events_per_request);

    // Validate subscription exists
    let subscription = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM subscriptions WHERE id = $1)"
    )
    .bind(subscription_id)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to validate subscription: {}", e))?;

    if !subscription {
        return Err(format!("Subscription not found: {}", subscription_id));
    }

    // Check replay age limit
    let age_limit = Utc::now() - chrono::Duration::days(config.max_age_days as i64);
    if timestamp < age_limit {
        return Err(format!(
            "Cannot replay events older than {} days",
            config.max_age_days
        ));
    }

    // Get ledger bounds for the timestamp range
    let start_ledger = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MIN(ledger) FROM events WHERE timestamp >= $1"
    )
    .bind(timestamp)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to get start ledger: {}", e))?;

    let Some(start_ledger) = start_ledger else {
        return Err("No events found from specified timestamp".to_string());
    };

    let end_ledger = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(ledger) FROM events"
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to get end ledger: {}", e))?
    .unwrap_or(start_ledger);

    // Count events in the range
    let event_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM events WHERE timestamp >= $1 AND ledger >= $2 AND ledger <= $3 LIMIT $4"
    )
    .bind(timestamp)
    .bind(start_ledger)
    .bind(end_ledger)
    .bind(limit as i64)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to count events: {}", e))?;

    if event_count > LARGE_REPLAY_THRESHOLD && !approved {
        warn!(
            subscription_id = %subscription_id,
            event_count = event_count,
            "Rejected large replay pending approval"
        );
        return Err(format!(
            "Replay would touch {} events, exceeding the {} event safety threshold; call estimate_replay_cost to confirm scope, then retry with approved=true",
            event_count, LARGE_REPLAY_THRESHOLD
        ));
    }

    // Create replay status record
    let replay_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO replay_status (id, subscription_id, start_ledger, end_ledger, total_events, delivered_events, failed_events, status, started_at, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
    )
    .bind(replay_id)
    .bind(subscription_id)
    .bind(start_ledger)
    .bind(end_ledger)
    .bind(event_count)
    .bind(0i64)
    .bind(0i64)
    .bind("pending")
    .bind(Utc::now())
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| format!("Failed to create replay status: {}", e))?;

    info!(
        replay_id = %replay_id,
        subscription_id = %subscription_id,
        timestamp = %timestamp,
        start_ledger = start_ledger,
        end_ledger = end_ledger,
        event_count = event_count,
        "Initiated event replay from timestamp"
    );

    Ok(ReplayResponse {
        replay_id,
        subscription_id,
        total_events: event_count,
        status: "pending".to_string(),
        message: format!(
            "Replay started with {} events from timestamp {}",
            event_count, timestamp
        ),
    })
}

/// Get replay status
pub async fn get_replay_status(
    pool: &PgPool,
    replay_id: Uuid,
) -> Result<ReplayStatus, String> {
    sqlx::query_as::<_, ReplayStatus>(
        "SELECT id, subscription_id, start_ledger, end_ledger, total_events, delivered_events, failed_events, status, error_message, started_at, completed_at, created_at FROM replay_status WHERE id = $1"
    )
    .bind(replay_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| format!("Failed to get replay status: {}", e))?
    .ok_or_else(|| format!("Replay not found: {}", replay_id))
}

/// List replay operations for a subscription
pub async fn list_replays(
    pool: &PgPool,
    subscription_id: Uuid,
    limit: i64,
) -> Result<Vec<ReplayStatus>, String> {
    sqlx::query_as::<_, ReplayStatus>(
        "SELECT id, subscription_id, start_ledger, end_ledger, total_events, delivered_events, failed_events, status, error_message, started_at, completed_at, created_at
         FROM replay_status
         WHERE subscription_id = $1
         ORDER BY created_at DESC
         LIMIT $2"
    )
    .bind(subscription_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("Failed to list replays: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_replay_config() {
        let config = ReplayConfig::default();
        assert_eq!(config.max_age_days, 90);
        assert_eq!(config.max_events_per_request, 1000);
    }

    #[test]
    fn test_limit_clamping() {
        let config = ReplayConfig::default();
        let requested_limit = Some(5000);
        let clamped = requested_limit
            .unwrap_or(config.max_events_per_request)
            .min(config.max_events_per_request);
        assert_eq!(clamped, config.max_events_per_request);
    }

    #[test]
    fn test_large_replay_requires_approval() {
        assert!(LARGE_REPLAY_THRESHOLD + 1 > LARGE_REPLAY_THRESHOLD);
        let estimate = ReplayCostEstimate {
            start_ledger: 0,
            end_ledger: 1,
            estimated_events: LARGE_REPLAY_THRESHOLD + 1,
            requires_approval: LARGE_REPLAY_THRESHOLD + 1 > LARGE_REPLAY_THRESHOLD,
        };
        assert!(estimate.requires_approval);
    }
}
