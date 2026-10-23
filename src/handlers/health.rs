//! Health-check handlers: liveness, readiness and dependency health.

#[allow(unused_imports)]
use super::*;

pub(super) async fn build_health_response(state: &AppState) -> (StatusCode, Value) {
    let mut db_ok = true;
    let db_status: &str;

    let timeout = Duration::from_millis(state.health_check_timeout_ms);

    let db_check =
        tokio::time::timeout(timeout, sqlx::query("SELECT 1").fetch_one(&state.pool)).await;

    match db_check {
        Ok(Ok(_)) => {
            db_status = "ok";
        }
        Ok(Err(sqlx::Error::PoolTimedOut)) => {
            db_ok = false;
            db_status = "pool_exhausted";
        }
        Ok(Err(_)) => {
            db_ok = false;
            db_status = "unreachable";
        }
        Err(_) => {
            // tokio timeout elapsed
            db_ok = false;
            db_status = "unreachable";
        }
    }

    // Check indexer status
    let indexer_status = if let Some(secs_ago) = state.health_state.is_indexer_stalled() {
        json!({
            "indexer": "stalled",
            "last_poll_secs_ago": secs_ago
        })
    } else {
        json!({"indexer": "ok"})
    };

    // Determine overall status
    let is_degraded =
        !db_ok || indexer_status.get("indexer").and_then(|v| v.as_str()) == Some("stalled");

    if is_degraded {
        let response = json!({
            "status": "degraded",
            "db": db_status,
        });
        // Merge indexer status
        let mut obj = response;
        if let Value::Object(ref mut map) = obj {
            if let Value::Object(indexer_map) = indexer_status {
                map.extend(indexer_map);
            }
        }
        (StatusCode::SERVICE_UNAVAILABLE, obj)
    } else {
        let response = json!({
            "status": "ok",
            "db": "ok",
            "indexer": "ok"
        });
        (StatusCode::OK, response)
    }
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "system",
    responses(
        (status = 200, description = "Service is healthy", body = serde_json::Value),
        (status = 503, description = "Service is degraded", body = ErrorResponse),
    )
)]
pub async fn health(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let (status, body) = build_health_response(&state).await;
    (status, Json(body))
}

#[utoipa::path(
    get,
    path = "/healthz/live",
    tag = "system",
    responses(
        (status = 200, description = "Process is alive", body = serde_json::Value),
    )
)]
pub async fn health_live() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({ "status": "alive" })))
}

#[utoipa::path(
    get,
    path = "/healthz/ready",
    tag = "system",
    responses(
        (status = 200, description = "Service is ready", body = serde_json::Value),
        (status = 503, description = "Service is not ready", body = ErrorResponse),
    )
)]
pub async fn health_ready(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    use crate::config::Role;

    match state.config.role {
        // ── ROLE=indexer ─────────────────────────────────────────────────────
        // Ready when the advisory lock is held (i.e. this pod is the active
        // indexer leader).  A standby pod that lost the lock is intentionally
        // not ready so that rolling updates don't stall on the standby, and
        // Kubernetes won't route traffic to it while it waits for promotion.
        Role::Indexer => {
            let is_leader = state
                .indexer_state
                .is_active_indexer
                .load(std::sync::atomic::Ordering::SeqCst);

            if is_leader {
                (
                    StatusCode::OK,
                    Json(json!({ "status": "ok", "role": "indexer", "lock": "held" })),
                )
            } else {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "status": "degraded",
                        "role": "indexer",
                        "lock": "standby",
                        "reason": "advisory lock not held; this replica is on standby"
                    })),
                )
            }
        }

        // ── ROLE=api ──────────────────────────────────────────────────────────
        // Ready when the database is reachable.  No indexer stall check —
        // api pods never run the indexer, so an indexer stall on another pod
        // should not make every api pod unready.
        Role::Api => {
            let timeout = Duration::from_millis(state.health_check_timeout_ms);
            let db_check =
                tokio::time::timeout(timeout, sqlx::query("SELECT 1").fetch_one(&state.pool))
                    .await;

            let (db_ok, db_status) = match db_check {
                Ok(Ok(_)) => (true, "ok"),
                Ok(Err(sqlx::Error::PoolTimedOut)) => (false, "pool_exhausted"),
                Ok(Err(_)) => (false, "unreachable"),
                Err(_) => (false, "timeout"),
            };

            if db_ok {
                (
                    StatusCode::OK,
                    Json(json!({ "status": "ok", "role": "api", "db": "ok" })),
                )
            } else {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({ "status": "degraded", "role": "api", "db": db_status })),
                )
            }
        }

        // ── ROLE=all (default) ────────────────────────────────────────────────
        // Backward-compatible: DB reachable AND indexer not stalled.
        Role::All => {
            let (status, body) = build_health_response(&state).await;
            (status, Json(body))
        }
    }
}

#[utoipa::path(
    get,
    path = "/healthz/postgres",
    tag = "system",
    responses(
        (status = 200, description = "PostgreSQL is healthy", body = serde_json::Value),
        (status = 503, description = "PostgreSQL is unhealthy", body = serde_json::Value),
    )
)]
pub async fn health_postgres(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let pg_health = crate::health_check::check_postgres(&state.pool, state.health_check_timeout_ms).await;
    
    let status_code = match pg_health.status {
        crate::health_check::HealthStatus::Ok => StatusCode::OK,
        crate::health_check::HealthStatus::Degraded => StatusCode::OK,
        crate::health_check::HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(json!(pg_health)))
}

#[utoipa::path(
    get,
    path = "/healthz/rpc",
    tag = "system",
    responses(
        (status = 200, description = "RPC endpoint is healthy", body = serde_json::Value),
        (status = 503, description = "RPC endpoint is unhealthy", body = serde_json::Value),
    )
)]
pub async fn health_rpc(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let rpc_health = crate::health_check::check_rpc(
        &state.config.stellar_rpc_url,
        state.health_check_timeout_ms,
    )
    .await;
    
    let status_code = match rpc_health.status {
        crate::health_check::HealthStatus::Ok => StatusCode::OK,
        crate::health_check::HealthStatus::Degraded => StatusCode::OK,
        crate::health_check::HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(json!(rpc_health)))
}

#[utoipa::path(
    get,
    path = "/healthz/external/{service}",
    tag = "system",
    params(("service" = String, Path, description = "External service name (e.g., 'webhooks', 'email')")),
    responses(
        (status = 200, description = "External service is healthy", body = serde_json::Value),
        (status = 400, description = "Invalid service name", body = serde_json::Value),
        (status = 503, description = "External service is unhealthy", body = serde_json::Value),
    )
)]
pub async fn health_external(
    State(_state): State<AppState>,
    Path(service): Path<String>,
) -> (StatusCode, Json<Value>) {
    // Validate service name
    let valid_services = vec!["webhooks", "email", "sms", "pagerduty"];
    if !valid_services.contains(&service.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("Unknown service: {}. Valid services are: {}", service, valid_services.join(", ")),
                "valid_services": valid_services,
            })),
        );
    }

    let external_health = crate::health_check::check_external_service(&service, 5000).await;
    
    let status_code = match external_health.status {
        crate::health_check::HealthStatus::Ok => StatusCode::OK,
        crate::health_check::HealthStatus::Degraded => StatusCode::OK,
        crate::health_check::HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(json!(external_health)))
}

/// Get pool health status
#[utoipa::path(
    get,
    path = "/v1/admin/pool-config/health",
    tag = "admin",
    responses(
        (status = 200, description = "Pool health status", body = Value),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_pool_health(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    crate::db_pool::management::check_pool_health(
        &state.db,
        state.pool_stats.clone(),
        state.config.db_max_connections,
        0.9,
    )
    .await
    .map(Json)
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))
}

/// Get overall statistics health score
#[utoipa::path(
    get,
    path = "/v1/admin/statistics/health",
    tag = "admin",
    responses(
        (status = 200, description = "Statistics health score (0-100)", body = StatisticsHealth),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Failed to get health score", body = ErrorResponse),
    )
)]
pub async fn get_statistics_health(
    State(state): State<AppState>,
) -> Result<Json<crate::models::StatisticsHealth>, (StatusCode, Json<Value>)> {
    let health_score = crate::statistics_management::get_statistics_health_score(&state.db)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))?;

    let stale = crate::statistics_management::detect_stale_statistics(&state.db)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))?;

    let stale_count = stale.iter().filter(|s| s.is_stale).count();
    let total_count = stale.len();

    let status = if health_score >= 80 {
        "healthy"
    } else if health_score >= 60 {
        "degraded"
    } else {
        "critical"
    };

    Ok(Json(crate::models::StatisticsHealth {
        health_score,
        status: status.to_string(),
        stale_tables_count: stale_count as u32,
        total_tables: total_count as u32,
        timestamp: chrono::Utc::now(),
    }))
}
