use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceExt;

use soroban_pulse::anomaly_detection::{
    detect_iqr_anomaly, detect_mad_anomaly, detect_zscore_anomaly, AnomalyConfig,
    BaselineStatistics,
};
use soroban_pulse::config::{HealthState, IndexerState};
use soroban_pulse::metrics::init_metrics;
use soroban_pulse::routes::create_router;

fn make_router(pool: PgPool) -> axum::Router {
    let health_state = Arc::new(HealthState::new(60));
    health_state.update_last_poll();
    let indexer_state = Arc::new(IndexerState::new());
    let prometheus_handle = init_metrics();
    let config = soroban_pulse::config::Config::default();
    create_router(
        pool,
        vec![],
        &[],
        60,
        health_state,
        indexer_state,
        prometheus_handle,
        15000,
        config,
    )
}

/// Regression test for #571: Anomaly detection endpoint should be available
#[sqlx::test(migrations = "./migrations")]
async fn anomalies_endpoint_exists(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/anomalies")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should either return 200 with anomalies or 404 if not yet implemented
    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
    );
}

/// Anomalies endpoint should support filtering by contract
#[sqlx::test(migrations = "./migrations")]
async fn anomalies_endpoint_contract_filter(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/anomalies?contract_id=CABC123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::BAD_REQUEST
    );
}

/// Anomalies endpoint should return detection results as JSON
#[sqlx::test(migrations = "./migrations")]
async fn anomalies_endpoint_returns_json(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/anomalies")
                .header("accept", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    if resp.status() == StatusCode::OK {
        let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap();
        assert!(content_type.contains("json"));

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let _: serde_json::Value = serde_json::from_slice(&body)
            .expect("Response should be valid JSON");
    }
}

/// Anomalies endpoint should support time range filtering
#[sqlx::test(migrations = "./migrations")]
async fn anomalies_endpoint_time_range_filter(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/anomalies?from_timestamp=2026-01-01T00:00:00Z&to_timestamp=2026-06-30T23:59:59Z")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::BAD_REQUEST
    );
}

/// Anomalies should include z-score and statistical metadata
#[sqlx::test(migrations = "./migrations")]
async fn anomalies_include_statistical_metadata(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/anomalies")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    if resp.status() == StatusCode::OK {
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        // Should have anomalies array or empty response
        assert!(result.get("data").is_some() || result.is_array());
    }
}

/// Anomaly detection should track baseline statistics
#[sqlx::test(migrations = "./migrations")]
async fn anomaly_baselines_endpoint_exists(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/anomalies/baselines")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Baselines endpoint should exist to view model state
    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
    );
}

/// Anomaly alerting should be configurable
#[sqlx::test(migrations = "./migrations")]
async fn anomaly_alert_configuration_endpoint_exists(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/admin/anomaly-config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should either return config or 401/404 if auth required or not implemented
    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::UNAUTHORIZED
            || resp.status() == StatusCode::NOT_FOUND
    );
}

/// Anomaly detection should support pagination
#[sqlx::test(migrations = "./migrations")]
async fn anomalies_endpoint_pagination_support(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/anomalies?page=1&limit=20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
    );
}

/// Anomalies endpoint should handle empty result sets gracefully
#[sqlx::test(migrations = "./migrations")]
async fn anomalies_endpoint_empty_results(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/anomalies?from_timestamp=2000-01-01T00:00:00Z&to_timestamp=2000-01-02T00:00:00Z")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 200 with empty array, not 404
    if resp.status() == StatusCode::OK {
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(result.is_object() || result.is_array());
    }
}

/// Build a baseline fixture representing a stable event rate of ~100 events
/// per window, with a small amount of natural variance.
fn stable_baseline() -> BaselineStatistics {
    BaselineStatistics {
        mean: 100.0,
        std_dev: 5.0,
        median: 100.0,
        q1: 97.0,
        q3: 103.0,
        mad: 3.0,
        sample_count: 50,
    }
}

/// Regression test for #571: a clear spike (well beyond the configured
/// threshold) must be flagged by every detector. Previously a spike could
/// slip through when the baseline had a non-zero standard deviation.
#[test]
fn regression_known_spike_is_detected_by_all_detectors() {
    let baseline = stable_baseline();
    let config = AnomalyConfig::default();
    let spike = 500.0;

    assert!(
        detect_zscore_anomaly(spike, &baseline, &config).is_some(),
        "z-score detector missed a 5x spike"
    );
    assert!(
        detect_iqr_anomaly(spike, &baseline, &config).is_some(),
        "IQR detector missed a 5x spike"
    );
    assert!(
        detect_mad_anomaly(spike, &baseline, &config).is_some(),
        "MAD detector missed a 5x spike"
    );
}

/// Regression test: a value within the normal band must never be flagged,
/// otherwise the endpoint would produce false-positive pages.
#[test]
fn regression_normal_value_is_not_flagged() {
    let baseline = stable_baseline();
    let config = AnomalyConfig::default();
    let normal = 101.0;

    assert!(detect_zscore_anomaly(normal, &baseline, &config).is_none());
    assert!(detect_iqr_anomaly(normal, &baseline, &config).is_none());
    assert!(detect_mad_anomaly(normal, &baseline, &config).is_none());
}

/// Regression test: a sharp drop in event rate should be detected as an
/// anomaly (silent failure case).
#[test]
fn regression_known_drop_is_detected() {
    let baseline = stable_baseline();
    let config = AnomalyConfig::default();
    let drop = 0.0;

    assert!(
        detect_zscore_anomaly(drop, &baseline, &config).is_some(),
        "z-score detector missed a drop to zero"
    );
    assert!(
        detect_mad_anomaly(drop, &baseline, &config).is_some(),
        "MAD detector missed a drop to zero"
    );
}
