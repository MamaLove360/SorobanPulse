# Webhook Delivery Improvements - Testing & Validation Strategy

## Overview

This document outlines comprehensive testing strategies for each implementation phase to ensure reliability, performance, and correctness before production deployment.

---

## Phase 1: Metrics & Observability - Testing

### Unit Tests

**File: `tests/metrics_test.rs`**

```rust
#[cfg(test)]
mod metrics_tests {
    use super::*;
    use metrics::{counter, histogram, gauge};

    #[test]
    fn test_webhook_delivery_latency_recording() {
        // Arrange
        let endpoint = "https://webhook.example.com";
        
        // Act
        record_webhook_delivery_latency(endpoint, 150);
        record_webhook_delivery_latency(endpoint, 200);
        record_webhook_delivery_latency(endpoint, 100);
        
        // Assert: Verify histogram is recorded (requires metrics test harness)
        // This is implementation-dependent on metrics library
    }

    #[test]
    fn test_delivery_queue_depth_tracking() {
        let subscription_id = "sub-123";
        record_delivery_queue_depth(subscription_id, 42);
        // Assert gauge set to 42
    }

    #[test]
    fn test_endpoint_health_status_recording() {
        record_endpoint_health_status("endpoint1", "healthy");
        record_endpoint_health_status("endpoint1", "degraded");
        record_endpoint_health_status("endpoint2", "unhealthy");
        // Assert three recordings
    }
}
```

### Integration Tests

**File: `tests/metrics_aggregator_integration_test.rs`**

```rust
#[tokio::test]
async fn test_metrics_aggregator_calculates_percentiles() {
    let pool = setup_test_db().await;
    
    // Insert test delivery data with known latencies
    insert_test_deliveries(&pool, vec![
        (100, true),   // 100ms, success
        (150, true),   // 150ms, success
        (200, false),  // 200ms, failure
        (250, true),   // 250ms, success
        (300, false),  // 300ms, failure
    ]).await;

    // Run aggregator
    run_metrics_aggregator_cycle(&pool).await.unwrap();

    // Query results
    let metrics: WebhookEndpointMetrics = sqlx::query_as(
        "SELECT * FROM webhook_endpoint_metrics WHERE endpoint_url = $1"
    )
    .bind("https://webhook.example.com")
    .fetch_one(&pool)
    .await
    .unwrap();

    // Verify calculations
    assert_eq!(metrics.total_attempts, 5);
    assert_eq!(metrics.successful, 3);
    assert_eq!(metrics.failed, 2);
    assert!(metrics.p50_latency_ms.unwrap() >= 100.0 && metrics.p50_latency_ms.unwrap() <= 150.0);
    assert!(metrics.p95_latency_ms.unwrap() >= 250.0 && metrics.p95_latency_ms.unwrap() <= 300.0);
    assert_eq!(metrics.success_rate_percent, Some(60.0)); // 3/5
}

#[tokio::test]
async fn test_metrics_aggregator_updates_endpoint_health() {
    let pool = setup_test_db().await;
    
    // Insert endpoint with 92% success rate
    insert_test_deliveries(&pool, vec![
        (100, true), (110, true), (120, true), (130, true), (140, true),
        (150, true), (160, true), (170, true), (180, true), (190, true),
        (200, false), (210, false),
    ]).await;

    run_metrics_aggregator_cycle(&pool).await.unwrap();

    // Check health status updated to "degraded"
    let health: String = sqlx::query_scalar(
        "SELECT health_status FROM rate_limit_endpoints WHERE endpoint_url = $1"
    )
    .bind("https://webhook.example.com")
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(health, "degraded");
}

#[tokio::test]
async fn test_metrics_aggregator_handles_no_data() {
    let pool = setup_test_db().await;
    
    // Run with no data
    let result = run_metrics_aggregator_cycle(&pool).await;
    
    assert!(result.is_ok()); // Should not panic
}
```

### Performance Tests

**File: `benches/metrics_aggregator_bench.rs`**

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_metrics_aggregator(c: &mut Criterion) {
    c.bench_function("aggregator_10k_endpoints", |b| {
        b.to_async().block_on(async {
            let pool = setup_bench_db().await;
            insert_bench_deliveries(&pool, 10000, 100).await; // 10k endpoints, 100 deliveries each
            run_metrics_aggregator_cycle(&pool).await
        });
    });

    c.bench_function("aggregator_percentile_calculation_1m_events", |b| {
        b.to_async().block_on(async {
            let pool = setup_bench_db().await;
            insert_bench_deliveries(&pool, 1, 1_000_000).await; // 1M events for single endpoint
            run_metrics_aggregator_cycle(&pool).await
        });
    });
}

criterion_group!(benches, bench_metrics_aggregator);
criterion_main!(benches);
```

### Validation Checklist

- [ ] Latency histogram records correct values
- [ ] Percentiles match expected values within ±5%
- [ ] Queue depth gauge updates correctly
- [ ] Health status updates based on success rate
- [ ] No metrics loss under concurrent recording
- [ ] Aggregator completes < 30s for 10k endpoints
- [ ] Database indexes perform efficiently

---

## Phase 2: DLQ Management - Testing

### Unit Tests

**File: `tests/dlq_failure_categorization_test.rs`**

```rust
#[cfg(test)]
mod dlq_categorization_tests {
    use super::*;

    #[test]
    fn test_failure_reason_from_connection_timeout() {
        let error = "Request error: Connection timeout";
        let reason = FailureReason::from_error(error);
        assert_eq!(reason, FailureReason::ConnectionTimeout);
        assert_eq!(reason.as_str(), "connection_timeout");
    }

    #[test]
    fn test_failure_reason_from_http_status() {
        let error = "HTTP 502: Bad Gateway";
        let reason = FailureReason::from_error(error);
        match reason {
            FailureReason::HttpError(502) => (),
            _ => panic!("Expected HttpError(502), got {:?}", reason),
        }
    }

    #[test]
    fn test_failure_reason_from_tls_error() {
        let error = "TLS certificate validation failed";
        let reason = FailureReason::from_error(error);
        assert_eq!(reason, FailureReason::TlsCertificate);
    }

    #[test]
    fn test_failure_reason_from_dns_error() {
        let error = "Failed to resolve DNS name";
        let reason = FailureReason::from_error(error);
        assert_eq!(reason, FailureReason::DnsResolution);
    }

    #[test]
    fn test_failure_reason_fallback_to_other() {
        let error = "Unknown error: foo bar";
        let reason = FailureReason::from_error(error);
        assert_eq!(reason, FailureReason::Other("Unknown error: foo bar".to_string()));
    }
}
```

### Integration Tests

**File: `tests/dlq_replay_integration_test.rs`**

```rust
#[tokio::test]
async fn test_replay_by_endpoint_url() {
    let pool = setup_test_db().await;
    
    // Insert failed webhooks
    insert_webhook_failures(&pool, vec![
        ("https://slow.example.com", "HTTP 502", "pending"),
        ("https://slow.example.com", "HTTP 502", "pending"),
        ("https://fast.example.com", "Connection timeout", "pending"),
    ]).await;

    // Replay only slow endpoint
    let result = replay_webhook_failures(&pool, ReplayRequest {
        endpoint_url: Some("https://slow.example.com".to_string()),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(result.replayed_count, 2);

    // Verify status updated
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM webhook_failures 
         WHERE url = $1 AND status = 'pending'"
    )
    .bind("https://slow.example.com")
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_replay_by_failure_reason() {
    let pool = setup_test_db().await;
    
    insert_webhook_failures(&pool, vec![
        ("https://example1.com", "Connection timeout", "pending"),
        ("https://example2.com", "Connection timeout", "pending"),
        ("https://example3.com", "HTTP 502", "pending"),
    ]).await;

    let result = replay_webhook_failures(&pool, ReplayRequest {
        failure_reason: Some("Connection timeout".to_string()),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(result.replayed_count, 2);
}

#[tokio::test]
async fn test_replay_with_date_range() {
    let pool = setup_test_db().await;
    
    let now = chrono::Utc::now();
    let one_hour_ago = now - chrono::Duration::hours(1);
    let two_hours_ago = now - chrono::Duration::hours(2);

    insert_webhook_failures_with_timestamp(&pool, vec![
        ("https://example.com", two_hours_ago, "pending"),
        ("https://example.com", one_hour_ago, "pending"),
        ("https://example.com", now, "pending"),
    ]).await;

    let result = replay_webhook_failures(&pool, ReplayRequest {
        created_after: Some(one_hour_ago),
        created_before: Some(now),
        ..Default::default()
    }).await.unwrap();

    assert_eq!(result.replayed_count, 2);
}

#[tokio::test]
async fn test_dlq_stats_endpoint() {
    let pool = setup_test_db().await;
    
    insert_webhook_failures(&pool, vec![
        ("https://ep1.com", "error", "pending"),
        ("https://ep1.com", "error", "pending"),
        ("https://ep1.com", "error", "pending"),
        ("https://ep2.com", "error", "pending"),
    ]).await;

    let stats = get_dlq_stats(&pool).await.unwrap();

    assert_eq!(stats.total_pending, 4);
    assert_eq!(stats.endpoints_affected, 2);
}
```

### API Contract Tests

**File: `tests/dlq_api_test.rs`**

```rust
#[tokio::test]
async fn test_replay_endpoint_returns_correct_schema() {
    let app = create_test_router().await;
    
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/admin/webhooks/dlq/replay")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"endpoint_url":"https://example.com"}"#))
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body = body_to_string(response.into_body()).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    
    assert!(json["replayed_count"].is_number());
}

#[tokio::test]
async fn test_dlq_stats_endpoint_authorization() {
    let app = create_test_router().await;
    
    // Without API key
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/admin/webhooks/dlq/stats")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
```

### Validation Checklist

- [ ] Failure categorization correctly identifies all error types
- [ ] Replay mechanism respects all filters
- [ ] Replayed count accurate
- [ ] DLQ stats calculation correct
- [ ] No data loss during replay
- [ ] Alert thresholds trigger correctly
- [ ] Audit trail recorded for all replays

---

## Phase 3: Reliability - Testing

### Unit Tests

**File: `tests/circuit_breaker_test.rs`**

```rust
#[cfg(test)]
mod circuit_breaker_tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_opens_after_threshold() {
        let mut cb = CircuitBreaker::new(
            "https://example.com",
            5,  // failure_threshold
            60, // recovery_timeout_secs
        );

        for _ in 0..5 {
            cb.record_failure();
        }

        assert_eq!(cb.state, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_circuit_breaker_rejects_in_open_state() {
        let mut cb = CircuitBreaker::new("https://example.com", 5, 60);
        cb.state = CircuitState::Open;
        cb.last_failure_at = Some(Instant::now());

        let result = cb.try_deliver().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_circuit_breaker_transitions_to_half_open() {
        let mut cb = CircuitBreaker::new("https://example.com", 5, 1); // 1 sec recovery
        cb.state = CircuitState::Open;
        cb.last_failure_at = Some(Instant::now() - Duration::from_secs(2));

        let result = cb.try_deliver().await;
        assert!(result.is_ok());
        assert_eq!(cb.state, CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn test_circuit_breaker_resets_on_success() {
        let mut cb = CircuitBreaker::new("https://example.com", 5, 60);
        cb.failure_count = 3;
        cb.state = CircuitState::Closed;

        cb.record_success();

        assert_eq!(cb.failure_count, 0);
        assert_eq!(cb.state, CircuitState::Closed);
    }
}
```

### Integration Tests

**File: `tests/slow_endpoint_detection_test.rs`**

```rust
#[tokio::test]
async fn test_slow_endpoint_detection() {
    let pool = setup_test_db().await;

    // Insert metrics with high p99 latency
    insert_endpoint_metrics(&pool, vec![
        WebhookEndpointMetrics {
            endpoint_url: "https://slow.example.com".to_string(),
            p99_latency_ms: Some(5500.0),  // Exceeds 5s threshold
            success_rate: 95.0,
            ..Default::default()
        },
    ]).await;

    detect_slow_endpoints(&pool).await.unwrap();

    // Verify health status updated
    let health: String = sqlx::query_scalar(
        "SELECT health_status FROM rate_limit_endpoints WHERE endpoint_url = $1"
    )
    .bind("https://slow.example.com")
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(health, "degraded");
}

#[tokio::test]
async fn test_slo_calculation() {
    let pool = setup_test_db().await;

    insert_endpoint_metrics(&pool, vec![
        WebhookEndpointMetrics {
            endpoint_url: "https://reliable.example.com".to_string(),
            total_attempts: 1000,
            successful: 995,
            ..Default::default()
        },
    ]).await;

    let slo = calculate_endpoint_slo(&pool, "https://reliable.example.com").await.unwrap();
    assert!(slo.slo_met);
    assert_eq!(slo.success_rate_percent, 99.5);
}
```

### Validation Checklist

- [ ] Circuit breaker opens correctly
- [ ] Half-open state allows test request
- [ ] Recovery timeout works as expected
- [ ] Slow endpoint detection identifies p99 > 5s
- [ ] SLO calculation accurate to 2 decimals
- [ ] Database state transitions consistent
- [ ] No race conditions under concurrent load

---

## Phase 4: Customer Visibility - Testing

### API Endpoint Tests

**File: `tests/delivery_status_api_test.rs`**

```rust
#[tokio::test]
async fn test_delivery_status_returns_correct_fields() {
    let app = create_test_router().await;
    let subscription_id = create_test_subscription(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&format!("/v1/webhooks/{}/status", subscription_id))
                .header("Authorization", "Bearer test_key")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body = body_to_string(response.into_body()).await;
    let json: DeliveryStatusResponse = serde_json::from_str(&body).unwrap();

    assert_eq!(json.subscription_id, subscription_id);
    assert!(json.success_rate_percent >= 0.0 && json.success_rate_percent <= 100.0);
    assert!(json.avg_latency_ms >= 0.0);
    assert!(json.p95_latency_ms >= json.avg_latency_ms);
    assert!(json.p99_latency_ms >= json.p95_latency_ms);
}

#[tokio::test]
async fn test_test_webhook_endpoint() {
    let app = create_test_router().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/test")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"callback_url":"https://example.com/webhook"}"#))
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    let body = body_to_string(response.into_body()).await;
    let result: TestWebhookResponse = serde_json::from_str(&body).unwrap();

    assert!(result.latency_ms > 0);
    assert!(result.http_status.is_some() || result.error.is_some());
}

#[tokio::test]
async fn test_test_webhook_ssrf_validation() {
    let app = create_test_router().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/test")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"callback_url":"http://127.0.0.1/webhook"}"#))
                .unwrap()
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
```

### Analytics Query Tests

**File: `tests/subscription_analytics_test.rs`**

```rust
#[tokio::test]
async fn test_analytics_returns_correct_calculations() {
    let pool = setup_test_db().await;
    let subscription_id = create_test_subscription(&pool).await;

    // Insert test delivery trace logs
    insert_trace_logs(&pool, subscription_id, vec![
        (100, Some(200), None),  // 100ms, 200 OK
        (150, Some(200), None),  // 150ms, 200 OK
        (200, Some(500), None),  // 200ms, 500 error
        (120, Some(200), None),  // 120ms, 200 OK
    ]).await;

    let analytics = get_subscription_analytics(&pool, subscription_id).await.unwrap();

    assert_eq!(analytics.total_delivered, 3);
    assert_eq!(analytics.total_failed, 1);
    assert_eq!(analytics.success_rate, 75.0);
    assert!(analytics.avg_latency_ms >= 117.0 && analytics.avg_latency_ms <= 143.0);
}
```

### Validation Checklist

- [ ] Delivery status shows all required fields
- [ ] Test webhook endpoint validates connectivity
- [ ] SSRF validation prevents private IPs
- [ ] Analytics calculations accurate
- [ ] Trace log retention cleanup works
- [ ] Customer permissions enforced
- [ ] Response times < 500ms

---

## Phase 5: Load & Stress Testing

### Capacity Tests

**File: `benches/dlq_replay_capacity_bench.rs`**

```rust
#[tokio::test]
async fn test_replay_1m_webhooks() {
    let pool = setup_bench_db().await;
    
    // Insert 1M failed webhooks
    insert_bench_failures(&pool, 1_000_000).await;

    let start = Instant::now();
    let result = replay_webhook_failures(&pool, ReplayRequest {
        ..Default::default()
    }).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(result.replayed_count, 1_000_000);
    assert!(elapsed.as_secs() < 10); // Should complete in < 10s
}

#[tokio::test]
async fn test_analytics_query_with_1m_trace_logs() {
    let pool = setup_bench_db().await;
    let subscription_id = create_test_subscription(&pool).await;

    insert_bench_trace_logs(&pool, subscription_id, 1_000_000).await;

    let start = Instant::now();
    let _analytics = get_subscription_analytics(&pool, subscription_id).await.unwrap();
    let elapsed = start.elapsed();

    assert!(elapsed.as_millis() < 500); // Should complete < 500ms
}
```

### Concurrent Load Tests

**File: `tests/concurrent_load_test.rs`**

```rust
#[tokio::test]
async fn test_concurrent_metric_recording_100k() {
    let pool = setup_test_db().await;

    let tasks: Vec<_> = (0..100)
        .map(|i| {
            let pool = pool.clone();
            tokio::spawn(async move {
                for j in 0..1000 {
                    record_webhook_delivery_latency(
                        &format!("endpoint-{}", i),
                        (j * 10) as u64,
                    );
                }
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap();
    }

    // Verify no data loss
    // (requires metrics test harness to verify)
}
```

---

## Production Readiness Checklist

### Pre-Deployment Testing
- [ ] All unit tests pass (100% critical path coverage)
- [ ] All integration tests pass against staging DB
- [ ] Load test completed (10k endpoints, 1M events)
- [ ] Performance baseline established
- [ ] Database migrations validated
- [ ] Rollback procedure tested

### Deployment Gates
- [ ] Code review approval (2 reviewers)
- [ ] Security review (SSRF, injection prevention)
- [ ] Documentation complete
- [ ] Runbook reviewed by ops team
- [ ] Alert rules configured
- [ ] Monitoring dashboards ready

### Post-Deployment Validation
- [ ] Health check endpoint returning 200
- [ ] Metrics flowing to Prometheus
- [ ] No increase in error rates
- [ ] Aggregator running without errors
- [ ] API endpoints responding < 100ms p99
- [ ] DLQ processing normally
- [ ] Zero customer-impacting issues within 1 hour

