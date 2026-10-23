# Webhook Delivery Improvements - Implementation Guide

This guide provides step-by-step instructions for implementing each track. Code examples are provided in the appendix.

## Phase 1: Metrics & Observability

### Step 1: Extend metrics.rs
Add these functions to track delivery latency, queue depth, and endpoint health:

```rust
// Record delivery latency per endpoint
pub fn record_webhook_delivery_latency(endpoint: &str, duration_ms: u64) {
    m::histogram!(
        "soroban_pulse_webhook_delivery_latency_ms",
        "endpoint" => endpoint.to_string(),
    ).record(duration_ms as f64);
}

// Track pending deliveries per subscription
pub fn record_delivery_queue_depth(subscription_id: &str, count: i64) {
    m::gauge!(
        "soroban_pulse_delivery_queue_depth",
        "subscription_id" => subscription_id.to_string(),
    ).set(count as f64);
}

// Track endpoint health: healthy | degraded | unhealthy
pub fn record_endpoint_health_status(endpoint: &str, status: &str) {
    m::gauge!(
        "soroban_pulse_endpoint_health_status",
        "endpoint" => endpoint.to_string(),
        "status" => status.to_string(),
    ).set(1.0);
}

// Record DLQ backlog per endpoint
pub fn record_dlq_backlog(endpoint: &str, count: i64) {
    m::gauge!(
        "soroban_pulse_dlq_backlog",
        "endpoint" => endpoint.to_string(),
    ).set(count as f64);
}
```

**Integration point:** Call `record_webhook_delivery_latency()` in `webhook.rs` after delivery attempt:
```rust
let start = Instant::now();
let result = client.post(&url).send().await;
let latency_ms = start.elapsed().as_millis() as u64;
metrics::record_webhook_delivery_latency(url, latency_ms);
```

### Step 2: Create webhook_endpoint_metrics table

```sql
CREATE TABLE IF NOT EXISTS webhook_endpoint_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_url TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    total_attempts INT NOT NULL DEFAULT 0,
    successful INT NOT NULL DEFAULT 0,
    failed INT NOT NULL DEFAULT 0,
    avg_latency_ms NUMERIC,
    p50_latency_ms NUMERIC,
    p95_latency_ms NUMERIC,
    p99_latency_ms NUMERIC,
    success_rate_percent NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(endpoint_url, period_start)
);

CREATE INDEX idx_endpoint_metrics_endpoint_time 
    ON webhook_endpoint_metrics(endpoint_url, period_start DESC);
CREATE INDEX idx_endpoint_metrics_period 
    ON webhook_endpoint_metrics(period_start DESC);
```

### Step 3: Implement metrics aggregator

Create `src/workers/metrics_aggregator.rs`:
- Poll every 5 minutes
- Query `delivery_queue` for hourly stats grouped by endpoint
- Calculate p50/p95/p99 using PostgreSQL percentile_cont()
- Insert into `webhook_endpoint_metrics`
- Update `rate_limit_endpoints.health_status` based on success rate trends

Wire into main.rs startup sequence after other workers.

---

## Phase 2: Dead-Letter Queue Management

### Step 1: Create failure analysis table

```sql
CREATE TABLE IF NOT EXISTS dlq_analysis (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_url TEXT NOT NULL,
    failure_reason TEXT NOT NULL,
    failure_count INT NOT NULL DEFAULT 1,
    last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(endpoint_url, failure_reason)
);

CREATE INDEX idx_dlq_analysis_failure_count ON dlq_analysis(failure_count DESC);
CREATE INDEX idx_dlq_analysis_endpoint ON dlq_analysis(endpoint_url);
```

### Step 2: Categorize failure reasons

Enhance webhook.rs error handling:
```rust
pub enum FailureReason {
    ConnectionTimeout,
    ConnectionRefused,
    TlsCertificate,
    DnsResolution,
    HttpError(u16),        // 4xx/5xx
    InvalidJson,
    Other(String),
}

impl FailureReason {
    pub fn from_error(e: &str) -> Self {
        if e.contains("timeout") { Self::ConnectionTimeout }
        else if e.contains("refused") { Self::ConnectionRefused }
        else if e.contains("TLS") || e.contains("certificate") { Self::TlsCertificate }
        else if e.contains("resolve") { Self::DnsResolution }
        else if e.contains("HTTP") { 
            // Extract status code: "HTTP 502: Bad Gateway"
            e.split_whitespace()
                .nth(1)
                .and_then(|s| s.parse::<u16>().ok())
                .map(|code| Self::HttpError(code))
                .unwrap_or(Self::Other(e.to_string()))
        }
        else { Self::Other(e.to_string()) }
    }
    
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ConnectionTimeout => "connection_timeout",
            Self::ConnectionRefused => "connection_refused",
            Self::TlsCertificate => "tls_certificate",
            Self::DnsResolution => "dns_resolution",
            Self::HttpError(_) => "http_error",
            Self::InvalidJson => "invalid_json",
            Self::Other(_) => "other",
        }
    }
}

pub async fn categorize_and_store_failure(
    pool: &PgPool,
    endpoint: &str,
    error: &str,
) {
    let reason = FailureReason::from_error(error);
    let reason_key = reason.as_str();
    
    sqlx::query(
        "INSERT INTO dlq_analysis (endpoint_url, failure_reason, failure_count, last_failed_at)
         VALUES ($1, $2, 1, NOW())
         ON CONFLICT (endpoint_url, failure_reason) DO UPDATE SET
            failure_count = failure_count + 1,
            last_failed_at = NOW()"
    )
    .bind(endpoint)
    .bind(reason_key)
    .execute(pool)
    .await
    .ok();
}
```

### Step 3: Create replay API endpoint

Create `src/handlers/webhook_replay.rs`:
```rust
#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    pub endpoint_url: Option<String>,
    pub failure_reason: Option<String>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub limit: Option<i32>,
}

pub async fn replay_webhook_failures(
    State(state): State<AppState>,
    Json(req): Json<ReplayRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    // 1. Build query with optional filters
    // 2. Update webhook_failures: status='pending', attempts=0, next_retry_at=NOW()
    // 3. Return count
    
    let mut query = "UPDATE webhook_failures SET status='pending', attempts=0 WHERE 1=1".to_string();
    if let Some(url) = &req.endpoint_url {
        query.push_str(&format!(" AND url = '{}'", url));
    }
    if let Some(reason) = &req.failure_reason {
        query.push_str(&format!(" AND last_error LIKE '%{}%'", reason));
    }
    // ... add more filters
    
    let result = sqlx::query(&query).execute(&state.pool).await?;
    
    Ok(Json(json!({
        "replayed_count": result.rows_affected()
    })))
}

pub async fn get_dlq_stats(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    // Return: total pending, by endpoint, by reason, oldest
}
```

### Step 4: Add DLQ alerts

Create PostgreSQL function to check alert thresholds:
```sql
CREATE OR REPLACE FUNCTION check_dlq_alerts()
RETURNS TABLE(endpoint_url TEXT, pending_count INT, alert_type TEXT) AS $$
    -- Alert 1: More than 1000 pending for any endpoint
    SELECT url, COUNT(*)::INT, 'BACKLOG_HIGH'
    FROM webhook_failures
    WHERE status = 'pending'
    GROUP BY url
    HAVING COUNT(*) > 1000
    
    UNION ALL
    
    -- Alert 2: Oldest pending > 24 hours
    SELECT url, 1, 'BACKLOG_STALE'
    FROM webhook_failures
    WHERE status = 'pending'
    GROUP BY url
    HAVING MIN(created_at) < NOW() - interval '24 hours';
$$ LANGUAGE SQL;
```

---

## Phase 3: Reliability Improvements

### Step 1: Implement circuit breaker

Create `src/circuit_breaker.rs`:
- Enum: Closed, Open, HalfOpen
- Track failures and recovery window
- Integrate into delivery worker

### Step 2: Extend rate_limit_endpoints

```sql
ALTER TABLE rate_limit_endpoints
ADD COLUMN IF NOT EXISTS circuit_state TEXT DEFAULT 'closed' 
    CHECK (circuit_state IN ('closed', 'open', 'half_open')),
ADD COLUMN IF NOT EXISTS circuit_opened_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS circuit_failure_count INT DEFAULT 0;
```

### Step 3: Integrate into delivery worker

In `run_delivery_worker()`:
```rust
// Before delivery attempt
let circuit = get_circuit_breaker(&endpoint).await?;
circuit.try_deliver()?;

// After delivery
match result {
    Ok(_) => circuit.record_success().await,
    Err(_) => {
        circuit.record_failure().await;
        if circuit.is_open().await {
            metrics::record_circuit_breaker_opened(&endpoint);
        }
    }
}
```

### Step 4: Add slow endpoint detection

In `run_metrics_aggregator`:
```rust
pub async fn detect_slow_endpoints(pool: &PgPool) -> Result<()> {
    let slow = sqlx::query_as::<_, (String, f64)>(
        "SELECT endpoint_url, p99_latency_ms FROM webhook_endpoint_metrics
         WHERE period_start > NOW() - interval '1 hour'
         AND p99_latency_ms > 5000"
    )
    .fetch_all(pool)
    .await?;
    
    for (endpoint, latency) in slow {
        sqlx::query(
            "UPDATE rate_limit_endpoints SET health_status='degraded'
             WHERE endpoint_url=$1"
        )
        .bind(&endpoint)
        .execute(pool)
        .await?;
        
        tracing::warn!(
            endpoint=%endpoint,
            p99_latency_ms=%latency,
            "Endpoint showing high latency"
        );
    }
    
    Ok(())
}
```

---

## Phase 4: Customer Visibility

### Step 1: Create delivery status endpoint

Create `src/handlers/webhook_status.rs`:
```rust
#[derive(Debug, Serialize)]
pub struct DeliveryStatusResponse {
    pub subscription_id: Uuid,
    pub endpoint_url: String,
    pub status: String,              // active|paused|error
    pub pending_count: i64,
    pub success_rate_percent: f64,
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub last_delivery_at: Option<DateTime<Utc>>,
    pub recent_failures: Vec<RecentFailure>,
    pub slo_met: bool,
}

pub async fn get_delivery_status(
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
) -> Result<Json<DeliveryStatusResponse>, AppError> {
    // 1. Get subscription + endpoint
    // 2. Query delivery_queue for pending count
    // 3. Query webhook_endpoint_metrics for success rate + latencies
    // 4. Query webhook_failures for recent 5 failures
    // 5. Calculate SLO: success_rate >= 99.5%
}
```

### Step 2: Create webhook trace logging

```sql
CREATE TABLE IF NOT EXISTS webhook_trace_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    subscription_id UUID NOT NULL,
    delivery_attempt_num INT NOT NULL,
    http_status INT,
    latency_ms INT,
    error TEXT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhook_trace_subscription 
    ON webhook_trace_log(subscription_id, timestamp DESC);
CREATE INDEX idx_webhook_trace_event 
    ON webhook_trace_log(event_id);
```

Call after every delivery attempt:
```rust
async fn log_delivery_trace(
    pool: &PgPool,
    event_id: Uuid,
    subscription_id: Uuid,
    attempt: u32,
    status: Option<u16>,
    latency_ms: u64,
    error: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO webhook_trace_log 
         (event_id, subscription_id, delivery_attempt_num, http_status, latency_ms, error)
         VALUES ($1, $2, $3, $4, $5, $6)"
    )
    .bind(event_id)
    .bind(subscription_id)
    .bind(attempt as i32)
    .bind(status.map(|s| s as i32))
    .bind(latency_ms as i32)
    .bind(error)
    .execute(pool)
    .await
    .ok();
}
```

### Step 3: Add analytics endpoint

```rust
pub async fn get_subscription_analytics(
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
    Query(params): Query<AnalyticsParams>,
) -> Result<Json<SubscriptionAnalyticsResponse>, AppError> {
    let period_hours = params.hours.unwrap_or(24);
    let cutoff = chrono::Utc::now() - chrono::Duration::hours(period_hours as i64);
    
    // Query webhook_trace_log grouped by outcome
    // Calculate: delivered, failed, pending, rates, latencies
}
```

### Step 4: Implement webhook test endpoint

```rust
pub async fn test_webhook(
    State(state): State<AppState>,
    Json(req): Json<TestWebhookRequest>,
) -> Result<Json<TestWebhookResponse>, AppError> {
    // 1. Validate SSRF
    subscriptions::validate_callback_url(&req.callback_url, &state.config.environment)?;
    
    // 2. Build test payload
    let payload = req.payload.unwrap_or_else(|| {
        json!({
            "type": "test",
            "timestamp": chrono::Utc::now(),
            "message": "Webhook delivery test"
        })
    });
    
    // 3. Send request
    let start = Instant::now();
    let client = subscriptions::build_delivery_client();
    let body = serde_json::to_vec(&payload)?;
    
    let response = match client.post(&req.callback_url)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let body_text = resp.text().await.unwrap_or_default();
            (Some(status), Some(body_text))
        }
        Err(e) => (None, None),
    };
    
    let latency_ms = start.elapsed().as_millis() as u64;
    
    Ok(Json(TestWebhookResponse {
        success: response.0.map(|s| s < 400).unwrap_or(false),
        http_status: response.0,
        latency_ms,
        response_body: response.1,
        error: None,
    }))
}
```

### Step 5: Add delivery history cleanup

In main.rs, schedule cleanup task:
```rust
tokio::spawn({
    let pool = pool.clone();
    async move {
        loop {
            let retention_days: i32 = std::env::var("WEBHOOK_DELIVERY_HISTORY_DAYS")
                .unwrap_or_else(|_| "90".to_string())
                .parse()
                .unwrap_or(90);
            
            let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
            
            if let Err(e) = sqlx::query(
                "DELETE FROM webhook_trace_log WHERE timestamp < $1"
            )
            .bind(cutoff)
            .execute(&pool)
            .await
            {
                tracing::error!(error=%e, "Failed to clean webhook trace logs");
            }
            
            tokio::time::sleep(tokio::time::Duration::from_secs(86400)).await; // Daily
        }
    }
});
```

---

## Phase 5: Documentation

See `docs/` directory for:
- `webhook-delivery-runbook.md` — Operational procedures
- `webhook-endpoint-integration.md` — Customer integration guide
- `webhook-delivery-slo.md` — SLO definition & measurement
- `circuit-breaker-strategy.md` — Circuit breaker behavior

---

## Testing Strategy

### Unit Tests
- Metrics recording functions
- Failure categorization logic
- Circuit breaker state transitions
- Latency calculation

### Integration Tests
- Metrics aggregator with real DB
- Replay mechanism with filters
- Delivery status API responses
- Test webhook endpoint

### Performance Tests
- Metrics aggregator on 10k+ endpoints
- Replay query with large result sets
- Analytics query latency

### E2E Tests
- End-to-end delivery with circuit breaker
- Slow endpoint detection
- DLQ recovery workflow

---

## Deployment Checklist

- [ ] Database migrations tested in staging
- [ ] Metrics aggregator runs without errors
- [ ] No performance regression on delivery worker
- [ ] All new endpoints authenticated properly
- [ ] Documentation reviewed and published
- [ ] Team trained on new dashboards
- [ ] Monitoring alerts configured
- [ ] Rollback plan tested

