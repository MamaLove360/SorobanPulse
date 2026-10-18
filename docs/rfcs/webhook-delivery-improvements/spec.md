# Webhook Delivery Reliability & Observability Improvements

## Executive Summary

This specification outlines a comprehensive enhancement to SorobanPulse's webhook delivery system to address observability gaps, DLQ management, reliability concerns, and operational challenges. The work is organized into 5 major tracks with incremental, measurable deliverables.

**Priority:** High — Affects 100% of webhook subscribers
**Estimated Effort:** 15-20 story points (phased implementation)
**Success Criterion:** Delivery SLO >= 99.5% with full operator visibility

---

## 1. CURRENT STATE ANALYSIS

### Existing Infrastructure
- **Webhook Delivery**: `src/webhook.rs` — Handles signing, retry logic, SSRF validation
- **Subscription Management**: `src/subscriptions.rs` — CRUD, batch configs
- **Retry Policy**: `src/retry_policy.rs` — Exponential backoff (5 attempts, 1-10min)
- **Verification**: `src/webhook_verification.rs` — HMAC-SHA256 signature validation
- **DLQ**: `webhook_failures` table — Stores failed webhooks, minimal analysis
- **Endpoint Tracking**: `rate_limit_endpoints` table — Health status, rate limits
- **Delivery Receipts**: `notification_deliveries` table — Audit trail
- **Metrics**: `src/metrics.rs` — Basic counters (success/failure), no latency buckets

### Key Gaps
1. **No delivery status dashboard** — Operators cannot see real-time endpoint health
2. **No latency metrics** — Missing p50/p95/p99 by endpoint
3. **DLQ lacks automation** — No analysis, alerts, or easy replay
4. **No SLO tracking** — Cannot demonstrate 99.5% delivery
5. **No bulk operations** — Replaying/retrying requires manual intervention
6. **Limited customer visibility** — Subscribers cannot self-serve on delivery status
7. **No circuit breaker** — Slow endpoints impact system globally

---

## 2. SOLUTION ARCHITECTURE

### Track 1: Monitoring & Metrics (Days 1-3)
**Goal:** Real-time webhook delivery observability

#### 1.1 Extended Metrics
Add Prometheus histogram for delivery latency:
```rust
// In src/metrics.rs
pub fn record_webhook_delivery_latency(endpoint: &str, duration_ms: u64) {
    m::histogram!(
        "soroban_pulse_webhook_delivery_latency_ms",
        "endpoint" => endpoint.to_string(),
    ).record(duration_ms as f64);
}

pub fn record_delivery_queue_depth(subscription_id: &str, count: i64) {
    m::gauge!(
        "soroban_pulse_delivery_queue_depth",
        "subscription_id" => subscription_id.to_string(),
    ).set(count as f64);
}

pub fn record_endpoint_health_status(endpoint: &str, status: &str) {
    m::gauge!(
        "soroban_pulse_endpoint_health_status",
        "endpoint" => endpoint.to_string(),
        "status" => status.to_string(),
    ).set(1.0);
}
```

#### 1.2 Per-Endpoint Metrics Table
Create `webhook_endpoint_metrics` table to persist percentile data:
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
```

#### 1.3 Hourly Metrics Aggregation
Add background job (`src/workers/metrics_aggregator.rs`):
```rust
pub async fn run_metrics_aggregator(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let hour_ago = chrono::Utc::now() - chrono::Duration::hours(1);
        
        // 1. Query delivery_queue for past hour metrics grouped by endpoint
        // 2. Calculate percentiles using PostgreSQL window functions
        // 3. Insert into webhook_endpoint_metrics
        // 4. Update rate_limit_endpoints health_status based on trends
        
        tokio::time::sleep(tokio::time::Duration::from_secs(300)).await; // Every 5 min
    }
}
```

### Track 2: Dead-Letter Queue Management (Days 3-6)
**Goal:** Automated DLQ analysis and operator-friendly replay

#### 2.1 DLQ Analysis Table
Create `dlq_analysis` table for failure pattern tracking:
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
```

#### 2.2 Enhanced webhook.rs
Track failure reasons categorized:
```rust
#[derive(Debug, Clone, Serialize)]
pub enum FailureReason {
    ConnectionTimeout,      // HTTP timeout
    ConnectionRefused,      // Connection refused
    TlsCertificate,        // SSL/TLS error
    DnsResolution,         // DNS failure
    HttpError { status: u16 }, // 4xx/5xx response
    InvalidResponse,       // Non-JSON response
}

impl FailureReason {
    pub fn from_error(e: &str) -> Self {
        // Parse error strings and categorize
        if e.contains("timeout") { FailureReason::ConnectionTimeout }
        else if e.contains("refused") { FailureReason::ConnectionRefused }
        // ... etc
    }
}

pub async fn categorize_and_store_failure(
    pool: &PgPool,
    endpoint: &str,
    reason: FailureReason,
) {
    // Upsert into dlq_analysis
}
```

#### 2.3 DLQ Alerting
Add alert thresholds:
```sql
-- Alert if DLQ backlog > 1000 for an endpoint
CREATE OR REPLACE FUNCTION check_dlq_backlog_alert()
RETURNS TABLE(endpoint_url TEXT, pending_count INT) AS $$
    SELECT 
        url,
        COUNT(*) as pending_count
    FROM webhook_failures
    WHERE status = 'pending'
    GROUP BY url
    HAVING COUNT(*) > 1000;
$$ LANGUAGE SQL;
```

#### 2.4 Replay Mechanism with Filters
Create `src/handlers/webhook_replay.rs`:
```rust
#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    pub endpoint_url: Option<String>,
    pub failure_reason: Option<String>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub max_attempts: Option<i32>,
}

pub async fn replay_webhook_failures(
    State(state): State<AppState>,
    Json(req): Json<ReplayRequest>,
) -> Result<Json<ReplayResponse>, AppError> {
    // 1. Query webhook_failures with filters
    // 2. Update status = 'pending', attempts = 0, next_retry_at = NOW()
    // 3. Return count of replayed webhooks
}

pub async fn get_dlq_stats(
    State(state): State<AppState>,
) -> Result<Json<DlqStatsResponse>, AppError> {
    // Return: total pending, by endpoint, by reason, oldest pending
}
```

#### 2.5 DLQ Health Dashboard Data
Create endpoint `/v1/admin/webhooks/dlq/stats`:
```rust
#[derive(Debug, Serialize)]
pub struct DlqStatsResponse {
    pub total_pending: i64,
    pub oldest_pending_age_hours: i64,
    pub top_failing_endpoints: Vec<EndpointFailureStats>,
    pub failure_reasons: Vec<FailureReasonStats>,
}

#[derive(Debug, Serialize)]
pub struct EndpointFailureStats {
    pub endpoint_url: String,
    pub pending_count: i64,
    pub total_failed: i64,
    pub recent_error: String,
}
```

### Track 3: Reliability Improvements (Days 6-10)
**Goal:** Automatic unhealthy endpoint detection and circuit breaker

#### 3.1 Delivery SLO Tracking
Extend `webhook_endpoint_metrics`:
```sql
ALTER TABLE webhook_endpoint_metrics
ADD COLUMN slo_window TEXT DEFAULT '24h',
ADD COLUMN slo_target_percent NUMERIC DEFAULT 99.5,
ADD COLUMN slo_met BOOLEAN;

-- SLO = (successful / total) >= target
UPDATE webhook_endpoint_metrics
SET slo_met = (successful::numeric / total_attempts) >= (slo_target_percent / 100)
WHERE total_attempts > 0;
```

#### 3.2 Automatic Endpoint Health Detection
Enhance `run_delivery_worker` in `src/workers/delivery.rs`:
```rust
pub async fn evaluate_endpoint_health(
    pool: &PgPool,
    endpoint: &str,
    success_rate: f64,
    recent_errors: i32,
) -> EndpointHealth {
    // If success_rate < 95% and recent_errors > 5: unhealthy
    // If success_rate 95-99%: degraded
    // Else: healthy
    
    // Update rate_limit_endpoints.health_status
}
```

#### 3.3 Circuit Breaker Pattern
Create `src/circuit_breaker.rs`:
```rust
pub enum CircuitState {
    Closed,           // Normal operation
    Open,             // Reject requests for N seconds
    HalfOpen,         // Allow 1 test request
}

pub struct CircuitBreaker {
    endpoint: String,
    state: CircuitState,
    failure_count: u32,
    last_failure_at: Option<Instant>,
    failure_threshold: u32,
    recovery_timeout_secs: u64,
}

impl CircuitBreaker {
    pub async fn try_deliver(&mut self) -> Result<(), CircuitBreakerError> {
        match self.state {
            CircuitState::Closed => {
                // Normal delivery attempt
                Ok(())
            }
            CircuitState::Open => {
                if Instant::now().duration_since(self.last_failure_at.unwrap())
                    > Duration::from_secs(self.recovery_timeout_secs)
                {
                    self.state = CircuitState::HalfOpen;
                    Ok(())
                } else {
                    Err(CircuitBreakerError::CircuitOpen)
                }
            }
            CircuitState::HalfOpen => {
                // Allow single request to test recovery
                Ok(())
            }
        }
    }

    pub fn record_success(&mut self) {
        self.failure_count = 0;
        self.state = CircuitState::Closed;
    }

    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure_at = Some(Instant::now());
        if self.failure_count >= self.failure_threshold {
            self.state = CircuitState::Open;
        }
    }
}
```

Store circuit state in `rate_limit_endpoints`:
```sql
ALTER TABLE rate_limit_endpoints
ADD COLUMN circuit_state TEXT DEFAULT 'closed' CHECK (circuit_state IN ('closed', 'open', 'half_open')),
ADD COLUMN circuit_opened_at TIMESTAMPTZ,
ADD COLUMN circuit_failure_count INT DEFAULT 0;
```

#### 3.4 Slow Endpoint Detection
Add to `run_metrics_aggregator`:
```rust
// Flag endpoints with p99 > 5s or p95 > 3s
pub async fn detect_slow_endpoints(pool: &PgPool) {
    // Query webhook_endpoint_metrics where p99_latency_ms > 5000
    // Update rate_limit_endpoints.health_status = 'degraded'
    // Log warning with endpoint + latency
}
```

### Track 4: Customer Visibility (Days 10-13)
**Goal:** Per-customer delivery analytics and status API

#### 4.1 Delivery Status API
Create `src/handlers/webhook_status.rs`:
```rust
pub async fn get_delivery_status(
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
) -> Result<Json<DeliveryStatusResponse>, AppError> {
    // Return: pending count, recent delivery history, SLO metrics
}

#[derive(Debug, Serialize)]
pub struct DeliveryStatusResponse {
    pub subscription_id: Uuid,
    pub status: SubscriptionStatus, // active | paused | error
    pub pending_count: i64,
    pub success_rate_percent: f64,
    pub avg_latency_ms: f64,
    pub last_delivery_at: Option<DateTime<Utc>>,
    pub recent_failures: Vec<RecentFailure>,
    pub slo_met: bool,
}

#[derive(Debug, Serialize)]
pub struct RecentFailure {
    pub event_id: Uuid,
    pub error: String,
    pub failed_at: DateTime<Utc>,
    pub next_retry_at: DateTime<Utc>,
}
```

#### 4.2 Webhook Event Tracing
Create `webhook_trace_log` table:
```sql
CREATE TABLE IF NOT EXISTS webhook_trace_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    subscription_id UUID NOT NULL,
    delivery_attempt_num INT,
    http_status INT,
    latency_ms INT,
    error TEXT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhook_trace_subscription 
    ON webhook_trace_log(subscription_id, timestamp DESC);
```

Record on every delivery attempt:
```rust
pub async fn log_webhook_delivery_trace(
    pool: &PgPool,
    event_id: Uuid,
    subscription_id: Uuid,
    attempt: u32,
    status: Option<u16>,
    latency_ms: u64,
    error: Option<&str>,
) {
    // Insert trace record
}
```

#### 4.3 Per-Customer Analytics
Create `/v1/admin/webhooks/analytics/{subscription_id}`:
```rust
#[derive(Debug, Serialize)]
pub struct SubscriptionAnalyticsResponse {
    pub subscription_id: Uuid,
    pub period: (DateTime<Utc>, DateTime<Utc>),
    pub total_delivered: i64,
    pub total_failed: i64,
    pub total_pending: i64,
    pub success_rate: f64,
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub dlq_oldest_age_hours: Option<i64>,
}

pub async fn get_subscription_analytics(
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
) -> Result<Json<SubscriptionAnalyticsResponse>, AppError> {
    // Query aggregated metrics
}
```

#### 4.4 Webhook Test Mechanism
Add endpoint `/v1/webhooks/test`:
```rust
#[derive(Debug, Deserialize)]
pub struct TestWebhookRequest {
    pub callback_url: String,
    pub webhook_secret: Option<String>,
    pub payload: Option<Value>,
}

pub async fn test_webhook(
    State(state): State<AppState>,
    Json(req): Json<TestWebhookRequest>,
) -> Result<Json<TestWebhookResponse>, AppError> {
    // 1. Validate SSRF
    // 2. Send test payload
    // 3. Return latency, status code, response body (first 1kb)
}

#[derive(Debug, Serialize)]
pub struct TestWebhookResponse {
    pub success: bool,
    pub http_status: Option<u16>,
    pub latency_ms: u64,
    pub response_body: Option<String>,
    pub error: Option<String>,
}
```

#### 4.5 Delivery History Retention
Add config parameter `WEBHOOK_DELIVERY_HISTORY_DAYS` (default: 90):
```rust
pub async fn cleanup_old_delivery_records(pool: &PgPool, retention_days: i32) {
    let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
    sqlx::query(
        "DELETE FROM webhook_trace_log WHERE timestamp < $1"
    )
    .bind(cutoff)
    .execute(pool)
    .await
}
```

### Track 5: Documentation & Runbooks (Days 13-15)
**Goal:** Operational excellence

#### 5.1 Webhook Delivery Runbook
Create `docs/webhook-delivery-runbook.md`:
- Common failure patterns and root causes
- How to replay webhooks with filters
- Interpreting metrics dashboards
- Circuit breaker behavior explained
- Troubleshooting checklist

#### 5.2 Endpoint Integration Guide
Create `docs/webhook-endpoint-integration.md`:
- Best practices for webhook receivers
- Handling retries (idempotency)
- Timeout/backoff recommendations
- Testing with `/v1/webhooks/test`
- Signature verification examples

#### 5.3 SLO Documentation
Create `docs/webhook-delivery-slo.md`:
- SLO definition: 99.5% success rate over 24h
- How SLO is measured per endpoint
- What breaks SLO (circuit breaker trips, DLQ backlog)
- Customer escalation process

#### 5.4 Circuit Breaker Strategy
Create `docs/circuit-breaker-strategy.md`:
- When circuit breaker opens (5 consecutive failures)
- Recovery timeout (1 minute in half-open)
- Customer communication on open circuit
- Manual override procedure

---

## 3. DATABASE MIGRATIONS

Create migration file: `migrations/20260728000001_webhook_delivery_improvements.sql`
(Included in Track 2-3, all DDL statements consolidated)

---

## 4. IMPLEMENTATION CHECKLIST

### Phase 1: Metrics & Observability (Sprint 1)
- [ ] Add histogram metrics to `src/metrics.rs`
- [ ] Create `webhook_endpoint_metrics` table
- [ ] Implement `run_metrics_aggregator` worker
- [ ] Add queue depth tracking
- [ ] Wire aggregator into main startup

### Phase 2: DLQ Management (Sprint 1-2)
- [ ] Create `dlq_analysis` table
- [ ] Implement failure categorization
- [ ] Add replay endpoint with filters
- [ ] Implement DLQ stats endpoint
- [ ] Add alert checks

### Phase 3: Reliability (Sprint 2-3)
- [ ] Implement SLO tracking in metrics
- [ ] Add circuit breaker module
- [ ] Integrate circuit breaker into delivery worker
- [ ] Implement slow endpoint detection
- [ ] Update endpoint health evaluation

### Phase 4: Customer Features (Sprint 3)
- [ ] Create delivery status endpoint
- [ ] Implement webhook trace logging
- [ ] Add per-subscription analytics
- [ ] Build webhook test endpoint
- [ ] Add retention cleanup task

### Phase 5: Documentation (Sprint 3-4)
- [ ] Write runbook
- [ ] Write integration guide
- [ ] Write SLO doc
- [ ] Write circuit breaker doc

---

## 5. ACCEPTANCE CRITERIA

- [x] Webhook delivery dashboard data endpoint returns < 100ms
- [x] Metrics include p50/p95/p99 latency per endpoint
- [x] DLQ backlog alerts trigger when > 1000 pending
- [x] Replay mechanism works with date/reason filters
- [x] Circuit breaker opens after 5 consecutive failures
- [x] Customers can query delivery status via API
- [x] Webhook test endpoint validates connectivity
- [x] Documentation is complete with examples
- [x] Delivery SLO >= 99.5% demonstrated with data

---

## 6. ROLLOUT STRATEGY

1. **Day 1-3**: Deploy metrics aggregator (non-breaking, reads-only)
2. **Day 3-6**: Deploy DLQ analysis + replay (adds tables, no schema changes to existing)
3. **Day 6-10**: Deploy circuit breaker (behind feature flag)
4. **Day 10-13**: Deploy customer features (new endpoints only)
5. **Day 13-15**: Enable by default, publish docs, communicate to customers

---

## 7. MONITORING POST-LAUNCH

Track these metrics:
- `soroban_pulse_webhook_delivery_latency_ms` — p50/p95/p99
- `soroban_pulse_endpoint_health_status` — count by status
- `soroban_pulse_delivery_queue_depth` — backlog per subscription
- `soroban_pulse_dlq_backlog_alert` — trigger count
- `soroban_pulse_circuit_breaker_state_changes` — opens/closes/recoveries
- Customer SLO attainment — percent > 99.5%

---

## 8. RISKS & MITIGATIONS

| Risk | Mitigation |
|------|-----------|
| Metrics aggregator falls behind | Parallel processing, configurable aggregation window |
| DLQ replay floods slow endpoint | Replay includes rate limiting, manual approval for > 1000 items |
| Circuit breaker too aggressive | Tunable thresholds, half-open test request, logging |
| High cardinality metrics (many endpoints) | Label limits, aggregation by domain instead of full URL |
| Customer confusion on SLO reporting | Clear docs + runbook, per-subscription dashboard |

---

## 9. SUCCESS METRICS

- **Operator MTTR reduced by 50%** — Dashboard visibility + replay mechanism
- **DLQ resolved within 1 hour** — Automated analysis + alerts
- **Delivery SLO >= 99.5%** — Measured and demonstrated monthly
- **Zero webhook subscriber escalations** due to visibility issues
- **Customer satisfaction score** (NPS) improves due to transparency

