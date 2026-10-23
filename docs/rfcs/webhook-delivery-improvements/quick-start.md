# Webhook Delivery Improvements - Quick Start Guide for Developers

## Overview

This guide provides the fastest path to understanding and implementing the webhook improvements. Read this first, then dive into detailed specs as needed.

---

## 5-Minute Executive Context

**What:** Add monitoring, DLQ management, reliability, and customer visibility to webhooks
**Why:** Current system has blind spots; operators spend hours investigating failures
**How:** 5 independent tracks that build on each other
**When:** 15-day sprint (start this week)
**Who:** 1 engineer + QA

---

## Architecture at a Glance

```
Current State:
├─ webhook.rs ──→ delivery_queue ──→ ??? (no visibility)
└─ webhook_failures ──→ DLQ (stuck, no analysis)

New State:
├─ webhook.rs ──→ delivery_queue ──→ webhook_endpoint_metrics (1h aggregation)
│                                 └─→ webhook_trace_log (every delivery)
│                                 └─→ circuit_breaker (health tracking)
│
├─ webhook_failures ──→ dlq_analysis (auto-categorized)
│                   └─→ replay endpoint (filtered recovery)
│
└─ Operator Dashboard ──→ Per-customer API ──→ Customer self-service
```

---

## Implementation Phases (Execution Order)

### Phase 1: Metrics (Days 1-3) — Foundation Layer
**What you'll add:**
- Latency histogram recording in `src/metrics.rs` (5 functions)
- Hourly aggregation job in `src/workers/metrics_aggregator.rs` (new file)
- Database table: `webhook_endpoint_metrics`
- API endpoint: `GET /v1/admin/webhooks/metrics/{endpoint_url}`

**Why first:** Everything else depends on good metrics
**Effort:** Low (~2 days)
**Testing:** Unit + integration tests for percentile calculation

**Quick checklist:**
```
□ Add 5 metric recording functions to metrics.rs
□ Create run_metrics_aggregator() worker
□ Add SQL migration for webhook_endpoint_metrics
□ Wire aggregator into main.rs startup
□ Verify metrics flowing (check Prometheus endpoint)
```

### Phase 2: DLQ Management (Days 3-6) — Operator Control
**What you'll add:**
- Failure reason categorization enum + logic
- API endpoint: `POST /v1/admin/webhooks/dlq/replay`
- API endpoint: `GET /v1/admin/webhooks/dlq/stats`
- Database tables: `dlq_analysis`, `webhook_replay_audit`

**Why second:** Requires metrics baseline; enables quick manual fixes
**Effort:** Low (~2 days)
**Testing:** Replay filters, stats calculation

**Quick checklist:**
```
□ Create FailureReason enum in webhook.rs
□ Add categorization logic to delivery error handling
□ Create replay handler in src/handlers/webhook_replay.rs
□ Add DLQ stats endpoint
□ Test replay filters (endpoint, reason, date)
```

### Phase 3: Reliability (Days 6-10) — Automatic Protection
**What you'll add:**
- Circuit breaker module: `src/circuit_breaker.rs`
- Extend `rate_limit_endpoints` with circuit state
- Slow endpoint detection in metrics aggregator
- Database tables: `circuit_breaker_events`

**Why third:** Prevents cascading failures; enables SLO tracking
**Effort:** Medium (~3-4 days)
**Testing:** State transitions, recovery timing, concurrent access

**Quick checklist:**
```
□ Create circuit_breaker.rs module (Closed/Open/HalfOpen states)
□ Add circuit_state column to rate_limit_endpoints
□ Integrate CB into delivery worker
□ Add slow endpoint detection (p99 > 5s = degraded)
□ Calculate SLO: success_rate >= 99.5%
□ Test half-open recovery window
```

### Phase 4: Customer Visibility (Days 10-13) — Self-Service
**What you'll add:**
- API endpoint: `GET /v1/webhooks/{id}/status`
- API endpoint: `POST /v1/webhooks/test`
- API endpoint: `GET /v1/webhooks/{id}/analytics`
- Database table: `webhook_trace_log`
- Scheduled cleanup task for trace logs

**Why fourth:** Reduces support load; gives customers transparency
**Effort:** Low (~2-3 days)
**Testing:** API contract tests, analytics accuracy

**Quick checklist:**
```
□ Create delivery_status endpoint
□ Create test_webhook endpoint (validate SSRF)
□ Create analytics endpoint (query trace_log)
□ Add trace_log table + indexes
□ Schedule 90-day cleanup task
□ Verify response times < 500ms
```

### Phase 5: Documentation (Days 13-15) — Operational Excellence
**What you'll create:**
- `docs/webhook-delivery-runbook.md` (troubleshooting)
- `docs/webhook-endpoint-integration.md` (customer guide)
- `docs/webhook-delivery-slo.md` (SLO definition)
- `docs/circuit-breaker-strategy.md` (behavior guide)

**Why last:** Team needs code context first
**Effort:** Low (~2 days)
**Output:** Ready for customer communication

---

## File Structure Reference

New files you'll create:
```
src/
├─ workers/
│  └─ metrics_aggregator.rs       (run_metrics_aggregator)
├─ circuit_breaker.rs             (CircuitBreaker struct + state machine)
├─ handlers/
│  ├─ webhook_replay.rs           (DLQ replay + stats)
│  └─ webhook_status.rs           (delivery status + analytics + test)
└─ (modifications to existing files)

migrations/
└─ 20260728000001_webhook_delivery_improvements.sql

docs/
├─ webhook-delivery-runbook.md
├─ webhook-endpoint-integration.md
├─ webhook-delivery-slo.md
└─ circuit-breaker-strategy.md

tests/
├─ metrics_aggregator_integration_test.rs
├─ dlq_replay_integration_test.rs
├─ circuit_breaker_test.rs
├─ delivery_status_api_test.rs
└─ (many more...)
```

---

## Code Patterns to Know

### 1. Metrics Recording (Used Everywhere)
```rust
// In src/metrics.rs
pub fn record_webhook_delivery_latency(endpoint: &str, duration_ms: u64) {
    m::histogram!(
        "soroban_pulse_webhook_delivery_latency_ms",
        "endpoint" => endpoint.to_string(),
    ).record(duration_ms as f64);
}

// Usage in webhook.rs
let start = Instant::now();
let result = client.post(url).send().await;
metrics::record_webhook_delivery_latency(url, start.elapsed().as_millis() as u64);
```

### 2. Database Aggregation (Phase 1)
```rust
// In metrics_aggregator.rs
pub async fn run_metrics_aggregator_cycle(pool: &PgPool) -> Result<()> {
    // Query delivery_queue last hour grouped by endpoint
    // Calculate p50/p95/p99 using PostgreSQL percentile_cont()
    // Upsert into webhook_endpoint_metrics
    sqlx::query(
        "INSERT INTO webhook_endpoint_metrics (endpoint_url, period_start, ...)
         SELECT url, date_trunc('hour', NOW()), ...
         FROM delivery_queue ... ON CONFLICT (endpoint_url, period_start) DO UPDATE SET ..."
    ).execute(pool).await?;
    Ok(())
}
```

### 3. Circuit Breaker State Machine (Phase 3)
```rust
// In circuit_breaker.rs
impl CircuitBreaker {
    pub async fn try_deliver(&mut self) -> Result<()> {
        match self.state {
            Closed => Ok(()),           // Allow delivery
            Open => {
                if now > last_failure + recovery_timeout {
                    self.state = HalfOpen; // Try recovery
                    Ok(())
                } else {
                    Err(CircuitOpen)     // Still blocked
                }
            }
            HalfOpen => Ok(()),         // Allow test request
        }
    }
    
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        if self.failure_count >= THRESHOLD {
            self.state = Open;
        }
    }
}
```

### 4. API Response Pattern (Phase 4)
```rust
#[derive(Serialize)]
pub struct DeliveryStatusResponse {
    pub subscription_id: Uuid,
    pub status: String,
    pub pending_count: i64,
    pub success_rate_percent: f64,
    pub slo_met: bool,
}

pub async fn get_delivery_status(
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
) -> Result<Json<DeliveryStatusResponse>, AppError> {
    // 1. Get subscription
    // 2. Query metrics
    // 3. Calculate SLO
    // 4. Return response
}
```

---

## Testing Checklist for Each Phase

### Phase 1: Metrics
```
□ Histogram records latencies correctly
□ Percentiles match expected values
□ Health status updates based on success rate
□ No performance regression on aggregator
□ Handles zero data gracefully
```

### Phase 2: DLQ
```
□ Failure categorization identifies all error types
□ Replay respects all filters (endpoint, reason, date)
□ Replayed count accurate
□ DLQ stats calculation correct
□ Audit trail recorded
```

### Phase 3: Reliability
```
□ Circuit breaker opens after 5 failures
□ Half-open allows recovery test
□ Slow endpoints marked degraded
□ SLO calculation accurate
□ No race conditions under load
```

### Phase 4: Visibility
```
□ Delivery status returns all fields
□ Test webhook validates connectivity
□ SSRF prevention works
□ Analytics query < 500ms
□ Cleanup task runs daily
```

### Phase 5: Documentation
```
□ Runbook covers common failures
□ Integration guide has examples
□ SLO definition is clear
□ Circuit breaker behavior explained
```

---

## Critical Decisions You'll Make

### Decision 1: SLO Target
**Options:** 99.5% vs 99.9%
**Recommendation:** Start with 99.5% (achievable, transparent)
**Impact:** Affects alert thresholds, customer SLA

### Decision 2: Circuit Breaker Threshold
**Options:** 5, 10, or 20 consecutive failures before opening?
**Recommendation:** 5 (fast detection, balances noise)
**Impact:** Affects how quickly slow endpoints are isolated

### Decision 3: Recovery Window
**Options:** 30s, 60s, or 2min before half-open test?
**Recommendation:** 60s (balances recovery speed vs not thrashing)
**Impact:** Affects recovery time in incident scenarios

### Decision 4: Trace Log Retention
**Options:** 30, 90, or 180 days?
**Recommendation:** 90 days (balances storage vs historical analysis)
**Impact:** Storage cost ~5MB per 1k endpoints

### Decision 5: Replay Approval
**Options:** Automatic vs manual approval for replays > 1000?
**Recommendation:** Manual approval >1000 (prevents accidents)
**Impact:** Recovery time +5min for large replays

---

## Integration Points (Where to Hook In)

### In `src/webhook.rs` (delivery logic)
```rust
// After delivery attempt
let start = Instant::now();
let result = retry_policy.execute_with_retry(|attempt| { ... }).await;
let latency_ms = start.elapsed().as_millis() as u64;

// NEW: Record metric
metrics::record_webhook_delivery_latency(&url, latency_ms as u64);

// NEW: Log trace
log_delivery_trace(&pool, event_id, subscription_id, attempt, status, latency_ms, error).await;

// NEW: Categorize and record failure
if let Err(ref e) = result {
    categorize_and_store_failure(&pool, &url, &format!("{:?}", e)).await;
}
```

### In `src/workers/delivery.rs` (worker loop)
```rust
// NEW: Before delivery attempt
let circuit = get_circuit_breaker(&endpoint).await?;
circuit.try_deliver().await?;

// ... attempt delivery ...

// NEW: After delivery
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

### In `src/main.rs` (startup)
```rust
// NEW: Add workers
tokio::spawn(run_metrics_aggregator(pool.clone()));

// NEW: Add cleanup task
tokio::spawn(run_trace_log_cleanup(pool.clone(), 90));
```

### In `src/routes.rs` (add new endpoints)
```rust
// NEW: Admin routes
.route("/v1/admin/webhooks/metrics/:endpoint", get(...))
.route("/v1/admin/webhooks/dlq/replay", post(...))
.route("/v1/admin/webhooks/dlq/stats", get(...))

// NEW: Customer routes
.route("/v1/webhooks/:id/status", get(...))
.route("/v1/webhooks/:id/analytics", get(...))
.route("/v1/webhooks/test", post(...))
```

---

## Common Pitfalls & How to Avoid Them

### Pitfall 1: Metric Cardinality Explosion
**Problem:** Every unique endpoint = new metric series
**Solution:** Aggregate by domain, not full URL; limit labels
**Prevention:** Add cardinality check in aggregator

### Pitfall 2: DLQ Replay Floods Slow Endpoints
**Problem:** Replaying 10k items to slow endpoint crashes it
**Solution:** Rate limit replays; require approval >1000
**Prevention:** Add safeguards in replay handler

### Pitfall 3: Circuit Breaker Ping-Ponging
**Problem:** Flaky endpoint opens/closes repeatedly
**Solution:** Minimum open duration (60s); half-open test reduces noise
**Prevention:** Add metrics to observe state transitions

### Pitfall 4: Analytics Query Timeout
**Problem:** Querying 1M trace logs is slow
**Solution:** Index by (subscription_id, timestamp); use materialized view
**Prevention:** Test with 1M rows during development

### Pitfall 5: Storage Growth
**Problem:** Trace logs fill up disk
**Solution:** Automatic cleanup job; configurable retention
**Prevention:** Schedule cleanup before launch

---

## Validation Before Shipping

### Code Review Checklist
```
□ All new functions have unit tests
□ All API endpoints authenticated
□ SSRF validation on user input
□ SQL injection prevention (parameterized queries)
□ No hardcoded secrets
□ Error handling complete
□ Logging at appropriate levels
```

### Performance Checklist
```
□ Aggregator < 500ms on 10k endpoints
□ API endpoints < 100ms p99
□ DLQ replay < 10s for 10k items
□ No memory leaks under load
```

### Operations Checklist
```
□ Runbook complete + ops team trained
□ Monitoring alerts configured
□ Dashboards ready
□ Customer communication drafted
□ Rollback plan documented
```

---

## Getting Help

### For Architecture Questions
→ Read: [`spec.md`](spec.md)

### For Implementation Details
→ Read: [`IMPLEMENTATION_GUIDE.md`](../../archive/IMPLEMENTATION_GUIDE.md) (archived)

### For Testing Strategy
→ Read: [`testing-and-validation.md`](testing-and-validation.md)

### For Database Schema
→ Read: [`proposed-migration.sql`](proposed-migration.sql)

### For Business Context
→ Read: [`executive-summary.md`](executive-summary.md)

---

## Success Looks Like

After 15 days:

✅ Operator can see any endpoint health in < 1 second
✅ Failed webhooks replay in < 5 minutes (no database queries)
✅ Slow endpoints isolated automatically (circuit breaker)
✅ Customer can check delivery status via API
✅ SLO >= 99.5% demonstrated with metrics
✅ MTTR improved by 50%+
✅ Zero customer-impacting incidents from webhooks

---

**Now dive into the full specs. Good luck! 🚀**

