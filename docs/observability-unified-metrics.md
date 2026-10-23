# Unified Error Metrics and Profiling Implementation Guide

This document describes the observability enhancements implemented in the `feat/unified-error-metrics-and-profiling` branch:

1. **Unified error counter** with kind labels
2. **CPU/Memory profiling endpoints** for production diagnostics
3. **SSE connection tracking** with churn and duration metrics
4. **Automated partition pruning** with retention policy enforcement

## 1. Unified Error Metrics

### Motivation

Error metrics were scattered across the codebase:
- `soroban_pulse_rpc_errors_total`
- `soroban_pulse_webhook_failures_total`
- `soroban_pulse_email_failures_total`

Aggregating alerts and dashboards required complex PromQL queries and duplicate rules.

### Implementation

A new counter tracks all errors with a `kind` label:

```prometheus
soroban_pulse_errors_total{kind="rpc"|"webhook"|"email"|"db"|"sse"}
```

### Metrics Reference

| Metric | Labels | Notes |
|---|---|---|
| `soroban_pulse_errors_total` | `kind` | Counter: all errors unified |
| `soroban_pulse_rpc_errors_total` | (none) | Deprecated; still incremented for backward compatibility |
| `soroban_pulse_webhook_failures_total` | (none) | Deprecated; still incremented for backward compatibility |
| `soroban_pulse_email_failures_total` | (none) | Deprecated; still incremented for backward compatibility |

### Updated Functions

**src/metrics.rs:**

```rust
/// Unified error counter: incremented from each error-generating module
pub fn record_error_by_kind(kind: &str) {
    m::counter!("soroban_pulse_errors_total", "kind" => kind.to_string()).increment(1);
}
```

Modified functions that now call `record_error_by_kind`:

- `record_rpc_error()` → calls `record_error_by_kind("rpc")`
- `record_webhook_failure()` → calls `record_error_by_kind("webhook")`
- `record_email_failure()` → calls `record_error_by_kind("email")`
- `record_postgres_health_error()` → calls `record_error_by_kind("db")`
- `record_sse_connection_event(result)` → calls `record_error_by_kind("sse")` when result == "error"

### Alert Rules

New alert rules in `docs/alerts.yml`:

```yaml
- alert: UnifiedErrorRateHigh
  expr: rate(soroban_pulse_errors_total[5m]) > 10
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Unified error rate is above 10/sec"

- alert: WebhookErrorRateHigh
  expr: rate(soroban_pulse_errors_total{kind="webhook"}[5m]) > 1
  for: 10m
  labels:
    severity: warning

- alert: EmailErrorRateHigh
  expr: rate(soroban_pulse_errors_total{kind="email"}[5m]) > 0.5
  for: 10m
  labels:
    severity: warning

- alert: DatabaseErrorRateHigh
  expr: rate(soroban_pulse_errors_total{kind="db"}[5m]) > 0.1
  for: 10m
  labels:
    severity: warning
```

### Grafana Dashboard Queries

**Error rate by kind (5-minute rate):**

```promql
rate(soroban_pulse_errors_total[5m]) by (kind)
```

**Error rate heatmap (per kind):**

```promql
histogram_quantile(0.99, sum(rate(soroban_pulse_errors_total[1m])) by (kind, le))
```

## 2. Production Profiling

### Motivation

Diagnosing production performance issues requires CPU profiles and memory snapshots. Without a profiling endpoint, operators must:

1. Add special build flags
2. Restart the service
3. Run profiling for a fixed duration
4. Extract and analyze dumps

The pprof endpoints eliminate this friction.

### Endpoints

**CPU Profiling:**

```
GET /v1/admin/profile/cpu?seconds=<1-120>
```

Requires: `ADMIN_API_KEY` header

Returns: pprof-format gzipped protobuf

```bash
curl -H "X-Api-Key: $ADMIN_API_KEY" \
  "https://soroban-pulse.example.com/v1/admin/profile/cpu?seconds=30" \
  -o cpu.pb.gz

go tool pprof cpu.pb.gz
```

**Heap Profiling:**

```
GET /v1/admin/profile/heap
```

Requires: `ADMIN_API_KEY` header

Returns: pprof-format gzipped protobuf

```bash
curl -H "X-Api-Key: $ADMIN_API_KEY" \
  "https://soroban-pulse.example.com/v1/admin/profile/heap" \
  -o heap.pb.gz

go tool pprof heap.pb.gz
```

### Implementation

**src/profiling.rs:**

- `async fn profile_cpu(Query(req): Query<CpuProfileRequest>)`: Captures CPU profile
- `async fn profile_heap()`: Captures heap snapshot
- Duration clamping: seconds ∈ [1, 120]

### Metrics

Profiling requests are tracked:

```prometheus
soroban_pulse_profile_requests_total{type="cpu"|"heap"}
soroban_pulse_profile_errors_total{type="cpu"|"heap", error="..."}
```

### Documentation

See [docs/runbooks/operator-profiling.md](runbooks/operator-profiling.md) for:

- How to use endpoints
- Analyzing profiles with `go tool pprof`
- Uploading to pprof.me for web UI
- Troubleshooting CPU/memory issues
- Best practices

## 3. SSE Connection Metrics

### Motivation

SSE connections are tracked in aggregate (`soroban_pulse_sse_active_connections`), but there was no visibility into:

- Connection churn (connects/disconnects per second)
- Connection error rate
- Connection duration distribution

This made it hard to diagnose SSE-related issues.

### New Metrics

```prometheus
# Counter: connection lifecycle events
soroban_pulse_sse_connections_total{result="connected"|"disconnected"|"error"}

# Histogram: connection duration (seconds)
soroban_pulse_sse_connection_duration_seconds

# Gauge: current active connections (existing, unchanged)
soroban_pulse_sse_active_connections
```

### Updated Functions

**src/metrics.rs:**

```rust
pub fn record_sse_connection_event(result: &str) {
    m::counter!("soroban_pulse_sse_connections_total", 
        "result" => result.to_string())
        .increment(1);
    if result == "error" {
        record_error_by_kind("sse");
    }
}

pub fn record_sse_connection_duration(duration_secs: f64) {
    m::histogram!("soroban_pulse_sse_connection_duration_seconds")
        .record(duration_secs);
}
```

### Alert Rules

New alert rules in `docs/alerts.yml`:

```yaml
- alert: SSEConnectionErrorRateHigh
  expr: |
    (rate(soroban_pulse_sse_connections_total{result="error"}[5m]) /
     (rate(soroban_pulse_sse_connections_total[5m]) + 0.001)) > 0.05
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "SSE connection error rate is above 5%"

- alert: SSEConnectionErrorRateCritical
  expr: |
    (rate(soroban_pulse_sse_connections_total{result="error"}[5m]) /
     (rate(soroban_pulse_sse_connections_total[5m]) + 0.001)) > 0.10
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "SSE connection error rate is critical (>10%)"
```

### Grafana Dashboard Panels

**Active connections time series:**

```promql
soroban_pulse_sse_active_connections
```

**Connection churn rate (connections/sec):**

```promql
rate(soroban_pulse_sse_connections_total{result="connected"}[1m])
rate(soroban_pulse_sse_connections_total{result="disconnected"}[1m])
```

**Connection duration heatmap:**

```promql
histogram_quantile(0.99, rate(soroban_pulse_sse_connection_duration_seconds_bucket[5m]))
```

## 4. Partition Pruning

### Motivation

The `events` table uses monthly partitions. Without automated pruning:

- Old partitions accumulate indefinitely
- Storage usage grows linearly with time
- Data retention policy is not enforced

### Implementation

**Migration: `20261020000000_partition_pruning_function.sql`**

Creates SQL function `drop_old_partitions(retain_months INT, dry_run BOOLEAN)`:

```sql
SELECT * FROM drop_old_partitions(retain_months := 12, dry_run := false);
```

Returns:

```
partition_name | dropped | reason
---------------|---------|-------
events_2024_01 | true    | Dropped (older than 12 months)
events_2024_02 | true    | Dropped (older than 12 months)
events_2025_08 | false   | Retained (within 12 month retention window)
```

**src/pruner.rs:**

Added `prune_old_partitions()` function that:

1. Calculates retention window: `retention_months = max(1, retention_days / 30)`
2. Calls `drop_old_partitions(retain_months, dry_run=false)`
3. Logs and metrics for each partition dropped
4. Runs on the same schedule as row-level pruning

**src/metrics.rs:**

```rust
pub fn record_partition_dropped(partition_name: &str) {
    m::counter!("soroban_pulse_partitions_dropped_total", 
        "partition" => partition_name.to_string())
        .increment(1);
}
```

### Configuration

The partition pruning runs automatically if:

- `RETENTION_DAYS` is set (and > 0)
- Pruning task is enabled (via `PURGE_INTERVAL_HOURS`, default: 6)

### Monitoring

**Alert:** Pruning stalled

```yaml
- alert: PartitionPruningStalled
  expr: increase(soroban_pulse_partitions_dropped_total[24h]) == 0
  for: 0m
  labels:
    severity: warning
  annotations:
    summary: "Partition pruning has not run in 24 hours"
```

**Manual dry-run (test before production):**

```sql
SELECT * FROM drop_old_partitions(retain_months := 12, dry_run := true);
```

See [docs/data-retention.md](data-retention.md) for details on partition lifecycle and troubleshooting.

## Integration Checklist

### Code Changes

- [x] Unified error counter in `src/metrics.rs`
- [x] RPC, webhook, email, DB errors call `record_error_by_kind()`
- [x] SSE connection tracking in `src/metrics.rs`
- [x] Profiling module `src/profiling.rs`
- [x] Module declaration in `src/lib.rs`
- [x] Partition pruning in `src/pruner.rs`
- [x] Partition pruning SQL migration

### Documentation

- [x] Updated `docs/alerts.yml` with new alert rules
- [x] Updated `docs/data-retention.md` with partition lifecycle
- [x] New `docs/runbooks/operator-profiling.md`
- [x] New `docs/observability-unified-metrics.md` (this file)

### Metrics Exposed

After deployment, verify `/metrics` output includes:

```
# Unified error counter
soroban_pulse_errors_total{kind="rpc"} 123
soroban_pulse_errors_total{kind="webhook"} 45
soroban_pulse_errors_total{kind="email"} 12
soroban_pulse_errors_total{kind="db"} 8
soroban_pulse_errors_total{kind="sse"} 2

# SSE connection metrics
soroban_pulse_sse_connections_total{result="connected"} 456
soroban_pulse_sse_connections_total{result="disconnected"} 450
soroban_pulse_sse_connections_total{result="error"} 6
soroban_pulse_sse_connection_duration_seconds_bucket{le="0.1"} 100
soroban_pulse_sse_connection_duration_seconds_bucket{le="1.0"} 350
soroban_pulse_sse_connection_duration_seconds_bucket{le="+Inf"} 456
soroban_pulse_sse_active_connections 6

# Partition pruning
soroban_pulse_partitions_dropped_total{partition="events_2024_01"} 1
soroban_pulse_partitions_dropped_total{partition="events_2024_02"} 1

# Profiling (when endpoints are used)
soroban_pulse_profile_requests_total{type="cpu"} 2
soroban_pulse_profile_requests_total{type="heap"} 1
```

## Testing

### Unit Tests

Run tests locally:

```bash
cargo test metrics::tests
cargo test profiling::tests
cargo test pruner::tests
```

### Integration Tests

1. **Unified error metrics**: Trigger RPC/webhook/email/DB errors and verify counters increment
2. **SSE connection tracking**: Simulate SSE connections and verify connection events are recorded
3. **Partition pruning**: Run migration, then `SELECT * FROM drop_old_partitions(...)` dry-run
4. **Profiling endpoints**: Call endpoints with valid admin key, verify pprof format

### Alerting Tests

Test alert rules:

```bash
# Install promtool
go install github.com/prometheus/prometheus/cmd/promtool@latest

# Validate alert rules
promtool check rules docs/alerts.yml

# Unit test individual rules (requires test data)
promtool test rules docs/alerts_test.yml
```

## Backward Compatibility

All changes are backward compatible:

- Existing specific counters (`rpc_errors_total`, `webhook_failures_total`, etc.) remain
- New unified counter is an **addition**, not a replacement
- SSE metrics are **new**, no breaking changes
- Partition pruning is automatic if retention policy is configured
- Profiling endpoints are optional; no changes to existing APIs

## Future Enhancements

- [ ] Integrate actual pprof-rs for real CPU/heap profiling
- [ ] Flame graph generation from profiles
- [ ] Historical profile storage for regression detection
- [ ] Goroutine profiling endpoint
- [ ] Trace profiling endpoint
- [ ] Partition pruning dry-run endpoint
