# Connection Pool Optimization — Issue #995

> **Warning:** This document describes in-progress optimizations. Do not modify
> pool settings in production without first profiling under realistic load.

## Overview

SorobanPulse uses SQLx's PostgreSQL connection pool. Poor pool sizing leads to
two failure modes:

- **Under-provisioned**: requests queue waiting for a free connection, adding
  latency and eventually timing out.
- **Over-provisioned**: too many idle connections waste PostgreSQL server
  resources and can exhaust the server's `max_connections` limit.

This document covers how to profile current usage, interpret the new wait-time
metrics, use the dynamic sizing recommendations, and run the load-testing
scenarios that ship with the project.

---

## 1. Profiling Connection Pool Usage

### 1.1 Prometheus Metrics

The following metrics are emitted by the unified pool module `src/db_pool/`
(monitor and adaptive tuner, both started once by `DbPool::init`):

| Metric | Type | Description |
|--------|------|-------------|
| `soroban_pulse_db_pool_utilization` | Gauge | Active connections / max (0–1) |
| `soroban_pulse_db_pool_active_connections` | Gauge | Connections currently in use |
| `soroban_pulse_db_pool_max_connections` | Gauge | Configured maximum |
| `soroban_pulse_db_pool_acquire_latency_seconds` | Histogram | Time from `pool.acquire()` returning |
| `soroban_pulse_db_pool_exhaustion_alerts_total` | Counter | Times utilization ≥ 90% |
| `soroban_pulse_db_pool_wait_seconds` | Histogram | **New (Issue #995)** — time a caller queued waiting for a slot |
| `soroban_pulse_db_pool_wait_timeout_total` | Counter | **New (Issue #995)** — waits exceeding 1 s |
| `soroban_pulse_db_pool_queue_depth` | Gauge | **New (Issue #995)** — callers currently waiting |
| `soroban_pulse_db_pool_size` / `_idle` / `_max` | Gauge | Open, idle and maximum connections |
| `soroban_pulse_db_pool_adaptive_target_min` / `_max` | Gauge | Adaptive tuner recommendations (Issue #817) |
| `soroban_pulse_db_pool_health_check_failures_total` | Counter | Failed adaptive-tuner keepalive pings |
| `soroban_pulse_db_pool_stale_connections_total` | Counter | Idle connections flagged as potentially stale |
| `soroban_pulse_db_pool_acquire_timeout_total` | Counter | Tracked acquisitions that timed out |

### 1.2 Grafana Quick-Start

Import `docs/grafana-dashboard.json` and look at the **Connection Pool** row.
Key panels:

- **Pool Utilization** — if p99 regularly exceeds 80%, the pool is too small.
- **Wait Time Histogram** — long tails (p99 > 50 ms) indicate pool starvation.
- **Queue Depth** — any sustained non-zero value means callers are waiting.

### 1.3 Log-Based Profiling

The pool monitor logs a snapshot every 15 seconds at `INFO` level:

```
Pool tuning recommendation (Issue #995)
  reason="Peak utilization 87% exceeds 85% — raise DB_MAX_CONNECTIONS to 13"
  suggested_max=Some(13)
  avg_wait_ms=Some(23.4)
  wait_timeouts=0
```

Enable structured logs (`RUST_LOG_FORMAT=json`) and pipe to `jq` to extract
pool events:

```bash
cargo run 2>&1 | jq 'select(.fields.suggested_max != null)'
```

---

## 2. Identifying Bottlenecks

### Checklist

- [ ] `soroban_pulse_db_pool_utilization` sustained above **0.85** → pool too small
- [ ] `soroban_pulse_db_pool_wait_seconds` p99 above **100 ms** → pool starvation
- [ ] `soroban_pulse_db_pool_queue_depth` non-zero at peak → requests queuing
- [ ] `soroban_pulse_db_pool_exhaustion_alerts_total` growing → pool maxed out
- [ ] `soroban_pulse_db_pool_utilization` sustained below **0.20** → pool too large

### Bottleneck Patterns

| Symptom | Likely Cause | Action |
|---------|-------------|--------|
| High utilization, short wait time | Pool sizing is fine, query throughput increasing | Monitor; increase max when p99 wait > 50 ms |
| High utilization, long wait time | Pool exhausted | Increase `DB_MAX_CONNECTIONS` |
| Low utilization, idle connections | Over-provisioned | Reduce `DB_MIN_CONNECTIONS` |
| Spiky wait times correlating with slow queries | Slow queries holding connections | Tune queries; lower `DB_STATEMENT_TIMEOUT_MS` |

---

## 3. Dynamic Pool Sizing

### 3.1 Recommendation Engine

`PoolStats::suggest_pool_size` in `src/db_pool/monitor.rs` computes
recommendations based on observed peak utilization:

| Condition | Recommendation |
|-----------|----------------|
| Peak util > 85% | Raise `DB_MAX_CONNECTIONS` by 25% |
| Peak util < 20% | Lower `DB_MIN_CONNECTIONS` to 1 |
| Otherwise | No change needed |

The recommendation is logged every monitor cycle (default 15 s). It does **not**
automatically change the pool at runtime because SQLx does not support live pool
resizing. Apply the suggested values by updating `.env` and restarting.

### 3.2 Configuration Variables

```dotenv
# Pool sizing
DB_MIN_CONNECTIONS=2          # Keep at least 2 idle connections
DB_MAX_CONNECTIONS=10         # Maximum concurrent connections

# Timeouts
DB_IDLE_TIMEOUT_SECS=600      # Recycle idle connections after 10 min
DB_MAX_LIFETIME_SECS=1800     # Force-recycle after 30 min regardless
DB_STATEMENT_TIMEOUT_MS=5000  # Kill runaway queries after 5 s
```

### 3.3 Sizing by Workload Class

| Workload | Recommended Min | Recommended Max |
|----------|----------------|----------------|
| Development / single user | 1 | 5 |
| Small production (< 50 req/s) | 2 | 10 |
| Medium production (50–200 req/s) | 5 | 25 |
| High-throughput (> 200 req/s) | 10 | 50+ |
| Multi-replica deployment | 2 per replica | 10–20 per replica |

**Note:** PostgreSQL's default `max_connections` is 100. Ensure
`DB_MAX_CONNECTIONS × replica_count` does not approach that limit or increase
`max_connections` in `postgresql.conf`.

---

## 4. Connection Wait Time Tracking

Issue #995 introduces explicit wait-time instrumentation. The
`acquire_tracked_with_wait` function wraps `pool.acquire()` and:

1. Increments `soroban_pulse_db_pool_queue_depth` on entry.
2. Records the wait duration in `soroban_pulse_db_pool_wait_seconds` on exit.
3. Increments `soroban_pulse_db_pool_wait_timeout_total` when wait > 1 s.
4. Decrements the queue depth gauge on exit.

### Alerting Rule

Add to `docs/alerts.yml`:

```yaml
- alert: PoolWaitTimeHigh
  expr: histogram_quantile(0.99, rate(soroban_pulse_db_pool_wait_seconds_bucket[5m])) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "DB pool p99 wait time > 100 ms"
    description: "p99 connection wait time is {{ $value | humanizeDuration }}. Consider increasing DB_MAX_CONNECTIONS."

- alert: PoolQueueDepthNonZero
  expr: soroban_pulse_db_pool_queue_depth > 0
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "DB pool queue depth non-zero for 2+ minutes"
```

---

## 5. Load Testing Scenarios

### 5.1 Existing k6 Scripts

The project ships several k6 scripts in `tests/load/`:

| Script | Scenario | Target |
|--------|----------|--------|
| `events.js` | 100 req/s constant rate, 30 s | `GET /v1/events` p99 < 200 ms |
| `stress.js` | Ramp to 10× normal load | Stability under stress |
| `burst.js` | Sudden spike then drop | Pool recovery time |
| `soak.js` | 1× normal load for 1 hour | Memory/connection leaks |
| `sustained_overload.js` | 200% of capacity for 10 min | Graceful degradation |

### 5.2 Pool-Focused Load Test

Run the full event load test and watch pool metrics in a separate terminal:

```bash
# Terminal 1 — run the service
make run

# Terminal 2 — watch pool metrics
watch -n 2 'curl -s http://localhost:3000/metrics | grep soroban_pulse_db_pool'

# Terminal 3 — run the load test
k6 run tests/load/events.js
```

### 5.3 Pool Exhaustion Simulation

To verify the pool handles exhaustion gracefully, temporarily set
`DB_MAX_CONNECTIONS=2` and run the burst test:

```bash
DB_MAX_CONNECTIONS=2 cargo run &
k6 run tests/load/burst.js
```

Expected behavior:
- `soroban_pulse_db_pool_queue_depth` spikes.
- `soroban_pulse_db_pool_wait_seconds` p99 rises.
- HTTP responses return 503 once the queue is full, not 500.
- After the burst, the pool recovers to normal utilization.

---

## 6. Benchmarking Before/After

Use `benches/db_queries.rs` to capture a baseline before and after any pool
change:

```bash
# Baseline
cargo bench --bench db_queries 2>&1 | tee /tmp/before.txt

# Apply pool changes (update .env)
cargo bench --bench db_queries 2>&1 | tee /tmp/after.txt

# Compare
diff /tmp/before.txt /tmp/after.txt
```

Key benchmarks:

| Benchmark | Description |
|-----------|-------------|
| `db/get_events_no_filter` | Paginated query, first page |
| `db/get_events_ledger_range` | Range-filtered query |
| `db/get_events_exact_count` | `COUNT(*)` — most expensive |
| `db/get_events_by_contract` | Contract-filtered query |

---

## 7. Multi-Replica Considerations

Each replica maintains its own connection pool. The total number of connections
to PostgreSQL is `DB_MAX_CONNECTIONS × replica_count`. For a 3-replica
deployment with `DB_MAX_CONNECTIONS=10`, PostgreSQL receives at most 30
connections.

Only one replica holds the advisory lock and actively indexes (leader). Standby
replicas still maintain connections for read queries. The
`soroban_pulse_indexer_is_leader` gauge tells you which replica is the current
leader.

### Per-Replica Tuning

For replica-aware pool sizing, lower `DB_MAX_CONNECTIONS` on standby replicas
since they handle read traffic only. Use Kubernetes ConfigMap or environment
variables per pod to differentiate:

```yaml
# Leader pod
env:
  - name: DB_MAX_CONNECTIONS
    value: "15"

# Standby pods
env:
  - name: DB_MAX_CONNECTIONS
    value: "8"
```

---

## 8. References

- `src/db_pool/mod.rs` — single pool initialisation path (`connect`, `DbPool::init`)
- `src/db_pool/monitor.rs` — pool monitor, wait-time tracking, sizing suggestions
- `src/db_pool/adaptive.rs` — advanced adaptive tuning (Issue #817)
- `src/db_pool/management.rs` — REST API helpers for pool stats (`GET /v1/admin/pool`)
- `docs/runbooks/db-pool-exhaustion.md` — on-call runbook
- `docs/alerts.yml` — Prometheus alerting rules
- `docs/grafana-dashboard.json` — Grafana dashboard
