# Capacity Planning Automation (Issue #589)

## Overview

Capacity Planning Automation predicts resource requirements and alerts on capacity thresholds. It analyzes growth trends, creates forecasts, and generates resource recommendations to prevent outages and optimize infrastructure spending.

## Features

### Growth Trend Analysis

Analyzes resource usage patterns to identify:

- **Increasing Trends** - Usage growing > 5% per period
- **Stable Trends** - Usage stable ± 5% per period
- **Decreasing Trends** - Usage declining > 5% per period

### Forecasting

Projects future resource needs:

- **7-day Forecast** - Near-term trend prediction
- **30-day Forecast** - Monthly capacity planning
- **Growth Rate** - Percentage change per period

### Resource Recommendations

Auto-generates recommendations with:

- **Current Usage** - Actual resource consumption
- **Recommended Capacity** - Needed capacity (with 20% buffer)
- **Urgency Level** - Critical, High, Medium, Low
- **Forecast Data** - Reasoning with projected usage

### Capacity Alerts

Real-time alerts when:

- **Alert Threshold** - 80% of capacity
- **Critical Level** - 100% of capacity
- **Severity Levels** - Critical, Warning, Info

## Tracked Metrics

| Metric          | Capacity | Alert Threshold | Period    |
| --------------- | -------- | --------------- | --------- |
| DB Connections  | 100      | 80              | Real-time |
| Memory (GB)     | 8        | 6.4             | Real-time |
| Storage (GB)    | 1024     | 819.2           | Hourly    |
| Events/sec      | 10000    | 8000            | Real-time |
| API Connections | 1000     | 750             | Real-time |

## Using Capacity Planner

### Basic Usage

```rust
use soroban_pulse::capacity_planning::CapacityPlanner;

let mut planner = CapacityPlanner::new();

// Record metric
planner.record_metric("db_connections".to_string(), 50.0);
planner.record_metric("db_connections".to_string(), 75.0);

// Analyze trend
if let Some(trend) = planner.analyze_growth_trend("db_connections") {
    println!("Growth: {:.1}%", trend.growth_rate_percent);
    println!("7-day forecast: {:.0}", trend.forecast_7days);
    println!("30-day forecast: {:.0}", trend.forecast_30days);
}
```

### Generate Recommendations

```rust
// Get single resource recommendation
if let Some(rec) = planner.generate_recommendation("memory_gb") {
    println!("Current: {:.1}GB", rec.current_capacity);
    println!("Recommended: {:.1}GB", rec.recommended_capacity);
    println!("Urgency: {:?}", rec.urgency);
}

// Get all recommendations
let recommendations = planner.get_recommendations_summary();
for rec in recommendations {
    if rec.urgency == CapacityUrgency::Critical {
        send_alert(&rec);
    }
}
```

### Check Alerts

```rust
// Check single metric
if let Some(alert) = planner.check_alert_threshold("storage_gb") {
    println!("Alert: {}", alert.message);
}

// Get all active alerts
let alerts = planner.get_active_alerts();
for alert in alerts {
    if alert.severity == AlertSeverity::Critical {
        notify_oncall(&alert);
    }
}
```

### Generate Forecast

```rust
// Generate 30-day forecast
if let Some(forecast) = planner.generate_forecast("events_per_sec", 30) {
    for (day, predicted_value) in forecast.forecast_data {
        println!("Day {}: {:.0} events/sec", day, predicted_value);
    }
}
```

## Capacity Forecast Dashboard

### Dashboard Components

1. **Current Usage** - Real-time resource consumption
2. **Forecast Graph** - 30-day trend projection
3. **Recommendations** - Highlighted capacity upgrades
4. **Alerts** - Active and historical alerts
5. **Trend Summary** - Growth rates by resource

### Accessing Dashboard

```bash
# View dashboard in browser
open http://localhost:3000/dashboards/capacity-planning

# Or via API
curl http://localhost:3000/api/v1/capacity/forecast?days=30

# With specific metric
curl http://localhost:3000/api/v1/capacity/forecast?metric=memory_gb&days=30
```

## Alert Configuration

### Setting Custom Thresholds

Configure in `config.toml`:

```toml
[capacity_planning]
# Database connections
db_connections_capacity = 100
db_connections_alert_percent = 80  # Alert at 80 connections

# Memory in GB
memory_capacity_gb = 8.0
memory_alert_percent = 80  # Alert at 6.4GB

# Storage in GB
storage_capacity_gb = 1024.0
storage_alert_percent = 80  # Alert at 819.2GB

# Event throughput
events_per_sec_capacity = 10000
events_per_sec_alert_percent = 80

# API concurrency
api_connections_capacity = 1000
api_connections_alert_percent = 75  # Alert at 750 connections

# Forecast interval (days)
forecast_days = 30

# Urgency thresholds
critical_days_remaining = 7  # < 7 days = critical
high_days_remaining = 14     # 7-14 days = high
medium_days_remaining = 30   # 14-30 days = medium
```

## Automation Integration

### Scheduled Capacity Analysis

Runs every hour:

```bash
# In crontab or scheduler
0 * * * * ./scripts/capacity_analysis.sh

# Manual trigger
./scripts/capacity_analysis.sh --force
```

### Alert Routing

Alerts route to:

1. **Application Logs** - All alerts logged
2. **Prometheus** - Metrics scraped by monitoring
3. **Email** - Critical alerts to ops team
4. **Slack** - High urgency alerts to #ops channel
5. **PagerDuty** - Critical cascaded alerts

### Auto-Scaling Integration

Recommendations can trigger auto-scaling:

```bash
# Query recommendations API
curl http://localhost:3000/api/v1/capacity/recommendations

# Output:
# {
#   "resource": "db_connections",
#   "recommended_capacity": 150,
#   "urgency": "HIGH"
# }

# Trigger scaling event
./scripts/scale_resource.sh db_connections 150
```

## Forecasting Algorithm

### Growth Rate Calculation

```
growth_rate = ((current - previous) / previous) * 100%

Trend Direction:
- Increasing: > 5%
- Stable: ±5%
- Decreasing: < -5%
```

### Capacity Forecast

```
forecast_value = current_value + (daily_change * days_ahead)

Where:
daily_change = current_value - previous_value

Example:
current = 100
previous = 90
daily_change = 10
forecast_7_days = 100 + (10 * 7) = 170
```

### Urgency Calculation

```
days_to_capacity = (capacity - current) / daily_change

Urgency:
- Critical: < 7 days
- High: 7-14 days
- Medium: 14-30 days
- Low: > 30 days
```

## Common Scenarios

### Database Connection Pool

```
Current: 85 connections
Capacity: 100
Growth: +5 connections/day
Forecast 7d: 120 connections

Result: CRITICAL - Exceeds capacity in 3 days
Recommendation: Increase to 150 connections
```

### Storage Growth

```
Current: 800GB
Capacity: 1024GB (1TB)
Growth: +10GB/day
Forecast 30d: 1100GB

Result: HIGH - Exceeds capacity in 22 days
Recommendation: Increase to 2TB
```

### Event Processing

```
Current: 7500 events/sec
Capacity: 10000 events/sec
Growth: -500 events/sec (declining)
Forecast 30d: 5000 events/sec

Result: LOW - No action needed
Status: Trending better than expected
```

## Monitoring and Adjustments

### Review Metrics

Daily review in dashboard shows:

- Accuracy of previous forecasts
- Actual vs. predicted usage
- Trend changes

### Adjust Forecasts

If prediction accuracy < 80%:

1. Increase data points collected
2. Review for seasonal patterns
3. Account for planned events (releases, migrations)

### Update Thresholds

Quarterly threshold review:

- Validate alert thresholds still appropriate
- Adjust for known seasonal patterns
- Update capacity limits based on business growth

## Integration Examples

### Kubernetes Auto-Scaling

```yaml
apiVersion: autoscaling.knative.dev/v1alpha1
kind: KPA
metadata:
  name: soroban-pulse
spec:
  scaleTargetRef:
    name: soroban-pulse
  maxScaleDownRate: "0.1"
  # Use capacity planning metrics
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: "70"
```

### CloudWatch Integration (AWS)

```python
# Put custom metrics
cloudwatch.put_metric_data(
    Namespace='SorobanPulse',
    MetricData=[
        {
            'MetricName': 'DatabaseConnections',
            'Value': 85,
            'Unit': 'Count'
        },
        {
            'MetricName': 'StorageUsageGB',
            'Value': 800,
            'Unit': 'Gigabytes'
        }
    ]
)
```

## Multi-replica sizing

The sections above cover a single instance. When you run several
SorobanPulse replicas against one PostgreSQL primary (the default Terraform
deployment runs `app_container_count = 2` ECS tasks), sizing changes in
three ways:

1. Every replica opens its **own** connection pool, so database connections
   grow linearly with the replica count.
2. Every replica takes part in **advisory-lock leader election** for the
   indexer, which pins connections even while the replica is a standby.
3. Only **one** replica (the leader) runs the indexer. The others are
   standbys that serve HTTP traffic and poll for the lock.

### How replicas use database connections

| Consumer | Connections per replica | Source |
|----------|------------------------|--------|
| HTTP handler pool (primary) | up to `DB_MAX_CONNECTIONS` | `db::create_pool` in `src/main.rs` |
| HTTP read pool (read replica) | up to `DB_MAX_CONNECTIONS` on the **replica** DB, only when `DATABASE_REPLICA_URL` is set; otherwise it shares the primary pool | `read_pool` in `src/main.rs` |
| Advisory-lock connection | 1 pinned connection, held for the life of the indexer task (leader **and** standby) | `lock_conn` in `src/indexer.rs` |
| Advisory-lock session | 1 more connection while the leader holds the lock (`pg_try_advisory_lock` is session-scoped) | `src/indexer.rs`, `src/advisory_lock.rs` |
| Indexer writes (leader only) | 1–2 connections during batch inserts | `src/indexer.rs` |

The lock and indexer connections come **out of** the same
`DB_MAX_CONNECTIONS` pool. They are not extra connections on top of it, but
they reduce what is left for HTTP requests.

### Advisory lock overhead

- **Connections:** budget `L = 2` connections per replica: the pinned lock
  connection, plus the session that holds the lock on the leader. Standbys
  use 1 of these 2 permanently.
- **Queries:** each standby runs one `SELECT pg_try_advisory_lock($1)` every
  `INDEXER_LOCK_RETRY_SECS` (default `30`). For `N` replicas that is
  `(N − 1) / INDEXER_LOCK_RETRY_SECS` queries per second, about 0.07 q/s for
  3 replicas, which is negligible for PostgreSQL.
- **Failover time:** when the leader dies, its session ends and the lock is
  released. A standby picks it up within at most `INDEXER_LOCK_RETRY_SECS`.
  Lowering the interval speeds up failover, but each standby then polls more
  often.
- **Multi-network:** each `chain_id` uses its own lock key (Issue #1063).
  With `K` networks indexed, use `L = 2 × K`.

### Standby indexer CPU baseline

A standby replica runs no indexing work, so its CPU use is its HTTP load
plus the lock poll, and the poll is close to zero. Size the task as follows:

- **Standby baseline:** use the HTTP-only CPU you measure for one replica at
  `R / N` requests per second. With the Terraform default
  (`ecs_task_cpu = 512`, 0.5 vCPU), an idle standby should stay well below
  5% CPU.
- **Leader:** standby baseline **plus** the indexer CPU, which depends on
  ledger close rate and event volume. Measure it on a single instance first.
- **Sizing rule:** any replica can become leader, so give **every** task
  enough CPU for the leader workload. Keep the *leader's* CPU below 70% (the
  auto-scaling target used elsewhere in this document).

### Formulas

| Symbol | Meaning | Default |
|--------|---------|---------|
| `N` | Replica count (`app_container_count`) | `2` |
| `S` | Deployment surge factor (`deployment_maximum_percent / 100`) | `2.0` |
| `P` | Pool size per replica (`DB_MAX_CONNECTIONS`) | `10` |
| `R` | Total request rate across all replicas (req/s) | — |
| `q` | Average DB queries per HTTP request | `1.5` (estimate) |
| `t` | Average query time in seconds | `0.02` (20 ms, estimate) |
| `h` | Headroom multiplier for bursts | `1.5` |
| `L` | Advisory-lock connections per replica (`2 × K` networks) | `2` |
| `I` | Indexer write connections (counted on every replica, since any can lead) | `2` |
| `M` | PostgreSQL `max_connections` | RDS: `LEAST(DBInstanceClassMemory / 9531392, 5000)` |
| `r` | Connections reserved for superusers, migrations and monitoring | `10` |

**1. Concurrent HTTP connections needed per replica** (Little's law):

```
C_http = ceil( (R / N) × q × t × h )
```

**2. Pool size per replica:**

```
P ≥ C_http + L + I
```

**3. Connections on the primary database** in steady state and during a
rolling deploy (ECS briefly runs old and new tasks side by side):

```
without DATABASE_REPLICA_URL:  C_primary = N × P
with    DATABASE_REPLICA_URL:  C_primary = N × P        (HTTP reads go to the replica)
                               C_replica = N × P

peak during deploy:            C_peak    = ceil(S × N) × P
```

**4. Constraint.** Keep 20% spare capacity for the CapacityPlanner alert
threshold:

```
C_peak ≤ 0.8 × (M − r)
```

**5. Largest pool you can safely configure** for a given `N`:

```
P_max = floor( 0.8 × (M − r) / ceil(S × N) )
```

If `P` from step 2 is larger than `P_max`, either move to a larger DB
instance class, set `DATABASE_REPLICA_URL` to offload reads, or put
PgBouncer in front of the primary.

### Worked example: 3 replicas at 100 req/s

Inputs: `N = 3`, `R = 100`, `q = 1.5`, `t = 0.02 s`, `h = 1.5`, `L = 2`,
`I = 2`, `S = 2.0`, and the Terraform default `db_instance_class = "db.t3.medium"`
(4 GiB RAM, which gives RDS `M ≈ 410`; confirm with `SHOW max_connections;`),
`r = 10`.

| Step | Calculation | Result |
|------|-------------|--------|
| 1. HTTP connections per replica | `ceil(100 / 3 × 1.5 × 0.02 × 1.5)` = `ceil(1.5)` | `C_http = 2` |
| 2. Minimum pool per replica | `2 + 2 + 2` | `P ≥ 6`; keep the default **`DB_MAX_CONNECTIONS = 10`** |
| 3a. Steady-state primary connections | `3 × 10` | `C_primary = 30` |
| 3b. Peak during rolling deploy | `ceil(2.0 × 3) × 10` = `6 × 10` | `C_peak = 60` |
| 4. Constraint check | `60 ≤ 0.8 × (410 − 10) = 320` | ✅ passes, 260 connections spare |
| 5. Largest safe pool | `floor(320 / 6)` | `P_max = 53` |

Recommended settings for this deployment:

```hcl
# terraform.tfvars
app_container_count = 3
db_instance_class   = "db.t3.medium"
ecs_task_cpu        = 512    # every task must be able to run the leader workload
ecs_task_memory     = 1024
```

```bash
# container environment
DB_MAX_CONNECTIONS=10
DB_MIN_CONNECTIONS=2          # 3 × 2 = 6 idle connections held on the primary
INDEXER_LOCK_RETRY_SECS=30    # failover in at most 30 s
```

The same formulas give the Terraform defaults (`N = 2`, `P = 10`):
`C_primary = 20` and `C_peak = 40`, well inside `0.8 × (410 − 10) = 320`.
That leaves room to scale to `N = 16` replicas at `P = 10`
(`ceil(2 × 16) × 10 = 320`) before you need a larger instance class or a
connection pooler.

### Scaling checklist

- [ ] Recalculate `C_peak` whenever you change `app_container_count`, `DB_MAX_CONNECTIONS` or `db_instance_class`.
- [ ] Add 2 connections per replica for **each** extra network in `chain_id`s.
- [ ] Alert on `soroban_pulse_db_pool_utilization` per replica, not just in aggregate.
- [ ] Check `soroban_pulse_indexer_is_leader`: exactly one replica should report `1`.

## Related Issues

- #588 - Performance Monitoring Dashboard
- #587 - Auto-scaling Policies
- #591 - Performance Regression Testing
