# Load Testing Runbook

Operational guide for running, interpreting, and maintaining the Soroban Pulse load test suite (issue #811).

## Table of Contents

- [Prerequisites](#prerequisites)
- [Scenario Overview](#scenario-overview)
- [Running Scenarios Locally](#running-scenarios-locally)
- [Historical Data Management](#historical-data-management)
- [Regression Detection](#regression-detection)
- [Trend Analysis](#trend-analysis)
- [Baseline Promotion](#baseline-promotion)
- [CI/CD Integration](#cicd-integration)
- [Interpreting Results](#interpreting-results)
  - [Steady-State](#steady-state-events_steady)
  - [Stress Test](#stress-test)
  - [Spike Test](#spike-test)
  - [Soak Test](#soak-test)
  - [Multi-Contract Queries](#multi-contract-queries)
  - [Webhook Delivery](#webhook-delivery)
    - [Retry Storm](#retry-storm)
    - [Sustained Load (10 min)](#sustained-load-10-min)
    - [DLQ Replay Under Concurrent Load](#dlq-replay-under-concurrent-load)
    - [Webhook SLO Thresholds](#webhook-slo-thresholds)
- [Performance Budgets](#performance-budgets)
- [Updating Baselines](#updating-baselines)
- [Webhook Delivery Setup](#webhook-delivery-setup)
- [Troubleshooting](#troubleshooting)
- [Related Docs](#related-docs)

---

## Prerequisites

### k6

```bash
# macOS
brew install k6

# Ubuntu / Debian
sudo gpg --no-default-keyring \
  --keyring /usr/share/keyrings/k6-archive-keyring.gpg \
  --keyserver hkp://keyserver.ubuntu.com:80 \
  --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install k6

# Verify
k6 version
```

### Node.js

The regression checker and analysis tools require Node.js ≥ 18:

```bash
node --version   # must be ≥ 18.0.0
```

### jq (for shell script helpers)

```bash
# macOS
brew install jq

# Ubuntu / Debian
sudo apt-get install -y jq
```

### Running the service

```bash
# Start PostgreSQL then run the service
cargo run

# Or with Docker Compose
make docker-up
```

---

## Scenario Overview

| Script | Purpose | Default duration | Runner(s) |
|---|---|---|---|
| `tests/load/events.js` | Steady-state 100 req/s baseline | 30 s | Every PR, every push to main |
| `tests/load/sse_stream.js` | SSE — 50 sustained connections + churn at 10 conn/s | ~1 m | Every PR, every push to main |
| `tests/load/stress.js` | Ramp 2×→10× (200→1 000 req/s) to find breaking point | ~18 m | Push to main, weekly, manual |
| `tests/load/spike.js` | Instant 10× burst (1 000 req/s × 30 s) + recovery | ~3.5 m | Push to main, weekly, manual |
| `tests/load/soak.js` | 24 h sustained load — detect leaks and drift | 24 h (overridable) | Weekly schedule, manual |
| `tests/load/multi_contract.js` | Real-world mixed query workload (7 weighted patterns) | ~9 m | Every PR, every push to main |
| `tests/load/webhook_delivery.js` | Webhook pipeline under concurrent load | 5 m | Push to main, weekly, manual |

---

## Running Scenarios Locally

```bash
# 1. Start the service (in another terminal)
cargo run

# 2. Run any scenario
k6 run tests/load/events.js
k6 run tests/load/sse_stream.js
k6 run tests/load/stress.js
k6 run tests/load/spike.js
k6 run tests/load/multi_contract.js
k6 run tests/load/webhook_delivery.js

# Short soak (validation run — override default 24 h)
k6 run -e SOAK_DURATION=30m tests/load/soak.js

# Point at a non-default host or inject an API key
k6 run -e BASE_URL=http://staging.example.com -e API_KEY=mykey tests/load/stress.js

# Save a JSON summary for regression check / archiving
mkdir -p tests/load/results
k6 run --out json=tests/load/results/stress_raw.json tests/load/stress.js
```

### Environment variables accepted by all scenarios

| Variable | Default | Description |
|---|---|---|
| `BASE_URL` | `http://localhost:3000` | Service base URL |
| `API_KEY` | — | Sent as `X-Api-Key` when set |
| `SOAK_DURATION` | `24h` | Soak test duration (`soak.js` only) |
| `CONTRACT_IDS` | built-in stubs | Comma-separated real contract IDs (`multi_contract.js`) |
| `ADMIN_API_KEY` | — | Admin key for replay endpoint (`webhook_delivery.js`) |
| `WEBHOOK_RECEIVER` | — | Stub receiver URL to poll (`webhook_delivery.js`) |
| `SCENARIO` | `sustained` | Webhook scenario to run: `sustained`, `retry_storm`, or `dlq_replay` (`webhook_delivery.js`) |
| `FAILURE_RATE` | `0.8` | Fraction of webhook endpoints forced to return 500 (`webhook_delivery.js`) |
| `DLQ_REPLAY_RATE` | `5` | DLQ replay requests per second issued during the replay scenario (`webhook_delivery.js`) |
| `WEBHOOK_DURATION` | `10m` | Sustained-load duration for the webhook scenarios (`webhook_delivery.js`) |

---

## Historical Data Management

Use `scripts/perf_regression.sh` to archive, purge, and inspect historical results.

### Archive a result

```bash
# Archive a k6 JSON summary into tests/load/results/history/<scenario>/
scripts/perf_regression.sh archive events_steady \
  tests/load/results/events_steady_raw.json 1M
```

This creates a dated file such as:
```
tests/load/results/history/events_steady/20260729T120000Z_1M.json
```
and appends a line to `tests/load/results/history/index.jsonl`.

### Purge old results

```bash
# Remove result files older than the default retention (90 days)
scripts/perf_regression.sh purge

# Custom retention period
scripts/perf_regression.sh purge --days 30
```

The `RETENTION_DAYS` environment variable sets the default:

```bash
RETENTION_DAYS=60 scripts/perf_regression.sh purge
```

---

## Regression Detection

After any run, compare against the baseline library:

```bash
# node tests/load/regression_check.js <scenario> <summary.json> [baselines.json] [db_size]
node tests/load/regression_check.js events_steady \
  tests/load/results/events_steady_raw.json \
  tests/load/baselines.json \
  1M
```

Or via the shell helper:

```bash
scripts/perf_regression.sh check events_steady \
  tests/load/results/events_steady_raw.json 1M
```

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | All checks passed — no regressions |
| `1` | One or more regressions detected |
| `2` | Bad arguments or missing file |

**Regression thresholds** (configured in `tests/load/baselines.json`):

| Dimension | Threshold |
|---|---|
| Latency (any percentile) | > 20 % above baseline |
| Error rate | > 2 percentage points above baseline |
| Memory growth (soak) | > 50 % above baseline |

Both passing and regressing runs are appended to the `history` array in `baselines.json` (capped at 200 entries) for audit purposes.

---

## Trend Analysis

Use `tests/load/analyze_results.js` to inspect trends across multiple runs.

### Show a trend table

```bash
# Last 10 runs for multi_contract scenario
node tests/load/analyze_results.js trend multi_contract

# Filter by database size, show last 5 runs
node tests/load/analyze_results.js trend stress --last 5 --db-size 1M
```

The trend subcommand reads the index at `tests/load/results/history/index.jsonl` (or falls back to scanning files directly). It prints a table of all tracked metrics across the selected runs plus an overall trend arrow:

```
=== TREND: events_steady — last 5 runs ===

  Metric                 20260720T...  20260721T...  20260722T...  ...
  ──────────────────────────────────────────────────────────────────
  p50 latency              8.2 ms        8.1 ms        8.3 ms   ...
  p95 latency             24.1 ms       23.8 ms       25.0 ms   ...
  p99 latency             44.6 ms       43.9 ms       46.2 ms   ...
  error rate             0.000 %       0.000 %       0.000 %   ...
  throughput              100           100           100       ...

  Overall trend (p50 latency): → STABLE (+1.2 % over 5 runs)
```

### Summary of a single file

```bash
node tests/load/analyze_results.js summary \
  tests/load/results/stress_raw.json --scenario stress
```

### Side-by-side comparison

```bash
node tests/load/analyze_results.js compare \
  tests/load/results/history/stress/20260701T000000Z_1M.json \
  tests/load/results/stress_raw.json \
  --scenario stress
```

Delta column shows percentage change with warning symbols (⚠ > 10 %, ✗ > 20 %, ✓ improved).

---

## Baseline Promotion

When a deliberate performance improvement lowers latency, update the baselines to avoid false positives on future runs.

### Promote via the analysis tool

```bash
# 1. Run the scenario and confirm the improvement is intentional
k6 run --out json=tests/load/results/events_raw.json tests/load/events.js

# 2. Promote the new numbers to baselines.json
node tests/load/analyze_results.js promote events_steady \
  tests/load/results/events_raw.json 1M

# Or via the shell helper:
scripts/perf_regression.sh promote events_steady \
  tests/load/results/events_raw.json 1M
```

### Promote manually

Edit `tests/load/baselines.json` directly. Update the relevant `p50_ms`, `p95_ms`, `p99_ms`, and `error_rate` fields under `scenarios.<name>.baselines.<db_size>`.

### Committing a baseline change

```bash
git add tests/load/baselines.json
git commit -m "perf(load): update events_steady baseline after index optimisation"
```

Never update baselines to cover up a regression — fix the regression first.

---

## CI/CD Integration

The workflow `.github/workflows/load-tests.yml` runs load tests automatically.

### Trigger matrix

| Job | On PR | On push to main | Weekly | Manual |
|---|---|---|---|---|
| `load-events-steady` | ✓ | ✓ | ✓ | ✓ |
| `load-multi-contract` | ✓ | ✓ | ✓ | ✓ |
| `load-stress` | — | ✓ | ✓ | ✓ |
| `load-spike` | — | ✓ | ✓ | ✓ |
| `load-soak` | — | — | ✓ | ✓ |
| `load-test-summary` (PR comment) | ✓ | — | — | — |

### Manual trigger

```
GitHub → Actions → "Load Tests" → Run workflow
```

Inputs:

| Input | Default | Description |
|---|---|---|
| `scenario` | (all) | Run a specific scenario only |
| `db_size` | `1M` | Baseline row to compare against |
| `soak_duration` | `30m` | Override soak duration (e.g. `24h` for full run) |

### Artifacts

Each job uploads results to GitHub Actions artifacts (retained 30 days; soak retained 90 days). Download them for offline analysis:

```bash
# Using GitHub CLI
gh run download <run-id> --name results-stress
```

### PR comment

When `load-events-steady` and `load-multi-contract` both complete on a PR, the `load-test-summary` job posts a comment with key p99 latency and error rate values.

---

## Interpreting Results

### Steady-State (`events_steady`)

**SLO:** p99 < 200 ms, error rate < 1 % at 100 req/s.

A regression here usually means:
- A new database migration added an unindexed column to the query path
- The connection pool is undersized for the test runner

Check `soroban_pulse_db_pool_size / soroban_pulse_db_pool_max` in Prometheus. If consistently near 1.0, increase `DB_MAX_CONNECTIONS`.

### Stress Test

Look for the **breaking point** — the request rate at which p99 exceeds 1 000 ms or error rate exceeds 5 %.

Expected degradation pattern:
```
200 req/s  → p99 ~80 ms   (normal)
400 req/s  → p99 ~150 ms  (some queuing)
600 req/s  → p99 ~300 ms  (pool contention)
800 req/s  → p99 ~800 ms  (saturation)
1000 req/s → p99 ~1500 ms (overloaded)
```

The **recovery** phase (last 2 min) must show p99 returning to within 20 % of the pre-stress baseline. A slow recovery indicates:
- Connection pool not draining (increase `DB_IDLE_TIMEOUT_SECS` or reduce `DB_MAX_CONNECTIONS`)
- Memory pressure causing GC pauses (check `soroban_pulse_process_memory_bytes`)

### Spike Test

Three phases appear in the output:

1. **Pre-spike baseline** — should match `events_steady` numbers.
2. **Spike** — p99 may reach several seconds; error rate may briefly exceed 10 %. Rate-limiting (429s) is acceptable and counted separately.
3. **Recovery** — p99 must drop below 200 ms within the 2-minute recovery window.

If recovery is slow, the connection pool is the first suspect. A 10× burst drains idle connections; they take time to be returned or the pool needs to warm up again.

### Soak Test

Compare `p95_ms_hour1` vs `p95_ms_hour12` vs `p95_ms_hour24`.

More than 10 % increase over 24 hours indicates latency drift, typically caused by:

| Symptom | Root cause | Fix |
|---|---|---|
| Gradual memory growth | Memory leak | Profile with `soroban_pulse_process_memory_bytes` |
| Table bloat | High write rate, autovacuum lagging | `VACUUM ANALYZE events;` |
| Index fragmentation | Hot-row updates | `REINDEX CONCURRENTLY idx_events_ledger;` |
| Connection pool saturation | SSE connections not returning slots | Audit SSE connection lifecycle |

### Multi-Contract Queries

Per-query-type p99 values make it easy to isolate regressions:

| High metric | Likely cause |
|---|---|
| `mc_contract_latency_ms` | Missing or bloated `idx_events_contract_id` |
| `mc_filter_latency_ms` | Missing composite index on `(event_type, ledger)` |
| `mc_pagination_latency_ms` | Deep OFFSET inefficiency (consider cursor-based pagination) |
| `mc_ndjson_latency_ms` | Serialisation bottleneck or large payload |
| `mc_tx_latency_ms` | Missing `idx_events_tx_hash` or index bloat |

### Webhook Delivery

The webhook test drives the admin replay endpoint and observes `soroban_pulse_webhook_failures_total` via `/metrics`.

A rising failure counter during the test indicates:
- Webhook receiver is rate-limiting or timing out
- Retry queue is saturated

Check `soroban_pulse_webhook_failures_total` in the Grafana dashboard.

`webhook_delivery.js` exposes three scenarios, selected with the `SCENARIO` env var:

| `SCENARIO` | Purpose | Default duration |
|---|---|---|
| `sustained` | Delivery throughput with 80 % of endpoints returning 500 | 10 m |
| `retry_storm` | Many endpoints failing simultaneously — retry backoff and DLQ growth | 10 m |
| `dlq_replay` | DLQ replay running concurrently with live delivery | 10 m |

```bash
# Sustained load with 80 % failing endpoints (default)
k6 run -e SCENARIO=sustained -e FAILURE_RATE=0.8 tests/load/webhook_delivery.js

# Retry storm — all endpoints failing at once
k6 run -e SCENARIO=retry_storm -e FAILURE_RATE=1.0 tests/load/webhook_delivery.js

# DLQ replay under concurrent load
k6 run -e SCENARIO=dlq_replay -e DLQ_REPLAY_RATE=5 tests/load/webhook_delivery.js
```

#### Retry Storm

Simulates a receiver outage where 80 % (or more) of endpoints return `500` simultaneously. The pipeline must absorb the retries without unbounded DLQ growth or starving successful deliveries.

Watch for:
- `webhook_dlq_depth` climbing monotonically — indicates retries are not being drained
- `webhook_retry_backoff_seconds` saturating at the max backoff — indicates persistent failure
- Successful delivery latency rising even though only 20 % of endpoints succeed

#### Sustained Load (10 min)

Runs a 10-minute steady load with `FAILURE_RATE` of endpoints returning 500. This is the primary SLO gate for the webhook pipeline.

**SLOs:**

| Metric | Threshold | k6 expression |
|---|---|---|
| DLQ depth | < 1000 | `webhook_dlq_depth: ['p(95)<1000', 'max<1000']` |
| Successful delivery p99 | < 5 s | `webhook_delivery_success_duration: ['p(99)<5000']` |
| Delivery error rate | < 5 % | `webhook_delivery_errors: ['rate<0.05']` |

#### DLQ Replay Under Concurrent Load

Issues `DLQ_REPLAY_RATE` replay requests per second against the admin replay endpoint while live delivery continues. Verifies that replaying the DLQ does not starve new deliveries and that the DLQ drains rather than grows.

Watch for:
- `webhook_dlq_depth` failing to decrease — replay is not keeping up with new failures
- `webhook_delivery_success_duration` p99 exceeding 5 s — replay is competing with live delivery
- Replay endpoint returning 429/503 — admin rate limit is too tight for the configured `DLQ_REPLAY_RATE`

#### Webhook SLO Thresholds

The script declares these thresholds in its `options.thresholds` block; a run that breaches any of them exits non-zero and fails CI:

```js
thresholds: {
  // DLQ must not grow unbounded under sustained failure
  webhook_dlq_depth: ['p(95)<1000', 'max<1000'],
  // Successful deliveries must still complete quickly
  webhook_delivery_success_duration: ['p(99)<5000'],
  // Overall delivery error rate
  webhook_delivery_errors: ['rate<0.05'],
}
```

**Interpreting failures:**

| Failing threshold | Likely cause | Action |
|---|---|---|
| `webhook_dlq_depth` | Retries not draining; backoff too aggressive or worker pool too small | Increase webhook worker concurrency; check `WEBHOOK_MAX_RETRIES` |
| `webhook_delivery_success_duration` | Replay or retry storm competing with live delivery | Reduce `DLQ_REPLAY_RATE`; scale webhook workers |
| `webhook_delivery_errors` | Receiver rejecting requests or pipeline saturated | Check receiver logs and `soroban_pulse_webhook_failures_total` |

---

## Performance Budgets

| Scenario | p99 budget | Error budget |
|---|---|---|
| Steady-state (100 req/s) | 200 ms | 1 % |
| Stress peak (1 000 req/s) | 2 000 ms | 5 % |
| Spike burst (10× for 30 s) | 5 000 ms | 10 % |
| Recovery (post-spike) | 200 ms | 1 % |
| Soak (24 h) | 500 ms | 1 % |
| Multi-contract queries | 400 ms | 1 % |
| Webhook delivery | 1 000 ms | 5 % |
| Webhook successful delivery (sustained, 80 % failures) | 5 000 ms | 5 % |
| Webhook DLQ depth (sustained, 10 min) | < 1 000 entries | — |
| Webhook DLQ replay (concurrent load) | 5 000 ms | 5 % |

These budgets are enforced as CI gates. A job that exceeds a budget exits with code 1 and blocks the merge.

---

## Updating Baselines

Baselines are stored in `tests/load/baselines.json` per scenario and per database size (`100k`, `1M`, `10M` events).

**When to update:**
- After a deliberate performance improvement
- After a capacity change (larger instance, more DB connections)
- After an intentional architectural change that shifts expected numbers

**Never update baselines to hide a regression.** Fix the regression, verify the fix, then promote.

**Procedure:**

```bash
# 1. Confirm the new numbers with a full test run
k6 run --out json=tests/load/results/multi_contract_raw.json \
  tests/load/multi_contract.js

# 2. Review the numbers
node tests/load/analyze_results.js summary \
  tests/load/results/multi_contract_raw.json --scenario multi_contract

# 3. Promote to baselines
node tests/load/analyze_results.js promote multi_contract \
  tests/load/results/multi_contract_raw.json 1M

# 4. Commit
git add tests/load/baselines.json
git commit -m "perf(load): update multi_contract baseline — composite index added"
```

---

## Webhook Delivery Setup

`webhook_delivery.js` needs a real or stubbed webhook receiver plus admin credentials.

### Option 1: WireMock stub

```bash
# Start WireMock
docker run -p 8080:8080 wiremock/wiremock --global-response-templating

# Register a catch-all mapping
curl -X POST http://localhost:8080/__admin/mappings \
  -d '{"request":{"method":"POST","urlPattern":"/.*"},"response":{"status":200}}'

# Run the load test
k6 run \
  -e ADMIN_API_KEY=your-admin-key \
  -e WEBHOOK_RECEIVER=http://localhost:8080/webhook \
  tests/load/webhook_delivery.js
```

### Option 2: webhook.site

```bash
# 1. Visit https://webhook.site and copy your unique URL
export WEBHOOK_URL="https://webhook.site/<your-uuid>"

# 2. Start the service with the webhook configured
WEBHOOK_URL=$WEBHOOK_URL WEBHOOK_SECRET=test-secret cargo run

# 3. Run the test
k6 run \
  -e ADMIN_API_KEY=your-admin-key \
  -e WEBHOOK_RECEIVER=$WEBHOOK_URL \
  tests/load/webhook_delivery.js
```

---

## Troubleshooting

### k6 "connection refused"

The service is not running or is listening on a different port.

```bash
# Verify service health
curl http://localhost:3000/healthz/ready

# Override BASE_URL
k6 run -e BASE_URL=http://localhost:8080 tests/load/events.js
```

### "VUs failed to be initialized" errors in k6

The `preAllocatedVUs` count is too low for the test runner machine. k6 will use `maxVUs` as the cap; this warning is informational and does not affect results.

### regression_check.js exits 2 with "metric not found"

The k6 JSON summary does not contain the expected metric. This usually means:
- The summary was produced by a different scenario than the one passed to `regression_check.js`
- k6 did not run long enough to emit any samples

Pass the correct `--scenario` flag and ensure the test completed at least one iteration.

### Soak test killed by CI timeout

GitHub-hosted runners have a 6-hour job limit. Use a self-hosted runner for full 24-hour soaks, or override `soak_duration` to a shorter interval:

```yaml
# In the workflow_dispatch inputs
soak_duration: 6h
```

### analyze_results.js trend shows no data

The `index.jsonl` file is missing or does not contain entries for the requested scenario. Archive at least one run first:

```bash
scripts/perf_regression.sh archive events_steady \
  tests/load/results/events_steady_raw.json 1M
```

### Memory growth in soak test exceeds threshold

1. Check `soroban_pulse_process_memory_bytes` in Prometheus over the soak window.
2. If RSS grows linearly with time, run a flamegraph against a live instance:
   ```bash
   cargo flamegraph --bin soroban-pulse
   ```
3. Common culprits: unbounded broadcast channel receiver list, leaking SSE connections, growing in-memory query cache with no eviction.

---

## Related Docs

- [Performance Tuning Guide](performance-tuning.md) — connection pool, query, and index tuning
- [Performance Regression Testing](performance-regression-testing.md) — overview of regression detection approach
- [Capacity Planning](capacity-planning.md) — scaling and resource forecasting
- [Deployment Guide](deployment.md) — production configuration
- [Grafana Dashboard](grafana-dashboard.json) — import for live SLO monitoring
- [Runbook: DB Pool Exhaustion](runbooks/db-pool-exhaustion.md)
- [Runbook: Indexer Lag](runbooks/indexer-lag.md)
- [Runbook: SSE Connections](runbooks/sse-connections.md)
