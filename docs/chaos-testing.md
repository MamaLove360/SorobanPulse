# Chaos Engineering Tests (Issue #921)

## Overview

Chaos tests verify that Soroban Pulse degrades gracefully — and recovers — when
its dependencies misbehave: the Stellar RPC endpoint returns errors or hangs,
the database is under lock contention, or the indexer stalls entirely. The
goal is not to find new bugs on every run (that's what fuzzing and property
tests are for — see [property-based-testing.md](property-based-testing.md)) but to pin
down *known* failure/recovery contracts so a future change can't silently
break them.

Two test files implement this:

| File | Focus | Origin |
|---|---|---|
| [`tests/chaos_tests.rs`](../tests/chaos_tests.rs) | RPC failures, DB duplicate/idempotency, latency, advisory-lock contention, graceful degradation | Issue #655 |
| [`tests/resilience/mod.rs`](../tests/resilience/mod.rs) | Network-partition-shaped subset (RPC unreachable/recovers, HTTP availability during stall, health-endpoint degradation) | Companion suite, polling-based assertions |

Both suites run against a real PostgreSQL instance (via `sqlx::test`, which
spins up an isolated schema per test) and a mocked RPC client — no Docker or
Toxiproxy required for the default run.

## Design principle: fault injection without a network proxy

Rather than routing traffic through an actual TCP proxy (Toxiproxy) to drop
packets or add latency, these tests inject faults at the `RpcClient` trait
boundary:

```rust
#[async_trait::async_trait]
impl RpcClient for MockRpcClient {
    async fn get_latest_ledger(&self, _url: &str) -> Result<u64, String> { ... }
    async fn get_events(&self, _url: &str, _start: u64, _cursor: Option<String>)
        -> Result<GetEventsResult, String> { ... }
}
```

`MockRpcClient` (defined locally in each test file) queues up `Result<T, String>`
responses with `push_get_events(...)` / `push_latest_ledger(...)`, and
`chaos_tests.rs`'s variant additionally supports `set_delay(Some(duration))`
to sleep before responding — simulating network latency without a real
socket. The indexer under test is a real `Indexer<MockRpcClient>` running
against the real Postgres pool, so the DB-side behavior (writes,
`ON CONFLICT DO NOTHING`, advisory locks) is exercised for real; only the RPC
transport is faked.

This keeps the default suite fast and Docker-free, which is why it runs in
standard CI. A second, optional layer exists for true network-level chaos —
see [Toxiproxy integration tests](#toxiproxy-integration-tests-chaos_integration) below.

## What's covered today

### Network partitions / RPC failures

`chaos_rpc_single_failure_propagates_error`, `chaos_rpc_server_error_is_handled`,
`chaos_rpc_malformed_response_is_handled`, `rpc_unreachable_returns_error`
(resilience) — connection-refused, HTTP-500-shaped, and malformed-JSON-shaped
RPC errors all propagate as `Err` from `fetch_and_store_events`, and none of
them write partial data to the `events` table.

### Delayed response scenarios

`chaos_network_latency_events_still_indexed`, `chaos_network_latency_then_error`,
`chaos_network_latency_sequential_requests` — inject 50–200ms of artificial
delay before each mocked RPC response and assert (a) the elapsed wall time
reflects the injected delay, and (b) indexing still completes correctly once
the response arrives.

### Partial failure injection

`chaos_rpc_intermittent_faults` alternates success/failure across six calls
and asserts the event count in the DB matches exactly the number of
successful calls — no over- or under-counting from the failures interleaved
between them. `chaos_error_then_empty_success_no_data_loss` covers the
error → empty-success → data-success sequence specifically, since an empty
success response is a separate code path from a populated one.

### Recovery behavior

- `chaos_rpc_five_failures_then_recovery` / `rpc_multiple_failures_then_recovery`:
  N consecutive failures followed by a success — the indexer must still
  advance `latest_ledger` and store the event on the first successful call
  after an outage, with no backlog corruption from the failed attempts.
- `chaos_health_recovers_after_indexer_resumes` / `health_reports_ok_after_indexer_resumes`:
  `/healthz/ready` returns to `{"status": "ok", "indexer": "ok"}` once
  `HealthState::update_last_poll()` is called again after a stall.

### Database-level behavior

- `chaos_db_duplicate_event_is_idempotent`: the same event delivered twice
  (e.g. after a retried RPC call) results in exactly one row, relying on the
  `ON CONFLICT DO NOTHING` production strategy rather than application-level
  deduplication.
- `chaos_db_large_event_batch`: a 100-event batch in a single RPC response
  stores completely without exhausting the pool or truncating the batch.

### Indexer lock contention (multi-replica)

`chaos_advisory_lock_only_one_holder` and
`chaos_advisory_lock_released_on_connection_drop` exercise
`pg_try_advisory_lock` / `pg_advisory_unlock` directly: only one session can
hold the leader lock at a time, and a crashed replica's lock is reclaimable
once its session drops. This is the mechanism behind
`soroban_pulse_indexer_is_leader` (see [metrics-reference.md](metrics-reference.md))
and the multi-replica deployment model described in
[connection-pool.md](connection-pool.md) and
[multi-deployment-architecture.md](multi-deployment-architecture.md).

### Graceful degradation

`chaos_http_available_during_indexer_failure` / `http_server_available_during_indexer_stall`:
`GET /v1/events` still returns `200` from the real router even with no
indexer running at all. `chaos_health_degrades_when_indexer_stalls` /
`health_reports_degraded_when_indexer_stalled`: `/healthz/ready` reports
`503` with `{"status": "degraded", "indexer": "stalled"}` once
`indexer_stall_timeout_secs` is exceeded — the HTTP layer and the indexer
loop fail independently, by design.

## Running the tests

```bash
# Full chaos suite (issue #655 tests)
cargo test --test chaos_tests -- --test-threads=1

# Resilience / network-partition subset
cargo test --test resilience -- --test-threads=1

# A single scenario
cargo test --test chaos_tests chaos_rpc_five_failures_then_recovery
```

`--test-threads=1` isn't required for correctness (each `sqlx::test` gets its
own schema) but keeps output readable when a test fails, since these tests
assert on wall-clock timing in places (the latency tests).

Both suites require a reachable `DATABASE_URL` (or the ambient Postgres
service in CI) because `#[sqlx::test]` provisions a database per test case
from `./migrations`.

### CI wiring

`.github/workflows/ci.yml` runs both jobs against a `postgres:15` service
container on every push/PR:

```yaml
resilience-tests:
  steps:
    - run: cargo test --test resilience

chaos-tests:
  name: Chaos Engineering Tests
  steps:
    - run: cargo test --test chaos_tests
```

Neither job is `continue-on-error`, so a chaos regression blocks merge like
any other test failure.

## Toxiproxy integration tests (`CHAOS_INTEGRATION`)

The mocked-transport tests above cover application-level fault handling but
can't exercise real TCP-level failure modes (mid-response connection resets,
partial reads, actual OS-level timeouts). Tests that need a real proxy are
gated behind the `CHAOS_INTEGRATION` environment variable so they don't run
by default:

```bash
CHAOS_INTEGRATION=1 cargo test --test chaos_tests -- --test-threads=1
```

To exercise this locally with [Toxiproxy](https://github.com/Shopify/toxiproxy):

```bash
# Start Toxiproxy and point the app's STELLAR_RPC_URL at the proxy port
toxiproxy-server &
toxiproxy-cli create stellar_rpc -l 127.0.0.1:26657 -u <real-rpc-host>:443

# Inject latency
toxiproxy-cli toxic add stellar_rpc -t latency -a latency=500

# Inject a full network partition (close the connection)
toxiproxy-cli toxic add stellar_rpc -t timeout -a timeout=0
```

This mode is not currently wired into CI — it's a local/manual verification
path, since it requires a running proxy process and is inherently slower and
flakier than the mocked-transport suite.

## Metrics collection during chaos runs

`chaos_http_available_during_indexer_failure` and the corresponding
resilience tests already construct a real `PrometheusHandle` via
`soroban_pulse::metrics::init_metrics()` and pass it into
`routes::create_router`, so the metrics pipeline is live during the test —
but today none of the chaos tests scrape and assert on the rendered output.
Relevant counters that *should* move during a chaos run (see
[metrics-reference.md](metrics-reference.md) for the full reference):

| Metric | Expected during... |
|---|---|
| `soroban_pulse_rpc_errors_total` | any RPC-failure scenario |
| `soroban_pulse_events_indexed_total` | recovery scenarios, after the failing calls |
| `soroban_pulse_indexer_lag_ledgers` | latency / intermittent-fault scenarios |
| `soroban_pulse_indexer_is_leader` | advisory-lock contention tests |

To assert on these in a test, render the handle and grep the Prometheus text
exposition format:

```rust
let rendered = prometheus_handle.render();
assert!(rendered.contains("soroban_pulse_rpc_errors_total 1"));
```

This is a known gap relative to the issue #921 checklist (see below) — it's
straightforward to add per-scenario once a convention for asserting on
rendered metrics text is settled, but no chaos test does it yet.

## Coverage vs. the issue #921 checklist

| Checklist item | Status |
|---|---|
| Chaos test framework for network partitions | ✅ `tests/resilience/mod.rs`, `tests/chaos_tests.rs` |
| Database failure simulations | ⚠️ Partial — duplicate/idempotency and large-batch behavior are covered; a true connection-loss-mid-query simulation is not |
| Connection pool exhaustion tests | ❌ Not yet a chaos scenario. `src/db_pool/monitor.rs` tracks exhaustion (`soroban_pulse_db_pool_exhaustion_alerts_total`, `DBPoolExhaustion` alert in `docs/alerts.yml`) but there's no test that drives the pool to exhaustion and asserts on recovery |
| Delayed response scenarios | ✅ `chaos_network_latency_*` |
| Partial failure injection | ✅ `chaos_rpc_intermittent_faults`, `chaos_error_then_empty_success_no_data_loss` |
| Metrics collection during chaos tests | ⚠️ Metrics pipeline is live in the relevant tests but not yet asserted on — see above |
| Assertions for recovery behavior | ✅ Recovery is asserted in every failure-mode test above |
| Document chaos testing in `docs/chaos-testing.md` | ✅ This document |

## Related documentation

- [troubleshooting.md](troubleshooting.md) — operator-facing playbook for the failure modes these tests encode
- [connection-pool.md](connection-pool.md) — pool sizing and exhaustion behavior
- [sli-slo.md](sli-slo.md) — how sustained degradation shows up in error budgets
- [load-testing-runbook.md](performance-tuning.md#load-testing-runbook) — throughput/latency under load rather than under fault
- [metrics-reference.md](metrics-reference.md) — full metric catalog referenced above
