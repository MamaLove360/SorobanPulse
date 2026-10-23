# Module Map

This document groups every module under `src/` by domain so you can quickly
find the right file when reading code or planning a contribution.

The top-level project structure is described in the [README Project Structure section](../README.md#project-structure).
For Stellar/Soroban terminology used throughout, see the [Glossary](glossary.md).

---

## Domain Index

| Domain | Modules |
|---|---|
| [Indexing & Ingestion](#indexing--ingestion) | core polling loop, backfill, dedup, cursor handling |
| [API & Routing](#api--routing) | HTTP routes, request handlers, OpenAPI |
| [Streaming (SSE)](#streaming-sse) | Server-Sent Events, SSE ring buffer, query streaming |
| [Notifications](#notifications) | Webhooks, email, SMS, push, chat integrations |
| [Storage & Database](#storage--database) | DB pool, query building, caching, partitioning, archival |
| [Observability](#observability) | Metrics, tracing, logging, alerting |
| [Security](#security) | Auth middleware, rate limiting, encryption, zero-trust |
| [Subscriptions & Filters](#subscriptions--filters) | Subscription management, filter DSL, event routing |
| [Stellar / Soroban Primitives](#stellar--soroban-primitives) | TOID parsing, XDR, ScVal, networks |
| [Data & Analytics](#data--analytics) | Aggregations, time series, anomaly detection, ML |
| [Infrastructure & Cloud](#infrastructure--cloud) | Message queues, cloud replication, cloud providers |
| [Multi-tenancy & SaaS](#multi-tenancy--saas) | Tenant isolation, GDPR, anonymisation, billing |
| [Code Generation & Tooling](#code-generation--tooling) | OpenAPI codegen, subscription scaffolding, ABI |
| [Middleware](#middleware) | Axum middleware stack |
| [Models](#models) | Shared data types |
| [Costs](#costs) | Cost tracking and forecasting |
| [Warehouse](#warehouse) | Data warehouse export (BigQuery, Snowflake) |
| [Binaries (`src/bin/`)](#binaries-srcbin) | Stand-alone utility binaries |

---

## Indexing & Ingestion

| Module | Purpose |
|---|---|
| `indexer.rs` | Main background task. Polls the Soroban RPC `getEvents` method in a loop, inserts new events, manages the advisory lock leader election. |
| `rpc_client.rs` | Thin HTTP client wrapper around the Soroban JSON-RPC endpoint. |
| `rpc_meta.rs` | Metadata helpers for RPC responses (pagination tokens, ledger info). |
| `backfill.rs` | One-shot backfill job for filling historical gaps within the RPC retention window. |
| `scheduled_replay.rs` | Scheduled / admin-triggered event replay for a ledger range. |
| `event_replay.rs` | Core replay logic: re-fetches and re-processes events from a stored cursor range. |
| `dedup.rs` | In-process bloom-filter deduplication before DB upsert. |
| `event_dedup_replicas.rs` | Cross-replica deduplication coordination for multi-instance deployments. |
| `bloom_filter.rs` | Bloom filter implementation used by `dedup.rs`. |
| `cursor_expiry_handler.rs` | Handles the case where the indexer's cursor has fallen outside the RPC retention window. |
| `ledger_hashes.rs` | Stores and looks up ledger hash values for integrity checks. |
| `normalizer.rs` | Normalises raw RPC event payloads (XDR decode, field coercion) before DB insert. |
| `advisory_lock.rs` | PostgreSQL session-level advisory lock used for single-leader election among replicas. |

---

## API & Routing

| Module | Purpose |
|---|---|
| `routes.rs` | Axum router: assembles all route handlers, versioned `/v1/` paths, deprecation aliases, admin routes, and static file serving. |
| `handlers.rs` | All HTTP request handler functions (events CRUD, SSE, admin, health, metrics, docs). |
| `integration_handlers.rs` | Handlers for third-party integration endpoints (GitHub, Slack, etc.). |
| `health_check.rs` | `/healthz/live` and `/healthz/ready` endpoint logic with DB ping and indexer-stall detection. |
| `resources.rs` | Static embedded assets (dashboard HTML, OpenAPI JSON). |
| `dashboard.rs` | Serves the built-in web dashboard at `/ui`. |
| `idempotency.rs` | Idempotency key middleware and store for mutating requests. |
| `graphql.rs` | GraphQL query layer (alternative to REST). |
| `graphql_subscriptions.rs` | GraphQL subscription support over WebSocket. |

---

## Streaming (SSE)

| Module | Purpose |
|---|---|
| `streaming_response.rs` | Core SSE broadcast channel: receives new events from the indexer and fans them out to all connected SSE clients. |
| `sse_ring_buffer.rs` | Fixed-size ring buffer used for SSE `Last-Event-ID` replay on reconnect. |
| `query_streaming.rs` | Streaming query responses for large result sets (NDJSON). |
| `stream_statistics.rs` | Per-stream statistics (connection count, message rate). |
| `push_preload.rs` | HTTP/2 server push and `Link: rel=preload` header injection. |
| `http3_support.rs` | Experimental HTTP/3 / QUIC transport layer. |

---

## Notifications

| Module | Purpose |
|---|---|
| `webhook.rs` | Webhook delivery: HTTP POST with retry, HMAC signing, and circuit-breaking. |
| `webhook_signing.rs` | HMAC-SHA256 signature generation and header attachment for webhook payloads. |
| `webhook_verification.rs` | Signature verification helpers (for inbound webhook receipts). |
| `webhook_circuit_breaker.rs` | Circuit breaker that stops retrying failing webhook endpoints. |
| `webhook_priority.rs` | Priority queue for webhook delivery ordering. |
| `webhook_logging.rs` | Structured logging of webhook request/response pairs. |
| `webhook_template.rs` | Handlebars template rendering for webhook payloads. |
| `email.rs` | Email delivery via SMTP / transactional providers. |
| `sms.rs` | SMS delivery via Twilio/provider abstraction. |
| `push_notification.rs` | Mobile push notifications (APNs, FCM). |
| `slack.rs` | Slack message delivery integration. |
| `discord.rs` | Discord webhook integration. |
| `teams.rs` | Microsoft Teams webhook integration. |
| `telegram.rs` | Telegram Bot API integration. |
| `pagerduty.rs` | PagerDuty incident creation and update. |
| `github.rs` | GitHub issue/comment creation from events. |
| `oncall.rs` | On-call scheduling and rotation for alert routing. |
| `alert_manager.rs` | Alertmanager-compatible webhook receiver and rule engine. |
| `notification_delivery.rs` | Delivery orchestrator: selects channel, applies rate limits, records receipts. |
| `notification_channel.rs` | Channel abstraction trait shared by all delivery backends. |
| `notification_admin.rs` | Admin API handlers for managing notification channels and rules. |
| `notification_batching.rs` | Batching of high-frequency notifications into digest messages. |
| `notification_dedup.rs` | Deduplication of duplicate notification triggers. |
| `notification_formatter.rs` | Formats event data for human-readable notification bodies. |
| `notification_rate_limit.rs` | Per-channel rate limiting for notification delivery. |

---

## Storage & Database

| Module | Purpose |
|---|---|
| `db.rs` | Database connection pool setup, migration runner, and pool health helpers. |
| `query_builder.rs` | Dynamic SQL query construction for the events endpoint (filters, pagination). |
| `query_cache.rs` | In-memory LRU cache for frequent read queries. |
| `query_plan_cache.rs` | PostgreSQL prepared-statement plan caching to reduce parse overhead. |
| `query_optimizer.rs` | Query rewrite rules that choose the most efficient index path. |
| `query_profiler.rs` | Slow-query detection and EXPLAIN ANALYZE integration. |
| `serialization_cache.rs` | Caches serialised JSON responses to avoid repeated serde work. |
| `saved_queries.rs` | User-defined saved query storage and retrieval. |
| `partition_manager.rs` | Manages time-based table partitions for the `events` table. |
| `archiver.rs` | Moves expired events to cold storage or deletes them per retention policy. |
| `pruner.rs` | Scheduled row deletion for expired data (simpler than full archival). |
| `index_monitor.rs` | Monitors index bloat and usage; warns when an index is unused or missing. |
| `stats_refresh.rs` | Triggers `ANALYZE` on stale tables to keep the query planner accurate. |
| `statistics_management.rs` | Advanced PostgreSQL statistics configuration (per-column targets). |
| `config_validation.rs` | Validates all environment configuration values at startup, emitting structured errors for missing or out-of-range settings. |
| `db_config_advisor.rs` | Reads `pg_settings` and recommends tuning parameters. |
| `retention_tiers.rs` | Multi-tier data retention: hot / warm / cold storage transitions. |
| `data_integrity.rs` | Consistency checks: detects gaps, orphaned rows, and checksum mismatches. |
| `data_quality.rs` | Validates event payload shapes and flags malformed rows. |
| `backup_verification.rs` | Verifies that PostgreSQL backup dumps are restorable and complete. |
| `replica_monitor.rs` | Tracks replication lag on PostgreSQL read replicas. |
| `db_pool/` | Unified connection pool: `connect` / `DbPool::init` (single init path), `monitor` (utilization + wait-time metrics), `adaptive` (sizing recommendations, health checks), `management` (admin API helpers). |
| `conditional_get.rs` | HTTP conditional GET (`ETag` / `Last-Modified`) support for caching. |
| `http_caching.rs` | HTTP cache control header injection (`Cache-Control`, `Vary`). |
| `compression_config.rs` | Configures response compression (gzip/zstd) levels and thresholds. |
| `parquet_export.rs` | Exports event data in Parquet format for analytics pipelines. |
| `bulk_export.rs` | Large-batch CSV/NDJSON export with streaming and progress tracking. |
| `batch_operations.rs` | Bulk write operations (batch insert, batch delete) with transaction management. |

---

## Observability

| Module | Purpose |
|---|---|
| `metrics.rs` | All Prometheus metric registrations and update helpers. |
| `observability.rs` | Unified observability bootstrap: wires tracing, metrics, and logging. |
| `distributed_tracing.rs` | OpenTelemetry span creation and propagation for cross-service tracing. |
| `resource_metrics.rs` | Process-level metrics (RSS memory, CPU) read from `/proc`. |
| `slo_tracker.rs` | Tracks SLO burn rate and emits alerts when error budgets are consumed. |
| `stream_statistics.rs` | Per-SSE-connection statistics (also listed under Streaming). |
| `log_analysis_tool.rs` | Parses structured JSON logs and generates summaries. |
| `audit_logging.rs` | Structured audit log emission for security-relevant actions. |
| `audit_trail.rs` | Immutable append-only audit trail stored in the database. |
| `anomaly_detection.rs` | Statistical anomaly detection on event rates for alerting. |
| `time_series.rs` | Time-series aggregation queries and materialised view management. |
| `capacity_planning.rs` | Growth forecasting and resource capacity recommendations. |
| `prometheus_remote_write.rs` | Pushes metrics to a remote Prometheus-compatible endpoint. |

---

## Security

| Module | Purpose |
|---|---|
| `zero_trust.rs` | Zero-trust policy engine: evaluates request context against access rules. |
| `encryption.rs` | AES-GCM encryption/decryption for sensitive fields (webhook secrets, API keys). |
| `reencrypt.rs` | Key rotation: re-encrypts stored secrets under a new key. |
| `rate_limiter.rs` | Per-IP and per-API-key rate limiting backed by an in-memory sliding window. |
| `content_filter.rs` | Filters event data for prohibited content patterns. |
| `anonymization.rs` | Replaces PII in event payloads with pseudonymous tokens (GDPR). |
| `gdpr.rs` | GDPR right-to-erasure and data subject request handling. |
| `compliance_report.rs` | Generates SOC 2 / GDPR compliance evidence reports. |
| `xdr_validation.rs` | Validates that XDR fields in incoming RPC data are well-formed. |
| `schema_validator.rs` | Validates event payloads against registered contract schemas. |
| `crypto/quantum_ready.rs` | Experimental post-quantum cryptography primitives. |

---

## Subscriptions & Filters

| Module | Purpose |
|---|---|
| `subscriptions.rs` | Subscription CRUD, matching, and lifecycle management. |
| `subscription_validator.rs` | Validates subscription filter expressions before saving. |
| `filter_dsl.rs` | Domain-specific language for event filter expressions (e.g. topic matching). |
| `event_router.rs` | Routes indexed events to matching subscriptions and notification channels. |
| `event_handler.rs` | Per-event processing: applies filters, triggers notifications, updates metrics. |
| `event_tagging.rs` | Attaches user-defined tags to events for filtering and display. |
| `cross_chain_correlation.rs` | Correlates events across multiple chains/networks. |

---

## Stellar / Soroban Primitives

| Module | Purpose |
|---|---|
| `toid.rs` | Parses Stellar TOID (Transaction Order ID) strings into structured `EventOrdinal` values for deterministic event ordering. See [Glossary — TOID](glossary.md#toid-transaction-order-id). |
| `scval_format.rs` | Converts XDR-decoded `ScVal` values to human-readable strings. See [Glossary — ScVal](glossary.md#scval-stellar-contract-value). |
| `xdr_validation.rs` | Validates raw base64-encoded XDR payloads from the RPC. See [Glossary — XDR](glossary.md#xdr-external-data-representation). |
| `networks.rs` | Stellar network passphrase constants (mainnet, testnet, futurenet). |
| `token_events.rs` | Parses SAC / SEP-41 token events (`transfer`, `mint`, `burn`, `approve`). See [Glossary — SAC](glossary.md#sac-stellar-asset-contract). |
| `account_events.rs` | Tracks account-level activity derived from indexed events. |
| `contract_metadata.rs` | Stores and retrieves human-readable contract metadata (name, description). |
| `contract_specs.rs` | Parses Soroban contract spec (ABI) from contract storage. |
| `contract_versions.rs` | Tracks deployed contract version history. |
| `abi.rs` | Contract ABI storage and lookup for decode-time schema resolution. |

---

## Data & Analytics

| Module | Purpose |
|---|---|
| `aggregation.rs` | Computes aggregate statistics over event sets (count, volume). |
| `event_aggregation.rs` | Higher-level event aggregation with grouping and windowing. |
| `time_series.rs` | Time-series queries and materialised views (also listed under Observability). |
| `anomaly_detection.rs` | Statistical outlier detection on event streams (also listed under Observability). |
| `stream_statistics.rs` | Live statistics on SSE stream connections (also listed under Streaming). |
| `ml_integration.rs` | ML model inference integration for event classification and prediction. |
| `model_serving.rs` | Serves embedded ML models for real-time inference. |
| `financial_accuracy.rs` | High-precision decimal arithmetic for financial event values. |
| `lua_transform.rs` | Lua scripting engine for user-defined event transformations. |
| `event_simulator.rs` | Generates synthetic events for testing and demos. |
| `event_compression.rs` | Compresses repeated event payloads to reduce storage. |

---

## Infrastructure & Cloud

| Module | Purpose |
|---|---|
| `kafka.rs` | Publishes events to an Apache Kafka topic. |
| `sqs.rs` | Publishes events to an AWS SQS queue. |
| `kinesis.rs` | Publishes events to an AWS Kinesis stream. |
| `eventbridge.rs` | Publishes events to AWS EventBridge. |
| `event_hubs.rs` | Publishes events to Azure Event Hubs. |
| `pubsub.rs` | Publishes events to Google Cloud Pub/Sub. |
| `queue_publisher.rs` | Abstraction layer over all queue backends (Kafka, SQS, Kinesis, …). |
| `cloud_provider.rs` | Detects current cloud environment and provides provider-specific helpers. |
| `cloud_replication.rs` | Replicates event data to object storage (S3, GCS, Azure Blob). |
| `deployment_orchestrator.rs` | Manages rolling deployments and configuration drift detection. |
| `graceful_shutdown.rs` | Handles SIGTERM/SIGINT: drains in-flight requests and flushes buffers. |
| `retry_policy.rs` | Configurable exponential back-off retry policies used across all outbound calls. |
| `http3_support.rs` | HTTP/3 / QUIC transport support (experimental; also listed under Streaming). |

---

## Multi-tenancy & SaaS

| Module | Purpose |
|---|---|
| `multi_tenancy.rs` | Tenant context extraction and per-tenant data isolation. |
| `saas_platform.rs` | SaaS plan management, feature gating, and usage tracking. |
| `gdpr.rs` | GDPR data subject rights (also listed under Security). |
| `anonymization.rs` | PII pseudonymisation (also listed under Security). |
| `timezone_locale.rs` | Per-tenant timezone and locale preferences. |
| `feature_flags.rs` | Runtime feature flag evaluation and rollout percentage controls. |

---

## Code Generation & Tooling

| Module | Purpose |
|---|---|
| `codegen/mod.rs` | Code generation entry point and shared utilities. |
| `codegen/openapi.rs` | Generates OpenAPI 3.0 JSON from route and model annotations. |
| `codegen/webhook.rs` | Generates typed webhook handler stubs for client SDKs. |
| `codegen/subscription.rs` | Generates subscription filter scaffold code. |
| `codegen/filter.rs` | Generates filter DSL parsers for various target languages. |
| `codegen/tests.rs` | Tests for the code generation output. |

---

## Middleware

All middleware lives under `src/middleware/` and is assembled by `middleware/builder.rs`.

| Module | Purpose |
|---|---|
| `middleware/builder.rs` | Constructs the full Axum middleware stack in the correct order. |
| `middleware/auth.rs` | API key authentication (`Authorization: Bearer` / `X-Api-Key`). |
| `middleware/rate_limit.rs` | Request-level rate limiting (delegates to `rate_limiter.rs`). |
| `middleware/security_headers.rs` | Injects security headers (CSP, HSTS, X-Frame-Options, …). |
| `middleware/request_id.rs` | Assigns a unique `X-Request-ID` to every request. |
| `middleware/tracing.rs` | Attaches OpenTelemetry span context to each request. |
| `middleware/response_middleware.rs` | Post-handler response transformations (deprecation headers, CORS). |
| `middleware/http_utils.rs` | Shared HTTP helper functions used by multiple middleware layers. |
| `middleware/ip_access.rs` | IP allowlist / blocklist enforcement. |
| `middleware/tenant.rs` | Extracts and validates the tenant identifier from request headers. |

---

## Models

| Module | Purpose |
|---|---|
| `models/event.rs` | `Event`, `EventType`, `ContractId`, pagination params, and all event-related response shapes. |
| `models/notification.rs` | `NotificationFormat`, `NotificationPriority`, and notification request/response types. |
| `models/integration.rs` | Request/response types for third-party integration configurations. |
| `models/mod.rs` | Re-exports all model types. |

---

## Costs

| Module | Purpose |
|---|---|
| `costs/calculator.rs` | Calculates per-tenant infrastructure costs from usage metrics. |
| `costs/forecast.rs` | Projects future costs based on growth trends. |
| `costs/report.rs` | Generates cost breakdown reports. |
| `costs/database.rs` | Database-specific cost tracking (storage, IOPS). |
| `costs/compute.rs` | Compute-specific cost tracking (CPU, memory). |
| `costs/models.rs` | Shared cost data types. |

---

## Warehouse

| Module | Purpose |
|---|---|
| `warehouse/mod.rs` | Warehouse export abstraction and scheduling. |
| `warehouse/bigquery.rs` | Exports events to Google BigQuery. |
| `warehouse/snowflake.rs` | Exports events to Snowflake. |
| `warehouse/transform.rs` | Transforms event rows into warehouse-ready schemas. |
| `warehouse/incremental.rs` | Incremental (delta) export tracking. |
| `warehouse/schema_mapping.rs` | Maps SorobanPulse columns to target warehouse schemas. |

---

## Binaries (`src/bin/`)

| Binary | Purpose |
|---|---|
| `gen_openapi.rs` | Dumps the OpenAPI spec to stdout. Run with `cargo run --bin gen_openapi`. |
| `dump_openapi.rs` | Alternative OpenAPI dump with additional formatting options. |
| `schema_cli.rs` | CLI for inspecting and validating contract schemas. |
| `gen_subscription_scaffold.rs` | Generates a typed subscription handler scaffold for a given contract. |
| `gen_postman.rs` | Generates a Postman collection from the OpenAPI spec. |
| `pg_tuning_advisor.rs` | Connects to the DB and prints recommended PostgreSQL `postgresql.conf` tuning parameters. |

---

## Where Do I Start?

| Contribution type | Start here |
|---|---|
| Fix a bug in event pagination or filtering | `src/handlers.rs`, `src/query_builder.rs`, `src/models/event.rs` |
| Add a new API endpoint | `src/routes.rs`, `src/handlers.rs` |
| Change the events table schema | `migrations/` (new file), then update `src/models/event.rs` and `src/handlers.rs` |
| Fix or extend the indexer | `src/indexer.rs`, `src/rpc_client.rs` |
| Add or fix a notification channel | `src/notification_delivery.rs`, the relevant `src/<channel>.rs` module |
| Work on SSE / streaming | `src/streaming_response.rs`, `src/sse_ring_buffer.rs` |
| Add a new metric | `src/metrics.rs`, then emit it from the relevant module |
| Security / auth change | `src/middleware/auth.rs`, `src/zero_trust.rs` |
| Documentation-only change | `docs/` — no Rust build required |
| SDK change | `sdk/typescript/`, `sdk/python/`, or `sdk/go/` |
| Dashboard / frontend change | `dashboard/src/` or `frontend/src/` |

---

*This map is checked by CI. If you add a new top-level `src/*.rs` module, add
it to the appropriate domain section above — the CI script `scripts/check-module-map.sh`
will fail the build otherwise.*
