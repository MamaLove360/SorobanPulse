# Environment Variable Reference


This file is **auto-generated** from `src/config.rs`. Do not edit it by hand — run `cargo run --bin gen_env_docs -- --write` instead.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `(required)` | PostgreSQL connection string. Also loadable from a file path via DATABASE_URL_FILE. |
| `DATABASE_REPLICA_URL` | `(none)` | Optional read-replica URL. When set, HTTP handlers use this pool; the indexer uses the primary. |
| `STELLAR_RPC_URL` | `https://soroban-testnet.stellar.org` | Soroban RPC endpoint. Must be HTTPS unless ALLOW_INSECURE_RPC=true. |
| `STELLAR_RPC_FALLBACK_URLS` | `(none)` | Comma-separated list of fallback RPC endpoints tried when the primary fails. |
| `STELLAR_RPC_HEADERS` | `(none)` | Semicolon-separated "Name: Value" pairs injected into every RPC request. Values are never logged. |
| `START_LEDGER` | `0` | Ledger sequence to start indexing from. 0 = start from the latest ledger at boot. |
| `START_LEDGER_FALLBACK` | `false` | When true, fall back to the latest ledger if START_LEDGER is no longer available on the RPC. |
| `PORT` | `3000` | HTTP server listening port. |
| `API_KEY` | `(disabled)` | Optional API key for request authentication. Also accepts API_KEY_SECONDARY for rotation. When unset all requests pass through. |
| `ADMIN_API_KEY` | `(disabled)` | Key required for /v1/admin/* endpoints, independent of API_KEY. Also accepts ADMIN_API_KEY_SECONDARY. When unset, admin endpoints fall back to API_KEY. |
| `DB_MAX_CONNECTIONS` | `10` | Maximum number of connections in the PostgreSQL connection pool. |
| `DB_MIN_CONNECTIONS` | `2` | Minimum number of idle connections kept open in the PostgreSQL pool. |
| `DB_IDLE_TIMEOUT_SECS` | `600` | Seconds before an idle connection is closed and removed from the pool. |
| `DB_MAX_LIFETIME_SECS` | `1800` | Maximum lifetime in seconds for a pooled database connection. |
| `DB_TEST_BEFORE_ACQUIRE` | `true` | When true, a cheap health-check query is run before returning a connection from the pool. |
| `BEHIND_PROXY` | `false` | Set to true when running behind a reverse proxy so the real client IP is extracted from X-Forwarded-For. |
| `RPC_CONNECT_TIMEOUT_SECS` | `5` | TCP connection timeout in seconds for RPC requests. |
| `RPC_REQUEST_TIMEOUT_SECS` | `30` | Total request timeout in seconds for RPC calls. |
| `ALLOWED_ORIGINS` | `*` | Comma-separated CORS allowed origins. Wildcard (*) is rejected in production-like environments. |
| `RATE_LIMIT_PER_MINUTE` | `60` | Maximum HTTP requests per IP per minute. Set to 0 to disable rate limiting. |
| `INDEXER_LAG_WARN_THRESHOLD` | `100` | Number of ledgers behind the tip before a warning is emitted. |
| `INDEXER_STALL_TIMEOUT_SECS` | `60` | Seconds without a successful indexer poll before the health check reports unhealthy. |
| `DB_STATEMENT_TIMEOUT_MS` | `5000` | PostgreSQL statement timeout in milliseconds applied to every query. |
| `INDEXER_POLL_INTERVAL_MS` | `5000` | Milliseconds between indexer polls when the chain is caught up. Range: [100, 60000]. |
| `INDEXER_ERROR_BACKOFF_MS` | `10000` | Milliseconds to wait after an indexer error before retrying. Range: [100, 60000]. |
| `SSE_KEEPALIVE_SECS` | `15` | SSE keep-alive ping interval in seconds (1–60). Prevents proxies from closing idle streams. |
| `SSE_MAX_CONNECTIONS` | `1000` | Global maximum number of concurrent SSE connections the server will accept. |
| `SSE_DRAIN_TIMEOUT_SECS` | `5` | Seconds to wait for SSE clients to drain before forcefully closing on shutdown. Range: [1, 60]. |
| `SSE_MULTI_MAX_CONTRACT_IDS` | `100` | Maximum number of contract IDs per multiplexed SSE connection. |
| `ENVIRONMENT` | `development` | Deployment environment: development, staging, or production. Controls strictness of config validation. |
| `MAX_BODY_SIZE_BYTES` | `1048576` | Maximum HTTP request body size in bytes (default 1 MiB). |
| `LOG_SAMPLE_RATE` | `1` | Fraction of requests to emit trace-level logs for (1 = all, 10 = every 10th). |
| `WEBHOOK_URL` | `(none)` | Target URL for webhook event delivery. When unset, webhook notifications are disabled. |
| `WEBHOOK_SECRET` | `(none)` | HMAC-SHA256 secret used to sign webhook payloads (X-Webhook-Signature header). |
| `WEBHOOK_CONTRACT_FILTER` | `(none)` | Comma-separated list of contract IDs to deliver via webhook. Empty = deliver all. |
| `WEBHOOK_RATE_LIMIT_PER_MINUTE` | `(none)` | Maximum webhook notifications per minute. 0 or unset disables the limit. |
| `WEBHOOK_RATE_LIMIT_PER_HOUR` | `(none)` | Maximum webhook notifications per hour. 0 or unset disables the limit. |
| `WEBHOOK_REQUIRE_HTTPS` | `false (true in production)` | When true, reject webhook URLs that do not use HTTPS. Automatically true in production-like environments. |
| `INDEXER_EVENT_TYPES` | `(all types)` | Comma-separated list of event types to index: contract, diagnostic, system. Empty = index all. |
| `EVENT_DATA_ENCRYPTION_KEY` | `(none)` | 64-hex-character AES-GCM key (32 bytes) for encrypting event_data at rest. Requires the encryption feature flag. |
| `EVENT_DATA_ENCRYPTION_KEY_OLD` | `(none)` | Previous encryption key for zero-downtime key rotation. Requires the encryption feature flag. |
| `INDEX_CHECK_INTERVAL_HOURS` | `24` | How often (in hours) the index usage monitor runs to report on PostgreSQL index utilisation. |
| `HEALTH_CHECK_TIMEOUT_MS` | `2000` | Timeout in milliseconds for the database ping performed by the health check endpoint. |
| `TLS_CERT_FILE` | `(none)` | Path to the PEM-encoded TLS certificate file for HTTPS termination. |
| `TLS_KEY_FILE` | `(none)` | Path to the PEM-encoded TLS private key file for HTTPS termination. |
| `BLOOM_FILTER_FP_RATE` | `0.001` | Target false-positive rate for the bloom filter used in contract existence checks. |
| `BLOOM_FILTER_CAPACITY` | `1000000` | Expected maximum number of items in the bloom filter before the FP rate degrades. |
| `ENABLE_CONTENT_DEDUP` | `false` | When true, a SHA-256 fingerprint of each event is checked before insert to prevent content-identical duplicates. |
| `DEDUP_WINDOW_SECS` | `3600` | Lookback window in seconds for content-fingerprint deduplication checks. |
| `KINESIS_STREAM_NAME` | `(none)` | AWS Kinesis stream name for event delivery. Requires the kinesis feature flag. |
| `AWS_REGION` | `(none)` | AWS region for Kinesis/SQS streaming integrations. |
| `SQS_QUEUE_URL` | `(none)` | AWS SQS queue URL for event delivery. Requires the sqs feature flag. |
| `SQS_DLQ_URL` | `(none)` | AWS SQS dead-letter queue URL for failed event deliveries. |
| `SQS_BATCH_SIZE` | `10` | Number of events per SQS SendMessageBatch call (max 10). |
| `EVENT_HUBS_CONNECTION_STRING` | `(none)` | Azure Event Hubs connection string for event streaming. |
| `EVENT_HUB_NAME` | `(none)` | Azure Event Hub name within the namespace. |
| `EVENT_HUBS_PARTITION_STRATEGY` | `contract_id` | Partition key strategy for Azure Event Hubs: contract_id or round_robin. |
| `PUBSUB_PROJECT_ID` | `(none)` | GCP project ID for Pub/Sub event delivery. Requires the pubsub feature flag. |
| `PUBSUB_TOPIC_ID` | `(none)` | GCP Pub/Sub topic ID for event delivery. |
| `PUBSUB_ENABLE_MESSAGE_ORDERING` | `true` | When true, Pub/Sub messages are published with an ordering key so subscribers receive them in order. |
| `MAX_EVENT_DATA_BYTES` | `65536` | Maximum allowed size of serialised event_data in bytes (default 64 KiB). |
| `EXPORT_MAX_ROWS` | `10000` | Maximum number of rows returned by the CSV/JSON export endpoints. |
| `CONTRACT_COUNT_CACHE_SIZE` | `1000` | Maximum number of entries in the per-contract event count cache. |
| `CONTRACT_COUNT_CACHE_TTL_SECS` | `30` | TTL in seconds for entries in the per-contract event count cache. |
| `INDEXER_LOCK_RETRY_SECS` | `30` | How often standby replicas retry acquiring the advisory lock to become the active indexer. |
| `MULTI_TENANT` | `false` | When true, every API key is mapped to a tenant_id and all queries are scoped to that tenant. |
| `TENANT_RATE_LIMIT_PER_MINUTE` | `(none)` | Per-tenant request rate limit (requests per minute). Only effective when MULTI_TENANT=true. |
| `TENANT_CONTRACT_FILTER` | `(none)` | Per-tenant contract allow-list. Format: "tenant1:C1,C2;tenant2:C3". Only effective when MULTI_TENANT=true. |
| `INDEXER_TENANT_ID` | `(none)` | The tenant_id stamped on every event this indexer instance inserts. Only meaningful when MULTI_TENANT=true. |
| `EMAIL_SMTP_HOST` | `(none)` | SMTP hostname for outbound email notifications. When unset, email notifications are disabled. |
| `EMAIL_SMTP_PORT` | `587` | SMTP port for outbound email (typically 587 for STARTTLS or 465 for implicit TLS). |
| `EMAIL_SMTP_USER` | `(none)` | SMTP authentication username. |
| `EMAIL_SMTP_PASSWORD` | `(none)` | SMTP authentication password. |
| `EMAIL_FROM` | `(none)` | Sender address used in the From header of notification emails. |
| `EMAIL_TO` | `(none)` | Comma-separated list of recipient addresses for email notifications. |
| `EMAIL_CONTRACT_FILTER` | `(none)` | Comma-separated contract IDs to send email notifications for. Empty = notify for all contracts. |
| `EMAIL_PUBLIC_BASE_URL` | `http://localhost:<PORT>` | Public base URL used to build unsubscribe links in notification emails (e.g. https://pulse.example.com). |
| `TWILIO_ACCOUNT_SID` | `(none)` | Twilio Account SID for SMS notifications. When unset, SMS notifications are disabled. |
| `TWILIO_AUTH_TOKEN` | `(none)` | Twilio Auth Token for SMS notifications. |
| `TWILIO_FROM_NUMBER` | `(none)` | Twilio sender phone number in E.164 format (e.g. +15551234567). |
| `SMS_TO_NUMBERS` | `(none)` | Comma-separated list of E.164 phone numbers to receive SMS notifications. |
| `SMS_CONTRACT_FILTER` | `(none)` | Comma-separated contract IDs to send SMS notifications for. Empty = notify for all contracts. |
| `REDIS_URL` | `(none)` | Redis connection URL for event streaming (e.g. redis://localhost:6379). When unset, Redis streaming is disabled. |
| `REDIS_STREAM_KEY` | `(none)` | Redis stream key name for event publishing (e.g. soroban:events). |
| `REDIS_BUFFER_MAX_SIZE` | `10000` | Maximum number of events buffered in memory during a Redis outage before dropping. |
| `STATS_REFRESH_INTERVAL_SECS` | `3600` | How often (in seconds) the background statistics refresh job runs. |
| `RETENTION_DAYS` | `90` | Number of days to retain indexed events before they are pruned. |
| `PRUNING_INTERVAL_HOURS` | `24` | How often (in hours) the event pruning job runs to enforce RETENTION_DAYS. |
| `SSE_REPLAY_MAX_EVENTS` | `1000` | Maximum number of past events replayed to a reconnecting SSE client via Last-Event-ID. |
| `SSE_MAX_CONNECTIONS_PER_IP` | `10` | Maximum concurrent SSE connections allowed per client IP address. |
| `SSE_MAX_LAG_BEFORE_DISCONNECT` | `0` | Number of events a slow SSE consumer may fall behind before being forcefully disconnected. 0 = disabled. |
| `INDEXER_IGNORE_CHECKPOINT` | `false` | When true, the indexer ignores any persisted ledger checkpoint and always starts from START_LEDGER. |
| `RPC_MAX_EVENTS_PER_PAGE` | `200` | Maximum number of events to request per RPC getEvents page. |
| `MAX_LEDGER_RANGE` | `100000` | Maximum ledger range allowed for diff/range query endpoints. |
| `SLOW_QUERY_THRESHOLD_MS` | `1000` | Queries exceeding this duration (ms) are logged at WARN level and counted in metrics. |
| `EVENT_TRANSFORM_SCRIPT` | `(none)` | Path to a Lua script for transforming events before storage. Requires the lua feature flag. |
| `EVENT_TRANSFORM_TIMEOUT_MS` | `100` | Maximum execution time in milliseconds for the Lua event transformation script. |
| `WEBHOOK_NOTIFICATION_FORMAT` | `raw` | Webhook payload format: raw (full event JSON) or template (rendered via WEBHOOK_MESSAGE_TEMPLATE). |
| `WEBHOOK_MESSAGE_TEMPLATE` | `(none)` | Handlebars template string for webhook payload rendering when WEBHOOK_NOTIFICATION_FORMAT=template. |
| `PAGERDUTY_ROUTING_KEY` | `(none)` | PagerDuty Events API v2 routing key (integration key). When unset, PagerDuty notifications are disabled. |
| `PAGERDUTY_SERVICE_NAME` | `Soroban Pulse` | Service name displayed in PagerDuty incidents. |
| `PAGERDUTY_CONTRACT_FILTER` | `(none)` | Comma-separated contract IDs to trigger PagerDuty alerts for. Empty = alert on all contracts. |
| `PAGERDUTY_EVENT_TYPE_FILTER` | `(none)` | Comma-separated event types to trigger PagerDuty alerts for. Empty = alert on all event types. |
| `PAGERDUTY_SEVERITY_MAPPING` | `{"contract":"error","diagnostic":"warning","system":"info"}` | JSON object mapping event types to PagerDuty severity levels (critical, error, warning, info). |
| `PAGERDUTY_AUTO_RESOLVE` | `true` | When true, PagerDuty incidents are automatically resolved after PAGERDUTY_AUTO_RESOLVE_THRESHOLD_MINUTES. |
| `PAGERDUTY_AUTO_RESOLVE_THRESHOLD_MINUTES` | `30` | Minutes of inactivity after which a PagerDuty incident is automatically resolved. |
| `STATS_CACHE_TTL_SECS` | `30` | TTL in seconds for cached statistics responses. |
| `SSE_SHUTDOWN_GRACE_PERIOD_MS` | `200` | Milliseconds between broadcasting the SSE close event and dropping the broadcast sender on shutdown. |
| `EMAIL_MAX_CONTRACTS_IN_DIGEST` | `20` | Maximum number of distinct contracts grouped in a single email digest. |
| `NOTIFICATION_DEFAULT_PRIORITY` | `medium` | Default notification priority level: low, medium, high, or critical. |
| `NOTIFICATION_PRIORITY_RULE_PATH` | `(none)` | JSONPath expression used to extract a value for priority rule evaluation. |
| `NOTIFICATION_PRIORITY_RULE_VALUE` | `(none)` | Value to match against the extracted JSONPath result for priority escalation. |
| `NOTIFICATION_PRIORITY_RULE_PRIORITY` | `(none)` | Priority level assigned when the rule matches: low, medium, high, or critical. |
| `ESCALATION_PRIMARY_CHANNEL` | `(none)` | Notification channel (webhook, email, sms, pagerduty) to try first before escalating. |
| `ESCALATION_CHANNEL` | `(none)` | Fallback notification channel to use after ESCALATION_DELAY_MINUTES if the primary channel fails. |
| `ESCALATION_DELAY_MINUTES` | `30` | Minutes to wait before escalating a failed notification to the ESCALATION_CHANNEL. |
| `ONCALL_PROVIDER` | `(none)` | On-call rotation provider: pagerduty. When unset, on-call routing is disabled. |
| `ONCALL_PAGERDUTY_API_KEY` | `(none)` | PagerDuty REST API key used to query on-call schedules. |
| `ONCALL_SCHEDULE_ID` | `(none)` | PagerDuty schedule ID to query for the current on-call responder. |
| `ONCALL_SCHEDULE_CACHE_TTL_SECS` | `300` | TTL in seconds for the cached on-call schedule lookup result. |
| `EVENT_COMPRESSION_ENABLED` | `false` | When true, event_data is gzip-compressed before writing to the event_data_compressed column. |
| `EVENT_COMPRESSION_MIGRATION_BATCH_SIZE` | `500` | Number of events processed per batch when migrating existing events to compressed format. |
| `CHAIN_ID` | `mainnet` | Chain identifier stamped on every indexed event (e.g. mainnet, testnet, futurenet). |
| `ADDITIONAL_CHAIN_IDS` | `(none)` | Comma-separated list of additional network chain_ids to spin up parallel indexer workers for. |
| `LEDGER_HASH_TRACKING_ENABLED` | `false` | When true, ledger hashes are stored and chain continuity is verified on startup. |
| `LEDGER_HASH_VALIDATION_DEPTH` | `1000` | Number of ledgers to check during startup hash-chain validation. |
| `ABI_CACHE_TTL_SECS` | `86400` | TTL in seconds for in-process contract ABI cache entries (default 24 h). |
| `ABI_CACHE_MAX_ENTRIES` | `1000` | Maximum number of contract ABIs kept in the in-process cache. |
| `SSE_RING_BUFFER_CAPACITY` | `10000` | Maximum number of events stored in the in-memory SSE replay ring buffer. Oldest events are evicted when full. |
| `QUERY_CACHE_TTL_SECS` | `300` | TTL in seconds for cached query results. Clamped to [300, 3600]. |
| `QUERY_CACHE_MAX_CAPACITY` | `1000` | Maximum number of entries in the query result cache. |
| `ENABLE_PUSH_PRELOAD` | `false` | When true, responses include HTTP Link preload headers for contract schema/ABI and the /v1/push/* endpoints are enabled. |
# Configuration reference

<!-- Generated by `cargo run --bin gen_config_docs` from `src/config/reference.rs`. Do not edit by hand. -->

SorobanPulse is configured through environment variables. Any variable can also be set in a TOML file pointed to by `CONFIG_FILE`; environment variables take precedence.

Variables marked with a feature flag only take effect when the binary is built with that Cargo feature (`cargo build --features <flag>`).

## Sections

- [Core](#core)
- [Database](#database)
- [Stellar RPC and indexer](#stellar-rpc-and-indexer)
- [Multi-chain](#multi-chain)
- [Authentication and access control](#authentication-and-access-control)
- [Rate limiting](#rate-limiting)
- [Multi-tenancy](#multi-tenancy)
- [API limits and caching](#api-limits-and-caching)
- [Server-sent events (SSE)](#server-sent-events-sse)
- [Logging and observability](#logging-and-observability)
- [Retention, deduplication and storage](#retention-deduplication-and-storage)
- [Encryption](#encryption)
- [Webhooks](#webhooks)
- [Email notifications](#email-notifications)
- [SMS notifications](#sms-notifications)
- [PagerDuty, escalation and on-call](#pagerduty-escalation-and-on-call)
- [Streaming integrations](#streaming-integrations)
- [Lua event transformation](#lua-event-transformation)
- [Dashboard](#dashboard)
- [SaaS platform](#saas-platform)
- [Machine learning](#machine-learning)
- [Docker Compose](#docker-compose)

## Core

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `ENVIRONMENT` | string | `development` | no | — | Deployment environment: `development`, `staging` or `production`. Staging and production reject `ALLOWED_ORIGINS=*` and require HTTPS webhooks by default. |
| `ROLE` | string | `all` | no | — | Which subsystems start on this instance: `all` (API + indexer), `api` (HTTP API only) or `indexer` (indexer plus health/metrics). |
| `CONFIG_FILE` | string | — | no | — | Path to an optional TOML file supplying any variable in this reference. Environment variables take precedence over file values. |
| `PORT` | integer | `3000` | no | — | HTTP server port to listen on. |
| `BEHIND_PROXY` | bool | `false` | no | — | Trust `X-Forwarded-For` / `X-Real-IP` for client IPs. Enable only behind a trusted reverse proxy. |
| `ALLOWED_ORIGINS` | list | `*` | no | — | Comma-separated CORS origins. `*` allows all origins and is rejected in staging/production. |
| `MAX_BODY_SIZE_BYTES` | integer | `1048576` | no | — | Maximum accepted HTTP request body size in bytes. |
| `TLS_CERT_FILE` | string | — | no | — | Path to a PEM certificate. When set together with `TLS_KEY_FILE`, the server terminates TLS itself. |
| `TLS_KEY_FILE` | string | — | no | — | Path to the PEM private key matching `TLS_CERT_FILE`. |
| `RUN_MIGRATIONS_ON_STARTUP` | bool | `true` | no | — | Run database migrations before starting the server and indexer. Set to `false` when migrations run as a separate job (e.g. the Helm migration Job). |

## Database

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `DATABASE_URL` | url | — | yes | — | PostgreSQL connection string, e.g. `postgres://user:pass@localhost:5432/soroban_pulse`. Required unless `DATABASE_URL_FILE` is set. |
| `DATABASE_URL_FILE` | string | — | no | — | Read the connection string from this file (Docker/Kubernetes secrets). Takes precedence over `DATABASE_URL`. |
| `DATABASE_REPLICA_URL` | url | — | no | — | Optional read-replica connection string. HTTP handlers read from the replica; the indexer always writes to the primary. |
| `DB_MAX_CONNECTIONS` | integer | `10` | no | — | Maximum connections in the PostgreSQL pool. |
| `DB_MIN_CONNECTIONS` | integer | `2` | no | — | Minimum idle connections kept in the pool. |
| `DB_IDLE_TIMEOUT_SECS` | integer | `600` | no | — | Close connections idle for longer than this many seconds. |
| `DB_MAX_LIFETIME_SECS` | integer | `1800` | no | — | Recycle connections older than this many seconds regardless of idle time. |
| `DB_TEST_BEFORE_ACQUIRE` | bool | `true` | no | — | Ping each connection before handing it out, transparently replacing stale connections. |
| `DB_STATEMENT_TIMEOUT_MS` | integer | `5000` | no | — | Maximum time a single SQL statement may run before PostgreSQL cancels it. |
| `HEALTH_CHECK_TIMEOUT_MS` | integer | `2000` | no | — | Timeout for the health check database ping (`SELECT 1`). |
| `SLOW_QUERY_THRESHOLD_MS` | integer | `1000` | no | — | Database queries slower than this are logged at WARN level. |
| `INDEX_CHECK_INTERVAL_HOURS` | integer | `24` | no | — | How often the index usage monitor runs. |

## Stellar RPC and indexer

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `STELLAR_RPC_URL` | url | `https://soroban-testnet.stellar.org` | no | — | Soroban RPC endpoint. Must be HTTPS and non-private unless `ALLOW_INSECURE_RPC=true`. |
| `STELLAR_RPC_FALLBACK_URLS` | list | — | no | — | Comma-separated fallback RPC endpoints, tried in order when the primary is unreachable. |
| `STELLAR_RPC_HEADERS` | string | — | no | — | Extra headers sent with every RPC request, as semicolon-separated `Name: Value` pairs (e.g. `X-API-Key: key; X-Org: acme`). Values are never logged. |
| `ALLOW_INSECURE_RPC` | bool | `false` | no | — | Allow HTTP and loopback/private RPC URLs. Local development only. |
| `RPC_CONNECT_TIMEOUT_SECS` | integer | `5` | no | — | TCP connect timeout for RPC requests. |
| `RPC_REQUEST_TIMEOUT_SECS` | integer | `30` | no | — | Total timeout for a single RPC request. |
| `RPC_MAX_EVENTS_PER_PAGE` | integer | `200` | no | — | Maximum events requested per `getEvents` page. |
| `START_LEDGER` | integer | `0` | no | — | Ledger sequence to start indexing from. `0` starts from the latest ledger. |
| `START_LEDGER_FALLBACK` | bool | `false` | no | — | When `START_LEDGER` is outside the RPC retention window, fall back to the oldest available ledger instead of failing. |
| `INDEXER_EVENT_TYPES` | list | — | no | — | Comma-separated event types to index: `contract`, `diagnostic`, `system`. Empty indexes all types. |
| `INDEXER_POLL_INTERVAL_MS` | integer | `5000` | no | — | Poll interval when no new ledgers are available (100–60000). |
| `INDEXER_ERROR_BACKOFF_MS` | integer | `10000` | no | — | Backoff after an RPC error (100–60000). |
| `INDEXER_LAG_WARN_THRESHOLD` | integer | `100` | no | — | Warn when the indexer falls this many ledgers behind the network. |
| `INDEXER_STALL_TIMEOUT_SECS` | integer | `60` | no | — | Seconds without progress after which the indexer is reported as stalled. |
| `INDEXER_LOCK_RETRY_SECS` | integer | `30` | no | — | How often standby replicas retry acquiring the indexer advisory lock. |
| `INDEXER_IGNORE_CHECKPOINT` | bool | `false` | no | — | Ignore the persisted indexer checkpoint and start from `START_LEDGER`. |
| `MAX_EVENT_DATA_BYTES` | integer | `65536` | no | — | Events whose serialized `event_data` exceeds this size are logged and skipped. |

## Multi-chain

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `CHAIN_ID` | string | `mainnet` | no | — | Chain identifier stamped on events indexed from `STELLAR_RPC_URL`. |
| `ADDITIONAL_CHAIN_IDS` | list | — | no | — | Comma-separated extra chain IDs to index concurrently. Each needs a matching `STELLAR_RPC_URL_<CHAIN_ID>`. |
| `STELLAR_RPC_URL_<CHAIN_ID>` | url | — | no | — | RPC endpoint for an entry in `ADDITIONAL_CHAIN_IDS`. The chain ID is upper-cased with non-alphanumerics replaced by `_` (e.g. `STELLAR_RPC_URL_FUTURENET`). |
| `LEDGER_HASH_TRACKING_ENABLED` | bool | `false` | no | — | Store ledger hashes and verify hash-chain continuity on startup. |
| `LEDGER_HASH_VALIDATION_DEPTH` | integer | `1000` | no | — | Number of ledgers checked during startup hash-chain validation. |

## Authentication and access control

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `API_KEY` | secret | — | no | — | API key for authenticated endpoints. When unset, authentication is disabled. |
| `API_KEY_SECONDARY` | secret | — | no | — | Second accepted API key, for rotating `API_KEY` without downtime. |
| `ADMIN_API_KEY` | secret | — | no | — | Key required for `/v1/admin/*` endpoints, independent of `API_KEY`. When unset, admin endpoints fall back to the `API_KEY` gate. |
| `ADMIN_API_KEY_SECONDARY` | secret | — | no | — | Second accepted admin key, for rotating `ADMIN_API_KEY` without downtime. |
| `IP_DENYLIST` | list | — | no | — | Comma-separated IPs/CIDR blocks to reject. |
| `IP_ALLOWLIST` | list | — | no | — | Comma-separated IPs/CIDR blocks to exclusively allow. Empty disables enforcement. |

## Rate limiting

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `RATE_LIMIT_PER_MINUTE` | integer | `60` | no | — | Per-IP request limit per minute. Exceeding it returns 429 with `Retry-After`. |
| `RATE_LIMIT_KEY_PER_MINUTE` | integer | — | no | — | Per-API-key sliding-window limit per minute. Unset or `0` disables. |
| `RATE_LIMIT_KEY_PER_HOUR` | integer | — | no | — | Per-API-key limit per hour. Unset or `0` disables. |
| `RATE_LIMIT_KEY_PER_DAY` | integer | — | no | — | Per-API-key limit per day. Unset or `0` disables. |
| `RATE_LIMIT_KEY_PER_MONTH` | integer | — | no | — | Per-API-key limit per rolling 30-day window. Unset or `0` disables. |

## Multi-tenancy

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `MULTI_TENANT` | bool | `false` | no | — | Enable multi-tenant mode. Every API key must then map to a tenant. |
| `TENANT_RATE_LIMIT_PER_MINUTE` | integer | — | no | — | Independent per-tenant request quota per minute (multi-tenant mode only). |
| `TENANT_CONTRACT_FILTER` | string | — | no | — | Per-tenant indexer contract filter as semicolon-separated `tenant:contract1,contract2` pairs. |
| `INDEXER_TENANT_ID` | string | — | no | — | Tenant ID stamped on every event this indexer inserts (multi-tenant mode only). |

## API limits and caching

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `EXPORT_MAX_ROWS` | integer | `10000` | no | — | Maximum rows returned by `GET /v1/events/export`. |
| `MAX_LEDGER_RANGE` | integer | `100000` | no | — | Maximum ledger span accepted by diff/range queries. |
| `BATCH_TX_MAX_SIZE` | integer | `100` | no | — | Maximum transaction hashes per batch lookup request. |
| `BATCH_EVENTS_MAX_SIZE` | integer | `10000` | no | — | Maximum events per bulk insert request. |
| `CONTRACT_COUNT_CACHE_SIZE` | integer | `1000` | no | — | Maximum entries in the per-contract event count cache. |
| `CONTRACT_COUNT_CACHE_TTL_SECS` | integer | `30` | no | — | TTL of contract count cache entries. |
| `AGGREGATION_CACHE_TTL_SECS` | integer | `60` | no | — | TTL of cached aggregation results. |
| `AGGREGATION_CACHE_MAX_ENTRIES` | integer | `1000` | no | — | Maximum cached aggregation results. |
| `QUERY_CACHE_TTL_SECS` | integer | `300` | no | — | TTL of cached query results, clamped to 300–3600. |
| `QUERY_CACHE_MAX_CAPACITY` | integer | `1000` | no | — | Maximum cached query results. |
| `STATS_CACHE_TTL_SECS` | integer | `30` | no | — | TTL of the cached `/v1/stats` response. |
| `STATS_REFRESH_INTERVAL_SECS` | integer | `3600` | no | — | How often materialized statistics are refreshed. |
| `ABI_CACHE_TTL_SECS` | integer | `86400` | no | — | TTL of the in-process contract ABI cache. |
| `ABI_CACHE_MAX_ENTRIES` | integer | `10000` | no | — | Maximum ABIs kept in the in-process cache. |
| `ENABLE_PUSH_PRELOAD` | bool | `false` | no | — | Add `Link` preload headers for a contract's schema and ABI to `/v1/events/contract/{contract_id}` responses. |

## Server-sent events (SSE)

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `SSE_KEEPALIVE_SECS` | integer | — | no | — | Keep-alive ping interval in seconds (1–60). Takes precedence over `SSE_KEEPALIVE_INTERVAL_MS`. |
| `SSE_KEEPALIVE_INTERVAL_MS` | integer | `15000` | no | — | Keep-alive ping interval in milliseconds (1000–60000). Keep it below your proxy's idle timeout. |
| `SSE_MAX_CONNECTIONS` | integer | `1000` | no | — | Maximum concurrent SSE connections. |
| `SSE_MAX_CONNECTIONS_PER_IP` | integer | `10` | no | — | Maximum concurrent SSE connections per client IP. |
| `SSE_MULTI_MAX_CONTRACT_IDS` | integer | `100` | no | — | Maximum contract IDs per multi-contract stream. |
| `SSE_DRAIN_TIMEOUT_SECS` | integer | `5` | no | — | Time allowed for SSE clients to drain during shutdown (1–60). |
| `SSE_SHUTDOWN_GRACE_PERIOD_MS` | integer | `200` | no | — | Delay between broadcasting the close event and dropping the SSE broadcaster. |
| `SSE_REPLAY_MAX_EVENTS` | integer | `1000` | no | — | Maximum events replayed on reconnect with `Last-Event-ID`. Takes precedence over `SSE_REPLAY_LIMIT`. |
| `SSE_REPLAY_LIMIT` | integer | `1000` | no | — | Legacy alias of `SSE_REPLAY_MAX_EVENTS`. |
| `SSE_RING_BUFFER_CAPACITY` | integer | `10000` | no | — | Events kept in the in-memory replay ring buffer; the oldest is evicted when full. |
| `SSE_MAX_LAG_BEFORE_DISCONNECT` | integer | `0` | no | — | Disconnect clients lagging this many events behind. `0` disables. |

## Logging and observability

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `RUST_LOG` | string | `info` | no | — | Log filter (`trace`, `debug`, `info`, `warn`, `error`, or per-module directives). |
| `RUST_LOG_FORMAT` | string | `text` | no | — | Log output format: `text` or `json`. |
| `LOG_SAMPLE_RATE` | integer | `1` | no | — | Sample debug-level logs: `1` logs everything, `100` logs 1%. INFO and above are never sampled. |
| `SLOW_REQUEST_THRESHOLD_MS` | integer | `500` | no | — | HTTP requests slower than this are logged at WARN level. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | url | — | no | `otel` | OTLP collector endpoint (e.g. `http://localhost:4317`). Enables OpenTelemetry tracing when set. |

## Retention, deduplication and storage

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `RETENTION_DAYS` | integer | `90` | no | — | Days to keep events before automatic pruning. `0` keeps events forever. |
| `PRUNING_INTERVAL_HOURS` | integer | `24` | no | — | How often the pruning task runs. |
| `ARCHIVE_AFTER_DAYS` | integer | `30` | no | — | Age in days after which events are moved to archive storage. |
| `BLOOM_FILTER_FP_RATE` | float | `0.001` | no | — | False-positive rate of the bloom pre-deduplication filter. |
| `BLOOM_FILTER_CAPACITY` | integer | `1000000` | no | — | Number of unique events the bloom filter is sized for. |
| `ENABLE_CONTENT_DEDUP` | bool | `false` | no | — | Skip events whose content fingerprint matches a recent row. |
| `DEDUP_WINDOW_SECS` | integer | `3600` | no | — | Lookback window for content fingerprint deduplication. |
| `EVENT_COMPRESSION_ENABLED` | bool | `false` | no | — | Gzip-compress `event_data` into `event_data_compressed`. |
| `EVENT_COMPRESSION_MIGRATION_BATCH_SIZE` | integer | `500` | no | — | Batch size when migrating existing events to compressed storage. |

## Encryption

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `EVENT_DATA_ENCRYPTION_KEY` | secret | — | no | `encryption` | 64-hex-character AES-256-GCM key encrypting `event_data` at rest. |
| `EVENT_DATA_ENCRYPTION_KEY_OLD` | secret | — | no | `encryption` | Previous event data key, kept during rotation while `POST /v1/admin/reencrypt` runs. |
| `INTEGRATION_ENCRYPTION_KEY` | secret | — | no | `encryption` | 64-hex-character AES-256-GCM key encrypting stored integration credentials (bot tokens, webhook URLs). |
| `INTEGRATION_ENCRYPTION_KEY_OLD` | secret | — | no | `encryption` | Previous integration key, used during rotation. |

## Webhooks

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `WEBHOOK_URL` | url | — | no | — | POST every newly indexed event as JSON to this URL. |
| `WEBHOOK_SECRET` | secret | — | no | — | HMAC-SHA256 secret. Each delivery carries `X-Signature-256: sha256=<hex>` over the raw body. |
| `WEBHOOK_CONTRACT_FILTER` | list | — | no | — | Comma-separated contract IDs; only matching events are delivered. Empty delivers all events. |
| `WEBHOOK_CONTENT_FILTER` | json | — | no | — | JSON content filter such as `{"path":"$.amount","op":"gt","value":"1000000"}`. Invalid filters are logged and ignored. |
| `WEBHOOK_RATE_LIMIT_PER_MINUTE` | integer | — | no | — | Maximum webhook deliveries per minute. Unset or `0` disables. |
| `WEBHOOK_RATE_LIMIT_PER_HOUR` | integer | — | no | — | Maximum webhook deliveries per hour. Unset or `0` disables. |
| `WEBHOOK_REQUIRE_HTTPS` | bool | — | no | — | Reject non-HTTPS webhook URLs. Defaults to `true` in staging/production and `false` otherwise. |
| `WEBHOOK_NOTIFICATION_FORMAT` | string | `raw` | no | — | Payload format: `raw` event JSON, `slack` or `discord`. |
| `WEBHOOK_MESSAGE_TEMPLATE` | string | — | no | — | Optional message template used to render webhook notifications. |

## Email notifications

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `EMAIL_SMTP_HOST` | string | — | no | — | SMTP host. Email notifications are enabled when set. |
| `EMAIL_SMTP_PORT` | integer | `587` | no | — | SMTP port (587 for STARTTLS). |
| `EMAIL_SMTP_USER` | string | — | no | — | SMTP username. |
| `EMAIL_SMTP_PASSWORD` | secret | — | no | — | SMTP password. Never logged. |
| `EMAIL_FROM` | string | — | no | — | Sender address. Required when `EMAIL_SMTP_HOST` is set. |
| `EMAIL_TO` | list | — | no | — | Comma-separated recipients. Required when `EMAIL_SMTP_HOST` is set. |
| `EMAIL_CONTRACT_FILTER` | list | — | no | — | Comma-separated contract IDs; only matching events trigger emails. |
| `EMAIL_SCHEDULE` | string | `immediate` | no | — | Delivery schedule: `immediate`, `hourly_digest`, `daily_digest` or `custom_cron`. |
| `EMAIL_DAILY_DIGEST_HOUR` | integer | `9` | no | — | UTC hour (0–23) at which the daily digest is sent. |
| `EMAIL_CRON` | string | — | no | — | Cron expression used when `EMAIL_SCHEDULE=custom_cron`. |
| `EMAIL_QUIET_HOURS_START` | string | — | no | — | Start of the quiet window (`HH:MM`, UTC) during which emails are held. |
| `EMAIL_QUIET_HOURS_END` | string | — | no | — | End of the quiet window (`HH:MM`, UTC). |
| `EMAIL_MAX_CONTRACTS_IN_DIGEST` | integer | `20` | no | — | Maximum contracts listed individually in a digest email. |
| `EMAIL_PUBLIC_BASE_URL` | url | — | no | — | Public base URL for unsubscribe links. Defaults to `http://localhost:<PORT>`. |

## SMS notifications

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `TWILIO_ACCOUNT_SID` | string | — | no | — | Twilio account SID. SMS notifications are enabled when the Twilio credentials are set. |
| `TWILIO_AUTH_TOKEN` | secret | — | no | — | Twilio auth token. |
| `TWILIO_FROM_NUMBER` | string | — | no | — | Sending phone number in E.164 format. |
| `SMS_TO_NUMBERS` | list | — | no | — | Comma-separated recipient phone numbers. |
| `SMS_CONTRACT_FILTER` | list | — | no | — | Comma-separated contract IDs; only matching events trigger SMS. |

## PagerDuty, escalation and on-call

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `PAGERDUTY_ROUTING_KEY` | secret | — | no | — | PagerDuty Events API v2 routing key. PagerDuty alerts are enabled when set. |
| `PAGERDUTY_SERVICE_NAME` | string | `Soroban Pulse` | no | — | Source name shown on PagerDuty incidents. |
| `PAGERDUTY_CONTRACT_FILTER` | list | — | no | — | Comma-separated contract IDs that trigger incidents. |
| `PAGERDUTY_EVENT_TYPE_FILTER` | list | — | no | — | Comma-separated event types that trigger incidents. |
| `PAGERDUTY_SEVERITY_MAPPING` | json | `{"contract":"error","diagnostic":"warning","system":"info"}` | no | — | JSON map from event type to PagerDuty severity. |
| `PAGERDUTY_AUTO_RESOLVE` | bool | `true` | no | — | Automatically resolve incidents after the threshold below. |
| `PAGERDUTY_AUTO_RESOLVE_THRESHOLD_MINUTES` | integer | `30` | no | — | Minutes without matching events before an incident is auto-resolved. |
| `NOTIFICATION_DEFAULT_PRIORITY` | string | `medium` | no | — | Default notification priority: `low`, `medium`, `high` or `critical`. |
| `NOTIFICATION_PRIORITY_RULE_PATH` | string | — | no | — | JSON path in event data evaluated by the priority rule. |
| `NOTIFICATION_PRIORITY_RULE_VALUE` | string | — | no | — | Value that `NOTIFICATION_PRIORITY_RULE_PATH` must match. |
| `NOTIFICATION_PRIORITY_RULE_PRIORITY` | string | — | no | — | Priority assigned when the rule matches. |
| `ESCALATION_PRIMARY_CHANNEL` | string | — | no | — | Channel notified first (e.g. `email`, `webhook`). |
| `ESCALATION_CHANNEL` | string | — | no | — | Channel escalated to when the primary is not acknowledged. |
| `ESCALATION_DELAY_MINUTES` | integer | `30` | no | — | Minutes before escalating to `ESCALATION_CHANNEL`. |
| `ONCALL_PROVIDER` | string | — | no | — | On-call rotation provider (e.g. `pagerduty`). |
| `ONCALL_PAGERDUTY_API_KEY` | secret | — | no | — | PagerDuty REST API key used to read on-call schedules. |
| `ONCALL_SCHEDULE_ID` | string | — | no | — | On-call schedule ID to route notifications to. |
| `ONCALL_SCHEDULE_CACHE_TTL_SECS` | integer | `300` | no | — | TTL of the cached on-call schedule. |
| `NOTIFICATION_AUDIT_LOG_RETENTION_DAYS` | integer | `90` | no | — | Days to keep `notification_audit_log` entries. |
| `NOTIFICATION_HEALTH_CHECK_INTERVAL_SECS` | integer | `300` | no | — | How often each notification channel is health-checked. |
| `NOTIFICATION_HEALTH_CHECK_EMAIL` | string | — | no | — | Target address for email channel health checks. |

## Streaming integrations

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `KAFKA_BROKERS` | list | — | no | `kafka` | Comma-separated Kafka brokers (e.g. `localhost:9092`). Publishing is enabled when set. |
| `KAFKA_TOPIC` | string | — | no | `kafka` | Kafka topic. Required when `KAFKA_BROKERS` is set. |
| `KAFKA_BATCH_SIZE` | integer | `16384` | no | `kafka` | Kafka producer batch size in bytes. |
| `KAFKA_LINGER_MS` | integer | `100` | no | `kafka` | Kafka producer linger time. |
| `KINESIS_STREAM_NAME` | string | — | no | `kinesis` | Kinesis Data Stream to publish to. Requires `AWS_REGION`. |
| `KINESIS_PARTITION_KEY_FIELD` | string | `contract_id` | no | `kinesis` | Partition key strategy: `contract_id`, `tx_hash` or `random`. |
| `AWS_REGION` | string | — | no | — | AWS region for Kinesis and SQS. Credentials come from the standard AWS chain. |
| `SQS_QUEUE_URL` | url | — | no | `sqs` | SQS queue URL. Publishing is enabled when set together with `AWS_REGION`. |
| `SQS_DLQ_URL` | url | — | no | `sqs` | Queue receiving entries that fail in a batch. |
| `SQS_BATCH_SIZE` | integer | `10` | no | `sqs` | Messages per `SendMessageBatch` call (AWS caps this at 10). |
| `EVENT_HUBS_CONNECTION_STRING` | secret | — | no | — | Azure Event Hubs connection string with Send permission. |
| `EVENT_HUB_NAME` | string | — | no | — | Event hub name. Optional when `EntityPath` is in the connection string. |
| `EVENT_HUBS_PARTITION_STRATEGY` | string | `contract_id` | no | — | Partition key strategy: `contract_id`, `tx_hash` or `ledger`. |
| `PUBSUB_PROJECT_ID` | string | — | no | `pubsub` | GCP project for Pub/Sub publishing. Requires `PUBSUB_TOPIC_ID`. |
| `PUBSUB_TOPIC_ID` | string | — | no | `pubsub` | Pub/Sub topic ID (without the `projects/…/topics/` prefix). |
| `PUBSUB_ENABLE_MESSAGE_ORDERING` | bool | `true` | no | `pubsub` | Publish with an ordering key per contract. |
| `GOOGLE_APPLICATION_CREDENTIALS` | string | — | no | `pubsub` | Path to GCP service account JSON. Leave unset on GCE/GKE/Cloud Run. |
| `REDIS_URL` | url | — | no | `redis-queue` | Redis URL. Each indexed event is published to a Redis Stream when set. |
| `REDIS_STREAM_KEY` | string | — | no | `redis-queue` | Redis Stream key events are appended to. |
| `REDIS_BUFFER_MAX_SIZE` | integer | `10000` | no | `redis-queue` | Events buffered in memory while Redis is unreachable; the oldest are dropped when full. |

## Lua event transformation

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `EVENT_TRANSFORM_SCRIPT` | string | — | no | `lua` | Path to a Lua script that transforms or filters events before storage. |
| `EVENT_TRANSFORM_TIMEOUT_MS` | integer | `100` | no | `lua` | Per-event script timeout; events whose script times out are skipped. |

## Dashboard

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `SERVE_DASHBOARD` | bool | `false` | no | `dashboard` | Serve the built web dashboard at `/ui`. |
| `DASHBOARD_DIR` | string | `web/dist` | no | `dashboard` | Directory containing the built dashboard. |
| `DASHBOARD_CONNECT_SRC` | list | — | no | `dashboard` | Extra origins appended to the dashboard's CSP `connect-src`. |

## SaaS platform

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `SAAS_ENABLED` | bool | `false` | no | — | Enable tenant management and billing integration. |
| `BILLING_PROVIDER` | string | — | no | — | Billing provider (e.g. `stripe`, `paddle`). |
| `BILLING_API_KEY` | secret | — | no | — | Billing provider API key. |
| `DEFAULT_TRIAL_DAYS` | integer | `14` | no | — | Trial length for new tenants. |
| `SAAS_PUBLIC_BASE_URL` | url | — | no | — | Public base URL used in tenant onboarding links. |

## Machine learning

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `ML_ANOMALY_DETECTION_ENABLED` | bool | `false` | no | — | Enable ML-based anomaly detection. |
| `ML_PATTERN_RECOGNITION_ENABLED` | bool | `false` | no | — | Enable event pattern recognition. |
| `ML_INTELLIGENT_FILTERING_ENABLED` | bool | `false` | no | — | Enable ML-driven event filtering. |
| `ML_MODEL_TRAINING_INTERVAL_HOURS` | integer | `24` | no | — | How often models are retrained. |
| `ML_MIN_TRAINING_SAMPLES` | integer | `100` | no | — | Minimum samples required before training. |
| `ML_CONFIDENCE_THRESHOLD` | float | `0.8` | no | — | Minimum model confidence (0.0–1.0) for a result to be acted on. |
| `ML_AUTO_RETRAIN` | bool | `true` | no | — | Retrain models automatically on the interval above. |

## Docker Compose

| Name | Type | Default | Required | Feature | Description |
|------|------|---------|----------|---------|-------------|
| `POSTGRES_USER` | string | — | no | — | PostgreSQL user created by the Compose `db` service. Must match `DATABASE_URL`. |
| `POSTGRES_PASSWORD` | secret | — | no | — | PostgreSQL password for the Compose `db` service. |
| `POSTGRES_DB` | string | — | no | — | Database created by the Compose `db` service. |

