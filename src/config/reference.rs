//! Declarative reference of every environment variable SorobanPulse reads
//! (Issue #1168).
//!
//! This table is the single source of truth for operator-facing
//! configuration docs. `cargo run --bin gen_config_docs` renders it into
//! `docs/configuration.md` and `.env.example`; `--check` fails when either
//! generated file is stale or when `src/config.rs` reads a variable that is
//! missing here.
//!
//! When adding a variable to `Config::from_env`, add a matching entry below
//! and run `make gen-config-docs`.

// Only consumed through the library crate by the gen_config_docs binary.
#![allow(dead_code)]

/// Where a variable is read from — controls how `gen_config_docs --check`
/// cross-references it against `src/config.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// Read by name in `src/config.rs`.
    Config,
    /// Read by `src/config.rs` under a computed name (e.g. a per-chain suffix).
    Pattern,
    /// Read outside `src/config.rs` (another module, Docker Compose, a library).
    External,
}

/// Documentation for a single environment variable.
#[derive(Debug, Clone, Copy)]
pub struct EnvVarDoc {
    pub name: &'static str,
    /// Human-readable value type: `string`, `integer`, `float`, `bool`, `list`, `url`, `secret`, `json`.
    pub kind: &'static str,
    /// Value used when the variable is unset, or `None` when the feature is disabled/unset.
    pub default: Option<&'static str>,
    pub required: bool,
    pub description: &'static str,
    /// Cargo feature the variable only has an effect under.
    pub feature: Option<&'static str>,
    /// Value written uncommented into `.env.example`; `None` writes a commented-out line.
    pub example: Option<&'static str>,
    pub source: Source,
}

impl EnvVarDoc {
    const fn new(
        name: &'static str,
        kind: &'static str,
        default: Option<&'static str>,
        description: &'static str,
    ) -> Self {
        Self {
            name,
            kind,
            default,
            required: false,
            description,
            feature: None,
            example: None,
            source: Source::Config,
        }
    }

    const fn required(mut self) -> Self {
        self.required = true;
        self
    }

    const fn feature(mut self, feature: &'static str) -> Self {
        self.feature = Some(feature);
        self
    }

    const fn example(mut self, example: &'static str) -> Self {
        self.example = Some(example);
        self
    }

    const fn source(mut self, source: Source) -> Self {
        self.source = source;
        self
    }
}

/// A titled group of related variables.
#[derive(Debug, Clone, Copy)]
pub struct Section {
    pub title: &'static str,
    pub vars: &'static [EnvVarDoc],
}

const fn v(
    name: &'static str,
    kind: &'static str,
    default: Option<&'static str>,
    description: &'static str,
) -> EnvVarDoc {
    EnvVarDoc::new(name, kind, default, description)
}

const N: Option<&str> = None;

const fn d(value: &'static str) -> Option<&'static str> {
    Some(value)
}

pub const SECTIONS: &[Section] = &[
    Section {
        title: "Core",
        vars: &[
            v("ENVIRONMENT", "string", d("development"), "Deployment environment: `development`, `staging` or `production`. Staging and production reject `ALLOWED_ORIGINS=*` and require HTTPS webhooks by default.").example("development"),
            v("ROLE", "string", d("all"), "Which subsystems start on this instance: `all` (API + indexer), `api` (HTTP API only) or `indexer` (indexer plus health/metrics)."),
            v("CONFIG_FILE", "string", N, "Path to an optional TOML file supplying any variable in this reference. Environment variables take precedence over file values."),
            v("PORT", "integer", d("3000"), "HTTP server port to listen on.").example("3000"),
            v("BEHIND_PROXY", "bool", d("false"), "Trust `X-Forwarded-For` / `X-Real-IP` for client IPs. Enable only behind a trusted reverse proxy."),
            v("ALLOWED_ORIGINS", "list", d("*"), "Comma-separated CORS origins. `*` allows all origins and is rejected in staging/production."),
            v("MAX_BODY_SIZE_BYTES", "integer", d("1048576"), "Maximum accepted HTTP request body size in bytes."),
            v("TLS_CERT_FILE", "string", N, "Path to a PEM certificate. When set together with `TLS_KEY_FILE`, the server terminates TLS itself."),
            v("TLS_KEY_FILE", "string", N, "Path to the PEM private key matching `TLS_CERT_FILE`."),
            v("RUN_MIGRATIONS_ON_STARTUP", "bool", d("true"), "Run database migrations before starting the server and indexer. Set to `false` when migrations run as a separate job (e.g. the Helm migration Job)."),
        ],
    },
    Section {
        title: "Database",
        vars: &[
            v("DATABASE_URL", "url", N, "PostgreSQL connection string, e.g. `postgres://user:pass@localhost:5432/soroban_pulse`. Required unless `DATABASE_URL_FILE` is set.").required().example("postgres://<USER>:<PASSWORD>@db:5432/soroban_pulse"),
            v("DATABASE_URL_FILE", "string", N, "Read the connection string from this file (Docker/Kubernetes secrets). Takes precedence over `DATABASE_URL`."),
            v("DATABASE_REPLICA_URL", "url", N, "Optional read-replica connection string. HTTP handlers read from the replica; the indexer always writes to the primary."),
            v("DB_MAX_CONNECTIONS", "integer", d("10"), "Maximum connections in the PostgreSQL pool.").example("10"),
            v("DB_MIN_CONNECTIONS", "integer", d("2"), "Minimum idle connections kept in the pool.").example("2"),
            v("DB_IDLE_TIMEOUT_SECS", "integer", d("600"), "Close connections idle for longer than this many seconds."),
            v("DB_MAX_LIFETIME_SECS", "integer", d("1800"), "Recycle connections older than this many seconds regardless of idle time."),
            v("DB_TEST_BEFORE_ACQUIRE", "bool", d("true"), "Ping each connection before handing it out, transparently replacing stale connections."),
            v("DB_STATEMENT_TIMEOUT_MS", "integer", d("5000"), "Maximum time a single SQL statement may run before PostgreSQL cancels it."),
            v("HEALTH_CHECK_TIMEOUT_MS", "integer", d("2000"), "Timeout for the health check database ping (`SELECT 1`)."),
            v("SLOW_QUERY_THRESHOLD_MS", "integer", d("1000"), "Database queries slower than this are logged at WARN level."),
            v("INDEX_CHECK_INTERVAL_HOURS", "integer", d("24"), "How often the index usage monitor runs."),
        ],
    },
    Section {
        title: "Stellar RPC and indexer",
        vars: &[
            v("STELLAR_RPC_URL", "url", d("https://soroban-testnet.stellar.org"), "Soroban RPC endpoint. Must be HTTPS and non-private unless `ALLOW_INSECURE_RPC=true`.").example("https://soroban-testnet.stellar.org"),
            v("STELLAR_RPC_FALLBACK_URLS", "list", N, "Comma-separated fallback RPC endpoints, tried in order when the primary is unreachable."),
            v("STELLAR_RPC_HEADERS", "string", N, "Extra headers sent with every RPC request, as semicolon-separated `Name: Value` pairs (e.g. `X-API-Key: key; X-Org: acme`). Values are never logged."),
            v("ALLOW_INSECURE_RPC", "bool", d("false"), "Allow HTTP and loopback/private RPC URLs. Local development only."),
            v("RPC_CONNECT_TIMEOUT_SECS", "integer", d("5"), "TCP connect timeout for RPC requests."),
            v("RPC_REQUEST_TIMEOUT_SECS", "integer", d("30"), "Total timeout for a single RPC request."),
            v("RPC_MAX_EVENTS_PER_PAGE", "integer", d("200"), "Maximum events requested per `getEvents` page."),
            v("START_LEDGER", "integer", d("0"), "Ledger sequence to start indexing from. `0` starts from the latest ledger.").example("0"),
            v("START_LEDGER_FALLBACK", "bool", d("false"), "When `START_LEDGER` is outside the RPC retention window, fall back to the oldest available ledger instead of failing."),
            v("INDEXER_EVENT_TYPES", "list", N, "Comma-separated event types to index: `contract`, `diagnostic`, `system`. Empty indexes all types."),
            v("INDEXER_POLL_INTERVAL_MS", "integer", d("5000"), "Poll interval when no new ledgers are available (100–60000)."),
            v("INDEXER_ERROR_BACKOFF_MS", "integer", d("10000"), "Backoff after an RPC error (100–60000)."),
            v("INDEXER_LAG_WARN_THRESHOLD", "integer", d("100"), "Warn when the indexer falls this many ledgers behind the network."),
            v("INDEXER_STALL_TIMEOUT_SECS", "integer", d("60"), "Seconds without progress after which the indexer is reported as stalled."),
            v("INDEXER_LOCK_RETRY_SECS", "integer", d("30"), "How often standby replicas retry acquiring the indexer advisory lock."),
            v("INDEXER_IGNORE_CHECKPOINT", "bool", d("false"), "Ignore the persisted indexer checkpoint and start from `START_LEDGER`."),
            v("MAX_EVENT_DATA_BYTES", "integer", d("65536"), "Events whose serialized `event_data` exceeds this size are logged and skipped."),
        ],
    },
    Section {
        title: "Multi-chain",
        vars: &[
            v("CHAIN_ID", "string", d("mainnet"), "Chain identifier stamped on events indexed from `STELLAR_RPC_URL`."),
            v("ADDITIONAL_CHAIN_IDS", "list", N, "Comma-separated extra chain IDs to index concurrently. Each needs a matching `STELLAR_RPC_URL_<CHAIN_ID>`."),
            v("STELLAR_RPC_URL_<CHAIN_ID>", "url", N, "RPC endpoint for an entry in `ADDITIONAL_CHAIN_IDS`. The chain ID is upper-cased with non-alphanumerics replaced by `_` (e.g. `STELLAR_RPC_URL_FUTURENET`).").source(Source::Pattern),
            v("LEDGER_HASH_TRACKING_ENABLED", "bool", d("false"), "Store ledger hashes and verify hash-chain continuity on startup."),
            v("LEDGER_HASH_VALIDATION_DEPTH", "integer", d("1000"), "Number of ledgers checked during startup hash-chain validation."),
        ],
    },
    Section {
        title: "Authentication and access control",
        vars: &[
            v("API_KEY", "secret", N, "API key for authenticated endpoints. When unset, authentication is disabled."),
            v("API_KEY_SECONDARY", "secret", N, "Second accepted API key, for rotating `API_KEY` without downtime."),
            v("ADMIN_API_KEY", "secret", N, "Key required for `/v1/admin/*` endpoints, independent of `API_KEY`. When unset, admin endpoints fall back to the `API_KEY` gate."),
            v("ADMIN_API_KEY_SECONDARY", "secret", N, "Second accepted admin key, for rotating `ADMIN_API_KEY` without downtime."),
            v("IP_DENYLIST", "list", N, "Comma-separated IPs/CIDR blocks to reject."),
            v("IP_ALLOWLIST", "list", N, "Comma-separated IPs/CIDR blocks to exclusively allow. Empty disables enforcement."),
        ],
    },
    Section {
        title: "Rate limiting",
        vars: &[
            v("RATE_LIMIT_PER_MINUTE", "integer", d("60"), "Per-IP request limit per minute. Exceeding it returns 429 with `Retry-After`."),
            v("RATE_LIMIT_KEY_PER_MINUTE", "integer", N, "Per-API-key sliding-window limit per minute. Unset or `0` disables."),
            v("RATE_LIMIT_KEY_PER_HOUR", "integer", N, "Per-API-key limit per hour. Unset or `0` disables."),
            v("RATE_LIMIT_KEY_PER_DAY", "integer", N, "Per-API-key limit per day. Unset or `0` disables."),
            v("RATE_LIMIT_KEY_PER_MONTH", "integer", N, "Per-API-key limit per rolling 30-day window. Unset or `0` disables."),
        ],
    },
    Section {
        title: "Multi-tenancy",
        vars: &[
            v("MULTI_TENANT", "bool", d("false"), "Enable multi-tenant mode. Every API key must then map to a tenant."),
            v("TENANT_RATE_LIMIT_PER_MINUTE", "integer", N, "Independent per-tenant request quota per minute (multi-tenant mode only)."),
            v("TENANT_CONTRACT_FILTER", "string", N, "Per-tenant indexer contract filter as semicolon-separated `tenant:contract1,contract2` pairs."),
            v("INDEXER_TENANT_ID", "string", N, "Tenant ID stamped on every event this indexer inserts (multi-tenant mode only)."),
        ],
    },
    Section {
        title: "API limits and caching",
        vars: &[
            v("EXPORT_MAX_ROWS", "integer", d("10000"), "Maximum rows returned by `GET /v1/events/export`."),
            v("MAX_LEDGER_RANGE", "integer", d("100000"), "Maximum ledger span accepted by diff/range queries."),
            v("BATCH_TX_MAX_SIZE", "integer", d("100"), "Maximum transaction hashes per batch lookup request."),
            v("BATCH_EVENTS_MAX_SIZE", "integer", d("10000"), "Maximum events per bulk insert request."),
            v("CONTRACT_COUNT_CACHE_SIZE", "integer", d("1000"), "Maximum entries in the per-contract event count cache."),
            v("CONTRACT_COUNT_CACHE_TTL_SECS", "integer", d("30"), "TTL of contract count cache entries."),
            v("AGGREGATION_CACHE_TTL_SECS", "integer", d("60"), "TTL of cached aggregation results."),
            v("AGGREGATION_CACHE_MAX_ENTRIES", "integer", d("1000"), "Maximum cached aggregation results."),
            v("QUERY_CACHE_TTL_SECS", "integer", d("300"), "TTL of cached query results, clamped to 300–3600."),
            v("QUERY_CACHE_MAX_CAPACITY", "integer", d("1000"), "Maximum cached query results."),
            v("STATS_CACHE_TTL_SECS", "integer", d("30"), "TTL of the cached `/v1/stats` response."),
            v("STATS_REFRESH_INTERVAL_SECS", "integer", d("3600"), "How often materialized statistics are refreshed."),
            v("ABI_CACHE_TTL_SECS", "integer", d("86400"), "TTL of the in-process contract ABI cache."),
            v("ABI_CACHE_MAX_ENTRIES", "integer", d("10000"), "Maximum ABIs kept in the in-process cache."),
            v("ENABLE_PUSH_PRELOAD", "bool", d("false"), "Add `Link` preload headers for a contract's schema and ABI to `/v1/events/contract/{contract_id}` responses."),
        ],
    },
    Section {
        title: "Server-sent events (SSE)",
        vars: &[
            v("SSE_KEEPALIVE_SECS", "integer", N, "Keep-alive ping interval in seconds (1–60). Takes precedence over `SSE_KEEPALIVE_INTERVAL_MS`."),
            v("SSE_KEEPALIVE_INTERVAL_MS", "integer", d("15000"), "Keep-alive ping interval in milliseconds (1000–60000). Keep it below your proxy's idle timeout."),
            v("SSE_MAX_CONNECTIONS", "integer", d("1000"), "Maximum concurrent SSE connections."),
            v("SSE_MAX_CONNECTIONS_PER_IP", "integer", d("10"), "Maximum concurrent SSE connections per client IP."),
            v("SSE_MULTI_MAX_CONTRACT_IDS", "integer", d("100"), "Maximum contract IDs per multi-contract stream."),
            v("SSE_DRAIN_TIMEOUT_SECS", "integer", d("5"), "Time allowed for SSE clients to drain during shutdown (1–60)."),
            v("SSE_SHUTDOWN_GRACE_PERIOD_MS", "integer", d("200"), "Delay between broadcasting the close event and dropping the SSE broadcaster."),
            v("SSE_REPLAY_MAX_EVENTS", "integer", d("1000"), "Maximum events replayed on reconnect with `Last-Event-ID`. Takes precedence over `SSE_REPLAY_LIMIT`."),
            v("SSE_REPLAY_LIMIT", "integer", d("1000"), "Legacy alias of `SSE_REPLAY_MAX_EVENTS`."),
            v("SSE_RING_BUFFER_CAPACITY", "integer", d("10000"), "Events kept in the in-memory replay ring buffer; the oldest is evicted when full."),
            v("SSE_MAX_LAG_BEFORE_DISCONNECT", "integer", d("0"), "Disconnect clients lagging this many events behind. `0` disables."),
        ],
    },
    Section {
        title: "Logging and observability",
        vars: &[
            v("RUST_LOG", "string", d("info"), "Log filter (`trace`, `debug`, `info`, `warn`, `error`, or per-module directives).").example("info").source(Source::External),
            v("RUST_LOG_FORMAT", "string", d("text"), "Log output format: `text` or `json`.").example("text").source(Source::External),
            v("LOG_SAMPLE_RATE", "integer", d("1"), "Sample debug-level logs: `1` logs everything, `100` logs 1%. INFO and above are never sampled."),
            v("SLOW_REQUEST_THRESHOLD_MS", "integer", d("500"), "HTTP requests slower than this are logged at WARN level.").source(Source::External),
            v("OTEL_EXPORTER_OTLP_ENDPOINT", "url", N, "OTLP collector endpoint (e.g. `http://localhost:4317`). Enables OpenTelemetry tracing when set.").feature("otel").source(Source::External),
        ],
    },
    Section {
        title: "Retention, deduplication and storage",
        vars: &[
            v("RETENTION_DAYS", "integer", d("90"), "Days to keep events before automatic pruning. `0` keeps events forever."),
            v("PRUNING_INTERVAL_HOURS", "integer", d("24"), "How often the pruning task runs."),
            v("ARCHIVE_AFTER_DAYS", "integer", d("30"), "Age in days after which events are moved to archive storage."),
            v("BLOOM_FILTER_FP_RATE", "float", d("0.001"), "False-positive rate of the bloom pre-deduplication filter."),
            v("BLOOM_FILTER_CAPACITY", "integer", d("1000000"), "Number of unique events the bloom filter is sized for."),
            v("ENABLE_CONTENT_DEDUP", "bool", d("false"), "Skip events whose content fingerprint matches a recent row."),
            v("DEDUP_WINDOW_SECS", "integer", d("3600"), "Lookback window for content fingerprint deduplication."),
            v("EVENT_COMPRESSION_ENABLED", "bool", d("false"), "Gzip-compress `event_data` into `event_data_compressed`."),
            v("EVENT_COMPRESSION_MIGRATION_BATCH_SIZE", "integer", d("500"), "Batch size when migrating existing events to compressed storage."),
        ],
    },
    Section {
        title: "Encryption",
        vars: &[
            v("EVENT_DATA_ENCRYPTION_KEY", "secret", N, "64-hex-character AES-256-GCM key encrypting `event_data` at rest.").feature("encryption"),
            v("EVENT_DATA_ENCRYPTION_KEY_OLD", "secret", N, "Previous event data key, kept during rotation while `POST /v1/admin/reencrypt` runs.").feature("encryption"),
            v("INTEGRATION_ENCRYPTION_KEY", "secret", N, "64-hex-character AES-256-GCM key encrypting stored integration credentials (bot tokens, webhook URLs).").feature("encryption"),
            v("INTEGRATION_ENCRYPTION_KEY_OLD", "secret", N, "Previous integration key, used during rotation.").feature("encryption"),
        ],
    },
    Section {
        title: "Webhooks",
        vars: &[
            v("WEBHOOK_URL", "url", N, "POST every newly indexed event as JSON to this URL."),
            v("WEBHOOK_SECRET", "secret", N, "HMAC-SHA256 secret. Each delivery carries `X-Signature-256: sha256=<hex>` over the raw body."),
            v("WEBHOOK_CONTRACT_FILTER", "list", N, "Comma-separated contract IDs; only matching events are delivered. Empty delivers all events."),
            v("WEBHOOK_CONTENT_FILTER", "json", N, "JSON content filter such as `{\"path\":\"$.amount\",\"op\":\"gt\",\"value\":\"1000000\"}`. Invalid filters are logged and ignored."),
            v("WEBHOOK_RATE_LIMIT_PER_MINUTE", "integer", N, "Maximum webhook deliveries per minute. Unset or `0` disables."),
            v("WEBHOOK_RATE_LIMIT_PER_HOUR", "integer", N, "Maximum webhook deliveries per hour. Unset or `0` disables."),
            v("WEBHOOK_REQUIRE_HTTPS", "bool", N, "Reject non-HTTPS webhook URLs. Defaults to `true` in staging/production and `false` otherwise."),
            v("WEBHOOK_NOTIFICATION_FORMAT", "string", d("raw"), "Payload format: `raw` event JSON, `slack` or `discord`."),
            v("WEBHOOK_MESSAGE_TEMPLATE", "string", N, "Optional message template used to render webhook notifications."),
        ],
    },
    Section {
        title: "Email notifications",
        vars: &[
            v("EMAIL_SMTP_HOST", "string", N, "SMTP host. Email notifications are enabled when set."),
            v("EMAIL_SMTP_PORT", "integer", d("587"), "SMTP port (587 for STARTTLS)."),
            v("EMAIL_SMTP_USER", "string", N, "SMTP username."),
            v("EMAIL_SMTP_PASSWORD", "secret", N, "SMTP password. Never logged."),
            v("EMAIL_FROM", "string", N, "Sender address. Required when `EMAIL_SMTP_HOST` is set."),
            v("EMAIL_TO", "list", N, "Comma-separated recipients. Required when `EMAIL_SMTP_HOST` is set."),
            v("EMAIL_CONTRACT_FILTER", "list", N, "Comma-separated contract IDs; only matching events trigger emails."),
            v("EMAIL_SCHEDULE", "string", d("immediate"), "Delivery schedule: `immediate`, `hourly_digest`, `daily_digest` or `custom_cron`."),
            v("EMAIL_DAILY_DIGEST_HOUR", "integer", d("9"), "UTC hour (0–23) at which the daily digest is sent."),
            v("EMAIL_CRON", "string", N, "Cron expression used when `EMAIL_SCHEDULE=custom_cron`."),
            v("EMAIL_QUIET_HOURS_START", "string", N, "Start of the quiet window (`HH:MM`, UTC) during which emails are held."),
            v("EMAIL_QUIET_HOURS_END", "string", N, "End of the quiet window (`HH:MM`, UTC)."),
            v("EMAIL_MAX_CONTRACTS_IN_DIGEST", "integer", d("20"), "Maximum contracts listed individually in a digest email."),
            v("EMAIL_PUBLIC_BASE_URL", "url", N, "Public base URL for unsubscribe links. Defaults to `http://localhost:<PORT>`."),
        ],
    },
    Section {
        title: "SMS notifications",
        vars: &[
            v("TWILIO_ACCOUNT_SID", "string", N, "Twilio account SID. SMS notifications are enabled when the Twilio credentials are set."),
            v("TWILIO_AUTH_TOKEN", "secret", N, "Twilio auth token."),
            v("TWILIO_FROM_NUMBER", "string", N, "Sending phone number in E.164 format."),
            v("SMS_TO_NUMBERS", "list", N, "Comma-separated recipient phone numbers."),
            v("SMS_CONTRACT_FILTER", "list", N, "Comma-separated contract IDs; only matching events trigger SMS."),
        ],
    },
    Section {
        title: "PagerDuty, escalation and on-call",
        vars: &[
            v("PAGERDUTY_ROUTING_KEY", "secret", N, "PagerDuty Events API v2 routing key. PagerDuty alerts are enabled when set."),
            v("PAGERDUTY_SERVICE_NAME", "string", d("Soroban Pulse"), "Source name shown on PagerDuty incidents."),
            v("PAGERDUTY_CONTRACT_FILTER", "list", N, "Comma-separated contract IDs that trigger incidents."),
            v("PAGERDUTY_EVENT_TYPE_FILTER", "list", N, "Comma-separated event types that trigger incidents."),
            v("PAGERDUTY_SEVERITY_MAPPING", "json", d("{\"contract\":\"error\",\"diagnostic\":\"warning\",\"system\":\"info\"}"), "JSON map from event type to PagerDuty severity."),
            v("PAGERDUTY_AUTO_RESOLVE", "bool", d("true"), "Automatically resolve incidents after the threshold below."),
            v("PAGERDUTY_AUTO_RESOLVE_THRESHOLD_MINUTES", "integer", d("30"), "Minutes without matching events before an incident is auto-resolved."),
            v("NOTIFICATION_DEFAULT_PRIORITY", "string", d("medium"), "Default notification priority: `low`, `medium`, `high` or `critical`."),
            v("NOTIFICATION_PRIORITY_RULE_PATH", "string", N, "JSON path in event data evaluated by the priority rule."),
            v("NOTIFICATION_PRIORITY_RULE_VALUE", "string", N, "Value that `NOTIFICATION_PRIORITY_RULE_PATH` must match."),
            v("NOTIFICATION_PRIORITY_RULE_PRIORITY", "string", N, "Priority assigned when the rule matches."),
            v("ESCALATION_PRIMARY_CHANNEL", "string", N, "Channel notified first (e.g. `email`, `webhook`)."),
            v("ESCALATION_CHANNEL", "string", N, "Channel escalated to when the primary is not acknowledged."),
            v("ESCALATION_DELAY_MINUTES", "integer", d("30"), "Minutes before escalating to `ESCALATION_CHANNEL`."),
            v("ONCALL_PROVIDER", "string", N, "On-call rotation provider (e.g. `pagerduty`)."),
            v("ONCALL_PAGERDUTY_API_KEY", "secret", N, "PagerDuty REST API key used to read on-call schedules."),
            v("ONCALL_SCHEDULE_ID", "string", N, "On-call schedule ID to route notifications to."),
            v("ONCALL_SCHEDULE_CACHE_TTL_SECS", "integer", d("300"), "TTL of the cached on-call schedule."),
            v("NOTIFICATION_AUDIT_LOG_RETENTION_DAYS", "integer", d("90"), "Days to keep `notification_audit_log` entries.").example("90").source(Source::External),
            v("NOTIFICATION_HEALTH_CHECK_INTERVAL_SECS", "integer", d("300"), "How often each notification channel is health-checked.").example("300").source(Source::External),
            v("NOTIFICATION_HEALTH_CHECK_EMAIL", "string", N, "Target address for email channel health checks.").source(Source::External),
        ],
    },
    Section {
        title: "Streaming integrations",
        vars: &[
            v("KAFKA_BROKERS", "list", N, "Comma-separated Kafka brokers (e.g. `localhost:9092`). Publishing is enabled when set.").feature("kafka"),
            v("KAFKA_TOPIC", "string", N, "Kafka topic. Required when `KAFKA_BROKERS` is set.").feature("kafka"),
            v("KAFKA_BATCH_SIZE", "integer", d("16384"), "Kafka producer batch size in bytes.").feature("kafka"),
            v("KAFKA_LINGER_MS", "integer", d("100"), "Kafka producer linger time.").feature("kafka"),
            v("KINESIS_STREAM_NAME", "string", N, "Kinesis Data Stream to publish to. Requires `AWS_REGION`.").feature("kinesis"),
            v("KINESIS_PARTITION_KEY_FIELD", "string", d("contract_id"), "Partition key strategy: `contract_id`, `tx_hash` or `random`.").feature("kinesis").source(Source::External),
            v("AWS_REGION", "string", N, "AWS region for Kinesis and SQS. Credentials come from the standard AWS chain."),
            v("SQS_QUEUE_URL", "url", N, "SQS queue URL. Publishing is enabled when set together with `AWS_REGION`.").feature("sqs"),
            v("SQS_DLQ_URL", "url", N, "Queue receiving entries that fail in a batch.").feature("sqs"),
            v("SQS_BATCH_SIZE", "integer", d("10"), "Messages per `SendMessageBatch` call (AWS caps this at 10).").feature("sqs"),
            v("EVENT_HUBS_CONNECTION_STRING", "secret", N, "Azure Event Hubs connection string with Send permission."),
            v("EVENT_HUB_NAME", "string", N, "Event hub name. Optional when `EntityPath` is in the connection string."),
            v("EVENT_HUBS_PARTITION_STRATEGY", "string", d("contract_id"), "Partition key strategy: `contract_id`, `tx_hash` or `ledger`."),
            v("PUBSUB_PROJECT_ID", "string", N, "GCP project for Pub/Sub publishing. Requires `PUBSUB_TOPIC_ID`.").feature("pubsub"),
            v("PUBSUB_TOPIC_ID", "string", N, "Pub/Sub topic ID (without the `projects/…/topics/` prefix).").feature("pubsub"),
            v("PUBSUB_ENABLE_MESSAGE_ORDERING", "bool", d("true"), "Publish with an ordering key per contract.").feature("pubsub"),
            v("GOOGLE_APPLICATION_CREDENTIALS", "string", N, "Path to GCP service account JSON. Leave unset on GCE/GKE/Cloud Run.").feature("pubsub").source(Source::External),
            v("REDIS_URL", "url", N, "Redis URL. Each indexed event is published to a Redis Stream when set.").feature("redis-queue"),
            v("REDIS_STREAM_KEY", "string", N, "Redis Stream key events are appended to.").feature("redis-queue"),
            v("REDIS_BUFFER_MAX_SIZE", "integer", d("10000"), "Events buffered in memory while Redis is unreachable; the oldest are dropped when full.").feature("redis-queue"),
        ],
    },
    Section {
        title: "Lua event transformation",
        vars: &[
            v("EVENT_TRANSFORM_SCRIPT", "string", N, "Path to a Lua script that transforms or filters events before storage.").feature("lua"),
            v("EVENT_TRANSFORM_TIMEOUT_MS", "integer", d("100"), "Per-event script timeout; events whose script times out are skipped.").feature("lua"),
        ],
    },
    Section {
        title: "Dashboard",
        vars: &[
            v("SERVE_DASHBOARD", "bool", d("false"), "Serve the built web dashboard at `/ui`.").feature("dashboard"),
            v("DASHBOARD_DIR", "string", d("web/dist"), "Directory containing the built dashboard.").feature("dashboard"),
            v("DASHBOARD_CONNECT_SRC", "list", N, "Extra origins appended to the dashboard's CSP `connect-src`.").feature("dashboard"),
        ],
    },
    Section {
        title: "SaaS platform",
        vars: &[
            v("SAAS_ENABLED", "bool", d("false"), "Enable tenant management and billing integration."),
            v("BILLING_PROVIDER", "string", N, "Billing provider (e.g. `stripe`, `paddle`)."),
            v("BILLING_API_KEY", "secret", N, "Billing provider API key."),
            v("DEFAULT_TRIAL_DAYS", "integer", d("14"), "Trial length for new tenants."),
            v("SAAS_PUBLIC_BASE_URL", "url", N, "Public base URL used in tenant onboarding links."),
        ],
    },
    Section {
        title: "Machine learning",
        vars: &[
            v("ML_ANOMALY_DETECTION_ENABLED", "bool", d("false"), "Enable ML-based anomaly detection."),
            v("ML_PATTERN_RECOGNITION_ENABLED", "bool", d("false"), "Enable event pattern recognition."),
            v("ML_INTELLIGENT_FILTERING_ENABLED", "bool", d("false"), "Enable ML-driven event filtering."),
            v("ML_MODEL_TRAINING_INTERVAL_HOURS", "integer", d("24"), "How often models are retrained."),
            v("ML_MIN_TRAINING_SAMPLES", "integer", d("100"), "Minimum samples required before training."),
            v("ML_CONFIDENCE_THRESHOLD", "float", d("0.8"), "Minimum model confidence (0.0–1.0) for a result to be acted on."),
            v("ML_AUTO_RETRAIN", "bool", d("true"), "Retrain models automatically on the interval above."),
        ],
    },
    Section {
        title: "Docker Compose",
        vars: &[
            v("POSTGRES_USER", "string", N, "PostgreSQL user created by the Compose `db` service. Must match `DATABASE_URL`.").example("<USER>").source(Source::External),
            v("POSTGRES_PASSWORD", "secret", N, "PostgreSQL password for the Compose `db` service.").example("<PASSWORD>").source(Source::External),
            v("POSTGRES_DB", "string", N, "Database created by the Compose `db` service.").example("soroban_pulse").source(Source::External),
        ],
    },
];

/// Iterate over every documented variable in section order.
pub fn all_vars() -> impl Iterator<Item = &'static EnvVarDoc> {
    SECTIONS.iter().flat_map(|s| s.vars.iter())
}
