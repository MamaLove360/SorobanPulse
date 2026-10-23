use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;
use secrecy::SecretString;

pub mod reference;
mod builder;
pub use builder::{ConfigBuilder, ConfigError, DatabaseSettings, Missing, RpcSettings, ServerSettings, Set};

/// Load an optional TOML config file. Returns an empty table if the file is
/// absent or CONFIG_FILE is not set — never an error.
fn load_config_file() -> toml::Table {
    let path = match env::var("CONFIG_FILE") {
        Ok(p) if !p.is_empty() => p,
        _ => return toml::Table::new(),
    };
    match std::fs::read_to_string(&path) {
        Ok(contents) => contents.parse::<toml::Table>().unwrap_or_else(|e| {
            eprintln!("Warning: failed to parse config file '{path}': {e}");
            toml::Table::new()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => toml::Table::new(),
        Err(e) => {
            eprintln!("Warning: could not read config file '{path}': {e}");
            toml::Table::new()
        }
    }
}

/// Return the env var value if set, otherwise fall back to the TOML table.
fn env_or_file(key: &str, file: &toml::Table) -> Option<String> {
    env::var(key).ok().filter(|v| !v.is_empty()).or_else(|| {
        file.get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    })
}

/// Like `env_or_file` but returns a default string when neither source has the key.
fn env_or_file_or(key: &str, file: &toml::Table, default: &str) -> String {
    env_or_file(key, file).unwrap_or_else(|| default.to_string())
}

/// Parse the optional webhook content filter (Issue #477) from
/// `WEBHOOK_CONTENT_FILTER`, which holds a JSON object such as
/// `{"path":"$.amount","op":"gt","value":"1000000"}`. An unparseable or invalid
/// filter is logged and ignored so a bad value never disables delivery silently
/// at startup.
fn parse_webhook_content_filter() -> Option<crate::content_filter::ContentFilter> {
    let raw = env::var("WEBHOOK_CONTENT_FILTER")
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    match serde_json::from_str::<crate::content_filter::ContentFilter>(&raw) {
        Ok(filter) => match filter.validate() {
            Ok(()) => Some(filter),
            Err(e) => {
                tracing::warn!(error = %e, "Ignoring invalid WEBHOOK_CONTENT_FILTER");
                None
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, "Ignoring unparseable WEBHOOK_CONTENT_FILTER");
            None
        }
    }
}

/// Shared operational state updated by the indexer and read by the /status handler.
pub struct IndexerState {
    pub current_ledger: AtomicU64,
    pub latest_ledger: AtomicU64,
    /// True when this replica holds the advisory lock and is actively indexing.
    pub is_active_indexer: AtomicBool,
    /// True when the indexer loop has been paused via the admin API.
    pub is_paused: AtomicBool,
    started_at: u64,
    /// Issue #627: Bloom filter for quick contract existence check
    pub bloom_filter: Option<Arc<crate::bloom_filter::EventBloomFilter>>,
}

impl IndexerState {
    pub fn new() -> Self {
        let started_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            current_ledger: AtomicU64::new(0),
            latest_ledger: AtomicU64::new(0),
            is_active_indexer: AtomicBool::new(false),
            is_paused: AtomicBool::new(false),
            started_at,
            bloom_filter: None,
        }
    }

    pub fn with_bloom_filter(mut self, bloom_filter: Arc<crate::bloom_filter::EventBloomFilter>) -> Self {
        self.bloom_filter = Some(bloom_filter);
        self
    }

    pub fn uptime_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.started_at)
    }
}

/// Deployment role — controls which subsystems start on this instance.
///
/// | Value      | Starts indexer? | Starts HTTP server? |
/// |------------|-----------------|---------------------|
/// | `All`      | ✓               | ✓ (default)         |
/// | `Api`      | ✗               | ✓                   |
/// | `Indexer`  | ✓               | ✓ (health + metrics only) |
///
/// Set via `ROLE=all|api|indexer`.  The default (`all`) preserves
/// backward-compatible behaviour — a single pod runs everything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    /// Run both the indexer and the full HTTP server (default).
    All,
    /// Run only the HTTP API server; skip indexer startup entirely.
    Api,
    /// Run only the indexer; expose a minimal HTTP server for health/metrics.
    Indexer,
}

impl Role {
    fn from_str(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "api" => Self::Api,
            "indexer" => Self::Indexer,
            _ => Self::All,
        }
    }

    /// Returns `true` when the indexer background task should be spawned.
    pub fn runs_indexer(&self) -> bool {
        matches!(self, Self::All | Self::Indexer)
    }

    /// Returns `true` when the full Axum HTTP server should bind.
    /// Even the `Indexer` role binds a minimal server for health/metrics.
    pub fn runs_http_server(&self) -> bool {
        true
    }

    /// Returns `true` when all API routes should be registered.
    /// The `Indexer` role only registers health + metrics routes.
    pub fn runs_full_api(&self) -> bool {
        matches!(self, Self::All | Self::Api)
    }
}

/// Deployment environment — controls strictness of defaults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Environment {
    Development,
    Staging,
    Production,
}

impl Environment {
    fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "production" | "prod" => Self::Production,
            "staging" | "stage" => Self::Staging,
            _ => Self::Development,
        }
    }

    /// Returns `true` for staging and production.
    pub fn is_production_like(&self) -> bool {
        matches!(self, Self::Staging | Self::Production)
    }
}

/// Shared state for health checks, accessible between the indexer and HTTP handlers
#[derive(Clone)]
pub struct HealthState {
    /// Unix timestamp of the last successful indexer poll
    pub last_indexer_poll: Arc<AtomicU64>,
    /// Timeout in seconds after which the indexer is considered stalled
    pub indexer_stall_timeout_secs: u64,
}

impl HealthState {
    pub fn new(indexer_stall_timeout_secs: u64) -> Self {
        Self {
            last_indexer_poll: Arc::new(AtomicU64::new(0)),
            indexer_stall_timeout_secs,
        }
    }

    /// Update the last poll timestamp to the current time
    pub fn update_last_poll(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_indexer_poll.store(now, Ordering::SeqCst);
    }

    /// Check if the indexer is stalled (no successful poll within the timeout)
    /// Returns Some(seconds_ago) if stalled, None if OK
    pub fn is_indexer_stalled(&self) -> Option<u64> {
        let last_poll = self.last_indexer_poll.load(Ordering::SeqCst);
        if last_poll == 0 {
            // No poll ever completed
            return Some(0);
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let elapsed = now.saturating_sub(last_poll);
        if elapsed > self.indexer_stall_timeout_secs {
            Some(elapsed)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    /// @env DATABASE_URL
    /// @default (required)
    /// @description PostgreSQL connection string. Also loadable from a file path via DATABASE_URL_FILE.
    pub database_url: String,

    /// @env DATABASE_REPLICA_URL
    /// @default (none)
    /// @description Optional read-replica URL. When set, HTTP handlers use this pool; the indexer uses the primary.
    pub database_replica_url: Option<String>,

    /// @env STELLAR_RPC_URL
    /// @default https://soroban-testnet.stellar.org
    /// @description Soroban RPC endpoint. Must be HTTPS unless ALLOW_INSECURE_RPC=true.
    pub stellar_rpc_url: String,

    /// @env STELLAR_RPC_FALLBACK_URLS
    /// @default (none)
    /// @description Comma-separated list of fallback RPC endpoints tried when the primary fails.
    pub stellar_rpc_fallback_urls: Vec<String>,

    /// @env STELLAR_RPC_HEADERS
    /// @default (none)
    /// @description Semicolon-separated "Name: Value" pairs injected into every RPC request. Values are never logged.
    pub rpc_headers: Vec<(String, String)>,

    /// @env START_LEDGER
    /// @default 0
    /// @description Ledger sequence to start indexing from. 0 = start from the latest ledger at boot.
    pub start_ledger: u64,

    /// @env START_LEDGER_FALLBACK
    /// @default false
    /// @description When true, fall back to the latest ledger if START_LEDGER is no longer available on the RPC.
    pub start_ledger_fallback: bool,

    /// @env PORT
    /// @default 3000
    /// @description HTTP server listening port.
    pub port: u16,

    /// @env API_KEY
    /// @default (disabled)
    /// @description Optional API key for request authentication. Also accepts API_KEY_SECONDARY for rotation. When unset all requests pass through.
    pub api_keys: Vec<SecretString>,

    /// @env ADMIN_API_KEY
    /// @default (disabled)
    /// @description Key required for /v1/admin/* endpoints, independent of API_KEY. Also accepts ADMIN_API_KEY_SECONDARY. When unset, admin endpoints fall back to API_KEY.
    pub admin_api_keys: Vec<SecretString>,

    /// @env DB_MAX_CONNECTIONS
    /// @default 10
    /// @description Maximum number of connections in the PostgreSQL connection pool.
    pub db_max_connections: u32,

    /// @env DB_MIN_CONNECTIONS
    /// @default 2
    /// @description Minimum number of idle connections kept open in the PostgreSQL pool.
    pub db_min_connections: u32,

    /// @env DB_IDLE_TIMEOUT_SECS
    /// @default 600
    /// @description Seconds before an idle connection is closed and removed from the pool.
    pub db_idle_timeout_secs: u64,

    /// @env DB_MAX_LIFETIME_SECS
    /// @default 1800
    /// @description Maximum lifetime in seconds for a pooled database connection.
    pub db_max_lifetime_secs: u64,

    /// @env DB_TEST_BEFORE_ACQUIRE
    /// @default true
    /// @description When true, a cheap health-check query is run before returning a connection from the pool.
    pub db_test_before_acquire: bool,

    /// @env BEHIND_PROXY
    /// @default false
    /// @description Set to true when running behind a reverse proxy so the real client IP is extracted from X-Forwarded-For.
    pub behind_proxy: bool,

    /// @env RPC_CONNECT_TIMEOUT_SECS
    /// @default 5
    /// @description TCP connection timeout in seconds for RPC requests.
    pub rpc_connect_timeout_secs: u64,

    /// @env RPC_REQUEST_TIMEOUT_SECS
    /// @default 30
    /// @description Total request timeout in seconds for RPC calls.
    pub rpc_request_timeout_secs: u64,

    /// @env ALLOWED_ORIGINS
    /// @default *
    /// @description Comma-separated CORS allowed origins. Wildcard (*) is rejected in production-like environments.
    pub allowed_origins: Vec<String>,

    /// @env RATE_LIMIT_PER_MINUTE
    /// @default 60
    /// @description Maximum HTTP requests per IP per minute. Set to 0 to disable rate limiting.
    pub rate_limit_per_minute: u32,

    /// @env INDEXER_LAG_WARN_THRESHOLD
    /// @default 100
    /// @description Number of ledgers behind the tip before a warning is emitted.
    /// Per-IP rate limit for `POST /v1/subscriptions` (Issue #1268).
    /// Stricter than the global `rate_limit_per_minute` to prevent DB
    /// connection exhaustion via subscription spam. Set via
    /// SUBSCRIPTION_RATE_LIMIT_PER_MINUTE (default 5).
    pub subscription_rate_limit_per_minute: u32,
    /// Per-API-key rate limit: max requests per minute (Issue #669).
    /// When set, each API key gets its own sliding-window quota stored in the
    /// `rate_limit_counters` table. `None` disables per-key enforcement.
    /// Set via RATE_LIMIT_KEY_PER_MINUTE env var.
    pub rate_limit_key_per_minute: Option<u32>,
    /// Per-API-key rate limit: max requests per hour (Issue #669).
    /// Set via RATE_LIMIT_KEY_PER_HOUR env var.
    pub rate_limit_key_per_hour: Option<u32>,
    /// Per-API-key rate limit: max requests per day (Issue #669).
    /// Set via RATE_LIMIT_KEY_PER_DAY env var.
    pub rate_limit_key_per_day: Option<u32>,
    /// Per-API-key rate limit: max requests per rolling 30-day window (Issue #941).
    /// Set via RATE_LIMIT_KEY_PER_MONTH env var.
    pub rate_limit_key_per_month: Option<u32>,
    /// IP addresses/CIDR blocks to block (Issue #942). Comma-separated,
    /// IPv4 or IPv6. Set via IP_DENYLIST env var. Empty disables enforcement.
    pub ip_denylist: Vec<String>,
    /// IP addresses/CIDR blocks to exclusively allow (Issue #942).
    /// Comma-separated, IPv4 or IPv6. Set via IP_ALLOWLIST env var. Empty
    /// disables enforcement (every IP allowed).
    pub ip_allowlist: Vec<String>,
    pub indexer_lag_warn_threshold: u64,

    /// @env INDEXER_STALL_TIMEOUT_SECS
    /// @default 60
    /// @description Seconds without a successful indexer poll before the health check reports unhealthy.
    pub indexer_stall_timeout_secs: u64,

    /// @env DB_STATEMENT_TIMEOUT_MS
    /// @default 5000
    /// @description PostgreSQL statement timeout in milliseconds applied to every query.
    pub db_statement_timeout_ms: u64,

    /// @env INDEXER_POLL_INTERVAL_MS
    /// @default 5000
    /// @description Milliseconds between indexer polls when the chain is caught up. Range: [100, 60000].
    pub indexer_poll_interval_ms: u64,

    /// @env INDEXER_ERROR_BACKOFF_MS
    /// @default 10000
    /// @description Milliseconds to wait after an indexer error before retrying. Range: [100, 60000].
    pub indexer_error_backoff_ms: u64,

    /// @env SSE_KEEPALIVE_SECS
    /// @default 15
    /// @description SSE keep-alive ping interval in seconds (1–60). Prevents proxies from closing idle streams.
    pub sse_keepalive_interval_ms: u64,

    /// @env SSE_MAX_CONNECTIONS
    /// @default 1000
    /// @description Global maximum number of concurrent SSE connections the server will accept.
    pub sse_max_connections: usize,

    /// @env SSE_DRAIN_TIMEOUT_SECS
    /// @default 5
    /// @description Seconds to wait for SSE clients to drain before forcefully closing on shutdown. Range: [1, 60].
    pub sse_drain_timeout_secs: u64,

    /// @env SSE_MULTI_MAX_CONTRACT_IDS
    /// @default 100
    /// @description Maximum number of contract IDs per multiplexed SSE connection.
    pub sse_multi_max_contract_ids: usize,

    /// @env ENVIRONMENT
    /// @default development
    /// @description Deployment environment: development, staging, or production. Controls strictness of config validation.
    pub environment: Environment,

    /// @env MAX_BODY_SIZE_BYTES
    /// @default 1048576
    /// @description Maximum HTTP request body size in bytes (default 1 MiB).
    pub max_body_size_bytes: usize,

    /// @env LOG_SAMPLE_RATE
    /// @default 1
    /// @description Fraction of requests to emit trace-level logs for (1 = all, 10 = every 10th).
    pub log_sample_rate: u32,

    /// @env WEBHOOK_URL
    /// @default (none)
    /// @description Target URL for webhook event delivery. When unset, webhook notifications are disabled.
    pub webhook_url: Option<String>,

    /// @env WEBHOOK_SECRET
    /// @default (none)
    /// @description HMAC-SHA256 secret used to sign webhook payloads (X-Webhook-Signature header).
    pub webhook_secret: Option<String>,

    /// @env WEBHOOK_CONTRACT_FILTER
    /// @default (none)
    /// @description Comma-separated list of contract IDs to deliver via webhook. Empty = deliver all.
    pub webhook_contract_filter: Vec<String>,

    /// @env WEBHOOK_RATE_LIMIT_PER_MINUTE
    /// @default (none)
    /// @description Maximum webhook notifications per minute. 0 or unset disables the limit.
    pub webhook_rate_limit_per_minute: Option<u32>,

    /// @env WEBHOOK_RATE_LIMIT_PER_HOUR
    /// @default (none)
    /// @description Maximum webhook notifications per hour. 0 or unset disables the limit.
    pub webhook_rate_limit_per_hour: Option<u32>,

    /// @env WEBHOOK_REQUIRE_HTTPS
    /// @default false (true in production)
    /// @description When true, reject webhook URLs that do not use HTTPS. Automatically true in production-like environments.
    pub webhook_require_https: bool,

    /// @env INDEXER_EVENT_TYPES
    /// @default (all types)
    /// @description Comma-separated list of event types to index: contract, diagnostic, system. Empty = index all.
    pub indexer_event_types: Vec<String>,

    /// @env EVENT_DATA_ENCRYPTION_KEY
    /// @default (none)
    /// @description 64-hex-character AES-GCM key (32 bytes) for encrypting event_data at rest. Requires the encryption feature flag.
    pub event_data_encryption_key: Option<[u8; 32]>,

    /// @env EVENT_DATA_ENCRYPTION_KEY_OLD
    /// @default (none)
    /// @description Previous encryption key for zero-downtime key rotation. Requires the encryption feature flag.
    pub event_data_encryption_key_old: Option<[u8; 32]>,

    /// @env INDEX_CHECK_INTERVAL_HOURS
    /// @default 24
    /// @description How often (in hours) the index usage monitor runs to report on PostgreSQL index utilisation.
    /// AES-GCM key for integration credentials (Issue #1162), separate from
    /// the event_data key (32 bytes, hex-encoded)
    pub integration_encryption_key: Option<[u8; 32]>,
    /// Previous integration credential key for rotation
    pub integration_encryption_key_old: Option<[u8; 32]>,
    /// How often the index usage monitor runs (hours)
    pub index_check_interval_hours: u64,

    /// @env HEALTH_CHECK_TIMEOUT_MS
    /// @default 2000
    /// @description Timeout in milliseconds for the database ping performed by the health check endpoint.
    pub health_check_timeout_ms: u64,

    /// @env TLS_CERT_FILE
    /// @default (none)
    /// @description Path to the PEM-encoded TLS certificate file for HTTPS termination.
    pub tls_cert_file: Option<String>,

    /// @env TLS_KEY_FILE
    /// @default (none)
    /// @description Path to the PEM-encoded TLS private key file for HTTPS termination.
    pub tls_key_file: Option<String>,

    /// @env BLOOM_FILTER_FP_RATE
    /// @default 0.001
    /// @description Target false-positive rate for the bloom filter used in contract existence checks.
    pub bloom_filter_fp_rate: f64,

    /// @env BLOOM_FILTER_CAPACITY
    /// @default 1000000
    /// @description Expected maximum number of items in the bloom filter before the FP rate degrades.
    pub bloom_filter_capacity: usize,

    /// @env BLOOM_EXPECTED_ITEMS
    /// @default 0
    /// @description Auto-sizing parameter: expected number of items per day. Set to >0 to enable auto-sizing (overrides BLOOM_FILTER_CAPACITY).
    pub bloom_expected_items: usize,

    /// @env BLOOM_FALSE_POSITIVE_RATE
    /// @default 0.001
    /// @description Auto-sizing parameter: target false-positive rate for auto-sizing. Used when BLOOM_EXPECTED_ITEMS > 0.
    pub bloom_false_positive_rate: f64,

    /// @env ENABLE_CONTENT_DEDUP
    /// @default false
    /// @description When true, a SHA-256 fingerprint of each event is checked before insert to prevent content-identical duplicates.
    pub enable_content_dedup: bool,

    /// @env DEDUP_WINDOW_SECS
    /// @default 3600
    /// @description Lookback window in seconds for content-fingerprint deduplication checks.
    pub dedup_window_secs: u64,

    /// @env KINESIS_STREAM_NAME
    /// @default (none)
    /// @description AWS Kinesis stream name for event delivery. Requires the kinesis feature flag.
    pub kinesis_stream_name: Option<String>,

    /// @env AWS_REGION
    /// @default (none)
    /// @description AWS region for Kinesis/SQS streaming integrations.
    pub aws_region: Option<String>,

    /// @env SQS_QUEUE_URL
    /// @default (none)
    /// @description AWS SQS queue URL for event delivery. Requires the sqs feature flag.
    pub sqs_queue_url: Option<String>,

    /// @env SQS_DLQ_URL
    /// @default (none)
    /// @description AWS SQS dead-letter queue URL for failed event deliveries.
    pub sqs_dlq_url: Option<String>,

    /// @env SQS_BATCH_SIZE
    /// @default 10
    /// @description Number of events per SQS SendMessageBatch call (max 10).
    pub sqs_batch_size: usize,

    /// @env EVENT_HUBS_CONNECTION_STRING
    /// @default (none)
    /// @description Azure Event Hubs connection string for event streaming.
    pub event_hubs_connection_string: Option<String>,

    /// @env EVENT_HUB_NAME
    /// @default (none)
    /// @description Azure Event Hub name within the namespace.
    pub event_hub_name: Option<String>,

    /// @env EVENT_HUBS_PARTITION_STRATEGY
    /// @default contract_id
    /// @description Partition key strategy for Azure Event Hubs: contract_id or round_robin.
    pub event_hubs_partition_strategy: String,

    /// @env PUBSUB_PROJECT_ID
    /// @default (none)
    /// @description GCP project ID for Pub/Sub event delivery. Requires the pubsub feature flag.
    pub pubsub_project_id: Option<String>,

    /// @env PUBSUB_TOPIC_ID
    /// @default (none)
    /// @description GCP Pub/Sub topic ID for event delivery.
    pub pubsub_topic_id: Option<String>,

    /// @env PUBSUB_ENABLE_MESSAGE_ORDERING
    /// @default true
    /// @description When true, Pub/Sub messages are published with an ordering key so subscribers receive them in order.
    pub pubsub_enable_message_ordering: bool,

    /// @env MAX_EVENT_DATA_BYTES
    /// @default 65536
    /// @description Maximum allowed size of serialised event_data in bytes (default 64 KiB).
    pub max_event_data_bytes: usize,

    /// @env EXPORT_MAX_ROWS
    /// @default 10000
    /// @description Maximum number of rows returned by the CSV/JSON export endpoints.
    pub export_max_rows: u64,

    /// @env CONTRACT_COUNT_CACHE_SIZE
    /// @default 1000
    /// @description Maximum number of entries in the per-contract event count cache.
    pub contract_count_cache_size: u64,

    /// @env CONTRACT_COUNT_CACHE_TTL_SECS
    /// @default 30
    /// @description TTL in seconds for entries in the per-contract event count cache.
    pub contract_count_cache_ttl_secs: u64,

    /// @env INDEXER_LOCK_RETRY_SECS
    /// @default 30
    /// @description How often standby replicas retry acquiring the advisory lock to become the active indexer.
    pub indexer_lock_retry_secs: u64,

    /// @env MULTI_TENANT
    /// @default false
    /// @description When true, every API key is mapped to a tenant_id and all queries are scoped to that tenant.
    pub multi_tenant: bool,

    /// @env TENANT_RATE_LIMIT_PER_MINUTE
    /// @default (none)
    /// @description Per-tenant request rate limit (requests per minute). Only effective when MULTI_TENANT=true.
    pub tenant_rate_limit_per_minute: Option<u32>,

    /// @env TENANT_CONTRACT_FILTER
    /// @default (none)
    /// @description Per-tenant contract allow-list. Format: "tenant1:C1,C2;tenant2:C3". Only effective when MULTI_TENANT=true.
    pub tenant_contract_filter: std::collections::HashMap<String, Vec<String>>,

    /// @env INDEXER_TENANT_ID
    /// @default (none)
    /// @description The tenant_id stamped on every event this indexer instance inserts. Only meaningful when MULTI_TENANT=true.
    pub indexer_tenant_id: Option<String>,

    /// @env EMAIL_SMTP_HOST
    /// @default (none)
    /// @description SMTP hostname for outbound email notifications. When unset, email notifications are disabled.
    pub email_smtp_host: Option<String>,

    /// @env EMAIL_SMTP_PORT
    /// @default 587
    /// @description SMTP port for outbound email (typically 587 for STARTTLS or 465 for implicit TLS).
    pub email_smtp_port: u16,

    /// @env EMAIL_SMTP_USER
    /// @default (none)
    /// @description SMTP authentication username.
    pub email_smtp_user: Option<String>,

    /// @env EMAIL_SMTP_PASSWORD
    /// @default (none)
    /// @description SMTP authentication password.
    pub email_smtp_password: Option<SecretString>,

    /// @env EMAIL_FROM
    /// @default (none)
    /// @description Sender address used in the From header of notification emails.
    pub email_from: Option<String>,

    /// @env EMAIL_TO
    /// @default (none)
    /// @description Comma-separated list of recipient addresses for email notifications.
    pub email_to: Vec<String>,

    /// @env EMAIL_CONTRACT_FILTER
    /// @default (none)
    /// @description Comma-separated contract IDs to send email notifications for. Empty = notify for all contracts.
    pub email_contract_filter: Vec<String>,

    /// @env EMAIL_SCHEDULE
    /// @default immediate
    /// @description Notification scheduling mode: "immediate", "daily" or "cron" (Issue #479).
    pub email_schedule: String,

    /// @env EMAIL_DAILY_DIGEST_HOUR
    /// @default 9
    /// @description Hour of day (0-23, UTC) at which the daily digest is sent.
    pub email_daily_digest_hour: u32,

    /// @env EMAIL_CRON
    /// @default (none)
    /// @description Cron expression used when EMAIL_SCHEDULE is "cron".
    pub email_cron: Option<String>,

    /// @env EMAIL_QUIET_HOURS_START
    /// @default (none)
    /// @description Start of the quiet-hours window ("HH:MM", UTC).
    pub email_quiet_hours_start: Option<String>,

    /// @env EMAIL_QUIET_HOURS_END
    /// @default (none)
    /// @description End of the quiet-hours window ("HH:MM", UTC).
    pub email_quiet_hours_end: Option<String>,

    /// @env EMAIL_PUBLIC_BASE_URL
    /// @default http://localhost:<PORT>
    /// @description Public base URL used to build unsubscribe links in notification emails (e.g. https://pulse.example.com).
    pub email_public_base_url: Option<String>,

    /// @env TWILIO_ACCOUNT_SID
    /// @default (none)
    /// @description Twilio Account SID for SMS notifications. When unset, SMS notifications are disabled.
    pub twilio_account_sid: Option<String>,

    /// @env TWILIO_AUTH_TOKEN
    /// @default (none)
    /// @description Twilio Auth Token for SMS notifications.
    pub twilio_auth_token: Option<SecretString>,

    /// @env TWILIO_FROM_NUMBER
    /// @default (none)
    /// @description Twilio sender phone number in E.164 format (e.g. +15551234567).
    pub twilio_from_number: Option<String>,

    /// @env SMS_TO_NUMBERS
    /// @default (none)
    /// @description Comma-separated list of E.164 phone numbers to receive SMS notifications.
    pub sms_to_numbers: Vec<String>,

    /// @env SMS_CONTRACT_FILTER
    /// @default (none)
    /// @description Comma-separated contract IDs to send SMS notifications for. Empty = notify for all contracts.
    pub sms_contract_filter: Vec<String>,

    // Notification retry policies (Issue #474)
    pub webhook_retry_policy: crate::retry_policy::RetryPolicy,
    pub email_retry_policy: crate::retry_policy::RetryPolicy,
    pub sms_retry_policy: crate::retry_policy::RetryPolicy,

    /// @env REDIS_URL
    /// @default (none)
    /// @description Redis connection URL for event streaming (e.g. redis://localhost:6379). When unset, Redis streaming is disabled.
    pub redis_url: Option<String>,

    /// @env REDIS_STREAM_KEY
    /// @default (none)
    /// @description Redis stream key name for event publishing (e.g. soroban:events).
    pub redis_stream_key: Option<String>,

    /// @env REDIS_BUFFER_MAX_SIZE
    /// @default 10000
    /// @description Maximum number of events buffered in memory during a Redis outage before dropping.
    pub redis_buffer_max_size: usize,

    /// @env STATS_REFRESH_INTERVAL_SECS
    /// @default 3600
    /// @description How often (in seconds) the background statistics refresh job runs.
    pub stats_refresh_interval_secs: u64,

    /// @env RETENTION_DAYS
    /// @default 90
    /// @description Number of days to retain indexed events before they are pruned.
    pub retention_days: u64,

    /// @env PRUNING_INTERVAL_HOURS
    /// @default 24
    /// @description How often (in hours) the event pruning job runs to enforce RETENTION_DAYS.
    pub pruning_interval_hours: u64,

    /// @env SSE_REPLAY_MAX_EVENTS
    /// @default 1000
    /// @description Maximum number of past events replayed to a reconnecting SSE client via Last-Event-ID.
    pub sse_replay_limit: u64,

    /// @env SSE_MAX_CONNECTIONS_PER_IP
    /// @default 10
    /// @description Maximum concurrent SSE connections allowed per client IP address.
    // Issue #454: SSE_REPLAY_MAX_EVENTS (alias for sse_replay_limit, read from SSE_REPLAY_MAX_EVENTS env var)
    // Issue #453 / #1268: Per-IP SSE connection limit
    pub sse_max_connections_per_ip: usize,

    /// @env SSE_MAX_LAG_BEFORE_DISCONNECT
    /// @default 0
    /// @description Number of events a slow SSE consumer may fall behind before being forcefully disconnected. 0 = disabled.
    pub sse_max_lag_before_disconnect: u64,

    /// @env INDEXER_IGNORE_CHECKPOINT
    /// @default false
    /// @description When true, the indexer ignores any persisted ledger checkpoint and always starts from START_LEDGER.
    pub indexer_ignore_checkpoint: bool,

    /// @env RPC_MAX_EVENTS_PER_PAGE
    /// @default 200
    /// @description Maximum number of events to request per RPC getEvents page.
    pub rpc_max_events_per_page: usize,

    /// @env MAX_LEDGER_RANGE
    /// @default 100000
    /// @description Maximum ledger range allowed for diff/range query endpoints.
    pub max_ledger_range: u64,

    /// @env SLOW_QUERY_THRESHOLD_MS
    /// @default 1000
    /// @description Queries exceeding this duration (ms) are logged at WARN level and counted in metrics.
    pub slow_query_threshold_ms: u64,

    /// @env EVENT_TRANSFORM_SCRIPT
    /// @default (none)
    /// @description Path to a Lua script for transforming events before storage. Requires the lua feature flag.
    pub event_transform_script: Option<String>,

    /// @env EVENT_TRANSFORM_TIMEOUT_MS
    /// @default 100
    /// @description Maximum execution time in milliseconds for the Lua event transformation script.
    pub event_transform_timeout_ms: u64,

    /// @env WEBHOOK_NOTIFICATION_FORMAT
    /// @default raw
    /// @description Webhook payload format: raw (full event JSON) or template (rendered via WEBHOOK_MESSAGE_TEMPLATE).
    pub webhook_notification_format: String,

    /// @env WEBHOOK_MESSAGE_TEMPLATE
    /// @default (none)
    /// @description Handlebars template string for webhook payload rendering when WEBHOOK_NOTIFICATION_FORMAT=template.
    pub webhook_message_template: Option<String>,

    /// @env PAGERDUTY_ROUTING_KEY
    /// @default (none)
    /// @description PagerDuty Events API v2 routing key (integration key). When unset, PagerDuty notifications are disabled.
    pub pagerduty_routing_key: Option<String>,

    /// @env PAGERDUTY_SERVICE_NAME
    /// @default Soroban Pulse
    /// @description Service name displayed in PagerDuty incidents.
    pub pagerduty_service_name: String,

    /// @env PAGERDUTY_CONTRACT_FILTER
    /// @default (none)
    /// @description Comma-separated contract IDs to trigger PagerDuty alerts for. Empty = alert on all contracts.
    pub pagerduty_contract_filter: Vec<String>,

    /// @env PAGERDUTY_EVENT_TYPE_FILTER
    /// @default (none)
    /// @description Comma-separated event types to trigger PagerDuty alerts for. Empty = alert on all event types.
    pub pagerduty_event_type_filter: Vec<String>,

    /// @env PAGERDUTY_SEVERITY_MAPPING
    /// @default {"contract":"error","diagnostic":"warning","system":"info"}
    /// @description JSON object mapping event types to PagerDuty severity levels (critical, error, warning, info).
    pub pagerduty_severity_mapping: std::collections::HashMap<String, String>,

    /// @env PAGERDUTY_AUTO_RESOLVE
    /// @default true
    /// @description When true, PagerDuty incidents are automatically resolved after PAGERDUTY_AUTO_RESOLVE_THRESHOLD_MINUTES.
    pub pagerduty_auto_resolve: bool,

    /// @env PAGERDUTY_AUTO_RESOLVE_THRESHOLD_MINUTES
    /// @default 30
    /// @description Minutes of inactivity after which a PagerDuty incident is automatically resolved.
    pub pagerduty_auto_resolve_threshold_minutes: i64,

    /// @env STATS_CACHE_TTL_SECS
    /// @default 30
    /// @description TTL in seconds for cached statistics responses.
    pub stats_cache_ttl_secs: u64,

    /// @env SSE_SHUTDOWN_GRACE_PERIOD_MS
    /// @default 200
    /// @description Milliseconds between broadcasting the SSE close event and dropping the broadcast sender on shutdown.
    pub sse_shutdown_grace_period_ms: u64,

    /// @env EMAIL_MAX_CONTRACTS_IN_DIGEST
    /// @default 20
    /// @description Maximum number of distinct contracts grouped in a single email digest.
    pub email_max_contracts_in_digest: usize,

    /// @env NOTIFICATION_DEFAULT_PRIORITY
    /// @default medium
    /// @description Default notification priority level: low, medium, high, or critical.
    pub notification_default_priority: String,

    /// @env NOTIFICATION_PRIORITY_RULE_PATH
    /// @default (none)
    /// @description JSONPath expression used to extract a value for priority rule evaluation.
    pub notification_priority_rule_path: Option<String>,

    /// @env NOTIFICATION_PRIORITY_RULE_VALUE
    /// @default (none)
    /// @description Value to match against the extracted JSONPath result for priority escalation.
    pub notification_priority_rule_value: Option<String>,

    /// @env NOTIFICATION_PRIORITY_RULE_PRIORITY
    /// @default (none)
    /// @description Priority level assigned when the rule matches: low, medium, high, or critical.
    pub notification_priority_rule_priority: Option<String>,

    /// @env ESCALATION_PRIMARY_CHANNEL
    /// @default (none)
    /// @description Notification channel (webhook, email, sms, pagerduty) to try first before escalating.
    pub escalation_primary_channel: Option<String>,

    /// @env ESCALATION_CHANNEL
    /// @default (none)
    /// @description Fallback notification channel to use after ESCALATION_DELAY_MINUTES if the primary channel fails.
    pub escalation_channel: Option<String>,

    /// @env ESCALATION_DELAY_MINUTES
    /// @default 30
    /// @description Minutes to wait before escalating a failed notification to the ESCALATION_CHANNEL.
    pub escalation_delay_minutes: u64,

    /// @env ONCALL_PROVIDER
    /// @default (none)
    /// @description On-call rotation provider: pagerduty. When unset, on-call routing is disabled.
    pub oncall_provider: Option<String>,

    /// @env ONCALL_PAGERDUTY_API_KEY
    /// @default (none)
    /// @description PagerDuty REST API key used to query on-call schedules.
    pub oncall_pagerduty_api_key: Option<SecretString>,

    /// @env ONCALL_SCHEDULE_ID
    /// @default (none)
    /// @description PagerDuty schedule ID to query for the current on-call responder.
    pub oncall_schedule_id: Option<String>,

    /// @env ONCALL_SCHEDULE_CACHE_TTL_SECS
    /// @default 300
    /// @description TTL in seconds for the cached on-call schedule lookup result.
    pub oncall_schedule_cache_ttl_secs: u64,

    /// @env EVENT_COMPRESSION_ENABLED
    /// @default false
    /// @description When true, event_data is gzip-compressed before writing to the event_data_compressed column.
    pub event_compression_enabled: bool,

    /// @env EVENT_COMPRESSION_MIGRATION_BATCH_SIZE
    /// @default 500
    /// @description Number of events processed per batch when migrating existing events to compressed format.
    pub event_compression_migration_batch_size: i64,

    /// @env CHAIN_ID
    /// @default mainnet
    /// @description Chain identifier stamped on every indexed event (e.g. mainnet, testnet, futurenet).
    pub chain_id: String,

    /// @env ADDITIONAL_CHAIN_IDS
    /// @default (none)
    /// @description Comma-separated list of additional network chain_ids to spin up parallel indexer workers for.
    pub additional_chain_ids: Vec<String>,

    /// @env LEDGER_HASH_TRACKING_ENABLED
    /// @default false
    /// @description When true, ledger hashes are stored and chain continuity is verified on startup.
    pub ledger_hash_tracking_enabled: bool,

    /// @env LEDGER_HASH_VALIDATION_DEPTH
    /// @default 1000
    /// @description Number of ledgers to check during startup hash-chain validation.
    pub ledger_hash_validation_depth: u64,

    /// @env ABI_CACHE_TTL_SECS
    /// @default 86400
    /// @description TTL in seconds for in-process contract ABI cache entries (default 24 h).
    pub abi_cache_ttl_secs: u64,

    /// @env ABI_CACHE_MAX_ENTRIES
    /// @default 1000
    /// @description Maximum number of contract ABIs kept in the in-process cache.
    pub abi_cache_max_entries: u64,

    /// @env SSE_RING_BUFFER_CAPACITY
    /// @default 10000
    /// @description Maximum number of events stored in the in-memory SSE replay ring buffer. Oldest events are evicted when full.
    pub sse_ring_buffer_capacity: usize,

    /// @env QUERY_CACHE_TTL_SECS
    /// @default 300
    /// @description TTL in seconds for cached query results. Clamped to [300, 3600].
    pub query_cache_ttl_secs: u64,

    /// @env QUERY_CACHE_MAX_CAPACITY
    /// @default 1000
    /// @description Maximum number of entries in the query result cache.
    pub query_cache_max_capacity: u64,

    /// @env ENABLE_PUSH_PRELOAD
    /// @default false
    /// @description When true, responses include HTTP Link preload headers for contract schema/ABI and the /v1/push/* endpoints are enabled.
    pub enable_push_preload: bool,

    // Issue #1147: Helm pre-install/pre-upgrade migration Job
    /// When true (default), migrations run automatically on startup before
    /// the HTTP server and indexer start.  Set RUN_MIGRATIONS_ON_STARTUP=false
    /// when the Helm migration Job is responsible for running migrations so
    /// that application pods skip the migration step entirely.
    pub run_migrations_on_startup: bool,

    // Issue #1112: built-in web dashboard
    /// Serve the web dashboard (web/dist) at /ui. Requires the `dashboard`
    /// cargo feature. Set SERVE_DASHBOARD=true to opt in.
    pub serve_dashboard: bool,
    /// Directory containing the built dashboard (index.html + assets/).
    /// Defaults to `web/dist`. Set via DASHBOARD_DIR.
    pub dashboard_dir: String,
    /// Extra origins the dashboard may call, appended to the `connect-src`
    /// directive of its Content-Security-Policy (comma-separated
    /// DASHBOARD_CONNECT_SRC). Same-origin API calls are always allowed.
    pub dashboard_connect_src: Vec<String>,

    // Issue #705: Kafka event publishing
    /// Comma-separated list of Kafka broker addresses (e.g., "localhost:9092,localhost:9093").
    /// When set, events are published to Kafka topic specified by kafka_topic.
    pub kafka_brokers: Option<String>,
    /// Kafka topic for event publishing. Required when kafka_brokers is set.
    pub kafka_topic: Option<String>,
    /// Kafka producer batch size (default: 16384 bytes).
    pub kafka_batch_size: usize,
    /// Kafka producer linger time in milliseconds (default: 100ms).
    pub kafka_linger_ms: u64,

    // Issue #841: SaaS Platform & Multi-Tenant Hosting
    /// Enable SaaS platform features including tenant management and billing integration
    pub saas_enabled: bool,
    /// Billing system integration (stripe, paddle, etc.)
    pub billing_provider: Option<String>,
    /// Billing system API key
    pub billing_api_key: Option<String>,
    /// Default trial period in days for new tenants
    pub default_trial_days: i32,
    /// Public base URL for tenant onboarding (e.g., https://pulse.example.com)
    pub saas_public_base_url: Option<String>,

    // Issue #842: AI/ML Integration & Intelligence Features
    /// Enable ML-based anomaly detection
    pub ml_anomaly_detection_enabled: bool,
    /// Enable pattern recognition
    pub ml_pattern_recognition_enabled: bool,
    /// Enable intelligent event filtering
    pub ml_intelligent_filtering_enabled: bool,
    /// ML model training interval in hours
    pub ml_model_training_interval_hours: u64,
    /// Minimum samples required for ML model training
    pub ml_min_training_samples: i64,
    /// ML model confidence threshold (0.0 to 1.0)
    pub ml_confidence_threshold: f64,
    /// Enable automatic model retraining
    pub ml_auto_retrain: bool,

    /// Deployment role: controls which subsystems start on this instance.
    /// Set via `ROLE=all|api|indexer`. Default: `all` (backward-compatible).
    pub role: Role,

    // Issue #1262: derived metrics DSL
    /// Path to a YAML file containing user-defined derived metric definitions.
    /// Set via DERIVED_METRICS_CONFIG. When unset, no derived metrics run.
    pub derived_metrics_config_path: Option<String>,
    /// Inline derived metric definitions (YAML string), used when
    /// DERIVED_METRICS_CONFIG is not set. Set via DERIVED_METRICS_INLINE.
    pub derived_metrics_inline: Option<String>,
    /// How often the derived metrics background job evaluates metrics (seconds).
    /// Set via DERIVED_METRICS_INTERVAL_SECS. Default: 60.
    pub derived_metrics_interval_secs: u64,

    // Issue #1263: incident correlation engine
    /// Sliding window (seconds) within which correlated signals are grouped
    /// into a single incident. Set via CORRELATION_WINDOW_SECS.
    /// Default: 300, clamped to [60, 3600].
    pub correlation_window_secs: u64,
    /// How often the incident detector evaluates correlation rules (seconds).
    /// Set via INCIDENT_DETECTOR_INTERVAL_SECS. Default: 30, clamped to [10, 300].
    pub incident_detector_interval_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/soroban_pulse".to_string(),
            database_replica_url: None,
            stellar_rpc_url: "https://soroban-testnet.stellar.org".to_string(),
            stellar_rpc_fallback_urls: Vec::new(),
            rpc_headers: Vec::new(),
            start_ledger: 0,
            start_ledger_fallback: false,
            port: 3000,
            api_keys: Vec::new(),
            admin_api_keys: Vec::new(),
            db_max_connections: 10,
            db_min_connections: 2,
            db_idle_timeout_secs: 600,
            db_max_lifetime_secs: 1800,
            db_test_before_acquire: true,
            behind_proxy: false,
            rpc_connect_timeout_secs: 5,
            rpc_request_timeout_secs: 30,
            allowed_origins: vec!["*".to_string()],
            rate_limit_per_minute: 60,
            subscription_rate_limit_per_minute: 5,
            rate_limit_key_per_minute: None,
            rate_limit_key_per_hour: None,
            rate_limit_key_per_day: None,
            rate_limit_key_per_month: None,
            ip_denylist: Vec::new(),
            ip_allowlist: Vec::new(),
            indexer_lag_warn_threshold: 100,
            indexer_stall_timeout_secs: 60,
            db_statement_timeout_ms: 5000,
            indexer_poll_interval_ms: 5000,
            indexer_error_backoff_ms: 10000,
            sse_keepalive_interval_ms: 15000,
            sse_max_connections: 1000,
            sse_drain_timeout_secs: 5,
            sse_multi_max_contract_ids: 100,
            environment: Environment::Development,
            max_body_size_bytes: 1024 * 1024, // 1 MB default
            log_sample_rate: 1,
            webhook_url: None,
            webhook_secret: None,
            webhook_contract_filter: Vec::new(),
            webhook_rate_limit_per_minute: None,
            webhook_rate_limit_per_hour: None,
            indexer_event_types: Vec::new(),
            event_data_encryption_key: None,
            event_data_encryption_key_old: None,
            integration_encryption_key: None,
            integration_encryption_key_old: None,
            index_check_interval_hours: 24,
            health_check_timeout_ms: 2000,
            tls_cert_file: None,
            tls_key_file: None,
            bloom_filter_fp_rate: 0.001,
            bloom_filter_capacity: 1_000_000,
            bloom_expected_items: 0,
            bloom_false_positive_rate: 0.001,
            enable_content_dedup: false,
            dedup_window_secs: 3600,
            kinesis_stream_name: None,
            aws_region: None,
            sqs_queue_url: None,
            sqs_dlq_url: None,
            sqs_batch_size: 10,
            event_hubs_connection_string: None,
            event_hub_name: None,
            event_hubs_partition_strategy: "contract_id".to_string(),
            pubsub_project_id: None,
            pubsub_topic_id: None,
            pubsub_enable_message_ordering: true,
            max_event_data_bytes: 65536,
            export_max_rows: 10_000,
            contract_count_cache_size: 1000,
            contract_count_cache_ttl_secs: 30,
            indexer_lock_retry_secs: 30,
            multi_tenant: false,
            tenant_rate_limit_per_minute: None,
            tenant_contract_filter: std::collections::HashMap::new(),
            indexer_tenant_id: None,
            email_smtp_host: None,
            email_smtp_port: 587,
            email_smtp_user: None,
            email_smtp_password: None,
            email_from: None,
            email_to: Vec::new(),
            email_contract_filter: Vec::new(),
            email_schedule: "immediate".to_string(),
            email_daily_digest_hour: 9,
            email_cron: None,
            email_quiet_hours_start: None,
            email_quiet_hours_end: None,
            email_public_base_url: None,
            redis_url: None,
            redis_stream_key: None,
            redis_buffer_max_size: 10_000,
            stats_refresh_interval_secs: 3600,
            retention_days: 90,
            pruning_interval_hours: 24,
            sse_replay_limit: 1000,
            sse_max_connections_per_ip: 5,
            sse_max_lag_before_disconnect: 0, // 0 = disabled
            indexer_ignore_checkpoint: false,
            webhook_require_https: false,
            rpc_max_events_per_page: 200,
            max_ledger_range: 100_000,
            slow_query_threshold_ms: 1000,
            event_transform_script: None,
            event_transform_timeout_ms: 100,
            webhook_notification_format: "raw".to_string(),
            webhook_message_template: None,
            pagerduty_routing_key: None,
            pagerduty_service_name: "Soroban Pulse".to_string(),
            pagerduty_contract_filter: Vec::new(),
            pagerduty_event_type_filter: Vec::new(),
            pagerduty_severity_mapping: {
                let mut map = std::collections::HashMap::new();
                map.insert("contract".to_string(), "error".to_string());
                map.insert("diagnostic".to_string(), "warning".to_string());
                map.insert("system".to_string(), "info".to_string());
                map
            },
            pagerduty_auto_resolve: true,
            pagerduty_auto_resolve_threshold_minutes: 30,
            stats_cache_ttl_secs: 30,
            twilio_account_sid: None,
            twilio_auth_token: None,
            twilio_from_number: None,
            sms_to_numbers: Vec::new(),
            sms_contract_filter: Vec::new(),
            webhook_retry_policy: crate::retry_policy::RetryPolicy::webhook_default(),
            email_retry_policy: crate::retry_policy::RetryPolicy::email_default(),
            sms_retry_policy: crate::retry_policy::RetryPolicy::sms_default(),
            sse_shutdown_grace_period_ms: 200,
            email_max_contracts_in_digest: 20,
            notification_default_priority: "medium".to_string(),
            notification_priority_rule_path: None,
            notification_priority_rule_value: None,
            notification_priority_rule_priority: None,
            escalation_primary_channel: None,
            escalation_channel: None,
            escalation_delay_minutes: 30,
            oncall_provider: None,
            oncall_pagerduty_api_key: None,
            oncall_schedule_id: None,
            oncall_schedule_cache_ttl_secs: 300,
            event_compression_enabled: false,
            event_compression_migration_batch_size: 500,
            chain_id: "mainnet".to_string(),
            additional_chain_ids: Vec::new(),
            ledger_hash_tracking_enabled: false,
            ledger_hash_validation_depth: 1_000,
            abi_cache_ttl_secs: crate::abi::DEFAULT_ABI_CACHE_TTL_SECS,
            abi_cache_max_entries: crate::abi::ABI_CACHE_MAX_ENTRIES,
            sse_ring_buffer_capacity: crate::sse_ring_buffer::DEFAULT_RING_BUFFER_CAPACITY,
            query_cache_ttl_secs: crate::query_cache::DEFAULT_TTL_SECS,
            query_cache_max_capacity: crate::query_cache::DEFAULT_MAX_CAPACITY,
            enable_push_preload: false,
            run_migrations_on_startup: true,
            serve_dashboard: false,
            dashboard_dir: "web/dist".to_string(),
            dashboard_connect_src: Vec::new(),
            kafka_brokers: None,
            kafka_topic: None,
            kafka_batch_size: 16384,
            kafka_linger_ms: 100,
            saas_enabled: false,
            billing_provider: None,
            billing_api_key: None,
            default_trial_days: 14,
            saas_public_base_url: None,
            ml_anomaly_detection_enabled: false,
            ml_pattern_recognition_enabled: false,
            ml_intelligent_filtering_enabled: false,
            ml_model_training_interval_hours: 24,
            ml_min_training_samples: 100,
            ml_confidence_threshold: 0.8,
            ml_auto_retrain: true,
            role: Role::All,
            derived_metrics_config_path: None,
            derived_metrics_inline: None,
            derived_metrics_interval_secs: 60,
            correlation_window_secs: 300,
            incident_detector_interval_secs: 30,
        }
    }
}

pub(crate) fn validate_rpc_url_checked(raw: &str, errors: &mut Vec<String>) -> Option<String> {
    validate_rpc_url_named("STELLAR_RPC_URL", raw, errors)
}

/// Validate a single RPC URL, attributing any errors to `var` (e.g.
/// "STELLAR_RPC_URL" or "STELLAR_RPC_FALLBACK_URLS"). On success returns the
/// URL with any embedded credentials stripped.
pub(crate) fn validate_rpc_url_named(var: &str, raw: &str, errors: &mut Vec<String>) -> Option<String> {
    let url = match Url::parse(raw) {
        Ok(u) => u,
        Err(e) => {
            errors.push(format!(
                "  {var}={raw:?} is not a valid URL: {e}. \
                 Expected a valid HTTPS URL (e.g., https://soroban-testnet.stellar.org)."
            ));
            return None;
        }
    };

    let allow_insecure = env::var("ALLOW_INSECURE_RPC")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
        .unwrap_or(false);

    match url.scheme() {
        "https" => {}
        "http" if allow_insecure => {}
        "http" => {
            errors.push(format!(
                "  {var}={raw:?} uses http. \
                 Set ALLOW_INSECURE_RPC=true to permit insecure connections, \
                 or use an https URL (e.g., https://soroban-testnet.stellar.org)."
            ));
            return None;
        }
        scheme => {
            errors.push(format!(
                "  {var}={raw:?} has disallowed scheme '{scheme}'. \
                 Only https is permitted (e.g., https://soroban-testnet.stellar.org)."
            ));
            return None;
        }
    }

    let host = url.host_str().unwrap_or("");
    if host.is_empty() {
        errors.push(format!(
            "  {var}={raw:?} has an empty host. \
             Expected a valid HTTPS URL (e.g., https://soroban-testnet.stellar.org)."
        ));
        return None;
    }

    if !allow_insecure {
        let is_loopback =
            host == "localhost" || host == "127.0.0.1" || host == "::1" || host.ends_with(".local");
        let is_private = host.starts_with("10.")
            || host.starts_with("192.168.")
            || host.starts_with("169.254.")
            || (host.starts_with("172.") && {
                host.split('.')
                    .nth(1)
                    .and_then(|o| o.parse::<u8>().ok())
                    .map(|o| (16..=31).contains(&o))
                    .unwrap_or(false)
            });
        if is_loopback || is_private {
            errors.push(format!(
                "  {var}={raw:?} points to a non-routable host '{host}'. \
                 Set ALLOW_INSECURE_RPC=true to allow this in development."
            ));
            return None;
        }
    }

    let mut safe = url.clone();
    let _ = safe.set_username("");
    let _ = safe.set_password(None);
    Some(safe.to_string())
}

/// Parse and validate STELLAR_RPC_FALLBACK_URLS: a comma-separated list of
/// fallback RPC endpoints used when the primary URL fails. Each entry is
/// validated with the same rules as STELLAR_RPC_URL.
#[cfg(test)]
fn parse_rpc_fallback_urls_checked(errors: &mut Vec<String>) -> Vec<String> {
    validate_rpc_fallback_urls(&read_rpc_fallback_urls(), errors)
}

/// Split STELLAR_RPC_FALLBACK_URLS into its (unvalidated) entries.
fn read_rpc_fallback_urls() -> Vec<String> {
    env::var("STELLAR_RPC_FALLBACK_URLS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Validate each fallback URL, dropping (and reporting) invalid entries.
fn validate_rpc_fallback_urls(urls: &[String], errors: &mut Vec<String>) -> Vec<String> {
    urls.iter()
        .filter_map(|u| validate_rpc_url_named("STELLAR_RPC_FALLBACK_URLS", u, errors))
        .collect()
}

/// Resolve the RPC endpoint list (issue #1256) without validating it.
///
/// `STELLAR_RPC_URLS` (comma-separated) takes precedence over
/// `STELLAR_RPC_URL`; its first entry becomes the primary and the rest are
/// failover candidates. `STELLAR_RPC_FALLBACK_URLS` entries are appended in
/// either case. Returns `(primary, others)` with duplicates removed.
/// Validation happens in [`ConfigBuilder::build`].
fn resolve_rpc_urls(
    urls_raw: Option<&str>,
    single_raw: &str,
    fallbacks: Vec<String>,
) -> (String, Vec<String>) {
    let mut candidates = urls_raw
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let primary = candidates.next().unwrap_or_else(|| single_raw.to_string());
    let mut others: Vec<String> = Vec::new();
    for url in candidates.chain(fallbacks) {
        if url != primary && !others.contains(&url) {
            others.push(url);
        }
    }
    (primary, others)
}

/// Resolve the RPC endpoint list (issue #1256).
///
/// `STELLAR_RPC_URLS` (comma-separated) takes precedence over
/// `STELLAR_RPC_URL`; its first entry becomes the primary and the rest are
/// failover candidates. `STELLAR_RPC_FALLBACK_URLS` entries are appended in
/// either case. Returns `(primary, others)` with duplicates removed.
#[cfg(test)]
fn resolve_rpc_urls_checked(
    urls_raw: Option<&str>,
    single_raw: &str,
    fallbacks: Vec<String>,
    errors: &mut Vec<String>,
) -> (String, Vec<String>) {
    let listed: Vec<String> = urls_raw
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|u| validate_rpc_url_named("STELLAR_RPC_URLS", u, errors))
        .collect();

    let mut candidates = listed.into_iter();
    let primary = candidates.next().unwrap_or_else(|| {
        validate_rpc_url_checked(single_raw, errors).unwrap_or_else(|| single_raw.to_string())
    });
    let mut others: Vec<String> = Vec::new();
    for url in candidates.chain(fallbacks) {
        if url != primary && !others.contains(&url) {
            others.push(url);
        }
    }
    (primary, others)
}

/// Read DATABASE_URL from DATABASE_URL_FILE if set, otherwise fall back to DATABASE_URL.
fn resolve_database_url_checked(errors: &mut Vec<String>) -> String {
    if let Ok(file_path) = env::var("DATABASE_URL_FILE") {
        match std::fs::read_to_string(&file_path) {
            Ok(contents) => return contents.trim().to_string(),
            Err(e) => {
                errors.push(format!(
                    "  DATABASE_URL_FILE={file_path:?} could not be read: {e}. \
                     Ensure the file exists and is readable."
                ));
                return String::new();
            }
        }
    }
    // A missing DATABASE_URL is reported by `ConfigBuilder::build` (via
    // `config_validation::check_database`), not here.
    env::var("DATABASE_URL").unwrap_or_default()
}

/// Parse STELLAR_RPC_HEADERS: semicolon-separated "Name: Value" pairs.
fn parse_rpc_headers_checked(errors: &mut Vec<String>) -> Vec<(String, String)> {
    let raw = match env::var("STELLAR_RPC_HEADERS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Vec::new(),
    };
    let mut headers = Vec::new();
    for pair in raw.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        match pair.split_once(':') {
            Some((name, value)) => {
                let name = name.trim().to_string();
                let value = value.trim().to_string();
                if name.is_empty() {
                    errors.push(format!(
                        "  STELLAR_RPC_HEADERS: header name is empty in {pair:?}. \
                         Expected 'Name: Value' format (e.g., X-API-Key: mykey)."
                    ));
                } else {
                    headers.push((name, value));
                }
            }
            None => {
                errors.push(format!(
                    "  STELLAR_RPC_HEADERS: invalid entry {pair:?}. \
                     Expected 'Name: Value' format (e.g., X-API-Key: mykey)."
                ));
            }
        }
    }
    headers
}

/// Parse INDEXER_EVENT_TYPES: comma-separated list of event types.
fn parse_indexer_event_types_checked(errors: &mut Vec<String>) -> Vec<String> {
    let raw = match env::var("INDEXER_EVENT_TYPES") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Vec::new(),
    };
    let valid = ["contract", "diagnostic", "system"];
    let mut types = Vec::new();
    for t in raw
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
    {
        if valid.contains(&t.as_str()) {
            types.push(t);
        } else {
            errors.push(format!(
                "  INDEXER_EVENT_TYPES={t:?} is not a valid event type. \
                 Valid values are: contract, diagnostic, system."
            ));
        }
    }
    types
}

/// Parse a 64-hex-char string into a 32-byte key, collecting errors instead of panicking.
fn parse_hex_key_checked(var: &str, value: &str, errors: &mut Vec<String>) -> Option<[u8; 32]> {
    if value.len() != 64 {
        errors.push(format!(
            "  {var} must be exactly 64 hex characters (32 bytes), got {} chars.",
            value.len()
        ));
        return None;
    }
    let mut key = [0u8; 32];
    for (i, chunk) in value.as_bytes().chunks(2).enumerate() {
        let hex = match std::str::from_utf8(chunk) {
            Ok(s) => s,
            Err(_) => {
                errors.push(format!("  {var} contains invalid UTF-8 at byte {i}."));
                return None;
            }
        };
        match u8::from_str_radix(hex, 16) {
            Ok(b) => key[i] = b,
            Err(_) => {
                errors.push(format!(
                    "  {var} contains non-hex character at byte {i} ({hex:?}). \
                     Expected a 64-character lowercase hex string."
                ));
                return None;
            }
        }
    }
    Some(key)
}

/// Parse a required integer env var, pushing a descriptive error on failure.
fn parse_int<T>(var: &str, raw: &str, example: &str, errors: &mut Vec<String>) -> Option<T>
where
    T: std::str::FromStr,
{
    raw.parse::<T>().ok().or_else(|| {
        errors.push(format!(
            "  {var}={raw:?} is not a valid integer. Expected a positive integer (e.g., {example})."
        ));
        None
    })
}

/// Parse a required bool env var ("true"/"false"), pushing a descriptive error on failure.
fn parse_bool(var: &str, raw: &str, errors: &mut Vec<String>) -> Option<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Some(true),
        "false" | "0" | "no" | "n" => Some(false),
        _ => {
            errors.push(format!(
                "  {var}={raw:?} is not a valid boolean. Expected true or false (e.g., true)."
            ));
            None
        }
    }
}

/// Parse an integer that must fall within [min, max], pushing a descriptive error on failure.
fn parse_int_range<T>(
    var: &str,
    raw: &str,
    min: T,
    max: T,
    example: &str,
    errors: &mut Vec<String>,
) -> Option<T>
where
    T: std::str::FromStr + PartialOrd + std::fmt::Display,
{
    match raw.parse::<T>() {
        Ok(v) if v >= min && v <= max => Some(v),
        Ok(v) => {
            errors.push(format!(
                "  {var}={v} is out of range. Expected a value between {min} and {max} (e.g., {example})."
            ));
            None
        }
        Err(_) => {
            errors.push(format!(
                "  {var}={raw:?} is not a valid integer. Expected a value between {min} and {max} (e.g., {example})."
            ));
            None
        }
    }
}

/// Parse TENANT_CONTRACT_FILTER env var.
///
/// Format: `"tenant1:CABC...,CDEF...;tenant2:CXYZ..."`
/// Each semicolon-separated segment is `tenant_id:contract1,contract2,...`.
/// Returns an empty map when the variable is unset or empty.
fn parse_tenant_contract_filter() -> std::collections::HashMap<String, Vec<String>> {
    let raw = match std::env::var("TENANT_CONTRACT_FILTER") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return std::collections::HashMap::new(),
    };
    let mut map = std::collections::HashMap::new();
    for segment in raw.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if let Some((tenant, contracts_str)) = segment.split_once(':') {
            let tenant = tenant.trim().to_string();
            let contracts: Vec<String> = contracts_str
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
            if !tenant.is_empty() && !contracts.is_empty() {
                map.insert(tenant, contracts);
            }
        }
    }
    map
}

/// One network to run a concurrent indexer worker against (Issue #1063).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkTarget {
    pub chain_id: String,
    pub rpc_url: String,
}

impl Config {
    /// Resolve the set of networks this deployment should index concurrently
    /// (Issue #1063). The primary network is always `chain_id` /
    /// `stellar_rpc_url`. Each entry in `additional_chain_ids` becomes a
    /// second worker, with its RPC URL taken from the
    /// `STELLAR_RPC_URL_<CHAIN_ID>` environment variable (chain id
    /// upper-cased, non-alphanumeric characters replaced with `_`) — e.g.
    /// `ADDITIONAL_CHAIN_IDS=testnet` reads `STELLAR_RPC_URL_TESTNET`.
    /// An additional chain id with no matching RPC URL is skipped with a
    /// warning rather than aborting the whole deployment.
    pub fn network_targets(&self) -> Vec<NetworkTarget> {
        let mut targets = vec![NetworkTarget {
            chain_id: self.chain_id.clone(),
            rpc_url: self.stellar_rpc_url.clone(),
        }];
        for chain_id in &self.additional_chain_ids {
            if chain_id == &self.chain_id {
                continue; // already the primary network
            }
            let env_key = format!(
                "STELLAR_RPC_URL_{}",
                chain_id
                    .to_ascii_uppercase()
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect::<String>()
            );
            match std::env::var(&env_key) {
                Ok(rpc_url) if !rpc_url.trim().is_empty() => {
                    targets.push(NetworkTarget {
                        chain_id: chain_id.clone(),
                        rpc_url,
                    });
                }
                _ => {
                    tracing::warn!(
                        chain_id = %chain_id,
                        env_key = %env_key,
                        "additional chain_id has no RPC URL configured; skipping this network"
                    );
                }
            }
        }
        targets
    }

    /// Returns the DATABASE_URL with credentials stripped — safe to log.
    pub fn safe_db_url(&self) -> String {
        Url::parse(&self.database_url)
            .map(|mut u| {
                let _ = u.set_username("");
                let _ = u.set_password(None);
                u.to_string()
            })
            .unwrap_or_else(|_| "<unparseable>".to_string())
    }

    /// Returns only header names (no values) — safe to log.
    pub fn safe_rpc_headers(&self) -> Vec<&str> {
        self.rpc_headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Read every setting from the environment / `CONFIG_FILE`, collecting
    /// parse errors instead of aborting. Cross-field and semantic validation
    /// happens once, in [`ConfigBuilder::build`] (issue #1247).
    fn read_env() -> (Self, Vec<String>) {
        let file = load_config_file();
        let mut errors: Vec<String> = Vec::new();

        let environment =
            Environment::from_str(&env_or_file_or("ENVIRONMENT", &file, "development"));

        let behind_proxy = env_or_file("BEHIND_PROXY", &file)
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
            .unwrap_or(false);

        let start_ledger = parse_int::<u64>(
            "START_LEDGER",
            &env_or_file_or("START_LEDGER", &file, "0"),
            "0",
            &mut errors,
        )
        .unwrap_or(0);

        let start_ledger_fallback = env_or_file("START_LEDGER_FALLBACK", &file)
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
            .unwrap_or(false);

        let port = parse_int::<u16>(
            "PORT",
            &env_or_file_or("PORT", &file, "3000"),
            "3000",
            &mut errors,
        )
        .unwrap_or(3000);

        let allowed_origins: Vec<String> = env_or_file_or("ALLOWED_ORIGINS", &file, "*")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let database_url = resolve_database_url_checked(&mut errors);

        // Validated (and credentials stripped) in `ConfigBuilder::build`.
        let (stellar_rpc_url, stellar_rpc_fallback_urls) = resolve_rpc_urls(
            env_or_file("STELLAR_RPC_URLS", &file).as_deref(),
            &env_or_file_or(
                "STELLAR_RPC_URL",
                &file,
                "https://soroban-testnet.stellar.org",
            ),
            read_rpc_fallback_urls(),
        );

        let db_max_connections = parse_int::<u32>(
            "DB_MAX_CONNECTIONS",
            &env_or_file_or("DB_MAX_CONNECTIONS", &file, "10"),
            "10",
            &mut errors,
        )
        .unwrap_or(10);

        let db_min_connections = parse_int::<u32>(
            "DB_MIN_CONNECTIONS",
            &env_or_file_or("DB_MIN_CONNECTIONS", &file, "2"),
            "2",
            &mut errors,
        )
        .unwrap_or(2);

        let db_idle_timeout_secs = parse_int::<u64>(
            "DB_IDLE_TIMEOUT_SECS",
            &env_or_file_or("DB_IDLE_TIMEOUT_SECS", &file, "600"),
            "600",
            &mut errors,
        )
        .unwrap_or(600);

        let db_max_lifetime_secs = parse_int::<u64>(
            "DB_MAX_LIFETIME_SECS",
            &env_or_file_or("DB_MAX_LIFETIME_SECS", &file, "1800"),
            "1800",
            &mut errors,
        )
        .unwrap_or(1800);

        let db_test_before_acquire = parse_bool(
            "DB_TEST_BEFORE_ACQUIRE",
            &env_or_file_or("DB_TEST_BEFORE_ACQUIRE", &file, "true"),
            &mut errors,
        )
        .unwrap_or(true);

        let rpc_connect_timeout_secs = parse_int::<u64>(
            "RPC_CONNECT_TIMEOUT_SECS",
            &env_or_file_or("RPC_CONNECT_TIMEOUT_SECS", &file, "5"),
            "5",
            &mut errors,
        )
        .unwrap_or(5);

        let rpc_request_timeout_secs = parse_int::<u64>(
            "RPC_REQUEST_TIMEOUT_SECS",
            &env_or_file_or("RPC_REQUEST_TIMEOUT_SECS", &file, "30"),
            "30",
            &mut errors,
        )
        .unwrap_or(30);

        let rate_limit_per_minute = parse_int::<u32>(
            "RATE_LIMIT_PER_MINUTE",
            &env_or_file_or("RATE_LIMIT_PER_MINUTE", &file, "60"),
            "60",
            &mut errors,
        )
        .unwrap_or(60);

        // Issue #1268: stricter per-IP limit for POST /v1/subscriptions.
        let subscription_rate_limit_per_minute = parse_int_range::<u32>(
            "SUBSCRIPTION_RATE_LIMIT_PER_MINUTE",
            &env_or_file_or("SUBSCRIPTION_RATE_LIMIT_PER_MINUTE", &file, "5"),
            1,
            u32::MAX,
            "5",
            &mut errors,
        )
        .unwrap_or(5);

        // Issue #669 / #666: Per-API-key rate limits (optional, sliding window).
        let rate_limit_key_per_minute = env_or_file("RATE_LIMIT_KEY_PER_MINUTE", &file)
            .and_then(|v| parse_int::<u32>("RATE_LIMIT_KEY_PER_MINUTE", &v, "1000", &mut errors))
            .filter(|&n| n > 0);

        let rate_limit_key_per_hour = env_or_file("RATE_LIMIT_KEY_PER_HOUR", &file)
            .and_then(|v| parse_int::<u32>("RATE_LIMIT_KEY_PER_HOUR", &v, "10000", &mut errors))
            .filter(|&n| n > 0);

        let rate_limit_key_per_day = env_or_file("RATE_LIMIT_KEY_PER_DAY", &file)
            .and_then(|v| parse_int::<u32>("RATE_LIMIT_KEY_PER_DAY", &v, "100000", &mut errors))
            .filter(|&n| n > 0);

        // Issue #941: monthly quota pool, on top of the existing minute/hour/day tiers.
        let rate_limit_key_per_month = env_or_file("RATE_LIMIT_KEY_PER_MONTH", &file)
            .and_then(|v| parse_int::<u32>("RATE_LIMIT_KEY_PER_MONTH", &v, "1000000", &mut errors))
            .filter(|&n| n > 0);

        // Issue #942: IP allow/deny lists (comma-separated IPs and/or CIDR blocks).
        let ip_denylist: Vec<String> = env_or_file_or("IP_DENYLIST", &file, "")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let ip_allowlist: Vec<String> = env_or_file_or("IP_ALLOWLIST", &file, "")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let indexer_lag_warn_threshold = parse_int::<u64>(
            "INDEXER_LAG_WARN_THRESHOLD",
            &env_or_file_or("INDEXER_LAG_WARN_THRESHOLD", &file, "100"),
            "100",
            &mut errors,
        )
        .unwrap_or(100);

        let indexer_stall_timeout_secs = parse_int::<u64>(
            "INDEXER_STALL_TIMEOUT_SECS",
            &env_or_file_or("INDEXER_STALL_TIMEOUT_SECS", &file, "60"),
            "60",
            &mut errors,
        )
        .unwrap_or(60);

        let db_statement_timeout_ms = parse_int::<u64>(
            "DB_STATEMENT_TIMEOUT_MS",
            &env_or_file_or("DB_STATEMENT_TIMEOUT_MS", &file, "5000"),
            "5000",
            &mut errors,
        )
        .unwrap_or(5000);

        let indexer_poll_interval_ms = parse_int_range::<u64>(
            "INDEXER_POLL_INTERVAL_MS",
            &env_or_file_or("INDEXER_POLL_INTERVAL_MS", &file, "5000"),
            100,
            60000,
            "5000",
            &mut errors,
        )
        .unwrap_or(5000);

        let indexer_error_backoff_ms = parse_int_range::<u64>(
            "INDEXER_ERROR_BACKOFF_MS",
            &env_or_file_or("INDEXER_ERROR_BACKOFF_MS", &file, "10000"),
            100,
            60000,
            "10000",
            &mut errors,
        )
        .unwrap_or(10000);

        // SSE_KEEPALIVE_SECS (seconds) takes precedence over SSE_KEEPALIVE_INTERVAL_MS (ms).
        let sse_keepalive_interval_ms =
            if let Some(secs_str) = env_or_file("SSE_KEEPALIVE_SECS", &file) {
                parse_int_range::<u64>("SSE_KEEPALIVE_SECS", &secs_str, 1, 60, "15", &mut errors)
                    .map(|s| s * 1000)
                    .unwrap_or(15000)
            } else {
                parse_int_range::<u64>(
                    "SSE_KEEPALIVE_INTERVAL_MS",
                    &env_or_file_or("SSE_KEEPALIVE_INTERVAL_MS", &file, "15000"),
                    1000,
                    60000,
                    "15000",
                    &mut errors,
                )
                .unwrap_or(15000)
            };

        let sse_max_connections = parse_int_range::<usize>(
            "SSE_MAX_CONNECTIONS",
            &env_or_file_or("SSE_MAX_CONNECTIONS", &file, "1000"),
            1,
            usize::MAX,
            "1000",
            &mut errors,
        )
        .unwrap_or(1000);

        // Issue #1268: per-IP SSE connection limit.
        let sse_max_connections_per_ip = parse_int_range::<usize>(
            "SSE_MAX_CONNECTIONS_PER_IP",
            &env_or_file_or("SSE_MAX_CONNECTIONS_PER_IP", &file, "5"),
            1,
            usize::MAX,
            "5",
            &mut errors,
        )
        .unwrap_or(5);

        let sse_drain_timeout_secs = parse_int_range::<u64>(
            "SSE_DRAIN_TIMEOUT_SECS",
            &env_or_file_or("SSE_DRAIN_TIMEOUT_SECS", &file, "5"),
            1,
            60,
            "5",
            &mut errors,
        )
        .unwrap_or(5);

        let sse_multi_max_contract_ids = parse_int_range::<usize>(
            "SSE_MULTI_MAX_CONTRACT_IDS",
            &env_or_file_or("SSE_MULTI_MAX_CONTRACT_IDS", &file, "100"),
            1,
            usize::MAX,
            "100",
            &mut errors,
        )
        .unwrap_or(100);

        let max_body_size_bytes = parse_int::<usize>(
            "MAX_BODY_SIZE_BYTES",
            &env_or_file_or("MAX_BODY_SIZE_BYTES", &file, "1048576"),
            "1048576",
            &mut errors,
        )
        .unwrap_or(1024 * 1024);

        let log_sample_rate = parse_int_range::<u32>(
            "LOG_SAMPLE_RATE",
            &env_or_file_or("LOG_SAMPLE_RATE", &file, "1"),
            1,
            u32::MAX,
            "1",
            &mut errors,
        )
        .unwrap_or(1);

        let contract_count_cache_size = parse_int::<u64>(
            "CONTRACT_COUNT_CACHE_SIZE",
            &env_or_file_or("CONTRACT_COUNT_CACHE_SIZE", &file, "1000"),
            "1000",
            &mut errors,
        )
        .unwrap_or(1000);

        let contract_count_cache_ttl_secs = parse_int::<u64>(
            "CONTRACT_COUNT_CACHE_TTL_SECS",
            &env_or_file_or("CONTRACT_COUNT_CACHE_TTL_SECS", &file, "30"),
            "30",
            &mut errors,
        )
        .unwrap_or(30);

        let indexer_lock_retry_secs = parse_int::<u64>(
            "INDEXER_LOCK_RETRY_SECS",
            &env_or_file_or("INDEXER_LOCK_RETRY_SECS", &file, "30"),
            "30",
            &mut errors,
        )
        .unwrap_or(30);

        let export_max_rows = parse_int::<u64>(
            "EXPORT_MAX_ROWS",
            &env_or_file_or("EXPORT_MAX_ROWS", &file, "10000"),
            "10000",
            &mut errors,
        )
        .unwrap_or(10_000);

        let health_check_timeout_ms = parse_int::<u64>(
            "HEALTH_CHECK_TIMEOUT_MS",
            &env_or_file_or("HEALTH_CHECK_TIMEOUT_MS", &file, "2000"),
            "2000",
            &mut errors,
        )
        .unwrap_or(2000);

        let index_check_interval_hours = parse_int::<u64>(
            "INDEX_CHECK_INTERVAL_HOURS",
            &env_or_file_or("INDEX_CHECK_INTERVAL_HOURS", &file, "24"),
            "24",
            &mut errors,
        )
        .unwrap_or(24);

        let archive_after_days = parse_int::<u32>(
            "ARCHIVE_AFTER_DAYS",
            &env_or_file_or("ARCHIVE_AFTER_DAYS", &file, "30"),
            "30",
            &mut errors,
        )
        .unwrap_or(30);

        let batch_tx_max_size = parse_int::<usize>(
            "BATCH_TX_MAX_SIZE",
            &env_or_file_or("BATCH_TX_MAX_SIZE", &file, "100"),
            "100",
            &mut errors,
        )
        .unwrap_or(100);

        let batch_events_max_size = parse_int::<usize>(
            "BATCH_EVENTS_MAX_SIZE",
            &env_or_file_or("BATCH_EVENTS_MAX_SIZE", &file, "10000"),
            "10000",
            &mut errors,
        )
        .unwrap_or(10_000);

        let aggregation_cache_ttl_secs = parse_int::<u64>(
            "AGGREGATION_CACHE_TTL_SECS",
            &env_or_file_or("AGGREGATION_CACHE_TTL_SECS", &file, "60"),
            "60",
            &mut errors,
        )
        .unwrap_or(60);

        let aggregation_cache_max_entries = parse_int::<u64>(
            "AGGREGATION_CACHE_MAX_ENTRIES",
            &env_or_file_or("AGGREGATION_CACHE_MAX_ENTRIES", &file, "1000"),
            "1000",
            &mut errors,
        )
        .unwrap_or(1_000);

        let event_data_encryption_key = env_or_file("EVENT_DATA_ENCRYPTION_KEY", &file)
            .and_then(|v| parse_hex_key_checked("EVENT_DATA_ENCRYPTION_KEY", &v, &mut errors));

        let event_data_encryption_key_old = env_or_file("EVENT_DATA_ENCRYPTION_KEY_OLD", &file)
            .and_then(|v| parse_hex_key_checked("EVENT_DATA_ENCRYPTION_KEY_OLD", &v, &mut errors));

        let integration_encryption_key = env_or_file("INTEGRATION_ENCRYPTION_KEY", &file)
            .and_then(|v| parse_hex_key_checked("INTEGRATION_ENCRYPTION_KEY", &v, &mut errors));

        let integration_encryption_key_old = env_or_file("INTEGRATION_ENCRYPTION_KEY_OLD", &file)
            .and_then(|v| parse_hex_key_checked("INTEGRATION_ENCRYPTION_KEY_OLD", &v, &mut errors));

        let rpc_headers = parse_rpc_headers_checked(&mut errors);
        let indexer_event_types = parse_indexer_event_types_checked(&mut errors);

        // Issue #1262: derived metrics DSL configuration.
        let derived_metrics_config_path = env_or_file("DERIVED_METRICS_CONFIG", &file)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let derived_metrics_inline = env_or_file("DERIVED_METRICS_INLINE", &file)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let derived_metrics_interval_secs = parse_int_range::<u64>(
            "DERIVED_METRICS_INTERVAL_SECS",
            &env_or_file_or("DERIVED_METRICS_INTERVAL_SECS", &file, "60"),
            10,
            3600,
            "60",
            &mut errors,
        )
        .unwrap_or(60);

        // Issue #1263: incident correlation configuration. Out-of-range values
        // are clamped (with a warning) rather than aborting startup, since a
        // mis-sized window should not take the whole deployment down.
        let correlation_window_secs = {
            let raw = env_or_file_or("CORRELATION_WINDOW_SECS", &file, "300");
            let parsed = raw.trim().parse::<u64>().unwrap_or_else(|_| {
                tracing::warn!(
                    value = %raw,
                    "CORRELATION_WINDOW_SECS is not a valid integer; using default 300"
                );
                300
            });
            let clamped = parsed.clamp(60, 3600);
            if clamped != parsed {
                tracing::warn!(
                    value = parsed,
                    clamped = clamped,
                    "CORRELATION_WINDOW_SECS out of range [60, 3600]; clamped"
                );
            }
            clamped
        };

        let incident_detector_interval_secs = {
            let raw = env_or_file_or("INCIDENT_DETECTOR_INTERVAL_SECS", &file, "30");
            let parsed = raw.trim().parse::<u64>().unwrap_or_else(|_| {
                tracing::warn!(
                    value = %raw,
                    "INCIDENT_DETECTOR_INTERVAL_SECS is not a valid integer; using default 30"
                );
                30
            });
            let clamped = parsed.clamp(10, 300);
            if clamped != parsed {
                tracing::warn!(
                    value = parsed,
                    clamped = clamped,
                    "INCIDENT_DETECTOR_INTERVAL_SECS out of range [10, 300]; clamped"
                );
            }
            clamped
        };

        tracing::info!(
            correlation_window_secs = correlation_window_secs,
            incident_detector_interval_secs = incident_detector_interval_secs,
            "incident correlation configuration resolved"
        );

        let config = Self {
            database_url,
            database_replica_url: env::var("DATABASE_REPLICA_URL")
                .ok()
                .filter(|s| !s.is_empty()),
            stellar_rpc_url,
            stellar_rpc_fallback_urls,
            rpc_headers,
            start_ledger,
            start_ledger_fallback,
            port,
            api_keys: {
                let mut keys = Vec::new();
                if let Some(key) = env_or_file("API_KEY", &file) {
                    keys.push(SecretString::new(key));
                }
                if let Some(key) = env_or_file("API_KEY_SECONDARY", &file) {
                    keys.push(SecretString::new(key));
                }
                keys
            },
            admin_api_keys: {
                let mut keys = Vec::new();
                if let Some(key) = env_or_file("ADMIN_API_KEY", &file) {
                    keys.push(SecretString::new(key));
                }
                if let Some(key) = env_or_file("ADMIN_API_KEY_SECONDARY", &file) {
                    keys.push(SecretString::new(key));
                }
                keys
            },
            db_max_connections,
            db_min_connections,
            db_idle_timeout_secs,
            db_max_lifetime_secs,
            db_test_before_acquire,
            behind_proxy,
            rpc_connect_timeout_secs,
            rpc_request_timeout_secs,
            allowed_origins,
            rate_limit_per_minute,
            subscription_rate_limit_per_minute,
            rate_limit_key_per_minute,
            rate_limit_key_per_hour,
            rate_limit_key_per_day,
            rate_limit_key_per_month,
            ip_denylist,
            ip_allowlist,
            indexer_lag_warn_threshold,
            indexer_stall_timeout_secs,
            db_statement_timeout_ms,
            indexer_poll_interval_ms,
            indexer_error_backoff_ms,
            sse_keepalive_interval_ms,
            sse_max_connections,
            sse_drain_timeout_secs,
            sse_multi_max_contract_ids,
            environment: environment.clone(),
            max_body_size_bytes,
            log_sample_rate,
            webhook_url: env::var("WEBHOOK_URL").ok().filter(|s| !s.is_empty()),
            webhook_secret: env::var("WEBHOOK_SECRET").ok().filter(|s| !s.is_empty()),
            webhook_contract_filter: env::var("WEBHOOK_CONTRACT_FILTER")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            webhook_rate_limit_per_minute: env::var("WEBHOOK_RATE_LIMIT_PER_MINUTE")
                .ok()
                .and_then(|v| v.trim().parse::<u32>().ok())
                .filter(|n| *n > 0),
            webhook_rate_limit_per_hour: env::var("WEBHOOK_RATE_LIMIT_PER_HOUR")
                .ok()
                .and_then(|v| v.trim().parse::<u32>().ok())
                .filter(|n| *n > 0),
            webhook_require_https: env_or_file("WEBHOOK_REQUIRE_HTTPS", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or_else(|| environment.is_production_like()),
            indexer_event_types,
            event_data_encryption_key,
            event_data_encryption_key_old,
            integration_encryption_key,
            integration_encryption_key_old,
            index_check_interval_hours,
            health_check_timeout_ms,
            tls_cert_file: env::var("TLS_CERT_FILE").ok().filter(|s| !s.is_empty()),
            tls_key_file: env::var("TLS_KEY_FILE").ok().filter(|s| !s.is_empty()),
            bloom_filter_fp_rate: env_or_file_or("BLOOM_FILTER_FP_RATE", &file, "0.001")
                .parse()
                .unwrap_or(0.001),
            bloom_filter_capacity: env_or_file_or("BLOOM_FILTER_CAPACITY", &file, "1000000")
                .parse()
                .unwrap_or(1_000_000),
            bloom_expected_items: env_or_file_or("BLOOM_EXPECTED_ITEMS", &file, "0")
                .parse()
                .unwrap_or(0),
            bloom_false_positive_rate: env_or_file_or("BLOOM_FALSE_POSITIVE_RATE", &file, "0.001")
                .parse()
                .unwrap_or(0.001),
            enable_content_dedup: env_or_file("ENABLE_CONTENT_DEDUP", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            dedup_window_secs: env_or_file_or("DEDUP_WINDOW_SECS", &file, "3600")
                .parse()
                .unwrap_or(3600),
            kinesis_stream_name: env::var("KINESIS_STREAM_NAME")
                .ok()
                .filter(|s| !s.is_empty()),
            aws_region: env::var("AWS_REGION").ok().filter(|s| !s.is_empty()),
            sqs_queue_url: env_or_file("SQS_QUEUE_URL", &file),
            sqs_dlq_url: env_or_file("SQS_DLQ_URL", &file),
            sqs_batch_size: env_or_file("SQS_BATCH_SIZE", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
            event_hubs_connection_string: env_or_file("EVENT_HUBS_CONNECTION_STRING", &file),
            event_hub_name: env_or_file("EVENT_HUB_NAME", &file),
            event_hubs_partition_strategy: env_or_file("EVENT_HUBS_PARTITION_STRATEGY", &file)
                .unwrap_or_else(|| "contract_id".to_string()),
            pubsub_project_id: env::var("PUBSUB_PROJECT_ID").ok().filter(|s| !s.is_empty()),
            pubsub_topic_id: env::var("PUBSUB_TOPIC_ID").ok().filter(|s| !s.is_empty()),
            pubsub_enable_message_ordering: env_or_file("PUBSUB_ENABLE_MESSAGE_ORDERING", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(true),
            max_event_data_bytes: parse_int::<usize>(
                "MAX_EVENT_DATA_BYTES",
                &env_or_file_or("MAX_EVENT_DATA_BYTES", &file, "65536"),
                "65536",
                &mut errors,
            )
            .unwrap_or(65536),
            export_max_rows,
            contract_count_cache_size,
            contract_count_cache_ttl_secs,
            indexer_lock_retry_secs,
            multi_tenant: env_or_file("MULTI_TENANT", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            tenant_rate_limit_per_minute: env_or_file("TENANT_RATE_LIMIT_PER_MINUTE", &file)
                .and_then(|v| v.trim().parse::<u32>().ok())
                .filter(|n| *n > 0),
            tenant_contract_filter: parse_tenant_contract_filter(),
            indexer_tenant_id: env_or_file("INDEXER_TENANT_ID", &file),
            email_smtp_host: env_or_file("EMAIL_SMTP_HOST", &file),
            email_smtp_port: env_or_file("EMAIL_SMTP_PORT", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(587),
            email_smtp_user: env_or_file("EMAIL_SMTP_USER", &file),
            email_smtp_password: env_or_file("EMAIL_SMTP_PASSWORD", &file)
                .map(SecretString::new),
            email_from: env_or_file("EMAIL_FROM", &file),
            email_to: env_or_file("EMAIL_TO", &file)
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            email_contract_filter: env_or_file("EMAIL_CONTRACT_FILTER", &file)
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            // Issue #479: notification scheduling
            email_schedule: env_or_file("EMAIL_SCHEDULE", &file)
                .map(|v| v.trim().to_ascii_lowercase())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| "immediate".to_string()),
            email_daily_digest_hour: env_or_file("EMAIL_DAILY_DIGEST_HOUR", &file)
                .and_then(|v| v.trim().parse().ok())
                .filter(|h| *h <= 23)
                .unwrap_or(9),
            email_cron: env_or_file("EMAIL_CRON", &file)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty()),
            email_quiet_hours_start: env_or_file("EMAIL_QUIET_HOURS_START", &file)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty()),
            email_quiet_hours_end: env_or_file("EMAIL_QUIET_HOURS_END", &file)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty()),
            email_public_base_url: env_or_file("EMAIL_PUBLIC_BASE_URL", &file),
            redis_url: env_or_file("REDIS_URL", &file),
            redis_stream_key: env_or_file("REDIS_STREAM_KEY", &file),
            redis_buffer_max_size: env_or_file("REDIS_BUFFER_MAX_SIZE", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
            stats_refresh_interval_secs: env_or_file("STATS_REFRESH_INTERVAL_SECS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(3600),
            retention_days: env_or_file("RETENTION_DAYS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(90),
            pruning_interval_hours: env_or_file("PRUNING_INTERVAL_HOURS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(24),
            sse_replay_limit: env_or_file("SSE_REPLAY_MAX_EVENTS", &file)
                .or_else(|| env_or_file("SSE_REPLAY_LIMIT", &file))
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            sse_max_connections_per_ip,
            sse_max_lag_before_disconnect: env_or_file("SSE_MAX_LAG_BEFORE_DISCONNECT", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            max_ledger_range: parse_int::<u64>(
                "MAX_LEDGER_RANGE",
                &env_or_file_or("MAX_LEDGER_RANGE", &file, "100000"),
                "100000",
                &mut errors,
            )
            .unwrap_or(100_000),
            indexer_ignore_checkpoint: env_or_file("INDEXER_IGNORE_CHECKPOINT", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            rpc_max_events_per_page: parse_int::<usize>(
                "RPC_MAX_EVENTS_PER_PAGE",
                &env_or_file_or("RPC_MAX_EVENTS_PER_PAGE", &file, "200"),
                "200",
                &mut errors,
            )
            .unwrap_or(200),
            slow_query_threshold_ms: parse_int::<u64>(
                "SLOW_QUERY_THRESHOLD_MS",
                &env_or_file_or("SLOW_QUERY_THRESHOLD_MS", &file, "1000"),
                "1000",
                &mut errors,
            )
            .unwrap_or(1000),
            event_transform_script: std::env::var("EVENT_TRANSFORM_SCRIPT").ok(),
            event_transform_timeout_ms: parse_int::<u64>(
                "EVENT_TRANSFORM_TIMEOUT_MS",
                &env_or_file_or("EVENT_TRANSFORM_TIMEOUT_MS", &file, "100"),
                "100",
                &mut errors,
            )
            .unwrap_or(100),
            webhook_notification_format: env_or_file("WEBHOOK_NOTIFICATION_FORMAT", &file)
                .unwrap_or_else(|| "raw".to_string()),
            webhook_message_template: env_or_file("WEBHOOK_MESSAGE_TEMPLATE", &file),
            pagerduty_routing_key: env_or_file("PAGERDUTY_ROUTING_KEY", &file),
            pagerduty_service_name: env_or_file("PAGERDUTY_SERVICE_NAME", &file)
                .unwrap_or_else(|| "Soroban Pulse".to_string()),
            pagerduty_contract_filter: env_or_file("PAGERDUTY_CONTRACT_FILTER", &file)
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            pagerduty_event_type_filter: env_or_file("PAGERDUTY_EVENT_TYPE_FILTER", &file)
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            pagerduty_severity_mapping: env_or_file("PAGERDUTY_SEVERITY_MAPPING", &file)
                .and_then(|v| serde_json::from_str(&v).ok())
                .unwrap_or_else(|| {
                    let mut map = std::collections::HashMap::new();
                    map.insert("contract".to_string(), "error".to_string());
                    map.insert("diagnostic".to_string(), "warning".to_string());
                    map.insert("system".to_string(), "info".to_string());
                    map
                }),
            pagerduty_auto_resolve: env_or_file("PAGERDUTY_AUTO_RESOLVE", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(true),
            pagerduty_auto_resolve_threshold_minutes: env_or_file("PAGERDUTY_AUTO_RESOLVE_THRESHOLD_MINUTES", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            stats_cache_ttl_secs: env_or_file("STATS_CACHE_TTL_SECS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            twilio_account_sid: env_or_file("TWILIO_ACCOUNT_SID", &file),
            twilio_auth_token: env_or_file("TWILIO_AUTH_TOKEN", &file).map(SecretString::new),
            twilio_from_number: env_or_file("TWILIO_FROM_NUMBER", &file),
            sms_to_numbers: env_or_file("SMS_TO_NUMBERS", &file)
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            sms_contract_filter: env_or_file("SMS_CONTRACT_FILTER", &file)
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            webhook_retry_policy: crate::retry_policy::RetryPolicy::webhook_default(),
            email_retry_policy: crate::retry_policy::RetryPolicy::email_default(),
            sms_retry_policy: crate::retry_policy::RetryPolicy::sms_default(),
            sse_shutdown_grace_period_ms: env_or_file("SSE_SHUTDOWN_GRACE_PERIOD_MS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(200),
            email_max_contracts_in_digest: env_or_file("EMAIL_MAX_CONTRACTS_IN_DIGEST", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(20),
            notification_default_priority: env_or_file("NOTIFICATION_DEFAULT_PRIORITY", &file)
                .unwrap_or_else(|| "medium".to_string()),
            notification_priority_rule_path: env_or_file("NOTIFICATION_PRIORITY_RULE_PATH", &file),
            notification_priority_rule_value: env_or_file("NOTIFICATION_PRIORITY_RULE_VALUE", &file),
            notification_priority_rule_priority: env_or_file("NOTIFICATION_PRIORITY_RULE_PRIORITY", &file),
            escalation_primary_channel: env_or_file("ESCALATION_PRIMARY_CHANNEL", &file),
            escalation_channel: env_or_file("ESCALATION_CHANNEL", &file),
            escalation_delay_minutes: env_or_file("ESCALATION_DELAY_MINUTES", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(30),
            oncall_provider: env_or_file("ONCALL_PROVIDER", &file),
            oncall_pagerduty_api_key: env_or_file("ONCALL_PAGERDUTY_API_KEY", &file)
                .map(SecretString::new),
            oncall_schedule_id: env_or_file("ONCALL_SCHEDULE_ID", &file),
            oncall_schedule_cache_ttl_secs: env_or_file("ONCALL_SCHEDULE_CACHE_TTL_SECS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            // Issue #610: Event compression
            event_compression_enabled: env_or_file("EVENT_COMPRESSION_ENABLED", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            event_compression_migration_batch_size: env_or_file(
                "EVENT_COMPRESSION_MIGRATION_BATCH_SIZE",
                &file,
            )
            .and_then(|v| v.parse().ok())
            .unwrap_or(500),
            // Issue #609: Multi-chain
            chain_id: env_or_file_or("CHAIN_ID", &file, "mainnet"),
            additional_chain_ids: env_or_file("ADDITIONAL_CHAIN_IDS", &file)
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            // Issue #608: Ledger hash tracking
            ledger_hash_tracking_enabled: env_or_file("LEDGER_HASH_TRACKING_ENABLED", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            ledger_hash_validation_depth: env_or_file("LEDGER_HASH_VALIDATION_DEPTH", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(1_000),
            // Issue #607: ABI cache
            abi_cache_ttl_secs: env_or_file("ABI_CACHE_TTL_SECS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(crate::abi::DEFAULT_ABI_CACHE_TTL_SECS),
            abi_cache_max_entries: env_or_file("ABI_CACHE_MAX_ENTRIES", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(crate::abi::ABI_CACHE_MAX_ENTRIES),
            // SSE reconnection ring buffer
            sse_ring_buffer_capacity: env_or_file("SSE_RING_BUFFER_CAPACITY", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(crate::sse_ring_buffer::DEFAULT_RING_BUFFER_CAPACITY),
            // Query result cache
            query_cache_ttl_secs: env_or_file("QUERY_CACHE_TTL_SECS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(crate::query_cache::DEFAULT_TTL_SECS),
            query_cache_max_capacity: env_or_file("QUERY_CACHE_MAX_CAPACITY", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(crate::query_cache::DEFAULT_MAX_CAPACITY),
            enable_push_preload: env_or_file("ENABLE_PUSH_PRELOAD", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
                .unwrap_or(false),
            // Issue #1147: set to false when using the Helm migration Job so
            // that application pods skip the startup migration step.
            run_migrations_on_startup: env_or_file("RUN_MIGRATIONS_ON_STARTUP", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
                .unwrap_or(true),
            serve_dashboard: env_or_file("SERVE_DASHBOARD", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
                .unwrap_or(false),
            dashboard_dir: env_or_file_or("DASHBOARD_DIR", &file, "web/dist"),
            dashboard_connect_src: env_or_file("DASHBOARD_CONNECT_SRC", &file)
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            // Issue #705: Kafka event publishing
            kafka_brokers: env_or_file("KAFKA_BROKERS", &file),
            kafka_topic: env_or_file("KAFKA_TOPIC", &file),
            kafka_batch_size: env_or_file("KAFKA_BATCH_SIZE", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(16384),
            kafka_linger_ms: env_or_file("KAFKA_LINGER_MS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            // Issue #841: SaaS Platform
            saas_enabled: env_or_file("SAAS_ENABLED", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            billing_provider: env_or_file("BILLING_PROVIDER", &file),
            billing_api_key: env_or_file("BILLING_API_KEY", &file),
            default_trial_days: env_or_file("DEFAULT_TRIAL_DAYS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(14),
            saas_public_base_url: env_or_file("SAAS_PUBLIC_BASE_URL", &file),
            // Issue #842: ML Integration
            ml_anomaly_detection_enabled: env_or_file("ML_ANOMALY_DETECTION_ENABLED", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            ml_pattern_recognition_enabled: env_or_file("ML_PATTERN_RECOGNITION_ENABLED", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            ml_intelligent_filtering_enabled: env_or_file("ML_INTELLIGENT_FILTERING_ENABLED", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(false),
            ml_model_training_interval_hours: env_or_file("ML_MODEL_TRAINING_INTERVAL_HOURS", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(24),
            ml_min_training_samples: env_or_file("ML_MIN_TRAINING_SAMPLES", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            ml_confidence_threshold: env_or_file("ML_CONFIDENCE_THRESHOLD", &file)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.8),
            ml_auto_retrain: env_or_file("ML_AUTO_RETRAIN", &file)
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
                .unwrap_or(true),
            role: Role::from_str(&env_or_file_or("ROLE", &file, "all")),
            derived_metrics_config_path,
            derived_metrics_inline,
            derived_metrics_interval_secs,
            correlation_window_secs,
            incident_detector_interval_secs,
        };
        (config, errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_environment_from_str() {
        assert_eq!(Environment::from_str("production"), Environment::Production);
        assert_eq!(Environment::from_str("prod"), Environment::Production);
        assert_eq!(Environment::from_str("staging"), Environment::Staging);
        assert_eq!(Environment::from_str("stage"), Environment::Staging);
        assert_eq!(
            Environment::from_str("development"),
            Environment::Development
        );
        assert_eq!(Environment::from_str("dev"), Environment::Development);
        assert_eq!(Environment::from_str("unknown"), Environment::Development);
    }

    #[test]
    fn test_environment_is_production_like() {
        assert!(!Environment::Development.is_production_like());
        assert!(Environment::Staging.is_production_like());
        assert!(Environment::Production.is_production_like());
    }

    #[test]
    fn test_indexer_state_new() {
        let state = IndexerState::new();
        assert_eq!(
            state
                .current_ledger
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            state
                .latest_ledger
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(state.started_at > 0);
    }

    #[test]
    fn test_indexer_state_uptime() {
        let state = IndexerState::new();
        let uptime1 = state.uptime_secs();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let uptime2 = state.uptime_secs();
        assert!(uptime2 >= uptime1);
    }

    #[test]
    fn test_health_state_new() {
        let health_state = HealthState::new(60);
        assert_eq!(health_state.indexer_stall_timeout_secs, 60);
        assert_eq!(
            health_state
                .last_indexer_poll
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[test]
    fn test_health_state_update_and_check() {
        let health_state = HealthState::new(60);
        assert_eq!(health_state.is_indexer_stalled(), Some(0));
        health_state.update_last_poll();
        assert_eq!(health_state.is_indexer_stalled(), None);
        // Polls are tracked at whole-second granularity, so backdate the last
        // poll instead of sleeping (a 10 ms sleep never crossed the threshold).
        let health_state_short = HealthState::new(0);
        health_state_short.last_indexer_poll.store(1, Ordering::SeqCst);
        assert!(health_state_short.is_indexer_stalled().is_some());
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.database_url, "postgres://localhost/soroban_pulse");
        assert_eq!(
            config.stellar_rpc_url,
            "https://soroban-testnet.stellar.org"
        );
        assert_eq!(config.port, 3000);
        assert_eq!(config.start_ledger, 0);
        assert!(!config.start_ledger_fallback);
        assert_eq!(config.environment, Environment::Development);
        assert_eq!(config.graphql_max_depth, 10);
        assert_eq!(config.graphql_max_complexity, 1000);
    }

    #[test]
    fn graphql_limits_parse_from_env() {
        env::set_var("GRAPHQL_MAX_DEPTH", "5");
        env::set_var("GRAPHQL_MAX_COMPLEXITY", "250");
        let mut errors = Vec::new();
        let depth = parse_int_range::<usize>(
            "GRAPHQL_MAX_DEPTH",
            &env_or_file_or("GRAPHQL_MAX_DEPTH", &load_config_file(), "10"),
            1,
            100,
            "10",
            &mut errors,
        );
        let complexity = parse_int_range::<usize>(
            "GRAPHQL_MAX_COMPLEXITY",
            &env_or_file_or("GRAPHQL_MAX_COMPLEXITY", &load_config_file(), "1000"),
            1,
            1_000_000,
            "1000",
            &mut errors,
        );
        env::remove_var("GRAPHQL_MAX_DEPTH");
        env::remove_var("GRAPHQL_MAX_COMPLEXITY");
        assert_eq!(depth, Some(5));
        assert_eq!(complexity, Some(250));
        assert!(errors.is_empty());
    }

    #[test]
    fn graphql_limits_reject_invalid_values() {
        let mut errors = Vec::new();
        let depth = parse_int_range::<usize>(
            "GRAPHQL_MAX_DEPTH",
            "0",
            1,
            100,
            "10",
            &mut errors,
        );
        let complexity = parse_int_range::<usize>(
            "GRAPHQL_MAX_COMPLEXITY",
            "not-a-number",
            1,
            1_000_000,
            "1000",
            &mut errors,
        );
        assert!(depth.is_none());
        assert!(complexity.is_none());
        assert_eq!(errors.len(), 2);
        assert!(errors[0].contains("GRAPHQL_MAX_DEPTH"));
        assert!(errors[1].contains("GRAPHQL_MAX_COMPLEXITY"));
    }

    #[test]
    fn test_config_safe_db_url() {
        let mut config = Config::default();
        config.database_url = "postgres://user:password@localhost/db".to_string();
        let safe_url = config.safe_db_url();
        assert!(!safe_url.contains("password"));
        assert!(safe_url.contains("localhost"));
    }

    #[test]
    fn test_config_safe_db_url_unparseable() {
        let mut config = Config::default();
        config.database_url = "not-a-url".to_string();
        assert_eq!(config.safe_db_url(), "<unparseable>");
    }

    #[test]
    fn test_safe_rpc_headers_returns_names_only() {
        let mut config = Config::default();
        config.rpc_headers = vec![
            ("X-API-Key".to_string(), "secret".to_string()),
            ("Authorization".to_string(), "Bearer token".to_string()),
        ];
        let safe = config.safe_rpc_headers();
        assert_eq!(safe, vec!["X-API-Key", "Authorization"]);
    }

    #[test]
    fn startup_log_fields_do_not_contain_credentials() {
        let mut config = Config::default();
        config.database_url = "postgres://admin:supersecret@db.example.com/mydb".to_string();
        let safe_db = config.safe_db_url();
        assert!(!safe_db.contains("supersecret"));
        assert!(!safe_db.contains("admin"));
    }

    // --- validate_rpc_url_checked ---

    #[test]
    fn validate_rpc_url_accepts_valid_https() {
        let mut errors = Vec::new();
        let out = validate_rpc_url_checked("https://soroban-testnet.stellar.org", &mut errors);
        assert_eq!(
            out.as_deref(),
            Some("https://soroban-testnet.stellar.org/")
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn validate_rpc_url_rejects_malformed() {
        let mut errors = Vec::new();
        // A typo like "htps://" parses as a URL with scheme "htps", which is
        // rejected as a disallowed scheme.
        let out = validate_rpc_url_checked("htps://soroban-testnet.stellar.org", &mut errors);
        assert!(out.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("STELLAR_RPC_URL"));
    }

    #[test]
    fn validate_rpc_url_rejects_garbage() {
        let mut errors = Vec::new();
        let out = validate_rpc_url_checked("not a url at all", &mut errors);
        assert!(out.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("is not a valid URL"));
    }

    #[test]
    fn validate_rpc_url_rejects_non_http_scheme() {
        let mut errors = Vec::new();
        let out = validate_rpc_url_checked("ftp://example.com", &mut errors);
        assert!(out.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("disallowed scheme"));
    }

    #[test]
    fn validate_rpc_url_strips_embedded_credentials() {
        let mut errors = Vec::new();
        let out = validate_rpc_url_checked("https://user:pass@rpc.example.com/", &mut errors);
        let out = out.expect("valid url");
        assert!(!out.contains("user"));
        assert!(!out.contains("pass"));
        assert!(errors.is_empty());
    }

    #[test]
    fn parse_rpc_fallback_urls_empty_when_unset() {
        env::remove_var("STELLAR_RPC_FALLBACK_URLS");
        let mut errors = Vec::new();
        assert!(parse_rpc_fallback_urls_checked(&mut errors).is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn parse_rpc_fallback_urls_validates_each_entry() {
        env::set_var(
            "STELLAR_RPC_FALLBACK_URLS",
            "https://rpc2.example.com, ftp://bad.example.com , https://rpc3.example.com",
        );
        let mut errors = Vec::new();
        let urls = parse_rpc_fallback_urls_checked(&mut errors);
        env::remove_var("STELLAR_RPC_FALLBACK_URLS");
        // The two valid https entries are kept; the ftp entry produces one error.
        assert_eq!(urls.len(), 2);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("STELLAR_RPC_FALLBACK_URLS"));
    }

    // --- resolve_rpc_urls_checked (issue #1256) ---

    #[test]
    fn resolve_rpc_urls_uses_single_url_when_list_unset() {
        let mut errors = Vec::new();
        let (primary, others) =
            resolve_rpc_urls_checked(None, "https://rpc1.example.com", vec![], &mut errors);
        assert_eq!(primary, "https://rpc1.example.com/");
        assert!(others.is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn resolve_rpc_urls_list_takes_precedence() {
        let mut errors = Vec::new();
        let (primary, others) = resolve_rpc_urls_checked(
            Some(" https://a.example.com , https://b.example.com,"),
            "https://ignored.example.com",
            vec![],
            &mut errors,
        );
        assert_eq!(primary, "https://a.example.com/");
        assert_eq!(others, vec!["https://b.example.com/".to_string()]);
        assert!(errors.is_empty());
    }

    #[test]
    fn resolve_rpc_urls_appends_fallbacks_without_duplicates() {
        let mut errors = Vec::new();
        let (primary, others) = resolve_rpc_urls_checked(
            Some("https://a.example.com,https://b.example.com"),
            "https://ignored.example.com",
            vec![
                "https://b.example.com/".to_string(),
                "https://a.example.com/".to_string(),
                "https://c.example.com/".to_string(),
            ],
            &mut errors,
        );
        assert_eq!(primary, "https://a.example.com/");
        assert_eq!(
            others,
            vec!["https://b.example.com/".to_string(), "https://c.example.com/".to_string()]
        );
    }

    #[test]
    fn resolve_rpc_urls_reports_invalid_list_entries() {
        let mut errors = Vec::new();
        let (primary, others) = resolve_rpc_urls_checked(
            Some("ftp://bad.example.com,https://good.example.com"),
            "https://ignored.example.com",
            vec![],
            &mut errors,
        );
        assert_eq!(primary, "https://good.example.com/");
        assert!(others.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("STELLAR_RPC_URLS"));
    }

    // --- parse_rpc_headers_checked ---

    #[test]
    fn test_parse_rpc_headers_empty() {
        env::remove_var("STELLAR_RPC_HEADERS");
        let mut errors = Vec::new();
        let headers = parse_rpc_headers_checked(&mut errors);
        assert!(headers.is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_rpc_headers_single() {
        env::set_var("STELLAR_RPC_HEADERS", "X-API-Key: mykey");
        let mut errors = Vec::new();
        let headers = parse_rpc_headers_checked(&mut errors);
        env::remove_var("STELLAR_RPC_HEADERS");
        assert_eq!(
            headers,
            vec![("X-API-Key".to_string(), "mykey".to_string())]
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_rpc_headers_multiple() {
        env::set_var("STELLAR_RPC_HEADERS", "X-API-Key: mykey; X-Custom: value");
        let mut errors = Vec::new();
        let headers = parse_rpc_headers_checked(&mut errors);
        env::remove_var("STELLAR_RPC_HEADERS");
        assert_eq!(headers.len(), 2);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_rpc_headers_invalid_format_collects_error() {
        env::set_var("STELLAR_RPC_HEADERS", "NoColonHere");
        let mut errors = Vec::new();
        let headers = parse_rpc_headers_checked(&mut errors);
        env::remove_var("STELLAR_RPC_HEADERS");
        assert!(headers.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("STELLAR_RPC_HEADERS"));
        assert!(errors[0].contains("Name: Value"));
    }

    // --- parse_indexer_event_types_checked ---

    #[test]
    fn test_parse_indexer_event_types_empty() {
        env::remove_var("INDEXER_EVENT_TYPES");
        let mut errors = Vec::new();
        assert!(parse_indexer_event_types_checked(&mut errors).is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_indexer_event_types_single() {
        env::set_var("INDEXER_EVENT_TYPES", "contract");
        let mut errors = Vec::new();
        let types = parse_indexer_event_types_checked(&mut errors);
        env::remove_var("INDEXER_EVENT_TYPES");
        assert_eq!(types, vec!["contract"]);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_indexer_event_types_multiple() {
        env::set_var("INDEXER_EVENT_TYPES", "contract,diagnostic");
        let mut errors = Vec::new();
        let types = parse_indexer_event_types_checked(&mut errors);
        env::remove_var("INDEXER_EVENT_TYPES");
        assert_eq!(types, vec!["contract", "diagnostic"]);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_indexer_event_types_invalid_collects_error() {
        env::set_var("INDEXER_EVENT_TYPES", "contract,invalid");
        let mut errors = Vec::new();
        let types = parse_indexer_event_types_checked(&mut errors);
        env::remove_var("INDEXER_EVENT_TYPES");
        assert_eq!(types, vec!["contract"]);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("INDEXER_EVENT_TYPES"));
        assert!(errors[0].contains("invalid"));
    }

    // --- parse_int / parse_bool / parse_int_range ---

    #[test]
    fn parse_int_valid_returns_value() {
        let mut errors = Vec::new();
        assert_eq!(
            parse_int::<u32>("PORT", "3000", "3000", &mut errors),
            Some(3000)
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn parse_int_invalid_collects_descriptive_error() {
        let mut errors = Vec::new();
        let result = parse_int::<u32>("DB_MAX_CONNECTIONS", "abc", "10", &mut errors);
        assert!(result.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("DB_MAX_CONNECTIONS"));
        assert!(errors[0].contains("\"abc\""));
        assert!(errors[0].contains("e.g., 10"));
    }

    #[test]
    fn parse_bool_valid_values() {
        let mut e = Vec::new();
        assert_eq!(parse_bool("X", "true", &mut e), Some(true));
        assert_eq!(parse_bool("X", "false", &mut e), Some(false));
        assert_eq!(parse_bool("X", "1", &mut e), Some(true));
        assert_eq!(parse_bool("X", "0", &mut e), Some(false));
        assert!(e.is_empty());
    }

    #[test]
    fn parse_bool_invalid_collects_descriptive_error() {
        let mut errors = Vec::new();
        let result = parse_bool("DB_TEST_BEFORE_ACQUIRE", "yes_please", &mut errors);
        assert!(result.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("DB_TEST_BEFORE_ACQUIRE"));
        assert!(errors[0].contains("\"yes_please\""));
        assert!(errors[0].contains("true or false"));
    }

    #[test]
    fn parse_int_range_out_of_range_collects_descriptive_error() {
        let mut errors = Vec::new();
        let result = parse_int_range::<u64>(
            "INDEXER_POLL_INTERVAL_MS",
            "50",
            100,
            60000,
            "5000",
            &mut errors,
        );
        assert!(result.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("INDEXER_POLL_INTERVAL_MS"));
        assert!(errors[0].contains("50"));
        assert!(errors[0].contains("100"));
        assert!(errors[0].contains("60000"));
    }

    // --- parse_hex_key_checked ---

    #[test]
    fn parse_hex_key_valid() {
        let mut errors = Vec::new();
        let key = parse_hex_key_checked("MY_KEY", &"ab".repeat(32), &mut errors);
        assert!(key.is_some());
        assert!(errors.is_empty());
    }

    #[test]
    fn parse_hex_key_wrong_length_collects_error() {
        let mut errors = Vec::new();
        let key = parse_hex_key_checked("MY_KEY", "tooshort", &mut errors);
        assert!(key.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("MY_KEY"));
        assert!(errors[0].contains("64 hex characters"));
    }

    #[test]
    fn parse_hex_key_non_hex_collects_error() {
        let mut errors = Vec::new();
        let key = parse_hex_key_checked("MY_KEY", &"zz".repeat(32), &mut errors);
        assert!(key.is_none());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("MY_KEY"));
        assert!(errors[0].contains("non-hex"));
    }

    // --- multiple errors collected together ---

    #[test]
    fn multiple_invalid_vars_all_reported() {
        let mut errors = Vec::new();
        parse_int::<u32>("PORT", "bad", "3000", &mut errors);
        parse_int::<u32>("DB_MAX_CONNECTIONS", "also_bad", "10", &mut errors);
        parse_bool("DB_TEST_BEFORE_ACQUIRE", "nope", &mut errors);
        assert_eq!(errors.len(), 3);
        assert!(errors[0].contains("PORT"));
        assert!(errors[1].contains("DB_MAX_CONNECTIONS"));
        assert!(errors[2].contains("DB_TEST_BEFORE_ACQUIRE"));
    }

    // --- resolve_database_url_checked ---

    #[test]
    fn missing_database_url_collects_error() {
        env::remove_var("DATABASE_URL");
        env::remove_var("DATABASE_URL_FILE");
        let mut errors = Vec::new();
        let url = resolve_database_url_checked(&mut errors);
        assert!(url.is_empty());
        // Issue #1247: reported once, by `ConfigBuilder::build`.
        assert!(errors.is_empty());
        let err = ConfigBuilder::new()
            .with_database(DatabaseSettings::new(url))
            .with_rpc(RpcSettings::new("https://soroban-testnet.stellar.org"))
            .build()
            .unwrap_err();
        let db_errors: Vec<_> = err.errors().iter().filter(|e| e.contains("DATABASE_URL")).collect();
        assert_eq!(db_errors.len(), 1, "{err}");
        assert!(db_errors[0].contains("postgres://"));
    }

    // --- config file helpers ---

    #[test]
    fn config_file_provides_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "PORT = \"9999\"\n").unwrap();
        env::remove_var("PORT");
        env::set_var("CONFIG_FILE", path.to_str().unwrap());
        let file = load_config_file();
        let port = env_or_file_or("PORT", &file, "3000");
        assert_eq!(port, "9999");
        env::remove_var("CONFIG_FILE");
    }

    #[test]
    fn env_var_takes_precedence_over_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "PORT = \"9999\"\n").unwrap();
        env::set_var("PORT", "7777");
        env::set_var("CONFIG_FILE", path.to_str().unwrap());
        let file = load_config_file();
        let port = env_or_file_or("PORT", &file, "3000");
        assert_eq!(port, "7777");
        env::remove_var("PORT");
        env::remove_var("CONFIG_FILE");
    }

    #[test]
    fn missing_config_file_is_not_an_error() {
        env::set_var("CONFIG_FILE", "/nonexistent/path/config.toml");
        let file = load_config_file();
        assert!(file.is_empty());
        env::remove_var("CONFIG_FILE");
    }

    // --- Issue #1262: derived metrics config ---

    #[test]
    fn derived_metrics_defaults() {
        let config = Config::default();
        assert!(config.derived_metrics_config_path.is_none());
        assert!(config.derived_metrics_inline.is_none());
        assert_eq!(config.derived_metrics_interval_secs, 60);
    }

    #[test]
    fn derived_metrics_config_path_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "DERIVED_METRICS_CONFIG = \"/etc/pulse/metrics.yaml\"\n\
             DERIVED_METRICS_INTERVAL_SECS = \"120\"\n",
        )
        .unwrap();
        env::remove_var("DERIVED_METRICS_CONFIG");
        env::remove_var("DERIVED_METRICS_INTERVAL_SECS");
        env::set_var("CONFIG_FILE", path.to_str().unwrap());
        let file = load_config_file();
        let cfg_path = env_or_file("DERIVED_METRICS_CONFIG", &file);
        let interval = env_or_file_or("DERIVED_METRICS_INTERVAL_SECS", &file, "60");
        env::remove_var("CONFIG_FILE");
        assert_eq!(cfg_path.as_deref(), Some("/etc/pulse/metrics.yaml"));
        assert_eq!(interval, "120");
    }

    // --- Issue #1263: incident correlation config ---

    #[test]
    fn incident_correlation_defaults() {
        let config = Config::default();
        assert_eq!(config.correlation_window_secs, 300);
        assert_eq!(config.incident_detector_interval_secs, 30);
    }

    #[test]
    fn correlation_window_clamps_out_of_range() {
        // Mirrors the clamping logic in from_env without touching global env.
        let clamp = |raw: &str| -> u64 {
            let parsed = raw.trim().parse::<u64>().unwrap_or(300);
            parsed.clamp(60, 3600)
        };
        assert_eq!(clamp("300"), 300);
        assert_eq!(clamp("10"), 60);
        assert_eq!(clamp("99999"), 3600);
        assert_eq!(clamp("not-a-number"), 300);
    }

    #[test]
    fn incident_detector_interval_clamps_out_of_range() {
        let clamp = |raw: &str| -> u64 {
            let parsed = raw.trim().parse::<u64>().unwrap_or(30);
            parsed.clamp(10, 300)
        };
        assert_eq!(clamp("30"), 30);
        assert_eq!(clamp("1"), 10);
        assert_eq!(clamp("1000"), 300);
        assert_eq!(clamp("garbage"), 30);
    }
}
