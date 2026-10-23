//! Issue #997: Unified configuration validation module.
//!
//! Provides a single entry-point for validating the fully-loaded [`Config`]
//! struct before the service starts.  All checks previously scattered across
//! `main.rs`, `config.rs` constructors, and individual modules are collected
//! here so operators receive a complete list of problems at startup rather than
//! discovering them one at a time.
//!
//! ## Design
//!
//! - [`validate`] performs every check and returns a [`ConfigValidationReport`].
//! - Errors are **fatal**: the service must not start.  Warnings are advisory.
//! - Each check is a small, independently-testable function.
//! - The report can be printed as a human-readable summary or serialised to JSON
//!   for tooling.
//!
//! ## Usage
//!
//! [`validate`] is invoked by `ConfigBuilder::build` (issue #1247), which
//! merges its errors with parse errors into a single `ConfigError` and logs
//! the warnings — callers should build configs through the builder rather
//! than calling this directly.

use serde::Serialize;
use tracing::{info, warn, error};

use crate::config::Config;

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

/// Outcome of a full configuration validation pass.
#[derive(Debug, Default, Serialize)]
pub struct ConfigValidationReport {
    /// Fatal problems — the service must not start when this is non-empty.
    pub errors: Vec<String>,
    /// Non-fatal advisory messages.
    pub warnings: Vec<String>,
}

impl ConfigValidationReport {
    /// Returns `true` when there are no fatal errors.
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Emit all errors at ERROR level and all warnings at WARN level.
    pub fn log(&self) {
        for e in &self.errors {
            error!(config_error = e.as_str(), "Configuration validation error");
        }
        for w in &self.warnings {
            warn!(config_warning = w.as_str(), "Configuration validation warning");
        }
        if self.is_ok() {
            info!(
                warnings = self.warnings.len(),
                "Configuration validation passed"
            );
        }
    }

    fn add_error(&mut self, msg: impl Into<String>) {
        self.errors.push(msg.into());
    }

    fn add_warning(&mut self, msg: impl Into<String>) {
        self.warnings.push(msg.into());
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validate the fully-loaded configuration and return a detailed report.
///
/// The returned report contains all errors and warnings found in a single pass.
/// Call [`ConfigValidationReport::is_ok`] to decide whether to abort startup.
pub fn validate(cfg: &Config) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();

    check_database(&mut report, cfg);
    check_rpc(&mut report, cfg);
    check_cors(&mut report, cfg);
    check_pool_sizing(&mut report, cfg);
    check_auth(&mut report, cfg);
    check_webhook(&mut report, cfg);
    check_bloom_filter(&mut report, cfg);
    check_sse(&mut report, cfg);
    check_rate_limits(&mut report, cfg);
    check_retention(&mut report, cfg);
    check_tls(&mut report, cfg);
    check_email(&mut report, cfg);
    check_encryption(&mut report, cfg);
    check_performance_thresholds(&mut report, cfg);

    report
}

// ---------------------------------------------------------------------------
// Individual check functions
// ---------------------------------------------------------------------------

fn check_database(report: &mut ConfigValidationReport, cfg: &Config) {
    if cfg.database_url.is_empty() {
        report.add_error(
            "DATABASE_URL is not set. Expected a PostgreSQL connection string \
             (e.g., postgres://user:pass@localhost/soroban_pulse).",
        );
    } else if !cfg.database_url.starts_with("postgres://")
        && !cfg.database_url.starts_with("postgresql://")
    {
        report.add_error(
            "DATABASE_URL must begin with postgres:// or postgresql://",
        );
    }

    if let Some(ref replica) = cfg.database_replica_url {
        if !replica.starts_with("postgres://") && !replica.starts_with("postgresql://") {
            report.add_error(
                "DATABASE_REPLICA_URL must begin with postgres:// or postgresql://",
            );
        }
    }

    if cfg.db_statement_timeout_ms == 0 {
        report.add_warning(
            "DB_STATEMENT_TIMEOUT_MS=0 disables query timeouts — runaway queries will not be cancelled",
        );
    }
}

fn check_rpc(report: &mut ConfigValidationReport, cfg: &Config) {
    // Issue #1247: one rule set for RPC URLs (syntax, scheme, host, private
    // ranges), shared with the config parser instead of a looser copy here.
    let mut errors = Vec::new();
    crate::config::validate_rpc_url_checked(&cfg.stellar_rpc_url, &mut errors);
    for url in &cfg.stellar_rpc_fallback_urls {
        crate::config::validate_rpc_url_named("STELLAR_RPC_FALLBACK_URLS", url, &mut errors);
    }
    for e in errors {
        report.add_error(e.trim().to_string());
    }

    if cfg.stellar_rpc_url.starts_with("http://") {
        report.add_warning(
            "STELLAR_RPC_URL uses plain HTTP — consider HTTPS for production",
        );
    }

    if cfg.rpc_connect_timeout_secs == 0 {
        report.add_warning("RPC_CONNECT_TIMEOUT_SECS=0 disables the RPC connection timeout");
    }
    if cfg.rpc_request_timeout_secs == 0 {
        report.add_warning("RPC_REQUEST_TIMEOUT_SECS=0 disables the RPC request timeout");
    }
}

/// Moved here from `Config::from_env` (issue #1247) so it runs once, in
/// `ConfigBuilder::build`, for both env-loaded and builder-constructed configs.
fn check_cors(report: &mut ConfigValidationReport, cfg: &Config) {
    if cfg.environment.is_production_like() && cfg.allowed_origins.iter().any(|o| o == "*") {
        report.add_error(format!(
            "ALLOWED_ORIGINS=* is not permitted in {:?}. \
             Set explicit origins (e.g., https://app.example.com) or use ENVIRONMENT=development.",
            cfg.environment
        ));
    }
    // Issue #938: fail fast on malformed origins instead of letting
    // build_cors() silently drop them from the CORS layer.
    for msg in crate::middleware::validate_cors_origins(&cfg.allowed_origins) {
        report.add_error(msg);
    }
}

fn check_pool_sizing(report: &mut ConfigValidationReport, cfg: &Config) {
    if cfg.db_max_connections == 0 {
        report.add_error("DB_MAX_CONNECTIONS must be at least 1");
    }
    if cfg.db_min_connections > cfg.db_max_connections {
        report.add_error(format!(
            "DB_MIN_CONNECTIONS ({}) must not exceed DB_MAX_CONNECTIONS ({})",
            cfg.db_min_connections, cfg.db_max_connections
        ));
    }
    if cfg.db_max_connections > 500 {
        report.add_warning(format!(
            "DB_MAX_CONNECTIONS={} is very large — most PostgreSQL deployments support at most 100–200 connections",
            cfg.db_max_connections
        ));
    }
    if cfg.db_idle_timeout_secs == 0 {
        report.add_warning(
            "DB_IDLE_TIMEOUT_SECS=0 disables idle connection recycling",
        );
    }
    if cfg.db_max_lifetime_secs > 0 && cfg.db_max_lifetime_secs < cfg.db_idle_timeout_secs {
        report.add_error(format!(
            "DB_MAX_LIFETIME_SECS ({}) must be ≥ DB_IDLE_TIMEOUT_SECS ({})",
            cfg.db_max_lifetime_secs, cfg.db_idle_timeout_secs
        ));
    }
}

fn check_auth(report: &mut ConfigValidationReport, cfg: &Config) {
    use secrecy::ExposeSecret;

    // Warn about short API keys.
    for key in &cfg.api_keys {
        if key.expose_secret().len() < 32 {
            report.add_warning(
                "API_KEY is shorter than 32 characters — use a cryptographically random key of at least 32 characters",
            );
            break;
        }
    }
    for key in &cfg.admin_api_keys {
        if key.expose_secret().len() < 32 {
            report.add_warning(
                "ADMIN_API_KEY is shorter than 32 characters — admin keys should be strong",
            );
            break;
        }
    }

    // Production without any auth key is suspicious.
    if cfg.environment.is_production_like()
        && cfg.api_keys.is_empty()
        && cfg.admin_api_keys.is_empty()
    {
        report.add_warning(
            "Running in production-like environment without any API_KEY or ADMIN_API_KEY set",
        );
    }
}

fn check_webhook(report: &mut ConfigValidationReport, cfg: &Config) {
    if let Some(ref url) = cfg.webhook_url {
        if url.is_empty() {
            report.add_error("WEBHOOK_URL must not be empty when set");
        } else if cfg.webhook_require_https && url.starts_with("http://") {
            report.add_error(
                "WEBHOOK_URL uses plain HTTP but WEBHOOK_REQUIRE_HTTPS=true",
            );
        } else if cfg.environment.is_production_like() && url.starts_with("http://") {
            report.add_warning(
                "WEBHOOK_URL uses plain HTTP — webhook payloads will not be encrypted in transit",
            );
        }
        if cfg.webhook_secret.is_none() {
            report.add_warning(
                "WEBHOOK_URL is set but WEBHOOK_SECRET is not — webhook payloads will not be HMAC-signed",
            );
        }
    }
}

fn check_bloom_filter(report: &mut ConfigValidationReport, cfg: &Config) {
    if cfg.bloom_filter_fp_rate <= 0.0 || cfg.bloom_filter_fp_rate >= 1.0 {
        report.add_error(format!(
            "BLOOM_FILTER_FP_RATE must be in (0, 1), got {}",
            cfg.bloom_filter_fp_rate
        ));
    }
    if cfg.bloom_filter_capacity == 0 {
        report.add_error("BLOOM_FILTER_CAPACITY must be at least 1");
    }
    if cfg.bloom_filter_fp_rate > 0.01 {
        report.add_warning(format!(
            "BLOOM_FILTER_FP_RATE={} is high (>1%) — increased false-positive rate means more unnecessary DB lookups",
            cfg.bloom_filter_fp_rate
        ));
    }
    // Issue #996: warn when capacity is so large memory could be a concern.
    let estimated_bytes =
        crate::bloom_filter::EventBloomFilter::estimate_memory_bytes(
            cfg.bloom_filter_capacity,
            cfg.bloom_filter_fp_rate,
        );
    if estimated_bytes > 512 * 1024 * 1024 {
        report.add_warning(format!(
            "BLOOM_FILTER_CAPACITY={} with fp_rate={} requires ~{} MiB of memory — consider reducing capacity or enabling filter rotation",
            cfg.bloom_filter_capacity,
            cfg.bloom_filter_fp_rate,
            estimated_bytes / (1024 * 1024)
        ));
    }
}

fn check_sse(report: &mut ConfigValidationReport, cfg: &Config) {
    if cfg.sse_keepalive_interval_ms < 1000 {
        report.add_warning(format!(
            "SSE_KEEPALIVE_SECS < 1 s ({} ms) — may cause excessive network traffic",
            cfg.sse_keepalive_interval_ms
        ));
    }
    if cfg.sse_max_connections == 0 {
        report.add_warning(
            "SSE_MAX_CONNECTIONS=0 is interpreted as unlimited — set an explicit cap for production",
        );
    }
    if cfg.sse_replay_limit > 100_000 {
        report.add_warning(format!(
            "SSE_REPLAY_MAX_EVENTS={} is very large — reconnect replays may consume significant memory and bandwidth",
            cfg.sse_replay_limit
        ));
    }
}

fn check_rate_limits(report: &mut ConfigValidationReport, cfg: &Config) {
    if cfg.rate_limit_per_minute == 0 && cfg.environment.is_production_like() {
        report.add_warning(
            "RATE_LIMIT_PER_MINUTE=0 disables IP-level rate limiting in a production-like environment",
        );
    }
    // Validate per-key limits are consistent.
    if let (Some(per_min), Some(per_hour)) =
        (cfg.rate_limit_key_per_minute, cfg.rate_limit_key_per_hour)
    {
        if per_min as u64 * 60 > per_hour as u64 {
            report.add_warning(format!(
                "RATE_LIMIT_KEY_PER_MINUTE ({per_min}/min × 60 = {}/hr) exceeds RATE_LIMIT_KEY_PER_HOUR ({per_hour}/hr) — the per-hour limit will always be hit first",
                per_min as u64 * 60
            ));
        }
    }
}

fn check_retention(report: &mut ConfigValidationReport, cfg: &Config) {
    if cfg.retention_days == 0 {
        report.add_warning(
            "RETENTION_DAYS=0 disables event pruning — the events table will grow without bound",
        );
    }
    if cfg.retention_days > 3650 {
        report.add_warning(format!(
            "RETENTION_DAYS={} keeps events for more than 10 years — consider a shorter retention window",
            cfg.retention_days
        ));
    }
}

fn check_tls(report: &mut ConfigValidationReport, cfg: &Config) {
    match (&cfg.tls_cert_file, &cfg.tls_key_file) {
        (Some(_), None) => {
            report.add_error("TLS_CERT_FILE is set but TLS_KEY_FILE is missing");
        }
        (None, Some(_)) => {
            report.add_error("TLS_KEY_FILE is set but TLS_CERT_FILE is missing");
        }
        (None, None) if cfg.environment.is_production_like() => {
            report.add_warning(
                "TLS is not configured — use a TLS-terminating reverse proxy in production",
            );
        }
        _ => {}
    }
}

fn check_email(report: &mut ConfigValidationReport, cfg: &Config) {
    if !cfg.email_to.is_empty() {
        if cfg.email_smtp_host.is_none() {
            report.add_error("EMAIL_TO is set but EMAIL_SMTP_HOST is not configured");
        }
        if cfg.email_from.is_none() {
            report.add_warning("EMAIL_TO is set but EMAIL_FROM is not configured — some SMTP servers will reject the message");
        }
    }
    if cfg.email_smtp_port == 0 {
        report.add_error("EMAIL_SMTP_PORT must not be 0");
    }
}

fn check_encryption(report: &mut ConfigValidationReport, cfg: &Config) {
    if cfg.event_data_encryption_key.is_some() && cfg.environment.is_production_like() {
        // Key is present — that's good. Just check rotation key.
        if cfg.event_data_encryption_key_old.is_none() {
            // Not an error, but worth noting for rotation readiness.
        }
    }
    // Nothing to warn about if encryption is off; it's optional.

    // Issue #1162: integration credentials are never stored in plaintext, so
    // the integration key is mandatory in production-like environments.
    if cfg.integration_encryption_key.is_none() {
        if cfg.environment.is_production_like() {
            report.add_error(
                "INTEGRATION_ENCRYPTION_KEY must be set in production: integration credentials \
                 (Slack/Telegram/GitHub tokens, webhook URLs, PagerDuty keys) are encrypted at rest",
            );
        } else {
            report.add_warning(
                "INTEGRATION_ENCRYPTION_KEY is not set: creating Slack/Discord/Telegram/GitHub/PagerDuty \
                 integrations will be rejected",
            );
        }
    }
    if cfg.integration_encryption_key.is_some()
        && cfg.integration_encryption_key == cfg.event_data_encryption_key
    {
        report.add_warning(
            "INTEGRATION_ENCRYPTION_KEY should differ from EVENT_DATA_ENCRYPTION_KEY",
        );
    }
    if cfg.integration_encryption_key_old.is_some() && cfg.integration_encryption_key.is_none() {
        report.add_error("INTEGRATION_ENCRYPTION_KEY_OLD is set but INTEGRATION_ENCRYPTION_KEY is not");
    }
}

fn check_performance_thresholds(report: &mut ConfigValidationReport, cfg: &Config) {
    if cfg.slow_query_threshold_ms == 0 {
        report.add_warning(
            "SLOW_QUERY_THRESHOLD_MS=0 will log every query as slow — set to a reasonable value such as 1000",
        );
    }
    if cfg.indexer_poll_interval_ms < 500 {
        report.add_warning(format!(
            "INDEXER_POLL_INTERVAL_MS={} is very low — this may cause excessive RPC calls",
            cfg.indexer_poll_interval_ms
        ));
    }
}

// ---------------------------------------------------------------------------
// Config schema documentation generator (Issue #997)
// ---------------------------------------------------------------------------

/// A machine-readable description of a single configuration field.
#[derive(Debug, Serialize)]
pub struct ConfigFieldDoc {
    /// Environment variable name.
    pub env_var: &'static str,
    /// Short description.
    pub description: &'static str,
    /// Default value as a string, or `None` if required.
    pub default: Option<&'static str>,
    /// Whether the field must be set for the service to start.
    pub required: bool,
    /// Category for grouping in docs.
    pub category: &'static str,
}

/// Returns documentation for every known configuration field.
///
/// This is used by `make docs-config` to auto-generate the configuration
/// reference table in `docs/configuration-management.md`.
pub fn config_schema() -> Vec<ConfigFieldDoc> {
    vec![
        // ── Database ──────────────────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "DATABASE_URL",
            description: "PostgreSQL connection string",
            default: None,
            required: true,
            category: "database",
        },
        ConfigFieldDoc {
            env_var: "DATABASE_REPLICA_URL",
            description: "Optional read-replica connection string; HTTP handlers use this pool while the indexer uses the primary",
            default: None,
            required: false,
            category: "database",
        },
        ConfigFieldDoc {
            env_var: "DB_MAX_CONNECTIONS",
            description: "Maximum number of connections in the PostgreSQL pool",
            default: Some("10"),
            required: false,
            category: "database",
        },
        ConfigFieldDoc {
            env_var: "DB_MIN_CONNECTIONS",
            description: "Minimum (idle) connections to keep open",
            default: Some("1"),
            required: false,
            category: "database",
        },
        ConfigFieldDoc {
            env_var: "DB_IDLE_TIMEOUT_SECS",
            description: "Seconds before an idle connection is closed and recycled",
            default: Some("600"),
            required: false,
            category: "database",
        },
        ConfigFieldDoc {
            env_var: "DB_MAX_LIFETIME_SECS",
            description: "Maximum age of a connection before it is recycled regardless of activity",
            default: Some("1800"),
            required: false,
            category: "database",
        },
        ConfigFieldDoc {
            env_var: "DB_STATEMENT_TIMEOUT_MS",
            description: "Per-query timeout in milliseconds (SET statement_timeout). 0 disables timeouts",
            default: Some("5000"),
            required: false,
            category: "database",
        },
        // ── RPC ──────────────────────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "STELLAR_RPC_URL",
            description: "Soroban RPC endpoint URL",
            default: Some("https://soroban-testnet.stellar.org"),
            required: false,
            category: "rpc",
        },
        ConfigFieldDoc {
            env_var: "STELLAR_RPC_URLS",
            description: "Comma-separated RPC endpoints for the same network; requests go to the lowest-latency healthy one. Overrides STELLAR_RPC_URL",
            default: None,
            required: false,
            category: "rpc",
        },
        ConfigFieldDoc {
            env_var: "STELLAR_RPC_FALLBACK_URLS",
            description: "Comma-separated fallback RPC URLs tried when the primary fails",
            default: None,
            required: false,
            category: "rpc",
        },
        ConfigFieldDoc {
            env_var: "RPC_CONNECT_TIMEOUT_SECS",
            description: "TCP connect timeout for RPC requests in seconds",
            default: Some("5"),
            required: false,
            category: "rpc",
        },
        ConfigFieldDoc {
            env_var: "RPC_REQUEST_TIMEOUT_SECS",
            description: "Total timeout for a single RPC request in seconds",
            default: Some("30"),
            required: false,
            category: "rpc",
        },
        // ── Auth ──────────────────────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "API_KEY",
            description: "Optional bearer token required for all endpoints except /health. Disabled when unset",
            default: None,
            required: false,
            category: "auth",
        },
        ConfigFieldDoc {
            env_var: "ADMIN_API_KEY",
            description: "Bearer token for /v1/admin/* endpoints, independent of API_KEY",
            default: None,
            required: false,
            category: "auth",
        },
        ConfigFieldDoc {
            env_var: "ADMIN_API_KEY_SECONDARY",
            description: "Secondary admin key for zero-downtime rotation",
            default: None,
            required: false,
            category: "auth",
        },
        // ── Server ───────────────────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "PORT",
            description: "HTTP server listen port",
            default: Some("3000"),
            required: false,
            category: "server",
        },
        ConfigFieldDoc {
            env_var: "RATE_LIMIT_PER_MINUTE",
            description: "Maximum requests per IP per minute. 0 disables IP-level rate limiting",
            default: Some("60"),
            required: false,
            category: "server",
        },
        ConfigFieldDoc {
            env_var: "RUST_LOG",
            description: "Log verbosity filter (trace, debug, info, warn, error)",
            default: Some("info"),
            required: false,
            category: "server",
        },
        ConfigFieldDoc {
            env_var: "RUST_LOG_FORMAT",
            description: "Log output format: text or json",
            default: Some("text"),
            required: false,
            category: "server",
        },
        // ── SSE ──────────────────────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "SSE_KEEPALIVE_SECS",
            description: "Interval in seconds between SSE keep-alive pings",
            default: Some("15"),
            required: false,
            category: "sse",
        },
        ConfigFieldDoc {
            env_var: "SSE_REPLAY_MAX_EVENTS",
            description: "Maximum number of events stored in the in-memory reconnect ring buffer",
            default: Some("1000"),
            required: false,
            category: "sse",
        },
        // ── Indexer ───────────────────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "START_LEDGER",
            description: "Ledger sequence to start indexing from. 0 means latest",
            default: Some("0"),
            required: false,
            category: "indexer",
        },
        ConfigFieldDoc {
            env_var: "INDEXER_LAG_WARN_THRESHOLD",
            description: "Number of ledgers of lag before a warning is emitted",
            default: Some("100"),
            required: false,
            category: "indexer",
        },
        ConfigFieldDoc {
            env_var: "INDEXER_LOCK_RETRY_SECS",
            description: "How often standby replicas retry the advisory lock to become leader",
            default: Some("30"),
            required: false,
            category: "indexer",
        },
        // ── Bloom filter / dedup ──────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "BLOOM_FILTER_CAPACITY",
            description: "Maximum number of events the bloom filter is sized for before rotation (Issue #996)",
            default: Some("1000000"),
            required: false,
            category: "dedup",
        },
        ConfigFieldDoc {
            env_var: "BLOOM_FILTER_FP_RATE",
            description: "Target false-positive rate for the bloom filter (0.0–1.0)",
            default: Some("0.001"),
            required: false,
            category: "dedup",
        },
        // ── Observability ─────────────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "SLOW_QUERY_THRESHOLD_MS",
            description: "Queries exceeding this duration are logged at WARN and counted in metrics",
            default: Some("1000"),
            required: false,
            category: "observability",
        },
        ConfigFieldDoc {
            env_var: "HEALTH_CHECK_TIMEOUT_MS",
            description: "Timeout for the health check DB ping in milliseconds",
            default: Some("2000"),
            required: false,
            category: "observability",
        },
        ConfigFieldDoc {
            env_var: "OTEL_EXPORTER_OTLP_ENDPOINT",
            description: "OpenTelemetry OTLP collector endpoint (requires otel feature build)",
            default: Some("http://localhost:4317"),
            required: false,
            category: "observability",
        },
        // ── Retention ────────────────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "RETENTION_DAYS",
            description: "Number of days to retain events before pruning",
            default: Some("90"),
            required: false,
            category: "retention",
        },
        ConfigFieldDoc {
            env_var: "PRUNING_INTERVAL_HOURS",
            description: "How often the pruner task runs in hours",
            default: Some("24"),
            required: false,
            category: "retention",
        },
        // ── Webhook ──────────────────────────────────────────────────────
        ConfigFieldDoc {
            env_var: "WEBHOOK_URL",
            description: "Destination URL for webhook event notifications",
            default: None,
            required: false,
            category: "webhook",
        },
        ConfigFieldDoc {
            env_var: "WEBHOOK_SECRET",
            description: "HMAC-SHA256 signing secret for webhook payloads",
            default: None,
            required: false,
            category: "webhook",
        },
        ConfigFieldDoc {
            env_var: "WEBHOOK_REQUIRE_HTTPS",
            description: "Reject webhook URLs that use plain HTTP",
            default: Some("false"),
            required: false,
            category: "webhook",
        },
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> Config {
        Config::default()
    }

    #[test]
    fn default_config_passes_validation() {
        let cfg = base_config();
        let report = validate(&cfg);
        // Default config should have no fatal errors.
        assert!(
            report.is_ok(),
            "Default config produced errors: {:?}",
            report.errors
        );
    }

    #[test]
    fn empty_database_url_is_an_error() {
        let mut cfg = base_config();
        cfg.database_url = String::new();
        let report = validate(&cfg);
        assert!(!report.is_ok());
        assert!(report.errors.iter().any(|e| e.contains("DATABASE_URL")));
    }

    #[test]
    fn invalid_database_url_scheme_is_an_error() {
        let mut cfg = base_config();
        cfg.database_url = "mysql://localhost/db".to_string();
        let report = validate(&cfg);
        assert!(!report.is_ok());
        assert!(report.errors.iter().any(|e| e.contains("DATABASE_URL")));
    }

    #[test]
    fn min_connections_exceeding_max_is_an_error() {
        let mut cfg = base_config();
        cfg.db_min_connections = 20;
        cfg.db_max_connections = 10;
        let report = validate(&cfg);
        assert!(!report.is_ok());
        assert!(report.errors.iter().any(|e| e.contains("DB_MIN_CONNECTIONS")));
    }

    #[test]
    fn zero_max_connections_is_an_error() {
        let mut cfg = base_config();
        cfg.db_max_connections = 0;
        let report = validate(&cfg);
        assert!(!report.is_ok());
    }

    #[test]
    fn invalid_bloom_fp_rate_is_an_error() {
        let mut cfg = base_config();
        cfg.bloom_filter_fp_rate = 1.5;
        let report = validate(&cfg);
        assert!(!report.is_ok());
        assert!(report.errors.iter().any(|e| e.contains("BLOOM_FILTER_FP_RATE")));
    }

    #[test]
    fn zero_bloom_capacity_is_an_error() {
        let mut cfg = base_config();
        cfg.bloom_filter_capacity = 0;
        let report = validate(&cfg);
        assert!(!report.is_ok());
    }

    #[test]
    fn invalid_rpc_url_is_an_error() {
        let mut cfg = base_config();
        cfg.stellar_rpc_url = "not-a-url".to_string();
        let report = validate(&cfg);
        assert!(!report.is_ok());
    }

    #[test]
    fn tls_cert_without_key_is_an_error() {
        let mut cfg = base_config();
        cfg.tls_cert_file = Some("/etc/certs/cert.pem".to_string());
        cfg.tls_key_file = None;
        let report = validate(&cfg);
        assert!(!report.is_ok());
        assert!(report.errors.iter().any(|e| e.contains("TLS_CERT_FILE")));
    }

    #[test]
    fn tls_key_without_cert_is_an_error() {
        let mut cfg = base_config();
        cfg.tls_cert_file = None;
        cfg.tls_key_file = Some("/etc/certs/key.pem".to_string());
        let report = validate(&cfg);
        assert!(!report.is_ok());
        assert!(report.errors.iter().any(|e| e.contains("TLS_KEY_FILE")));
    }

    #[test]
    fn http_webhook_url_with_require_https_is_an_error() {
        let mut cfg = base_config();
        cfg.webhook_url = Some("http://example.com/hook".to_string());
        cfg.webhook_require_https = true;
        let report = validate(&cfg);
        assert!(!report.is_ok());
        assert!(report.errors.iter().any(|e| e.contains("WEBHOOK_URL")));
    }

    #[test]
    fn per_key_rate_limit_inconsistency_is_a_warning() {
        let mut cfg = base_config();
        // 100/min × 60 = 6000/hr > 1000/hr — should warn
        cfg.rate_limit_key_per_minute = Some(100);
        cfg.rate_limit_key_per_hour = Some(1000);
        let report = validate(&cfg);
        assert!(report.is_ok(), "should not be a fatal error");
        assert!(!report.warnings.is_empty());
    }

    #[test]
    fn max_lifetime_less_than_idle_timeout_is_an_error() {
        let mut cfg = base_config();
        cfg.db_idle_timeout_secs = 600;
        cfg.db_max_lifetime_secs = 300;
        let report = validate(&cfg);
        assert!(!report.is_ok());
        assert!(report.errors.iter().any(|e| e.contains("DB_MAX_LIFETIME_SECS")));
    }

    #[test]
    fn config_schema_covers_required_fields() {
        let schema = config_schema();
        let has_db_url = schema.iter().any(|f| f.env_var == "DATABASE_URL");
        let has_rpc_url = schema.iter().any(|f| f.env_var == "STELLAR_RPC_URL");
        assert!(has_db_url, "schema must document DATABASE_URL");
        assert!(has_rpc_url, "schema must document STELLAR_RPC_URL");
    }

    #[test]
    fn config_schema_required_fields_have_no_default() {
        let schema = config_schema();
        for field in schema.iter().filter(|f| f.required) {
            assert!(
                field.default.is_none(),
                "required field {} should not have a default",
                field.env_var
            );
        }
    }

    #[test]
    fn report_log_does_not_panic_on_empty_report() {
        let report = ConfigValidationReport::default();
        report.log(); // should not panic
    }
}
