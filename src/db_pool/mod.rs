//! Unified database connection pool (issue #1244).
//!
//! Previously three top-level modules — `connection_pool`, `adaptive_pool`
//! and `pool_management` — each covered part of the pool story, and both
//! `main.rs` and `routes.rs` spawned their own monitor tasks (so every pool
//! metric was emitted twice). They are consolidated here by concern:
//!
//! | Sub-module     | Concern |
//! |----------------|---------|
//! | [`monitor`]    | Utilization sampling, exhaustion alerts, wait-time tracking (`soroban_pulse_db_pool_*` metrics) |
//! | [`adaptive`]   | Sliding-window sizing recommendations, health checks, hot-reloadable tuner config |
//! | [`management`] | Admin API helpers: pool statistics, config validation, tuning guide |
//!
//! [`connect`] is the only place a `PgPool` is configured, and
//! [`DbPool::init`] is the only place the monitors are started. Migrations
//! (with their advisory lock) stay in [`crate::db`].

pub mod adaptive;
pub mod management;
pub mod monitor;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};
use tracing::{info, warn};

pub use adaptive::{AdaptivePoolConfig, AdaptivePoolStatus, AdaptiveTunerState};
pub use monitor::{PoolMonitorConfig, PoolStats};

/// Connection-pool options, resolved once from [`crate::config::Config`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbPoolSettings {
    pub max_connections: u32,
    pub min_connections: u32,
    pub statement_timeout_ms: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
    pub test_before_acquire: bool,
}

impl DbPoolSettings {
    pub fn from_config(config: &crate::config::Config) -> Self {
        Self {
            max_connections: config.db_max_connections,
            min_connections: config.db_min_connections,
            statement_timeout_ms: config.db_statement_timeout_ms,
            idle_timeout_secs: config.db_idle_timeout_secs,
            max_lifetime_secs: config.db_max_lifetime_secs,
            test_before_acquire: config.db_test_before_acquire,
        }
    }
}

impl Default for DbPoolSettings {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_connections: 2,
            statement_timeout_ms: 5000,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1800,
            test_before_acquire: true,
        }
    }
}

/// Open a Postgres pool with `settings`. Every pool in the service — primary,
/// read replica, tooling — is built here so options are never duplicated.
pub async fn connect(database_url: &str, settings: &DbPoolSettings) -> Result<PgPool, sqlx::Error> {
    info!(
        min_connections = settings.min_connections,
        max_connections = settings.max_connections,
        statement_timeout_ms = settings.statement_timeout_ms,
        idle_timeout_secs = settings.idle_timeout_secs,
        max_lifetime_secs = settings.max_lifetime_secs,
        test_before_acquire = settings.test_before_acquire,
        "Configuring Postgres connection pool"
    );

    let statement_timeout_ms = settings.statement_timeout_ms;
    PgPoolOptions::new()
        .max_connections(settings.max_connections)
        .min_connections(settings.min_connections)
        .idle_timeout(Duration::from_secs(settings.idle_timeout_secs))
        .max_lifetime(Duration::from_secs(settings.max_lifetime_secs))
        .test_before_acquire(settings.test_before_acquire)
        .after_connect(move |conn, _| {
            Box::pin(async move {
                conn.execute(format!("SET statement_timeout = '{statement_timeout_ms}ms'").as_str())
                    .await
                    .map(|_| ())
            })
        })
        .connect(database_url)
        .await
}

/// [`connect`], retrying up to `attempts` times with `delay` between tries.
pub async fn connect_with_retry(
    database_url: &str,
    settings: &DbPoolSettings,
    attempts: u32,
    delay: Duration,
) -> Result<PgPool, sqlx::Error> {
    let attempts = attempts.max(1);
    let mut attempt = 1;
    loop {
        match connect(database_url, settings).await {
            Ok(pool) => return Ok(pool),
            Err(e) if attempt < attempts => {
                warn!(attempt, error = %e, "DB connection failed, retrying...");
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Handles to the background pool monitors, shared with HTTP handlers.
#[derive(Clone)]
pub struct PoolMonitors {
    pub stats: Arc<PoolStats>,
    pub adaptive: Arc<AdaptiveTunerState>,
}

impl PoolMonitors {
    fn spawn(pool: &PgPool, settings: &DbPoolSettings) -> Self {
        let stats = monitor::spawn_pool_monitor(
            pool.clone(),
            PoolMonitorConfig {
                max_connections: settings.max_connections,
                min_connections: settings.min_connections,
                ..PoolMonitorConfig::default()
            },
        );
        let adaptive = adaptive::spawn_adaptive_monitor(
            pool.clone(),
            AdaptivePoolConfig::default(),
            settings.max_connections,
            settings.min_connections,
        );
        Self { stats, adaptive }
    }

    /// Monitor state with no background tasks — used when a router is built
    /// without going through [`DbPool::init`] (tests, tooling).
    pub fn detached() -> Self {
        Self {
            stats: PoolStats::new(),
            adaptive: AdaptiveTunerState::detached(AdaptivePoolConfig::default()),
        }
    }
}

static MONITORS: OnceLock<PoolMonitors> = OnceLock::new();

/// The monitors started by [`DbPool::init`], or detached ones if the
/// process never initialised a primary pool.
pub fn monitors() -> PoolMonitors {
    MONITORS.get().cloned().unwrap_or_else(PoolMonitors::detached)
}

/// The primary connection pool plus its monitors.
#[derive(Clone)]
pub struct DbPool {
    pool: PgPool,
    monitors: PoolMonitors,
}

impl DbPool {
    /// The single pool initialisation path: connect (with retries), start the
    /// utilization monitor and adaptive tuner exactly once, and log a startup
    /// snapshot.
    pub async fn init(
        database_url: &str,
        settings: &DbPoolSettings,
        attempts: u32,
        retry_delay: Duration,
    ) -> Result<Self, sqlx::Error> {
        let pool = connect_with_retry(database_url, settings, attempts, retry_delay).await?;
        let monitors = MONITORS
            .get_or_init(|| PoolMonitors::spawn(&pool, settings))
            .clone();
        monitor::log_pool_snapshot(&pool, settings.max_connections);
        Ok(Self { pool, monitors })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn monitors(&self) -> &PoolMonitors {
        &self.monitors
    }

    pub fn into_pool(self) -> PgPool {
        self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_from_config_maps_every_db_field() {
        let config = crate::config::Config {
            db_max_connections: 42,
            db_min_connections: 7,
            db_statement_timeout_ms: 1234,
            db_idle_timeout_secs: 99,
            db_max_lifetime_secs: 999,
            db_test_before_acquire: false,
            ..crate::config::Config::default()
        };
        assert_eq!(
            DbPoolSettings::from_config(&config),
            DbPoolSettings {
                max_connections: 42,
                min_connections: 7,
                statement_timeout_ms: 1234,
                idle_timeout_secs: 99,
                max_lifetime_secs: 999,
                test_before_acquire: false,
            }
        );
    }

    #[test]
    fn monitors_default_to_detached_without_init() {
        let monitors = monitors();
        assert_eq!(monitors.stats.queue_depth(), 0);
        assert!(monitors.adaptive.latest_snapshot().is_none());
    }

    /// Every pool metric is emitted under the `soroban_pulse_db_pool_` prefix.
    #[tokio::test]
    async fn pool_metrics_use_db_pool_prefix() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/test")
            .unwrap();
        ::metrics::with_local_recorder(&recorder, || {
            crate::metrics::update_pool_utilization(&pool, 10);
            crate::metrics::update_pool_queue_depth(0);
            crate::metrics::record_pool_exhaustion_alert();
            crate::metrics::record_pool_acquire_latency(Duration::from_millis(1));
            crate::metrics::record_pool_wait_time(Duration::from_millis(1));
            crate::metrics::record_pool_wait_timeout();
            crate::metrics::update_pool_adaptive_targets(2, 10);
            crate::metrics::record_pool_health_check_failure();
            crate::metrics::record_pool_stale_connections(1);
            crate::metrics::record_pool_acquire_timeout();
        });
        let rendered = handle.render();
        for name in [
            "soroban_pulse_db_pool_utilization",
            "soroban_pulse_db_pool_active_connections",
            "soroban_pulse_db_pool_max_connections",
            "soroban_pulse_db_pool_size",
            "soroban_pulse_db_pool_idle",
            "soroban_pulse_db_pool_max",
            "soroban_pulse_db_pool_queue_depth",
            "soroban_pulse_db_pool_exhaustion_alerts_total",
            "soroban_pulse_db_pool_acquire_latency_seconds",
            "soroban_pulse_db_pool_wait_seconds",
            "soroban_pulse_db_pool_wait_timeout_total",
            "soroban_pulse_db_pool_adaptive_target_min",
            "soroban_pulse_db_pool_adaptive_target_max",
            "soroban_pulse_db_pool_health_check_failures_total",
            "soroban_pulse_db_pool_stale_connections_total",
            "soroban_pulse_db_pool_acquire_timeout_total",
        ] {
            assert!(rendered.contains(name), "{name} missing from:\n{rendered}");
        }
    }

    #[test]
    fn detached_tuner_accepts_config_updates_and_rollback() {
        let monitors = PoolMonitors::detached();
        let updated = AdaptivePoolConfig {
            config_version: 2,
            ..AdaptivePoolConfig::default()
        };
        monitors.adaptive.apply_config(updated).unwrap();
        assert_eq!(monitors.adaptive.current_config().config_version, 2);
        assert_eq!(monitors.adaptive.rollback().unwrap().config_version, 1);
    }
}
