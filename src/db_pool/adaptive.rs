//! Adaptive pool tuning (Issue #817; moved under `db_pool` by #1244).
//!
//! Implements adaptive pool sizing, advanced metrics, connection health checks,
//! leak detection, and a runtime configuration API for the database connection pool.
//!
//! ## Architecture
//!
//! The adaptive tuner runs as a background task and maintains a sliding window of
//! utilization samples. Based on observed patterns it calculates recommended
//! min/max connection counts and emits those as metrics + log advice so operators
//! can hot-reload config without restarting the service.
//!
//! ## Metrics emitted
//! | Name | Kind | Description |
//! |------|------|-------------|
//! | `soroban_pulse_db_pool_acquire_timeout_total` | Counter | Acquisition timeouts |
//! | `soroban_pulse_db_pool_health_check_failures_total` | Counter | Failed keepalive pings |
//! | `soroban_pulse_db_pool_stale_connections_total` | Counter | Idle connections flagged stale |
//! | `soroban_pulse_db_pool_adaptive_target_min` | Gauge | Recommended min_connections |
//! | `soroban_pulse_db_pool_adaptive_target_max` | Gauge | Recommended max_connections |
//!
//! Queue depth is reported by [`super::monitor`] as `soroban_pulse_db_pool_queue_depth`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::watch;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of utilization samples held in the sliding window.
const WINDOW_SIZE: usize = 60;

/// Fraction of the window that must be above the high-water mark before
/// the tuner recommends increasing max_connections.
const SCALE_UP_FRACTION: f64 = 0.7;

/// Fraction of the window that must be below the low-water mark before
/// the tuner recommends reducing min_connections.
const SCALE_DOWN_FRACTION: f64 = 0.8;

/// Utilization high-water mark for scale-up decisions.
const HIGH_WATER: f64 = 0.75;

/// Utilization low-water mark for scale-down decisions.
const LOW_WATER: f64 = 0.25;

// ---------------------------------------------------------------------------
// Runtime config (hot-reloadable)
// ---------------------------------------------------------------------------

/// Runtime-mutable pool configuration. Changes take effect on the next tuning
/// cycle without restarting the process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptivePoolConfig {
    /// Maximum connections ceiling the tuner may recommend.
    pub max_connections_ceiling: u32,
    /// Minimum connections floor the tuner may recommend.
    pub min_connections_floor: u32,
    /// Enable automatic advisory logging of tuning recommendations.
    pub adaptive_enabled: bool,
    /// Enable periodic connection health checks.
    pub health_checks_enabled: bool,
    /// Interval between health checks.
    pub health_check_interval_secs: u64,
    /// Age (seconds) above which an idle connection is considered stale.
    pub stale_connection_age_secs: u64,
    /// Sampling interval.
    pub sample_interval_secs: u64,
    /// Enable A/B configuration testing (records both configs in metrics).
    pub ab_testing_enabled: bool,
    /// Label for the current configuration variant (used in A/B metrics).
    pub config_variant: String,
    /// Configuration version for rollback tracking.
    pub config_version: u32,
}

impl Default for AdaptivePoolConfig {
    fn default() -> Self {
        Self {
            max_connections_ceiling: 200,
            min_connections_floor: 1,
            adaptive_enabled: true,
            health_checks_enabled: true,
            health_check_interval_secs: 30,
            stale_connection_age_secs: 600,
            sample_interval_secs: 15,
            ab_testing_enabled: false,
            config_variant: "default".to_string(),
            config_version: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// Advanced metrics counters
// ---------------------------------------------------------------------------

/// Atomic counters for advanced pool metrics.
#[derive(Debug, Default)]
pub struct AdvancedPoolCounters {
    /// Total connection acquisition timeouts.
    pub acquire_timeouts: AtomicU64,
    /// Total failed health-check pings.
    pub health_check_failures: AtomicU64,
    /// Total stale connections cleaned up.
    pub stale_cleaned: AtomicU64,
    /// Sum of acquire latencies in microseconds (for average calculation).
    pub acquire_latency_sum_us: AtomicU64,
    /// Number of acquire latency samples.
    pub acquire_latency_count: AtomicU64,
    /// Peak queue depth seen.
    pub peak_queue_depth: AtomicU64,
}

impl AdvancedPoolCounters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record a successful connection acquisition with its latency.
    pub fn record_acquire(&self, latency: Duration) {
        let us = latency.as_micros() as u64;
        self.acquire_latency_sum_us.fetch_add(us, Ordering::Relaxed);
        self.acquire_latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an acquisition timeout.
    pub fn record_timeout(&self) {
        self.acquire_timeouts.fetch_add(1, Ordering::Relaxed);
        crate::metrics::record_pool_acquire_timeout();
    }

    /// Record a health-check failure.
    pub fn record_health_failure(&self) {
        self.health_check_failures.fetch_add(1, Ordering::Relaxed);
        crate::metrics::record_pool_health_check_failure();
    }

    /// Record a stale connection cleanup.
    pub fn record_stale_cleaned(&self) {
        self.stale_cleaned.fetch_add(1, Ordering::Relaxed);
    }

    /// Update peak queue depth if current is higher.
    pub fn update_queue_depth(&self, depth: u64) {
        let _ = self
            .peak_queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
                if depth > prev { Some(depth) } else { None }
            });
    }

    /// Average acquire latency in milliseconds.
    pub fn avg_acquire_latency_ms(&self) -> f64 {
        let count = self.acquire_latency_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let sum_us = self.acquire_latency_sum_us.load(Ordering::Relaxed);
        (sum_us as f64 / count as f64) / 1000.0
    }
}

// ---------------------------------------------------------------------------
// Utilization sliding window
// ---------------------------------------------------------------------------

/// A fixed-size sliding window of utilization samples used for adaptive scaling decisions.
#[derive(Debug)]
struct UtilizationWindow {
    samples: VecDeque<f64>,
    capacity: usize,
}

impl UtilizationWindow {
    fn new(capacity: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, sample: f64) {
        if self.samples.len() >= self.capacity {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    fn fraction_above(&self, threshold: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let above = self.samples.iter().filter(|&&s| s >= threshold).count();
        above as f64 / self.samples.len() as f64
    }

    fn fraction_below(&self, threshold: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let below = self.samples.iter().filter(|&&s| s < threshold).count();
        below as f64 / self.samples.len() as f64
    }

    fn average(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        self.samples.iter().sum::<f64>() / self.samples.len() as f64
    }

    fn is_full(&self) -> bool {
        self.samples.len() >= self.capacity
    }

    fn standard_deviation(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        let avg = self.average();
        let variance = self.samples
            .iter()
            .map(|s| (s - avg).powi(2))
            .sum::<f64>() / (self.samples.len() - 1) as f64;
        variance.sqrt()
    }
}

// ---------------------------------------------------------------------------
// Predictive load analysis
// ---------------------------------------------------------------------------

/// Prediction model using exponential smoothing for connection demand forecasting
#[derive(Debug, Clone)]
struct DemandPredictor {
    /// Exponential smoothing factor (0.1-0.3 typical, higher = more responsive)
    alpha: f64,
    /// Last observed utilization
    last_observation: f64,
    /// Exponentially smoothed level
    level: f64,
    /// Exponentially smoothed trend
    trend: f64,
    /// Trend smoothing factor
    beta: f64,
}

impl DemandPredictor {
    fn new() -> Self {
        Self {
            alpha: 0.2,
            last_observation: 0.0,
            level: 0.0,
            trend: 0.0,
            beta: 0.1,
        }
    }

    /// Update the predictor with a new observation and return predicted next utilization
    fn update(&mut self, observation: f64) -> f64 {
        self.last_observation = observation;

        // Update level
        let new_level = self.alpha * observation + (1.0 - self.alpha) * (self.level + self.trend);
        let new_trend =
            self.beta * (new_level - self.level) + (1.0 - self.beta) * self.trend;

        self.level = new_level;
        self.trend = new_trend;

        // Predict next value
        self.level + self.trend
    }

    /// Get the current prediction for 1 step ahead
    fn predict(&self) -> f64 {
        (self.level + self.trend).min(1.0).max(0.0)
    }
}

// ---------------------------------------------------------------------------
// Adaptive tuner state
// ---------------------------------------------------------------------------

/// Snapshot produced by the adaptive tuner on each cycle.
#[derive(Debug, Clone, Serialize)]
pub struct TuningSnapshot {
    /// Current recommended minimum connections.
    pub recommended_min: u32,
    /// Current recommended maximum connections.
    pub recommended_max: u32,
    /// Average utilization over the sliding window.
    pub avg_utilization: f64,
    /// Whether scale-up is advised.
    pub scale_up_advised: bool,
    /// Whether scale-down is advised.
    pub scale_down_advised: bool,
    /// Current configuration version.
    pub config_version: u32,
    /// Unix timestamp (seconds) of this snapshot.
    pub timestamp_secs: u64,
    /// Predicted next utilization (ML-based forecast)
    pub predicted_utilization: f64,
    /// Standard deviation of recent utilization samples
    pub utilization_std_dev: f64,
}

/// Shared adaptive tuner state, accessible from HTTP handlers.
#[derive(Debug)]
pub struct AdaptiveTunerState {
    /// Latest tuning snapshot. `None` until first cycle completes.
    pub latest_snapshot: RwLock<Option<TuningSnapshot>>,
    /// Advanced counters.
    pub counters: Arc<AdvancedPoolCounters>,
    /// Runtime config sender — write a new config to hot-reload.
    pub config_tx: watch::Sender<AdaptivePoolConfig>,
    /// Config history for rollback (up to 5 versions).
    config_history: RwLock<VecDeque<AdaptivePoolConfig>>,
}

impl AdaptiveTunerState {
    /// Tuner state with no background task attached — for routers built
    /// without [`super::DbPool::init`] (e.g. tests). Config updates are still
    /// stored and readable; there is simply no tuner consuming them.
    pub fn detached(config: AdaptivePoolConfig) -> Arc<Self> {
        Self::new(AdvancedPoolCounters::new(), config).0
    }

    fn new(
        counters: Arc<AdvancedPoolCounters>,
        config: AdaptivePoolConfig,
    ) -> (Arc<Self>, watch::Receiver<AdaptivePoolConfig>) {
        let (tx, rx) = watch::channel(config);
        let state = Arc::new(Self {
            latest_snapshot: RwLock::new(None),
            counters,
            config_tx: tx,
            config_history: RwLock::new(VecDeque::with_capacity(5)),
        });
        (state, rx)
    }

    /// Apply a new runtime configuration (hot-reload). Saves current config to
    /// history before applying so it can be rolled back.
    pub fn apply_config(&self, new_config: AdaptivePoolConfig) -> Result<(), String> {
        // Save current to history.
        {
            let current = self.config_tx.borrow().clone();
            let mut history = self
                .config_history
                .write()
                .map_err(|e| format!("lock poisoned: {e}"))?;
            if history.len() >= 5 {
                history.pop_front();
            }
            history.push_back(current);
        }
        // `send_replace` stores the value even when the tuner task is not
        // running (detached state), unlike `send`.
        self.config_tx.send_replace(new_config);
        Ok(())
    }

    /// Roll back to the previous configuration version.
    pub fn rollback(&self) -> Result<AdaptivePoolConfig, String> {
        let prev = {
            let mut history = self
                .config_history
                .write()
                .map_err(|e| format!("lock poisoned: {e}"))?;
            history
                .pop_back()
                .ok_or_else(|| "no previous configuration to roll back to".to_string())?
        };
        self.config_tx.send_replace(prev.clone());
        Ok(prev)
    }

    /// Return the current runtime configuration.
    pub fn current_config(&self) -> AdaptivePoolConfig {
        self.config_tx.borrow().clone()
    }

    /// Return the latest tuning snapshot, if available.
    pub fn latest_snapshot(&self) -> Option<TuningSnapshot> {
        self.latest_snapshot.read().ok()?.clone()
    }

    fn store_snapshot(&self, snapshot: TuningSnapshot) {
        if let Ok(mut guard) = self.latest_snapshot.write() {
            *guard = Some(snapshot);
        }
    }
}

// ---------------------------------------------------------------------------
// Spawn adaptive monitor
// ---------------------------------------------------------------------------

/// Spawn the adaptive pool monitor background task.
///
/// Returns an `Arc<AdaptiveTunerState>` that handlers can use to query the
/// latest tuning recommendation and to hot-reload configuration at runtime.
pub fn spawn_adaptive_monitor(
    pool: PgPool,
    initial_config: AdaptivePoolConfig,
    current_max: u32,
    current_min: u32,
) -> Arc<AdaptiveTunerState> {
    let counters = AdvancedPoolCounters::new();
    let (state, mut config_rx) = AdaptiveTunerState::new(Arc::clone(&counters), initial_config);
    let state_clone = Arc::clone(&state);

    tokio::spawn(async move {
        let mut window = UtilizationWindow::new(WINDOW_SIZE);
        let mut last_health_check = Instant::now();
        let mut recommended_min = current_min;
        let mut recommended_max = current_max;
        let mut demand_predictor = DemandPredictor::new();

        loop {
            // Reload config if it has changed.
            let config = config_rx.borrow().clone();
            let interval = Duration::from_secs(config.sample_interval_secs.max(1));

            tokio::time::sleep(interval).await;

            // Re-borrow after sleeping (might have changed).
            let config = config_rx.borrow_and_update().clone();

            // --- Sample pool utilization ----------------------------------------
            let size = pool.size();
            let idle = pool.num_idle();
            let active = size.saturating_sub(idle as u32);
            let utilization = if current_max > 0 {
                active as f64 / current_max as f64
            } else {
                0.0
            };

            window.push(utilization);

            // Approximate queue depth as connections beyond idle but approaching max.
            let queue_depth = if active as f64 > current_max as f64 * 0.9 {
                (active as f64 - current_max as f64 * 0.9).max(0.0) as u64
            } else {
                0
            };
            counters.update_queue_depth(queue_depth);

            // --- Adaptive sizing recommendations ---------------------------------
            let avg_util = window.average();
            let scale_up = config.adaptive_enabled
                && window.is_full()
                && window.fraction_above(HIGH_WATER) >= SCALE_UP_FRACTION;
            let scale_down = config.adaptive_enabled
                && window.is_full()
                && window.fraction_below(LOW_WATER) >= SCALE_DOWN_FRACTION;

            if scale_up {
                let new_max = (recommended_max as f64 * 1.25)
                    .ceil() as u32
                    .min(config.max_connections_ceiling);
                if new_max > recommended_max {
                    recommended_max = new_max;
                    warn!(
                        recommended_max,
                        avg_utilization = format!("{:.1}%", avg_util * 100.0),
                        config_variant = %config.config_variant,
                        "Adaptive tuner: scale-up recommended — restart with DB_MAX_CONNECTIONS={recommended_max}"
                    );
                }
            } else if scale_down {
                let new_min = ((recommended_min as f64 * 0.75).floor() as u32)
                    .max(config.min_connections_floor);
                if new_min < recommended_min {
                    recommended_min = new_min;
                    info!(
                        recommended_min,
                        avg_utilization = format!("{:.1}%", avg_util * 100.0),
                        config_variant = %config.config_variant,
                        "Adaptive tuner: scale-down recommended — restart with DB_MIN_CONNECTIONS={recommended_min}"
                    );
                }
            }

            // --- Health checks ---------------------------------------------------
            if config.health_checks_enabled
                && last_health_check.elapsed()
                    >= Duration::from_secs(config.health_check_interval_secs)
            {
                last_health_check = Instant::now();
                match sqlx::query("SELECT 1").execute(&pool).await {
                    Ok(_) => {
                        debug!("Pool health check passed");
                    }
                    Err(e) => {
                        counters.record_health_failure();
                        warn!(error = %e, "Pool health check failed — connection may be stale");
                    }
                }
            }

            // --- Stale connection tracking (advisory) ----------------------------
            // SQLx recycles connections automatically, but we track the configured
            // idle timeout as a proxy for stale connection detection.
            let stale_threshold = Duration::from_secs(config.stale_connection_age_secs);
            let since_epoch = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or(Duration::ZERO);
            // A proxy: if pool has more idle connections than min and they've been
            // idle for longer than the threshold, count them as "potentially stale".
            if idle as u32 > current_min
                && since_epoch.as_secs() % config.stale_connection_age_secs.max(1) < interval.as_secs()
            {
                let stale_estimate = idle as u32 - current_min;
                if stale_estimate > 0 {
                    counters
                        .stale_cleaned
                        .fetch_add(stale_estimate as u64, Ordering::Relaxed);
                    crate::metrics::record_pool_stale_connections(u64::from(stale_estimate));
                    debug!(
                        stale_estimate,
                        "Pool stale connection advisory: {} connections may benefit from cleanup",
                        stale_estimate
                    );
                }
            }

            // --- Publish snapshot ------------------------------------------------
            let predicted_util = demand_predictor.update(utilization);
            let std_dev = window.standard_deviation();
            let snapshot = TuningSnapshot {
                recommended_min,
                recommended_max,
                avg_utilization: avg_util,
                scale_up_advised: scale_up,
                scale_down_advised: scale_down,
                config_version: config.config_version,
                timestamp_secs: since_epoch.as_secs(),
                predicted_utilization: predicted_util,
                utilization_std_dev: std_dev,
            };
            state_clone.store_snapshot(snapshot);
            crate::metrics::update_pool_adaptive_targets(recommended_min, recommended_max);

            debug!(
                utilization = format!("{:.1}%", utilization * 100.0),
                avg_utilization = format!("{:.1}%", avg_util * 100.0),
                recommended_min,
                recommended_max,
                "Adaptive pool tuner cycle complete"
            );
        }
    });

    state
}

// ---------------------------------------------------------------------------
// Acquire with timeout tracking
// ---------------------------------------------------------------------------

/// Acquire a pool connection, recording latency and timeout events.
///
/// Returns the connection on success, or an error if the pool times out.
/// Use this wrapper in background workers where detailed latency attribution
/// is needed.
pub async fn acquire_with_tracking(
    pool: &PgPool,
    counters: &Arc<AdvancedPoolCounters>,
    timeout: Duration,
) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, sqlx::Error> {
    let start = Instant::now();
    let result = tokio::time::timeout(timeout, pool.acquire()).await;

    match result {
        Ok(Ok(conn)) => {
            counters.record_acquire(start.elapsed());
            Ok(conn)
        }
        Ok(Err(e)) => {
            counters.record_acquire(start.elapsed());
            Err(e)
        }
        Err(_elapsed) => {
            counters.record_timeout();
            Err(sqlx::Error::PoolTimedOut)
        }
    }
}

// ---------------------------------------------------------------------------
// API response types
// ---------------------------------------------------------------------------

/// Response body for `GET /v1/admin/pool-config/adaptive`.
#[derive(Debug, Serialize)]
pub struct AdaptivePoolStatus {
    pub latest_snapshot: Option<TuningSnapshot>,
    pub current_config: AdaptivePoolConfig,
    pub acquire_timeouts_total: u64,
    pub health_check_failures_total: u64,
    pub stale_cleaned_total: u64,
    pub avg_acquire_latency_ms: f64,
    pub peak_queue_depth: u64,
}

impl AdaptivePoolStatus {
    pub fn from_state(state: &AdaptiveTunerState) -> Self {
        let c = &state.counters;
        Self {
            latest_snapshot: state.latest_snapshot(),
            current_config: state.current_config(),
            acquire_timeouts_total: c.acquire_timeouts.load(Ordering::Relaxed),
            health_check_failures_total: c.health_check_failures.load(Ordering::Relaxed),
            stale_cleaned_total: c.stale_cleaned.load(Ordering::Relaxed),
            avg_acquire_latency_ms: c.avg_acquire_latency_ms(),
            peak_queue_depth: c.peak_queue_depth.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utilization_window_fraction_above() {
        let mut w = UtilizationWindow::new(10);
        for v in [0.1, 0.2, 0.8, 0.9, 0.95] {
            w.push(v);
        }
        // 3 of 5 samples (0.8, 0.9, 0.95) are >= 0.75
        assert!((w.fraction_above(0.75) - 0.6).abs() < 0.01);
    }

    #[test]
    fn utilization_window_fraction_below() {
        let mut w = UtilizationWindow::new(10);
        for v in [0.1, 0.2, 0.3, 0.8, 0.9] {
            w.push(v);
        }
        // 3 of 5 (0.1, 0.2, 0.3) are below 0.4
        assert!((w.fraction_below(0.4) - 0.6).abs() < 0.01);
    }

    #[test]
    fn utilization_window_eviction() {
        let mut w = UtilizationWindow::new(3);
        w.push(0.1);
        w.push(0.2);
        w.push(0.3);
        w.push(0.4); // evicts 0.1
        assert_eq!(w.samples.len(), 3);
        assert!(!w.samples.contains(&0.1f64));
        assert!(w.samples.contains(&0.4f64));
    }

    #[test]
    fn utilization_window_average() {
        let mut w = UtilizationWindow::new(5);
        for v in [0.2, 0.4, 0.6] {
            w.push(v);
        }
        assert!((w.average() - 0.4).abs() < 0.001);
    }

    #[test]
    fn advanced_counters_avg_latency() {
        let c = AdvancedPoolCounters::default();
        c.record_acquire(Duration::from_millis(10));
        c.record_acquire(Duration::from_millis(30));
        // Average should be 20ms
        assert!((c.avg_acquire_latency_ms() - 20.0).abs() < 0.1);
    }

    #[test]
    fn advanced_counters_timeout() {
        let c = AdvancedPoolCounters::default();
        c.record_timeout();
        c.record_timeout();
        assert_eq!(c.acquire_timeouts.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn advanced_counters_peak_queue_depth() {
        let c = AdvancedPoolCounters::default();
        c.update_queue_depth(5);
        c.update_queue_depth(10);
        c.update_queue_depth(3);
        assert_eq!(c.peak_queue_depth.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn adaptive_pool_config_defaults() {
        let cfg = AdaptivePoolConfig::default();
        assert!(cfg.adaptive_enabled);
        assert!(cfg.health_checks_enabled);
        assert_eq!(cfg.config_version, 1);
    }

    #[tokio::test]
    async fn adaptive_tuner_state_config_rollback() {
        let initial = AdaptivePoolConfig {
            config_version: 1,
            config_variant: "v1".to_string(),
            ..Default::default()
        };
        let counters = AdvancedPoolCounters::new();
        let (state, _rx) = AdaptiveTunerState::new(counters, initial);

        let v2 = AdaptivePoolConfig {
            config_version: 2,
            config_variant: "v2".to_string(),
            ..Default::default()
        };
        state.apply_config(v2).unwrap();
        assert_eq!(state.current_config().config_version, 2);

        let rolled_back = state.rollback().unwrap();
        assert_eq!(rolled_back.config_version, 1);
        assert_eq!(state.current_config().config_version, 1);
    }

    #[tokio::test]
    async fn adaptive_tuner_state_rollback_empty() {
        let initial = AdaptivePoolConfig::default();
        let counters = AdvancedPoolCounters::new();
        let (state, _rx) = AdaptiveTunerState::new(counters, initial);
        // No history yet — rollback should fail gracefully.
        assert!(state.rollback().is_err());
    }

    #[test]
    fn adaptive_pool_status_from_state() {
        let config = AdaptivePoolConfig::default();
        let counters = AdvancedPoolCounters::new();
        let (state, _rx) = AdaptiveTunerState::new(Arc::clone(&counters), config);
        counters.record_timeout();
        counters.record_health_failure();

        let status = AdaptivePoolStatus::from_state(&state);
        assert_eq!(status.acquire_timeouts_total, 1);
        assert_eq!(status.health_check_failures_total, 1);
    }
}
