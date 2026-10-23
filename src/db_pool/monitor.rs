//! Issue #622: Connection pool metrics and auto-scaling logic.
//! Issue #995: Connection wait-time tracking and dynamic pool sizing.
//!
//! Monitors database connection pool utilization and emits alerts when the
//! pool approaches exhaustion.  Because `sqlx::PgPool` does not support
//! runtime resizing, auto-scaling is implemented as a recommendation engine:
//! it tracks peak utilization and logs actionable tuning advice so operators
//! can adjust `DB_MAX_CONNECTIONS` / `DB_MIN_CONNECTIONS` at next restart.
//!
//! # Issue #995 enhancements
//! - **Connection wait time tracking**: `acquire_tracked_with_wait` records how
//!   long callers queue for a pool slot (`soroban_pulse_db_pool_wait_seconds`).
//! - **Queue depth gauge**: sampled every monitor tick so operators can see
//!   queueing pressure in real time (`soroban_pulse_db_pool_queue_depth`).
//! - **Wait-timeout counter**: requests that wait >1 s are counted separately
//!   (`soroban_pulse_db_pool_wait_timeout_total`).
//! - **Dynamic sizing guidance**: `suggest_pool_size` computes p99-based min/max
//!   recommendations that appear in the `/v1/admin/pool` response.
//!
//! # Metrics emitted
//! | Name | Kind | Description |
//! |------|------|-------------|
//! | `soroban_pulse_db_pool_utilization` | Gauge | Active / max (0.0–1.0) |
//! | `soroban_pulse_db_pool_active_connections` | Gauge | In-use connections |
//! | `soroban_pulse_db_pool_max_connections` | Gauge | Configured maximum |
//! | `soroban_pulse_db_pool_acquire_latency_seconds` | Histogram | Time to get a connection |
//! | `soroban_pulse_db_pool_exhaustion_alerts_total` | Counter | Times util ≥ 90 % |
//! | `soroban_pulse_db_pool_wait_seconds` | Histogram | Wait time for a pool slot |
//! | `soroban_pulse_db_pool_wait_timeout_total` | Counter | Waits exceeding 1 s |
//! | `soroban_pulse_db_pool_queue_depth` | Gauge | Pending acquisition requests |

use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::metrics;

/// Shared utilization peak tracker.  Updated by the monitor task and read by
/// the `/status` endpoint (future) and the tuning recommender.
#[derive(Debug, Default)]
pub struct PoolStats {
    /// Highest utilization fraction seen since process start (×1000 fixed-point).
    peak_utilization_milli: AtomicU64,
    /// Total number of exhaustion events (util ≥ 90 %).
    exhaustion_event_count: AtomicU64,
    /// Issue #995: current number of callers waiting for a pool slot.
    queue_depth: AtomicUsize,
    /// Issue #995: total number of waits that exceeded 1 s.
    wait_timeout_count: AtomicU64,
    /// Issue #995: sum of all wait times in microseconds (for avg calculation).
    wait_time_sum_us: AtomicU64,
    /// Issue #995: total number of tracked acquisitions.
    wait_sample_count: AtomicU64,
}

impl PoolStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn update(&self, utilization: f64) {
        let milli = (utilization * 1000.0) as u64;
        let _ = self
            .peak_utilization_milli
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
                if milli > prev { Some(milli) } else { None }
            });
    }

    pub fn peak_utilization(&self) -> f64 {
        self.peak_utilization_milli.load(Ordering::Relaxed) as f64 / 1000.0
    }

    pub fn exhaustion_events(&self) -> u64 {
        self.exhaustion_event_count.load(Ordering::Relaxed)
    }

    // ── Issue #995: wait-time helpers ─────────────────────────────────────

    /// Increment the queue depth counter (call before waiting for a connection).
    pub fn enter_queue(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::update_pool_queue_depth(depth);
    }

    /// Decrement the queue depth counter (call after acquiring a connection).
    pub fn leave_queue(&self) {
        let depth = self.queue_depth.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
        metrics::update_pool_queue_depth(depth);
    }

    /// Record a completed acquisition wait with its elapsed duration.
    pub fn record_wait(&self, elapsed: Duration) {
        let us = elapsed.as_micros() as u64;
        self.wait_time_sum_us.fetch_add(us, Ordering::Relaxed);
        self.wait_sample_count.fetch_add(1, Ordering::Relaxed);
        if elapsed.as_secs() >= 1 {
            self.wait_timeout_count.fetch_add(1, Ordering::Relaxed);
            metrics::record_pool_wait_timeout();
        }
        metrics::record_pool_wait_time(elapsed);
    }

    /// Returns the average connection wait time in milliseconds, or `None` if no
    /// samples have been collected yet.
    pub fn avg_wait_ms(&self) -> Option<f64> {
        let count = self.wait_sample_count.load(Ordering::Relaxed);
        if count == 0 {
            return None;
        }
        let sum_us = self.wait_time_sum_us.load(Ordering::Relaxed);
        Some(sum_us as f64 / count as f64 / 1000.0)
    }

    /// Returns the total number of acquisitions that waited >1 s.
    pub fn wait_timeout_count(&self) -> u64 {
        self.wait_timeout_count.load(Ordering::Relaxed)
    }

    /// Returns the current queue depth (callers waiting for a slot).
    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
    }

    // ── Issue #995: dynamic sizing recommendation ─────────────────────────

    /// Compute a recommended `(min_connections, max_connections)` pair based on
    /// observed peak utilization and current pool settings.
    ///
    /// Rules:
    /// - If peak util > 85 %: recommend raising max by 25 % (capped at `ceiling`).
    /// - If peak util < 20 %: recommend lowering min to half (floor 1).
    /// - Otherwise: no change needed.
    pub fn suggest_pool_size(&self, current_max: u32, ceiling: u32) -> PoolSizeSuggestion {
        let peak = self.peak_utilization();
        if peak > 0.85 {
            let suggested_max = ((current_max as f64 * 1.25) as u32).min(ceiling);
            PoolSizeSuggestion {
                suggested_max: Some(suggested_max),
                suggested_min: None,
                reason: format!(
                    "Peak utilization {:.0}% exceeds 85% — raise DB_MAX_CONNECTIONS to {suggested_max}",
                    peak * 100.0
                ),
            }
        } else if peak < 0.20 && current_max > 2 {
            let suggested_min = 1u32;
            PoolSizeSuggestion {
                suggested_max: None,
                suggested_min: Some(suggested_min),
                reason: format!(
                    "Peak utilization {:.0}% is below 20% — reduce DB_MIN_CONNECTIONS to {suggested_min}",
                    peak * 100.0
                ),
            }
        } else {
            PoolSizeSuggestion {
                suggested_max: None,
                suggested_min: None,
                reason: format!(
                    "Pool sizing looks healthy (peak util {:.0}%)",
                    peak * 100.0
                ),
            }
        }
    }
}

/// Recommendation produced by [`PoolStats::suggest_pool_size`].
#[derive(Debug, Clone)]
pub struct PoolSizeSuggestion {
    /// Suggested new `DB_MAX_CONNECTIONS`, or `None` if no increase is needed.
    pub suggested_max: Option<u32>,
    /// Suggested new `DB_MIN_CONNECTIONS`, or `None` if no decrease is needed.
    pub suggested_min: Option<u32>,
    /// Human-readable explanation.
    pub reason: String,
}

/// Configuration for the pool monitor.
#[derive(Debug, Clone)]
pub struct PoolMonitorConfig {
    /// Configured maximum pool size (from `DB_MAX_CONNECTIONS`).
    pub max_connections: u32,
    /// Configured minimum pool size (from `DB_MIN_CONNECTIONS`).
    pub min_connections: u32,
    /// Utilization fraction above which an exhaustion alert is fired (default 0.9).
    pub exhaustion_threshold: f64,
    /// How often to sample and emit metrics (default 15 s).
    pub sample_interval: Duration,
}

impl Default for PoolMonitorConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_connections: 1,
            exhaustion_threshold: 0.9,
            sample_interval: Duration::from_secs(15),
        }
    }
}

/// Spawn a background task that periodically samples pool utilization and
/// emits metrics / warnings.  Returns a handle to the shared [`PoolStats`].
pub fn spawn_pool_monitor(
    pool: PgPool,
    config: PoolMonitorConfig,
) -> Arc<PoolStats> {
    spawn_pool_monitor_with_handle(pool, config).0
}

/// Like [`spawn_pool_monitor`], but also returns the task's `JoinHandle` so
/// it can be registered with the `JobScheduler` (issue #1248).
pub fn spawn_pool_monitor_with_handle(
    pool: PgPool,
    config: PoolMonitorConfig,
) -> (Arc<PoolStats>, tokio::task::JoinHandle<()>) {
    let stats = PoolStats::new();
    let stats_clone = Arc::clone(&stats);

    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.sample_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let size = pool.size();
            let idle = pool.num_idle();
            let active = size.saturating_sub(idle as u32);
            let max = config.max_connections;
            let utilization = if max > 0 { active as f64 / max as f64 } else { 0.0 };

            // Emit Prometheus metrics.
            metrics::update_pool_utilization(&pool, max);

            // Issue #995: emit queue depth.
            metrics::update_pool_queue_depth(stats_clone.queue_depth());

            // Track peak.
            stats_clone.update(utilization);

            // Exhaustion alert.
            if utilization >= config.exhaustion_threshold {
                stats_clone
                    .exhaustion_event_count
                    .fetch_add(1, Ordering::Relaxed);
                metrics::record_pool_exhaustion_alert();

                warn!(
                    utilization = format!("{:.1}%", utilization * 100.0),
                    active_connections = active,
                    max_connections = max,
                    "DB connection pool near exhaustion — consider increasing DB_MAX_CONNECTIONS"
                );
            }

            // Periodic tuning advice (Issue #995: include wait-time info).
            let peak = stats_clone.peak_utilization();
            let suggestion = stats_clone.suggest_pool_size(max, max.saturating_mul(4).max(50));
            if suggestion.suggested_max.is_some() || suggestion.suggested_min.is_some() {
                info!(
                    reason = suggestion.reason.as_str(),
                    suggested_max = ?suggestion.suggested_max,
                    suggested_min = ?suggestion.suggested_min,
                    avg_wait_ms = ?stats_clone.avg_wait_ms(),
                    wait_timeouts = stats_clone.wait_timeout_count(),
                    "Pool tuning recommendation (Issue #995)"
                );
            } else if peak < 0.3 && config.min_connections as f64 > max as f64 * 0.2 {
                info!(
                    peak_utilization = format!("{:.1}%", peak * 100.0),
                    current_min = config.min_connections,
                    suggestion = (config.min_connections / 2).max(1),
                    "Pool utilization is low — you may reduce DB_MIN_CONNECTIONS"
                );
            }
        }
    });

    (stats, handle)
}

/// Acquire a connection from the pool and record the latency.
/// Callers should prefer `pool.acquire()` directly for hot paths; this
/// wrapper is intended for background workers where latency attribution
/// is valuable.
pub async fn acquire_tracked(pool: &PgPool) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, sqlx::Error> {
    let start = Instant::now();
    let conn = pool.acquire().await?;
    let elapsed = start.elapsed();
    metrics::record_pool_acquire_latency(elapsed);
    Ok(conn)
}

/// Issue #995: Acquire a connection and record both acquire latency and queue wait time.
///
/// Unlike [`acquire_tracked`], this variant also increments/decrements the queue
/// depth gauge and records the wait time in the shared [`PoolStats`].
pub async fn acquire_tracked_with_wait(
    pool: &PgPool,
    stats: &Arc<PoolStats>,
) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, sqlx::Error> {
    stats.enter_queue();
    let start = Instant::now();
    let result = pool.acquire().await;
    let elapsed = start.elapsed();
    stats.leave_queue();
    match result {
        Ok(conn) => {
            stats.record_wait(elapsed);
            metrics::record_pool_acquire_latency(elapsed);
            Ok(conn)
        }
        Err(e) => Err(e),
    }
}

/// Emit a snapshot of pool metrics to the log and as structured fields.
/// Useful for startup diagnostics or on-demand admin queries.
pub fn log_pool_snapshot(pool: &PgPool, max_connections: u32) {
    let size = pool.size();
    let idle = pool.num_idle();
    let active = size.saturating_sub(idle as u32);
    let utilization = if max_connections > 0 {
        active as f64 / max_connections as f64
    } else {
        0.0
    };

    info!(
        pool_size = size,
        pool_idle = idle,
        pool_active = active,
        pool_max = max_connections,
        utilization = format!("{:.1}%", utilization * 100.0),
        "DB connection pool snapshot"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_stats_peak_tracks_highest() {
        let stats = PoolStats::default();
        stats.update(0.5);
        stats.update(0.8);
        stats.update(0.6);
        assert!((stats.peak_utilization() - 0.8).abs() < 0.001);
    }

    #[test]
    fn pool_stats_peak_does_not_regress() {
        let stats = PoolStats::default();
        stats.update(0.9);
        stats.update(0.1);
        assert!((stats.peak_utilization() - 0.9).abs() < 0.001);
    }

    #[test]
    fn pool_monitor_config_defaults() {
        let cfg = PoolMonitorConfig::default();
        assert_eq!(cfg.exhaustion_threshold, 0.9);
        assert_eq!(cfg.sample_interval, Duration::from_secs(15));
    }

    #[test]
    fn exhaustion_threshold_boundary() {
        let cfg = PoolMonitorConfig::default();
        // Utilization exactly at threshold should trigger alert.
        assert!(0.9 >= cfg.exhaustion_threshold);
        assert!(0.89 < cfg.exhaustion_threshold);
    }

    // ── Issue #995: wait-time tracking tests ─────────────────────────────

    #[test]
    fn queue_depth_increments_and_decrements() {
        let stats = PoolStats::default();
        assert_eq!(stats.queue_depth(), 0);
        stats.enter_queue();
        stats.enter_queue();
        assert_eq!(stats.queue_depth(), 2);
        stats.leave_queue();
        assert_eq!(stats.queue_depth(), 1);
        stats.leave_queue();
        assert_eq!(stats.queue_depth(), 0);
    }

    #[test]
    fn queue_depth_does_not_underflow() {
        let stats = PoolStats::default();
        // Decrement below zero should saturate to 0.
        stats.leave_queue();
        assert_eq!(stats.queue_depth(), 0);
    }

    #[test]
    fn avg_wait_ms_none_when_no_samples() {
        let stats = PoolStats::default();
        assert!(stats.avg_wait_ms().is_none());
    }

    #[test]
    fn avg_wait_ms_computed_correctly() {
        let stats = PoolStats::default();
        stats.record_wait(Duration::from_millis(100));
        stats.record_wait(Duration::from_millis(300));
        let avg = stats.avg_wait_ms().expect("should have samples");
        assert!((avg - 200.0).abs() < 1.0, "avg should be ~200 ms, got {avg}");
    }

    #[test]
    fn wait_timeout_counted_for_long_waits() {
        let stats = PoolStats::default();
        stats.record_wait(Duration::from_millis(500));
        assert_eq!(stats.wait_timeout_count(), 0);
        stats.record_wait(Duration::from_secs(2));
        assert_eq!(stats.wait_timeout_count(), 1);
    }

    #[test]
    fn suggest_pool_size_scale_up_when_peak_high() {
        let stats = PoolStats::default();
        stats.update(0.90); // peak = 90%
        let suggestion = stats.suggest_pool_size(10, 100);
        assert!(suggestion.suggested_max.is_some(), "should suggest scale-up");
        assert!(suggestion.suggested_max.unwrap() > 10);
    }

    #[test]
    fn suggest_pool_size_scale_down_when_peak_low() {
        let stats = PoolStats::default();
        stats.update(0.05); // peak = 5%
        let suggestion = stats.suggest_pool_size(10, 100);
        assert!(suggestion.suggested_min.is_some(), "should suggest scale-down");
    }

    #[test]
    fn suggest_pool_size_no_action_when_healthy() {
        let stats = PoolStats::default();
        stats.update(0.50); // peak = 50%
        let suggestion = stats.suggest_pool_size(10, 100);
        assert!(suggestion.suggested_max.is_none());
        assert!(suggestion.suggested_min.is_none());
    }

    #[test]
    fn suggest_pool_size_respects_ceiling() {
        let stats = PoolStats::default();
        stats.update(0.99);
        let suggestion = stats.suggest_pool_size(100, 100);
        // Suggested max must not exceed the ceiling.
        if let Some(max) = suggestion.suggested_max {
            assert!(max <= 100);
        }
    }
}
