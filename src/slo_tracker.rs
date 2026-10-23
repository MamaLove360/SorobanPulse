// SLI / SLO Tracking (Issue #696)
//
// This module implements the Service Level Indicator (SLI) and Service Level
// Objective (SLO) tracking framework. It is the in-process counterpart of the
// Grafana SLI/SLO dashboard [`docs/sli-slo.md`] and the Prometheus alert rules
// in [`docs/alerts.yml`].
//
// Design overview
// ---------------
// * An **SLO** is a target for an SLI over a rolling window. For example,
//   "99% of HTTP requests in any 30-day window return in <250 ms" is an SLO.
// * An **SLI** is the underlying measurement that we compare against the
//   target (e.g., a histogram of request durations).
// * The **error budget** is `1 - target`. If the SLO target is 99%, the error
//   budget is 1%. The budget is consumed by "bad events" — request latencies
//   above the target, 5xx errors, etc.
// * **Burn rate** is the speed at which we consume the budget. A burn rate of
//   1.0 means we will exhaust the budget exactly at the end of the window if
//   we keep going at the same pace. Burn rates > 1 are an early warning;
//   burn rates > 2 are critical (Google SRE workbook guidance).
//
// The tracker runs in-process. SLI samples are appended by the same code that
// records Prometheus metrics (`record_http_request_duration`, `record_rpc_error`,
// etc.). A periodic background evaluator (every 60 s by default) recomputes
// completion ratio, error budget remaining, and burn rate for every SLO and
// republishes them as gauges, so the Grafana dashboard and Prometheus alerts
// stay aligned with the canonical report exposed at
// `GET /v1/admin/slo/report`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, watch};

extern crate metrics as m;

/// Default evaluation interval for the background SLO task.
pub const DEFAULT_EVAL_INTERVAL_SECS: u64 = 60;

/// Maximum number of SLI samples retained per SLO in memory.
///
/// 65 536 samples is enough for ~18 hours at one sample per second, which is
/// more than enough resolution for the 30-day rolling window because the
/// publish rate is far lower (typically one update per event completion).
pub const MAX_SAMPLES_PER_SLO: usize = 65_536;

/// Type of underlying measurement backing an SLO.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SliType {
    /// Latency-based SLO: sample value is a duration in seconds, target is the
    /// upper bound (e.g., 0.25 for "p99 < 250 ms"). A sample is "good" when
    /// `value <= target`.
    Latency,
    /// Error rate SLO: sample values are success counts. The SLO target is the
    /// success ratio (e.g., 0.99 for "99% success"). A sample is "good" when
    /// `value == 1.0`.
    ErrorRate,
    /// Availability SLO: treated like an error rate where `1.0` means up and
    /// `0.0` means down. Burn rate > 1 means we will starve the budget.
    Availability,
    /// Throughput / saturation SLO: sample value is the measured rate
    /// (events/sec, requests/sec). The target is the ceiling. A sample is
    /// good when `value <= target`.
    ThroughputSaturation,
}

impl SliType {
    /// Convert to a stable string identifier used as a Prometheus label value
    /// and in JSON reports.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SliType::Latency => "latency",
            SliType::ErrorRate => "error_rate",
            SliType::Availability => "availability",
            SliType::ThroughputSaturation => "throughput_saturation",
        }
    }
}

/// Status of a single SLO at the moment the report is generated.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SloStatus {
    /// Completion ratio >= target within the window. Healthy.
    Met,
    /// Completion ratio is below target but above the at-risk threshold and
    /// the burn rate has not exceeded 2×. Worth paying attention to.
    AtRisk,
    /// Completion ratio is below the at-risk threshold, or burn rate > 2,
    /// within the window. Action required.
    Breached,
}

impl SloStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SloStatus::Met => "met",
            SloStatus::AtRisk => "at_risk",
            SloStatus::Breached => "breached",
        }
    }
}

/// Definition of a single SLO.
///
/// `target` is the SLO target expressed as a ratio in `[0.0, 1.0]` for
/// `ErrorRate` / `Availability` / `ThroughputSaturation` or in seconds for
/// `Latency`. The interpretation is decided by `sli_type`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloDefinition {
    /// Stable identifier — used as a Prometheus label and JSON key.
    pub name: String,
    /// Short human-readable description (used in dashboards and alert msgs).
    pub description: String,
    /// Component label (`http`, `indexer`, `replica`, …) used for grouping in
    /// the dashboard.
    pub component: String,
    /// Underlying SLI type.
    pub sli_type: SliType,
    /// Target value. Units depend on `sli_type` (ratio vs. seconds).
    pub target: f64,
    /// Rolling evaluation window in seconds (e.g., 30 days = 2_592_000).
    pub window_secs: u64,
}

impl SloDefinition {
    /// Error budget expressed as a ratio in `[0.0, 1.0]`.
    ///
    /// * `ErrorRate` / `Availability`: the budget is `1 - target`. A 99%
    ///   target gives a 1% error budget.
    /// * `Latency` / `ThroughputSaturation`: the budget side-steps the
    ///   units of `target` entirely. The companion metric in
    ///   [`compute_report_for`](Self::compute_report_for) (private) maps
    ///   each bad sample to a dimensionless excess ratio
    ///   `(value - target) / target` (clamped to zero) and divides the sum
    ///   by the sample count. That ratio is the amount of budget consumed
    ///   in `[0, ∞)`, so by definition the budget matches the denominator
    ///   scale of `1.0`. Returning `1.0` here keeps `condition_ratio / budget`
    ///   well-defined regardless of whether `target` is fractions-of-unity
    ///   (`0.25` for 250 ms latency) or absolute counts (`100` for ledger
    ///   lag).
    #[must_use]
    pub fn error_budget(&self) -> f64 {
        match self.sli_type {
            SliType::ErrorRate | SliType::Availability => (1.0 - self.target).max(0.0),
            SliType::Latency | SliType::ThroughputSaturation => 1.0,
        }
    }

    /// Whether a single SLI sample `value` counts as "good" against this SLO.
    #[must_use]
    pub fn is_good(&self, value: f64) -> bool {
        match self.sli_type {
            SliType::ErrorRate | SliType::Availability => value >= self.target,
            SliType::Latency => value <= self.target,
            SliType::ThroughputSaturation => value <= self.target,
        }
    }
}

/// Single SLI observation recorded against an SLO.
///
/// For an `ErrorRate`/`Availability` SLO, `value` should be `1.0` for "good"
/// and `0.0` for "bad". For a `Latency` SLO, `value` is the request duration
/// in seconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliSample {
    pub timestamp: SystemTime,
    pub value: f64,
}

/// Computed SLO report for a single SLO at a single point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloReport {
    pub name: String,
    pub description: String,
    pub component: String,
    pub sli_type: String,
    pub target: f64,
    pub window_secs: u64,
    /// Fraction of "good" samples in the window, in `[0.0, 1.0]`.
    pub completion_ratio: f64,
    /// Ratio of error budget consumed in this window, in `[0.0, ∞)`. Values > 1
    /// mean the budget was exhausted.
    pub error_budget_consumed: f64,
    /// Ratio of budget remaining in `[0.0, 1.0]`. 1.0 = full, 0.0 = empty.
    pub error_budget_remaining: f64,
    /// Estimated burn rate (good events per window / budget). 1.0 = on track to
    /// exhaust budget exactly at end of window.
    pub burn_rate: f64,
    /// Current health status derived from completion + burn rate.
    pub status: SloStatus,
    /// Number of SLI samples used to compute the report (after window filter).
    pub sample_count: u64,
    /// Number of "good" samples used to compute the report.
    pub good_count: u64,
    /// Last sample value observed (NaN if no samples).
    pub last_sli_value: f64,
    /// Unix timestamp (seconds) when the report was generated.
    pub generated_at: u64,
}

/// Aggregate report covering every tracked SLO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloAggregateReport {
    pub generated_at: u64,
    pub slos: Vec<SloReport>,
    /// Counters grouped by status for quick dashboard rendering.
    pub counts: SloStatusCounts,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SloStatusCounts {
    pub met: usize,
    pub at_risk: usize,
    pub breached: usize,
}

/// In-process tracker for SLOs and SLI samples.
///
/// Thread-safe; the background evaluator holds an `Arc<SloTracker>` so it can
/// recompute reports concurrently with recording.
#[derive(Debug)]
pub struct SloTracker {
    definitions: HashMap<String, SloDefinition>,
    /// SLI samples keyed by SLO name. Newest pushed at the back.
    samples: HashMap<String, VecDeque<SliSample>>,
}

impl SloTracker {
    /// Create a tracker pre-populated with the Soroban Pulse default SLOs.
    #[must_use]
    pub fn with_defaults() -> Self {
        let mut tracker = Self {
            definitions: HashMap::new(),
            samples: HashMap::new(),
        };
        for def in default_slo_definitions() {
            tracker
                samples
                .entry(def.name.clone())
                .or_insert_with(VecDeque::new);
            tracker.definitions.insert(def.name.clone(), def);
        }
        tracker
    }

    /// Create an empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            definitions: HashMap::new(),
            samples: HashMap::new(),
        }
    }

    /// Register a new SLO definition. Returns `true` if a new SLO was added,
    /// `false` if a definition with the same name already existed (in which
    /// case the existing entry is left untouched).
    pub fn register(&mut self, def: SloDefinition) -> bool {
        if self.definitions.contains_key(&def.name) {
            return false;
        }
        self.samples
            .insert(def.name.clone(), VecDeque::new());
        self.definitions.insert(def.name.clone(), def);
        true
    }

    /// Names of every registered SLO.
    #[must_use]
    pub fn slo_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.definitions.keys().cloned().collect();
        names.sort();
        names
    }

    /// Number of registered SLOs.
    #[must_use]
    pub fn definition_count(&self) -> usize {
        self.definitions.len()
    }

    /// Reference to a single SLO definition by name, if it exists.
    #[must_use]
    pub fn definition(&self, name: &str) -> Option<&SloDefinition> {
        self.definitions.get(name)
    }

    /// Append a new SLI sample to the named SLO. Returns `false` if the SLO is
    /// not registered (a misconfiguration). Samples older than the SLO window
    /// are evicted so the report always reflects the rolling window.
    pub fn record_sample(&mut self, slo_name: &str, value: f64) -> bool {
        let Some(def) = self.definitions.get(slo_name) else {
            return false;
        };
        let window = Duration::from_secs(def.window_secs);
        let queue = self
            .samples
            .entry(slo_name.to_string())
            .or_insert_with(VecDeque::new);
        queue.push_back(SliSample {
            timestamp: SystemTime::now(),
            value,
        });
        // Evict samples outside the rolling window.
        let cutoff = SystemTime::now()
            .checked_sub(window)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        while let Some(front) = queue.front() {
            if front.timestamp < cutoff {
                queue.pop_front();
            } else {
                break;
            }
        }
        // Bound memory in case the window is huge and the publish rate is high.
        while queue.len() > MAX_SAMPLES_PER_SLO {
            queue.pop_front();
        }
        true
    }

    /// Number of samples currently retained for the named SLO.
    #[must_use]
    pub fn sample_count(&self, slo_name: &str) -> usize {
        self.samples
            .get(slo_name)
            .map(VecDeque::len)
            .unwrap_or(0)
    }

    /// Compute the canonical report for every registered SLO.
    #[must_use]
    pub fn generate_report(&self) -> SloAggregateReport {
        let now = unix_now_secs();
        let mut slos: Vec<SloReport> = self
            .definitions
            .values()
            .map(|def| self.compute_report_for(def, now))
            .collect();
        // Sort by status severity then by name for stable presentation.
        slos.sort_by(|a, b| {
            status_rank(&b.status)
                .cmp(&status_rank(&a.status))
                .then(a.name.cmp(&b.name))
        });

        let mut counts = SloStatusCounts::default();
        for r in &slos {
            match r.status {
                SloStatus::Met => counts.met += 1,
                SloStatus::AtRisk => counts.at_risk += 1,
                SloStatus::Breached => counts.breached += 1,
            }
        }

        SloAggregateReport {
            generated_at: now,
            slos,
            counts,
        }
    }

    /// Compute a single SLO report (also used by the background task).
    ///
    /// Error-budget semantics depend on the SLI type:
    ///
    /// * `ErrorRate` / `Availability`: bad ratio = `1 - completion_ratio`.
    ///   Error budget = `1 - target`. The reported `error_budget_remaining` is
    ///   the fraction of that budget not yet spent and is in `[0.0, 1.0]`.
    /// * `Latency` / `ThroughputSaturation`: a sample is "good" when its
    ///   value is `<= target`. The bad ratio replaces "off-target events"
    ///   with their normalized excess: `max(value - target, 0) / target`.
    ///   This gives a meaningful burn rate for `target = 0.25` (250 ms) where
    ///   a 500 ms observation is +100% excess, etc.
    fn compute_report_for(&self, def: &SloDefinition, now_secs: u64) -> SloReport {
        let samples = self.samples.get(&def.name);
        let mut total: u64 = 0;
        let mut good: u64 = 0;
        let mut last_value = f64::NAN;
        // Sum of `((bad excess) / target)` for the window. For ErrorRate this
        // reduces to `bad_count / total`; for Latency it accumulates weighted
        // excess so a partial regression costs more than a single dropped
        // sample.
        let mut bad_excess_ratio: f64 = 0.0;

        if let Some(queue) = samples {
            for sample in queue.iter() {
                total += 1;
                if def.is_good(sample.value) {
                    good += 1;
                } else {
                    bad_excess_ratio += excess_ratio(def, sample.value);
                }
                // VecDeque is push-back; the last value is the tail.
                last_value = sample.value;
            }
        }

        let completion_ratio = if total == 0 {
            1.0
        } else {
            good as f64 / total as f64
        };
        let bad_ratio = (1.0 - completion_ratio).max(0.0);

        // `error_budget_consumed` is the share of the error budget already
        // spent, in `[0.0, ∞)`. Values > 1 mean the budget was exhausted.
        let condition_ratio = match def.sli_type {
            SliType::ErrorRate | SliType::Availability => bad_ratio,
            SliType::Latency | SliType::ThroughputSaturation => {
                if total == 0 {
                    0.0
                } else {
                    bad_excess_ratio / total as f64
                }
            }
        };
        let budget = def.error_budget().max(f64::MIN_POSITIVE);
        let error_budget_consumed = condition_ratio / budget;
        let error_budget_remaining = (1.0 - condition_ratio).max(0.0);

        // Burn rate: how fast we are spending the budget. A burn rate of 1
        // means we'll exhaust exactly at the end of the window if the
        // current bad rate persists. Burn rates > 2 are critical.
        let burn_rate = if budget <= 0.0 {
            if condition_ratio > 0.0 {
                f64::INFINITY
            } else {
                0.0
            }
        } else {
            condition_ratio / budget
        };

        let status = classify_status(def.target, completion_ratio, burn_rate);

        SloReport {
            name: def.name.clone(),
            description: def.description.clone(),
            component: def.component.clone(),
            sli_type: def.sli_type.as_str().to_string(),
            target: def.target,
            window_secs: def.window_secs,
            completion_ratio,
            error_budget_consumed,
            error_budget_remaining,
            burn_rate,
            status,
            sample_count: total,
            good_count: good,
            last_sli_value: last_value,
            generated_at: now_secs,
        }
    }

    /// Spawn a background task that periodically recomputes every SLO and
    /// republishes the metrics used by the Grafana dashboard and Prometheus
    /// alert rules. Returns immediately; the task exits on shutdown signal.
    pub fn spawn_evaluator(
        tracker: Arc<RwLock<Self>>,
        mut shutdown_rx: watch::Receiver<bool>,
        interval_secs: u64,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if interval_secs == 0 {
            // Guard against misconfiguration that would pin the CPU.
            return None;
        }
        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Skip the immediate fire; we just published metrics at startup.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let report = {
                            let guard = tracker.read().await;
                            guard.generate_report()
                        };
                        emit_report(&report);

                        let breached_count = report
                            .slos
                            .iter()
                            .filter(|r| r.status == SloStatus::Breached)
                            .count();
                        if breached_count > 0 {
                            tracing::warn!(
                                breached = breached_count,
                                at_risk = report.counts.at_risk,
                                "SLO evaluation: at least one SLO is breached",
                            );
                        } else {
                            tracing::debug!(
                                met = report.counts.met,
                                at_risk = report.counts.at_risk,
                                "SLO evaluation completed",
                            );
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        tracing::debug!("SLO evaluator shutting down");
                        break;
                    }
                }
            }
        }))
    }
}

impl Default for SloTracker {
    fn default() -> Self {
        Self::with_defaults()
    }
}

// ── Built-in SLO definitions ────────────────────────────────────────────────

/// Built-in SLO definitions for the Soroban Pulse managed service. Numbers are
/// aligned with [`docs/api-sla.md`] and the targets in
/// [`docs/alerts.yml`].
///
/// Windows are deliberately shorter than the 30-day SLA for two reasons:
///
/// 1. PromQL / Grafana can downsample these windows into 30-day rollups
///    without additional storage.
/// 2. The in-process tracker keeps at most `MAX_SAMPLES_PER_SLO` samples per
///    SLO; using a 24-hour window keeps memory bounded while still being
///    responsive to regressions.
#[must_use]
pub fn default_slo_definitions() -> Vec<SloDefinition> {
    vec![
        SloDefinition {
            name: "http_availability".to_string(),
            description: "Fraction of HTTP responses that are not 5xx errors".to_string(),
            component: "http".to_string(),
            sli_type: SliType::ErrorRate,
            target: 0.99,
            window_secs: 24 * 60 * 60,
        },
        SloDefinition {
            name: "http_p99_latency".to_string(),
            description: "99% of HTTP requests complete within target latency".to_string(),
            component: "http".to_string(),
            sli_type: SliType::Latency,
            target: 0.25,
            window_secs: 24 * 60 * 60,
        },
        SloDefinition {
            name: "indexer_lag".to_string(),
            description: "Indexer lag measured in ledgers stays within bound".to_string(),
            component: "indexer".to_string(),
            sli_type: SliType::ThroughputSaturation,
            target: 100.0,
            window_secs: 60 * 60,
        },
        SloDefinition {
            name: "webhook_delivery_success".to_string(),
            description: "Webhook deliveries succeed at the documented rate".to_string(),
            component: "webhook".to_string(),
            sli_type: SliType::ErrorRate,
            target: 0.95,
            window_secs: 60 * 60,
        },
        SloDefinition {
            name: "notification_delivery_latency".to_string(),
            description: "Notification p95 delivery latency stays under 30 s".to_string(),
            component: "notification".to_string(),
            sli_type: SliType::Latency,
            target: 30.0,
            window_secs: 60 * 60,
        },
        SloDefinition {
            name: "replica_replay_lag".to_string(),
            description: "Streaming replica replay lag stays under the warning bound".to_string(),
            component: "replica".to_string(),
            sli_type: SliType::ThroughputSaturation,
            target: 30.0,
            window_secs: 60 * 60,
        },
    ]
}

// ── Global accessor ─────────────────────────────────────────────────────────

/// Process-wide shared tracker. Initialized once from `main` so that handlers
/// can fetch the current SLO report without having to plumb an extra field
/// through every layer of [`AppState`](crate::routes::AppState).
static TRACKER: std::sync::OnceLock<SharedTracker> = std::sync::OnceLock::new();

/// Install the process-wide tracker. Should be called exactly once at
/// startup, before the HTTP server begins accepting requests.
///
/// Returns `true` if the tracker was installed, `false` if a tracker was
/// already installed (in which case the existing one is kept).
pub fn install(tracker: SharedTracker) -> bool {
    TRACKER.set(tracker).is_ok()
}

/// Get a clone of the process-wide tracker, if one has been installed.
#[must_use]
pub fn current() -> Option<SharedTracker> {
    TRACKER.get().cloned()
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Compute the rank used for status sorting in [`SloTracker::generate_report`].
fn status_rank(status: &SloStatus) -> u8 {
    match status {
        SloStatus::Breached => 2,
        SloStatus::AtRisk => 1,
        SloStatus::Met => 0,
    }
}

/// Compute the weighted excess ratio for a single "bad" sample.
///
/// * For `Latency` / `ThroughputSaturation`: `(value - target) / target`, clamped
///   at zero. An observation of `target * 3` contributes 2.0 to the
///   accumulated bad ratio; an observation of exactly `target` contributes 0.
/// * For `ErrorRate` / `Availability`: the "excess" is always 1 (a single 5xx or
///   outage minute).
fn excess_ratio(def: &SloDefinition, value: f64) -> f64 {
    match def.sli_type {
        SliType::ErrorRate | SliType::Availability => 1.0,
        SliType::Latency | SliType::ThroughputSaturation => {
            if def.target <= 0.0 {
                return 1.0;
            }
            ((value - def.target) / def.target).max(0.0)
        }
    }
}

/// Decide the [`SloStatus`] from the completion ratio and burn rate.
///
/// The thresholds follow the Google SRE workbook burn-rate alert guidance:
/// burn rates > 2 for any non-trivial window are "page immediately"
/// territory; we surface those as `Breached` regardless of completion
/// ratio because they predict exhaustion within the window.
fn classify_status(_target: f64, completion_ratio: f64, burn_rate: f64) -> SloStatus {
    // The status decision is driven from the unitless `burn_rate` plus an
    // "all-good" lane so it stays correct whether `target` is expressed in
    // fractions (`0.99` availability) or in absolute units (`100` ledger
    // lag). Comparing `completion_ratio` (always `[0, 1]`) directly to such
    // targets either labels every SLO as breached (target > 1) or as `Met`
    // even when a regression is in flight (target < 1).
    if burn_rate.is_infinite() || burn_rate >= 2.0 {
        return SloStatus::Breached;
    }
    if burn_rate >= 1.0 || completion_ratio < 1.0 {
        SloStatus::AtRisk
    } else {
        SloStatus::Met
    }
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Publish every report as Prometheus gauges and counters.
///
/// Naming follows the `soroban_pulse_slo_*` convention used throughout the
/// codebase so the existing Grafana dashboards automatically pick the new
/// panels up (see [`docs/sli-slo.md`]).
fn emit_report(report: &SloAggregateReport) {
    m::gauge!("soroban_pulse_slo_tracked_count").set(report.slos.len() as f64);
    m::gauge!("soroban_pulse_slo_met_count").set(report.counts.met as f64);
    m::gauge!("soroban_pulse_slo_at_risk_count").set(report.counts.at_risk as f64);
    m::gauge!("soroban_pulse_slo_breached_count").set(report.counts.breached as f64);

    for r in &report.slos {
        m::gauge!(
            "soroban_pulse_slo_completion_ratio",
            "slo" => r.name.clone(),
            "component" => r.component.clone()
        )
        .set(r.completion_ratio);
        m::gauge!(
            "soroban_pulse_slo_error_budget_remaining",
            "slo" => r.name.clone(),
            "component" => r.component.clone()
        )
        .set(r.error_budget_remaining);
        m::gauge!(
            "soroban_pulse_slo_error_budget_consumed",
            "slo" => r.name.clone(),
            "component" => r.component.clone()
        )
        .set(r.error_budget_consumed);
        m::gauge!(
            "soroban_pulse_slo_burn_rate",
            "slo" => r.name.clone(),
            "component" => r.component.clone()
        )
        .set(if r.burn_rate.is_finite() {
            r.burn_rate
        } else {
            // Prometheus does not accept NaN/Inf; clamp for visibility.
            100.0
        });
        m::gauge!(
            "soroban_pulse_sli_current_value",
            "slo" => r.name.clone(),
            "component" => r.component.clone()
        )
        .set(if r.last_sli_value.is_finite() {
            r.last_sli_value
        } else {
            0.0
        });
        if r.status != SloStatus::Met {
            m::counter!(
                "soroban_pulse_slo_evaluation_total",
                "slo" => r.name.clone(),
                "status" => r.status.as_str().to_string()
            )
            .increment(1);
        }
    }
}

/// Convenience: shared tracker, usable from both the background task and the
/// HTTP handler layer.
pub type SharedTracker = Arc<RwLock<SloTracker>>;

/// Wrap a [`SloTracker`] in a shared [`SharedTracker`] for use across tasks.
#[must_use]
pub fn shared_tracker() -> SharedTracker {
    Arc::new(RwLock::new(SloTracker::with_defaults()))
}

/// Append an SLI sample to a shared tracker, logging on failure rather than
/// panicking (used from request hot paths where any panicking lock acquisition
/// would be undesirable).
pub async fn record_sli_sample(tracker: &SharedTracker, slo_name: &str, value: f64) {
    let mut guard = tracker.write().await;
    if !guard.record_sample(slo_name, value) {
        tracing::debug!(slo = %slo_name, "record_sli_sample called for unknown SLO");
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn default_tracker_registers_known_slos() {
        let tracker = SloTracker::with_defaults();
        assert!(tracker.definition_count() >= 6);
        assert!(tracker.definition("http_availability").is_some());
        assert!(tracker.definition("indexer_lag").is_some());
    }

    #[test]
    fn record_sample_rejects_unknown_slo() {
        let mut tracker = SloTracker::with_defaults();
        assert!(!tracker.record_sample("does_not_exist", 1.0));
    }

    #[test]
    fn error_rate_slo_classification() {
        let def = SloDefinition {
            name: "test".to_string(),
            description: "test".to_string(),
            component: "test".to_string(),
            sli_type: SliType::ErrorRate,
            target: 0.99,
            window_secs: 3600,
        };
        assert!(def.is_good(1.0));
        assert!(!def.is_good(0.0));
        assert!(approx_eq(def.error_budget(), 0.01));
    }

    #[test]
    fn latency_slo_classification() {
        let def = SloDefinition {
            name: "test".to_string(),
            description: "test".to_string(),
            component: "test".to_string(),
            sli_type: SliType::Latency,
            target: 0.25,
            window_secs: 3600,
        };
        assert!(def.is_good(0.249));
        assert!(!def.is_good(0.251));
    }

    #[test]
    fn classify_status_basic() {
        assert_eq!(classify_status(0.99, 1.0, 0.0), SloStatus::Met);
        assert_eq!(classify_status(0.99, 0.985, 0.5), SloStatus::AtRisk);
        assert_eq!(classify_status(0.99, 0.95, 0.5), SloStatus::Breached);
        assert_eq!(classify_status(0.99, 0.999, 3.0), SloStatus::Breached);
    }

    #[test]
    fn aggregate_report_counts_statuses() {
        let mut tracker = SloTracker::with_defaults();
        // Drive one SLO to breach by submitting a ratio of bad samples.
        for _ in 0..10 {
            tracker.record_sample("http_availability", 0.0);
        }
        // Drive the others with good samples.
        tracker.record_sample("indexer_lag", 10.0);
        tracker.record_sample("webhook_delivery_success", 1.0);
        let report = tracker.generate_report();
        assert!(report.slos.iter().any(|r| r.name == "http_availability"
            && r.status == SloStatus::Breached));
        // Counts must add up to the number of SLOs.
        let total = report.counts.met + report.counts.at_risk + report.counts.breached;
        assert_eq!(total, report.slos.len());
    }

    #[test]
    fn report_includes_descriptions_and_types() {
        let tracker = SloTracker::with_defaults();
        let report = tracker.generate_report();
        for slo in &report.slos {
            assert!(!slo.description.is_empty(), "missing description for {}", slo.name);
            assert!(!slo.sli_type.is_empty(), "missing sli_type for {}", slo.name);
        }
    }

    #[test]
    fn burn_rate_is_finite_when_budget_valid() {
        let mut tracker = SloTracker::with_defaults();
        for _ in 0..5 {
            tracker.record_sample("webhook_delivery_success", 1.0);
        }
        let report = tracker.generate_report();
        let r = report
            .slos
            .iter()
            .find(|r| r.name == "webhook_delivery_success")
            .expect("slo exists");
        assert!(r.burn_rate.is_finite(), "burn rate should never be NaN");
        assert!(r.burn_rate >= 0.0);
    }

    #[tokio::test]
    async fn record_sli_sample_helper_records_correctly() {
        let tracker = shared_tracker();
        record_sli_sample(&tracker, "http_availability", 1.0).await;
        assert_eq!(tracker.read().await.sample_count("http_availability"), 1);
    }
}
