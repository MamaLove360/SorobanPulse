use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

const DEFAULT_ERROR_RATE_WINDOW_SECS: u64 = 300;
const DEFAULT_ROLLBACK_THRESHOLD: f64 = 0.05;

/// Feature flag context for evaluation
#[derive(Clone, Debug)]
pub struct FeatureFlagContext {
    pub contract_id: Option<String>,
    pub user_id: Option<String>,
    pub ip_address: Option<String>,
    pub region: Option<String>,
}

pub struct FeatureFlagWatcher {
    pool: PgPool,
    window_secs: u64,
    rollback_threshold: f64,
}

impl FeatureFlagWatcher {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            window_secs: DEFAULT_ERROR_RATE_WINDOW_SECS,
            rollback_threshold: DEFAULT_ROLLBACK_THRESHOLD,
        }
    }

    async fn current_error_rate(&self) -> Option<f64> {
        let row: Option<(i64, i64)> = sqlx::query_as(
            "SELECT
                SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END)::bigint,
                COUNT(*)::bigint
             FROM request_logs
             WHERE created_at > NOW() - ($1 || ' seconds')::interval",
        )
        .bind(self.window_secs as i64)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();

        row.and_then(|(errors, total)| {
            if total == 0 {
                None
            } else {
                Some(errors as f64 / total as f64)
            }
        })
    }

    async fn rollback_enabled_flags(&self, error_rate: f64) {
        let flags: Vec<(uuid::Uuid, String)> = match sqlx::query_as(
            "SELECT id, name FROM feature_flags WHERE enabled = TRUE AND auto_rollback = TRUE",
        )
        .fetch_all(&self.pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to fetch feature flags for rollback check");
                return;
            }
        };

        for (id, name) in flags {
            if let Err(e) = sqlx::query(
                "UPDATE feature_flags SET enabled = FALSE, updated_at = NOW() WHERE id = $1",
            )
            .bind(id)
            .execute(&self.pool)
            .await
            {
                tracing::warn!(flag_id = %id, error = %e, "Failed to rollback feature flag");
                continue;
            }

            tracing::warn!(
                flag_name = %name,
                flag_id = %id,
                error_rate = error_rate,
                threshold = self.rollback_threshold,
                "Feature flag auto-rolled back due to error rate spike",
            );
            crate::metrics::record_feature_flag_rollback(&name);

            let _ = sqlx::query(
                "INSERT INTO feature_flag_audit (flag_id, action, reason, triggered_by)
                 VALUES ($1, 'rollback', $2, 'auto-rollback')",
            )
            .bind(id)
            .bind(format!(
                "Auto-rollback: error rate {:.2}% exceeded threshold {:.2}%",
                error_rate * 100.0,
                self.rollback_threshold * 100.0
            ))
            .execute(&self.pool)
            .await;
        }
    }

    pub async fn run_once(&self) {
        if let Some(rate) = self.current_error_rate().await {
            extern crate metrics as m;
            m::gauge!("soroban_pulse_feature_flag_error_rate").set(rate);

            if rate > self.rollback_threshold {
                self.rollback_enabled_flags(rate).await;
            }
        }
    }
}

/// Evaluate whether a feature flag should be enabled for a given context
pub async fn is_feature_enabled(
    pool: &PgPool,
    flag_name: &str,
    context: &FeatureFlagContext,
) -> Result<bool, sqlx::Error> {
    let row: Option<(bool, i32, Option<Vec<String>>, Option<Vec<String>>, Option<Vec<String>>, Option<Vec<String>>)> =
        sqlx::query_as(
            "SELECT enabled, rollout_percentage, target_contract_ids, target_user_ids, target_ips, target_regions
             FROM feature_flags WHERE name = $1",
        )
        .bind(flag_name)
        .fetch_optional(pool)
        .await?;

    match row {
        None => Ok(false), // Feature flag doesn't exist
        Some((false, _, _, _, _, _)) => Ok(false), // Feature flag is disabled globally
        Some((true, rollout_pct, target_contracts, target_users, target_ips, target_regions)) => {
            // Check targeting: if any targeting rules are set, require a match
            let has_targeting = target_contracts.is_some() || target_users.is_some() || target_ips.is_some() || target_regions.is_some();
            
            if has_targeting {
                let mut target_matched = false;

                if let Some(ref contracts) = target_contracts {
                    if let Some(ref contract_id) = context.contract_id {
                        if contracts.contains(contract_id) {
                            target_matched = true;
                        }
                    }
                }

                if let Some(ref users) = target_users {
                    if let Some(ref user_id) = context.user_id {
                        if users.contains(user_id) {
                            target_matched = true;
                        }
                    }
                }

                if let Some(ref ips) = target_ips {
                    if let Some(ref ip) = context.ip_address {
                        if ips.contains(ip) {
                            target_matched = true;
                        }
                    }
                }

                if let Some(ref regions) = target_regions {
                    if let Some(ref region) = context.region {
                        if regions.contains(region) {
                            target_matched = true;
                        }
                    }
                }

                if !target_matched {
                    return Ok(false);
                }
            }

            // Check percentage-based rollout
            let hash = compute_rollout_hash(flag_name, context);
            let bucket = (hash % 100) as i32;
            Ok(bucket < rollout_pct)
        }
    }
}

/// Compute a deterministic hash for percentage-based rollout
fn compute_rollout_hash(flag_name: &str, context: &FeatureFlagContext) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    flag_name.hash(&mut hasher);

    // Use contract ID as the primary targeting identifier for rollout consistency
    if let Some(ref contract_id) = context.contract_id {
        contract_id.hash(&mut hasher);
    } else if let Some(ref user_id) = context.user_id {
        user_id.hash(&mut hasher);
    } else if let Some(ref ip) = context.ip_address {
        ip.hash(&mut hasher);
    }

    hasher.finish()
}

/// A single named variant in an A/B (or A/B/n) test, with a relative weight
/// used for proportional bucketing. Weights need not sum to 100; they are
/// normalized at assignment time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlagVariant {
    pub name: String,
    pub weight: u32,
}

/// Deterministically assigns a context to one of the given variants based on
/// the same hash used for percentage rollout, so a given user/contract always
/// lands in the same variant for a given flag.
pub fn assign_variant<'a>(
    flag_name: &str,
    context: &FeatureFlagContext,
    variants: &'a [FlagVariant],
) -> Option<&'a FlagVariant> {
    if variants.is_empty() {
        return None;
    }
    let total_weight: u32 = variants.iter().map(|v| v.weight).sum();
    if total_weight == 0 {
        return None;
    }

    let hash = compute_rollout_hash(flag_name, context);
    let mut bucket = (hash % total_weight as u64) as u32;

    for variant in variants {
        if bucket < variant.weight {
            return Some(variant);
        }
        bucket -= variant.weight;
    }
    variants.last()
}

/// Per-flag evaluation counters used to power flag metrics/dashboards.
#[derive(Debug, Default)]
struct FlagCounters {
    evaluations: AtomicU64,
    enabled: AtomicU64,
    disabled: AtomicU64,
    variant_assignments: std::sync::Mutex<HashMap<String, u64>>,
}

/// Tracks how often each feature flag is evaluated, its enabled/disabled
/// split, and A/B variant distribution.
#[derive(Debug, Default)]
pub struct FlagMetrics {
    counters: std::sync::Mutex<HashMap<String, Arc<FlagCounters>>>,
}

impl FlagMetrics {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn counters_for(&self, flag_name: &str) -> Arc<FlagCounters> {
        let mut guard = self.counters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .entry(flag_name.to_string())
            .or_insert_with(|| Arc::new(FlagCounters::default()))
            .clone()
    }

    pub fn record_evaluation(&self, flag_name: &str, enabled: bool) {
        let counters = self.counters_for(flag_name);
        counters.evaluations.fetch_add(1, Ordering::Relaxed);
        if enabled {
            counters.enabled.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.disabled.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_variant_assignment(&self, flag_name: &str, variant_name: &str) {
        let counters = self.counters_for(flag_name);
        let mut variants = counters.variant_assignments.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *variants.entry(variant_name.to_string()).or_insert(0) += 1;
    }

    /// Snapshot of evaluation counts and enabled-rate per flag, for a metrics endpoint.
    pub fn snapshot(&self) -> Vec<FlagMetricsSnapshot> {
        let guard = self.counters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .iter()
            .map(|(name, counters)| {
                let evaluations = counters.evaluations.load(Ordering::Relaxed);
                let enabled = counters.enabled.load(Ordering::Relaxed);
                let disabled = counters.disabled.load(Ordering::Relaxed);
                let variants = counters.variant_assignments.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
                FlagMetricsSnapshot {
                    flag_name: name.clone(),
                    evaluations,
                    enabled,
                    disabled,
                    enabled_rate: if evaluations == 0 {
                        0.0
                    } else {
                        enabled as f64 / evaluations as f64
                    },
                    variant_assignments: variants,
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlagMetricsSnapshot {
    pub flag_name: String,
    pub evaluations: u64,
    pub enabled: u64,
    pub disabled: u64,
    pub enabled_rate: f64,
    pub variant_assignments: HashMap<String, u64>,
}

/// Same as `is_feature_enabled`, but also records the outcome into `metrics`
/// so evaluation counts and enabled rates can be exposed on a flag dashboard.
pub async fn is_feature_enabled_with_metrics(
    pool: &PgPool,
    flag_name: &str,
    context: &FeatureFlagContext,
    metrics: &FlagMetrics,
) -> Result<bool, sqlx::Error> {
    let result = is_feature_enabled(pool, flag_name, context).await?;
    metrics.record_evaluation(flag_name, result);
    Ok(result)
}

pub fn spawn(pool: PgPool, interval_secs: u64, mut shutdown_rx: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let watcher = FeatureFlagWatcher::new(pool);
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    watcher.run_once().await;
                }
                _ = shutdown_rx.changed() => {
                    tracing::debug!("Feature flag watcher shutting down");
                    break;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_threshold_is_five_percent() {
        assert!((DEFAULT_ROLLBACK_THRESHOLD - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn default_window_is_five_minutes() {
        assert_eq!(DEFAULT_ERROR_RATE_WINDOW_SECS, 300);
    }

    #[test]
    fn error_rate_zero_total_returns_none() {
        let rate: Option<f64> = {
            let total: i64 = 0;
            if total == 0 { None } else { Some(0.0) }
        };
        assert!(rate.is_none());
    }

    #[test]
    fn error_rate_calculation() {
        let errors: i64 = 10;
        let total: i64 = 100;
        let rate = errors as f64 / total as f64;
        assert!((rate - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn rollout_hash_consistent() {
        let context = FeatureFlagContext {
            contract_id: Some("CABC123".to_string()),
            user_id: None,
            ip_address: None,
            region: None,
        };
        let hash1 = compute_rollout_hash("my-flag", &context);
        let hash2 = compute_rollout_hash("my-flag", &context);
        assert_eq!(hash1, hash2, "Hash should be deterministic");
    }

    #[test]
    fn rollout_hash_differs_by_flag() {
        let context = FeatureFlagContext {
            contract_id: Some("CABC123".to_string()),
            user_id: None,
            ip_address: None,
            region: None,
        };
        let hash1 = compute_rollout_hash("flag-a", &context);
        let hash2 = compute_rollout_hash("flag-b", &context);
        assert_ne!(hash1, hash2, "Hash should differ by flag name");
    }

    #[test]
    fn rollout_percentage_distribution() {
        // Verify that the rollout hash distribution is roughly uniform
        let mut context = FeatureFlagContext {
            contract_id: Some("contract-1".to_string()),
            user_id: None,
            ip_address: None,
            region: None,
        };

        let mut enabled_count = 0;
        let total = 1000;
        for i in 0..total {
            context.contract_id = Some(format!("contract-{}", i));
            let hash = compute_rollout_hash("test-flag", &context);
            let bucket = (hash % 100) as i32;
            if bucket < 50 { // 50% rollout
                enabled_count += 1;
            }
        }

        // Should be close to 50% (allow 10% variance)
        let ratio = enabled_count as f64 / total as f64;
        assert!(ratio > 0.4 && ratio < 0.6, "Rollout distribution should be ~50%, got {}", ratio);
    }

    #[test]
    fn assign_variant_is_deterministic() {
        let context = FeatureFlagContext {
            contract_id: Some("CABC123".to_string()),
            user_id: None,
            ip_address: None,
            region: None,
        };
        let variants = vec![
            FlagVariant { name: "control".into(), weight: 50 },
            FlagVariant { name: "treatment".into(), weight: 50 },
        ];
        let first = assign_variant("ab-flag", &context, &variants).unwrap().name.clone();
        let second = assign_variant("ab-flag", &context, &variants).unwrap().name.clone();
        assert_eq!(first, second);
    }

    #[test]
    fn assign_variant_returns_none_for_empty_variants() {
        let context = FeatureFlagContext {
            contract_id: Some("CABC123".to_string()),
            user_id: None,
            ip_address: None,
            region: None,
        };
        assert!(assign_variant("ab-flag", &context, &[]).is_none());
    }

    #[test]
    fn assign_variant_distribution_matches_weights() {
        let variants = vec![
            FlagVariant { name: "a".into(), weight: 90 },
            FlagVariant { name: "b".into(), weight: 10 },
        ];
        let mut a_count = 0;
        let total = 1000;
        for i in 0..total {
            let context = FeatureFlagContext {
                contract_id: Some(format!("contract-{}", i)),
                user_id: None,
                ip_address: None,
                region: None,
            };
            if assign_variant("weighted-flag", &context, &variants).unwrap().name == "a" {
                a_count += 1;
            }
        }
        let ratio = a_count as f64 / total as f64;
        assert!(ratio > 0.8 && ratio < 1.0, "Expected ~90% in variant a, got {}", ratio);
    }

    #[test]
    fn flag_metrics_records_evaluations_and_enabled_rate() {
        let metrics = FlagMetrics::default();
        metrics.record_evaluation("my-flag", true);
        metrics.record_evaluation("my-flag", false);
        metrics.record_evaluation("my-flag", true);

        let snapshot = metrics.snapshot();
        let entry = snapshot.iter().find(|s| s.flag_name == "my-flag").unwrap();
        assert_eq!(entry.evaluations, 3);
        assert_eq!(entry.enabled, 2);
        assert_eq!(entry.disabled, 1);
        assert!((entry.enabled_rate - (2.0 / 3.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn flag_metrics_records_variant_assignments() {
        let metrics = FlagMetrics::default();
        metrics.record_variant_assignment("ab-flag", "control");
        metrics.record_variant_assignment("ab-flag", "control");
        metrics.record_variant_assignment("ab-flag", "treatment");

        let snapshot = metrics.snapshot();
        let entry = snapshot.iter().find(|s| s.flag_name == "ab-flag").unwrap();
        assert_eq!(entry.variant_assignments.get("control"), Some(&2));
        assert_eq!(entry.variant_assignments.get("treatment"), Some(&1));
    }
}
