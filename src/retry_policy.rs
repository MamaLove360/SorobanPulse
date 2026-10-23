use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

/// Retry strategy enum for different backoff patterns.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetryStrategy {
    #[serde(rename = "exponential")]
    Exponential,
    #[serde(rename = "linear")]
    Linear,
    #[serde(rename = "fixed")]
    Fixed,
}

impl RetryStrategy {
    pub fn calculate_backoff(&self, attempt: u32, initial_ms: u64, multiplier: f64) -> u64 {
        match self {
            RetryStrategy::Exponential => {
                (initial_ms as f64 * multiplier.powi((attempt - 1) as i32)) as u64
            }
            RetryStrategy::Linear => initial_ms * attempt as u64,
            RetryStrategy::Fixed => initial_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub backoff_multiplier: f64,
    pub max_backoff_ms: u64,
    #[serde(default)]
    pub strategy: Option<RetryStrategy>,
    #[serde(default)]
    pub use_jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_backoff_ms: 60000,
            strategy: Some(RetryStrategy::Exponential),
            use_jitter: false,
        }
    }
}

impl RetryPolicy {
    pub fn webhook_default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_backoff_ms: 600_000,
            strategy: Some(RetryStrategy::Exponential),
            use_jitter: true,
        }
    }

    pub fn email_default() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff_ms: 0,
            backoff_multiplier: 1.0,
            max_backoff_ms: 0,
            strategy: Some(RetryStrategy::Fixed),
            use_jitter: false,
        }
    }

    pub fn sms_default() -> Self {
        Self {
            max_attempts: 2,
            initial_backoff_ms: 2000,
            backoff_multiplier: 1.5,
            max_backoff_ms: 10000,
            strategy: Some(RetryStrategy::Linear),
            use_jitter: true,
        }
    }

    /// Calculate backoff duration with optional jitter to prevent thundering herd.
    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        if attempt == 0 || self.initial_backoff_ms == 0 {
            return Duration::from_millis(0);
        }

        let strategy = self.strategy.unwrap_or(RetryStrategy::Exponential);
        let backoff_ms = strategy.calculate_backoff(attempt, self.initial_backoff_ms, self.backoff_multiplier);
        let mut capped_backoff = backoff_ms.min(self.max_backoff_ms);

        if self.use_jitter {
            capped_backoff = Self::apply_jitter(capped_backoff);
        }

        Duration::from_millis(capped_backoff)
    }

    /// Apply full jitter: random value between 0 and backoff_ms.
    /// This prevents thundering herd problem where multiple clients retry simultaneously.
    fn apply_jitter(backoff_ms: u64) -> u64 {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        let mut hasher = RandomState::new().build_hasher();
        hasher.write_usize(std::process::id() as usize);
        hasher.write_u64(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64);

        let hash = hasher.finish();
        let jitter_ratio = (hash as f64 / u64::MAX as f64).clamp(0.0, 1.0);
        (backoff_ms as f64 * jitter_ratio) as u64
    }

    pub async fn execute_with_retry<F, Fut, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut(u32) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        // Always make at least one attempt, even if `max_attempts` is 0.
        let max_attempts = self.max_attempts.max(1);
        let mut attempt = 1;
        loop {
            match operation(attempt).await {
                Ok(result) => return Ok(result),
                Err(error) if attempt < max_attempts => {
                    let backoff = self.calculate_backoff(attempt);
                    warn!(
                        attempt = attempt,
                        max_attempts = max_attempts,
                        backoff_ms = backoff.as_millis(),
                        error = %error,
                        "Operation failed, retrying after backoff"
                    );
                    sleep(backoff).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Executes `operation`, recording attempt counts and outcomes into `metrics`
    /// keyed by `policy_name` for later inspection via the retry status dashboard.
    pub async fn execute_with_retry_metrics<F, Fut, T, E>(
        &self,
        policy_name: &str,
        metrics: &RetryMetrics,
        mut operation: F,
    ) -> Result<T, E>
    where
        F: FnMut(u32) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        // Always make at least one attempt, even if `max_attempts` is 0.
        let max_attempts = self.max_attempts.max(1);
        let mut attempt = 1;
        loop {
            metrics.record_attempt(policy_name);
            match operation(attempt).await {
                Ok(result) => {
                    metrics.record_success(policy_name, attempt);
                    return Ok(result);
                }
                Err(error) if attempt < max_attempts => {
                    let backoff = self.calculate_backoff(attempt);
                    metrics.record_retry(policy_name, backoff);
                    warn!(
                        attempt = attempt,
                        max_attempts = max_attempts,
                        backoff_ms = backoff.as_millis(),
                        error = %error,
                        "Operation failed, retrying after backoff"
                    );
                    sleep(backoff).await;
                    attempt += 1;
                }
                Err(error) => {
                    metrics.record_exhausted(policy_name);
                    return Err(error);
                }
            }
        }
    }
}

/// Named, reusable retry policies. Allows selecting a policy by identifier
/// (e.g. from configuration) rather than constructing one inline.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetryPolicyRegistry {
    policies: HashMap<String, RetryPolicy>,
}

impl RetryPolicyRegistry {
    pub fn with_defaults() -> Self {
        let mut policies = HashMap::new();
        policies.insert("webhook".to_string(), RetryPolicy::webhook_default());
        policies.insert("email".to_string(), RetryPolicy::email_default());
        policies.insert("sms".to_string(), RetryPolicy::sms_default());
        Self { policies }
    }

    pub fn register(&mut self, name: impl Into<String>, policy: RetryPolicy) {
        self.policies.insert(name.into(), policy);
    }

    pub fn get(&self, name: &str) -> Option<&RetryPolicy> {
        self.policies.get(name)
    }
}

/// Per-attempt counters for a single named retry policy.
#[derive(Debug, Default)]
struct PolicyCounters {
    attempts: AtomicU64,
    successes: AtomicU64,
    retries: AtomicU64,
    exhausted: AtomicU64,
    total_backoff_ms: AtomicU64,
}

/// Aggregated retry metrics across all named policies, suitable for backing
/// a retry status dashboard.
#[derive(Debug, Default)]
pub struct RetryMetrics {
    counters: std::sync::Mutex<HashMap<String, Arc<PolicyCounters>>>,
}

impl RetryMetrics {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn counters_for(&self, policy_name: &str) -> Arc<PolicyCounters> {
        let mut guard = self.counters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .entry(policy_name.to_string())
            .or_insert_with(|| Arc::new(PolicyCounters::default()))
            .clone()
    }

    pub fn record_attempt(&self, policy_name: &str) {
        self.counters_for(policy_name)
            .attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_success(&self, policy_name: &str, _attempt: u32) {
        self.counters_for(policy_name)
            .successes
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self, policy_name: &str, backoff: Duration) {
        let counters = self.counters_for(policy_name);
        counters.retries.fetch_add(1, Ordering::Relaxed);
        counters
            .total_backoff_ms
            .fetch_add(backoff.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_exhausted(&self, policy_name: &str) {
        self.counters_for(policy_name)
            .exhausted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Builds a snapshot of all tracked policies for a retry status dashboard.
    pub fn dashboard_snapshot(&self) -> Vec<RetryDashboardEntry> {
        let guard = self.counters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .iter()
            .map(|(name, counters)| {
                let attempts = counters.attempts.load(Ordering::Relaxed);
                let successes = counters.successes.load(Ordering::Relaxed);
                let retries = counters.retries.load(Ordering::Relaxed);
                let exhausted = counters.exhausted.load(Ordering::Relaxed);
                let total_backoff_ms = counters.total_backoff_ms.load(Ordering::Relaxed);
                RetryDashboardEntry {
                    policy_name: name.clone(),
                    attempts,
                    successes,
                    retries,
                    exhausted,
                    success_rate: if attempts == 0 {
                        0.0
                    } else {
                        successes as f64 / attempts as f64
                    },
                    avg_backoff_ms: if retries == 0 {
                        0.0
                    } else {
                        total_backoff_ms as f64 / retries as f64
                    },
                }
            })
            .collect()
    }
}

/// A single row in the retry status dashboard, summarizing one named policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetryDashboardEntry {
    pub policy_name: String,
    pub attempts: u64,
    pub successes: u64,
    pub retries: u64,
    pub exhausted: u64,
    pub success_rate: f64,
    pub avg_backoff_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_backoff_exponential() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_backoff_ms: 5000,
            strategy: Some(RetryStrategy::Exponential),
            use_jitter: false,
        };

        assert_eq!(policy.calculate_backoff(0), Duration::from_millis(0));
        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(1000));
        assert_eq!(policy.calculate_backoff(2), Duration::from_millis(2000));
        assert_eq!(policy.calculate_backoff(3), Duration::from_millis(4000));

        let policy_with_cap = RetryPolicy {
            max_attempts: 5,
            initial_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_backoff_ms: 3000,
            strategy: Some(RetryStrategy::Exponential),
            use_jitter: false,
        };
        assert_eq!(policy_with_cap.calculate_backoff(3), Duration::from_millis(3000));
        assert_eq!(policy_with_cap.calculate_backoff(4), Duration::from_millis(3000));
    }

    #[test]
    fn test_calculate_backoff_linear() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 1000,
            backoff_multiplier: 1.0,
            max_backoff_ms: 10000,
            strategy: Some(RetryStrategy::Linear),
            use_jitter: false,
        };

        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(1000));
        assert_eq!(policy.calculate_backoff(2), Duration::from_millis(2000));
        assert_eq!(policy.calculate_backoff(3), Duration::from_millis(3000));
    }

    #[test]
    fn test_calculate_backoff_fixed() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 1000,
            backoff_multiplier: 1.0,
            max_backoff_ms: 1000,
            strategy: Some(RetryStrategy::Fixed),
            use_jitter: false,
        };

        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(1000));
        assert_eq!(policy.calculate_backoff(2), Duration::from_millis(1000));
        assert_eq!(policy.calculate_backoff(3), Duration::from_millis(1000));
    }

    #[test]
    fn test_calculate_backoff_with_jitter() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_backoff_ms: 5000,
            strategy: Some(RetryStrategy::Exponential),
            use_jitter: true,
        };

        let d1 = policy.calculate_backoff(1);
        let d2 = policy.calculate_backoff(2);

        assert!(d1 <= Duration::from_millis(1000), "Jittered backoff should not exceed base");
        assert!(d2 <= Duration::from_millis(2000), "Jittered backoff should not exceed base");
    }

    #[tokio::test]
    async fn test_execute_with_retry_success() {
        let policy = RetryPolicy::default();
        let mut call_count = 0;

        let result = policy.execute_with_retry(|_attempt| {
            call_count += 1;
            async move {
                if call_count < 2 {
                    Err("temporary error")
                } else {
                    Ok("success")
                }
            }
        }).await;

        assert_eq!(result, Ok("success"));
        assert_eq!(call_count, 2);
    }

    #[tokio::test]
    async fn test_execute_with_retry_failure() {
        let policy = RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            backoff_multiplier: 1.0,
            max_backoff_ms: 1,
            strategy: Some(RetryStrategy::Exponential),
            use_jitter: false,
        };
        let mut call_count = 0;

        let result: Result<(), &str> = policy.execute_with_retry(|_attempt| {
            call_count += 1;
            async move { Err("persistent error") }
        }).await;

        assert_eq!(result, Err("persistent error"));
        assert_eq!(call_count, 2);
    }

    #[test]
    fn test_retry_strategy_calculation() {
        assert_eq!(RetryStrategy::Exponential.calculate_backoff(1, 1000, 2.0), 1000);
        assert_eq!(RetryStrategy::Exponential.calculate_backoff(2, 1000, 2.0), 2000);
        assert_eq!(RetryStrategy::Linear.calculate_backoff(2, 1000, 1.0), 2000);
        assert_eq!(RetryStrategy::Fixed.calculate_backoff(5, 1000, 1.0), 1000);
    }

    #[test]
    fn test_jitter_is_less_than_base() {
        let backoff = 1000u64;
        let jittered = RetryPolicy::apply_jitter(backoff);
        assert!(jittered <= backoff, "Jittered value should not exceed base");
    }

    #[test]
    fn test_registry_has_default_policies() {
        let registry = RetryPolicyRegistry::with_defaults();
        assert!(registry.get("webhook").is_some());
        assert!(registry.get("email").is_some());
        assert!(registry.get("sms").is_some());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_registry_register_custom_policy() {
        let mut registry = RetryPolicyRegistry::with_defaults();
        registry.register(
            "custom",
            RetryPolicy {
                max_attempts: 10,
                ..RetryPolicy::default()
            },
        );
        assert_eq!(registry.get("custom").unwrap().max_attempts, 10);
    }

    #[tokio::test]
    async fn test_execute_with_retry_metrics_records_success() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 1,
            backoff_multiplier: 1.0,
            max_backoff_ms: 1,
            strategy: Some(RetryStrategy::Fixed),
            use_jitter: false,
        };
        let metrics = RetryMetrics::default();
        let mut call_count = 0;

        let result: Result<&str, &str> = policy
            .execute_with_retry_metrics("webhook", &metrics, |_attempt| {
                call_count += 1;
                async move {
                    if call_count < 2 {
                        Err("temporary error")
                    } else {
                        Ok("success")
                    }
                }
            })
            .await;

        assert_eq!(result, Ok("success"));
        let snapshot = metrics.dashboard_snapshot();
        let entry = snapshot.iter().find(|e| e.policy_name == "webhook").unwrap();
        assert_eq!(entry.attempts, 2);
        assert_eq!(entry.successes, 1);
        assert_eq!(entry.retries, 1);
    }

    #[tokio::test]
    async fn test_execute_with_retry_metrics_records_exhaustion() {
        let policy = RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            backoff_multiplier: 1.0,
            max_backoff_ms: 1,
            strategy: Some(RetryStrategy::Fixed),
            use_jitter: false,
        };
        let metrics = RetryMetrics::default();

        let result: Result<&str, &str> = policy
            .execute_with_retry_metrics("email", &metrics, |_attempt| async move {
                Err("persistent error")
            })
            .await;

        assert_eq!(result, Err("persistent error"));
        let snapshot = metrics.dashboard_snapshot();
        let entry = snapshot.iter().find(|e| e.policy_name == "email").unwrap();
        assert_eq!(entry.exhausted, 1);
        assert_eq!(entry.success_rate, 0.0);
    }

    #[test]
    fn test_dashboard_snapshot_success_rate() {
        let metrics = RetryMetrics::default();
        metrics.record_attempt("sms");
        metrics.record_attempt("sms");
        metrics.record_success("sms", 2);
        let snapshot = metrics.dashboard_snapshot();
        let entry = snapshot.iter().find(|e| e.policy_name == "sms").unwrap();
        assert_eq!(entry.success_rate, 0.5);
    }

    #[test]
    fn test_max_attempts_respected_across_strategies() {
        for strategy in [RetryStrategy::Exponential, RetryStrategy::Linear, RetryStrategy::Fixed] {
            let policy = RetryPolicy {
                max_attempts: 4,
                initial_backoff_ms: 10,
                backoff_multiplier: 2.0,
                max_backoff_ms: 100,
                strategy: Some(strategy),
                use_jitter: false,
            };
            assert!(policy.calculate_backoff(4) <= Duration::from_millis(100));
        }
    }
}