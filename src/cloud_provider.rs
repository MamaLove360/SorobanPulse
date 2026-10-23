//! Issue #834: Multi-Cloud & Hybrid Deployment — Cloud Abstraction Layer.
//!
//! Provides a unified `CloudEventPublisher` trait that abstracts over
//! provider-specific publishers (Kinesis, Pub/Sub, Event Hubs, Kafka, SQS),
//! a `CloudProviderRegistry` for multi-provider failover, and health-check
//! support so unhealthy providers are bypassed automatically.

use crate::models::SorobanEvent;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

// ── Cloud provider enum ──────────────────────────────────────────────────────

/// Supported cloud (and on-premise) deployment targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CloudProvider {
    Aws,
    Gcp,
    Azure,
    OnPremise,
}

impl fmt::Display for CloudProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Aws => write!(f, "aws"),
            Self::Gcp => write!(f, "gcp"),
            Self::Azure => write!(f, "azure"),
            Self::OnPremise => write!(f, "on-premise"),
        }
    }
}

// ── Error types ──────────────────────────────────────────────────────────────

/// Unified error type for cloud publishing operations.
#[derive(Debug, Clone)]
pub enum CloudPublishError {
    /// Serialization failed (usually JSON).
    Serialization(String),
    /// The provider returned an error (network, auth, quota, etc.).
    ProviderError { provider: CloudProvider, message: String },
    /// The provider is currently marked unhealthy.
    ProviderUnhealthy(CloudProvider),
    /// All providers in the registry failed.
    AllProvidersFailed(Vec<(CloudProvider, String)>),
    /// Configuration error — e.g. missing credentials.
    Configuration(String),
}

impl fmt::Display for CloudPublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
            Self::ProviderError { provider, message } => {
                write!(f, "{provider} error: {message}")
            }
            Self::ProviderUnhealthy(p) => write!(f, "provider {p} is unhealthy"),
            Self::AllProvidersFailed(failures) => {
                write!(f, "all providers failed: ")?;
                for (i, (p, msg)) in failures.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{p}: {msg}")?;
                }
                Ok(())
            }
            Self::Configuration(msg) => write!(f, "configuration error: {msg}"),
        }
    }
}

impl std::error::Error for CloudPublishError {}

// ── Unified publisher trait ──────────────────────────────────────────────────

/// Cloud-agnostic event publisher.  Every concrete provider adapter implements
/// this trait so the rest of the system can publish events without knowing
/// which cloud it is talking to.
#[async_trait]
pub trait CloudEventPublisher: Send + Sync {
    /// Which cloud provider this publisher targets.
    fn provider(&self) -> CloudProvider;

    /// Publish a single event to the cloud.
    async fn publish(&self, event: &SorobanEvent) -> Result<(), CloudPublishError>;

    /// Return `true` if the provider is currently reachable / healthy.
    async fn health_check(&self) -> bool;
}

// ── Provider configuration ───────────────────────────────────────────────────

/// Generic, serialisable configuration block that can be mapped to any
/// provider-specific settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudProviderConfig {
    /// Which cloud this config targets.
    pub provider: CloudProvider,
    /// Provider region (e.g. "us-east-1", "us-central1", "eastus").
    pub region: String,
    /// Free-form key/value pairs forwarded to the concrete adapter.
    #[serde(default)]
    pub settings: HashMap<String, String>,
}

// ── Health state ─────────────────────────────────────────────────────────────

/// Per-provider health tracking.
#[derive(Debug, Clone)]
pub struct ProviderHealth {
    pub healthy: bool,
    pub consecutive_failures: u32,
    pub last_check: Option<std::time::Instant>,
}

impl Default for ProviderHealth {
    fn default() -> Self {
        Self {
            healthy: true,
            consecutive_failures: 0,
            last_check: None,
        }
    }
}

// ── Provider registry with failover ──────────────────────────────────────────

/// Manages multiple `CloudEventPublisher` instances and provides automatic
/// failover: if the primary publisher fails, the registry tries each secondary
/// in order until one succeeds.
pub struct CloudProviderRegistry {
    /// Publishers ordered by priority (index 0 = primary).
    publishers: Vec<Arc<dyn CloudEventPublisher>>,
    /// Per-provider health state, keyed by `CloudProvider`.
    health: Arc<RwLock<HashMap<CloudProvider, ProviderHealth>>>,
    /// Maximum consecutive failures before marking a provider unhealthy.
    max_failures: u32,
}

impl CloudProviderRegistry {
    /// Create a new registry from an ordered list of publishers.
    /// The first publisher is the primary; the rest are secondaries.
    pub fn new(publishers: Vec<Arc<dyn CloudEventPublisher>>) -> Self {
        let mut health = HashMap::new();
        for p in &publishers {
            health.entry(p.provider()).or_insert_with(ProviderHealth::default);
        }
        Self {
            publishers,
            health: Arc::new(RwLock::new(health)),
            max_failures: 3,
        }
    }

    /// Override the default failure threshold (3).
    #[must_use]
    pub fn with_max_failures(mut self, n: u32) -> Self {
        self.max_failures = n;
        self
    }

    /// Publish an event through the registry.  Tries the primary first, then
    /// falls back to secondaries in order.
    pub async fn publish(&self, event: &SorobanEvent) -> Result<(), CloudPublishError> {
        let mut failures: Vec<(CloudProvider, String)> = Vec::new();

        for publisher in &self.publishers {
            let provider = publisher.provider();

            // Skip providers that are currently marked unhealthy.
            {
                let health = self.health.read().await;
                if let Some(ph) = health.get(&provider) {
                    if !ph.healthy {
                        warn!(provider = %provider, "skipping unhealthy provider");
                        failures.push((provider, "unhealthy".to_string()));
                        continue;
                    }
                }
            }

            match publisher.publish(event).await {
                Ok(()) => {
                    // Reset failure counter on success.
                    let mut health = self.health.write().await;
                    if let Some(ph) = health.get_mut(&provider) {
                        ph.consecutive_failures = 0;
                        ph.healthy = true;
                        ph.last_check = Some(std::time::Instant::now());
                    }
                    return Ok(());
                }
                Err(e) => {
                    let msg = e.to_string();
                    error!(provider = %provider, error = %msg, "publish failed, trying next provider");
                    failures.push((provider, msg));

                    // Track failure.
                    let mut health = self.health.write().await;
                    if let Some(ph) = health.get_mut(&provider) {
                        ph.consecutive_failures += 1;
                        ph.last_check = Some(std::time::Instant::now());
                        if ph.consecutive_failures >= self.max_failures {
                            ph.healthy = false;
                            warn!(
                                provider = %provider,
                                failures = ph.consecutive_failures,
                                "provider marked unhealthy after {} consecutive failures",
                                self.max_failures,
                            );
                        }
                    }
                }
            }
        }

        Err(CloudPublishError::AllProvidersFailed(failures))
    }

    /// Run health checks on every registered provider and update internal
    /// state accordingly.
    pub async fn check_health(&self) {
        for publisher in &self.publishers {
            let provider = publisher.provider();
            let ok = publisher.health_check().await;

            let mut health = self.health.write().await;
            let ph = health.entry(provider).or_default();
            ph.last_check = Some(std::time::Instant::now());
            if ok {
                ph.healthy = true;
                ph.consecutive_failures = 0;
                info!(provider = %provider, "health check passed");
            } else {
                ph.consecutive_failures += 1;
                if ph.consecutive_failures >= self.max_failures {
                    ph.healthy = false;
                }
                warn!(
                    provider = %provider,
                    consecutive_failures = ph.consecutive_failures,
                    "health check failed",
                );
            }
        }
    }

    /// Mark a specific provider as healthy again (manual recovery).
    pub async fn mark_healthy(&self, provider: CloudProvider) {
        let mut health = self.health.write().await;
        if let Some(ph) = health.get_mut(&provider) {
            ph.healthy = true;
            ph.consecutive_failures = 0;
            info!(provider = %provider, "provider manually marked healthy");
        }
    }

    /// Return a snapshot of the current health state.
    pub async fn health_snapshot(&self) -> HashMap<CloudProvider, ProviderHealth> {
        self.health.read().await.clone()
    }

    /// Return the number of registered publishers.
    pub fn publisher_count(&self) -> usize {
        self.publishers.len()
    }
}

// ── Mock publisher for testing ───────────────────────────────────────────────

/// A mock publisher used by unit tests.  It records every event it receives and
/// can be configured to fail on demand.
#[derive(Clone)]
pub struct MockCloudPublisher {
    provider: CloudProvider,
    pub published: Arc<std::sync::Mutex<Vec<SorobanEvent>>>,
    pub fail_with: Arc<std::sync::Mutex<Option<String>>>,
    pub healthy: Arc<std::sync::Mutex<bool>>,
}

impl MockCloudPublisher {
    pub fn new(provider: CloudProvider) -> Self {
        Self {
            provider,
            published: Arc::new(std::sync::Mutex::new(Vec::new())),
            fail_with: Arc::new(std::sync::Mutex::new(None)),
            healthy: Arc::new(std::sync::Mutex::new(true)),
        }
    }

    /// Configure the mock to fail with the given message.
    pub fn set_fail(&self, msg: &str) {
        *self.fail_with.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(msg.to_string());
    }

    /// Clear the failure so subsequent publishes succeed.
    pub fn clear_fail(&self) {
        *self.fail_with.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// Set the health state.
    pub fn set_healthy(&self, h: bool) {
        *self.healthy.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = h;
    }

    /// Return the number of successfully published events.
    pub fn published_count(&self) -> usize {
        self.published.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len()
    }
}

#[async_trait]
impl CloudEventPublisher for MockCloudPublisher {
    fn provider(&self) -> CloudProvider {
        self.provider
    }

    async fn publish(&self, event: &SorobanEvent) -> Result<(), CloudPublishError> {
        if let Some(ref msg) = *self.fail_with.lock().unwrap_or_else(std::sync::PoisonError::into_inner) {
            return Err(CloudPublishError::ProviderError {
                provider: self.provider,
                message: msg.clone(),
            });
        }
        self.published.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(event.clone());
        Ok(())
    }

    async fn health_check(&self) -> bool {
        *self.healthy.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn make_event() -> SorobanEvent {
        SorobanEvent {
            id: None,
            contract_id: "CABC123".into(),
            event_type: "contract".into(),
            tx_hash: "deadbeef".into(),
            ledger: 100,
            ledger_closed_at: "2026-04-27T00:00:00Z".into(),
            ledger_hash: None,
            in_successful_call: true,
            value: Value::Null,
            topic: None,
            tenant_id: None,
        }
    }

    #[test]
    fn cloud_provider_display() {
        assert_eq!(CloudProvider::Aws.to_string(), "aws");
        assert_eq!(CloudProvider::Gcp.to_string(), "gcp");
        assert_eq!(CloudProvider::Azure.to_string(), "azure");
        assert_eq!(CloudProvider::OnPremise.to_string(), "on-premise");
    }

    #[test]
    fn cloud_provider_serde_roundtrip() {
        let json = serde_json::to_string(&CloudProvider::Aws).unwrap();
        assert_eq!(json, r#""aws""#);
        let back: CloudProvider = serde_json::from_str(&json).unwrap();
        assert_eq!(back, CloudProvider::Aws);
    }

    #[tokio::test]
    async fn mock_publisher_records_events() {
        let mock = MockCloudPublisher::new(CloudProvider::Aws);
        let event = make_event();
        mock.publish(&event).await.unwrap();
        assert_eq!(mock.published_count(), 1);
    }

    #[tokio::test]
    async fn mock_publisher_fails_when_configured() {
        let mock = MockCloudPublisher::new(CloudProvider::Gcp);
        mock.set_fail("quota exceeded");
        let event = make_event();
        let err = mock.publish(&event).await.unwrap_err();
        assert!(err.to_string().contains("quota exceeded"));
    }

    #[tokio::test]
    async fn registry_failover() {
        let primary = Arc::new(MockCloudPublisher::new(CloudProvider::Aws));
        let secondary = Arc::new(MockCloudPublisher::new(CloudProvider::Gcp));

        primary.set_fail("aws down");

        let registry = CloudProviderRegistry::new(vec![
            primary.clone() as Arc<dyn CloudEventPublisher>,
            secondary.clone() as Arc<dyn CloudEventPublisher>,
        ]);

        let event = make_event();
        registry.publish(&event).await.unwrap();

        assert_eq!(primary.published_count(), 0);
        assert_eq!(secondary.published_count(), 1);
    }
}
