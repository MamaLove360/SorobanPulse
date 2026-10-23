//! PagerDuty integration — Issue #951
//!
//! Implements:
//! - PagerDuty Events API v2 (trigger / acknowledge / resolve)
//! - Incident deduplication via `dedup_key`
//! - Escalation policy management (fetch + cache from PD REST API)
//! - On-call user lookup (delegates to [`crate::oncall::OnCallScheduler`])
//! - Acknowledgment tracking persisted to `pagerduty_incidents`
//! - Auto-resolve of stale open incidents
//! - Per-subscription configuration via `pagerduty_integrations` DB table

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{metrics, models::SorobanEvent};

/// PagerDuty Events API v2 endpoint
const PD_EVENTS_URL: &str = "https://events.pagerduty.com/v2/enqueue";
/// PagerDuty REST API base
const PD_API_BASE: &str = "https://api.pagerduty.com";
/// Maximum delivery retry attempts
const MAX_ATTEMPTS: u32 = 3;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for a PagerDuty integration.
///
/// A single instance corresponds to one row in `pagerduty_integrations`.
#[derive(Debug, Clone)]
pub struct PagerDutyConfig {
    /// Integration (routing) key for the Events API v2.
    pub routing_key: String,
    /// Human-readable service name included in incident payloads.
    pub service_name: String,
    /// Optional PagerDuty REST API key for schedule / escalation lookups.
    pub api_key: Option<String>,
    /// Optional escalation policy ID to attach when triggering incidents.
    pub escalation_policy_id: Option<String>,
    /// Only forward events from these contract IDs. Empty = all contracts.
    pub contract_filter: Vec<String>,
    /// Only forward these event types. Empty = all types.
    pub event_type_filter: Vec<String>,
    /// Maps event type → PagerDuty severity (`critical`, `error`, `warning`, `info`).
    pub severity_mapping: HashMap<String, String>,
    /// Whether to auto-resolve open incidents when no new events arrive for
    /// `auto_resolve_threshold_minutes`.
    pub auto_resolve: bool,
    pub auto_resolve_threshold_minutes: i64,
}

impl Default for PagerDutyConfig {
    fn default() -> Self {
        let mut severity_mapping = HashMap::new();
        severity_mapping.insert("contract".to_string(), "error".to_string());
        severity_mapping.insert("diagnostic".to_string(), "warning".to_string());
        severity_mapping.insert("system".to_string(), "info".to_string());

        Self {
            routing_key: String::new(),
            service_name: "Soroban Pulse".to_string(),
            api_key: None,
            escalation_policy_id: None,
            contract_filter: Vec::new(),
            event_type_filter: Vec::new(),
            severity_mapping,
            auto_resolve: true,
            auto_resolve_threshold_minutes: 30,
        }
    }
}

// ---------------------------------------------------------------------------
// Incident / API response types
// ---------------------------------------------------------------------------

/// A PagerDuty incident record as stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PagerDutyIncident {
    pub id: Uuid,
    pub integration_id: Option<Uuid>,
    pub dedup_key: String,
    pub incident_key: Option<String>,
    pub contract_id: String,
    pub event_type: String,
    pub status: String,
    pub acknowledged_at: Option<chrono::DateTime<chrono::Utc>>,
    pub acknowledged_by: Option<String>,
    pub resolved_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Escalation policy returned by the PagerDuty REST API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EscalationPolicy {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub escalation_rules: Vec<EscalationRule>,
}

/// A single rule within an escalation policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EscalationRule {
    pub id: String,
    pub escalation_delay_in_minutes: u32,
    pub targets: Vec<EscalationTarget>,
}

/// A notification target (user or schedule) in an escalation rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EscalationTarget {
    pub id: String,
    #[serde(rename = "type")]
    pub target_type: String,
    pub name: Option<String>,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// PagerDuty API client.
///
/// Create one per integration configuration and share it across async tasks
/// via [`Arc`].
pub struct PagerDutyClient {
    client: Client,
    pub config: PagerDutyConfig,
}

impl PagerDutyClient {
    /// Construct a client from an explicit config.
    pub fn new(config: PagerDutyConfig) -> Result<Self, crate::error::InitError> {
        let client = crate::net::outbound::build_http_client(
            Client::builder().timeout(Duration::from_secs(10)),
            "pagerduty",
        )?;

        Ok(Self { client, config }
    }

    /// Build a client from the global application config when a routing key is
    /// configured.  Returns `None` when PagerDuty is disabled.
    pub fn from_app_config(config: &crate::config::Config) -> Option<Arc<Self>> {
        let routing_key = config.pagerduty_routing_key.clone()?;
        let pd_config = PagerDutyConfig {
            routing_key,
            service_name: config.pagerduty_service_name.clone(),
            api_key: None, // set separately via ONCALL_PAGERDUTY_API_KEY
            escalation_policy_id: None,
            contract_filter: config.pagerduty_contract_filter.clone(),
            event_type_filter: config.pagerduty_event_type_filter.clone(),
            severity_mapping: config.pagerduty_severity_mapping.clone(),
            auto_resolve: config.pagerduty_auto_resolve,
            auto_resolve_threshold_minutes: config.pagerduty_auto_resolve_threshold_minutes,
        };
        match Self::new(pd_config) {
            Ok(client) => Some(Arc::new(client)),
            Err(e) => {
                error!(error = %e, "PagerDuty notifications disabled");
                None
            }
        }
    }

    // -----------------------------------------------------------------------
    // Event Actions
    // -----------------------------------------------------------------------

    /// Trigger a new PagerDuty incident for `event`.
    ///
    /// Returns the deduplication key on success, which can be used for later
    /// acknowledge / resolve calls.
    pub async fn trigger_incident(
        &self,
        event: &SorobanEvent,
        integration_id: Option<Uuid>,
        pool: Option<&sqlx::PgPool>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let dedup_key = Self::make_dedup_key(&event.contract_id, &event.event_type.to_string());
        let severity = self
            .config
            .severity_mapping
            .get(&event.event_type.to_string())
            .cloned()
            .unwrap_or_else(|| "error".to_string());

        let mut payload = json!({
            "routing_key": self.config.routing_key,
            "event_action": "trigger",
            "dedup_key": dedup_key,
            "payload": {
                "summary": format!(
                    "Soroban contract event: {} on {}",
                    event.event_type, event.contract_id
                ),
                "source": self.config.service_name,
                "severity": severity,
                "component": "soroban-contract",
                "group": event.contract_id,
                "class": event.event_type,
                "custom_details": {
                    "contract_id": event.contract_id,
                    "event_type": event.event_type,
                    "tx_hash": event.tx_hash,
                    "ledger": event.ledger,
                    "timestamp": event.timestamp,
                    "event_data": event.event_data
                }
            }
        });

        // Attach escalation policy when configured
        if let Some(ref policy_id) = self.config.escalation_policy_id {
            payload["escalation_policy"] = json!({ "id": policy_id });
        }

        let response = self.send_event(payload).await?;

        let incident_key = response
            .get("incident_key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        if let Some(pool) = pool {
            self.upsert_incident(
                &dedup_key,
                integration_id,
                &event.contract_id,
                &event.event_type.to_string(),
                incident_key.as_deref(),
                "triggered",
                pool,
            )
            .await;
        }

        info!(
            contract_id = %event.contract_id,
            event_type = %event.event_type,
            dedup_key = %dedup_key,
            severity = %severity,
            "PagerDuty incident triggered"
        );

        Ok(dedup_key)
    }

    /// Acknowledge an open incident identified by `dedup_key`.
    ///
    /// `acknowledged_by` is free-form text (e.g. the operator's name or email)
    /// and is persisted in `pagerduty_incidents.acknowledged_by`.
    pub async fn acknowledge_incident(
        &self,
        dedup_key: &str,
        acknowledged_by: Option<&str>,
        pool: Option<&sqlx::PgPool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = json!({
            "routing_key": self.config.routing_key,
            "event_action": "acknowledge",
            "dedup_key": dedup_key
        });

        self.send_event(payload).await?;

        if let Some(pool) = pool {
            if let Err(e) = sqlx::query(
                "UPDATE pagerduty_incidents
                 SET status = 'acknowledged',
                     acknowledged_at = NOW(),
                     acknowledged_by = $2,
                     updated_at = NOW()
                 WHERE dedup_key = $1",
            )
            .bind(dedup_key)
            .bind(acknowledged_by)
            .execute(pool)
            .await
            {
                error!(error = %e, dedup_key = %dedup_key, "Failed to update acknowledged status");
            }
        }

        info!(dedup_key = %dedup_key, acknowledged_by = ?acknowledged_by, "PagerDuty incident acknowledged");
        Ok(())
    }

    /// Resolve an open or acknowledged incident identified by `dedup_key`.
    pub async fn resolve_incident(
        &self,
        dedup_key: &str,
        pool: Option<&sqlx::PgPool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = json!({
            "routing_key": self.config.routing_key,
            "event_action": "resolve",
            "dedup_key": dedup_key
        });

        self.send_event(payload).await?;

        if let Some(pool) = pool {
            if let Err(e) = sqlx::query(
                "UPDATE pagerduty_incidents
                 SET status = 'resolved',
                     resolved_at = NOW(),
                     updated_at = NOW()
                 WHERE dedup_key = $1",
            )
            .bind(dedup_key)
            .execute(pool)
            .await
            {
                error!(error = %e, dedup_key = %dedup_key, "Failed to update resolved status");
            }
        }

        info!(dedup_key = %dedup_key, "PagerDuty incident resolved");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Escalation Policies
    // -----------------------------------------------------------------------

    /// Fetch an escalation policy from the PagerDuty REST API.
    ///
    /// Optionally caches the result in `pagerduty_escalation_policies`.
    pub async fn get_escalation_policy(
        &self,
        policy_id: &str,
        integration_id: Option<Uuid>,
        pool: Option<&sqlx::PgPool>,
    ) -> Result<EscalationPolicy, Box<dyn std::error::Error + Send + Sync>> {
        let api_key = self
            .config
            .api_key
            .as_deref()
            .ok_or("PagerDuty REST API key not configured")?;

        let url = format!("{}/escalation_policies/{}", PD_API_BASE, policy_id);

        let resp = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.pagerduty+json;version=2")
            .header("Authorization", format!("Token token={}", api_key))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!(
                "PagerDuty escalation policy fetch failed: {} — {}",
                status, body
            )
            .into());
        }

        let body: Value = resp.json().await?;
        let policy_value = body
            .get("escalation_policy")
            .ok_or("Missing 'escalation_policy' in response")?;

        let policy: EscalationPolicy = serde_json::from_value(policy_value.clone())?;

        // Persist to cache
        if let (Some(pool), Some(int_id)) = (pool, integration_id) {
            if let Err(e) = sqlx::query(
                "INSERT INTO pagerduty_escalation_policies
                    (integration_id, policy_id, policy_name, policy_json, fetched_at)
                 VALUES ($1, $2, $3, $4, NOW())
                 ON CONFLICT (integration_id, policy_id) DO UPDATE SET
                    policy_name = EXCLUDED.policy_name,
                    policy_json = EXCLUDED.policy_json,
                    fetched_at  = NOW()",
            )
            .bind(int_id)
            .bind(&policy.id)
            .bind(&policy.name)
            .bind(policy_value)
            .execute(pool)
            .await
            {
                warn!(error = %e, "Failed to cache escalation policy");
            }
        }

        Ok(policy)
    }

    /// List all escalation policies from the PagerDuty REST API.
    pub async fn list_escalation_policies(
        &self,
    ) -> Result<Vec<EscalationPolicy>, Box<dyn std::error::Error + Send + Sync>> {
        let api_key = self
            .config
            .api_key
            .as_deref()
            .ok_or("PagerDuty REST API key not configured")?;

        let url = format!("{}/escalation_policies", PD_API_BASE);
        let resp = self
            .client
            .get(&url)
            .header("Accept", "application/vnd.pagerduty+json;version=2")
            .header("Authorization", format!("Token token={}", api_key))
            .query(&[("limit", "100")])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!(
                "PagerDuty escalation policies list failed: {} — {}",
                status, body
            )
            .into());
        }

        let body: Value = resp.json().await?;
        let policies: Vec<EscalationPolicy> = body
            .get("escalation_policies")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        Ok(policies)
    }

    // -----------------------------------------------------------------------
    // On-call lookup
    // -----------------------------------------------------------------------

    /// Look up the current on-call user for the configured schedule.
    ///
    /// Uses the same [`crate::oncall::OnCallScheduler`] that the rest of the
    /// system uses so the result is shared and cached.
    pub async fn current_oncall(
        scheduler: &crate::oncall::OnCallScheduler,
    ) -> Option<crate::oncall::OnCallContact> {
        scheduler.current_oncall().await
    }

    // -----------------------------------------------------------------------
    // Auto-resolve
    // -----------------------------------------------------------------------

    /// Resolve all open incidents whose contract has not produced new events
    /// within `threshold_minutes`.
    pub async fn auto_resolve_stale_incidents(
        &self,
        pool: &sqlx::PgPool,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        if !self.config.auto_resolve {
            return Ok(0);
        }

        let threshold = self.config.auto_resolve_threshold_minutes;

        // Find stale triggered/acknowledged incidents
        let stale: Vec<(String, String)> = sqlx::query_as(
            "SELECT pi.dedup_key, pi.contract_id
             FROM pagerduty_incidents pi
             WHERE pi.status IN ('triggered', 'acknowledged')
               AND NOT EXISTS (
                   SELECT 1 FROM events e
                   WHERE e.contract_id = pi.contract_id
                     AND e.event_type   = pi.event_type
                     AND e.created_at   > NOW() - ($1::bigint * INTERVAL '1 minute')
               )",
        )
        .bind(threshold)
        .fetch_all(pool)
        .await?;

        let mut resolved = 0u64;
        for (dedup_key, contract_id) in &stale {
            match self.resolve_incident(dedup_key, Some(pool)).await {
                Ok(_) => {
                    resolved += 1;
                    info!(
                        dedup_key = %dedup_key,
                        contract_id = %contract_id,
                        "Auto-resolved stale PagerDuty incident"
                    );
                }
                Err(e) => {
                    error!(
                        error = %e,
                        dedup_key = %dedup_key,
                        contract_id = %contract_id,
                        "Failed to auto-resolve PagerDuty incident"
                    );
                }
            }
        }

        Ok(resolved)
    }

    // -----------------------------------------------------------------------
    // Filtering
    // -----------------------------------------------------------------------

    /// Returns `true` when `event` matches the configured contract/event-type
    /// filters and should produce an incident.
    pub fn should_trigger(&self, event: &SorobanEvent) -> bool {
        let contract_ok = self.config.contract_filter.is_empty()
            || self
                .config
                .contract_filter
                .contains(&event.contract_id);

        let type_ok = self.config.event_type_filter.is_empty()
            || self
                .config
                .event_type_filter
                .contains(&event.event_type.to_string());

        contract_ok && type_ok
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Canonical deduplication key for a (contract_id, event_type) pair.
    pub fn make_dedup_key(contract_id: &str, event_type: &str) -> String {
        format!("soroban-pulse-{}-{}", contract_id, event_type)
    }

    /// POST to the PagerDuty Events API with exponential back-off.
    async fn send_event(
        &self,
        payload: Value,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut backoff_ms = 1_000u64;

        for attempt in 1..=MAX_ATTEMPTS {
            match self
                .client
                .post(PD_EVENTS_URL)
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    let body: Value = resp.json().await?;
                    return Ok(body);
                }
                Ok(resp) => {
                    warn!(
                        status = %resp.status(),
                        body   = %resp.text().await.unwrap_or_default(),
                        attempt,
                        "PagerDuty API non-success response"
                    );
                }
                Err(e) => {
                    warn!(error = %e, attempt, "PagerDuty API request error");
                }
            }

            if attempt < MAX_ATTEMPTS {
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2;
            }
        }

        metrics::record_pagerduty_failure();
        Err(format!(
            "PagerDuty delivery failed after {} attempts",
            MAX_ATTEMPTS
        )
        .into())
    }

    /// Insert or update an incident row in the database.
    async fn upsert_incident(
        &self,
        dedup_key: &str,
        integration_id: Option<Uuid>,
        contract_id: &str,
        event_type: &str,
        incident_key: Option<&str>,
        status: &str,
        pool: &sqlx::PgPool,
    ) {
        if let Err(e) = sqlx::query(
            "INSERT INTO pagerduty_incidents
                 (dedup_key, integration_id, contract_id, event_type, incident_key, status)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (dedup_key) DO UPDATE SET
                 incident_key = COALESCE(EXCLUDED.incident_key, pagerduty_incidents.incident_key),
                 status       = EXCLUDED.status,
                 resolved_at  = CASE WHEN EXCLUDED.status = 'triggered' THEN NULL
                                     ELSE pagerduty_incidents.resolved_at END,
                 updated_at   = NOW()",
        )
        .bind(dedup_key)
        .bind(integration_id)
        .bind(contract_id)
        .bind(event_type)
        .bind(incident_key)
        .bind(status)
        .execute(pool)
        .await
        {
            error!(error = %e, dedup_key = %dedup_key, "Failed to upsert PagerDuty incident");
        })
    }
}

// ---------------------------------------------------------------------------
// Convenience top-level function
// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_client() -> PagerDutyClient {
        let mut severity_mapping = HashMap::new();
        severity_mapping.insert("contract".to_string(), "error".to_string());
        severity_mapping.insert("diagnostic".to_string(), "warning".to_string());
        severity_mapping.insert("system".to_string(), "info".to_string());

        PagerDutyClient::new(PagerDutyConfig {
            routing_key: "test-routing-key".to_string(),
            service_name: "Test Service".to_string(),
            api_key: Some("test-api-key".to_string()),
            escalation_policy_id: None,
            contract_filter: Vec::new(),
            event_type_filter: Vec::new(),
            severity_mapping,
            auto_resolve: true,
            auto_resolve_threshold_minutes: 30,
        })
        .unwrap()
    }

    fn make_event(contract_id: &str, event_type: &str) -> SorobanEvent {
        SorobanEvent {
            id: Uuid::new_v4(),
            contract_id: contract_id.to_string(),
            event_type: event_type.parse().unwrap_or_default(),
            tx_hash: "abc123".to_string(),
            ledger: 1_234_567,
            timestamp: chrono::Utc::now(),
            event_data: serde_json::json!({}),
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_client_creation() {
        let client = make_client();
        assert_eq!(client.config.routing_key, "test-routing-key");
        assert_eq!(client.config.service_name, "Test Service");
        assert!(client.config.auto_resolve);
    }

    #[test]
    fn test_make_dedup_key() {
        let key = PagerDutyClient::make_dedup_key("CABC", "contract");
        assert_eq!(key, "soroban-pulse-CABC-contract");
    }

    #[test]
    fn test_should_trigger_no_filter() {
        let client = make_client();
        let event = make_event("CABC", "contract");
        assert!(client.should_trigger(&event));
    }

    #[test]
    fn test_should_trigger_contract_filter_match() {
        let mut config = PagerDutyConfig::default();
        config.routing_key = "key".to_string();
        config.contract_filter = vec!["CABC".to_string()];
        let client = PagerDutyClient::new(config).unwrap();
        let event = make_event("CABC", "contract");
        assert!(client.should_trigger(&event));
    }

    #[test]
    fn test_should_trigger_contract_filter_no_match() {
        let mut config = PagerDutyConfig::default();
        config.routing_key = "key".to_string();
        config.contract_filter = vec!["COTHER".to_string()];
        let client = PagerDutyClient::new(config).unwrap();
        let event = make_event("CABC", "contract");
        assert!(!client.should_trigger(&event));
    }

    #[test]
    fn test_should_trigger_event_type_filter() {
        let mut config = PagerDutyConfig::default();
        config.routing_key = "key".to_string();
        config.event_type_filter = vec!["system".to_string()];
        let client = PagerDutyClient::new(config).unwrap();

        let event_match = make_event("CABC", "system");
        let event_miss = make_event("CABC", "contract");
        assert!(client.should_trigger(&event_match));
        assert!(!client.should_trigger(&event_miss));
    }

    #[test]
    fn test_severity_mapping() {
        let client = make_client();
        let sev = client
            .config
            .severity_mapping
            .get("contract")
            .unwrap()
            .as_str();
        assert_eq!(sev, "error");
        let sev2 = client
            .config
            .severity_mapping
            .get("diagnostic")
            .unwrap()
            .as_str();
        assert_eq!(sev2, "warning");
    }

    #[test]
    fn test_default_config() {
        let cfg = PagerDutyConfig::default();
        assert!(cfg.auto_resolve);
        assert_eq!(cfg.auto_resolve_threshold_minutes, 30);
        assert!(cfg.contract_filter.is_empty());
        assert!(cfg.event_type_filter.is_empty());
        assert!(!cfg.severity_mapping.is_empty());
    }

    #[test]
    fn test_escalation_policy_serde() {
        let policy = EscalationPolicy {
            id: "P1234".to_string(),
            name: "Default Policy".to_string(),
            description: Some("Test policy".to_string()),
            escalation_rules: vec![EscalationRule {
                id: "R1".to_string(),
                escalation_delay_in_minutes: 30,
                targets: vec![EscalationTarget {
                    id: "U1".to_string(),
                    target_type: "user_reference".to_string(),
                    name: Some("Alice".to_string()),
                }],
            }],
        };

        let json = serde_json::to_string(&policy).unwrap();
        let back: EscalationPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "P1234");
        assert_eq!(back.escalation_rules.len(), 1);
        assert_eq!(back.escalation_rules[0].targets[0].name.as_deref(), Some("Alice"));
    }

    #[test]
    fn test_dedup_key_unique_per_contract_type() {
        let k1 = PagerDutyClient::make_dedup_key("CA", "contract");
        let k2 = PagerDutyClient::make_dedup_key("CA", "system");
        let k3 = PagerDutyClient::make_dedup_key("CB", "contract");
        assert_ne!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k2, k3);
    }
}
