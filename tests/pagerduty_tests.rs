//! Comprehensive tests for the PagerDuty integration — Issue #951
//!
//! Tests cover:
//! - Client construction and configuration
//! - Deduplication key generation
//! - Event filtering (contract and event-type filters)
//! - Severity mapping
//! - Escalation policy serialization/deserialization
//! - Auto-resolve configuration
//! - `should_trigger` skips filtered events
//! - On-call scheduler creation (unit test — no network)
//! - `PagerDutyConfig` default values

use std::collections::HashMap;

use soroban_pulse::pagerduty::{
    EscalationPolicy, EscalationRule, EscalationTarget, PagerDutyClient, PagerDutyConfig,
};
use soroban_pulse::models::SorobanEvent;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn default_severity() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("contract".to_string(), "error".to_string());
    m.insert("diagnostic".to_string(), "warning".to_string());
    m.insert("system".to_string(), "info".to_string());
    m
}

fn make_client_with(
    contract_filter: Vec<String>,
    event_type_filter: Vec<String>,
) -> PagerDutyClient {
    PagerDutyClient::new(PagerDutyConfig {
        routing_key: "r-key".to_string(),
        service_name: "Test".to_string(),
        api_key: None,
        escalation_policy_id: None,
        contract_filter,
        event_type_filter,
        severity_mapping: default_severity(),
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
        tx_hash: "deadbeef".to_string(),
        ledger: 999_999,
        timestamp: chrono::Utc::now(),
        event_data: serde_json::json!({"amount": 1000}),
        created_at: chrono::Utc::now(),
    }
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

#[test]
fn client_stores_routing_key_and_service_name() {
    let client = make_client_with(vec![], vec![]);
    assert_eq!(client.config.routing_key, "r-key");
    assert_eq!(client.config.service_name, "Test");
}

#[test]
fn default_config_has_sensible_values() {
    let cfg = PagerDutyConfig::default();
    assert!(cfg.auto_resolve, "auto_resolve should default to true");
    assert_eq!(cfg.auto_resolve_threshold_minutes, 30);
    assert!(cfg.contract_filter.is_empty());
    assert!(cfg.event_type_filter.is_empty());
    assert!(!cfg.severity_mapping.is_empty());
}

// ---------------------------------------------------------------------------
// Deduplication key
// ---------------------------------------------------------------------------

#[test]
fn dedup_key_format() {
    let key = PagerDutyClient::make_dedup_key("CABC1234", "contract");
    assert_eq!(key, "soroban-pulse-CABC1234-contract");
}

#[test]
fn dedup_keys_differ_by_contract_id() {
    let k1 = PagerDutyClient::make_dedup_key("CA", "contract");
    let k2 = PagerDutyClient::make_dedup_key("CB", "contract");
    assert_ne!(k1, k2);
}

#[test]
fn dedup_keys_differ_by_event_type() {
    let k1 = PagerDutyClient::make_dedup_key("CA", "contract");
    let k2 = PagerDutyClient::make_dedup_key("CA", "diagnostic");
    assert_ne!(k1, k2);
}

#[test]
fn dedup_key_is_deterministic() {
    let k1 = PagerDutyClient::make_dedup_key("CA", "system");
    let k2 = PagerDutyClient::make_dedup_key("CA", "system");
    assert_eq!(k1, k2);
}

// ---------------------------------------------------------------------------
// Filtering — should_trigger
// ---------------------------------------------------------------------------

#[test]
fn should_trigger_with_no_filters() {
    let client = make_client_with(vec![], vec![]);
    assert!(client.should_trigger(&make_event("ANY", "contract")));
    assert!(client.should_trigger(&make_event("ANY", "system")));
}

#[test]
fn should_trigger_contract_filter_match() {
    let client = make_client_with(vec!["CABC".to_string()], vec![]);
    assert!(client.should_trigger(&make_event("CABC", "contract")));
}

#[test]
fn should_trigger_contract_filter_no_match() {
    let client = make_client_with(vec!["COTHER".to_string()], vec![]);
    assert!(!client.should_trigger(&make_event("CABC", "contract")));
}

#[test]
fn should_trigger_event_type_filter_match() {
    let client = make_client_with(vec![], vec!["system".to_string()]);
    assert!(client.should_trigger(&make_event("ANY", "system")));
}

#[test]
fn should_trigger_event_type_filter_no_match() {
    let client = make_client_with(vec![], vec!["system".to_string()]);
    assert!(!client.should_trigger(&make_event("ANY", "contract")));
}

#[test]
fn should_trigger_both_filters_must_match() {
    let client = make_client_with(
        vec!["CABC".to_string()],
        vec!["diagnostic".to_string()],
    );
    // Both match
    assert!(client.should_trigger(&make_event("CABC", "diagnostic")));
    // Contract matches, type doesn't
    assert!(!client.should_trigger(&make_event("CABC", "contract")));
    // Type matches, contract doesn't
    assert!(!client.should_trigger(&make_event("COTHER", "diagnostic")));
    // Neither matches
    assert!(!client.should_trigger(&make_event("COTHER", "contract")));
}

#[test]
fn should_trigger_multiple_contract_ids() {
    let client = make_client_with(
        vec!["CA1".to_string(), "CA2".to_string(), "CA3".to_string()],
        vec![],
    );
    assert!(client.should_trigger(&make_event("CA1", "contract")));
    assert!(client.should_trigger(&make_event("CA2", "contract")));
    assert!(client.should_trigger(&make_event("CA3", "contract")));
    assert!(!client.should_trigger(&make_event("CA4", "contract")));
}

// ---------------------------------------------------------------------------
// Severity mapping
// ---------------------------------------------------------------------------

#[test]
fn severity_maps_contract_to_error() {
    let client = make_client_with(vec![], vec![]);
    assert_eq!(
        client.config.severity_mapping.get("contract").map(String::as_str),
        Some("error")
    );
}

#[test]
fn severity_maps_diagnostic_to_warning() {
    let client = make_client_with(vec![], vec![]);
    assert_eq!(
        client.config.severity_mapping.get("diagnostic").map(String::as_str),
        Some("warning")
    );
}

#[test]
fn severity_maps_system_to_info() {
    let client = make_client_with(vec![], vec![]);
    assert_eq!(
        client.config.severity_mapping.get("system").map(String::as_str),
        Some("info")
    );
}

#[test]
fn custom_severity_mapping_is_stored() {
    let mut custom = HashMap::new();
    custom.insert("contract".to_string(), "critical".to_string());

    let client = PagerDutyClient::new(PagerDutyConfig {
        routing_key: "key".to_string(),
        severity_mapping: custom,
        ..PagerDutyConfig::default()
    })
    .unwrap();

    assert_eq!(
        client.config.severity_mapping.get("contract").map(String::as_str),
        Some("critical")
    );
}

// ---------------------------------------------------------------------------
// Escalation policy serialization
// ---------------------------------------------------------------------------

#[test]
fn escalation_policy_round_trips_via_json() {
    let policy = EscalationPolicy {
        id: "P12345".to_string(),
        name: "Critical Path".to_string(),
        description: Some("Escalate critical soroban alerts".to_string()),
        escalation_rules: vec![
            EscalationRule {
                id: "R1".to_string(),
                escalation_delay_in_minutes: 5,
                targets: vec![EscalationTarget {
                    id: "U1".to_string(),
                    target_type: "user_reference".to_string(),
                    name: Some("Alice".to_string()),
                }],
            },
            EscalationRule {
                id: "R2".to_string(),
                escalation_delay_in_minutes: 30,
                targets: vec![EscalationTarget {
                    id: "S1".to_string(),
                    target_type: "schedule_reference".to_string(),
                    name: Some("On-Call Schedule".to_string()),
                }],
            },
        ],
    };

    let json = serde_json::to_string(&policy).expect("serialize");
    let back: EscalationPolicy = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(back.id, "P12345");
    assert_eq!(back.name, "Critical Path");
    assert_eq!(back.escalation_rules.len(), 2);
    assert_eq!(back.escalation_rules[0].escalation_delay_in_minutes, 5);
    assert_eq!(back.escalation_rules[1].escalation_delay_in_minutes, 30);
    assert_eq!(
        back.escalation_rules[0].targets[0].name.as_deref(),
        Some("Alice")
    );
}

#[test]
fn escalation_target_type_is_preserved() {
    let target = EscalationTarget {
        id: "S1".to_string(),
        target_type: "schedule_reference".to_string(),
        name: None,
    };
    let json = serde_json::to_string(&target).unwrap();
    let back: EscalationTarget = serde_json::from_str(&json).unwrap();
    assert_eq!(back.target_type, "schedule_reference");
    assert!(back.name.is_none());
}

// ---------------------------------------------------------------------------
// Auto-resolve configuration
// ---------------------------------------------------------------------------

#[test]
fn auto_resolve_defaults_to_true() {
    let cfg = PagerDutyConfig::default();
    assert!(cfg.auto_resolve);
}

#[test]
fn auto_resolve_threshold_defaults_to_30() {
    let cfg = PagerDutyConfig::default();
    assert_eq!(cfg.auto_resolve_threshold_minutes, 30);
}

#[test]
fn auto_resolve_can_be_disabled() {
    let client = PagerDutyClient::new(PagerDutyConfig {
        routing_key: "key".to_string(),
        auto_resolve: false,
        ..PagerDutyConfig::default()
    })
    .unwrap();
    assert!(!client.config.auto_resolve);
}

// ---------------------------------------------------------------------------
// Configuration from application config
// ---------------------------------------------------------------------------

#[test]
fn from_app_config_returns_none_when_routing_key_absent() {
    let config = soroban_pulse::config::Config::default();
    // Default config has no pagerduty_routing_key
    let client = PagerDutyClient::from_app_config(&config);
    assert!(client.is_none());
}

// ---------------------------------------------------------------------------
// Incident deduplication — same contract+type yields same key
// ---------------------------------------------------------------------------

#[test]
fn same_event_twice_produces_same_dedup_key() {
    let e1 = make_event("CABC", "contract");
    let e2 = make_event("CABC", "contract");
    let k1 = PagerDutyClient::make_dedup_key(&e1.contract_id, &e1.event_type.to_string());
    let k2 = PagerDutyClient::make_dedup_key(&e2.contract_id, &e2.event_type.to_string());
    assert_eq!(k1, k2, "Two events with same contract/type must share dedup key");
}

#[test]
fn different_contracts_never_share_dedup_key() {
    let contracts = ["CA1", "CA2", "CA3", "CA4", "CA5"];
    let keys: Vec<String> = contracts
        .iter()
        .map(|c| PagerDutyClient::make_dedup_key(c, "contract"))
        .collect();

    for i in 0..keys.len() {
        for j in (i + 1)..keys.len() {
            assert_ne!(
                keys[i], keys[j],
                "Contracts {} and {} must not share a dedup key",
                contracts[i], contracts[j]
            );
        }
    }
}
