//! Data Integrity & Corruption Detection (Issue #823).
//!
//! Provides read-only checks over stored data:
//!   - Row-level format/constraint validation for `events`, re-checking the same
//!     rules enforced at ingestion time (contract_id/tx_hash format, ledger
//!     positivity, timestamp sanity, `event_data` shape). This is defense in
//!     depth against corruption introduced by paths that bypass application
//!     validation, such as replay jobs, logical replication, or manual fixups.
//!   - Format validation for `contract_id` values stored in the schema/ABI
//!     registry tables.
//!   - Ledger hash-chain continuity, reusing `ledger_hashes::verify_hash_chain`.
//!
//! Detected issues are reported, not silently repaired: a corrupted event
//! payload cannot be safely reconstructed from the database alone, so
//! `run_integrity_scan` never mutates data. Callers decide how to act on a
//! non-healthy `IntegrityReport` (page an operator, quarantine rows, etc.).

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

/// No legitimate Soroban event can predate the Stellar mainnet genesis ledger.
fn stellar_genesis() -> DateTime<Utc> {
    // 2015-09-30T00:00:00Z; the constant is always in range.
    DateTime::from_timestamp(1_443_571_200, 0).unwrap_or(DateTime::<Utc>::MIN_UTC)
}

/// Timestamps this far beyond "now" are treated as corrupt rather than clock skew.
const MAX_FUTURE_SKEW_MINUTES: i64 = 5;

/// Severity of a detected integrity issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegritySeverity {
    /// Data violates a hard invariant (malformed identifier, broken hash chain).
    Critical,
    /// Data is suspicious but not provably wrong (e.g. a borderline timestamp).
    Warning,
    Info,
}

/// A single detected integrity problem.
#[derive(Debug, Clone, Serialize)]
pub struct IntegrityIssue {
    pub table: String,
    pub check_name: String,
    pub severity: IntegritySeverity,
    pub description: String,
    pub row_identifier: Option<String>,
}

impl IntegrityIssue {
    fn new(
        table: &str,
        check_name: &str,
        severity: IntegritySeverity,
        description: impl Into<String>,
        row_identifier: Option<String>,
    ) -> Self {
        Self {
            table: table.to_string(),
            check_name: check_name.to_string(),
            severity,
            description: description.into(),
            row_identifier,
        }
    }
}

/// Result of running one or more integrity checks.
#[derive(Debug, Clone, Serialize)]
pub struct IntegrityReport {
    pub generated_at: DateTime<Utc>,
    pub tables_checked: Vec<String>,
    pub rows_checked: u64,
    pub issues: Vec<IntegrityIssue>,
}

impl IntegrityReport {
    pub fn critical_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == IntegritySeverity::Critical)
            .count()
    }

    pub fn warning_count(&self) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == IntegritySeverity::Warning)
            .count()
    }

    /// A report is healthy if it contains no critical issues. Warnings alone
    /// do not indicate corruption — they flag data worth a human look.
    pub fn is_healthy(&self) -> bool {
        self.critical_count() == 0
    }

    pub fn summary(&self) -> String {
        format!(
            "{} table(s) checked, {} row(s) scanned, {} critical / {} warning issue(s) found",
            self.tables_checked.len(),
            self.rows_checked,
            self.critical_count(),
            self.warning_count()
        )
    }
}

/// A minimal, DB-shape-independent view of an `events` row used for validation.
#[derive(Debug, Clone)]
pub struct EventRecordRef<'a> {
    pub id: &'a str,
    pub contract_id: &'a str,
    pub tx_hash: &'a str,
    pub ledger: i64,
    pub timestamp: DateTime<Utc>,
    pub event_data: &'a Value,
}

fn is_valid_contract_id(contract_id: &str) -> bool {
    contract_id.len() == 56
        && contract_id.starts_with('C')
        && contract_id.chars().all(|c| c.is_ascii_alphanumeric())
}

fn is_valid_tx_hash(tx_hash: &str) -> bool {
    tx_hash.len() == 64 && tx_hash.chars().all(|c| c.is_ascii_hexdigit())
}

/// Re-validate a single event row against the same rules enforced at ingestion
/// time. Returns one `IntegrityIssue` per violated rule (a single row can
/// surface multiple issues).
pub fn validate_event_record(record: &EventRecordRef<'_>) -> Vec<IntegrityIssue> {
    let mut issues = Vec::new();
    let row_id = || Some(record.id.to_string());

    if !is_valid_contract_id(record.contract_id) {
        issues.push(IntegrityIssue::new(
            "events",
            "contract_id_format",
            IntegritySeverity::Critical,
            format!(
                "contract_id '{}' is not a valid 56-character Strkey",
                record.contract_id
            ),
            row_id(),
        ));
    }

    if !is_valid_tx_hash(record.tx_hash) {
        issues.push(IntegrityIssue::new(
            "events",
            "tx_hash_format",
            IntegritySeverity::Critical,
            format!(
                "tx_hash '{}' is not a 64-character hex digest",
                record.tx_hash
            ),
            row_id(),
        ));
    }

    if record.ledger <= 0 {
        issues.push(IntegrityIssue::new(
            "events",
            "non_positive_ledger",
            IntegritySeverity::Critical,
            format!("ledger {} must be positive", record.ledger),
            row_id(),
        ));
    }

    let now = Utc::now();
    if record.timestamp < stellar_genesis() {
        issues.push(IntegrityIssue::new(
            "events",
            "timestamp_before_genesis",
            IntegritySeverity::Critical,
            format!(
                "timestamp {} predates the Stellar mainnet genesis",
                record.timestamp
            ),
            row_id(),
        ));
    } else if record.timestamp > now + Duration::minutes(MAX_FUTURE_SKEW_MINUTES) {
        issues.push(IntegrityIssue::new(
            "events",
            "timestamp_in_future",
            IntegritySeverity::Warning,
            format!(
                "timestamp {} is more than {} minutes ahead of now ({})",
                record.timestamp, MAX_FUTURE_SKEW_MINUTES, now
            ),
            row_id(),
        ));
    }

    match record.event_data {
        Value::Object(map) => {
            if let Some(v) = map.get("value") {
                if !v.is_null() && !v.is_object() {
                    issues.push(IntegrityIssue::new(
                        "events",
                        "event_data_value_shape",
                        IntegritySeverity::Critical,
                        "event_data.value must be an object or null",
                        row_id(),
                    ));
                }
            }
            if let Some(t) = map.get("topic") {
                if !t.is_null() && !t.is_array() {
                    issues.push(IntegrityIssue::new(
                        "events",
                        "event_data_topic_shape",
                        IntegritySeverity::Critical,
                        "event_data.topic must be an array or null",
                        row_id(),
                    ));
                }
            }
        }
        other => {
            issues.push(IntegrityIssue::new(
                "events",
                "event_data_not_object",
                IntegritySeverity::Critical,
                format!("event_data must be a JSON object, found {:?}", other),
                row_id(),
            ));
        }
    }

    issues
}

/// Validate a `contract_id` stored in a schema/ABI registry table against the
/// same Strkey format enforced for `events.contract_id`.
pub fn validate_registry_contract_id(table: &str, contract_id: &str) -> Option<IntegrityIssue> {
    if is_valid_contract_id(contract_id) {
        None
    } else {
        Some(IntegrityIssue::new(
            table,
            "contract_id_format",
            IntegritySeverity::Warning,
            format!(
                "contract_id '{}' is not a valid 56-character Strkey",
                contract_id
            ),
            Some(contract_id.to_string()),
        ))
    }
}

/// Run a bounded integrity scan: ledger hash-chain continuity, format
/// validation over the most recently indexed events, and format validation
/// over registered contract schemas/ABIs.
///
/// This is read-only. No automatic repair is attempted — see the module docs
/// for why blind repair of event data would be unsafe.
pub async fn run_integrity_scan(
    pool: &PgPool,
    recent_events_limit: i64,
) -> Result<IntegrityReport, sqlx::Error> {
    let mut issues = Vec::new();
    let mut tables_checked = Vec::new();
    let mut rows_checked: u64 = 0;

    // 1. Hash-chain continuity (checksum verification) over the last 1,000 ledgers.
    let latest_ledger: Option<i64> = sqlx::query_scalar("SELECT MAX(ledger) FROM ledger_hashes")
        .fetch_one(pool)
        .await?;
    if let Some(latest) = latest_ledger {
        tables_checked.push("ledger_hashes".to_string());
        let from = (latest as u64).saturating_sub(1_000);
        let mismatches = crate::ledger_hashes::verify_hash_chain(pool, from, latest as u64).await?;
        if mismatches > 0 {
            issues.push(IntegrityIssue::new(
                "ledger_hashes",
                "hash_chain_continuity",
                IntegritySeverity::Critical,
                format!(
                    "{} broken link(s) in the ledger hash chain between {} and {}",
                    mismatches, from, latest
                ),
                None,
            ));
        }
    }

    // 2. Structural validation of the most recently indexed events.
    let rows: Vec<(Uuid, String, String, i64, DateTime<Utc>, Value)> = sqlx::query_as(
        "SELECT id, contract_id, tx_hash, ledger, timestamp, event_data
         FROM events
         ORDER BY ledger DESC
         LIMIT $1",
    )
    .bind(recent_events_limit)
    .fetch_all(pool)
    .await?;

    if !rows.is_empty() {
        tables_checked.push("events".to_string());
    }
    for (id, contract_id, tx_hash, ledger, timestamp, event_data) in &rows {
        let id_str = id.to_string();
        let record = EventRecordRef {
            id: &id_str,
            contract_id,
            tx_hash,
            ledger: *ledger,
            timestamp: *timestamp,
            event_data,
        };
        issues.extend(validate_event_record(&record));
        rows_checked += 1;
    }

    // 3. Format validation for contract schema/ABI registry entries.
    let schema_ids: Vec<(String,)> = sqlx::query_as("SELECT contract_id FROM contract_schemas")
        .fetch_all(pool)
        .await?;
    if !schema_ids.is_empty() {
        tables_checked.push("contract_schemas".to_string());
    }
    for (contract_id,) in &schema_ids {
        rows_checked += 1;
        if let Some(issue) = validate_registry_contract_id("contract_schemas", contract_id) {
            issues.push(issue);
        }
    }

    let abi_ids: Vec<(String,)> = sqlx::query_as("SELECT contract_id FROM contract_abis")
        .fetch_all(pool)
        .await?;
    if !abi_ids.is_empty() {
        tables_checked.push("contract_abis".to_string());
    }
    for (contract_id,) in &abi_ids {
        rows_checked += 1;
        if let Some(issue) = validate_registry_contract_id("contract_abis", contract_id) {
            issues.push(issue);
        }
    }

    Ok(IntegrityReport {
        generated_at: Utc::now(),
        tables_checked,
        rows_checked,
        issues,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn valid_record<'a>(id: &'a str, event_data: &'a Value) -> EventRecordRef<'a> {
        EventRecordRef {
            id,
            contract_id: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM",
            tx_hash: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            ledger: 1000,
            timestamp: Utc::now(),
            event_data,
        }
    }

    #[test]
    fn validate_event_record_accepts_valid_record() {
        let data = json!({"value": {"amount": 1}, "topic": [{"sym": "swap"}]});
        let record = valid_record("id-1", &data);
        assert!(validate_event_record(&record).is_empty());
    }

    #[test]
    fn validate_event_record_accepts_null_value_and_topic() {
        let data = json!({"value": null, "topic": null});
        let record = valid_record("id-2", &data);
        assert!(validate_event_record(&record).is_empty());
    }

    #[test]
    fn validate_event_record_flags_bad_contract_id() {
        let data = json!({});
        let mut record = valid_record("id-3", &data);
        record.contract_id = "not-a-real-contract-id";
        let issues = validate_event_record(&record);
        assert!(issues.iter().any(|i| i.check_name == "contract_id_format"));
        assert_eq!(issues[0].severity, IntegritySeverity::Critical);
    }

    #[test]
    fn validate_event_record_flags_bad_tx_hash() {
        let data = json!({});
        let mut record = valid_record("id-4", &data);
        record.tx_hash = "too-short";
        let issues = validate_event_record(&record);
        assert!(issues.iter().any(|i| i.check_name == "tx_hash_format"));
    }

    #[test]
    fn validate_event_record_flags_non_positive_ledger() {
        let data = json!({});
        let mut record = valid_record("id-5", &data);
        record.ledger = 0;
        let issues = validate_event_record(&record);
        assert!(issues.iter().any(|i| i.check_name == "non_positive_ledger"));
    }

    #[test]
    fn validate_event_record_flags_pre_genesis_timestamp() {
        let data = json!({});
        let mut record = valid_record("id-6", &data);
        record.timestamp = Utc.with_ymd_and_hms(2010, 1, 1, 0, 0, 0).unwrap();
        let issues = validate_event_record(&record);
        assert!(issues
            .iter()
            .any(|i| i.check_name == "timestamp_before_genesis"
                && i.severity == IntegritySeverity::Critical));
    }

    #[test]
    fn validate_event_record_flags_future_timestamp_as_warning() {
        let data = json!({});
        let mut record = valid_record("id-7", &data);
        record.timestamp = Utc::now() + Duration::hours(1);
        let issues = validate_event_record(&record);
        assert!(issues
            .iter()
            .any(|i| i.check_name == "timestamp_in_future"
                && i.severity == IntegritySeverity::Warning));
    }

    #[test]
    fn validate_event_record_flags_non_object_value() {
        let data = json!({"value": "not-an-object", "topic": null});
        let record = valid_record("id-8", &data);
        let issues = validate_event_record(&record);
        assert!(issues
            .iter()
            .any(|i| i.check_name == "event_data_value_shape"));
    }

    #[test]
    fn validate_event_record_flags_non_array_topic() {
        let data = json!({"value": null, "topic": "not-an-array"});
        let record = valid_record("id-9", &data);
        let issues = validate_event_record(&record);
        assert!(issues
            .iter()
            .any(|i| i.check_name == "event_data_topic_shape"));
    }

    #[test]
    fn validate_event_record_flags_non_object_event_data() {
        let data = json!("just a string");
        let record = valid_record("id-10", &data);
        let issues = validate_event_record(&record);
        assert!(issues
            .iter()
            .any(|i| i.check_name == "event_data_not_object"));
    }

    #[test]
    fn validate_registry_contract_id_accepts_valid() {
        assert!(validate_registry_contract_id(
            "contract_schemas",
            "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM"
        )
        .is_none());
    }

    #[test]
    fn validate_registry_contract_id_flags_invalid() {
        let issue = validate_registry_contract_id("contract_abis", "bad-id").unwrap();
        assert_eq!(issue.table, "contract_abis");
        assert_eq!(issue.severity, IntegritySeverity::Warning);
    }

    #[test]
    fn integrity_report_is_healthy_with_no_critical_issues() {
        let report = IntegrityReport {
            generated_at: Utc::now(),
            tables_checked: vec!["events".to_string()],
            rows_checked: 10,
            issues: vec![IntegrityIssue::new(
                "events",
                "timestamp_in_future",
                IntegritySeverity::Warning,
                "borderline",
                None,
            )],
        };
        assert!(report.is_healthy());
        assert_eq!(report.critical_count(), 0);
        assert_eq!(report.warning_count(), 1);
    }

    #[test]
    fn integrity_report_unhealthy_with_critical_issue() {
        let report = IntegrityReport {
            generated_at: Utc::now(),
            tables_checked: vec!["events".to_string()],
            rows_checked: 1,
            issues: vec![IntegrityIssue::new(
                "events",
                "tx_hash_format",
                IntegritySeverity::Critical,
                "bad hash",
                Some("id-1".to_string()),
            )],
        };
        assert!(!report.is_healthy());
        assert_eq!(report.critical_count(), 1);
    }

    #[test]
    fn integrity_report_summary_contains_counts() {
        let report = IntegrityReport {
            generated_at: Utc::now(),
            tables_checked: vec!["events".to_string(), "contract_schemas".to_string()],
            rows_checked: 42,
            issues: vec![],
        };
        let summary = report.summary();
        assert!(summary.contains("2 table"));
        assert!(summary.contains("42 row"));
        assert!(summary.contains("0 critical"));
    }
}
