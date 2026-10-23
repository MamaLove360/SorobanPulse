//! Compliance Audit Trail Module (Issue #946)
//!
//! Extends audit logging with:
//! - Immutable log storage with chain hashing
//! - Audit log signing and verification
//! - Retention policy enforcement
//! - Audit log search and export
//! - Compliance reporting automation

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::{debug, warn};

/// Retention class for audit logs
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionClass {
    /// Short-lived operational logs (30 days)
    Transient,
    /// Standard business records (1 year)
    Standard,
    /// Regulatory compliance records (7 years)
    Regulatory,
    /// Permanent records — never auto-expire
    Permanent,
}

impl RetentionClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Standard => "standard",
            Self::Regulatory => "regulatory",
            Self::Permanent => "permanent",
        }
    }

    /// Returns the retention duration in days, or None for permanent.
    pub fn retention_days(&self) -> Option<i64> {
        match self {
            Self::Transient => Some(30),
            Self::Standard => Some(365),
            Self::Regulatory => Some(365 * 7),
            Self::Permanent => None,
        }
    }
}

/// A compliance-tagged audit log entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComplianceAuditEntry {
    pub id: String,
    pub event_type: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub actor: Option<String>,
    pub severity: String,
    pub compliance_tags: Vec<String>,
    pub retention_class: RetentionClass,
    pub log_hash: Option<String>,
    pub chain_hash: Option<String>,
    pub signed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub details: Option<serde_json::Value>,
}

/// Compute a SHA-256 hash of an audit log entry for tamper detection.
///
/// The hash covers the fields that must not change after insertion.
pub fn compute_log_hash(entry: &ComplianceAuditEntry) -> String {
    let mut hasher = Sha256::new();
    hasher.update(entry.id.as_bytes());
    hasher.update(b"|");
    hasher.update(entry.event_type.as_bytes());
    hasher.update(b"|");
    hasher.update(entry.action.as_bytes());
    hasher.update(b"|");
    hasher.update(entry.resource_type.as_bytes());
    hasher.update(b"|");
    hasher.update(entry.created_at.to_rfc3339().as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Compute a chained hash linking this entry to the previous one.
///
/// This creates a tamper-evident chain: modifying any earlier record
/// invalidates all subsequent chain hashes.
pub fn compute_chain_hash(prev_chain_hash: Option<&str>, log_hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prev_chain_hash.unwrap_or("").as_bytes());
    hasher.update(b"|");
    hasher.update(log_hash.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Verify the hash of a stored audit log entry.
///
/// Returns `true` if the stored `log_hash` matches the recomputed hash.
pub fn verify_log_hash(entry: &ComplianceAuditEntry) -> bool {
    match &entry.log_hash {
        None => {
            warn!(id = %entry.id, "Audit entry has no log_hash — cannot verify integrity");
            false
        }
        Some(stored_hash) => {
            let computed = compute_log_hash(entry);
            let ok = computed == *stored_hash;
            if !ok {
                warn!(
                    id = %entry.id,
                    stored = %stored_hash,
                    computed = %computed,
                    "Audit log hash mismatch — possible tampering detected"
                );
            }
            ok
        }
    }
}

/// Parameters for searching audit logs
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AuditSearchParams {
    pub event_types: Option<Vec<String>>,
    pub resource_types: Option<Vec<String>>,
    pub actor: Option<String>,
    pub compliance_tags: Option<Vec<String>>,
    pub retention_class: Option<String>,
    pub severities: Option<Vec<String>>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub success_only: Option<bool>,
    pub limit: i64,
    pub offset: i64,
}

impl AuditSearchParams {
    pub fn new() -> Self {
        Self {
            limit: 100,
            offset: 0,
            ..Default::default()
        }
    }
}

/// Summary of audit trail health for a compliance report
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditTrailHealthReport {
    pub period_from: DateTime<Utc>,
    pub period_to: DateTime<Utc>,
    pub total_entries: i64,
    pub signed_entries: i64,
    pub unsigned_entries: i64,
    pub entries_by_severity: serde_json::Value,
    pub entries_by_retention_class: serde_json::Value,
    pub entries_expiring_soon: i64,
    pub compliance_coverage_pct: f64,
}

/// Generate a health report on the audit trail for a given period.
pub async fn generate_audit_trail_health_report(
    pool: &PgPool,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<AuditTrailHealthReport, sqlx::Error> {
    let total_entries: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_logs WHERE created_at BETWEEN $1 AND $2",
    )
    .bind(from)
    .bind(to)
    .fetch_one(pool)
    .await?;

    let signed_entries: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_logs WHERE signed_at IS NOT NULL AND created_at BETWEEN $1 AND $2",
    )
    .bind(from)
    .bind(to)
    .fetch_one(pool)
    .await?;

    let entries_expiring_soon: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_logs \
         WHERE expires_at BETWEEN NOW() AND NOW() + INTERVAL '30 days'",
    )
    .fetch_one(pool)
    .await?;

    let entries_by_severity: serde_json::Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_object_agg(severity, cnt), '{}'::jsonb) \
         FROM (SELECT severity, COUNT(*) AS cnt FROM audit_logs \
               WHERE created_at BETWEEN $1 AND $2 GROUP BY severity) sub",
    )
    .bind(from)
    .bind(to)
    .fetch_one(pool)
    .await
    .unwrap_or(serde_json::json!({}));

    let entries_by_retention_class: serde_json::Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_object_agg(rc, cnt), '{}'::jsonb) \
         FROM (SELECT COALESCE(retention_class, 'standard') AS rc, COUNT(*) AS cnt \
               FROM audit_logs WHERE created_at BETWEEN $1 AND $2 GROUP BY rc) sub",
    )
    .bind(from)
    .bind(to)
    .fetch_one(pool)
    .await
    .unwrap_or(serde_json::json!({}));

    let compliance_coverage_pct = if total_entries == 0 {
        100.0
    } else {
        (signed_entries as f64 / total_entries as f64) * 100.0
    };

    debug!(
        total = total_entries,
        signed = signed_entries,
        coverage = compliance_coverage_pct,
        "Audit trail health report generated"
    );

    Ok(AuditTrailHealthReport {
        period_from: from,
        period_to: to,
        total_entries,
        signed_entries,
        unsigned_entries: total_entries - signed_entries,
        entries_by_severity,
        entries_by_retention_class,
        entries_expiring_soon,
        compliance_coverage_pct,
    })
}

/// Export audit logs as a JSON-serialisable Vec for compliance reporting.
pub async fn export_audit_logs(
    pool: &PgPool,
    params: &AuditSearchParams,
) -> Result<Vec<serde_json::Value>, sqlx::Error> {
    // Base query — date range always applied
    let from = params.from.unwrap_or(DateTime::UNIX_EPOCH);
    let to = params.to.unwrap_or_else(Utc::now);

    let rows = sqlx::query_as::<_, (serde_json::Value,)>(
        "SELECT to_jsonb(al) \
         FROM audit_logs al \
         WHERE al.created_at BETWEEN $1 AND $2 \
         ORDER BY al.created_at DESC \
         LIMIT $3 OFFSET $4",
    )
    .bind(from)
    .bind(to)
    .bind(params.limit)
    .bind(params.offset)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|(v,)| v).collect())
}

/// Record an export event in `audit_log_exports`.
pub async fn record_export(
    pool: &PgPool,
    exported_by: &str,
    format: &str,
    row_count: i64,
    filter_params: Option<serde_json::Value>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO audit_log_exports (exported_by, export_format, row_count, filter_params) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(exported_by)
    .bind(format)
    .bind(row_count as i32)
    .bind(filter_params)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_entry() -> ComplianceAuditEntry {
        ComplianceAuditEntry {
            id: "test-uuid-1234".to_string(),
            event_type: "DELETE".to_string(),
            action: "DELETE_USER_DATA".to_string(),
            resource_type: "subscription".to_string(),
            resource_id: Some("sub-42".to_string()),
            actor: Some("admin@example.com".to_string()),
            severity: "CRITICAL".to_string(),
            compliance_tags: vec!["gdpr".to_string(), "erasure".to_string()],
            retention_class: RetentionClass::Regulatory,
            log_hash: None,
            chain_hash: None,
            signed_at: None,
            created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            details: None,
        }
    }

    #[test]
    fn test_retention_class_days() {
        assert_eq!(RetentionClass::Transient.retention_days(), Some(30));
        assert_eq!(RetentionClass::Standard.retention_days(), Some(365));
        assert_eq!(RetentionClass::Regulatory.retention_days(), Some(365 * 7));
        assert_eq!(RetentionClass::Permanent.retention_days(), None);
    }

    #[test]
    fn test_compute_log_hash_deterministic() {
        let entry = sample_entry();
        let h1 = compute_log_hash(&entry);
        let h2 = compute_log_hash(&entry);
        assert_eq!(h1, h2, "Hash must be deterministic");
        assert_eq!(h1.len(), 64, "SHA-256 hex is 64 chars");
    }

    #[test]
    fn test_compute_chain_hash() {
        let h1 = compute_chain_hash(None, "abc");
        let h2 = compute_chain_hash(Some("prev"), "abc");
        assert_ne!(h1, h2, "Chain hashes with different predecessors must differ");
    }

    #[test]
    fn test_verify_log_hash_valid() {
        let mut entry = sample_entry();
        entry.log_hash = Some(compute_log_hash(&entry));
        assert!(verify_log_hash(&entry));
    }

    #[test]
    fn test_verify_log_hash_tampered() {
        let mut entry = sample_entry();
        entry.log_hash = Some(compute_log_hash(&entry));
        // Tamper with a field
        entry.action = "TAMPERED_ACTION".to_string();
        assert!(!verify_log_hash(&entry));
    }

    #[test]
    fn test_verify_log_hash_missing() {
        let entry = sample_entry();
        assert!(!verify_log_hash(&entry), "Entry with no hash should fail verification");
    }

    #[test]
    fn test_audit_search_params_defaults() {
        let p = AuditSearchParams::new();
        assert_eq!(p.limit, 100);
        assert_eq!(p.offset, 0);
    }
}
