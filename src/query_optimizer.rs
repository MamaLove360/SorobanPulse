//! Issue #818: Query Optimization & Execution Plan Analysis
//!
//! Provides automatic EXPLAIN analysis, a query hints system, slow-query
//! tracking, index-usage detection, and an optimization dashboard.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{debug, info, warn};

// ── Slow query tracker ────────────────────────────────────────────────────

/// One recorded slow-query event.
#[derive(Debug, Clone, Serialize)]
pub struct SlowQueryRecord {
    pub query_fingerprint: String,
    pub sample_query: String,
    pub duration_ms: f64,
    pub recorded_at: DateTime<Utc>,
    pub call_count: u64,
    pub total_duration_ms: f64,
    pub avg_duration_ms: f64,
    pub max_duration_ms: f64,
}

/// Ring-buffer backed store for slow queries (top-N by avg latency).
#[derive(Debug, Default)]
pub struct SlowQueryTracker {
    records: RwLock<HashMap<String, SlowQueryRecord>>,
    threshold_ms: f64,
}

impl SlowQueryTracker {
    pub fn new(threshold_ms: f64) -> Arc<Self> {
        Arc::new(Self {
            records: RwLock::new(HashMap::new()),
            threshold_ms,
        })
    }

    /// Record a query execution. Only persists if duration exceeds threshold.
    pub fn record(&self, query: &str, duration: Duration) {
        let ms = duration.as_secs_f64() * 1000.0;
        if ms < self.threshold_ms {
            return;
        }
        let fp = fingerprint(query);
        let mut store = self.records.write().unwrap_or_else(|e| e.into_inner());
        let entry = store.entry(fp.clone()).or_insert_with(|| SlowQueryRecord {
            query_fingerprint: fp,
            sample_query: truncate(query, 500),
            duration_ms: ms,
            recorded_at: Utc::now(),
            call_count: 0,
            total_duration_ms: 0.0,
            avg_duration_ms: 0.0,
            max_duration_ms: 0.0,
        });
        entry.call_count += 1;
        entry.total_duration_ms += ms;
        entry.avg_duration_ms = entry.total_duration_ms / entry.call_count as f64;
        if ms > entry.max_duration_ms {
            entry.max_duration_ms = ms;
        }
        if ms > entry.duration_ms {
            entry.duration_ms = ms;
        }
    }

    /// Return the top-N slow queries ordered by average duration descending.
    pub fn top_slow(&self, n: usize) -> Vec<SlowQueryRecord> {
        let store = self.records.read().unwrap_or_else(|e| e.into_inner());
        let mut list: Vec<_> = store.values().cloned().collect();
        list.sort_by(|a, b| b.avg_duration_ms.total_cmp(&a.avg_duration_ms));
        list.truncate(n);
        list
    }

    pub fn clear(&self) {
        self.records.write().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

// ── Query hints ───────────────────────────────────────────────────────────

/// Supported query hint types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryHintKind {
    /// Force use of a specific index: `/*+ IndexScan(table index) */`
    IndexScan,
    /// Disable sequential scan: `SET enable_seqscan = off`
    DisableSeqScan,
    /// Override parallel worker count: `SET max_parallel_workers_per_gather = N`
    ParallelWorkers,
    /// Override planner row estimate: inject CTE with explicit row count hint
    CardinalityHint,
    /// Force nested-loop join
    NestLoop,
    /// Force hash join
    HashJoin,
    /// Force merge join
    MergeJoin,
}

/// A single query hint specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryHint {
    pub kind: QueryHintKind,
    /// Free-form value (index name, worker count, row estimate…)
    pub value: String,
    /// Optional table name the hint applies to.
    pub table: Option<String>,
}

/// Apply hints to a query string and/or return session-level SET statements.
///
/// Returns `(modified_query, session_set_statements)`.
pub fn apply_hints(query: &str, hints: &[QueryHint]) -> (String, Vec<String>) {
    let mut sets: Vec<String> = Vec::new();
    let mut comment_parts: Vec<String> = Vec::new();

    for hint in hints {
        match hint.kind {
            QueryHintKind::IndexScan => {
                if let Some(ref table) = hint.table {
                    comment_parts.push(format!("IndexScan({} {})", table, hint.value));
                }
            }
            QueryHintKind::DisableSeqScan => {
                sets.push("SET LOCAL enable_seqscan = off".to_string());
            }
            QueryHintKind::ParallelWorkers => {
                let n: u32 = hint.value.parse().unwrap_or(2);
                sets.push(format!("SET LOCAL max_parallel_workers_per_gather = {n}"));
            }
            QueryHintKind::CardinalityHint => {
                // Cardinality hints are advisory only — log them.
                debug!(value = %hint.value, "Cardinality hint applied (advisory)");
            }
            QueryHintKind::NestLoop => {
                sets.push("SET LOCAL enable_nestloop = on".to_string());
                sets.push("SET LOCAL enable_hashjoin = off".to_string());
                sets.push("SET LOCAL enable_mergejoin = off".to_string());
            }
            QueryHintKind::HashJoin => {
                sets.push("SET LOCAL enable_hashjoin = on".to_string());
                sets.push("SET LOCAL enable_nestloop = off".to_string());
                sets.push("SET LOCAL enable_mergejoin = off".to_string());
            }
            QueryHintKind::MergeJoin => {
                sets.push("SET LOCAL enable_mergejoin = on".to_string());
                sets.push("SET LOCAL enable_nestloop = off".to_string());
                sets.push("SET LOCAL enable_hashjoin = off".to_string());
            }
        }
    }

    let modified = if comment_parts.is_empty() {
        query.to_string()
    } else {
        format!("/*+ {} */ {}", comment_parts.join(" "), query)
    };

    (modified, sets)
}

// ── EXPLAIN analysis ──────────────────────────────────────────────────────

/// Parsed result from EXPLAIN (ANALYZE, FORMAT JSON).
#[derive(Debug, Clone, Serialize)]
pub struct ExplainAnalysis {
    pub query_fingerprint: String,
    pub planning_time_ms: f64,
    pub execution_time_ms: f64,
    pub total_cost: f64,
    pub actual_rows: f64,
    pub estimated_rows: f64,
    pub row_estimate_error_pct: f64,
    pub seq_scans: Vec<String>,
    pub index_scans: Vec<String>,
    pub warnings: Vec<String>,
    pub recommendations: Vec<String>,
}

/// Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) on `query` and return analysis.
///
/// **Note**: This executes the query for real. Only call on read-only or
/// safe statements, or wrap in a transaction that you ROLLBACK.
pub async fn analyze_query(pool: &PgPool, query: &str) -> Result<ExplainAnalysis, sqlx::Error> {
    let explain_sql = format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}");
    let row: (serde_json::Value,) = sqlx::query_as(&explain_sql).fetch_one(pool).await?;
    parse_explain_json(&row.0, query)
}

/// Run EXPLAIN (no ANALYZE — safe, no execution) and return analysis.
pub async fn explain_query(pool: &PgPool, query: &str) -> Result<ExplainAnalysis, sqlx::Error> {
    let explain_sql = format!("EXPLAIN (FORMAT JSON) {query}");
    let row: (serde_json::Value,) = sqlx::query_as(&explain_sql).fetch_one(pool).await?;
    parse_explain_json(&row.0, query)
}

fn parse_explain_json(
    value: &serde_json::Value,
    query: &str,
) -> Result<ExplainAnalysis, sqlx::Error> {
    let plan_obj = value
        .get(0)
        .ok_or_else(|| io_err("missing plan array element"))?;

    let plan = plan_obj
        .get("Plan")
        .ok_or_else(|| io_err("missing Plan key"))?;

    let total_cost = plan.get("Total Cost").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let estimated_rows = plan.get("Plan Rows").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let actual_rows = plan.get("Actual Rows").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let planning_time_ms = plan_obj
        .get("Planning Time")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let execution_time_ms = plan_obj
        .get("Execution Time")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let row_estimate_error_pct = if estimated_rows > 0.0 {
        ((actual_rows - estimated_rows).abs() / estimated_rows * 100.0).min(9999.0)
    } else {
        0.0
    };

    let mut seq_scans = Vec::new();
    let mut index_scans = Vec::new();
    collect_scans(plan, &mut seq_scans, &mut index_scans);

    let mut warnings = Vec::new();
    let mut recommendations = Vec::new();

    if !seq_scans.is_empty() {
        warnings.push(format!("Sequential scan on: {}", seq_scans.join(", ")));
        for t in &seq_scans {
            recommendations.push(format!(
                "Consider adding an index on '{t}' for frequently filtered columns"
            ));
        }
    }
    if row_estimate_error_pct > 100.0 {
        warnings.push(format!(
            "Row estimate error {row_estimate_error_pct:.0}% — consider running ANALYZE"
        ));
        recommendations.push("Run ANALYZE on affected tables to refresh statistics".to_string());
    }
    if total_cost > 10_000.0 {
        warnings.push(format!("High estimated cost: {total_cost:.0}"));
        recommendations.push("Review query predicates and ensure composite indexes cover WHERE clause".to_string());
    }

    Ok(ExplainAnalysis {
        query_fingerprint: fingerprint(query),
        planning_time_ms,
        execution_time_ms,
        total_cost,
        actual_rows,
        estimated_rows,
        row_estimate_error_pct,
        seq_scans,
        index_scans,
        warnings,
        recommendations,
    })
}

fn collect_scans(
    node: &serde_json::Value,
    seq_scans: &mut Vec<String>,
    index_scans: &mut Vec<String>,
) {
    if let Some(node_type) = node.get("Node Type").and_then(|v| v.as_str()) {
        let table = node
            .get("Relation Name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        match node_type {
            "Seq Scan" => seq_scans.push(table),
            "Index Scan" | "Index Only Scan" | "Bitmap Index Scan" => index_scans.push(table),
            _ => {}
        }
    }
    if let Some(plans) = node.get("Plans").and_then(|v| v.as_array()) {
        for child in plans {
            collect_scans(child, seq_scans, index_scans);
        }
    }
}

// ── Index usage analysis ──────────────────────────────────────────────────

/// Index usage statistics from pg_stat_user_indexes.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct IndexUsageStat {
    pub schema_name: String,
    pub table_name: String,
    pub index_name: String,
    pub index_scans: i64,
    pub index_tup_read: i64,
    pub index_tup_fetch: i64,
    pub is_unused: bool,
}

/// Query pg_stat_user_indexes and return usage stats, flagging unused indexes.
pub async fn get_index_usage(pool: &PgPool) -> Result<Vec<IndexUsageStat>, sqlx::Error> {
    let rows = sqlx::query_as::<_, IndexUsageStat>(
        r#"
        SELECT
            schemaname         AS schema_name,
            relname            AS table_name,
            indexrelname       AS index_name,
            idx_scan           AS index_scans,
            idx_tup_read       AS index_tup_read,
            idx_tup_fetch      AS index_tup_fetch,
            (idx_scan = 0)     AS is_unused
        FROM pg_stat_user_indexes
        ORDER BY idx_scan ASC, relname
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// ── Optimization recommendations ──────────────────────────────────────────

/// A single actionable optimization recommendation.
#[derive(Debug, Clone, Serialize)]
pub struct OptimizationRecommendation {
    pub priority: &'static str,
    pub category: &'static str,
    pub description: String,
    pub action: String,
}

/// Generate recommendations based on index usage stats.
pub fn generate_recommendations(
    index_stats: &[IndexUsageStat],
    slow_queries: &[SlowQueryRecord],
) -> Vec<OptimizationRecommendation> {
    let mut recs = Vec::new();

    // Unused indexes
    for idx in index_stats.iter().filter(|i| i.is_unused) {
        recs.push(OptimizationRecommendation {
            priority: "medium",
            category: "index",
            description: format!(
                "Index '{}' on '{}' has never been used",
                idx.index_name, idx.table_name
            ),
            action: format!("DROP INDEX CONCURRENTLY {};", idx.index_name),
        });
    }

    // Frequently slow queries
    for sq in slow_queries.iter().take(10) {
        recs.push(OptimizationRecommendation {
            priority: "high",
            category: "slow_query",
            description: format!(
                "Query fingerprint '{}' averages {:.1}ms over {} calls",
                sq.query_fingerprint, sq.avg_duration_ms, sq.call_count
            ),
            action: format!(
                "Run EXPLAIN ANALYZE on: {}",
                truncate(&sq.sample_query, 120)
            ),
        });
    }

    if recs.is_empty() {
        recs.push(OptimizationRecommendation {
            priority: "info",
            category: "general",
            description: "No immediate optimizations identified.".to_string(),
            action: "Continue monitoring query performance.".to_string(),
        });
    }

    recs
}

// ── Dashboard ─────────────────────────────────────────────────────────────

/// Full optimization dashboard payload.
#[derive(Debug, Clone, Serialize)]
pub struct OptimizationDashboard {
    pub top_slow_queries: Vec<SlowQueryRecord>,
    pub index_usage: Vec<IndexUsageStat>,
    pub recommendations: Vec<OptimizationRecommendation>,
    pub generated_at: DateTime<Utc>,
}

pub async fn build_dashboard(
    pool: &PgPool,
    tracker: &SlowQueryTracker,
) -> Result<OptimizationDashboard, sqlx::Error> {
    let top_slow = tracker.top_slow(10);
    let index_usage = get_index_usage(pool).await?;
    let recommendations = generate_recommendations(&index_usage, &top_slow);

    Ok(OptimizationDashboard {
        top_slow_queries: top_slow,
        index_usage,
        recommendations,
        generated_at: Utc::now(),
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Normalise a query to a stable fingerprint by collapsing whitespace and
/// replacing literal values with `?` placeholders.
pub fn fingerprint(query: &str) -> String {
    use std::fmt::Write;
    // Collapse whitespace
    let normalized: String = query.split_whitespace().collect::<Vec<_>>().join(" ");
    // Replace $N parameter placeholders and numeric literals with ?
    let mut result = String::with_capacity(normalized.len());
    let mut chars = normalized.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek().map_or(false, |nc| nc.is_ascii_digit()) {
            let _ = write!(result, "?");
            while chars.peek().map_or(false, |nc| nc.is_ascii_digit()) {
                chars.next();
            }
        } else if c.is_ascii_digit()
            && result.ends_with(|p: char| p.is_whitespace() || p == '=' || p == '(')
        {
            let _ = write!(result, "?");
            while chars.peek().map_or(false, |nc| nc.is_ascii_digit() || *nc == '.') {
                chars.next();
            }
        } else {
            result.push(c);
        }
    }
    // Use first 16 chars of sha256 as stable short key
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(result.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    hash[..16].to_string()
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

fn io_err(msg: &str) -> sqlx::Error {
    sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_stable() {
        let q = "SELECT * FROM events WHERE id = $1";
        assert_eq!(fingerprint(q), fingerprint(q));
    }

    #[test]
    fn fingerprint_differs_for_different_queries() {
        let a = "SELECT * FROM events WHERE id = $1";
        let b = "SELECT * FROM events WHERE ledger = $1";
        assert_ne!(fingerprint(a), fingerprint(b));
    }

    #[test]
    fn slow_query_tracker_records_above_threshold() {
        let t = SlowQueryTracker::new(100.0);
        t.record("SELECT 1", Duration::from_millis(200));
        let top = t.top_slow(10);
        assert_eq!(top.len(), 1);
        assert!((top[0].avg_duration_ms - 200.0).abs() < 1.0);
    }

    #[test]
    fn slow_query_tracker_ignores_fast_queries() {
        let t = SlowQueryTracker::new(100.0);
        t.record("SELECT 1", Duration::from_millis(50));
        assert!(t.top_slow(10).is_empty());
    }

    #[test]
    fn slow_query_tracker_accumulates() {
        let t = SlowQueryTracker::new(10.0);
        let q = "SELECT * FROM events";
        t.record(q, Duration::from_millis(100));
        t.record(q, Duration::from_millis(300));
        let top = t.top_slow(1);
        assert_eq!(top[0].call_count, 2);
        assert!((top[0].avg_duration_ms - 200.0).abs() < 1.0);
        assert!((top[0].max_duration_ms - 300.0).abs() < 1.0);
    }

    #[test]
    fn apply_hints_disable_seqscan() {
        let (q, sets) = apply_hints("SELECT 1", &[QueryHint {
            kind: QueryHintKind::DisableSeqScan,
            value: String::new(),
            table: None,
        }]);
        assert_eq!(q, "SELECT 1");
        assert!(sets.iter().any(|s| s.contains("enable_seqscan")));
    }

    #[test]
    fn apply_hints_index_scan_comment() {
        let (q, sets) = apply_hints("SELECT * FROM events", &[QueryHint {
            kind: QueryHintKind::IndexScan,
            value: "idx_events_ledger".to_string(),
            table: Some("events".to_string()),
        }]);
        assert!(q.contains("/*+"));
        assert!(q.contains("IndexScan(events idx_events_ledger)"));
        assert!(sets.is_empty());
    }

    #[test]
    fn apply_hints_hash_join() {
        let (_, sets) = apply_hints("SELECT 1", &[QueryHint {
            kind: QueryHintKind::HashJoin,
            value: String::new(),
            table: None,
        }]);
        assert!(sets.iter().any(|s| s.contains("enable_hashjoin = on")));
        assert!(sets.iter().any(|s| s.contains("enable_nestloop = off")));
    }

    #[test]
    fn generate_recommendations_unused_index() {
        let idx = IndexUsageStat {
            schema_name: "public".to_string(),
            table_name: "events".to_string(),
            index_name: "idx_unused".to_string(),
            index_scans: 0,
            index_tup_read: 0,
            index_tup_fetch: 0,
            is_unused: true,
        };
        let recs = generate_recommendations(&[idx], &[]);
        assert!(recs.iter().any(|r| r.category == "index"));
        assert!(recs.iter().any(|r| r.action.contains("DROP INDEX")));
    }
}
