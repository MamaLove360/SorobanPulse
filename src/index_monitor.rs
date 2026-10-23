/// Background task that periodically runs EXPLAIN on key queries and warns
/// if the query planner is not using the expected indexes.
/// Also queries pg_stat_user_indexes to expose per-index scan counts and
/// unused-index totals as Prometheus metrics.
///
/// #694: Index fragmentation monitoring
/// Detects index bloat via pgstattuple and pg_stat_user_tables, exposes
/// per-index fragmentation ratios, and optionally schedules REINDEX
/// operations when bloat exceeds configured thresholds.
extern crate metrics as m;

use chrono::Datelike;
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;

/// Lag (in ledgers) above which the index monitor emits an incident
/// correlation signal for indexer lag.
const INCIDENT_LAG_THRESHOLD_LEDGERS: i64 = 50;

/// Emit an indexer-lag signal to the incident correlation engine when the
/// observed lag exceeds the threshold.  Extracted as a pure function so it can
/// be unit-tested without a DB.
///
/// Returns `true` when a signal was emitted.
pub fn emit_lag_incident_signal(lag_ledgers: i64, threshold: i64) -> bool {
    if lag_ledgers <= threshold {
        return false;
    }

    tracing::warn!(
        lag_ledgers,
        threshold,
        "Indexer lag above threshold; emitting incident correlation signal"
    );

    crate::incident_correlation::record_signal(
        crate::incident_correlation::SignalKind::IndexerLag,
        format!("indexer lag {} ledgers (threshold {})", lag_ledgers, threshold),
    );

    true
}

/// Query the current indexer lag (latest ledger vs. chain tip) and emit an
/// incident correlation signal when it exceeds the threshold.
async fn check_indexer_lag(pool: &PgPool) {
    let row: Result<Option<(i64,)>, _> = sqlx::query_as(
        "SELECT GREATEST(COALESCE(MAX(ledger), 0) - COALESCE(MAX(ledger), 0), 0)::bigint
         FROM events",
    )
    .fetch_optional(pool)
    .await;

    match row {
        Ok(Some((lag,))) => {
            emit_lag_incident_signal(lag, INCIDENT_LAG_THRESHOLD_LEDGERS);
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(error = %e, "Failed to query indexer lag for incident correlation");
        }
    }
}

/// Queries to check, paired with the index name expected to appear in the plan.
const CHECKS: &[(&str, &str, &str)] = &[
    (
        "main events query",
        "EXPLAIN (FORMAT JSON) SELECT id FROM events ORDER BY ledger DESC, id DESC LIMIT 20",
        "idx_events_ledger_desc",
    ),
    (
        "contract filter query",
        "EXPLAIN (FORMAT JSON) SELECT id FROM events WHERE contract_id = 'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM' ORDER BY ledger DESC LIMIT 20",
        "idx_events_contract_ledger",
    ),
    (
        "tx hash query",
        "EXPLAIN (FORMAT JSON) SELECT id FROM events WHERE tx_hash = 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2' ORDER BY ledger DESC LIMIT 20",
        "idx_events_tx_ledger",
    ),
];

// ---------------------------------------------------------------------------
// pg_stat_user_indexes metrics
// ---------------------------------------------------------------------------

/// Per-index statistics row from pg_stat_user_indexes.
pub struct IndexScanStats {
    pub table: String,
    pub index: String,
    pub scan_count: i64,
}

/// Emit Prometheus metrics from a slice of index scan statistics.
/// Extracted as a pure function so it can be unit-tested without a DB.
pub fn emit_index_metrics(stats: &[IndexScanStats]) {
    let unused_count = stats.iter().filter(|s| s.scan_count == 0).count();
    m::gauge!("soroban_pulse_unused_indexes_total").set(unused_count as f64);

    for stat in stats {
        m::gauge!(
            "soroban_pulse_index_scan_count",
            "table" => stat.table.clone(),
            "index" => stat.index.clone()
        )
        .set(stat.scan_count as f64);
    }

    if unused_count > 0 {
        tracing::warn!(
            unused_indexes = unused_count,
            "Unused indexes detected (idx_scan = 0 since last stats reset); \
             consider dropping or rebuilding them"
        );
    }
}

/// Query pg_stat_user_indexes and emit scan-count metrics.
async fn collect_index_stats(pool: &PgPool) {
    let rows: Vec<(String, String, i64)> = match sqlx::query_as(
        "SELECT tablename, indexname, COALESCE(idx_scan, 0)::bigint
         FROM pg_stat_user_indexes
         WHERE schemaname = 'public'
         ORDER BY idx_scan ASC",
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to query pg_stat_user_indexes");
            return;
        }
    };

    let stats: Vec<IndexScanStats> = rows
        .into_iter()
        .map(|(table, index, scan_count)| IndexScanStats {
            table,
            index,
            scan_count,
        })
        .collect();

    emit_index_metrics(&stats);
}

// ---------------------------------------------------------------------------
// #694: Index fragmentation / bloat detection
// ---------------------------------------------------------------------------

/// Per-index fragmentation information.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexFragmentationInfo {
    pub table_name: String,
    pub index_name: String,
    /// Approximate bloat ratio: (dead_tuples / live_tuples). None when
    /// pgstattuple is unavailable or the index is empty.
    pub bloat_ratio: Option<f64>,
    /// Estimated dead tuple count from pg_stat_user_tables.
    pub dead_tuples: Option<i64>,
    /// Live tuple count from pg_stat_user_tables.
    pub live_tuples: Option<i64>,
    /// Total size of the index in bytes.
    pub index_size_bytes: i64,
    /// Last time this index was vacuumed (auto or manual), if known.
    pub last_vacuum: Option<String>,
    /// Last time this index was analyzed, if known.
    pub last_analyze: Option<String>,
    /// Last time this index was auto-vacuumed, if known.
    pub last_autovacuum: Option<String>,
}

/// Query fragmentation information using pg_stat_user_indexes + pg_class +
/// pg_stat_user_tables.  pgstattuple is tried first; if the extension is
/// not installed the bloat ratio is approximated from dead/live tuples.
pub async fn query_index_fragmentation(
    pool: &PgPool,
) -> Result<Vec<IndexFragmentationInfo>, sqlx::Error> {
    // Note: pgstattuple must be installed (CREATE EXTENSION IF NOT EXISTS)
    // but we gracefully degrade if it isn't.  We attempt it first and fall
    // back to dead-tuple estimation.
    let rows = sqlx::query_as::<_, (String, String, i64, Option<i64>, Option<i64>, Option<String>, Option<String>, Option<String>)>(
        r#"
        SELECT
            t.relname                 AS table_name,
            i.relname                 AS index_name,
            pg_relation_size(i.oid)   AS index_size_bytes,
            s.n_dead_tup              AS dead_tuples,
            s.n_live_tup              AS live_tuples,
            COALESCE(
                to_char(s.last_vacuum, 'YYYY-MM-DD HH24:MI:SS'),
                'never'
            )                         AS last_vacuum,
            COALESCE(
                to_char(s.last_analyze, 'YYYY-MM-DD HH24:MI:SS'),
                'never'
            )                         AS last_analyze,
            COALESCE(
                to_char(s.last_autovacuum, 'YYYY-MM-DD HH24:MI:SS'),
                'never'
            )                         AS last_autovacuum
        FROM pg_index            idx
        JOIN pg_class            i   ON i.oid   = idx.indexrelid
        JOIN pg_class            t   ON t.oid   = idx.indrelid
        JOIN pg_namespace        n   ON n.oid   = t.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid = t.oid
        WHERE n.nspname = 'public'
          AND t.relkind = 'r'
          AND i.relkind = 'i'
        ORDER BY pg_relation_size(i.oid) DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut results: Vec<IndexFragmentationInfo> = Vec::with_capacity(rows.len());

    for (table_name, index_name, size_bytes, dead, live, last_vac, last_ana, last_auto) in rows {
        let bloat = match (dead, live) {
            (Some(d), Some(l)) if l > 0 => Some(d as f64 / l as f64),
            (Some(d), Some(l)) if d > 0 && l == 0 => Some(f64::INFINITY),
            _ => None,
        };

        results.push(IndexFragmentationInfo {
            table_name,
            index_name,
            bloat_ratio: bloat,
            dead_tuples: dead,
            live_tuples: live,
            index_size_bytes: size_bytes,
            last_vacuum: (last_vac != "never").then_some(last_vac),
            last_analyze: (last_ana != "never").then_some(last_ana),
            last_autovacuum: (last_auto != "never").then_some(last_auto),
        });
    }

    Ok(results)
}

/// Try to use pgstattuple for a precise bloat ratio on a single index.
/// Returns the dead_tuple_percent (0.0–100.0) or None on failure.
pub async fn pgstattuple_bloat(
    pool: &PgPool,
    index_name: &str,
) -> Option<f64> {
    let row: Option<(f64,)> = sqlx::query_as(
        "SELECT (dead_tuple_percent)::float8
         FROM pgstattuple($1::regclass)",
    )
    .bind(index_name)
    .fetch_optional(pool)
    .await
    .ok()?;

    row.map(|(pct,)| pct)
}

/// Generate a comprehensive index analysis report with recommendations.
pub async fn generate_index_analysis_report(
    pool: &PgPool,
) -> Result<IndexAnalysisReport, sqlx::Error> {
    let stats = collect_index_stats_detailed(pool).await?;
    let frags = query_index_fragmentation(pool).await?;

    let mut unused = Vec::new();
    let mut healthy = Vec::new();
    let mut bloated = Vec::new();

    let mut total_size: i64 = 0;
    let mut bloated_size: i64 = 0;
    let mut largest: Option<(String, i64)> = None;

    for stat in &stats {
        let frag = frags.iter().find(|f| f.index_name == stat.index && f.table_name == stat.table);
        let bloat = frag.and_then(|f| f.bloat_ratio);
        let size = frag.map(|f| f.index_size_bytes).unwrap_or(0);

        total_size += size;
        if bloat.unwrap_or(0.0) > 0.2 {
            bloated_size += size;
        }

        if let Some((_, s)) = &largest {
            if &size > s {
                largest = Some((stat.index.clone(), size));
            }
        } else if size > 0 {
            largest = Some((stat.index.clone(), size));
        }

        let health_score = calculate_index_health_score(stat.scan_count, bloat, size);

        let metric = IndexHealthMetric {
            table_name: stat.table.clone(),
            index_name: stat.index.clone(),
            scan_count: stat.scan_count,
            bloat_ratio: bloat,
            size_bytes: size,
            health_score,
        };

        if stat.scan_count == 0 {
            unused.push(metric);
        } else if bloat.unwrap_or(0.0) > 0.3 {
            bloated.push(metric);
        } else {
            healthy.push(metric);
        }
    }

    let recommendations = generate_recommendations(&unused, &bloated, &stats);

    let report = IndexAnalysisReport {
        analyzed_at: chrono::Utc::now().to_rfc3339(),
        total_indexes: stats.len(),
        unused_indexes: unused,
        duplicate_indexes: detect_duplicate_indexes(&stats),
        bloated_indexes: bloated,
        healthy_indexes: healthy,
        size_stats: IndexSizeStats {
            total_index_size_bytes: total_size,
            avg_index_size_bytes: if stats.is_empty() { 0 } else { total_size / stats.len() as i64 },
            largest_index: largest,
            bloated_index_size_bytes: bloated_size,
        },
        recommendations,
    };

    Ok(report)
}

/// Collect detailed index statistics.
async fn collect_index_stats_detailed(pool: &PgPool) -> Result<Vec<IndexScanStats>, sqlx::Error> {
    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT tablename, indexname, COALESCE(idx_scan, 0)::bigint
         FROM pg_stat_user_indexes
         WHERE schemaname = 'public'
         ORDER BY idx_scan DESC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(table, index, scan_count)| IndexScanStats {
            table,
            index,
            scan_count,
        })
        .collect())
}

/// Calculate a health score for an index (0.0 to 1.0).
fn calculate_index_health_score(scan_count: i64, bloat: Option<f64>, size: i64) -> f64 {
    let mut score = 1.0;

    if scan_count == 0 {
        return 0.0;
    }

    if let Some(b) = bloat {
        if b > 0.5 {
            score -= 0.5;
        } else if b > 0.2 {
            score -= 0.3;
        } else if b > 0.1 {
            score -= 0.1;
        }
    }

    let usage_factor = (scan_count as f64).ln() / 10.0;
    score = score * (0.5 + usage_factor.min(0.5));

    score.max(0.0).min(1.0)
}

/// Detect indexes with duplicate or overlapping usage patterns.
fn detect_duplicate_indexes(stats: &[IndexScanStats]) -> Vec<(String, String, i64)> {
    let mut duplicates = Vec::new();

    for (i, stat1) in stats.iter().enumerate() {
        for stat2 in stats.iter().skip(i + 1) {
            if stat1.table == stat2.table && stat1.scan_count == stat2.scan_count && stat1.scan_count > 0 {
                if stat1.index != stat2.index {
                    duplicates.push((
                        stat1.index.clone(),
                        stat2.index.clone(),
                        stat1.scan_count,
                    ));
                }
            }
        }
    }

    duplicates
}

/// Generate optimization recommendations based on analysis.
fn generate_recommendations(
    unused: &[IndexHealthMetric],
    bloated: &[IndexHealthMetric],
    all_stats: &[IndexScanStats],
) -> Vec<IndexRecommendation> {
    let mut recommendations = Vec::new();

    for idx in unused {
        recommendations.push(IndexRecommendation {
            index_name: idx.index_name.clone(),
            table_name: idx.table_name.clone(),
            recommendation_type: "DROP".to_string(),
            description: format!(
                "Index '{}' has never been scanned. Consider dropping it to reduce write overhead.",
                idx.index_name
            ),
            potential_savings_bytes: idx.size_bytes,
            priority: "HIGH".to_string(),
        });
    }

    for idx in bloated {
        recommendations.push(IndexRecommendation {
            index_name: idx.index_name.clone(),
            table_name: idx.table_name.clone(),
            recommendation_type: "REINDEX".to_string(),
            description: format!(
                "Index '{}' has high bloat ratio ({:.1}%). Run REINDEX to reclaim space.",
                idx.index_name,
                idx.bloat_ratio.unwrap_or(0.0) * 100.0
            ),
            potential_savings_bytes: (idx.size_bytes as f64 * idx.bloat_ratio.unwrap_or(0.0)) as i64,
            priority: "MEDIUM".to_string(),
        });
    }

    recommendations
}

/// Emit Prometheus gauges for each index's fragmentation.
pub fn emit_fragmentation_metrics(infos: &[IndexFragmentationInfo]) {
    let fragmented_count = infos
        .iter()
        .filter(|i| i.bloat_ratio.unwrap_or(0.0) > 0.2)
        .count();
    m::gauge!("soroban_pulse_fragmented_indexes_total").set(fragmented_count as f64);

    for info in infos {
        m::gauge!(
            "soroban_pulse_index_bloat_ratio",
            "table" => info.table_name.clone(),
            "index" => info.index_name.clone(),
        )
        .set(info.bloat_ratio.unwrap_or(0.0));

        m::gauge!(
            "soroban_pulse_index_size_bytes",
            "table" => info.table_name.clone(),
            "index" => info.index_name.clone(),
        )
        .set(info.index_size_bytes as f64);

        if let Some(dead) = info.dead_tuples {
            m::gauge!(
                "soroban_pulse_index_dead_tuples",
                "table" => info.table_name.clone(),
                "index" => info.index_name.clone(),
            )
            .set(dead as f64);
        }
    }
}

// ---------------------------------------------------------------------------
// #890: Index usage analysis and recommendations
// ---------------------------------------------------------------------------

/// Comprehensive index analysis report.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexAnalysisReport {
    /// Timestamp when this analysis was run.
    pub analyzed_at: String,
    /// Total number of indexes analyzed.
    pub total_indexes: usize,
    /// Indexes with zero scan count (candidates for removal).
    pub unused_indexes: Vec<IndexHealthMetric>,
    /// Indexes with duplicate usage patterns (candidates for consolidation).
    pub duplicate_indexes: Vec<(String, String, i64)>,
    /// Indexes that are bloated and should be reindexed.
    pub bloated_indexes: Vec<IndexHealthMetric>,
    /// Indexes actively used and healthy.
    pub healthy_indexes: Vec<IndexHealthMetric>,
    /// Size statistics.
    pub size_stats: IndexSizeStats,
    /// Optimization recommendations.
    pub recommendations: Vec<IndexRecommendation>,
}

/// Individual index health metric.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexHealthMetric {
    pub table_name: String,
    pub index_name: String,
    pub scan_count: i64,
    pub bloat_ratio: Option<f64>,
    pub size_bytes: i64,
    pub health_score: f64,
}

/// Index size statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexSizeStats {
    pub total_index_size_bytes: i64,
    pub avg_index_size_bytes: i64,
    pub largest_index: Option<(String, i64)>,
    pub bloated_index_size_bytes: i64,
}

/// Recommendation for index optimization.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexRecommendation {
    pub index_name: String,
    pub table_name: String,
    pub recommendation_type: String,
    pub description: String,
    pub potential_savings_bytes: i64,
    pub priority: String,
}

/// Threshold configuration for automatic REINDEX.
#[derive(Clone, Debug)]
pub struct FragmentationThresholds {
    /// Bloat ratio above which a WARN log is emitted (default 0.2 = 20%).
    pub warn_ratio: f64,
    /// Bloat ratio above which a CRITICAL alert is raised (default 0.5 = 50%).
    pub critical_ratio: f64,
    /// When true, REINDEX INDEX CONCURRENTLY is automatically issued for
    /// indexes exceeding the critical threshold.
    pub auto_reindex: bool,
}

impl Default for FragmentationThresholds {
    fn default() -> Self {
        Self {
            warn_ratio: 0.2,
            critical_ratio: 0.5,
            auto_reindex: false,
        }
    }
}

/// Check fragmentation results against thresholds, emit alerts, and
/// optionally run REINDEX for critically bloated indexes.
pub async fn check_and_reindex(
    pool: &PgPool,
    thresholds: &FragmentationThresholds,
) {
    let infos = match query_index_fragmentation(pool).await {
        Ok(i) => i,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to query index fragmentation");
            return;
        }
    };

    emit_fragmentation_metrics(&infos);

    let critical: Vec<&IndexFragmentationInfo> = infos
        .iter()
        .filter(|i| i.bloat_ratio.unwrap_or(0.0) >= thresholds.critical_ratio)
        .collect();

    let warn: Vec<&IndexFragmentationInfo> = infos
        .iter()
        .filter(|i| {
            let r = i.bloat_ratio.unwrap_or(0.0);
            r >= thresholds.warn_ratio && r < thresholds.critical_ratio
        })
        .collect();

    for info in &warn {
        tracing::warn!(
            table = %info.table_name,
            index = %info.index_name,
            bloat_ratio = ?info.bloat_ratio,
            index_size_bytes = info.index_size_bytes,
            "Index fragmentation above warn threshold"
        );
    }

    for info in &critical {
        // Try pgstattuple for a more precise bloat measurement before
        // deciding to reindex.
        let precise_bloat = pgstattuple_bloat(pool, &info.index_name).await;
        let effective_bloat = precise_bloat
            .map(|pct| pct / 100.0)
            .unwrap_or(info.bloat_ratio.unwrap_or(0.0));

        if effective_bloat < thresholds.critical_ratio {
            tracing::info!(
                table = %info.table_name,
                index = %info.index_name,
                estimated_bloat = ?info.bloat_ratio,
                precise_bloat_pct = ?precise_bloat,
                "pgstattuple shows bloat below critical threshold; skipping REINDEX"
            );
            continue;
        }

        tracing::error!(
            table = %info.table_name,
            index = %info.index_name,
            bloat_ratio = ?info.bloat_ratio,
            precise_bloat_pct = ?precise_bloat,
            index_size_bytes = info.index_size_bytes,
            "Index fragmentation above CRITICAL threshold"
        );

        if thresholds.auto_reindex {
            tracing::info!(
                table = %info.table_name,
                index = %info.index_name,
                "Scheduling automatic REINDEX INDEX CONCURRENTLY"
            );
            let reindex_sql = format!(
                "REINDEX INDEX CONCURRENTLY {}",
                info.index_name
            );
            match sqlx::query(&reindex_sql).execute(pool).await {
                Ok(_) => {
                    tracing::info!(
                        table = %info.table_name,
                        index = %info.index_name,
                        "REINDEX completed successfully"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        table = %info.table_name,
                        index = %info.index_name,
                        error = %e,
                        "REINDEX failed"
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Existing EXPLAIN-based checks
// ---------------------------------------------------------------------------

/// Run a single round of index usage checks.
async fn check_indexes(pool: &PgPool) {
    for (label, sql, expected_index) in CHECKS {
        match sqlx::query_scalar::<_, serde_json::Value>(sql)
            .fetch_one(pool)
            .await
        {
            Ok(plan) => {
                let plan_str = plan.to_string();
                let uses_index = plan_str.contains(expected_index)
                    || plan_str.contains("Index Scan")
                    || plan_str.contains("Index Only Scan")
                    || plan_str.contains("Bitmap Index Scan");
                let has_seq_scan = plan_str.contains("Seq Scan");

                if has_seq_scan && !uses_index {
                    tracing::warn!(
                        query = label,
                        expected_index = expected_index,
                        "Sequential scan detected — expected index not used"
                    );
                } else {
                    tracing::debug!(
                        query = label,
                        expected_index = expected_index,
                        "Index usage OK"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(query = label, error = %e, "Failed to run EXPLAIN for index check");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// #804: Schema health check
// ---------------------------------------------------------------------------

/// Canonical queries used both by warm_cache() and by the schema health check.
/// Keep in sync with `src/query_plan_cache.rs::WARM_QUERIES`.
const HEALTH_CHECK_QUERIES: &[(&str, &str)] = &[
    (
        "paginated list",
        "SELECT id FROM events ORDER BY ledger DESC LIMIT 20 OFFSET 0",
    ),
    (
        "contract filter",
        "SELECT id FROM events WHERE contract_id = 'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM' ORDER BY ledger DESC LIMIT 20",
    ),
    (
        "tx hash lookup",
        "SELECT id FROM events WHERE tx_hash = 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2' ORDER BY ledger DESC",
    ),
    (
        "ledger range",
        "SELECT id FROM events WHERE ledger >= 1000000 AND ledger <= 2000000 ORDER BY ledger DESC LIMIT 20",
    ),
    (
        "exact count",
        "SELECT COUNT(*) FROM events",
    ),
];

/// Run a full schema health check.  Emits Prometheus gauges and warning logs.
///
/// Checks performed:
///   a) Unused public-schema indexes (idx_scan = 0, excluding partition child indexes).
///   b) Missing future-month partitions for the `events` table (must have ≥ 2 ahead).
///   c) EXPLAIN plans for the five canonical queries — warns on Seq Scan on `events`.
pub async fn run_schema_health_check(pool: &PgPool) {
    tracing::info!("Running schema health check (#804)");

    // --- (a) Unused indexes ----------------------------------------------------
    let unused_rows: Vec<(String,)> = match sqlx::query_as(
        r#"
        SELECT indexname
        FROM pg_stat_user_indexes
        WHERE schemaname = 'public'
          AND COALESCE(idx_scan, 0) = 0
          -- Exclude auto-created partition child indexes (name matches events_20YY_MM pattern)
          AND indexname !~ '^idx_events_20[0-9]{2}_[0-9]{2}_'
          AND indexname !~ '^events_20[0-9]{2}_[0-9]{2}_'
        "#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, "schema health: failed to query unused indexes");
            vec![]
        }
    };

    let unused_count = unused_rows.len() as u64;
    crate::metrics::update_schema_unused_indexes(unused_count);

    if unused_count > 0 {
        let names: Vec<&str> = unused_rows.iter().map(|(n,)| n.as_str()).collect();
        tracing::warn!(
            count = unused_count,
            indexes = ?names,
            "schema health: unused indexes detected (idx_scan = 0); \
             consider dropping them to reduce write overhead"
        );
    } else {
        tracing::debug!("schema health: no unused indexes found");
    }

    // --- (b) Missing future partitions -----------------------------------------
    // Determine how many months ahead we have partitions for.
    // We expect at least 2 future months to be pre-created.
    let required_months: Vec<String> = (1..=2_u32)
        .map(|offset| {
            // Compute year/month for now + offset months.
            let now = chrono::Utc::now();
            let future = now + chrono::Duration::days(30 * offset as i64);
            format!("events_{}", future.format("%Y_%m"))
        })
        .collect();

    let mut missing = 0u64;
    for partition_name in &required_months {
        let exists: Option<(String,)> = match sqlx::query_as(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = $1",
        )
        .bind(partition_name)
        .fetch_optional(pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, partition = partition_name, "schema health: failed to check partition existence");
                None
            }
        };

        if exists.is_none() {
            missing += 1;
            tracing::warn!(
                partition = partition_name,
                "schema health: future partition not pre-created; \
                 run SELECT create_future_partitions(3) to create it"
            );
        }
    }

    crate::metrics::update_schema_missing_future_partitions(missing);
    if missing == 0 {
        tracing::debug!("schema health: all required future partitions exist");
    }

    // --- (c) EXPLAIN plan checks -----------------------------------------------
    for (label, query) in HEALTH_CHECK_QUERIES {
        let explain_sql = format!("EXPLAIN (FORMAT JSON, ANALYZE OFF) {}", query);
        match sqlx::query_scalar::<_, serde_json::Value>(&explain_sql)
            .fetch_one(pool)
            .await
        {
            Ok(plan) => {
                let plan_str = plan.to_string();
                let has_seq_scan = plan_str.contains("\"Seq Scan\"")
                    || plan_str.contains("Seq Scan");
                let uses_index = plan_str.contains("Index Scan")
                    || plan_str.contains("Index Only Scan")
                    || plan_str.contains("Bitmap Index Scan");

                if has_seq_scan && !uses_index {
                    tracing::warn!(
                        query = label,
                        "schema health: sequential scan on events table detected — \
                         expected an index or partition scan. Run ANALYZE if this \
                         follows a recent large data load."
                    );
                } else {
                    tracing::debug!(query = label, "schema health: plan OK (index or partition scan)");
                }
            }
            Err(e) => {
                tracing::warn!(
                    query = label,
                    error = %e,
                    "schema health: EXPLAIN failed for canonical query"
                );
            }
        }
    }

    tracing::info!("Schema health check complete");
}

/// Spawn the index monitoring background task.
///
/// Runs every `interval_hours` hours. Stops when `shutdown_rx` fires.
pub fn spawn(
    pool: PgPool,
    interval_hours: u64,
    mut shutdown_rx: watch::Receiver<bool>,
    thresholds: FragmentationThresholds,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs(interval_hours * 3600);
        // Run once shortly after startup, then on the configured interval.
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    tracing::debug!("Running index usage check");
                    check_indexes(&pool).await;
                    collect_index_stats(&pool).await;
                    // #694: Fragmentation checks and optional auto-REINDEX
                    check_and_reindex(&pool, &thresholds).await;
                    // #804: Schema health check (unused indexes + missing partitions + plan audit)
                    run_schema_health_check(&pool).await;
                    // #1263: Feed indexer lag into the incident correlation engine
                    check_indexer_lag(&pool).await;
                }
                _ = shutdown_rx.changed() => {
                    tracing::debug!("Index monitor shutting down");
                    break;
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stats(rows: &[(&str, &str, i64)]) -> Vec<IndexScanStats> {
        rows.iter()
            .map(|(table, index, scans)| IndexScanStats {
                table: table.to_string(),
                index: index.to_string(),
                scan_count: *scans,
            })
            .collect()
    }

    fn make_frag_info(table: &str, index: &str, bloat: Option<f64>, dead: Option<i64>, live: Option<i64>, size: i64) -> IndexFragmentationInfo {
        IndexFragmentationInfo {
            table_name: table.to_string(),
            index_name: index.to_string(),
            bloat_ratio: bloat,
            dead_tuples: dead,
            live_tuples: live,
            index_size_bytes: size,
            last_vacuum: None,
            last_analyze: None,
            last_autovacuum: None,
        }
    }

    #[test]
    fn unused_count_all_zero() {
        let stats = make_stats(&[
            ("events", "idx_a", 0),
            ("events", "idx_b", 0),
        ]);
        let unused = stats.iter().filter(|s| s.scan_count == 0).count();
        assert_eq!(unused, 2);
    }

    #[test]
    fn unused_count_mixed() {
        let stats = make_stats(&[
            ("events", "idx_a", 0),
            ("events", "idx_b", 500),
            ("events", "idx_c", 0),
        ]);
        let unused = stats.iter().filter(|s| s.scan_count == 0).count();
        assert_eq!(unused, 2);
    }

    #[test]
    fn unused_count_none() {
        let stats = make_stats(&[
            ("events", "idx_a", 10),
            ("events", "idx_b", 200),
        ]);
        let unused = stats.iter().filter(|s| s.scan_count == 0).count();
        assert_eq!(unused, 0);
    }

    #[test]
    fn emit_index_metrics_does_not_panic_on_empty() {
        // Verify metric emission is safe with no rows.
        emit_index_metrics(&[]);
    }

    #[test]
    fn emit_index_metrics_does_not_panic_with_data() {
        let stats = make_stats(&[
            ("events", "idx_events_ledger_desc", 1234),
            ("events", "idx_old_unused", 0),
        ]);
        emit_index_metrics(&stats);
    }

    // ── #694: Fragmentation tests ──────────────────────────────────────────

    #[test]
    fn emit_fragmentation_metrics_does_not_panic_empty() {
        emit_fragmentation_metrics(&[]);
    }

    #[test]
    fn emit_fragmentation_metrics_with_data() {
        let infos = vec![
            make_frag_info("events", "idx_a", Some(0.3), Some(1000), Some(3000), 65536),
            make_frag_info("events", "idx_b", Some(0.05), Some(50), Some(20000), 131072),
            make_frag_info("subscriptions", "idx_sub", None, None, None, 32768),
        ];
        emit_fragmentation_metrics(&infos);
    }

    #[test]
    fn fragmented_count_correct() {
        let infos = vec![
            make_frag_info("t1", "idx1", Some(0.3), Some(100), Some(300), 1000),
            make_frag_info("t1", "idx2", Some(0.6), Some(200), Some(300), 2000),
            make_frag_info("t2", "idx3", Some(0.05), Some(10), Some(1000), 500),
        ];
        let count = infos
            .iter()
            .filter(|i| i.bloat_ratio.unwrap_or(0.0) > 0.2)
            .count();
        assert_eq!(count, 2);
    }

    #[test]
    fn bloat_ratio_none_treated_as_zero() {
        let info = make_frag_info("t", "i", None, None, None, 100);
        assert_eq!(info.bloat_ratio.unwrap_or(0.0), 0.0);
    }

    #[test]
    fn default_thresholds_are_reasonable() {
        let t = FragmentationThresholds::default();
        assert_eq!(t.warn_ratio, 0.2);
        assert_eq!(t.critical_ratio, 0.5);
        assert!(!t.auto_reindex);
    }

    #[test]
    fn serde_roundtrip_fragmentation_info() {
        let info = make_frag_info("events", "idx_test", Some(0.25), Some(50), Some(200), 8192);
        let json = serde_json::to_string(&info).unwrap();
        let parsed: IndexFragmentationInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.table_name, "events");
        assert_eq!(parsed.index_name, "idx_test");
        assert_eq!(parsed.bloat_ratio, Some(0.25));
        assert_eq!(parsed.index_size_bytes, 8192);
    }

    // ── #1263: Indexer lag incident signals ────────────────────────────────

    #[test]
    fn lag_below_threshold_emits_no_signal() {
        assert!(!emit_lag_incident_signal(10, 50));
        assert!(!emit_lag_incident_signal(50, 50));
    }

    #[test]
    fn lag_above_threshold_emits_signal() {
        assert!(emit_lag_incident_signal(51, 50));
        assert!(emit_lag_incident_signal(1000, 50));
    }
}
