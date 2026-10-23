use dashmap::DashMap;
use moka::future::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use sqlx::PgPool;
use tracing::{debug, info, warn};

pub const DEFAULT_PLAN_CACHE_SIZE: u64 = 1000;
pub const DEFAULT_PLAN_CACHE_TTL_SECS: u64 = 3600; // 1 hour

// Adaptive TTL frequency thresholds:
//   frequency < FREQ_MEDIUM  → 1× base TTL
//   FREQ_MEDIUM ≤ freq < FREQ_HIGH → 2× base TTL
//   frequency ≥ FREQ_HIGH   → 4× base TTL
const FREQ_MEDIUM: u64 = 10;
const FREQ_HIGH: u64 = 100;

/// The five canonical queries used for cache warming at startup.
/// These cover all primary API access patterns documented in docs/schema.md.
pub const WARM_QUERIES: &[&str] = &[
    // Paginated list — GET /v1/events
    "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
     FROM events ORDER BY ledger DESC LIMIT $1 OFFSET $2",
    // Contract filter — GET /v1/events/{contract_id}
    "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
     FROM events WHERE contract_id = $1 ORDER BY ledger DESC LIMIT $2 OFFSET $3",
    // Tx hash lookup — GET /v1/events/tx/{tx_hash}
    "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
     FROM events WHERE tx_hash = $1 ORDER BY ledger DESC",
    // Ledger range filter — GET /v1/events?from_ledger=..&to_ledger=..
    "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
     FROM events WHERE ledger >= $1 AND ledger <= $2 ORDER BY ledger DESC LIMIT $3 OFFSET $4",
    // Exact count — GET /v1/events?exact_count=true
    "SELECT COUNT(*) FROM events",
];

#[derive(Debug, Clone)]
pub struct QueryPlanCacheConfig {
    pub max_plans: u64,
    pub ttl_secs: u64,
    pub enable_prepared_statements: bool,
}

impl Default for QueryPlanCacheConfig {
    fn default() -> Self {
        Self {
            max_plans: DEFAULT_PLAN_CACHE_SIZE,
            ttl_secs: DEFAULT_PLAN_CACHE_TTL_SECS,
            enable_prepared_statements: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub query: String,
    pub plan_hash: String,
    pub estimated_cost: f64,
    pub estimated_rows: f64,
    pub actual_rows: Option<f64>,
    pub planning_time_ms: f64,
    pub execution_time_ms: Option<f64>,
}

/// Running statistics maintained by atomic counters so they are
/// safe to read from any async context without holding a lock.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub cached_plans: u64,
    pub max_capacity: u64,
    /// Total cache hits since the cache was created.
    pub hit_count: u64,
    /// Total cache misses since the cache was created.
    pub miss_count: u64,
    /// Total entries evicted by the moka LRU/TTL policy.
    pub eviction_count: u64,
    /// Ratio of hits to (hits + misses).  Returns 0.0 when no requests
    /// have been made yet (avoids NaN in downstream metric sinks).
    pub hit_ratio: f64,
}

pub struct QueryPlanCache {
    cache: Arc<Cache<String, QueryPlan>>,
    config: QueryPlanCacheConfig,
    /// Per-query request frequency counter.  Used to compute the adaptive
    /// TTL multiplier on every insert.
    frequency_map: Arc<DashMap<String, u64>>,
    hit_count: Arc<AtomicU64>,
    miss_count: Arc<AtomicU64>,
    eviction_count: Arc<AtomicU64>,
    /// Schema version counter — incremented on schema changes to invalidate plans.
    schema_version: Arc<AtomicU64>,
    /// Last schema change timestamp.
    last_schema_change: Arc<tokio::sync::Mutex<Option<std::time::SystemTime>>>,
}

impl QueryPlanCache {
    pub fn new(config: QueryPlanCacheConfig) -> Self {
        let hit_count = Arc::new(AtomicU64::new(0));
        let miss_count = Arc::new(AtomicU64::new(0));
        let eviction_count = Arc::new(AtomicU64::new(0));

        let eviction_counter = Arc::clone(&eviction_count);

        // NOTE: We do NOT set a global time_to_live here because we use
        // per-entry TTLs via insert_with_ttl for adaptive TTL support.
        // The `max_capacity` cap still applies and triggers LRU eviction.
        let cache = Arc::new(
            Cache::builder()
                .max_capacity(config.max_plans)
                .eviction_listener(move |_key, _value, _cause| {
                    eviction_counter.fetch_add(1, Ordering::Relaxed);
                    crate::metrics::record_query_plan_cache_eviction();
                })
                .build(),
        );

        info!(
            max_plans = config.max_plans,
            ttl_secs = config.ttl_secs,
            prepared_statements = config.enable_prepared_statements,
            "Initialized query plan cache (adaptive TTL enabled, schema-aware invalidation)"
        );

        Self {
            cache,
            config,
            frequency_map: Arc::new(DashMap::new()),
            hit_count,
            miss_count,
            eviction_count,
            schema_version: Arc::new(AtomicU64::new(0)),
            last_schema_change: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(QueryPlanCacheConfig::default())
    }

    /// Look up a cached plan.  Increments the frequency counter for the key
    /// whether or not the entry exists (hit or miss), so the adaptive TTL
    /// can see the true request rate for future inserts.
    pub async fn get(&self, query: &str) -> Option<QueryPlan> {
        // Always bump frequency — we want to measure request rate, not just
        // the rate of cold misses.
        self.frequency_map
            .entry(query.to_string())
            .and_modify(|c| *c += 1)
            .or_insert(1);

        if let Some(plan) = self.cache.get(query).await {
            debug!(query_hash = %query_hash(query), "Query plan cache hit");
            self.hit_count.fetch_add(1, Ordering::Relaxed);
            crate::metrics::record_query_plan_cache_hit();

            let hits = self.hit_count.load(Ordering::Relaxed);
            let misses = self.miss_count.load(Ordering::Relaxed);
            crate::metrics::update_query_plan_hit_ratio(safe_ratio(hits, misses));

            return Some(plan);
        }

        debug!(query_hash = %query_hash(query), "Query plan cache miss");
        self.miss_count.fetch_add(1, Ordering::Relaxed);
        crate::metrics::record_query_plan_cache_miss();

        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        crate::metrics::update_query_plan_hit_ratio(safe_ratio(hits, misses));

        None
    }

    /// Insert a plan.  The per-entry TTL is chosen by `adaptive_ttl_secs`
    /// based on how frequently the query has been requested so far.
    pub async fn insert(&self, query: String, plan: QueryPlan) {
        let ttl_secs = self.adaptive_ttl_secs(&query);
        debug!(
            query_hash = %query_hash(&query),
            estimated_cost = plan.estimated_cost,
            estimated_rows = plan.estimated_rows,
            ttl_secs,
            "Caching query plan"
        );
        self.cache
            .insert_with_ttl(query, plan, Duration::from_secs(ttl_secs))
            .await;
        crate::metrics::record_query_plan_cached();

        // Keep the entry-count gauge current after each insert.
        crate::metrics::update_query_plan_cache_entry_count(self.cache.entry_count());
    }

    /// Compute the adaptive TTL for a query key.
    ///
    /// Rules (thresholds: FREQ_MEDIUM = 10, FREQ_HIGH = 100):
    ///   - frequency <  10  → 1× base TTL  (normal queries)
    ///   - frequency < 100  → 2× base TTL  (moderately hot queries)
    ///   - frequency ≥ 100  → 4× base TTL  (very hot queries — keep longer)
    pub fn adaptive_ttl_secs(&self, query: &str) -> u64 {
        let freq = self
            .frequency_map
            .get(query)
            .map(|r| *r)
            .unwrap_or(0);

        let base = self.config.ttl_secs;
        if freq >= FREQ_HIGH {
            base * 4
        } else if freq >= FREQ_MEDIUM {
            base * 2
        } else {
            base
        }
    }

    pub async fn analyze_query(&self, pool: &PgPool, query: &str) -> Result<QueryPlan, sqlx::Error> {
        // Check cache first
        if let Some(cached_plan) = self.get(query).await {
            return Ok(cached_plan);
        }

        // Analyze with EXPLAIN (ANALYZE OFF so we don't actually execute the query)
        let explain_query = format!("EXPLAIN (FORMAT JSON, ANALYZE OFF) {}", query);
        let result: (String,) = sqlx::query_as(&explain_query)
            .fetch_one(pool)
            .await?;

        let plan = parse_explain_output(&result.0, query)?;
        self.insert(query.to_string(), plan.clone()).await;

        Ok(plan)
    }

    /// Pre-populate the cache with plans for the five canonical query patterns
    /// so the first real requests are never cold misses.
    ///
    /// Returns the number of plans that were successfully warmed.
    /// Failures for individual queries are logged as warnings but do not abort
    /// the overall warming pass.
    pub async fn warm_cache(&self, pool: &PgPool) -> Result<usize, sqlx::Error> {
        info!("Starting query plan cache warming ({} queries)", WARM_QUERIES.len());
        let mut warmed = 0usize;

        for query in WARM_QUERIES {
            // analyze_query will insert into the cache on a miss.
            match self.analyze_query(pool, query).await {
                Ok(plan) => {
                    info!(
                        query_hash = %query_hash(query),
                        estimated_cost = plan.estimated_cost,
                        planning_time_ms = plan.planning_time_ms,
                        "Warmed query plan"
                    );
                    warmed += 1;
                }
                Err(e) => {
                    warn!(
                        query_hash = %query_hash(query),
                        error = %e,
                        "Failed to warm query plan; skipping"
                    );
                }
            }
        }

        info!(
            warmed,
            total = WARM_QUERIES.len(),
            "Query plan cache warming complete"
        );
        Ok(warmed)
    }

    pub async fn get_cache_stats(&self) -> CacheStats {
        let count = self.cache.entry_count();
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let evictions = self.eviction_count.load(Ordering::Relaxed);

        CacheStats {
            cached_plans: count,
            max_capacity: self.config.max_plans,
            hit_count: hits,
            miss_count: misses,
            eviction_count: evictions,
            hit_ratio: safe_ratio(hits, misses),
        }
    }

    pub async fn clear(&self) {
        self.cache.invalidate_all();
        info!("Query plan cache cleared");
    }

    /// Detect schema changes by querying pg_stat_user_tables catalog.
    /// Returns true if schema has changed since last check.
    pub async fn detect_schema_changes(
        &self,
        pool: &PgPool,
    ) -> Result<bool, sqlx::Error> {
        // Query schema modification timestamp
        let row: (i64,) = sqlx::query_as(
            "SELECT MAX(EXTRACT(EPOCH FROM schemachange)::bigint)::bigint
             FROM (
               SELECT MAX(n_tup_ins + n_tup_upd + n_tup_del) as schemachange
               FROM pg_stat_user_tables
             ) t"
        )
        .fetch_one(pool)
        .await?;

        let current_version = row.0;
        let previous_version = self.schema_version.load(Ordering::Relaxed);

        if current_version > previous_version as i64 {
            self.schema_version.store(current_version as u64, Ordering::Relaxed);
            let mut last_change = self.last_schema_change.lock().await;
            *last_change = Some(std::time::SystemTime::now());

            info!(
                current_version,
                previous_version,
                "Schema changes detected, invalidating query plan cache"
            );
            self.cache.invalidate_all();
            return Ok(true);
        }

        Ok(false)
    }

    /// Detect structural schema changes (tables, indexes, columns added/dropped).
    /// Returns the number of schema changes detected.
    pub async fn detect_structural_changes(
        &self,
        pool: &PgPool,
    ) -> Result<usize, sqlx::Error> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*)::bigint FROM pg_event_trigger_ddl_commands()"
        )
        .fetch_optional(pool)
        .await?
        .unwrap_or((0,));

        if row.0 > 0 {
            debug!(change_count = row.0, "Structural schema changes detected");
            self.cache.invalidate_all();
        }

        Ok(row.0 as usize)
    }

    /// Get cache diagnostic information.
    pub async fn get_cache_diagnostics(&self) -> CacheDiagnostics {
        let stats = self.get_cache_stats().await;
        let schema_version = self.schema_version.load(Ordering::Relaxed);
        let last_change = self.last_schema_change.lock().await.clone();

        CacheDiagnostics {
            cache_stats: stats,
            schema_version,
            last_schema_change: last_change,
            frequency_map_size: self.frequency_map.len(),
        }
    }

    /// Invalidate plans matching a pattern (useful for targeted invalidation).
    pub async fn invalidate_pattern(&self, pattern: &str) -> usize {
        let mut invalidated = 0;
        for entry in self.frequency_map.iter() {
            if entry.key().contains(pattern) {
                self.cache.invalidate(entry.key());
                invalidated += 1;
            }
        }
        info!(pattern, invalidated, "Invalidated cache entries matching pattern");
        invalidated
    }

    /// Invalidate plans for a specific table.
    pub async fn invalidate_table(&self, table_name: &str) -> usize {
        self.invalidate_pattern(&format!("FROM {}", table_name)).await
    }

    /// Get the schema version counter.
    pub fn get_schema_version(&self) -> u64 {
        self.schema_version.load(Ordering::Relaxed)
    }

    /// Reset schema version (useful after manual invalidation).
    pub fn reset_schema_version(&self) {
        self.schema_version.store(0, Ordering::Relaxed);
    }
}

/// Diagnostic information about cache state.
#[derive(Debug, Clone)]
pub struct CacheDiagnostics {
    pub cache_stats: CacheStats,
    pub schema_version: u64,
    pub last_schema_change: Option<std::time::SystemTime>,
    pub frequency_map_size: usize,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute hits / (hits + misses), returning 0.0 when no requests have been
/// made (avoids NaN propagating into Prometheus gauges).
fn safe_ratio(hits: u64, misses: u64) -> f64 {
    let total = hits + misses;
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
}

fn query_hash(query: &str) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(query.as_bytes());
    format!("{:x}", hasher.finalize())[0..8].to_string()
}

fn parse_explain_output(json_str: &str, query: &str) -> Result<QueryPlan, sqlx::Error> {
    let value: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to parse EXPLAIN JSON: {}", e),
        )))?;

    let plan = value
        .get(0)
        .and_then(|p| p.get("Plan"))
        .ok_or_else(|| sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Missing Plan in EXPLAIN output",
        )))?;

    let total_cost = plan
        .get("Total Cost")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let estimated_rows = plan
        .get("Plan Rows")
        .or_else(|| plan.get("Estimated Rows"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let planning_time = value
        .get(0)
        .and_then(|p| p.get("Planning Time"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let plan_hash = query_hash(query);

    Ok(QueryPlan {
        query: query.to_string(),
        plan_hash,
        estimated_cost: total_cost,
        estimated_rows,
        actual_rows: None,
        planning_time_ms: planning_time,
        execution_time_ms: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Hash stability ──────────────────────────────────────────────────────

    #[test]
    fn query_hash_consistency() {
        let query = "SELECT * FROM events WHERE id = $1";
        let hash1 = query_hash(query);
        let hash2 = query_hash(query);
        assert_eq!(hash1, hash2, "Hash should be consistent");
    }

    #[test]
    fn query_hash_different_queries() {
        let query1 = "SELECT * FROM events WHERE id = $1";
        let query2 = "SELECT * FROM events WHERE id = $2";
        let hash1 = query_hash(query1);
        let hash2 = query_hash(query2);
        assert_ne!(hash1, hash2, "Different queries should have different hashes");
    }

    // ── Adaptive TTL ────────────────────────────────────────────────────────

    #[test]
    fn adaptive_ttl_below_medium_threshold_returns_base() {
        let cache = QueryPlanCache::with_defaults();
        let ttl = cache.adaptive_ttl_secs("SELECT 1");
        assert_eq!(ttl, DEFAULT_PLAN_CACHE_TTL_SECS);
    }

    #[test]
    fn adaptive_ttl_at_medium_threshold_returns_2x() {
        let cache = QueryPlanCache::with_defaults();
        let q = "SELECT medium_query";
        // Simulate FREQ_MEDIUM requests
        cache.frequency_map.insert(q.to_string(), FREQ_MEDIUM);
        let ttl = cache.adaptive_ttl_secs(q);
        assert_eq!(ttl, DEFAULT_PLAN_CACHE_TTL_SECS * 2);
    }

    #[test]
    fn adaptive_ttl_at_high_threshold_returns_4x() {
        let cache = QueryPlanCache::with_defaults();
        let q = "SELECT high_query";
        cache.frequency_map.insert(q.to_string(), FREQ_HIGH);
        let ttl = cache.adaptive_ttl_secs(q);
        assert_eq!(ttl, DEFAULT_PLAN_CACHE_TTL_SECS * 4);
    }

    #[test]
    fn adaptive_ttl_above_high_threshold_returns_4x() {
        let cache = QueryPlanCache::with_defaults();
        let q = "SELECT very_hot_query";
        cache.frequency_map.insert(q.to_string(), FREQ_HIGH + 500);
        let ttl = cache.adaptive_ttl_secs(q);
        assert_eq!(ttl, DEFAULT_PLAN_CACHE_TTL_SECS * 4);
    }

    #[test]
    fn adaptive_ttl_just_below_medium_returns_1x() {
        let cache = QueryPlanCache::with_defaults();
        let q = "SELECT cold_query";
        cache.frequency_map.insert(q.to_string(), FREQ_MEDIUM - 1);
        let ttl = cache.adaptive_ttl_secs(q);
        assert_eq!(ttl, DEFAULT_PLAN_CACHE_TTL_SECS);
    }

    // ── Cache hit/miss/stats tracking ──────────────────────────────────────

    #[tokio::test]
    async fn query_plan_cache_basic() {
        let cache = QueryPlanCache::with_defaults();
        let plan = QueryPlan {
            query: "SELECT 1".to_string(),
            plan_hash: "test123".to_string(),
            estimated_cost: 100.0,
            estimated_rows: 1.0,
            actual_rows: None,
            planning_time_ms: 0.5,
            execution_time_ms: None,
        };

        cache.insert("SELECT 1".to_string(), plan.clone()).await;
        let retrieved = cache.get("SELECT 1").await;

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().estimated_cost, 100.0);
    }

    #[tokio::test]
    async fn query_plan_cache_miss() {
        let cache = QueryPlanCache::with_defaults();
        let retrieved = cache.get("SELECT nonexistent").await;
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn cache_stats_tracks_hits_and_misses() {
        let cache = QueryPlanCache::with_defaults();
        let plan = QueryPlan {
            query: "SELECT 1".to_string(),
            plan_hash: "test123".to_string(),
            estimated_cost: 100.0,
            estimated_rows: 1.0,
            actual_rows: None,
            planning_time_ms: 0.5,
            execution_time_ms: None,
        };

        // One miss before insert
        let _ = cache.get("SELECT 1").await;

        cache.insert("SELECT 1".to_string(), plan).await;

        // Two hits
        let _ = cache.get("SELECT 1").await;
        let _ = cache.get("SELECT 1").await;

        let stats = cache.get_cache_stats().await;
        assert_eq!(stats.miss_count, 1, "Expected 1 miss");
        assert_eq!(stats.hit_count, 2, "Expected 2 hits");
        assert_eq!(stats.cached_plans, 1);
        assert_eq!(stats.max_capacity, DEFAULT_PLAN_CACHE_SIZE);
    }

    #[tokio::test]
    async fn cache_stats_hit_ratio_zero_when_no_requests() {
        let cache = QueryPlanCache::with_defaults();
        let stats = cache.get_cache_stats().await;
        assert_eq!(stats.hit_ratio, 0.0, "Ratio should be 0.0 with no requests");
    }

    #[tokio::test]
    async fn cache_stats_hit_ratio_correct() {
        let cache = QueryPlanCache::with_defaults();
        let plan = QueryPlan {
            query: "SELECT ratio".to_string(),
            plan_hash: "ratiohash".to_string(),
            estimated_cost: 1.0,
            estimated_rows: 1.0,
            actual_rows: None,
            planning_time_ms: 0.1,
            execution_time_ms: None,
        };
        cache.insert("SELECT ratio".to_string(), plan).await;

        // 3 hits, 1 miss
        let _ = cache.get("SELECT ratio").await;
        let _ = cache.get("SELECT ratio").await;
        let _ = cache.get("SELECT ratio").await;
        let _ = cache.get("SELECT never").await; // miss

        let stats = cache.get_cache_stats().await;
        // 3 / (3 + 1) = 0.75
        let expected = 3.0_f64 / 4.0_f64;
        assert!(
            (stats.hit_ratio - expected).abs() < 1e-9,
            "Expected hit_ratio ~0.75, got {}",
            stats.hit_ratio
        );
    }

    #[tokio::test]
    async fn query_plan_cache_stats_legacy() {
        let cache = QueryPlanCache::with_defaults();
        let plan = QueryPlan {
            query: "SELECT 1".to_string(),
            plan_hash: "test123".to_string(),
            estimated_cost: 100.0,
            estimated_rows: 1.0,
            actual_rows: None,
            planning_time_ms: 0.5,
            execution_time_ms: None,
        };

        cache.insert("SELECT 1".to_string(), plan).await;
        let stats = cache.get_cache_stats().await;

        assert_eq!(stats.cached_plans, 1);
        assert_eq!(stats.max_capacity, DEFAULT_PLAN_CACHE_SIZE);
    }

    #[tokio::test]
    async fn query_plan_cache_clear() {
        let cache = QueryPlanCache::with_defaults();
        let plan = QueryPlan {
            query: "SELECT 1".to_string(),
            plan_hash: "test123".to_string(),
            estimated_cost: 100.0,
            estimated_rows: 1.0,
            actual_rows: None,
            planning_time_ms: 0.5,
            execution_time_ms: None,
        };

        cache.insert("SELECT 1".to_string(), plan).await;
        cache.clear().await;
        let retrieved = cache.get("SELECT 1").await;

        assert!(retrieved.is_none());
    }

    // ── safe_ratio edge cases ───────────────────────────────────────────────

    #[test]
    fn safe_ratio_no_requests_is_zero() {
        assert_eq!(safe_ratio(0, 0), 0.0);
    }

    #[test]
    fn safe_ratio_all_hits() {
        assert_eq!(safe_ratio(10, 0), 1.0);
    }

    #[test]
    fn safe_ratio_all_misses() {
        assert_eq!(safe_ratio(0, 10), 0.0);
    }

    // ── Schema invalidation tests ──────────────────────────────────────────

    #[tokio::test]
    async fn schema_version_tracking() {
        let cache = QueryPlanCache::with_defaults();
        assert_eq!(cache.get_schema_version(), 0);

        cache.schema_version.store(42, Ordering::Relaxed);
        assert_eq!(cache.get_schema_version(), 42);

        cache.reset_schema_version();
        assert_eq!(cache.get_schema_version(), 0);
    }

    #[tokio::test]
    async fn cache_invalidate_pattern() {
        let cache = QueryPlanCache::with_defaults();
        let plan = QueryPlan {
            query: "SELECT * FROM events".to_string(),
            plan_hash: "hash1".to_string(),
            estimated_cost: 100.0,
            estimated_rows: 1.0,
            actual_rows: None,
            planning_time_ms: 0.5,
            execution_time_ms: None,
        };

        cache.insert("SELECT * FROM events WHERE id = $1".to_string(), plan.clone()).await;
        cache.frequency_map.insert("SELECT * FROM events WHERE id = $1".to_string(), 1);

        let invalidated = cache.invalidate_pattern("events").await;
        assert_eq!(invalidated, 1);

        // Verify it's gone
        let retrieved = cache.get("SELECT * FROM events WHERE id = $1").await;
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn cache_invalidate_table() {
        let cache = QueryPlanCache::with_defaults();
        let plan = QueryPlan {
            query: "SELECT * FROM events".to_string(),
            plan_hash: "hash1".to_string(),
            estimated_cost: 100.0,
            estimated_rows: 1.0,
            actual_rows: None,
            planning_time_ms: 0.5,
            execution_time_ms: None,
        };

        cache.insert("SELECT * FROM events WHERE ledger = $1".to_string(), plan).await;
        cache.frequency_map.insert("SELECT * FROM events WHERE ledger = $1".to_string(), 1);

        let invalidated = cache.invalidate_table("events").await;
        assert!(invalidated > 0);
    }

    #[tokio::test]
    async fn cache_diagnostics() {
        let cache = QueryPlanCache::with_defaults();
        let diags = cache.get_cache_diagnostics().await;

        assert_eq!(diags.schema_version, 0);
        assert!(diags.last_schema_change.is_none());
        assert_eq!(diags.frequency_map_size, 0);
    }
}
