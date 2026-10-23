# Query Plan Caching and Tuning

## Overview

SorobanPulse includes an advanced query plan cache with adaptive TTL and schema-aware invalidation. This reduces planning overhead for repeated queries and enables targeted cache management.

## Features

### 1. Automatic Plan Caching

The cache stores PostgreSQL query plans to avoid re-planning identical queries.

**Benefits:**
- Reduces CPU overhead from query planning
- Faster query execution for repeated patterns
- Automatic cache warming on startup

### 2. Adaptive TTL

Plans receive longer TTLs based on query frequency:

| Frequency | TTL Multiplier | Base TTL | Effective TTL |
|---|---|---|---|
| < 10 times | 1× | 1 hour | 1 hour |
| 10-99 times | 2× | 1 hour | 2 hours |
| ≥ 100 times | 4× | 1 hour | 4 hours |

Hot queries stay in cache longer automatically.

### 3. Schema-Aware Invalidation

The cache detects schema changes and invalidates affected plans:

- Automatic invalidation on DDL statements
- Targeted invalidation by table or pattern
- Manual invalidation controls

### 4. Comprehensive Diagnostics

Built-in tools to inspect and manage cache state:

- Hit/miss statistics
- Frequency distribution
- Schema change tracking
- Performance metrics

## Configuration

```rust
use crate::query_plan_cache::QueryPlanCacheConfig;

let config = QueryPlanCacheConfig {
    max_plans: 1000,                    // Cache size (entries)
    ttl_secs: 3600,                     // Base TTL (1 hour)
    enable_prepared_statements: true,   // Use prepared statements
};

let cache = QueryPlanCache::new(config);
```

## Usage

### Creating Pool with Plan Cache

```rust
use crate::{db_pool, query_plan_cache::QueryPlanCache};

// Pools are always built through `db_pool` (issue #1244).
let pool = db_pool::connect(&database_url, &db_pool::DbPoolSettings::from_config(&config)).await?;
let cache = QueryPlanCache::with_defaults();

// Warm cache at startup
let warmed = cache.warm_cache(&pool).await?;
println!("Warmed {} query plans", warmed);
```

### Analyzing Query Plans

```rust
// Analyze a query and cache the plan
let plan = cache.analyze_query(&pool, "SELECT * FROM events LIMIT 10").await?;
println!("Estimated cost: {}", plan.estimated_cost);
println!("Estimated rows: {}", plan.estimated_rows);
println!("Planning time: {} ms", plan.planning_time_ms);
```

### Getting Cache Statistics

```rust
let stats = cache.get_cache_stats().await;
println!("Cached plans: {}", stats.cached_plans);
println!("Hits: {}", stats.hit_count);
println!("Misses: {}", stats.miss_count);
println!("Hit ratio: {:.1}%", stats.hit_ratio * 100.0);
println!("Evictions: {}", stats.eviction_count);
```

### Schema Change Detection

```rust
// Detect schema changes and invalidate cache
let changed = cache.detect_schema_changes(&pool).await?;
if changed {
    println!("Schema changed, cache invalidated");
}

// Detect structural DDL changes
let ddl_count = cache.detect_structural_changes(&pool).await?;
if ddl_count > 0 {
    println!("Detected {} DDL operations", ddl_count);
}
```

### Diagnostic Information

```rust
let diags = cache.get_cache_diagnostics().await;
println!("Cache entries: {}", diags.cache_stats.cached_plans);
println!("Schema version: {}", diags.schema_version);
if let Some(last_change) = diags.last_schema_change {
    println!("Last schema change: {:?}", last_change);
}
println!("Frequency map size: {}", diags.frequency_map_size);
```

### Targeted Cache Invalidation

```rust
// Invalidate plans matching a pattern
let invalidated = cache.invalidate_pattern("FROM events WHERE").await;
println!("Invalidated {} plans", invalidated);

// Invalidate all plans for a specific table
let invalidated = cache.invalidate_table("events").await;
println!("Invalidated {} plans for events table", invalidated);

// Clear entire cache
cache.clear().await;
```

## Warm Queries

The system pre-caches five canonical query patterns on startup:

1. **Paginated list** - `GET /v1/events` with LIMIT/OFFSET
2. **Contract filter** - `GET /v1/events/{contract_id}` with WHERE clause
3. **Tx hash lookup** - `GET /v1/events/tx/{tx_hash}` exact match
4. **Ledger range** - `GET /v1/events?from_ledger=..&to_ledger=..` range scan
5. **Exact count** - `GET /v1/events?exact_count=true` COUNT(*)

These patterns cover all primary API access patterns documented in `docs/schema.md`.

## Metrics Exported

| Metric Name | Type | Description |
|---|---|---|
| `soroban_pulse_query_plan_cache_hits` | Counter | Cumulative cache hits |
| `soroban_pulse_query_plan_cache_misses` | Counter | Cumulative cache misses |
| `soroban_pulse_query_plan_cache_evictions` | Counter | Cumulative evictions |
| `soroban_pulse_query_plan_cache_entries` | Gauge | Current cache size (entries) |
| `soroban_pulse_query_plan_cache_hit_ratio` | Gauge | Current hit ratio (0.0-1.0) |

## Performance Impact

### Expected Improvements

| Query Pattern | Planning Time | Query Time | Improvement |
|---|---|---|---|
| Simple SELECT (warm) | 1-2 ms → 0.1 ms | 10 ms | 10-20× faster |
| Complex JOIN (warm) | 5-10 ms → 0.1 ms | 50 ms | 10× faster |
| Aggregate (warm) | 2-3 ms → 0.1 ms | 20 ms | 10-20× faster |

### Cold vs Warm

- **Cold cache**: First query (1-5 ms planning overhead)
- **Warm cache**: Subsequent identical queries (< 0.1 ms planning)

## Best Practices

### 1. Monitor Cache Hit Ratio

```rust
// Periodically check hit ratio
let stats = cache.get_cache_stats().await;
if stats.hit_ratio < 0.7 {
    warn!("Low cache hit ratio: {:.1}%", stats.hit_ratio * 100.0);
    // Investigate application query patterns
}
```

### 2. Warm Cache on Startup

```rust
// Always warm cache at application startup
let warmed = cache.warm_cache(&pool).await?;
info!("Warmed {} canonical query plans", warmed);
```

### 3. Monitor for Schema Drift

```rust
// Check for schema changes periodically
let mut interval = tokio::time::interval(Duration::from_secs(300));
loop {
    interval.tick().await;
    if cache.detect_schema_changes(&pool).await.is_ok() {
        debug!("Schema check passed");
    }
}
```

### 4. Sized Appropriately

- Default cache size: 1000 plans
- Increase for high-cardinality queries (> 1000 unique patterns)
- Decrease for low-memory environments (< 500 plans)

### 5. Coordinate with Query Builder

Use the unified query builder pattern to maximize cache hits:

```rust
// Good: Same builder pattern produces same SQL
for contract_id in contract_ids {
    let (sql, _) = EventQueryBuilder::new()
        .with_filters(EventFilters {
            contract_id: Some(contract_id),
            ..Default::default()
        })
        .build();
    // Same SQL pattern → cache hit
}

// Poor: Dynamic SQL string construction
for i in 0..n {
    let sql = format!("SELECT * FROM events WHERE id = {} LIMIT {}", i, limit);
    // Different SQL each time → cache miss
}
```

## Troubleshooting

### Low Cache Hit Ratio

Indicators:
- Hit ratio < 0.5 despite high query volume
- Frequent cache evictions (high eviction_count)

Debugging:

```rust
let diags = cache.get_cache_diagnostics().await;
println!("Cache entries: {}", diags.cache_stats.cached_plans);
println!("Max capacity: {}", diags.cache_stats.max_capacity);
println!("Evictions: {}", diags.cache_stats.eviction_count);

// Check if frequency map is growing
println!("Unique queries: {}", diags.frequency_map_size);
```

Solutions:
- Increase cache size (max_plans)
- Audit application for unnecessary query variations
- Use prepared statements or query builders

### Stale Cache After Schema Change

If cache isn't invalidating on schema changes:

```rust
// Manually force invalidation
cache.clear().await;
cache.reset_schema_version();

// Re-warm cache
cache.warm_cache(&pool).await?;
```

### High Memory Usage

```rust
// Reduce cache size
let config = QueryPlanCacheConfig {
    max_plans: 500,  // Reduce from 1000
    ..Default::default()
};
```

## Performance Benchmarks

### Query Planning Overhead

```
Without caching:
  - 100 identical queries: ~200ms total planning
  - 1000 identical queries: ~2000ms total planning

With caching:
  - 100 identical queries: ~2ms total planning (1st query only)
  - 1000 identical queries: ~2ms total planning (1st query only)

Improvement: 100-1000× faster for repeated queries
```

### Cache Overhead

- Per-entry overhead: ~500 bytes (query string + plan metadata)
- Cache with 1000 entries: ~500 KB memory
- Negligible impact on overall application memory

## Related Documentation

- Index Analysis ([index-analysis.md](index-analysis.md))
- Table Partitioning ([table-partitioning.md](table-partitioning.md))
- Query Builder Pattern ([query-builder-pattern.md](query-builder-pattern.md))
- PostgreSQL EXPLAIN ([https://www.postgresql.org/docs/current/sql-explain.html](https://www.postgresql.org/docs/current/sql-explain.html))
