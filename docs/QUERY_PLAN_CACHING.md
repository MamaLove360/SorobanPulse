# PostgreSQL Query Plan Caching (Issue #689)

## Overview

This document describes the query plan caching feature that reduces database planning overhead by caching EXPLAIN outputs and reusing them across similar queries.

## Problem Statement

PostgreSQL must create an execution plan for every query. For frequently repeated queries (even with different parameter values), this planning overhead accumulates:

- Simple query: 0.5-2ms planning time
- Complex query: 2-10ms planning time
- 1000 queries/sec × 1ms avg = 1 second of planning overhead per second

## Solution: Query Plan Caching

The query plan cache stores and reuses query execution plans based on query structure (parameterized queries are treated the same).

### Benefits

1. **Reduced Planning Overhead** - 0.5-2ms saved per cached query
2. **Faster Query Execution** - Immediate plan availability
3. **Improved Throughput** - More time for actual execution
4. **Better Scalability** - Handles higher query volumes

## Architecture

### QueryPlanCache Structure

```rust
pub struct QueryPlanCache {
    cache: Arc<Cache<String, QueryPlan>>,
    config: QueryPlanCacheConfig,
}
```

### QueryPlan Structure

```rust
pub struct QueryPlan {
    pub query: String,
    pub plan_hash: String,
    pub estimated_cost: f64,
    pub estimated_rows: f64,
    pub actual_rows: Option<f64>,
    pub planning_time_ms: f64,
    pub execution_time_ms: Option<f64>,
}
```

## Configuration

### Cache Size

Default: 1,000 plans
- Adjust based on query diversity
- Monitor cache hit ratio

### TTL (Time To Live)

Default: 3,600 seconds (1 hour)
- Plans remain valid as long as schema doesn't change
- Automatic invalidation on migration

### Prepared Statements

Enabled by default. Benefits:
- Client-side query validation
- Protection against SQL injection
- Better performance for repeated queries

## Usage

### Basic Usage

```rust
let cache = QueryPlanCache::with_defaults();

// Analyze query (uses cache if available)
let plan = cache.analyze_query(&pool, query_str).await?;
println!("Estimated cost: {}", plan.estimated_cost);
println!("Planning time: {}ms", plan.planning_time_ms);
```

### With Custom Configuration

```rust
let config = QueryPlanCacheConfig {
    max_plans: 5000,
    ttl_secs: 7200,
    enable_prepared_statements: true,
};
let cache = QueryPlanCache::new(config);
```

### Creating Pool with Plan Cache

```rust
// Pools are always built through `db_pool` (issue #1244).
let pool = db_pool::connect(&database_url, &db_pool::DbPoolSettings::from_config(&config)).await?;
let cache = QueryPlanCache::with_defaults();
```

## Performance Metrics

### Metrics Collected

1. **Cache Hit Rate**
   ```
   soroban_pulse_query_plan_cache_hits_total      # counter — cumulative hits
   soroban_pulse_query_plan_cache_misses_total    # counter — cumulative misses
   soroban_pulse_query_plan_cache_hit_ratio       # gauge   — hits/(hits+misses), 0–1
   ```

2. **Planning Time**
   ```
   soroban_pulse_query_planning_time_ms           # histogram — per-query planning time
   ```

3. **Plans Cached & Evicted** *(added in #802)*
   ```
   soroban_pulse_query_plans_cached_total         # counter — cumulative inserts
   soroban_pulse_query_plan_cache_evictions_total # counter — LRU/TTL evictions
   soroban_pulse_query_plan_cache_entry_count     # gauge   — live entries right now
   ```

### Monitoring Queries

```promql
# Cache hit ratio
rate(soroban_pulse_query_plan_cache_hits_total[5m]) / 
  (rate(soroban_pulse_query_plan_cache_hits_total[5m]) + 
   rate(soroban_pulse_query_plan_cache_misses_total[5m]))

# Average planning time
histogram_quantile(0.95, rate(soroban_pulse_query_planning_time_ms_bucket[5m]))

# Total time saved by caching (estimate)
rate(soroban_pulse_query_plan_cache_hits_total[5m]) * 1.5  # avg 1.5ms per query
```

## Best Practices

### 1. Use Parameterized Queries

Parameterized queries share the same cache entry:
```rust
// ✓ Good - uses cache entry
let plan = cache.analyze_query(&pool, 
    "SELECT * FROM events WHERE id = $1").await?;

// ✗ Bad - separate cache entry
let plan = cache.analyze_query(&pool,
    &format!("SELECT * FROM events WHERE id = {}", id)).await?;
```

### 2. Appropriate Cache Size

```rust
// Low-cardinality queries (e.g., standard API endpoints)
max_plans: 100

// Medium-cardinality queries
max_plans: 1000  // default

// High-cardinality queries (many different queries)
max_plans: 10000
```

### 3. TTL Settings

```rust
// Frequently changing schema
ttl_secs: 300  // 5 minutes

// Stable production schema
ttl_secs: 3600  // 1 hour (default)
```

### 4. Monitor Hit Rates

Track cache effectiveness:
- **Target**: 80%+ hit ratio for stable workloads
- **Alert**: <60% hit ratio may indicate:
  - Cache too small
  - Too many distinct queries
  - Schema changes invalidating plans

## EXPLAIN OUTPUT Integration

The cache integrates with PostgreSQL's EXPLAIN output:

```sql
EXPLAIN (FORMAT JSON, ANALYZE OFF)
SELECT * FROM events WHERE id = $1;
```

Extracted metrics:
- Total Cost
- Estimated Rows
- Planning Time
- Node Type
- Indexes Used

## Connection Pool Integration

Prepared statements work best with persistent connections:

```rust
let pool = PgPoolOptions::new()
    .max_connections(50)
    .min_connections(5)
    .test_before_acquire(true)
    .connect(database_url)
    .await?;
```

Benefits of persistent connections:
1. Prepared statements stay prepared
2. Cache efficiency maximized
3. Reduced connection overhead

## Limitations

1. **Plan Invalidation**
   - Plans become invalid after schema changes
   - Use TTL to expire stale plans
   - Manual invalidation on migrations

2. **Parameter Sensitivity**
   - Different parameter types = same plan
   - Statistics-based optimization may vary

3. **Version Compatibility**
   - Plans tied to PostgreSQL version
   - Upgrade may require cache flush

## Migration Strategy

### Phase 1: Monitor
- Enable with default settings
- Monitor hit rates in staging

### Phase 2: Optimize
- Adjust cache size based on query patterns
- Tune TTL for your schema change frequency

### Phase 3: Production
- Deploy with validated configuration
- Monitor metrics continuously
- Set up alerting for low hit rates

## Troubleshooting

### Low Hit Rates
1. Check cache size: `cache.get_cache_stats().await`
2. Review query patterns
3. Increase `max_plans` if needed
4. Verify parameterized query usage

### Memory Usage
Monitor cache size:
```
soroban_pulse_query_plans_cached_total
  / soroban_pulse_query_plan_cache_max_capacity
```

Typical memory per cached plan: ~500 bytes - 2KB

### Cache Misses After Schema Changes
- Plans automatically expire after TTL
- Manual invalidation: `cache.clear().await`
- Consider TTL for change frequency

## Testing

Run plan cache benchmarks:
```bash
cargo bench --bench query_planning
```

Benchmarks measure:
- Query hashing performance
- Cache lookup performance
- EXPLAIN parsing overhead
- Prepared statement creation

## Future Enhancements

1. **Statistics-aware Caching** - Invalidate when statistics change
2. ~~**Warm-up Cache** - Pre-populate on startup~~ — **Implemented in #802** via `warm_cache()` called at startup, priming the five canonical query patterns
3. ~~**Adaptive TTL** - Extend TTL for frequently used queries~~ — **Implemented in #802**: queries with ≥10 requests get 2× TTL; ≥100 requests get 4× TTL
4. **Distributed Caching** - Share cache across replicas
5. **Plan Cost Tracking** - Alert on slow plans
6. **Custom Serializers** - Optimize plan storage

## References

- [PostgreSQL EXPLAIN Documentation](https://www.postgresql.org/docs/current/sql-explain.html)
- [Query Planner Optimization](https://www.postgresql.org/docs/current/planner.html)
- [Prepared Statements](https://www.postgresql.org/docs/current/sql-prepare.html)
