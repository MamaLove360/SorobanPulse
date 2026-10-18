# Table Partitioning Strategy

## Overview

SorobanPulse uses PostgreSQL range partitioning to improve query performance and enable efficient data archival. The system supports both timestamp-based (monthly) and ledger-based partitioning.

## Partitioning Strategies

### 1. Timestamp-Based Partitioning (Monthly)

Events are automatically partitioned by month using the `timestamp` column.

**Benefits:**
- Natural time-based decay (recent data queried most)
- Easy archival of old data
- Effective partition pruning for range queries

**Partition Naming:** `events_YYYY_MM`

### 2. Ledger-Based Partitioning (Range)

For ledger sequence-based queries, events can be partitioned by ledger ranges.

**Benefits:**
- Aligned with blockchain block heights
- Efficient for ledger range queries
- Natural boundaries for data rotation

**Partition Naming:** `events_ledger_SSSSSSSSSS_EEEEEEEEEE`

**Example:**
- `events_ledger_0000000000_1000000000` (ledgers 0-999,999,999)
- `events_ledger_1000000000_2000000000` (ledgers 1B-1.999B)

## Configuration

### Ledger Partition Config

```rust
use crate::partition_manager::LedgerPartitionConfig;

let config = LedgerPartitionConfig {
    ledger_range_per_partition: 1_000_000,      // 1M ledgers per partition
    auto_create_partitions: true,               // Auto-create future partitions
    max_partitions_before_archival: 12,         // Archive when > 12 active
};
```

### Archival Config

```rust
use crate::partition_manager::PartitionArchivalConfig;

let config = PartitionArchivalConfig {
    archive_after_months: 12,                   // Archive after 1 year
    delete_after_months: 24,                    // Drop after 2 years
    archive_table_prefix: "archive_".to_string(),
    dry_run: false,                             // Execute archival
};
```

## Usage

### Starting the Partition Manager

```rust
use crate::partition_manager;

let pool = create_pool().await?;

// Spawn partition management background task
// Creates future partitions and refreshes statistics every 24 hours
partition_manager::spawn(
    pool.clone(),
    86400,              // 24 hours in seconds
    3,                  // 3 months ahead
    shutdown_signal,
);
```

### Creating Ledger Partitions

```rust
// Create a specific ledger partition
let name = partition_manager::create_ledger_partition(&pool, 0, 1_000_000).await?;
println!("Created: {}", name);

// Automatically create next 5 ledger partitions
let created = partition_manager::create_future_ledger_partitions(
    &pool,
    &config,
    5
).await?;
```

### Partition Rotation

```rust
// Rotate partitions: archive old ones, create new ones
partition_manager::rotate_ledger_partitions(&pool, &config).await?;
```

### Analyzing Partitions

```rust
// List all partitions
let partitions = partition_manager::list_ledger_partitions(&pool).await?;
for p in partitions {
    println!("{}: ledgers {}-{} ({} rows, {} bytes)",
        p.partition_name, p.start_ledger, p.end_ledger,
        p.row_count, p.size_bytes
    );
}

// Get comprehensive statistics
let stats = partition_manager::get_ledger_partition_stats(&pool).await?;
println!("Total: {}, Active: {}, Archived: {}",
    stats.total_partitions,
    stats.active_partitions,
    stats.archived_partitions
);

// Analyze partition pruning effectiveness
let report = partition_manager::analyze_partition_pruning(
    &pool,
    from_ts,
    to_ts
).await?;
println!("Pruning effectiveness: {:.1}%",
    report.pruning_effectiveness * 100.0
);
```

## Partition Lifecycle

```
[Active] → [Hot] → [Warm] → [Cold] → [Archive] → [Delete]
```

1. **Active** (0-1 month old)
   - Receives new inserts
   - Indexes maintained
   - Most queries hit this

2. **Hot** (1-3 months old)
   - Frequently accessed
   - Statistics refreshed regularly
   - Full indexes available

3. **Warm** (3-6 months old)
   - Occasional queries
   - Less frequent statistics updates
   - Can consolidate indexes

4. **Cold** (6-12 months old)
   - Rarely queried
   - Candidate for archival
   - Minimal maintenance

5. **Archive** (12-24 months old)
   - Detached from main table
   - Low-access use cases only
   - Can use compressed storage

6. **Delete** (24+ months old)
   - Dropped from database
   - May be backed up separately

## Metrics Exported

| Metric Name | Type | Description |
|---|---|---|
| `soroban_pulse_partition_count` | Gauge | Total number of partitions |
| `soroban_pulse_hot_partitions_count` | Gauge | Number of recently accessed partitions |
| `soroban_pulse_partition_total_size_bytes` | Gauge | Total size of all partitions |
| `soroban_pulse_partition_skew_max` | Gauge | Maximum row count skew across partitions |
| `soroban_pulse_partition_created_total` | Counter | Partitions created since startup |
| `soroban_pulse_archived_partitions_total` | Counter | Partitions archived since startup |
| `soroban_pulse_ledger_partitions_total` | Gauge | Total ledger-based partitions |
| `soroban_pulse_ledger_partitions_active` | Gauge | Active ledger partitions |
| `soroban_pulse_ledger_partitions_archived` | Gauge | Archived ledger partitions |

## Performance Considerations

### Partition Pruning

Effective partition pruning requires:

```sql
-- Good: Partition pruning enabled
SELECT * FROM events 
WHERE timestamp >= '2026-01-01' AND timestamp < '2026-02-01'
LIMIT 100;

-- Poor: Forces full scan (no pruning)
SELECT * FROM events 
WHERE EXTRACT(MONTH FROM timestamp) = 1
LIMIT 100;
```

### Optimal Partition Size

- **Timestamp-based**: 1 month (natural calendar boundaries)
- **Ledger-based**: 1-5 million ledgers (balance between scan speed and partition overhead)

### Index Strategy per Partition

Each partition should have:
1. Primary key index (automatic)
2. Common filter column index (e.g., contract_id)
3. Range query index (e.g., ledger or timestamp)

## Archival Workflow

### Manual Archival

```rust
let config = PartitionArchivalConfig {
    archive_after_months: 12,
    delete_after_months: 24,
    dry_run: true,  // Test first!
    ..Default::default()
};

let cold = partition_manager::identify_cold_partitions(&pool, 12).await?;
for partition in cold {
    // Review before archiving
    println!("Archive {}: {} rows", partition.table_name, partition.row_count);
    
    let result = partition_manager::archive_partition(&pool, &partition.table_name, &config).await?;
    println!("{}", result);
}
```

### Automatic Archival

For production: set `dry_run: false` in the rotation job.

## Capacity Planning

```rust
let forecast = partition_manager::forecast_capacity(&pool, 90).await?;

println!("Current: {} GB", forecast.current_size_bytes / (1024*1024*1024));
println!("Growth: {} MB/day", 
    forecast.growth_rate_bytes_per_day / (1024*1024));
println!("Days until 1TB: {:?}", forecast.days_until_threshold);

for (partition_name, est_size) in forecast.forecast_partitions {
    println!("  {}: {} MB", partition_name, est_size / (1024*1024));
}
```

## Troubleshooting

### Uneven Partition Sizes

Check row count skew:

```rust
let skew = partition_manager::calculate_partition_skew(&pool).await?;
for (name, count, factor) in skew {
    if factor > 2.0 {
        println!("High skew: {} (factor: {:.2}x)", name, factor);
    }
}
```

### Missing Future Partitions

Manually create:

```rust
partition_manager::create_future_partitions(&pool, 12).await?;
```

### Slow Partition Queries

Refresh statistics:

```rust
partition_manager::refresh_partition_statistics(&pool).await?;
```

## Related Documentation

- Query Plan Caching ([query-plan-tuning.md](query-plan-tuning.md))
- Index Analysis ([index-analysis.md](index-analysis.md))
- Database Migrations ([../migrations/](../migrations/))
