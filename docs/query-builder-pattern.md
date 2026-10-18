# Unified Query Builder Pattern

## Overview

The query builder module provides a type-safe, fluent interface for constructing SQL queries. This consolidates scattered SQL strings into a single, reusable, and maintainable location.

## Design Principles

1. **Type Safety**: Catch errors at compile time, not runtime
2. **Fluent API**: Chain methods for readable query construction
3. **Reusability**: Build complex queries from simple components
4. **Maintainability**: SQL strings in one place, easy to update
5. **Performance**: Leverage database statement caching

## Architecture

### QueryBuilder Trait

All query builders implement the common `QueryBuilder` trait:

```rust
pub trait QueryBuilder {
    /// Build and return the SQL query string.
    fn build_sql(&self) -> String;

    /// Get the number of bind parameters required.
    fn bind_count(&self) -> usize;

    /// Build both the data query and count query.
    fn build_pair(&self) -> (String, String);
}
```

### EventQueryBuilder

The primary builder for event queries.

```rust
let builder = EventQueryBuilder::new()
    .with_filters(filters)
    .paginate(page, limit)
    .sort_by("ledger", SortDirection::Desc)
    .build();

let (sql, count_sql) = builder;
```

## Basic Usage

### Creating Simple Queries

```rust
use crate::query_builder::{EventQueryBuilder, EventFilters};

// Default query: all events, page 1, 20 per page
let (sql, count_sql) = EventQueryBuilder::new().build();

// Query with filters
let filters = EventFilters {
    contract_id: Some("CAAAA...".into()),
    from_ledger: Some(1_000_000),
    to_ledger: Some(2_000_000),
    ..Default::default()
};

let (sql, count_sql) = EventQueryBuilder::new()
    .with_filters(filters)
    .build();
```

### Pagination

```rust
// Page 2, 50 items per page
let builder = EventQueryBuilder::new()
    .paginate(2, 50)
    .build();

// Accessing pagination info
let builder = EventQueryBuilder::new().paginate(3, 15);
let p = builder.get_pagination();
assert_eq!(p.page, 3);
assert_eq!(p.limit(), 15);  // Clamped to MAX_LIMIT
```

### Sorting

```rust
use crate::query_builder::SortDirection;

// Sort by timestamp ascending
let (sql, _) = EventQueryBuilder::new()
    .sort_by("timestamp", SortDirection::Asc)
    .build();

// Sort by created_at descending (default column is "ledger")
let (sql, _) = EventQueryBuilder::new()
    .sort_by("created_at", SortDirection::Desc)
    .build();

// Accessing sort configuration
let builder = EventQueryBuilder::new().sort_by("id", SortDirection::Asc);
let (column, direction) = builder.get_sort();
assert_eq!(column, "id");
assert_eq!(direction, SortDirection::Asc);
```

## Advanced Features

### Custom WHERE Clauses

```rust
// Add arbitrary WHERE clauses
let (sql, _) = EventQueryBuilder::new()
    .with_where_clause("status = 'active'".to_string())
    .with_where_clause("priority > 5".to_string())
    .build();

// Accessing WHERE clauses
let builder = EventQueryBuilder::new()
    .with_where_clause("x = 1".to_string())
    .with_where_clause("y = 2".to_string());

let clauses = builder.where_clauses();
assert_eq!(clauses.len(), 2);
```

### Complex Filters

```rust
use crate::query_builder::FilterOp;

// Add complex filter with operations
let builder = EventQueryBuilder::new()
    .add_complex_filter("status", FilterOp::NotEquals, Some("'archived'"))
    .add_complex_filter("score", FilterOp::GreaterThanOrEqual, None);
```

### Combining Features

```rust
// Build a complex query
let (sql, count_sql) = EventQueryBuilder::new()
    .with_filters(EventFilters {
        contract_id: Some("C123".into()),
        event_type: Some("contract".into()),
        ..Default::default()
    })
    .with_where_clause("status = 'completed'".to_string())
    .sort_by("timestamp", SortDirection::Desc)
    .paginate(1, 100)
    .build();

// Execute query
let rows: Vec<Event> = sqlx::query_as(&sql)
    .bind("C123")  // contract_id filter
    .bind("contract")  // event_type filter
    .bind(100)  // limit
    .bind(0)  // offset
    .fetch_all(&pool)
    .await?;
```

## Named Queries

For frequently-used queries, use the `queries` module:

```rust
use crate::query_builder::queries;

// Get events by contract
let rows: Vec<Event> = sqlx::query_as(queries::GET_EVENTS_BY_CONTRACT)
    .bind("CABC...")
    .fetch_all(&pool)
    .await?;

// Get events by transaction hash
let rows = sqlx::query_as(queries::GET_EVENTS_BY_TX_HASH)
    .bind("a1b2c3...")
    .fetch_all(&pool)
    .await?;

// Get approximate count (fast, may be off by a few rows)
let (count,): (i64,) = sqlx::query_as(queries::GET_EVENTS_APPROXIMATE_COUNT)
    .fetch_one(&pool)
    .await?;

// Get exact count (slow, but accurate)
let (count,): (i64,) = sqlx::query_as(queries::GET_EVENTS_EXACT_COUNT)
    .fetch_one(&pool)
    .await?;
```

## Filter Operations

The `FilterOp` enum supports common SQL operations:

```rust
use crate::query_builder::FilterOp;

let ops = [
    FilterOp::Equals,               // =
    FilterOp::NotEquals,            // !=
    FilterOp::GreaterThan,          // >
    FilterOp::LessThan,             // <
    FilterOp::GreaterThanOrEqual,   // >=
    FilterOp::LessThanOrEqual,      // <=
    FilterOp::In,                   // IN (...)
    FilterOp::Like,                 // LIKE '%...'
];

// Use in filters
for op in ops {
    println!("{}", op.as_sql());
}
```

## Validation

The builder includes validation helpers:

```rust
use crate::query_builder::{validate_ledger_range, validate_pagination, validate_event_type};

// Validate ledger range
validate_ledger_range(Some(100), Some(200))?;   // OK
validate_ledger_range(Some(200), Some(100))?;   // Error: inverted

// Validate pagination
validate_pagination(1, 20)?;                     // OK
validate_pagination(0, 20)?;                     // Error: page must be ≥ 1

// Validate event type
validate_event_type("contract")?;                // OK
validate_event_type("unknown")?;                 // Error: invalid type
```

## Constraint Constants

```rust
use crate::query_builder::{MAX_LIMIT, DEFAULT_LIMIT};

const MAX_LIMIT: u64 = 1_000;       // Maximum rows per page
const DEFAULT_LIMIT: u64 = 20;      // Default page size

// Page 1, up to 1000 items
let p = Pagination {
    page: 1,
    limit: 2000,  // Clamped to MAX_LIMIT (1000)
};
assert_eq!(p.limit(), 1_000);
```

## Best Practices

### 1. Use Builders for Dynamic Queries

```rust
// Good: Dynamic query via builder
for filter in filters {
    let builder = EventQueryBuilder::new()
        .with_filters(filter);
    // Process...
}

// Poor: String interpolation
for filter in filters {
    let sql = format!("SELECT * FROM events WHERE contract_id = '{}'", filter);
    // Vulnerable to SQL injection
}
```

### 2. Validate Early

```rust
// Validate input before building
validate_ledger_range(from_ledger, to_ledger)?;
validate_pagination(page, limit)?;

let builder = EventQueryBuilder::new()
    .with_filters(EventFilters {
        from_ledger,
        to_ledger,
        ..Default::default()
    })
    .paginate(page, limit);
```

### 3. Reuse Builders

```rust
// Create base builder once
let base = EventQueryBuilder::new()
    .sort_by("ledger", SortDirection::Desc);

// Reuse for different filters
let builder1 = base.clone()
    .with_filters(EventFilters { contract_id: Some("A".into()), ..Default::default() });
let builder2 = base.clone()
    .with_filters(EventFilters { contract_id: Some("B".into()), ..Default::default() });
```

### 4. Document Custom Clauses

```rust
// When adding custom WHERE clauses, document the intent
let builder = EventQueryBuilder::new()
    .with_where_clause("created_at > NOW() - INTERVAL '1 day'".to_string());
    // ^ Custom clause: events from last 24 hours
```

## API Reference

### EventFilters

```rust
pub struct EventFilters {
    pub contract_id: Option<String>,      // Filter by contract
    pub from_ledger: Option<i64>,         // Lower ledger bound (inclusive)
    pub to_ledger: Option<i64>,           // Upper ledger bound (inclusive)
    pub event_type: Option<String>,       // "contract", "diagnostic", "system"
    pub tenant_id: Option<String>,        // Multi-tenant filter
}
```

### Pagination

```rust
pub struct Pagination {
    pub page: u64,                        // 1-based page number
    pub limit: u64,                       // Rows per page (clamped to MAX_LIMIT)
}

impl Pagination {
    pub fn offset(self) -> u64;           // Compute OFFSET for SQL
    pub fn limit(self) -> u64;            // Get clamped limit
}
```

### SortDirection

```rust
pub enum SortDirection {
    Asc,
    Desc,
}
```

## Performance Considerations

### Query Plan Caching

The query builder works seamlessly with query plan caching:

```rust
// Same builder pattern → Same SQL → Cache hit
for i in 0..1000 {
    let (sql, _) = EventQueryBuilder::new()
        .paginate(i / 50 + 1, 50)
        .build();
    // First query: plan cached
    // Subsequent queries: use cached plan
}
```

### Bind Parameter Ordering

When executing queries, bind parameters in the order they appear:

```rust
let builder = EventQueryBuilder::new()
    .with_filters(EventFilters {
        tenant_id: Some("t1".into()),      // Parameter 1
        contract_id: Some("c1".into()),    // Parameter 2
        event_type: Some("contract".into()), // Parameter 3
        ..Default::default()
    })
    .paginate(1, 20);                       // Parameters 4, 5

let (sql, _) = builder.build();

let rows = sqlx::query_as::<_, Event>(&sql)
    .bind("t1")                             // tenant_id
    .bind("c1")                             // contract_id
    .bind("contract")                       // event_type
    .bind(20)                               // LIMIT
    .bind(0)                                // OFFSET
    .fetch_all(&pool)
    .await?;
```

## Extending the Builder

To add new query builders:

1. Create a new struct implementing `QueryBuilder` trait
2. Add builder methods following the fluent pattern
3. Implement `build()` to return (sql, count_sql) pair
4. Add tests for SQL generation and bind parameters
5. Document in this guide

Example:

```rust
pub struct CustomQueryBuilder {
    // ... fields
}

impl QueryBuilder for CustomQueryBuilder {
    fn build_sql(&self) -> String {
        // Generate SQL
    }

    fn bind_count(&self) -> usize {
        // Count parameters
    }

    fn build_pair(&self) -> (String, String) {
        // Return (data_sql, count_sql)
    }
}

impl CustomQueryBuilder {
    pub fn new() -> Self { /* ... */ }
    #[must_use]
    pub fn with_filter(mut self, f: Filter) -> Self { /* ... */ }
}
```

## Related Documentation

- Query Plan Caching ([query-plan-tuning.md](query-plan-tuning.md))
- API Documentation ([api-usage.md](api-usage.md))
- Database Schema ([schema.md](schema.md))
