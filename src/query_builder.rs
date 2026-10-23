//! # Query builder module — Issue #664
//!
//! Moves raw, scattered SQL strings into a single, type-safe location.
//!
//! ## Design
//! - [`EventQueryBuilder`] builds the most complex query in the codebase:
//!   paginated event listing with optional filters.
//! - Named query constants ([`queries`]) collect short, frequently reused
//!   SQL statements so they have a canonical home and can be updated in one
//!   place.
//! - A thin validation layer ([`validate_ledger_range`],
//!   [`validate_pagination`]) catches bad parameter combinations before they
//!   reach the database.
//!
//! ## Extension Points
//! - Add new filters by pushing a clause to `EventQueryBuilder::where_clauses`
//!   and binding the parameter in `build`.
//! - Add new named queries to the [`queries`] module.
//! - Add new validation helpers as free functions or inherent methods.
//!
//! ## Example
//!
//! ```rust,ignore
//! use crate::query_builder::{EventQueryBuilder, EventFilters};
//!
//! let filters = EventFilters {
//!     contract_id: Some("CABC...".into()),
//!     from_ledger: Some(1_000_000),
//!     to_ledger: None,
//!     event_type: None,
//!     tenant_id: None,
//! };
//!
//! let (sql, count_sql) = EventQueryBuilder::new()
//!     .with_filters(filters)
//!     .paginate(1, 20)
//!     .build();
//!
//! let rows: Vec<Event> = sqlx::query_as(&sql)
//!     .fetch_all(&pool)
//!     .await?;
//! ```

use crate::error::{AppError, ValidationErrorDetail};

// ---------------------------------------------------------------------------
// Unified QueryBuilder trait
// ---------------------------------------------------------------------------

/// Common interface for all query builders.
pub trait QueryBuilder {
    /// Build and return the SQL query string.
    fn build_sql(&self) -> String;

    /// Get the number of bind parameters required.
    fn bind_count(&self) -> usize;

    /// Build both the data query and count query.
    fn build_pair(&self) -> (String, String);
}

/// Filter operation types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    In,
    Like,
}

impl FilterOp {
    pub fn as_sql(&self) -> &str {
        match self {
            FilterOp::Equals => "=",
            FilterOp::NotEquals => "!=",
            FilterOp::GreaterThan => ">",
            FilterOp::LessThan => "<",
            FilterOp::GreaterThanOrEqual => ">=",
            FilterOp::LessThanOrEqual => "<=",
            FilterOp::In => "IN",
            FilterOp::Like => "LIKE",
        }
    }
}

/// Generic filter clause for query building.
#[derive(Debug, Clone)]
pub struct FilterClause {
    pub column: String,
    pub op: FilterOp,
    pub value: Option<String>,
    pub param_index: usize,
}

// ---------------------------------------------------------------------------
// Named queries
// ---------------------------------------------------------------------------

/// A collection of named SQL queries used across the codebase.
///
/// Each constant has a short doc comment explaining its purpose and the
/// parameters it expects (`$1`, `$2`, …).
pub mod queries {
    /// `$1` = contract_id (text)
    pub const GET_EVENTS_BY_CONTRACT: &str =
        "SELECT id, contract_id, event_type, tx_hash, ledger, \
         timestamp, event_data, created_at \
         FROM events \
         WHERE contract_id = $1 \
         ORDER BY ledger DESC, id DESC";

    /// `$1` = tx_hash (text)
    pub const GET_EVENTS_BY_TX_HASH: &str =
        "SELECT id, contract_id, event_type, tx_hash, ledger, \
         timestamp, event_data, created_at \
         FROM events \
         WHERE tx_hash = $1 \
         ORDER BY ledger DESC, id DESC";

    /// Approximate total event count using PostgreSQL statistics.
    /// Fast but may differ from `COUNT(*)` by a few rows.
    pub const GET_EVENTS_APPROXIMATE_COUNT: &str =
        "SELECT reltuples::bigint AS estimate \
         FROM pg_class \
         WHERE relname = 'events'";

    /// Exact total event count.  Use only when `exact_count=true` is
    /// requested since this requires a sequential scan on large tables.
    pub const GET_EVENTS_EXACT_COUNT: &str = "SELECT COUNT(*) FROM events";

    /// Health-check ping — just verifies the connection is alive.
    pub const HEALTH_CHECK: &str = "SELECT 1";

    /// Fetch the most recent ledger that has been indexed.
    pub const GET_LATEST_INDEXED_LEDGER: &str =
        "SELECT MAX(ledger) FROM events";

    /// `$1` = contract_id (text)
    pub const GET_CONTRACT_EVENT_COUNTS: &str =
        "SELECT event_type, event_day, event_count, unique_tx_count, last_event_at \
         FROM contract_event_daily \
         WHERE contract_id = $1 \
         ORDER BY event_day DESC \
         LIMIT 90";

    /// Delete all events for a contract.  `$1` = contract_id (text).
    pub const DELETE_EVENTS_BY_CONTRACT: &str =
        "DELETE FROM events WHERE contract_id = $1";
}

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

/// Optional filter parameters for event queries.
#[derive(Debug, Clone, Default)]
pub struct EventFilters {
    /// Restrict to events emitted by this contract.
    pub contract_id: Option<String>,
    /// Include only events at or after this ledger sequence number.
    pub from_ledger: Option<i64>,
    /// Include only events at or before this ledger sequence number.
    pub to_ledger: Option<i64>,
    /// Filter by event type (`"contract"`, `"diagnostic"`, `"system"`).
    pub event_type: Option<String>,
    /// Restrict to a specific tenant (multi-tenant mode).
    pub tenant_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

/// Pagination parameters.
#[derive(Debug, Clone, Copy)]
pub struct Pagination {
    /// 1-based page number.
    pub page: u64,
    /// Maximum rows per page (capped at [`MAX_LIMIT`]).
    pub limit: u64,
}

/// Hard upper bound on the number of rows a single query may return.
pub const MAX_LIMIT: u64 = 1_000;

/// Default page size when the caller does not specify one.
pub const DEFAULT_LIMIT: u64 = 20;

impl Default for Pagination {
    fn default() -> Self {
        Self {
            page: 1,
            limit: DEFAULT_LIMIT,
        }
    }
}

impl Pagination {
    /// Compute the SQL `OFFSET` value for this page.
    pub fn offset(self) -> u64 {
        (self.page.saturating_sub(1)).saturating_mul(self.limit)
    }

    /// Return the clamped limit value to use in SQL.
    pub fn limit(self) -> u64 {
        self.limit.min(MAX_LIMIT)
    }
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Validate that `from_ledger` ≤ `to_ledger` when both are present.
///
/// Returns `Err(AppError::Validation)` when the range is inverted.
pub fn validate_ledger_range(
    from_ledger: Option<i64>,
    to_ledger: Option<i64>,
) -> Result<(), AppError> {
    match (from_ledger, to_ledger) {
        (Some(from), Some(to)) if from > to => Err(AppError::ValidationWithDetails(
            "from_ledger must be ≤ to_ledger".to_string(),
            vec![ValidationErrorDetail {
                instance_path: "/from_ledger".to_string(),
                schema_path: "properties/from_ledger/maximum".to_string(),
                message: format!("from_ledger ({from}) must not exceed to_ledger ({to})"),
            }],
        )),
        _ => Ok(()),
    }
}

/// Validate that `page` ≥ 1 and `limit` is in the range 1..=`MAX_LIMIT`.
pub fn validate_pagination(page: u64, limit: u64) -> Result<(), AppError> {
    let mut errors = Vec::new();

    if page == 0 {
        errors.push(ValidationErrorDetail {
            instance_path: "/page".to_string(),
            schema_path: "properties/page/minimum".to_string(),
            message: "page must be ≥ 1".to_string(),
        });
    }

    if limit == 0 {
        errors.push(ValidationErrorDetail {
            instance_path: "/limit".to_string(),
            schema_path: "properties/limit/minimum".to_string(),
            message: "limit must be ≥ 1".to_string(),
        });
    }

    if limit > MAX_LIMIT {
        errors.push(ValidationErrorDetail {
            instance_path: "/limit".to_string(),
            schema_path: format!("properties/limit/maximum ({})", MAX_LIMIT),
            message: format!("limit must be ≤ {MAX_LIMIT}"),
        });
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(AppError::ValidationWithDetails(
            "invalid pagination parameters".to_string(),
            errors,
        ))
    }
}

/// Validate an event type string against the accepted vocabulary.
pub fn validate_event_type(event_type: &str) -> Result<(), AppError> {
    const VALID: &[&str] = &["contract", "diagnostic", "system"];
    if VALID
        .iter()
        .any(|v| v.eq_ignore_ascii_case(event_type))
    {
        Ok(())
    } else {
        Err(AppError::ValidationWithDetails(
            format!("unknown event_type '{event_type}'"),
            vec![ValidationErrorDetail {
                instance_path: "/event_type".to_string(),
                schema_path: "properties/event_type/enum".to_string(),
                message: format!(
                    "must be one of: {}",
                    VALID.join(", ")
                ),
            }],
        ))
    }
}

// ---------------------------------------------------------------------------
// EventQueryBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for the paginated `SELECT … FROM events` query.
///
/// Builds two SQL strings:
/// - The main data query (with `LIMIT` / `OFFSET`).
/// - An optional count query (for the `total` field in the response).
///
/// Parameter binding positions are tracked automatically — add a new filter
/// with [`push_where`] and call `self.next_param()` to get the next `$N`.
///
/// ## Example
/// ```rust,ignore
/// let (sql, count_sql, bind_count) = EventQueryBuilder::new()
///     .with_filters(filters)
///     .paginate(page, limit)
///     .build();
/// ```
#[derive(Clone)]
pub struct EventQueryBuilder {
    filters: EventFilters,
    pagination: Pagination,
    sort_column: String,
    sort_direction: SortDirection,
    param_idx: usize,
    where_clauses: Vec<String>,
}

/// Sort direction for the query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl Default for EventQueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryBuilder for EventQueryBuilder {
    fn build_sql(&self) -> String {
        self.clone().build().0
    }

    fn bind_count(&self) -> usize {
        self.where_param_count() + 2
    }

    fn build_pair(&self) -> (String, String) {
        self.clone().build()
    }
}

impl EventQueryBuilder {
    const BASE_COLS: &'static str =
        "id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at";

    /// Create a new builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            filters: EventFilters::default(),
            pagination: Pagination::default(),
            sort_column: "ledger".to_string(),
            sort_direction: SortDirection::Desc,
            param_idx: 0,
            where_clauses: Vec::new(),
        }
    }

    /// Fluent method to add a custom WHERE clause.
    #[must_use]
    pub fn with_where_clause(mut self, clause: String) -> Self {
        self.where_clauses.push(clause);
        self
    }

    /// Get the list of active WHERE clauses.
    pub fn where_clauses(&self) -> &[String] {
        &self.where_clauses
    }

    /// Apply the given filters.
    #[must_use]
    pub fn with_filters(mut self, filters: EventFilters) -> Self {
        self.filters = filters;
        self
    }

    /// Apply pagination (page is 1-based).
    #[must_use]
    pub fn paginate(mut self, page: u64, limit: u64) -> Self {
        self.pagination = Pagination {
            page,
            limit: limit.min(MAX_LIMIT),
        };
        self
    }

    /// Override the sort column (default: `"ledger"`).  Only trusted,
    /// allow-listed column names should be passed here — the value is
    /// interpolated directly into SQL.
    ///
    /// Accepted values: `"ledger"`, `"timestamp"`, `"created_at"`, `"id"`.
    #[must_use]
    pub fn sort_by(mut self, column: impl Into<String>, direction: SortDirection) -> Self {
        let col = column.into();
        // Allow-list to prevent SQL injection from caller-controlled sort columns.
        if matches!(
            col.as_str(),
            "ledger" | "timestamp" | "created_at" | "id"
        ) {
            self.sort_column = col;
            self.sort_direction = direction;
        }
        self
    }

    /// Allocate the next positional parameter index and return `$N`.
    fn next_param(&mut self) -> String {
        self.param_idx += 1;
        format!("${}", self.param_idx)
    }

    /// Build the WHERE clause fragments based on the configured filters.
    ///
    /// Returns `(where_string, bind_count)` where `bind_count` is the number
    /// of parameters that must be bound in the same order when executing.
    fn build_where_clauses(&mut self) -> String {
        let mut clauses: Vec<String> = Vec::new();

        if self.filters.tenant_id.is_some() {
            let p = self.next_param();
            clauses.push(format!("tenant_id = {p}"));
        }
        if self.filters.contract_id.is_some() {
            let p = self.next_param();
            clauses.push(format!("contract_id = {p}"));
        }
        if self.filters.event_type.is_some() {
            let p = self.next_param();
            clauses.push(format!("LOWER(event_type) = LOWER({p})"));
        }
        if self.filters.from_ledger.is_some() {
            let p = self.next_param();
            clauses.push(format!("ledger >= {p}"));
        }
        if self.filters.to_ledger.is_some() {
            let p = self.next_param();
            clauses.push(format!("ledger <= {p}"));
        }

        // Merge any custom clauses pushed by the caller.
        clauses.extend(self.where_clauses.drain(..));

        if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        }
    }

    /// Finalise the builder and return the data SQL string.
    ///
    /// Returns `(data_sql, count_sql)`.
    ///
    /// Bind parameters for `data_sql` (in order):
    /// 1. `tenant_id` (if set)
    /// 2. `contract_id` (if set)
    /// 3. `event_type` (if set)
    /// 4. `from_ledger` (if set)
    /// 5. `to_ledger` (if set)
    /// 6. `limit` (always last before offset)
    /// 7. `offset` (always last)
    ///
    /// The count query shares the same WHERE clause parameters 1–5.
    pub fn build(mut self) -> (String, String) {
        let where_clause = self.build_where_clauses();

        let direction = match self.sort_direction {
            SortDirection::Asc => "ASC",
            SortDirection::Desc => "DESC",
        };
        let order = format!("ORDER BY {} {}, id {}", self.sort_column, direction, direction);

        let limit_param = self.next_param();
        let offset_param = self.next_param();

        let data_sql = format!(
            "SELECT {cols} FROM events {where_clause} {order} LIMIT {limit_param} OFFSET {offset_param}",
            cols = Self::BASE_COLS,
        );

        let count_sql = format!("SELECT COUNT(*) FROM events {where_clause}");

        (data_sql, count_sql)
    }

    /// Returns the number of WHERE-clause bind parameters (excludes LIMIT/OFFSET).
    pub fn where_param_count(&self) -> usize {
        let f = &self.filters;
        [
            f.tenant_id.is_some(),
            f.contract_id.is_some(),
            f.event_type.is_some(),
            f.from_ledger.is_some(),
            f.to_ledger.is_some(),
        ]
        .iter()
        .filter(|&&b| b)
        .count()
    }

    /// Add a complex filter clause with multiple conditions.
    #[must_use]
    pub fn add_complex_filter(
        mut self,
        column: &str,
        op: FilterOp,
        value: Option<&str>,
    ) -> Self {
        let param_str = self.next_param();
        let clause = match (op, value) {
            (FilterOp::In, Some(value)) => {
                format!("{} IN ({})", column, value)
            }
            (op, _) => {
                format!("{} {} {}", column, op.as_sql(), param_str)
            }
        };
        self.where_clauses.push(clause);
        self
    }

    /// Get current pagination settings.
    pub fn get_pagination(&self) -> Pagination {
        self.pagination
    }

    /// Get current sort configuration.
    pub fn get_sort(&self) -> (&str, SortDirection) {
        (&self.sort_column, self.sort_direction)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_query_has_no_where_clause() {
        let (sql, count_sql) = EventQueryBuilder::new().build();
        assert!(!sql.contains("WHERE"), "expected no WHERE, got: {sql}");
        assert!(!count_sql.contains("WHERE"), "count: {count_sql}");
    }

    #[test]
    fn contract_id_filter_adds_where_clause() {
        let filters = EventFilters {
            contract_id: Some("CABC".into()),
            ..Default::default()
        };
        let (sql, _) = EventQueryBuilder::new().with_filters(filters).build();
        assert!(sql.contains("contract_id = $1"), "sql: {sql}");
    }

    #[test]
    fn ledger_range_filter_uses_correct_params() {
        let filters = EventFilters {
            from_ledger: Some(100),
            to_ledger: Some(200),
            ..Default::default()
        };
        let (sql, _) = EventQueryBuilder::new().with_filters(filters).build();
        assert!(sql.contains("ledger >= $1"), "sql: {sql}");
        assert!(sql.contains("ledger <= $2"), "sql: {sql}");
    }

    #[test]
    fn all_filters_assign_sequential_params() {
        let filters = EventFilters {
            tenant_id: Some("t1".into()),
            contract_id: Some("C1".into()),
            event_type: Some("contract".into()),
            from_ledger: Some(10),
            to_ledger: Some(20),
        };
        let builder = EventQueryBuilder::new().with_filters(filters);
        assert_eq!(builder.where_param_count(), 5);
        let (sql, _) = builder.build();
        assert!(sql.contains("$1") && sql.contains("$5"), "sql: {sql}");
    }

    #[test]
    fn pagination_produces_limit_offset_params() {
        let (sql, _) = EventQueryBuilder::new().paginate(2, 10).build();
        // No filters → LIMIT=$1, OFFSET=$2
        assert!(sql.contains("LIMIT $1 OFFSET $2"), "sql: {sql}");
    }

    #[test]
    fn sort_direction_default_is_desc() {
        let (sql, _) = EventQueryBuilder::new().build();
        assert!(sql.contains("DESC"), "sql: {sql}");
    }

    #[test]
    fn sort_by_asc_changes_direction() {
        let (sql, _) = EventQueryBuilder::new()
            .sort_by("timestamp", SortDirection::Asc)
            .build();
        assert!(sql.contains("ASC"), "sql: {sql}");
    }

    #[test]
    fn sort_by_unknown_column_is_ignored() {
        let builder =
            EventQueryBuilder::new().sort_by("injected; DROP TABLE events;--", SortDirection::Asc);
        // Default ledger column preserved.
        assert_eq!(builder.sort_column, "ledger");
    }

    #[test]
    fn pagination_offset_computed_correctly() {
        let p = Pagination { page: 3, limit: 20 };
        assert_eq!(p.offset(), 40);
        assert_eq!(p.limit(), 20);
    }

    #[test]
    fn pagination_clamps_limit_to_max() {
        let p = Pagination {
            page: 1,
            limit: MAX_LIMIT + 100,
        };
        assert_eq!(p.limit(), MAX_LIMIT);
    }

    #[test]
    fn validate_ledger_range_accepts_valid_range() {
        assert!(validate_ledger_range(Some(100), Some(200)).is_ok());
        assert!(validate_ledger_range(None, Some(200)).is_ok());
        assert!(validate_ledger_range(Some(100), None).is_ok());
        assert!(validate_ledger_range(None, None).is_ok());
    }

    #[test]
    fn validate_ledger_range_rejects_inverted() {
        assert!(validate_ledger_range(Some(200), Some(100)).is_err());
    }

    #[test]
    fn validate_pagination_rejects_zero_page() {
        assert!(validate_pagination(0, 20).is_err());
    }

    #[test]
    fn validate_pagination_rejects_zero_limit() {
        assert!(validate_pagination(1, 0).is_err());
    }

    #[test]
    fn validate_pagination_rejects_over_max_limit() {
        assert!(validate_pagination(1, MAX_LIMIT + 1).is_err());
    }

    #[test]
    fn validate_pagination_accepts_valid_params() {
        assert!(validate_pagination(1, 20).is_ok());
        assert!(validate_pagination(1, MAX_LIMIT).is_ok());
    }

    #[test]
    fn validate_event_type_accepts_valid_types() {
        for t in &["contract", "Contract", "CONTRACT", "diagnostic", "system"] {
            assert!(validate_event_type(t).is_ok(), "should accept '{t}'");
        }
    }

    #[test]
    fn validate_event_type_rejects_unknown() {
        assert!(validate_event_type("unknown_type").is_err());
    }

    #[test]
    fn count_query_shares_where_clause() {
        let filters = EventFilters {
            contract_id: Some("CABC".into()),
            ..Default::default()
        };
        let (_, count_sql) = EventQueryBuilder::new().with_filters(filters).build();
        assert!(
            count_sql.contains("WHERE contract_id = $1"),
            "count: {count_sql}"
        );
    }

    #[test]
    fn named_query_constants_are_non_empty() {
        assert!(!queries::GET_EVENTS_BY_CONTRACT.is_empty());
        assert!(!queries::GET_EVENTS_BY_TX_HASH.is_empty());
        assert!(!queries::GET_EVENTS_APPROXIMATE_COUNT.is_empty());
        assert!(!queries::GET_EVENTS_EXACT_COUNT.is_empty());
        assert!(!queries::HEALTH_CHECK.is_empty());
    }

    #[test]
    fn query_builder_trait_implementation() {
        let builder = EventQueryBuilder::new()
            .with_filters(EventFilters {
                contract_id: Some("C1".into()),
                ..Default::default()
            });
        assert_eq!(builder.bind_count(), 3); // 1 filter + LIMIT + OFFSET
        let sql = builder.clone().build_sql();
        assert!(!sql.is_empty());
    }

    #[test]
    fn filter_op_as_sql() {
        assert_eq!(FilterOp::Equals.as_sql(), "=");
        assert_eq!(FilterOp::GreaterThan.as_sql(), ">");
        assert_eq!(FilterOp::LessThan.as_sql(), "<");
        assert_eq!(FilterOp::Like.as_sql(), "LIKE");
    }

    #[test]
    fn query_builder_with_custom_where_clause() {
        let (sql, _) = EventQueryBuilder::new()
            .with_where_clause("custom_column = TRUE".to_string())
            .build();
        assert!(sql.contains("custom_column = TRUE"), "sql: {sql}");
    }

    #[test]
    fn query_builder_get_pagination() {
        let builder = EventQueryBuilder::new().paginate(2, 50);
        let p = builder.get_pagination();
        assert_eq!(p.page, 2);
        assert_eq!(p.limit, 50);
    }

    #[test]
    fn query_builder_get_sort() {
        let builder = EventQueryBuilder::new().sort_by("timestamp", SortDirection::Asc);
        let (col, dir) = builder.get_sort();
        assert_eq!(col, "timestamp");
        assert_eq!(dir, SortDirection::Asc);
    }

    #[test]
    fn query_builder_add_complex_filter() {
        let builder = EventQueryBuilder::new().add_complex_filter("status", FilterOp::Equals, None);
        assert!(!builder.where_clauses().is_empty());
    }

    #[test]
    fn query_builder_where_clauses_accessor() {
        let builder = EventQueryBuilder::new()
            .with_where_clause("clause1".to_string())
            .with_where_clause("clause2".to_string());
        let clauses = builder.where_clauses();
        assert_eq!(clauses.len(), 2);
        assert_eq!(clauses[0], "clause1");
        assert_eq!(clauses[1], "clause2");
    }

    #[test]
    fn clone_builder_preserves_state() {
        let builder1 = EventQueryBuilder::new()
            .with_filters(EventFilters {
                contract_id: Some("C1".into()),
                ..Default::default()
            })
            .paginate(3, 15);

        let builder2 = builder1.clone();
        assert_eq!(builder1.where_param_count(), builder2.where_param_count());
        assert_eq!(builder1.get_pagination().page, builder2.get_pagination().page);
    }
}
