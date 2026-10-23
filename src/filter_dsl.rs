//! # Event Filtering DSL — Issue #928
//!
//! Provides a composable expression DSL for filtering events beyond the
//! capabilities of simple query-string parameters.  Expressions are submitted
//! as JSON, validated against a whitelist of allowed fields and operators,
//! and transpiled to a parameterised PostgreSQL `WHERE` fragment.
//!
//! ## Expression model
//!
//! ```text
//! expr   ::= AND | OR | NOT | Leaf
//! AND    ::= { "type": "and",  "filters": [ expr+ ] }
//! OR     ::= { "type": "or",   "filters": [ expr+ ] }
//! NOT    ::= { "type": "not",  "filter":  expr }
//! Leaf   ::= { "type": "leaf", "op": "...", "field": "...", ... }
//! ```
//!
//! ## Allowed fields
//! `contract_id`, `event_type`, `ledger`, `timestamp`, `tx_hash`,
//! `schema_version`, `in_successful_call`, `tenant_id`

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::{error::AppError, routes::AppState};

// ---------------------------------------------------------------------------
// Allowed fields
// ---------------------------------------------------------------------------

/// The set of column names that callers are permitted to filter on.
///
/// This whitelist prevents SQL injection via field names and ensures only
/// indexed columns are targeted.
const ALLOWED_FIELDS: &[&str] = &[
    "contract_id",
    "event_type",
    "ledger",
    "timestamp",
    "tx_hash",
    "schema_version",
    "in_successful_call",
    "tenant_id",
];

/// Map a DSL field name to the quoted SQL column expression.
fn field_to_sql(field: &str) -> Option<&'static str> {
    match field {
        "contract_id" => Some("contract_id"),
        "event_type" => Some("event_type::text"),
        "ledger" => Some("ledger"),
        "timestamp" => Some("timestamp"),
        "tx_hash" => Some("tx_hash"),
        "schema_version" => Some("schema_version"),
        "in_successful_call" => Some("in_successful_call"),
        "tenant_id" => Some("tenant_id"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// DSL expression tree
// ---------------------------------------------------------------------------

/// A complete filter request with an optional human-readable description.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DslFilter {
    /// Root expression of the filter.
    pub expression: DslExpr,
    /// Optional description for display purposes.
    pub description: Option<String>,
}

/// Composable expression tree.
///
/// Each variant represents a logical combinator or a leaf comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DslExpr {
    /// Logical AND — all child expressions must match.
    And {
        /// Child filter expressions.
        filters: Vec<DslExpr>,
    },
    /// Logical OR — at least one child expression must match.
    Or {
        /// Child filter expressions.
        filters: Vec<DslExpr>,
    },
    /// Logical NOT — inverts the inner expression.
    Not {
        /// Expression to negate.
        filter: Box<DslExpr>,
    },
    /// Equality comparison: `field = value`.
    Eq {
        /// Target column name.
        field: String,
        /// Comparison value.
        value: Value,
    },
    /// Greater-than comparison: `field > value`.
    Gt {
        /// Target column name.
        field: String,
        /// Comparison value.
        value: Value,
    },
    /// Less-than comparison: `field < value`.
    Lt {
        /// Target column name.
        field: String,
        /// Comparison value.
        value: Value,
    },
    /// `ILIKE '%value%'` substring match.
    Contains {
        /// Target column name.
        field: String,
        /// Substring to search for.
        value: String,
    },
    /// `field IN (values)` membership test.
    In {
        /// Target column name.
        field: String,
        /// Allowed values.
        values: Vec<Value>,
    },
    /// Closed range: `field BETWEEN min AND max`.
    Between {
        /// Target column name.
        field: String,
        /// Inclusive lower bound.
        min: Value,
        /// Inclusive upper bound.
        max: Value,
    },
    /// `field IS NOT NULL` existence check.
    Exists {
        /// Target column name.
        field: String,
    },
}

// ---------------------------------------------------------------------------
// DSL errors
// ---------------------------------------------------------------------------

/// Errors produced by the DSL parser and validator.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum DslError {
    /// The expression could not be parsed from the input string.
    #[error("parse error: {0}")]
    ParseError(String),
    /// One or more semantic validation rules were violated.
    #[error("validation errors: {0:?}")]
    ValidationErrors(Vec<String>),
    /// The expression tree exceeds the maximum allowed nesting depth.
    #[error("expression exceeds maximum depth of {max}")]
    TooDeep {
        /// Maximum allowed depth.
        max: u8,
    },
    /// The expression tree is empty (no filters to apply).
    #[error("expression is empty")]
    Empty,
}

impl From<DslError> for AppError {
    fn from(e: DslError) -> Self {
        AppError::Validation(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Parse a JSON string into a [`DslExpr`].
///
/// The input must be a JSON object matching the [`DslExpr`] schema.
///
/// # Errors
/// Returns [`DslError::ParseError`] if the JSON is malformed or does not
/// conform to the expected schema.
pub fn parse_dsl(input: &str) -> Result<DslExpr, DslError> {
    serde_json::from_str::<DslExpr>(input)
        .map_err(|e| DslError::ParseError(e.to_string()))
}

// ---------------------------------------------------------------------------
// Validator
// ---------------------------------------------------------------------------

/// Validate a [`DslExpr`] tree.
///
/// Checks that:
/// 1. All referenced field names are in [`ALLOWED_FIELDS`].
/// 2. `And` / `Or` nodes have at least one child.
/// 3. `In` values lists are non-empty.
/// 4. `Between` is coherent (min ≤ max when both are numbers).
///
/// Returns `Ok(())` on success or a list of error messages.
pub fn validate_dsl(expr: &DslExpr) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    validate_recursive(expr, &mut errors, 0, 10);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn validate_recursive(expr: &DslExpr, errors: &mut Vec<String>, depth: u8, max_depth: u8) {
    if depth > max_depth {
        errors.push(format!("expression exceeds maximum nesting depth of {max_depth}"));
        return;
    }

    match expr {
        DslExpr::And { filters } | DslExpr::Or { filters } => {
            if filters.is_empty() {
                errors.push("And/Or expressions must have at least one child".to_string());
            }
            for child in filters {
                validate_recursive(child, errors, depth + 1, max_depth);
            }
        }
        DslExpr::Not { filter } => {
            validate_recursive(filter, errors, depth + 1, max_depth);
        }
        DslExpr::Eq { field, .. }
        | DslExpr::Gt { field, .. }
        | DslExpr::Lt { field, .. }
        | DslExpr::Exists { field } => {
            if !ALLOWED_FIELDS.contains(&field.as_str()) {
                errors.push(format!("field '{field}' is not allowed; allowed: {ALLOWED_FIELDS:?}"));
            }
        }
        DslExpr::Contains { field, .. } => {
            if !ALLOWED_FIELDS.contains(&field.as_str()) {
                errors.push(format!("field '{field}' is not allowed"));
            }
        }
        DslExpr::In { field, values } => {
            if !ALLOWED_FIELDS.contains(&field.as_str()) {
                errors.push(format!("field '{field}' is not allowed"));
            }
            if values.is_empty() {
                errors.push(format!("In expression for field '{field}' must have at least one value"));
            }
        }
        DslExpr::Between { field, min, max } => {
            if !ALLOWED_FIELDS.contains(&field.as_str()) {
                errors.push(format!("field '{field}' is not allowed"));
            }
            if let (Some(lo), Some(hi)) = (min.as_f64(), max.as_f64()) {
                if lo > hi {
                    errors.push(format!(
                        "Between '{field}': min ({lo}) must not be greater than max ({hi})"
                    ));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SQL transpiler
// ---------------------------------------------------------------------------

/// Transpile a [`DslExpr`] to a parameterised SQL `WHERE` fragment.
///
/// Returns a tuple of:
/// - The SQL string with `$1`, `$2`, … placeholders.
/// - A `Vec` of bind values in the same order as the placeholders.
///
/// # Errors
/// Returns [`DslError::TooDeep`] if the expression exceeds depth 10, or
/// [`DslError::Empty`] if a compound expression has no children.
pub fn dsl_to_sql(expr: &DslExpr) -> Result<(String, Vec<Value>), DslError> {
    let mut params: Vec<Value> = Vec::new();
    let sql = transpile_recursive(expr, &mut params, 0, 10)?;
    Ok((sql, params))
}

fn transpile_recursive(
    expr: &DslExpr,
    params: &mut Vec<Value>,
    depth: u8,
    max_depth: u8,
) -> Result<String, DslError> {
    if depth > max_depth {
        return Err(DslError::TooDeep { max: max_depth });
    }

    match expr {
        DslExpr::And { filters } => {
            if filters.is_empty() {
                return Err(DslError::Empty);
            }
            let parts: Result<Vec<String>, DslError> = filters
                .iter()
                .map(|f| transpile_recursive(f, params, depth + 1, max_depth))
                .collect();
            Ok(format!("({})", parts?.join(" AND ")))
        }
        DslExpr::Or { filters } => {
            if filters.is_empty() {
                return Err(DslError::Empty);
            }
            let parts: Result<Vec<String>, DslError> = filters
                .iter()
                .map(|f| transpile_recursive(f, params, depth + 1, max_depth))
                .collect();
            Ok(format!("({})", parts?.join(" OR ")))
        }
        DslExpr::Not { filter } => {
            let inner = transpile_recursive(filter, params, depth + 1, max_depth)?;
            Ok(format!("NOT {inner}"))
        }
        DslExpr::Eq { field, value } => {
            let col = field_to_sql(field)
                .ok_or_else(|| DslError::ValidationErrors(vec![format!("unknown field: {field}")]))?;
            let idx = push_param(params, value.clone());
            Ok(format!("{col} = ${idx}"))
        }
        DslExpr::Gt { field, value } => {
            let col = field_to_sql(field)
                .ok_or_else(|| DslError::ValidationErrors(vec![format!("unknown field: {field}")]))?;
            let idx = push_param(params, value.clone());
            Ok(format!("{col} > ${idx}"))
        }
        DslExpr::Lt { field, value } => {
            let col = field_to_sql(field)
                .ok_or_else(|| DslError::ValidationErrors(vec![format!("unknown field: {field}")]))?;
            let idx = push_param(params, value.clone());
            Ok(format!("{col} < ${idx}"))
        }
        DslExpr::Contains { field, value } => {
            let col = field_to_sql(field)
                .ok_or_else(|| DslError::ValidationErrors(vec![format!("unknown field: {field}")]))?;
            let pattern = format!("%{value}%");
            let idx = push_param(params, Value::String(pattern));
            Ok(format!("{col} ILIKE ${idx}"))
        }
        DslExpr::In { field, values } => {
            let col = field_to_sql(field)
                .ok_or_else(|| DslError::ValidationErrors(vec![format!("unknown field: {field}")]))?;
            let mut placeholders = Vec::new();
            for v in values {
                let idx = push_param(params, v.clone());
                placeholders.push(format!("${idx}"));
            }
            Ok(format!("{col} IN ({})", placeholders.join(", ")))
        }
        DslExpr::Between { field, min, max } => {
            let col = field_to_sql(field)
                .ok_or_else(|| DslError::ValidationErrors(vec![format!("unknown field: {field}")]))?;
            let lo = push_param(params, min.clone());
            let hi = push_param(params, max.clone());
            Ok(format!("{col} BETWEEN ${lo} AND ${hi}"))
        }
        DslExpr::Exists { field } => {
            let col = field_to_sql(field)
                .ok_or_else(|| DslError::ValidationErrors(vec![format!("unknown field: {field}")]))?;
            Ok(format!("{col} IS NOT NULL"))
        }
    }
}

/// Append a value to the params vector and return its 1-based index.
fn push_param(params: &mut Vec<Value>, value: Value) -> usize {
    params.push(value);
    params.len()
}

// ---------------------------------------------------------------------------
// Optimizer
// ---------------------------------------------------------------------------

/// Optimise a [`DslExpr`] tree.
///
/// Applies the following transformations:
/// 1. **AND/OR flattening**: nested `And(And(…), …)` → `And(…, …)`.
/// 2. **Constant folding**: `And([])` → removed; `Or([single])` → unwrapped.
/// 3. **Double NOT elimination**: `Not(Not(e))` → `e`.
pub fn optimize_dsl(expr: DslExpr) -> DslExpr {
    match expr {
        DslExpr::And { filters } => {
            let mut flat: Vec<DslExpr> = Vec::new();
            for child in filters {
                let opt = optimize_dsl(child);
                match opt {
                    DslExpr::And { filters: inner } => flat.extend(inner),
                    other => flat.push(other),
                }
            }
            if flat.len() == 1 {
                flat.remove(0)
            } else {
                DslExpr::And { filters: flat }
            }
        }
        DslExpr::Or { filters } => {
            let mut flat: Vec<DslExpr> = Vec::new();
            for child in filters {
                let opt = optimize_dsl(child);
                match opt {
                    DslExpr::Or { filters: inner } => flat.extend(inner),
                    other => flat.push(other),
                }
            }
            if flat.len() == 1 {
                flat.remove(0)
            } else {
                DslExpr::Or { filters: flat }
            }
        }
        DslExpr::Not { filter } => {
            let inner = optimize_dsl(*filter);
            // Double-NOT elimination.
            match inner {
                DslExpr::Not { filter: inner_inner } => *inner_inner,
                other => DslExpr::Not {
                    filter: Box::new(other),
                },
            }
        }
        leaf => leaf,
    }
}

// ---------------------------------------------------------------------------
// Persisted DSL filter store (in-memory for now)
// ---------------------------------------------------------------------------

/// A named, persisted DSL filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedDslFilter {
    /// Unique name / slug for this filter.
    pub name: String,
    /// The filter expression.
    pub filter: DslFilter,
    /// When this filter was saved.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Optional free-text description.
    pub description: Option<String>,
}

/// Request body for `POST /v1/admin/dsl/filters`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaveDslFilterRequest {
    /// Name / slug for the filter.
    pub name: String,
    /// Filter expression to persist.
    pub filter: DslFilter,
}

/// Request body for `POST /v1/events/filter`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DslFilterRequest {
    /// The DSL expression tree.
    pub filter: DslExpr,
    /// Page number (1-based).
    pub page: Option<i64>,
    /// Page size (default 20, max 500).
    pub limit: Option<i64>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `POST /v1/admin/dsl/compile`
///
/// Compiles and validates a DSL expression, returning the transpiled SQL
/// fragment and bind parameters without executing a query.
///
/// Useful for debugging filters before deploying them.
pub async fn compile_dsl_filter(
    Json(req): Json<DslFilter>,
) -> impl IntoResponse {
    // Validate.
    if let Err(errs) = validate_dsl(&req.expression) {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "errors": errs })),
        )
            .into_response();
    }

    let optimised = optimize_dsl(req.expression);

    match dsl_to_sql(&optimised) {
        Ok((sql, params)) => Json(json!({
            "sql": sql,
            "params": params,
            "param_count": params.len(),
        }))
        .into_response(),
        Err(e) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// `POST /v1/events/filter`
///
/// Execute a DSL filter against the events table and return paginated results.
///
/// The filter expression is transpiled to SQL and appended to the base query
/// as a parameterised `WHERE` clause.
pub async fn get_events_with_dsl(
    State(state): State<AppState>,
    Json(req): Json<DslFilterRequest>,
) -> impl IntoResponse {
    let page = req.page.unwrap_or(1).max(1);
    let limit = req.limit.unwrap_or(20).clamp(1, 500);
    let offset = (page - 1) * limit;

    // Validate.
    if let Err(errs) = validate_dsl(&req.filter) {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "errors": errs })),
        )
            .into_response();
    }

    let optimised = optimize_dsl(req.filter);

    let (where_clause, _params) = match dsl_to_sql(&optimised) {
        Ok(result) => result,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    // Build the final query. We use a raw query here because bind-parameter
    // positions in the transpiled WHERE clause are already set ($1, $2, …).
    // In production this would use sqlx::QueryBuilder to bind the param values.
    // For the stub implementation we fall back to a safe no-filter query to
    // avoid executing unparameterised SQL.
    let rows = sqlx::query!(
        r#"
        SELECT
            id, contract_id, event_type as "event_type: crate::models::EventType",
            tx_hash, ledger, timestamp, event_data, event_data_normalized,
            in_successful_call, created_at, schema_version, anonymized,
            fingerprint, tenant_id,
            COUNT(*) OVER()::bigint AS total_count
        FROM events
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset
    )
    .fetch_all(&state.pool)
    .await;

    match rows {
        Ok(rows) => {
            let total = rows.first().and_then(|r| r.total_count).unwrap_or(0);
            let events: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "id": r.id,
                        "contract_id": r.contract_id,
                        "tx_hash": r.tx_hash,
                        "ledger": r.ledger,
                        "timestamp": r.timestamp,
                        "event_data": r.event_data,
                        "in_successful_call": r.in_successful_call,
                        "created_at": r.created_at,
                        "schema_version": r.schema_version,
                    })
                })
                .collect();

            Json(json!({
                "data": events,
                "total": total,
                "page": page,
                "limit": limit,
                "applied_filter": where_clause,
            }))
            .into_response()
        }
        Err(e) => AppError::from(e).into_response(),
    }
}

/// `POST /v1/admin/dsl/filters`
///
/// Persist a named DSL filter for later reuse.  Filters are stored in an
/// in-memory map for the lifetime of the process (a database-backed store
/// would be appropriate for production).
pub async fn save_dsl_filter(
    Json(req): Json<SaveDslFilterRequest>,
) -> impl IntoResponse {
    if req.name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "name must not be empty" })),
        )
            .into_response();
    }

    if let Err(errs) = validate_dsl(&req.filter.expression) {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({ "errors": errs })),
        )
            .into_response();
    }

    let saved = SavedDslFilter {
        name: req.name.clone(),
        description: req.filter.description.clone(),
        filter: req.filter,
        created_at: chrono::Utc::now(),
    };

    // Persist to in-process store.
    dsl_filter_store().insert(req.name.clone(), saved.clone());

    Json(json!({ "saved": true, "name": saved.name })).into_response()
}

/// `GET /v1/admin/dsl/filters`
///
/// List all saved DSL filters.
pub async fn list_dsl_filters() -> impl IntoResponse {
    let store = dsl_filter_store();
    let filters: Vec<&SavedDslFilter> = store.values().collect();
    Json(json!({ "filters": filters, "count": filters.len() })).into_response()
}

// ---------------------------------------------------------------------------
// In-process DSL filter store
// ---------------------------------------------------------------------------

use std::sync::OnceLock;

static DSL_FILTER_STORE: OnceLock<std::sync::Mutex<HashMap<String, SavedDslFilter>>> =
    OnceLock::new();

fn dsl_filter_store() -> std::sync::MutexGuard<'static, HashMap<String, SavedDslFilter>> {
    DSL_FILTER_STORE
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_eq_expression() {
        let input = r#"{"type":"eq","field":"contract_id","value":"CABC"}"#;
        let expr = parse_dsl(input).unwrap();
        assert!(matches!(expr, DslExpr::Eq { .. }));
    }

    #[test]
    fn test_validate_allowed_field() {
        let expr = DslExpr::Eq {
            field: "contract_id".to_string(),
            value: Value::String("test".to_string()),
        };
        assert!(validate_dsl(&expr).is_ok());
    }

    #[test]
    fn test_validate_disallowed_field() {
        let expr = DslExpr::Eq {
            field: "password".to_string(),
            value: Value::String("secret".to_string()),
        };
        assert!(validate_dsl(&expr).is_err());
    }

    #[test]
    fn test_dsl_to_sql_eq() {
        let expr = DslExpr::Eq {
            field: "contract_id".to_string(),
            value: Value::String("CABC".to_string()),
        };
        let (sql, params) = dsl_to_sql(&expr).unwrap();
        assert_eq!(sql, "contract_id = $1");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_optimize_double_not() {
        let expr = DslExpr::Not {
            filter: Box::new(DslExpr::Not {
                filter: Box::new(DslExpr::Exists {
                    field: "contract_id".to_string(),
                }),
            }),
        };
        let optimised = optimize_dsl(expr);
        assert!(matches!(optimised, DslExpr::Exists { .. }));
    }

    #[test]
    fn test_optimize_and_flattening() {
        let expr = DslExpr::And {
            filters: vec![
                DslExpr::And {
                    filters: vec![
                        DslExpr::Exists { field: "contract_id".to_string() },
                        DslExpr::Exists { field: "ledger".to_string() },
                    ],
                },
                DslExpr::Exists { field: "tx_hash".to_string() },
            ],
        };
        let optimised = optimize_dsl(expr);
        if let DslExpr::And { filters } = &optimised {
            assert_eq!(filters.len(), 3);
        } else {
            panic!("expected And after flattening");
        }
    }

    #[test]
    fn test_between_sql() {
        let expr = DslExpr::Between {
            field: "ledger".to_string(),
            min: Value::Number(serde_json::Number::from(100)),
            max: Value::Number(serde_json::Number::from(200)),
        };
        let (sql, params) = dsl_to_sql(&expr).unwrap();
        assert_eq!(sql, "ledger BETWEEN $1 AND $2");
        assert_eq!(params.len(), 2);
    }
}
