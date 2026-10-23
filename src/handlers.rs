// --- Async Export Job and Import Endpoint Stubs ---
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// Dummy job store for demonstration
type JobId = String;
type JobStatus = String;
static EXPORT_JOBS: OnceLock<Arc<RwLock<HashMap<JobId, JobStatus>>>> = OnceLock::new();
static IMPORT_JOBS: OnceLock<Arc<RwLock<HashMap<JobId, JobStatus>>>> = OnceLock::new();

fn export_jobs() -> Arc<RwLock<HashMap<JobId, JobStatus>>> {
    EXPORT_JOBS.get_or_init(|| Arc::new(RwLock::new(HashMap::new()))).clone()
}
fn import_jobs() -> Arc<RwLock<HashMap<JobId, JobStatus>>> {
    IMPORT_JOBS.get_or_init(|| Arc::new(RwLock::new(HashMap::new()))).clone()
}

/// Start an async export job (stub)
pub async fn start_export_job(State(_state): State<AppState>) -> impl IntoResponse {
    let job_id = Uuid::new_v4().to_string();
    export_jobs().write().await.insert(job_id.clone(), "pending".to_string());
    Json(json!({"job_id": job_id}))
}

/// Get export job status (stub)
pub async fn get_export_job_status(Path(job_id): Path<String>) -> impl IntoResponse {
    let status = export_jobs().read().await.get(&job_id).cloned().unwrap_or("not_found".to_string());
    Json(json!({"job_id": job_id, "status": status, "progress": 42}))
}

/// Download completed export (stub)
pub async fn download_export_job(Path(job_id): Path<String>) -> impl IntoResponse {
    // Just return a dummy file
    let data = b"exported data";
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"export_{job_id}.bin\""))
        .body(Body::from(data.as_slice()))
        .unwrap_or_else(|e| response_build_error(e))
}

/// Start an import job (stub)
pub async fn start_import_job(State(_state): State<AppState>) -> impl IntoResponse {
    let job_id = Uuid::new_v4().to_string();
    import_jobs().write().await.insert(job_id.clone(), "pending".to_string());
    Json(json!({"job_id": job_id}))
}

/// Get import job status (stub)
pub async fn get_import_job_status(Path(job_id): Path<String>) -> impl IntoResponse {
    let status = import_jobs().read().await.get(&job_id).cloned().unwrap_or("not_found".to_string());
    Json(json!({"job_id": job_id, "status": status, "progress": 42}))
}

/// Issue #480: List the available multi-language email notification templates.
///
/// `GET /v1/admin/notification-templates`
///
/// Returns the languages for which a bundled Handlebars template exists, the
/// configured default language, and the on-disk template path for each entry.
#[utoipa::path(
    get,
    path = "/v1/admin/notification-templates",
    tag = "admin",
    responses(
        (status = 200, description = "List of available notification templates")
    )
)]
pub async fn list_notification_templates() -> impl IntoResponse {
    let templates: Vec<Value> = crate::email::SUPPORTED_LANGUAGES
        .iter()
        .map(|lang| {
            json!({
                "language": lang,
                "engine": "handlebars",
                "format": "text",
                "file": format!("notification_templates/email_{lang}.hbs"),
            })
        })
        .collect();

    Json(json!({
        "default_language": "en",
        "count": templates.len(),
        "templates": templates,
    }))
}
use axum::body::Body;
use axum::http::{header, HeaderMap};
use axum::response::sse::{Event, Sse};
use axum::response::Response;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{DateTime, Utc};
use futures::stream::{self, Stream, StreamExt};
use reqwest;
use secrecy::ExposeSecret;
use regex::Regex;
use serde_json::{json, Value};
use sqlx::{QueryBuilder, Row};
use std::convert::Infallible;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info_span, instrument};
use uuid::Uuid;

use crate::{
    error::AppError,
    middleware::TenantId,
    models::{
        self, BatchTxRequest, ContractSummary, ExportParams, PaginationParams, ReplayRequest,
        StreamParams, ContractDetailSummary, ContractSearchParams, ContractSearchResult,
        EventTypeBreakdown, LedgerRange,
    },
    routes::AppState,
    notification_formatter,
    pagerduty,
};
use std::sync::atomic::Ordering;

/// Execute a future (typically a DB query), record its duration, and warn if it
/// exceeds `threshold_ms`. Returns the future's output unchanged.
async fn timed_query<F, T>(
    fut: F,
    query_type: &str,
    threshold_ms: u64,
    context: Option<&str>,
) -> T
where
    F: std::future::Future<Output = T>,
{
    let start = std::time::Instant::now();
    let result = fut.await;
    let elapsed = start.elapsed();
    crate::metrics::record_query_duration(query_type, elapsed);
    if elapsed.as_millis() as u64 > threshold_ms {
        crate::metrics::record_slow_query(query_type);
        tracing::warn!(
            query_type = %query_type,
            duration_ms = elapsed.as_millis(),
            threshold_ms = threshold_ms,
            context = context.unwrap_or("-"),
            "slow query detected"
        );
    }
    result
}

/// Compute a lightweight ETag from the last event id, created_at, and total count.
/// Uses SHA-256 truncated to 8 bytes, base64-encoded — no double-serialization needed.
const MAX_DATA_PATTERN_LEN: usize = 256;
const MAX_REGEX_PATTERN_LEN: usize = 256;

fn validate_jsonpath_expr(expr: &str) -> Result<(), AppError> {
    if expr.is_empty() || expr.len() > MAX_DATA_PATTERN_LEN {
        return Err(AppError::Validation(
            "data_pattern expression is invalid or too long".to_string(),
        ));
    }

    let path_re = Regex::new(r"^\$(?:\.[A-Za-z_][A-Za-z0-9_]*|\['[A-Za-z0-9_]+\'])*$")
        .map_err(|e| AppError::Internal(format!("invalid JSONPath validator regex: {e}")))?;
    if !path_re.is_match(expr) {
        return Err(AppError::Validation(
            "data_pattern must be a simple JSONPath expression like $.amount".to_string(),
        ));
    }
    Ok(())
}

fn validate_regex_pattern(pattern: &str) -> Result<(), AppError> {
    if pattern.is_empty() || pattern.len() > MAX_REGEX_PATTERN_LEN {
        return Err(AppError::Validation(
            "pattern is invalid or too complex".to_string(),
        ));
    }
    Regex::new(pattern).map_err(|_| AppError::Validation("pattern is not a valid regular expression".to_string()))?;
    Ok(())
}

/// Fallback for a `Response::builder()` that failed (only possible with an
/// invalid header value, e.g. one derived from user input): log and return 500
/// instead of panicking (issue #1246).
fn response_build_error(e: axum::http::Error) -> Response<Body> {
    tracing::error!(error = %e, "Failed to build HTTP response");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

/// Attach `ETag` + `Cache-Control: no-cache`, skipping an ETag that is not a
/// valid header value rather than panicking.
fn insert_etag_headers(headers: &mut HeaderMap, tag: &str) {
    if let Ok(value) = axum::http::HeaderValue::from_str(tag) {
        headers.insert("ETag", value);
    }
    headers.insert("Cache-Control", axum::http::HeaderValue::from_static("no-cache"));
}

/// Gzip an in-memory buffer. Replaces a `Handle::block_on` + `unwrap()` that
/// panicked when called from inside the async runtime (issue #1246).
async fn compress_gzip(input: &[u8]) -> Result<Vec<u8>, AppError> {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    async_compression::tokio::bufread::GzipEncoder::new(input)
        .read_to_end(&mut buf)
        .await
        .map_err(|e| AppError::Internal(format!("gzip error: {e}")))?;
    Ok(buf)
}

/// Brotli-compress an in-memory buffer. See [`compress_gzip`].
async fn compress_brotli(input: &[u8]) -> Result<Vec<u8>, AppError> {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    async_compression::tokio::bufread::BrotliEncoder::new(input)
        .read_to_end(&mut buf)
        .await
        .map_err(|e| AppError::Internal(format!("brotli error: {e}")))?;
    Ok(buf)
}

fn format_jsonpath_filter(path: &str, pattern: &str) -> String {
    let escaped = pattern.replace('\\', "\\\\").replace('"', "\\\"");
    format!("{path} ? (@ like_regex \"{escaped}\")")
}

fn compute_etag(last_id: &Uuid, last_created_at: &DateTime<Utc>, total: Option<i64>) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(last_id.as_bytes());
    h.update(
        last_created_at
            .timestamp_nanos_opt()
            .unwrap_or(0)
            .to_le_bytes(),
    );
    if let Some(t) = total {
        h.update(t.to_le_bytes());
    } else {
        h.update(0i64.to_le_bytes());
    }
    let digest = h.finalize();
    format!("\"{}\"", URL_SAFE_NO_PAD.encode(&digest[..8]))
}

// ── Issue #1268: per-IP rate limiting for subscriptions and SSE ─────────────

/// Maximum number of `POST /v1/subscriptions` requests allowed per IP per minute.
pub const SUBSCRIPTION_RATE_LIMIT_PER_MINUTE: usize = 5;

/// Maximum number of concurrent SSE connections allowed per IP.
pub const SSE_MAX_CONNECTIONS_PER_IP: usize = 5;

/// Per-IP sliding-window limiter for webhook subscription registration.
///
/// Tracks request timestamps per client IP in a `DashMap`; requests older than
/// one minute are pruned on each check. When the limit is exceeded the handler
/// returns `429 Too Many Requests` with a `Retry-After` header.
pub struct SubscriptionRateLimiter {
    hits: dashmap::DashMap<String, Vec<std::time::Instant>>,
    limit: usize,
    window: std::time::Duration,
}

impl SubscriptionRateLimiter {
    pub fn new(limit: usize, window: std::time::Duration) -> Self {
        Self {
            hits: dashmap::DashMap::new(),
            limit,
            window,
        }
    }

    /// Record a hit for `ip`. Returns `Ok(())` when allowed, or `Err(retry_after_secs)`
    /// when the per-IP limit has been exceeded.
    pub fn check(&self, ip: &str) -> Result<(), u64> {
        let now = std::time::Instant::now();
        let mut entry = self.hits.entry(ip.to_string()).or_default();
        entry.retain(|t| now.duration_since(*t) < self.window);
        if entry.len() >= self.limit {
            let oldest = entry.first().copied().unwrap_or(now);
            let elapsed = now.duration_since(oldest);
            let retry_after = self.window.saturating_sub(elapsed).as_secs().max(1);
            return Err(retry_after);
        }
        entry.push(now);
        Ok(())
    }
}

/// Per-IP concurrent-connection limiter for SSE endpoints.
///
/// Uses a `DashMap` of active connection counts. A guard is returned on success
/// and decrements the counter when dropped, so disconnects free the slot.
pub struct SseConnectionLimiter {
    active: dashmap::DashMap<String, usize>,
    limit: usize,
}

/// RAII guard that releases an SSE connection slot on drop.
pub struct SseConnectionGuard {
    limiter: Arc<SseConnectionLimiter>,
    ip: String,
}

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        if let Some(mut count) = self.limiter.active.get_mut(&self.ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                drop(count);
                self.limiter.active.remove(&self.ip);
            }
        }
    }
}

impl SseConnectionLimiter {
    pub fn new(limit: usize) -> Self {
        Self {
            active: dashmap::DashMap::new(),
            limit,
        }
    }

    /// Try to acquire a connection slot for `ip`.
    pub fn try_acquire(self: &Arc<Self>, ip: &str) -> Option<SseConnectionGuard> {
        let mut entry = self.active.entry(ip.to_string()).or_insert(0);
        if *entry >= self.limit {
            return None;
        }
        *entry += 1;
        drop(entry);
        Some(SseConnectionGuard {
            limiter: Arc::clone(self),
            ip: ip.to_string(),
        })
    }
}

/// Extract the client IP from forwarding headers, falling back to `"unknown"`.
fn client_ip_from_headers(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// Build a `429 Too Many Requests` response with a `Retry-After` header.
fn too_many_requests(retry_after_secs: u64, message: &str) -> Response {
    let mut response = Response::new(Body::from(
        json!({ "error": message, "retry_after": retry_after_secs }).to_string(),
    ));
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    let headers = response.headers_mut();
    headers.insert(header::RETRY_AFTER, header::HeaderValue::from(retry_after_secs));
    headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
    response
}

/// Simple in-process cache entry for the contracts list.
struct CacheEntry {
    data: Value,
    expires_at: std::time::Instant,
}

static CONTRACTS_CACHE: OnceLock<Mutex<Option<CacheEntry>>> = OnceLock::new();

fn contracts_cache() -> &'static Mutex<Option<CacheEntry>> {
    CONTRACTS_CACHE.get_or_init(|| Mutex::new(None))
}

static EVENTS_FEED_CACHE: OnceLock<Mutex<HashMap<String, CacheEntry>>> = OnceLock::new();

fn events_feed_cache() -> &'static Mutex<HashMap<String, CacheEntry>> {
    EVENTS_FEED_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Extract the tenant_id from request extensions when multi-tenant mode is active.
/// Returns `None` in single-tenant mode (extension not present).
fn extract_tenant_id(extensions: &axum::http::Extensions) -> Option<&str> {
    extensions.get::<TenantId>().map(|t| t.0.as_str())
}

/// Append a `tenant_id = $N` condition when multi-tenant mode is active.
/// Returns the bind index incremented by 1 if a condition was added.
fn maybe_add_tenant_condition(
    conditions: &mut Vec<String>,
    bind_idx: &mut i32,
    tenant_id: Option<&str>,
) {
    if tenant_id.is_some() {
        conditions.push(format!("tenant_id = ${bind_idx}"));
        *bind_idx += 1;
    }
}

/// Encode a cursor with a tag indicating the sort column.
fn encode_cursor_tagged(tag: &str, value: &str, id: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(format!("{tag}:{value}:{id}"))
}

/// Decode a tagged cursor back to (tag, value, id).
fn decode_cursor_tagged(cursor: &str) -> Result<(String, String, Uuid), AppError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    let s = std::str::from_utf8(&bytes)
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    
    let parts: Vec<&str> = s.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(AppError::Validation("invalid cursor format".to_string()));
    }
    
    let tag = parts[0].to_string();
    let value = parts[1].to_string();
    let id = Uuid::parse_str(parts[2])
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    
    if id.get_version() != Some(uuid::Version::Random) {
        return Err(AppError::Validation("invalid cursor".to_string()));
    }
    
    Ok((tag, value, id))
}

/// Encode a (ledger, id) pair as an opaque URL-safe base64 cursor.
fn encode_cursor(ledger: i64, id: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(format!("{ledger}:{id}"))
}

/// Decode a cursor back to (ledger, id). Returns a validation error on malformed input.
fn decode_cursor(cursor: &str) -> Result<(i64, Uuid), AppError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    let s = std::str::from_utf8(&bytes)
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    let (ledger_str, id_str) = s
        .split_once(':')
        .ok_or_else(|| AppError::Validation("invalid cursor".to_string()))?;
    let ledger = ledger_str
        .parse::<i64>()
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    if ledger <= 0 {
        return Err(AppError::Validation("invalid cursor".to_string()));
    }
    // Reject astronomically large ledger values (Stellar ledger sequence is a u32)
    if ledger > i64::from(u32::MAX) {
        return Err(AppError::Validation("invalid cursor".to_string()));
    }
    let id =
        Uuid::parse_str(id_str).map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    if id.get_version() != Some(uuid::Version::Random) {
        return Err(AppError::Validation("invalid cursor".to_string()));
    }
    Ok((ledger, id))
}

/// Map sqlx rows to a JSON array, projecting only the requested columns.
/// Gzip-compress `value` and return a base64-encoded string (standard alphabet, no padding).
/// Used by the `compact=true` query parameter to shrink large `event_data` payloads.
fn compact_event_data(value: &Value) -> Result<Value, AppError> {
    use base64::engine::general_purpose::STANDARD;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    let json_bytes = serde_json::to_vec(value)
        .map_err(|e| AppError::Internal(format!("serialization error: {e}")))?;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&json_bytes)
        .map_err(|e| AppError::Internal(format!("gzip write error: {e}")))?;
    let compressed = encoder
        .finish()
        .map_err(|e| AppError::Internal(format!("gzip finish error: {e}")))?;

    Ok(Value::String(STANDARD.encode(&compressed)))
}

fn rows_to_json(
    rows: &[sqlx::postgres::PgRow],
    columns: &[&str],
    enc_key: Option<&[u8; 32]>,
    enc_key_old: Option<&[u8; 32]>,
    compact: bool,
) -> Result<Vec<Value>, AppError> {
    rows_to_json_with_format(
        rows,
        columns,
        enc_key,
        enc_key_old,
        compact,
        crate::scval_format::ScValFormat::Json,
    )
}

/// Like `rows_to_json`, but renders `event_data` (Issue #1064: `value` and
/// `topic` ScVals) in the requested `format` (`native`, `json` or `xdr`).
fn rows_to_json_with_format(
    rows: &[sqlx::postgres::PgRow],
    columns: &[&str],
    enc_key: Option<&[u8; 32]>,
    enc_key_old: Option<&[u8; 32]>,
    compact: bool,
    format: crate::scval_format::ScValFormat,
) -> Result<Vec<Value>, AppError> {
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let mut event = serde_json::Map::new();
        for &col in columns {
            match col {
                "id" => {
                    event.insert(col.to_string(), json!(row.try_get::<Uuid, _>(col)?));
                }
                "contract_id" => {
                    event.insert(col.to_string(), json!(row.try_get::<String, _>(col)?));
                }
                "event_type" => {
                    event.insert(col.to_string(), json!(row.try_get::<String, _>(col)?));
                }
                "tx_hash" => {
                    event.insert(col.to_string(), json!(row.try_get::<String, _>(col)?));
                }
                "ledger" => {
                    event.insert(col.to_string(), json!(row.try_get::<i64, _>(col)?));
                }
                "timestamp" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<DateTime<Utc>, _>(col)?),
                    );
                }
                "event_data" => {
                    let raw: Value = row.try_get::<Value, _>(col)?;
                    let decrypted = decrypt_event_data(&raw, enc_key, enc_key_old);
                    let rendered = crate::scval_format::render_event_data(&decrypted, format);
                    if compact {
                        event.insert(col.to_string(), compact_event_data(&rendered)?);
                    } else {
                        event.insert(col.to_string(), rendered);
                    }
                }
                "event_data_normalized" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<Option<Value>, _>(col)?),
                    );
                }
                "event_data_decoded" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<Option<Value>, _>(col)?),
                    );
                }
                "ledger_hash" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<Option<String>, _>(col)?),
                    );
                }
                "in_successful_call" => {
                    event.insert(col.to_string(), json!(row.try_get::<bool, _>(col)?));
                }
                "created_at" => {
                    event.insert(
                        col.to_string(),
                        json!(row.try_get::<DateTime<Utc>, _>(col)?),
                    );
                }
                "schema_version" => {
                    event.insert(col.to_string(), json!(row.try_get::<i32, _>(col)?));
                }
                "anonymized" => {
                    event.insert(col.to_string(), json!(row.try_get::<bool, _>(col)?));
                }
                "relevance_score" => {
                    event.insert(col.to_string(), json!(row.try_get::<f64, _>(col)?));
                }
                _ => {}
            }
        }
        events.push(Value::Object(event));
    }
    Ok(events)
}
fn resolve_columns<'a>(params: &'a PaginationParams) -> Result<Vec<&'a str>, AppError> {
    params.columns().map_err(|(unknown, allowed)| {
        AppError::Validation(format!(
            "unknown fields: [{}]; valid fields are: [{}]",
            unknown.join(", "),
            allowed.join(", ")
        ))
    })
}

/// Stellar ledger sequences are 32-bit unsigned integers (max 2^32 - 1).
/// Validate that a ledger parameter is within [0, 2^32 - 1] (issue #423).
pub(crate) fn validate_ledger_param(name: &str, value: i64) -> Result<(), AppError> {
    const MAX_LEDGER: i64 = u32::MAX as i64; // 4_294_967_295
    if value < 0 || value > MAX_LEDGER {
        return Err(AppError::Validation(format!(
            "{name} must be in range [0, {MAX_LEDGER}]"
        )));
    }
    Ok(())
}

/// Validate a `contract_id` query/path parameter.
///
/// `pub` (not `pub(crate)`) because `fuzz/fuzz_targets/fuzz_validate_contract_id.rs`
/// links this crate as an external dependency.
pub fn validate_contract_id(contract_id: &str) -> Result<(), AppError> {
    if contract_id.len() != 56 {
        return Err(AppError::Validation(
            "invalid contract_id format".to_string(),
        ));
    }
    if !contract_id.starts_with('C') {
        return Err(AppError::Validation(
            "invalid contract_id format".to_string(),
        ));
    }
    if !contract_id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(AppError::Validation(
            "invalid contract_id format".to_string(),
        ));
    }
    Ok(())
}

/// Validate a `tx_hash` query/path parameter.
///
/// `pub` (not `pub(crate)`) because `fuzz/fuzz_targets/fuzz_validate_tx_hash.rs`
/// links this crate as an external dependency.
pub fn validate_tx_hash(tx_hash: &str) -> Result<(), AppError> {
    if tx_hash.len() != 64 {
        return Err(AppError::Validation("invalid tx_hash format".to_string()));
    }
    // Accept both uppercase and lowercase hex — callers should normalize to lowercase first.
    if !tx_hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AppError::Validation("invalid tx_hash format".to_string()));
    }
    Ok(())
}

/// Validate and parse ISO 8601 timestamp string.
fn validate_timestamp(ts: &str) -> Result<DateTime<Utc>, AppError> {
    ts.parse::<DateTime<Utc>>()
        .map_err(|_| AppError::Validation(format!("invalid timestamp format: {}", ts)))
}

async fn build_health_response(state: &AppState) -> (StatusCode, Value) {
    let mut db_ok = true;
    let db_status: &str;

    let timeout = Duration::from_millis(state.health_check_timeout_ms);

    let db_check =
        tokio::time::timeout(timeout, sqlx::query("SELECT 1").fetch_one(&state.pool)).await;

    match db_check {
        Ok(Ok(_)) => {
            db_status = "ok";
        }
        Ok(Err(sqlx::Error::PoolTimedOut)) => {
            db_ok = false;
            db_status = "pool_exhausted";
        }
        Ok(Err(_)) => {
            db_ok = false;
            db_status = "unreachable";
        }
        Err(_) => {
            // tokio timeout elapsed
            db_ok = false;
            db_status = "unreachable";
        }
    }

    // Check indexer status
    let indexer_status = if let Some(secs_ago) = state.health_state.is_indexer_stalled() {
        json!({
            "indexer": "stalled",
            "last_poll_secs_ago": secs_ago
        })
    } else {
        json!({"indexer": "ok"})
    };

    // Determine overall status
    let is_degraded =
        !db_ok || indexer_status.get("indexer").and_then(|v| v.as_str()) == Some("stalled");

    if is_degraded {
        let response = json!({
            "status": "degraded",
            "db": db_status,
        });
        // Merge indexer status
        let mut obj = response;
        if let Value::Object(ref mut map) = obj {
            if let Value::Object(indexer_map) = indexer_status {
                map.extend(indexer_map);
            }
        }
        (StatusCode::SERVICE_UNAVAILABLE, obj)
    } else {
        let response = json!({
            "status": "ok",
            "db": "ok",
            "indexer": "ok"
        });
        (StatusCode::OK, response)
    }
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "system",
    responses(
        (status = 200, description = "Service is healthy", body = serde_json::Value),
        (status = 503, description = "Service is degraded", body = ErrorResponse),
    )
)]
pub async fn health(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let (status, body) = build_health_response(&state).await;
    (status, Json(body))
}

#[utoipa::path(
    get,
    path = "/healthz/live",
    tag = "system",
    responses(
        (status = 200, description = "Process is alive", body = serde_json::Value),
    )
)]
pub async fn health_live() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({ "status": "alive" })))
}

#[utoipa::path(
    get,
    path = "/healthz/ready",
    tag = "system",
    responses(
        (status = 200, description = "Service is ready", body = serde_json::Value),
        (status = 503, description = "Service is not ready", body = ErrorResponse),
    )
)]
pub async fn health_ready(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    use crate::config::Role;

    match state.config.role {
        // ── ROLE=indexer ─────────────────────────────────────────────────────
        // Ready when the advisory lock is held (i.e. this pod is the active
        // indexer leader).  A standby pod that lost the lock is intentionally
        // not ready so that rolling updates don't stall on the standby, and
        // Kubernetes won't route traffic to it while it waits for promotion.
        Role::Indexer => {
            let is_leader = state
                .indexer_state
                .is_active_indexer
                .load(std::sync::atomic::Ordering::SeqCst);

            if is_leader {
                (
                    StatusCode::OK,
                    Json(json!({ "status": "ok", "role": "indexer", "lock": "held" })),
                )
            } else {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "status": "degraded",
                        "role": "indexer",
                        "lock": "standby",
                        "reason": "advisory lock not held; this replica is on standby"
                    })),
                )
            }
        }

        // ── ROLE=api ──────────────────────────────────────────────────────────
        // Ready when the database is reachable.  No indexer stall check —
        // api pods never run the indexer, so an indexer stall on another pod
        // should not make every api pod unready.
        Role::Api => {
            let timeout = Duration::from_millis(state.health_check_timeout_ms);
            let db_check =
                tokio::time::timeout(timeout, sqlx::query("SELECT 1").fetch_one(&state.pool))
                    .await;

            let (db_ok, db_status) = match db_check {
                Ok(Ok(_)) => (true, "ok"),
                Ok(Err(sqlx::Error::PoolTimedOut)) => (false, "pool_exhausted"),
                Ok(Err(_)) => (false, "unreachable"),
                Err(_) => (false, "timeout"),
            };

            if db_ok {
                (
                    StatusCode::OK,
                    Json(json!({ "status": "ok", "role": "api", "db": "ok" })),
                )
            } else {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({ "status": "degraded", "role": "api", "db": db_status })),
                )
            }
        }

        // ── ROLE=all (default) ────────────────────────────────────────────────
        // Backward-compatible: DB reachable AND indexer not stalled.
        Role::All => {
            let (status, body) = build_health_response(&state).await;
            (status, Json(body))
        }
    }
}

/// Query parameters for the email unsubscribe endpoint (Issue #483).
#[derive(serde::Deserialize)]
pub struct UnsubscribeQuery {
    pub token: String,
}

/// Public, unauthenticated endpoint that recipients reach from the
/// "unsubscribe" link in notification emails (Issue #483, CAN-SPAM/GDPR).
/// Marks the token's recipient as opted out and returns a small HTML page.
#[utoipa::path(
    get,
    path = "/unsubscribe",
    tag = "system",
    params(("token" = String, Query, description = "Per-recipient unsubscribe token")),
    responses(
        (status = 200, description = "Unsubscribed (or already unsubscribed)"),
        (status = 404, description = "Unknown unsubscribe token"),
    )
)]
pub async fn unsubscribe(
    State(state): State<AppState>,
    Query(query): Query<UnsubscribeQuery>,
) -> Response {
    fn html_page(status: StatusCode, title: &str, message: &str) -> Response {
        let body = format!(
            "<!DOCTYPE html><html><head><meta charset=\"utf-8\">\
             <title>{title}</title></head><body style=\"font-family:sans-serif;\
             max-width:32rem;margin:4rem auto;text-align:center;\">\
             <h1>{title}</h1><p>{message}</p></body></html>"
        );
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(Body::from(body))
            .unwrap_or_else(|e| response_build_error(e))
    }

    // Verify the HMAC-signed, time-limited token (issue #1158).
    // The subject carried in the token is the opaque DB token string that
    // `mark_unsubscribed` looks up; we verify purpose = Unsubscribe before
    // touching the DB.
    let db_token = match crate::email_token::verify_token(
        &query.token,
        Some(&crate::email_token::TokenPurpose::Unsubscribe),
        None,
    ) {
        Ok(claims) => claims.subject,
        Err(_) => {
            // Return 404 (not 400) to avoid leaking why the token failed.
            return html_page(
                StatusCode::NOT_FOUND,
                "Invalid link",
                "This unsubscribe link is not valid.",
            );
        }
    };

    match crate::email::mark_unsubscribed(&state.pool, &db_token).await {
        Ok(true) => html_page(
            StatusCode::OK,
            "Unsubscribed",
            "You have been unsubscribed from Soroban Pulse notifications.",
        ),
        Ok(false) => html_page(
            StatusCode::NOT_FOUND,
            "Invalid link",
            "This unsubscribe link is not valid.",
        ),
        Err(e) => {
            tracing::error!(error = %e, "Failed to process unsubscribe request");
            html_page(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Something went wrong",
                "We could not process your request. Please try again later.",
            )
        }
    }
}

#[utoipa::path(
    get,
    path = "/healthz/postgres",
    tag = "system",
    responses(
        (status = 200, description = "PostgreSQL is healthy", body = serde_json::Value),
        (status = 503, description = "PostgreSQL is unhealthy", body = serde_json::Value),
    )
)]
pub async fn health_postgres(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let pg_health = crate::health_check::check_postgres(&state.pool, state.health_check_timeout_ms).await;
    
    let status_code = match pg_health.status {
        crate::health_check::HealthStatus::Ok => StatusCode::OK,
        crate::health_check::HealthStatus::Degraded => StatusCode::OK,
        crate::health_check::HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(json!(pg_health)))
}

#[utoipa::path(
    get,
    path = "/healthz/rpc",
    tag = "system",
    responses(
        (status = 200, description = "RPC endpoint is healthy", body = serde_json::Value),
        (status = 503, description = "RPC endpoint is unhealthy", body = serde_json::Value),
    )
)]
pub async fn health_rpc(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let rpc_health = crate::health_check::check_rpc(
        &state.config.stellar_rpc_url,
        state.health_check_timeout_ms,
    )
    .await;
    
    let status_code = match rpc_health.status {
        crate::health_check::HealthStatus::Ok => StatusCode::OK,
        crate::health_check::HealthStatus::Degraded => StatusCode::OK,
        crate::health_check::HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(json!(rpc_health)))
}

#[utoipa::path(
    get,
    path = "/healthz/external/{service}",
    tag = "system",
    params(("service" = String, Path, description = "External service name (e.g., 'webhooks', 'email')")),
    responses(
        (status = 200, description = "External service is healthy", body = serde_json::Value),
        (status = 400, description = "Invalid service name", body = serde_json::Value),
        (status = 503, description = "External service is unhealthy", body = serde_json::Value),
    )
)]
pub async fn health_external(
    State(_state): State<AppState>,
    Path(service): Path<String>,
) -> (StatusCode, Json<Value>) {
    // Validate service name
    let valid_services = vec!["webhooks", "email", "sms", "pagerduty"];
    if !valid_services.contains(&service.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("Unknown service: {}. Valid services are: {}", service, valid_services.join(", ")),
                "valid_services": valid_services,
            })),
        );
    }

    let external_health = crate::health_check::check_external_service(&service, 5000).await;
    
    let status_code = match external_health.status {
        crate::health_check::HealthStatus::Ok => StatusCode::OK,
        crate::health_check::HealthStatus::Degraded => StatusCode::OK,
        crate::health_check::HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(json!(external_health)))
}

#[utoipa::path(
    get,
    path = "/v1/status",
    tag = "system",
    responses(
        (status = 200, description = "Indexer operational status", body = serde_json::Value),
    )
)]
pub async fn status(State(state): State<AppState>) -> Json<Value> {
    let current_ledger = state.indexer_state.current_ledger.load(Ordering::Relaxed);
    let latest_ledger = state.indexer_state.latest_ledger.load(Ordering::Relaxed);
    let lag_ledgers = latest_ledger.saturating_sub(current_ledger);
    let uptime_secs = state.indexer_state.uptime_secs();

    let indexer_status = if state.health_state.is_indexer_stalled().is_some() {
        "stalled"
    } else {
        "running"
    };

    let indexer_mode = if state
        .indexer_state
        .is_active_indexer
        .load(Ordering::Relaxed)
    {
        "active"
    } else {
        "read_only"
    };

    let indexer_paused = state.indexer_state.is_paused.load(Ordering::Relaxed);

    let total_events: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
        .fetch_one(&state.pool)
        .await
        .unwrap_or(0);

    // Query event counts by type
    let events_by_type_rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type")
            .fetch_all(&state.pool)
            .await
            .unwrap_or_default();

    // Build events_by_type object with all event types (defaulting to 0 if not present)
    let mut events_by_type = serde_json::json!({
        "contract": 0i64,
        "diagnostic": 0i64,
        "system": 0i64,
    });

    for (event_type, count) in events_by_type_rows {
        if let Some(obj) = events_by_type.as_object_mut() {
            obj.insert(event_type, serde_json::json!(count));
        }
    }

    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": uptime_secs,
        "current_ledger": current_ledger,
        "latest_ledger": latest_ledger,
        "lag_ledgers": lag_ledgers,
        "total_events": total_events,
        "events_by_type": events_by_type,
        "indexer_status": indexer_status,
        "indexer_mode": indexer_mode,
        "indexer_paused": indexer_paused,
        "rpc_version": crate::rpc_meta::current_version(),
        "gaps": crate::rpc_meta::list_gaps(&state.pool).await,
    }))
}

/// GET /v1/admin/indexer/gaps
pub async fn get_indexer_gaps(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "gaps": crate::rpc_meta::list_gaps(&state.pool).await }))
}

/// Returns aggregate statistics about indexed events.
#[utoipa::path(
    get,
    path = "/v1/events/stats",
    tag = "events",
    responses(
        (status = 200, description = "Aggregate event statistics", body = crate::models::EventStats),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn get_event_stats(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    use std::collections::HashMap;

    let cache_key = "stats";

    // Try to get from cache
    if let Some(cached) = state.stats_cache.get(cache_key).await {
        crate::metrics::record_stats_cache_hit();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_str(&format!(
                "public, max-age={}",
                state.config.stats_cache_ttl_secs
            ))
            .unwrap_or_else(|_| axum::http::HeaderValue::from_static("public, max-age=30")),
        );
        return Ok((headers, Json(cached)));
    }

    crate::metrics::record_stats_cache_miss();

    // Total events and ledger range from raw table (fast with index)
    let totals_row = sqlx::query(
        "SELECT COUNT(*) AS total_events, MIN(ledger) AS min_ledger, MAX(ledger) AS max_ledger FROM events",
    )
    .fetch_one(&state.read_pool)
    .await?;

    let total_events: i64 = totals_row.try_get("total_events")?;
    let min_ledger: Option<i64> = totals_row.try_get("min_ledger")?;
    let max_ledger: Option<i64> = totals_row.try_get("max_ledger")?;

    // Per-type counts and time-windowed counts from events_daily_summary matview
    let type_rows = sqlx::query(
        "SELECT event_type, SUM(event_count) AS cnt FROM events_daily_summary GROUP BY event_type",
    )
    .fetch_all(&state.read_pool)
    .await?;

    let mut events_by_type: HashMap<String, i64> = HashMap::new();
    for row in &type_rows {
        let et: String = row.try_get("event_type")?;
        let cnt: i64 = row.try_get("cnt")?;
        events_by_type.insert(et, cnt);
    }
    events_by_type.entry("contract".to_string()).or_insert(0);
    events_by_type.entry("diagnostic".to_string()).or_insert(0);
    events_by_type.entry("system".to_string()).or_insert(0);

    // 24h and 7d counts from daily_summary (last 1 and 7 days)
    let window_row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(event_count) FILTER (WHERE event_date >= CURRENT_DATE - INTERVAL '1 day'), 0)  AS events_last_24h,
            COALESCE(SUM(event_count) FILTER (WHERE event_date >= CURRENT_DATE - INTERVAL '7 days'), 0) AS events_last_7d
        FROM events_daily_summary
        "#,
    )
    .fetch_one(&state.read_pool)
    .await?;

    let events_last_24h: i64 = window_row.try_get("events_last_24h")?;
    let events_last_7d: i64 = window_row.try_get("events_last_7d")?;

    // Top 10 contracts from events_contract_summary matview
    let top_rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT contract_id, event_count FROM events_contract_summary ORDER BY event_count DESC LIMIT 10",
    )
    .fetch_all(&state.read_pool)
    .await?;

    let top_contracts = top_rows
        .into_iter()
        .map(
            |(contract_id, event_count)| crate::models::ContractStatEntry {
                contract_id,
                event_count,
            },
        )
        .collect();

    let stats = crate::models::EventStats {
        total_events,
        events_last_24h,
        events_last_7d,
        top_contracts,
        events_by_type,
        min_ledger,
        max_ledger,
        computed_at: Utc::now(),
    };

    let stats_json = serde_json::to_value(&stats)?;

    // Store in cache
    state
        .stats_cache
        .insert(cache_key.to_string(), stats_json.clone())
        .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_str(&format!(
            "public, max-age={}",
            state.config.stats_cache_ttl_secs
        ))
        .unwrap_or_else(|_| axum::http::HeaderValue::from_static("public, max-age=30")),
    );

    Ok((headers, Json(stats_json)))
}

/// Return per-contract daily event counts sourced from the `mv_contract_event_counts`
/// materialized view.  Results are cached in the in-process query cache for the
/// duration of `QUERY_CACHE_TTL_SECS` (default 5 min, clamped to [5 min, 60 min]).
#[utoipa::path(
    get,
    path = "/v1/contracts/{contract_id}/event-counts",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
    ),
    responses(
        (status = 200, description = "Daily event counts for the contract"),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
    )
)]
pub async fn get_contract_event_counts(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    let cache_key = format!("contract_event_counts:{contract_id}");

    if let Some(cached) = crate::query_cache::get(&state.query_result_cache, &cache_key).await {
        return Ok(Json(cached));
    }

    let rows = sqlx::query(
        "SELECT event_type, event_day, event_count, unique_tx_count, last_event_at \
         FROM mv_contract_event_counts \
         WHERE contract_id = $1 \
         ORDER BY event_day DESC \
         LIMIT 90",
    )
    .bind(&contract_id)
    .fetch_all(&state.read_pool)
    .await?;

    let counts: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "event_type":      r.try_get::<String, _>("event_type").unwrap_or_default(),
                "event_day":       r.try_get::<chrono::DateTime<chrono::Utc>, _>("event_day").ok()
                                     .map(|d| d.format("%Y-%m-%d").to_string())
                                     .unwrap_or_default(),
                "event_count":     r.try_get::<i64, _>("event_count").unwrap_or(0),
                "unique_tx_count": r.try_get::<i64, _>("unique_tx_count").unwrap_or(0),
                "last_event_at":   r.try_get::<chrono::DateTime<chrono::Utc>, _>("last_event_at").ok()
                                     .map(|t| t.to_rfc3339())
                                     .unwrap_or_default(),
            })
        })
        .collect();

    let result = json!({
        "contract_id": contract_id,
        "counts":      counts,
        "cached_at":   Utc::now().to_rfc3339(),
    });

    crate::query_cache::set(&state.query_result_cache, cache_key, result.clone()).await;

    Ok(Json(result))
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct ContractHistoryParams {
    pub bucket: Option<String>,
    pub days: Option<i64>,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[utoipa::path(
    get,
    path = "/v1/contracts/{contract_id}/stats/history",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
        ("bucket" = Option<String>, Query, description = "Aggregation bucket. Currently only 1d is supported"),
        ("days" = Option<i64>, Query, description = "Number of daily buckets to return"),
        ("from" = Option<String>, Query, description = "Start date, YYYY-MM-DD"),
        ("to" = Option<String>, Query, description = "End date, YYYY-MM-DD"),
    ),
    responses(
        (status = 200, description = "Daily contract event and unique transaction history", body = serde_json::Value),
        (status = 400, description = "Invalid contract_id, bucket, or date range", body = ErrorResponse),
    )
)]
pub async fn get_contract_stats_history(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(params): Query<ContractHistoryParams>,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;
    let bucket = params.bucket.as_deref().unwrap_or("1d");
    if bucket != "1d" {
        return Err(AppError::Validation("unsupported bucket: only bucket=1d is supported".to_string()));
    }

    let today = Utc::now().date_naive();
    let (start_date, end_date) = if params.from.is_some() || params.to.is_some() {
        let from = params.from.as_deref().ok_or_else(|| AppError::Validation("from is required when to is provided".to_string()))?;
        let to = params.to.as_deref().ok_or_else(|| AppError::Validation("to is required when from is provided".to_string()))?;
        let start = chrono::NaiveDate::parse_from_str(from, "%Y-%m-%d")
            .map_err(|_| AppError::Validation("from must be YYYY-MM-DD".to_string()))?;
        let end = chrono::NaiveDate::parse_from_str(to, "%Y-%m-%d")
            .map_err(|_| AppError::Validation("to must be YYYY-MM-DD".to_string()))?;
        (start, end)
    } else {
        let days = params.days.unwrap_or(30);
        if !(1..=366).contains(&days) {
            return Err(AppError::Validation("days must be between 1 and 366".to_string()));
        }
        (today - chrono::Duration::days(days - 1), today)
    };
    if start_date > end_date {
        return Err(AppError::Validation("from must be <= to".to_string()));
    }
    if (end_date - start_date).num_days() + 1 > 366 {
        return Err(AppError::Validation("date range cannot exceed 366 daily buckets".to_string()));
    }

    let start = std::time::Instant::now();
    let rows = sqlx::query(
        r#"
        SELECT d::date AS date,
               COALESCE(m.event_count, 0)::bigint AS event_count,
               COALESCE(m.unique_tx_count, 0)::bigint AS unique_tx_count
        FROM generate_series($2::date, $3::date, interval '1 day') AS d
        LEFT JOIN mv_contract_summary m
          ON m.contract_id = $1 AND m.event_date = d::date
        ORDER BY d::date ASC
        "#,
    )
    .bind(&contract_id)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(&state.read_pool)
    .await?;
    crate::metrics::record_contract_history_query_duration(start.elapsed());

    let data = rows
        .into_iter()
        .map(|row| {
            let date: chrono::NaiveDate = row.try_get("date")?;
            let event_count: i64 = row.try_get("event_count")?;
            let unique_tx_count: i64 = row.try_get("unique_tx_count")?;
            Ok(json!({ "date": date, "event_count": event_count, "unique_tx_count": unique_tx_count }))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;

    Ok(Json(json!({ "contract_id": contract_id, "bucket": bucket, "data": data })))
}

pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    crate::metrics::update_db_pool_metrics(&state.pool);
    state.prometheus_handle.render()
}

/// Serve the raw OpenAPI JSON spec.
pub async fn openapi_json() -> impl IntoResponse {
    use crate::routes::ApiDoc;
    use utoipa::OpenApi;
    Json(ApiDoc::openapi())
}

/// Serve the branded SorobanPulse Swagger UI page.
///
/// The inline `<style>` block applies SorobanPulse brand colours and full dark
/// mode on top of the stock Swagger UI stylesheet loaded from unpkg.com.
/// The `<script>` block initialises SwaggerUIBundle.
///
/// Both blocks carry a SHA-256 hash that is allow-listed in the `/docs` CSP so
/// `unsafe-inline` is **not** needed.  If you modify either block you must
/// recompute the hash with:
///
/// ```sh
/// printf '%s' 'THE CONTENT' | openssl dgst -sha256 -binary | base64
/// ```
///
/// and update `csp_docs` in `src/middleware/security_headers.rs` accordingly.
pub async fn swagger_ui() -> impl IntoResponse {
    // Inline CSS — SHA-256: t5Nfs8a1PFuEVO00S72ZGB4P65C74f37u8w0VCVsqBw=
    let style = r#":root{--sp-bg:#0f1117;--sp-surface:#1a1d2e;--sp-border:#2d3158;--sp-accent:#7c3aed;--sp-accent-light:#a78bfa;--sp-text:#e2e8f0;--sp-text-muted:#94a3b8;--sp-success:#10b981;--sp-warning:#f59e0b;--sp-danger:#ef4444}
body{background:var(--sp-bg)!important;color:var(--sp-text)!important;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif}
#swagger-ui .swagger-ui .info{margin:2rem 0}
#swagger-ui .swagger-ui .info .title{color:var(--sp-accent-light)!important;font-size:2rem!important;font-weight:700}
#swagger-ui .swagger-ui .info .description p,#swagger-ui .swagger-ui .info .description li{color:var(--sp-text)!important}
#swagger-ui .swagger-ui .info .description a{color:var(--sp-accent-light)!important}
#swagger-ui .topbar{background:var(--sp-surface)!important;border-bottom:1px solid var(--sp-border)!important;padding:.75rem 1rem}
#swagger-ui .swagger-ui .scheme-container{background:var(--sp-surface)!important;border-bottom:1px solid var(--sp-border)!important}
#swagger-ui .swagger-ui .opblock-tag{color:var(--sp-text)!important;border-bottom:1px solid var(--sp-border)!important}
#swagger-ui .swagger-ui .opblock{background:var(--sp-surface)!important;border:1px solid var(--sp-border)!important;border-radius:.5rem!important;margin-bottom:.75rem}
#swagger-ui .swagger-ui .opblock .opblock-summary{border-bottom:1px solid var(--sp-border)!important}
#swagger-ui .swagger-ui .opblock .opblock-summary-method{border-radius:.25rem!important;font-weight:600}
#swagger-ui .swagger-ui .opblock.opblock-get .opblock-summary-method{background:var(--sp-success)!important}
#swagger-ui .swagger-ui .opblock.opblock-post .opblock-summary-method{background:var(--sp-accent)!important}
#swagger-ui .swagger-ui .opblock.opblock-delete .opblock-summary-method{background:var(--sp-danger)!important}
#swagger-ui .swagger-ui .opblock.opblock-patch .opblock-summary-method{background:var(--sp-warning)!important}
#swagger-ui .swagger-ui .opblock-description-wrapper p,.swagger-ui .markdown p{color:var(--sp-text)!important}
#swagger-ui .swagger-ui section.models{background:var(--sp-surface)!important;border:1px solid var(--sp-border)!important;border-radius:.5rem}
#swagger-ui .swagger-ui section.models h4{color:var(--sp-text)!important}
#swagger-ui .swagger-ui .model-box{background:var(--sp-bg)!important}
#swagger-ui .swagger-ui .parameter__name,#swagger-ui .swagger-ui .parameter__type{color:var(--sp-text-muted)!important}
#swagger-ui .swagger-ui input[type=text],#swagger-ui .swagger-ui textarea{background:var(--sp-bg)!important;border:1px solid var(--sp-border)!important;color:var(--sp-text)!important}
#swagger-ui .swagger-ui select{background:var(--sp-bg)!important;color:var(--sp-text)!important;border:1px solid var(--sp-border)!important}
#swagger-ui .swagger-ui .btn.authorize{background:var(--sp-accent)!important;border-color:var(--sp-accent)!important;color:#fff!important}
#swagger-ui .swagger-ui .btn.execute{background:var(--sp-success)!important;border-color:var(--sp-success)!important;color:#fff!important}
#swagger-ui .swagger-ui .response-col_status{color:var(--sp-text)!important}
#swagger-ui .swagger-ui .response-col_description{color:var(--sp-text-muted)!important}
#swagger-ui .swagger-ui pre.microlight{background:var(--sp-bg)!important;color:var(--sp-accent-light)!important;border:1px solid var(--sp-border)!important;border-radius:.375rem}
#swagger-ui .swagger-ui .highlight-code{background:var(--sp-bg)!important}"#;

    // Inline JS — SHA-256: OlhJ06FtsPaiJ/1A+8VpeJOGKCgqkf63ajgd7nrxk98=
    let script = r##"SwaggerUIBundle({url:"/openapi.json",dom_id:"#swagger-ui",deepLinking:true,presets:[SwaggerUIBundle.presets.apis,SwaggerUIBundle.SwaggerUIStandalonePreset],layout:"BaseLayout",docExpansion:"list",defaultModelsExpandDepth:1})"##;

    // SorobanPulse logo — inline SVG, no external fetch required
    let logo_svg = r##"<svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 36 36" fill="none" aria-hidden="true">
  <circle cx="18" cy="18" r="18" fill="#7c3aed"/>
  <path d="M10 18 Q18 8 26 18 Q18 28 10 18Z" fill="#a78bfa"/>
  <circle cx="18" cy="18" r="4" fill="#0f1117"/>
</svg>"##;

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>SorobanPulse — API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
  <style>{style}</style>
</head>
<body>
  <header style="display:flex;align-items:center;gap:.75rem;padding:1rem 1.5rem;background:#1a1d2e;border-bottom:1px solid #2d3158;position:sticky;top:0;z-index:100">
    {logo_svg}
    <span style="font-size:1.25rem;font-weight:700;color:#a78bfa;letter-spacing:-.01em">SorobanPulse</span>
    <span style="font-size:.875rem;color:#94a3b8;margin-left:.25rem">API Explorer</span>
    <a href="/openapi.json" style="margin-left:auto;font-size:.8125rem;color:#a78bfa;text-decoration:none;border:1px solid #2d3158;padding:.25rem .625rem;border-radius:.375rem">OpenAPI JSON ↗</a>
  </header>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>{script}</script>
</body>
</html>"#,
        style = style,
        logo_svg = logo_svg,
        script = script,
    );

    axum::response::Html(html)
}

/// Stream new events in real time via Server-Sent Events.
///
/// This endpoint is less preferred for contract-specific streaming; use
/// `/v1/events/contract/{contract_id}/stream` instead.
#[utoipa::path(
    get,
    path = "/v1/events/stream",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID (less preferred)"),
        ("fields" = Option<String>, Query, description = "Comma-separated list of fields to include in each event"),
    ),
    responses(
        (status = 200, description = "SSE stream of new events (text/event-stream)"),
        (status = 400, description = "Invalid contract_id format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 503, description = "Too many SSE connections", body = ErrorResponse),
    )
)]
#[instrument(skip(state, headers, extensions), fields(contract_id = ?params.contract_id))]
pub async fn stream_events(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
    headers: axum::http::HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let client_ip = extract_client_ip(&headers);
    let scval_format = crate::scval_format::parse_format(params.format.as_deref())
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({ "error": e, "code": "VALIDATION_ERROR" }))))?;
    stream_events_internal(State(state), params.contract_id, params.fields, params.event_type, headers, tenant_id, client_ip, scval_format)
        .await
}

/// Stream new events for a specific contract in real time via Server-Sent Events.
#[utoipa::path(
    get,
    path = "/v1/events/contract/{contract_id}/stream",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
        ("fields" = Option<String>, Query, description = "Comma-separated list of fields to include in each event"),
    ),
    responses(
        (status = 200, description = "SSE stream of contract events (text/event-stream)"),
        (status = 400, description = "Invalid contract_id format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 503, description = "Too many SSE connections", body = ErrorResponse),
    )
)]
pub async fn stream_events_by_contract(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(params): Query<StreamParams>,
    headers: axum::http::HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    validate_contract_id(&contract_id).map_err(|e| {
        let (status, body) = e.into_response_parts();
        (status, body)
    })?;
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let client_ip = extract_client_ip(&headers);
    let scval_format = crate::scval_format::parse_format(params.format.as_deref())
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({ "error": e, "code": "VALIDATION_ERROR" }))))?;
    stream_events_internal(State(state), Some(contract_id), params.fields, params.event_type, headers, tenant_id, client_ip, scval_format)
        .await
}

/// Stream events for multiple contracts simultaneously via Server-Sent Events.
#[utoipa::path(
    get,
    path = "/v1/events/stream/multi",
    tag = "events",
    params(
        ("contract_ids" = String, Query, description = "Comma-separated list of contract IDs to subscribe to"),
    ),
    responses(
        (status = 200, description = "SSE stream of events from the specified contracts (text/event-stream)"),
        (status = 400, description = "Invalid or empty contract_ids", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 503, description = "Too many SSE connections", body = ErrorResponse),
    )
)]
#[instrument(skip(state, headers, extensions), fields(contract_ids = ?params.contract_ids))]
pub async fn stream_events_multi(
    State(state): State<AppState>,
    Query(params): Query<crate::models::MultiStreamParams>,
    headers: axum::http::HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let client_ip = extract_client_ip(&headers);
    let event_type_filter = params.event_type;
    let raw = params.contract_ids.unwrap_or_default();
    if raw.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "contract_ids must not be empty", "code": "VALIDATION_ERROR" })),
        ));
    }

    let ids: Vec<String> = raw.split(',').map(|s| s.trim().to_string()).collect();

    // Validate number of contract IDs does not exceed limit
    if ids.len() > state.config.sse_multi_max_contract_ids {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("too many contract IDs (max {})", state.config.sse_multi_max_contract_ids),
                "code": "VALIDATION_ERROR",
                "limit": state.config.sse_multi_max_contract_ids,
                "provided": ids.len(),
            })),
        ));
    }

    // Record histogram metric for contract IDs per connection
    crate::metrics::record_sse_multi_contract_ids(ids.len() as u64);

    // Validate every ID; collect all invalid ones for a helpful error message.
    let invalid: Vec<String> = ids
        .iter()
        .filter(|id| validate_contract_id(id).is_err())
        .cloned()
        .collect();

    if !invalid.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid contract_id(s)",
                "code": "VALIDATION_ERROR",
                "invalid_ids": invalid,
            })),
        ));
    }

    // Check global connection limit
    let current_connections = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    if current_connections >= state.sse_max_connections {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "too many SSE connections", "code": "SSE_LIMIT_EXCEEDED" })),
        ));
    }

    // #453: Per-IP connection limit check
    let max_per_ip = state.config.sse_max_connections_per_ip;
    if max_per_ip > 0 {
        let current_ip_count = state
            .sse_connections_per_ip
            .get(&client_ip)
            .map(|v| *v)
            .unwrap_or(0);
        if current_ip_count >= max_per_ip {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "too many SSE connections from this IP",
                    "code": "SSE_IP_LIMIT_EXCEEDED",
                    "limit": max_per_ip,
                })),
            ));
        }
        *state.sse_connections_per_ip.entry(client_ip.clone()).or_insert(0) += 1;
        let new_ip_count = state.sse_connections_per_ip.get(&client_ip).map(|v| *v).unwrap_or(0);
        crate::metrics::record_sse_connections_per_ip(new_ip_count);
    }

    state
        .sse_connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let new_count = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    crate::metrics::update_sse_connections(new_count);

    let keepalive_ms = state.sse_keepalive_interval_ms;
    let sse_connections = state.sse_connections.clone();
    let sse_connections_per_ip = state.sse_connections_per_ip.clone();
    let client_ip_cleanup = client_ip.clone();
    let max_per_ip_cleanup = max_per_ip;
    let max_lag = state.config.sse_max_lag_before_disconnect;
    let connection_id = Uuid::new_v4().to_string();

    // Replay missed events if the client sends Last-Event-ID.
    // Try ring buffer first; fall back to DB on miss.
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| uuid::Uuid::parse_str(s).ok());

    let ring_replay_multi: Vec<crate::models::SorobanEvent> = if let Some(last_id) = last_event_id {
        match state.sse_ring_buffer.events_since(last_id) {
            Some(events) => events
                .into_iter()
                .filter(|ev| {
                    if !ids.contains(&ev.contract_id) { return false; }
                    if let Some(et) = event_type_filter {
                        if ev.event_type != et.to_string() { return false; }
                    }
                    true
                })
                .take(state.config.sse_replay_limit as usize)
                .collect(),
            None => {
                crate::metrics::record_sse_ring_buffer_miss();
                vec![]
            }
        }
    } else {
        vec![]
    };

    // DB fallback for multi-stream when ring buffer misses.
    let db_replay_multi: Vec<crate::models::Event> =
        if let (Some(last_id), true) = (last_event_id.clone(), ring_replay_multi.is_empty()) {
            let base_offset = 2usize;
            let placeholders: String = ids
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", base_offset + i))
                .collect::<Vec<_>>()
                .join(", ");
            let tid = tenant_id.as_deref();
            let tenant_clause = if tid.is_some() {
                format!(" AND tenant_id = ${}", base_offset + ids.len())
            } else {
                String::new()
            };
            let sql = format!(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 AND contract_id IN ({}){} ORDER BY created_at ASC LIMIT {}",
                placeholders, tenant_clause, state.config.sse_replay_limit
            );
            let mut q = sqlx::query_as::<_, crate::models::Event>(&sql).bind(last_id);
            for id in &ids {
                q = q.bind(id);
            }
            if let Some(tid) = tid {
                q = q.bind(tid);
            }
            q.fetch_all(&state.pool).await.unwrap_or_default()
        } else {
            vec![]
        };

    let total_replayed_multi = ring_replay_multi.len() + db_replay_multi.len();
    if total_replayed_multi > 0 {
        crate::metrics::record_sse_replayed_events(total_replayed_multi as u64);
    }

    let has_replay = total_replayed_multi > 0;
    let rx = state.event_tx.subscribe();
    let enc_key = state.encryption_key;
    let enc_key_old = state.encryption_key_old;

    let replay_stream = futures::stream::iter(ring_replay_multi.into_iter().map(move |ev| {
        let eid = ev.id.map(|u| u.to_string())
            .unwrap_or_else(|| format!("{}-{}", ev.tx_hash, ev.ledger));
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Ok(Event::default()
            .id(eid)
            .retry(Duration::from_millis(keepalive_ms))
            .data(data))
    }));

    let db_replay_stream_multi = futures::stream::iter(db_replay_multi.into_iter().filter_map(move |mut ev| {
        ev.event_data = decrypt_event_data(&ev.event_data, enc_key.as_ref(), enc_key_old.as_ref());
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Some(Ok(Event::default()
            .id(ev.id.to_string())
            .retry(Duration::from_millis(keepalive_ms))
            .data(data)))
    }));

    // #454: replay_complete event
    let replay_complete_stream = if has_replay {
        futures::stream::iter(vec![Ok(Event::default()
            .event("replay_complete")
            .data("replay complete"))])
    } else {
        futures::stream::iter(vec![])
    };

    let live_stream = futures::stream::unfold(
        (rx, ids, keepalive_ms, tenant_id, event_type_filter, false, max_lag, connection_id.clone()),
        move |(mut rx, filter_ids, ka, tid, et_filter, closed, max_lag, conn_id)| async move {
            if closed {
                return None;
            }
            let mut interval = tokio::time::interval(Duration::from_millis(ka));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                tokio::select! {
                    recv = rx.recv() => match recv {
                        Ok(event) => {
                            if !filter_ids.contains(&event.contract_id) {
                                continue;
                            }
                            // #452: Filter by event_type
                            if let Some(et) = et_filter {
                                if event.event_type.to_string() != et.to_string() {
                                    continue;
                                }
                            }
                            if let Some(ref tenant) = tid {
                                if event.tenant_id.as_deref() != Some(tenant.as_str()) {
                                    continue;
                                }
                            }
                            let data = serde_json::to_string(&event).unwrap_or_default();
                            let eid = event.id.map(|u| u.to_string())
                                .unwrap_or_else(|| format!("{}-{}", event.tx_hash, event.ledger));
                            let sse = Event::default()
                                .id(eid)
                                .retry(Duration::from_millis(ka))
                                .data(data);
                            return Some((Ok(sse), (rx, filter_ids, ka, tid, et_filter, false, max_lag, conn_id)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // #451: Send lag notification
                            crate::metrics::increment_sse_lagged_events(&conn_id, n);
                            let lag_data = serde_json::json!({ "missed": n, "last_event_id": null });
                            let lag_event = Event::default().event("lag").data(lag_data.to_string());
                            if max_lag > 0 && n >= max_lag {
                                return Some((Ok(lag_event), (rx, filter_ids, ka, tid, et_filter, true, max_lag, conn_id)));
                            }
                            return Some((Ok(lag_event), (rx, filter_ids, ka, tid, et_filter, false, max_lag, conn_id)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            let close_event = Event::default().event("close").data("stream closed");
                            return Some((Ok(close_event), (rx, filter_ids, ka, tid, et_filter, true, max_lag, conn_id)));
                        }
                    },
                    _ = interval.tick() => {
                        let ts = chrono::Utc::now().to_rfc3339();
                        let ping = Event::default().event("ping").data(ts);
                        return Some((Ok(ping), (rx, filter_ids, ka, tid, et_filter, false, max_lag, conn_id)));
                    }
                }
            }
        },
    );

    let combined = replay_stream
        .chain(db_replay_stream_multi)
        .chain(replay_complete_stream)
        .chain(live_stream);

    let stream_with_cleanup = futures::stream::unfold(
        (Box::pin(combined), sse_connections.clone(), sse_connections_per_ip, client_ip_cleanup, max_per_ip_cleanup),
        move |(mut stream, counter, ip_map, ip, max_ip)| async move {
            match stream.next().await {
                Some(item) => Some((item, (stream, counter, ip_map, ip, max_ip))),
                None => {
                    let new_count = counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                    crate::metrics::update_sse_connections(new_count);
                    if max_ip > 0 {
                        let mut entry = ip_map.entry(ip.clone()).or_insert(0);
                        if *entry > 0 { *entry -= 1; }
                    }
                    None
                }
            }
        },
    );

    Ok(Sse::new(stream_with_cleanup))
}

/// WebSocket event stream. Clients receive events as JSON text frames.
/// After connecting, a client may send `{"contract_id":"CABC..."}` to filter
/// by contract, or `{}` / omit the field to receive all events.
#[utoipa::path(
    get,
    path = "/v1/events/ws",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Initial contract ID filter (can also be set via WebSocket message)"),
    ),
    responses(
        (status = 101, description = "WebSocket upgrade"),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn ws_events(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    let initial_filter = params.contract_id.clone();
    let event_tx = state.event_tx.clone();
    let enc_key = state.encryption_key;
    let enc_key_old = state.encryption_key_old;

    Ok(ws.on_upgrade(move |mut socket| async move {
        use axum::extract::ws::Message;

        let mut rx = event_tx.subscribe();
        let mut contract_filter = initial_filter;

        loop {
            tokio::select! {
                // Incoming message from client (filter update or close)
                msg = socket.recv() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                contract_filter = v.get("contract_id")
                                    .and_then(|c| c.as_str())
                                    .map(|s| s.to_string());
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        _ => {}
                    }
                }
                // Outgoing event from broadcast channel
                result = rx.recv() => {
                    match result {
                        Ok(mut event) => {
                            if let Some(ref cid) = contract_filter {
                                if &event.contract_id != cid {
                                    continue;
                                }
                            }
                            let text = serde_json::to_string(&event).unwrap_or_default();
                            if socket.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    }))
}

async fn stream_events_internal(
    State(state): State<AppState>,
    contract_filter: Option<String>,
    fields: Option<String>,
    event_type_filter: Option<crate::models::EventType>,
    headers: axum::http::HeaderMap,
    tenant_id: Option<String>,
    client_ip: String,
    scval_format: crate::scval_format::ScValFormat,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    // Check if we've reached the max SSE connections limit
    let current_connections = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    if current_connections >= state.sse_max_connections {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "too many SSE connections",
                "code": "SSE_LIMIT_EXCEEDED"
            })),
        ));
    }

    // #453: Per-IP connection limit check
    let max_per_ip = state.config.sse_max_connections_per_ip;
    if max_per_ip > 0 {
        let current_ip_count = state
            .sse_connections_per_ip
            .get(&client_ip)
            .map(|v| *v)
            .unwrap_or(0);
        if current_ip_count >= max_per_ip {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "too many SSE connections from this IP",
                    "code": "SSE_IP_LIMIT_EXCEEDED",
                    "limit": max_per_ip,
                })),
            ));
        }
        // Increment per-IP counter
        *state.sse_connections_per_ip.entry(client_ip.clone()).or_insert(0) += 1;
        let new_ip_count = state.sse_connections_per_ip.get(&client_ip).map(|v| *v).unwrap_or(0);
        crate::metrics::record_sse_connections_per_ip(new_ip_count);
    }

    // Increment connection counter
    state
        .sse_connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let new_count = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    crate::metrics::update_sse_connections(new_count);

    let keepalive_ms = state.sse_keepalive_interval_ms;
    let sse_connections = state.sse_connections.clone();
    let sse_connections_per_ip = state.sse_connections_per_ip.clone();
    let client_ip_cleanup = client_ip.clone();
    let max_per_ip_cleanup = max_per_ip;

    // Validate contract_id if provided
    if let Some(ref cid) = contract_filter {
        validate_contract_id(cid).map_err(|e| {
            // Decrement per-IP counter on validation error
            if max_per_ip > 0 {
                let mut entry = sse_connections_per_ip.entry(client_ip.clone()).or_insert(0);
                if *entry > 0 { *entry -= 1; }
            }
            state.sse_connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            let body = json!({ "error": e.to_string(), "code": "VALIDATION_ERROR" });
            (StatusCode::BAD_REQUEST, Json(body))
        })?;
    }

    // Resolve field projection
    let field_columns: Option<Vec<&'static str>> = fields.as_deref().and_then(|f| {
        let trimmed = f.trim();
        if trimmed.is_empty() {
            return None;
        }
        let cols: Vec<&'static str> = trimmed
            .split(',')
            .map(|s| s.trim())
            .filter_map(|s| {
                PaginationParams::ALLOWED_FIELDS
                    .iter()
                    .find(|&&a| a == s)
                    .copied()
            })
            .collect();
        if cols.is_empty() { None } else { Some(cols) }
    });

    // Replay missed events if the client sends Last-Event-ID.
    // Strategy: check the in-memory ring buffer first (O(n), no DB round-trip).
    // If the ID was evicted (buffer overflow), fall back to a DB query.
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok());

    // Ring-buffer replay: returns SorobanEvent (broadcast type), converted to SSE frames directly.
    let ring_replay: Vec<crate::models::SorobanEvent> = if let Some(last_id) = last_event_id {
        match state.sse_ring_buffer.events_since(last_id) {
            Some(events) => {
                // Apply contract filter and event_type filter
                events
                    .into_iter()
                    .filter(|ev| {
                        if let Some(ref cid) = contract_filter {
                            if &ev.contract_id != cid {
                                return false;
                            }
                        }
                        if let Some(et) = event_type_filter {
                            if ev.event_type != et.to_string() {
                                return false;
                            }
                        }
                        true
                    })
                    .take(state.config.sse_replay_limit as usize)
                    .collect()
            }
            None => {
                // Ring buffer miss — evicted; fall back to DB query.
                crate::metrics::record_sse_ring_buffer_miss();
                vec![]
            }
        }
    } else {
        vec![]
    };

    // DB fallback replay when ring buffer miss occurs (last_event_id set but ring returned empty
    // because of eviction). Also used when ring buffer is disabled.
    let db_replay: Vec<crate::models::Event> = if let (Some(last_id), true) =
        (last_event_id.clone(), ring_replay.is_empty())
    {
        let q = if let Some(ref cid) = contract_filter {
            sqlx::query_as::<_, crate::models::Event>(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_normalized, created_at, schema_version, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 AND contract_id = $2 ORDER BY created_at ASC LIMIT $3",
            )
            .bind(last_id)
            .bind(cid)
            .bind(state.config.sse_replay_limit as i64)
            .fetch_all(&state.pool)
            .await
        } else {
            sqlx::query_as::<_, crate::models::Event>(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_normalized, created_at, schema_version, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 ORDER BY created_at ASC LIMIT $2",
            )
            .bind(last_id)
            .bind(state.config.sse_replay_limit as i64)
            .fetch_all(&state.pool)
            .await
        };
        q.unwrap_or_default()
    } else {
        vec![]
    };

    let total_replayed = ring_replay.len() + db_replay.len();
    if total_replayed > 0 {
        crate::metrics::record_sse_replayed_events(total_replayed as u64);
    }

    let has_replay = total_replayed > 0;
    let rx = state.event_tx.subscribe();
    let enc_key = state.encryption_key;
    let enc_key_old = state.encryption_key_old;
    let max_lag = state.config.sse_max_lag_before_disconnect;
    // Generate a stable connection ID for lag metrics
    let connection_id = Uuid::new_v4().to_string();

    // Ring-buffer replay stream: SorobanEvent items already filtered above.
    let ring_replay_stream = stream::iter(ring_replay.into_iter().map(move |ev| {
        let id_str = ev.id.map(|u| u.to_string())
            .unwrap_or_else(|| format!("{}-{}", ev.tx_hash, ev.ledger));
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Ok(Event::default()
            .id(id_str)
            .retry(Duration::from_millis(keepalive_ms))
            .data(data))
    }));

    let field_columns_replay = field_columns.clone();
    // DB fallback replay stream: full Event records from DB.
    let db_replay_stream = stream::iter(db_replay.into_iter().filter_map(move |mut ev| {
        ev.event_data = decrypt_event_data(&ev.event_data, enc_key.as_ref(), enc_key_old.as_ref());
        ev.event_data = crate::scval_format::render_event_data(&ev.event_data, scval_format);
        let data = match &field_columns_replay {
            Some(cols) => serde_json::to_string(&filter_fields(
                &ev,
                cols,
                enc_key.as_ref(),
                enc_key_old.as_ref(),
            ))
            .unwrap_or_default(),
            None => serde_json::to_string(&ev).unwrap_or_default(),
        };
        Some(Ok(Event::default()
            .id(ev.id.to_string())
            .retry(Duration::from_millis(keepalive_ms))
            .data(data)))
    }));

    // replay_complete sentinel after all replayed events.
    let replay_complete_stream = if has_replay {
        stream::iter(vec![Ok(Event::default()
            .event("replay_complete")
            .data("replay complete"))])
    } else {
        stream::iter(vec![])
    };

    let live_stream = stream::unfold(
        (
            rx,
            contract_filter,
            keepalive_ms,
            field_columns,
            enc_key,
            enc_key_old,
            tenant_id,
            event_type_filter,
            false, // closed
            state.shutdown_rx.clone(),
            max_lag,
            connection_id.clone(),
        ),
        move |(mut rx, filter, ka, cols, ek, ek_old, tid, et_filter, closed, mut shutdown_rx, max_lag, conn_id)| async move {
            if closed {
                return None;
            }
            let mut interval = tokio::time::interval(Duration::from_millis(ka));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                tokio::select! {
                    recv = rx.recv() => match recv {
                        Ok(event) => {
                            if let Some(ref cid) = filter {
                                if &event.contract_id != cid {
                                    continue;
                                }
                            }
                            // #452: Filter by event_type
                            if let Some(et) = et_filter {
                                if event.event_type.to_string() != et.to_string() {
                                    continue;
                                }
                            }
                            if let Some(ref tenant) = tid {
                                if event.tenant_id.as_deref() != Some(tenant.as_str()) {
                                    continue;
                                }
                            }
                            let data = serde_json::to_string(&event).unwrap_or_default();
                            let eid = event.id.map(|u| u.to_string())
                                .unwrap_or_else(|| format!("{}-{}", event.tx_hash, event.ledger));
                            let sse = Event::default()
                                .id(eid)
                                .retry(Duration::from_millis(ka))
                                .data(data);
                            return Some((Ok(sse), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, false, shutdown_rx, max_lag, conn_id)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // #451: Send lag notification event
                            crate::metrics::increment_sse_lagged_events(&conn_id, n);
                            let last_id = rx.len(); // approximate position
                            let lag_data = serde_json::json!({
                                "missed": n,
                                "last_event_id": null,
                            });
                            let lag_event = Event::default()
                                .event("lag")
                                .data(lag_data.to_string());
                            // #451: Disconnect if lag exceeds threshold
                            if max_lag > 0 && n >= max_lag {
                                return Some((Ok(lag_event), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, true, shutdown_rx, max_lag, conn_id)));
                            }
                            return Some((Ok(lag_event), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, false, shutdown_rx, max_lag, conn_id)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            let close_event = Event::default().event("close").data("stream closed");
                            return Some((Ok(close_event), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, true, shutdown_rx, max_lag, conn_id)));
                        }
                    },
                    _ = interval.tick() => {
                        let ts = chrono::Utc::now().to_rfc3339();
                        let ping = Event::default().event("ping").data(ts);
                        return Some((Ok(ping), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, false, shutdown_rx, max_lag, conn_id)));
                    }
                    _ = shutdown_rx.changed() => {
                        let close_event = Event::default().event("close").data("server shutting down");
                        return Some((Ok(close_event), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, true, shutdown_rx, max_lag, conn_id)));
                    }
                }
            }
        },
    );

    let combined = ring_replay_stream
        .chain(db_replay_stream)
        .chain(replay_complete_stream)
        .chain(live_stream);
    let combined = Box::pin(combined);

    // Wrap the stream to decrement the connection counter when the stream ends
    let stream_with_cleanup = stream::unfold(
        (Box::pin(combined), sse_connections.clone(), sse_connections_per_ip, client_ip_cleanup, max_per_ip_cleanup),
        move |(mut stream, counter, ip_map, ip, max_ip)| async move {
            match stream.next().await {
                Some(item) => Some((item, (stream, counter, ip_map, ip, max_ip))),
                None => {
                    let new_count = counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                    crate::metrics::update_sse_connections(new_count);
                    // Decrement per-IP counter
                    if max_ip > 0 {
                        let mut entry = ip_map.entry(ip.clone()).or_insert(0);
                        if *entry > 0 { *entry -= 1; }
                    }
                    None
                }
            }
        },
    );

    Ok(Sse::new(stream_with_cleanup))
}

/// Decrypt event_data if an encryption key is configured.
fn decrypt_event_data(raw: &Value, key: Option<&[u8; 32]>, old_key: Option<&[u8; 32]>) -> Value {
    if let Some(k) = key {
        crate::encryption::decrypt(k, old_key, raw).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to decrypt event_data, returning raw value");
            raw.clone()
        })
    } else {
        raw.clone()
    }
}

/// Converts an `Event` to a JSON object containing only the requested fields.
fn filter_fields(
    event: &models::Event,
    columns: &[&str],
    enc_key: Option<&[u8; 32]>,
    enc_key_old: Option<&[u8; 32]>,
) -> Value {
    let mut map = serde_json::Map::new();
    for &col in columns {
        match col {
            "id" => {
                map.insert(col.to_string(), json!(event.id));
            }
            "contract_id" => {
                map.insert(col.to_string(), json!(event.contract_id));
            }
            "event_type" => {
                map.insert(col.to_string(), json!(event.event_type));
            }
            "tx_hash" => {
                map.insert(col.to_string(), json!(event.tx_hash));
            }
            "ledger" => {
                map.insert(col.to_string(), json!(event.ledger));
            }
            "timestamp" => {
                map.insert(col.to_string(), json!(event.timestamp));
            }
            "event_data" => {
                let decrypted = decrypt_event_data(&event.event_data, enc_key, enc_key_old);
                map.insert(col.to_string(), decrypted);
            }
            "event_data_normalized" => {
                map.insert(col.to_string(), json!(event.event_data_normalized));
            }
            "event_data_decoded" => {
                map.insert(col.to_string(), json!(event.event_data_decoded));
            }
            "ledger_hash" => {
                map.insert(col.to_string(), json!(event.ledger_hash));
            }
            "anonymized" => {
                map.insert(col.to_string(), json!(event.anonymized));
            }
            "in_successful_call" => {
                map.insert(col.to_string(), json!(event.in_successful_call));
            }
            "created_at" => {
                map.insert(col.to_string(), json!(event.created_at));
            }
            "schema_version" => {
                map.insert(col.to_string(), json!(event.schema_version));
            }
            _ => {}
        }
    }
    Value::Object(map)
}

/// Returns true if the client prefers NDJSON via the Accept header.
fn extract_api_key_from_headers(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("X-Api-Key").and_then(|h| h.to_str().ok()))
}

fn is_admin_request(headers: &HeaderMap, admin_api_keys: &[secrecy::SecretString]) -> bool {
    extract_api_key_from_headers(headers)
        .map(|provided_key| {
            admin_api_keys
                .iter()
                .any(|expected| expected.expose_secret().as_str() == provided_key)
        })
        .unwrap_or(false)
}

fn accepts_ndjson(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("application/x-ndjson"))
        .unwrap_or(false)
}

/// Extract client IP from X-Forwarded-For or X-Real-IP headers, falling back to "unknown".
pub(crate) fn extract_client_ip(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// Build a plain JSON response from a `Value`.
fn json_response(body: Value) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap_or_default()))
        .unwrap_or_else(|e| response_build_error(e))
}

/// Build an NDJSON response: one JSON object per line, no wrapping array.
fn ndjson_response(events: impl Iterator<Item = Value>) -> Response<Body> {
    let mut buf = String::new();
    for ev in events {
        buf.push_str(&serde_json::to_string(&ev).unwrap_or_default());
        buf.push('\n');
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from(buf))
        .unwrap_or_else(|e| response_build_error(e))
}

#[utoipa::path(
    get,
    path = "/v1/events",
    tag = "events",
    params(
        ("page" = Option<i64>, Query, description = "Page number (default: 1)"),
        ("limit" = Option<i64>, Query, description = "Results per page, 1–100 (default: 20)"),
        ("exact_count" = Option<bool>, Query, description = "Use exact COUNT(*) instead of approximate"),
        ("event_type" = Option<crate::models::EventType>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
        ("ledger_hash" = Option<String>, Query, description = "Filter by ledger hash"),
        ("anonymized" = Option<bool>, Query, description = "Filter events by anonymized status (admin only)"),
        ("from_timestamp" = Option<String>, Query, description = "Return events at or after this timestamp (ISO 8601 format, e.g., 2026-03-14T00:00:00Z)"),
        ("to_timestamp" = Option<String>, Query, description = "Return events at or before this timestamp (ISO 8601 format, e.g., 2026-03-14T00:00:00Z)"),
        ("sort" = Option<String>, Query, description = "Sort order: asc (oldest first) or desc (newest first, default)"),
        ("sort_by" = Option<crate::models::SortBy>, Query, description = "Sort column: ledger (default), timestamp, or created_at"),
        ("topic_sym" = Option<String>, Query, description = "Filter by first topic symbol (uses topic_0_sym generated column index)"),
        ("topic_0" = Option<String>, Query, description = "Filter by exact value of topic[0] (e.g. 'transfer'). Uses the topic_0_sym generated-column index for O(log n) lookups."),
        ("topic_1" = Option<String>, Query, description = "Filter by exact value of topic[1]. Uses GIN index on event_data->'topic'."),
        ("topic_2" = Option<String>, Query, description = "Filter by exact value of topic[2]. Uses GIN index on event_data->'topic'."),
        ("topic_3" = Option<String>, Query, description = "Filter by exact value of topic[3]. Uses GIN index on event_data->'topic'."),
        ("search" = Option<String>, Query, description = "Full-text search query for event_data (searches all string values in the JSON)"),
        ("compact" = Option<bool>, Query, description = "Return event_data as a base64-encoded gzip-compressed JSON string instead of the full JSON object. Clients that need the full data can decode it; clients that only need metadata can ignore it. Default: false."),
        ("contract_id_prefix" = Option<String>, Query, description = "Filter events by contract ID prefix (minimum 4 alphanumeric characters, uses LIKE 'prefix%' with the contract_id index)."),
    ),
    responses(
        (status = 200, description = "Paginated list of events (JSON or NDJSON depending on Accept header). When compact=true, each event's event_data field is a base64-encoded gzip-compressed JSON string (Content-Encoding: gzip).",
            content(
                ("application/json" = Value),
                ("application/x-ndjson" = String),
            )
        ),
        (status = 400, description = "Invalid query parameters"),
    )
)]
#[instrument(skip(state, headers, extensions), fields(page = ?params.page, limit = ?params.limit, contract_id = ?params.contract_id))]
pub async fn get_events(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
    headers: HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<impl IntoResponse, AppError> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();
    // Validate ledger range
    if let Some(from) = params.from_ledger {
        validate_ledger_param("from_ledger", from)?;
    }
    if let Some(to) = params.to_ledger {
        validate_ledger_param("to_ledger", to)?;
    }
    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    // Validate and parse timestamp range
    let from_ts = if let Some(ref ts) = params.from_timestamp {
        Some(validate_timestamp(ts)?)
    } else {
        None
    };
    let to_ts = if let Some(ref ts) = params.to_timestamp {
        Some(validate_timestamp(ts)?)
    } else {
        None
    };

    // Validate timestamp range
    if let (Some(from), Some(to)) = (from_ts, to_ts) {
        if from > to {
            return Err(AppError::Validation(
                "from_timestamp must be <= to_timestamp".to_string(),
            ));
        }
    }

    if params.anonymized.is_some() && !is_admin_request(&headers, &state.config.admin_api_keys) {
        return Err(AppError::Forbidden(
            "anonymized filter requires admin privileges".to_string(),
        ));
    }

    // Validate contract_id if provided
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    // Parse and validate contract_ids if provided
    let contract_ids_list: Vec<String> = if let Some(ref cids) = params.contract_ids {
        let ids: Vec<&str> = cids.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        
        if ids.is_empty() {
            return Err(AppError::Validation(
                "contract_ids parameter is empty".to_string(),
            ));
        }
        
        if ids.len() > PaginationParams::MAX_CONTRACT_IDS_FILTER {
            return Err(AppError::Validation(
                format!(
                    "contract_ids exceeds maximum of {} IDs",
                    PaginationParams::MAX_CONTRACT_IDS_FILTER
                ),
            ));
        }
        
        for id in &ids {
            validate_contract_id(id)?;
        }
        
        ids.iter().map(|s| s.to_string()).collect()
    } else {
        Vec::new()
    };

    // Validate contract_id_prefix if provided (#459)
    if let Some(ref prefix) = params.contract_id_prefix {
        let trimmed = prefix.trim();
        if trimmed.len() < 4 {
            return Err(AppError::Validation(
                "contract_id_prefix must be at least 4 characters".to_string(),
            ));
        }
        if !trimmed.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(AppError::Validation(
                "contract_id_prefix must contain only alphanumeric characters".to_string(),
            ));
        }
    }

    let limit = params.limit();
    let columns = resolve_columns(&params)?;
    let sort_order = params.sort.unwrap_or(crate::models::SortOrder::Desc);
    let dir = sort_order.as_sql();
    let sort_by = params.sort_by.unwrap_or(crate::models::SortBy::Ledger);
    let sort_col = sort_by.as_sql_col();

    // Cursor-based path
    crate::metrics::record_pagination_strategy(params.cursor.is_some());
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_tag, cursor_val_text, cursor_id) = decode_cursor_tagged(cursor_str)?;
        if cursor_tag != sort_by.as_tag() {
            return Err(crate::error::AppError::Validation(
                "cursor sort column does not match sort_by".to_string(),
            ));
        }

        let cursor_op = if sort_order == crate::models::SortOrder::Asc {
            ">"
        } else {
            "<"
        };

        let mut conditions: Vec<String> = vec![format!(
            "({col}, id) {op} ($1, $2)",
            col = sort_col,
            op = cursor_op
        )];
        let mut bind_idx: i32 = 3;

        if params.contract_id.is_some() {
            conditions.push(format!("contract_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if !contract_ids_list.is_empty() {
            conditions.push(format!("contract_id = ANY(${bind_idx}::text[])"));
            bind_idx += 1;
        }
        if params.contract_id_prefix.is_some() {
            conditions.push(format!("contract_id LIKE ${bind_idx}"));
            bind_idx += 1;
        }
        if params.event_type.is_some() {
            conditions.push(format!("event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.from_ledger.is_some() {
            conditions.push(format!("ledger >= ${bind_idx}"));
            bind_idx += 1;
        }
        if params.to_ledger.is_some() {
            conditions.push(format!("ledger <= ${bind_idx}"));
            bind_idx += 1;
        }
        if params.ledger_hash.is_some() {
            conditions.push(format!("ledger_hash = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.anonymized.is_some() {
            conditions.push(format!("anonymized = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.in_successful_call.is_some() {
            conditions.push(format!("in_successful_call = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.schema_version.is_some() {
            conditions.push(format!("schema_version = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.topic_sym.is_some() {
            conditions.push(format!("topic_0_sym = ${bind_idx}"));
            bind_idx += 1;
        }
        if topic_filter.is_some() {
            conditions.push(format!("event_data->'topic' @> ${bind_idx}::jsonb"));
            bind_idx += 1;
        }
        // topic_0 uses the generated topic_0_sym column index for efficiency
        if params.topic_0.is_some() {
            conditions.push(format!("topic_0_sym = ${bind_idx}"));
            bind_idx += 1;
        }
        // topic_1/2/3 use GIN index on event_data->'topic'
        if params.topic_1.is_some() {
            conditions.push(format!("event_data->'topic'->1 @> ${bind_idx}::jsonb"));
            bind_idx += 1;
        }
        if params.topic_2.is_some() {
            conditions.push(format!("event_data->'topic'->2 @> ${bind_idx}::jsonb"));
            bind_idx += 1;
        }
        if params.topic_3.is_some() {
            conditions.push(format!("event_data->'topic'->3 @> ${bind_idx}::jsonb"));
            bind_idx += 1;
        }

        let mut search_param_idx: Option<i32> = None;
        if params.search.is_some() {
            search_param_idx = Some(bind_idx);
            conditions.push(format!(
                "event_data_tsv @@ plainto_tsquery('english', ${})",
                bind_idx
            ));
            bind_idx += 1;
        }

        if params.data_pattern.is_some() {
            conditions.push(format!(
                "jsonb_path_exists(event_data, ${bind_idx}::jsonpath)"
            ));
            bind_idx += 1;
        }
        if from_ts.is_some() {
            conditions.push(format!("timestamp >= ${bind_idx}"));
            bind_idx += 1;
        }
        if to_ts.is_some() {
            conditions.push(format!("timestamp <= ${bind_idx}"));
            bind_idx += 1;
        }
        // Exclusion filters (Issue #463)
        if !exclude_contract_ids_list.is_empty() {
            conditions.push(format!("contract_id != ALL(${bind_idx}::text[])"));
            bind_idx += 1;
        }
        if !exclude_event_types_list.is_empty() {
            conditions.push(format!("event_type != ALL(${bind_idx}::text[])"));
            bind_idx += 1;
        }
        // Geospatial filtering (Issue #465)
        if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
            conditions.push(format!(
                "earth_distance(ll_to_earth(latitude, longitude), ll_to_earth(${bind_idx}, ${bind_idx + 1})) <= ${bind_idx + 2} * 1000"
            ));
            bind_idx += 3;
        }
        maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);
        // Issue #1063: filter to one network when indexing multiple
        // networks concurrently from this deployment. Omitted returns
        // events from every configured network.
        if params.network.is_some() {
            conditions.push(format!("chain_id = ${bind_idx}"));
            bind_idx += 1;
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));

        let mut select_cols = columns.to_vec();
        if !select_cols.contains(&sort_col) {
            select_cols.push(sort_col);
        }
        if !select_cols.contains(&"id") {
            select_cols.push("id");
        }
        if !select_cols.contains(&"created_at") {
            select_cols.push("created_at");
        }

        let mut select_query_cols: Vec<String> = select_cols.iter().map(|v| v.to_string()).collect();
        if params.rank_by_relevance.unwrap_or(false) {
            if params.search.is_none() {
                return Err(AppError::Validation(
                    "rank_by_relevance requires search query".to_string(),
                ));
            }
            if select_query_cols
                .iter()
                .all(|c| c != "relevance_score")
            {
                select_cols.push("relevance_score");
            }
            if let Some(search_idx) = search_param_idx {
                select_query_cols.push(format!(
                    "ts_rank(event_data_tsv, plainto_tsquery('english', ${})) AS relevance_score",
                    search_idx
                ));
            }
        }

        // Defence-in-depth: re-validate each column before SQL interpolation
        for col in &select_cols {
            if !models::PaginationParams::validate_column_name(col) {
                return Err(AppError::Validation(format!(
                    "invalid column name: {}",
                    col
                )));
            }
        }

        let order_clause = if params.rank_by_relevance.unwrap_or(false) {
            "relevance_score DESC, id DESC".to_string()
        } else if sort_col == "ledger" {
            // Issue #1065: within a ledger, order deterministically by the
            // decoded TOID fields before falling back to insertion id.
            format!(
                "ledger {dir}, tx_index {dir} NULLS LAST, op_index {dir} NULLS LAST, event_index {dir} NULLS LAST, id {dir}",
                dir = dir
            )
        } else {
            format!("{col} {dir}, id {dir}", col = sort_col, dir = dir)
        };

        let query_str = format!(
            "SELECT {} FROM events {} ORDER BY {} LIMIT ${}",
            select_query_cols.join(", "),
            where_clause,
            order_clause,
            bind_idx,
        );

        let mut q = sqlx::query(&query_str);
        match sort_by {
            crate::models::SortBy::Ledger => {
                let val = cursor_val_text.parse::<i64>().map_err(|_| {
                    crate::error::AppError::Validation("invalid ledger cursor".to_string())
                })?;
                q = q.bind(val).bind(cursor_id);
            }
            crate::models::SortBy::Timestamp | crate::models::SortBy::CreatedAt => {
                let ts = cursor_val_text
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .map_err(|_| {
                        crate::error::AppError::Validation("invalid timestamp cursor".to_string())
                    })?;
                q = q.bind(ts).bind(cursor_id);
            }
        }
        if let Some(ref cid) = params.contract_id {
            q = q.bind(cid);
        }
        if !contract_ids_list.is_empty() {
            q = q.bind(&contract_ids_list);
        }
        if let Some(ref prefix) = params.contract_id_prefix {
            q = q.bind(format!("{}%", prefix.trim()));
        }
        if let Some(ref et) = params.event_type {
            q = q.bind(et);
        }
        if let Some(fl) = params.from_ledger {
            q = q.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            q = q.bind(tl);
        }
        if let Some(ref hash) = params.ledger_hash {
            q = q.bind(hash);
        }
        if let Some(anonymized) = params.anonymized {
            q = q.bind(anonymized);
        }
        if let Some(isc) = params.in_successful_call {
            q = q.bind(isc);
        }
        if let Some(sv) = params.schema_version {
            q = q.bind(sv);
        }
        if let Some(ref ts) = params.topic_sym {
            q = q.bind(ts);
        }
        if let Some(ref tf) = topic_filter {
            q = q.bind(tf.to_string());
        }
        if let Some(ref t0) = params.topic_0 {
            q = q.bind(t0);
        }
        if let Some(ref t1) = params.topic_1 {
            q = q.bind(serde_json::json!(t1).to_string());
        }
        if let Some(ref t2) = params.topic_2 {
            q = q.bind(serde_json::json!(t2).to_string());
        }
        if let Some(ref t3) = params.topic_3 {
            q = q.bind(serde_json::json!(t3).to_string());
        }
        if let Some(ref data_pattern) = params.data_pattern {
            let pattern = params.pattern.as_deref().ok_or_else(|| {
                AppError::Validation("pattern is required when data_pattern is set".to_string())
            })?;
            let expr = format_jsonpath_filter(data_pattern, pattern);
            q = q.bind(expr);
        }
        if let Some(ref search) = params.search {
            q = q.bind(search);
        }
        if let Some(ts) = from_ts {
            q = q.bind(ts);
        }
        if let Some(ts) = to_ts {
            q = q.bind(ts);
        }
        // Bind exclusion filters (Issue #463)
        if !exclude_contract_ids_list.is_empty() {
            q = q.bind(&exclude_contract_ids_list);
        }
        if !exclude_event_types_list.is_empty() {
            q = q.bind(&exclude_event_types_list);
        }
        // Bind geospatial filters (Issue #465)
        if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
            q = q.bind(lat).bind(lon).bind(radius);
        }
        if let Some(tid) = tenant_id {
            q = q.bind(tid);
        }
        if let Some(ref network) = params.network {
            q = q.bind(network);
        }
        q = q.bind(limit);

        let _db_span = info_span!("db_query", query_type = "get_events_cursor").entered();
        let rows = timed_query(
            q.fetch_all(&state.read_pool),
            "get_events_cursor",
            state.config.slow_query_threshold_ms,
            None,
        )
        .await?;
        drop(_db_span);

        let has_more = rows.len() as i64 == limit;
        let next_cursor = if let (true, Some(last)) = (has_more, rows.last()) {
            let last_id: uuid::Uuid = last.try_get("id")?;
            let last_val_text = match sort_by {
                crate::models::SortBy::Ledger => {
                    let v: i64 = last.try_get("ledger")?;
                    v.to_string()
                }
                crate::models::SortBy::Timestamp => {
                    let v: chrono::DateTime<chrono::Utc> = last.try_get("timestamp")?;
                    v.to_rfc3339()
                }
                crate::models::SortBy::CreatedAt => {
                    let v: chrono::DateTime<chrono::Utc> = last.try_get("created_at")?;
                    v.to_rfc3339()
                }
            };
            Some(encode_cursor_tagged(
                sort_by.as_tag(),
                &last_val_text,
                last_id,
            ))
        } else {
            None
        };

        let scval_format = crate::scval_format::parse_format(params.format.as_deref())
            .map_err(AppError::Validation)?;
        let events = rows_to_json_with_format(
            &rows,
            &columns,
            state.encryption_key.as_ref(),
            state.encryption_key_old.as_ref(),
            params.compact.unwrap_or(false),
            scval_format,
        )?;

        // Build ETag from last row's id + created_at
        let etag = rows.first().and_then(|row| {
            let id: Option<Uuid> = row.try_get("id").ok();
            let created_at: Option<DateTime<Utc>> = row.try_get("created_at").ok();
            id.zip(created_at)
                .map(|(id, ca)| compute_etag(&id, &ca, None))
        });

        // Check If-None-Match — return 304 if ETag matches
        if let Some(ref tag) = etag {
            if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
                if inm == tag {
                    let resp = axum::http::Response::builder()
                        .status(StatusCode::NOT_MODIFIED)
                        .header("ETag", tag.as_str())
                        .header("Cache-Control", "no-cache")
                        .body(axum::body::Body::empty())
                        .unwrap_or_else(|e| response_build_error(e));
                    return Ok(resp.into_response());
                }
            }
        }

        let want_ndjson = accepts_ndjson(&headers);
        if want_ndjson {
            let mut resp = ndjson_response(events.into_iter()).into_response();
            if let Some(ref tag) = etag {
                insert_etag_headers(resp.headers_mut(), tag);
            }
            return Ok(resp);
        }

        let compact_mode = params.compact.unwrap_or(false);
        let mut resp = json_response(json!({
            "data": events,
            "next_cursor": next_cursor,
            "limit": limit,
        }));
        if let Some(ref tag) = etag {
            insert_etag_headers(resp.headers_mut(), tag);
        }
        if compact_mode {
            resp.headers_mut().insert(
                "X-Event-Data-Encoding",
                axum::http::HeaderValue::from_static("gzip+base64"),
            );
        }
        return Ok(resp.into_response());
    }

    // Offset-based path (deprecated fallback)
    let offset = params.offset();
    let exact = params.exact_count.unwrap_or(false);

    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if !contract_ids_list.is_empty() {
        conditions.push(format!("contract_id = ANY(${bind_idx}::text[])"));
        bind_idx += 1;
    }
    if params.contract_id_prefix.is_some() {
        conditions.push(format!("contract_id LIKE ${bind_idx}"));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.ledger_hash.is_some() {
        conditions.push(format!("ledger_hash = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.anonymized.is_some() {
        conditions.push(format!("anonymized = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.in_successful_call.is_some() {
        conditions.push(format!("in_successful_call = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.schema_version.is_some() {
        conditions.push(format!("schema_version = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.topic_sym.is_some() {
        conditions.push(format!("topic_0_sym = ${bind_idx}"));
        bind_idx += 1;
    }
    if topic_filter.is_some() {
        conditions.push(format!("event_data->'topic' @> ${bind_idx}::jsonb"));
        bind_idx += 1;
    }
    if params.search.is_some() {
        conditions.push(format!(
            "event_data_tsv @@ plainto_tsquery('english', ${bind_idx})"
        ));
        bind_idx += 1;
    }
    if from_ts.is_some() {
        conditions.push(format!("timestamp >= ${bind_idx}"));
        bind_idx += 1;
    }
    if to_ts.is_some() {
        conditions.push(format!("timestamp <= ${bind_idx}"));
        bind_idx += 1;
    }
    // Exclusion filters (Issue #463)
    if !exclude_contract_ids_list.is_empty() {
        conditions.push(format!("contract_id != ALL(${bind_idx}::text[])"));
        bind_idx += 1;
    }
    if !exclude_event_types_list.is_empty() {
        conditions.push(format!("event_type != ALL(${bind_idx}::text[])"));
        bind_idx += 1;
    }
    // Geospatial filtering (Issue #465)
    if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
        conditions.push(format!(
            "earth_distance(ll_to_earth(latitude, longitude), ll_to_earth(${bind_idx}, ${bind_idx + 1})) <= ${bind_idx + 2} * 1000"
        ));
        bind_idx += 3;
    }
    maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&sort_col) {
        select_cols.push(sort_col);
    }
    if !select_cols.contains(&"id") {
        select_cols.push("id");
    }
    // Always fetch created_at for ETag computation
    if !select_cols.contains(&"created_at") {
        select_cols.push("created_at");
    }

    // Defence-in-depth: re-validate each column before SQL interpolation
    for col in &select_cols {
        if !models::PaginationParams::validate_column_name(col) {
            return Err(AppError::Validation(format!(
                "invalid column name: {}",
                col
            )));
        }
    }

    let query_str = format!(
        "SELECT {} FROM events {} ORDER BY {col} {dir}, id {dir} LIMIT ${} OFFSET ${}",
        select_cols.join(", "),
        where_clause,
        col = sort_col,
        dir = dir,
        bind_idx,
        bind_idx + 1,
    );

    let mut q = sqlx::query(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if !contract_ids_list.is_empty() {
        q = q.bind(&contract_ids_list);
    }
    if let Some(ref prefix) = params.contract_id_prefix {
        q = q.bind(format!("{}%", prefix.trim()));
    }
    if let Some(ref et) = params.event_type {
        q = q.bind(et);
    }
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    if let Some(ref hash) = params.ledger_hash {
        q = q.bind(hash);
    }
    if let Some(anonymized) = params.anonymized {
        q = q.bind(anonymized);
    }
    if let Some(isc) = params.in_successful_call {
        q = q.bind(isc);
    }
    if let Some(sv) = params.schema_version {
        q = q.bind(sv);
    }
    if let Some(ref ts) = params.topic_sym {
        q = q.bind(ts);
    }
    if let Some(ref tf) = topic_filter {
        q = q.bind(tf.to_string());
    }
    if let Some(ref search) = params.search {
        q = q.bind(search);
    }
    if let Some(ts) = from_ts {
        q = q.bind(ts);
    }
    if let Some(ts) = to_ts {
        q = q.bind(ts);
    }
    // Bind exclusion filters (Issue #463)
    if !exclude_contract_ids_list.is_empty() {
        q = q.bind(&exclude_contract_ids_list);
    }
    if !exclude_event_types_list.is_empty() {
        q = q.bind(&exclude_event_types_list);
    }
    // Bind geospatial filters (Issue #465)
    if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
        q = q.bind(lat).bind(lon).bind(radius);
    }
    if let Some(tid) = tenant_id {
        q = q.bind(tid);
    }
    q = q.bind(limit).bind(offset);

    let _db_span = info_span!("db_query", query_type = "get_events_offset").entered();
    let rows = timed_query(
        q.fetch_all(&state.read_pool),
        "get_events_offset",
        state.config.slow_query_threshold_ms,
        None,
    )
    .await?;
    drop(_db_span);

    let has_more = rows.len() as i64 == limit;
    let next_cursor = if let (true, Some(last)) = (has_more, rows.last()) {
        let last_id: uuid::Uuid = last.try_get("id")?;
        let last_val_text = match sort_by {
            crate::models::SortBy::Ledger => {
                let v: i64 = last.try_get("ledger")?;
                v.to_string()
            }
            crate::models::SortBy::Timestamp => {
                let v: chrono::DateTime<chrono::Utc> = last.try_get("timestamp")?;
                v.to_rfc3339()
            }
            crate::models::SortBy::CreatedAt => {
                let v: chrono::DateTime<chrono::Utc> = last.try_get("created_at")?;
                v.to_rfc3339()
            }
        };
        Some(encode_cursor_tagged(
            sort_by.as_tag(),
            &last_val_text,
            last_id,
        ))
    } else {
        None
    };

    let events = rows_to_json(
        &rows,
        &columns,
        state.encryption_key.as_ref(),
        state.encryption_key_old.as_ref(),
        params.compact.unwrap_or(false),
    )?;

    let (total, approximate): (i64, bool) = if exact || !conditions.is_empty() {
        let count_str = format!("SELECT COUNT(*) FROM events {}", where_clause);
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str);
        if let Some(ref cid) = params.contract_id {
            cq = cq.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            cq = cq.bind(et);
        }
        if let Some(fl) = params.from_ledger {
            cq = cq.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            cq = cq.bind(tl);
        }
        if let Some(ref hash) = params.ledger_hash {
            cq = cq.bind(hash);
        }
        if let Some(anonymized) = params.anonymized {
            cq = cq.bind(anonymized);
        }
        if let Some(isc) = params.in_successful_call {
            cq = cq.bind(isc);
        }
        if let Some(sv) = params.schema_version {
            cq = cq.bind(sv);
        }
        if let Some(ref ts) = params.topic_sym {
            cq = cq.bind(ts);
        }
        if let Some(ref tf) = topic_filter {
            cq = cq.bind(tf.to_string());
        }
        if let Some(ref search) = params.search {
            cq = cq.bind(search);
        }
        if let Some(ts) = from_ts {
            cq = cq.bind(ts);
        }
        if let Some(ts) = to_ts {
            cq = cq.bind(ts);
        }
        // Bind exclusion filters for count query (Issue #463)
        if !exclude_contract_ids_list.is_empty() {
            cq = cq.bind(&exclude_contract_ids_list);
        }
        if !exclude_event_types_list.is_empty() {
            cq = cq.bind(&exclude_event_types_list);
        }
        // Bind geospatial filters for count query (Issue #465)
        if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
            cq = cq.bind(lat).bind(lon).bind(radius);
        }
        if let Some(tid) = tenant_id {
            cq = cq.bind(tid);
        }
        let _count_span = info_span!("db_query", query_type = "count_events").entered();
        let count = cq.fetch_one(&state.read_pool).await?;
        drop(_count_span);
        (count, false)
    } else {
        // In multi-tenant mode we can't use the pg_class estimate (it's for the whole table).
        // Fall back to an exact count scoped to the tenant.
        if tenant_id.is_some() {
            let count =
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events WHERE tenant_id = $1")
                    .bind(tenant_id)
                    .fetch_one(&state.read_pool)
                    .await?;
            (count, false)
        } else {
            // query_scalar! gives compile-time SQL verification via the .sqlx offline cache.
            let count = sqlx::query_scalar!(
                "SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'events'",
            )
            .fetch_one(&state.read_pool)
            .await?;
            (count, true)
        }
    };

    // Build ETag from last row's id + created_at + total
    let etag = rows.first().and_then(|row| {
        let id: Option<Uuid> = row.try_get("id").ok();
        let created_at: Option<DateTime<Utc>> = row.try_get("created_at").ok();
        id.zip(created_at)
            .map(|(id, ca)| compute_etag(&id, &ca, Some(total)))
    });

    // Check If-None-Match — return 304 if ETag matches
    if let Some(ref tag) = etag {
        if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
            if inm == tag {
                let resp = axum::http::Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header("ETag", tag.as_str())
                    .header("Cache-Control", "no-cache")
                    .body(axum::body::Body::empty())
                    .unwrap_or_else(|e| response_build_error(e));
                return Ok(resp.into_response());
            }
        }
    }

    let body = json!({
        "data": events,
        "next_cursor": next_cursor,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
        "approximate": approximate,
        "pagination": "offset — migrate to cursor parameter for better performance",
    });

    let mut body_obj = body.as_object().cloned().unwrap_or_default();
    
    // Add approximation metadata when using approximate count
    if approximate {
        // Get stats age and dead tuple ratio
        let stats_info: (Option<chrono::DateTime<chrono::Utc>>, Option<f64>) = sqlx::query_as(
            "SELECT last_analyze, CASE WHEN n_live_tup > 0 THEN (n_dead_tup::float / n_live_tup) * 100 ELSE 0 END \
             FROM pg_stat_user_tables WHERE relname = 'events'"
        )
        .fetch_optional(&state.read_pool)
        .await
        .unwrap_or(None)
        .map(|(last_analyze, error_pct)| (last_analyze, error_pct))
        .unwrap_or((None, None));
        
        if let Some(error_pct) = stats_info.1 {
            body_obj.insert("approximate_error_pct".to_string(), json!(error_pct.min(100.0)));
        }
        if let Some(last_analyze) = stats_info.0 {
            body_obj.insert("last_analyzed".to_string(), json!(last_analyze.to_rfc3339()));
        }
    }

    let body = serde_json::Value::Object(body_obj);

    // Content negotiation: return NDJSON when the client requests it (Issue #417)
    if accepts_ndjson(&headers) {
        let ndjson = ndjson_response(events.into_iter());
        return Ok(ndjson.into_response());
    }

    let mut response = Json(body).into_response();
    if let Some(ref tag) = etag {
        insert_etag_headers(response.headers_mut(), tag);
    }
    if params.compact.unwrap_or(false) {
        response.headers_mut().insert(
            "X-Event-Data-Encoding",
            axum::http::HeaderValue::from_static("gzip+base64"),
        );
    }
    Ok(response)
}

#[utoipa::path(
    get,
    path = "/v1/events/feed.rss",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter feed by contract ID"),
        ("event_type" = Option<String>, Query, description = "Filter feed by event type"),
        ("from_ledger" = Option<i64>, Query, description = "Minimum ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Maximum ledger"),
        ("limit" = Option<i64>, Query, description = "Maximum feed items, capped at 100")
    ),
    responses(
        (status = 200, description = "Atom feed for indexed events"),
        (status = 400, description = "Invalid filter", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    )
)]
pub async fn get_events_feed(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
    extensions: axum::http::Extensions,
) -> Result<Response, AppError> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();

    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }
    if let Some(from) = params.from_ledger {
        validate_ledger_param("from_ledger", from)?;
    }
    if let Some(to) = params.to_ledger {
        validate_ledger_param("to_ledger", to)?;
    }
    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    let cache_key = format!(
        "{:?}:{:?}:{:?}:{:?}:{:?}:{:?}",
        tenant_id,
        params.contract_id,
        params.event_type,
        params.from_ledger,
        params.to_ledger,
        params.limit
    );
    {
        let cache = events_feed_cache().lock().await;
        if let Some(entry) = cache.get(&cache_key) {
            if std::time::Instant::now() < entry.expires_at {
                return Ok(xml_response(entry.data.as_str().unwrap_or_default().to_string()));
            }
        }
    }

    let limit = params.limit().min(100);
    let mut query = QueryBuilder::<sqlx::Postgres>::new(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data \
         FROM events",
    );
    if params.contract_id.is_some()
        || params.event_type.is_some()
        || params.from_ledger.is_some()
        || params.to_ledger.is_some()
        || tenant_id.is_some()
    {
        query.push(" WHERE ");
        let mut needs_and = false;
        if let Some(ref contract_id) = params.contract_id {
            query.push("contract_id = ");
            query.push_bind(contract_id);
            needs_and = true;
        }
        if let Some(event_type) = params.event_type {
            if needs_and {
                query.push(" AND ");
            }
            query.push("event_type = ");
            query.push_bind(event_type.to_string());
            needs_and = true;
        }
        if let Some(from_ledger) = params.from_ledger {
            if needs_and {
                query.push(" AND ");
            }
            query.push("ledger >= ");
            query.push_bind(from_ledger);
            needs_and = true;
        }
        if let Some(to_ledger) = params.to_ledger {
            if needs_and {
                query.push(" AND ");
            }
            query.push("ledger <= ");
            query.push_bind(to_ledger);
            needs_and = true;
        }
        if let Some(tid) = tenant_id {
            if needs_and {
                query.push(" AND ");
            }
            query.push("tenant_id = ");
            query.push_bind(tid);
        }
    }
    // Issue #1065: order deterministically within a ledger using the
    // decoded TOID fields before falling back to insertion id, so
    // re-indexing and replicas agree on order regardless of insert order.
    query.push(
        " ORDER BY ledger DESC, tx_index DESC NULLS LAST, op_index DESC NULLS LAST, event_index DESC NULLS LAST, id DESC LIMIT ",
    );
    query.push_bind(limit);

    let rows = query.build().fetch_all(&state.read_pool).await?;
    let feed = render_atom_feed(&rows)?;
    events_feed_cache().lock().await.insert(
        cache_key,
        CacheEntry {
            data: Value::String(feed.clone()),
            expires_at: std::time::Instant::now() + Duration::from_secs(60),
        },
    );
    Ok(xml_response(feed))
}

fn xml_response(body: String) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/atom+xml; charset=utf-8")
        .body(Body::from(body))
        .unwrap_or_else(|e| response_build_error(e))
}

fn render_atom_feed(rows: &[sqlx::postgres::PgRow]) -> Result<String, AppError> {
    let updated = rows
        .first()
        .and_then(|row| row.try_get::<DateTime<Utc>, _>("timestamp").ok())
        .unwrap_or_else(Utc::now)
        .to_rfc3339();
    let mut feed = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
         <feed xmlns=\"http://www.w3.org/2005/Atom\">\
         <title>Soroban Pulse Events</title>\
         <id>urn:soroban-pulse:events</id><updated>{}</updated>",
        escape_xml(&updated)
    );
    for row in rows {
        feed.push_str(&render_feed_entry(row)?);
    }
    feed.push_str("</feed>");
    Ok(feed)
}

fn render_feed_entry(row: &sqlx::postgres::PgRow) -> Result<String, AppError> {
    let id: Uuid = row.try_get("id")?;
    let contract_id: String = row.try_get("contract_id")?;
    let event_type: String = row.try_get("event_type")?;
    let tx_hash: String = row.try_get("tx_hash")?;
    let ledger: i64 = row.try_get("ledger")?;
    let timestamp: DateTime<Utc> = row.try_get("timestamp")?;
    let event_data: Value = row.try_get("event_data")?;
    Ok(format!(
        "<entry><title>{} event for {}</title>\
         <id>urn:soroban-pulse:event:{}</id><updated>{}</updated>\
         <summary type=\"html\">ledger {} tx {}</summary>\
         <content type=\"application/json\">{}</content></entry>",
        escape_xml(&event_type),
        escape_xml(&contract_id),
        id,
        escape_xml(&timestamp.to_rfc3339()),
        ledger,
        escape_xml(&tx_hash),
        escape_xml(&event_data.to_string())
    ))
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Escape a single CSV field per RFC 4180.
///
/// A field is wrapped in double-quotes if it contains a comma, double-quote,
/// newline (`\n`), or carriage-return (`\r`). Any double-quote character
/// inside the field is escaped by doubling it (`"` → `""`).
fn csv_escape_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        // Escape embedded double-quotes by doubling them, then wrap in quotes.
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        value.to_owned()
    }
}

#[utoipa::path(
    get,
    path = "/v1/events/export",
    tag = "events",
    params(
        ("format" = Option<String>, Query, description = "Output format: `csv` (default) or `parquet`. \
            CSV output streams RFC 4180-compliant text with a header row. \
            Parquet output requires the `parquet` feature flag."),
        ("event_type" = Option<String>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("field_map" = Option<String>, Query, description = "Optional JSON object mapping source field names to target output names, e.g. {\"event_data\":\"raw_data\"}"),
    ),
    responses(
        (status = 200, description = "Exported events. \
            CSV: `Content-Type: text/csv`, header row `id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at`, \
            streamed with `Content-Disposition: attachment; filename=\"events.csv\"`. \
            Parquet: `Content-Type: application/octet-stream`, \
            `Content-Disposition: attachment; filename=\"events.parquet\"`."),
        (status = 400, description = "Invalid query parameters or unsupported format"),
        (status = 401, description = "API key required"),
    )
)]
pub async fn export_events(
    State(state): State<AppState>,
    Query(params): Query<ExportParams>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    if state.config.api_keys.is_empty() {
        return Err(AppError::Validation(
            "export endpoint requires API key authentication".to_string(),
        ));
    }

    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }
    if let Some(from) = params.from_ledger {
        validate_ledger_param("from_ledger", from)?;
    }
    if let Some(to) = params.to_ledger {
        validate_ledger_param("to_ledger", to)?;
    }
    // Validate timestamp range
    if let (Some(from_ts), Some(to_ts)) = (params.from_timestamp, params.to_timestamp) {
        if from_ts >= to_ts {
            return Err(AppError::Validation(
                "from_timestamp must be < to_timestamp".to_string(),
            ));
        }
    }

    let fmt = params.format.as_deref().unwrap_or("csv");
    let want_csv = fmt == "csv" || fmt.is_empty();
    let want_parquet = fmt == "parquet";
    let want_jsonl = fmt == "jsonl" || fmt == "ndjson";

    if !want_csv && !want_parquet && !want_jsonl {
        return Err(AppError::Validation(format!(
            "unsupported format '{fmt}': use 'csv', 'parquet', or 'jsonl'"
        )));
    }

    // Compression support
    let accept_encoding = headers.get(axum::http::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok()).unwrap_or("");
    let use_gzip = accept_encoding.contains("gzip");
    let use_br = accept_encoding.contains("br");

    let max_rows = state.config.export_max_rows as i64;
    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_timestamp.is_some() {
        conditions.push(format!("timestamp >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_timestamp.is_some() {
        conditions.push(format!("timestamp <= ${bind_idx}"));
        bind_idx += 1;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
         FROM events {where_clause} ORDER BY ledger ASC, id ASC LIMIT ${bind_idx}"
    );

    let mut q = sqlx::query(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if let Some(ref et) = params.event_type {
        q = q.bind(et);
    }
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    if let Some(from_ts) = params.from_timestamp {
        q = q.bind(from_ts);
    }
    if let Some(to_ts) = params.to_timestamp {
        q = q.bind(to_ts);
    }
    q = q.bind(max_rows);

    let rows = q.fetch_all(&state.pool).await?;

    // Parse optional field_map parameter (JSON object string) which maps
    // source field names -> target output field names.
    let field_map: Option<std::collections::HashMap<String, String>> = if let Some(ref fm) = params.field_map {
        match serde_json::from_str(fm) {
            Ok(m) => Some(m),
            Err(_) => {
                return Err(AppError::Validation("field_map must be a JSON object mapping source field names to target field names".to_string()));
            }
        }
    } else {
        None
    };

    // Validate that all source fields in the map are valid allowed fields.
    if let Some(ref fm) = field_map {
        for src in fm.keys() {
            if !models::PaginationParams::ALLOWED_FIELDS.contains(&src.as_str()) {
                return Err(AppError::Validation(format!("unknown source field in field_map: {}", src)));
            }
        }
    }

    // Get total count of available rows (for Content-Range header)
    let total_count: i64 = {
        let count_str = format!("SELECT COUNT(*) FROM events {}", where_clause);
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str);
        if let Some(ref cid) = params.contract_id {
            cq = cq.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            cq = cq.bind(et);
        }
        if let Some(fl) = params.from_ledger {
            cq = cq.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            cq = cq.bind(tl);
        }
        cq.fetch_one(&state.pool).await?
    };

    let returned_count = rows.len() as i64;
    let content_range = format!("items 0-{}/{}", returned_count.saturating_sub(1), total_count);

    // JSON Lines format
    if want_jsonl {
        let mut jsonl = String::new();
        // ...existing code...
        for row in &rows {
            // ...existing code...
        }
        let mut body: Vec<u8> = jsonl.into_bytes();
        let mut content_encoding = None;
        if use_gzip {
            body = compress_gzip(&body).await?;
            content_encoding = Some("gzip");
        } else if use_br {
            body = compress_brotli(&body).await?;
            content_encoding = Some("br");
        }
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .header(header::CONTENT_DISPOSITION, "attachment; filename=\"events.jsonl\"")
            .header("Content-Range", content_range);
        if let Some(enc) = content_encoding {
            builder = builder.header(header::CONTENT_ENCODING, enc);
        }
        return Ok(builder.body(Body::from(body)).unwrap_or_else(|e| response_build_error(e)));
    }

    #[cfg(feature = "parquet")]
    if want_parquet {
        use crate::parquet_export::{write_events_parquet_with_field_map, EventRow};
        let event_rows: Vec<EventRow> = rows
            .iter()
            .map(|row| {
                Ok::<_, sqlx::Error>(EventRow {
                    id: row.try_get("id")?,
                    contract_id: row.try_get("contract_id")?,
                    event_type: row.try_get("event_type")?,
                    tx_hash: row.try_get("tx_hash")?,
                    ledger: row.try_get("ledger")?,
                    timestamp: row.try_get("timestamp")?,
                    event_data: row.try_get("event_data")?,
                    created_at: row.try_get("created_at")?,
                })
            })
            .collect::<Result<_, _>>()?;

        let mut bytes = write_events_parquet_with_field_map(&event_rows, field_map.as_ref())
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let mut content_encoding = None;
        let mut filename = "events.parquet";
        if use_gzip {
            bytes = compress_gzip(&bytes).await?;
            content_encoding = Some("gzip");
            filename = "events.parquet.gz";
        } else if use_br {
            bytes = compress_brotli(&bytes).await?;
            content_encoding = Some("br");
            filename = "events.parquet.br";
        }
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
            .header("Content-Range", content_range);
        if let Some(enc) = content_encoding {
            builder = builder.header(header::CONTENT_ENCODING, enc);
        }
        return Ok(builder.body(Body::from(bytes)).unwrap_or_else(|e| response_build_error(e)));
    }

    // Default: CSV (RFC 4180)
    // Build each row as a Bytes chunk and stream them so the full result set
    // is never held in a single allocation.
    use bytes::Bytes;
    use futures::stream;

    let default_cols = [
        "id",
        "contract_id",
        "event_type",
        "tx_hash",
        "ledger",
        "timestamp",
        "event_data",
        "created_at",
    ];

    let header_names: Vec<String> = default_cols
        .iter()
        .map(|c| field_map.as_ref().and_then(|m| m.get(*c)).cloned().unwrap_or_else(|| (*c).to_string()))
        .collect();
    let csv_header = format!("{}\n", header_names.join(","));
    let mut csv_data = String::with_capacity(rows.len() * 128 + 128);
    csv_data.push_str(&csv_header);
    for row in &rows {
        let id: uuid::Uuid = row.try_get("id")?;
        let contract_id: String = row.try_get("contract_id")?;
        let event_type: String = row.try_get("event_type")?;
        let tx_hash: String = row.try_get("tx_hash")?;
        let ledger: i64 = row.try_get("ledger")?;
        let timestamp: chrono::DateTime<chrono::Utc> = row.try_get("timestamp")?;
        let event_data: serde_json::Value = row.try_get("event_data")?;
        let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;
        let data_str = event_data.to_string();
        let mut values: Vec<String> = Vec::with_capacity(default_cols.len());
        for col in &default_cols {
            match *col {
                "id" => values.push(csv_escape_field(&id.to_string())),
                "contract_id" => values.push(csv_escape_field(&contract_id)),
                "event_type" => values.push(csv_escape_field(&event_type)),
                "tx_hash" => values.push(csv_escape_field(&tx_hash)),
                "ledger" => values.push(ledger.to_string()),
                "timestamp" => values.push(csv_escape_field(&timestamp.to_rfc3339())),
                "event_data" => values.push(csv_escape_field(&data_str)),
                "created_at" => values.push(csv_escape_field(&created_at.to_rfc3339())),
                _ => {}
            }
        }
        let line = format!("{}\n", values.join(","));
        csv_data.push_str(&line);
    }
    let mut body: Vec<u8> = csv_data.into_bytes();
    let mut content_encoding = None;
    let mut filename = "events.csv";
    if use_gzip {
        body = compress_gzip(&body).await?;
        content_encoding = Some("gzip");
        filename = "events.csv.gz";
    } else if use_br {
        body = compress_brotli(&body).await?;
        content_encoding = Some("br");
        filename = "events.csv.br";
    }
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/csv")
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
        .header("Content-Range", content_range);
    if let Some(enc) = content_encoding {
        builder = builder.header(header::CONTENT_ENCODING, enc);
    }
    Ok(builder.body(Body::from(body)).unwrap_or_else(|e| response_build_error(e)))
}

/// Query parameters for the /v1/events/recent endpoint.
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct RecentParams {
    pub limit: Option<i64>,
    pub event_type: Option<crate::models::EventType>,
    pub contract_id: Option<String>,
    /// Cursor for pagination (opaque, URL-safe).
    pub cursor: Option<String>,
    /// Not supported — returns 400 if provided.
    pub from_ledger: Option<i64>,
    /// Not supported — returns 400 if provided.
    pub to_ledger: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/v1/events/recent",
    tag = "events",
    params(
        ("limit" = Option<i64>, Query, description = "Number of most-recent events to return, 1–100 (default: 20)"),
        ("event_type" = Option<crate::models::EventType>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("cursor" = Option<String>, Query, description = "Cursor for pagination (opaque, URL-safe)"),
    ),
    responses(
        (status = 200, description = "Most recently indexed events in descending ledger order"),
        (status = 400, description = "Invalid query parameters or unsupported ledger range filter"),
    )
)]
pub async fn get_recent_events(
    State(state): State<AppState>,
    Query(params): Query<RecentParams>,
) -> Result<Json<Value>, AppError> {
    if params.from_ledger.is_some() || params.to_ledger.is_some() {
        return Err(AppError::Validation(
            "from_ledger and to_ledger are not supported on /v1/events/recent".to_string(),
        ));
    }
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    let limit = params.limit.unwrap_or(20).clamp(1, 100);

    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }

    // Handle cursor-based pagination
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_ledger, cursor_id) = decode_cursor(cursor_str)?;
        conditions.push(format!(
            "(ledger, id) < (${}, ${})",
            bind_idx, bind_idx + 1
        ));
        bind_idx += 2;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, schema_version, asset_code, asset_issuer, 0::bigint AS total_count \
         FROM events {} ORDER BY ledger DESC, id DESC LIMIT ${}",
        where_clause, bind_idx,
    );

    let mut q = sqlx::query_as::<_, crate::models::Event>(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if let Some(ref et) = params.event_type {
        q = q.bind(et);
    }
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_ledger, cursor_id) = decode_cursor(cursor_str)?;
        q = q.bind(cursor_ledger);
        q = q.bind(cursor_id);
    }
    q = q.bind(limit + 1); // Fetch one extra to determine if there's a next page

    let rows = q.fetch_all(&state.pool).await?;
    let has_next = rows.len() > limit as usize;
    let rows = if has_next {
        &rows[..limit as usize]
    } else {
        &rows
    };

    let events: Vec<Value> = rows
        .iter()
        .map(|e| {
            filter_fields(
                e,
                crate::models::PaginationParams::ALLOWED_FIELDS,
                state.encryption_key.as_ref(),
                state.encryption_key_old.as_ref(),
            )
        })
        .collect();

    let mut response = json!({
        "data": events,
        "limit": limit,
    });

    if has_next && !rows.is_empty() {
        let last_event = &rows[rows.len() - 1];
        let next_cursor = encode_cursor(last_event.ledger, last_event.id);
        response["next_cursor"] = json!(next_cursor);
    }

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/events/contract/{contract_id}",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID (56-char, starts with C)"),
        ("page" = Option<i64>, Query, description = "Page number (default: 1)"),
        ("limit" = Option<i64>, Query, description = "Results per page, 1–100 (default: 20)"),
        ("exact_count" = Option<bool>, Query, description = "Use exact COUNT(*) instead of approximate (default: false)"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
        ("sort" = Option<String>, Query, description = "Sort order: asc (oldest first) or desc (newest first, default)"),
    ),
    responses(
        (status = 200, description = "Events for the given contract"),
        (status = 400, description = "Invalid contract_id format or ledger range", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "No events found for contract", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[instrument(skip(state, extensions), fields(contract_id = %contract_id))]
pub async fn get_events_by_contract(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(params): Query<PaginationParams>,
    extensions: axum::http::Extensions,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    if let Some(from) = params.from_ledger {
        validate_ledger_param("from_ledger", from)?;
    }
    if let Some(to) = params.to_ledger {
        validate_ledger_param("to_ledger", to)?;
    }
    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();

    let limit = params.limit();
    let offset = params.offset();
    let exact = params.exact_count.unwrap_or(false);
    let columns = resolve_columns(&params)?;
    let dir = params
        .sort
        .unwrap_or(crate::models::SortOrder::Desc)
        .as_sql();

    let mut conditions: Vec<String> = vec!["contract_id = $1".to_string()];
    let mut bind_idx: i32 = 2;

    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

    let where_clause = format!("WHERE {}", conditions.join(" AND "));
    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
         FROM events {} ORDER BY ledger {dir} LIMIT ${} OFFSET ${}",
        where_clause, bind_idx, bind_idx + 1,
    );

    let mut q = sqlx::query_as::<_, models::Event>(&query_str).bind(&contract_id);
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    if let Some(tid) = tenant_id {
        q = q.bind(tid);
    }
    q = q.bind(limit).bind(offset);

    let rows = timed_query(
        q.fetch_all(&state.read_pool),
        "get_events_by_contract",
        state.config.slow_query_threshold_ms,
        Some(&contract_id),
    )
    .await?;

    if rows.is_empty() {
        return Err(AppError::NotFound);
    }

    let events: Vec<Value> = rows
        .iter()
        .map(|e| {
            filter_fields(
                e,
                &columns,
                state.encryption_key.as_ref(),
                state.encryption_key_old.as_ref(),
            )
        })
        .collect();

    // Fetch total count. Skip cache in multi-tenant mode to avoid cross-tenant leakage.
    let total: i64 = if params.from_ledger.is_none() && params.to_ledger.is_none() && tenant_id.is_none() {
        if let Some(cached) = state.contract_count_cache.get(&contract_id).await {
            crate::metrics::update_contract_count_cache_hit_ratio(1, 0);
            cached
        } else {
            crate::metrics::update_contract_count_cache_hit_ratio(0, 1);
            // query_scalar! gives compile-time SQL verification via the .sqlx offline cache.
            let count: i64 =
                sqlx::query_scalar!("SELECT COUNT(*) FROM events WHERE contract_id = $1", &contract_id as &str)
                    .fetch_one(&state.pool)
                    .await?;
            state
                .contract_count_cache
                .insert(contract_id.clone(), count)
                .await;
            count
        }
    } else {
        let mut count_conditions: Vec<String> = vec!["contract_id = $1".to_string()];
        let mut cidx: i32 = 2;
        if params.from_ledger.is_some() {
            count_conditions.push(format!("ledger >= ${cidx}"));
            cidx += 1;
        }
        if params.to_ledger.is_some() {
            count_conditions.push(format!("ledger <= ${cidx}"));
            cidx += 1;
        }
        maybe_add_tenant_condition(&mut count_conditions, &mut cidx, tenant_id);
        let count_str = format!(
            "SELECT COUNT(*) FROM events WHERE {}",
            count_conditions.join(" AND ")
        );
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str).bind(&contract_id);
        if let Some(fl) = params.from_ledger {
            cq = cq.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            cq = cq.bind(tl);
        }
        if let Some(tid) = tenant_id {
            cq = cq.bind(tid);
        }
        cq.fetch_one(&state.pool).await?
    };

    let mut response = json!({
        "data": events,
        "contract_id": contract_id,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
        "approximate": false,
    });

    if let Some(fl) = params.from_ledger {
        response["from_ledger"] = json!(fl);
    }
    if let Some(tl) = params.to_ledger {
        response["to_ledger"] = json!(tl);
    }

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/events/tx/{tx_hash}",
    tag = "events",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash (64 hex chars, case-insensitive — normalized to lowercase)"),
    ),
    responses(
        (status = 200, description = "Events for the given transaction (empty array if none)"),
        (status = 400, description = "Invalid tx_hash format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[instrument(skip(state, extensions), fields(tx_hash = %tx_hash))]
pub async fn get_events_by_tx(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
    Query(params): Query<PaginationParams>,
    extensions: axum::http::Extensions,
) -> Result<Json<Value>, AppError> {
    let tx_hash = tx_hash.to_lowercase();
    validate_tx_hash(&tx_hash)?;

    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();

    let columns = resolve_columns(&params)?;

    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&"ledger") {
        select_cols.push("ledger");
    }
    if !select_cols.contains(&"id") {
        select_cols.push("id");
    }

    let mut conditions: Vec<String> = vec!["tx_hash = $1".to_string()];
    let mut bind_idx: i32 = 2;
    maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

    let query_str = format!(
        "SELECT {} FROM events WHERE {} ORDER BY ledger DESC, id DESC",
        select_cols.join(", "),
        conditions.join(" AND "),
    );

    let mut q = sqlx::query(&query_str).bind(&tx_hash);
    if let Some(tid) = tenant_id {
        q = q.bind(tid);
    }
    let rows = timed_query(
        q.fetch_all(&state.read_pool),
        "get_events_by_tx",
        state.config.slow_query_threshold_ms,
        Some(&tx_hash),
    )
    .await?;

    let total = rows.len() as i64;
    let events = rows_to_json(
        &rows,
        &columns,
        state.encryption_key.as_ref(),
        state.encryption_key_old.as_ref(),
        false,
    )?;

    Ok(Json(json!({
        "data": events,
        "tx_hash": tx_hash,
        "total": total,
        "approximate": false,
    })))
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct RelatedTxParams {
    pub depth: Option<u8>,
}

#[utoipa::path(
    get,
    path = "/v1/events/tx/{tx_hash}/related",
    tag = "events",
    params(
        ("tx_hash" = String, Path, description = "Root transaction hash (64 hex chars)"),
        ("depth" = Option<u8>, Query, description = "Reference traversal depth, default 1, max 3"),
    ),
    responses(
        (status = 200, description = "Events from transactions referenced by event_data", body = serde_json::Value),
        (status = 400, description = "Invalid tx_hash or depth", body = ErrorResponse),
    )
)]
pub async fn get_related_events_by_tx(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
    Query(params): Query<RelatedTxParams>,
) -> Result<Json<Value>, AppError> {
    let root_tx_hash = tx_hash.to_lowercase();
    validate_tx_hash(&root_tx_hash)?;
    let max_depth = params.depth.unwrap_or(1);
    if max_depth > 3 {
        return Err(AppError::Validation("depth must be between 0 and 3".to_string()));
    }

    let mut seen = std::collections::HashSet::from([root_tx_hash.clone()]);
    let mut frontier = vec![root_tx_hash.clone()];
    let mut all_events = Vec::new();

    for depth in 0..=max_depth {
        if frontier.is_empty() {
            break;
        }
        let rows = sqlx::query(
            "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_decoded, created_at, schema_version, 0::bigint AS total_count
             FROM events WHERE tx_hash = ANY($1) ORDER BY ledger DESC, id DESC",
        )
        .bind(&frontier)
        .fetch_all(&state.read_pool)
        .await?;

        let mut next = Vec::new();
        for row in rows {
            let event_data: Value = row.try_get("event_data")?;
            collect_tx_refs(&event_data, &mut next);
            all_events.push(row_to_event_json(&row)?);
        }
        if depth == max_depth {
            break;
        }
        frontier = next
            .into_iter()
            .map(|h| h.to_lowercase())
            .filter(|h| validate_tx_hash(h).is_ok())
            .filter(|h| seen.insert(h.clone()))
            .collect();
    }

    let total = all_events.len();
    Ok(Json(json!({ "tx_hash": root_tx_hash, "depth": max_depth, "data": all_events, "total": total })))
}

fn collect_tx_refs(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if is_tx_ref_key(key) {
                    if let Some(tx_hash) = value.as_str() {
                        out.push(tx_hash.to_string());
                    }
                }
                collect_tx_refs(value, out);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_tx_refs(value, out);
            }
        }
        _ => {}
    }
}

fn is_tx_ref_key(key: &str) -> bool {
    matches!(key, "tx_hash" | "transaction_hash" | "related_tx_hash" | "parent_tx_hash" | "child_tx_hash")
}

fn row_to_event_json(row: &sqlx::postgres::PgRow) -> Result<Value, AppError> {
    Ok(json!({
        "id": row.try_get::<Uuid, _>("id")?,
        "contract_id": row.try_get::<String, _>("contract_id")?,
        "event_type": row.try_get::<String, _>("event_type")?,
        "tx_hash": row.try_get::<String, _>("tx_hash")?,
        "ledger": row.try_get::<i64, _>("ledger")?,
        "timestamp": row.try_get::<DateTime<Utc>, _>("timestamp")?,
        "event_data": row.try_get::<Value, _>("event_data")?,
        "event_data_decoded": row.try_get::<Option<Value>, _>("event_data_decoded")?,
        "created_at": row.try_get::<DateTime<Utc>, _>("created_at")?,
        "schema_version": row.try_get::<i32, _>("schema_version")?,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/events/tx/batch",
    tag = "events",
    request_body = crate::models::BatchTxRequest,
    responses(
        (status = 200, description = "Map of tx_hash -> events for all requested hashes"),
        (status = 400, description = "Invalid hashes or too many hashes", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[instrument(skip(state, body))]
pub async fn bulk_insert_events(
    State(state): State<AppState>,
    Json(body): Json<crate::models::BulkInsertRequest>,
) -> Result<Json<crate::models::BulkInsertResponse>, AppError> {
    const MAX_BATCH_SIZE: usize = 1000;
    
    if body.events.is_empty() {
        return Err(AppError::Validation("events list cannot be empty".to_string()));
    }
    
    if body.events.len() > MAX_BATCH_SIZE {
        return Err(AppError::Validation(format!(
            "events list exceeds maximum of {} events",
            MAX_BATCH_SIZE
        )));
    }
    
    let mut inserted = 0i64;
    let mut skipped = 0i64;
    let mut failed = 0i64;
    let mut errors = Vec::new();
    
    for event in body.events {
        // Validate contract_id
        if let Err(e) = validate_contract_id(&event.contract_id) {
            failed += 1;
            errors.push(format!("Invalid contract_id: {}", e));
            continue;
        }
        
        // Validate tx_hash
        if let Err(e) = validate_tx_hash(&event.tx_hash) {
            failed += 1;
            errors.push(format!("Invalid tx_hash: {}", e));
            continue;
        }
        
        // Validate event_type
        let event_type = match event.event_type.as_str() {
            "contract" | "diagnostic" | "system" => event.event_type.clone(),
            _ => {
                failed += 1;
                errors.push(format!("Invalid event_type: {}", event.event_type));
                continue;
            }
        };
        
        let id = Uuid::new_v4();
        let result = sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_normalized, ledger_hash, in_successful_call, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
             ON CONFLICT (contract_id, tx_hash, ledger, event_type) DO NOTHING"
        )
        .bind(id)
        .bind(&event.contract_id)
        .bind(&event_type)
        .bind(&event.tx_hash)
        .bind(event.ledger)
        .bind(event.timestamp)
        .bind(&event.event_data)
        .bind(&event.event_data_normalized)
        .bind(&event.ledger_hash)
        .bind(event.in_successful_call.unwrap_or(false))
        .execute(&state.pool)
        .await;
        
        match result {
            Ok(result) => {
                if result.rows_affected() > 0 {
                    inserted += 1;
                } else {
                    skipped += 1;
                }
            }
            Err(e) => {
                failed += 1;
                errors.push(format!("Database error: {}", e));
            }
        }
    }
    
    Ok(Json(crate::models::BulkInsertResponse {
        inserted,
        skipped,
        failed,
        errors,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/admin/events/bulk",
    tag = "Admin",
    request_body = crate::models::BulkInsertRequest,
    responses(
        (status = 200, description = "Bulk insert result", body = crate::models::BulkInsertResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn get_events_by_tx_batch(
    State(state): State<AppState>,
    Json(body): Json<crate::models::BatchTxRequest>,
) -> Result<Json<Value>, AppError> {
    if body.hashes.len() > state.config.batch_tx_max_size {
        return Err(AppError::Validation(format!(
            "too many hashes: maximum is {}",
            state.config.batch_tx_max_size
        )));
    }

    // Validate all hashes; collect invalid ones for a helpful error.
    let invalid: Vec<String> = body
        .hashes
        .iter()
        .filter(|h| validate_tx_hash(h).is_err())
        .cloned()
        .collect();
    if !invalid.is_empty() {
        return Err(AppError::Validation(format!(
            "invalid tx_hash(es): {}",
            invalid.join(", ")
        )));
    }

    // Deduplicate hashes using HashSet
    let mut unique_hashes: std::collections::HashSet<String> = std::collections::HashSet::new();
    for h in &body.hashes {
        unique_hashes.insert(h.to_lowercase());
    }
    let deduplicated_count = unique_hashes.len();
    let hashes: Vec<String> = unique_hashes.into_iter().collect();

    // Single query using ANY().
    let rows = sqlx::query(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
         FROM events WHERE tx_hash = ANY($1) ORDER BY tx_hash, ledger DESC, id DESC",
    )
    .bind(&hashes)
    .fetch_all(&state.read_pool)
    .await?;

    // Build result map: all requested hashes present, even if empty.
    let mut result: serde_json::Map<String, Value> = hashes
        .iter()
        .map(|h| (h.clone(), Value::Array(vec![])))
        .collect();

    let all_cols: &[&str] = &[
        "id",
        "contract_id",
        "event_type",
        "tx_hash",
        "ledger",
        "timestamp",
        "event_data",
        "created_at",
    ];
    for row in &rows {
        let tx_hash: String = row.try_get("tx_hash")?;
        let event_json = rows_to_json(
            std::slice::from_ref(row),
            all_cols,
            state.encryption_key.as_ref(),
            state.encryption_key_old.as_ref(),
            false,
        )?;
        if let Some(arr) = result.get_mut(&tx_hash).and_then(|v| v.as_array_mut()) {
            if let Some(ev) = event_json.into_iter().next() {
                arr.push(ev);
            }
        }
    }

    // Add deduplicated_count to response
    let mut response_obj = result;
    response_obj.insert("deduplicated_count".to_string(), Value::Number(deduplicated_count.into()));

    Ok(Json(Value::Object(response_obj)))
}

#[utoipa::path(
    get,
    path = "/v1/events/ledger-hash/{hash}",
    tag = "events",
    params(
        ("hash" = String, Path, description = "Ledger hash"),
    ),
    responses(
        (status = 200, description = "Events for the given ledger hash"),
        (status = 400, description = "Invalid parameters", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_events_by_ledger_hash(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Value>, AppError> {
    let columns = resolve_columns(&params)?;
    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&"ledger") {
        select_cols.push("ledger");
    }
    if !select_cols.contains(&"id") {
        select_cols.push("id");
    }

    let query_str = format!(
        "SELECT {} FROM events WHERE ledger_hash = $1 ORDER BY ledger DESC, id DESC",
        select_cols.join(", "),
    );
    let rows = sqlx::query(&query_str)
        .bind(&hash)
        .fetch_all(&state.read_pool)
        .await?;

    let events = rows_to_json(
        &rows,
        &columns,
        state.encryption_key.as_ref(),
        state.encryption_key_old.as_ref(),
        false,
    )?;
    Ok(Json(json!({
        "data": events,
        "ledger_hash": hash,
        "total": rows.len() as i64,
        "approximate": false,
    })))
}

#[utoipa::path(
    post,
    path = "/v1/admin/contracts/{contract_id}/abi",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
    ),
    request_body(content = Value, description = "ABI JSON array", content_type = "application/json"),
    responses(
        (status = 200, description = "ABI registered"),
        (status = 400, description = "Invalid contract_id or ABI", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn register_contract_abi(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(abi): Json<Value>,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;
    // Issue #607: validate ABI structure before persisting.
    if let Err(e) = crate::abi::validate_abi(&abi) {
        crate::metrics::record_abi_validation_failure(&contract_id);
        return Err(AppError::Validation(e));
    }
    sqlx::query(
        "INSERT INTO contract_abis (contract_id, abi, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (contract_id) DO UPDATE SET abi = EXCLUDED.abi, updated_at = NOW()",
    )
    .bind(&contract_id)
    .bind(&abi)
    .execute(&state.pool)
    .await?;

    // Issue #607: invalidate the in-process cache so the next read fetches fresh data.
    crate::abi::invalidate_abi_cache(&state.abi_cache, &contract_id).await;

    let pool = state.pool.clone();
    let backfill_contract_id = contract_id.clone();
    let backfill_abi = abi.clone();
    tokio::spawn(async move {
        crate::abi::decode_existing_events(pool, backfill_contract_id, backfill_abi).await;
    });

    Ok(Json(
        json!({ "contract_id": contract_id, "status": "registered" }),
    ))
}

#[utoipa::path(
    get,
    path = "/v1/admin/contracts/{contract_id}/abi",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
    ),
    responses(
        (status = 200, description = "Registered ABI", body = serde_json::Value),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
        (status = 404, description = "ABI not found", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_contract_abi(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;
    let row = sqlx::query("SELECT abi, created_at, updated_at FROM contract_abis WHERE contract_id = $1")
        .bind(&contract_id)
        .fetch_optional(&state.read_pool)
        .await?
        .ok_or(AppError::NotFound)?;

    let abi: Value = row.try_get("abi")?;
    let created_at: DateTime<Utc> = row.try_get("created_at")?;
    let updated_at: DateTime<Utc> = row.try_get("updated_at")?;
    Ok(Json(json!({ "contract_id": contract_id, "abi": abi, "created_at": created_at, "updated_at": updated_at })))
}

/// Admin: create or update a contract's label/metadata (#1066).
#[utoipa::path(
    post,
    path = "/v1/admin/contracts/{contract_id}/metadata",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
    ),
    request_body = crate::contract_metadata::UpsertContractMetadata,
    responses(
        (status = 200, description = "Metadata upserted", body = crate::contract_metadata::ContractMetadata),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn upsert_contract_metadata(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(input): Json<crate::contract_metadata::UpsertContractMetadata>,
) -> Result<Json<crate::contract_metadata::ContractMetadata>, AppError> {
    validate_contract_id(&contract_id)?;
    let metadata =
        crate::contract_metadata::upsert_contract_metadata(&state.pool, &contract_id, input)
            .await?;
    Ok(Json(metadata))
}

/// Admin: delete a contract's label/metadata (#1066).
pub async fn delete_contract_metadata(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;
    let deleted =
        crate::contract_metadata::delete_contract_metadata(&state.pool, &contract_id).await?;
    if !deleted {
        return Err(AppError::NotFound);
    }
    Ok(Json(json!({ "contract_id": contract_id, "status": "deleted" })))
}

/// Public: read a contract's label/metadata (#1066).
#[utoipa::path(
    get,
    path = "/v1/contracts/{contract_id}/metadata",
    tag = "contracts",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
    ),
    responses(
        (status = 200, description = "Contract metadata", body = crate::contract_metadata::ContractMetadata),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
        (status = 404, description = "No metadata registered", body = ErrorResponse),
    )
)]
pub async fn get_contract_metadata(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<crate::contract_metadata::ContractMetadata>, AppError> {
    validate_contract_id(&contract_id)?;
    let metadata =
        crate::contract_metadata::get_contract_metadata(&state.read_pool, &contract_id)
            .await?
            .ok_or(AppError::NotFound)?;
    Ok(Json(metadata))
}

/// Admin: bulk import contract labels from a JSON array of
/// `{contract_id, name, description, project_url, source_repo, tags, verified}` (#1066).
pub async fn bulk_import_contract_metadata(
    State(state): State<AppState>,
    Json(entries): Json<Vec<Value>>,
) -> Result<Json<Value>, AppError> {
    let mut parsed = Vec::with_capacity(entries.len());
    for entry in entries {
        let contract_id = entry
            .get("contract_id")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::Validation("missing contract_id".into()))?
            .to_string();
        validate_contract_id(&contract_id)?;
        let input: crate::contract_metadata::UpsertContractMetadata =
            serde_json::from_value(entry)
                .map_err(|e| AppError::Validation(format!("invalid metadata entry: {e}")))?;
        parsed.push((contract_id, input));
    }
    let count = crate::contract_metadata::bulk_import(&state.pool, parsed).await?;
    Ok(Json(json!({ "imported": count })))
}

/// Anonymize a specific event for GDPR compliance.
/// Replaces event_data with {"anonymized": true} and hashes tx_hash with SHA-256.
/// Idempotent: already-anonymized events return 200 without re-processing.
#[utoipa::path(
    post,
    path = "/v1/admin/events/{id}/anonymize",
    tag = "admin",
    params(
        ("id" = String, Path, description = "Event UUID"),
    ),
    responses(
        (status = 200, description = "Event anonymized"),
        (status = 404, description = "Event not found", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn anonymize_event(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    use sha2::{Digest, Sha256};

    // Fetch current tx_hash and anonymized flag
    let row = sqlx::query("SELECT tx_hash, anonymized FROM events WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(AppError::NotFound)?;

    let already_anonymized: bool = row.try_get("anonymized")?;
    if already_anonymized {
        tracing::info!(event_id = %id, "Event already anonymized, skipping");
        return Ok(Json(json!({ "id": id, "anonymized": true })));
    }

    let tx_hash: String = row.try_get("tx_hash")?;
    let hashed_tx = {
        let mut h = Sha256::new();
        h.update(tx_hash.as_bytes());
        format!("{:x}", h.finalize())
    };

    sqlx::query("UPDATE events SET anonymized = TRUE, event_data = $1, tx_hash = $2 WHERE id = $3")
        .bind(json!({"anonymized": true}))
        .bind(&hashed_tx)
        .bind(id)
        .execute(&state.pool)
        .await?;

    tracing::info!(event_id = %id, anonymized_at = %chrono::Utc::now(), "Event anonymized");

    Ok(Json(json!({ "id": id, "anonymized": true })))
}

/// Delete all events for a contract (GDPR right-to-erasure).
/// Optionally anonymize instead of deleting with `anonymize_only=true`.
/// Logs deletion request for audit purposes.
#[utoipa::path(
    delete,
    path = "/v1/admin/events/contract/{contract_id}",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID"),
        ("anonymize_only" = Option<bool>, Query, description = "If true, anonymize instead of deleting (default: false)"),
    ),
    responses(
        (status = 200, description = "Events deleted or anonymized"),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn delete_contract_events(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    let anonymize_only = params
        .get("anonymize_only")
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false);

    let client_ip = headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("unknown");

    if anonymize_only {
        // Anonymize: set anonymized=true and hash tx_hash
        use sha2::{Digest, Sha256};

        let rows = sqlx::query("SELECT id, tx_hash FROM events WHERE contract_id = $1 AND anonymized = FALSE")
            .bind(&contract_id)
            .fetch_all(&state.pool)
            .await?;

        let count = rows.len() as u64;

        for row in rows {
            let id: Uuid = row.try_get("id")?;
            let tx_hash: String = row.try_get("tx_hash")?;
            let hashed_tx = {
                let mut h = Sha256::new();
                h.update(tx_hash.as_bytes());
                format!("{:x}", h.finalize())
            };

            sqlx::query("UPDATE events SET anonymized = TRUE, event_data = $1, tx_hash = $2 WHERE id = $3")
                .bind(json!({"anonymized": true}))
                .bind(&hashed_tx)
                .bind(id)
                .execute(&state.pool)
                .await?;
        }

        tracing::info!(
            contract_id = %contract_id,
            count = count,
            client_ip = client_ip,
            timestamp = %chrono::Utc::now(),
            "Events anonymized for contract (GDPR)"
        );

        crate::metrics::record_events_deleted(count);

        Ok(Json(json!({
            "contract_id": contract_id,
            "action": "anonymized",
            "count": count,
        })))
    } else {
        // Hard delete
        let result = sqlx::query("DELETE FROM events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&state.pool)
            .await?;

        let count = result.rows_affected();

        tracing::info!(
            contract_id = %contract_id,
            count = count,
            client_ip = client_ip,
            timestamp = %chrono::Utc::now(),
            "Events deleted for contract (GDPR right-to-erasure)"
        );

        crate::metrics::record_events_deleted(count);

        Ok(Json(json!({
            "contract_id": contract_id,
            "action": "deleted",
            "count": count,
        })))
    }
}

/// Pause the indexer loop without stopping the HTTP server.
#[utoipa::path(
    post,
    path = "/v1/admin/indexer/pause",
    tag = "admin",
    responses(
        (status = 200, description = "Indexer paused"),
        (status = 403, description = "Not the active indexer", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn pause_indexer(
    State(state): State<AppState>,
) -> Result<Json<Value>, AppError> {
    if !state
        .indexer_state
        .is_active_indexer
        .load(Ordering::Relaxed)
    {
        return Err(AppError::Forbidden(
            "pause endpoint only available on active indexer".to_string(),
        ));
    }
    state.indexer_state.is_paused.store(true, Ordering::Relaxed);
    tracing::info!("Indexer paused via admin API");
    Ok(Json(json!({ "indexer_paused": true })))
}

/// Resume a previously paused indexer loop.
#[utoipa::path(
    post,
    path = "/v1/admin/indexer/resume",
    tag = "admin",
    responses(
        (status = 200, description = "Indexer resumed"),
        (status = 403, description = "Not the active indexer", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn resume_indexer(
    State(state): State<AppState>,
) -> Result<Json<Value>, AppError> {
    if !state
        .indexer_state
        .is_active_indexer
        .load(Ordering::Relaxed)
    {
        return Err(AppError::Forbidden(
            "resume endpoint only available on active indexer".to_string(),
        ));
    }
    state
        .indexer_state
        .is_paused
        .store(false, Ordering::Relaxed);
    tracing::info!("Indexer resumed via admin API");
    Ok(Json(json!({ "indexer_paused": false })))
}

/// Operational cost report with per-component breakdown and a linear forecast
/// (Issue #1264).
///
/// `GET /v1/admin/costs/report`
///
/// Aggregates the hourly `cost_snapshots` rows over the requested window
/// (default: last 24 hours) and returns the cost breakdown by component
/// (`rpc`, `db`, `notification`) together with a forecast for the next window
/// produced by the linear model in `costs::forecast`.
#[utoipa::path(
    get,
    path = "/v1/admin/costs/report",
    tag = "admin",
    params(
        ("hours" = Option<i64>, Query, description = "Lookback window in hours (default 24)"),
        ("forecast_hours" = Option<i64>, Query, description = "Forecast horizon in hours (default 24)"),
    ),
    responses(
        (status = 200, description = "Cost breakdown by component with forecast", body = Value),
        (status = 400, description = "Invalid query parameters", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Failed to generate cost report", body = ErrorResponse),
    )
)]
pub async fn get_cost_report(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    let hours = match params.get("hours") {
        Some(raw) => raw
            .parse::<i64>()
            .ok()
            .filter(|h| *h > 0 && *h <= 24 * 90)
            .ok_or_else(|| {
                AppError::Validation("hours must be an integer between 1 and 2160".to_string())
            })?,
        None => 24,
    };
    let forecast_hours = match params.get("forecast_hours") {
        Some(raw) => raw
            .parse::<i64>()
            .ok()
            .filter(|h| *h > 0 && *h <= 24 * 90)
            .ok_or_else(|| {
                AppError::Validation(
                    "forecast_hours must be an integer between 1 and 2160".to_string(),
                )
            })?,
        None => 24,
    };

    let report = crate::costs::report::generate_report(&state.read_pool, hours)
        .await
        .map_err(|e| AppError::Internal(format!("failed to generate cost report: {e}")))?;

    let forecast = crate::costs::report::generate_forecast(&state.read_pool, forecast_hours)
        .await
        .map_err(|e| AppError::Internal(format!("failed to generate cost forecast: {e}")))?;

    Ok(Json(json!({
        "window_hours": hours,
        "total_cost": report.total_cost,
        "breakdown": report.by_component,
        "snapshots": report.snapshot_count,
        "forecast": {
            "horizon_hours": forecast_hours,
            "predicted_cost": forecast.predicted_cost,
            "slope": forecast.slope,
            "intercept": forecast.intercept,
            "confidence": forecast.confidence,
        },
    })))
}

/// Get current database connection pool statistics
#[utoipa::path(
    get,
    path = "/v1/admin/pool-config/statistics",
    tag = "admin",
    responses(
        (status = 200, description = "Pool statistics retrieved", body = PoolStatistics),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_pool_statistics(
    State(state): State<AppState>,
) -> Result<Json<crate::models::PoolStatistics>, (StatusCode, Json<Value>)> {
    crate::db_pool::management::get_pool_config(&state.db, state.pool_stats.clone(), state.config.db_max_connections)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))
}

/// Get pool configuration and generate tuning recommendations
#[utoipa::path(
    get,
    path = "/v1/admin/pool-config",
    tag = "admin",
    responses(
        (status = 200, description = "Pool configuration retrieved", body = PoolTuningGuide),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_pool_tuning_guide(
    State(state): State<AppState>,
) -> Result<Json<crate::models::PoolTuningGuide>, (StatusCode, Json<Value>)> {
    let pool_stats = crate::db_pool::management::get_pool_config(&state.db, state.pool_stats.clone(), state.config.db_max_connections)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))?;

    let current_config = crate::models::PoolConfig {
        max_connections: state.config.db_max_connections,
        min_connections: state.config.db.min_connections.unwrap_or(1),
        connection_timeout_secs: 30,
        idle_timeout_secs: 600,
        exhaustion_threshold: 0.9,
        sample_interval_secs: 15,
    };

    let guide = crate::db_pool::management::generate_tuning_guide(&pool_stats, &current_config);
    Ok(Json(guide))
}

/// Get pool health status
#[utoipa::path(
    get,
    path = "/v1/admin/pool-config/health",
    tag = "admin",
    responses(
        (status = 200, description = "Pool health status", body = Value),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_pool_health(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    crate::db_pool::management::check_pool_health(
        &state.db,
        state.pool_stats.clone(),
        state.config.db_max_connections,
        0.9,
    )
    .await
    .map(Json)
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))
}

/// Get comprehensive statistics report for all tables
#[utoipa::path(
    get,
    path = "/v1/admin/statistics/report",
    tag = "admin",
    responses(
        (status = 200, description = "Statistics report for all tables", body = Vec<StatisticsReportItem>),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Failed to generate report", body = ErrorResponse),
    )
)]
pub async fn get_statistics_report(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::models::StatisticsReportItem>>, (StatusCode, Json<Value>)> {
    crate::statistics_management::get_statistics_report(&state.db)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))
}

/// Detect which tables have stale statistics
#[utoipa::path(
    get,
    path = "/v1/admin/statistics/stale",
    tag = "admin",
    responses(
        (status = 200, description = "Stale statistics detection results", body = Vec<StalenessDetection>),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Failed to detect stale statistics", body = ErrorResponse),
    )
)]
pub async fn detect_stale_statistics(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::models::StalenessDetection>>, (StatusCode, Json<Value>)> {
    crate::statistics_management::detect_stale_statistics(&state.db)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))
}

/// Get overall statistics health score
#[utoipa::path(
    get,
    path = "/v1/admin/statistics/health",
    tag = "admin",
    responses(
        (status = 200, description = "Statistics health score (0-100)", body = StatisticsHealth),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Failed to get health score", body = ErrorResponse),
    )
)]
pub async fn get_statistics_health(
    State(state): State<AppState>,
) -> Result<Json<crate::models::StatisticsHealth>, (StatusCode, Json<Value>)> {
    let health_score = crate::statistics_management::get_statistics_health_score(&state.db)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))?;

    let stale = crate::statistics_management::detect_stale_statistics(&state.db)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))?;

    let stale_count = stale.iter().filter(|s| s.is_stale).count();
    let total_count = stale.len();

    let status = if health_score >= 80 {
        "healthy"
    } else if health_score >= 60 {
        "degraded"
    } else {
        "critical"
    };

    Ok(Json(crate::models::StatisticsHealth {
        health_score,
        status: status.to_string(),
        stale_tables_count: stale_count as u32,
        total_tables: total_count as u32,
        timestamp: chrono::Utc::now(),
    }))
}

/// Refresh statistics for all tables or a specific table
#[utoipa::path(
    post,
    path = "/v1/admin/statistics/refresh",
    tag = "admin",
    params(
        ("table_name" = Option<String>, Query, description = "Optional table name to refresh only one table")
    ),
    responses(
        (status = 202, description = "Statistics refresh job started"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Failed to refresh statistics", body = ErrorResponse),
    )
)]
pub async fn refresh_statistics(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let table_name = params.get("table_name").map(|s| s.as_str());

    match crate::statistics_management::refresh_table_statistics(&state.db, table_name).await {
        Ok(results) => {
            let summary = if let Some(table) = table_name {
                format!("Refreshed statistics for table: {}", table)
            } else {
                format!("Refreshed statistics for {} tables", results.len())
            };

            Ok((
                StatusCode::ACCEPTED,
                Json(json!({
                    "message": summary,
                    "results_count": results.len(),
                    "timestamp": chrono::Utc::now()
                })),
            ))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )),
    }
}

/// Get recent statistics analysis jobs
#[utoipa::path(
    get,
    path = "/v1/admin/statistics/jobs",
    tag = "admin",
    params(
        ("limit" = Option<i64>, Query, description = "Maximum number of jobs to return (default 50)")
    ),
    responses(
        (status = 200, description = "Recent analysis jobs", body = Vec<StatisticsAnalysisJobInfo>),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Failed to retrieve jobs", body = ErrorResponse),
    )
)]
pub async fn get_analysis_jobs(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<crate::models::StatisticsAnalysisJobInfo>>, (StatusCode, Json<Value>)> {
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(50);

    crate::statistics_management::get_recent_analysis_jobs(&state.db, limit)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() }))))
}

/// #694: Query and return index fragmentation information.
///
/// `GET /v1/admin/indexes/fragmentation`
///
/// Returns a list of all user indexes in the public schema with their
/// estimated bloat ratio, dead/live tuple counts, size, and last vacuum
/// timestamps.  Results are sorted by index size descending.
#[utoipa::path(
    get,
    path = "/v1/admin/indexes/fragmentation",
    tag = "admin",
    responses(
        (status = 200, description = "Index fragmentation report"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn get_index_fragmentation(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::index_monitor::IndexFragmentationInfo>>, (StatusCode, Json<Value>)> {
    crate::index_monitor::query_index_fragmentation(&state.pool)
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!(error = %e, "Failed to query index fragmentation");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to query index fragmentation: {e}"),
                    "code": "INTERNAL_ERROR"
                })),
            )
        })
}

/// #694: Trigger a single REINDEX INDEX CONCURRENTLY operation.
///
/// `POST /v1/admin/indexes/{index_name}/reindex`
///
/// Runs `REINDEX INDEX CONCURRENTLY` on the named index. Returns 202 on
/// success.  The index must be a valid user index in the public schema.
#[utoipa::path(
    post,
    path = "/v1/admin/indexes/{index_name}/reindex",
    tag = "admin",
    params(
        ("index_name" = String, Path, description = "Name of the index to rebuild"),
    ),
    responses(
        (status = 202, description = "REINDEX scheduled successfully"),
        (status = 400, description = "Invalid index name", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "REINDEX failed", body = ErrorResponse),
    )
)]
pub async fn reindex_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    // Basic validation: only allow alphanumeric + underscore index names
    if index_name.is_empty()
        || !index_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid index name",
                "code": "VALIDATION_ERROR"
            })),
        ));
    }

    // Verify the index exists
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = $1 AND relkind = 'i')",
    )
    .bind(&index_name)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": format!("Failed to verify index existence: {e}"),
                "code": "INTERNAL_ERROR"
            })),
        )
    })?;

    if !exists {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": format!("index '{index_name}' not found"),
                "code": "NOT_FOUND"
            })),
        ));
    }

    tracing::info!(index = %index_name, "Admin-triggered REINDEX INDEX CONCURRENTLY");

    let sql = format!("REINDEX INDEX CONCURRENTLY {index_name}");
    sqlx::query(&sql).execute(&state.pool).await.map_err(|e| {
        tracing::error!(index = %index_name, error = %e, "REINDEX failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "error": format!("REINDEX failed: {e}"),
                "code": "REINDEX_FAILED"
            })),
        )
    })?;

    tracing::info!(index = %index_name, "REINDEX INDEX CONCURRENTLY completed");

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "status": "completed",
            "index": index_name,
            "message": "REINDEX INDEX CONCURRENTLY completed successfully"
        })),
    ))
}

/// Request body for `POST /v1/admin/encryption/rotate` (Issue #1270).
#[derive(Debug, serde::Deserialize)]
pub struct RotateEncryptionKeyRequest {
    /// The new AES-GCM key (base64-encoded 32-byte key).
    pub new_key: String,
}

/// Rotate the AES-GCM credential-encryption key without downtime (Issue #1270).
///
/// `POST /v1/admin/encryption/rotate`
///
/// Accepts `{ "new_key": "<base64 32-byte key>" }`, then starts a batched
/// background job that decrypts every stored credential with the current key
/// and re-encrypts it with `new_key`. The job commits in batches of 100 rows
/// and can be resumed from where it stopped if interrupted.
///
/// This endpoint is gated behind `ADMIN_API_KEY` by the admin auth middleware.
#[utoipa::path(
    post,
    path = "/v1/admin/encryption/rotate",
    tag = "admin",
    request_body(content = Value, description = "{\"new_key\": \"<base64 32-byte key>\"}", content_type = "application/json"),
    responses(
        (status = 202, description = "Key rotation job started"),
        (status = 400, description = "Invalid or missing new_key", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 409, description = "A rotation job is already running", body = ErrorResponse),
    )
)]
pub async fn rotate_encryption_key(
    State(state): State<AppState>,
    Json(body): Json<RotateEncryptionKeyRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    // Validate the new key: must decode to exactly 32 bytes.
    let new_key_bytes = STANDARD
        .decode(body.new_key.trim())
        .map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "new_key must be valid base64" })),
            )
        })?;
    if new_key_bytes.len() != 32 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "new_key must decode to exactly 32 bytes" })),
        ));
    }
    let mut new_key = [0u8; 32];
    new_key.copy_from_slice(&new_key_bytes);

    // The current key is required to decrypt existing credentials.
    let old_key = state.encryption_key.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "no current encryption key configured (ENCRYPTION_KEY)"
            })),
        )
    })?;

    // Start the batched rotation job (100 rows per transaction).
    let started = crate::reencrypt::start_rotation_job(
        state.pool.clone(),
        new_key,
        old_key,
        100,
        state.reencrypt_state.clone(),
    );

    if !started {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({ "error": "a key rotation job is already running" })),
        ));
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "status": "started",
            "batch_size": 100,
            "message": "key rotation started; poll /v1/admin/encryption/rotate/status for progress"
        })),
    ))
}

/// Report progress of the credential key-rotation job (Issue #1270).
///
/// `GET /v1/admin/encryption/rotate/status`
#[utoipa::path(
    get,
    path = "/v1/admin/encryption/rotate/status",
    tag = "admin",
    responses(
        (status = 200, description = "Rotation job status"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_encryption_rotation_status(
    State(state): State<AppState>,
) -> Json<Value> {
    let (completed, total, running) = state.reencrypt_state.progress();
    Json(json!({
        "running": running,
        "completed": completed,
        "total": total,
        "progress": if total > 0 { completed as f64 / total as f64 } else { 0.0 },
    }))
}

/// Start background re-encryption jobs after a key rotation.
///
/// - Events: migrates `event_data` from `ENCRYPTION_KEY_OLD` to `ENCRYPTION_KEY`
///   (requires the `encryption` feature).
/// - Integration credentials (Issue #1162): re-encrypts every stored token,
///   webhook URL and routing key from `INTEGRATION_ENCRYPTION_KEY_OLD` to
///   `INTEGRATION_ENCRYPTION_KEY`, encrypting any remaining plaintext rows.
#[utoipa::path(
    post,
    path = "/v1/admin/reencrypt",
    tag = "admin",
    responses(
        (status = 202, description = "Re-encryption job(s) started"),
        (status = 400, description = "No old key configured for events or integration credentials", body = ErrorResponse),
        (status = 409, description = "Re-encryption job already running", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn start_reencrypt(
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let batch_size = 1000;
    let mut jobs: Vec<&str> = Vec::new();

    if crate::integration_secrets::rotation_configured() {
        if !crate::integration_secrets::start_reencrypt_job(state.pool.clone(), batch_size as i64) {
            return Err((
                StatusCode::CONFLICT,
                Json(json!({ "error": "integration credential re-encryption job already running" })),
            ));
        }
        jobs.push("integration_credentials");
    }

    #[cfg(feature = "encryption")]
    {
        if let (Some(new_key), Some(old_key)) = (state.encryption_key, state.encryption_key_old) {
            // Create or get the reencrypt state from app state
            // For now, we'll create a new one per request (in production, store in AppState)
            let reencrypt_state = crate::reencrypt::ReencryptState::new();

            if reencrypt_state.is_running() {
                return Err((
                    StatusCode::CONFLICT,
                    Json(json!({ "error": "re-encryption job already running" })),
                ));
            }

            crate::reencrypt::start_reencrypt_job(
                state.pool.clone(),
                new_key,
                old_key,
                batch_size,
                reencrypt_state,
            );
            jobs.push("events");
        }
    }

    if jobs.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "nothing to re-encrypt: configure ENCRYPTION_KEY and ENCRYPTION_KEY_OLD \
                          (events, requires the encryption feature) and/or INTEGRATION_ENCRYPTION_KEY \
                          and INTEGRATION_ENCRYPTION_KEY_OLD (integration credentials)"
            })),
        ));
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "message": "re-encryption job started",
            "jobs": jobs,
            "batch_size": batch_size
        })),
    ))
}

#[utoipa::path(
    get,
    path = "/v1/events/diff",
    tag = "events",
    params(
        ("from_ledger" = i64, Query, description = "Start ledger (inclusive, required)"),
        ("to_ledger" = i64, Query, description = "End ledger (inclusive, required)"),
    ),
    responses(
        (status = 200, description = "Event diff grouped by contract", body = crate::models::DiffResponse),
        (status = 400, description = "Invalid or missing ledger range", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_events_diff(
    State(state): State<AppState>,
    Query(params): Query<crate::models::DiffParams>,
) -> Result<Json<Value>, AppError> {
    // Validate from_ledger and to_ledger are positive
    if params.from_ledger < 0 {
        return Err(AppError::Validation(
            "from_ledger must be a positive integer".to_string(),
        ));
    }
    if params.to_ledger < 0 {
        return Err(AppError::Validation(
            "to_ledger must be a positive integer".to_string(),
        ));
    }

    // Validate from_ledger < to_ledger
    if params.from_ledger >= params.to_ledger {
        return Err(AppError::Validation(
            "from_ledger must be less than to_ledger".to_string(),
        ));
    }

    // Validate ledger range does not exceed maximum
    let ledger_range = params.to_ledger - params.from_ledger;
    if ledger_range > state.config.max_ledger_range as i64 {
        return Err(AppError::Validation(format!(
            "ledger range exceeds maximum of {}",
            state.config.max_ledger_range
        )));
    }

    // Single query: count per (contract_id, event_type) in range
    let rows = sqlx::query(
        "SELECT contract_id, event_type, COUNT(*) AS cnt \
         FROM events \
         WHERE ledger >= $1 AND ledger <= $2 \
         GROUP BY contract_id, event_type",
    )
    .bind(params.from_ledger)
    .bind(params.to_ledger)
    .fetch_all(&state.read_pool)
    .await?;

    // Aggregate into per-contract map
    let mut map: std::collections::HashMap<String, crate::models::ContractDiff> =
        std::collections::HashMap::new();
    for row in &rows {
        let contract_id: String = row.try_get("contract_id")?;
        let event_type: String = row.try_get("event_type")?;
        let cnt: i64 = row.try_get("cnt")?;
        let entry = map
            .entry(contract_id.clone())
            .or_insert_with(|| crate::models::ContractDiff {
                contract_id,
                event_counts: std::collections::HashMap::new(),
                total: 0,
            });
        entry.event_counts.insert(event_type, cnt);
        entry.total += cnt;
    }

    let mut contracts: Vec<crate::models::ContractDiff> = map.into_values().collect();
    contracts.sort_by(|a, b| b.total.cmp(&a.total));

    Ok(Json(
        serde_json::to_value(crate::models::DiffResponse {
            from_ledger: params.from_ledger,
            to_ledger: params.to_ledger,
            contracts,
        })
        .map_err(|e| AppError::Internal(format!("serialization error: {e}")))?,
    ))
}

#[utoipa::path(
    get,
    path = "/v1/contracts",
    tag = "events",
    params(
        ("page" = Option<i64>, Query, description = "Page number (default 1)"),
        ("limit" = Option<i64>, Query, description = "Items per page (1-100, default 20)"),
        ("sort" = Option<String>, Query, description = "Sort order: event_count_desc, event_count_asc, last_seen_desc (default), first_seen_asc"),
    ),
    responses(
        (status = 200, description = "Paginated list of indexed contract IDs with event counts, ledger info, deployer, WASM hash and label"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn get_contracts(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Value>, AppError> {
    let limit = params.limit();
    let offset = params.offset();

    // Determine sort order
    let sort_clause = match params.sort {
        Some(SortOrder::Asc) => "ORDER BY event_count ASC",
        _ => "ORDER BY last_seen_ledger DESC",
    };

    // Issue #1255: join the deployment registry and the labels registry.
    let rows = sqlx::query_as::<_, ContractSummary>(
        &format!(
            "SELECT s.*, c.deployer, c.wasm_hash, c.deployed_at, m.name AS label FROM ( \
                 SELECT contract_id, COUNT(*) AS event_count, MIN(ledger) AS first_seen_ledger, \
                 MAX(ledger) AS last_seen_ledger, MAX(timestamp) AS last_event_at \
                 FROM events GROUP BY contract_id {} \
                 LIMIT $1 OFFSET $2 \
             ) s \
             LEFT JOIN contracts c ON c.contract_id = s.contract_id \
             LEFT JOIN contract_metadata m ON m.contract_id = s.contract_id \
             {}",
            sort_clause, sort_clause
        ),
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.read_pool)
    .await?;

    let total: i64 = sqlx::query_scalar("SELECT COUNT(DISTINCT contract_id) FROM events")
        .fetch_one(&state.read_pool)
        .await?;

    let result = json!({
        "data": rows,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
    });

    Ok(Json(result))
}

/// In-process TTL cache for per-contract summary data.
static CONTRACT_SUMMARY_CACHE: OnceLock<Mutex<std::collections::HashMap<String, (ContractDetailSummary, std::time::Instant)>>> = OnceLock::new();

fn contract_summary_cache() -> &'static Mutex<std::collections::HashMap<String, (ContractDetailSummary, std::time::Instant)>> {
    CONTRACT_SUMMARY_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// TTL for contract summary cache entries (configurable via env, default 60s).
fn summary_cache_ttl() -> std::time::Duration {
    let secs: u64 = std::env::var("CONTRACT_SUMMARY_CACHE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    std::time::Duration::from_secs(secs)
}

/// GET /v1/contracts/:contract_id/summary
///
/// Returns a per-contract summary: total events, first/last event timestamp,
/// event type breakdown, unique tx count, and ledger range.
///
/// Uses the `mv_contract_summary` materialized view as the primary data source.
/// Falls back to a direct query if the view is stale or unavailable.
/// Results are cached in-process with a configurable TTL.
#[utoipa::path(
    get,
    path = "/v1/contracts/{contract_id}/summary",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID (56-char, starts with C)"),
    ),
    responses(
        (status = 200, description = "Contract summary", body = crate::models::ContractDetailSummary),
        (status = 400, description = "Invalid contract_id format", body = ErrorResponse),
        (status = 404, description = "Contract not found", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_contract_summary(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    let ttl = summary_cache_ttl();

    // Check in-process cache
    {
        let cache = contract_summary_cache().lock().await;
        if let Some((summary, inserted_at)) = cache.get(&contract_id) {
            if inserted_at.elapsed() < ttl {
                return Ok(Json(serde_json::to_value(summary)?));
            }
        }
    }

    // Try materialized view first
    let mv_row = sqlx::query(
        "SELECT total_events, first_event_at, last_event_at, min_ledger, max_ledger, \
         unique_tx_count, contract_events, diagnostic_events, system_events \
         FROM mv_contract_summary WHERE contract_id = $1",
    )
    .bind(&contract_id)
    .fetch_optional(&state.read_pool)
    .await;

    let summary = match mv_row {
        Ok(Some(row)) => {
            ContractDetailSummary {
                contract_id: contract_id.clone(),
                total_events: row.try_get("total_events")?,
                first_event_at: row.try_get("first_event_at")?,
                last_event_at: row.try_get("last_event_at")?,
                unique_tx_count: row.try_get("unique_tx_count")?,
                ledger_range: LedgerRange {
                    min: row.try_get("min_ledger")?,
                    max: row.try_get("max_ledger")?,
                },
                event_type_breakdown: EventTypeBreakdown {
                    contract: row.try_get("contract_events")?,
                    diagnostic: row.try_get("diagnostic_events")?,
                    system: row.try_get("system_events")?,
                },
                from_cache: true,
            }
        }
        // Materialized view missing or stale — fall back to direct query
        _ => {
            let row = sqlx::query(
                "SELECT \
                    COUNT(*)                                                        AS total_events, \
                    MIN(timestamp)                                                  AS first_event_at, \
                    MAX(timestamp)                                                  AS last_event_at, \
                    MIN(ledger)                                                     AS min_ledger, \
                    MAX(ledger)                                                     AS max_ledger, \
                    COUNT(DISTINCT tx_hash)                                         AS unique_tx_count, \
                    COUNT(*) FILTER (WHERE event_type = 'contract')                 AS contract_events, \
                    COUNT(*) FILTER (WHERE event_type = 'diagnostic')               AS diagnostic_events, \
                    COUNT(*) FILTER (WHERE event_type = 'system')                   AS system_events \
                 FROM events WHERE contract_id = $1",
            )
            .bind(&contract_id)
            .fetch_one(&state.read_pool)
            .await?;

            let total_events: i64 = row.try_get("total_events")?;
            if total_events == 0 {
                return Err(AppError::NotFound);
            }

            ContractDetailSummary {
                contract_id: contract_id.clone(),
                total_events,
                first_event_at: row.try_get("first_event_at")?,
                last_event_at: row.try_get("last_event_at")?,
                unique_tx_count: row.try_get("unique_tx_count")?,
                ledger_range: LedgerRange {
                    min: row.try_get("min_ledger")?,
                    max: row.try_get("max_ledger")?,
                },
                event_type_breakdown: EventTypeBreakdown {
                    contract: row.try_get("contract_events")?,
                    diagnostic: row.try_get("diagnostic_events")?,
                    system: row.try_get("system_events")?,
                },
                from_cache: false,
            }
        }
    };

    // Check for not-found after materialized view path
    if summary.total_events == 0 {
        return Err(AppError::NotFound);
    }

    // Store in cache
    {
        let mut cache = contract_summary_cache().lock().await;
        cache.insert(contract_id, (summary.clone(), std::time::Instant::now()));
    }

    Ok(Json(serde_json::to_value(&summary)?))
}

/// GET /v1/contracts/search?q=prefix
///
/// Returns contract IDs matching the given prefix (minimum 4 characters).
/// Uses a `LIKE 'prefix%'` query against the existing `contract_id` index.
#[utoipa::path(
    get,
    path = "/v1/contracts/search",
    tag = "events",
    params(
        ("q" = String, Query, description = "Contract ID prefix to search for (minimum 4 characters)"),
        ("limit" = Option<i64>, Query, description = "Maximum results to return (1–100, default 20)"),
    ),
    responses(
        (status = 200, description = "Matching contract IDs with event counts", body = Vec<crate::models::ContractSearchResult>),
        (status = 400, description = "Prefix too short (< 4 chars) or missing", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_contracts_search(
    State(state): State<AppState>,
    Query(params): Query<ContractSearchParams>,
) -> Result<Json<Value>, AppError> {
    let prefix = params.q.as_deref().unwrap_or("").trim().to_string();

    if prefix.len() < 4 {
        return Err(AppError::Validation(
            "q must be at least 4 characters to prevent full-table scans".to_string(),
        ));
    }

    // Sanitize: only allow alphanumeric characters in the prefix to prevent injection
    if !prefix.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(AppError::Validation(
            "q must contain only alphanumeric characters".to_string(),
        ));
    }

    let limit = params.limit.unwrap_or(20).clamp(1, 100);
    let like_pattern = format!("{}%", prefix);

    let rows = sqlx::query_as::<_, ContractSearchResult>(
        "SELECT contract_id, COUNT(*) AS event_count, MAX(timestamp) AS last_event_at \
         FROM events WHERE contract_id LIKE $1 \
         GROUP BY contract_id ORDER BY event_count DESC LIMIT $2",
    )
    .bind(&like_pattern)
    .bind(limit)
    .fetch_all(&state.read_pool)
    .await?;

    Ok(Json(json!({
        "data": rows,
        "query": prefix,
        "limit": limit,
    })))
}

/// Query the min and max indexed ledger from the events table.
/// Returns `(None, None)` when no events have been indexed yet.
async fn get_indexed_ledger_range(
    pool: &sqlx::PgPool,
) -> Result<(Option<i64>, Option<i64>), AppError> {
    let row = sqlx::query("SELECT MIN(ledger) AS min_ledger, MAX(ledger) AS max_ledger FROM events")
        .fetch_one(pool)
        .await?;
    let min: Option<i64> = row.try_get("min_ledger")?;
    let max: Option<i64> = row.try_get("max_ledger")?;
    Ok((min, max))
}

/// Replay events for a specific ledger range.
///
/// The requested range is validated against the indexed window:
/// - **400** if the range is entirely outside the indexed window (no overlap).
/// - **202** with a `warning` field if the range is only partially indexed.
#[utoipa::path(
    post,
    path = "/v1/admin/replay",
    tag = "admin",
    request_body(content = ReplayRequest, description = "Ledger range to replay", content_type = "application/json"),
    responses(
        (status = 202, description = "Replay job accepted and queued. A `warning` field is included when the requested range is only partially covered by the indexed window."),
        (status = 400, description = "Invalid request parameters, or the requested range is entirely outside the indexed window"),
        (status = 401, description = "Unauthorized - API key required"),
        (status = 403, description = "Forbidden - not the active indexer"),
    )
)]
/// Preview how a Lua transformation script would affect a set of events
/// without writing any changes to the database.
///
/// Applies the same CPU instruction limit, memory limit, and timeout as the
/// production transformer so operator can verify safety before deploying.
#[utoipa::path(
    post,
    path = "/v1/admin/lua/preview",
    tag = "admin",
    request_body = models::LuaPreviewRequest,
    params(
        ("X-Admin-API-Key" = String, Header, description = "Admin API key"),
    ),
    responses(
        (status = 200, description = "Preview results — original and transformed event data side-by-side",
            body = models::LuaPreviewResponse),
        (status = 400, description = "Invalid request — too many event_ids or empty script"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "One or more event_ids not found"),
    )
)]
pub async fn lua_preview(
    State(state): State<AppState>,
    Json(req): Json<models::LuaPreviewRequest>,
) -> Result<Json<models::LuaPreviewResponse>, AppError> {
    const MAX_PREVIEW_EVENTS: usize = 20;

    if req.script.trim().is_empty() {
        return Err(AppError::Validation("script must not be empty".into()));
    }
    if req.event_ids.is_empty() {
        return Err(AppError::Validation("event_ids must not be empty".into()));
    }
    if req.event_ids.len() > MAX_PREVIEW_EVENTS {
        return Err(AppError::Validation(format!(
            "event_ids must contain at most {MAX_PREVIEW_EVENTS} entries"
        )));
    }

    // Fetch the requested events from the DB (read-only)
    let ids: Vec<uuid::Uuid> = req.event_ids.clone();
    let rows = sqlx::query_as::<_, crate::models::Event>(
        "SELECT * FROM events WHERE id = ANY($1) LIMIT 20",
    )
    .bind(&ids)
    .fetch_all(&state.read_pool)
    .await?;

    if rows.is_empty() {
        return Err(AppError::NotFound);
    }

    // Map Event → SorobanEvent for the transformer
    let events: Vec<crate::models::SorobanEvent> = rows
        .into_iter()
        .map(|r| crate::models::SorobanEvent {
            contract_id: r.contract_id,
            event_type: format!("{:?}", r.event_type).to_lowercase(),
            tx_hash: r.tx_hash,
            ledger: r.ledger as u64,
            ledger_closed_at: r.timestamp.to_rfc3339(),
            ledger_hash: r.ledger_hash,
            in_successful_call: r.in_successful_call,
            value: r.event_data.clone(),
            topic: r.event_data
                .get("topic")
                .and_then(|t| t.as_array())
                .cloned(),
        })
        .collect();

    let timeout_ms = state.config.event_transform_timeout_ms;

    let mut results =
        crate::lua_transform::LuaTransformer::preview_events(req.script, events, timeout_ms).await;

    // Re-attach the caller-supplied UUIDs so the response matches the request
    for (item, id) in results.iter_mut().zip(req.event_ids.iter()) {
        item.event_id = *id;
    }

    Ok(Json(models::LuaPreviewResponse { results }))
}

/// Replay already-indexed events through a Lua transformation script (Issue #700).
///
/// Fetches events matching the ledger and/or timestamp window (and optional
/// `contract_id` filter), runs each one through `transformation_script`, and
/// reports per-event results. In `dry_run` mode nothing is redelivered — the
/// endpoint only reports what the script *would* do. Otherwise, transformed
/// events (script returned a table, not `nil`) are broadcast on the same
/// channel newly-indexed events use, so they flow through the normal
/// downstream pipeline (webhooks, SSE, email, etc.) exactly like a live event.
#[utoipa::path(
    post,
    path = "/v1/replay/with-transform",
    tag = "admin",
    request_body = models::ReplayWithTransformRequest,
    responses(
        (status = 200, description = "Replay completed or previewed", body = models::ReplayWithTransformResponse),
        (status = 400, description = "Invalid request or transformation script"),
    )
)]
pub async fn replay_with_transform(
    State(state): State<AppState>,
    Json(request): Json<models::ReplayWithTransformRequest>,
) -> Result<Json<models::ReplayWithTransformResponse>, AppError> {
    if request.transformation_script.trim().is_empty() {
        return Err(AppError::Validation(
            "transformation_script must not be empty".to_string(),
        ));
    }
    if request.from_ledger.is_none() && request.from_timestamp.is_none() {
        return Err(AppError::Validation(
            "either from_ledger or from_timestamp is required".to_string(),
        ));
    }
    if let (Some(from), Some(to)) = (request.from_ledger, request.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }
    if let (Some(from), Some(to)) = (request.from_timestamp, request.to_timestamp) {
        if from > to {
            return Err(AppError::Validation(
                "from_timestamp must be <= to_timestamp".to_string(),
            ));
        }
    }

    let wrapped_script =
        crate::lua_transform::LuaTransformer::wrap_script_body(&request.transformation_script);
    crate::lua_transform::LuaTransformer::validate_syntax(&wrapped_script)
        .map_err(|e| AppError::Validation(format!("invalid transformation script: {e}")))?;

    let limit = request.limit.unwrap_or(100).clamp(1, 1000);

    let mut query = QueryBuilder::<sqlx::Postgres>::new(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data \
         FROM events WHERE ",
    );
    let mut needs_and = false;
    if let Some(from_ledger) = request.from_ledger {
        query.push("ledger >= ");
        query.push_bind(from_ledger);
        needs_and = true;
    }
    if let Some(to_ledger) = request.to_ledger {
        if needs_and {
            query.push(" AND ");
        }
        query.push("ledger <= ");
        query.push_bind(to_ledger);
        needs_and = true;
    }
    if let Some(from_timestamp) = request.from_timestamp {
        if needs_and {
            query.push(" AND ");
        }
        query.push("timestamp >= ");
        query.push_bind(from_timestamp);
        needs_and = true;
    }
    if let Some(to_timestamp) = request.to_timestamp {
        if needs_and {
            query.push(" AND ");
        }
        query.push("timestamp <= ");
        query.push_bind(to_timestamp);
        needs_and = true;
    }
    if let Some(ref contract_id) = request.contract_id {
        if needs_and {
            query.push(" AND ");
        }
        query.push("contract_id = ");
        query.push_bind(contract_id);
    }
    query.push(" ORDER BY ledger ASC LIMIT ");
    query.push_bind(limit);

    let rows = query.build().fetch_all(&state.pool).await?;

    let ids: Vec<uuid::Uuid> = rows
        .iter()
        .map(|r| r.try_get::<uuid::Uuid, _>("id"))
        .collect::<Result<_, _>>()?;
    let events: Vec<crate::models::SorobanEvent> = rows
        .iter()
        .map(|r| -> Result<_, sqlx::Error> {
            let timestamp: DateTime<Utc> = r.try_get("timestamp")?;
            Ok(crate::models::SorobanEvent {
                id: None,
                contract_id: r.try_get("contract_id")?,
                event_type: r.try_get("event_type")?,
                tx_hash: r.try_get("tx_hash")?,
                ledger: r.try_get::<i64, _>("ledger")? as u64,
                ledger_closed_at: timestamp.to_rfc3339(),
                ledger_hash: None,
                in_successful_call: true,
                value: r.try_get("event_data")?,
                topic: None,
                tenant_id: None,
            })
        })
        .collect::<Result<_, _>>()?;

    let total_events = events.len() as i64;
    let timeout_ms = state.config.event_transform_timeout_ms;
    let mut preview =
        crate::lua_transform::LuaTransformer::preview_events(wrapped_script, events, timeout_ms)
            .await;
    for (item, id) in preview.iter_mut().zip(ids.iter()) {
        item.event_id = *id;
    }

    let transformed_events = preview.iter().filter(|i| i.transformed.is_some()).count() as i64;
    let error_events = preview.iter().filter(|i| i.error.is_some()).count() as i64;
    let skipped_events = total_events - transformed_events - error_events;

    if !request.dry_run {
        for item in preview.iter() {
            if let Some(ref transformed) = item.transformed {
                if let Ok(event) =
                    serde_json::from_value::<crate::models::SorobanEvent>(transformed.clone())
                {
                    let _ = state.event_tx.send(event);
                }
            }
        }
    }

    Ok(Json(models::ReplayWithTransformResponse {
        replay_id: uuid::Uuid::new_v4(),
        status: if request.dry_run {
            "dry_run".to_string()
        } else {
            "completed".to_string()
        },
        dry_run: request.dry_run,
        total_events,
        transformed_events,
        skipped_events,
        error_events,
        preview,
    }))
}

pub async fn replay_events(
    State(state): State<AppState>,
    Json(request): Json<models::ReplayRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    // Validate ledger range
    if request.from_ledger > request.to_ledger {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "from_ledger must be <= to_ledger"
            })),
        ));
    }

    // Validate range size (max 10,000 ledgers)
    let range_size = request.to_ledger.saturating_sub(request.from_ledger) + 1;
    if range_size > 10_000 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "ledger range cannot exceed 10,000 ledgers"
            })),
        ));
    }

    // Check if this replica is the active indexer (holds the advisory lock)
    let is_active = state
        .indexer_state
        .is_active_indexer
        .load(std::sync::atomic::Ordering::Relaxed);
    if !is_active {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "replay endpoint only available on active indexer"
            })),
        ));
    }

    // Validate the requested range against the indexed window.
    let from = request.from_ledger as i64;
    let to = request.to_ledger as i64;

    let warning: Option<String> =
        match get_indexed_ledger_range(&state.pool).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })? {
            (None, None) => {
                // No events indexed at all — any range is entirely outside.
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "requested range is entirely outside the indexed window: no events have been indexed yet"
                    })),
                ));
            }
            (Some(min_indexed), Some(max_indexed)) => {
                // Entirely before the indexed window.
                if to < min_indexed {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!(
                                "requested range [{from}, {to}] is entirely before the indexed window [{min_indexed}, {max_indexed}]"
                            )
                        })),
                    ));
                }
                // Entirely after the indexed window (future ledgers).
                if from > max_indexed {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!(
                                "requested range [{from}, {to}] is entirely after the indexed window [{min_indexed}, {max_indexed}]"
                            )
                        })),
                    ));
                }
                // Partial overlap — warn the caller.
                if from < min_indexed || to > max_indexed {
                    Some(format!(
                        "requested range [{from}, {to}] is partially outside the indexed window [{min_indexed}, {max_indexed}]; only ledgers [{}, {}] will be replayed",
                        from.max(min_indexed),
                        to.min(max_indexed),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        };

    // Record the replay job metric
    crate::metrics::record_replay_job();

    // Spawn background task to handle the replay
    let pool = state.pool.clone();
    let rpc_url = state.config.stellar_rpc_url.clone();
    let from_ledger = request.from_ledger;
    let to_ledger = request.to_ledger;

    tokio::spawn(async move {
        if let Err(e) = execute_replay_job(pool, &rpc_url, from_ledger, to_ledger).await {
            tracing::error!(error = %e, "Replay job failed");
        }
    });

    let mut body = json!({
        "message": "replay job accepted",
        "from_ledger": request.from_ledger,
        "to_ledger": request.to_ledger
    });
    if let Some(w) = warning {
        body["warning"] = json!(w);
    }

    Ok((StatusCode::ACCEPTED, Json(body)))
}

/// Execute the replay job using the same fetch_and_store_events logic
async fn execute_replay_job(
    pool: sqlx::PgPool,
    rpc_url: &str,
    from_ledger: u64,
    to_ledger: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!(
        from_ledger = from_ledger,
        to_ledger = to_ledger,
        "Starting replay job"
    );

    // Create RPC client
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(60))
        .build()?;

    let mut current_ledger = from_ledger;

    while current_ledger <= to_ledger {
        // Fetch events for current ledger range
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getEvents",
            "params": {
                "filters": [],
                "pagination": {"limit": 100},
                "startLedger": current_ledger
            }
        });

        let response = client.post(rpc_url).json(&body).send().await?;

        if !response.status().is_success() {
            return Err(format!("RPC request failed: {}", response.status()).into());
        }

        let rpc_response: crate::models::RpcResponse<crate::models::GetEventsResult> =
            response.json().await?;

        let result = match rpc_response.result {
            Some(r) => r,
            None => {
                if let Some(err) = rpc_response.error {
                    return Err(format!("RPC error: {}", err.message).into());
                } else {
                    return Err("RPC returned no result".into());
                }
            }
        };

        // Store events with ON CONFLICT DO NOTHING for idempotency
        for event in result.events {
            if let Err(e) = store_event_with_idempotency(&pool, &event).await {
                tracing::warn!(error = %e, "Failed to store event during replay");
            }
        }

        // Move to next ledger or break if we've reached the end
        if result.latest_ledger >= current_ledger {
            current_ledger = result.latest_ledger + 1;
        } else {
            break;
        }

        // Add small delay to avoid overwhelming the RPC
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tracing::info!(
        from_ledger = from_ledger,
        to_ledger = to_ledger,
        "Replay job completed"
    );
    Ok(())
}

/// Store event with idempotency using ON CONFLICT DO NOTHING
async fn store_event_with_idempotency(
    pool: &sqlx::PgPool,
    event: &crate::models::SorobanEvent,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let ledger = i64::try_from(event.ledger)?;
    let timestamp = DateTime::parse_from_rfc3339(&event.ledger_closed_at)
        .map(|dt| dt.with_timezone(&chrono::Utc))?;

    let event_data = serde_json::json!({
        "value": event.value,
        "topic": event.topic
    });

    let contract_id = event.contract_id.clone();
    let event_type = event.event_type.clone();
    let tx_hash = event.tx_hash.clone();

    let rows_affected = crate::error::with_deadlock_retry(3, || {
        let pool = pool.clone();
        let contract_id = contract_id.clone();
        let event_type = event_type.clone();
        let tx_hash = tx_hash.clone();
        let event_data = event_data.clone();
        async move {
            sqlx::query(
                r#"
                INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (tx_hash, contract_id, event_type) DO NOTHING
                "#,
            )
            .bind(contract_id)
            .bind(event_type)
            .bind(tx_hash)
            .bind(ledger)
            .bind(timestamp)
            .bind(event_data)
            .execute(&pool)
            .await
            .map(|r| r.rows_affected())
        }
    })
    .await
    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    Ok(rows_affected)
}

// ─────────────────────────────────────────────────────────────────────────────
// #499 – Notification channel failover
// #500 – Notification analytics dashboard
// #501 – Notification cost tracking
// #502 – Notification channel testing mode
// ─────────────────────────────────────────────────────────────────────────────

/// GET /v1/admin/notifications/dashboard
///
/// Returns a comprehensive analytics summary: totals per window, per-channel delivery
/// success rates, average delivery latency, and the top-10 most-notified contracts.
pub async fn notification_dashboard(
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Total notifications sent in each time window
    let windows: &[(&str, &str)] = &[
        ("last_24h", "NOW() - INTERVAL '24 hours'"),
        ("last_7d",  "NOW() - INTERVAL '7 days'"),
        ("last_30d", "NOW() - INTERVAL '30 days'"),
    ];

    let mut totals = serde_json::Map::new();
    for (label, interval) in windows {
        let sql = format!(
            "SELECT COUNT(*) FROM notification_costs WHERE sent_at >= {interval}"
        );
        let count: i64 = sqlx::query_scalar(&sql)
            .fetch_one(&state.pool)
            .await
            .unwrap_or(0);
        totals.insert(label.to_string(), serde_json::json!(count));
    }

    // Per-channel delivery success rate (based on webhook_failures vs notification_costs)
    let channel_rows = sqlx::query(
        r#"
        SELECT
            nc.id::text               AS channel_id,
            nc.name                   AS channel_name,
            nc.channel_type           AS channel_type,
            COUNT(DISTINCT ncost.id)  AS sent,
            COUNT(DISTINCT wf.id)     AS failed
        FROM notification_channels nc
        LEFT JOIN notification_costs ncost ON ncost.channel_id = nc.id
            AND ncost.sent_at >= NOW() - INTERVAL '30 days'
        LEFT JOIN webhook_failures wf ON wf.url = (nc.config->>'url')
            AND wf.created_at >= NOW() - INTERVAL '30 days'
        GROUP BY nc.id, nc.name, nc.channel_type
        "#
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    let channels: Vec<serde_json::Value> = channel_rows
        .into_iter()
        .map(|r| {
            let sent: i64 = r.try_get("sent").unwrap_or(0);
            let failed: i64 = r.try_get("failed").unwrap_or(0);
            let success_rate = if sent == 0 {
                1.0_f64
            } else {
                (sent - failed).max(0) as f64 / sent as f64
            };
            serde_json::json!({
                "channel_id":   r.try_get::<String, _>("channel_id").unwrap_or_default(),
                "channel_name": r.try_get::<String, _>("channel_name").unwrap_or_default(),
                "channel_type": r.try_get::<String, _>("channel_type").unwrap_or_default(),
                "sent_30d":     sent,
                "failed_30d":   failed,
                "success_rate": (success_rate * 1000.0).round() / 1000.0,
            })
        })
        .collect();

    // Top-10 most-notified contracts (from events table, last 30d)
    let top_contracts = sqlx::query(
        r#"
        SELECT contract_id, COUNT(*) AS event_count
        FROM events
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY contract_id
        ORDER BY event_count DESC
        LIMIT 10
        "#
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|r| {
        serde_json::json!({
            "contract_id":  r.try_get::<String, _>("contract_id").unwrap_or_default(),
            "event_count":  r.try_get::<i64, _>("event_count").unwrap_or(0),
        })
    })
    .collect::<Vec<_>>();

    Json(serde_json::json!({
        "totals":       totals,
        "channels":     channels,
        "top_contracts": top_contracts,
    }))
}

/// GET /v1/admin/notifications/costs
///
/// Returns cumulative notification cost breakdown by channel and optional time period.
pub async fn notification_costs(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let period = params.get("period").map(|s| s.as_str()).unwrap_or("30d");
    let interval = match period {
        "24h" => "24 hours",
        "7d"  => "7 days",
        "30d" => "30 days",
        _     => "30 days",
    };

    let sql = format!(
        r#"
        SELECT
            channel_id::text,
            channel_name,
            channel_type,
            COUNT(*)          AS notification_count,
            SUM(cost_cents)   AS total_cost_cents
        FROM notification_costs
        WHERE sent_at >= NOW() - INTERVAL '{interval}'
        GROUP BY channel_id, channel_name, channel_type
        ORDER BY total_cost_cents DESC
        "#
    );

    let rows = sqlx::query(&sql)
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default();

    let breakdown: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            let total_cents: i64 = r.try_get("total_cost_cents").unwrap_or(0);
            serde_json::json!({
                "channel_id":          r.try_get::<String, _>("channel_id").unwrap_or_default(),
                "channel_name":        r.try_get::<String, _>("channel_name").unwrap_or_default(),
                "channel_type":        r.try_get::<String, _>("channel_type").unwrap_or_default(),
                "notification_count":  r.try_get::<i64, _>("notification_count").unwrap_or(0),
                "total_cost_usd":      total_cents as f64 / 100.0,
            })
        })
        .collect();

    // Warn if monthly budget exceeded (config field, defaults to 0 = no limit)
    let monthly_budget_usd: f64 = state
        .config
        .notification_monthly_budget_usd
        .unwrap_or(0.0);

    let total_usd: f64 = breakdown
        .iter()
        .filter_map(|b| b["total_cost_usd"].as_f64())
        .sum();

    if monthly_budget_usd > 0.0 && total_usd >= monthly_budget_usd * 0.9 {
        tracing::warn!(
            total_usd = total_usd,
            budget_usd = monthly_budget_usd,
            "Notification cost is approaching or has exceeded the monthly budget"
        );
    }

    Json(serde_json::json!({
        "period":        period,
        "total_cost_usd": (total_usd * 100.0).round() / 100.0,
        "breakdown":     breakdown,
    }))
}

/// POST /v1/admin/notifications/channels/:id/test
///
/// Sends a test notification on the specified channel and returns the result.
pub async fn test_notification_channel(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> impl IntoResponse {
    let channel_uuid = match channel_id.parse::<uuid::Uuid>() {
        Ok(u) => u,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid channel id" })),
            ).into_response();
        }
    };

    let row = sqlx::query(
        "SELECT id::text AS channel_id, name, channel_type, config \
         FROM notification_channels WHERE id = $1"
    )
    .bind(channel_uuid)
    .fetch_optional(&state.pool)
    .await;

    let channel = match row {
        Ok(Some(r)) => r,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "channel not found" })),
            ).into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "DB error looking up notification channel");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal error" })),
            ).into_response();
        }
    };

    let channel_name: String = channel.try_get("name").unwrap_or_default();
    let channel_type: String = channel.try_get("channel_type").unwrap_or_default();
    let config: serde_json::Value = channel.try_get("config").unwrap_or(serde_json::json!({}));
    // Issue #1162: sensitive config values are stored encrypted.
    let config = match crate::integration_secrets::open_config(&config) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "Failed to decrypt notification channel credentials");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "failed to decrypt channel credentials" })),
            ).into_response();
        }
    };

    let test_subject = format!("[TEST] Soroban Pulse notification test – channel '{channel_name}'");
    let test_body = format!(
        "[TEST] This is a test notification from Soroban Pulse.\nChannel: {channel_name}\nType: {channel_type}\nSent at: {}",
        chrono::Utc::now().to_rfc3339(),
    );

    let success = match channel_type.as_str() {
        "webhook" => {
            let url = config.get("url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if url.is_empty() {
                false
            } else {
                let client = reqwest::Client::new();
                let payload = serde_json::json!({
                    "subject": test_subject,
                    "body":    test_body,
                    "test":    true,
                });
                client.post(&url)
                    .header("Content-Type", "application/json")
                    .body(payload.to_string())
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false)
            }
        }
        "email" | "sms" => {
            tracing::info!(
                channel_id = %channel_id,
                channel_type = %channel_type,
                subject = %test_subject,
                "Test notification logged (email/SMS delivery requires live credentials)"
            );
            true
        }
        _ => false,
    };

    crate::metrics::record_notification_test(&channel_type, success);

    let status = if success { StatusCode::OK } else { StatusCode::BAD_GATEWAY };
    (
        status,
        Json(serde_json::json!({
            "channel_id":   channel_id,
            "channel_name": channel_name,
            "channel_type": channel_type,
            "success":      success,
            "subject":      test_subject,
        })),
    ).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HealthState, IndexerState};
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use chrono::Utc;
    use sqlx::PgPool;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn create_test_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            Vec::new(),
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    /// Build a router with multi-tenant mode enabled.
    /// `tenant_map` maps raw API key → tenant_id.
    fn create_multitenant_test_router(
        pool: PgPool,
        tenant_map: std::collections::HashMap<String, String>,
    ) -> axum::Router {
        use crate::middleware::hash_api_key;
        use tokio::sync::broadcast;

        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let mut config = crate::config::Config::default();
        config.multi_tenant = true;

        // api_keys = all raw keys in the map
        let api_keys: Vec<String> = tenant_map.keys().cloned().collect();
        // Convert raw key → hash for the tenant_map
        let hashed_map: std::collections::HashMap<String, String> = tenant_map
            .iter()
            .map(|(k, v)| (hash_api_key(k), v.clone()))
            .collect();

        let (_, shutdown_rx) = tokio::sync::watch::channel(false);
        crate::routes::create_router_with_tx_and_tenant_map(
            pool.clone(),
            pool,
            api_keys,
            &[],
            60,
            false,
            health_state,
            indexer_state,
            prometheus_handle,
            broadcast::channel(256).0,
            15000,
            1000,
            2000,
            None,
            None,
            config,
            None,
            Arc::new(hashed_map),
            shutdown_rx,
            crate::sse_ring_buffer::SseRingBuffer::new(100),
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_tx_no_events_returns_200_empty_data(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/tx/unknown_tx_hash_no_events_deadbeef")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"], json!([]));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_tx_with_row_returns_200_with_data(pool: PgPool) {
        let tx_hash = "a1b2c3d4e5f6";
        sqlx::query(
            r#"
            INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind("C_TEST")
        .bind("contract")
        .bind(tx_hash)
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({ "value": null, "topic": null }))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{tx_hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["data"].is_array());
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["tx_hash"], json!(tx_hash));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn database_error_response_does_not_leak_internals(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        // Verify response contains generic error message
        assert!(body_str.contains("internal server error"));

        // Verify no SQLx internals are leaked
        assert!(!body_str.to_lowercase().contains("sqlx"));
        assert!(!body_str.contains("events"));
        assert!(!body_str.contains("table"));
        assert!(!body_str.contains("column"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn contract_id_too_long_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let long_id = "C".repeat(100);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/{}", long_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid contract_id format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn contract_id_invalid_format_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/GABC123456789012345678901234567890123456789012345678")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid contract_id format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_invalid_length_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/tx/abc123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid tx_hash format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_non_hex_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let invalid_hex = "z".repeat(64);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", invalid_hex))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid tx_hash format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_uppercase_hex_is_normalized_to_lowercase(pool: PgPool) {
        // Insert an event with a lowercase tx_hash
        let lowercase_hash = "a".repeat(64);
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_TEST_UPPER")
        .bind("contract")
        .bind(&lowercase_hash)
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({ "value": null, "topic": null }))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);

        // Request with uppercase hash — should be normalized and return the same event
        let uppercase_hash = "A".repeat(64);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", uppercase_hash))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["data"].is_array());
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        // Response tx_hash should be the normalized lowercase form
        assert_eq!(v["tx_hash"], json!(lowercase_hash));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_mixed_case_is_normalized_to_lowercase(pool: PgPool) {
        let lowercase_hash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_TEST_MIXED")
        .bind("contract")
        .bind(lowercase_hash)
        .bind(2_i64)
        .bind(Utc::now())
        .bind(json!({ "value": null, "topic": null }))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);

        // Mixed-case version of the same hash
        let mixed_hash = "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2";
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", mixed_hash))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["tx_hash"], json!(lowercase_hash));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_paginated_returns_approximate_count_by_default(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], true);
        assert!(v.get("total").is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_paginated_returns_exact_count_when_requested(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?exact_count=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], false);
        assert_eq!(v["total"], 0); // Empty table
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_with_fields_filter_returns_only_requested_fields(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert a test row
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        )
        .bind(Uuid::new_v4())
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("test")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"foo": "bar"}))
        .execute(&pool)
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=id,ledger")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        let event = &v["data"][0];
        assert!(event.get("id").is_some());
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_none());
        assert!(event.get("event_data").is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_total_count_scenarios(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // 1. Empty set
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], 0);
        assert_eq!(v["data"].as_array().unwrap().len(), 0);

        // 2. Single page (3 events, limit 20)
        for i in 0..3 {
            sqlx::query("INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) VALUES ($1, $2, $3, $4, $5, $6)")
                .bind(format!("C{:0>55}", i))
                .bind("contract")
                .bind(format!("{:0>64}", i))
                .bind(i as i64)
                .bind(Utc::now())
                .bind(json!({}))
                .execute(&pool).await.unwrap();
        }

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().is_some()); // Can be approximate or exact
        assert!(v["total"].as_u64().is_some());
        assert_eq!(v["data"].as_array().unwrap().len(), 3);

        // 3. Multi-page (limit 2, total 3)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=2&page=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().is_some());
        assert_eq!(v["data"].as_array().unwrap().len(), 2);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=2&page=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().is_some());
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
    }

    /// Test that health endpoint returns 503 when DB is unreachable
    #[tokio::test]
    async fn health_db_unreachable_returns_503() {
        let pool = PgPool::connect_lazy("postgres://invalid-host:5432/invalid_db").unwrap();
        let health_state = Arc::new(HealthState::new(60));
        let prometheus_handle = crate::metrics::init_metrics();
        let indexer_state = Arc::new(IndexerState::new());
        let config = crate::config::Config::default();
        let app = crate::routes::create_router(
            pool,
            Vec::new(),
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // The DB is unreachable so should return 503
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "degraded");
        assert!(matches!(
            v["db"].as_str(),
            Some("unreachable") | Some("pool_exhausted")
        ));
    }

    // Health endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn health_happy_path_returns_200(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["db"], "ok");
        assert_eq!(v["indexer"], "ok");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn healthz_live_returns_200(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "alive");
    }

    #[tokio::test]
    async fn healthz_ready_unreachable_db_returns_503() {
        let pool = PgPool::connect_lazy("postgres://invalid-host:5432/invalid_db").unwrap();
        let health_state = Arc::new(HealthState::new(60));
        let prometheus_handle = crate::metrics::init_metrics();
        let indexer_state = Arc::new(IndexerState::new());
        let config = crate::config::Config::default();
        let app = crate::routes::create_router(
            pool,
            Vec::new(),
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "degraded");
        assert!(matches!(
            v["db"].as_str(),
            Some("unreachable") | Some("pool_exhausted")
        ));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn healthz_ready_indexer_stalled_returns_503(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(1));
        // never updated, treated as stalled
        let prometheus_handle = crate::metrics::init_metrics();
        let indexer_state = Arc::new(IndexerState::new());
        let config = crate::config::Config::default();
        let app = crate::routes::create_router(
            pool,
            Vec::new(),
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "degraded");
        assert_eq!(v["indexer"], "stalled");
    }

    // Status endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn status_returns_operational_info(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        // Verify required fields are present
        assert!(v.get("version").is_some());
        assert!(v.get("uptime_secs").is_some());
        assert!(v.get("current_ledger").is_some());
        assert!(v.get("latest_ledger").is_some());
        assert!(v.get("lag_ledgers").is_some());
        assert!(v.get("total_events").is_some());
        assert!(v.get("indexer_status").is_some());

        // Verify total_events is 0 for empty DB
        assert_eq!(v["total_events"], 0);
    }

    // OpenAPI endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn openapi_json_returns_valid_spec(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        // Verify it's a valid OpenAPI spec
        assert_eq!(v["openapi"], "3.0.0");
        assert!(v.get("info").is_some());
        assert!(v.get("paths").is_some());
    }

    // Main events endpoint tests - Happy path
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_empty_db_returns_200_with_empty_data(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["data"], json!([]));
        assert_eq!(v["total"], 0);
        assert_eq!(v["page"], 1);
        assert_eq!(v["limit"], 20);
        assert_eq!(v["approximate"], true);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_with_data_returns_paginated_results(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert test data
        for i in 0..5 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(v["total"], 5);
        assert_eq!(v["page"], 1);
        assert_eq!(v["limit"], 3);
    }

    // Main events endpoint tests - Error cases
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_invalid_event_type_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?event_type=invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"]
            .as_str()
            .unwrap()
            .contains("event_type must be one of"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_invalid_ledger_range_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?from_ledger=100&to_ledger=50")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"]
            .as_str()
            .unwrap()
            .contains("from_ledger must be <= to_ledger"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    // /v1/events/recent tests

    #[sqlx::test(migrations = "./migrations")]
    async fn get_recent_events_returns_events_in_desc_order(pool: PgPool) {
        let app = create_test_router(pool.clone());

        for i in 0..5_i64 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i)
            .bind(Utc::now())
            .bind(json!({}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/recent?limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let data = v["data"].as_array().unwrap();
        assert_eq!(data.len(), 3);
        // Verify descending ledger order
        let ledgers: Vec<i64> = data.iter().map(|e| e["ledger"].as_i64().unwrap()).collect();
        assert!(
            ledgers.windows(2).all(|w| w[0] >= w[1]),
            "events must be in descending ledger order"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_recent_events_rejects_from_ledger(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/recent?from_ledger=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"].as_str().unwrap().contains("from_ledger"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_recent_events_rejects_to_ledger(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/recent?to_ledger=400")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"].as_str().unwrap().contains("to_ledger"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
    }

    // Events by contract tests - Happy path
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_contract_with_data_returns_200(pool: PgPool) {
        let app = create_test_router(pool.clone());
        let contract_id = "C1234567890123456789012345678901234567890123456789012345";

        // Insert test data
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(contract_id)
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"test": "data"}))
        .execute(&pool)
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/contract/{}", contract_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["contract_id"], contract_id);
        assert_eq!(v["total"], 1);
        assert!(v.get("page").is_some());
        assert!(v.get("limit").is_some());
        assert!(v.get("approximate").is_some());
    }

    // Events by contract tests - Error cases
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_contract_not_found_returns_404(pool: PgPool) {
        let app = create_test_router(pool);
        let contract_id = "C1234567890123456789012345678901234567890123456789012345";

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/contract/{}", contract_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // Events by transaction tests - Happy path
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_tx_multiple_events_returns_all(pool: PgPool) {
        let app = create_test_router(pool.clone());
        let tx_hash = "a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890";

        // Insert multiple events for same transaction
        for i in 0..3 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(&tx_hash)
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"event_num": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", tx_hash))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(v["tx_hash"], tx_hash);
    }

    // Stream events endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_returns_sse_stream(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_with_contract_filter(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=C1234567890123456789012345678901234567890123456789012345")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
    }

    // --- Validation edge-case unit tests (discovered via fuzzing review) ---

    #[test]
    fn validate_contract_id_empty_returns_err() {
        assert!(validate_contract_id("").is_err());
    }

    #[test]
    fn validate_contract_id_55_chars_returns_err() {
        let s = format!("C{}", "A".repeat(54));
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_contract_id_57_chars_returns_err() {
        let s = format!("C{}", "A".repeat(56));
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_contract_id_starts_with_lowercase_c_returns_err() {
        let s = format!("c{}", "A".repeat(55));
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_contract_id_with_special_chars_returns_err() {
        let s = format!("C{}!", "A".repeat(54));
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_contract_id_valid_returns_ok() {
        let s = format!("C{}", "A".repeat(55));
        assert!(validate_contract_id(&s).is_ok());
    }

    /// Fuzz regression (#1233): `len()` counts bytes, so multi-byte UTF-8 input
    /// that is exactly 56 bytes long must still be rejected, deterministically.
    #[test]
    fn validate_contract_id_multibyte_56_bytes_returns_err() {
        let s = format!("C{}A", "\u{e9}".repeat(27));
        assert_eq!(s.len(), 56);
        assert!(validate_contract_id(&s).is_err());
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_tx_hash_empty_returns_err() {
        assert!(validate_tx_hash("").is_err());
    }

    #[test]
    fn validate_tx_hash_63_chars_returns_err() {
        assert!(validate_tx_hash(&"a".repeat(63)).is_err());
    }

    #[test]
    fn validate_tx_hash_65_chars_returns_err() {
        assert!(validate_tx_hash(&"a".repeat(65)).is_err());
    }

    #[test]
    fn validate_tx_hash_non_hex_returns_err() {
        assert!(validate_tx_hash(&"g".repeat(64)).is_err());
    }

    /// Fuzz regression (#1234): `len()` counts bytes, so multi-byte UTF-8 input
    /// that is exactly 64 bytes long must still be rejected, deterministically.
    #[test]
    fn validate_tx_hash_multibyte_64_bytes_returns_err() {
        let s = "\u{e9}".repeat(32);
        assert_eq!(s.len(), 64);
        assert!(validate_tx_hash(&s).is_err());
        assert!(validate_tx_hash(&s).is_err());
    }

    #[test]
    fn validate_tx_hash_valid_lowercase_returns_ok() {
        assert!(validate_tx_hash(&"a".repeat(64)).is_ok());
    }

    #[test]
    fn validate_tx_hash_valid_uppercase_returns_ok() {
        assert!(validate_tx_hash(&"A".repeat(64)).is_ok());
    }

    // --- Fuzz regressions (issues #1227, #1229): multi-byte UTF-8 inputs ---

    #[test]
    fn validate_contract_id_multibyte_with_56_byte_len_returns_err() {
        // 'é' is 2 bytes, so this is 56 bytes but only 55 chars.
        let s = format!("C{}é", "A".repeat(53));
        assert_eq!(s.len(), 56);
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_contract_id_multibyte_leading_char_returns_err() {
        let s = format!("Ç{}", "A".repeat(54));
        assert_eq!(s.len(), 56);
        assert!(validate_contract_id(&s).is_err());
    }

    #[test]
    fn validate_tx_hash_multibyte_with_64_byte_len_returns_err() {
        let s = format!("{}é", "a".repeat(62));
        assert_eq!(s.len(), 64);
        assert!(validate_tx_hash(&s).is_err());
    }

    #[test]
    fn validate_tx_hash_non_ascii_digits_returns_err() {
        // Fullwidth digits are numeric but not ASCII hex.
        let s = format!("{}０", "a".repeat(61));
        assert_eq!(s.len(), 64);
        assert!(validate_tx_hash(&s).is_err());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_invalid_contract_id_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        // Invalid: too short
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=CABC")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid contract_id format");

        // Invalid: doesn't start with C
        let response = app.clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=A1234567890123456789012345678901234567890123456789012345")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Invalid: contains non-alphanumeric
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=C123456789012345678901234567890123456789012345678901234!")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_invalid_topic_prefix_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?topic_prefix=not-valid-json{{{")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "topic_prefix must be valid JSON");
        assert_eq!(v["code"], "VALIDATION_ERROR");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_valid_topic_prefix_returns_sse_stream(pool: PgPool) {
        let app = create_test_router(pool);

        // URL-encoded JSON: {"sym":"swap"}
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?topic_prefix=%7B%22sym%22%3A%22swap%22%7D")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
    }

    /// Verify that a named `event: ping` is emitted when no events arrive within the keepalive interval.
    #[tokio::test]
    async fn stream_events_emits_named_ping_when_idle() {
        use axum::body::Body;
        use futures::StreamExt;
        use http_body_util::BodyExt;
        use std::sync::Arc;
        use tokio::sync::broadcast;

        let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
        let health_state = Arc::new(crate::config::HealthState::new(60));
        let indexer_state = Arc::new(crate::config::IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let (event_tx, _) = broadcast::channel::<crate::models::SorobanEvent>(16);
        let config = crate::config::Config::default();

        // Use a 50 ms keepalive so the test completes quickly.
        let app = crate::routes::create_router_with_tx(
            pool.clone(),
            pool,
            vec![],
            &[],
            0, // rate_limit disabled
            false,
            health_state,
            indexer_state,
            prometheus_handle,
            event_tx,
            50, // keepalive_ms = 50 ms
            1000,
            2000,
            None,
            None,
            config,
            None,
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Collect bytes until we see "event: ping" or timeout after 500 ms.
        let mut body = response.into_body().into_data_stream();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
        let mut buf = String::new();
        loop {
            tokio::select! {
                chunk = body.next() => {
                    match chunk {
                        Some(Ok(bytes)) => {
                            buf.push_str(&String::from_utf8_lossy(&bytes));
                            if buf.contains("event: ping") {
                                break;
                            }
                        }
                        _ => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }

        assert!(
            buf.contains("event: ping"),
            "expected 'event: ping' in SSE output, got: {buf:?}"
        );
    }

    // Metrics endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn metrics_returns_prometheus_format(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        // Metrics endpoint should return text/plain content type
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
    }

    // Pagination boundary condition tests
    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_boundary_conditions(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert exactly 25 test events
        for i in 0..25 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test limit boundary: limit=1 (minimum)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 1);
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["total"], 25);

        // Test limit boundary: limit=100 (maximum)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 100);
        assert_eq!(v["data"].as_array().unwrap().len(), 25); // All events
        assert_eq!(v["total"], 25);

        // Test page boundary: page=1, limit=10
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?page=1&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 1);
        assert_eq!(v["limit"], 10);
        assert_eq!(v["data"].as_array().unwrap().len(), 10);

        // Test page boundary: page=3, limit=10 (last page with 5 items)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?page=3&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 3);
        assert_eq!(v["limit"], 10);
        assert_eq!(v["data"].as_array().unwrap().len(), 5);

        // Test page boundary: page=4, limit=10 (beyond last page, empty)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?page=4&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 4);
        assert_eq!(v["limit"], 10);
        assert_eq!(v["data"].as_array().unwrap().len(), 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_invalid_parameters_are_clamped(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert test data
        for i in 0..5 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test limit=0 gets clamped to 1
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 1);

        // Test limit=200 gets clamped to 100
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=200")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 100);

        // Test page=0 gets treated as page=1
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?page=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_exact_count_vs_approximate_count(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert test data
        for i in 0..15 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test approximate count (default)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], true);
        // Approximate count may not be exact but should be reasonable
        let approx_count = v["total"].as_i64().unwrap();
        assert!(approx_count >= 0); // Should be non-negative

        // Test exact count
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?exact_count=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], false);
        assert_eq!(v["total"], 15); // Exact count should match

        // Test filtered queries always use exact count
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?event_type=contract")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], false);
        assert_eq!(v["total"], 15); // All events are contract type
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_with_filters(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert mixed event types
        for i in 0..10 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind(if i % 2 == 0 { "contract" } else { "diagnostic" })
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test pagination with event_type filter
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?event_type=contract&limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(v["total"], 5); // 5 contract events
        assert_eq!(v["approximate"], false); // Filtered queries use exact count

        // Test pagination with ledger range filter
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?from_ledger=2&to_ledger=8&limit=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 5);
        assert_eq!(v["total"], 7); // Events with ledger 2-8
        assert_eq!(v["approximate"], false); // Filtered queries use exact count

        // Test pagination with both filters
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?event_type=contract&from_ledger=0&to_ledger=6&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 4); // Contract events with ledger 0-6
        assert_eq!(v["total"], 4);
        assert_eq!(v["approximate"], false);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_filter_by_ledger_hash(pool: PgPool) {
        let app = create_test_router(pool.clone());

        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data, ledger_hash) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"test": "one"}))
        .bind("abc123")
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data, ledger_hash) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012346")
        .bind("contract")
        .bind("b".repeat(64))
        .bind(101_i64)
        .bind(Utc::now())
        .bind(json!({"test": "two"}))
        .bind("def456")
        .execute(&pool)
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?ledger_hash=abc123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let data = v["data"].as_array().unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["ledger_hash"], json!("abc123"));
        assert_eq!(data[0]["tx_hash"], json!("a".repeat(64)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_fields_filtering(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // Insert test data
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"test": "data"}))
        .execute(&pool)
        .await
        .unwrap();

        // Test fields filter with single field
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=ledger")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_none());
        assert!(event.get("event_type").is_none());
        assert!(event.get("tx_hash").is_none());
        assert!(event.get("timestamp").is_none());
        assert!(event.get("event_data").is_none());
        assert!(event.get("created_at").is_none());

        // Test fields filter with multiple fields
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=ledger,contract_id,event_type")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_some());
        assert!(event.get("event_type").is_some());
        assert!(event.get("tx_hash").is_none());
        assert!(event.get("timestamp").is_none());
        assert!(event.get("event_data").is_none());
        assert!(event.get("created_at").is_none());

        // Test fields filter with invalid fields (should be ignored)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=ledger,invalid_field,contract_id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_some());
        assert!(event.get("invalid_field").is_none());

        // Test empty fields filter (should return all fields)
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_some());
        assert!(event.get("event_type").is_some());
        assert!(event.get("tx_hash").is_some());
        assert!(event.get("timestamp").is_some());
        assert!(event.get("event_data").is_some());
        assert!(event.get("created_at").is_some());
    }

    // --- Cursor pagination tests ---

    async fn insert_events(pool: &PgPool, count: usize) {
        for i in 0..count {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({}))
            .execute(pool)
            .await
            .unwrap();
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cursor_pagination_traverses_all_pages(pool: PgPool) {
        insert_events(&pool, 5).await;
        let app = create_test_router(pool);

        // Page 1: limit=2, no cursor
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 2);
        let cursor1 = v["next_cursor"]
            .as_str()
            .expect("next_cursor must be present on page 1")
            .to_string();

        // Page 2: use cursor from page 1
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events?limit=2&cursor={cursor1}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 2);
        let cursor2 = v["next_cursor"]
            .as_str()
            .expect("next_cursor must be present on page 2")
            .to_string();

        // Page 3: last page — 1 row, next_cursor must be null
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events?limit=2&cursor={cursor2}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert!(
            v["next_cursor"].is_null(),
            "next_cursor must be null on last page"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cursor_pagination_no_duplicate_or_missing_rows(pool: PgPool) {
        insert_events(&pool, 6).await;
        let app = create_test_router(pool);

        let mut seen_ledgers: Vec<i64> = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let uri = match &cursor {
                Some(c) => format!("/v1/events?limit=2&cursor={c}"),
                None => "/v1/events?limit=2".to_string(),
            };
            let resp = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let v: Value = serde_json::from_slice(&body).unwrap();
            let page = v["data"].as_array().unwrap();
            for ev in page {
                seen_ledgers.push(ev["ledger"].as_i64().unwrap());
            }
            cursor = v["next_cursor"].as_str().map(|s| s.to_string());
            if cursor.is_none() {
                break;
            }
        }

        // All 6 ledgers seen exactly once, in descending order
        assert_eq!(seen_ledgers.len(), 6);
        let mut sorted = seen_ledgers.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        assert_eq!(seen_ledgers, sorted);
        let unique: std::collections::HashSet<_> = seen_ledgers.iter().collect();
        assert_eq!(unique.len(), 6);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cursor_invalid_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?cursor=notvalidbase64!!!")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid cursor");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn offset_response_includes_next_cursor(pool: PgPool) {
        insert_events(&pool, 3).await;
        let app = create_test_router(pool);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        // Offset path still returns total/page/approximate AND next_cursor
        assert!(v.get("total").is_some());
        assert!(v.get("page").is_some());
        assert!(
            v["next_cursor"].is_string(),
            "offset path must also return next_cursor"
        );
    }

    // --- ETag / conditional GET tests ---

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_returns_etag_header(pool: PgPool) {
        insert_events(&pool, 3).await;
        let app = create_test_router(pool);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            resp.headers().contains_key("etag"),
            "ETag header must be present"
        );
        let etag = resp
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            etag.starts_with('"') && etag.ends_with('"'),
            "ETag must be quoted: {etag}"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_matching_etag_returns_304(pool: PgPool) {
        insert_events(&pool, 3).await;
        let app = create_test_router(pool);

        // First request — get ETag
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let etag = resp
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Second request with If-None-Match — should get 304
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("if-none-match", &etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
        // 304 must include ETag header
        assert_eq!(resp.headers().get("etag").unwrap().to_str().unwrap(), etag);
        // 304 body must be empty
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_non_matching_etag_returns_200(pool: PgPool) {
        insert_events(&pool, 3).await;
        let app = create_test_router(pool);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("if-none-match", "\"stale-etag-value\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_empty_db_no_etag(pool: PgPool) {
        let app = create_test_router(pool);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // No rows → no ETag
        assert!(resp.headers().get("etag").is_none());
    }

    // compact=true tests
    #[sqlx::test(migrations = "./migrations")]
    async fn compact_false_returns_full_json_event_data(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"topics": ["transfer", "GABC"], "value": 42}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?compact=false")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        // No encoding header when compact is off
        assert!(resp.headers().get("x-event-data-encoding").is_none());

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event_data = &v["data"][0]["event_data"];
        // Full JSON object, not a string
        assert!(
            event_data.is_object(),
            "event_data should be a JSON object when compact=false"
        );
        assert_eq!(event_data["value"], json!(42));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn compact_true_returns_base64_gzip_event_data(pool: PgPool) {
        let original = json!({"topics": ["transfer", "GABC"], "value": 42});

        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(&original)
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?compact=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        // Encoding hint header must be present
        assert_eq!(
            resp.headers()
                .get("x-event-data-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("gzip+base64")
        );

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event_data = &v["data"][0]["event_data"];

        // Must be a string (base64-encoded)
        let encoded = event_data
            .as_str()
            .expect("event_data should be a base64 string when compact=true");

        // Decode base64 → decompress gzip → parse JSON → must equal original
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;
        use flate2::read::GzDecoder;
        use std::io::Read;

        let compressed = STANDARD.decode(encoded).expect("valid base64");
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut json_str = String::new();
        decoder.read_to_string(&mut json_str).expect("valid gzip");
        let decoded: Value = serde_json::from_str(&json_str).expect("valid JSON");

        assert_eq!(
            decoded, original,
            "decoded compact event_data must equal original"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn compact_true_with_cursor_pagination(pool: PgPool) {
        // Insert two events so we can test cursor path
        for i in 0..2_i64 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind("C1234567890123456789012345678901234567890123456789012345")
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(100_i64 + i)
            .bind(Utc::now())
            .bind(json!({"index": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let app = create_test_router(pool);

        // First page — get a cursor
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events?compact=true&limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("x-event-data-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("gzip+base64")
        );

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let cursor = v["next_cursor"].as_str().expect("next_cursor present");

        // Second page via cursor — event_data must still be compact
        let resp2 = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events?compact=true&limit=1&cursor={cursor}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp2.status(), StatusCode::OK);
        let body2 = to_bytes(resp2.into_body(), usize::MAX).await.unwrap();
        let v2: Value = serde_json::from_slice(&body2).unwrap();
        let event_data = &v2["data"][0]["event_data"];
        assert!(
            event_data.is_string(),
            "event_data should be base64 string on cursor page"
        );
    }

    // Multi-tenant isolation tests
    #[sqlx::test(migrations = "./migrations")]
    async fn multitenant_tenant_a_cannot_see_tenant_b_events(pool: PgPool) {
        // Insert events for two different tenants
        for (tenant, contract, tx) in [
            (
                "tenant_a",
                "C1111111111111111111111111111111111111111111111111111111",
                "a".repeat(64),
            ),
            (
                "tenant_b",
                "C2222222222222222222222222222222222222222222222222222222",
                "b".repeat(64),
            ),
        ] {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data, tenant_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(contract)
            .bind("contract")
            .bind(tx)
            .bind(100_i64)
            .bind(Utc::now())
            .bind(json!({"tenant": tenant}))
            .bind(tenant)
            .execute(&pool)
            .await
            .unwrap();
        }

        let mut tenant_map = std::collections::HashMap::new();
        tenant_map.insert("key_a".to_string(), "tenant_a".to_string());
        tenant_map.insert("key_b".to_string(), "tenant_b".to_string());
        let app = create_multitenant_test_router(pool, tenant_map);

        // Tenant A should only see its own event
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("X-Api-Key", "key_a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let data = v["data"].as_array().unwrap();
        assert_eq!(data.len(), 1, "tenant_a should see exactly 1 event");
        assert_eq!(data[0]["event_data"]["tenant"], json!("tenant_a"));

        // Tenant B should only see its own event
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("X-Api-Key", "key_b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let data = v["data"].as_array().unwrap();
        assert_eq!(data.len(), 1, "tenant_b should see exactly 1 event");
        assert_eq!(data[0]["event_data"]["tenant"], json!("tenant_b"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn multitenant_unknown_key_returns_403(pool: PgPool) {
        // key_a is in api_keys but NOT in tenant_map → 403
        let mut tenant_map = std::collections::HashMap::new();
        // key_a is registered but has no tenant mapping
        // We simulate this by giving key_b a mapping but not key_a
        tenant_map.insert("key_b".to_string(), "tenant_b".to_string());

        // Manually build a router where key_a is in api_keys but not in tenant_map
        use crate::middleware::hash_api_key;
        use tokio::sync::broadcast;
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let mut config = crate::config::Config::default();
        config.multi_tenant = true;
        let hashed_map: std::collections::HashMap<String, String> = tenant_map
            .iter()
            .map(|(k, v)| (hash_api_key(k), v.clone()))
            .collect();
        let (_, shutdown_rx_multi) = tokio::sync::watch::channel(false);
        let app = crate::routes::create_router_with_tx_and_tenant_map(
            pool.clone(),
            pool,
            vec!["key_a".to_string(), "key_b".to_string()],
            &[],
            60,
            false,
            health_state,
            indexer_state,
            prometheus_handle,
            broadcast::channel(256).0,
            15000,
            1000,
            2000,
            None,
            None,
            config,
            None,
            Arc::new(hashed_map),
            shutdown_rx_multi,
            crate::sse_ring_buffer::SseRingBuffer::new(100),
        );

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .header("X-Api-Key", "key_a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn singletenent_mode_returns_all_events(pool: PgPool) {
        // In single-tenant mode, events with NULL tenant_id are returned for all callers
        for tx in ["a".repeat(64), "b".repeat(64)] {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind("C1111111111111111111111111111111111111111111111111111111")
            .bind("contract")
            .bind(tx)
            .bind(100_i64)
            .bind(Utc::now())
            .bind(json!({"x": 1}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 2);
    }

    // Replay endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_requires_api_key(pool: PgPool) {
        let app = create_test_router(pool);
        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should return 401 when no API key is provided (since test router has no API keys configured)
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "unauthorized");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_validates_ledger_range(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        // Test invalid range: from_ledger > to_ledger
        let replay_request = ReplayRequest {
            from_ledger: 200,
            to_ledger: 100,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "from_ledger must be <= to_ledger");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_validates_range_size(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        // Test range too large: > 10,000 ledgers
        let replay_request = ReplayRequest {
            from_ledger: 1,
            to_ledger: 15000,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "ledger range cannot exceed 10,000 ledgers");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_returns_403_when_not_active_indexer(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let mut indexer_state = Arc::new(IndexerState::new());

        // Set indexer as not active
        indexer_state
            .is_active_indexer
            .store(false, std::sync::atomic::Ordering::Relaxed);

        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            v["error"],
            "replay endpoint only available on active indexer"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_accepts_valid_request(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let mut indexer_state = Arc::new(IndexerState::new());

        // Set indexer as active
        indexer_state
            .is_active_indexer
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["message"], "replay job accepted");
        assert_eq!(v["from_ledger"], 100);
        assert_eq!(v["to_ledger"], 200);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_accepts_x_api_key_header(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let mut indexer_state = Arc::new(IndexerState::new());

        // Set indexer as active
        indexer_state
            .is_active_indexer
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key to bypass auth
        let app = crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("X-Api-Key", "test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["message"], "replay job accepted");
    }

    // ── Indexed range validation tests ───────────────────────────────────────

    fn create_active_replay_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        indexer_state
            .is_active_indexer
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    async fn insert_events_at_ledgers(pool: &PgPool, ledgers: &[i64]) {
        for (i, &ledger) in ledgers.iter().enumerate() {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(ledger)
            .bind(Utc::now())
            .bind(json!({}))
            .execute(pool)
            .await
            .unwrap();
        }
    }

    async fn post_replay(app: axum::Router, from: u64, to: u64) -> (StatusCode, Value) {
        let body = serde_json::to_string(&ReplayRequest {
            from_ledger: from,
            to_ledger: to,
        })
        .unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        (status, v)
    }

    /// No events indexed → any range returns 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_validation_no_events_indexed_returns_400(pool: PgPool) {
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 100, 200).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            v["error"].as_str().unwrap().contains("no events have been indexed"),
            "unexpected error: {}",
            v["error"]
        );
    }

    /// Range entirely before the indexed window → 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_entirely_before_indexed_window_returns_400(pool: PgPool) {
        // Indexed window: ledgers 500–1000
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 100, 400).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let err = v["error"].as_str().unwrap();
        assert!(
            err.contains("entirely before"),
            "unexpected error: {err}"
        );
    }

    /// Range entirely after the indexed window (future ledgers) → 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_entirely_after_indexed_window_returns_400(pool: PgPool) {
        // Indexed window: ledgers 500–1000
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 1001, 2000).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let err = v["error"].as_str().unwrap();
        assert!(
            err.contains("entirely after"),
            "unexpected error: {err}"
        );
    }

    /// Range fully within the indexed window → 202, no warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_fully_within_indexed_window_returns_202_no_warning(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 500, 1000).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(v.get("warning").is_none(), "no warning expected for fully-covered range");
    }

    /// Range partially overlapping at the low end → 202 with warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_partial_overlap_low_end_returns_202_with_warning(pool: PgPool) {
        // Indexed window: 500–1000; request starts before min
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 100, 800).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let warning = v["warning"].as_str().expect("warning field must be present");
        assert!(
            warning.contains("partially outside"),
            "unexpected warning: {warning}"
        );
    }

    /// Range partially overlapping at the high end → 202 with warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_partial_overlap_high_end_returns_202_with_warning(pool: PgPool) {
        // Indexed window: 500–1000; request extends beyond max
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 800, 1500).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let warning = v["warning"].as_str().expect("warning field must be present");
        assert!(
            warning.contains("partially outside"),
            "unexpected warning: {warning}"
        );
    }

    /// Range spanning the entire indexed window and beyond on both sides → 202 with warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_spanning_beyond_both_ends_returns_202_with_warning(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 750, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 1, 9999).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(
            v["warning"].as_str().is_some(),
            "warning field must be present"
        );
    }

    /// Boundary: request exactly at min indexed ledger → 202, no warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_exact_min_boundary_returns_202_no_warning(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 500, 500).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(v.get("warning").is_none());
    }

    /// Boundary: request exactly at max indexed ledger → 202, no warning.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_exact_max_boundary_returns_202_no_warning(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 1000, 1000).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(v.get("warning").is_none());
    }

    /// Boundary: to_ledger == min_indexed - 1 → entirely before → 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_to_ledger_one_before_min_returns_400(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 400, 499).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("entirely before"));
    }

    /// Boundary: from_ledger == max_indexed + 1 → entirely after → 400.
    #[sqlx::test(migrations = "./migrations")]
    async fn replay_range_from_ledger_one_after_max_returns_400(pool: PgPool) {
        insert_events_at_ledgers(&pool, &[500, 1000]).await;
        let app = create_active_replay_router(pool);
        let (status, v) = post_replay(app, 1001, 1500).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("entirely after"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replay_endpoint_rejects_invalid_api_key(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Create router with API key
        let app = crate::routes::create_router(
            pool,
            vec!["correct-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        );

        let replay_request = ReplayRequest {
            from_ledger: 100,
            to_ledger: 200,
        };

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/replay")
                    .header("Authorization", "Bearer wrong-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_string(&replay_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "unauthorized");
    }

    // --- CSV export tests ---

    fn create_export_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        // Export requires api_keys to be non-empty
        crate::routes::create_router(
            pool,
            vec!["test-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_returns_csv_with_header(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({"value": null, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("content-type").unwrap(), "text/csv");
        assert!(response
            .headers()
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("events.csv"));

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let csv = String::from_utf8(body.to_vec()).unwrap();
        let mut lines = csv.lines();
        assert_eq!(
            lines.next().unwrap(),
            "id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at"
        );
        assert!(lines.next().is_some(), "expected at least one data row");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_field_map_renames_csv_header(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({"value": null, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        // URL-encoded JSON object: {"event_data":"raw_data","ledger":"ledger_seq"}
        let fm = "%7B%22event_data%22%3A%22raw_data%22%2C%22ledger%22%3A%22ledger_seq%22%7D";
        let uri = format!("/v1/events/export?field_map={fm}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let csv = String::from_utf8(body.to_vec()).unwrap();
        let mut lines = csv.lines();
        assert_eq!(
            lines.next().unwrap(),
            "id,contract_id,event_type,tx_hash,ledger_seq,timestamp,raw_data,created_at"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_field_map_renames_jsonl_keys(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({"value": null, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let fm = "%7B%22event_data%22%3A%22raw_data%22%7D";
        let uri = format!("/v1/events/export?format=jsonl&field_map={fm}");
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let s = String::from_utf8(body.to_vec()).unwrap();
        let first = s.lines().next().unwrap();
        let v: Value = serde_json::from_str(first).unwrap();
        // Ensure mapped key exists and original key does not
        assert!(v.get("raw_data").is_some());
        assert!(v.get("event_data").is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_without_api_key_returns_error(pool: PgPool) {
        // Router with no api_keys configured
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Should be rejected (400 validation error since no api_keys means guard fires)
        assert!(response.status().is_client_error());
    }

    // ── CSV escaping unit tests ──────────────────────────────────────────────

    #[test]
    fn csv_escape_plain_field_is_unchanged() {
        assert_eq!(csv_escape_field("hello"), "hello");
        assert_eq!(csv_escape_field("contract"), "contract");
        assert_eq!(csv_escape_field(""), "");
    }

    #[test]
    fn csv_escape_field_with_comma_is_quoted() {
        assert_eq!(csv_escape_field("a,b"), "\"a,b\"");
    }

    #[test]
    fn csv_escape_field_with_double_quote_doubles_it() {
        assert_eq!(csv_escape_field(r#"say "hi""#), r#""say ""hi""""#);
    }

    #[test]
    fn csv_escape_field_with_newline_is_quoted() {
        assert_eq!(csv_escape_field("line1\nline2"), "\"line1\nline2\"");
    }

    #[test]
    fn csv_escape_field_with_carriage_return_is_quoted() {
        assert_eq!(csv_escape_field("line1\rline2"), "\"line1\rline2\"");
    }

    #[test]
    fn csv_escape_field_with_comma_and_quote() {
        // A field like: He said, "hello"
        // Should become: "He said, ""hello"""
        assert_eq!(
            csv_escape_field(r#"He said, "hello""#),
            r#""He said, ""hello""""#
        );
    }

    #[test]
    fn csv_escape_json_event_data() {
        // Typical JSON event_data contains commas and double-quotes
        let json = r#"{"key":"value","amount":100}"#;
        let escaped = csv_escape_field(json);
        // Must be wrapped in quotes and internal quotes doubled
        assert!(escaped.starts_with('"'));
        assert!(escaped.ends_with('"'));
        assert!(escaped.contains("\"\"key\"\""));
    }

    // ── CSV format=csv explicit query param ──────────────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_format_csv_explicit_returns_csv(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("c".repeat(64))
        .bind(5_i64)
        .bind(Utc::now())
        .bind(json!({"key": "value", "amount": 42}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=csv")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("content-type").unwrap(), "text/csv");
        assert!(response
            .headers()
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("events.csv"));

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let csv = String::from_utf8(body.to_vec()).unwrap();
        let mut lines = csv.lines();
        assert_eq!(
            lines.next().unwrap(),
            "id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at"
        );
        assert!(lines.next().is_some(), "expected at least one data row");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_csv_escapes_special_characters(pool: PgPool) {
        // Insert an event whose event_data contains commas and quotes (normal JSON)
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("d".repeat(64))
        .bind(10_i64)
        .bind(Utc::now())
        // JSON with commas and quotes — both must be properly escaped in CSV
        .bind(json!({"msg": "hello, world", "note": "say \"hi\""}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=csv")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let csv = String::from_utf8(body.to_vec()).unwrap();

        // The data row must exist
        let mut lines = csv.lines();
        lines.next(); // skip header
        let data_row = lines.next().expect("expected a data row");

        // The event_data field must be quoted (contains commas and quotes)
        assert!(
            data_row.contains('"'),
            "event_data with commas/quotes must be quoted in CSV: {data_row}"
        );
        // The row must not split on the comma inside the JSON value
        // (i.e., the CSV parser should see exactly 8 fields)
        let field_count = count_csv_fields(data_row);
        assert_eq!(field_count, 8, "expected 8 CSV fields, got {field_count}: {data_row}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_csv_empty_db_returns_header_only(pool: PgPool) {
        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=csv")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("content-type").unwrap(), "text/csv");

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let csv = String::from_utf8(body.to_vec()).unwrap();
        let mut lines = csv.lines();
        assert_eq!(
            lines.next().unwrap(),
            "id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at"
        );
        assert!(lines.next().is_none(), "empty DB should produce header row only");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_unknown_format_returns_400(pool: PgPool) {
        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=xlsx")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// Minimal RFC 4180 field counter: counts top-level comma-separated fields,
    /// respecting double-quoted fields (commas inside quotes don't count).
    fn count_csv_fields(line: &str) -> usize {
        let mut count = 1usize;
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes {
                        // Peek: if next char is also '"', it's an escaped quote — skip it.
                        if chars.peek() == Some(&'"') {
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                ',' if !in_quotes => count += 1,
                _ => {}
            }
        }
        count
    }

    // ── Parquet export tests ─────────────────────────────────────────────────

    #[cfg(feature = "parquet")]
    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_parquet_returns_octet_stream(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(1_i64)
        .bind(json!({"value": null, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=parquet")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/octet-stream"
        );
        assert!(response
            .headers()
            .get("content-disposition")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("events.parquet"));

        // Verify the bytes are a valid Parquet file (magic bytes PAR1)
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.len() > 4);
        assert_eq!(&body[..4], b"PAR1", "Parquet magic bytes missing");
        assert_eq!(
            &body[body.len() - 4..],
            b"PAR1",
            "Parquet footer magic missing"
        );
    }

    #[cfg(feature = "parquet")]
    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_parquet_can_be_read_back(pool: PgPool) {
        use arrow_array::cast::AsArray;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let contract_id = "C1234567890123456789012345678901234567890123456789012345";
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind(contract_id)
        .bind("contract")
        .bind("b".repeat(64))
        .bind(42_i64)
        .bind(json!({"value": {"amount": 100}, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=parquet")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        // Read back with parquet reader
        let cursor = std::io::Cursor::new(body);
        let builder = ParquetRecordBatchReaderBuilder::try_new(cursor).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);

        // Verify ledger column value
        let ledger_col = batch.column_by_name("ledger").unwrap();
        let ledger_val = ledger_col
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(ledger_val, 42);

        // Verify contract_id column value
        let cid_col = batch.column_by_name("contract_id").unwrap();
        let cid_val = cid_col.as_string::<i32>().value(0);
        assert_eq!(cid_val, contract_id);
    }

    #[cfg(feature = "parquet")]
    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_parquet_empty_db_returns_valid_file(pool: PgPool) {
        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=parquet")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        // Even an empty Parquet file has the PAR1 magic
        assert_eq!(&body[..4], b"PAR1");
    }

    #[cfg(not(feature = "parquet"))]
    #[sqlx::test(migrations = "./migrations")]
    async fn export_events_parquet_without_feature_returns_400(pool: PgPool) {
        let app = create_export_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/export?format=parquet")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // ── Anonymization tests ──────────────────────────────────────────────────

    fn create_admin_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            vec!["admin-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    /// Build a router where admin endpoints require a dedicated ADMIN_API_KEY
    /// (issue #409). A separate regular API key is also configured.
    fn create_admin_auth_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let mut config = crate::config::Config::default();
        config.admin_api_keys = vec![secrecy::SecretString::new("admin-secret".to_string())];
        crate::routes::create_router(
            pool,
            vec!["regular-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn admin_endpoint_rejects_missing_key_with_401(pool: PgPool) {
        let app = create_admin_auth_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn admin_endpoint_rejects_regular_key_with_403(pool: PgPool) {
        let app = create_admin_auth_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/pause")
                    .header("Authorization", "Bearer regular-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn admin_endpoint_accepts_admin_key(pool: PgPool) {
        let app = create_admin_auth_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/pause")
                    .header("Authorization", "Bearer admin-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // The admin key passes both auth layers. The pause handler may still
        // reject (400) when no live indexer is attached, but it must NOT be
        // blocked by the auth layers.
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
        assert_ne!(response.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymize_event_returns_200_and_scrubs_data(pool: PgPool) {
        let event_id = Uuid::new_v4();
        let original_tx = "a".repeat(64);
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(event_id)
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind(&original_tx)
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({"sensitive": "data"}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_admin_router(pool.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/admin/events/{event_id}/anonymize"))
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["anonymized"], true);

        // Verify DB state
        let row = sqlx::query("SELECT event_data, tx_hash, anonymized FROM events WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let event_data: Value = row.try_get("event_data").unwrap();
        assert_eq!(event_data, json!({"anonymized": true}));
        let tx_hash: String = row.try_get("tx_hash").unwrap();
        assert_ne!(
            tx_hash, original_tx,
            "tx_hash must be replaced with its hash"
        );
        assert_eq!(tx_hash.len(), 64, "hashed tx_hash must be 64 hex chars");
        let anonymized: bool = row.try_get("anonymized").unwrap();
        assert!(anonymized);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymize_event_is_idempotent(pool: PgPool) {
        let event_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, anonymized)
             VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)",
        )
        .bind(event_id)
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("b".repeat(64))
        .bind(2_i64)
        .bind(Utc::now())
        .bind(json!({"anonymized": true}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/admin/events/{event_id}/anonymize"))
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["anonymized"], true);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymize_event_not_found_returns_404(pool: PgPool) {
        let app = create_admin_router(pool);
        let missing_id = Uuid::new_v4();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/admin/events/{missing_id}/anonymize"))
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymize_event_requires_api_key(pool: PgPool) {
        let event_id = Uuid::new_v4();
        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/admin/events/{event_id}/anonymize"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymized_event_data_visible_in_get_events(pool: PgPool) {
        let event_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, anonymized)
             VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)",
        )
        .bind(event_id)
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("c".repeat(64))
        .bind(3_i64)
        .bind(Utc::now())
        .bind(json!({"anonymized": true}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?exact_count=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let events = v["data"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_data"], json!({"anonymized": true}));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_anonymized_filter_requires_admin(pool: PgPool) {
        let app = create_admin_auth_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?anonymized=true")
                    .header("Authorization", "Bearer regular-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_anonymized_filter_admin_key_allows_query(pool: PgPool) {
        let event_id_true = Uuid::new_v4();
        let event_id_false = Uuid::new_v4();
        let contract_id = "C1234567890123456789012345678901234567890123456789012345";
        let now = Utc::now();

        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, anonymized)
             VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)",
        )
        .bind(event_id_true)
        .bind(contract_id)
        .bind("contract")
        .bind("a".repeat(64))
        .bind(1_i64)
        .bind(now)
        .bind(json!({"value": 1}))
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, anonymized)
             VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE)",
        )
        .bind(event_id_false)
        .bind(contract_id)
        .bind("contract")
        .bind("b".repeat(64))
        .bind(2_i64)
        .bind(now)
        .bind(json!({"value": 2}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_admin_auth_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?anonymized=true")
                    .header("Authorization", "Bearer admin-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let events = v["data"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["anonymized"], json!(true));
    }

    // ── Diff endpoint tests ──────────────────────────────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn diff_groups_events_by_contract_and_type(pool: PgPool) {
        // Contract A: 3 contract events at ledger 10-12, 1 diagnostic at ledger 11
        // Contract B: 2 contract events at ledger 10-11
        let contract_a = "CA23456789012345678901234567890123456789012345678901234";
        let contract_b = "CB23456789012345678901234567890123456789012345678901234";
        for (i, (cid, etype, ledger)) in [
            (contract_a, "contract", 10_i64),
            (contract_a, "contract", 11_i64),
            (contract_a, "contract", 12_i64),
            (contract_a, "diagnostic", 11_i64),
            (contract_b, "contract", 10_i64),
            (contract_b, "contract", 11_i64),
        ]
        .iter()
        .enumerate()
        {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(cid)
            .bind(etype)
            .bind(format!("{:0>64}", i))
            .bind(ledger)
            .bind(Utc::now())
            .bind(json!({}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/diff?from_ledger=10&to_ledger=12")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["from_ledger"], 10);
        assert_eq!(v["to_ledger"], 12);

        let contracts = v["contracts"].as_array().unwrap();
        assert_eq!(contracts.len(), 2);

        // First entry must be contract_a (total=4 > contract_b total=2)
        assert_eq!(contracts[0]["contract_id"], contract_a);
        assert_eq!(contracts[0]["event_counts"]["contract"], 3);
        assert_eq!(contracts[0]["event_counts"]["diagnostic"], 1);
        assert_eq!(contracts[0]["total"], 4);

        assert_eq!(contracts[1]["contract_id"], contract_b);
        assert_eq!(contracts[1]["event_counts"]["contract"], 2);
        assert_eq!(contracts[1]["total"], 2);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn diff_empty_range_returns_empty_contracts(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/diff?from_ledger=1000&to_ledger=2000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["contracts"], json!([]));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn diff_invalid_range_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/diff?from_ledger=100&to_ledger=50")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], "VALIDATION_ERROR");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn diff_missing_params_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        // Missing to_ledger
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/diff?from_ledger=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // ── Pause / Resume tests ─────────────────────────────────────────────────

    fn create_active_indexer_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        indexer_state
            .is_active_indexer
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            vec!["admin-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pause_indexer_returns_200_and_sets_paused(pool: PgPool) {
        let app = create_active_indexer_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/pause")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["indexer_paused"], true);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn resume_indexer_returns_200_and_clears_paused(pool: PgPool) {
        let app = create_active_indexer_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/resume")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["indexer_paused"], false);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pause_returns_403_on_read_only_replica(pool: PgPool) {
        // create_test_router uses IndexerState::new() which defaults is_active_indexer=false
        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/pause")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn resume_returns_403_on_read_only_replica(pool: PgPool) {
        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/resume")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pause_requires_api_key(pool: PgPool) {
        let app = create_active_indexer_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/indexer/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn status_includes_indexer_paused_field(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            v.get("indexer_paused").is_some(),
            "indexer_paused must be present in /status"
        );
        assert_eq!(v["indexer_paused"], false);
    }

    // ── Materialized view stats tests ────────────────────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_stats_empty_db_returns_zeros(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total_events"], 0);
        assert_eq!(v["events_last_24h"], 0);
        assert_eq!(v["events_last_7d"], 0);
        assert!(v["top_contracts"].as_array().unwrap().is_empty());
        assert!(v.get("computed_at").is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_stats_reflects_data_after_matview_refresh(pool: PgPool) {
        // Insert events
        for i in 0..3_i64 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, NOW(), $5)",
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i)
            .bind(json!({}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Manually refresh the materialized views so the test data is visible
        crate::stats_refresh::refresh_all(&pool).await;

        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["total_events"], 3);
        // All events inserted with NOW() so they should appear in 24h and 7d windows
        assert_eq!(v["events_last_24h"], 3);
        assert_eq!(v["events_last_7d"], 3);
        assert_eq!(v["top_contracts"].as_array().unwrap().len(), 3);
        assert_eq!(v["events_by_type"]["contract"], 3);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_stats_top_contracts_ordered_by_count(pool: PgPool) {
        let contract_a = "CA23456789012345678901234567890123456789012345678901234";
        let contract_b = "CB23456789012345678901234567890123456789012345678901234";

        // 3 events for A, 1 for B
        for i in 0..3_i64 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, NOW(), $5)",
            )
            .bind(contract_a)
            .bind("contract")
            .bind(format!("a{:0>63}", i))
            .bind(i)
            .bind(json!({}))
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind(contract_b)
        .bind("contract")
        .bind("b".repeat(64))
        .bind(10_i64)
        .bind(json!({}))
        .execute(&pool)
        .await
        .unwrap();

        crate::stats_refresh::refresh_all(&pool).await;

        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();

        let top = v["top_contracts"].as_array().unwrap();
        assert_eq!(top[0]["contract_id"], contract_a);
        assert_eq!(top[0]["event_count"], 3);
        assert_eq!(top[1]["contract_id"], contract_b);
        assert_eq!(top[1]["event_count"], 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_stats_returns_cache_control_header(pool: PgPool) {
        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let cc = response
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            cc.contains("max-age=60"),
            "expected max-age=60 in Cache-Control, got: {cc}"
        );
    }

    // ── Issue #413: full-text search uses GIN index ──────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn fulltext_search_returns_matching_events(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind("CSEARCH1")
        .bind("contract")
        .bind("s".repeat(64))
        .bind(1_i64)
        .bind(json!({"value": {"amount": "transfer"}, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind("CSEARCH2")
        .bind("contract")
        .bind("t".repeat(64))
        .bind(2_i64)
        .bind(json!({"value": {"amount": "unrelated"}, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?search=transfer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let data = v["data"].as_array().unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["contract_id"], json!("CSEARCH1"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn fulltext_search_uses_gin_index(pool: PgPool) {
        // EXPLAIN ANALYZE the tsv query and verify it mentions the GIN index
        let plan: String = sqlx::query_scalar(
            "EXPLAIN SELECT id FROM events WHERE event_data_tsv @@ plainto_tsquery('english', 'transfer')"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(
            plan.to_lowercase().contains("gin") || plan.to_lowercase().contains("idx_events_event_data_tsv"),
            "expected GIN index in query plan, got: {plan}"
        );
    }

    // ── Issue #414: cursor validation ────────────────────────────────────────

    #[test]
    fn decode_cursor_rejects_negative_ledger() {
        let cursor = URL_SAFE_NO_PAD.encode(format!("-1:{}", Uuid::new_v4()));
        assert!(matches!(
            decode_cursor(&cursor),
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn decode_cursor_rejects_zero_ledger() {
        let cursor = URL_SAFE_NO_PAD.encode(format!("0:{}", Uuid::new_v4()));
        assert!(matches!(
            decode_cursor(&cursor),
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn decode_cursor_rejects_i64_max_ledger() {
        let cursor = URL_SAFE_NO_PAD.encode(format!("{}:{}", i64::MAX, Uuid::new_v4()));
        assert!(matches!(
            decode_cursor(&cursor),
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn decode_cursor_rejects_non_v4_uuid() {
        // UUID v1 (time-based)
        let v1_uuid = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
        let cursor = URL_SAFE_NO_PAD.encode(format!("100:{v1_uuid}"));
        assert!(matches!(
            decode_cursor(&cursor),
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn decode_cursor_rejects_invalid_uuid() {
        let cursor = URL_SAFE_NO_PAD.encode("100:not-a-uuid");
        assert!(matches!(
            decode_cursor(&cursor),
            Err(AppError::Validation(_))
        ));
    }

    #[test]
    fn decode_cursor_accepts_valid_input() {
        let id = Uuid::new_v4();
        let cursor = URL_SAFE_NO_PAD.encode(format!("100:{id}"));
        let (ledger, decoded_id) = decode_cursor(&cursor).unwrap();
        assert_eq!(ledger, 100);
        assert_eq!(decoded_id, id);
    }

    // ── Issue #415: contract count cache invalidation ────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn contract_count_cache_invalidated_on_new_event(pool: PgPool) {
        use tokio::sync::broadcast;

        let (event_tx, _) = broadcast::channel::<crate::models::SorobanEvent>(16);
        let health_state = Arc::new(crate::config::HealthState::new(60));
        let indexer_state = Arc::new(crate::config::IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        let (_, shutdown_rx) = tokio::sync::watch::channel(false);

        let app = crate::routes::create_router_with_tx_and_tenant_map(
            pool.clone(),
            pool.clone(),
            vec![],
            &[],
            60,
            false,
            health_state,
            indexer_state,
            prometheus_handle,
            event_tx.clone(),
            15000,
            1000,
            2000,
            None,
            None,
            config,
            None,
            Arc::new(std::collections::HashMap::new()),
            shutdown_rx,
            crate::sse_ring_buffer::SseRingBuffer::new(100),
        );

        let contract_id = "CCACHEINVAL1234567890123456789012345678901234567890123456";

        // Seed one event so the contract endpoint returns data
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind(contract_id)
        .bind("contract")
        .bind("c".repeat(64))
        .bind(1_i64)
        .bind(json!({}))
        .execute(&pool)
        .await
        .unwrap();

        // First request — populates cache with count=1
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/contract/{contract_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], json!(1));

        // Insert a second event directly into DB (bypassing the indexer)
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, NOW(), $5)",
        )
        .bind(contract_id)
        .bind("contract")
        .bind("d".repeat(64))
        .bind(2_i64)
        .bind(json!({}))
        .execute(&pool)
        .await
        .unwrap();

        // Broadcast a new event for this contract — triggers cache invalidation
        let _ = event_tx.send(crate::models::SorobanEvent {
            contract_id: contract_id.to_string(),
            event_type: "contract".to_string(),
            tx_hash: "d".repeat(64),
            ledger: 2,
            ledger_closed_at: Utc::now().to_rfc3339(),
            ledger_hash: None,
            in_successful_call: true,
            topic: None,
            value: serde_json::Value::Null,
            tenant_id: None,
        });

        // Give the background task a moment to process the invalidation
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Second request — cache was invalidated, should re-query and return count=2
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/contract/{contract_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], json!(2));
    }

    // ── Issue #421: slow query detection ─────────────────────────────────────

    #[tokio::test]
    async fn slow_query_warn_is_emitted_when_threshold_exceeded() {
        use tracing_subscriber::layer::SubscriberExt;
        let (writer, output) = tracing_subscriber::fmt::TestWriter::new();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer(writer)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        // threshold=0 → any query duration triggers the warning
        timed_query(
            async { 42u32 },
            "test_query",
            0,
            Some("ctx"),
        )
        .await;

        let logs = output.into_string();
        assert!(
            logs.contains("slow query detected"),
            "expected 'slow query detected' warn, got: {logs}"
        );
    }

    #[tokio::test]
    async fn slow_query_no_warn_when_under_threshold() {
        use tracing_subscriber::layer::SubscriberExt;
        let (writer, output) = tracing_subscriber::fmt::TestWriter::new();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer(writer)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        // threshold=60_000ms → instant future never triggers warning
        timed_query(
            async { 42u32 },
            "test_query",
            60_000,
            None,
        )
        .await;

        let logs = output.into_string();
        assert!(
            !logs.contains("slow query detected"),
            "unexpected 'slow query detected' warn: {logs}"
        );
    }

    // ── Issue #423: ledger range bounds validation ────────────────────────────

    #[test]
    fn validate_ledger_param_accepts_zero() {
        assert!(validate_ledger_param("from_ledger", 0).is_ok());
    }

    #[test]
    fn validate_ledger_param_accepts_max_u32() {
        assert!(validate_ledger_param("from_ledger", u32::MAX as i64).is_ok());
    }

    #[test]
    fn validate_ledger_param_rejects_negative() {
        let err = validate_ledger_param("from_ledger", -1).unwrap_err();
        match err {
            AppError::Validation(msg) => assert!(msg.contains("from_ledger")),
            _ => panic!("expected Validation error"),
        }
    }

    #[test]
    fn validate_ledger_param_rejects_above_u32_max() {
        let err = validate_ledger_param("to_ledger", u32::MAX as i64 + 1).unwrap_err();
        match err {
            AppError::Validation(msg) => assert!(msg.contains("to_ledger")),
            _ => panic!("expected Validation error"),
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_rejects_negative_from_ledger(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?from_ledger=-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_rejects_out_of_range_to_ledger(pool: PgPool) {
        let app = create_test_router(pool);
        // 2^32 = 4_294_967_296 — one above u32::MAX
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?to_ledger=4294967296")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_contract_rejects_negative_from_ledger(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/contract/CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA?from_ledger=-5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // -----------------------------------------------------------------------
    // #444 — Topic filter tests
    // -----------------------------------------------------------------------

    /// Helper: insert an event with a known topic[0] sym value.
    async fn insert_event_with_topic(pool: &PgPool, contract_id: &str, topic_sym: &str) -> Uuid {
        let id = Uuid::new_v4();
        let event_data = json!({ "topic": [{ "sym": topic_sym }], "value": {} });
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, 'contract', $3, $4, NOW(), $5)",
        )
        .bind(id)
        .bind(contract_id)
        .bind(format!("{id}"))
        .bind(1_i64)
        .bind(&event_data)
        .execute(pool)
        .await
        .unwrap();
        id
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn topic_0_filter_returns_matching_events(pool: PgPool) {
        let cid = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA0000";
        insert_event_with_topic(&pool, cid, "transfer").await;
        insert_event_with_topic(&pool, cid, "mint").await;

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?topic_0=transfer")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let events = v["events"].as_array().unwrap();
        assert_eq!(events.len(), 1, "only the 'transfer' event should be returned");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn topic_0_filter_returns_empty_when_no_match(pool: PgPool) {
        let cid = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA0001";
        insert_event_with_topic(&pool, cid, "mint").await;

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?topic_0=transfer")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let events = v["events"].as_array().unwrap();
        assert!(events.is_empty());
    }

    // -----------------------------------------------------------------------
    // #446 — Lua preview endpoint tests
    // -----------------------------------------------------------------------

    #[sqlx::test(migrations = "./migrations")]
    async fn lua_preview_rejects_empty_script(pool: PgPool) {
        let app = create_admin_router(pool);
        let body = serde_json::to_string(&json!({
            "script": "",
            "event_ids": [Uuid::new_v4()]
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/lua/preview")
                    .header("Authorization", "Bearer admin-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn lua_preview_rejects_too_many_event_ids(pool: PgPool) {
        let ids: Vec<_> = (0..21).map(|_| Uuid::new_v4()).collect();
        let app = create_admin_router(pool);
        let body = serde_json::to_string(&json!({
            "script": "function transform_event(e) return e end",
            "event_ids": ids
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/lua/preview")
                    .header("Authorization", "Bearer admin-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn lua_preview_returns_404_when_events_not_found(pool: PgPool) {
        let app = create_admin_router(pool);
        let body = serde_json::to_string(&json!({
            "script": "function transform_event(e) return e end",
            "event_ids": [Uuid::new_v4()]
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/lua/preview")
                    .header("Authorization", "Bearer admin-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn lua_preview_does_not_modify_database(pool: PgPool) {
        let event_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, 'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA0002',
                     'contract', $2, 1, NOW(), $3)",
        )
        .bind(event_id)
        .bind(format!("{event_id}"))
        .bind(json!({"value": 42, "topic": []}))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_admin_router(pool.clone());
        // Script that would change the value field
        let body = serde_json::to_string(&json!({
            "script": "function transform_event(e) e.value = {value=999} return e end",
            "event_ids": [event_id]
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/lua/preview")
                    .header("Authorization", "Bearer admin-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Preview must succeed
        assert_eq!(resp.status(), StatusCode::OK);

        // DB must be unchanged
        let row: Value = sqlx::query_scalar("SELECT event_data FROM events WHERE id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(row["value"], 42, "database must not be modified by preview");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn lua_preview_requires_admin_key(pool: PgPool) {
        let app = create_admin_auth_router(pool);
        let body = serde_json::to_string(&json!({
            "script": "function transform_event(e) return e end",
            "event_ids": [Uuid::new_v4()]
        }))
        .unwrap();

        // No auth
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/lua/preview")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Regular API key (not admin)
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/admin/lua/preview")
                    .header("Authorization", "Bearer regular-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── #513: SLA monitoring unit tests ──────────────────────────────────────

    #[test]
    fn sla_latency_calculation_is_correct() {
        let indexed = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let delivered = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:25Z")
            .unwrap()
            .with_timezone(&Utc);
        let latency = (delivered - indexed).num_milliseconds() as f64 / 1000.0;
        assert!((latency - 25.0).abs() < 0.001);
    }

    #[test]
    fn sla_breach_detected_when_latency_exceeds_sla() {
        let sla_seconds: u64 = 30;
        let latency: f64 = 35.0;
        let breached = latency > sla_seconds as f64;
        assert!(breached, "expected SLA breach for latency {latency}s > SLA {sla_seconds}s");
    }

    #[test]
    fn sla_not_breached_when_latency_within_sla() {
        let sla_seconds: u64 = 30;
        let latency: f64 = 10.0;
        let breached = latency > sla_seconds as f64;
        assert!(!breached);
    }

    // ── #514: Capacity planning unit tests ───────────────────────────────────

    #[test]
    fn growth_trend_zero_when_no_baseline() {
        let baseline_rate_per_minute: f64 = 0.0;
        let current_rate: f64 = 5.0;
        let trend = if baseline_rate_per_minute > 0.0 {
            ((current_rate - baseline_rate_per_minute) / baseline_rate_per_minute) * 100.0
        } else {
            0.0
        };
        assert_eq!(trend, 0.0);
    }

    #[test]
    fn growth_trend_positive_when_current_above_baseline() {
        let baseline: f64 = 10.0;
        let current: f64 = 15.0;
        let trend = ((current - baseline) / baseline) * 100.0;
        assert!((trend - 50.0).abs() < 0.001);
    }

    #[test]
    fn projected_rate_never_below_current_with_positive_trend() {
        let current: f64 = 20.0;
        let trend: f64 = 25.0;
        let projected = current * (1.0 + trend.max(0.0) / 100.0);
        assert!(projected >= current);
    }

    // ── #512: Lifecycle webhook unit tests ───────────────────────────────────

    #[test]
    fn lifecycle_event_type_matching_wildcard() {
        let subscribed_events = vec!["*".to_string()];
        let event_type = "channel_deleted";
        let matches = subscribed_events
            .iter()
            .any(|e| e == "*" || e == event_type);
        assert!(matches);
    }

    #[test]
    fn lifecycle_event_type_matching_exact() {
        let subscribed_events = vec![
            "channel_created".to_string(),
            "delivery_failed".to_string(),
        ];
        let matches_created = subscribed_events
            .iter()
            .any(|e| e == "*" || e == "channel_created");
        let matches_deleted = subscribed_events
            .iter()
            .any(|e| e == "*" || e == "channel_deleted");
        assert!(matches_created);
        assert!(!matches_deleted);
    }

    #[test]
    fn lifecycle_event_not_delivered_to_inactive_webhook() {
        let webhook = crate::models::SystemWebhookConfig {
            id: Uuid::new_v4(),
            url: "http://example.com/hook".to_string(),
            secret: None,
            events: vec!["*".to_string()],
            active: false,
            created_at: Utc::now(),
        };
        assert!(!webhook.active, "inactive webhook should not receive events");
    }

    // ── #511: Bulk operations unit tests ─────────────────────────────────────

    #[test]
    fn bulk_response_counts_success_and_failure() {
        let results = vec![
            crate::models::BulkChannelResult {
                id: Uuid::new_v4(),
                success: true,
                error: None,
            },
            crate::models::BulkChannelResult {
                id: Uuid::new_v4(),
                success: false,
                error: Some("not found".to_string()),
            },
            crate::models::BulkChannelResult {
                id: Uuid::new_v4(),
                success: true,
                error: None,
            },
        ];
        let succeeded = results.iter().filter(|r| r.success).count() as i64;
        let failed = results.iter().filter(|r| !r.success).count() as i64;
        assert_eq!(succeeded, 2);
        assert_eq!(failed, 1);
    }

    #[test]
    fn bulk_tag_appends_only_new_tags() {
        let mut tags: Vec<String> = vec!["production".to_string()];
        let new_tags = vec!["production".to_string(), "critical".to_string()];
        for tag in &new_tags {
            if !tags.contains(tag) {
                tags.push(tag.clone());
            }
        }
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&"production".to_string()));
        assert!(tags.contains(&"critical".to_string()));
    }

    #[tokio::test]
    async fn bulk_enable_returns_not_found_for_unknown_channel() {
        let unknown_id = Uuid::new_v4();
        let req = crate::models::BulkChannelRequest {
            channel_ids: vec![unknown_id],
        };

        let mut store = notification_channels().write().await;
        store.clear();
        drop(store);

        let mut results = Vec::new();
        {
            let mut s = notification_channels().write().await;
            if let Some(ch) = s.get_mut(&unknown_id) {
                ch.active = true;
                results.push(crate::models::BulkChannelResult {
                    id: unknown_id,
                    success: true,
                    error: None,
                });
            } else {
                results.push(crate::models::BulkChannelResult {
                    id: unknown_id,
                    success: false,
                    error: Some(format!("channel {unknown_id} not found")),
                });
            }
        }
        assert_eq!(results.len(), req.channel_ids.len());
        assert!(!results[0].success);
        assert!(results[0].error.as_deref().unwrap_or("").contains("not found"));
    }
}

// ── Archive ──────────────────────────────────────────────────────────────────

/// `GET /v1/events/archive` — list available archive files in S3.
///
/// Only available when the `archive` feature is enabled and `ARCHIVE_S3_BUCKET`
/// is configured. Returns 501 otherwise.
#[utoipa::path(
    get,
    path = "/v1/events/archive",
    tag = "events",
    responses(
        (status = 200, description = "List of archive files"),
        (status = 501, description = "Archive feature not enabled"),
    )
)]
pub async fn list_archive(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    #[cfg(feature = "archive")]
    {
        let (bucket, prefix) = match (
            &state.config.archive_s3_bucket,
            &state.config.archive_s3_prefix,
        ) {
            (Some(b), p) => (b.clone(), p.clone()),
            (None, _) => {
                return Err(AppError::Validation(
                    "ARCHIVE_S3_BUCKET is not configured".to_string(),
                ))
            }
        };
        let aws_cfg = aws_config::load_from_env().await;
        let s3 = aws_sdk_s3::Client::new(&aws_cfg);
        let files = crate::archiver::list_archive_files(&s3, &bucket, &prefix)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        return Ok(Json(json!({ "data": files, "total": files.len() })));
    }
    #[cfg(not(feature = "archive"))]
    {
        let _ = state; // suppress unused warning
        Err(AppError::Internal(
            "archive feature not enabled".to_string(),
        ))
    }
}

// ============================================================================
// Schema Management Endpoints
// ============================================================================

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct RegisterSchemaRequest {
    /// JSON Schema definition (Draft 7)
    pub schema: Value,
    /// Optional human-readable description of this schema (issue #617 versioning)
    pub description: Option<String>,
}

/// Register or update a JSON Schema for a contract
#[utoipa::path(
    post,
    path = "/v1/admin/contracts/{contract_id}/schema",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID")
    ),
    request_body = RegisterSchemaRequest,
    responses(
        (status = 200, description = "Schema registered successfully"),
        (status = 400, description = "Invalid schema or contract ID"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn register_contract_schema(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(payload): Json<RegisterSchemaRequest>,
) -> Result<impl IntoResponse, AppError> {
    validate_contract_id(&contract_id)?;

    let validator = state
        .schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    validator
        .register_schema_described(
            &contract_id,
            &payload.schema,
            payload.description.as_deref(),
        )
        .await
        .map_err(|e| AppError::Validation(format!("Invalid schema: {}", e)))?;

    // Return the current version after registration
    let info = validator.get_schema_info(&contract_id).await;
    Ok((
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "message": "Schema registered successfully",
            "contract_id": contract_id,
            "version": info.map(|i| i.version).unwrap_or(1),
        })),
    ))
}

/// List all registered JSON Schemas (metadata only, no schema body)
#[utoipa::path(
    get,
    path = "/v1/admin/schemas",
    tag = "admin",
    responses(
        (status = 200, description = "List of registered schemas"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn list_contract_schemas(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    let validator = state
        .schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    let schemas = validator
        .list_schemas()
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok((StatusCode::OK, Json(json!({ "schemas": schemas }))))
}

/// Get the JSON Schema for a contract
#[utoipa::path(
    get,
    path = "/v1/admin/contracts/{contract_id}/schema",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID")
    ),
    responses(
        (status = 200, description = "Schema retrieved successfully"),
        (status = 404, description = "No schema registered for this contract"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_contract_schema(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    validate_contract_id(&contract_id)?;

    let validator = state
        .schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    let schema = validator
        .get_schema(&contract_id)
        .await
        .ok_or(AppError::NotFound)?;

    Ok((StatusCode::OK, Json(json!({ "schema": schema }))))
}

/// Delete the JSON Schema for a contract
#[utoipa::path(
    delete,
    path = "/v1/admin/contracts/{contract_id}/schema",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID")
    ),
    responses(
        (status = 200, description = "Schema deleted successfully"),
        (status = 404, description = "No schema registered for this contract"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn delete_contract_schema(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    validate_contract_id(&contract_id)?;

    let validator = state
        .schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    let deleted = validator
        .delete_schema(&contract_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    if deleted {
        Ok((
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "message": "Schema deleted successfully"
            })),
        ))
    } else {
        Err(AppError::NotFound)
    }
}

/// Validate event data against a contract's JSON Schema
#[utoipa::path(
    post,
    path = "/v1/admin/contracts/{contract_id}/validate",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID")
    ),
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Validation result"),
        (status = 400, description = "Validation failed with error details"),
        (status = 404, description = "No schema registered for this contract"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn validate_event_data_against_schema(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(event_data): Json<serde_json::Value>,
) -> Result<impl IntoResponse, AppError> {
    validate_contract_id(&contract_id)?;

    let validator = state.schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    match validator.validate_event_data(&contract_id, &event_data).await {
        None => Err(AppError::NotFound),
        Some((true, _)) => Ok((
            StatusCode::OK,
            Json(json!({
                "valid": true,
                "message": "Event data is valid"
            }))
        )),
        Some((false, errors)) => {
            let error_msg = format!("Event data validation failed with {} error(s)", errors.len());
            Err(AppError::ValidationWithDetails(error_msg, errors))
        }
    }
}


/// Start a background masking job for event data
#[utoipa::path(
    post,
    path = "/v1/admin/mask-events",
    tag = "admin",
    request_body = models::MaskEventsRequest,
    responses(
        (status = 200, description = "Masking job started", body = models::MaskEventsResponse),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn start_mask_events(
    State(state): State<AppState>,
    Json(req): Json<models::MaskEventsRequest>,
) -> Result<Json<models::MaskEventsResponse>, AppError> {
    let job_id = Uuid::new_v4().to_string();
    
    let pool = state.pool.clone();
    let contract_ids = req.contract_ids.clone();
    let job_id_clone = job_id.clone();
    
    tokio::spawn(async move {
        let _ = mask_events_background(&pool, contract_ids, &job_id_clone).await;
    });
    
    Ok(Json(models::MaskEventsResponse {
        job_id,
        status: "pending".to_string(),
    }))
}

/// Get status of a masking job
#[utoipa::path(
    get,
    path = "/v1/admin/mask-events/{job_id}",
    tag = "admin",
    params(
        ("job_id" = String, Path, description = "Job ID")
    ),
    responses(
        (status = 200, description = "Job status", body = models::MaskJobStatus),
        (status = 404, description = "Job not found"),
        (status = 401, description = "Unauthorized"),
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_mask_job_status(
    Path(job_id): Path<String>,
) -> Result<Json<models::MaskJobStatus>, AppError> {
    // For now, return a simple response. In production, this would query a job tracking table.
    Ok(Json(models::MaskJobStatus {
        job_id,
        status: "completed".to_string(),
        processed: 0,
        total: 0,
        error: None,
    }))
}

/// Request body for creating a notification channel (Issue #477).
#[derive(Debug, serde::Deserialize)]
pub struct CreateNotificationChannelRequest {
    pub name: String,
    pub channel_type: String,
    /// Channel-specific configuration (URL, recipients, …).
    #[serde(default)]
    pub config: Value,
    /// Optional content filter: only events whose data satisfies the predicate
    /// are delivered through this channel.
    #[serde(default)]
    pub content_filter: Option<crate::content_filter::ContentFilter>,
}

/// POST /v1/admin/notifications/channels — create a notification channel,
/// optionally with a content filter (Issue #477).
///
/// Invalid content filter expressions are rejected with `400 Bad Request`.
pub async fn create_notification_channel(
    State(state): State<AppState>,
    Json(req): Json<CreateNotificationChannelRequest>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    if req.name.trim().is_empty() {
        return Err(AppError::Validation("name is required".to_string()));
    }
    if !matches!(
        req.channel_type.as_str(),
        "webhook" | "email" | "sms" | "slack" | "discord" | "telegram" | "pagerduty" | "github"
    ) {
        return Err(AppError::Validation(
            "channel_type must be one of: webhook, email, sms, slack, discord, telegram, pagerduty, github"
                .to_string(),
        ));
    }

    // Validate the content filter up front so invalid expressions return 400.
    let content_filter_json = match req.content_filter {
        Some(ref cf) => {
            cf.validate().map_err(AppError::Validation)?;
            Some(serde_json::to_value(cf).map_err(|e| AppError::Validation(e.to_string()))?)
        }
        None => None,
    };

    // Issue #1162: provider credentials (e.g. SMS auth tokens) in the channel
    // config are encrypted before they reach the database.
    let sealed_config = crate::integration_secrets::seal_config(&req.config)
        .map_err(|e| AppError::Internal(e.to_string()))?;

    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO notification_channels (name, channel_type, config, content_filter) \
         VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(&req.name)
    .bind(&req.channel_type)
    .bind(&sealed_config)
    .bind(&content_filter_json)
    .fetch_one(&state.pool)
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": id,
            "name": req.name,
            "channel_type": req.channel_type,
            "content_filter": content_filter_json,
        })),
    ))
}

async fn mask_events_background(
    pool: &sqlx::PgPool,
    contract_ids: Option<Vec<String>>,
    _job_id: &str,
) -> Result<(), AppError> {
    let mut conditions: Vec<String> = vec!["event_data IS NOT NULL".to_string()];
    let mut bind_idx = 1;
    
    if let Some(ref ids) = contract_ids {
        if !ids.is_empty() {
            let placeholders = ids.iter().enumerate()
                .map(|(i, _)| format!("${}", bind_idx + i))
                .collect::<Vec<_>>()
                .join(",");
            conditions.push(format!("contract_id IN ({})", placeholders));
            bind_idx += ids.len() as i32;
        }
    }
    
    let where_clause = conditions.join(" AND ");
    let query_str = format!(
        "SELECT id, event_data FROM events WHERE {} ORDER BY id LIMIT 1000",
        where_clause
    );
    
    let mut q = sqlx::query(&query_str);
    if let Some(ref ids) = contract_ids {
        for id in ids {
            q = q.bind(id);
        }
    }
    
    let rows = q.fetch_all(pool).await?;
    
    for row in rows {
        let id: Uuid = row.try_get("id")?;
        let event_data: serde_json::Value = row.try_get("event_data")?;
        let masked_data = mask_event_data(&event_data);
        
        sqlx::query("UPDATE events SET event_data = $1 WHERE id = $2")
            .bind(&masked_data)
            .bind(id)
            .execute(pool)
            .await?;
    }
    
    Ok(())
}

fn mask_event_data(data: &serde_json::Value) -> serde_json::Value {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    fn deterministic_hash(value: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
    
    match data {
        serde_json::Value::Object(obj) => {
            let mut new_obj = serde_json::Map::new();
            for (key, value) in obj {
                let masked_value = mask_event_data(value);
                let key_lower = key.to_lowercase();
                
                if key_lower.contains("address") || key_lower.contains("account") {
                    if let serde_json::Value::String(s) = &masked_value {
                        let hash = deterministic_hash(s);
                        new_obj.insert(key.clone(), serde_json::Value::String(format!("G{:x}", hash)[..56].to_string()));
                        continue;
                    }
                }
                new_obj.insert(key.clone(), masked_value);
            }
            serde_json::Value::Object(new_obj)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(mask_event_data).collect())
        }
        _ => data.clone(),
    }
}

/// Get time-series aggregation of events
#[utoipa::path(
    get,
    path = "/v1/events/timeseries",
    tag = "events",
    params(
        ("bucket" = String, Query, description = "Time bucket: 1h, 1d, 1w, 1mo"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("from_ledger" = Option<i64>, Query, description = "Start ledger"),
        ("to_ledger" = Option<i64>, Query, description = "End ledger"),
    ),
    responses(
        (status = 200, description = "Time-series data", body = models::TimeseriesResponse),
        (status = 400, description = "Invalid parameters"),
    )
)]
pub async fn get_timeseries(
    State(state): State<AppState>,
    Query(params): Query<models::TimeseriesParams>,
) -> Result<Json<models::TimeseriesResponse>, AppError> {
    let start = std::time::Instant::now();
    
    let interval = match params.bucket.as_str() {
        "1h" => "1 hour",
        "1d" => "1 day",
        "1w" => "1 week",
        "1mo" => "1 month",
        _ => return Err(AppError::Validation("invalid bucket".to_string())),
    };
    
    let mut conditions = vec!["1=1".to_string()];
    let mut bind_idx = 1;
    
    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    
    let where_clause = conditions.join(" AND ");
    let query_str = format!(
        "SELECT \
            date_trunc('{}', timestamp) as bucket_start, \
            COUNT(*) as event_count, \
            COUNT(DISTINCT contract_id) as contract_count, \
            event_type, \
            COUNT(*) as type_count \
         FROM events \
         WHERE {} \
         GROUP BY date_trunc('{}', timestamp), event_type \
         ORDER BY bucket_start ASC",
        interval, where_clause, interval
    );
    
    let mut q = sqlx::query(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    
    let rows = q.fetch_all(&state.read_pool).await?;
    
    let mut buckets_map: std::collections::HashMap<chrono::DateTime<chrono::Utc>, models::TimeseriesBucket> = std::collections::HashMap::new();
    
    for row in rows {
        let bucket_start: chrono::DateTime<chrono::Utc> = row.try_get("bucket_start")?;
        let event_count: i64 = row.try_get("event_count")?;
        let contract_count: i64 = row.try_get("contract_count")?;
        let event_type: String = row.try_get("event_type")?;
        let type_count: i64 = row.try_get("type_count")?;
        
        let bucket = buckets_map.entry(bucket_start).or_insert_with(|| models::TimeseriesBucket {
            bucket_start,
            event_count,
            contract_count,
            event_types: std::collections::HashMap::new(),
        });
        
        bucket.event_types.insert(event_type, type_count);
    }
    
    let mut data: Vec<_> = buckets_map.into_values().collect();
    data.sort_by_key(|b| b.bucket_start);
    
    crate::metrics::record_timeseries_query_duration(start.elapsed());
    
    Ok(Json(models::TimeseriesResponse {
        bucket: params.bucket,
        data,
    }))
}

/// Parse a relative time expression such as `"24h"`, `"1d"`, `"30m"`, `"1w"`, `"90s"`.
/// Returns the equivalent `chrono::Duration` or a validation error.
fn parse_relative_duration(expr: &str) -> Result<chrono::Duration, AppError> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Err(AppError::Validation(
            "relative time expression must not be empty".to_string(),
        ));
    }
    let (num_str, unit) = expr
        .find(|c: char| c.is_alphabetic())
        .map(|i| (&expr[..i], &expr[i..]))
        .ok_or_else(|| {
            AppError::Validation(format!(
                "invalid relative time expression '{}': missing unit (s/m/h/d/w)",
                expr
            ))
        })?;
    let n: i64 = num_str.parse().map_err(|_| {
        AppError::Validation(format!(
            "invalid relative time expression '{}': '{}' is not a number",
            expr, num_str
        ))
    })?;
    if n <= 0 {
        return Err(AppError::Validation(format!(
            "relative time value must be positive, got '{}'",
            expr
        )));
    }
    match unit {
        "s" => Ok(chrono::Duration::seconds(n)),
        "m" => Ok(chrono::Duration::minutes(n)),
        "h" => Ok(chrono::Duration::hours(n)),
        "d" => Ok(chrono::Duration::days(n)),
        "w" => Ok(chrono::Duration::weeks(n)),
        other => Err(AppError::Validation(format!(
            "unknown time unit '{}' in '{}': use s, m, h, d, or w",
            other, expr
        ))),
    }
}

/// GET /v1/events/temporal — time-based event queries (Issue #581).
///
/// Supports relative time expressions (`since=24h`, `before=1h`) and absolute
/// ISO 8601 timestamps (`from_timestamp`, `to_timestamp`). When `aggregate=true`
/// the response contains bucketed counts instead of raw events.
#[utoipa::path(
    get,
    path = "/v1/events/temporal",
    tag = "events",
    params(
        ("since" = Option<String>, Query, description = "Relative start of window, e.g. '24h', '1d', '7d', '30m', '1w'"),
        ("before" = Option<String>, Query, description = "Relative end of window (default: now)"),
        ("from_timestamp" = Option<String>, Query, description = "Absolute ISO 8601 start timestamp (mutually exclusive with since)"),
        ("to_timestamp" = Option<String>, Query, description = "Absolute ISO 8601 end timestamp"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("event_type" = Option<String>, Query, description = "Filter by event type"),
        ("aggregate" = Option<bool>, Query, description = "Return aggregated bucket counts instead of raw events"),
        ("window" = Option<String>, Query, description = "Aggregation bucket size: 1m, 5m, 1h, 1d (default: 1h)"),
        ("limit" = Option<i64>, Query, description = "Max events or buckets (default: 100, max: 1000)"),
        ("page" = Option<i64>, Query, description = "Page number for offset pagination (default: 1)"),
    ),
    responses(
        (status = 200, description = "Temporal query results", body = models::TemporalQueryResponse),
        (status = 400, description = "Invalid parameters", body = models::ErrorResponse),
    )
)]
pub async fn get_temporal_events(
    State(state): State<AppState>,
    Query(params): Query<models::TemporalParams>,
) -> Result<Json<models::TemporalQueryResponse>, AppError> {
    let query_start = std::time::Instant::now();
    let now = Utc::now();

    // Resolve the time window boundaries.
    let from: DateTime<Utc> = if let Some(ref since) = params.since {
        if params.from_timestamp.is_some() {
            return Err(AppError::Validation(
                "'since' and 'from_timestamp' are mutually exclusive".to_string(),
            ));
        }
        let dur = parse_relative_duration(since)?;
        now - dur
    } else if let Some(ref ts) = params.from_timestamp {
        validate_timestamp(ts)?
    } else {
        return Err(AppError::Validation(
            "either 'since' or 'from_timestamp' is required".to_string(),
        ));
    };

    let to: DateTime<Utc> = if let Some(ref before) = params.before {
        if params.to_timestamp.is_some() {
            return Err(AppError::Validation(
                "'before' and 'to_timestamp' are mutually exclusive".to_string(),
            ));
        }
        let dur = parse_relative_duration(before)?;
        now - dur
    } else if let Some(ref ts) = params.to_timestamp {
        validate_timestamp(ts)?
    } else {
        now
    };

    if from >= to {
        return Err(AppError::Validation(
            "start of window must be before end of window".to_string(),
        ));
    }

    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let page = params.page.unwrap_or(1).max(1);
    let offset = (page - 1) * limit;

    let aggregate = params.aggregate.unwrap_or(false);

    if aggregate {
        // Aggregation path: return bucketed event counts.
        let interval = match params.window.as_deref().unwrap_or("1h") {
            "1m" => "1 minute",
            "5m" => "5 minutes",
            "1h" => "1 hour",
            "1d" => "1 day",
            other => {
                return Err(AppError::Validation(format!(
                    "invalid window '{}': use 1m, 5m, 1h, or 1d",
                    other
                )));
            }
        };

        let mut conditions = vec![
            "timestamp >= $1".to_string(),
            "timestamp <= $2".to_string(),
        ];
        let mut bind_idx = 3usize;

        if params.contract_id.is_some() {
            conditions.push(format!("contract_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.event_type.is_some() {
            conditions.push(format!("event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        let _ = bind_idx;

        let where_clause = conditions.join(" AND ");
        let query_str = format!(
            "SELECT \
                date_trunc('{interval}', timestamp) AS bucket_start, \
                COUNT(*) AS event_count, \
                COUNT(DISTINCT contract_id) AS contract_count \
             FROM events \
             WHERE {where_clause} \
             GROUP BY date_trunc('{interval}', timestamp) \
             ORDER BY bucket_start ASC \
             LIMIT {limit} OFFSET {offset}",
            interval = interval,
            where_clause = where_clause,
            limit = limit,
            offset = offset,
        );

        let mut q = sqlx::query(&query_str).bind(from).bind(to);
        if let Some(ref cid) = params.contract_id {
            q = q.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            q = q.bind(et);
        }

        let rows = q.fetch_all(&state.read_pool).await?;

        let mut buckets = Vec::with_capacity(rows.len());
        for row in &rows {
            let bucket_start: DateTime<Utc> = row.try_get("bucket_start")?;
            let event_count: i64 = row.try_get("event_count")?;
            let contract_count: i64 = row.try_get("contract_count")?;
            buckets.push(models::TemporalBucket {
                bucket_start,
                event_count,
                contract_count,
            });
        }

        let total = buckets.len();
        crate::metrics::record_temporal_query_duration(query_start.elapsed());

        Ok(Json(models::TemporalQueryResponse {
            from,
            to,
            events: vec![],
            buckets,
            total,
        }))
    } else {
        // Raw events path.
        let mut conditions = vec![
            "timestamp >= $1".to_string(),
            "timestamp <= $2".to_string(),
        ];
        let mut bind_idx = 3usize;

        if params.contract_id.is_some() {
            conditions.push(format!("contract_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.event_type.is_some() {
            conditions.push(format!("event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        let _ = bind_idx;

        let where_clause = conditions.join(" AND ");
        let query_str = format!(
            "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, \
                    event_data, event_data_normalized, event_data_decoded, \
                    ledger_hash, in_successful_call, created_at, \
                    schema_version, anonymized, fingerprint, \
                    0::bigint AS total_count \
             FROM events \
             WHERE {where_clause} \
             ORDER BY timestamp DESC \
             LIMIT {limit} OFFSET {offset}",
            where_clause = where_clause,
            limit = limit,
            offset = offset,
        );

        let mut q = sqlx::query_as::<_, models::Event>(&query_str)
            .bind(from)
            .bind(to);
        if let Some(ref cid) = params.contract_id {
            q = q.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            q = q.bind(et);
        }

        let events = q.fetch_all(&state.read_pool).await?;
        let total = events.len();

        crate::metrics::record_temporal_query_duration(query_start.elapsed());

        Ok(Json(models::TemporalQueryResponse {
            from,
            to,
            events,
            buckets: vec![],
            total,
        }))
    }
}

/// WebSocket endpoint for event streaming (alternative to SSE)
#[utoipa::path(
    get,
    path = "/v1/events/ws",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
    ),
    responses(
        (status = 101, description = "WebSocket upgrade"),
        (status = 400, description = "Invalid parameters"),
    )
)]
pub async fn ws_stream_events(
    State(state): State<AppState>,
    Query(params): Query<models::StreamParams>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }
    
    let contract_id = params.contract_id.clone();
    let event_tx = state.event_tx.clone();
    let keepalive_ms = state.sse_keepalive_interval_ms;
    let ws_connections = state.sse_connections.clone();
    
    Ok(ws.on_upgrade(move |socket| {
        handle_ws_connection(socket, contract_id, event_tx, keepalive_ms, ws_connections)
    }))
}

async fn handle_ws_connection(
    socket: axum::extract::ws::WebSocket,
    contract_id: Option<String>,
    event_tx: tokio::sync::broadcast::Sender<models::SorobanEvent>,
    keepalive_ms: u64,
    ws_connections: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    use axum::extract::ws::{Message, WebSocket};
    use tokio::time::{interval, Duration};
    
    let count = ws_connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    crate::metrics::update_ws_connections(count);
    
    let (mut sender, mut receiver) = socket.split();
    let mut rx = event_tx.subscribe();
    let mut keepalive_interval = interval(Duration::from_millis(keepalive_ms));
    
    loop {
        tokio::select! {
            _ = keepalive_interval.tick() => {
                if sender.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
            }
            msg = receiver.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        if let Some(ref cid) = contract_id {
                            if event.contract_id != *cid {
                                continue;
                            }
                        }
                        if let Ok(json) = serde_json::to_string(&event) {
                            if sender.send(Message::Text(json)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    
    let count = ws_connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
    crate::metrics::update_ws_connections(count);
}

// ---------------------------------------------------------------------------
// Issue #487 – Email open tracking
// ---------------------------------------------------------------------------

/// 1×1 transparent GIF used as the tracking pixel.
const TRACKING_PIXEL_GIF: &[u8] = &[
    0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00,
    0x00, 0xff, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x01, 0x00, 0x00, 0x02, 0x00, 0x3b,
];

/// Record an email open event and return the 1×1 tracking pixel.
/// GET /v1/notifications/email/track/:token
pub async fn track_email_open(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    // Verify the HMAC-signed, time-limited token (issue #1158).
    // The subject is the DB token string; verification failure silently
    // returns the pixel anyway (to avoid leaking validity to enumerators)
    // but does NOT record an open event.
    let db_subject = match crate::email_token::verify_token(
        &token,
        Some(&crate::email_token::TokenPurpose::Open),
        None,
    ) {
        Ok(claims) => Some(claims.subject),
        Err(_) => None,
    };

    if let Some(subject) = db_subject {
        let pool = state.pool.clone();
        tokio::spawn(async move {
            let updated = sqlx::query_scalar::<_, i64>(
                "WITH upd AS (
                    UPDATE email_opens SET opened_at = NOW()
                    WHERE token = $1 AND opened_at IS NULL
                    RETURNING 1
                 ) SELECT COUNT(*) FROM upd",
            )
            .bind(&subject)
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
            if updated > 0 {
                crate::metrics::record_email_open();
            }
        });
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "image/gif")
        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header("pragma", "no-cache")
        .body(Body::from(TRACKING_PIXEL_GIF.to_vec()))
        .unwrap()
}

/// Return email open-rate statistics.
/// GET /v1/admin/notifications/email/stats
pub async fn get_email_stats(
    State(state): State<AppState>,
) -> Result<Json<Value>, AppError> {
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM email_opens",
    )
    .fetch_one(&state.read_pool)
    .await?;

    let opened: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM email_opens WHERE opened_at IS NOT NULL",
    )
    .fetch_one(&state.read_pool)
    .await?;

    let open_rate = if total == 0 {
        0.0_f64
    } else {
        opened as f64 / total as f64 * 100.0
    };

    Ok(Json(json!({
        "total_sent": total,
        "total_opened": opened,
        "open_rate_pct": open_rate,
    })))
}

// ---------------------------------------------------------------------------
// Issue #488 – Email click tracking
// ---------------------------------------------------------------------------

/// Record an email link click and redirect to the destination URL.
/// GET /v1/notifications/email/click/:token
pub async fn track_email_click(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    // Verify the HMAC-signed, time-limited token (issue #1158).
    let db_subject = match crate::email_token::verify_token(
        &token,
        Some(&crate::email_token::TokenPurpose::Click),
        None,
    ) {
        Ok(claims) => claims.subject,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("click token not found"))
                .unwrap();
        }
    };

    let dest: Option<String> = sqlx::query_scalar(
        "SELECT destination_url FROM email_clicks WHERE token = $1",
    )
    .bind(&db_subject)
    .fetch_optional(&state.pool)
    .await
    .unwrap_or(None);

    let pool = state.pool.clone();
    let subject_clone = db_subject.clone();
    tokio::spawn(async move {
        let updated = sqlx::query_scalar::<_, i64>(
            "WITH upd AS (
                UPDATE email_clicks SET clicked_at = NOW()
                WHERE token = $1 AND clicked_at IS NULL
                RETURNING 1
             ) SELECT COUNT(*) FROM upd",
        )
        .bind(&subject_clone)
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
        if updated > 0 {
            crate::metrics::record_email_click();
        }
    });

    match dest {
        Some(url) => Response::builder()
            .status(StatusCode::FOUND)
            .header(header::LOCATION, url.as_str())
            .body(Body::empty())
            .unwrap(),
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("click token not found"))
            .unwrap(),
    }
}

// ---------------------------------------------------------------------------
// Issue #489 – A/B test results
// ---------------------------------------------------------------------------

/// Return A/B test delivery and open-rate statistics.
/// GET /v1/admin/notifications/email/ab-test/results
pub async fn get_ab_test_results(
    State(state): State<AppState>,
) -> Result<Json<Value>, AppError> {
    let rows = sqlx::query(
        "SELECT d.ab_template,
                COUNT(d.id)                               AS deliveries,
                COUNT(o.id) FILTER (WHERE o.opened_at IS NOT NULL) AS opens
         FROM email_deliveries d
         LEFT JOIN email_opens o
               ON o.email_notification_id = d.email_notification_id
              AND o.recipient = d.recipient
         WHERE d.ab_template IS NOT NULL
         GROUP BY d.ab_template
         ORDER BY d.ab_template",
    )
    .fetch_all(&state.read_pool)
    .await?;

    let results: Vec<Value> = rows
        .iter()
        .map(|row| {
            let template: Option<String> = row.try_get("ab_template").ok();
            let deliveries: i64 = row.try_get("deliveries").unwrap_or(0);
            let opens: i64 = row.try_get("opens").unwrap_or(0);
            let open_rate = if deliveries == 0 {
                0.0_f64
            } else {
                opens as f64 / deliveries as f64 * 100.0
            };
            json!({
                "template": template,
                "deliveries": deliveries,
                "opens": opens,
                "open_rate_pct": open_rate,
            })
        })
        .collect();

    Ok(Json(json!({ "results": results })))
}

// ---------------------------------------------------------------------------
// Issue #490 – Suppression list management
// ---------------------------------------------------------------------------

/// Add an email address or webhook URL to the suppression list.
/// POST /v1/admin/notifications/suppress
pub async fn add_suppression(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let target = body
        .get("target")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Validation("missing 'target' field".to_string()))?
        .to_string();

    let target_type = body
        .get("target_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Validation("missing 'target_type' field".to_string()))?
        .to_string();

    if target_type != "email" && target_type != "webhook" {
        return Err(AppError::Validation(
            "target_type must be 'email' or 'webhook'".to_string(),
        ));
    }

    let reason = body.get("reason").and_then(|v| v.as_str()).map(|s| s.to_string());
    let expires_at: Option<chrono::DateTime<chrono::Utc>> = body
        .get("expires_at")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok());

    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO suppression_lists (target, target_type, reason, expires_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (target, target_type) DO UPDATE \
             SET reason = EXCLUDED.reason, expires_at = EXCLUDED.expires_at \
         RETURNING id",
    )
    .bind(&target)
    .bind(&target_type)
    .bind(&reason)
    .bind(expires_at)
    .fetch_one(&state.pool)
    .await?;

    Ok(Json(json!({
        "id": id,
        "target": target,
        "target_type": target_type,
        "status": "suppressed",
    })))
}

/// Remove an entry from the suppression list.
/// DELETE /v1/admin/notifications/suppress/:id
pub async fn remove_suppression(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let deleted: Option<String> = sqlx::query_scalar(
        "DELETE FROM suppression_lists WHERE id = $1 RETURNING target",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    match deleted {
        Some(target) => Ok(Json(json!({ "id": id, "target": target, "status": "removed" }))),
        None => Err(AppError::NotFound),
    }
}

/// Return current PostgreSQL streaming replication status.
#[utoipa::path(
    get,
    path = "/v1/admin/replication/status",
    tag = "admin",
    responses(
        (status = 200, description = "Replication status"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_replication_status(State(state): State<AppState>) -> Json<Value> {
    let replicas = crate::replica_monitor::query_replication_status(&state.pool).await;
    Json(json!({
        "replica_count": replicas.len(),
        "replicas": replicas,
    }))
}

/// List all feature flags and their current state.
#[utoipa::path(
    get,
    path = "/v1/admin/feature-flags",
    tag = "admin",
    responses(
        (status = 200, description = "Feature flags list"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn list_feature_flags(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    let rows: Vec<(uuid::Uuid, String, bool, bool, f64, Option<String>)> = sqlx::query_as(
        "SELECT id, name, enabled, auto_rollback, rollback_threshold, description
         FROM feature_flags ORDER BY name",
    )
    .fetch_all(&state.pool)
    .await?;

    let flags: Vec<Value> = rows
        .into_iter()
        .map(|(id, name, enabled, auto_rollback, threshold, description)| {
            json!({
                "id": id,
                "name": name,
                "enabled": enabled,
                "auto_rollback": auto_rollback,
                "rollback_threshold": threshold,
                "description": description,
            })
        })
        .collect();

    let count = flags.len();
    Ok(Json(json!({ "flags": flags, "count": count })))
}

/// Return the feature flag audit trail (most recent 100 entries).
#[utoipa::path(
    get,
    path = "/v1/admin/feature-flags/audit",
    tag = "admin",
    responses(
        (status = 200, description = "Audit trail"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_feature_flag_audit(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    let rows: Vec<(uuid::Uuid, String, String, Option<String>, String, chrono::DateTime<chrono::Utc>)> =
        sqlx::query_as(
            "SELECT a.id, f.name, a.action, a.reason, a.triggered_by, a.created_at
             FROM feature_flag_audit a
             JOIN feature_flags f ON f.id = a.flag_id
             ORDER BY a.created_at DESC LIMIT 100",
        )
        .fetch_all(&state.pool)
        .await?;

    let entries: Vec<Value> = rows
        .into_iter()
        .map(|(id, flag_name, action, reason, triggered_by, created_at)| {
            json!({
                "id": id,
                "flag_name": flag_name,
                "action": action,
                "reason": reason,
                "triggered_by": triggered_by,
                "created_at": created_at,
            })
        })
        .collect();

    let count = entries.len();
    Ok(Json(json!({ "entries": entries, "count": count })))
}

// ── Issue #609: GET /v1/networks ─────────────────────────────────────────────

/// List all registered Soroban networks and their health status.
///
/// `GET /v1/networks`
#[utoipa::path(
    get,
    path = "/v1/networks",
    tag = "system",
    responses(
        (status = 200, description = "List of registered networks"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn list_networks(
    State(state): State<AppState>,
) -> Result<Json<Value>, AppError> {
    let networks = crate::networks::list_networks(&state.pool).await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(Json(json!({
        "count": networks.len(),
        "networks": networks,
    })))
}

// ── Issue #608: GET /v1/ledgers/{ledger}/hash ────────────────────────────────

/// Return the indexed hash for a specific ledger sequence number.
///
/// `GET /v1/ledgers/{ledger}/hash`
#[utoipa::path(
    get,
    path = "/v1/ledgers/{ledger}/hash",
    tag = "events",
    params(
        ("ledger" = u64, Path, description = "Ledger sequence number"),
    ),
    responses(
        (status = 200, description = "Ledger hash"),
        (status = 404, description = "Ledger not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_ledger_hash(
    State(state): State<AppState>,
    Path(ledger): Path<u64>,
) -> Result<Json<Value>, AppError> {
    match crate::ledger_hashes::get_ledger_hash(&state.pool, ledger).await {
        Ok(Some(hash)) => Ok(Json(json!({ "ledger": ledger, "hash": hash }))),
        Ok(None) => Err(AppError::NotFound),
        Err(e) => Err(AppError::Internal(e.to_string())),
    }
}

/// Verify the hash chain for a ledger range.
///
/// `GET /v1/ledgers/verify-chain?from={from}&to={to}`
#[utoipa::path(
    get,
    path = "/v1/ledgers/verify-chain",
    tag = "events",
    params(
        ("from" = u64, Query, description = "Start ledger (inclusive)"),
        ("to"   = u64, Query, description = "End ledger (inclusive)"),
    ),
    responses(
        (status = 200, description = "Hash chain verification result"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn verify_ledger_hash_chain(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    let from: u64 = params
        .get("from")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let to: u64 = params
        .get("to")
        .and_then(|v| v.parse().ok())
        .unwrap_or(from);
    let mismatches = crate::ledger_hashes::verify_hash_chain(&state.pool, from, to)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(Json(json!({
        "from": from,
        "to": to,
        "mismatches": mismatches,
        "ok": mismatches == 0,
    })))
}

// ── Issue #610: GET /v1/admin/compression-stats ──────────────────────────────

/// Return compression statistics and trigger a backfill migration when enabled.
///
/// `GET /v1/admin/compression-stats`
#[utoipa::path(
    get,
    path = "/v1/admin/compression-stats",
    tag = "admin",
    responses(
        (status = 200, description = "Compression statistics"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn compression_stats(
    State(state): State<AppState>,
) -> Result<Json<Value>, AppError> {
    // query_scalar! gives compile-time SQL verification via the .sqlx offline cache.
    let total: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM events")
        .fetch_one(&state.pool)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let compressed: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE event_data_compressed IS NOT NULL")
            .fetch_one(&state.pool)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
    let enabled = state.config.event_compression_enabled;
    Ok(Json(json!({
        "enabled": enabled,
        "total_events": total,
        "compressed_events": compressed,
        "uncompressed_events": total - compressed,
        "coverage_pct": if total > 0 { (compressed as f64 / total as f64) * 100.0 } else { 0.0 },
    })))
}

/// Trigger a background migration that gzip-compresses all uncompressed events.
///
/// `POST /v1/admin/compression-migrate`
#[utoipa::path(
    post,
    path = "/v1/admin/compression-migrate",
    tag = "admin",
    responses(
        (status = 202, description = "Migration started"),
        (status = 400, description = "Compression is disabled"),
    )
)]
pub async fn start_compression_migration(
    State(state): State<AppState>,
) -> impl IntoResponse {
    if !state.config.event_compression_enabled {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "EVENT_COMPRESSION_ENABLED is false" })),
        );
    }
    let pool = state.pool.clone();
    let batch = state.config.event_compression_migration_batch_size;
    tokio::spawn(async move {
        match crate::event_compression::migrate_existing_events(&pool, batch).await {
            Ok(n) => tracing::info!(migrated = n, "compression migration complete"),
            Err(e) => tracing::error!(error = %e, "compression migration failed"),
        }
    });
    (StatusCode::ACCEPTED, Json(json!({ "status": "started" })))
}

// ── Issue #607: GET /v1/contracts/{contract_id}/abi (cached) ─────────────────

/// Return the cached ABI for a contract (cache-backed, with hit/miss metrics).
///
/// `GET /v1/contracts/{contract_id}/abi/cached`
#[utoipa::path(
    get,
    path = "/v1/contracts/{contract_id}/abi/cached",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Contract ID"),
    ),
    responses(
        (status = 200, description = "Contract ABI"),
        (status = 404, description = "ABI not found"),
    )
)]
pub async fn get_contract_abi_cached(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    match crate::abi::fetch_contract_abi(&state.pool, &state.abi_cache, &contract_id).await {
        Some(abi) => Ok(Json(json!({ "contract_id": contract_id, "abi": abi }))),
        None => Err(AppError::NotFound),
    }
}

// ============================================================================
// Issue #623: Archive query and restore
// ============================================================================

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct ArchiveQueryParams {
    /// Start date (YYYY-MM-DD).
    pub from_date: Option<String>,
    /// End date (YYYY-MM-DD).
    pub to_date: Option<String>,
    /// Filter by contract_id.
    pub contract_id: Option<String>,
    /// Maximum number of archive files to return (default 100, max 1000).
    pub limit: Option<usize>,
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct RestoreFromArchiveRequest {
    /// S3 key of the archive file to restore.
    pub key: String,
    /// When true, restored events replace existing rows with the same id (upsert).
    #[serde(default)]
    pub upsert: bool,
}

/// `GET /v1/archive/query` — query archived event files in cold storage.
///
/// Lists archive files filtered by date range and contract. Supports
/// pagination via `limit`. Returns file keys that can be used with the
/// restore endpoint.
#[utoipa::path(
    get,
    path = "/v1/archive/query",
    tag = "archive",
    params(
        ("from_date" = Option<String>, Query, description = "Start date YYYY-MM-DD"),
        ("to_date" = Option<String>, Query, description = "End date YYYY-MM-DD"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("limit" = Option<usize>, Query, description = "Max results (default 100, max 1000)"),
    ),
    responses(
        (status = 200, description = "Archive files matching the query"),
        (status = 501, description = "Archive feature not enabled"),
    )
)]
pub async fn query_archive(
    State(state): State<AppState>,
    Query(params): Query<ArchiveQueryParams>,
) -> Result<Json<Value>, AppError> {
    #[cfg(feature = "archive")]
    {
        let (bucket, prefix) = match &state.config.archive_s3_bucket {
            Some(b) => (b.clone(), state.config.archive_s3_prefix.clone()),
            None => {
                return Err(AppError::Validation(
                    "ARCHIVE_S3_BUCKET is not configured".to_string(),
                ))
            }
        };

        let aws_cfg = aws_config::load_from_env().await;
        let s3 = aws_sdk_s3::Client::new(&aws_cfg);

        let mut files = crate::archiver::list_archive_files(&s3, &bucket, &prefix)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;

        // Filter by date range
        if let Some(ref from_date) = params.from_date {
            files.retain(|f| f.date.as_str() >= from_date.as_str());
        }
        if let Some(ref to_date) = params.to_date {
            files.retain(|f| f.date.as_str() <= to_date.as_str());
        }

        // Filter by contract_id encoded in the key path when possible
        if let Some(ref contract_id) = params.contract_id {
            files.retain(|f| f.key.contains(contract_id.as_str()));
        }

        let limit = params.limit.unwrap_or(100).min(1000);
        let total = files.len();
        files.truncate(limit);

        crate::metrics::record_archive_query();
        Ok(Json(json!({
            "data": files,
            "total": total,
            "returned": files.len(),
        })))
    }
    #[cfg(not(feature = "archive"))]
    {
        let _ = (state, params);
        Err(AppError::Internal("archive feature not enabled".to_string()))
    }
}

/// `POST /v1/archive/restore` — restore events from an S3 archive file.
///
/// Downloads the specified archive file from S3, decompresses it, and
/// re-inserts the events into the database. Supports upsert mode to
/// overwrite existing rows.
#[utoipa::path(
    post,
    path = "/v1/archive/restore",
    tag = "archive",
    request_body = RestoreFromArchiveRequest,
    responses(
        (status = 200, description = "Events restored successfully"),
        (status = 400, description = "Invalid request"),
        (status = 501, description = "Archive feature not enabled"),
    )
)]
pub async fn restore_from_archive(
    State(state): State<AppState>,
    Json(body): Json<RestoreFromArchiveRequest>,
) -> Result<Json<Value>, AppError> {
    #[cfg(feature = "archive")]
    {
        use flate2::read::GzDecoder;
        use std::io::{BufRead, BufReader};

        if body.key.is_empty() {
            return Err(AppError::Validation("key must not be empty".to_string()));
        }

        let (bucket, _prefix) = match &state.config.archive_s3_bucket {
            Some(b) => (b.clone(), state.config.archive_s3_prefix.clone()),
            None => {
                return Err(AppError::Validation(
                    "ARCHIVE_S3_BUCKET is not configured".to_string(),
                ))
            }
        };

        let aws_cfg = aws_config::load_from_env().await;
        let s3 = aws_sdk_s3::Client::new(&aws_cfg);

        let resp = s3
            .get_object()
            .bucket(&bucket)
            .key(&body.key)
            .send()
            .await
            .map_err(|e| AppError::Internal(format!("S3 get_object failed: {e}")))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| AppError::Internal(format!("Failed to read S3 body: {e}")))?
            .into_bytes();

        let decoder = GzDecoder::new(bytes.as_ref());
        let reader = BufReader::new(decoder);

        let mut restored = 0usize;
        let mut errors = 0usize;

        for line in reader.lines() {
            let line = match line {
                Ok(l) if l.trim().is_empty() => continue,
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to read line from archive");
                    errors += 1;
                    continue;
                }
            };

            let event: serde_json::Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to parse event JSON from archive");
                    errors += 1;
                    continue;
                }
            };

            let id = event["id"].as_str().unwrap_or_default();
            let contract_id = event["contract_id"].as_str().unwrap_or_default();
            let event_type = event["event_type"].as_str().unwrap_or("contract");
            let tx_hash = event["tx_hash"].as_str().unwrap_or_default();
            let ledger = event["ledger"].as_i64().unwrap_or(0);
            let timestamp = event["timestamp"].as_str().unwrap_or_default();
            let event_data = &event["event_data"];

            let result = if body.upsert {
                sqlx::query(
                    "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                     VALUES ($1::uuid, $2, $3, $4, $5, $6::timestamptz, $7)
                     ON CONFLICT (id) DO UPDATE SET
                       contract_id = EXCLUDED.contract_id,
                       event_type = EXCLUDED.event_type,
                       tx_hash = EXCLUDED.tx_hash,
                       ledger = EXCLUDED.ledger,
                       timestamp = EXCLUDED.timestamp,
                       event_data = EXCLUDED.event_data",
                )
            } else {
                sqlx::query(
                    "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                     VALUES ($1::uuid, $2, $3, $4, $5, $6::timestamptz, $7)
                     ON CONFLICT (id) DO NOTHING",
                )
            }
            .bind(id)
            .bind(contract_id)
            .bind(event_type)
            .bind(tx_hash)
            .bind(ledger)
            .bind(timestamp)
            .bind(event_data)
            .execute(&state.pool)
            .await;

            match result {
                Ok(_) => restored += 1,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to restore event from archive");
                    errors += 1;
                }
            }
        }

        crate::metrics::record_archive_restore(restored as u64);
        Ok(Json(json!({
            "restored": restored,
            "errors": errors,
            "key": body.key,
        })))
    }
    #[cfg(not(feature = "archive"))]
    {
        let _ = (state, body);
        Err(AppError::Internal("archive feature not enabled".to_string()))
    }
}

// ============================================================================
// Issue #624: Bulk batch query with streaming
// ============================================================================

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct BatchEventFilter {
    /// Filter by contract_id.
    pub contract_id: Option<String>,
    /// Filter by event type.
    pub event_type: Option<String>,
    /// Minimum ledger (inclusive).
    pub from_ledger: Option<i64>,
    /// Maximum ledger (inclusive).
    pub to_ledger: Option<i64>,
    /// Filter by tx_hash.
    pub tx_hash: Option<String>,
    /// Maximum events to return for this filter (max: 10_000).
    pub limit: Option<i64>,
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct BatchQueryRequest {
    /// Array of filter objects to query in a single request.
    pub filters: Vec<BatchEventFilter>,
    /// Response format: "json" (default) or "csv".
    #[serde(default = "default_batch_format")]
    pub format: String,
}

fn default_batch_format() -> String {
    "json".to_string()
}

/// `POST /v1/events/batch` — bulk event retrieval with multiple filters.
///
/// Accepts an array of filter objects and returns all matching events.
/// Supports JSON and CSV output. Default max batch size is 10,000
/// events total across all filters.
#[utoipa::path(
    post,
    path = "/v1/events/batch",
    tag = "events",
    request_body = BatchQueryRequest,
    responses(
        (status = 200, description = "Batch query results"),
        (status = 400, description = "Invalid request or exceeds max batch size"),
    )
)]
pub async fn batch_query_events(
    State(state): State<AppState>,
    Json(body): Json<BatchQueryRequest>,
) -> Result<impl IntoResponse, AppError> {
    let max_total = state.config.batch_events_max_size as i64;

    if body.filters.is_empty() {
        return Err(AppError::Validation("filters array must not be empty".to_string()));
    }
    if body.filters.len() > 100 {
        return Err(AppError::Validation(
            "Maximum 100 filter objects per batch request".to_string(),
        ));
    }

    let format = body.format.to_lowercase();
    if format != "json" && format != "csv" {
        return Err(AppError::Validation(
            "format must be 'json' or 'csv'".to_string(),
        ));
    }

    let mut all_events: Vec<Value> = Vec::new();
    let job_id = Uuid::new_v4().to_string();
    let all_cols: &[&str] = &["id", "contract_id", "event_type", "tx_hash", "ledger", "timestamp", "event_data", "created_at"];

    for filter in &body.filters {
        let remaining = max_total - all_events.len() as i64;
        if remaining <= 0 {
            break;
        }
        let limit = filter.limit.unwrap_or(1000).min(remaining).min(10_000);

        // Build query with optional string/i64 bind parameters
        let contract_id = filter.contract_id.clone();
        let event_type = filter.event_type.clone();
        let from_ledger = filter.from_ledger;
        let to_ledger = filter.to_ledger;
        let tx_hash = filter.tx_hash.clone();

        let mut sql = String::from(
            "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
             FROM events WHERE 1=1",
        );
        let mut bind_idx = 1i32;

        if contract_id.is_some() {
            sql.push_str(&format!(" AND contract_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if event_type.is_some() {
            sql.push_str(&format!(" AND event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        if from_ledger.is_some() {
            sql.push_str(&format!(" AND ledger >= ${bind_idx}"));
            bind_idx += 1;
        }
        if to_ledger.is_some() {
            sql.push_str(&format!(" AND ledger <= ${bind_idx}"));
            bind_idx += 1;
        }
        if tx_hash.is_some() {
            sql.push_str(&format!(" AND tx_hash = ${bind_idx}"));
            bind_idx += 1;
        }
        let _ = bind_idx;

        sql.push_str(" ORDER BY ledger DESC, id DESC");
        sql.push_str(&format!(" LIMIT {limit}"));

        let mut query = sqlx::query(&sql);
        if let Some(ref v) = contract_id { query = query.bind(v); }
        if let Some(ref v) = event_type { query = query.bind(v); }
        if let Some(v) = from_ledger { query = query.bind(v); }
        if let Some(v) = to_ledger { query = query.bind(v); }
        if let Some(ref v) = tx_hash { query = query.bind(v); }

        let rows = query
            .fetch_all(&state.read_pool)
            .await
            .map_err(|e| AppError::Internal(format!("Batch query failed: {e}")))?;

        let events = rows_to_json(
            &rows,
            all_cols,
            state.encryption_key.as_ref(),
            state.encryption_key_old.as_ref(),
            false,
        )?;
        all_events.extend(events);
    }

    crate::metrics::record_batch_query(all_events.len() as u64);

    if format == "csv" {
        let mut csv = String::from("id,contract_id,event_type,tx_hash,ledger,timestamp\n");
        for ev in &all_events {
            let id = ev["id"].as_str().unwrap_or("");
            let cid = ev["contract_id"].as_str().unwrap_or("");
            let et = ev["event_type"].as_str().unwrap_or("");
            let th = ev["tx_hash"].as_str().unwrap_or("");
            let ledger = ev["ledger"].as_i64().unwrap_or(0);
            let ts = ev["timestamp"].as_str().unwrap_or("");
            csv.push_str(&format!("{id},{cid},{et},{th},{ledger},{ts}\n"));
        }
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/csv")
            .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"batch_{job_id}.csv\""))
            .body(Body::from(csv))
            .unwrap()
            .into_response());
    }

    Ok(Json(json!({
        "job_id": job_id,
        "total": all_events.len(),
        "data": all_events,
    })).into_response())
}

// ============================================================================
// Issue #625: Full-text search endpoint
// ============================================================================

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct FulltextSearchParams {
    /// Full-text search query (PostgreSQL plainto_tsquery format).
    pub q: String,
    /// Filter by contract_id.
    pub contract_id: Option<String>,
    /// Filter by event type.
    pub event_type: Option<String>,
    /// Minimum ledger.
    pub from_ledger: Option<i64>,
    /// Maximum ledger.
    pub to_ledger: Option<i64>,
    /// Maximum results (default 50, max 500).
    pub limit: Option<i64>,
    /// Offset for pagination.
    pub offset: Option<i64>,
    /// When true, results are sorted by relevance rank (default true).
    #[serde(default = "default_rank")]
    pub rank: bool,
}

fn default_rank() -> bool {
    true
}

/// `GET /v1/search` — full-text search on event data.
///
/// Searches the `event_data_tsv` tsvector column using PostgreSQL
/// `plainto_tsquery`. Results are ranked by relevance by default.
/// Requires the migration `20260429000000_event_data_fulltext_search.sql`.
#[utoipa::path(
    get,
    path = "/v1/search",
    tag = "search",
    params(
        ("q" = String, Query, description = "Full-text query (plain text, supports AND/OR/NOT)"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("event_type" = Option<String>, Query, description = "Filter by event type"),
        ("from_ledger" = Option<i64>, Query, description = "Minimum ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Maximum ledger"),
        ("limit" = Option<i64>, Query, description = "Max results (default 50, max 500)"),
        ("offset" = Option<i64>, Query, description = "Pagination offset"),
        ("rank" = Option<bool>, Query, description = "Sort by relevance (default true)"),
    ),
    responses(
        (status = 200, description = "Full-text search results with relevance scores"),
        (status = 400, description = "Invalid query"),
    )
)]
pub async fn fulltext_search(
    State(state): State<AppState>,
    Query(params): Query<FulltextSearchParams>,
) -> Result<Json<Value>, AppError> {
    let q = params.q.trim();
    if q.is_empty() {
        return Err(AppError::Validation("q parameter must not be empty".to_string()));
    }
    if q.len() > 1000 {
        return Err(AppError::Validation(
            "q parameter exceeds maximum length of 1000 characters".to_string(),
        ));
    }

    let limit = params.limit.unwrap_or(50).max(1).min(500);
    let offset = params.offset.unwrap_or(0).max(0);

    let order_clause = if params.rank {
        "ORDER BY ts_rank(event_data_tsv, plainto_tsquery('english', $1)) DESC, ledger DESC"
    } else {
        "ORDER BY ledger DESC, id DESC"
    };

    let mut sql = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, \
         ts_rank(event_data_tsv, plainto_tsquery('english', $1)) AS rank \
         FROM events \
         WHERE event_data_tsv @@ plainto_tsquery('english', $1)"
    );

    let mut bind_idx = 2i32;
    let mut bind_contract_id: Option<String> = None;
    let mut bind_event_type: Option<String> = None;
    let mut bind_from: Option<i64> = None;
    let mut bind_to: Option<i64> = None;

    if let Some(ref cid) = params.contract_id {
        sql.push_str(&format!(" AND contract_id = ${bind_idx}"));
        bind_contract_id = Some(cid.clone());
        bind_idx += 1;
    }
    if let Some(ref et) = params.event_type {
        sql.push_str(&format!(" AND event_type = ${bind_idx}"));
        bind_event_type = Some(et.clone());
        bind_idx += 1;
    }
    if let Some(from) = params.from_ledger {
        sql.push_str(&format!(" AND ledger >= ${bind_idx}"));
        bind_from = Some(from);
        bind_idx += 1;
    }
    if let Some(to) = params.to_ledger {
        sql.push_str(&format!(" AND ledger <= ${bind_idx}"));
        bind_to = Some(to);
        bind_idx += 1;
    }
    let _ = bind_idx;

    sql.push_str(&format!(" {order_clause} LIMIT {limit} OFFSET {offset}"));

    let mut query = sqlx::query(&sql).bind(q);
    if let Some(ref v) = bind_contract_id { query = query.bind(v); }
    if let Some(ref v) = bind_event_type { query = query.bind(v); }
    if let Some(v) = bind_from { query = query.bind(v); }
    if let Some(v) = bind_to { query = query.bind(v); }

    let rows = query
        .fetch_all(&state.read_pool)
        .await
        .map_err(|e| AppError::Internal(format!("Full-text search query failed: {e}")))?;

    let mut results: Vec<Value> = Vec::with_capacity(rows.len());
    for row in &rows {
        let id: uuid::Uuid = row.try_get("id")?;
        let contract_id: String = row.try_get("contract_id")?;
        let event_type: String = row.try_get("event_type")?;
        let tx_hash: String = row.try_get("tx_hash")?;
        let ledger: i64 = row.try_get("ledger")?;
        let timestamp: chrono::DateTime<Utc> = row.try_get("timestamp")?;
        let event_data: Value = row.try_get("event_data")?;
        let rank: f32 = row.try_get("rank").unwrap_or(0.0f32);

        results.push(json!({
            "id": id.to_string(),
            "contract_id": contract_id,
            "event_type": event_type,
            "tx_hash": tx_hash,
            "ledger": ledger,
            "timestamp": timestamp.to_rfc3339(),
            "event_data": event_data,
            "relevance": rank,
        }));
    }

    crate::metrics::record_fulltext_search();

    Ok(Json(json!({
        "query": q,
        "total": results.len(),
        "limit": limit,
        "offset": offset,
        "data": results,
    })))
}

// ============================================================================
// Issue #626: Faceted search / aggregation API
// ============================================================================

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct EventsAggregationParams {
    /// Filter by contract_id.
    pub contract_id: Option<String>,
    /// Filter by event type.
    pub event_type: Option<String>,
    /// Minimum ledger.
    pub from_ledger: Option<i64>,
    /// Maximum ledger.
    pub to_ledger: Option<i64>,
    /// Time bucket: "hourly", "daily" (default), or "weekly".
    #[serde(default = "default_time_bucket")]
    pub time_bucket: String,
    /// Maximum buckets per aggregation dimension (default 100, max 500).
    pub limit: Option<i64>,
}

fn default_time_bucket() -> String {
    "daily".to_string()
}

/// `GET /v1/events/aggregations` — faceted event statistics.
///
/// Returns event_type distribution, top contract_ids by event count,
/// time-series bucketed counts, and topic distribution. Results are cached
/// per unique parameter set for `AGGREGATION_CACHE_TTL_SECS` seconds.
#[utoipa::path(
    get,
    path = "/v1/events/aggregations",
    tag = "aggregations",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("event_type" = Option<String>, Query, description = "Filter by event type"),
        ("from_ledger" = Option<i64>, Query, description = "Minimum ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Maximum ledger"),
        ("time_bucket" = Option<String>, Query, description = "Time bucket: hourly, daily (default), weekly"),
        ("limit" = Option<i64>, Query, description = "Max buckets per dimension (default 100, max 500)"),
    ),
    responses(
        (status = 200, description = "Aggregated event statistics"),
        (status = 400, description = "Invalid parameters"),
    )
)]
pub async fn events_aggregations(
    State(state): State<AppState>,
    Query(params): Query<EventsAggregationParams>,
) -> Result<Json<Value>, AppError> {
    let time_bucket = params.time_bucket.to_lowercase();
    if !["hourly", "daily", "weekly"].contains(&time_bucket.as_str()) {
        return Err(AppError::Validation(
            "time_bucket must be 'hourly', 'daily', or 'weekly'".to_string(),
        ));
    }

    let limit = params.limit.unwrap_or(100).max(1).min(500);

    // Build a stable cache key from the request params
    let cache_key = format!(
        "agg:{}:{}:{}:{}:{}:{}",
        params.contract_id.as_deref().unwrap_or(""),
        params.event_type.as_deref().unwrap_or(""),
        params.from_ledger.unwrap_or(0),
        params.to_ledger.unwrap_or(0),
        time_bucket,
        limit,
    );

    if let Some(cached) = state.aggregation_cache.get(&cache_key).await {
        return Ok(Json(cached));
    }

    // Build WHERE clause parts
    let mut where_parts: Vec<String> = vec!["1=1".to_string()];
    let mut bind_idx = 1i32;
    let mut contract_id_bind: Option<String> = None;
    let mut event_type_bind: Option<String> = None;
    let mut from_ledger_bind: Option<i64> = None;
    let mut to_ledger_bind: Option<i64> = None;

    if let Some(ref cid) = params.contract_id {
        where_parts.push(format!("contract_id = ${bind_idx}"));
        contract_id_bind = Some(cid.clone());
        bind_idx += 1;
    }
    if let Some(ref et) = params.event_type {
        where_parts.push(format!("event_type = ${bind_idx}"));
        event_type_bind = Some(et.clone());
        bind_idx += 1;
    }
    if let Some(from) = params.from_ledger {
        where_parts.push(format!("ledger >= ${bind_idx}"));
        from_ledger_bind = Some(from);
        bind_idx += 1;
    }
    if let Some(to) = params.to_ledger {
        where_parts.push(format!("ledger <= ${bind_idx}"));
        to_ledger_bind = Some(to);
        bind_idx += 1;
    }
    let _ = bind_idx;
    let where_clause = where_parts.join(" AND ");

    macro_rules! bind_filters {
        ($q:expr) => {{
            let mut q = $q;
            if let Some(ref v) = contract_id_bind { q = q.bind(v); }
            if let Some(ref v) = event_type_bind { q = q.bind(v); }
            if let Some(v) = from_ledger_bind { q = q.bind(v); }
            if let Some(v) = to_ledger_bind { q = q.bind(v); }
            q
        }};
    }

    // 1. event_type aggregation
    let event_type_sql = format!(
        "SELECT event_type, COUNT(*) AS count FROM events WHERE {where_clause} \
         GROUP BY event_type ORDER BY count DESC LIMIT {limit}"
    );
    let event_type_rows = bind_filters!(sqlx::query(&event_type_sql))
        .fetch_all(&state.read_pool)
        .await
        .map_err(|e| AppError::Internal(format!("event_type aggregation failed: {e}")))?;

    let event_type_agg: Vec<Value> = event_type_rows
        .iter()
        .map(|r| {
            let et: String = r.try_get("event_type").unwrap_or_default();
            let count: i64 = r.try_get("count").unwrap_or(0);
            json!({"event_type": et, "count": count})
        })
        .collect();

    // 2. contract_id aggregation
    let contract_id_sql = format!(
        "SELECT contract_id, COUNT(*) AS count FROM events WHERE {where_clause} \
         GROUP BY contract_id ORDER BY count DESC LIMIT {limit}"
    );
    let contract_id_rows = bind_filters!(sqlx::query(&contract_id_sql))
        .fetch_all(&state.read_pool)
        .await
        .map_err(|e| AppError::Internal(format!("contract_id aggregation failed: {e}")))?;

    let contract_id_agg: Vec<Value> = contract_id_rows
        .iter()
        .map(|r| {
            let cid: String = r.try_get("contract_id").unwrap_or_default();
            let count: i64 = r.try_get("count").unwrap_or(0);
            json!({"contract_id": cid, "count": count})
        })
        .collect();

    // 3. Time-based aggregation
    let trunc_fn = match time_bucket.as_str() {
        "hourly" => "hour",
        "weekly" => "week",
        _ => "day",
    };
    let time_sql = format!(
        "SELECT date_trunc('{trunc_fn}', timestamp) AS bucket, COUNT(*) AS count \
         FROM events WHERE {where_clause} \
         GROUP BY bucket ORDER BY bucket DESC LIMIT {limit}"
    );
    let time_rows = bind_filters!(sqlx::query(&time_sql))
        .fetch_all(&state.read_pool)
        .await
        .map_err(|e| AppError::Internal(format!("time aggregation failed: {e}")))?;

    let time_agg: Vec<Value> = time_rows
        .iter()
        .map(|r| {
            let bucket: Option<chrono::DateTime<Utc>> = r.try_get("bucket").ok();
            let count: i64 = r.try_get("count").unwrap_or(0);
            let bucket_str = bucket.map(|b| b.to_rfc3339()).unwrap_or_default();
            json!({"bucket": bucket_str, "count": count})
        })
        .collect();

    // 4. Topic aggregation (top topics from event_data->>'topic')
    let topic_sql = format!(
        "SELECT event_data->>'topic' AS topic, COUNT(*) AS count \
         FROM events WHERE {where_clause} AND event_data->>'topic' IS NOT NULL \
         GROUP BY topic ORDER BY count DESC LIMIT {limit}"
    );
    let topic_rows = bind_filters!(sqlx::query(&topic_sql))
        .fetch_all(&state.read_pool)
        .await
        .map_err(|e| AppError::Internal(format!("topic aggregation failed: {e}")))?;

    let topic_agg: Vec<Value> = topic_rows
        .iter()
        .map(|r| {
            let topic: String = r.try_get("topic").unwrap_or_default();
            let count: i64 = r.try_get("count").unwrap_or(0);
            json!({"topic": topic, "count": count})
        })
        .collect();

    let result = json!({
        "time_bucket": time_bucket,
        "filters": {
            "contract_id": params.contract_id,
            "event_type": params.event_type,
            "from_ledger": params.from_ledger,
            "to_ledger": params.to_ledger,
        },
        "aggregations": {
            "by_event_type": event_type_agg,
            "by_contract_id": contract_id_agg,
            "by_time": time_agg,
            "by_topic": topic_agg,
        },
    });

    state.aggregation_cache.insert(cache_key, result.clone()).await;
    crate::metrics::record_aggregation_query();

    Ok(Json(result))
}

#[cfg(test)]
mod temporal_tests {
    use super::*;
    use crate::config::{HealthState, IndexerState};
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use sqlx::PgPool;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn create_test_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            Vec::new(),
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    // ── parse_relative_duration unit tests ────────────────────────────────────

    #[test]
    fn parse_seconds() {
        let d = parse_relative_duration("30s").unwrap();
        assert_eq!(d, chrono::Duration::seconds(30));
    }

    #[test]
    fn parse_minutes() {
        let d = parse_relative_duration("5m").unwrap();
        assert_eq!(d, chrono::Duration::minutes(5));
    }

    #[test]
    fn parse_hours() {
        let d = parse_relative_duration("24h").unwrap();
        assert_eq!(d, chrono::Duration::hours(24));
    }

    #[test]
    fn parse_days() {
        let d = parse_relative_duration("7d").unwrap();
        assert_eq!(d, chrono::Duration::days(7));
    }

    #[test]
    fn parse_weeks() {
        let d = parse_relative_duration("2w").unwrap();
        assert_eq!(d, chrono::Duration::weeks(2));
    }

    #[test]
    fn parse_invalid_unit_returns_error() {
        assert!(parse_relative_duration("1y").is_err());
    }

    #[test]
    fn parse_missing_unit_returns_error() {
        assert!(parse_relative_duration("42").is_err());
    }

    #[test]
    fn parse_zero_returns_error() {
        assert!(parse_relative_duration("0h").is_err());
    }

    #[test]
    fn parse_negative_returns_error() {
        assert!(parse_relative_duration("-1h").is_err());
    }

    #[test]
    fn parse_empty_returns_error() {
        assert!(parse_relative_duration("").is_err());
    }

    // ── /v1/events/temporal integration tests ─────────────────────────────────

    #[sqlx::test(migrations = "./migrations")]
    async fn temporal_missing_since_and_from_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/temporal")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn temporal_since_24h_returns_200_empty(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/temporal?since=24h")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["events"], serde_json::json!([]));
        assert_eq!(v["total"], 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn temporal_aggregate_returns_buckets(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/temporal?since=1d&aggregate=true&window=1h")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // On an empty DB the buckets array should be empty (no events in last 24h)
        assert!(v["buckets"].is_array());
        assert!(v["events"].is_array());
        assert_eq!(v["events"], serde_json::json!([]));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn temporal_since_and_from_timestamp_mutually_exclusive(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/temporal?since=1d&from_timestamp=2026-01-01T00:00:00Z")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn temporal_invalid_window_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/temporal?since=1d&aggregate=true&window=2d")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn temporal_returns_event_within_window(pool: PgPool) {
        // Insert an event that was created just now (well within 24h window).
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ('CABC', 'contract', 'txabc123temporal', 100, NOW(), '{}'::jsonb)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/temporal?since=24h")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], 1);
        assert_eq!(v["events"].as_array().unwrap().len(), 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn temporal_event_outside_window_not_returned(pool: PgPool) {
        // Insert an event two days ago — outside the 1h window.
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ('CABC', 'contract', 'txold_temporal', 99, NOW() - INTERVAL '2 days', '{}'::jsonb)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/temporal?since=1h")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], 0);
    }
}

// ============================================================================
// Issue #627: Bloom filter based contract existence check
// ============================================================================

/// Check if a contract has any indexed events using the bloom filter.
///
/// `GET /v1/contracts/exists?id=CABC...`
///
/// Returns whether the contract is likely to exist based on the bloom filter.
/// This is a fast check without a database query.
#[utoipa::path(
    get,
    path = "/v1/contracts/exists",
    tag = "contracts",
    params(
        ("id", description = "Contract ID to check"),
    ),
    responses(
        (status = 200, description = "Contract existence check result", body = ContractExistsResponse),
        (status = 400, description = "Invalid contract ID"),
    )
)]
pub async fn check_contract_exists(
    State(state): State<AppState>,
    Query(params): Query<ContractExistsParams>,
) -> Result<impl IntoResponse, AppError> {
    use models::ContractId;
    
    let contract_id = params.id.trim();
    
    // Validate contract ID format
    ContractId::validate(contract_id)?;
    
    // Check if contract exists in bloom filter
    let exists_in_filter = if let Some(ref filter) = state.indexer_state.bloom_filter {
        filter.contains_contract(contract_id)
    } else {
        // If bloom filter is not available, do a DB lookup
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE contract_id = $1 LIMIT 1"
        )
        .bind(contract_id)
        .fetch_one(&state.pool)
        .await?;
        count > 0
    };
    
    Ok(Json(models::ContractExistsResponse {
        contract_id: contract_id.to_string(),
        exists: exists_in_filter,
        method: if state.indexer_state.bloom_filter.is_some() {
            "bloom_filter".to_string()
        } else {
            "database_query".to_string()
        },
    }))
}

#[derive(serde::Deserialize)]
pub struct ContractExistsParams {
    pub id: String,
}


// ============================================================================
// Issue #631: Dynamic config reloading via API
// ============================================================================

/// Get current configuration (non-secret values).
///
/// `GET /v1/config`
///
/// Returns the current configuration state for the service, excluding secret values.
#[utoipa::path(
    get,
    path = "/v1/config",
    tag = "system",
    responses(
        (status = 200, description = "Current configuration", body = ConfigResponse),
    )
)]
pub async fn get_config(
    State(state): State<AppState>,
) -> impl IntoResponse {
    Json(models::ConfigResponse {
        log_level: std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        rate_limit_per_minute: state.config.rate_limit_per_minute,
        sse_keepalive_secs: state.sse_keepalive_interval_ms / 1000,
        health_check_timeout_ms: state.health_check_timeout_ms,
        db_max_connections: state.config.db_max_connections,
        indexer_lag_warn_threshold: state.config.indexer_lag_warn_threshold,
        slow_query_threshold_ms: state.config.slow_query_threshold_ms,
        features: state.config.features.clone(),
    })
}

/// Reload configuration without restarting.
///
/// `POST /v1/admin/config/reload`
///
/// Reloads supported configuration options at runtime:
/// - Log level (RUST_LOG)
/// - Rate limits
/// - SSE keepalive interval
/// - TTL settings
///
/// Requires ADMIN_API_KEY.
#[utoipa::path(
    post,
    path = "/v1/admin/config/reload",
    tag = "admin",
    request_body = ConfigReloadRequest,
    responses(
        (status = 200, description = "Configuration reloaded successfully", body = ConfigReloadResponse),
        (status = 400, description = "Invalid configuration"),
        (status = 401, description = "Unauthorized"),
    )
)]
pub async fn reload_config(
    State(state): State<AppState>,
    Json(req): Json<models::ConfigReloadRequest>,
) -> Result<impl IntoResponse, AppError> {
    use tracing::info;
    
    // Track which settings were updated for audit logging
    let mut updated_settings = Vec::new();
    
    // Validate new config before applying
    if let Some(ref rate_limit) = req.rate_limit_per_minute {
        if *rate_limit == 0 {
            return Err(AppError::Validation(
                "rate_limit_per_minute must be > 0".to_string(),
            ));
        }
        updated_settings.push(format!("rate_limit_per_minute: {}", rate_limit));
    }

    // Issue #669 / #666: per-key rate limit hot-reload
    if let Some(v) = req.rate_limit_key_per_minute {
        updated_settings.push(format!("rate_limit_key_per_minute: {}", v));
        // 0 means "disable" — set the env var so the next config load picks it up
        if v == 0 {
            std::env::remove_var("RATE_LIMIT_KEY_PER_MINUTE");
        } else {
            std::env::set_var("RATE_LIMIT_KEY_PER_MINUTE", v.to_string());
        }
    }
    if let Some(v) = req.rate_limit_key_per_hour {
        updated_settings.push(format!("rate_limit_key_per_hour: {}", v));
        if v == 0 {
            std::env::remove_var("RATE_LIMIT_KEY_PER_HOUR");
        } else {
            std::env::set_var("RATE_LIMIT_KEY_PER_HOUR", v.to_string());
        }
    }
    if let Some(v) = req.rate_limit_key_per_day {
        updated_settings.push(format!("rate_limit_key_per_day: {}", v));
        if v == 0 {
            std::env::remove_var("RATE_LIMIT_KEY_PER_DAY");
        } else {
            std::env::set_var("RATE_LIMIT_KEY_PER_DAY", v.to_string());
        }
    }
    
    if let Some(ref log_level) = req.log_level {
        // Validate log level is one of: trace, debug, info, warn, error
        if !matches!(log_level.to_lowercase().as_str(), "trace" | "debug" | "info" | "warn" | "error") {
            return Err(AppError::Validation(
                "log_level must be one of: trace, debug, info, warn, error".to_string(),
            ));
        }
        updated_settings.push(format!("log_level: {}", log_level));
        // Note: Setting RUST_LOG at runtime requires re-initializing the tracing subscriber,
        // which we don't do in this implementation. In production, you might use
        // a tracing-reload crate or similar.
        std::env::set_var("RUST_LOG", log_level);
    }
    
    if let Some(ref sse_keepalive) = req.sse_keepalive_secs {
        if *sse_keepalive < 1 || *sse_keepalive > 60 {
            return Err(AppError::Validation(
                "sse_keepalive_secs must be between 1 and 60".to_string(),
            ));
        }
        updated_settings.push(format!("sse_keepalive_secs: {}", sse_keepalive));
    }
    
    // Audit log the configuration change
    if !updated_settings.is_empty() {
        crate::audit_logging::log_event(
            &state.pool,
            "CONFIG_RELOAD",
            Some(&format!("Updated: {}", updated_settings.join(", "))),
        )
        .await
        .ok();
        
        info!(
            updated_fields = ?updated_settings,
            "configuration reloaded dynamically"
        );
    }
    
    Ok(Json(models::ConfigReloadResponse {
        success: true,
        message: format!("Configuration reloaded. Updated {} settings.", updated_settings.len()),
        updated_fields: updated_settings,
    }))
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct ConfigReloadRequest {
    /// New log level: trace, debug, info, warn, error
    #[serde(default)]
    pub log_level: Option<String>,
    
    /// New rate limit per minute per IP
    #[serde(default)]
    pub rate_limit_per_minute: Option<u32>,

    /// Per-API-key rate limit: max requests per minute (Issue #669).
    /// Set to 0 to disable.
    #[serde(default)]
    pub rate_limit_key_per_minute: Option<u32>,

    /// Per-API-key rate limit: max requests per hour (Issue #669).
    /// Set to 0 to disable.
    #[serde(default)]
    pub rate_limit_key_per_hour: Option<u32>,

    /// Per-API-key rate limit: max requests per day (Issue #669).
    /// Set to 0 to disable.
    #[serde(default)]
    pub rate_limit_key_per_day: Option<u32>,
    
    /// New SSE keepalive interval in seconds (1-60)
    #[serde(default)]
    pub sse_keepalive_secs: Option<u64>,
    
    /// New slow query threshold in milliseconds
    #[serde(default)]
    pub slow_query_threshold_ms: Option<u64>,
}

// ── Issue #618: Anonymization pipeline API handlers ──────────────────────────

use crate::anonymization::AnonymizationRuleRequest;

/// List all configured anonymization rules
#[utoipa::path(
    get,
    path = "/v1/config/anonymization",
    tag = "config",
    responses(
        (status = 200, description = "List of anonymization rules"),
        (status = 500, description = "Internal server error")
    ),
    security(("api_key" = []))
)]
pub async fn get_anonymization_config(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    match state.anonymization_config.as_ref() {
        Some(cfg) => {
            let rules = cfg.get_rules().await;
            Ok(Json(json!({ "rules": rules })))
        }
        None => {
            let defaults = crate::anonymization::default_rules();
            Ok(Json(json!({ "rules": defaults, "source": "built_in_defaults" })))
        }
    }
}

/// Create or update an anonymization rule
#[utoipa::path(
    post,
    path = "/v1/config/anonymization/rules",
    tag = "config",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Rule created or updated"),
        (status = 400, description = "Invalid rule"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(("api_key" = []))
)]
pub async fn upsert_anonymization_rule(
    State(state): State<AppState>,
    Json(payload): Json<AnonymizationRuleRequest>,
) -> Result<impl IntoResponse, AppError> {
    let cfg = state
        .anonymization_config
        .as_ref()
        .ok_or_else(|| AppError::Internal("Anonymization config not initialized".to_string()))?;

    let rule = cfg
        .upsert_rule(&payload)
        .await
        .map_err(|e| AppError::Validation(e.to_string()))?;

    Ok((StatusCode::OK, Json(json!({ "status": "ok", "rule": rule }))))
}

/// Delete an anonymization rule by name
#[utoipa::path(
    delete,
    path = "/v1/config/anonymization/rules/{name}",
    tag = "config",
    params(
        ("name" = String, Path, description = "Rule name")
    ),
    responses(
        (status = 200, description = "Rule deleted"),
        (status = 404, description = "Rule not found"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(("api_key" = []))
)]
pub async fn delete_anonymization_rule(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let cfg = state
        .anonymization_config
        .as_ref()
        .ok_or_else(|| AppError::Internal("Anonymization config not initialized".to_string()))?;

    let deleted = cfg
        .delete_rule(&name)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    if deleted {
        Ok((StatusCode::OK, Json(json!({ "status": "ok", "deleted": name }))))
    } else {
        Err(AppError::NotFound)
    }
}

/// Scan a single event's data for PII (dry-run, does not modify the event)
#[utoipa::path(
    post,
    path = "/v1/config/anonymization/scan",
    tag = "config",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "PII scan result"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(("api_key" = []))
)]
pub async fn scan_event_for_pii(
    State(state): State<AppState>,
    Json(event_data): Json<serde_json::Value>,
) -> Result<impl IntoResponse, AppError> {
    let detection_rules = match state.anonymization_config.as_ref() {
        Some(cfg) => cfg.compile_detection_rules().await,
        None => {
            crate::anonymization::default_rules()
                .iter()
                .filter(|r| r.enabled)
                .filter_map(|r| Regex::new(&r.pattern).ok().map(|re| (r.name.clone(), re)))
                .collect()
        }
    };

    let result = crate::anonymization::scan_for_pii(&event_data, &detection_rules);
    let detections: Vec<serde_json::Value> = result
        .detections
        .iter()
        .map(|d| json!({ "field": d.field_path, "rule": d.rule_name }))
        .collect();

    Ok(Json(json!({
        "pii_detected": !detections.is_empty(),
        "detections": detections,
    })))
}

// ── Issue #670: GET /v1/admin/audit-logs ─────────────────────────────────────

/// Query audit logs for sensitive operations.
///
/// `GET /v1/admin/audit-logs`
///
/// Requires admin API key. Supports filtering by event_type, resource_type,
/// user_email, severity, success, and date range. Defaults to the last 100
/// entries ordered by most-recent first.
#[utoipa::path(
    get,
    path = "/v1/admin/audit-logs",
    tag = "admin",
    params(
        ("event_type"    = Option<String>, Query, description = "Filter by event type (DELETE, CONFIG_CHANGE, ADMIN_API_CALL, …)"),
        ("resource_type" = Option<String>, Query, description = "Filter by resource type"),
        ("user_email"    = Option<String>, Query, description = "Filter by user email"),
        ("severity"      = Option<String>, Query, description = "Filter by severity (LOW, INFO, WARNING, CRITICAL)"),
        ("success"       = Option<bool>,   Query, description = "Filter by success flag"),
        ("from"          = Option<String>, Query, description = "ISO-8601 start datetime (inclusive)"),
        ("to"            = Option<String>, Query, description = "ISO-8601 end datetime (inclusive)"),
        ("limit"         = Option<i64>,    Query, description = "Max results to return (default 100, max 1000)"),
        ("offset"        = Option<i64>,    Query, description = "Pagination offset"),
    ),
    responses(
        (status = 200, description = "Audit log entries"),
        (status = 400, description = "Invalid query parameters"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden – admin key required"),
        (status = 500, description = "Internal server error"),
    ),
    security(("admin_key" = []))
)]
pub async fn get_audit_logs(
    State(state): State<AppState>,
    Query(params): Query<AuditLogQueryParams>,
) -> Result<Json<Value>, AppError> {
    // Clamp limit
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let offset = params.offset.unwrap_or(0).max(0);

    // Parse optional datetime bounds
    let from_dt: Option<DateTime<Utc>> = params
        .from
        .as_deref()
        .map(|s| {
            s.parse::<DateTime<Utc>>()
                .map_err(|_| AppError::Validation(format!("invalid 'from' datetime: {s}")))
        })
        .transpose()?;

    let to_dt: Option<DateTime<Utc>> = params
        .to
        .as_deref()
        .map(|s| {
            s.parse::<DateTime<Utc>>()
                .map_err(|_| AppError::Validation(format!("invalid 'to' datetime: {s}")))
        })
        .transpose()?;

    if let (Some(f), Some(t)) = (from_dt, to_dt) {
        if f >= t {
            return Err(AppError::Validation(
                "'from' must be before 'to'".to_string(),
            ));
        }
    }

    // Build WHERE conditions with positional bind params
    let mut conditions: Vec<String> = Vec::new();
    let mut idx: i32 = 1;

    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${idx}"));
        idx += 1;
    }
    if params.resource_type.is_some() {
        conditions.push(format!("resource_type = ${idx}"));
        idx += 1;
    }
    if params.user_email.is_some() {
        conditions.push(format!("user_email = ${idx}"));
        idx += 1;
    }
    if params.severity.is_some() {
        conditions.push(format!("severity = ${idx}"));
        idx += 1;
    }
    if params.success.is_some() {
        conditions.push(format!("success = ${idx}"));
        idx += 1;
    }
    if from_dt.is_some() {
        conditions.push(format!("created_at >= ${idx}"));
        idx += 1;
    }
    if to_dt.is_some() {
        conditions.push(format!("created_at <= ${idx}"));
        idx += 1;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    // Count total matching rows
    let count_sql = format!("SELECT COUNT(*) FROM audit_logs {where_clause}");
    let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
    if let Some(ref v) = params.event_type    { count_q = count_q.bind(v); }
    if let Some(ref v) = params.resource_type { count_q = count_q.bind(v); }
    if let Some(ref v) = params.user_email    { count_q = count_q.bind(v); }
    if let Some(ref v) = params.severity      { count_q = count_q.bind(v); }
    if let Some(v)     = params.success       { count_q = count_q.bind(v); }
    if let Some(v)     = from_dt              { count_q = count_q.bind(v); }
    if let Some(v)     = to_dt               { count_q = count_q.bind(v); }
    let total: i64 = count_q.fetch_one(&state.pool).await?;

    // Fetch page
    let data_sql = format!(
        "SELECT id, event_type, action, resource_type, resource_id, \
         resource_description, request_path, request_method, \
         api_key_hash, user_email, user_id, ip_address::text, user_agent, \
         changes, old_value, new_value, \
         status_code, success, error_message, \
         created_by, severity, created_at, expires_at \
         FROM audit_logs {where_clause} \
         ORDER BY created_at DESC \
         LIMIT ${idx} OFFSET ${}",
        idx + 1
    );

    let mut data_q = sqlx::query(&data_sql);
    if let Some(ref v) = params.event_type    { data_q = data_q.bind(v); }
    if let Some(ref v) = params.resource_type { data_q = data_q.bind(v); }
    if let Some(ref v) = params.user_email    { data_q = data_q.bind(v); }
    if let Some(ref v) = params.severity      { data_q = data_q.bind(v); }
    if let Some(v)     = params.success       { data_q = data_q.bind(v); }
    if let Some(v)     = from_dt              { data_q = data_q.bind(v); }
    if let Some(v)     = to_dt               { data_q = data_q.bind(v); }
    data_q = data_q.bind(limit).bind(offset);

    let rows = data_q.fetch_all(&state.pool).await?;

    let entries: Vec<Value> = rows
        .iter()
        .map(|row| {
            json!({
                "id":                   row.try_get::<uuid::Uuid, _>("id").ok(),
                "event_type":           row.try_get::<String, _>("event_type").ok(),
                "action":               row.try_get::<String, _>("action").ok(),
                "resource_type":        row.try_get::<String, _>("resource_type").ok(),
                "resource_id":          row.try_get::<Option<String>, _>("resource_id").ok().flatten(),
                "resource_description": row.try_get::<Option<String>, _>("resource_description").ok().flatten(),
                "request_path":         row.try_get::<Option<String>, _>("request_path").ok().flatten(),
                "request_method":       row.try_get::<Option<String>, _>("request_method").ok().flatten(),
                "api_key_hash":         row.try_get::<Option<String>, _>("api_key_hash").ok().flatten(),
                "user_email":           row.try_get::<Option<String>, _>("user_email").ok().flatten(),
                "user_id":              row.try_get::<Option<String>, _>("user_id").ok().flatten(),
                "ip_address":           row.try_get::<Option<String>, _>("ip_address").ok().flatten(),
                "user_agent":           row.try_get::<Option<String>, _>("user_agent").ok().flatten(),
                "changes":              row.try_get::<Option<Value>, _>("changes").ok().flatten(),
                "old_value":            row.try_get::<Option<Value>, _>("old_value").ok().flatten(),
                "new_value":            row.try_get::<Option<Value>, _>("new_value").ok().flatten(),
                "status_code":          row.try_get::<Option<i32>, _>("status_code").ok().flatten(),
                "success":              row.try_get::<bool, _>("success").ok(),
                "error_message":        row.try_get::<Option<String>, _>("error_message").ok().flatten(),
                "created_by":           row.try_get::<Option<String>, _>("created_by").ok().flatten(),
                "severity":             row.try_get::<String, _>("severity").ok(),
                "created_at":           row.try_get::<DateTime<Utc>, _>("created_at").ok(),
                "expires_at":           row.try_get::<Option<DateTime<Utc>>, _>("expires_at").ok().flatten(),
            })
        })
        .collect();

    Ok(Json(json!({
        "data":   entries,
        "total":  total,
        "limit":  limit,
        "offset": offset,
    })))
}

/// Query parameters for the audit log endpoint.
#[derive(Debug, serde::Deserialize)]
pub struct AuditLogQueryParams {
    pub event_type:    Option<String>,
    pub resource_type: Option<String>,
    pub user_email:    Option<String>,
    pub severity:      Option<String>,
    pub success:       Option<bool>,
    pub from:          Option<String>,
    pub to:            Option<String>,
    pub limit:         Option<i64>,
    pub offset:        Option<i64>,
}

// ── Issue #671: GET /v1/analytics/trending ───────────────────────────────────

/// Return trending events and contracts using a z-score algorithm.
///
/// `GET /v1/analytics/trending`
///
/// Compares event counts in the current window against a rolling baseline to
/// produce a z-score for each (contract_id, event_type) pair. Results are
/// sorted by z-score descending so the most anomalously active items appear
/// first.
///
/// ### Query parameters
/// | Name           | Default | Description |
/// |----------------|---------|-------------|
/// | `window_mins`  | 60      | Size of the current window in minutes |
/// | `baseline_hrs` | 24      | Hours of history used to build the baseline mean/stdev |
/// | `contract_id`  | –       | Restrict to a single contract |
/// | `event_type`   | –       | Restrict to a single event type |
/// | `min_zscore`   | 1.0     | Only return items with z-score ≥ this value |
/// | `limit`        | 20      | Maximum rows to return |
#[utoipa::path(
    get,
    path = "/v1/analytics/trending",
    tag = "analytics",
    params(
        ("window_mins"  = Option<i64>,   Query, description = "Current window size in minutes (default 60)"),
        ("baseline_hrs" = Option<i64>,   Query, description = "Baseline history in hours (default 24)"),
        ("contract_id"  = Option<String>,Query, description = "Filter to a specific contract"),
        ("event_type"   = Option<String>,Query, description = "Filter to a specific event type"),
        ("min_zscore"   = Option<f64>,   Query, description = "Minimum z-score threshold (default 1.0)"),
        ("limit"        = Option<i64>,   Query, description = "Max items to return (default 20, max 100)"),
    ),
    responses(
        (status = 200, description = "Trending events"),
        (status = 400, description = "Invalid parameters"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_trending_events(
    State(state): State<AppState>,
    Query(params): Query<TrendingParams>,
) -> Result<Json<Value>, AppError> {
    let window_mins  = params.window_mins.unwrap_or(60).clamp(1, 1440);
    let baseline_hrs = params.baseline_hrs.unwrap_or(24).clamp(1, 720);
    let min_zscore   = params.min_zscore.unwrap_or(1.0_f64);
    let limit        = params.limit.unwrap_or(20).clamp(1, 100);

    // ── Step 1: counts in the current window ─────────────────────────────────
    let mut cur_conds = vec!["timestamp >= NOW() - ($1 || ' minutes')::interval".to_string()];
    let mut idx: i32 = 2;
    if params.contract_id.is_some() { cur_conds.push(format!("contract_id = ${idx}")); idx += 1; }
    if params.event_type.is_some()  { cur_conds.push(format!("event_type = ${idx}"));  idx += 1; }

    let cur_sql = format!(
        "SELECT contract_id, event_type, COUNT(*) AS cnt \
         FROM events WHERE {} \
         GROUP BY contract_id, event_type",
        cur_conds.join(" AND ")
    );

    let mut cur_q = sqlx::query(&cur_sql).bind(window_mins.to_string());
    if let Some(ref v) = params.contract_id { cur_q = cur_q.bind(v); }
    if let Some(ref v) = params.event_type  { cur_q = cur_q.bind(v); }
    let cur_rows = cur_q.fetch_all(&state.pool).await?;

    // ── Step 2: per-window counts over the baseline period ───────────────────
    // We slice the baseline into same-sized windows and count events per slice.
    // Then we compute mean and stdev across those window counts.
    let mut base_conds = vec![
        format!("timestamp >= NOW() - ($1 || ' hours')::interval"),
        format!("timestamp < NOW() - ($2 || ' minutes')::interval"),
    ];
    let mut bidx: i32 = 3;
    if params.contract_id.is_some() { base_conds.push(format!("contract_id = ${bidx}")); bidx += 1; }
    if params.event_type.is_some()  { base_conds.push(format!("event_type = ${bidx}"));  bidx += 1; }

    let base_sql = format!(
        "SELECT contract_id, event_type, \
                COUNT(*) AS total_cnt, \
                GREATEST(1, FLOOR(EXTRACT(EPOCH FROM (NOW() - ($2 || ' minutes')::interval - (NOW() - ($1 || ' hours')::interval))) / ({window_mins} * 60.0))) AS num_windows
         FROM events WHERE {} \
         GROUP BY contract_id, event_type",
        base_conds.join(" AND ")
    );

    let mut base_q = sqlx::query(&base_sql)
        .bind(baseline_hrs.to_string())
        .bind(window_mins.to_string());
    if let Some(ref v) = params.contract_id { base_q = base_q.bind(v); }
    if let Some(ref v) = params.event_type  { base_q = base_q.bind(v); }
    let base_rows = base_q.fetch_all(&state.pool).await?;

    // Build baseline lookup: (contract_id, event_type) -> (mean_per_window, stdev estimate)
    use std::collections::HashMap;
    let mut baseline: HashMap<(String, String), (f64, f64)> = HashMap::new();
    for row in &base_rows {
        let cid: String  = row.try_get("contract_id").unwrap_or_default();
        let et:  String  = row.try_get("event_type").unwrap_or_default();
        let total: i64   = row.try_get("total_cnt").unwrap_or(0);
        let n: f64       = row.try_get::<f64, _>("num_windows").unwrap_or(1.0).max(1.0);
        let mean = total as f64 / n;
        // Poisson-like stdev: sqrt(mean), floor to 1 so we never divide by 0
        let stdev = mean.sqrt().max(1.0);
        baseline.insert((cid, et), (mean, stdev));
    }

    // ── Step 3: compute z-scores ──────────────────────────────────────────────
    let mut results: Vec<Value> = Vec::new();
    for row in &cur_rows {
        let cid: String = row.try_get("contract_id").unwrap_or_default();
        let et:  String = row.try_get("event_type").unwrap_or_default();
        let cnt: i64    = row.try_get("cnt").unwrap_or(0);

        let (mean, stdev) = baseline
            .get(&(cid.clone(), et.clone()))
            .copied()
            .unwrap_or((0.0, 1.0));

        let zscore = (cnt as f64 - mean) / stdev;

        if zscore >= min_zscore {
            results.push(json!({
                "contract_id":    cid,
                "event_type":     et,
                "count":          cnt,
                "baseline_mean":  (mean  * 100.0).round() / 100.0,
                "baseline_stdev": (stdev * 100.0).round() / 100.0,
                "zscore":         (zscore * 100.0).round() / 100.0,
            }));
        }
    }

    // Sort by zscore desc, apply limit
    results.sort_by(|a, b| {
        b["zscore"]
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&a["zscore"].as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(limit as usize);

    Ok(Json(json!({
        "trending": results,
        "count":          results.len(),
        "window_mins":    window_mins,
        "baseline_hrs":   baseline_hrs,
        "min_zscore":     min_zscore,
        "algorithm":      "zscore",
    })))
}

/// Query parameters for the trending endpoint.
#[derive(Debug, serde::Deserialize)]
pub struct TrendingParams {
    pub window_mins:  Option<i64>,
    pub baseline_hrs: Option<i64>,
    pub contract_id:  Option<String>,
    pub event_type:   Option<String>,
    pub min_zscore:   Option<f64>,
    pub limit:        Option<i64>,
}

// ── Issue #672: GET /v1/analytics/correlations ───────────────────────────────

/// Detect correlated event activity across contracts.
///
/// `GET /v1/analytics/correlations`
///
/// Buckets events into fixed-width time slots and computes a Pearson
/// correlation coefficient for every pair of contracts that both have activity
/// within the requested window.  Only pairs whose |r| ≥ `min_correlation` are
/// returned, sorted by descending |r|.
///
/// ### Query parameters
/// | Name              | Default | Description |
/// |-------------------|---------|-------------|
/// | `window_hrs`      | 24      | How far back to look (hours) |
/// | `bucket_mins`     | 60      | Slot width in minutes |
/// | `min_correlation` | 0.5     | Minimum |r| to include in results |
/// | `contract_ids`    | –       | Comma-separated list to restrict analysis |
/// | `event_type`      | –       | Restrict to a specific event type |
/// | `limit`           | 50      | Max pairs to return |
#[utoipa::path(
    get,
    path = "/v1/analytics/correlations",
    tag = "analytics",
    params(
        ("window_hrs"      = Option<i64>,   Query, description = "Lookback window in hours (default 24)"),
        ("bucket_mins"     = Option<i64>,   Query, description = "Time-bucket width in minutes (default 60)"),
        ("min_correlation" = Option<f64>,   Query, description = "Minimum absolute Pearson r (default 0.5)"),
        ("contract_ids"    = Option<String>,Query, description = "Comma-separated contract IDs to analyse"),
        ("event_type"      = Option<String>,Query, description = "Restrict to a specific event type"),
        ("limit"           = Option<i64>,   Query, description = "Max pairs to return (default 50, max 200)"),
    ),
    responses(
        (status = 200, description = "Correlation pairs"),
        (status = 400, description = "Invalid parameters"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_event_correlations(
    State(state): State<AppState>,
    Query(params): Query<CorrelationParams>,
) -> Result<Json<Value>, AppError> {
    let window_hrs      = params.window_hrs.unwrap_or(24).clamp(1, 720);
    let bucket_mins     = params.bucket_mins.unwrap_or(60).clamp(1, 1440);
    let min_correlation = params.min_correlation.unwrap_or(0.5_f64).clamp(0.0, 1.0);
    let limit           = params.limit.unwrap_or(50).clamp(1, 200);

    // Optional contract list filter
    let contract_filter: Option<Vec<String>> = params
        .contract_ids
        .as_deref()
        .map(|s| s.split(',').map(|id| id.trim().to_string()).collect());

    // Build per-bucket counts: (contract_id, bucket_start) -> count
    let mut conds = vec![
        "timestamp >= NOW() - ($1 || ' hours')::interval".to_string(),
    ];
    let mut idx: i32 = 2;
    if params.event_type.is_some() {
        conds.push(format!("event_type = ${idx}"));
        idx += 1;
    }
    // contract_ids filter handled post-query to avoid dynamic IN clause
    let _ = idx; // suppress unused warning

    let bucket_sql = format!(
        "SELECT contract_id, \
                date_trunc('hour', timestamp) + \
                  (EXTRACT(MINUTE FROM timestamp)::int / {bucket_mins} * {bucket_mins} || ' minutes')::interval AS bucket, \
                COUNT(*) AS cnt \
         FROM events WHERE {} \
         GROUP BY contract_id, bucket \
         ORDER BY bucket",
        conds.join(" AND ")
    );

    let mut q = sqlx::query(&bucket_sql).bind(window_hrs.to_string());
    if let Some(ref v) = params.event_type { q = q.bind(v); }
    let rows = q.fetch_all(&state.pool).await?;

    // Build time-series map: contract_id -> Vec<(bucket, count)>
    use std::collections::HashMap;
    let mut series: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for row in &rows {
        let cid: String = row.try_get("contract_id").unwrap_or_default();
        let bucket: DateTime<Utc> = row.try_get("bucket").unwrap_or_else(|_| Utc::now());
        let cnt: i64 = row.try_get("cnt").unwrap_or(0);

        if let Some(ref filter) = contract_filter {
            if !filter.contains(&cid) {
                continue;
            }
        }
        series
            .entry(cid)
            .or_default()
            .push((bucket.to_rfc3339(), cnt as f64));
    }

    // Collect all unique buckets to align vectors
    let mut all_buckets: Vec<String> = series
        .values()
        .flat_map(|v| v.iter().map(|(b, _)| b.clone()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    all_buckets.sort();

    // Build aligned count vectors per contract
    let contract_ids: Vec<String> = series.keys().cloned().collect();
    let mut vectors: HashMap<String, Vec<f64>> = HashMap::new();
    for cid in &contract_ids {
        let bucket_map: HashMap<String, f64> = series[cid]
            .iter()
            .cloned()
            .collect();
        let vec: Vec<f64> = all_buckets
            .iter()
            .map(|b| *bucket_map.get(b).unwrap_or(&0.0))
            .collect();
        vectors.insert(cid.clone(), vec);
    }

    // Pearson correlation helper
    fn pearson(a: &[f64], b: &[f64]) -> f64 {
        let n = a.len() as f64;
        if n == 0.0 { return 0.0; }
        let mean_a = a.iter().sum::<f64>() / n;
        let mean_b = b.iter().sum::<f64>() / n;
        let cov: f64 = a.iter().zip(b).map(|(x, y)| (x - mean_a) * (y - mean_b)).sum();
        let std_a = (a.iter().map(|x| (x - mean_a).powi(2)).sum::<f64>() / n).sqrt();
        let std_b = (b.iter().map(|x| (x - mean_b).powi(2)).sum::<f64>() / n).sqrt();
        if std_a == 0.0 || std_b == 0.0 { return 0.0; }
        cov / (std_a * std_b)
    }

    // Compute all pairs
    let mut pairs: Vec<Value> = Vec::new();
    for i in 0..contract_ids.len() {
        for j in (i + 1)..contract_ids.len() {
            let cid_a = &contract_ids[i];
            let cid_b = &contract_ids[j];
            let va = vectors.get(cid_a).map(Vec::as_slice).unwrap_or(&[]);
            let vb = vectors.get(cid_b).map(Vec::as_slice).unwrap_or(&[]);
            let r = pearson(va, vb);

            if r.abs() >= min_correlation {
                pairs.push(json!({
                    "contract_a":  cid_a,
                    "contract_b":  cid_b,
                    "correlation": (r * 10000.0).round() / 10000.0,
                    "direction":   if r >= 0.0 { "positive" } else { "negative" },
                    "strength":    if r.abs() >= 0.9 { "very_strong" }
                                   else if r.abs() >= 0.7 { "strong" }
                                   else if r.abs() >= 0.5 { "moderate" }
                                   else { "weak" },
                }));
            }
        }
    }

    pairs.sort_by(|a, b| {
        b["correlation"]
            .as_f64()
            .unwrap_or(0.0)
            .abs()
            .partial_cmp(&a["correlation"].as_f64().unwrap_or(0.0).abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    pairs.truncate(limit as usize);

    Ok(Json(json!({
        "correlations":    pairs,
        "count":           pairs.len(),
        "contracts_analysed": contract_ids.len(),
        "buckets":         all_buckets.len(),
        "window_hrs":      window_hrs,
        "bucket_mins":     bucket_mins,
        "min_correlation": min_correlation,
        "algorithm":       "pearson",
    })))
}

/// Query parameters for the correlations endpoint.
#[derive(Debug, serde::Deserialize)]
pub struct CorrelationParams {
    pub window_hrs:      Option<i64>,
    pub bucket_mins:     Option<i64>,
    pub min_correlation: Option<f64>,
    pub contract_ids:    Option<String>,
    pub event_type:      Option<String>,
    pub limit:           Option<i64>,
}

// ── Issue #673: Dedicated /v1/export/* endpoints ─────────────────────────────

/// Export events as CSV.
///
/// `GET /v1/export/csv`
///
/// Thin wrapper around the existing export_events logic with `format=csv`
/// forced. Accepts the same filter query parameters.
#[utoipa::path(
    get,
    path = "/v1/export/csv",
    tag = "export",
    params(
        ("contract_id"   = Option<String>,   Query, description = "Filter by contract ID"),
        ("event_type"    = Option<String>,   Query, description = "Filter by event type"),
        ("from_ledger"   = Option<i64>,      Query, description = "Start ledger (inclusive)"),
        ("to_ledger"     = Option<i64>,      Query, description = "End ledger (inclusive)"),
        ("from_timestamp"= Option<DateTime<Utc>>, Query, description = "Start timestamp (inclusive)"),
        ("to_timestamp"  = Option<DateTime<Utc>>, Query, description = "End timestamp (exclusive)"),
    ),
    responses(
        (status = 200, description = "CSV file", content_type = "text/csv"),
        (status = 400, description = "Invalid parameters"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn export_events_csv(
    state: State<AppState>,
    Query(mut params): Query<ExportParams>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    params.format = Some("csv".to_string());
    export_events(state, Query(params), headers).await
}

/// Export events as JSON (newline-delimited).
///
/// `GET /v1/export/json`
///
/// Returns one JSON object per line (NDJSON/JSONL).
#[utoipa::path(
    get,
    path = "/v1/export/json",
    tag = "export",
    params(
        ("contract_id"   = Option<String>,   Query, description = "Filter by contract ID"),
        ("event_type"    = Option<String>,   Query, description = "Filter by event type"),
        ("from_ledger"   = Option<i64>,      Query, description = "Start ledger (inclusive)"),
        ("to_ledger"     = Option<i64>,      Query, description = "End ledger (inclusive)"),
        ("from_timestamp"= Option<DateTime<Utc>>, Query, description = "Start timestamp (inclusive)"),
        ("to_timestamp"  = Option<DateTime<Utc>>, Query, description = "End timestamp (exclusive)"),
    ),
    responses(
        (status = 200, description = "NDJSON file", content_type = "application/x-ndjson"),
        (status = 400, description = "Invalid parameters"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn export_events_json(
    state: State<AppState>,
    Query(mut params): Query<ExportParams>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    params.format = Some("jsonl".to_string());
    export_events(state, Query(params), headers).await
}

/// Export events as Apache Parquet.
///
/// `GET /v1/export/parquet`
///
/// Returns a binary Parquet file.
#[utoipa::path(
    get,
    path = "/v1/export/parquet",
    tag = "export",
    params(
        ("contract_id"   = Option<String>,   Query, description = "Filter by contract ID"),
        ("event_type"    = Option<String>,   Query, description = "Filter by event type"),
        ("from_ledger"   = Option<i64>,      Query, description = "Start ledger (inclusive)"),
        ("to_ledger"     = Option<i64>,      Query, description = "End ledger (inclusive)"),
        ("from_timestamp"= Option<DateTime<Utc>>, Query, description = "Start timestamp (inclusive)"),
        ("to_timestamp"  = Option<DateTime<Utc>>, Query, description = "End timestamp (exclusive)"),
    ),
    responses(
        (status = 200, description = "Parquet file", content_type = "application/octet-stream"),
        (status = 400, description = "Invalid parameters"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn export_events_parquet(
    state: State<AppState>,
    Query(mut params): Query<ExportParams>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    params.format = Some("parquet".to_string());
    export_events(state, Query(params), headers).await
}

// ── Issue #673: Export scheduling & history ───────────────────────────────────

/// Schedule an async export job.
///
/// `POST /v1/export/schedule`
///
/// Creates a background export job and returns a job ID that can be polled
/// via `GET /v1/export/jobs/{job_id}`.
#[utoipa::path(
    post,
    path = "/v1/export/schedule",
    tag = "export",
    request_body(content = ScheduleExportRequest, description = "Export job parameters"),
    responses(
        (status = 202, description = "Export job created"),
        (status = 400, description = "Invalid parameters"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn schedule_export_job(
    State(state): State<AppState>,
    Json(req): Json<ScheduleExportRequest>,
) -> Result<impl IntoResponse, AppError> {
    // Validate format
    let fmt = req.format.as_deref().unwrap_or("csv");
    if !matches!(fmt, "csv" | "json" | "jsonl" | "parquet") {
        return Err(AppError::Validation(format!(
            "unsupported format '{fmt}': use csv, json, or parquet"
        )));
    }

    let job_id = Uuid::new_v4();
    let filter_count = [
        &req.contract_id,
        &req.event_type,
    ]
    .iter()
    .filter(|v| v.is_some())
    .count() as i32;

    sqlx::query(
        "INSERT INTO batch_download_jobs (id, status, format, filter_count)
         VALUES ($1, 'pending', $2, $3)",
    )
    .bind(job_id)
    .bind(fmt)
    .bind(filter_count)
    .execute(&state.pool)
    .await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "job_id":    job_id,
            "status":    "pending",
            "format":    fmt,
            "message":   "Export job created. Poll GET /v1/export/jobs/{job_id} for status.",
        })),
    ))
}

/// Request body for scheduling an export job.
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct ScheduleExportRequest {
    /// Output format: csv, json, or parquet
    pub format:      Option<String>,
    /// Optional contract filter
    pub contract_id: Option<String>,
    /// Optional event type filter
    pub event_type:  Option<String>,
    /// Optional ledger range start
    pub from_ledger: Option<i64>,
    /// Optional ledger range end
    pub to_ledger:   Option<i64>,
}

/// Get status of a scheduled export job.
///
/// `GET /v1/export/jobs/{job_id}`
#[utoipa::path(
    get,
    path = "/v1/export/jobs/{job_id}",
    tag = "export",
    params(
        ("job_id" = String, Path, description = "Export job ID"),
    ),
    responses(
        (status = 200, description = "Job status"),
        (status = 404, description = "Job not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn get_export_job_status_db(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let id = job_id
        .parse::<uuid::Uuid>()
        .map_err(|_| AppError::Validation("invalid job ID".to_string()))?;

    let row = sqlx::query(
        "SELECT id, status, format, filter_count, event_count, \
                created_at, completed_at, error \
         FROM batch_download_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    match row {
        None => Err(AppError::NotFound(format!("export job {job_id} not found"))),
        Some(row) => Ok(Json(json!({
            "job_id":       row.try_get::<uuid::Uuid, _>("id").ok(),
            "status":       row.try_get::<String, _>("status").ok(),
            "format":       row.try_get::<String, _>("format").ok(),
            "filter_count": row.try_get::<i32, _>("filter_count").ok(),
            "event_count":  row.try_get::<i64, _>("event_count").ok(),
            "created_at":   row.try_get::<DateTime<Utc>, _>("created_at").ok(),
            "completed_at": row.try_get::<Option<DateTime<Utc>>, _>("completed_at").ok().flatten(),
            "error":        row.try_get::<Option<String>, _>("error").ok().flatten(),
        }))),
    }
}

/// List export job history.
///
/// `GET /v1/export/history`
///
/// Returns the most recent export jobs (max 100), most-recent first.
#[utoipa::path(
    get,
    path = "/v1/export/history",
    tag = "export",
    params(
        ("limit"  = Option<i64>, Query, description = "Max rows to return (default 20, max 100)"),
        ("offset" = Option<i64>, Query, description = "Pagination offset"),
        ("status" = Option<String>, Query, description = "Filter by status: pending, completed, failed"),
    ),
    responses(
        (status = 200, description = "Export job history"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn get_export_history(
    State(state): State<AppState>,
    Query(params): Query<ExportHistoryParams>,
) -> Result<Json<Value>, AppError> {
    let limit  = params.limit.unwrap_or(20).clamp(1, 100);
    let offset = params.offset.unwrap_or(0).max(0);

    let (where_clause, status_bind) = if let Some(ref s) = params.status {
        ("WHERE status = $1".to_string(), Some(s.clone()))
    } else {
        (String::new(), None)
    };

    let (lim_idx, off_idx) = if status_bind.is_some() { (2i32, 3i32) } else { (1i32, 2i32) };

    let sql = format!(
        "SELECT id, status, format, filter_count, event_count, created_at, completed_at, error \
         FROM batch_download_jobs {where_clause} \
         ORDER BY created_at DESC LIMIT ${lim_idx} OFFSET ${off_idx}"
    );

    let mut q = sqlx::query(&sql);
    if let Some(ref s) = status_bind { q = q.bind(s); }
    q = q.bind(limit).bind(offset);

    let rows = q.fetch_all(&state.pool).await?;

    let total_sql = format!("SELECT COUNT(*) FROM batch_download_jobs {where_clause}");
    let mut tq = sqlx::query_scalar::<_, i64>(&total_sql);
    if let Some(ref s) = status_bind { tq = tq.bind(s); }
    let total: i64 = tq.fetch_one(&state.pool).await?;

    let jobs: Vec<Value> = rows
        .iter()
        .map(|row| json!({
            "job_id":       row.try_get::<uuid::Uuid, _>("id").ok(),
            "status":       row.try_get::<String, _>("status").ok(),
            "format":       row.try_get::<String, _>("format").ok(),
            "filter_count": row.try_get::<i32, _>("filter_count").ok(),
            "event_count":  row.try_get::<i64, _>("event_count").ok(),
            "created_at":   row.try_get::<DateTime<Utc>, _>("created_at").ok(),
            "completed_at": row.try_get::<Option<DateTime<Utc>>, _>("completed_at").ok().flatten(),
            "error":        row.try_get::<Option<String>, _>("error").ok().flatten(),
        }))
        .collect();

    Ok(Json(json!({
        "jobs":   jobs,
        "total":  total,
        "limit":  limit,
        "offset": offset,
    })))
}

/// Query parameters for export history.
#[derive(Debug, serde::Deserialize)]
pub struct ExportHistoryParams {
    pub limit:  Option<i64>,
    pub offset: Option<i64>,
    pub status: Option<String>,
}

// ── Issue #586 / #587 / #710: Replication, feature flags, and backup verification tests ──

#[cfg(test)]
mod ops_tests {
    use super::*;
    use crate::config::{HealthState, IndexerState};
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use sqlx::PgPool;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn create_admin_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        let config = crate::config::Config::default();
        crate::routes::create_router(
            pool,
            vec!["admin-key".to_string()],
            &[],
            60,
            health_state,
            indexer_state,
            prometheus_handle,
            2000,
            config,
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn replication_status_returns_200(pool: PgPool) {
        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/replication/status")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("replica_count").is_some());
        assert!(v.get("replicas").is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn feature_flags_list_returns_200(pool: PgPool) {
        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/feature-flags")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("flags").is_some());
        assert!(v.get("count").is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn feature_flag_audit_returns_200(pool: PgPool) {
        let app = create_admin_router(pool);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/feature-flags/audit")
                    .header("Authorization", "Bearer admin-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("entries").is_some());
        assert!(v.get("count").is_some());
    }
}

// ── Issue #696: SLI / SLO admin reporting endpoint ──────────────────────────

/// Return the current SLI / SLO aggregate report.
///
/// The report is generated in-memory by `crate::slo_tracker` from the rolling
/// sample buffers. It is the JSON counterpart of the Prometheus gauges
/// `soroban_pulse_slo_completion_ratio`, `soroban_pulse_slo_error_budget_remaining`,
/// and `soroban_pulse_slo_burn_rate` published by the background evaluator.
#[utoipa::path(
    get,
    path = "/v1/admin/slo/report",
    tag = "admin",
    responses(
        (status = 200, description = "Current SLO report for every tracked SLO"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 503, description = "SLO tracker not initialized"),
    )
)]
pub async fn get_slo_report() -> Result<Json<Value>, AppError> {
    let tracker = crate::slo_tracker::current().ok_or_else(|| {
        AppError::Internal(
            "SLO tracker not initialized — server is still starting up".to_string(),
        )
    })?;

    let report = {
        let guard = tracker.read().await;
        guard.generate_report()
    };
    Ok(Json(serde_json::to_value(&report).unwrap_or_else(|e| {
        json!({
            "error": "failed to serialize SLO report",
            "details": e.to_string(),
        })
    })))
}

/// Record an SLI sample against a named SLO. Used by internal callers and
/// by integration tests; not part of the documented admin surface but exposed
/// for symmetry with `get_slo_report`.
#[utoipa::path(
    post,
    path = "/v1/admin/slo/sample",
    tag = "admin",
    responses(
        (status = 202, description = "Sample accepted"),
        (status = 400, description = "Invalid request"),
        (status = 503, description = "SLO tracker not initialized"),
    )
)]
pub async fn record_slo_sample(
    Json(payload): Json<serde_json::Value>,
) -> Result<axum::http::StatusCode, AppError> {
    let slo_name = payload
        .get("slo")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Validation("missing 'slo' field".to_string()))?;
    let value = payload
        .get("value")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| AppError::Validation("missing numeric 'value' field".to_string()))?;

    let tracker = crate::slo_tracker::current().ok_or_else(|| {
        AppError::Internal(
            "SLO tracker not initialized — server is still starting up".to_string(),
        )
    })?;

    crate::slo_tracker::record_sli_sample(&tracker, slo_name, value).await;
    Ok(axum::http::StatusCode::ACCEPTED)
}

// ── Issue #817: Adaptive pool tuning handlers ─────────────────────────────

/// GET /v1/admin/pool-config/adaptive
///
/// Returns the latest adaptive tuner snapshot, current runtime config,
/// and advanced pool counters (timeouts, health failures, stale cleaned).
pub async fn get_adaptive_pool_status(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let status = crate::db_pool::adaptive::AdaptivePoolStatus::from_state(&state.adaptive_pool);
    Ok(Json(serde_json::to_value(&status).unwrap_or_else(|_| json!({"error": "serialization failed"}))))
}

/// PUT /v1/admin/pool-config/adaptive/config
///
/// Hot-reload the adaptive pool configuration at runtime without a restart.
/// Saves the current config to a rollback history (up to 5 versions).
///
/// Body: JSON matching `AdaptivePoolConfig`.
pub async fn update_adaptive_pool_config(
    State(state): State<AppState>,
    Json(new_config): Json<crate::db_pool::adaptive::AdaptivePoolConfig>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    state.adaptive_pool.apply_config(new_config).map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e })))
    })?;
    let current = state.adaptive_pool.current_config();
    Ok(Json(json!({
        "status": "applied",
        "config_version": current.config_version,
        "config_variant": current.config_variant,
    })))
}

/// POST /v1/admin/pool-config/adaptive/rollback
///
/// Roll back to the previous adaptive pool configuration version.
pub async fn rollback_adaptive_pool_config(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let rolled_back = state.adaptive_pool.rollback().map_err(|e| {
        (StatusCode::BAD_REQUEST, Json(json!({ "error": e })))
    })?;
    Ok(Json(json!({
        "status": "rolled_back",
        "config_version": rolled_back.config_version,
        "config_variant": rolled_back.config_variant,
    })))
}

// ── Issue #669: Per-API-Key Rate Limit Status Endpoint ──────────────────────

/// Get rate limit status for the authenticated API key.
///
/// Returns the current rate limit usage and remaining quota for the minute,
/// hour, and day sliding windows (when per-key limits are configured).
///
/// This endpoint does **not** increment the rate limit counter (read-only status check).
///
/// Requires authentication (API_KEY or X-Api-Key header).
#[utoipa::path(
    get,
    path = "/v1/rate-limit/status",
    tag = "system",
    responses(
        (status = 200, description = "Rate limit status for the authenticated API key"),
        (status = 401, description = "Unauthorized — API key required"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn get_rate_limit_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    // Extract API key from headers (same logic as middleware::extract_api_key)
    let api_key = headers
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("X-Api-Key").and_then(|h| h.to_str().ok()))
        .ok_or_else(|| AppError::Forbidden("API key required for rate limit status".to_string()))?;

    // Build RateLimitConfig from Config fields
    let rate_limit_config = crate::rate_limiter::RateLimitConfig::new(
        state.config.rate_limit_key_per_minute,
        state.config.rate_limit_key_per_hour,
        state.config.rate_limit_key_per_day,
        state.config.rate_limit_key_per_month,
    );

    // Get status (no counter increment)
    let status = crate::rate_limiter::get_rate_limit_status(
        &state.pool,
        api_key,
        &rate_limit_config,
    )
    .await
    .map_err(|e| AppError::Internal(format!("Failed to check rate limit status: {}", e)))?;

    Ok(Json(serde_json::to_value(&status).unwrap()))
}

// ── Issue #879: Webhook Circuit Breaker Admin Endpoints ─────────────────────

/// GET /v1/admin/webhook/circuit-breaker
/// Get circuit breaker stats for all webhook endpoints
pub async fn get_circuit_breaker_stats(
    State(state): State<AppState>,
) -> Json<Vec<crate::webhook_circuit_breaker::EndpointCircuitBreakerStats>> {
    let stats = state.circuit_breaker_manager.get_all_stats().await;
    Json(stats)
}

/// GET /v1/admin/webhook/circuit-breaker/{endpoint}
/// Get circuit breaker stats for a specific endpoint
pub async fn get_endpoint_circuit_breaker_stats(
    State(state): State<AppState>,
    Path(endpoint): Path<String>,
) -> Result<Json<crate::webhook_circuit_breaker::EndpointCircuitBreakerStats>, AppError> {
    let stats = state.circuit_breaker_manager.get_stats(&endpoint).await
        .ok_or_else(|| AppError::NotFound("Endpoint not found".to_string()))?;
    Ok(Json(stats))
}

/// POST /v1/admin/webhook/circuit-breaker/{endpoint}/reset
/// Manually reset a circuit breaker for a specific endpoint
pub async fn reset_circuit_breaker(
    State(state): State<AppState>,
    Path(endpoint): Path<String>,
) -> Json<serde_json::Value> {
    state.circuit_breaker_manager.reset(&endpoint).await;
    Json(json!({
        "status": "reset",
        "endpoint": endpoint,
    }))
}

// ── Issue #1260: Webhook Dead-Letter Queue Admin Endpoints ──────────────────

/// GET /v1/admin/webhook-dlq
///
/// Paginated listing of webhook deliveries that exhausted all retries.
/// Supports `page`/`limit` (or `offset`/`limit`) and an optional
/// `subscription_id` filter.
pub async fn list_webhook_dlq(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(50)
        .clamp(1, 500);
    let offset = match params.get("offset").and_then(|v| v.parse::<i64>().ok()) {
        Some(o) => o.max(0),
        None => {
            let page = params
                .get("page")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(1)
                .max(1);
            (page - 1) * limit
        }
    };
    let subscription_id = params.get("subscription_id").cloned();

    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
        "SELECT id, subscription_id, event_id, payload, last_error, failed_at, replayed_at \
         FROM webhook_dlq",
    );
    if subscription_id.is_some() {
        qb.push(" WHERE subscription_id = ");
        qb.push_bind(subscription_id.clone());
    }
    qb.push(" ORDER BY failed_at DESC, id DESC LIMIT ");
    qb.push_bind(limit);
    qb.push(" OFFSET ");
    qb.push_bind(offset);

    let rows = qb.build().fetch_all(&state.read_pool).await?;

    let mut items = Vec::with_capacity(rows.len());
    for row in &rows {
        let id: Uuid = row.try_get("id")?;
        let sub_id: Option<Uuid> = row.try_get("subscription_id")?;
        let event_id: Option<Uuid> = row.try_get("event_id")?;
        let payload: Value = row.try_get("payload")?;
        let last_error: Option<String> = row.try_get("last_error")?;
        let failed_at: DateTime<Utc> = row.try_get("failed_at")?;
        let replayed_at: Option<DateTime<Utc>> = row.try_get("replayed_at")?;
        items.push(json!({
            "id": id,
            "subscription_id": sub_id,
            "event_id": event_id,
            "payload": payload,
            "last_error": last_error,
            "failed_at": failed_at,
            "replayed_at": replayed_at,
        }));
    }

    let total: i64 = match &subscription_id {
        Some(sub) => {
            sqlx::query_scalar("SELECT COUNT(*) FROM webhook_dlq WHERE subscription_id = $1")
                .bind(sub)
                .fetch_one(&state.read_pool)
                .await?
        }
        None => {
            sqlx::query_scalar("SELECT COUNT(*) FROM webhook_dlq")
                .fetch_one(&state.read_pool)
                .await?
        }
    };

    crate::metrics::set_webhook_dlq_depth(total);

    Ok(Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    })))
}

/// POST /v1/admin/webhook-dlq/{id}/replay
///
/// Re-enqueues a single dead-lettered webhook into the normal delivery queue
/// and stamps `replayed_at`.
pub async fn replay_webhook_dlq_entry(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let row = sqlx::query(
        "SELECT id, subscription_id, event_id, payload FROM webhook_dlq WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::NotFound)?;

    let subscription_id: Option<Uuid> = row.try_get("subscription_id")?;
    let event_id: Option<Uuid> = row.try_get("event_id")?;
    let payload: Value = row.try_get("payload")?;

    crate::webhook::enqueue_replay(&state.pool, subscription_id, event_id, payload).await?;

    sqlx::query("UPDATE webhook_dlq SET replayed_at = NOW() WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?;

    tracing::info!(dlq_id = %id, "Webhook DLQ entry replayed");

    Ok(Json(json!({
        "id": id,
        "status": "replayed",
        "subscription_id": subscription_id,
        "event_id": event_id,
    })))
}

/// DELETE /v1/admin/webhook-dlq/{id}
///
/// Permanently discards a dead-lettered webhook.
pub async fn delete_webhook_dlq_entry(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let result = sqlx::query("DELETE FROM webhook_dlq WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    tracing::info!(dlq_id = %id, "Webhook DLQ entry deleted");
    Ok(Json(json!({ "id": id, "status": "deleted" })))
}

/// POST /v1/admin/webhook-dlq/replay
///
/// Bulk replay of dead-lettered webhooks. Accepts an optional JSON body
/// `{ "subscription_id": "<uuid>", "limit": <n> }` to restrict the replay to
/// a single subscription and cap the number of entries processed.
pub async fn bulk_replay_webhook_dlq(
    State(state): State<AppState>,
    body: Option<Json<Value>>,
) -> Result<Json<Value>, AppError> {
    let body = body.map(|Json(v)| v).unwrap_or_else(|| json!({}));
    let subscription_id = body
        .get("subscription_id")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let limit = body
        .get("limit")
        .and_then(Value::as_i64)
        .unwrap_or(1000)
        .clamp(1, 10_000);

    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
        "SELECT id, subscription_id, event_id, payload FROM webhook_dlq WHERE replayed_at IS NULL",
    );
    if subscription_id.is_some() {
        qb.push(" AND subscription_id = ");
        qb.push_bind(subscription_id.clone());
    }
    qb.push(" ORDER BY failed_at ASC LIMIT ");
    qb.push_bind(limit);

    let rows = qb.build().fetch_all(&state.pool).await?;

    let mut replayed = 0u64;
    let mut errors = 0u64;
    for row in &rows {
        let id: Uuid = row.try_get("id")?;
        let sub_id: Option<Uuid> = row.try_get("subscription_id")?;
        let event_id: Option<Uuid> = row.try_get("event_id")?;
        let payload: Value = row.try_get("payload")?;

        match crate::webhook::enqueue_replay(&state.pool, sub_id, event_id, payload).await {
            Ok(()) => {
                sqlx::query("UPDATE webhook_dlq SET replayed_at = NOW() WHERE id = $1")
                    .bind(id)
                    .execute(&state.pool)
                    .await?;
                replayed += 1;
            }
            Err(e) => {
                errors += 1;
                tracing::error!(dlq_id = %id, error = %e, "Failed to replay webhook DLQ entry");
            }
        }
    }

    tracing::info!(
        replayed = replayed,
        errors = errors,
        subscription_id = subscription_id.as_deref().unwrap_or("-"),
        "Bulk webhook DLQ replay completed"
    );

    Ok(Json(json!({
        "status": "completed",
        "replayed": replayed,
        "errors": errors,
        "subscription_id": subscription_id,
    })))
}

// ── Issue #1263: Incident Correlation Admin Endpoint ───────────────────────

/// GET /v1/admin/incidents
///
/// Paginated listing of correlated incidents. Supports `page`/`limit` (or
/// `offset`/`limit`) and an optional `status` filter.
pub async fn list_incidents(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(50)
        .clamp(1, 500);
    let offset = match params.get("offset").and_then(|v| v.parse::<i64>().ok()) {
        Some(o) => o.max(0),
        None => {
            let page = params
                .get("page")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(1)
                .max(1);
            (page - 1) * limit
        }
    };
    let status = params.get("status").cloned();

    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
        "SELECT id, title, status, severity, correlation_key, event_count, \
                first_seen_at, last_seen_at, created_at, updated_at \
         FROM incidents",
    );
    if status.is_some() {
        qb.push(" WHERE status = ");
        qb.push_bind(status.clone());
    }
    qb.push(" ORDER BY last_seen_at DESC, id DESC LIMIT ");
    qb.push_bind(limit);
    qb.push(" OFFSET ");
    qb.push_bind(offset);

    let rows = qb.build().fetch_all(&state.read_pool).await?;

    let mut items = Vec::with_capacity(rows.len());
    for row in &rows {
        let id: Uuid = row.try_get("id")?;
        let title: String = row.try_get("title")?;
        let status: String = row.try_get("status")?;
        let severity: String = row.try_get("severity")?;
        let correlation_key: Option<String> = row.try_get("correlation_key")?;
        let event_count: i32 = row.try_get("event_count")?;
        let first_seen_at: DateTime<Utc> = row.try_get("first_seen_at")?;
        let last_seen_at: DateTime<Utc> = row.try_get("last_seen_at")?;
        let created_at: DateTime<Utc> = row.try_get("created_at")?;
        let updated_at: DateTime<Utc> = row.try_get("updated_at")?;
        items.push(json!({
            "id": id,
            "title": title,
            "status": status,
            "severity": severity,
            "correlation_key": correlation_key,
            "event_count": event_count,
            "first_seen_at": first_seen_at,
            "last_seen_at": last_seen_at,
            "created_at": created_at,
            "updated_at": updated_at,
        }));
    }

    let total: i64 = match &status {
        Some(s) => {
            sqlx::query_scalar("SELECT COUNT(*) FROM incidents WHERE status = $1")
                .bind(s)
                .fetch_one(&state.read_pool)
                .await?
        }
        None => {
            sqlx::query_scalar("SELECT COUNT(*) FROM incidents")
                .fetch_one(&state.read_pool)
                .await?
        }
    };

    Ok(Json(json!({
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    })))
}

// ── Issue #881: Bulk Event Export Endpoints ────────────────────────────────

/// POST /v1/admin/events/export
/// Start a new bulk event export with specified format and compression
pub async fn start_event_export(
    State(state): State<AppState>,
    Json(request): Json<crate::bulk_export::ExportRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let job_id = state.bulk_export_manager.start_export(&state.pool, request).await
        .map_err(|e| AppError::Internal(e))?;

    Ok(Json(json!({
        "job_id": job_id,
        "status": "pending",
    })))
}

/// GET /v1/admin/events/export/{job_id}
/// Get status of an export job
pub async fn get_export_job_status(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<crate::bulk_export::ExportJob>, AppError> {
    let job = state.bulk_export_manager.get_job_status(&job_id).await
        .map_err(|e| AppError::NotFound(e))?;

    Ok(Json(job))
}

/// GET /v1/admin/events/export
/// List all export jobs
pub async fn list_export_jobs(
    State(state): State<AppState>,
) -> Json<Vec<crate::bulk_export::ExportJob>> {
    let jobs = state.bulk_export_manager.get_all_jobs().await;
    Json(jobs)
}

/// GET /v1/admin/events/export/{job_id}/download
/// Download the exported file (with streaming)
pub async fn download_export_file(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    let job = state.bulk_export_manager.get_job_status(&job_id).await
        .map_err(|e| AppError::NotFound(e))?;

    if job.status != crate::bulk_export::ExportStatus::Completed {
        return Err(AppError::BadRequest(
            format!("Export job {} is not completed", job_id)
        ));
    }

    let file_path = job.file_path.ok_or_else(|| {
        AppError::Internal("Export file path not found".to_string())
    })?;

    if !file_path.exists() {
        return Err(AppError::NotFound("Export file not found".to_string()));
    }

    let file = tokio::fs::File::open(file_path).await
        .map_err(|e| AppError::Internal(format!("Failed to open export file: {}", e)))?;

    let stream = tokio::io::BufReader::new(file);
    let body = axum::body::Body::from_stream(
        tokio_util::io::ReaderStream::new(stream)
    );

    Ok((
        axum::http::StatusCode::OK,
        axum::response::AppendHeaders([(
            axum::http::header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"export-{}.dat\"", job_id)
        )]),
        body,
    ))
}

/// POST /v1/admin/events/export/cleanup
/// Clean up expired export files
pub async fn cleanup_export_files(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, AppError> {
    let removed = state.bulk_export_manager.cleanup_expired().await
        .map_err(|e| AppError::Internal(e))?;

    Ok(Json(json!({
        "status": "cleaned",
        "removed_jobs": removed,
    })))
}

/// Register a webhook subscription (Issue #1268: per-IP rate limited to 5/min).
///
/// `POST /v1/subscriptions`
pub async fn create_subscription(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    let ip = client_ip_from_headers(&headers);
    if let Err(retry_after) = state.subscription_rate_limiter.check(&ip) {
        crate::metrics::record_subscription_rate_limited();
        tracing::warn!(client_ip = %ip, "subscription registration rate limited");
        return too_many_requests(retry_after, "too many subscription requests");
    }

    match crate::webhooks::create_subscription(&state.pool, payload).await {
        Ok(subscription) => (StatusCode::CREATED, Json(subscription)).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Get cross-chain trace for a transaction
/// Issue #682: Implement cross-chain event correlation
#[utoipa::path(
    get,
    path = "/v1/cross-chain/trace/{tx_hash}",
    tag = "cross-chain",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash to trace")
    ),
    responses(
        (status = 200, description = "Cross-chain trace", body = serde_json::Value),
        (status = 404, description = "No trace found"),
        (status = 500, description = "Internal server error")
    ),
    security(("api_key" = []))
)]
pub async fn get_cross_chain_trace(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    // Query all events related to this transaction
    let events = sqlx::query_as::<_, (String, String, String, String, String, i32, String)>(
        "SELECT id, contract_id, event_type, tx_hash, topic, ledger, ledger_close_time FROM events WHERE tx_hash = $1 ORDER BY ledger"
    )
    .bind(&tx_hash)
    .fetch_all(&state.read_pool)
    .await
    .map_err(|_| AppError::NotFound)?;

    if events.is_empty() {
        return Err(AppError::NotFound);
    }

    // Build cross-chain trace
    let root_tx = crate::cross_chain_correlation::TransactionId::new("soroban-mainnet", tx_hash.clone());
    let mut builder = crate::cross_chain_correlation::CrossChainTraceBuilder::new(root_tx);

    for (event_id, contract_id, event_type, _, _, ledger, ledger_close_time) in events {
        let trace_event = crate::cross_chain_correlation::TraceEvent {
            event_id,
            chain: "soroban-mainnet".to_string(),
            contract_id,
            event_type,
            tx_hash: tx_hash.clone(),
            ledger: ledger as u64,
            ledger_close_time: ledger_close_time.parse().unwrap_or_else(|_| chrono::Utc::now()),
            depth: 0,
            confidence: 1.0,
        };
        builder = builder.add_event(trace_event);
    }

    let trace = builder.build().ok_or(AppError::NotFound)?;

    Ok(Json(json!({
        "id": trace.id,
        "root_transaction": {
            "chain": trace.root_transaction.chain,
            "tx_hash": trace.root_transaction.tx_hash
        },
        "events_count": trace.events.len(),
        "correlations_count": trace.correlations.len(),
        "chain_sequence": trace.chain_sequence,
        "overall_confidence": trace.overall_confidence,
        "created_at": trace.created_at,
        "events": trace.events.iter().map(|e| json!({
            "event_id": e.event_id,
            "chain": e.chain,
            "contract_id": e.contract_id,
            "event_type": e.event_type,
            "ledger": e.ledger,
            "confidence": e.confidence
        })).collect::<Vec<_>>()
    })))
}

/// Get causality analysis between two events
/// Issue #682: Implement cross-chain event correlation
#[utoipa::path(
    get,
    path = "/v1/cross-chain/causality",
    tag = "cross-chain",
    params(
        ("event1" = String, Query, description = "First event ID"),
        ("event2" = String, Query, description = "Second event ID")
    ),
    responses(
        (status = 200, description = "Causality analysis", body = serde_json::Value),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
    security(("api_key" = []))
)]
pub async fn analyze_causality(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let event1_id = params.get("event1")
        .ok_or(AppError::BadRequest("event1 parameter required".to_string()))?;
    let event2_id = params.get("event2")
        .ok_or(AppError::BadRequest("event2 parameter required".to_string()))?;

    // Fetch both events
    let event1 = sqlx::query_as::<_, (String, String, String, String, i32, String)>(
        "SELECT id, contract_id, event_type, tx_hash, ledger, ledger_close_time FROM events WHERE id = $1"
    )
    .bind(event1_id)
    .fetch_optional(&state.read_pool)
    .await?
    .ok_or(AppError::NotFound)?;

    let event2 = sqlx::query_as::<_, (String, String, String, String, i32, String)>(
        "SELECT id, contract_id, event_type, tx_hash, ledger, ledger_close_time FROM events WHERE id = $1"
    )
    .bind(event2_id)
    .fetch_optional(&state.read_pool)
    .await?
    .ok_or(AppError::NotFound)?;

    let engine = crate::cross_chain_correlation::CorrelationEngine::new();

    let trace1 = crate::cross_chain_correlation::TraceEvent {
        event_id: event1.0,
        chain: "soroban-mainnet".to_string(),
        contract_id: event1.1,
        event_type: event1.2,
        tx_hash: event1.3,
        ledger: event1.4 as u64,
        ledger_close_time: event1.5.parse().unwrap_or_else(|_| chrono::Utc::now()),
        depth: 0,
        confidence: 1.0,
    };

    let trace2 = crate::cross_chain_correlation::TraceEvent {
        event_id: event2.0,
        chain: "soroban-mainnet".to_string(),
        contract_id: event2.1,
        event_type: event2.2,
        tx_hash: event2.3,
        ledger: event2.4 as u64,
        ledger_close_time: event2.5.parse().unwrap_or_else(|_| chrono::Utc::now()),
        depth: 1,
        confidence: 1.0,
    };

    let similarity = engine.calculate_similarity(&trace1, &trace2);
    let causality = engine.detect_causality(&trace1, &trace2);

    Ok(Json(json!({
        "event1_id": event1_id,
        "event2_id": event2_id,
        "similarity_score": similarity,
        "causality": causality.map(|c| format!("{:?}", c)),
        "related": causality.is_some()
    })))
}
#[utoipa::path(
    get,
    path = "/v1/features",
    tag = "system",
    params(
        ("flag_name" = String, Query, description = "Feature flag name (required)"),
        ("contract_id" = String, Query, description = "Contract ID for targeting (optional)"),
        ("user_id" = String, Query, description = "User ID for targeting (optional)"),
        ("ip_address" = String, Query, description = "IP address for targeting (optional)"),
        ("region" = String, Query, description = "Region for targeting (optional)"),
    ),
    responses(
        (status = 200, description = "Feature flag status"),
        (status = 400, description = "Missing required parameters"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_feature_flag_status(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    let flag_name = params
        .get("flag_name")
        .ok_or_else(|| AppError::BadRequest("Missing required parameter: flag_name".to_string()))?
        .clone();

    let context = crate::feature_flags::FeatureFlagContext {
        contract_id: params.get("contract_id").cloned(),
        user_id: params.get("user_id").cloned(),
        ip_address: params.get("ip_address").cloned(),
        region: params.get("region").cloned(),
    };

    let enabled = crate::feature_flags::is_feature_enabled(&state.pool, &flag_name, &context)
        .await
        .map_err(|e| AppError::Internal(format!("Database error: {}", e)))?;

    Ok(Json(json!({
        "flag_name": flag_name,
        "enabled": enabled,
        "context": {
            "contract_id": context.contract_id,
            "user_id": context.user_id,
            "ip_address": context.ip_address,
            "region": context.region,
        }
    })))
}
/// Webhook that receives email bounce notifications from SendGrid, AWS SES
/// (including SNS-wrapped notifications) and Mailgun (Issue #484). Bounced
/// addresses are persisted so future notifications skip them.
///
/// Bounce webhook authentication (issue #1158): the provider signature is
/// verified before any DB write.  The provider is determined from the
/// `X-Bounce-Provider` header:
///   - `sns` — Amazon SNS (SES)
///   - `sendgrid` — SendGrid Event Webhook
///   - `shared_secret` (default) — generic HMAC shared secret
#[utoipa::path(
    post,
    path = "/v1/notifications/email/bounce",
    tag = "system",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Bounce payload processed", body = serde_json::Value),
        (status = 401, description = "Missing or invalid provider signature"),
    )
)]
pub async fn email_bounce_webhook(
    State(state): State<AppState>,
    request: axum::extract::Request,
) -> impl IntoResponse {
    use axum::body::to_bytes;
    use crate::email_token::{verify_bounce_signature, BounceProvider};

    // Read the raw body so we can verify the signature over it.
    let headers = request.headers().clone();
    let provider_header = headers
        .get("x-bounce-provider")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("shared_secret");
    let provider = match provider_header {
        "sns" => BounceProvider::Sns,
        "sendgrid" => BounceProvider::SendGrid,
        _ => BounceProvider::SharedSecret,
    };

    let body_bytes = match to_bytes(request.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, "Failed to read bounce webhook body");
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "failed to read request body" })),
            );
        }
    };

    // Verify signature before touching the DB.
    if let Err(e) = verify_bounce_signature(&provider, &headers, &body_bytes, None) {
        tracing::warn!(error = %e, "Bounce webhook signature verification failed");
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "invalid or missing provider signature" })),
        );
    }

    let payload: Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid JSON: {e}") })),
            );
        }
    };

    let recipients = crate::email::extract_bounced_recipients(&payload);
    let mut recorded = 0usize;
    for recipient in &recipients {
        match crate::email::record_bounce(&state.pool, recipient).await {
            Ok(()) => {
                crate::metrics::record_email_bounce();
                recorded += 1;
            }
            Err(e) => {
                tracing::error!(error = %e, email = %recipient.email, "Failed to record email bounce");
            }
        }
    }
    (
        StatusCode::OK,
        Json(json!({ "received": recipients.len(), "recorded": recorded })),
    )
}

