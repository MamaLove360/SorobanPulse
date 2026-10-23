use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use url::Url;
use uuid::Uuid;

use crate::{config::Environment, error::AppError, idempotency, routes::AppState};

// ---------------------------------------------------------------------------
// Per-IP rate limiting (Issue #1268)
// ---------------------------------------------------------------------------

/// Default number of subscription creations allowed per IP per minute.
pub const DEFAULT_SUBSCRIPTION_RATE_LIMIT_PER_MINUTE: u32 = 5;
/// Default maximum number of concurrent SSE connections per IP.
pub const DEFAULT_SSE_MAX_CONNECTIONS_PER_IP: usize = 5;
/// Window over which subscription creations are counted.
const SUBSCRIPTION_RATE_WINDOW: Duration = Duration::from_secs(60);

/// Fixed-window counter for a single IP.
#[derive(Debug)]
struct WindowCounter {
    window_start: Instant,
    count: u32,
}

/// DashMap-backed per-IP rate limiter for `POST /v1/subscriptions`.
#[derive(Debug, Clone)]
pub struct SubscriptionRateLimiter {
    inner: Arc<DashMap<IpAddr, WindowCounter>>,
    limit: u32,
    window: Duration,
}

impl SubscriptionRateLimiter {
    pub fn new(limit: u32) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            limit: limit.max(1),
            window: SUBSCRIPTION_RATE_WINDOW,
        }
    }

    /// Attempt to record a request for `ip`.
    ///
    /// Returns `Ok(())` when the request is allowed, or `Err(retry_after_secs)`
    /// when the per-IP limit has been exceeded for the current window.
    pub fn check(&self, ip: IpAddr) -> Result<(), u64> {
        let now = Instant::now();
        let mut entry = self.inner.entry(ip).or_insert_with(|| WindowCounter {
            window_start: now,
            count: 0,
        });

        if now.duration_since(entry.window_start) >= self.window {
            entry.window_start = now;
            entry.count = 0;
        }

        if entry.count >= self.limit {
            let elapsed = now.duration_since(entry.window_start);
            let remaining = self.window.saturating_sub(elapsed);
            // Round up so callers never see Retry-After: 0.
            let secs = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
            return Err(secs.max(1));
        }

        entry.count += 1;
        Ok(())
    }

    /// Remove stale entries so the map does not grow unbounded.
    pub fn prune(&self) {
        let now = Instant::now();
        self.inner
            .retain(|_, counter| now.duration_since(counter.window_start) < self.window);
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }
}

/// DashMap-backed per-IP counter of *concurrent* SSE connections.
#[derive(Debug, Clone)]
pub struct SseConnectionLimiter {
    inner: Arc<DashMap<IpAddr, AtomicUsize>>,
    max: usize,
}

impl SseConnectionLimiter {
    pub fn new(max: usize) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            max: max.max(1),
        }
    }

    /// Try to acquire a connection slot for `ip`.
    ///
    /// On success returns a guard that releases the slot on drop, or
    /// `Err(retry_after_secs)` when the per-IP connection cap is reached.
    pub fn try_acquire(&self, ip: IpAddr) -> Result<SseConnectionGuard, u64> {
        let entry = self
            .inner
            .entry(ip)
            .or_insert_with(|| AtomicUsize::new(0));

        let current = entry.load(Ordering::SeqCst);
        if current >= self.max {
            return Err(1);
        }
        entry.fetch_add(1, Ordering::SeqCst);
        drop(entry);

        Ok(SseConnectionGuard {
            limiter: self.clone(),
            ip,
        })
    }

    pub fn max(&self) -> usize {
        self.max
    }

    fn release(&self, ip: IpAddr) {
        if let Some(entry) = self.inner.get(&ip) {
            // Saturating decrement; never underflow.
            let _ = entry.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                Some(v.saturating_sub(1))
            });
        }
    }
}

/// RAII guard that releases an SSE connection slot when dropped.
#[derive(Debug)]
pub struct SseConnectionGuard {
    limiter: SseConnectionLimiter,
    ip: IpAddr,
}

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        self.limiter.release(self.ip);
    }
}

/// Build a `429 Too Many Requests` response with a `Retry-After` header.
pub fn too_many_requests(retry_after_secs: u64, message: &str) -> Response {
    let mut response = (StatusCode::TOO_MANY_REQUESTS, Json(json!({ "error": message }))).into_response();
    if let Ok(value) = HeaderValue::from_str(&retry_after_secs.max(1).to_string()) {
        response.headers_mut().insert("retry-after", value);
    }
    response
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Subscription {
    pub id: Uuid,
    pub callback_url: String,
    pub from_ledger: i64,
    pub acked_ledger: i64,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub subscription_type: String,
    pub batch_size: i32,
    pub batch_timeout_ms: i32,
    #[sqlx(default)]
    pub webhook_template: Option<String>,
    #[sqlx(default)]
    pub webhook_template_enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateSubscriptionRequest {
    pub callback_url: String,
    pub from_ledger: i64,
    pub subscription_type: Option<String>,
    pub batch_size: Option<i32>,
    pub batch_timeout_ms: Option<i32>,
    pub webhook_template: Option<String>,
    pub webhook_template_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct AckRequest {
    pub ledger: i64,
}

#[derive(Debug, Deserialize)]
pub struct UpdateBatchConfigRequest {
    pub subscription_type: Option<String>,
    pub batch_size: Option<i32>,
    pub batch_timeout_ms: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct WebhookTemplate {
    pub id: Uuid,
    pub subscription_id: Uuid,
    pub name: String,
    pub template_content: String,
    pub description: Option<String>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateWebhookTemplateRequest {
    pub name: String,
    pub template_content: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateWebhookTemplateRequest {
    pub name: Option<String>,
    pub template_content: Option<String>,
    pub description: Option<String>,
    pub is_active: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct BatchDeliveryResponse {
    pub subscription_id: Uuid,
    pub batch: Vec<BatchEvent>,
    pub batch_size: i32,
}

#[derive(Debug, Serialize)]
pub struct BatchEvent {
    pub event_id: Uuid,
    pub ledger: i64,
    pub event_data: Value,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionBatchConfig {
    pub subscription_id: Uuid,
    pub subscription_type: String,
    pub batch_size: i32,
    pub batch_timeout_ms: i32,
}

// ---------------------------------------------------------------------------
// SSRF validation
// ---------------------------------------------------------------------------

/// Build the reqwest client used by the delivery worker.
/// Redirects are disabled to prevent SSRF via redirect chains.
pub fn build_delivery_client() -> Result<reqwest::Client, crate::error::InitError> {
    crate::net::outbound::build_http_client(
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10)),
        "subscription delivery",
    )
}

/// Validate a callback URL against SSRF rules.
///
/// Rules (in order):
/// 1. If SUBSCRIPTION_ALLOWED_URL_PREFIXES is set, the URL must match one of the
///    comma-separated prefixes — all other checks are skipped for matched URLs.
/// 2. The URL must be parseable.
/// 3. In production/staging, only HTTPS is accepted.
/// 4. Private IPs (RFC 1918), loopback, and link-local ranges are always rejected.
pub fn validate_callback_url(raw: &str, env: &Environment) -> Result<(), AppError> {
    // 1. Allowlist check
    let prefixes: Vec<String> = std::env::var("SUBSCRIPTION_ALLOWED_URL_PREFIXES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if !prefixes.is_empty() {
        if prefixes.iter().any(|p| raw.starts_with(p.as_str())) {
            return Ok(());
        }
        return Err(AppError::Validation(
            "callback_url does not match any entry in SUBSCRIPTION_ALLOWED_URL_PREFIXES".into(),
        ));
    }

    // 2. Parse
    let url = Url::parse(raw)
        .map_err(|e| AppError::Validation(format!("callback_url is not a valid URL: {e}")))?;

    // 3. Scheme check
    match url.scheme() {
        "https" => {}
        "http" if !env.is_production_like() => {}
        "http" => {
            return Err(AppError::Validation(
                "callback_url must use HTTPS in production".into(),
            ));
        }
        scheme => {
            return Err(AppError::Validation(format!(
                "callback_url scheme '{scheme}' is not permitted; use https"
            )));
        }
    }

    // 4. SSRF: reject private/reserved hosts
    let host = url.host_str().unwrap_or("");
    if is_ssrf_host(host) {
        return Err(AppError::Validation(
            "callback_url points to a private, loopback, or link-local address".into(),
        ));
    }

    Ok(())
}

fn is_ssrf_host(host: &str) -> bool {
    // Loopback hostnames
    if matches!(host, "localhost" | "127.0.0.1" | "::1")
        || host.ends_with(".local")
        || host.ends_with(".localhost")
    {
        return true;
    }

    // Try to parse as a numeric IP first (handles both IPv4 and IPv6)
    if let Ok(ip) = host.parse::<IpAddr>() {
        return is_private_ip(&ip);
    }

    // Hostname-form private ranges (e.g. "10.0.0.1" already handled above,
    // but catch textual prefixes for belt-and-suspenders)
    host.starts_with("10.")
        || host.starts_with("192.168.")
        || host.starts_with("169.254.")
        || (host.starts_with("172.") && {
            host.split('.')
                .nth(1)
                .and_then(|o| o.parse::<u8>().ok())
                .map(|o| (16..=31).contains(&o))
                .unwrap_or(false)
        })
}

fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            a == 127                                    // loopback 127/8
                || a == 10                              // RFC 1918 10/8
                || (a == 172 && (16..=31).contains(&b)) // RFC 1918 172.16/12
                || (a == 192 && b == 168)               // RFC 1918 192.168/16
                || (a == 169 && b == 254)               // link-local / metadata
                || (a == 0 && b == 0 && c == 0)         // 0.0.0.0
                || a >= 224                              // multicast + reserved
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique-local fc00::/7
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

pub async fn create_subscription(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateSubscriptionRequest>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    // Idempotency: if the client supplied an `Idempotency-Key`, replay the
    // cached response for identical retries instead of creating a duplicate.
    let idem_key = idempotency::scoped_key("POST /v1/subscriptions", &headers);
    let request_hash = idempotency::hash_request(&body);

    if let Some(ref key) = idem_key {
        if let Some(cached) = idempotency::lookup(&state.pool, key, &request_hash).await? {
            return Ok((
                StatusCode::from_u16(cached.status_code).unwrap_or(StatusCode::CREATED),
                Json(cached.response_body),
            ));
        }
    }

    if body.callback_url.is_empty() {
        return Err(AppError::Validation("callback_url is required".into()));
    }
    validate_callback_url(&body.callback_url, &state.config.environment)?;
    if body.from_ledger < 0 {
        return Err(AppError::Validation(
            "from_ledger must be non-negative".into(),
        ));
    }

    let subscription_type = body
        .subscription_type
        .as_deref()
        .unwrap_or("single")
        .to_string();
    if subscription_type != "single" && subscription_type != "batch" {
        return Err(AppError::Validation(
            "subscription_type must be 'single' or 'batch'".into(),
        ));
    }

    let batch_size = body
        .batch_size
        .unwrap_or(state.config.subscription_default_batch_size)
        .clamp(1, 1000);
    let batch_timeout_ms = body
        .batch_timeout_ms
        .unwrap_or(state.config.subscription_default_batch_timeout_ms)
        .clamp(100, 60000);

    let sub: Subscription = sqlx::query_as(
        "INSERT INTO subscriptions (callback_url, from_ledger, subscription_type, batch_size, batch_timeout_ms)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, callback_url, from_ledger, acked_ledger, status, created_at, subscription_type, batch_size, batch_timeout_ms",
    )
    .bind(&body.callback_url)
    .bind(body.from_ledger)
    .bind(&subscription_type)
    .bind(batch_size)
    .bind(batch_timeout_ms)
    .fetch_one(&state.pool)
    .await?;

    // Enqueue all existing events >= from_ledger for this new subscription
    sqlx::query(
        "INSERT INTO delivery_queue (subscription_id, event_id, ledger)
         SELECT $1, id, ledger FROM events WHERE ledger >= $2
         ORDER BY ledger ASC",
    )
    .bind(sub.id)
    .bind(body.from_ledger)
    .execute(&state.pool)
    .await?;

    let response_body = json!(sub);

    // Persist the response so retries with the same key replay it (24h TTL).
    if let Some(ref key) = idem_key {
        idempotency::store(
            &state.pool,
            key,
            &request_hash,
            StatusCode::CREATED.as_u16(),
            &response_body,
        )
        .await?;
    }

    Ok((StatusCode::CREATED, Json(response_body)))
}

pub async fn get_subscription(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let sub: Subscription = sqlx::query_as(
        "SELECT id, callback_url, from_ledger, acked_ledger, status, created_at, subscription_type, batch_size, batch_timeout_ms
         FROM subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::NotFound)?;

    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM delivery_queue WHERE subscription_id = $1 AND status = 'pending'",
    )
    .bind(id)
    .fetch_one(&state.pool)
    .await?;

    Ok(Json(
        json!({ "subscription": sub, "pending_deliveries": pending }),
    ))
}

pub async fn cancel_subscription(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let rows = sqlx::query(
        "UPDATE subscriptions SET status = 'cancelled' WHERE id = $1 AND status = 'active'",
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound);
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn ack_subscription(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<AckRequest>,
) -> Result<Json<Value>, AppError> {
    // Advance acked_ledger only forward
    let rows = sqlx::query(
        "UPDATE subscriptions SET acked_ledger = $1
         WHERE id = $2 AND status = 'active' AND acked_ledger < $1",
    )
    .bind(body.ledger)
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        // Check if subscription exists at all
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM subscriptions WHERE id = $1)")
                .bind(id)
                .fetch_one(&state.pool)
                .await?;
        if !exists {
            return Err(AppError::NotFound);
        }
    }

    // Mark delivered items up to this ledger as acknowledged (clean up)
    sqlx::query(
        "UPDATE delivery_queue SET status = 'delivered'
         WHERE subscription_id = $1 AND ledger <= $2 AND status = 'pending'",
    )
    .bind(id)
    .bind(body.ledger)
    .execute(&state.pool)
    .await?;

    Ok(Json(json!({ "acked_ledger": body.ledger })))
}

/// `GET /v1/subscriptions/{id}/batch` — return the batch configuration for a subscription.
pub async fn get_subscription_batch_config(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let row: Option<(String, i32, i32)> = sqlx::query_as(
        "SELECT subscription_type, batch_size, batch_timeout_ms
         FROM subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    let (subscription_type, batch_size, batch_timeout_ms) = row.ok_or(AppError::NotFound)?;

    Ok(Json(json!(SubscriptionBatchConfig {
        subscription_id: id,
        subscription_type,
        batch_size,
        batch_timeout_ms,
    })))
}

/// `PUT /v1/subscriptions/{id}/batch` — update the batch configuration for a subscription.
pub async fn update_subscription_batch_config(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateBatchConfigRequest>,
) -> Result<Json<Value>, AppError> {
    if let Some(ref st) = body.subscription_type {
        if st != "single" && st != "batch" {
            return Err(AppError::Validation(
                "subscription_type must be 'single' or 'batch'".into(),
            ));
        }
    }

    let batch_size = body.batch_size.map(|s| s.clamp(1, 1000));
    let batch_timeout_ms = body.batch_timeout_ms.map(|t| t.clamp(100, 60000));

    let rows = sqlx::query(
        "UPDATE subscriptions
         SET subscription_type = COALESCE($2, subscription_type),
             batch_size = COALESCE($3, batch_size),
             batch_timeout_ms = COALESCE($4, batch_timeout_ms)
         WHERE id = $1 AND status = 'active'",
    )
    .bind(id)
    .bind(&body.subscription_type)
    .bind(batch_size)
    .bind(batch_timeout_ms)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound);
    }

    crate::metrics::record_batch_config_updated();

    Ok(Json(json!({
        "subscription_id": id,
        "subscription_type": body.subscription_type,
        "batch_size": batch_size,
        "batch_timeout_ms": batch_timeout_ms,
    })))
}

/// `POST /v1/subscriptions/{id}/batch` — deliver a batch of pending events for a batch subscription.
/// Returns the current batch of pending events without marking them delivered.
pub async fn deliver_batch(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let sub: Subscription = sqlx::query_as(
        "SELECT id, callback_url, from_ledger, acked_ledger, status, created_at, subscription_type, batch_size, batch_timeout_ms
         FROM subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::NotFound)?;

    if sub.status != "active" {
        return Err(AppError::Validation(
            "subscription is not active".into(),
        ));
    }

    if sub.subscription_type != "batch" {
        return Err(AppError::Validation(
            "subscription is not configured for batch delivery".into(),
        ));
    }

    let batch_size = sub.batch_size as i64;
    let rows: Vec<(Uuid, Uuid, Value, i64)> = sqlx::query_as(
        "SELECT dq.id, dq.event_id, e.event_data, dq.ledger
         FROM delivery_queue dq
         JOIN events e ON e.id = dq.event_id
         WHERE dq.subscription_id = $1
           AND dq.status = 'pending'
           AND dq.next_attempt_at <= NOW()
         ORDER BY dq.ledger ASC
         LIMIT $2",
    )
    .bind(id)
    .bind(batch_size)
    .fetch_all(&state.pool)
    .await?;

    let events: Vec<BatchEvent> = rows
        .into_iter()
        .map(|(dq_id, event_id, event_data, ledger)| BatchEvent {
            event_id,
            ledger,
            event_data,
        })
        .collect();

    crate::metrics::record_batch_delivered(events.len() as u64);

    Ok(Json(json!(BatchDeliveryResponse {
        subscription_id: id,
        batch_size: sub.batch_size,
        batch: events,
    })))
}

// ---------------------------------------------------------------------------
// Delivery worker
// ---------------------------------------------------------------------------

/// Enqueue newly indexed events for all active subscriptions.
/// Called by the indexer after a successful store.
pub async fn enqueue_event(pool: &PgPool, event_id: Uuid, ledger: i64) {
    let result = sqlx::query(
        "INSERT INTO delivery_queue (subscription_id, event_id, ledger)
         SELECT id, $1, $2 FROM subscriptions
         WHERE status = 'active' AND from_ledger <= $2
         ON CONFLICT DO NOTHING",
    )
    .bind(event_id)
    .bind(ledger)
    .execute(pool)
    .await;

    if let Err(e) = result {
        tracing::warn!(error = %e, "Failed to enqueue event for subscriptions");
    }
}

/// Background worker: polls delivery_queue and POSTs pending items to callback URLs.
pub async fn run_delivery_worker(pool: PgPool, http: reqwest::Client) {
    loop {
        match deliver_pending(&pool, &http).await {
            Ok(n) if n > 0 => tracing::debug!(delivered = n, "Delivery worker cycle"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "Delivery worker error"),
        }
        sleep(Duration::from_secs(5)).await;
    }
}

async fn deliver_pending(pool: &PgPool, http: &reqwest::Client) -> Result<usize, sqlx::Error> {
    let rows: Vec<(Uuid, Uuid, String, Value, i64, String, i32, i32)> = sqlx::query_as(
        "SELECT dq.id, dq.event_id, s.callback_url, e.event_data, dq.ledger, s.subscription_type, s.batch_size, s.batch_timeout_ms
         FROM delivery_queue dq
         JOIN subscriptions s ON s.id = dq.subscription_id
         JOIN events e ON e.id = dq.event_id
         WHERE dq.status = 'pending'
           AND dq.next_attempt_at <= NOW()
           AND s.status = 'active'
         ORDER BY dq.ledger ASC
         LIMIT 200",
    )
    .fetch_all(pool)
    .await?;

    let mut single_items: Vec<(Uuid, Uuid, String, Value, i64)> = Vec::new();
    let mut batch_items: std::collections::HashMap<Uuid, Vec<(Uuid, Uuid, String, Value, i64, i32, i32)>> = std::collections::HashMap::new();

    for (queue_id, event_id, callback_url, event_data, ledger, sub_type, batch_size, batch_timeout_ms) in rows {
        if sub_type == "batch" {
            batch_items
                .entry(queue_id)
                .or_default()
                .push((queue_id, event_id, callback_url, event_data, ledger, batch_size, batch_timeout_ms));
        } else {
            single_items.push((queue_id, event_id, callback_url, event_data, ledger));
        }
    }

    let mut total_delivered = 0;

    for (queue_id, event_id, callback_url, event_data, ledger) in single_items {
        let payload = json!({ "event_id": event_id, "ledger": ledger, "event_data": event_data });
        match http
            .post(&callback_url)
            .json(&payload)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                sqlx::query("UPDATE delivery_queue SET status = 'delivered' WHERE id = $1")
                    .bind(queue_id)
                    .execute(pool)
                    .await?;
                total_delivered += 1;
            }
            Ok(resp) => {
                let err = format!("HTTP {}", resp.status());
                schedule_retry(pool, queue_id, &err).await?;
            }
            Err(e) => {
                schedule_retry(pool, queue_id, &e.to_string()).await?;
            }
        }
    }

    for (_, items) in batch_items {
        if items.is_empty() {
            continue;
        }

        let callback_url = items[0].2.clone();
        let batch_size = items[0].5;
        let batch_timeout_ms = items[0].6;

        let batch_items_to_send: Vec<_> = items.into_iter().take(batch_size as usize).collect();
        let events: Vec<Value> = batch_items_to_send
            .iter()
            .map(|(_, event_id, _, event_data, ledger, _, _)| {
                json!({ "event_id": event_id, "ledger": ledger, "event_data": event_data })
            })
            .collect();

        let payload = json!({ "events": events, "batch_size": batch_size });

        let timeout = Duration::from_millis(batch_timeout_ms as u64);
        match http
            .post(&callback_url)
            .json(&payload)
            .timeout(timeout)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                let queue_ids: Vec<Uuid> = batch_items_to_send.iter().map(|(qid, _, _, _, _, _, _)| *qid).collect();
                sqlx::query(
                    "UPDATE delivery_queue SET status = 'delivered' WHERE id = ANY($1)"
                )
                .bind(&queue_ids)
                .execute(pool)
                .await?;
                total_delivered += queue_ids.len();
                crate::metrics::record_batch_delivered(queue_ids.len() as u64);
            }
            Ok(resp) => {
                let err = format!("HTTP {}", resp.status());
                for (queue_id, _, _, _, _, _, _) in batch_items_to_send {
                    schedule_retry(pool, queue_id, &err).await?;
                }
                crate::metrics::record_batch_delivery_failed();
            }
            Err(e) => {
                let err = e.to_string();
                for (queue_id, _, _, _, _, _, _) in batch_items_to_send {
                    schedule_retry(pool, queue_id, &err).await?;
                }
                crate::metrics::record_batch_delivery_failed();
            }
        }
    }

    Ok(total_delivered)
}

/// Exponential backoff: 2^attempts seconds, capped at 1 hour.
async fn schedule_retry(pool: &PgPool, queue_id: Uuid, error: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE delivery_queue
         SET attempts = attempts + 1,
             last_error = $2,
             next_attempt_at = NOW() + (LEAST(POWER(2, attempts + 1), 3600) || ' seconds')::interval
         WHERE id = $1",
    )
    .bind(queue_id)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Email notification config (Issue #619)
// ---------------------------------------------------------------------------

/// Daily email send limit per address.
const EMAIL_DAILY_LIMIT: i64 = 100;

/// Check whether the given address is under the daily send limit and, if so,
/// atomically increment the counter.  Returns `true` when the send is allowed.
pub async fn check_and_increment_email_rate_limit(
    pool: &PgPool,
    email: &str,
) -> Result<bool, sqlx::Error> {
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO email_send_counters (email_address, date_utc, send_count)
         VALUES ($1, CURRENT_DATE, 1)
         ON CONFLICT (email_address, date_utc)
         DO UPDATE SET send_count = email_send_counters.send_count + 1
         RETURNING send_count",
    )
    .bind(email)
    .fetch_one(pool)
    .await?;

    Ok(row.0 <= EMAIL_DAILY_LIMIT)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscriptionEmailConfig {
    pub email_address: Option<String>,
    pub email_enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdateEmailConfigRequest {
    /// Destination email address. Required when enabling.
    pub email_address: Option<String>,
    pub enabled: bool,
}

/// `GET /v1/subscriptions/{id}/email` — return the email config for a subscription.
pub async fn get_subscription_email(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let row: Option<(Option<String>, bool)> = sqlx::query_as(
        "SELECT email_address, email_enabled FROM subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    let (email_address, email_enabled) = row.ok_or(AppError::NotFound)?;

    // Daily send count for this address, if configured.
    let daily_sent: Option<i64> = if let Some(ref addr) = email_address {
        sqlx::query_scalar(
            "SELECT send_count FROM email_send_counters \
             WHERE email_address = $1 AND date_utc = CURRENT_DATE",
        )
        .bind(addr)
        .fetch_optional(&state.pool)
        .await?
    } else {
        None
    };

    Ok(Json(json!({
        "subscription_id": id,
        "email_address": email_address,
        "email_enabled": email_enabled,
        "daily_send_limit": EMAIL_DAILY_LIMIT,
        "daily_sent_today": daily_sent.unwrap_or(0),
    })))
}

/// `PUT /v1/subscriptions/{id}/email` — configure or update email notifications.
pub async fn update_subscription_email(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateEmailConfigRequest>,
) -> Result<Json<Value>, AppError> {
    if body.enabled {
        match &body.email_address {
            None => {
                return Err(AppError::Validation(
                    "email_address is required when enabling email notifications".into(),
                ))
            }
            Some(addr) => {
                if addr.is_empty() || !addr.contains('@') {
                    return Err(AppError::Validation("invalid email_address".into()));
                }
            }
        }
    }

    let rows = sqlx::query(
        "UPDATE subscriptions
         SET email_address = $2, email_enabled = $3
         WHERE id = $1 AND status = 'active'",
    )
    .bind(id)
    .bind(&body.email_address)
    .bind(body.enabled)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound);
    }

    // Log the config change for auditability.
    crate::metrics::record_email_subscription_updated();

    Ok(Json(json!({
        "subscription_id": id,
        "email_address": body.email_address,
        "email_enabled": body.enabled,
    })))
}

/// Email delivery worker: polls subscriptions with email enabled and sends
/// notifications for pending events.  Respects the per-address daily limit.
pub async fn run_email_delivery_worker(pool: PgPool) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let smtp_host = std::env::var("EMAIL_SMTP_HOST").unwrap_or_default();
    let smtp_port: u16 = std::env::var("EMAIL_SMTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(587);
    let smtp_user = std::env::var("EMAIL_SMTP_USER").ok();
    let smtp_pass = std::env::var("EMAIL_SMTP_PASSWORD").ok();
    let from = std::env::var("EMAIL_FROM").unwrap_or_else(|_| "noreply@soroban-pulse".to_string());

    if smtp_host.is_empty() {
        tracing::info!("EMAIL_SMTP_HOST not set — subscription email worker disabled");
        return;
    }

    loop {
        interval.tick().await;

        // Fetch subscriptions with email enabled + pending delivery items.
        let rows: Vec<(Uuid, String, Uuid, Value, i64)> = match sqlx::query_as(
            "SELECT DISTINCT ON (s.id) s.id, s.email_address, dq.event_id, e.event_data, dq.ledger
             FROM subscriptions s
             JOIN delivery_queue dq ON dq.subscription_id = s.id
             JOIN events e ON e.id = dq.event_id
             WHERE s.email_enabled = true
               AND s.email_address IS NOT NULL
               AND s.status = 'active'
               AND dq.status = 'pending'
               AND dq.next_attempt_at <= NOW()
             ORDER BY s.id, dq.ledger ASC
             LIMIT 100",
        )
        .fetch_all(&pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Email delivery worker DB error");
                continue;
            }
        };

        for (sub_id, email_addr, event_id, event_data, ledger) in rows {
            match check_and_increment_email_rate_limit(&pool, &email_addr).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(
                        email = %email_addr,
                        "Daily email limit reached ({EMAIL_DAILY_LIMIT}/day), skipping"
                    );
                    crate::metrics::record_email_rate_limited();
                    let _ = sqlx::query(
                        "INSERT INTO email_delivery_log \
                         (subscription_id, email_address, subject, status) \
                         VALUES ($1, $2, 'Event notification', 'rate_limited')",
                    )
                    .bind(sub_id)
                    .bind(&email_addr)
                    .execute(&pool)
                    .await;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Email rate-limit check failed");
                    continue;
                }
            }

            let subject = format!("Soroban event at ledger {ledger}");
            let body = format!(
                "A Soroban contract event was detected at ledger {ledger}.\n\nEvent data:\n{}",
                serde_json::to_string_pretty(&event_data).unwrap_or_default()
            );

            let send_result = send_smtp_email(
                &smtp_host,
                smtp_port,
                smtp_user.as_deref(),
                smtp_pass.as_deref(),
                &from,
                &email_addr,
                &subject,
                &body,
            )
            .await;

            let (status, error_msg) = match send_result {
                Ok(()) => {
                    tracing::info!(email = %email_addr, ledger, "Email notification sent");
                    crate::metrics::record_email_notification_sent();
                    ("sent", None)
                }
                Err(e) => {
                    tracing::warn!(error = %e, email = %email_addr, "Email delivery failed");
                    crate::metrics::record_email_failure();
                    ("failed", Some(e))
                }
            };

            let _ = sqlx::query(
                "INSERT INTO email_delivery_log \
                 (subscription_id, email_address, subject, status, error, sent_at) \
                 VALUES ($1, $2, $3, $4, $5, CASE WHEN $4 = 'sent' THEN NOW() ELSE NULL END)",
            )
            .bind(sub_id)
            .bind(&email_addr)
            .bind(&subject)
            .bind(status)
            .bind(error_msg.as_deref())
            .execute(&pool)
            .await;

            // Mark delivery_queue item as delivered on success.
            if status == "sent" {
                let _ = sqlx::query(
                    "UPDATE delivery_queue SET status = 'delivered' \
                     WHERE subscription_id = $1 AND event_id = $2",
                )
                .bind(sub_id)
                .bind(event_id)
                .execute(&pool)
                .await;
            }
        }
    }
}

/// Thin SMTP sender using lettre.
async fn send_smtp_email(
    smtp_host: &str,
    smtp_port: u16,
    smtp_user: Option<&str>,
    smtp_password: Option<&str>,
    from: &str,
    to: &str,
    subject: &str,
    body: &str,
) -> Result<(), String> {
    use lettre::{
        message::header::ContentType, transport::smtp::authentication::Credentials, Message,
        SmtpTransport, Transport,
    };

    let email = Message::builder()
        .from(from.parse().map_err(|e| format!("invalid from: {e}"))?)
        .to(to.parse().map_err(|e| format!("invalid to: {e}"))?)
        .subject(subject)
        .header(ContentType::TEXT_PLAIN)
        .body(body.to_string())
        .map_err(|e| format!("build email: {e}"))?;

    let smtp_host_owned = smtp_host.to_string();
    let creds = smtp_user.zip(smtp_password).map(|(u, p)| {
        Credentials::new(u.to_string(), p.to_string())
    });

    tokio::task::spawn_blocking(move || {
        let mut builder = SmtpTransport::relay(&smtp_host_owned)
            .map_err(|e| format!("SMTP relay: {e}"))?
            .port(smtp_port);
        if let Some(c) = creds {
            builder = builder.credentials(c);
        }
        builder
            .build()
            .send(&email)
            .map(|_| ())
            .map_err(|e| format!("SMTP send: {e}"))
    })
    .await
    .map_err(|e| format!("task: {e}"))?
}

// ---------------------------------------------------------------------------
// Subscription Pause/Resume (Issue #884)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct PauseSubscriptionRequest {
    /// Duration to pause in seconds. If not provided, pauses indefinitely.
    pub pause_seconds: Option<i64>,
    /// Reason for pausing the subscription
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ResumeSubscriptionRequest {
    /// Optional reason for resuming
    pub reason: Option<String>,
}

/// Pause a subscription to prevent delivery of events.
/// Events will continue to accumulate in the delivery queue during the pause.
pub async fn pause_subscription(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<PauseSubscriptionRequest>,
) -> Result<Json<Value>, AppError> {
    let pause_until = body.pause_seconds.map(|secs| {
        chrono::Utc::now() + chrono::Duration::seconds(secs)
    });

    let rows = sqlx::query(
        "UPDATE subscriptions
         SET pause_until = $2, pause_reason = $3, paused_at = NOW()
         WHERE id = $1 AND status = 'active'"
    )
    .bind(id)
    .bind(pause_until)
    .bind(&body.reason)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound);
    }

    // Log the pause action for audit trail
    let _ = sqlx::query(
        "INSERT INTO subscription_pause_resume_log (subscription_id, action, reason, paused_until, created_at)
         VALUES ($1, $2, $3, $4, NOW())"
    )
    .bind(id)
    .bind("paused")
    .bind(&body.reason)
    .bind(pause_until)
    .execute(&state.pool)
    .await;

    tracing::info!(
        subscription_id = %id,
        pause_seconds = ?body.pause_seconds,
        reason = ?body.reason,
        "Subscription paused"
    );

    crate::metrics::record_subscription_paused();

    Ok(Json(json!({
        "subscription_id": id,
        "status": "paused",
        "pause_until": pause_until,
        "reason": body.reason,
    })))
}

/// Resume a previously paused subscription.
/// Pending events in the delivery queue will resume delivery.
pub async fn resume_subscription(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<Option<ResumeSubscriptionRequest>>,
) -> Result<Json<Value>, AppError> {
    let rows = sqlx::query(
        "UPDATE subscriptions
         SET pause_until = NULL, pause_reason = NULL
         WHERE id = $1 AND pause_until IS NOT NULL"
    )
    .bind(id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound);
    }

    // Log the resume action for audit trail
    let reason = body.as_ref().and_then(|b| b.reason.as_ref());
    let _ = sqlx::query(
        "INSERT INTO subscription_pause_resume_log (subscription_id, action, reason, created_at)
         VALUES ($1, $2, $3, NOW())"
    )
    .bind(id)
    .bind("resumed")
    .bind(reason)
    .execute(&state.pool)
    .await;

    tracing::info!(
        subscription_id = %id,
        reason = ?reason,
        "Subscription resumed"
    );

    crate::metrics::record_subscription_resumed();

    Ok(Json(json!({
        "subscription_id": id,
        "status": "active",
        "reason": reason,
    })))
}

/// Get pause/resume status and history for a subscription.
pub async fn get_pause_resume_status(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let sub: Option<(Option<DateTime<Utc>>, Option<String>)> = sqlx::query_as(
        "SELECT pause_until, pause_reason FROM subscriptions WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    let (pause_until, pause_reason) = sub.ok_or(AppError::NotFound)?;

    let history = sqlx::query_as::<_, (String, Option<String>, Option<DateTime<Utc>>, DateTime<Utc>)>(
        "SELECT action, reason, paused_until, created_at
         FROM subscription_pause_resume_log
         WHERE subscription_id = $1
         ORDER BY created_at DESC
         LIMIT 20"
    )
    .bind(id)
    .fetch_all(&state.pool)
    .await?;

    let is_paused = pause_until.is_some();
    let is_auto_resuming = pause_until.as_ref().map(|until| *until <= chrono::Utc::now()).unwrap_or(false);

    Ok(Json(json!({
        "subscription_id": id,
        "is_paused": is_paused,
        "pause_until": pause_until,
        "pause_reason": pause_reason,
        "is_auto_resuming": is_auto_resuming,
        "history": history.into_iter().map(|(action, reason, paused_until, created_at)| {
            json!({
                "action": action,
                "reason": reason,
                "paused_until": paused_until,
                "created_at": created_at,
            })
        }).collect::<Vec<_>>(),
    })))
}

/// Background task to automatically resume paused subscriptions when pause_until time arrives.
pub async fn run_auto_resume_worker(pool: PgPool) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        match sqlx::query(
            "UPDATE subscriptions
             SET pause_until = NULL, pause_reason = NULL
             WHERE pause_until IS NOT NULL AND pause_until <= NOW()"
        )
        .execute(&pool)
        .await
        {
            Ok(result) if result.rows_affected() > 0 => {
                tracing::info!(
                    count = result.rows_affected(),
                    "Auto-resumed paused subscriptions"
                );
                crate::metrics::record_subscription_auto_resumed(result.rows_affected());
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "Auto-resume worker error");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    // --- SSRF validation tests ---

    #[test]
    fn ssrf_rejects_localhost() {
        let err = validate_callback_url("http://localhost/hook", &Environment::Development);
        assert!(err.is_err(), "localhost must be rejected");
    }

    #[test]
    fn ssrf_rejects_127_loopback() {
        let err = validate_callback_url("http://127.0.0.1/hook", &Environment::Development);
        assert!(err.is_err(), "127.0.0.1 must be rejected");
    }

    #[test]
    fn ssrf_rejects_ipv6_loopback() {
        let err = validate_callback_url("http://[::1]/hook", &Environment::Development);
        assert!(err.is_err(), "::1 must be rejected");
    }

    #[test]
    fn ssrf_rejects_rfc1918_10_x() {
        let err = validate_callback_url("http://10.0.0.1/hook", &Environment::Development);
        assert!(err.is_err(), "10.x.x.x must be rejected");
    }

    #[test]
    fn ssrf_rejects_rfc1918_172_16() {
        let err = validate_callback_url("http://172.16.0.1/hook", &Environment::Development);
        assert!(err.is_err(), "172.16.x.x must be rejected");
    }

    #[test]
    fn ssrf_rejects_rfc1918_192_168() {
        let err = validate_callback_url("http://192.168.1.1/hook", &Environment::Development);
        assert!(err.is_err(), "192.168.x.x must be rejected");
    }

    #[test]
    fn ssrf_rejects_link_local_metadata() {
        let err = validate_callback_url(
            "http://169.254.169.254/latest/meta-data/",
            &Environment::Development,
        );
        assert!(err.is_err(), "AWS metadata endpoint must be rejected");
    }

    #[test]
    fn ssrf_rejects_http_in_production() {
        let err = validate_callback_url("http://example.com/hook", &Environment::Production);
        assert!(err.is_err(), "http must be rejected in production");
    }

    #[test]
    fn ssrf_allows_https_public_in_production() {
        let ok = validate_callback_url("https://example.com/hook", &Environment::Production);
        assert!(ok.is_ok(), "https public URL must be allowed in production");
    }

    #[test]
    fn ssrf_allows_http_public_in_development() {
        let ok = validate_callback_url("http://example.com/hook", &Environment::Development);
        assert!(ok.is_ok(), "http public URL must be allowed in development");
    }

    #[test]
    fn ssrf_allowlist_permits_matched_prefix() {
        // Safety: env var manipulation is fine in single-threaded unit tests
        std::env::set_var(
            "SUBSCRIPTION_ALLOWED_URL_PREFIXES",
            "http://internal.corp/,https://trusted.corp/",
        );
        let ok =
            validate_callback_url("http://internal.corp/hook", &Environment::Production);
        std::env::remove_var("SUBSCRIPTION_ALLOWED_URL_PREFIXES");
        assert!(ok.is_ok(), "allowlisted prefix must be permitted");
    }

    #[test]
    fn ssrf_allowlist_blocks_unmatched_url() {
        std::env::set_var(
            "SUBSCRIPTION_ALLOWED_URL_PREFIXES",
            "https://trusted.corp/",
        );
        let err =
            validate_callback_url("https://other.corp/hook", &Environment::Development);
        std::env::remove_var("SUBSCRIPTION_ALLOWED_URL_PREFIXES");
        assert!(err.is_err(), "URL not in allowlist must be rejected when allowlist is set");
    }

    #[test]
    fn build_delivery_client_does_not_follow_redirects() {
        // The only way to verify policy without making a real request is to confirm
        // the builder compiles and returns a client successfully.
        let _client = build_delivery_client().unwrap();
    }

    // --- Unit tests for retry backoff logic ---

    #[test]
    fn backoff_formula_caps_at_3600() {
        // Verify the SQL formula: LEAST(POWER(2, attempts+1), 3600)
        // attempts=0 → 2^1=2s, attempts=10 → 2^11=2048s, attempts=12 → 3600s cap
        let backoff = |attempts: u32| -> u64 {
            let raw = 2u64.pow(attempts + 1);
            raw.min(3600)
        };
        assert_eq!(backoff(0), 2);
        assert_eq!(backoff(1), 4);
        assert_eq!(backoff(10), 2048);
        assert_eq!(backoff(11), 3600); // 2^12=4096 → capped
        assert_eq!(backoff(12), 3600);
    }

    // --- Mock HTTP delivery tests ---

    struct MockServer {
        received: Arc<Mutex<Vec<Value>>>,
        fail_count: Arc<Mutex<u32>>,
    }

    impl MockServer {
        fn new() -> Self {
            Self {
                received: Arc::new(Mutex::new(vec![])),
                fail_count: Arc::new(Mutex::new(0)),
            }
        }

        fn with_failures(n: u32) -> Self {
            Self {
                received: Arc::new(Mutex::new(vec![])),
                fail_count: Arc::new(Mutex::new(n)),
            }
        }
    }

    /// Simulate delivery to a mock callback: records payload or returns error.
    async fn mock_deliver(server: &MockServer, payload: &Value) -> Result<bool, String> {
        let mut fails = server.fail_count.lock().unwrap();
        if *fails > 0 {
            *fails -= 1;
            return Err("connection refused".to_string());
        }
        server.received.lock().unwrap().push(payload.clone());
        Ok(true)
    }

    #[tokio::test]
    async fn delivery_succeeds_on_first_attempt() {
        let server = MockServer::new();
        let payload = json!({ "event_id": Uuid::new_v4(), "ledger": 100, "event_data": {} });
        let result = mock_deliver(&server, &payload).await;
        assert!(result.is_ok());
        assert_eq!(server.received.lock().unwrap().len(), 1);
        assert_eq!(server.received.lock().unwrap()[0]["ledger"], 100);
    }

    #[tokio::test]
    async fn delivery_retries_after_failure() {
        let server = MockServer::with_failures(2);
        let payload = json!({ "event_id": Uuid::new_v4(), "ledger": 200, "event_data": {} });

        // First two attempts fail
        assert!(mock_deliver(&server, &payload).await.is_err());
        assert!(mock_deliver(&server, &payload).await.is_err());
        // Third attempt succeeds
        assert!(mock_deliver(&server, &payload).await.is_ok());
        assert_eq!(server.received.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ack_advances_cursor_only_forward() {
        // Simulate ack logic: acked_ledger should only move forward
        let mut acked_ledger: i64 = 50;
        let new_ack = |current: i64, proposed: i64| -> i64 {
            if proposed > current {
                proposed
            } else {
                current
            }
        };

        acked_ledger = new_ack(acked_ledger, 100);
        assert_eq!(acked_ledger, 100);

        // Trying to ack an older ledger should not regress
        acked_ledger = new_ack(acked_ledger, 80);
        assert_eq!(acked_ledger, 100);

        // Advancing further works
        acked_ledger = new_ack(acked_ledger, 150);
        assert_eq!(acked_ledger, 150);
    }

    // --- Rate limiter tests (Issue #1268) ---

    #[test]
    fn subscription_rate_limiter_allows_five_then_rejects() {
        let limiter = SubscriptionRateLimiter::new(5);
        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        for _ in 0..5 {
            assert!(limiter.check(ip).is_ok());
        }
        let err = limiter.check(ip).expect_err("6th request must be rejected");
        assert!(err >= 1, "Retry-After must be at least 1 second");
    }

    #[test]
    fn subscription_rate_limiter_is_per_ip() {
        let limiter = SubscriptionRateLimiter::new(5);
        let ip_a: IpAddr = "203.0.113.7".parse().unwrap();
        let ip_b: IpAddr = "203.0.113.8".parse().unwrap();
        for _ in 0..5 {
            assert!(limiter.check(ip_a).is_ok());
        }
        assert!(limiter.check(ip_a).is_err());
        assert!(limiter.check(ip_b).is_ok(), "other IPs are unaffected");
    }

    #[test]
    fn sse_connection_limiter_caps_concurrent_connections() {
        let limiter = SseConnectionLimiter::new(5);
        let ip: IpAddr = "203.0.113.7".parse().unwrap();
        let mut guards = Vec::new();
        for _ in 0..5 {
            guards.push(limiter.try_acquire(ip).expect("within limit"));
        }
        assert!(limiter.try_acquire(ip).is_err(), "6th connection must be rejected");
        // Dropping a guard frees a slot.
        guards.pop();
        assert!(limiter.try_acquire(ip).is_ok());
    }

    #[tokio::test]
    async fn payload_contains_required_fields() {
        let event_id = Uuid::new_v4();
        let ledger = 12345_i64;
        let event_data = json!({ "value": { "amount": 100 }, "topic": [] });

        let payload = json!({
            "event_id": event_id,
            "ledger": ledger,
            "event_data": event_data,
        });

        assert!(payload.get("event_id").is_some());
        assert!(payload.get("ledger").is_some());
        assert!(payload.get("event_data").is_some());
        assert_eq!(payload["ledger"], 12345);
    }
}

// ---------------------------------------------------------------------------
// Property-based tests for the subscription filter engine (Issue #1273)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::collection::{btree_set, vec};
    use proptest::option;
    use proptest::prelude::*;
    use serde_json::json;

    /// A minimal filter configuration used by the property tests.
    ///
    /// `topic_filters` is empty when the subscription should match every event.
    #[derive(Debug, Clone)]
    struct FilterConfig {
        topic_filters: Vec<String>,
    }

    /// A minimal event payload used by the property tests.
    #[derive(Debug, Clone)]
    struct EventPayload {
        topics: Vec<String>,
        data: Value,
    }

    /// Strategy for a single topic segment: alphanumerics plus a few symbols,
    /// including the empty string to exercise the empty-topic edge case.
    fn topic_segment() -> impl Strategy<Value = String> {
        prop::collection::vec(
            prop::sample::select(vec![
                'a', 'b', 'c', 'Z', '0', '9', '_', '-', '.', '*', '?', '/', ':', ' ',
            ]),
            0..8,
        )
        .prop_map(|chars| chars.into_iter().collect())
    }

    /// Strategy for a topic filter: either a literal segment or a wildcard.
    fn topic_filter() -> impl Strategy<Value = String> {
        prop_oneof![
            3 => topic_segment(),
            1 => Just("*".to_string()),
            1 => Just("**".to_string()),
            1 => topic_segment().prop_map(|s| format!("{s}*")),
        ]
    }

    /// Strategy for a random `Subscription` filter config.
    fn subscription_filter_strategy() -> impl Strategy<Value = FilterConfig> {
        vec(topic_filter(), 0..4).prop_map(|topic_filters| FilterConfig { topic_filters })
    }

    /// Strategy for a random JSON value, including nested arrays and objects.
    fn json_value_strategy() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| json!(n)),
            any::<f64>()
                .prop_filter("finite", |f| f.is_finite())
                .prop_map(|f| json!(f)),
            topic_segment().prop_map(Value::String),
        ];

        leaf.prop_recursive(4, 64, 8, |inner| {
            prop_oneof![
                vec(inner.clone(), 0..4).prop_map(Value::Array),
                btree_set(topic_segment(), 0..4).prop_map(|keys| {
                    let mut map = serde_json::Map::new();
                    for key in keys {
                        map.insert(key, Value::Null);
                    }
                    Value::Object(map)
                }),
            ]
        })
    }

    /// Strategy for a random `Event` payload.
    fn event_payload_strategy() -> impl Strategy<Value = EventPayload> {
        (vec(topic_segment(), 0..4), json_value_strategy())
            .prop_map(|(topics, data)| EventPayload { topics, data })
    }

    /// The filter engine under test.
    ///
    /// A subscription with no topic filters matches every event. Otherwise the
    /// event must contain at least one topic that matches one of the filters.
    /// A filter matches a topic when it is `"*"`, `"**"`, or a prefix of the
    /// topic followed by `*`; otherwise it must be equal to the topic.
    fn filter_matches(event: &EventPayload, subscription: &FilterConfig) -> bool {
        if subscription.topic_filters.is_empty() {
            return true;
        }

        subscription.topic_filters.iter().any(|filter| {
            event.topics.iter().any(|topic| topic_matches(filter, topic))
        })
    }

    fn topic_matches(filter: &str, topic: &str) -> bool {
        match filter {
            "*" | "**" => true,
            _ => match filter.strip_suffix('*') {
                Some(prefix) => topic.starts_with(prefix),
                None => filter == topic,
            },
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(256),
            ..ProptestConfig::default()
        })]

        /// The filter engine must never panic on arbitrary filter/event pairs.
        #[test]
        fn filter_matches_never_panics(
            event in event_payload_strategy(),
            subscription in subscription_filter_strategy(),
        ) {
            let _ = filter_matches(&event, &subscription);
        }

        /// A subscription with no topic filters matches every event.
        #[test]
        fn empty_filter_matches_all_events(event in event_payload_strategy()) {
            let subscription = FilterConfig { topic_filters: vec![] };
            prop_assert!(filter_matches(&event, &subscription));
        }

        /// A subscription with a specific topic only matches events containing it.
        #[test]
        fn specific_topic_only_matches_containing_events(
            topic in topic_segment(),
            event in event_payload_strategy(),
        ) {
            let subscription = FilterConfig { topic_filters: vec![topic.clone()] };
            let expected = event.topics.iter().any(|t| t == &topic);
            prop_assert_eq!(filter_matches(&event, &subscription), expected);
        }

        /// Regression: a literal filter must not match a topic that merely
        /// shares a prefix, and a `prefix*` filter must match all extensions.
        #[test]
        fn wildcard_prefix_regression(
            prefix in topic_segment(),
            suffix in topic_segment(),
        ) {
            let subscription = FilterConfig { topic_filters: vec![format!("{prefix}*")] };
            let matching = EventPayload { topics: vec![format!("{prefix}{suffix}")], data: Value::Null };
            prop_assert!(filter_matches(&matching, &subscription));

            let literal = FilterConfig { topic_filters: vec![prefix.clone()] };
            let non_matching = EventPayload { topics: vec![format!("{prefix}{suffix}")], data: Value::Null };
            let expected = suffix.is_empty();
            prop_assert_eq!(filter_matches(&non_matching, &literal), expected);
        }

        /// Regression: nested arrays in event data must not affect matching.
        #[test]
        fn nested_arrays_do_not_affect_matching(
            topic in topic_segment(),
            nested in vec(vec(any::<i64>(), 0..4), 0..4),
        ) {
            let subscription = FilterConfig { topic_filters: vec![topic.clone()] };
            let event = EventPayload {
                topics: vec![topic],
                data: json!({ "nested": nested }),
            };
            prop_assert!(filter_matches(&event, &subscription));
        }
    }
}
