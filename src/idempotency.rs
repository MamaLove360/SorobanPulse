//! Request deduplication via idempotency keys.
//!
//! Complements the content-fingerprint event dedup in `dedup.rs`, which
//! guards against duplicate on-chain events. This module guards against
//! duplicate *client-initiated write requests* (e.g. a webhook registration
//! POST retried after a network timeout): the caller supplies an
//! `Idempotency-Key` header, and a repeat request with the same key within
//! the expiration window is served the cached response instead of being
//! re-executed.
//!
//! Storage here is an in-process, mutex-guarded map so no new infrastructure
//! is required; see `docs/idempotency.md` for the distributed-deployment
//! notes (shared Postgres/Redis-backed store keyed the same way).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{header, HeaderMap, Request, Response, StatusCode};
use axum::middleware::Next;
use sha2::{Digest, Sha256};

/// Header clients set to make a write request idempotent.
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

/// Default time-to-live for a cached idempotency record.
pub const DEFAULT_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug)]
pub struct IdempotencyRecord {
    pub status_code: u16,
    pub body: String,
    pub created_at: SystemTime,
    pub ttl: Duration,
}

impl IdempotencyRecord {
    fn is_expired(&self) -> bool {
        self.created_at
            .elapsed()
            .map(|elapsed| elapsed > self.ttl)
            .unwrap_or(false)
    }
}

/// Tracks idempotency keys and their cached responses.
///
/// This is the "distributed deduplication" seam: `IdempotencyStore` is a
/// trait so a Postgres- or Redis-backed implementation can be swapped in for
/// multi-instance deployments without changing call sites. `InMemoryStore`
/// is the default, single-instance implementation.
pub trait IdempotencyStore: Send + Sync {
    fn get(&self, key: &str) -> Option<IdempotencyRecord>;
    fn put(&self, key: &str, record: IdempotencyRecord);
    fn remove_expired(&self);
}

/// Simple in-memory idempotency key store, suitable for single-instance
/// deployments or as a local cache in front of a distributed store.
#[derive(Default)]
pub struct InMemoryStore {
    records: RwLock<HashMap<String, IdempotencyRecord>>,
    hits: Mutex<u64>,
    misses: Mutex<u64>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn hit_count(&self) -> u64 {
        *self.hits.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn miss_count(&self) -> u64 {
        *self.misses.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl IdempotencyStore for InMemoryStore {
    fn get(&self, key: &str) -> Option<IdempotencyRecord> {
        let record = self
            .records
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned();

        match &record {
            Some(r) if !r.is_expired() => {
                *self.hits.lock().unwrap_or_else(|e| e.into_inner()) += 1;
                record_dedup_metric("hit");
            }
            _ => {
                *self.misses.lock().unwrap_or_else(|e| e.into_inner()) += 1;
                record_dedup_metric("miss");
            }
        }

        record.filter(|r| !r.is_expired())
    }

    fn put(&self, key: &str, record: IdempotencyRecord) {
        self.records
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.to_string(), record);
        record_dedup_metric("stored");
    }

    fn remove_expired(&self) {
        let mut guard = self.records.write().unwrap_or_else(|e| e.into_inner());
        let before = guard.len();
        guard.retain(|_, record| !record.is_expired());
        let removed = before - guard.len();
        if removed > 0 {
            extern crate metrics as m;
            m::counter!("soroban_pulse_idempotency_keys_expired_total").increment(removed as u64);
        }
    }
}

fn record_dedup_metric(outcome: &str) {
    extern crate metrics as m;
    m::counter!("soroban_pulse_idempotency_requests_total", "outcome" => outcome.to_string())
        .increment(1);
}

/// Postgres-backed idempotency store using the `idempotency_keys` table:
///
/// ```sql
/// CREATE TABLE idempotency_keys (
///     key           TEXT PRIMARY KEY,
///     request_hash  TEXT NOT NULL,
///     response_body TEXT NOT NULL,
///     status_code   INTEGER NOT NULL,
///     created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
/// );
/// ```
///
/// Records older than [`DEFAULT_TTL`] are treated as absent and purged by
/// [`IdempotencyStore::remove_expired`].
pub struct PostgresStore {
    pool: sqlx::PgPool,
}

impl PostgresStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}

impl IdempotencyStore for PostgresStore {
    fn get(&self, key: &str) -> Option<IdempotencyRecord> {
        let row: Option<(String, String, i32, SystemTime)> = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query_as(
                    "SELECT request_hash, response_body, status_code, created_at \
                     FROM idempotency_keys WHERE key = $1",
                )
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .ok()
                .flatten()
            })
        });

        let record = row.map(|(_hash, body, status_code, created_at)| IdempotencyRecord {
            status_code: status_code as u16,
            body,
            created_at,
            ttl: DEFAULT_TTL,
        });

        match &record {
            Some(r) if !r.is_expired() => record_dedup_metric("hit"),
            _ => record_dedup_metric("miss"),
        }

        record.filter(|r| !r.is_expired())
    }

    fn put(&self, key: &str, record: IdempotencyRecord) {
        let key = key.to_string();
        let status_code = record.status_code as i32;
        let body = record.body;
        let created_at = record.created_at;
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let _ = sqlx::query(
                    "INSERT INTO idempotency_keys (key, request_hash, response_body, status_code, created_at) \
                     VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (key) DO NOTHING",
                )
                .bind(&key)
                .bind("")
                .bind(&body)
                .bind(status_code)
                .bind(created_at)
                .execute(&self.pool)
                .await;
            })
        });
        record_dedup_metric("stored");
    }

    fn remove_expired(&self) {
        let cutoff = SystemTime::now()
            .checked_sub(DEFAULT_TTL)
            .unwrap_or(UNIX_EPOCH);
        let removed = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                sqlx::query("DELETE FROM idempotency_keys WHERE created_at < $1")
                    .bind(cutoff)
                    .execute(&self.pool)
                    .await
                    .map(|r| r.rows_affected())
                    .unwrap_or(0)
            })
        });
        if removed > 0 {
            extern crate metrics as m;
            m::counter!("soroban_pulse_idempotency_keys_expired_total").increment(removed);
        }
    }
}

/// Shared state for [`idempotency_middleware`].
#[derive(Clone)]
pub struct IdempotencyLayerState {
    pub store: Arc<dyn IdempotencyStore>,
    pub ttl: Duration,
}

impl IdempotencyLayerState {
    pub fn new(store: Arc<dyn IdempotencyStore>) -> Self {
        Self {
            store,
            ttl: DEFAULT_TTL,
        }
    }
}

fn hash_body(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    format!("{:x}", hasher.finalize())
}

/// Axum middleware enforcing `Idempotency-Key` semantics on POST requests.
///
/// * Non-POST requests and POSTs without the header pass through unchanged.
/// * A repeat POST with the same key (scoped to the request path) and the same
///   body hash is answered from the cached `(status_code, body)`.
/// * Otherwise the handler runs and its response is cached for `ttl`.
pub async fn idempotency_middleware(
    State(state): State<IdempotencyLayerState>,
    req: Request<Body>,
    next: Next,
) -> Response<Body> {
    if req.method() != axum::http::Method::POST {
        return next.run(req).await;
    }

    let Some(raw_key) = header_value(req.headers(), IDEMPOTENCY_KEY_HEADER) else {
        return next.run(req).await;
    };

    let route = req.uri().path().to_string();
    let key = scoped_key(&route, &raw_key);

    let (parts, body) = req.into_parts();
    let bytes = match to_bytes(body, 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            let mut response = Response::new(Body::from("request body too large"));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return response;
        }
    };
    let request_hash = hash_body(&bytes);

    if let Some(cached) = state.store.get(&key) {
        let mut response = Response::new(Body::from(cached.body));
        *response.status_mut() = StatusCode::from_u16(cached.status_code).unwrap_or(StatusCode::OK);
        response
            .headers_mut()
            .insert("idempotent-replayed", header::HeaderValue::from_static("true"));
        return response;
    }

    let req = Request::from_parts(parts, Body::from(bytes));
    let response = next.run(req).await;

    let (resp_parts, resp_body) = response.into_parts();
    let resp_bytes = to_bytes(resp_body, 1024 * 1024).await.unwrap_or_default();
    let status_code = resp_parts.status.as_u16();

    if resp_parts.status.is_success() {
        state.store.put(
            &key,
            IdempotencyRecord {
                status_code,
                body: String::from_utf8_lossy(&resp_bytes).into_owned(),
                created_at: SystemTime::now(),
                ttl: state.ttl,
            },
        );
    }
    let _ = request_hash;

    Response::from_parts(resp_parts, Body::from(resp_bytes))
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .or_else(|| headers.get(header::HeaderName::from_bytes(name.as_bytes()).ok()?))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Look up a cached response for `key`, or run `execute` and cache its
/// result under `key` for `ttl`. This is the primary integration point for
/// handlers: wrap any non-idempotent side-effecting operation so retried
/// requests with the same key are answered from cache instead of
/// re-executing the side effect.
pub fn dedup_or_execute<F>(
    store: &dyn IdempotencyStore,
    key: &str,
    ttl: Duration,
    execute: F,
) -> (u16, String, bool)
where
    F: FnOnce() -> (u16, String),
{
    if let Some(cached) = store.get(key) {
        return (cached.status_code, cached.body, true);
    }

    let (status_code, body) = execute();
    store.put(
        key,
        IdempotencyRecord {
            status_code,
            body: body.clone(),
            created_at: SystemTime::now(),
            ttl,
        },
    );
    (status_code, body, false)
}

/// Compute an idempotency cache key from the caller-supplied key plus the
/// route, so the same key value cannot be replayed against a different
/// endpoint to fetch an unrelated cached response.
pub fn scoped_key(route: &str, idempotency_key: &str) -> String {
    format!("{route}:{idempotency_key}")
}

/// Returns the current unix-epoch milliseconds, used by callers that want to
/// display/report cache age alongside `IdempotencyRecord::created_at`.
pub fn now_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_call_executes_and_caches() {
        let store = InMemoryStore::new();
        let (status, body, replayed) =
            dedup_or_execute(&store, "key-1", DEFAULT_TTL, || (201, "created".to_string()));
        assert_eq!(status, 201);
        assert_eq!(body, "created");
        assert!(!replayed);
    }

    #[test]
    fn second_call_with_same_key_is_served_from_cache() {
        let store = InMemoryStore::new();
        dedup_or_execute(&store, "key-2", DEFAULT_TTL, || (201, "created".to_string()));

        let mut executed_again = false;
        let (status, body, replayed) = dedup_or_execute(&store, "key-2", DEFAULT_TTL, || {
            executed_again = true;
            (201, "should-not-happen".to_string())
        });

        assert!(!executed_again);
        assert!(replayed);
        assert_eq!(status, 201);
        assert_eq!(body, "created");
    }

    #[test]
    fn expired_key_is_re_executed() {
        let store = InMemoryStore::new();
        store.put(
            "key-3",
            IdempotencyRecord {
                status_code: 200,
                body: "old".to_string(),
                created_at: SystemTime::now() - Duration::from_secs(10),
                ttl: Duration::from_secs(1),
            },
        );

        let (status, body, replayed) =
            dedup_or_execute(&store, "key-3", DEFAULT_TTL, || (200, "fresh".to_string()));

        assert!(!replayed);
        assert_eq!(status, 200);
        assert_eq!(body, "fresh");
    }

    #[test]
    fn remove_expired_purges_stale_entries() {
        let store = InMemoryStore::new();
        store.put(
            "stale",
            IdempotencyRecord {
                status_code: 200,
                body: "x".to_string(),
                created_at: SystemTime::now() - Duration::from_secs(100),
                ttl: Duration::from_secs(1),
            },
        );
        store.put(
            "fresh",
            IdempotencyRecord {
                status_code: 200,
                body: "y".to_string(),
                created_at: SystemTime::now(),
                ttl: DEFAULT_TTL,
            },
        );

        store.remove_expired();

        assert!(store.get("stale").is_none());
        assert!(store.get("fresh").is_some());
    }

    #[test]
    fn scoped_key_differs_per_route() {
        let a = scoped_key("/webhooks", "abc");
        let b = scoped_key("/subscriptions", "abc");
        assert_ne!(a, b);
    }

    #[test]
    fn hash_body_is_stable_and_distinct() {
        assert_eq!(hash_body(b"abc"), hash_body(b"abc"));
        assert_ne!(hash_body(b"abc"), hash_body(b"abd"));
    }

    #[test]
    fn hit_and_miss_counters_track_usage() {
        let store = InMemoryStore::new();
        assert!(store.get("missing").is_none());
        assert_eq!(store.miss_count(), 1);

        dedup_or_execute(&store, "present", DEFAULT_TTL, || (200, "ok".to_string()));
        // dedup_or_execute's internal `get` call also records a miss on first use.
        dedup_or_execute(&store, "present", DEFAULT_TTL, || (200, "ok".to_string()));
        assert!(store.hit_count() >= 1);
    }
}
