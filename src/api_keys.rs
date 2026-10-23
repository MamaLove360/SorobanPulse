//! Scoped API keys stored hashed in the database — Issue #1258.
//!
//! - Keys are generated as `sp_<base58(32 random bytes)>`; only
//!   `sha256(key)` (hex) is persisted, so a database leak does not leak keys.
//! - Each key carries a set of [`ApiKeyScope`]s. `admin` implies every scope.
//! - Keys are validated against the database on every request (no in-memory
//!   cache), so a revoked key is rejected on the very next request.
//! - The legacy `API_KEY` / `ADMIN_API_KEY` env vars keep working unchanged;
//!   see [`crate::middleware::auth`].
//!
//! Admin endpoints (`/v1/admin/api-keys`) require the `admin` scope or an
//! `ADMIN_API_KEY`.

use axum::{
    extract::{Path, State},
    http::{Method, StatusCode},
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{error::AppError, middleware::hash_api_key, routes::AppState};

/// Prefix of every database-backed API key.
pub const KEY_PREFIX: &str = "sp_";

/// Number of random bytes in a generated key (before base58 encoding).
const KEY_RANDOM_BYTES: usize = 32;

/// Maximum length of a key's human-readable name.
const MAX_NAME_LEN: usize = 100;

// ---------------------------------------------------------------------------
// Scopes
// ---------------------------------------------------------------------------

/// Permission granted to an API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub enum ApiKeyScope {
    /// Read events, contracts, stats and streams (`GET`/`HEAD` requests).
    #[serde(rename = "read:events")]
    ReadEvents,
    /// Create, update and delete subscriptions.
    #[serde(rename = "write:subscriptions")]
    WriteSubscriptions,
    /// Full access, including `/v1/admin/*`. Implies every other scope.
    #[serde(rename = "admin")]
    Admin,
}

impl ApiKeyScope {
    pub const ALL: [ApiKeyScope; 3] = [
        ApiKeyScope::ReadEvents,
        ApiKeyScope::WriteSubscriptions,
        ApiKeyScope::Admin,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ApiKeyScope::ReadEvents => "read:events",
            ApiKeyScope::WriteSubscriptions => "write:subscriptions",
            ApiKeyScope::Admin => "admin",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|scope| scope.as_str() == s)
    }
}

/// `POST` routes that only read data and therefore need `read:events`.
const READ_ONLY_POST_PATHS: &[&str] = &["/v1/events/tx/batch", "/graphql"];

/// The scope a request needs, derived from its method and path.
///
/// - `/v1/admin/*` (and unversioned `/admin/*`) → `admin`
/// - `GET` / `HEAD` / `OPTIONS`, and read-only `POST` queries → `read:events`
/// - mutating requests on subscription routes → `write:subscriptions`
/// - any other mutating request → `admin` (least privilege by default)
pub fn required_scope(method: &Method, path: &str) -> ApiKeyScope {
    let is_admin_path = path == "/v1/admin"
        || path.starts_with("/v1/admin/")
        || path == "/admin"
        || path.starts_with("/admin/");
    if is_admin_path {
        return ApiKeyScope::Admin;
    }

    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
        || (*method == Method::POST && READ_ONLY_POST_PATHS.contains(&path))
    {
        return ApiKeyScope::ReadEvents;
    }

    if path
        .split('/')
        .any(|segment| segment == "subscriptions")
    {
        return ApiKeyScope::WriteSubscriptions;
    }

    ApiKeyScope::Admin
}

// ---------------------------------------------------------------------------
// Key generation
// ---------------------------------------------------------------------------

const BASE58_ALPHABET: &[u8; 58] =
    b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// Bitcoin-alphabet base58 encoding.
fn base58_encode(input: &[u8]) -> String {
    let leading_zeros = input.iter().take_while(|&&b| b == 0).count();

    // Little-endian base58 digits.
    let mut digits: Vec<u8> = Vec::with_capacity(input.len() * 138 / 100 + 1);
    for &byte in input {
        let mut carry = u32::from(byte);
        for digit in digits.iter_mut() {
            carry += u32::from(*digit) << 8;
            *digit = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }

    let mut out = String::with_capacity(leading_zeros + digits.len());
    out.extend(std::iter::repeat('1').take(leading_zeros));
    out.extend(digits.iter().rev().map(|&d| BASE58_ALPHABET[d as usize] as char));
    out
}

/// Generate a new random API key: `sp_<base58(32 random bytes)>`.
pub fn generate_api_key() -> String {
    let mut bytes = [0u8; KEY_RANDOM_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    format!("{KEY_PREFIX}{}", base58_encode(&bytes))
}

// ---------------------------------------------------------------------------
// Lookup (used by the auth middleware)
// ---------------------------------------------------------------------------

/// An authenticated database-backed key, injected as a request extension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedApiKey {
    pub id: Uuid,
    pub name: String,
    pub scopes: Vec<ApiKeyScope>,
    pub tenant_id: Option<String>,
}

impl ScopedApiKey {
    /// Whether the key grants `scope`. `admin` grants everything.
    pub fn has_scope(&self, scope: ApiKeyScope) -> bool {
        self.scopes.contains(&ApiKeyScope::Admin) || self.scopes.contains(&scope)
    }
}

#[derive(sqlx::FromRow)]
struct ActiveKeyRow {
    id: Uuid,
    name: String,
    scopes: Vec<String>,
    tenant_id: Option<String>,
}

/// Look up an active (non-revoked) key by its raw value.
///
/// Always hits the database so revocation takes effect immediately. Also
/// refreshes `last_used_at`, at most once a minute so authentication does
/// not turn every request into a row update.
pub async fn lookup_active_key(
    pool: &PgPool,
    raw_key: &str,
) -> Result<Option<ScopedApiKey>, sqlx::Error> {
    if !raw_key.starts_with(KEY_PREFIX) {
        return Ok(None);
    }

    // The data-modifying CTE runs even though the outer SELECT does not
    // reference it.
    let row: Option<ActiveKeyRow> = sqlx::query_as(
        r#"
        WITH active AS (
            SELECT id, name, scopes, tenant_id, last_used_at
            FROM api_keys
            WHERE key_hash = $1 AND revoked_at IS NULL
        ), touch AS (
            UPDATE api_keys k
            SET last_used_at = NOW()
            FROM active
            WHERE k.id = active.id
              AND (active.last_used_at IS NULL
                   OR active.last_used_at < NOW() - INTERVAL '60 seconds')
        )
        SELECT id, name, scopes, tenant_id FROM active
        "#,
    )
    .bind(hash_api_key(raw_key))
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| ScopedApiKey {
        id: r.id,
        name: r.name,
        scopes: r.scopes.iter().filter_map(|s| ApiKeyScope::parse(s)).collect(),
        tenant_id: r.tenant_id,
    }))
}

// ---------------------------------------------------------------------------
// Admin API
// ---------------------------------------------------------------------------

/// Request body for `POST /v1/admin/api-keys`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateApiKeyRequest {
    /// Human-readable label, e.g. `"billing-service"`.
    #[schema(example = "billing-service")]
    pub name: String,
    /// Scopes to grant. At least one is required.
    pub scopes: Vec<ApiKeyScope>,
    /// Tenant to bind the key to (multi-tenant deployments only).
    #[serde(default)]
    pub tenant_id: Option<String>,
}

/// Response for `POST /v1/admin/api-keys`. `key` is only ever returned here.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CreatedApiKey {
    pub id: Uuid,
    pub name: String,
    /// The plaintext key. Store it now — it cannot be retrieved again.
    #[schema(example = "sp_4Yh3...")]
    pub key: String,
    pub scopes: Vec<ApiKeyScope>,
    pub tenant_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// An API key as listed by `GET /v1/admin/api-keys` (never includes the key).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ApiKeyInfo {
    pub id: Uuid,
    pub name: String,
    pub scopes: Vec<ApiKeyScope>,
    pub tenant_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct ApiKeyRow {
    id: Uuid,
    name: String,
    scopes: Vec<String>,
    tenant_id: Option<String>,
    created_at: DateTime<Utc>,
    last_used_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
}

impl From<ApiKeyRow> for ApiKeyInfo {
    fn from(r: ApiKeyRow) -> Self {
        ApiKeyInfo {
            id: r.id,
            name: r.name,
            scopes: r.scopes.iter().filter_map(|s| ApiKeyScope::parse(s)).collect(),
            tenant_id: r.tenant_id,
            created_at: r.created_at,
            last_used_at: r.last_used_at,
            revoked_at: r.revoked_at,
        }
    }
}

/// Validate and normalise a create request: trimmed name, de-duplicated
/// scopes in canonical order.
fn validate_create(req: CreateApiKeyRequest) -> Result<CreateApiKeyRequest, AppError> {
    let name = req.name.trim().to_owned();
    if name.is_empty() {
        return Err(AppError::Validation("name must not be empty".into()));
    }
    if name.chars().count() > MAX_NAME_LEN {
        return Err(AppError::Validation(format!(
            "name must be at most {MAX_NAME_LEN} characters"
        )));
    }
    if req.scopes.is_empty() {
        return Err(AppError::Validation("at least one scope is required".into()));
    }
    let scopes = ApiKeyScope::ALL
        .into_iter()
        .filter(|s| req.scopes.contains(s))
        .collect();
    let tenant_id = req
        .tenant_id
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty());
    Ok(CreateApiKeyRequest { name, scopes, tenant_id })
}

/// Create a scoped API key. The plaintext key is returned once.
#[utoipa::path(
    post,
    path = "/v1/admin/api-keys",
    tag = "admin",
    request_body = CreateApiKeyRequest,
    responses(
        (status = 201, description = "Key created; `key` is shown only once", body = CreatedApiKey),
        (status = 400, description = "Invalid name or scopes"),
        (status = 401, description = "Missing credentials"),
        (status = 403, description = "Caller lacks the `admin` scope"),
    )
)]
pub async fn create_api_key(
    State(state): State<AppState>,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<impl IntoResponse, AppError> {
    let req = validate_create(req)?;
    let key = generate_api_key();
    let scope_strs: Vec<&str> = req.scopes.iter().map(|s| s.as_str()).collect();

    let (id, created_at): (Uuid, DateTime<Utc>) = sqlx::query_as(
        "INSERT INTO api_keys (name, key_hash, scopes, tenant_id)
         VALUES ($1, $2, $3, $4)
         RETURNING id, created_at",
    )
    .bind(&req.name)
    .bind(hash_api_key(&key))
    .bind(&scope_strs)
    .bind(&req.tenant_id)
    .fetch_one(&state.pool)
    .await?;

    tracing::info!(api_key_id = %id, name = %req.name, scopes = ?scope_strs, "API key created");

    Ok((
        StatusCode::CREATED,
        Json(CreatedApiKey {
            id,
            name: req.name,
            key,
            scopes: req.scopes,
            tenant_id: req.tenant_id,
            created_at,
        }),
    ))
}

/// List all API keys, newest first. Plaintext keys are never returned.
#[utoipa::path(
    get,
    path = "/v1/admin/api-keys",
    tag = "admin",
    responses(
        (status = 200, description = "All API keys, including revoked ones", body = [ApiKeyInfo]),
        (status = 401, description = "Missing credentials"),
        (status = 403, description = "Caller lacks the `admin` scope"),
    )
)]
pub async fn list_api_keys(
    State(state): State<AppState>,
) -> Result<Json<Vec<ApiKeyInfo>>, AppError> {
    let rows: Vec<ApiKeyRow> = sqlx::query_as(
        "SELECT id, name, scopes, tenant_id, created_at, last_used_at, revoked_at
         FROM api_keys
         ORDER BY created_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(ApiKeyInfo::from).collect()))
}

/// Revoke an API key. Takes effect on the next request; idempotent.
#[utoipa::path(
    delete,
    path = "/v1/admin/api-keys/{id}",
    tag = "admin",
    params(("id" = Uuid, Path, description = "API key id")),
    responses(
        (status = 204, description = "Key revoked"),
        (status = 401, description = "Missing credentials"),
        (status = 403, description = "Caller lacks the `admin` scope"),
        (status = 404, description = "No key with this id"),
    )
)]
pub async fn revoke_api_key(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let result = sqlx::query(
        "UPDATE api_keys SET revoked_at = COALESCE(revoked_at, NOW()) WHERE id = $1",
    )
    .bind(id)
    .execute(&state.pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    tracing::info!(api_key_id = %id, "API key revoked");
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base58_known_vectors() {
        assert_eq!(base58_encode(b""), "");
        assert_eq!(base58_encode(&[0]), "1");
        assert_eq!(base58_encode(&[0, 0, 1]), "112");
        assert_eq!(base58_encode(b"hello world"), "StV1DL6CwTryKyV");
        assert_eq!(base58_encode(&[0xff; 4]), "7YXq9G");
    }

    #[test]
    fn generated_keys_have_prefix_base58_body_and_are_unique() {
        let a = generate_api_key();
        let b = generate_api_key();
        assert_ne!(a, b);
        for key in [&a, &b] {
            let body = key.strip_prefix(KEY_PREFIX).expect("sp_ prefix");
            // 32 bytes encode to 43 or 44 base58 characters (fewer only with
            // leading zero bytes, which encode as '1').
            assert!((32..=44).contains(&body.len()), "unexpected length: {key}");
            assert!(body.bytes().all(|c| BASE58_ALPHABET.contains(&c)));
        }
    }

    #[test]
    fn scope_round_trips_through_serde_and_parse() {
        for scope in ApiKeyScope::ALL {
            let json = serde_json::to_string(&scope).unwrap();
            assert_eq!(json, format!("\"{}\"", scope.as_str()));
            assert_eq!(serde_json::from_str::<ApiKeyScope>(&json).unwrap(), scope);
            assert_eq!(ApiKeyScope::parse(scope.as_str()), Some(scope));
        }
        assert_eq!(ApiKeyScope::parse("root"), None);
        assert!(serde_json::from_str::<ApiKeyScope>("\"root\"").is_err());
    }

    #[test]
    fn admin_scope_implies_all() {
        let key = ScopedApiKey {
            id: Uuid::nil(),
            name: "k".into(),
            scopes: vec![ApiKeyScope::Admin],
            tenant_id: None,
        };
        assert!(ApiKeyScope::ALL.into_iter().all(|s| key.has_scope(s)));

        let reader = ScopedApiKey { scopes: vec![ApiKeyScope::ReadEvents], ..key };
        assert!(reader.has_scope(ApiKeyScope::ReadEvents));
        assert!(!reader.has_scope(ApiKeyScope::WriteSubscriptions));
        assert!(!reader.has_scope(ApiKeyScope::Admin));
    }

    #[test]
    fn required_scope_by_route() {
        use ApiKeyScope::*;
        let cases = [
            (Method::GET, "/v1/events", ReadEvents),
            (Method::HEAD, "/v1/contracts", ReadEvents),
            (Method::GET, "/v1/subscriptions", ReadEvents),
            (Method::POST, "/v1/events/tx/batch", ReadEvents),
            (Method::POST, "/graphql", ReadEvents),
            (Method::POST, "/v1/subscriptions", WriteSubscriptions),
            (Method::DELETE, "/v1/subscriptions/abc", WriteSubscriptions),
            (Method::PATCH, "/v1/subscriptions/abc/pause", WriteSubscriptions),
            (Method::GET, "/v1/admin/api-keys", Admin),
            (Method::POST, "/v1/admin/api-keys", Admin),
            (Method::GET, "/admin/schemas", Admin),
            (Method::POST, "/v1/saved-queries", Admin),
            // A path merely containing the word must not match.
            (Method::POST, "/v1/subscriptionsx", Admin),
        ];
        for (method, path, expected) in cases {
            assert_eq!(required_scope(&method, path), expected, "{method} {path}");
        }
    }

    fn create_req(name: &str, scopes: Vec<ApiKeyScope>) -> CreateApiKeyRequest {
        CreateApiKeyRequest { name: name.into(), scopes, tenant_id: None }
    }

    #[test]
    fn validate_create_rejects_bad_input() {
        assert!(validate_create(create_req("  ", vec![ApiKeyScope::Admin])).is_err());
        assert!(validate_create(create_req("svc", vec![])).is_err());
        assert!(validate_create(create_req(&"x".repeat(101), vec![ApiKeyScope::Admin])).is_err());
    }

    #[test]
    fn validate_create_normalises() {
        let req = CreateApiKeyRequest {
            name: "  svc  ".into(),
            scopes: vec![
                ApiKeyScope::Admin,
                ApiKeyScope::ReadEvents,
                ApiKeyScope::ReadEvents,
            ],
            tenant_id: Some("  ".into()),
        };
        let req = validate_create(req).unwrap();
        assert_eq!(req.name, "svc");
        assert_eq!(req.scopes, vec![ApiKeyScope::ReadEvents, ApiKeyScope::Admin]);
        assert_eq!(req.tenant_id, None);
    }
}
