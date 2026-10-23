//! Authentication middleware — Issue #663
//!
//! Provides:
//! - [`AuthState`] — shared state injected into the auth layer.
//! - [`auth_middleware`] — validates `Authorization: Bearer` / `X-Api-Key`
//!   headers and, in multi-tenant mode, resolves the tenant.
//! - [`AdminAuthState`] — shared state for the admin-only layer.
//! - [`admin_auth_middleware`] — gates `/v1/admin/*` endpoints.
//! - [`TenantId`] — request extension carrying the resolved tenant.
//! - [`hash_api_key`] — SHA-256 hex digest helper.
//! - [`require_admin_middleware`] — strict admin gate for key management.
//!
//! ## Database-backed scoped keys (issue #1258)
//!
//! When [`AuthState::pool`] is set, keys starting with `sp_` are validated
//! against the `api_keys` table on every request (no caching, so revocation
//! takes effect immediately) and must carry the scope the route requires
//! (see [`crate::api_keys::required_scope`]). Unknown or revoked `sp_` keys
//! fall through to the legacy env-var key check, which keeps working
//! unchanged for backward compatibility.

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
    Json,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// The resolved tenant for the current request.
///
/// Injected as a request extension by [`auth_middleware`] when multi-tenant
/// mode is enabled.  Handlers read it via
/// `req.extensions().get::<TenantId>()`.
#[derive(Clone, Debug)]
pub struct TenantId(pub String);

/// The authenticated principal (owner identity) for the current request.
///
/// Injected by [`auth_middleware`] on every non-public request: a stable,
/// non-reversible identifier derived from the API key, or `"anonymous"` when
/// authentication is disabled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal(pub String);

impl Principal {
    pub const ANONYMOUS: &'static str = "anonymous";

    pub fn from_api_key(key: &str) -> Self {
        Principal(format!("key:{}", &hash_api_key(key)[..16]))
    }

    pub fn anonymous() -> Self {
        Principal(Self::ANONYMOUS.to_string())
    }
}

/// Shared state for the global authentication middleware layer.
#[derive(Clone)]
pub struct AuthState {
    /// Regular API keys.
    pub api_keys: Vec<String>,
    /// Admin API keys — also accepted at the global gate so admin requests
    /// pass through to the admin-only layer for privilege checking.
    pub admin_api_keys: Vec<String>,
    /// SHA-256(key) → tenant_id mapping; populated only in multi-tenant mode.
    pub tenant_map: Arc<std::collections::HashMap<String, String>>,
    /// Whether multi-tenant mode is active.
    pub multi_tenant: bool,
    /// Database pool for scoped-key lookups; `None` disables DB-backed keys.
    pub pool: Option<sqlx::PgPool>,
    /// Require a valid key even when no env-var `api_keys` are configured
    /// (`REQUIRE_API_KEY=true`), for deployments that only use DB-backed keys.
    pub require_auth: bool,
}

impl AuthState {
    /// Whether requests must present a valid key.
    fn auth_enabled(&self) -> bool {
        self.require_auth || !self.api_keys.is_empty()
    }
}

/// Shared state for the admin-only authentication layer.
#[derive(Clone)]
pub struct AdminAuthState {
    /// Keys that grant admin access.  When empty the layer is a no-op and
    /// admin routes fall back to whatever the regular auth layer enforced.
    pub admin_api_keys: Vec<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Constant-time check whether `key` equals any of `candidates`.
pub(crate) fn key_matches_any(key: &str, candidates: &[String]) -> bool {
    candidates.iter().any(|expected| {
        let m: bool = key.as_bytes().ct_eq(expected.as_bytes()).into();
        m
    })
}

/// Extract the API key from either the `Authorization: Bearer <key>` header
/// or the `X-Api-Key` header.
pub(crate) fn extract_api_key(req: &Request) -> Option<&str> {
    let bearer = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    let x_api_key = req.headers().get("X-Api-Key").and_then(|h| h.to_str().ok());
    bearer.or(x_api_key)
}

/// SHA-256 hex digest of a raw API key — used as the lookup key in
/// `tenant_map` so plain-text keys are never stored in memory after hashing.
pub fn hash_api_key(key: &str) -> String {
    let mut h = Sha256::new();
    h.update(key.as_bytes());
    format!("{:x}", h.finalize())
}

/// Paths that never require authentication.
fn is_public_path(path: &str) -> bool {
    path == "/health"
        || path.starts_with("/healthz/")
        || path == "/unsubscribe"
        || path == "/v1/meta"
        || crate::dashboard::is_dashboard_path(path)
}

/// Resolve a provided `sp_` key against the database.
///
/// Returns `None` when DB-backed keys are disabled, no `sp_` key was sent, the
/// key is unknown or revoked, or the lookup failed (logged) — in every case
/// the caller falls back to the env-var key check.
async fn lookup_scoped_key(
    state: &AuthState,
    provided_key: Option<&str>,
) -> Option<crate::api_keys::ScopedApiKey> {
    let pool = state.pool.as_ref()?;
    let key = provided_key.filter(|k| k.starts_with(crate::api_keys::KEY_PREFIX))?;
    match crate::api_keys::lookup_active_key(pool, key).await {
        Ok(found) => found,
        Err(e) => {
            tracing::warn!(error = %e, "scoped API key lookup failed; falling back to env-var keys");
            None
        }
    }
}

/// Whether the request was authenticated with a DB key holding `admin`.
fn has_admin_scoped_key(req: &Request) -> bool {
    req.extensions()
        .get::<crate::api_keys::ScopedApiKey>()
        .map_or(false, |k| k.has_scope(crate::api_keys::ApiKeyScope::Admin))
}

// ---------------------------------------------------------------------------
// Middleware functions
// ---------------------------------------------------------------------------

/// Global authentication middleware.
///
/// - Skips `/health`, `/healthz/*`, `/unsubscribe` and the `/ui` dashboard (public paths).
/// - When `api_keys` is empty, auth is disabled and all requests pass.
/// - In multi-tenant mode, resolves the tenant and injects [`TenantId`].
pub async fn auth_middleware(
    State(state): State<Arc<AuthState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    // Public paths — always bypass auth.
    // `/ui` serves the static dashboard shell (issue #1112); its data calls
    // go through the authenticated API. `/v1/meta` is capability discovery
    // (issue #1259).
    if is_public_path(req.uri().path()) {
        return Ok(next.run(req).await);
    }

    // Database-backed scoped keys (issue #1258). The key is copied out so no
    // borrow of the (non-`Sync`) request is held across the lookup.
    let provided_owned = extract_api_key(&req).map(str::to_owned);
    if let Some(scoped) = lookup_scoped_key(&state, provided_owned.as_deref()).await {
        let required = crate::api_keys::required_scope(req.method(), req.uri().path());
        if !scoped.has_scope(required) {
            return Err((
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "insufficient scope",
                    "required_scope": required.as_str(),
                })),
            ));
        }

        let key = provided_owned.unwrap_or_default();
        req.extensions_mut().insert(Principal::from_api_key(&key));

        // Admin-scoped keys are global, like env-var admin keys.
        if state.multi_tenant && !scoped.has_scope(crate::api_keys::ApiKeyScope::Admin) {
            let tenant = scoped
                .tenant_id
                .clone()
                .or_else(|| state.tenant_map.get(&hash_api_key(&key)).cloned());
            match tenant {
                Some(tid) => {
                    req.extensions_mut().insert(TenantId(tid));
                }
                None => {
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(serde_json::json!({
                            "error": "api key is not associated with a tenant"
                        })),
                    ));
                }
            }
        }

        req.extensions_mut().insert(scoped);
        return Ok(next.run(req).await);
    }

    if state.auth_enabled() {
        let provided_key = provided_owned.as_deref();

        let is_admin = provided_key.map_or(false, |k| key_matches_any(k, &state.admin_api_keys));
        let is_valid =
            is_admin || provided_key.map_or(false, |k| key_matches_any(k, &state.api_keys));

        if !is_valid {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            ));
        }

        req.extensions_mut()
            .insert(Principal::from_api_key(provided_key.unwrap_or("")));

        // Multi-tenant: resolve and inject tenant_id.  Admin keys are global
        // and skip tenant resolution.
        if state.multi_tenant && !is_admin {
            let key = provided_key.unwrap_or("");
            let key_hash = hash_api_key(key);
            match state.tenant_map.get(&key_hash) {
                Some(tid) => {
                    req.extensions_mut().insert(TenantId(tid.clone()));
                }
                None => {
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(serde_json::json!({
                            "error": "api key is not associated with a tenant"
                        })),
                    ));
                }
            }
        }
    } else {
        req.extensions_mut().insert(Principal::anonymous());
    }

    Ok(next.run(req).await)
}

/// Admin-only authentication middleware.
///
/// Guards `/v1/admin/*` endpoints independently of the global API key:
/// - No key → **401 Unauthorized**.
/// - Non-admin key → **403 Forbidden**.
/// - Admin key, or a DB key with the `admin` scope → passes through.
/// - No admin keys configured → no-op (backward-compatible fallback).
pub async fn admin_auth_middleware(
    State(state): State<Arc<AdminAuthState>>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    if state.admin_api_keys.is_empty() || has_admin_scoped_key(&req) {
        return Ok(next.run(req).await);
    }

    match extract_api_key(&req) {
        None => Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "admin authentication required" })),
        )),
        Some(key) if key_matches_any(key, &state.admin_api_keys) => Ok(next.run(req).await),
        Some(_) => Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "admin privileges required" })),
        )),
    }
}

/// Strict admin gate for API key management (issue #1258).
///
/// Unlike [`admin_auth_middleware`] this is never a no-op: minting keys must
/// not be possible with a regular key just because `ADMIN_API_KEY` is unset.
/// Must run inside [`auth_middleware`], which resolves DB-backed keys.
///
/// - DB key with the `admin` scope, or an `ADMIN_API_KEY` → passes through.
/// - No key → **401 Unauthorized**; any other key → **403 Forbidden**.
pub async fn require_admin_middleware(
    State(state): State<Arc<AdminAuthState>>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    if has_admin_scoped_key(&req) {
        return Ok(next.run(req).await);
    }
    match extract_api_key(&req) {
        None => Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "admin authentication required" })),
        )),
        Some(key) if key_matches_any(key, &state.admin_api_keys) => Ok(next.run(req).await),
        Some(_) => Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "admin privileges required" })),
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get, Router};
    use tower::ServiceExt;

    fn make_auth_app(api_keys: Vec<String>) -> Router {
        let state = Arc::new(AuthState {
            api_keys,
            admin_api_keys: vec![],
            tenant_map: Arc::new(std::collections::HashMap::new()),
            multi_tenant: false,
            pool: None,
            require_auth: false,
        });
        Router::new()
            .route("/test", get(|| async { "OK" }))
            .route("/v1/meta", get(|| async { "OK" }))
            .route("/health", get(|| async { "OK" }))
            .route("/healthz/live", get(|| async { "OK" }))
            .route_layer(axum::middleware::from_fn_with_state(
                state,
                auth_middleware,
            ))
    }

    #[tokio::test]
    async fn no_keys_configured_passes_all() {
        let resp = make_auth_app(vec![])
            .oneshot(axum::http::Request::get("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn bearer_token_accepted() {
        let resp = make_auth_app(vec!["secret".into()])
            .oneshot(
                axum::http::Request::get("/test")
                    .header("Authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn x_api_key_accepted() {
        let resp = make_auth_app(vec!["secret".into()])
            .oneshot(
                axum::http::Request::get("/test")
                    .header("X-Api-Key", "secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn wrong_key_returns_401() {
        let resp = make_auth_app(vec!["secret".into()])
            .oneshot(
                axum::http::Request::get("/test")
                    .header("X-Api-Key", "wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn health_bypasses_auth() {
        let app = make_auth_app(vec!["secret".into()]);
        for path in ["/health", "/healthz/live"] {
            let resp = app
                .clone()
                .oneshot(axum::http::Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[test]
    fn hash_is_deterministic_and_hex() {
        let h = hash_api_key("key");
        assert_eq!(h, hash_api_key("key"));
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn admin_layer_requires_admin_key() {
        let state = Arc::new(AdminAuthState {
            admin_api_keys: vec!["admin".into()],
        });
        let app = Router::new()
            .route("/admin", get(|| async { "ok" }))
            .route_layer(axum::middleware::from_fn_with_state(
                state,
                admin_auth_middleware,
            ));

        // No key → 401
        let resp = app
            .clone()
            .oneshot(axum::http::Request::get("/admin").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Wrong key → 403
        let resp = app
            .clone()
            .oneshot(
                axum::http::Request::get("/admin")
                    .header("X-Api-Key", "regular")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // Correct key → 200
        let resp = app
            .oneshot(
                axum::http::Request::get("/admin")
                    .header("X-Api-Key", "admin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

#[cfg(test)]
mod scoped_key_tests {
    use super::*;
    use crate::api_keys::{ApiKeyScope, ScopedApiKey};
    use axum::{body::Body, routing::get, Router};
    use tower::ServiceExt;

    fn auth_state(api_keys: Vec<String>, require_auth: bool) -> Arc<AuthState> {
        Arc::new(AuthState {
            api_keys,
            admin_api_keys: vec![],
            tenant_map: Arc::new(std::collections::HashMap::new()),
            multi_tenant: false,
            pool: None,
            require_auth,
        })
    }

    async fn status(app: &Router, path: &str, key: Option<&str>) -> StatusCode {
        let mut req = axum::http::Request::get(path);
        if let Some(k) = key {
            req = req.header("X-Api-Key", k);
        }
        app.clone().oneshot(req.body(Body::empty()).unwrap()).await.unwrap().status()
    }

    #[tokio::test]
    async fn meta_is_public_even_with_auth() {
        let app = Router::new()
            .route("/v1/meta", get(|| async { "OK" }))
            .route("/v1/events", get(|| async { "OK" }))
            .route_layer(axum::middleware::from_fn_with_state(
                auth_state(vec!["secret".into()], false),
                auth_middleware,
            ));
        assert_eq!(status(&app, "/v1/meta", None).await, StatusCode::OK);
        assert_eq!(status(&app, "/v1/events", None).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn require_auth_without_env_keys_rejects_unknown_keys() {
        let app = Router::new()
            .route("/v1/events", get(|| async { "OK" }))
            .route_layer(axum::middleware::from_fn_with_state(
                auth_state(vec![], true),
                auth_middleware,
            ));
        assert_eq!(status(&app, "/v1/events", None).await, StatusCode::UNAUTHORIZED);
        assert_eq!(
            status(&app, "/v1/events", Some("sp_unknown")).await,
            StatusCode::UNAUTHORIZED
        );
    }

    fn require_admin_app(admin_keys: Vec<String>, injected: Option<ScopedApiKey>) -> Router {
        let state = Arc::new(AdminAuthState { admin_api_keys: admin_keys });
        Router::new()
            .route("/v1/admin/api-keys", get(|| async { "OK" }))
            .route_layer(axum::middleware::from_fn_with_state(
                state,
                require_admin_middleware,
            ))
            // Stands in for auth_middleware having resolved a DB key.
            .layer(axum::middleware::from_fn(move |mut req: Request, next: Next| {
                let injected = injected.clone();
                async move {
                    if let Some(k) = injected {
                        req.extensions_mut().insert(k);
                    }
                    next.run(req).await
                }
            }))
    }

    fn scoped(scopes: Vec<ApiKeyScope>) -> ScopedApiKey {
        ScopedApiKey { id: uuid::Uuid::nil(), name: "k".into(), scopes, tenant_id: None }
    }

    #[tokio::test]
    async fn require_admin_accepts_env_admin_key_only() {
        let app = require_admin_app(vec!["admin".into()], None);
        assert_eq!(status(&app, "/v1/admin/api-keys", None).await, StatusCode::UNAUTHORIZED);
        assert_eq!(
            status(&app, "/v1/admin/api-keys", Some("regular")).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(status(&app, "/v1/admin/api-keys", Some("admin")).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn require_admin_is_not_a_noop_without_admin_keys() {
        let app = require_admin_app(vec![], None);
        assert_eq!(
            status(&app, "/v1/admin/api-keys", Some("anything")).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn require_admin_checks_scoped_key_scope() {
        let admin = require_admin_app(vec![], Some(scoped(vec![ApiKeyScope::Admin])));
        assert_eq!(status(&admin, "/v1/admin/api-keys", Some("sp_x")).await, StatusCode::OK);

        let reader = require_admin_app(vec![], Some(scoped(vec![ApiKeyScope::ReadEvents])));
        assert_eq!(
            status(&reader, "/v1/admin/api-keys", Some("sp_x")).await,
            StatusCode::FORBIDDEN
        );
    }
}

#[cfg(test)]
mod principal_tests {
    use super::Principal;

    #[test]
    fn principals_differ_per_key_and_are_stable() {
        assert_ne!(Principal::from_api_key("a"), Principal::from_api_key("b"));
        assert_eq!(Principal::from_api_key("a"), Principal::from_api_key("a"));
        assert_eq!(Principal::anonymous().0, "anonymous");
    }
}
