//! Authentication and Authorization Bypass Tests
//!
//! These tests systematically probe every bypass scenario for the two-layer
//! auth system (global API key gate + admin-only layer):
//!
//! - Missing / malformed credentials
//! - Header scheme variations
//! - Admin key escalation paths
//! - Public endpoint exemptions
//! - Multi-tenant isolation
//! - Edge cases: empty keys, whitespace, very long inputs, null bytes
//! - Constant-time comparison properties
//! - Key hashing correctness

use axum::{body::Body, http::StatusCode, routing::get, Router};
use soroban_pulse::middleware::auth::{
    admin_auth_middleware, auth_middleware, hash_api_key, key_matches_any, AdminAuthState,
    AuthState, TenantId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Router factory helpers
// ---------------------------------------------------------------------------

fn auth_app(api_keys: Vec<String>) -> Router {
    let state = Arc::new(AuthState {
        api_keys,
        admin_api_keys: vec![],
        tenant_map: Arc::new(HashMap::new()),
        multi_tenant: false,
        pool: None,
        require_auth: false,
    });
    Router::new()
        .route("/test", get(|| async { "OK" }))
        .route("/health", get(|| async { "OK" }))
        .route("/healthz/live", get(|| async { "OK" }))
        .route("/healthz/ready", get(|| async { "OK" }))
        .route("/unsubscribe", get(|| async { "OK" }))
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            auth_middleware,
        ))
}

fn auth_app_with_admin(api_keys: Vec<String>, admin_keys: Vec<String>) -> Router {
    let state = Arc::new(AuthState {
        api_keys: api_keys.clone(),
        admin_api_keys: admin_keys.clone(),
        tenant_map: Arc::new(HashMap::new()),
        multi_tenant: false,
        pool: None,
        require_auth: false,
    });
    Router::new()
        .route("/test", get(|| async { "OK" }))
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            auth_middleware,
        ))
}

fn admin_app(admin_keys: Vec<String>) -> Router {
    let state = Arc::new(AdminAuthState {
        admin_api_keys: admin_keys,
    });
    Router::new()
        .route("/v1/admin/action", get(|| async { "admin OK" }))
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            admin_auth_middleware,
        ))
}

fn multitenant_app(api_keys: Vec<String>, tenant_map: HashMap<String, String>) -> Router {
    let state = Arc::new(AuthState {
        api_keys,
        admin_api_keys: vec![],
        tenant_map: Arc::new(tenant_map),
        multi_tenant: true,
        pool: None,
        require_auth: false,
    });
    Router::new()
        .route("/test", get(|| async { "OK" }))
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            auth_middleware,
        ))
}

// ===========================================================================
// Open API (no keys configured)
// ===========================================================================

#[tokio::test]
async fn no_keys_configured_all_requests_pass() {
    let resp = auth_app(vec![])
        .oneshot(
            axum::http::Request::get("/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn no_keys_configured_request_without_header_passes() {
    let resp = auth_app(vec![])
        .oneshot(
            axum::http::Request::get("/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Basic auth scenarios
// ===========================================================================

#[tokio::test]
async fn no_auth_header_returns_401_when_keys_configured() {
    let resp = auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn correct_bearer_token_returns_200() {
    let resp = auth_app(vec!["my-key".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("Authorization", "Bearer my-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn correct_x_api_key_returns_200() {
    let resp = auth_app(vec!["my-key".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "my-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn wrong_key_returns_401() {
    let resp = auth_app(vec!["correct".into()])
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

// ===========================================================================
// Header scheme variations
// ===========================================================================

#[tokio::test]
async fn bearer_without_space_rejected() {
    // "Bearersecret" should not be parsed as valid bearer token "secret"
    let resp = auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("Authorization", "Bearersecret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn basic_auth_scheme_not_accepted_as_bearer() {
    // Basic base64("secret:") should not be treated as a valid key
    let resp = auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("Authorization", "Basic c2VjcmV0Og==")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn both_headers_present_bearer_wins() {
    // Bearer key is valid, X-Api-Key is wrong — Bearer should win
    let resp = auth_app(vec!["bearer-key".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("Authorization", "Bearer bearer-key")
                .header("X-Api-Key", "wrong-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn only_x_api_key_wrong_returns_401() {
    let resp = auth_app(vec!["correct".into()])
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

// ===========================================================================
// Multiple configured keys
// ===========================================================================

#[tokio::test]
async fn any_valid_key_in_list_grants_access() {
    let resp = auth_app(vec!["key-a".into(), "key-b".into(), "key-c".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "key-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn key_not_in_list_returns_401() {
    let resp = auth_app(vec!["key-a".into(), "key-b".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "key-c")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ===========================================================================
// Admin key integration
// ===========================================================================

#[tokio::test]
async fn admin_key_accepted_at_regular_gate() {
    // Admin key must also be accepted at the global auth gate
    let state = Arc::new(AuthState {
        api_keys: vec!["regular-key".into()],
        admin_api_keys: vec!["admin-key".into()],
        tenant_map: Arc::new(HashMap::new()),
        multi_tenant: false,
        pool: None,
        require_auth: false,
    });
    let app = Router::new()
        .route("/test", get(|| async { "OK" }))
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            auth_middleware,
        ));
    let resp = app
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "admin-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn regular_key_blocked_at_admin_layer_with_403() {
    let resp = admin_app(vec!["admin-secret".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .header("X-Api-Key", "regular-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn admin_key_accepted_at_admin_layer() {
    let resp = admin_app(vec!["admin-secret".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .header("X-Api-Key", "admin-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn no_admin_keys_configured_admin_layer_is_noop() {
    let resp = admin_app(vec![])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // No admin keys = no-op, so request passes through
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_no_key_returns_401() {
    let resp = admin_app(vec!["admin".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ===========================================================================
// Public endpoint exemptions
// ===========================================================================

#[tokio::test]
async fn health_endpoint_bypasses_auth() {
    let resp = auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn healthz_live_bypasses_auth() {
    let resp = auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/healthz/live")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn healthz_ready_bypasses_auth() {
    let resp = auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/healthz/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn unsubscribe_endpoint_bypasses_auth() {
    let resp = auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/unsubscribe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Edge cases: malformed / edge-case keys
// ===========================================================================

#[tokio::test]
async fn empty_x_api_key_returns_401() {
    let resp = auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn very_long_key_does_not_panic() {
    let long_key = "x".repeat(100_000);
    let resp = auth_app(vec!["correct".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", &long_key)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Should gracefully return 401, not panic
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn prefix_of_valid_key_is_rejected() {
    let resp = auth_app(vec!["secret-full-key".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn suffix_of_valid_key_is_rejected() {
    let resp = auth_app(vec!["prefix-secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ===========================================================================
// Multi-tenant isolation
// ===========================================================================

#[tokio::test]
async fn multitenant_valid_key_with_tenant_passes() {
    let key = "tenant-key-1";
    let hash = hash_api_key(key);
    let mut tenant_map = HashMap::new();
    tenant_map.insert(hash, "tenant-abc".to_string());

    let resp = multitenant_app(vec![key.into()], tenant_map)
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", key)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn multitenant_valid_key_without_tenant_mapping_returns_403() {
    // Key is valid but not in tenant_map → 403 (not associated with a tenant)
    let key = "valid-key-no-tenant";
    let resp = multitenant_app(vec![key.into()], HashMap::new())
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", key)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// ===========================================================================
// Hash and constant-time comparison properties
// ===========================================================================

#[test]
fn hash_api_key_is_deterministic() {
    assert_eq!(hash_api_key("key"), hash_api_key("key"));
}

#[test]
fn hash_api_key_produces_64_hex_chars() {
    let hash = hash_api_key("any-key");
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn hash_api_key_different_inputs_produce_different_hashes() {
    assert_ne!(hash_api_key("key-a"), hash_api_key("key-b"));
}

#[test]
fn hash_api_key_handles_empty_string() {
    let hash = hash_api_key("");
    assert_eq!(hash.len(), 64); // SHA-256 of empty string is still 64 hex chars
}

#[test]
fn hash_api_key_handles_very_long_input() {
    let long = "a".repeat(1_000_000);
    let hash = hash_api_key(&long);
    assert_eq!(hash.len(), 64); // Must not panic
}

#[test]
fn key_matches_any_returns_false_for_empty_candidates() {
    assert!(!key_matches_any("key", &[]));
}

#[test]
fn key_matches_any_returns_true_for_exact_match() {
    let candidates = vec!["secret".to_string()];
    assert!(key_matches_any("secret", &candidates));
}

#[test]
fn key_matches_any_returns_false_for_non_match() {
    let candidates = vec!["secret".to_string()];
    assert!(!key_matches_any("wrong", &candidates));
}

#[test]
fn key_matches_any_returns_false_for_partial_match() {
    let candidates = vec!["secret-full".to_string()];
    assert!(!key_matches_any("secret", &candidates));
}

#[test]
fn key_matches_any_finds_match_in_middle_of_list() {
    let candidates = vec!["a".into(), "b".into(), "target".into(), "c".into()];
    assert!(key_matches_any("target", &candidates));
}

#[test]
fn key_matches_any_with_different_lengths_does_not_panic() {
    // Ensures constant-time comparison doesn't crash on different-length inputs
    let candidates = vec!["short".to_string()];
    assert!(!key_matches_any("this-is-a-much-longer-key", &candidates));

    let candidates = vec!["this-is-a-much-longer-key".to_string()];
    assert!(!key_matches_any("short", &candidates));
}

#[test]
fn same_visual_unicode_different_bytes_do_not_match() {
    // "café" with precomposed é vs decomposed e + combining accent
    let precomposed = "caf\u{00e9}"; // é as single codepoint
    let decomposed = "cafe\u{0301}"; // e + combining acute accent

    // These look the same visually but are different byte sequences
    assert_ne!(precomposed, decomposed, "Rust string equality is byte-level");

    let candidates = vec![precomposed.to_string()];
    assert!(
        !key_matches_any(decomposed, &candidates),
        "Unicode-normalized visually-identical keys must NOT match"
    );
}

#[test]
fn null_byte_key_does_not_match_non_null_key() {
    let candidates = vec!["valid".to_string()];
    let null_key = "valid\x00extra";
    assert!(!key_matches_any(null_key, &candidates));
}
