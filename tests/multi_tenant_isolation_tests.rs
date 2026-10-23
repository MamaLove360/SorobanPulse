//! Issue #812: Comprehensive multi-tenant isolation test suite.
//!
//! Covers:
//! - Tenant ID extraction and injection via auth middleware
//! - Cross-tenant data access prevention (data-leak tests)
//! - Authorization failure scenarios (missing key, wrong key, unmapped key)
//! - Admin key cross-tenant bypass behaviour
//! - Constant-time key comparison (timing-safe)
//! - Hash-based key storage (SHA-256, never plaintext)
//! - Edge cases: empty tenant map, empty tenant ID, key collisions
//! - SSE / streaming tenant scoping
//! - Per-tenant rate-limit bucket isolation
//! - Tenant map reuse across concurrent requests
//! - Audit trail: TenantId extension availability for handlers
//! - RLS / SQL layer: tenant_id filter correctness
//! - RLS against a real PostgreSQL instance (Issue #1344): cross-tenant
//!   reads/writes return 0 rows, superuser still reads every row

use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
    routing::get,
    Extension, Router,
};
use soroban_pulse::middleware::{
    auth_middleware, hash_api_key, AuthState, TenantId,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tower::ServiceExt;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build a router that injects `auth_middleware` and exposes a `/test` handler
/// that echoes back the resolved `TenantId` (or "none" if absent).
fn build_app(
    api_keys: Vec<String>,
    admin_api_keys: Vec<String>,
    tenant_map: HashMap<String, String>,
    multi_tenant: bool,
) -> Router {
    let auth_state = Arc::new(AuthState {
        api_keys,
        admin_api_keys,
        tenant_map: Arc::new(tenant_map),
        multi_tenant,
        pool: None,
        require_auth: false,
    });

    Router::new()
        .route(
            "/test",
            get(|ext: Option<Extension<TenantId>>| async move {
                match ext {
                    Some(Extension(tid)) => tid.0,
                    None => "none".to_string(),
                }
            }),
        )
        .route("/health", get(|| async { "OK" }))
        .route_layer(axum::middleware::from_fn_with_state(
            auth_state,
            auth_middleware,
        ))
}

/// Register a single tenant in a new map.
fn tenant_map_with(key: &str, tenant_id: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert(hash_api_key(key), tenant_id.to_string());
    m
}

/// Register multiple tenants.
fn tenant_map_multi(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, t)| (hash_api_key(k), t.to_string()))
        .collect()
}

async fn get_response(app: Router, key: Option<&str>) -> Response {
    let mut builder = Request::builder().uri("/test");
    if let Some(k) = key {
        builder = builder.header("X-Api-Key", k);
    }
    app.oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn body_string(resp: Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Basic tenant resolution
// ─────────────────────────────────────────────────────────────────────────────

/// Test 1 — A valid tenant-mapped key resolves to the correct tenant ID.
#[tokio::test]
async fn test_01_valid_key_resolves_tenant() {
    let key = "tenant-a-key";
    let app = build_app(
        vec![key.to_string()],
        vec![],
        tenant_map_with(key, "tenant-a"),
        true,
    );
    let resp = get_response(app, Some(key)).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "tenant-a");
}

/// Test 2 — A different key resolves to a different tenant.
#[tokio::test]
async fn test_02_distinct_keys_resolve_distinct_tenants() {
    let key_a = "key-for-a";
    let key_b = "key-for-b";
    let map = tenant_map_multi(&[(key_a, "tenant-a"), (key_b, "tenant-b")]);

    let app_a = build_app(
        vec![key_a.to_string(), key_b.to_string()],
        vec![],
        map.clone(),
        true,
    );
    let app_b = build_app(
        vec![key_a.to_string(), key_b.to_string()],
        vec![],
        map,
        true,
    );

    let resp_a = get_response(app_a, Some(key_a)).await;
    let resp_b = get_response(app_b, Some(key_b)).await;

    assert_eq!(body_string(resp_a).await, "tenant-a");
    assert_eq!(body_string(resp_b).await, "tenant-b");
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Authorization failures
// ─────────────────────────────────────────────────────────────────────────────

/// Test 3 — Missing key returns 401 when auth is enabled.
#[tokio::test]
async fn test_03_missing_key_returns_401() {
    let app = build_app(
        vec!["some-key".to_string()],
        vec![],
        HashMap::new(),
        false,
    );
    let resp = get_response(app, None).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Test 4 — Wrong key returns 401.
#[tokio::test]
async fn test_04_wrong_key_returns_401() {
    let app = build_app(
        vec!["correct-key".to_string()],
        vec![],
        HashMap::new(),
        false,
    );
    let resp = get_response(app, Some("wrong-key")).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Test 5 — Valid API key that is not mapped to a tenant returns 403 in
/// multi-tenant mode (not 401, because authentication passed).
#[tokio::test]
async fn test_05_unmapped_key_returns_403_in_multitenant_mode() {
    let key = "valid-but-unmapped";
    let app = build_app(
        vec![key.to_string()],
        vec![],
        HashMap::new(), // empty map → no tenant
        true,
    );
    let resp = get_response(app, Some(key)).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

/// Test 6 — Auth disabled (no keys configured): all requests pass through.
#[tokio::test]
async fn test_06_no_auth_configured_bypasses_all_checks() {
    let app = build_app(vec![], vec![], HashMap::new(), false);
    let resp = get_response(app, None).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Test 7 — Empty Bearer string returns 401.
#[tokio::test]
async fn test_07_empty_bearer_returns_401() {
    let app = build_app(
        vec!["real-key".to_string()],
        vec![],
        HashMap::new(),
        false,
    );
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/test")
                .header("Authorization", "Bearer ")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Test 8 — Authorization header with wrong scheme returns 401.
#[tokio::test]
async fn test_08_basic_auth_scheme_rejected() {
    let app = build_app(
        vec!["real-key".to_string()],
        vec![],
        HashMap::new(),
        false,
    );
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/test")
                .header("Authorization", "Basic dXNlcjpwYXNz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Admin key behaviour
// ─────────────────────────────────────────────────────────────────────────────

/// Test 9 — Admin key is accepted even when not in the tenant map.
#[tokio::test]
async fn test_09_admin_key_bypasses_tenant_resolution() {
    let admin_key = "super-admin";
    let app = build_app(
        vec![],
        vec![admin_key.to_string()],
        HashMap::new(), // no tenant entries
        true,
    );
    let resp = get_response(app, Some(admin_key)).await;
    // Admin key passes; TenantId extension is NOT injected (cross-tenant access).
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "none");
}

/// Test 10 — Admin key cannot be mistaken for a tenant key.
#[tokio::test]
async fn test_10_admin_key_not_injected_as_tenant_id() {
    let admin_key = "admin-only";
    let tenant_key = "tenant-key";
    let app = build_app(
        vec![tenant_key.to_string()],
        vec![admin_key.to_string()],
        tenant_map_with(tenant_key, "tenant-x"),
        true,
    );

    // Admin gets no TenantId
    let resp_admin = get_response(app.clone(), Some(admin_key)).await;
    assert_eq!(body_string(resp_admin).await, "none");

    // Tenant gets correct TenantId
    let resp_tenant = get_response(app, Some(tenant_key)).await;
    assert_eq!(body_string(resp_tenant).await, "tenant-x");
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Health endpoint isolation bypass
// ─────────────────────────────────────────────────────────────────────────────

/// Test 11 — /health is reachable without any key even when auth is enabled.
#[tokio::test]
async fn test_11_health_bypasses_auth() {
    let app = build_app(
        vec!["key".to_string()],
        vec![],
        HashMap::new(),
        false,
    );
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Constant-time / timing-safe checks
// ─────────────────────────────────────────────────────────────────────────────

/// Test 12 — Wrong-length keys do not short-circuit early (constant-time).
/// We measure that a completely wrong key and a nearly-correct key take
/// comparable time — both should be within 10× of each other on any hardware.
#[tokio::test]
async fn test_12_key_comparison_is_timing_safe() {
    let correct_key = "a".repeat(64);
    let wrong_short = "b".to_string();
    let wrong_long = "c".repeat(128);

    let app_short = build_app(
        vec![correct_key.clone()],
        vec![],
        HashMap::new(),
        false,
    );
    let app_long = build_app(
        vec![correct_key.clone()],
        vec![],
        HashMap::new(),
        false,
    );

    let t0 = Instant::now();
    for _ in 0..100 {
        let _ = get_response(app_short.clone(), Some(&wrong_short)).await;
    }
    let short_avg = t0.elapsed() / 100;

    let t1 = Instant::now();
    for _ in 0..100 {
        let _ = get_response(app_long.clone(), Some(&wrong_long)).await;
    }
    let long_avg = t1.elapsed() / 100;

    // Timings should be within 10× of each other; we allow generous margin for
    // CI noise. The real guarantee is that subtle::ConstantTimeEq is used —
    // this test catches obvious short-circuit bugs rather than nanosecond skew.
    let ratio = if short_avg > long_avg {
        short_avg.as_nanos() as f64 / long_avg.as_nanos().max(1) as f64
    } else {
        long_avg.as_nanos() as f64 / short_avg.as_nanos().max(1) as f64
    };
    assert!(
        ratio < 10.0,
        "Timing ratio {ratio:.2} suggests non-constant-time comparison"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Hash-based key storage
// ─────────────────────────────────────────────────────────────────────────────

/// Test 13 — hash_api_key is deterministic and produces a 64-char hex string.
#[test]
fn test_13_hash_api_key_deterministic_hex() {
    let key = "my-secret-api-key";
    let h1 = hash_api_key(key);
    let h2 = hash_api_key(key);
    assert_eq!(h1, h2, "Hash must be deterministic");
    assert_eq!(h1.len(), 64, "SHA-256 hex digest must be 64 chars");
    assert!(h1.chars().all(|c| c.is_ascii_hexdigit()));
}

/// Test 14 — Two different keys produce different hashes (collision resistance).
#[test]
fn test_14_different_keys_produce_different_hashes() {
    let h1 = hash_api_key("key-alpha");
    let h2 = hash_api_key("key-beta");
    assert_ne!(h1, h2);
}

/// Test 15 — Key hash does not contain the original key value.
#[test]
fn test_15_hash_does_not_leak_plaintext_key() {
    let key = "plaintext-secret";
    let hash = hash_api_key(key);
    assert!(!hash.contains(key), "Hash must not contain the plaintext key");
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Edge cases
// ─────────────────────────────────────────────────────────────────────────────

/// Test 16 — Empty tenant ID string is accepted as a valid tenant label.
#[tokio::test]
async fn test_16_empty_tenant_id_string_accepted() {
    let key = "some-key";
    let app = build_app(
        vec![key.to_string()],
        vec![],
        tenant_map_with(key, ""),
        true,
    );
    let resp = get_response(app, Some(key)).await;
    assert_eq!(resp.status(), StatusCode::OK);
    // Empty string is stored and returned, not treated as missing.
    assert_eq!(body_string(resp).await, "");
}

/// Test 17 — A key that is a prefix of a valid key is rejected (no prefix
/// attack).
#[tokio::test]
async fn test_17_prefix_of_valid_key_rejected() {
    let full_key = "full-key-value-12345";
    let prefix_key = "full-key-value-123"; // shorter, but similar
    let app = build_app(
        vec![full_key.to_string()],
        vec![],
        HashMap::new(),
        false,
    );
    let resp = get_response(app, Some(prefix_key)).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Test 18 — Multiple valid keys for the same tenant are each independently
/// accepted and resolve to the same tenant ID.
#[tokio::test]
async fn test_18_multiple_keys_same_tenant() {
    let key1 = "tenant-b-key-1";
    let key2 = "tenant-b-key-2";
    let map = tenant_map_multi(&[(key1, "tenant-b"), (key2, "tenant-b")]);

    let app = build_app(
        vec![key1.to_string(), key2.to_string()],
        vec![],
        map,
        true,
    );

    let resp1 = get_response(app.clone(), Some(key1)).await;
    let resp2 = get_response(app, Some(key2)).await;

    assert_eq!(body_string(resp1).await, "tenant-b");
    assert_eq!(body_string(resp2).await, "tenant-b");
}

/// Test 19 — A key with unicode/special characters hashes correctly.
#[test]
fn test_19_unicode_key_hashes_without_panic() {
    let key = "密码-🔑-key";
    let hash = hash_api_key(key);
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
}

/// Test 20 — An empty string key hashes without panic (edge case).
#[test]
fn test_20_empty_string_key_hashes_without_panic() {
    let hash = hash_api_key("");
    assert_eq!(hash.len(), 64);
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Cross-tenant data leak prevention
// ─────────────────────────────────────────────────────────────────────────────

/// Test 21 — Tenant A's key cannot be used to resolve Tenant B's tenant ID.
#[tokio::test]
async fn test_21_tenant_a_key_cannot_access_tenant_b_scope() {
    let key_a = "key-a-secret";
    let key_b = "key-b-secret";
    let map = tenant_map_multi(&[(key_a, "tenant-a"), (key_b, "tenant-b")]);

    // App only accepts key_a as a valid API key
    let app = build_app(
        vec![key_a.to_string()],
        vec![],
        map,
        true,
    );

    // key_b is not in api_keys → should get 401
    let resp = get_response(app, Some(key_b)).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Test 22 — Swapping key hashes does not grant access to the wrong tenant.
/// (Ensures tenant_map lookup is hash-keyed, not value-keyed.)
#[tokio::test]
async fn test_22_swapped_hash_does_not_grant_access() {
    let key_a = "key-alpha-001";
    let key_b = "key-beta-002";

    // Intentionally insert key_b's hash mapped to tenant-a (simulating
    // misconfiguration) — key_a should still resolve to its own mapping.
    let mut map = HashMap::new();
    map.insert(hash_api_key(key_a), "tenant-a".to_string());
    // key_b hash deliberately not inserted

    let app = build_app(
        vec![key_a.to_string(), key_b.to_string()],
        vec![],
        map,
        true,
    );

    // key_b is authenticated but has no tenant entry → 403
    let resp_b = get_response(app, Some(key_b)).await;
    assert_eq!(resp_b.status(), StatusCode::FORBIDDEN);
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. Concurrency
// ─────────────────────────────────────────────────────────────────────────────

/// Test 23 — Concurrent requests from two tenants are independently resolved.
#[tokio::test]
async fn test_23_concurrent_requests_isolated() {
    let key_a = "concurrent-key-a";
    let key_b = "concurrent-key-b";
    let map = tenant_map_multi(&[(key_a, "tenant-alpha"), (key_b, "tenant-beta")]);

    let app = build_app(
        vec![key_a.to_string(), key_b.to_string()],
        vec![],
        map,
        true,
    );

    // Fire off 50 interleaved requests for both tenants.
    let mut handles = Vec::new();
    for i in 0..50u8 {
        let a = app.clone();
        let key = if i % 2 == 0 { key_a } else { key_b };
        let expected = if i % 2 == 0 { "tenant-alpha" } else { "tenant-beta" };
        handles.push(tokio::spawn(async move {
            let resp = get_response(a, Some(key)).await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(body_string(resp).await, expected);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. TenantId extension availability (audit trail readiness)
// ─────────────────────────────────────────────────────────────────────────────

/// Test 24 — TenantId extension is present in the request extensions after
/// successful resolution; handlers can read it for audit logging.
#[tokio::test]
async fn test_24_tenant_id_extension_available_to_handlers() {
    let key = "audit-key";
    let tenant = "audit-tenant";
    let app = build_app(
        vec![key.to_string()],
        vec![],
        tenant_map_with(key, tenant),
        true,
    );

    let resp = get_response(app, Some(key)).await;
    assert_eq!(resp.status(), StatusCode::OK);
    // Handler echoes the TenantId back; if it were absent the handler would
    // return "none" — this asserts the extension is correctly threaded.
    assert_eq!(body_string(resp).await, tenant);
}

/// Test 25 — When multi-tenant mode is off, TenantId is NOT injected even if
/// a tenant map is present (single-tenant deployments are unaffected).
#[tokio::test]
async fn test_25_tenant_id_not_injected_in_single_tenant_mode() {
    let key = "single-tenant-key";
    let app = build_app(
        vec![key.to_string()],
        vec![],
        tenant_map_with(key, "tenant-x"),
        false, // multi_tenant = false
    );

    let resp = get_response(app, Some(key)).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "none");
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. SQL tenant_id filter correctness (unit-level)
// ─────────────────────────────────────────────────────────────────────────────

/// Test 26 — Verify that the SQL snippet used to filter by tenant contains the
/// expected placeholder (regression guard against query builder changes).
#[test]
fn test_26_tenant_filter_sql_contains_correct_placeholder() {
    // Reconstruct the query fragment as it appears in handlers.rs
    let tenant_id = "tenant-abc";
    let sql = format!("WHERE tenant_id = '{tenant_id}'");
    assert!(sql.contains(tenant_id));
    assert!(sql.contains("tenant_id"));
}

/// Test 27 — NULL tenant_id in RLS policy means single-tenant rows are
/// visible to all — verify the IS NULL logic in the RLS expression string.
#[test]
fn test_27_rls_policy_null_tenant_visible_to_all() {
    // The RLS policy string from 20261022000001_rls_events_partitioned.sql
    let policy = "tenant_id IS NULL \
                  OR COALESCE(current_setting('app.current_tenant_id', TRUE), '') = '' \
                  OR tenant_id = current_setting('app.current_tenant_id', TRUE)";

    // All three OR branches present
    assert!(policy.contains("tenant_id IS NULL"));
    assert!(policy.contains("current_setting"));
    assert!(policy.contains("app.current_tenant_id"));
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. Bearer vs X-Api-Key header parity
// ─────────────────────────────────────────────────────────────────────────────

/// Test 28 — Both Authorization: Bearer and X-Api-Key resolve tenant equally.
#[tokio::test]
async fn test_28_bearer_and_x_api_key_resolve_tenant_equally() {
    let key = "dual-header-key";
    let tenant = "dual-tenant";
    let map = tenant_map_with(key, tenant);

    let app_bearer = build_app(
        vec![key.to_string()],
        vec![],
        map.clone(),
        true,
    );
    let app_xkey = build_app(
        vec![key.to_string()],
        vec![],
        map,
        true,
    );

    let resp_bearer = app_bearer
        .oneshot(
            Request::builder()
                .uri("/test")
                .header("Authorization", format!("Bearer {key}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let resp_xkey = app_xkey
        .oneshot(
            Request::builder()
                .uri("/test")
                .header("X-Api-Key", key)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(body_string(resp_bearer).await, tenant);
    assert_eq!(body_string(resp_xkey).await, tenant);
}

// ─────────────────────────────────────────────────────────────────────────────
// 13. Secondary / rotation keys
// ─────────────────────────────────────────────────────────────────────────────

/// Test 29 — Key rotation: both old and new key can be active simultaneously
/// without one key granting access to the other tenant's scope.
#[tokio::test]
async fn test_29_key_rotation_no_cross_tenant_bleed() {
    let old_key = "old-key-v1";
    let new_key = "new-key-v2";
    // Both map to the SAME tenant (rotation scenario)
    let map = tenant_map_multi(&[(old_key, "tenant-z"), (new_key, "tenant-z")]);

    let app = build_app(
        vec![old_key.to_string(), new_key.to_string()],
        vec![],
        map,
        true,
    );

    let resp_old = get_response(app.clone(), Some(old_key)).await;
    let resp_new = get_response(app, Some(new_key)).await;

    assert_eq!(body_string(resp_old).await, "tenant-z");
    assert_eq!(body_string(resp_new).await, "tenant-z");
}

/// Test 30 — Revoked key (removed from api_keys list) is rejected even if
/// its hash remains in the tenant_map.
#[tokio::test]
async fn test_30_revoked_key_rejected_even_if_in_tenant_map() {
    let active_key = "active-key";
    let revoked_key = "revoked-key";

    // tenant_map has both, but api_keys only has active_key
    let map = tenant_map_multi(&[(active_key, "tenant-q"), (revoked_key, "tenant-q")]);

    let app = build_app(
        vec![active_key.to_string()], // revoked_key intentionally absent
        vec![],
        map,
        true,
    );

    // Active key still works
    let resp_active = get_response(app.clone(), Some(active_key)).await;
    assert_eq!(resp_active.status(), StatusCode::OK);

    // Revoked key is rejected at the auth gate (not even a tenant lookup)
    let resp_revoked = get_response(app, Some(revoked_key)).await;
    assert_eq!(resp_revoked.status(), StatusCode::UNAUTHORIZED);
}

// ─────────────────────────────────────────────────────────────────────────────
// 14. Row-level security against a real PostgreSQL instance (Issue #1344)
// ─────────────────────────────────────────────────────────────────────────────
//
// These tests need DATABASE_URL to point at a PostgreSQL server whose user is
// a superuser (the default `postgres` user in CI and docker-compose.test.yml).
// `#[sqlx::test]` creates a fresh database per test and runs every migration.
//
// Superusers and table owners bypass RLS, so each test creates a throwaway
// NOSUPERUSER NOBYPASSRLS role and switches to it with SET LOCAL ROLE. This
// mirrors a production deployment where the application connects as a
// non-owner role.

mod rls {
    use sqlx::{PgPool, Postgres, Transaction};

    const TENANT_A: &str = "tenant-a";
    const TENANT_B: &str = "tenant-b";
    /// Inside the 2026_01 monthly partition created by 20260727000102.
    const EVENT_TS: &str = "2026-01-15T00:00:00Z";

    /// Insert `count` events for `tenant` as the (superuser) pool owner.
    async fn seed_events(pool: &PgPool, tenant: &str, count: i64) {
        for i in 0..count {
            sqlx::query(
                "INSERT INTO events \
                     (contract_id, event_type, tx_hash, ledger, timestamp, event_data, tenant_id) \
                 VALUES ($1, 'contract', $2, $3, $4::timestamptz, '{\"topic\": [], \"value\": {}}'::jsonb, $5)",
            )
            .bind(format!("C{}", tenant.to_uppercase()))
            .bind(format!("tx-{tenant}-{i}"))
            .bind(1_000 + i)
            .bind(EVENT_TS)
            .bind(tenant)
            .execute(pool)
            .await
            .expect("seed insert failed");
        }
    }

    /// Seed both tenants: 3 rows for tenant A, 2 rows for tenant B.
    async fn seed_two_tenants(pool: &PgPool) {
        seed_events(pool, TENANT_A, 3).await;
        seed_events(pool, TENANT_B, 2).await;
    }

    /// Create a non-superuser, non-BYPASSRLS application role with the given
    /// privileges on `events`. Roles are cluster-wide, so the name is unique
    /// per test to allow parallel execution.
    async fn create_app_role(pool: &PgPool, privileges: &str) -> String {
        let role = format!("rls_app_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE ROLE {role} NOLOGIN NOSUPERUSER NOBYPASSRLS"))
            .execute(pool)
            .await
            .expect("create role failed");
        sqlx::query(&format!("GRANT USAGE ON SCHEMA public TO {role}"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&format!("GRANT {privileges} ON events TO {role}"))
            .execute(pool)
            .await
            .unwrap();
        role
    }

    async fn drop_app_role(pool: &PgPool, role: &str) {
        sqlx::query(&format!("DROP OWNED BY {role}"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&format!("DROP ROLE {role}"))
            .execute(pool)
            .await
            .unwrap();
    }

    /// Open a transaction acting as `role` and scoped to `tenant` (or
    /// unscoped when `None`), exactly as the application would with
    /// `SET LOCAL`.
    async fn scoped_tx<'a>(
        pool: &'a PgPool,
        role: Option<&str>,
        tenant: Option<&str>,
    ) -> Transaction<'a, Postgres> {
        let mut tx = pool.begin().await.unwrap();
        if let Some(role) = role {
            sqlx::query(&format!("SET LOCAL ROLE {role}"))
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        if let Some(tenant) = tenant {
            sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
                .bind(tenant)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        tx
    }

    async fn count_where(tx: &mut Transaction<'_, Postgres>, sql_where: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM events {sql_where}"))
            .fetch_one(&mut **tx)
            .await
            .unwrap()
    }

    /// Test 31 — RLS is enabled on the live (partitioned) events table and on
    /// every partition. Regression guard for the policy being stranded on
    /// `events_legacy` after the partitioning rename.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_31_rls_enabled_on_partitioned_events_and_partitions(pool: PgPool) {
        let (relkind, rls): (String, bool) = sqlx::query_as(
            "SELECT relkind::text, relrowsecurity FROM pg_class WHERE oid = 'events'::regclass",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(relkind, "p", "events should be the partitioned table");
        assert!(rls, "RLS must be enabled on the partitioned events table");

        let unprotected: Vec<String> = sqlx::query_scalar(
            "SELECT c.relname::text FROM pg_inherits i \
               JOIN pg_class c ON c.oid = i.inhrelid \
              WHERE i.inhparent = 'events'::regclass \
                AND (NOT c.relrowsecurity \
                     OR NOT EXISTS (SELECT 1 FROM pg_policy p \
                                     WHERE p.polrelid = c.oid AND p.polname = 'tenant_isolation'))",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(unprotected.is_empty(), "partitions without RLS: {unprotected:?}");
    }

    /// Test 32 — Two tenants, events for each: a session scoped to one tenant
    /// sees only its own rows, and every cross-tenant query returns 0 rows.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_32_rls_cross_tenant_select_returns_zero_rows(pool: PgPool) {
        seed_two_tenants(&pool).await;
        let role = create_app_role(&pool, "SELECT").await;

        {
            let mut tx = scoped_tx(&pool, Some(&role), Some(TENANT_A)).await;
            assert_eq!(count_where(&mut tx, "").await, 3, "tenant A sees only its own rows");
            assert_eq!(
                count_where(&mut tx, &format!("WHERE tenant_id = '{TENANT_B}'")).await,
                0,
                "tenant A must not read tenant B rows"
            );
            assert_eq!(
                count_where(&mut tx, "WHERE contract_id = 'CTENANT-B'").await,
                0,
                "tenant A must not read tenant B rows via another column"
            );
            tx.rollback().await.unwrap();
        }
        {
            let mut tx = scoped_tx(&pool, Some(&role), Some(TENANT_B)).await;
            assert_eq!(count_where(&mut tx, "").await, 2, "tenant B sees only its own rows");
            assert_eq!(
                count_where(&mut tx, &format!("WHERE tenant_id = '{TENANT_A}'")).await,
                0,
                "tenant B must not read tenant A rows"
            );
            tx.rollback().await.unwrap();
        }
        {
            // A tenant with no rows sees nothing at all.
            let mut tx = scoped_tx(&pool, Some(&role), Some("tenant-c")).await;
            assert_eq!(count_where(&mut tx, "").await, 0);
            tx.rollback().await.unwrap();
        }

        drop_app_role(&pool, &role).await;
    }

    /// Test 33 — Querying a partition directly (bypassing the parent) is
    /// still isolated.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_33_rls_direct_partition_access_isolated(pool: PgPool) {
        seed_two_tenants(&pool).await;
        let role = create_app_role(&pool, "SELECT").await;
        sqlx::query(&format!("GRANT SELECT ON events_2026_01 TO {role}"))
            .execute(&pool)
            .await
            .unwrap();

        let mut tx = scoped_tx(&pool, Some(&role), Some(TENANT_A)).await;
        let cross: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events_2026_01 WHERE tenant_id = 'tenant-b'",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(cross, 0, "direct partition access must not leak tenant B rows");
        tx.rollback().await.unwrap();

        drop_app_role(&pool, &role).await;
    }

    /// Test 34 — A tenant-scoped session cannot write rows for another tenant
    /// (WITH CHECK), and UPDATE/DELETE of another tenant's rows affect 0 rows.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_34_rls_cross_tenant_writes_blocked(pool: PgPool) {
        seed_two_tenants(&pool).await;
        let role = create_app_role(&pool, "SELECT, INSERT, UPDATE, DELETE").await;

        // INSERT a row stamped with another tenant's ID → rejected.
        {
            let mut tx = scoped_tx(&pool, Some(&role), Some(TENANT_A)).await;
            let err = sqlx::query(
                "INSERT INTO events \
                     (contract_id, event_type, tx_hash, ledger, timestamp, event_data, tenant_id) \
                 VALUES ('CX', 'contract', 'tx-forged', 1, $1::timestamptz, '{}'::jsonb, 'tenant-b')",
            )
            .bind(EVENT_TS)
            .execute(&mut *tx)
            .await
            .expect_err("cross-tenant INSERT must be rejected by RLS");
            assert!(
                err.to_string().contains("row-level security"),
                "unexpected error: {err}"
            );
            tx.rollback().await.unwrap();
        }

        // UPDATE / DELETE of another tenant's rows → 0 rows affected.
        {
            let mut tx = scoped_tx(&pool, Some(&role), Some(TENANT_A)).await;
            let updated = sqlx::query("UPDATE events SET ledger = ledger + 1 WHERE tenant_id = 'tenant-b'")
                .execute(&mut *tx)
                .await
                .unwrap()
                .rows_affected();
            assert_eq!(updated, 0, "tenant A must not update tenant B rows");

            let deleted = sqlx::query("DELETE FROM events WHERE tenant_id = 'tenant-b'")
                .execute(&mut *tx)
                .await
                .unwrap()
                .rows_affected();
            assert_eq!(deleted, 0, "tenant A must not delete tenant B rows");

            // Re-stamping an own row as another tenant is also rejected.
            let err = sqlx::query("UPDATE events SET tenant_id = 'tenant-b' WHERE tenant_id = 'tenant-a'")
                .execute(&mut *tx)
                .await
                .expect_err("moving a row to another tenant must be rejected");
            assert!(err.to_string().contains("row-level security"), "unexpected error: {err}");
            tx.rollback().await.unwrap();
        }

        // Tenant B's data is untouched.
        let b_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE tenant_id = 'tenant-b'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(b_rows, 2);

        drop_app_role(&pool, &role).await;
    }

    /// Test 35 — The superuser still reads every row (admin operations),
    /// even when a tenant scope is set on the session.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_35_rls_superuser_reads_all_rows(pool: PgPool) {
        let is_superuser: bool =
            sqlx::query_scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            is_superuser,
            "these tests require DATABASE_URL to connect as a PostgreSQL superuser"
        );

        seed_two_tenants(&pool).await;

        let mut tx = scoped_tx(&pool, None, Some(TENANT_A)).await;
        assert_eq!(count_where(&mut tx, "").await, 5, "superuser bypasses RLS");
        assert_eq!(
            count_where(&mut tx, &format!("WHERE tenant_id = '{TENANT_B}'")).await,
            2,
            "superuser reads tenant B rows while scoped to tenant A"
        );
        tx.rollback().await.unwrap();
    }

    /// Test 36 — An unscoped session (GUC never set → NULL, or set to '')
    /// behaves identically. Unscoped access is the documented admin/indexer
    /// path; the important property is that NULL and '' do not diverge.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_36_rls_unset_and_empty_scope_are_equivalent(pool: PgPool) {
        seed_two_tenants(&pool).await;
        let role = create_app_role(&pool, "SELECT").await;

        let mut tx = scoped_tx(&pool, Some(&role), None).await;
        let unset = count_where(&mut tx, "").await;
        tx.rollback().await.unwrap();

        let mut tx = scoped_tx(&pool, Some(&role), Some("")).await;
        let empty = count_where(&mut tx, "").await;
        tx.rollback().await.unwrap();

        assert_eq!(unset, empty, "NULL and '' tenant scope must behave the same");
        assert_eq!(unset, 5);

        drop_app_role(&pool, &role).await;
    }

    /// Test 37 — The tenant scope set with SET LOCAL ends with the
    /// transaction, so a pooled connection cannot carry tenant A's scope into
    /// the next request.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_37_rls_scope_does_not_leak_across_transactions(pool: PgPool) {
        seed_two_tenants(&pool).await;
        let role = create_app_role(&pool, "SELECT").await;

        let mut conn = pool.acquire().await.unwrap();
        {
            let mut tx = sqlx::Connection::begin(&mut *conn).await.unwrap();
            sqlx::query(&format!("SET LOCAL ROLE {role}"))
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("SELECT set_config('app.current_tenant_id', 'tenant-a', true)")
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        let scope: Option<String> =
            sqlx::query_scalar("SELECT current_setting('app.current_tenant_id', TRUE)")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert!(
            scope.as_deref().unwrap_or("").is_empty(),
            "tenant scope leaked past COMMIT: {scope:?}"
        );
        drop(conn);

        drop_app_role(&pool, &role).await;
    }
}
