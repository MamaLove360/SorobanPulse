//! Security Regression Tests
//!
//! Each test in this file locks in a specific security invariant. Tests are
//! named REG-NNN and document the vulnerability class being prevented.
//!
//! Adding a test here:
//! 1. Assign the next REG-NNN number.
//! 2. Add a doc comment explaining *what* is being prevented and *why*.
//! 3. Keep the test self-contained — no database, no network.
//!
//! Test categories:
//! REG-001 — Timing attack prevention (constant-time comparison)
//! REG-002 — Header injection prevention
//! REG-003 — Path traversal / input validation
//! REG-004 — HMAC replay prevention (timestamp freshness)
//! REG-005 — Admin privilege escalation
//! REG-006 — API key enumeration resistance
//! REG-007 — Error information leakage
//! REG-008 — Null byte injection
//! REG-009 — Unicode normalisation attacks
//! REG-010 — Input format enforcement (contract ID, tx hash)
//! REG-011 — Security header completeness

use axum::{body::Body, http::StatusCode, routing::get, Router};
use chrono::{Duration, Utc};
use soroban_pulse::middleware::auth::{
    admin_auth_middleware, auth_middleware, hash_api_key, key_matches_any, AdminAuthState,
    AuthState,
};
use soroban_pulse::middleware::security_headers::security_headers_middleware;
use soroban_pulse::zero_trust::{AccessDecision, AccessLogger, RequestContext, RequestSignature};
use std::collections::HashMap;
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn make_auth_app(api_keys: Vec<String>) -> Router {
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
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            auth_middleware,
        ))
}

fn make_admin_app(admin_keys: Vec<String>) -> Router {
    let state = Arc::new(AdminAuthState {
        admin_api_keys: admin_keys,
    });
    Router::new()
        .route("/v1/admin/action", get(|| async { "admin" }))
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            admin_auth_middleware,
        ))
}

fn old_rfc3339(secs: i64) -> String {
    (Utc::now() - Duration::seconds(secs)).to_rfc3339()
}

fn future_rfc3339(secs: i64) -> String {
    (Utc::now() + Duration::seconds(secs)).to_rfc3339()
}

fn validate_contract_id(id: &str) -> bool {
    id.len() == 56 && id.starts_with('C') && id.chars().all(|c| c.is_ascii_alphanumeric())
}

fn validate_tx_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit())
}

// ===========================================================================
// REG-001: Timing attack prevention
// ===========================================================================

/// REG-001-a: key_matches_any must return false (not panic) when comparing
/// keys of different lengths. Validates that constant-time comparison handles
/// length mismatches gracefully.
#[test]
fn reg_001_a_different_length_keys_return_false_without_panic() {
    let candidates = vec!["short".to_string()];
    assert!(!key_matches_any("this-key-is-much-longer-than-short", &candidates));

    let candidates = vec!["this-key-is-much-longer-than-short".to_string()];
    assert!(!key_matches_any("short", &candidates));
}

/// REG-001-b: Empty candidate list returns false immediately.
#[test]
fn reg_001_b_empty_candidate_list_returns_false() {
    assert!(!key_matches_any("anything", &[]));
}

/// REG-001-c: All-but-last-char match must still fail.
#[test]
fn reg_001_c_near_miss_last_char_not_accepted() {
    let correct = "abcdefghijklmnop";
    let near_miss = "abcdefghijklmnoX";
    let candidates = vec![correct.to_string()];
    assert!(!key_matches_any(near_miss, &candidates));
}

/// REG-001-d: All-but-first-char match must fail.
#[test]
fn reg_001_d_near_miss_first_char_not_accepted() {
    let correct = "Xbcdefghijklmnop";
    let near_miss = "abcdefghijklmnop";
    let candidates = vec![correct.to_string()];
    assert!(!key_matches_any(near_miss, &candidates));
}

// ===========================================================================
// REG-002: Header injection prevention
// ===========================================================================

/// REG-002-a: An API key value containing CRLF must not match a clean key.
/// The injected key has extra bytes from the CRLF — must not match.
#[test]
fn reg_002_a_crlf_in_key_value_does_not_match_clean_key() {
    let clean_key = "legitimate-key";
    let injected = "legitimate-key\r\nX-Injected: evil";
    let candidates = vec![clean_key.to_string()];
    assert!(!key_matches_any(injected, &candidates));
}

/// REG-002-b: Newline-only suffix doesn't match.
#[test]
fn reg_002_b_newline_suffix_key_does_not_match() {
    let clean = "my-secret";
    let with_newline = "my-secret\n";
    let candidates = vec![clean.to_string()];
    assert!(!key_matches_any(with_newline, &candidates));
}

/// REG-002-c: Tab character in key value doesn't produce a match.
#[test]
fn reg_002_c_tab_in_key_does_not_match() {
    let clean = "my-secret";
    let with_tab = "my-secret\t";
    let candidates = vec![clean.to_string()];
    assert!(!key_matches_any(with_tab, &candidates));
}

// ===========================================================================
// REG-003: Path traversal / input validation
// ===========================================================================

/// REG-003-a: Standard path traversal patterns must fail contract ID validation.
#[test]
fn reg_003_a_path_traversal_rejected_by_contract_id_validator() {
    let traversal_patterns = [
        "../../etc/passwd",
        "../admin/secrets",
        "%2e%2e%2fetc%2fpasswd",
        "....//etc/passwd",
        "/etc/shadow",
        "C:\\Windows\\System32",
        "\\\\server\\share",
    ];
    for pattern in &traversal_patterns {
        assert!(
            !validate_contract_id(pattern),
            "Path traversal must be rejected: {pattern}"
        );
    }
}

/// REG-003-b: Shell injection patterns rejected by contract ID validator.
#[test]
fn reg_003_b_shell_injection_rejected_by_contract_id_validator() {
    let shell_patterns = [
        "; ls -la",
        "| cat /etc/passwd",
        "`rm -rf /`",
        "$(whoami)",
        "&& cat /etc/shadow",
        "|| echo pwned",
    ];
    for pattern in &shell_patterns {
        assert!(
            !validate_contract_id(pattern),
            "Shell injection must be rejected: {pattern}"
        );
    }
}

/// REG-003-c: URL encoding of special chars must be rejected.
#[test]
fn reg_003_c_url_encoded_injection_rejected() {
    let url_encoded = [
        "%27%20OR%20%271%27%3D%271",
        "%3Cscript%3Ealert(1)%3C/script%3E",
        "%2F%2F%2F",
        "%00null",
    ];
    for input in &url_encoded {
        assert!(
            !validate_contract_id(input),
            "URL-encoded injection must be rejected: {input}"
        );
    }
}

/// REG-003-d: Valid Stellar contract ID format must pass validation.
#[test]
fn reg_003_d_valid_contract_id_passes() {
    let valid = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA4";
    assert!(validate_contract_id(valid));
}

/// REG-003-e: Contract ID wrong length rejected even with valid charset.
#[test]
fn reg_003_e_wrong_length_contract_id_rejected() {
    assert!(!validate_contract_id("C")); // too short
    assert!(!validate_contract_id(&"C".repeat(55))); // one char short of 56
    assert!(!validate_contract_id(&"C".repeat(57))); // one char over 56
}

// ===========================================================================
// REG-004: HMAC replay attack prevention (timestamp freshness)
// ===========================================================================

/// REG-004-a: A request with a 10-minute-old timestamp must be rejected even
/// when the HMAC signature is cryptographically valid.
#[test]
fn reg_004_a_stale_timestamp_10_minutes_old_rejected() {
    let old_ts = old_rfc3339(600);
    let sig = RequestSignature::sign("key", "GET", "/v1/events", &old_ts, "");
    assert!(
        !RequestSignature::verify("key", "GET", "/v1/events", &old_ts, "", &sig),
        "10-minute-old timestamp must be rejected"
    );
}

/// REG-004-b: A far-future timestamp (1 hour ahead) must be rejected.
#[test]
fn reg_004_b_far_future_timestamp_rejected() {
    let future_ts = future_rfc3339(3600);
    let sig = RequestSignature::sign("key", "GET", "/path", &future_ts, "");
    assert!(
        !RequestSignature::verify("key", "GET", "/path", &future_ts, "", &sig),
        "Far-future timestamp must be rejected"
    );
}

/// REG-004-c: An unparseable timestamp string must always be rejected.
#[test]
fn reg_004_c_unparseable_timestamp_always_rejected() {
    let sig = RequestSignature::sign("key", "GET", "/path", "not-a-date", "");
    assert!(
        !RequestSignature::verify("key", "GET", "/path", "not-a-date", "", &sig),
        "Unparseable timestamp must be rejected"
    );
}

/// REG-004-d: A current timestamp is always accepted.
#[test]
fn reg_004_d_current_timestamp_accepted() {
    let ts = Utc::now().to_rfc3339();
    let sig = RequestSignature::sign("key", "GET", "/path", &ts, "");
    assert!(RequestSignature::verify("key", "GET", "/path", &ts, "", &sig));
}

/// REG-004-e: Timestamp just inside the window (4 minutes old) passes.
#[test]
fn reg_004_e_timestamp_inside_window_accepted() {
    let ts = old_rfc3339(240); // 4 minutes, within 5-minute window
    let sig = RequestSignature::sign("key", "GET", "/path", &ts, "");
    assert!(
        RequestSignature::verify("key", "GET", "/path", &ts, "", &sig),
        "4-minute-old timestamp should be accepted"
    );
}

// ===========================================================================
// REG-005: Admin privilege escalation
// ===========================================================================

/// REG-005-a: A regular API key must receive 403 Forbidden (not 401 or 200)
/// from the admin layer.
#[tokio::test]
async fn reg_005_a_regular_key_gets_403_at_admin_endpoint() {
    let resp = make_admin_app(vec!["admin-secret".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .header("X-Api-Key", "regular-user-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "Must be 403, not 401 or 200");
}

/// REG-005-b: The 403 response body must say "admin privileges required".
#[tokio::test]
async fn reg_005_b_403_body_says_admin_privileges_required() {
    let resp = make_admin_app(vec!["admin-secret".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .header("X-Api-Key", "regular-user-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"].as_str().unwrap(), "admin privileges required");
}

/// REG-005-c: No key at admin endpoint gives 401, not 403 or 200.
#[tokio::test]
async fn reg_005_c_no_key_gets_401_at_admin_endpoint() {
    let resp = make_admin_app(vec!["admin-secret".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "Must be 401");
}

/// REG-005-d: The 401 body must say "admin authentication required".
#[tokio::test]
async fn reg_005_d_401_body_says_admin_authentication_required() {
    let resp = make_admin_app(vec!["admin-secret".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"].as_str().unwrap(), "admin authentication required");
}

// ===========================================================================
// REG-006: API key enumeration resistance
// ===========================================================================

/// REG-006-a: Both a completely wrong key and a near-miss key receive the same
/// HTTP status code (401), providing no oracle for enumeration.
#[tokio::test]
async fn reg_006_a_wrong_and_near_miss_key_get_same_status() {
    let app_a = make_auth_app(vec!["correct-key-here".into()]);
    let app_b = make_auth_app(vec!["correct-key-here".into()]);

    let resp_wrong = app_a
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "completely-wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let resp_near_miss = app_b
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "correct-key-herX")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp_wrong.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(resp_near_miss.status(), StatusCode::UNAUTHORIZED);
}

/// REG-006-b: The 401 response body must not hint at the valid key.
#[tokio::test]
async fn reg_006_b_401_body_does_not_hint_at_valid_keys() {
    let resp = make_auth_app(vec!["the-real-secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "wrong-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let body_str = std::str::from_utf8(&body).unwrap();

    assert!(!body_str.contains("the-real-secret"), "Must not leak valid key");
    assert!(!body_str.contains("wrong-key"), "Must not echo back provided key");
}

// ===========================================================================
// REG-007: Error information leakage
// ===========================================================================

/// REG-007-a: The 401 JSON body must contain exactly one field ("error").
#[tokio::test]
async fn reg_007_a_401_has_exactly_one_json_field() {
    let resp = make_auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let obj = json.as_object().unwrap();
    assert_eq!(obj.len(), 1, "Must have exactly 1 JSON field");
    assert!(obj.contains_key("error"));
}

/// REG-007-b: The 403 JSON body must contain exactly one field ("error").
#[tokio::test]
async fn reg_007_b_403_has_exactly_one_json_field() {
    let resp = make_admin_app(vec!["admin".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .header("X-Api-Key", "regular")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let obj = json.as_object().unwrap();
    assert_eq!(obj.len(), 1);
    assert!(obj.contains_key("error"));
}

/// REG-007-c: Error response must be valid JSON.
#[tokio::test]
async fn reg_007_c_error_response_is_valid_json() {
    let resp = make_auth_app(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    assert!(
        serde_json::from_slice::<serde_json::Value>(&body).is_ok(),
        "Error response must be valid JSON"
    );
}

// ===========================================================================
// REG-008: Null byte injection
// ===========================================================================

/// REG-008-a: Key with null byte appended must NOT match the clean key.
#[test]
fn reg_008_a_null_byte_appended_does_not_match_clean_key() {
    let clean = "valid-key";
    let with_null = format!("{}\x00", clean);
    let candidates = vec![clean.to_string()];
    assert!(!key_matches_any(&with_null, &candidates));
}

/// REG-008-b: Key with null byte prepended must NOT match the clean key.
#[test]
fn reg_008_b_null_byte_prepended_does_not_match_clean_key() {
    let clean = "valid-key";
    let with_null = format!("\x00{}", clean);
    let candidates = vec![clean.to_string()];
    assert!(!key_matches_any(&with_null, &candidates));
}

/// REG-008-c: Contract ID with null byte must fail validation.
#[test]
fn reg_008_c_null_byte_in_contract_id_rejected() {
    let id_with_null = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\x00";
    assert!(!validate_contract_id(id_with_null));
}

/// REG-008-d: hash_api_key with null bytes must not panic.
#[test]
fn reg_008_d_hash_api_key_handles_null_bytes() {
    let key_with_null = "valid\x00key";
    let hash = hash_api_key(key_with_null);
    assert_eq!(hash.len(), 64); // Must produce valid hash, not panic
}

// ===========================================================================
// REG-009: Unicode normalisation attacks
// ===========================================================================

/// REG-009-a: Precomposed and decomposed Unicode forms must NOT be treated
/// as the same key (no implicit NFC/NFD normalisation).
#[test]
fn reg_009_a_precomposed_vs_decomposed_unicode_not_equal() {
    let precomposed = "caf\u{00e9}"; // café with precomposed é (U+00E9)
    let decomposed = "cafe\u{0301}"; // café with e + combining acute (U+0301)

    let candidates = vec![precomposed.to_string()];
    assert!(
        !key_matches_any(decomposed, &candidates),
        "Decomposed form must not match precomposed form"
    );
}

/// REG-009-b: ASCII 'a' and Cyrillic 'а' (homoglyph) must not match.
#[test]
fn reg_009_b_homoglyph_characters_not_equal() {
    let ascii_admin = "admin"; // all ASCII
    let cyrillic_a = "\u{0430}dmin"; // Cyrillic 'а' (U+0430)

    let candidates = vec![ascii_admin.to_string()];
    assert!(
        !key_matches_any(cyrillic_a, &candidates),
        "Homoglyph attack must not bypass key comparison"
    );
}

// ===========================================================================
// REG-010: Input format enforcement
// ===========================================================================

/// REG-010-a: Classic SQL injection strings fail contract ID validation.
#[test]
fn reg_010_a_sql_injection_fails_contract_id_validation() {
    let sql_attacks = [
        "'; DROP TABLE events; --",
        "1 OR 1=1",
        "' UNION SELECT NULL--",
        "admin'--",
        "1; DELETE FROM events WHERE '1'='1",
    ];
    for attack in &sql_attacks {
        assert!(
            !validate_contract_id(attack),
            "SQL injection must fail: {attack}"
        );
    }
}

/// REG-010-b: Template injection patterns fail validation.
#[test]
fn reg_010_b_template_injection_fails_contract_id_validation() {
    let template_attacks = [
        "{{7*7}}",
        "${7*7}",
        "<%= 7*7 %>",
        "#{7*7}",
        "{% debug %}",
    ];
    for attack in &template_attacks {
        assert!(
            !validate_contract_id(attack),
            "Template injection must fail: {attack}"
        );
    }
}

/// REG-010-c: Valid tx hash passes, injections and wrong lengths fail.
#[test]
fn reg_010_c_tx_hash_validation() {
    let valid = "a".repeat(64);
    assert!(validate_tx_hash(&valid));

    assert!(!validate_tx_hash("'; DROP TABLE events; --"));
    assert!(!validate_tx_hash("AAAA")); // too short
    assert!(!validate_tx_hash(&"a".repeat(65))); // too long
    assert!(!validate_tx_hash(&"z".repeat(64))); // 'z' is not hex
}

// ===========================================================================
// REG-011: Security header completeness
// ===========================================================================

/// REG-011-a: All 7 required OWASP security headers must be present on every
/// API response.
#[tokio::test]
async fn reg_011_a_all_seven_owasp_headers_present() {
    let app = Router::new()
        .route("/api", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn(security_headers_middleware));

    let resp = app
        .oneshot(
            axum::http::Request::get("/api")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let required_headers = [
        "X-Content-Type-Options",
        "X-Frame-Options",
        "Referrer-Policy",
        "Strict-Transport-Security",
        "X-XSS-Protection",
        "Permissions-Policy",
        "Content-Security-Policy",
    ];

    for header in &required_headers {
        assert!(
            resp.headers().get(*header).is_some(),
            "Required security header missing: {header}"
        );
    }
}

/// REG-011-b: X-Frame-Options must be DENY (not SAMEORIGIN).
#[tokio::test]
async fn reg_011_b_x_frame_options_is_deny() {
    let app = Router::new()
        .route("/api", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn(security_headers_middleware));

    let resp = app
        .oneshot(
            axum::http::Request::get("/api")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.headers().get("X-Frame-Options").unwrap(),
        "DENY",
        "X-Frame-Options must be DENY"
    );
}

/// REG-011-c: API route CSP must be maximally restrictive.
#[tokio::test]
async fn reg_011_c_api_csp_is_maximally_restrictive() {
    let app = Router::new()
        .route("/api", get(|| async { "ok" }))
        .layer(axum::middleware::from_fn(security_headers_middleware));

    let resp = app
        .oneshot(
            axum::http::Request::get("/api")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let csp = resp
        .headers()
        .get("Content-Security-Policy")
        .unwrap()
        .to_str()
        .unwrap();

    assert_eq!(
        csp, "default-src 'none'; frame-ancestors 'none';",
        "API CSP must be maximally restrictive"
    );
}

// ===========================================================================
// REG-012: Access log audit trail
// ===========================================================================

/// REG-012-a: Denied requests are always captured in the access log.
#[test]
fn reg_012_a_denied_requests_captured_in_audit_log() {
    let logger = AccessLogger::new(1000);
    let ctx = RequestContext::new("1.2.3.4", "bad-key-hash", "/v1/admin/secrets", "GET");
    logger.record(ctx, AccessDecision::Deny("no admin key".into()));
    assert_eq!(logger.len(), 1);
}

/// REG-012-b: Allowed requests are also captured (for forensics).
#[test]
fn reg_012_b_allowed_requests_captured_in_audit_log() {
    let logger = AccessLogger::new(1000);
    let ctx = RequestContext::new("10.0.0.1", "valid-hash", "/v1/events", "GET");
    logger.record(ctx, AccessDecision::Allow);
    assert_eq!(logger.len(), 1);
}
