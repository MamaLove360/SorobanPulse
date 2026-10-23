//! Integration tests for all `/v1/admin/*` endpoints.
//!
//! These tests spin up a real Axum test server backed by a test database
//! (see `tests/test_helpers.rs`) and exercise each admin endpoint with:
//!   * a happy-path request (expects 200),
//!   * a request with no API key (expects 401),
//!   * a request with a wrong API key (expects 403),
//!   * a request with a malformed body (expects 422).
//!
//! Run with: `cargo test --test admin_tests`

mod test_helpers;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use test_helpers::{build_test_app, VALID_ADMIN_KEY, WRONG_ADMIN_KEY};

/// Helper: build a request with an optional API key header and a JSON body.
fn json_request(
    method: Method,
    uri: &str,
    api_key: Option<&str>,
    body: Option<serde_json::Value>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);

    if let Some(key) = api_key {
        builder = builder.header("x-api-key", key);
    }

    match body {
        Some(value) => builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(value.to_string()))
            .expect("failed to build request"),
        None => builder
            .body(Body::empty())
            .expect("failed to build request"),
    }
}

/// Helper: send a request through the app and return the status code.
async fn send(app: axum::Router, req: Request<Body>) -> StatusCode {
    let response = app.oneshot(req).await.expect("request failed");
    response.status()
}

// ---------------------------------------------------------------------------
// Pause indexer: POST /v1/admin/indexer/pause
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pause_indexer_happy_path() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/indexer/pause",
        Some(VALID_ADMIN_KEY),
        None,
    );
    assert_eq!(send(app, req).await, StatusCode::OK);
}

#[tokio::test]
async fn pause_indexer_unauthorized_without_key() {
    let app = build_test_app().await;
    let req = json_request(Method::POST, "/v1/admin/indexer/pause", None, None);
    assert_eq!(send(app, req).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn pause_indexer_forbidden_with_wrong_key() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/indexer/pause",
        Some(WRONG_ADMIN_KEY),
        None,
    );
    assert_eq!(send(app, req).await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn pause_indexer_unprocessable_with_bad_body() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/indexer/pause",
        Some(VALID_ADMIN_KEY),
        Some(json!({ "reason": 12345 })),
    );
    assert_eq!(send(app, req).await, StatusCode::UNPROCESSABLE_ENTITY);
}

// ---------------------------------------------------------------------------
// Resume indexer: POST /v1/admin/indexer/resume
// ---------------------------------------------------------------------------

#[tokio::test]
async fn resume_indexer_happy_path() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/indexer/resume",
        Some(VALID_ADMIN_KEY),
        None,
    );
    assert_eq!(send(app, req).await, StatusCode::OK);
}

#[tokio::test]
async fn resume_indexer_unauthorized_without_key() {
    let app = build_test_app().await;
    let req = json_request(Method::POST, "/v1/admin/indexer/resume", None, None);
    assert_eq!(send(app, req).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn resume_indexer_forbidden_with_wrong_key() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/indexer/resume",
        Some(WRONG_ADMIN_KEY),
        None,
    );
    assert_eq!(send(app, req).await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn resume_indexer_unprocessable_with_bad_body() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/indexer/resume",
        Some(VALID_ADMIN_KEY),
        Some(json!({ "reason": 12345 })),
    );
    assert_eq!(send(app, req).await, StatusCode::UNPROCESSABLE_ENTITY);
}

// ---------------------------------------------------------------------------
// Replay: POST /v1/admin/replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_happy_path() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/replay",
        Some(VALID_ADMIN_KEY),
        Some(json!({ "from_ledger": 1, "to_ledger": 10 })),
    );
    assert_eq!(send(app, req).await, StatusCode::OK);
}

#[tokio::test]
async fn replay_unauthorized_without_key() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/replay",
        None,
        Some(json!({ "from_ledger": 1, "to_ledger": 10 })),
    );
    assert_eq!(send(app, req).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn replay_forbidden_with_wrong_key() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/replay",
        Some(WRONG_ADMIN_KEY),
        Some(json!({ "from_ledger": 1, "to_ledger": 10 })),
    );
    assert_eq!(send(app, req).await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn replay_unprocessable_with_bad_body() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/replay",
        Some(VALID_ADMIN_KEY),
        Some(json!({ "from_ledger": "not-a-number", "to_ledger": [] })),
    );
    assert_eq!(send(app, req).await, StatusCode::UNPROCESSABLE_ENTITY);
}

// ---------------------------------------------------------------------------
// Anonymize: POST /v1/admin/anonymize
// ---------------------------------------------------------------------------

#[tokio::test]
async fn anonymize_happy_path() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/anonymize",
        Some(VALID_ADMIN_KEY),
        Some(json!({ "account": "GABC123" })),
    );
    assert_eq!(send(app, req).await, StatusCode::OK);
}

#[tokio::test]
async fn anonymize_unauthorized_without_key() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/anonymize",
        None,
        Some(json!({ "account": "GABC123" })),
    );
    assert_eq!(send(app, req).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn anonymize_forbidden_with_wrong_key() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/anonymize",
        Some(WRONG_ADMIN_KEY),
        Some(json!({ "account": "GABC123" })),
    );
    assert_eq!(send(app, req).await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn anonymize_unprocessable_with_bad_body() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/anonymize",
        Some(VALID_ADMIN_KEY),
        Some(json!({ "account": 42 })),
    );
    assert_eq!(send(app, req).await, StatusCode::UNPROCESSABLE_ENTITY);
}

// ---------------------------------------------------------------------------
// Schema management: POST /v1/admin/schema
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schema_management_happy_path() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/schema",
        Some(VALID_ADMIN_KEY),
        Some(json!({ "action": "migrate" })),
    );
    assert_eq!(send(app, req).await, StatusCode::OK);
}

#[tokio::test]
async fn schema_management_unauthorized_without_key() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/schema",
        None,
        Some(json!({ "action": "migrate" })),
    );
    assert_eq!(send(app, req).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn schema_management_forbidden_with_wrong_key() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/schema",
        Some(WRONG_ADMIN_KEY),
        Some(json!({ "action": "migrate" })),
    );
    assert_eq!(send(app, req).await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn schema_management_unprocessable_with_bad_body() {
    let app = build_test_app().await;
    let req = json_request(
        Method::POST,
        "/v1/admin/schema",
        Some(VALID_ADMIN_KEY),
        Some(json!({ "action": 12345 })),
    );
    assert_eq!(send(app, req).await, StatusCode::UNPROCESSABLE_ENTITY);
}