//! Contract tests for `GET /v1/meta` (issue #1259).
//!
//! Asserts the response schema served by the real handler, and that it agrees
//! with the `MetaResponse` component published in the OpenAPI document that
//! the SDKs are generated from.

use axum::{body::Body, http::Request, http::StatusCode, routing::get, Router};
use serde_json::Value;
use soroban_pulse::meta::{get_meta, MetaResponse};
use soroban_pulse::routes::ApiDoc;
use tower::ServiceExt;
use utoipa::OpenApi;

const FIELDS: [&str; 4] = ["features", "version", "build_sha", "api_version"];

async fn fetch_meta() -> (StatusCode, Value) {
    let app = Router::new().route("/v1/meta", get(get_meta));
    let resp = app
        .oneshot(Request::get("/v1/meta").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn meta_response_matches_contract() {
    let (status, body) = fetch_meta().await;
    assert_eq!(status, StatusCode::OK);

    let obj = body.as_object().expect("response must be a JSON object");
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let mut expected = FIELDS.to_vec();
    expected.sort_unstable();
    assert_eq!(keys, expected, "unexpected field set: {body}");

    let features = body["features"].as_array().expect("features must be an array");
    assert!(features.iter().all(|f| f.as_str().is_some_and(|s| !s.is_empty())));
    assert!(body["version"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(body["build_sha"].as_str().is_some_and(|s| !s.is_empty()));
    assert_eq!(body["api_version"], "v1");

    // Round-trips into the typed struct the SDKs mirror.
    let typed: MetaResponse = serde_json::from_value(body).unwrap();
    assert_eq!(typed, MetaResponse::current());
}

#[tokio::test]
async fn meta_reports_compiled_features() {
    let (_, body) = fetch_meta().await;
    let features: Vec<&str> = body["features"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();

    // Checked for both the default and `--all-features` builds.
    for (name, enabled) in [
        ("default", cfg!(feature = "default")),
        ("dashboard", cfg!(feature = "dashboard")),
        ("classical-crypto", cfg!(feature = "classical-crypto")),
        ("quantum-crypto", cfg!(feature = "quantum-crypto")),
        ("kafka", cfg!(feature = "kafka")),
        ("sqs", cfg!(feature = "sqs")),
        ("pubsub", cfg!(feature = "pubsub")),
    ] {
        assert_eq!(features.contains(&name), enabled, "feature `{name}`");
    }
}

#[test]
fn openapi_documents_meta_endpoint_and_schema() {
    let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();

    let op = &doc["paths"]["/v1/meta"]["get"];
    assert!(op.is_object(), "GET /v1/meta missing from OpenAPI");
    assert!(op["security"].is_null() || op["security"] == serde_json::json!([]));

    let schema = &doc["components"]["schemas"]["MetaResponse"];
    let mut props: Vec<&str> = schema["properties"]
        .as_object()
        .expect("MetaResponse schema must have properties")
        .keys()
        .map(String::as_str)
        .collect();
    props.sort_unstable();
    let mut expected = FIELDS.to_vec();
    expected.sort_unstable();
    assert_eq!(props, expected);

    let mut required: Vec<&str> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    required.sort_unstable();
    assert_eq!(required, expected, "all MetaResponse fields are required");
}
