//! Test helper utilities for resilience and integration tests.

use std::time::Duration;
use tokio::time::timeout;

/// Poll a condition with configurable timeout and interval.
///
/// Repeatedly checks the condition function until it returns true or the timeout is exceeded.
/// This is more reliable than fixed sleep-based waits on slow CI runners.
///
/// # Arguments
/// * `condition` - A function that returns true when the desired state is reached
/// * `timeout_duration` - Maximum time to wait for the condition
/// * `poll_interval` - How often to check the condition
///
/// # Returns
/// * `Ok(())` if the condition became true within the timeout
/// * `Err(String)` if the timeout was exceeded
///
/// # Example
/// ```ignore
/// wait_for(
///     || async { db.count_events().await.unwrap() > 0 },
///     Duration::from_secs(30),
///     Duration::from_millis(100),
/// ).await?;
/// ```
pub async fn wait_for<F, Fut>(
    mut condition: F,
    timeout_duration: Duration,
    poll_interval: Duration,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    timeout(timeout_duration, async {
        loop {
            if condition().await {
                return Ok(());
            }
            tokio::time::sleep(poll_interval).await;
        }
    })
    .await
    .map_err(|_| format!("Condition not met within {:?}", timeout_duration))?
}

// ---------------------------------------------------------------------------
// Admin integration test helpers
// ---------------------------------------------------------------------------

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use sqlx::PgPool;
use tower::ServiceExt;

/// Build an Axum test router wired to the given database pool and admin API key.
///
/// The returned router exposes the `/v1/admin/*` endpoints under test. It is
/// intended to be driven in-process via `tower::ServiceExt::oneshot`, so no
/// real TCP listener is required.
///
/// # Arguments
/// * `pool` - A `PgPool` connected to the test database
/// * `admin_api_key` - The API key the router should accept for admin routes
pub fn build_admin_test_router(pool: PgPool, admin_api_key: &str) -> Router {
    crate::build_router_with_admin_key(pool, admin_api_key.to_string())
}

/// Send a request to the router, optionally attaching an `Authorization` header.
///
/// # Arguments
/// * `router` - The router under test (cloned per call)
/// * `method` - HTTP method
/// * `uri` - Request URI (e.g. `/v1/admin/pause`)
/// * `api_key` - Optional bearer token to send in the `Authorization` header
/// * `body` - Optional JSON body
///
/// # Returns
/// The `StatusCode` produced by the router.
pub async fn send_admin_request(
    router: Router,
    method: Method,
    uri: &str,
    api_key: Option<&str>,
    body: Option<serde_json::Value>,
) -> StatusCode {
    let mut builder = Request::builder().method(method).uri(uri);

    if let Some(key) = api_key {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {}", key));
    }

    let body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&value).expect("serialize request body"))
        }
        None => Body::empty(),
    };

    let request = builder.body(body).expect("build request");

    let response = router
        .oneshot(request)
        .await
        .expect("router call should not fail");

    response.status()
}

/// Assert that a response status equals the expected status.
///
/// Panics with a descriptive message on mismatch, which keeps test failures
/// easy to diagnose.
pub fn assert_status(actual: StatusCode, expected: StatusCode) {
    assert_eq!(
        actual, expected,
        "expected status {}, got {}",
        expected, actual
    );
}