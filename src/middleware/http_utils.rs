//! HTTP utility middleware — Issue #663
//!
//! Contains:
//! - [`head_middleware`] — transparent HEAD → GET conversion (Issue #422).
//! - [`cache_middleware`] — route-aware `Cache-Control` / `ETag` injection.

use axum::{extract::Request, middleware::Next, response::Response};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// HEAD middleware
// ---------------------------------------------------------------------------

/// Transparently handles HTTP HEAD requests (Issue #422).
///
/// Converts HEAD to GET internally, runs the handler, then discards the
/// response body while preserving all headers (including `Content-Length` and
/// `ETag`).
pub async fn head_middleware(req: Request, next: Next) -> Response {
    use axum::http::Method;

    if req.method() != Method::HEAD {
        return next.run(req).await;
    }

    let (mut parts, body) = req.into_parts();
    parts.method = Method::GET;
    let get_req = Request::from_parts(parts, body);
    let response = next.run(get_req).await;

    let (mut resp_parts, resp_body) = response.into_parts();
    let body_bytes = axum::body::to_bytes(resp_body, usize::MAX)
        .await
        .unwrap_or_default();

    resp_parts.headers.insert(
        axum::http::header::CONTENT_LENGTH,
        axum::http::HeaderValue::from(body_bytes.len()),
    );

    Response::from_parts(resp_parts, axum::body::Body::empty())
}

// ---------------------------------------------------------------------------
// Cache middleware
// ---------------------------------------------------------------------------

/// Adds route-aware `Cache-Control` and `ETag` headers.
///
/// | Path pattern                  | Cache-Control                              |
/// |-------------------------------|--------------------------------------------|
/// | `/v1/events/tx/:hash`         | `public, max-age=3600, immutable`          |
/// | `/v1/events` (with to_ledger) | `public, max-age=60`                       |
/// | `/v1/events` (no filter)      | `public, max-age=5, stale-while-revalidate=10` |
/// | `/v1/events/contract/:id`     | `public, max-age=5, stale-while-revalidate=10` |
/// | everything else               | no caching                                 |
pub async fn cache_middleware(req: Request, next: Next) -> Response {
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();

    let response = next.run(req).await;

    let cache_control = if path.ends_with("/tx/")
        || (path.contains("/tx/") && !path.contains('?'))
    {
        "public, max-age=3600, immutable"
    } else if path == "/v1/events" || path == "/events" {
        if query.contains("to_ledger") {
            "public, max-age=60"
        } else {
            "public, max-age=5, stale-while-revalidate=10"
        }
    } else if path.contains("/contract/") {
        "public, max-age=5, stale-while-revalidate=10"
    } else {
        return response;
    };

    let (mut parts, body) = response.into_parts();

    parts.headers.insert(
        "Cache-Control",
        axum::http::HeaderValue::from_static(cache_control),
    );

    if let Ok(body_bytes) = axum::body::to_bytes(body, usize::MAX).await {
        let mut hasher = Sha256::new();
        hasher.update(&body_bytes);
        let hash = format!("{:x}", hasher.finalize());
        let etag = format!("\"{}\"", &hash[..16]);

        parts.headers.insert(
            "ETag",
            etag.parse()
                .unwrap_or_else(|_| axum::http::HeaderValue::from_static("\"unknown\"")),
        );

        Response::from_parts(parts, axum::body::Body::from(body_bytes))
    } else {
        Response::from_parts(parts, axum::body::Body::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::StatusCode, routing::get, Router};
    use tower::ServiceExt;

    #[tokio::test]
    async fn non_head_request_passes_through() {
        let app = Router::new()
            .route("/test", get(|| async { "body content" }))
            .layer(axum::middleware::from_fn(head_middleware));

        let resp = app
            .oneshot(axum::http::Request::get("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(bytes, "body content");
    }

    #[tokio::test]
    async fn head_request_returns_empty_body_with_content_length() {
        let app = Router::new()
            .route("/test", get(|| async { "body content" }))
            .layer(axum::middleware::from_fn(head_middleware));

        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .method("HEAD")
                    .uri("/test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        // Content-Length should match "body content" byte length (12).
        assert_eq!(
            resp.headers()
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap(),
            "12"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn events_route_gets_cache_headers() {
        let app = Router::new()
            .route("/v1/events", get(|| async { "[]" }))
            .layer(axum::middleware::from_fn(cache_middleware));

        let resp = app
            .oneshot(
                axum::http::Request::get("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let cc = resp
            .headers()
            .get("Cache-Control")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(cc.contains("max-age=5"));
        assert!(resp.headers().contains_key("ETag"));
    }

    #[tokio::test]
    async fn unmatched_route_gets_no_cache_headers() {
        let app = Router::new()
            .route("/other", get(|| async { "data" }))
            .layer(axum::middleware::from_fn(cache_middleware));

        let resp = app
            .oneshot(
                axum::http::Request::get("/other")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(resp.headers().get("Cache-Control").is_none());
    }
}
