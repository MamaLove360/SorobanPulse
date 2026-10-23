//! Response-processing middleware — Issue #989.
//!
//! Contains middleware functions extracted from `routes.rs`:
//! - [`deprecation_middleware`] — adds `Deprecation`, `Sunset`, and `Link` headers.
//! - [`rate_limit_reject_middleware`] — rewrites 429 responses to standard JSON.
//! - [`slow_request_middleware`] — logs requests exceeding a duration threshold.

use axum::{
    body::Body,
    extract::MatchedPath,
    http::{HeaderValue, Request},
    middleware::Next,
    response::Response,
};
use std::time::Instant;

/// Middleware that adds deprecation headers to responses for
/// unversioned deprecated API aliases.
pub async fn deprecation_middleware(req: Request<Body>, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let mut resp = next.run(req).await;
    resp.headers_mut()
        .insert("Deprecation", HeaderValue::from_static("true"));
    resp.headers_mut().insert(
        "Sunset",
        HeaderValue::from_static("Sat, 24 Oct 2026 00:00:00 GMT"),
    );
    let versioned_path = format!("/v1{}", path);
    let link_value = format!("<{}>; rel=\"successor-version\"", versioned_path);
    resp.headers_mut().insert(
        "Link",
        HeaderValue::from_str(&link_value).unwrap_or_else(|_| {
            HeaderValue::from_static("</v1/events>; rel=\"successor-version\"")
        }),
    );
    resp
}

/// Middleware that rewrites 429 Too Many Requests responses to the
/// standard JSON ErrorResponse format.
pub async fn rate_limit_reject_middleware(req: Request<Body>, next: Next) -> Response {
    let resp = next.run(req).await;
    if resp.status() == axum::http::StatusCode::TOO_MANY_REQUESTS {
        crate::metrics::record_rate_limit_rejected();
        return rate_limit_json_response(resp);
    }
    resp
}

/// Rewrite a 429 Too Many Requests response to the standard JSON ErrorResponse
/// format. Preserves all rate-limit headers from the original.
fn rate_limit_json_response(original: Response<Body>) -> Response<Body> {
    use axum::http::header;
    let correlation_id = crate::error::get_request_id();
    let body = serde_json::json!({
        "error": "rate limit exceeded",
        "code": "RATE_LIMIT_EXCEEDED",
        "correlation_id": correlation_id,
    });
    let json_bytes = body.to_string();
    let mut builder = axum::response::Response::builder()
        .status(axum::http::StatusCode::TOO_MANY_REQUESTS)
        .header(header::CONTENT_TYPE, "application/json");
    for (name, value) in original.headers() {
        if name != header::CONTENT_TYPE {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(Body::from(json_bytes))
        .unwrap_or_else(|_| {
            axum::response::IntoResponse::into_response(axum::http::StatusCode::TOO_MANY_REQUESTS)
        })
}

/// Middleware that logs slow requests exceeding a configurable threshold.
pub fn slow_request_middleware(
    slow_request_threshold_ms: u64,
) -> impl Fn(Request<Body>, Next) -> _ {
    move |req: Request<Body>, next: Next| async move {
        let method = req.method().as_str().to_string();
        let route = req
            .extensions()
            .get::<MatchedPath>()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "<unknown>".to_string());
        let request_id = req
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_owned();
        let start = Instant::now();
        let response = next.run(req).await;
        let duration = start.elapsed();
        let status = response.status().as_u16().to_string();
        crate::metrics::record_http_request_duration(duration, &method, &route, &status);
        if duration.as_millis() as u64 > slow_request_threshold_ms {
            tracing::warn!(
                method = %method,
                path = %route,
                status = %status,
                duration_ms = duration.as_millis(),
                request_id = %request_id,
                "slow request"
            );
        }
        response
    }
}

/// Middleware that wraps [`security_headers_middleware_with_config`]
/// with a pre-loaded config.
pub fn security_headers_with_config(
    config: super::security_headers::SecurityHeadersConfig,
) -> impl Fn(Request<Body>, Next) -> _ {
    move |req, next| {
        let config = config.clone();
        super::security_headers::security_headers_middleware_with_config(config, req, next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::StatusCode, routing::get, Router};
    use tower::ServiceExt;

    #[tokio::test]
    async fn deprecation_middleware_adds_headers() {
        let app = Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(deprecation_middleware));
        let resp = app
            .oneshot(axum::http::Request::get("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.headers().get("Deprecation").unwrap(), "true");
        assert_eq!(
            resp.headers().get("Sunset").unwrap(),
            "Sat, 24 Oct 2026 00:00:00 GMT"
        );
    }

    #[tokio::test]
    async fn rate_limit_reject_middleware_passes_through() {
        let app = Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(rate_limit_reject_middleware));
        let resp = app
            .oneshot(axum::http::Request::get("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn slow_request_middleware_passes_through() {
        let app = Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(slow_request_middleware(10_000)));
        let resp = app
            .oneshot(axum::http::Request::get("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
