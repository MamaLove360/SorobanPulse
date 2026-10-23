use axum::{
    body::Body,
    extract::Request,
    http::{header, HeaderValue, Response},
    middleware::Next,
};

/// Strict CSP for API routes: no scripts, styles, or other resources allowed.
const API_CSP: &str = "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'";

/// CSP for the Swagger UI served at `/docs`.
/// Allows the Swagger UI CDN (unpkg.com) plus inline scripts/styles that the
/// Swagger UI bundle relies on to render.
const SWAGGER_CSP: &str = "default-src 'self'; \
    script-src 'self' 'unsafe-inline' https://unpkg.com; \
    style-src 'self' 'unsafe-inline' https://unpkg.com; \
    img-src 'self' data: https://unpkg.com; \
    font-src 'self' data: https://unpkg.com; \
    connect-src 'self'; \
    frame-ancestors 'none'; \
    base-uri 'self'; \
    form-action 'self'";

const HSTS: &str = "max-age=31536000; includeSubDomains";
const X_CONTENT_TYPE_OPTIONS: &str = "nosniff";

/// Returns true when the request path targets the Swagger UI.
fn is_swagger_ui_path(path: &str) -> bool {
    path == "/docs" || path.starts_with("/docs/")
}

/// Global security headers middleware.
///
/// Applies to every response, including `GET /docs`. The CSP differs between
/// the Swagger UI (which needs the CDN and inline assets) and API routes
/// (which get a strict `default-src 'none'` policy).
pub async fn security_headers_middleware(req: Request, next: Next) -> Response<Body> {
    let is_swagger = is_swagger_ui_path(req.uri().path());

    let mut response = next.run(req).await;
    let headers = response.headers_mut();

    let csp = if is_swagger { SWAGGER_CSP } else { API_CSP };
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(csp),
    );

    headers.insert(
        header::STRICT_TRANSPORT_SECURITY,
        HeaderValue::from_static(HSTS),
    );

    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static(X_CONTENT_TYPE_OPTIONS),
    );

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::get,
        Router,
    };
    use tower::ServiceExt;

    fn test_router() -> Router {
        Router::new()
            .route("/api/ping", get(|| async { "pong" }))
            .route("/docs", get(|| async { "swagger" }))
            .layer(middleware::from_fn(security_headers_middleware))
    }

    async fn get_headers(path: &str) -> axum::http::HeaderMap {
        let response = test_router()
            .oneshot(
                HttpRequest::builder()
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.headers().clone()
    }

    #[tokio::test]
    async fn api_route_has_strict_csp_and_security_headers() {
        let headers = get_headers("/api/ping").await;

        assert_eq!(
            headers.get(header::CONTENT_SECURITY_POLICY).unwrap(),
            API_CSP
        );
        assert_eq!(
            headers.get(header::STRICT_TRANSPORT_SECURITY).unwrap(),
            HSTS
        );
        assert_eq!(
            headers.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(),
            X_CONTENT_TYPE_OPTIONS
        );
    }

    #[tokio::test]
    async fn docs_route_has_swagger_csp_and_security_headers() {
        let headers = get_headers("/docs").await;

        let csp = headers
            .get(header::CONTENT_SECURITY_POLICY)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(csp.contains("https://unpkg.com"));
        assert!(csp.contains("'unsafe-inline'"));
        assert_eq!(
            headers.get(header::STRICT_TRANSPORT_SECURITY).unwrap(),
            HSTS
        );
        assert_eq!(
            headers.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(),
            X_CONTENT_TYPE_OPTIONS
        );
    }
}