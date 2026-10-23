//! Middleware builder — Issue #663
//!
//! [`MiddlewareStack`] is a fluent builder that assembles the application's
//! middleware in a well-defined, documented order.
//!
//! ## Ordering (outer → inner)
//!
//! 1. **Security headers** — added to every response unconditionally.
//! 2. **Request ID** — must run before any code that emits error responses.
//! 3. **Tracing** — should run after the request ID is set.
//! 4. **Authentication** — gates protected routes.
//! 5. **Cache** — applied last so it can inspect the completed response.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use crate::middleware::builder::MiddlewareStack;
//! use std::sync::Arc;
//!
//! let stack = MiddlewareStack::new()
//!     .with_security_headers()
//!     .with_request_id()
//!     .with_tracing()
//!     .with_auth(auth_state)
//!     .with_cache();
//!
//! let router = stack.apply(my_router);
//! ```

use axum::Router;
use std::sync::Arc;

use super::{
    auth::{AdminAuthState, AuthState, admin_auth_middleware, auth_middleware},
    http_utils::{cache_middleware, head_middleware},
    request_id::request_id_middleware,
    security_headers::security_headers_middleware,
    tracing::tracing_middleware,
};

/// Fluent builder for assembling the middleware stack onto an Axum router.
///
/// Each `with_*` method records that the corresponding middleware should be
/// applied when [`MiddlewareStack::apply`] is called.  The methods can be
/// called in any order — the builder always applies middleware in the correct
/// documented order.
pub struct MiddlewareStack {
    security_headers: bool,
    request_id: bool,
    tracing: bool,
    head: bool,
    cache: bool,
    auth_state: Option<Arc<AuthState>>,
    admin_auth_state: Option<Arc<AdminAuthState>>,
}

impl Default for MiddlewareStack {
    fn default() -> Self {
        Self::new()
    }
}

impl MiddlewareStack {
    /// Create a new empty builder with no middleware selected.
    pub fn new() -> Self {
        Self {
            security_headers: false,
            request_id: false,
            tracing: false,
            head: false,
            cache: false,
            auth_state: None,
            admin_auth_state: None,
        }
    }

    /// Enable OWASP security headers on all responses.
    #[must_use]
    pub fn with_security_headers(mut self) -> Self {
        self.security_headers = true;
        self
    }

    /// Enable request-ID extraction from `X-Request-ID` headers.
    #[must_use]
    pub fn with_request_id(mut self) -> Self {
        self.request_id = true;
        self
    }

    /// Enable distributed-tracing header extraction.
    #[must_use]
    pub fn with_tracing(mut self) -> Self {
        self.tracing = true;
        self
    }

    /// Enable transparent HEAD-to-GET conversion.
    #[must_use]
    pub fn with_head(mut self) -> Self {
        self.head = true;
        self
    }

    /// Enable route-aware Cache-Control / ETag injection.
    #[must_use]
    pub fn with_cache(mut self) -> Self {
        self.cache = true;
        self
    }

    /// Enable API key authentication with the given shared state.
    #[must_use]
    pub fn with_auth(mut self, state: Arc<AuthState>) -> Self {
        self.auth_state = Some(state);
        self
    }

    /// Enable admin-endpoint authentication with the given shared state.
    #[must_use]
    pub fn with_admin_auth(mut self, state: Arc<AdminAuthState>) -> Self {
        self.admin_auth_state = Some(state);
        self
    }

    /// Apply all selected middleware to `router` and return the wrapped router.
    ///
    /// Middleware is applied in the documented order regardless of the order
    /// in which the `with_*` methods were called.
    pub fn apply(self, mut router: Router) -> Router {
        // Note: Axum applies `layer`/`route_layer` calls in reverse — the last
        // `.layer()` call is the outermost (first to execute) layer.  We add
        // layers in reverse priority order so that the first one listed in the
        // docs (security_headers) ends up outermost.

        // 5. Cache (innermost — closest to the handler)
        if self.cache {
            router = router.layer(axum::middleware::from_fn(cache_middleware));
        }

        // 4b. Admin auth
        if let Some(state) = self.admin_auth_state {
            router = router.layer(axum::middleware::from_fn_with_state(
                state,
                admin_auth_middleware,
            ));
        }

        // 4a. Regular auth
        if let Some(state) = self.auth_state {
            router =
                router.layer(axum::middleware::from_fn_with_state(state, auth_middleware));
        }

        // 3. Tracing
        if self.tracing {
            router = router.layer(axum::middleware::from_fn(tracing_middleware));
        }

        // 2b. HEAD conversion
        if self.head {
            router = router.layer(axum::middleware::from_fn(head_middleware));
        }

        // 2a. Request ID
        if self.request_id {
            router = router.layer(axum::middleware::from_fn(request_id_middleware));
        }

        // 1. Security headers (outermost)
        if self.security_headers {
            router = router.layer(axum::middleware::from_fn(security_headers_middleware));
        }

        router
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::StatusCode, routing::get};
    use tower::ServiceExt;

    #[tokio::test]
    async fn empty_stack_does_not_panic() {
        let app = MiddlewareStack::new()
            .apply(Router::new().route("/", get(|| async { "ok" })));

        let resp = app
            .oneshot(axum::http::Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn security_headers_applied_via_builder() {
        let app = MiddlewareStack::new()
            .with_security_headers()
            .apply(Router::new().route("/", get(|| async { "ok" })));

        let resp = app
            .oneshot(axum::http::Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(resp.headers().contains_key("X-Content-Type-Options"));
    }

    #[tokio::test]
    async fn request_id_applied_via_builder() {
        let app = MiddlewareStack::new()
            .with_request_id()
            .apply(Router::new().route("/", get(|| async { "ok" })));

        let resp = app
            .oneshot(
                axum::http::Request::get("/")
                    .header("x-request-id", "abc-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cache_applied_to_events_route() {
        let app = MiddlewareStack::new()
            .with_cache()
            .apply(Router::new().route("/v1/events", get(|| async { "[]" })));

        let resp = app
            .oneshot(
                axum::http::Request::get("/v1/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.headers().contains_key("Cache-Control"));
    }

    #[tokio::test]
    async fn auth_applied_via_builder_rejects_missing_key() {
        let state = Arc::new(AuthState {
            api_keys: vec!["k".into()],
            admin_api_keys: vec![],
            tenant_map: Arc::new(std::collections::HashMap::new()),
            multi_tenant: false,
            pool: None,
            require_auth: false,
        });

        let app = MiddlewareStack::new()
            .with_auth(state)
            .apply(Router::new().route("/secret", get(|| async { "data" })));

        let resp = app
            .oneshot(
                axum::http::Request::get("/secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn builder_is_default_constructable() {
        let _: MiddlewareStack = MiddlewareStack::default();
    }
}
