//! Middleware module — Issue #663
//!
//! The monolithic middleware file has been refactored into focused sub-modules:
//!
//! | Sub-module          | Contents                                       |
//! |---------------------|------------------------------------------------|
//! | `request_id`        | `request_id_middleware`                        |
//! | `tracing`           | `tracing_middleware`, `request_tracking_middleware` |
//! | `auth`              | `auth_middleware`, `admin_auth_middleware`, `require_admin_middleware`, `AuthState`, `AdminAuthState`, `TenantId`, `hash_api_key` |
//! | `security_headers`  | `security_headers_middleware`                  |
//! | `http_utils`        | `head_middleware`, `cache_middleware`           |
//! | `builder`           | [`MiddlewareStack`] fluent builder             |
//!
//! ## Middleware ordering documentation
//!
//! The correct execution order (outer → inner) is:
//! 1. Security headers — unconditional, must be outermost.
//! 2. Request ID — propagate correlation ID before any error can be emitted.
//! 3. Distributed tracing — enrich span after request ID is set.
//! 4. Authentication / admin auth — gate after observability layers.
//! 5. Cache — closest to the handler so it can see the completed response.
//!
//! Use [`MiddlewareStack`] to assemble this order without manual bookkeeping.
//!
//! ## Backward compatibility
//!
//! All previously public symbols are re-exported at the module root so that
//! existing call sites (`crate::middleware::auth_middleware`, etc.) continue to
//! compile without change.

pub mod auth;
pub mod builder;
pub mod http_utils;
pub mod ip_access;
pub mod rate_limit;
pub mod request_id;
pub mod response_middleware;
pub mod security_headers;
pub mod tenant;
pub mod tracing;

// ---------------------------------------------------------------------------
// Flat re-exports for backward compatibility
// ---------------------------------------------------------------------------

pub use auth::{
    AdminAuthState, AuthState, TenantId, admin_auth_middleware, auth_middleware, hash_api_key,
    require_admin_middleware,
};
pub use builder::MiddlewareStack;
pub use http_utils::{cache_middleware, head_middleware};
pub use ip_access::ip_access_control_middleware;
pub use rate_limit::rate_limit_headers_middleware;
pub use request_id::{correlation_id_middleware, request_id_middleware, CORRELATION_ID_HEADER};
pub use response_middleware::{deprecation_middleware, rate_limit_reject_middleware, security_headers_with_config, slow_request_middleware};
pub use security_headers::security_headers_middleware;
pub use tenant::{tenant_context_middleware, TenantExtractor};
pub use tracing::{request_tracking_middleware, tracing_middleware};
