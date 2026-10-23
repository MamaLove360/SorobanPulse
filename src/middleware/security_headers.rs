//! Security headers middleware — Issue #663
//!
//! Adds the full OWASP-recommended set of HTTP security headers to every
//! response (Issue #566), with configuration management and CORS origin
//! validation (Issue #938). See `docs/security-headers.md` for the
//! operator-facing reference.

use axum::{extract::Request, http::HeaderValue, middleware::Next, response::Response};

/// Runtime-configurable knobs for [`security_headers_middleware`].
///
/// Defaults match the previous hardcoded behavior; every field can be
/// overridden via environment variable so operators can tune headers per
/// deployment without a code change (Issue #938: "Create header
/// configuration management").
#[derive(Clone, Debug)]
pub struct SecurityHeadersConfig {
    /// `Strict-Transport-Security` max-age in seconds.
    pub hsts_max_age_secs: u64,
    /// Whether `includeSubDomains` is appended to HSTS.
    pub hsts_include_subdomains: bool,
    /// Whether `preload` is appended to HSTS.
    pub hsts_preload: bool,
    /// `X-Frame-Options` value (`DENY`, `SAMEORIGIN`).
    pub frame_options: String,
    /// `Referrer-Policy` value.
    pub referrer_policy: String,
    /// Strict CSP applied to all routes except `/docs`.
    pub csp_default: String,
    /// Relaxed CSP applied to `/docs` (Swagger UI needs `unpkg.com`).
    pub csp_docs: String,
}

impl Default for SecurityHeadersConfig {
    fn default() -> Self {
        Self {
            hsts_max_age_secs: 31_536_000,
            hsts_include_subdomains: true,
            hsts_preload: true,
            frame_options: "DENY".to_string(),
            referrer_policy: "no-referrer".to_string(),
            csp_default: "default-src 'none'; frame-ancestors 'none';".to_string(),
            csp_docs: "default-src 'self'; \
                       script-src 'self' https://unpkg.com \
                         'sha256-OlhJ06FtsPaiJ/1A+8VpeJOGKCgqkf63ajgd7nrxk98='; \
                       style-src 'self' https://unpkg.com \
                         'sha256-t5Nfs8a1PFuEVO00S72ZGB4P65C74f37u8w0VCVsqBw='; \
                       img-src 'self' data:; \
                       connect-src 'self'; \
                       frame-ancestors 'none';"
                .to_string(),
        }
    }
}

impl SecurityHeadersConfig {
    /// Loads overrides from environment variables, falling back to
    /// [`Default::default`] for anything unset or unparsable.
    ///
    /// Recognized variables: `SECURITY_HSTS_MAX_AGE`,
    /// `SECURITY_HSTS_INCLUDE_SUBDOMAINS`, `SECURITY_HSTS_PRELOAD`,
    /// `SECURITY_FRAME_OPTIONS`, `SECURITY_REFERRER_POLICY`,
    /// `SECURITY_CSP_DEFAULT`, `SECURITY_CSP_DOCS`.
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            hsts_max_age_secs: std::env::var("SECURITY_HSTS_MAX_AGE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.hsts_max_age_secs),
            hsts_include_subdomains: std::env::var("SECURITY_HSTS_INCLUDE_SUBDOMAINS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.hsts_include_subdomains),
            hsts_preload: std::env::var("SECURITY_HSTS_PRELOAD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.hsts_preload),
            frame_options: std::env::var("SECURITY_FRAME_OPTIONS")
                .unwrap_or(defaults.frame_options),
            referrer_policy: std::env::var("SECURITY_REFERRER_POLICY")
                .unwrap_or(defaults.referrer_policy),
            csp_default: std::env::var("SECURITY_CSP_DEFAULT").unwrap_or(defaults.csp_default),
            csp_docs: std::env::var("SECURITY_CSP_DOCS").unwrap_or(defaults.csp_docs),
        }
    }

    fn hsts_value(&self) -> String {
        let mut v = format!("max-age={}", self.hsts_max_age_secs);
        if self.hsts_include_subdomains {
            v.push_str("; includeSubDomains");
        }
        if self.hsts_preload {
            v.push_str("; preload");
        }
        v
    }
}

/// Applies OWASP security headers to every response, using `config` to
/// derive header values.
///
/// Headers set:
/// - `X-Content-Type-Options: nosniff`
/// - `X-Frame-Options`
/// - `Referrer-Policy`
/// - `Strict-Transport-Security`
/// - `X-XSS-Protection: 1; mode=block`
/// - `Permissions-Policy: …` (all powerful features disabled)
/// - `Content-Security-Policy` (strict for API routes; relaxed for `/docs`;
///   the `/ui` dashboard keeps the policy set by `crate::dashboard`)
pub async fn security_headers_middleware_with_config(
    config: SecurityHeadersConfig,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_owned();
    let mut response = next.run(req).await;

    let h = response.headers_mut();

    h.insert("X-Content-Type-Options", HeaderValue::from_static("nosniff"));
    if let Ok(v) = config.frame_options.parse() {
        h.insert("X-Frame-Options", v);
    }
    if let Ok(v) = config.referrer_policy.parse() {
        h.insert("Referrer-Policy", v);
    }
    if let Ok(v) = config.hsts_value().parse() {
        h.insert("Strict-Transport-Security", v);
    }
    h.insert("X-XSS-Protection", HeaderValue::from_static("1; mode=block"));
    h.insert(
        "Permissions-Policy",
        HeaderValue::from_static(
            "accelerometer=(), ambient-light-sensor=(), autoplay=(), camera=(), \
             encrypted-media=(), fullscreen=(), geolocation=(), gyroscope=(), \
             magnetometer=(), microphone=(), midi=(), payment=(), usb=()",
        ),
    );

    let csp = if path == "/docs" {
        &config.csp_docs
    } else {
        &config.csp_default
    };
    // The /ui dashboard sets its own policy (see crate::dashboard); keep it.
    if !(crate::dashboard::is_dashboard_path(&path) && h.contains_key("Content-Security-Policy")) {
        if let Ok(v) = csp.parse() {
            h.insert("Content-Security-Policy", v);
        }
    }

    response
}

/// Backwards-compatible entry point using [`SecurityHeadersConfig::default`].
pub async fn security_headers_middleware(req: Request, next: Next) -> Response {
    security_headers_middleware_with_config(SecurityHeadersConfig::default(), req, next).await
}

/// Validates a list of CORS `Access-Control-Allow-Origin` candidates.
///
/// An origin is valid if it is the literal wildcard `"*"`, or a bare
/// `scheme://host[:port]` string with no path, query, fragment, or
/// trailing slash and a scheme of `http` or `https`. Returns the list of
/// human-readable error messages for every invalid entry (empty if all
/// are valid). Used both at config-load time (to fail fast on typos
/// instead of silently dropping them from the CORS layer) and available
/// for ad-hoc validation elsewhere (Issue #938: "Implement CORS policy
/// validation").
pub fn validate_cors_origins(origins: &[String]) -> Vec<String> {
    let mut errors = Vec::new();
    for origin in origins {
        if origin == "*" {
            continue;
        }
        if let Err(reason) = validate_single_origin(origin) {
            errors.push(format!("Invalid CORS origin '{origin}': {reason}"));
        }
    }
    errors
}

fn validate_single_origin(origin: &str) -> Result<(), &'static str> {
    let (scheme, rest) = origin
        .split_once("://")
        .ok_or("must be an absolute origin like https://example.com")?;
    if scheme != "http" && scheme != "https" {
        return Err("scheme must be http or https");
    }
    if rest.is_empty() {
        return Err("missing host");
    }
    if rest.contains('/') || rest.contains('?') || rest.contains('#') {
        return Err("must not contain a path, query, or fragment");
    }
    if rest.contains('*') {
        return Err("wildcards are only allowed as the entire value ('*')");
    }
    if origin.parse::<axum::http::HeaderValue>().is_err() {
        return Err("not a valid HTTP header value");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::StatusCode, routing::get, Router};
    use tower::ServiceExt;

    async fn app() -> Router {
        Router::new()
            .route("/api", get(|| async { "ok" }))
            .route("/docs", get(|| async { "swagger" }))
            .layer(axum::middleware::from_fn(security_headers_middleware))
    }

    #[tokio::test]
    async fn all_owasp_headers_present_on_api_route() {
        let resp = app()
            .await
            .oneshot(axum::http::Request::get("/api").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let h = resp.headers();
        assert_eq!(h.get("X-Content-Type-Options").unwrap(), "nosniff");
        assert_eq!(h.get("X-Frame-Options").unwrap(), "DENY");
        assert_eq!(h.get("Referrer-Policy").unwrap(), "no-referrer");
        assert_eq!(h.get("X-XSS-Protection").unwrap(), "1; mode=block");
        assert!(h.get("Content-Security-Policy").is_some());
        assert!(h.get("Strict-Transport-Security").is_some());
        assert!(h.get("Permissions-Policy").is_some());
    }

    #[tokio::test]
    async fn docs_route_gets_relaxed_csp() {
        let resp = app()
            .await
            .oneshot(axum::http::Request::get("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let csp = resp
            .headers()
            .get("Content-Security-Policy")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(csp.contains("unpkg.com"));
    }

    #[tokio::test]
    async fn docs_csp_has_no_unsafe_inline() {
        let resp = app()
            .await
            .oneshot(axum::http::Request::get("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let csp = resp
            .headers()
            .get("Content-Security-Policy")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            !csp.contains("unsafe-inline"),
            "docs CSP must not contain unsafe-inline; got: {csp}"
        );
    }

    #[tokio::test]
    async fn docs_csp_has_sha256_hashes() {
        let resp = app()
            .await
            .oneshot(axum::http::Request::get("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let csp = resp
            .headers()
            .get("Content-Security-Policy")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            csp.contains("sha256-"),
            "docs CSP should contain SHA-256 hashes for inline blocks; got: {csp}"
        );
        // CSS hash
        assert!(
            csp.contains("sha256-t5Nfs8a1PFuEVO00S72ZGB4P65C74f37u8w0VCVsqBw="),
            "docs CSP missing CSS sha256 hash; got: {csp}"
        );
        // JS hash
        assert!(
            csp.contains("sha256-OlhJ06FtsPaiJ/1A+8VpeJOGKCgqkf63ajgd7nrxk98="),
            "docs CSP missing JS sha256 hash; got: {csp}"
        );
    }

    #[tokio::test]
    async fn api_route_gets_strict_csp() {
        let resp = app()
            .await
            .oneshot(axum::http::Request::get("/api").body(Body::empty()).unwrap())
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
    async fn config_overrides_are_applied() {
        let config = SecurityHeadersConfig {
            frame_options: "SAMEORIGIN".to_string(),
            hsts_max_age_secs: 3600,
            hsts_include_subdomains: false,
            hsts_preload: false,
            ..SecurityHeadersConfig::default()
        };
        let app = Router::new()
            .route("/api", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(move |req, next| {
                let config = config.clone();
                async move { security_headers_middleware_with_config(config, req, next).await }
            }));
        let resp = app
            .oneshot(axum::http::Request::get("/api").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let h = resp.headers();
        assert_eq!(h.get("X-Frame-Options").unwrap(), "SAMEORIGIN");
        assert_eq!(h.get("Strict-Transport-Security").unwrap(), "max-age=3600");
    }

    #[test]
    fn wildcard_origin_is_valid() {
        assert!(validate_cors_origins(&["*".to_string()]).is_empty());
    }

    #[test]
    fn well_formed_origins_are_valid() {
        let origins = vec![
            "https://app.example.com".to_string(),
            "http://localhost:3000".to_string(),
        ];
        assert!(validate_cors_origins(&origins).is_empty());
    }

    #[test]
    fn origin_with_path_is_rejected() {
        let errors = validate_cors_origins(&["https://example.com/path".to_string()]);
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn origin_missing_scheme_is_rejected() {
        let errors = validate_cors_origins(&["example.com".to_string()]);
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn origin_with_embedded_wildcard_is_rejected() {
        let errors = validate_cors_origins(&["https://*.example.com".to_string()]);
        assert_eq!(errors.len(), 1);
    }
}
