//! Issue #1112: serve the built web dashboard (`web/dist`) at `/ui`.
//!
//! Enabled at runtime with `SERVE_DASHBOARD=true` and at compile time by the
//! `dashboard` cargo feature (on by default). The routes:
//!
//! - `GET /ui/assets/*` — Vite's content-hashed bundles, served with
//!   `Cache-Control: public, max-age=31536000, immutable`. A missing asset is a
//!   real 404, never the SPA shell.
//! - `GET /ui/*` — any other path falls back to `index.html` so client-side
//!   routes survive a reload. `index.html` is served `no-cache` so a new deploy
//!   is picked up immediately.
//!
//! These routes bypass API-key auth and rate limiting (they serve static files
//! only; every data call the dashboard makes goes through the normal API) and
//! carry their own Content-Security-Policy, see [`content_security_policy`].

use axum::Router;

use crate::config::Config;

/// URL prefix the dashboard is mounted at. Must match `base` in web/vite.config.ts.
pub const MOUNT_PATH: &str = "/ui";

/// Returns true for request paths served by the dashboard.
pub fn is_dashboard_path(path: &str) -> bool {
    path == MOUNT_PATH || path.starts_with("/ui/")
}

/// Content-Security-Policy for the dashboard shell and its assets.
///
/// The Vite build emits no inline scripts or styles, so `'self'` is enough for
/// both. `connect-src` allows the same-origin API (fetch + SSE) plus any
/// origins listed in `DASHBOARD_CONNECT_SRC`, for operators who point the
/// dashboard at a different API host.
pub fn content_security_policy(extra_connect_src: &[String]) -> String {
    let mut connect = String::from("'self'");
    for origin in extra_connect_src {
        // Drop anything that could terminate the directive or inject another one.
        if !origin.is_empty() && !origin.contains(&[';', ',', '\'', ' ', '\n', '\r'][..]) {
            connect.push(' ');
            connect.push_str(origin);
        }
    }
    format!(
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; \
         font-src 'self'; connect-src {connect}; manifest-src 'self'; object-src 'none'; \
         base-uri 'self'; form-action 'self'; frame-ancestors 'none'"
    )
}

/// Build the `/ui` router, or an empty router when the dashboard is disabled.
pub fn router<S>(config: &Config) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    if !config.serve_dashboard {
        return Router::new();
    }
    serve(config)
}

#[cfg(not(feature = "dashboard"))]
fn serve<S>(_config: &Config) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    tracing::warn!(
        "SERVE_DASHBOARD=true but this binary was built without the `dashboard` feature; /ui is disabled"
    );
    Router::new()
}

#[cfg(feature = "dashboard")]
fn serve<S>(config: &Config) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    use axum::{
        extract::Request,
        http::{header, HeaderValue, StatusCode},
        middleware::Next,
        response::{IntoResponse, Response},
    };
    use std::path::Path;
    use tower_http::services::{ServeDir, ServeFile};

    let dir = Path::new(&config.dashboard_dir);
    let index = dir.join("index.html");
    if !index.is_file() {
        tracing::warn!(
            dashboard_dir = %dir.display(),
            "SERVE_DASHBOARD=true but index.html was not found; build web/ or set DASHBOARD_DIR"
        );
    } else {
        tracing::info!(dashboard_dir = %dir.display(), "serving web dashboard at {MOUNT_PATH}");
    }

    // DASHBOARD_CONNECT_SRC is operator input; fall back to a strict policy
    // rather than aborting startup if it produces an invalid header value.
    let csp = HeaderValue::from_str(&content_security_policy(&config.dashboard_connect_src))
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Invalid DASHBOARD_CONNECT_SRC; using a strict dashboard CSP");
            HeaderValue::from_static("default-src 'self'")
        });

    let spa = ServeDir::new(dir)
        .precompressed_gzip()
        .precompressed_br()
        .fallback(ServeFile::new(index));

    Router::new()
        .nest_service(MOUNT_PATH, spa)
        .layer(axum::middleware::from_fn(move |req: Request, next: Next| {
            let csp = csp.clone();
            async move {
                let is_asset = req.uri().path().starts_with("/ui/assets/");
                let mut res: Response = next.run(req).await;
                let is_html = res
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|ct| ct.starts_with("text/html"));

                // A missing hashed asset must not be answered with the SPA
                // shell (and then cached as immutable).
                if is_asset && is_html {
                    return StatusCode::NOT_FOUND.into_response();
                }

                let cache = if is_asset && res.status().is_success() {
                    "public, max-age=31536000, immutable"
                } else {
                    "no-cache"
                };
                let h = res.headers_mut();
                h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
                h.insert(header::CONTENT_SECURITY_POLICY, csp);
                res
            }
        }))
}
