//! Rate-limit response-header middleware — Issue #669
//!
//! This module provides [`rate_limit_headers_middleware`], which injects
//! standard `X-RateLimit-*` headers into every response when per-API-key
//! rate limits are configured.
//!
//! ## Headers injected
//!
//! | Header                         | Description                          |
//! |-------------------------------|--------------------------------------|
//! | `X-RateLimit-Limit-Minute`    | Configured per-minute limit          |
//! | `X-RateLimit-Remaining-Minute`| Remaining requests in the minute     |
//! | `X-RateLimit-Limit-Hour`      | Configured per-hour limit            |
//! | `X-RateLimit-Remaining-Hour`  | Remaining requests in the hour       |
//! | `X-RateLimit-Limit-Day`       | Configured per-day limit             |
//! | `X-RateLimit-Remaining-Day`   | Remaining requests in the day        |
//! | `X-RateLimit-Reset`           | Unix timestamp when window resets    |
//! | `Retry-After`                 | Seconds until reset (on 429 only)    |
//!
//! When no per-key limits are configured (`RATE_LIMIT_KEY_PER_MINUTE`,
//! `RATE_LIMIT_KEY_PER_HOUR`, `RATE_LIMIT_KEY_PER_DAY` all unset) this
//! middleware is a no-op and adds no headers.

use axum::{
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

/// Issue #941: per-API-key rate limit *enforcement* middleware.
///
/// Prior to this change, this middleware only ever called
/// `get_rate_limit_status` — a read-only status lookup — *after* already
/// running the request handler via `next.run(req).await`, purely to
/// decorate the (already-completed) response with informational
/// `X-RateLimit-*` headers. `check_rate_limit`, the function that actually
/// increments counters and reports whether a request should be allowed,
/// had no callers anywhere in the codebase: no request was ever actually
/// blocked, regardless of `RATE_LIMIT_KEY_PER_*` configuration. This now
/// calls `check_rate_limit` *before* `next.run`, and returns `429 Too Many
/// Requests` instead of proxying through when a key is over its quota.
///
/// Injects standard headers into every response:
///
/// - `X-RateLimit-Limit-Minute` / `X-RateLimit-Remaining-Minute`
/// - `X-RateLimit-Limit-Hour`   / `X-RateLimit-Remaining-Hour`
/// - `X-RateLimit-Limit-Day`    / `X-RateLimit-Remaining-Day`
/// - `X-RateLimit-Limit-Month`  / `X-RateLimit-Remaining-Month`
/// - `X-RateLimit-Reset` / `Retry-After` (on a 429 response)
///
/// When no per-key limits are configured (`RATE_LIMIT_KEY_PER_MINUTE` etc.
/// all unset) this middleware is a no-op and adds no headers.
pub async fn rate_limit_headers_middleware(
    State(state): State<crate::routes::AppState>,
    req: Request,
    next: Next,
) -> Response {
    // Only enforce/inject headers when at least one per-key window is configured.
    let cfg = &state.config;
    let is_configured = cfg.rate_limit_key_per_minute.is_some()
        || cfg.rate_limit_key_per_hour.is_some()
        || cfg.rate_limit_key_per_day.is_some()
        || cfg.rate_limit_key_per_month.is_some();

    if !is_configured {
        return next.run(req).await;
    }

    // Extract API key — requests with no key are handled by the auth
    // middleware/handlers themselves; this middleware neither blocks nor
    // decorates them.
    let api_key = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| req.headers().get("X-Api-Key").and_then(|h| h.to_str().ok()))
        .map(|s| s.to_string());

    let Some(key) = api_key else {
        return next.run(req).await;
    };

    let rate_limit_config = crate::rate_limiter::RateLimitConfig::new(
        cfg.rate_limit_key_per_minute,
        cfg.rate_limit_key_per_hour,
        cfg.rate_limit_key_per_day,
        cfg.rate_limit_key_per_month,
    );

    // Enforce: increments the counter and reports whether this request is
    // allowed, atomically (see check_rate_limit's transaction).
    let checked =
        crate::rate_limiter::check_rate_limit(&state.pool, &key, &rate_limit_config).await;

    let (is_allowed, status) = match checked {
        Ok(result) => result,
        // A rate-limiter storage failure must never itself take the API
        // down — fail open, matching this middleware's prior behavior of
        // silently skipping header injection on error.
        Err(_) => return next.run(req).await,
    };

    if !is_allowed {
        let mut response = StatusCode::TOO_MANY_REQUESTS.into_response();
        apply_rate_limit_headers(&mut response, &status);
        return response;
    }

    let mut response = next.run(req).await;
    apply_rate_limit_headers(&mut response, &status);
    response
}

fn apply_rate_limit_headers(response: &mut Response<Body>, status: &crate::rate_limiter::RateLimitStatus) {
    {
        let headers = response.headers_mut();

        // Per-minute window headers
        if let (Some(limit), Some(remaining)) = (status.limit_minute, status.remaining_minute) {
            let _ = headers.insert(
                "X-RateLimit-Limit-Minute",
                axum::http::HeaderValue::from(limit),
            );
            let _ = headers.insert(
                "X-RateLimit-Remaining-Minute",
                axum::http::HeaderValue::from(remaining),
            );
        }

        // Per-hour window headers
        if let (Some(limit), Some(remaining)) = (status.limit_hour, status.remaining_hour) {
            let _ = headers.insert(
                "X-RateLimit-Limit-Hour",
                axum::http::HeaderValue::from(limit),
            );
            let _ = headers.insert(
                "X-RateLimit-Remaining-Hour",
                axum::http::HeaderValue::from(remaining),
            );
        }

        // Per-day window headers
        if let (Some(limit), Some(remaining)) = (status.limit_day, status.remaining_day) {
            let _ = headers.insert(
                "X-RateLimit-Limit-Day",
                axum::http::HeaderValue::from(limit),
            );
            let _ = headers.insert(
                "X-RateLimit-Remaining-Day",
                axum::http::HeaderValue::from(remaining),
            );
        }

        // Per-month window headers
        if let (Some(limit), Some(remaining)) = (status.limit_month, status.remaining_month) {
            let _ = headers.insert(
                "X-RateLimit-Limit-Month",
                axum::http::HeaderValue::from(limit),
            );
            let _ = headers.insert(
                "X-RateLimit-Remaining-Month",
                axum::http::HeaderValue::from(remaining),
            );
        }

        // Reset timestamp (when currently limited)
        if let Some(reset_at) = status.reset_at {
            let _ = headers.insert(
                "X-RateLimit-Reset",
                axum::http::HeaderValue::from(reset_at),
            );
        }

        // Retry-After on 429 responses (seconds until reset)
        if status.is_rate_limited {
            if let Some(reset_at) = status.reset_at {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                let retry_after = (reset_at - now).max(1);
                let _ = headers.insert(
                    "Retry-After",
                    axum::http::HeaderValue::from(retry_after),
                );
            }
        }
    }
}
