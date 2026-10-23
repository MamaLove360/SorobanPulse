# Security Headers

SorobanPulse applies the OWASP-recommended set of HTTP security headers to
every response via `security_headers_middleware` in
`src/middleware/security_headers.rs`, and validates CORS origins at
startup via `validate_cors_origins`.

## Headers applied

| Header | Default value | Purpose |
|---|---|---|
| `X-Content-Type-Options` | `nosniff` | Prevents MIME-sniffing away from the declared `Content-Type`. |
| `X-Frame-Options` | `DENY` | Blocks the API/UI from being framed (clickjacking). |
| `Referrer-Policy` | `no-referrer` | Never leaks the full request URL to a downstream `Referer`. |
| `Strict-Transport-Security` | `max-age=31536000; includeSubDomains; preload` | Forces HTTPS for one year, including subdomains, and opts into browser preload lists. |
| `X-XSS-Protection` | `1; mode=block` | Legacy reflected-XSS filter for older browsers. |
| `Permissions-Policy` | all powerful features disabled | Denies camera, microphone, geolocation, USB, payment, etc. by default. |
| `Content-Security-Policy` | `default-src 'none'; frame-ancestors 'none';` on API routes; a relaxed policy allowing `unpkg.com` on `/docs` for Swagger UI assets | Restricts what resources a response may load/execute. |

## Configuration management

Every header value above is overridable at deploy time via environment
variable, through `SecurityHeadersConfig::from_env()`:

| Variable | Default | Notes |
|---|---|---|
| `SECURITY_HSTS_MAX_AGE` | `31536000` | Seconds. |
| `SECURITY_HSTS_INCLUDE_SUBDOMAINS` | `true` | `true`/`false`. |
| `SECURITY_HSTS_PRELOAD` | `true` | `true`/`false`. |
| `SECURITY_FRAME_OPTIONS` | `DENY` | `DENY` or `SAMEORIGIN`. |
| `SECURITY_REFERRER_POLICY` | `no-referrer` | Any valid `Referrer-Policy` token. |
| `SECURITY_CSP_DEFAULT` | see table above | Applied to every route except `/docs`. |
| `SECURITY_CSP_DOCS` | see table above | Applied only to `/docs`. |

Any variable that is unset, or fails to parse into a valid HTTP header
value, falls back to the built-in default rather than breaking the
response — the middleware never omits a header outright.

For programmatic use (e.g. tests, alternate entry points), construct a
`SecurityHeadersConfig` directly and pass it to
`security_headers_middleware_with_config`.

## CORS policy validation

Allowed origins are configured via `ALLOWED_ORIGINS` (comma-separated) and
consumed by `build_cors` in `src/routes.rs`. At config-load time,
`ConfigBuilder::build` (in `src/config/builder.rs`, via
`config_validation::check_cors`) runs every configured origin through
`validate_cors_origins`, which rejects:

- any value without an `http://` or `https://` scheme,
- any value containing a path, query string, or fragment,
- any value with an embedded wildcard (e.g. `https://*.example.com`) —
  only the literal, standalone value `*` is accepted as a wildcard,
- any value that isn't a valid HTTP header value.

A malformed origin now fails startup with a clear error instead of being
silently dropped from the CORS allow-list (the previous behavior of
`build_cors`, which used `filter_map(|o| o.parse().ok())`). Separately,
`ALLOWED_ORIGINS=*` continues to be rejected outright in production-like
environments (`Environment::is_production_like()`), forcing an explicit
origin list there.

## Testing

`src/middleware/security_headers.rs` includes unit tests asserting:

- all OWASP headers are present on a normal API route,
- `/docs` receives the relaxed, `unpkg.com`-permitting CSP,
- non-`/docs` routes receive the strict `default-src 'none'` CSP,
- `SecurityHeadersConfig` overrides (e.g. `X-Frame-Options`, HSTS max-age)
  are reflected in the response,
- `validate_cors_origins` accepts `*` and well-formed origins, and rejects
  origins with paths, missing schemes, or embedded wildcards.
