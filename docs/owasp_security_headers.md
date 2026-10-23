# OWASP API Security Headers (Issue #566)

## Overview

Soroban Pulse implements comprehensive OWASP API security headers to protect against common web vulnerabilities. These headers are automatically added to all API responses to enhance security.

## Implemented Headers

### 1. X-Content-Type-Options: nosniff

**Purpose**: Prevent MIME type sniffing attacks

**Value**: `nosniff`

**How it works**:
- Tells browsers to respect the `Content-Type` header
- Prevents browsers from attempting to detect or override the MIME type
- Protects against attacks where files with incorrect MIME types could be executed

**Browser support**: All modern browsers

**Example**:
```
X-Content-Type-Options: nosniff
```

---

### 2. X-Frame-Options: DENY

**Purpose**: Prevent clickjacking attacks

**Value**: `DENY`

**How it works**:
- Prevents your API responses from being displayed in a frame/iframe
- Protects against clickjacking where an attacker frames your page to trick users
- The `DENY` value is the most restrictive - no one can frame the content

**Browser support**: All modern browsers

**Example**:
```
X-Frame-Options: DENY
```

**Alternative values**:
- `SAMEORIGIN`: Allow framing only from same origin
- `ALLOW-FROM uri`: Allow framing from specific URI (deprecated)

---

### 3. Content-Security-Policy (CSP)

**Purpose**: Mitigate XSS and injection attacks

**Values**:
- **For `/docs` (Swagger UI)**:
  ```
  default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; 
  style-src 'self' 'unsafe-inline' https://unpkg.com; img-src 'self' data:; 
  connect-src 'self'; frame-ancestors 'none';
  ```

- **For API endpoints**:
  ```
  default-src 'none'; frame-ancestors 'none';
  ```

**How it works**:
- Restricts what resources (scripts, styles, fonts, etc.) can be loaded
- Prevents inline scripts and styles (except where explicitly allowed)
- For API endpoints: extremely restrictive since they only return JSON
- For documentation: allows Swagger UI assets from `unpkg.com`

**Browser support**: All modern browsers

#### Swagger UI (`/docs`) specifics

The Swagger UI page served at `GET /docs` is the only endpoint that needs a
relaxed CSP, because the UI loads its JavaScript and CSS bundles from the
`unpkg.com` CDN and injects inline bootstrap scripts/styles:

```
Content-Security-Policy: default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; style-src 'self' 'unsafe-inline' https://unpkg.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none';
```

- `script-src 'self' 'unsafe-inline' https://unpkg.com` – allows the local
  Swagger initializer, the inline bootstrap script, and the Swagger UI bundle
  from the CDN. No other third-party script origins are permitted.
- `style-src 'self' 'unsafe-inline' https://unpkg.com` – allows the Swagger UI
  stylesheet from the CDN and the inline styles it injects.
- `img-src 'self' data:` – allows the favicon and inline `data:` images used by
  the UI.
- `connect-src 'self'` – the UI may only call the API on the same origin.
- `frame-ancestors 'none'` – the docs page cannot be framed (clickjacking).

If you self-host the Swagger UI assets instead of using the CDN, drop
`https://unpkg.com` from `script-src` and `style-src` so only `'self'` (plus
`'unsafe-inline'` where required) is allowed.

The `/docs` response also carries the following headers:

```
Strict-Transport-Security: max-age=31536000; includeSubDomains
X-Content-Type-Options: nosniff
```

- `Strict-Transport-Security` forces browsers to use HTTPS for the docs page
  (and all subdomains) for one year, preventing protocol downgrade attacks.
- `X-Content-Type-Options: nosniff` stops browsers from MIME-sniffing the HTML
  and asset responses, so a mislabelled response cannot be executed as script.

#### Verifying the Swagger UI headers

Check the headers with `curl -I`:

```bash
curl -I https://api.example.com/docs
```

Expected (abridged) output:

```
HTTP/2 200
content-type: text/html; charset=utf-8
content-security-policy: default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; style-src 'self' 'unsafe-inline' https://unpkg.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none';
strict-transport-security: max-age=31536000; includeSubDomains
x-content-type-options: nosniff
```

Then confirm the UI still renders: open `https://api.example.com/docs` in a
browser, check the DevTools console for CSP violations (there should be none),
and exercise an endpoint via "Try it out".

To scan with OWASP ZAP:

```bash
# Baseline passive scan of the docs page
docker run --rm -t ghcr.io/zaproxy/zaproxy:stable zap-baseline.py \
  -t https://api.example.com/docs -I

# Or with a local ZAP install
zap-cli quick-scan --self-contained https://api.example.com/docs
```

Review the ZAP report for missing-header alerts (CSP, HSTS,
`X-Content-Type-Options`) and CSP-related warnings; all should be absent.

---

### 4. Strict-Transport-Security (HSTS)

**Purpose**: Enforce HTTPS connections

**Value**: `max-age=31536000; includeSubDomains`

**How it works**:
- Tells browsers to only use HTTPS for communication with the server
- `max-age=31536000`: Policy valid for 1 year (in seconds)
- `includeSubDomains`: Apply policy to all subdomains
- Prevents downgrade attacks and man-in-the-middle attacks
- Applied to **all** responses, including the Swagger UI at `/docs`
- `preload` can optionally be appended once the domain is ready for browser
  preload lists (it is irreversible in practice, so enable it deliberately)

**Browser support**: All modern browsers

**Important**: Only set on HTTPS responses

**Example**:
```
Strict-Transport-Security: max-age=31536000; includeSubDomains
```

---

### 5. X-XSS-Protection

**Purpose**: Enable XSS protection in older browsers

**Value**: `1; mode=block`

**How it works**:
- `1`: Enable XSS filter
- `mode=block`: Block the page if XSS attack is detected
- Provides fallback protection for browsers that don't support CSP
- Deprecated in modern browsers but useful for legacy support

**Browser support**: Older browsers (IE, older Edge, Safari)

**Note**: Modern browsers rely on CSP instead

**Example**:
```
X-XSS-Protection: 1; mode=block
```

---

### 6. Referrer-Policy

**Purpose**: Control referrer information sent in requests

**Value**: `no-referrer`

**How it works**:
- `no-referrer`: Never send referrer information
- Prevents information leakage about where users came from
- Protects user privacy

**Browser support**: All modern browsers

**Alternative values**:
- `strict-origin-when-cross-origin`: Safe default for most sites
- `no-referrer-when-downgrade`: Send referrer on HTTPS→HTTPS only
- `same-origin`: Send referrer for same-origin requests only

**Example**:
```
Referrer-Policy: no-referrer
```

---

### 7. Permissions-Policy

**Purpose**: Control which browser features can be used

**Value**: Restricts: accelerometer, ambient-light-sensor, autoplay, camera, encrypted-media, fullscreen, geolocation, gyroscope, magnetometer, microphone, midi, payment, usb

**How it works**:
- Formerly known as Feature-Policy
- Restricts access to powerful browser features
- All features set to empty `()` - fully disabled
- Prevents malicious code from accessing sensitive APIs

**Browser support**: Modern browsers (Chrome, Edge, Opera)

**Example**:
```
Permissions-Policy: accelerometer=(), ambient-light-sensor=(), autoplay=(), 
camera=(), encrypted-media=(), fullscreen=(), geolocation=(), gyroscope=(), 
magnetometer=(), microphone=(), midi=(), payment=(), usb=()
```

---

## Header Priority

Soroban Pulse applies headers in this order (most to least protective):
1. Permissions-Policy (disable features)
2. Strict-Transport-Security (enforce HTTPS)
3. X-Content-Type-Options (prevent MIME sniffing)
4. X-Frame-Options (prevent clickjacking)
5. Content-Security-Policy (prevent XSS)
6. X-XSS-Protection (legacy XSS protection)
7. Referrer-Policy (prevent information leakage)

## OWASP Top 10 Coverage

These headers address OWASP Top 10 vulnerabilities:

| OWASP Top 10 | Header | Mitigation |
|---|---|---|
| A01:2021 – Broken Access Control | X-Frame-Options | Prevents unauthorized framing |
| A03:2021 – Injection | Content-Security-Policy | Prevents code injection |
| A04:2021 – Insecure Design | Strict-Transport-Security | Enforces secure transport |
| A05:2021 – Security Misconfiguration | All headers | Ensures secure defaults |
| A06:2021 – Vulnerable and Outdated Components | Permissions-Policy | Restricts dangerous APIs |
| A07:2021 – Authentication | Strict-Transport-Security | Prevents credential interception |
| A08:2021 – Software and Data Integrity | X-Content-Type-Options | Prevents file type confusion |
| A09:2021 – Logging and Monitoring | X-Frame-Options | Helps detect attacks |
| A10:2021 – SSRF | Permissions-Policy | Restricts network features |

## Testing Headers

### Using curl

```bash
curl -i https://api.example.com/health
```

Look for these headers in the response:
```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Strict-Transport-Security: max-age=31536000; includeSubDomains
X-XSS-Protection: 1; mode=block
Permissions-Policy: ...
Content-Security-Policy: ...
Referrer-Policy: no-referrer
```

### Swagger UI (`/docs`)

```bash
curl -I https://api.example.com/docs
```

The docs page must return the Swagger-specific CSP (see
[Swagger UI specifics](#swagger-ui-docs-specifics)), plus:

```
Strict-Transport-Security: max-age=31536000; includeSubDomains
X-Content-Type-Options: nosniff
```

### OWASP ZAP

Run a passive baseline scan against the docs page and confirm no
missing-header or CSP alerts are reported:

```bash
docker run --rm -t ghcr.io/zaproxy/zaproxy:stable zap-baseline.py \
  -t https://api.example.com/docs -I
```

### Using online tools

- [SSL Labs](https://www.ssllabs.com/ssltest/)
- [Security Headers](https://securityheaders.com/)
- [Mozilla Observatory](https://observatory.mozilla.org/)

### Programmatic testing

```python
import requests

response = requests.get("https://api.example.com/health")

required_headers = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Strict-Transport-Security": lambda v: "max-age=31536000" in v and "includeSubDomains" in v,
    "X-XSS-Protection": "1; mode=block",
    "Permissions-Policy": lambda v: len(v) > 0,
    "Content-Security-Policy": lambda v: len(v) > 0,
    "Referrer-Policy": "no-referrer",
}

for header, expected_value in required_headers.items():
    actual = response.headers.get(header)
    if callable(expected_value):
        assert expected_value(actual), f"{header} check failed"
    else:
        assert actual == expected_value, f"{header}: expected {expected_value}, got {actual}"

print("✓ All security headers verified!")

# Swagger UI must allow the CDN and inline assets in its CSP
docs = requests.get("https://api.example.com/docs")
csp = docs.headers.get("Content-Security-Policy", "")
assert "https://unpkg.com" in csp, "Swagger CDN missing from CSP"
assert "script-src" in csp and "style-src" in csp, "CSP missing script/style directives"
assert docs.headers.get("X-Content-Type-Options") == "nosniff"
assert "max-age=31536000" in docs.headers.get("Strict-Transport-Security", "")

print("✓ Swagger UI security headers verified!")
```

## Configuration

These headers are applied automatically and cannot be disabled. They are set on all responses from Soroban Pulse.

If you need different security header values:

1. Create a custom middleware wrapper
2. Override the `security_headers_middleware` function
3. Set headers according to your requirements

Example custom middleware:
```rust
pub async fn custom_security_headers_middleware(req: Request, next: Next) -> Response {
    let mut response = next.run(req).await;
    
    // Override specific headers if needed
    response
        .headers_mut()
        .insert("Strict-Transport-Security", "max-age=63072000".parse().unwrap());
    
    response
}
```

## Best Practices

1. **Always use HTTPS**: Strict-Transport-Security only works over HTTPS
2. **Monitor headers**: Regularly check that headers are being sent correctly
3. **Keep updated**: Monitor security advisories and update policies as needed
4. **Test changes**: Verify headers before deploying to production
5. **Document policies**: Keep records of why each header is configured
6. **Regular audits**: Use security scanning tools to verify compliance

## Troubleshooting

### Headers not appearing in response

**Possible causes**:
- Proxy/WAF stripping headers
- Caching layer removing headers
- Incorrect environment configuration
- Browser dev tools filtering

**Solution**:
```bash
# Check raw headers with curl
curl -i -v https://api.example.com/health
```

### CSP too restrictive

**Issue**: Legitimate requests blocked by CSP

**Solution**:
- Check browser console for CSP violations
- Review CSP report-uri logs
- Adjust CSP policy if needed
- Use `Content-Security-Policy-Report-Only` for testing

### HSTS issues

**Issue**: Cannot reach API after enabling HSTS

**Solution**:
- Start with short max-age value
- Test before enabling `preload`
- Cannot be undone quickly - be careful!

## Security Headers Score

A proper security headers configuration can achieve an A+ grade on [securityheaders.com](https://securityheaders.com/).

Soroban Pulse's header configuration is designed to:
- ✅ Prevent MIME type sniffing
- ✅ Prevent clickjacking
- ✅ Enforce HTTPS (HSTS `max-age=31536000; includeSubDomains`)
- ✅ Mitigate XSS attacks (including a CSP tailored to the Swagger UI at `/docs`)
- ✅ Control browser features
- ✅ Prevent information leakage
- ✅ Support legacy browsers

## References

- [OWASP Security Headers](https://owasp.org/www-project-secure-headers/)
- [MDN Web Docs - HTTP Headers](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers)
- [NIST Secure Software Development Framework](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-218.pdf)
- [CWE Top 25 Most Dangerous Software Weaknesses](https://cwe.mitre.org/top25/)

## Support

For issues or questions about security headers:

1. Check [OWASP Secure Headers](https://cheatsheetseries.owasp.org/cheatsheets/Secure_Headers_Cheat_Sheet.html)
2. Review the [MDN HTTP Headers guide](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers)
3. Test your API on [securityheaders.com](https://securityheaders.com/)
4. Open an issue on GitHub
