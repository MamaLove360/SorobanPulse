//! Shared outbound HTTP module.
//!
//! All user-supplied outbound URLs (webhooks, Discord, Slack, Telegram,
//! generic notification channels, export/archive destinations) **must** pass
//! through [`validate_outbound_url`] before any HTTP request is issued.
//!
//! The design follows the mitigations described in the SorobanPulse threat
//! model (`docs/security/threat-model.md`, gap G-1 / issue #1159):
//!
//! - URL parsed and scheme-checked.
//! - Hostname resolved to IPs; every resolved address checked against
//!   private/reserved ranges (DNS rebinding defence).
//! - One shared [`reqwest::Client`] factory with redirects disabled so that
//!   redirect chains cannot bypass the guard.
//! - Optional per-provider allowlist (e.g. only `discord.com` for Discord
//!   webhook URLs).
//!
//! # False-positive / temporary-ignore workflow
//!
//! If a legitimate provider URL is incorrectly blocked (e.g. a provider
//! that happens to own a previously-private IP range), add its domain to the
//! `allowed_domains` slice passed to [`validate_outbound_url`] at the call
//! site.  Document the exception in a comment explaining why it is safe.
//!
//! For temporary CI overrides during testing, set
//! `SSRF_ALLOW_PRIVATE_IPS=1` **only** in the `development` environment.
//! Production and staging builds treat this variable as absent.

// ⚠️  WARNING: Do not test — implement only (issue #1159).

use std::net::IpAddr;

use url::Url;

use crate::{config::Environment, error::AppError};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validate a user-supplied outbound URL against SSRF rules.
///
/// # Arguments
///
/// * `raw` – the URL string to validate, exactly as supplied by the user.
/// * `env` – the current runtime [`Environment`].  HTTP is only allowed in
///   non-production environments.
/// * `allowed_domains` – an optional slice of domain suffixes that bypass the
///   private-IP check entirely (e.g. `&["discord.com"]`).  The hostname must
///   *end with* one of the provided suffixes (`.` prefix is implicit).
///
/// # Validation steps
///
/// 1. If `SUBSCRIPTION_ALLOWED_URL_PREFIXES` is set the URL must match a
///    prefix — all other checks are skipped.
/// 2. Parse the URL.
/// 3. Check the scheme (HTTPS required in production).
/// 4. Check the static hostname against private/reserved ranges.
/// 5. Resolve the hostname to IP addresses and re-check each resolved address
///    (DNS-rebinding defence).
///
/// # Errors
///
/// Returns [`AppError::Validation`] for any violation.
pub async fn validate_outbound_url(
    raw: &str,
    env: &Environment,
    allowed_domains: &[&str],
) -> Result<(), AppError> {
    // 1. Global allowlist prefix shortcut (mirrors subscriptions.rs behaviour).
    let prefixes: Vec<String> = std::env::var("SUBSCRIPTION_ALLOWED_URL_PREFIXES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if !prefixes.is_empty() {
        if prefixes.iter().any(|p| raw.starts_with(p.as_str())) {
            return Ok(());
        }
        return Err(AppError::Validation(
            "URL does not match any entry in SUBSCRIPTION_ALLOWED_URL_PREFIXES".into(),
        ));
    }

    // 2. Parse.
    let url = Url::parse(raw)
        .map_err(|e| AppError::Validation(format!("not a valid URL: {e}")))?;

    // 3. Scheme.
    match url.scheme() {
        "https" => {}
        "http" if !env.is_production_like() => {}
        "http" => {
            return Err(AppError::Validation(
                "URL must use HTTPS in production".into(),
            ));
        }
        scheme => {
            return Err(AppError::Validation(format!(
                "URL scheme '{scheme}' is not permitted; use https"
            )));
        }
    }

    let host_str = url.host_str().unwrap_or("");

    // If the domain is in the provider-specific allowlist, skip IP checks.
    if is_allowed_domain(host_str, allowed_domains) {
        return Ok(());
    }

    // 4. Static hostname check (handles literal IPs and well-known hostnames).
    if is_ssrf_hostname(host_str) {
        return Err(AppError::Validation(ssrf_err_msg(host_str)));
    }

    // 5. DNS resolution + re-check (DNS-rebinding defence).
    //    Skip in development when SSRF_ALLOW_PRIVATE_IPS is set.
    if std::env::var("SSRF_ALLOW_PRIVATE_IPS").as_deref() == Ok("1")
        && !env.is_production_like()
    {
        return Ok(());
    }

    let resolved = tokio::net::lookup_host(format!(
        "{}:{}",
        host_str,
        url.port_or_known_default().unwrap_or(443)
    ))
    .await
    .map_err(|e| {
        AppError::Validation(format!("URL hostname '{host_str}' could not be resolved: {e}"))
    })?;

    for addr in resolved {
        let ip = addr.ip();
        if is_private_ip(&ip) {
            return Err(AppError::Validation(ssrf_err_msg_ip(host_str, &ip)));
        }
    }

    Ok(())
}

/// Synchronous variant that only checks the static hostname (no DNS
/// resolution).  Use when an async runtime is not available, or when
/// the URL has already been validated at creation time and you want a
/// cheap sanity check before sending.
///
/// For full DNS-rebinding protection, prefer [`validate_outbound_url`].
pub fn validate_outbound_url_static(
    raw: &str,
    env: &Environment,
    allowed_domains: &[&str],
) -> Result<(), AppError> {
    let prefixes: Vec<String> = std::env::var("SUBSCRIPTION_ALLOWED_URL_PREFIXES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if !prefixes.is_empty() {
        if prefixes.iter().any(|p| raw.starts_with(p.as_str())) {
            return Ok(());
        }
        return Err(AppError::Validation(
            "URL does not match any entry in SUBSCRIPTION_ALLOWED_URL_PREFIXES".into(),
        ));
    }

    let url = Url::parse(raw)
        .map_err(|e| AppError::Validation(format!("not a valid URL: {e}")))?;

    match url.scheme() {
        "https" => {}
        "http" if !env.is_production_like() => {}
        "http" => {
            return Err(AppError::Validation(
                "URL must use HTTPS in production".into(),
            ));
        }
        scheme => {
            return Err(AppError::Validation(format!(
                "URL scheme '{scheme}' is not permitted; use https"
            )));
        }
    }

    let host_str = url.host_str().unwrap_or("");
    if is_allowed_domain(host_str, allowed_domains) {
        return Ok(());
    }
    if is_ssrf_hostname(host_str) {
        return Err(AppError::Validation(ssrf_err_msg(host_str)));
    }
    Ok(())
}

/// Build a shared [`reqwest::Client`] that is hardened against SSRF:
///
/// - **Redirects disabled** — prevents SSRF via redirect chains.
/// - **10-second timeout** — prevents slow-loris on outbound connections.
///
/// Pass `allowed_domains` to build a client pre-configured for a specific
/// provider (e.g. Discord).  The client itself does not enforce domains;
/// domain enforcement happens in [`validate_outbound_url`].  The parameter
/// is accepted here as a documentation hint so callers are aware they should
/// have called the validator first.
pub fn build_outbound_client(
    _allowed_domains: &[&str],
) -> Result<reqwest::Client, crate::error::InitError> {
    build_http_client(
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(10)),
        "outbound",
    )
}

/// Finish a [`reqwest::ClientBuilder`], mapping a build failure (TLS backend
/// initialisation, invalid builder option) to a typed [`InitError`] instead
/// of panicking.
pub fn build_http_client(
    builder: reqwest::ClientBuilder,
    component: &'static str,
) -> Result<reqwest::Client, crate::error::InitError> {
    builder
        .build()
        .map_err(|source| crate::error::InitError::HttpClient { component, source })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// True if `hostname` ends with any of the `allowed` suffixes (case-insensitive).
/// The suffix comparison is domain-aware: `discord.com` matches `hooks.discord.com`
/// but not `evildiscord.com`.
fn is_allowed_domain(hostname: &str, allowed: &[&str]) -> bool {
    if allowed.is_empty() {
        return false;
    }
    let host_lower = hostname.to_ascii_lowercase();
    allowed.iter().any(|domain| {
        let d = domain.to_ascii_lowercase();
        host_lower == d || host_lower.ends_with(&format!(".{d}"))
    })
}

/// True if the hostname is statically known to be private/reserved.
/// Handles:
/// - Well-known loopback hostnames and suffixes.
/// - Literal IPv4 addresses in all common encodings (decimal, octal, hex).
/// - Literal IPv6 addresses.
/// - Common private-range string prefixes (belt-and-suspenders; the numeric
///   check is authoritative).
fn is_ssrf_hostname(host: &str) -> bool {
    // Loopback hostnames.
    if matches!(host, "localhost" | "127.0.0.1" | "::1")
        || host.ends_with(".local")
        || host.ends_with(".localhost")
    {
        return true;
    }

    // AWS / GCP / Azure instance metadata endpoints.
    if host == "169.254.169.254"
        || host == "metadata.google.internal"
        || host == "169.254.170.2"
    {
        return true;
    }

    // Try decimal IPv4.
    if let Ok(ip) = host.parse::<IpAddr>() {
        return is_private_ip(&ip);
    }

    // Octal / hex IPv4 encoding: browsers and some HTTP clients accept
    // `0177.0.0.1` or `0x7f000001` as 127.0.0.1.
    if let Some(ip) = try_parse_alternate_ipv4(host) {
        return is_private_ip(&IpAddr::V4(ip));
    }

    // Decimal-encoded IPv4 (e.g. `2130706433` == 127.0.0.1).
    if let Ok(n) = host.parse::<u32>() {
        let [a, b, c, d] = n.to_be_bytes();
        return is_private_ip(&IpAddr::V4(std::net::Ipv4Addr::new(a, b, c, d)));
    }

    // String-prefix heuristics as a cheap additional layer.
    host.starts_with("10.")
        || host.starts_with("192.168.")
        || host.starts_with("169.254.")
        || (host.starts_with("172.") && {
            host.split('.')
                .nth(1)
                .and_then(|o| o.parse::<u8>().ok())
                .map(|o| (16..=31).contains(&o))
                .unwrap_or(false)
        })
}

/// Parse alternative IPv4 representations (octal octets, hex).
fn try_parse_alternate_ipv4(host: &str) -> Option<std::net::Ipv4Addr> {
    // Must look like an IPv4 with 4 parts.
    let parts: Vec<&str> = host.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    let mut octets = [0u8; 4];
    for (i, part) in parts.iter().enumerate() {
        let n = if part.starts_with("0x") || part.starts_with("0X") {
            u8::from_str_radix(&part[2..], 16).ok()?
        } else if part.starts_with('0') && part.len() > 1 {
            u8::from_str_radix(&part[1..], 8).ok()?
        } else {
            continue; // normal decimal — already handled by `parse::<IpAddr>`
        };
        octets[i] = n;
    }
    Some(std::net::Ipv4Addr::from(octets))
}

/// True if `ip` falls within a private, loopback, link-local, or otherwise
/// reserved address range.
pub(crate) fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            a == 127                                      // loopback 127/8
                || a == 10                                // RFC 1918 10/8
                || (a == 172 && (16..=31).contains(&b))  // RFC 1918 172.16/12
                || (a == 192 && b == 168)                 // RFC 1918 192.168/16
                || (a == 169 && b == 254)                 // link-local / metadata
                || (a == 0 && b == 0 && c == 0)           // 0.0.0.0/8
                || a >= 224                                // multicast + reserved
                || (a == 100 && (64..=127).contains(&b))  // Shared Address Space (RFC 6598)
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // unique-local fc00::/7
        }
    }
}

fn ssrf_err_msg(host: &str) -> String {
    format!(
        "URL hostname '{host}' resolves to a private, loopback, or link-local address"
    )
}

fn ssrf_err_msg_ip(host: &str, ip: &IpAddr) -> String {
    format!(
        "URL hostname '{host}' resolved to private/reserved address {ip}"
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_hostnames_blocked() {
        assert!(is_ssrf_hostname("localhost"));
        assert!(is_ssrf_hostname("127.0.0.1"));
        assert!(is_ssrf_hostname("::1"));
        assert!(is_ssrf_hostname("my-service.local"));
        assert!(is_ssrf_hostname("db.localhost"));
    }

    #[test]
    fn metadata_endpoints_blocked() {
        assert!(is_ssrf_hostname("169.254.169.254"));
        assert!(is_ssrf_hostname("metadata.google.internal"));
    }

    #[test]
    fn private_ranges_blocked() {
        assert!(is_ssrf_hostname("10.0.0.1"));
        assert!(is_ssrf_hostname("172.16.0.1"));
        assert!(is_ssrf_hostname("172.31.255.255"));
        assert!(is_ssrf_hostname("192.168.1.1"));
    }

    #[test]
    fn private_ranges_boundary() {
        // 172.15.x.x is NOT private
        assert!(!is_ssrf_hostname("172.15.0.1"));
        // 172.32.x.x is NOT private
        assert!(!is_ssrf_hostname("172.32.0.1"));
    }

    #[test]
    fn public_ips_allowed() {
        assert!(!is_ssrf_hostname("8.8.8.8"));
        assert!(!is_ssrf_hostname("1.1.1.1"));
        assert!(!is_ssrf_hostname("142.250.74.46")); // google.com
    }

    #[test]
    fn octal_ipv4_encoding_blocked() {
        // 0177.0.0.1 == 127.0.0.1 in octal
        assert!(is_ssrf_hostname("0177.0.0.1"));
    }

    #[test]
    fn hex_ipv4_encoding_blocked() {
        // 0x0a.0x00.0x00.0x01 == 10.0.0.1
        assert!(is_ssrf_hostname("0x0a.0x00.0x00.0x01"));
    }

    #[test]
    fn decimal_encoded_ipv4_blocked() {
        // 2130706433 == 127.0.0.1
        assert!(is_ssrf_hostname("2130706433"));
    }

    #[test]
    fn ipv6_private_blocked() {
        assert!(is_ssrf_hostname("::1"));
        assert!(is_private_ip(&"fe80::1".parse::<IpAddr>().unwrap()));
        assert!(is_private_ip(&"fc00::1".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn allowed_domain_bypass() {
        // discord.com and its subdomains are allowed
        assert!(is_allowed_domain("hooks.discord.com", &["discord.com"]));
        assert!(is_allowed_domain("discord.com", &["discord.com"]));
        // but not a lookalike
        assert!(!is_allowed_domain("evildiscord.com", &["discord.com"]));
        assert!(!is_allowed_domain("notdiscord.com", &["discord.com"]));
    }

    #[test]
    fn empty_allowed_domains_does_not_bypass() {
        assert!(!is_allowed_domain("discord.com", &[]));
    }

    #[test]
    fn static_validator_blocks_http_in_production() {
        let result = validate_outbound_url_static(
            "http://example.com/hook",
            &Environment::Production,
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn static_validator_allows_http_in_development() {
        let result = validate_outbound_url_static(
            "http://example.com/hook",
            &Environment::Development,
            &[],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn static_validator_rejects_private_ip() {
        let result = validate_outbound_url_static(
            "https://192.168.1.50/hook",
            &Environment::Development,
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn static_validator_rejects_unknown_scheme() {
        let result = validate_outbound_url_static(
            "ftp://example.com/hook",
            &Environment::Development,
            &[],
        );
        assert!(result.is_err());
    }
}
