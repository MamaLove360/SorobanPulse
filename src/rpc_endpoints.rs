//! Latency-scored RPC endpoint selection with automatic failover — Issue #1256.
//!
//! [`RpcEndpoints`] tracks the health of every configured Stellar RPC URL
//! (`STELLAR_RPC_URLS`, or `STELLAR_RPC_URL` + `STELLAR_RPC_FALLBACK_URLS`):
//!
//! - A background [`RpcLatencyProbe`] times `getLatestLedger` against each
//!   endpoint every [`PROBE_INTERVAL`] and keeps the last [`LATENCY_WINDOW`]
//!   samples. Only probe timings are used, so endpoints are compared on the
//!   same cheap call rather than on whatever the indexer happened to send.
//! - Requests go to the endpoint with the lowest p50 latency
//!   ([`RpcEndpoints::candidates`]); the rest are failover candidates.
//! - After [`MAX_CONSECUTIVE_ERRORS`] consecutive transport failures an
//!   endpoint is marked unavailable and only re-probed every
//!   [`UNAVAILABLE_REPROBE_INTERVAL`]. A successful probe or request revives it.
//!
//! Per-endpoint metrics: `soroban_pulse_rpc_endpoint_latency_ms` (p50) and
//! `soroban_pulse_rpc_endpoint_available` (1/0), labelled by the endpoint's
//! origin so API keys embedded in the URL path never reach Prometheus.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::metrics;

/// How often available endpoints are probed.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(30);
/// How often an unavailable endpoint is re-probed.
pub const UNAVAILABLE_REPROBE_INTERVAL: Duration = Duration::from_secs(60);
/// Consecutive failures after which an endpoint is marked unavailable.
pub const MAX_CONSECUTIVE_ERRORS: u32 = 3;
/// Number of recent latency samples the p50 is computed over.
pub const LATENCY_WINDOW: usize = 10;

/// Tolerance for probe scheduling jitter when checking whether a re-probe is due.
const SCHEDULE_SLACK: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
struct EndpointHealth {
    /// Recent probe latencies in milliseconds, oldest first.
    samples: VecDeque<f64>,
    consecutive_errors: u32,
    unavailable: bool,
    last_probe: Option<Instant>,
}

impl EndpointHealth {
    fn p50_ms(&self) -> Option<f64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted: Vec<f64> = self.samples.iter().copied().collect();
        sorted.sort_by(f64::total_cmp);
        let mid = sorted.len() / 2;
        Some(if sorted.len() % 2 == 0 {
            (sorted[mid - 1] + sorted[mid]) / 2.0
        } else {
            sorted[mid]
        })
    }
}

/// Health and latency state for a fixed, ordered set of RPC endpoints.
///
/// Configuration order (primary first) breaks ties, so before the first probe
/// completes requests behave exactly like the old primary-then-fallbacks order.
#[derive(Debug)]
pub struct RpcEndpoints {
    urls: Vec<String>,
    labels: Vec<String>,
    health: Vec<Mutex<EndpointHealth>>,
}

impl RpcEndpoints {
    /// Build from an ordered URL list, dropping duplicates.
    ///
    /// # Panics
    /// If `urls` is empty — config validation guarantees at least one URL.
    pub fn new(urls: Vec<String>) -> Self {
        let mut unique: Vec<String> = Vec::with_capacity(urls.len());
        for url in urls {
            if !unique.contains(&url) {
                unique.push(url);
            }
        }
        assert!(!unique.is_empty(), "at least one RPC endpoint is required");

        let labels = unique.iter().map(|u| endpoint_label(u)).collect();
        let health = unique.iter().map(|_| Mutex::default()).collect();
        let endpoints = Self { urls: unique, labels, health };
        for idx in 0..endpoints.len() {
            metrics::set_rpc_endpoint_available(endpoints.label(idx), true);
        }
        endpoints
    }

    pub fn len(&self) -> usize {
        self.urls.len()
    }

    pub fn is_empty(&self) -> bool {
        self.urls.is_empty()
    }

    pub fn url(&self, idx: usize) -> &str {
        &self.urls[idx]
    }

    /// Redacted label (scheme, host and port) safe for logs and metrics.
    pub fn label(&self, idx: usize) -> &str {
        &self.labels[idx]
    }

    fn lock(&self, idx: usize) -> std::sync::MutexGuard<'_, EndpointHealth> {
        // A panic while holding the lock cannot leave the state inconsistent
        // in a way that matters for routing, so recover from poisoning.
        self.health[idx].lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Median of the recent probe latencies, if any have been recorded.
    pub fn p50_latency_ms(&self, idx: usize) -> Option<f64> {
        self.lock(idx).p50_ms()
    }

    pub fn is_available(&self, idx: usize) -> bool {
        !self.lock(idx).unavailable
    }

    /// Endpoint indices in the order requests should try them.
    ///
    /// Available endpoints come first: those without a recent error before
    /// those with one, then by ascending p50 latency (unmeasured last), then
    /// configuration order. Unavailable endpoints are appended as a last
    /// resort so a request still has somewhere to go if every endpoint is down.
    pub fn candidates(&self) -> Vec<usize> {
        let mut keyed: Vec<(bool, bool, f64, usize)> = (0..self.len())
            .map(|idx| {
                let h = self.lock(idx);
                (
                    h.unavailable,
                    h.consecutive_errors > 0,
                    h.p50_ms().unwrap_or(f64::INFINITY),
                    idx,
                )
            })
            .collect();
        keyed.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then(a.2.total_cmp(&b.2))
                .then(a.3.cmp(&b.3))
        });
        keyed.into_iter().map(|(.., idx)| idx).collect()
    }

    /// The endpoint requests are currently routed to.
    pub fn best(&self) -> usize {
        self.candidates()[0]
    }

    /// Record a successful request: clears the error streak and revives the
    /// endpoint if it was unavailable.
    pub fn record_success(&self, idx: usize) {
        let revived = {
            let mut h = self.lock(idx);
            h.consecutive_errors = 0;
            std::mem::replace(&mut h.unavailable, false)
        };
        if revived {
            info!(endpoint = %self.label(idx), "RPC endpoint is available again");
            metrics::set_rpc_endpoint_available(self.label(idx), true);
        }
    }

    /// Record a successful probe that took `latency`.
    pub fn record_latency(&self, idx: usize, latency: Duration) {
        let p50 = {
            let mut h = self.lock(idx);
            if h.samples.len() == LATENCY_WINDOW {
                h.samples.pop_front();
            }
            h.samples.push_back(latency.as_secs_f64() * 1000.0);
            h.p50_ms()
        };
        if let Some(p50) = p50 {
            metrics::set_rpc_endpoint_latency_ms(self.label(idx), p50);
        }
        self.record_success(idx);
    }

    /// Record a failed request or probe. Returns `true` if this failure made
    /// the endpoint unavailable.
    pub fn record_failure(&self, idx: usize) -> bool {
        let newly_unavailable = {
            let mut h = self.lock(idx);
            h.consecutive_errors = h.consecutive_errors.saturating_add(1);
            if !h.unavailable && h.consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                h.unavailable = true;
                // Old samples describe a healthy endpoint; start fresh on revival.
                h.samples.clear();
                true
            } else {
                false
            }
        };
        if newly_unavailable {
            warn!(
                endpoint = %self.label(idx),
                errors = MAX_CONSECUTIVE_ERRORS,
                "RPC endpoint marked unavailable; re-probing every {}s",
                UNAVAILABLE_REPROBE_INTERVAL.as_secs()
            );
            metrics::set_rpc_endpoint_available(self.label(idx), false);
        }
        newly_unavailable
    }

    /// Whether endpoint `idx` should be probed at `now`, given that available
    /// endpoints are probed every [`PROBE_INTERVAL`] and unavailable ones every
    /// [`UNAVAILABLE_REPROBE_INTERVAL`].
    pub fn due_for_probe(&self, idx: usize, now: Instant) -> bool {
        let h = self.lock(idx);
        let interval = if h.unavailable {
            UNAVAILABLE_REPROBE_INTERVAL
        } else {
            PROBE_INTERVAL
        };
        match h.last_probe {
            None => true,
            Some(last) => now.saturating_duration_since(last) + SCHEDULE_SLACK >= interval,
        }
    }

    fn mark_probed(&self, idx: usize, at: Instant) {
        self.lock(idx).last_probe = Some(at);
    }
}

/// `scheme://host[:port]` of `url`, without credentials, path or query.
pub fn endpoint_label(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(u) => match (u.host_str(), u.port()) {
            (Some(host), Some(port)) => format!("{}://{host}:{port}", u.scheme()),
            (Some(host), None) => format!("{}://{host}", u.scheme()),
            (None, _) => "invalid".to_string(),
        },
        Err(_) => "invalid".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Background probe
// ---------------------------------------------------------------------------

/// Background task that measures endpoint latency and revives endpoints.
pub struct RpcLatencyProbe {
    client: reqwest::Client,
    headers: reqwest::header::HeaderMap,
    endpoints: Arc<RpcEndpoints>,
}

impl RpcLatencyProbe {
    pub fn new(
        client: reqwest::Client,
        headers: reqwest::header::HeaderMap,
        endpoints: Arc<RpcEndpoints>,
    ) -> Self {
        Self { client, headers, endpoints }
    }

    /// Probe every [`PROBE_INTERVAL`] until `shutdown` flips to `true`.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(PROBE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => self.probe_due(Instant::now()).await,
                res = shutdown.changed() => {
                    if res.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    /// Probe every endpoint that is due, concurrently.
    pub async fn probe_due(&self, now: Instant) {
        let due: Vec<usize> = (0..self.endpoints.len())
            .filter(|&idx| self.endpoints.due_for_probe(idx, now))
            .collect();
        futures::future::join_all(due.into_iter().map(|idx| self.probe_one(idx, now))).await;
    }

    async fn probe_one(&self, idx: usize, now: Instant) {
        self.endpoints.mark_probed(idx, now);
        let started = Instant::now();
        match self.ping(self.endpoints.url(idx)).await {
            Ok(()) => self.endpoints.record_latency(idx, started.elapsed()),
            Err(e) => {
                warn!(endpoint = %self.endpoints.label(idx), error = %e, "RPC latency probe failed");
                self.endpoints.record_failure(idx);
            }
        }
    }

    async fn ping(&self, url: &str) -> Result<(), String> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": "getLatestLedger" });
        let resp: serde_json::Value = self
            .client
            .post(url)
            .headers(self.headers.clone())
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        if resp.get("result").is_some_and(|r| !r.is_null()) {
            Ok(())
        } else {
            Err("getLatestLedger returned no result".to_string())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoints(n: usize) -> RpcEndpoints {
        RpcEndpoints::new((0..n).map(|i| format!("https://rpc{i}.example.com")).collect())
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn deduplicates_and_keeps_config_order_before_probes() {
        let e = RpcEndpoints::new(vec![
            "https://a.example.com".into(),
            "https://b.example.com".into(),
            "https://a.example.com".into(),
        ]);
        assert_eq!(e.len(), 2);
        assert_eq!(e.candidates(), vec![0, 1]);
    }

    #[test]
    fn p50_is_median_of_window() {
        let e = endpoints(1);
        for l in [50, 10, 30] {
            e.record_latency(0, ms(l));
        }
        assert_eq!(e.p50_latency_ms(0), Some(30.0));
        e.record_latency(0, ms(40));
        assert_eq!(e.p50_latency_ms(0), Some(35.0));
    }

    #[test]
    fn window_drops_oldest_samples() {
        let e = endpoints(1);
        for _ in 0..LATENCY_WINDOW {
            e.record_latency(0, ms(1000));
        }
        for _ in 0..LATENCY_WINDOW {
            e.record_latency(0, ms(10));
        }
        assert_eq!(e.p50_latency_ms(0), Some(10.0));
    }

    #[test]
    fn routes_to_lowest_latency_endpoint() {
        let e = endpoints(3);
        e.record_latency(0, ms(300));
        e.record_latency(1, ms(50));
        e.record_latency(2, ms(120));
        assert_eq!(e.candidates(), vec![1, 2, 0]);
        assert_eq!(e.best(), 1);
    }

    #[test]
    fn unmeasured_endpoints_sort_after_measured_ones() {
        let e = endpoints(2);
        e.record_latency(1, ms(500));
        assert_eq!(e.candidates(), vec![1, 0]);
    }

    #[test]
    fn single_error_demotes_but_does_not_disable() {
        let e = endpoints(2);
        e.record_latency(0, ms(10));
        e.record_latency(1, ms(100));
        assert!(!e.record_failure(0));
        assert!(e.is_available(0));
        assert_eq!(e.candidates(), vec![1, 0]);
    }

    #[test]
    fn three_consecutive_errors_mark_unavailable_and_fail_over() {
        let e = endpoints(2);
        e.record_latency(0, ms(10));
        e.record_latency(1, ms(100));
        assert!(!e.record_failure(0));
        assert!(!e.record_failure(0));
        assert!(e.record_failure(0), "third failure must disable the endpoint");
        assert!(!e.record_failure(0), "only reported once");
        assert!(!e.is_available(0));
        assert_eq!(e.p50_latency_ms(0), None, "stale samples are discarded");
        // Unavailable endpoints remain as a last resort.
        assert_eq!(e.candidates(), vec![1, 0]);
    }

    #[test]
    fn success_resets_error_streak() {
        let e = endpoints(1);
        e.record_failure(0);
        e.record_failure(0);
        e.record_success(0);
        assert!(!e.record_failure(0));
        assert!(!e.record_failure(0));
        assert!(e.is_available(0));
    }

    #[test]
    fn successful_probe_revives_endpoint() {
        let e = endpoints(2);
        for _ in 0..MAX_CONSECUTIVE_ERRORS {
            e.record_failure(0);
        }
        e.record_latency(1, ms(100));
        e.record_latency(0, ms(20));
        assert!(e.is_available(0));
        assert_eq!(e.best(), 0);
    }

    #[test]
    fn probe_schedule_depends_on_availability() {
        let e = endpoints(2);
        let t0 = Instant::now();
        assert!(e.due_for_probe(0, t0), "never probed");

        e.mark_probed(0, t0);
        assert!(!e.due_for_probe(0, t0 + Duration::from_secs(10)));
        assert!(e.due_for_probe(0, t0 + PROBE_INTERVAL));

        for _ in 0..MAX_CONSECUTIVE_ERRORS {
            e.record_failure(1);
        }
        e.mark_probed(1, t0);
        assert!(!e.due_for_probe(1, t0 + PROBE_INTERVAL));
        assert!(e.due_for_probe(1, t0 + UNAVAILABLE_REPROBE_INTERVAL));
    }

    #[test]
    fn label_strips_credentials_path_and_query() {
        assert_eq!(
            endpoint_label("https://user:pw@rpc.example.com/v1/SECRET?key=x"),
            "https://rpc.example.com"
        );
        assert_eq!(endpoint_label("http://localhost:8000/"), "http://localhost:8000");
        assert_eq!(endpoint_label("not a url"), "invalid");
    }
}
