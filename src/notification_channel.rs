//! Unified notification channel interface (issues #809, #1245).
//!
//! Every outbound notification integration — Slack, Discord, Telegram, Teams,
//! SMS, PagerDuty and GitHub — implements [`NotificationChannel`]. The
//! [`NotificationDispatcher`] fans an event out to every registered channel,
//! so adding a new integration means implementing this one trait; the
//! dispatcher itself never needs editing.
//!
//! Channel-specific capabilities (Slack threads, Telegram buttons, PagerDuty
//! acknowledge/resolve, ...) remain available directly on each client; the
//! trait covers the shared "deliver this event" path.
//!
//! The trait operates on [`SorobanEvent`], the type carried by the indexer's
//! event broadcast channel. Clients that format the persisted
//! [`crate::models::Event`] model convert via `Event::from(&SorobanEvent)`.

use std::sync::Arc;
use std::time::Instant;

use futures::future::join_all;
use tokio::sync::broadcast;
use tracing::{debug, error, warn};

use crate::models::{Event, SorobanEvent};
use crate::notification_delivery::NotificationError;

/// Attempts each HTTP-based channel makes before reporting a failure. Matches
/// the retry count the per-module `deliver_*` helpers used before #1245.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn transient(e: BoxError) -> NotificationError {
    NotificationError::Transient(e.to_string())
}

/// A destination that can be notified about indexed events.
#[async_trait::async_trait]
pub trait NotificationChannel: Send + Sync {
    /// Short, stable identifier used in logs and metrics (e.g. "slack").
    fn channel_name(&self) -> &'static str;

    /// Channel-specific filtering (contract / event-type allow-lists, ...).
    /// Events for which this returns `false` are skipped by the dispatcher.
    fn accepts(&self, _event: &SorobanEvent) -> bool {
        true
    }

    /// Deliver a notification for `event`, retrying as appropriate for the
    /// channel, and return a provider-specific receipt (message id, incident
    /// dedup key, ...) on success.
    async fn send_event_notification(&self, event: &SorobanEvent) -> Result<String, NotificationError>;
}

/// Result of delivering one event to one channel.
#[derive(Debug)]
pub struct ChannelOutcome {
    pub channel: &'static str,
    pub result: Result<String, NotificationError>,
}

/// Fans events out to every registered [`NotificationChannel`].
#[derive(Clone, Default)]
pub struct NotificationDispatcher {
    channels: Vec<Arc<dyn NotificationChannel>>,
}

impl NotificationDispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a channel. Builder-style for chaining at startup.
    #[must_use]
    pub fn with_channel(mut self, channel: Arc<dyn NotificationChannel>) -> Self {
        self.channels.push(channel);
        self
    }

    pub fn register(&mut self, channel: Arc<dyn NotificationChannel>) {
        self.channels.push(channel);
    }

    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    pub fn channel_names(&self) -> Vec<&'static str> {
        self.channels.iter().map(|c| c.channel_name()).collect()
    }

    /// Deliver `event` to every channel that accepts it, concurrently.
    ///
    /// A failing channel never affects the others; each failure is logged and
    /// counted, and every attempted delivery is returned to the caller.
    pub async fn dispatch(&self, event: &SorobanEvent) -> Vec<ChannelOutcome> {
        let deliveries = self
            .channels
            .iter()
            .filter(|channel| channel.accepts(event))
            .map(|channel| async move {
                let name = channel.channel_name();
                let start = Instant::now();
                let result = channel.send_event_notification(event).await;
                crate::metrics::record_notification_delivery_latency(name, start.elapsed().as_secs_f64());
                match &result {
                    Ok(receipt) => {
                        crate::metrics::record_notification_delivery_success();
                        debug!(channel = name, receipt = %receipt, contract_id = %event.contract_id, "Notification delivered");
                    }
                    Err(e) => {
                        crate::metrics::record_notification_delivery_failure();
                        error!(
                            channel = name,
                            error = %e,
                            contract_id = %event.contract_id,
                            event_type = %event.event_type,
                            "Failed to deliver notification"
                        );
                    }
                }
                ChannelOutcome { channel: name, result }
            });
        join_all(deliveries).await
    }

    /// Consume events from the indexer broadcast channel and dispatch each one
    /// until the channel closes.
    pub fn spawn(self, mut event_rx: broadcast::Receiver<SorobanEvent>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match event_rx.recv().await {
                    Ok(event) => {
                        self.dispatch(&event).await;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "Notification dispatcher lagged; some events were not delivered");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Channel implementations
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl NotificationChannel for crate::slack::SlackClient {
    fn channel_name(&self) -> &'static str {
        "slack"
    }

    async fn send_event_notification(&self, event: &SorobanEvent) -> Result<String, NotificationError> {
        self.send_with_retry(&Event::from(event), DEFAULT_MAX_RETRIES)
            .await
            .map_err(transient)
    }
}

#[async_trait::async_trait]
impl NotificationChannel for crate::discord::DiscordClient {
    fn channel_name(&self) -> &'static str {
        "discord"
    }

    async fn send_event_notification(&self, event: &SorobanEvent) -> Result<String, NotificationError> {
        self.send_with_retry(&Event::from(event), DEFAULT_MAX_RETRIES)
            .await
            .map_err(transient)
    }
}

#[async_trait::async_trait]
impl NotificationChannel for crate::telegram::TelegramClient {
    fn channel_name(&self) -> &'static str {
        "telegram"
    }

    async fn send_event_notification(&self, event: &SorobanEvent) -> Result<String, NotificationError> {
        self.send_with_retry(&Event::from(event), DEFAULT_MAX_RETRIES)
            .await
            .map_err(transient)
    }
}

#[async_trait::async_trait]
impl NotificationChannel for crate::teams::TeamsClient {
    fn channel_name(&self) -> &'static str {
        "teams"
    }

    async fn send_event_notification(&self, event: &SorobanEvent) -> Result<String, NotificationError> {
        // Teams incoming webhooks return no message id.
        self.send_with_retry(&Event::from(event), &[], &[], DEFAULT_MAX_RETRIES)
            .await
            .map(|()| String::new())
            .map_err(transient)
    }
}

#[async_trait::async_trait]
impl NotificationChannel for crate::github::GitHubClient {
    fn channel_name(&self) -> &'static str {
        "github"
    }

    async fn send_event_notification(&self, event: &SorobanEvent) -> Result<String, NotificationError> {
        self.send_with_retry(event, DEFAULT_MAX_RETRIES)
            .await
            .map_err(transient)
    }
}

#[async_trait::async_trait]
impl NotificationChannel for crate::pagerduty::PagerDutyClient {
    fn channel_name(&self) -> &'static str {
        "pagerduty"
    }

    fn accepts(&self, event: &SorobanEvent) -> bool {
        self.should_trigger(event)
    }

    /// Returns the incident dedup key. `trigger_incident` retries internally.
    async fn send_event_notification(&self, event: &SorobanEvent) -> Result<String, NotificationError> {
        self.trigger_incident(event, None, None).await.map_err(transient)
    }
}

// The SMS implementation lives in `crate::sms` because it needs access to
// the notifier's private Twilio plumbing.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_event() -> SorobanEvent {
        SorobanEvent {
            contract_id: "CCONTRACTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string(),
            event_type: "contract".to_string(),
            tx_hash: "ab".repeat(32),
            ledger: 1234,
            ledger_closed_at: "2026-01-01T00:00:00Z".to_string(),
            value: serde_json::json!({ "amount": 10 }),
            ..SorobanEvent::default()
        }
    }

    /// In-memory channel used to exercise the dispatcher contract.
    struct MockChannel {
        name: &'static str,
        accept: bool,
        fail: bool,
        calls: AtomicUsize,
    }

    impl MockChannel {
        fn new(name: &'static str, accept: bool, fail: bool) -> Arc<Self> {
            Arc::new(Self { name, accept, fail, calls: AtomicUsize::new(0) })
        }
    }

    #[async_trait::async_trait]
    impl NotificationChannel for MockChannel {
        fn channel_name(&self) -> &'static str {
            self.name
        }

        fn accepts(&self, _event: &SorobanEvent) -> bool {
            self.accept
        }

        async fn send_event_notification(&self, _event: &SorobanEvent) -> Result<String, NotificationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(NotificationError::Transient("boom".to_string()))
            } else {
                Ok(format!("{}-receipt", self.name))
            }
        }
    }

    #[tokio::test]
    async fn dispatcher_delivers_to_every_accepting_channel() {
        let a = MockChannel::new("a", true, false);
        let b = MockChannel::new("b", true, false);
        let skipped = MockChannel::new("skipped", false, false);
        let dispatcher = NotificationDispatcher::new()
            .with_channel(a.clone())
            .with_channel(b.clone())
            .with_channel(skipped.clone());

        let outcomes = dispatcher.dispatch(&sample_event()).await;

        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(|o| o.result.is_ok()));
        assert_eq!(a.calls.load(Ordering::SeqCst), 1);
        assert_eq!(b.calls.load(Ordering::SeqCst), 1);
        assert_eq!(skipped.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dispatcher_isolates_failing_channels() {
        let ok = MockChannel::new("ok", true, false);
        let bad = MockChannel::new("bad", true, true);
        let dispatcher = NotificationDispatcher::new().with_channel(bad).with_channel(ok);

        let outcomes = dispatcher.dispatch(&sample_event()).await;

        let ok_outcome = outcomes.iter().find(|o| o.channel == "ok").unwrap();
        let bad_outcome = outcomes.iter().find(|o| o.channel == "bad").unwrap();
        assert_eq!(ok_outcome.result.as_deref().unwrap(), "ok-receipt");
        assert!(matches!(bad_outcome.result, Err(NotificationError::Transient(_))));
    }

    #[tokio::test]
    async fn empty_dispatcher_is_a_no_op() {
        let dispatcher = NotificationDispatcher::new();
        assert!(dispatcher.is_empty());
        assert!(dispatcher.dispatch(&sample_event()).await.is_empty());
    }

    #[tokio::test]
    async fn spawned_dispatcher_drains_broadcast_channel() {
        let channel = MockChannel::new("mock", true, false);
        let (tx, rx) = broadcast::channel(8);
        let handle = NotificationDispatcher::new().with_channel(channel.clone()).spawn(rx);
        tx.send(sample_event()).unwrap();
        tx.send(sample_event()).unwrap();
        drop(tx);
        handle.await.unwrap();
        assert_eq!(channel.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn slack_channel_posts_to_webhook() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "ts": "1700000000.0001" })))
            .expect(1)
            .mount(&server)
            .await;
        let slack = crate::slack::SlackClient::new(crate::slack::SlackConfig {
            webhook_url: Some(server.uri()),
            bot_token: None,
            channel: "#alerts".to_string(),
        })
        .unwrap();

        let channel: &dyn NotificationChannel = &slack;
        assert_eq!(channel.channel_name(), "slack");
        let receipt = channel.send_event_notification(&sample_event()).await.unwrap();
        assert_eq!(receipt, "1700000000.0001");
    }

    #[tokio::test]
    async fn slack_channel_reports_transient_error_after_retries() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let slack = crate::slack::SlackClient::new(crate::slack::SlackConfig {
            webhook_url: Some(server.uri()),
            bot_token: None,
            channel: "#alerts".to_string(),
        })
        .unwrap();

        let result = NotificationChannel::send_event_notification(&slack, &sample_event()).await;
        assert!(matches!(result, Err(NotificationError::Transient(_))));
    }

    #[test]
    fn discord_channel_name() {
        let discord = crate::discord::DiscordClient::new(crate::discord::DiscordConfig {
            webhook_url: "https://discord.com/api/webhooks/1/abc".to_string(),
            bot_name: None,
            avatar_url: None,
            bot_token: None,
            channel_id: None,
        })
        .unwrap();
        assert_eq!(NotificationChannel::channel_name(&discord), "discord");
    }

    #[test]
    fn telegram_channel_name() {
        let telegram = crate::telegram::TelegramClient::new(crate::telegram::TelegramConfig {
            bot_token: "123:abc".to_string(),
            chat_id: "-100".to_string(),
        })
        .unwrap();
        assert_eq!(NotificationChannel::channel_name(&telegram), "telegram");
    }

    #[test]
    fn teams_channel_name() {
        let teams = crate::teams::TeamsClient::new(crate::teams::TeamsConfig {
            webhook_url: "https://example.webhook.office.com/hook".to_string(),
        })
        .unwrap();
        assert_eq!(NotificationChannel::channel_name(&teams), "teams");
    }

    #[test]
    fn github_channel_name() {
        let github =
            crate::github::GitHubClient::new("token".to_string(), "owner".to_string(), "repo".to_string()).unwrap();
        assert_eq!(NotificationChannel::channel_name(&github), "github");
    }

    #[test]
    fn pagerduty_channel_applies_contract_filter() {
        let event = sample_event();
        let matching = crate::pagerduty::PagerDutyClient::new(crate::pagerduty::PagerDutyConfig {
            routing_key: "key".to_string(),
            contract_filter: vec![event.contract_id.clone()],
            ..crate::pagerduty::PagerDutyConfig::default()
        })
        .unwrap();
        let other = crate::pagerduty::PagerDutyClient::new(crate::pagerduty::PagerDutyConfig {
            routing_key: "key".to_string(),
            contract_filter: vec!["CSOMETHINGELSE".to_string()],
            ..crate::pagerduty::PagerDutyConfig::default()
        })
        .unwrap();

        assert_eq!(NotificationChannel::channel_name(&matching), "pagerduty");
        assert!(NotificationChannel::accepts(&matching, &event));
        assert!(!NotificationChannel::accepts(&other, &event));
    }

    #[test]
    fn soroban_event_converts_to_event_model() {
        let event = Event::from(&sample_event());
        assert_eq!(event.event_type, crate::models::EventType::Contract);
        assert_eq!(event.ledger, 1234);
        assert_eq!(event.timestamp.to_rfc3339(), "2026-01-01T00:00:00+00:00");
        assert_eq!(event.event_data["amount"], 10);
    }
}
