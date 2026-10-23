# Notification Channel Interface

Every outbound notification integration implements a single trait,
`NotificationChannel` (`src/notification_channel.rs`):

```rust
#[async_trait::async_trait]
pub trait NotificationChannel: Send + Sync {
    /// Stable identifier used in logs and metrics ("slack", "pagerduty", ...).
    fn channel_name(&self) -> &'static str;

    /// Channel-specific filtering; defaults to accepting every event.
    fn accepts(&self, _event: &SorobanEvent) -> bool { true }

    /// Deliver (with the channel's own retries) and return a provider receipt.
    async fn send_event_notification(&self, event: &SorobanEvent)
        -> Result<String, NotificationError>;
}
```

Implemented by `SlackClient`, `DiscordClient`, `TelegramClient`, `TeamsClient`,
`GitHubClient`, `PagerDutyClient` (filters via `should_trigger`) and
`SmsNotifier` (contract filter + critical-event check).

## Dispatch

`NotificationDispatcher` fans each event out to every registered channel
concurrently. A failing channel never blocks the others, and every attempt
records `soroban_pulse_notification_delivery_{success,failure}_total` and
the per-channel delivery latency histogram.

```rust
let dispatcher = NotificationDispatcher::new()
    .with_channel(Arc::new(sms_notifier))
    .with_channel(Arc::new(slack_client));
dispatcher.spawn(event_tx.subscribe());
```

`src/main.rs` builds the dispatcher from the environment-configured channels
(currently SMS via `TWILIO_*`) and subscribes it to the indexer's event
broadcast channel.

## Adding a channel

1. Implement `NotificationChannel` for the new client.
2. Register it with the dispatcher where it is configured.

The dispatcher itself never needs to change.

Email, SMS and push additionally implement `notification_delivery::DeliveryChannel`,
the lower-level single-target delivery primitive driven by `deliver_with_retry`.
