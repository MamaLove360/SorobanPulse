# Slack integration

Implemented in `src/slack.rs` (`SlackClient`).

## Configuration

```rust
SlackConfig {
    webhook_url: Option<String>, // Incoming Webhook URL
    bot_token: Option<String>,   // Bot User OAuth Token (xoxb-...), for chat.postMessage
    channel: String,             // e.g. "#notifications"
}
```

At least one of `webhook_url` or `bot_token` must be set, depending on which
send path is used.

## Sending notifications

- `send_event_notification(event, thread_ts)` — posts a Slack Block Kit
  message (header + fields + attachment) via the configured webhook URL.
  Color-codes the attachment by `EventType` (contract = blue, diagnostic =
  orange, system = red). Pass `thread_ts` (a prior message's `ts`) to reply
  in-thread instead of posting a new top-level message.
- `send_with_retry(event, max_retries)` — wraps `send_event_notification`
  with exponential backoff (1s, 2s, 4s, ...) and records
  `soroban_pulse_slack_failures_total` if every attempt fails.
- `NotificationChannel` — `SlackClient` implements the shared channel trait
  via `send_with_retry` (3 attempts); see
  [notification-channel-interface.md](notification-channel-interface.md).

## Bot API / user mentions

`send_message_with_bot(content, mention_users)` posts via
`chat.postMessage` using `bot_token`, prefixing `content` with `<@user_id>`
mentions for each entry in `mention_users`. Requires the bot to have
`chat:write` scope and be a member of the target channel.

## OAuth app installation

`SlackClient::exchange_code_for_token(oauth_config, code)` exchanges an
OAuth authorization code for a bot access token via
`https://slack.com/api/oauth.v2.access`, for the "Add to Slack" install
flow. `SlackOAuthConfig` holds `client_id`, `client_secret`, and
`redirect_uri` from your Slack app's settings.

## Known limitations

- `add_button_actions` is currently a no-op placeholder. Updating an
  already-sent message's interactive elements requires either an
  `attachments`/`blocks` update via `chat.update` (bot token) or handling
  Slack's interactivity callback endpoint — neither is wired up yet.
- Thread replies (`thread_ts`) require the *first* message's `ts`, which
  `send_event_notification` returns on success — callers are responsible
  for persisting it if they want to thread subsequent updates.
