# Discord integration

Implemented in `src/discord.rs` (`DiscordClient`).

## Configuration

```rust
DiscordConfig {
    webhook_url: String,          // Incoming Webhook URL
    bot_name: Option<String>,     // Overrides the webhook's default username
    avatar_url: Option<String>,   // Overrides the webhook's default avatar
    bot_token: Option<String>,    // Bot token, for role mentions via the Bot API
    channel_id: Option<String>,   // Channel ID, required alongside bot_token
}
```

## Sending notifications

- `send_event_notification(event, thread_id)` — posts a rich embed (title,
  fields for contract ID / tx hash / ledger / timestamp, and event data as a
  code block when present) via the webhook. Color-coded by `EventType`.
  `thread_id`, when set, posts into an existing thread using Discord's
  `?thread_id=` webhook query parameter — **not** a JSON body field, which
  Discord silently ignores.
- `send_message(content)` — a plain text message via the same webhook.
- `send_with_retry(event, max_retries)` — exponential backoff (1s, 2s, 4s,
  ...) around `send_event_notification`, recording
  `soroban_pulse_discord_failures_total` if every attempt fails.
- `NotificationChannel` — `DiscordClient` implements the shared channel trait
  (3 attempts); see [notification-channel-interface.md](notification-channel-interface.md).

## Role mentions (Bot API)

Discord webhooks can't reliably ping roles across every server config, so
role mentions go through the Bot API instead:
`send_message_with_role_mentions(content, role_ids)` posts to
`POST /channels/{channel_id}/messages` with `Authorization: Bot {bot_token}`,
prefixing `content` with `<@&role_id>` for each ID and listing them under
`allowed_mentions.roles` — Discord silently drops a `<@&id>` mention that
isn't also allow-listed there. Requires `bot_token` and `channel_id`.

## Known limitations

- `update_message` always returns an error: Discord webhooks don't support
  editing a previously sent message. Editing requires the Bot API
  (`PATCH /webhooks/{id}/{token}/messages/{message_id}`), not yet
  implemented.
- Color coding currently has four buckets (contract/diagnostic/system/
  default); there's no per-event custom color override.
