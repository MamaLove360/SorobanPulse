# Microsoft Teams integration

Implemented in `src/teams.rs` (`TeamsClient`).

## Configuration

```rust
TeamsConfig {
    webhook_url: String, // Incoming Webhook connector URL
}
```

Create the webhook via a Teams channel's **Connectors** (or the newer
**Workflows** app for "Post to a channel when a webhook request is
received") and paste the resulting URL in as `webhook_url`.

## Sending notifications

`send_event_notification(event, mentions, actions)` posts an [Adaptive
Card](https://adaptivecards.io/) (schema 1.4) containing:

- A title `TextBlock`, colored by `EventType` (contract = Accent,
  diagnostic = Warning, system = Attention)
- A `FactSet` with contract ID, event type, transaction hash, ledger, and
  timestamp
- Event data as a monospace code block, when present

`send_with_retry(event, mentions, actions, max_retries)` wraps this with
exponential backoff (1s, 2s, 4s, ...), recording
`soroban_pulse_teams_failures_total` if every attempt fails.
`TeamsClient` implements `NotificationChannel` (3 attempts, no mentions or
actions) so it can be registered with the `NotificationDispatcher`; see
[notification-channel-interface.md](notification-channel-interface.md).

## User mentions

Teams doesn't support a plain `<@id>` mention syntax — mentions are
structured entities attached to the card and referenced by a
`<at>Name</at>` placeholder in the card body text. Pass one or more
`TeamsMention { aad_object_id, display_name }` (the user's Azure AD object
ID and display name); `send_event_notification` appends a `<at>Name</at>`
text block and the corresponding `msteams.entities` mention entry for
each.

## Action buttons

Pass `TeamsAction { title, url }` entries to add `Action.OpenUrl` buttons
to the card — e.g. linking to the contract's explorer page or the dashboard
event detail view.

## Not implemented

- **App registration / bot framework**: this integration is
  incoming-webhook only (like the Slack/Discord webhook paths). A full
  Teams app registration (Azure Bot Service, manifest, tenant
  admin-consent flow) is a substantially larger undertaking than a webhook
  URL and is not implemented here.
- **Channel management** (listing/creating channels via Microsoft Graph)
  requires the app-registration flow above and app-level Graph API
  permissions; not implemented.
