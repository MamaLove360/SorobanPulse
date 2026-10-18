# Soroban Pulse Integration Guide

This document provides comprehensive guidance for setting up and using the four new platform integrations for event notifications.

## Table of Contents

- [GitHub Integration](#github-integration)
- [Discord Integration](#discord-integration)
- [Slack Integration](#slack-integration)
- [Telegram Integration](#telegram-integration)

## GitHub Integration

### Issue #674

The GitHub integration enables automatic issue creation and PR comments for Soroban contract events.

### Features

- **Automatic Issue Creation**: Create GitHub issues for specified event types
- **PR Comments**: Add comments to pull requests with event details
- **Custom Templates**: Use customizable templates for issue titles and descriptions
- **OAuth Support**: Secure token-based authentication with GitHub

### Setup

#### 1. Create GitHub OAuth Application

1. Go to GitHub Settings → Developer settings → OAuth Apps
2. Click "New OAuth App"
3. Fill in the form:
   - **Application name**: Soroban Pulse
   - **Homepage URL**: `https://your-domain.com`
   - **Authorization callback URL**: `https://your-domain.com/integrations/github/callback`
4. Copy the Client ID and Client Secret

#### 2. Setup Integration via API

```bash
curl -X POST http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/github \
  -H "Content-Type: application/json" \
  -d '{
    "access_token": "github_pat_...",
    "owner": "your-github-org",
    "repository": "your-repo",
    "issue_title_template": "Soroban Event: {event_type}",
    "issue_body_template": "Event Data: {event_data}",
    "auto_create_issues": true,
    "pr_comment_enabled": true
  }'
```

### Configuration Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `access_token` | String | Yes | GitHub Personal Access Token with repo scope |
| `owner` | String | Yes | GitHub repository owner (user or org) |
| `repository` | String | Yes | Repository name |
| `issue_title_template` | String | No | Custom issue title template |
| `issue_body_template` | String | No | Custom issue body template |
| `auto_create_issues` | Boolean | No | Automatically create issues (default: true) |
| `pr_comment_enabled` | Boolean | No | Enable PR comments (default: false) |

### Security

- Access tokens are stored encrypted in the database
- Tokens are never exposed in API responses
- Use fine-grained personal access tokens with minimal required scopes

## Discord Integration

### Issue #675

The Discord integration sends formatted notifications to Discord channels using webhooks.

### Features

- **Webhook Support**: Send messages via Discord webhooks
- **Embedded Messages**: Rich formatting using Discord embeds
- **Custom Bot Names**: Customize bot display name and avatar
- **Thread Support**: Optional message threading for organization
- **Color-Coded Events**: Different colors for event types

### Setup

#### 1. Create Discord Webhook

1. Go to your Discord server → Channel Settings → Integrations
2. Click "Create Webhook"
3. Customize the webhook name and avatar
4. Copy the webhook URL

#### 2. Setup Integration via API

```bash
curl -X POST http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/discord \
  -H "Content-Type: application/json" \
  -d '{
    "webhook_url": "https://discord.com/api/webhooks/...",
    "bot_name": "Soroban Pulse",
    "avatar_url": "https://example.com/avatar.png",
    "embed_enabled": true,
    "thread_support": false
  }'
```

### Configuration Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `webhook_url` | String | Yes | Discord webhook URL |
| `bot_name` | String | No | Custom bot display name |
| `avatar_url` | String | No | URL to custom bot avatar |
| `embed_enabled` | Boolean | No | Use Discord embeds (default: true) |
| `thread_support` | Boolean | No | Use message threads (default: false) |

### Message Format

Notifications are sent as embeds with:
- Event type as title
- Contract ID in description
- Transaction hash, ledger, and timestamp as fields
- Event data in a code block
- Color-coded by event type (Blue: contract, Orange: diagnostic, Red: system)

### Security

- Webhook URLs are stored encrypted
- Only POST permissions are used
- Webhook can be restricted to specific channels

## Slack Integration

### Issue #676

The Slack integration sends rich notifications to Slack channels using Block Kit formatting.

### Features

- **Block Kit Formatting**: Advanced message layout using Slack Block Kit
- **Interactive Buttons**: Add actionable buttons to notifications
- **User Mentions**: Automatically mention users in notifications
- **Thread Support**: Optional message threading
- **OAuth Integration**: Secure app installation with fine-grained permissions

### Setup

#### 1. Create Slack App

1. Go to https://api.slack.com/apps
2. Click "Create New App" → "From scratch"
3. Name your app: "Soroban Pulse"
4. Select your workspace

#### 2. Configure Bot Token Scopes

In your app settings, go to "OAuth & Permissions" and add scopes:
- `chat:write`
- `chat:write.public`
- `channels:read` (for channel lookup)
- `users:read` (for user mentions)

#### 3. Install App to Workspace

Click "Install to Workspace" and authorize the permissions.

#### 4. Setup Integration via API

```bash
curl -X POST http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/slack \
  -H "Content-Type: application/json" \
  -d '{
    "webhook_url": "https://hooks.slack.com/services/...",
    "bot_token": "xoxb-...",
    "channel": "#notifications",
    "block_kit_enabled": true,
    "thread_support": false,
    "user_mentions_enabled": false
  }'
```

### Configuration Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `webhook_url` | String | No | Slack incoming webhook URL (for webhook mode) |
| `bot_token` | String | No | Slack bot token (for app mode) |
| `channel` | String | Yes | Channel name or ID to send messages to |
| `block_kit_enabled` | Boolean | No | Use Block Kit formatting (default: true) |
| `thread_support` | Boolean | No | Use message threads (default: false) |
| `user_mentions_enabled` | Boolean | No | Enable user mentions (default: false) |

### Message Format

Notifications use Block Kit with:
- Header block with event type
- Section blocks with event details (contract, transaction, ledger, type)
- Timestamp information
- Event data in code block
- Optional button actions

### Security

- Bot tokens are stored encrypted
- Use OAuth for production deployments
- Fine-grained scopes limit permissions
- Webhooks can be rotated independently

## Telegram Integration

### Issue #677

The Telegram integration sends notifications to Telegram users and groups via Bot API.

### Features

- **User Subscriptions**: Subscribe individual users to notifications
- **Group Support**: Send notifications to Telegram groups
- **Inline Buttons**: Add interactive buttons to messages
- **Webhook Support**: Use webhooks for reliable delivery
- **Retry Logic**: Automatic retry on delivery failure
- **Message Threading**: Organize notifications in topics (Telegram groups)

### Setup

#### 1. Create Telegram Bot

1. Open Telegram and search for `@BotFather`
2. Send `/newbot` command
3. Follow prompts to name your bot
4. Copy the bot token (format: `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`)

#### 2. Get Chat ID

For groups:
1. Add your bot to the group
2. Send a message in the group
3. Call: `https://api.telegram.org/bot{TOKEN}/getUpdates`
4. Find the `chat` object's `id` field (negative for groups)

For users:
1. Start a chat with your bot
2. Send any message
3. Call `getUpdates` endpoint
4. Find the `chat` object's `id` field

#### 3. Setup Integration via API

```bash
curl -X POST http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/telegram \
  -H "Content-Type: application/json" \
  -d '{
    "bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
    "chat_id": "-1001234567890",
    "webhook_enabled": false,
    "webhook_url": null,
    "message_thread_support": false,
    "button_support": true
  }'
```

### Configuration Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `bot_token` | String | Yes | Telegram Bot API token |
| `chat_id` | String | Yes | Chat ID (user or group) |
| `webhook_enabled` | Boolean | No | Use webhooks instead of polling (default: false) |
| `webhook_url` | String | No | Webhook URL for updates (if webhook_enabled) |
| `message_thread_support` | Boolean | No | Use topic threads in groups (default: false) |
| `button_support` | Boolean | No | Add inline buttons to messages (default: true) |

### Message Format

Messages are sent with:
- Event type as title with emoji
- Contract ID in monospace
- Transaction hash in monospace
- Ledger number
- Timestamp
- Event data in JSON code block
- Optional inline buttons for actions

### Security

- Bot tokens are stored encrypted
- Never share bot tokens publicly
- Webhooks should use HTTPS
- Restrict bot permissions to specific chats

### Subscription Management

Subscribe/unsubscribe users programmatically:

```bash
# Subscribe user
curl -X POST http://localhost:8000/v1/integrations/telegram/{integration_id}/subscribe \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "123456789",
    "username": "@username"
  }'

# Unsubscribe user
curl -X POST http://localhost:8000/v1/integrations/telegram/{integration_id}/unsubscribe \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "123456789"
  }'
```

## Common Operations

### List Integrations

```bash
curl http://localhost:8000/v1/subscriptions/{subscription_id}
```

Response will include integration details for configured platforms.

### Get Integration Details

```bash
# GitHub
curl http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/github

# Discord
curl http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/discord

# Slack
curl http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/slack

# Telegram
curl http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/telegram
```

### Update Integration

To update an integration, send a POST request with new configuration:

```bash
curl -X POST http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/discord \
  -H "Content-Type: application/json" \
  -d '{
    "webhook_url": "https://discord.com/api/webhooks/new-url",
    "bot_name": "New Name"
  }'
```

### Delete Integration

```bash
curl -X DELETE http://localhost:8000/v1/subscriptions/{subscription_id}/integrations/github
```

## Error Handling

All integrations include:

- **Automatic Retries**: Failed deliveries are retried up to 3 times with exponential backoff
- **Error Logging**: All failures are logged with detailed error messages
- **Metrics Recording**: Failed deliveries are tracked in Prometheus metrics
- **Validation**: Integration tokens and URLs are validated on setup

### Metrics

Monitor integration health using Prometheus:

```
soroban_pulse_github_failures_total
soroban_pulse_discord_failures_total
soroban_pulse_slack_failures_total
soroban_pulse_telegram_failures_total
```

## Troubleshooting

### GitHub

- **401 Unauthorized**: Check token expiration and permissions
- **404 Not Found**: Verify owner and repository names
- **403 Forbidden**: Token may not have repo scope

### Discord

- **Invalid Webhook**: Verify webhook URL is current (can change)
- **429 Rate Limited**: Reduce message frequency or use threading
- **403 Forbidden**: Webhook may have been deleted

### Slack

- **token_revoked**: App token was revoked, reinstall app
- **not_in_channel**: Add bot to the channel first
- **invalid_channel**: Verify channel name or ID format

### Telegram

- **400 Bad Request**: Check chat_id format (negative for groups)
- **401 Unauthorized**: Verify bot token is correct
- **403 Forbidden**: Bot may be blocked or not member of group

## Best Practices

1. **Use Environment Variables**: Store tokens and secrets in environment variables
2. **Regular Rotation**: Periodically rotate authentication tokens
3. **Monitoring**: Set up alerts for integration failures
4. **Testing**: Send test events before production deployment
5. **Logging**: Review logs regularly for issues
6. **Permissions**: Use minimal required permissions for each platform
7. **Rate Limiting**: Be aware of platform rate limits
8. **Encryption**: Keep sensitive data encrypted at rest

## API Reference

See the OpenAPI documentation at `/v1/docs` for detailed endpoint specifications.

## Support

For issues or feature requests, please open an issue on GitHub:
https://github.com/Soroban-Pulse/SorobanPulse/issues
