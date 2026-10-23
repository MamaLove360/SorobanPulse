use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    config::Environment,
    metrics,
    models::{Event, EventType},
    net::outbound,
};

/// Validate a Discord webhook URL against the shared SSRF guard.
///
/// Only `discord.com` and its subdomains are allowed as the destination host.
/// Call this at channel-creation time and again just before firing the request
/// (DNS-rebinding defence via the async [`outbound::validate_outbound_url`]).
pub async fn validate_discord_webhook_url(url: &str, env: &Environment) -> crate::error::AppError {
    match outbound::validate_outbound_url(url, env, &["discord.com"]).await {
        Ok(()) => crate::error::AppError::Validation(String::new()), // placeholder – use Result in callers
        Err(e) => e,
    }
}

/// Validate a Discord webhook URL synchronously (cheap static check).
///
/// Use at storage time; combine with [`validate_discord_webhook_url`] at
/// request time for full DNS-rebinding protection.
pub fn validate_discord_webhook_url_static(url: &str, env: &Environment) -> Result<(), crate::error::AppError> {
    outbound::validate_outbound_url_static(url, env, &["discord.com"])
}

/// Discord webhook configuration
#[derive(Debug, Clone)]
pub struct DiscordConfig {
    pub webhook_url: String,
    pub bot_name: Option<String>,
    pub avatar_url: Option<String>,
    /// Bot token for the Discord Bot API (channel messages, role mentions).
    /// Webhooks alone can't mention roles by ID reliably across guilds
    /// without the bot's `MENTION_EVERYONE`/allowed_mentions permission
    /// context, so role mentions go through this instead of the webhook URL.
    pub bot_token: Option<String>,
    /// Discord channel/guild-text-channel ID, required for bot-API sends.
    pub channel_id: Option<String>,
}

/// Discord API client for webhook-based notifications
pub struct DiscordClient {
    client: Client,
    webhook_url: String,
    bot_name: Option<String>,
    avatar_url: Option<String>,
    bot_token: Option<String>,
    channel_id: Option<String>,
}

/// Prefixes `content` with `<@&role_id>` mentions for each of `role_ids`,
/// space-joined. Pulled out of `send_message_with_role_mentions` so the
/// formatting is independently testable without a live HTTP call.
fn prefix_with_role_mentions(content: &str, role_ids: &[&str]) -> String {
    if role_ids.is_empty() {
        return content.to_string();
    }
    let mention_text = role_ids
        .iter()
        .map(|role| format!("<@&{}>", role))
        .collect::<Vec<_>>()
        .join(" ");
    format!("{} {}", mention_text, content)
}

impl DiscordClient {
    pub fn new(config: DiscordConfig) -> Result<Self, crate::error::InitError> {
        // Use the shared outbound client: redirects disabled, 10-second timeout.
        // The webhook_url is validated by `validate_discord_webhook_url` at
        // creation time; we use `build_outbound_client` here so that the same
        // hardened client is used for every subsequent request.
        let client = crate::net::outbound::build_outbound_client(&["discord.com"])?;

        Ok(Self {
            client,
            webhook_url: config.webhook_url,
            bot_name: config.bot_name,
            avatar_url: config.avatar_url,
            bot_token: config.bot_token,
            channel_id: config.channel_id,
        })
    }

    /// Send a formatted message to Discord using embeds. `thread_id`, when
    /// set, posts into an existing thread — passed as the `thread_id` query
    /// parameter on the webhook execute URL, per Discord's webhook API
    /// (NOT a JSON body field, which Discord silently ignores).
    pub async fn send_event_notification(
        &self,
        event: &Event,
        thread_id: Option<String>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let color = match event.event_type {
            EventType::Contract => 3447003,   // Blue
            EventType::Diagnostic => 10181046, // Orange
            EventType::System => 15158332,     // Red
        };

        let mut embed = json!({
            "title": format!("Soroban Event: {}", event.event_type),
            "description": format!("Contract: {}", event.contract_id),
            "color": color,
            "fields": [
                {
                    "name": "Transaction Hash",
                    "value": format!("`{}`", event.tx_hash),
                    "inline": true
                },
                {
                    "name": "Ledger",
                    "value": event.ledger.to_string(),
                    "inline": true
                },
                {
                    "name": "Timestamp",
                    "value": event.timestamp.to_string(),
                    "inline": false
                },
                {
                    "name": "Event Type",
                    "value": event.event_type.to_string(),
                    "inline": true
                }
            ],
            "footer": {
                "text": "Soroban Pulse"
            }
        });

        // Add event data as a code block if available
        if !event.event_data.is_null() {
            if let Ok(data_str) = serde_json::to_string_pretty(&event.event_data) {
                embed["fields"].as_array_mut().map(|fields| {
                    fields.push(json!({
                        "name": "Event Data",
                        "value": format!("```json\n{}\n```", data_str),
                        "inline": false
                    }))
                });
            }
        }

        let mut payload = json!({
            "embeds": [embed]
        });

        if let Some(name) = &self.bot_name {
            payload["username"] = json!(name);
        }

        if let Some(avatar) = &self.avatar_url {
            payload["avatar_url"] = json!(avatar);
        }

        // Discord posts into a thread via a `?thread_id=` query parameter on
        // the webhook execute URL, not a JSON body field (the body field is
        // silently ignored by Discord's API).
        let mut request = self.client.post(&self.webhook_url);
        if let Some(thread) = &thread_id {
            request = request.query(&[("thread_id", thread.as_str())]);
        }

        let response = request.json(&payload).send().await?;

        if response.status().is_success() {
            let message_id = Uuid::new_v4().to_string();
            info!(
                webhook_url = %self.webhook_url,
                message_id = %message_id,
                thread_id = ?thread_id,
                contract_id = %event.contract_id,
                "Discord notification sent"
            );

            Ok(message_id)
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(
                status = %response.status(),
                body = %error_body,
                "Failed to send Discord notification"
            );
            Err(format!("Discord API error: {}", error_body).into())
        }
    }

    /// Send a simple text message to Discord
    pub async fn send_message(&self, content: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut payload = json!({
            "content": content
        });

        if let Some(name) = &self.bot_name {
            payload["username"] = json!(name);
        }

        let response = self
            .client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let message_id = Uuid::new_v4().to_string();
            info!(
                webhook_url = %self.webhook_url,
                message_id = %message_id,
                "Discord message sent"
            );

            Ok(message_id)
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(
                status = %response.status(),
                body = %error_body,
                "Failed to send Discord message"
            );
            Err(format!("Discord API error: {}", error_body).into())
        }
    }

    /// Send a message via the Discord Bot API (not a webhook), with role
    /// mentions. Requires `bot_token` and `channel_id` to be configured.
    pub async fn send_message_with_role_mentions(
        &self,
        content: &str,
        role_ids: Vec<&str>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let bot_token = self.bot_token.as_ref().ok_or("No bot token configured")?;
        let channel_id = self.channel_id.as_ref().ok_or("No channel ID configured")?;

        let text = prefix_with_role_mentions(content, &role_ids);

        // Discord requires roles to be explicitly whitelisted in
        // `allowed_mentions` — a bare `<@&id>` in `content` alone is not
        // pinged unless the role ID is also listed here.
        let payload = json!({
            "content": text,
            "allowed_mentions": { "roles": role_ids }
        });

        let response = self
            .client
            .post(format!("https://discord.com/api/v10/channels/{}/messages", channel_id))
            .header("Authorization", format!("Bot {}", bot_token))
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;
            let message_id = body
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or("No message id in response")?;

            info!(channel_id = %channel_id, message_id = %message_id, "Discord message sent via bot with role mentions");
            Ok(message_id)
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(status = %response.status(), body = %error_body, "Failed to send Discord bot message");
            Err(format!("Discord API error: {}", error_body).into())
        }
    }

    /// Send event to Discord with retry logic
    pub async fn send_with_retry(
        &self,
        event: &Event,
        max_retries: u32,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut backoff_ms = 1000u64;

        for attempt in 1..=max_retries {
            match self.send_event_notification(event, None).await {
                Ok(message_id) => {
                    info!(attempt = attempt, message_id = %message_id, "Discord notification sent successfully");
                    return Ok(message_id);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        attempt = attempt,
                        "Discord API request failed"
                    );
                }
            }

            if attempt < max_retries {
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2;
            }
        }

        metrics::record_discord_failure();
        Err("Discord delivery failed after retries".into())
    }

    /// Update an existing message in Discord
    pub async fn update_message(
        &self,
        message_id: &str,
        content: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Note: Discord webhooks don't support message editing directly
        // This would require using the Discord API with a bot token instead
        Err("Message editing not supported via webhooks".into())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discord_client_creation() {
        let config = DiscordConfig {
            webhook_url: "https://discord.com/api/webhooks/test".to_string(),
            bot_name: Some("Soroban Bot".to_string()),
            avatar_url: Some("https://example.com/avatar.png".to_string()),
            bot_token: None,
            channel_id: None,
        };

        let client = DiscordClient::new(config).unwrap();
        assert_eq!(client.webhook_url, "https://discord.com/api/webhooks/test");
        assert_eq!(client.bot_name, Some("Soroban Bot".to_string()));
    }

    #[test]
    fn test_discord_config_creation() {
        let config = DiscordConfig {
            webhook_url: "https://discord.com/api/webhooks/test".to_string(),
            bot_name: None,
            avatar_url: None,
            bot_token: None,
            channel_id: None,
        };

        assert_eq!(config.webhook_url, "https://discord.com/api/webhooks/test");
        assert!(config.bot_name.is_none());
    }

    #[test]
    fn test_prefix_with_role_mentions_no_roles() {
        assert_eq!(prefix_with_role_mentions("Deploy complete", &[]), "Deploy complete");
    }

    #[test]
    fn test_prefix_with_role_mentions_single_role() {
        assert_eq!(
            prefix_with_role_mentions("Deploy complete", &["123456"]),
            "<@&123456> Deploy complete"
        );
    }

    #[test]
    fn test_prefix_with_role_mentions_multiple_roles() {
        assert_eq!(
            prefix_with_role_mentions("Incident opened", &["111", "222"]),
            "<@&111> <@&222> Incident opened"
        );
    }
}
