use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    metrics,
    models::{Event, EventType},
};

/// Slack OAuth configuration
#[derive(Debug, Clone)]
pub struct SlackOAuthConfig {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
}

/// Slack App configuration
#[derive(Debug, Clone)]
pub struct SlackConfig {
    pub webhook_url: Option<String>,
    pub bot_token: Option<String>,
    pub channel: String,
}

/// Slack API client for webhook and bot-based notifications
pub struct SlackClient {
    client: Client,
    webhook_url: Option<String>,
    bot_token: Option<String>,
    channel: String,
}

impl SlackClient {
    pub fn new(config: SlackConfig) -> Result<Self, crate::error::InitError> {
        let client = crate::net::outbound::build_http_client(
            Client::builder().timeout(Duration::from_secs(10)),
            "slack",
        )?;

        Ok(Self {
            client,
            webhook_url: config.webhook_url,
            bot_token: config.bot_token,
            channel: config.channel,
        })
    }

    /// Send a formatted message to Slack using Block Kit
    pub async fn send_event_notification(
        &self,
        event: &Event,
        thread_ts: Option<String>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let color = match event.event_type {
            EventType::Contract => "#0099FF",   // Blue
            EventType::Diagnostic => "#FF9900", // Orange
            EventType::System => "#FF0000",     // Red
        };

        let blocks = json!([
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": format!("Soroban Event: {}", event.event_type),
                    "emoji": true
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": format!("*Contract ID:*\n`{}`", event.contract_id)
                    },
                    {
                        "type": "mrkdwn",
                        "text": format!("*Event Type:*\n{}", event.event_type)
                    },
                    {
                        "type": "mrkdwn",
                        "text": format!("*Transaction Hash:*\n`{}`", event.tx_hash)
                    },
                    {
                        "type": "mrkdwn",
                        "text": format!("*Ledger:*\n{}", event.ledger)
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": format!("*Timestamp:* {}", event.timestamp)
                }
            }
        ]);

        let mut attachment = json!({
            "fallback": format!("Soroban Event: {} on {}", event.event_type, event.contract_id),
            "color": color,
            "text": format!("Event data: ```{}```", serde_json::to_string_pretty(&event.event_data).unwrap_or_default())
        });

        let mut payload = json!({
            "channel": self.channel,
            "blocks": blocks,
            "attachments": [attachment]
        });

        // Add thread_ts for threaded messages
        if let Some(ts) = thread_ts {
            payload["thread_ts"] = json!(ts);
        }

        let url = self.webhook_url.as_ref()
            .ok_or("No webhook URL configured")?;

        let response = self
            .client
            .post(url)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;
            let message_ts = body
                .get("ts")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or("No message timestamp in response")?;

            info!(
                channel = %self.channel,
                message_ts = %message_ts,
                contract_id = %event.contract_id,
                "Slack notification sent"
            );

            Ok(message_ts)
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(
                status = %response.status(),
                body = %error_body,
                "Failed to send Slack notification"
            );
            Err(format!("Slack API error: {}", error_body).into())
        }
    }

    /// Send a message using Slack Bot API
    pub async fn send_message_with_bot(
        &self,
        content: &str,
        mention_users: Vec<&str>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let bot_token = self.bot_token.as_ref()
            .ok_or("No bot token configured")?;

        let mention_text = if !mention_users.is_empty() {
            mention_users.iter()
                .map(|user| format!("<@{}>", user))
                .collect::<Vec<_>>()
                .join(" ")
        } else {
            String::new()
        };

        let text = if mention_text.is_empty() {
            content.to_string()
        } else {
            format!("{} {}", mention_text, content)
        };

        let payload = json!({
            "channel": self.channel,
            "text": text,
            "mrkdwn": true
        });

        let response = self
            .client
            .post("https://slack.com/api/chat.postMessage")
            .header("Authorization", format!("Bearer {}", bot_token))
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;
            if body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                let message_ts = body
                    .get("ts")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or("No message timestamp in response")?;

                info!(
                    channel = %self.channel,
                    message_ts = %message_ts,
                    "Slack message sent via bot"
                );

                Ok(message_ts)
            } else {
                let error = body.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
                Err(format!("Slack API error: {}", error).into())
            }
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(
                status = %response.status(),
                body = %error_body,
                "Failed to send Slack message"
            );
            Err(format!("Slack API error: {}", error_body).into())
        }
    }

    /// Add interactive buttons to a message
    pub async fn add_button_actions(
        &self,
        message_ts: &str,
        buttons: Vec<(&str, &str)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Note: Updating attachments requires ephemeral messages or app interaction
        // This is typically handled through Slack's interactivity endpoints
        Ok(())
    }

    /// Send event to Slack with retry logic
    pub async fn send_with_retry(
        &self,
        event: &Event,
        max_retries: u32,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut backoff_ms = 1000u64;

        for attempt in 1..=max_retries {
            match self.send_event_notification(event, None).await {
                Ok(message_ts) => {
                    info!(attempt = attempt, message_ts = %message_ts, "Slack notification sent successfully");
                    return Ok(message_ts);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        attempt = attempt,
                        "Slack API request failed"
                    );
                }
            }

            if attempt < max_retries {
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2;
            }
        }

        metrics::record_slack_failure();
        Err("Slack delivery failed after retries".into())
    }

    /// Get OAuth access token from authorization code
    pub async fn exchange_code_for_token(
        config: &SlackOAuthConfig,
        code: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        let payload = [
            ("client_id", config.client_id.as_str()),
            ("client_secret", config.client_secret.as_str()),
            ("code", code),
            ("redirect_uri", config.redirect_uri.as_str()),
        ];

        let response = client
            .post("https://slack.com/api/oauth.v2.access")
            .form(&payload)
            .send()
            .await?;

        let body: Value = response.json().await?;
        if body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            let access_token = body
                .get("access_token")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .ok_or("No access token in response")?;

            info!("Slack OAuth token exchanged successfully");
            Ok(access_token)
        } else {
            let error = body.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
            Err(format!("Slack OAuth error: {}", error).into())
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slack_client_creation() {
        let config = SlackConfig {
            webhook_url: Some("https://hooks.slack.com/services/test".to_string()),
            bot_token: Some("xoxb-test-token".to_string()),
            channel: "#notifications".to_string(),
        };

        let client = SlackClient::new(config).unwrap();
        assert_eq!(client.channel, "#notifications");
        assert!(client.webhook_url.is_some());
        assert!(client.bot_token.is_some());
    }

    #[test]
    fn test_slack_oauth_config_creation() {
        let config = SlackOAuthConfig {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            redirect_uri: "http://localhost:3000/callback".to_string(),
        };

        assert_eq!(config.client_id, "test-client-id");
        assert_eq!(config.client_secret, "test-client-secret");
    }
}
