use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{metrics, models::Event};

/// Telegram Bot configuration
#[derive(Debug, Clone)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub chat_id: String,
}

/// Telegram Bot API client for notifications
pub struct TelegramClient {
    client: Client,
    bot_token: String,
    chat_id: String,
    user_subscriptions: HashMap<String, Vec<String>>, // user_id -> chat_ids
}

impl TelegramClient {
    pub fn new(config: TelegramConfig) -> Result<Self, crate::error::InitError> {
        let client = crate::net::outbound::build_http_client(
            Client::builder().timeout(Duration::from_secs(10)),
            "telegram",
        )?;

        Ok(Self {
            client,
            bot_token: config.bot_token,
            chat_id: config.chat_id,
            user_subscriptions: HashMap::new(),
        })
    }

    /// Send a formatted message to Telegram
    pub async fn send_event_notification(
        &self,
        event: &Event,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let message = format!(
            "🔔 *Soroban Event Notification*\n\n\
            *Event Type:* {}\n\
            *Contract ID:* `{}`\n\
            *Transaction Hash:* `{}`\n\
            *Ledger:* {}\n\
            *Timestamp:* {}\n\n\
            *Event Data:*\n```json\n{}```",
            event.event_type,
            event.contract_id,
            event.tx_hash,
            event.ledger,
            event.timestamp,
            serde_json::to_string_pretty(&event.event_data).unwrap_or_default()
        );

        let payload = json!({
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": true
        });

        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;
            if body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                let message_id = body
                    .get("result")
                    .and_then(|r| r.get("message_id"))
                    .and_then(|v| v.as_u64())
                    .map(|n| n.to_string())
                    .ok_or("No message ID in response")?;

                info!(
                    chat_id = %self.chat_id,
                    message_id = %message_id,
                    contract_id = %event.contract_id,
                    "Telegram notification sent"
                );

                Ok(message_id)
            } else {
                let error_code = body.get("error_code").and_then(|v| v.as_u64()).unwrap_or(0);
                let error_desc = body.get("description").and_then(|v| v.as_str()).unwrap_or("Unknown error");
                Err(format!("Telegram API error ({}): {}", error_code, error_desc).into())
            }
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(
                status = %response.status(),
                body = %error_body,
                "Failed to send Telegram notification"
            );
            Err(format!("Telegram API error: {}", error_body).into())
        }
    }

    /// Send a simple text message to Telegram
    pub async fn send_message(&self, text: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let payload = json!({
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown"
        });

        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;
            if body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                let message_id = body
                    .get("result")
                    .and_then(|r| r.get("message_id"))
                    .and_then(|v| v.as_u64())
                    .map(|n| n.to_string())
                    .ok_or("No message ID in response")?;

                info!(
                    chat_id = %self.chat_id,
                    message_id = %message_id,
                    "Telegram message sent"
                );

                Ok(message_id)
            } else {
                let error_desc = body.get("description").and_then(|v| v.as_str()).unwrap_or("Unknown error");
                Err(format!("Telegram API error: {}", error_desc).into())
            }
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(
                status = %response.status(),
                body = %error_body,
                "Failed to send Telegram message"
            );
            Err(format!("Telegram API error: {}", error_body).into())
        }
    }

    /// Send a message with inline buttons
    pub async fn send_message_with_buttons(
        &self,
        text: &str,
        buttons: Vec<(String, String)>, // (button_text, callback_data)
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut inline_keyboard = Vec::new();
        for (text, data) in buttons {
            inline_keyboard.push(json!({
                "text": text,
                "callback_data": data
            }));
        }

        let payload = json!({
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "reply_markup": {
                "inline_keyboard": [inline_keyboard]
            }
        });

        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;
            if body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                let message_id = body
                    .get("result")
                    .and_then(|r| r.get("message_id"))
                    .and_then(|v| v.as_u64())
                    .map(|n| n.to_string())
                    .ok_or("No message ID in response")?;

                Ok(message_id)
            } else {
                let error_desc = body.get("description").and_then(|v| v.as_str()).unwrap_or("Unknown error");
                Err(format!("Telegram API error: {}", error_desc).into())
            }
        } else {
            Err("Failed to send Telegram message with buttons".into())
        }
    }

    /// Setup webhook for Telegram bot
    pub async fn setup_webhook(
        &self,
        webhook_url: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = json!({
            "url": webhook_url,
            "allowed_updates": ["message", "callback_query"]
        });

        let url = format!(
            "https://api.telegram.org/bot{}/setWebhook",
            self.bot_token
        );

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await?;

        let body: Value = response.json().await?;
        if body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            info!(
                webhook_url = %webhook_url,
                "Telegram webhook setup successful"
            );
            Ok(())
        } else {
            let error_desc = body.get("description").and_then(|v| v.as_str()).unwrap_or("Unknown error");
            Err(format!("Failed to setup webhook: {}", error_desc).into())
        }
    }

    /// Add user subscription to notifications
    pub fn subscribe_user(&mut self, user_id: String, chat_id: String) {
        self.user_subscriptions
            .entry(user_id)
            .or_insert_with(Vec::new)
            .push(chat_id);
    }

    /// Remove user subscription
    pub fn unsubscribe_user(&mut self, user_id: &str, chat_id: &str) {
        if let Some(chats) = self.user_subscriptions.get_mut(user_id) {
            chats.retain(|id| id != chat_id);
            if chats.is_empty() {
                self.user_subscriptions.remove(user_id);
            }
        }
    }

    /// Send event to Telegram with retry logic
    pub async fn send_with_retry(
        &self,
        event: &Event,
        max_retries: u32,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut backoff_ms = 1000u64;

        for attempt in 1..=max_retries {
            match self.send_event_notification(event).await {
                Ok(message_id) => {
                    info!(attempt = attempt, message_id = %message_id, "Telegram notification sent successfully");
                    return Ok(message_id);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        attempt = attempt,
                        "Telegram API request failed"
                    );
                }
            }

            if attempt < max_retries {
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2;
            }
        }

        metrics::record_telegram_failure();
        Err("Telegram delivery failed after retries".into())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telegram_client_creation() {
        let config = TelegramConfig {
            bot_token: "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11".to_string(),
            chat_id: "-1001234567890".to_string(),
        };

        let client = TelegramClient::new(config).unwrap();
        assert_eq!(client.chat_id, "-1001234567890");
        assert!(client.user_subscriptions.is_empty());
    }

    #[test]
    fn test_user_subscription_management() {
        let config = TelegramConfig {
            bot_token: "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11".to_string(),
            chat_id: "-1001234567890".to_string(),
        };

        let mut client = TelegramClient::new(config).unwrap();

        client.subscribe_user("user1".to_string(), "chat1".to_string());
        assert!(client.user_subscriptions.contains_key("user1"));

        client.unsubscribe_user("user1", "chat1");
        assert!(!client.user_subscriptions.contains_key("user1"));
    }
}
