use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::{
    metrics,
    models::{Event, EventType},
};

/// A user to @-mention in a Teams Adaptive Card, via the `msteams.entities`
/// mention pattern (Teams doesn't support a plain `<@id>`-style mention
/// syntax the way Slack/Discord do — mentions are structured entities
/// attached to the card, referenced from a `<at>Name</at>` placeholder in
/// the card body text).
#[derive(Debug, Clone)]
pub struct TeamsMention {
    /// Azure AD object ID of the user being mentioned.
    pub aad_object_id: String,
    pub display_name: String,
}

/// An Adaptive Card action button (`Action.OpenUrl`).
#[derive(Debug, Clone)]
pub struct TeamsAction {
    pub title: String,
    pub url: String,
}

/// Microsoft Teams incoming-webhook configuration.
#[derive(Debug, Clone)]
pub struct TeamsConfig {
    pub webhook_url: String,
}

/// Teams API client for Adaptive Card webhook notifications.
pub struct TeamsClient {
    client: Client,
    webhook_url: String,
}

impl TeamsClient {
    pub fn new(config: TeamsConfig) -> Result<Self, crate::error::InitError> {
        let client = crate::net::outbound::build_http_client(
            Client::builder().timeout(Duration::from_secs(10)),
            "teams",
        )?;

        Ok(Self {
            client,
            webhook_url: config.webhook_url,
        })
    }

    /// Builds the Adaptive Card body for an event, with optional @-mentions
    /// and action buttons.
    fn build_card(
        event: &Event,
        mentions: &[TeamsMention],
        actions: &[TeamsAction],
    ) -> Value {
        let color = match event.event_type {
            EventType::Contract => "Accent",
            EventType::Diagnostic => "Warning",
            EventType::System => "Attention",
        };

        let mut body = vec![
            json!({
                "type": "TextBlock",
                "text": format!("Soroban Event: {}", event.event_type),
                "weight": "Bolder",
                "size": "Medium",
                "color": color,
                "wrap": true
            }),
            json!({
                "type": "FactSet",
                "facts": [
                    { "title": "Contract ID", "value": event.contract_id },
                    { "title": "Event Type", "value": event.event_type.to_string() },
                    { "title": "Transaction Hash", "value": event.tx_hash },
                    { "title": "Ledger", "value": event.ledger.to_string() },
                    { "title": "Timestamp", "value": event.timestamp.to_string() }
                ]
            }),
        ];

        if !event.event_data.is_null() {
            if let Ok(data_str) = serde_json::to_string_pretty(&event.event_data) {
                body.push(json!({
                    "type": "TextBlock",
                    "text": format!("```\n{}\n```", data_str),
                    "wrap": true,
                    "fontType": "Monospace"
                }));
            }
        }

        let mut entities = Vec::new();
        if !mentions.is_empty() {
            let mention_text = mentions
                .iter()
                .map(|m| format!("<at>{}</at>", m.display_name))
                .collect::<Vec<_>>()
                .join(" ");

            body.push(json!({
                "type": "TextBlock",
                "text": mention_text,
                "wrap": true
            }));

            for mention in mentions {
                entities.push(json!({
                    "type": "mention",
                    "text": format!("<at>{}</at>", mention.display_name),
                    "mentioned": {
                        "id": mention.aad_object_id,
                        "name": mention.display_name
                    }
                }));
            }
        }

        let card_actions: Vec<Value> = actions
            .iter()
            .map(|a| {
                json!({
                    "type": "Action.OpenUrl",
                    "title": a.title,
                    "url": a.url
                })
            })
            .collect();

        let mut card = json!({
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": body,
        });

        if !entities.is_empty() {
            card["msteams"] = json!({ "entities": entities });
        }
        if !card_actions.is_empty() {
            card["actions"] = json!(card_actions);
        }

        card
    }

    /// Send an Adaptive Card notification for an event, with optional
    /// @-mentions and action buttons.
    pub async fn send_event_notification(
        &self,
        event: &Event,
        mentions: &[TeamsMention],
        actions: &[TeamsAction],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let card = Self::build_card(event, mentions, actions);

        let payload = json!({
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": card
                }
            ]
        });

        let response = self
            .client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            info!(
                webhook_url = %self.webhook_url,
                contract_id = %event.contract_id,
                "Teams notification sent"
            );
            Ok(())
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(
                status = %response.status(),
                body = %error_body,
                "Failed to send Teams notification"
            );
            Err(format!("Teams API error: {}", error_body).into())
        }
    }

    /// Send event to Teams with retry logic.
    pub async fn send_with_retry(
        &self,
        event: &Event,
        mentions: &[TeamsMention],
        actions: &[TeamsAction],
        max_retries: u32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut backoff_ms = 1000u64;

        for attempt in 1..=max_retries {
            match self.send_event_notification(event, mentions, actions).await {
                Ok(()) => {
                    info!(attempt = attempt, "Teams notification sent successfully");
                    return Ok(());
                }
                Err(e) => {
                    warn!(error = %e, attempt = attempt, "Teams API request failed");
                }
            }

            if attempt < max_retries {
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2;
            }
        }

        metrics::record_teams_failure();
        Err("Teams delivery failed after retries".into())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn sample_event() -> Event {
        Event {
            id: Uuid::new_v4(),
            contract_id: "CONTRACT123".to_string(),
            event_type: EventType::Contract,
            tx_hash: "TXHASH123".to_string(),
            ledger: 12345,
            timestamp: Utc::now(),
            event_data: json!({ "key": "value" }),
            event_data_normalized: None,
            event_data_decoded: None,
            ledger_hash: None,
            in_successful_call: true,
            created_at: Utc::now(),
            schema_version: 1,
            anonymized: false,
            fingerprint: None,
            tenant_id: "default".to_string(),
            total_count: 0,
        }
    }

    #[test]
    fn test_teams_client_creation() {
        let config = TeamsConfig {
            webhook_url: "https://outlook.office.com/webhook/test".to_string(),
        };
        let client = TeamsClient::new(config).unwrap();
        assert_eq!(client.webhook_url, "https://outlook.office.com/webhook/test");
    }

    #[test]
    fn test_build_card_includes_core_fields() {
        let event = sample_event();
        let card = TeamsClient::build_card(&event, &[], &[]);

        assert_eq!(card["type"], "AdaptiveCard");
        let body = card["body"].as_array().expect("body must be an array");
        assert!(!body.is_empty());
        let serialized = card.to_string();
        assert!(serialized.contains("CONTRACT123"));
        assert!(serialized.contains("TXHASH123"));
    }

    #[test]
    fn test_build_card_color_by_event_type() {
        let mut contract_event = sample_event();
        contract_event.event_type = EventType::Contract;
        let card = TeamsClient::build_card(&contract_event, &[], &[]);
        assert_eq!(card["body"][0]["color"], "Accent");

        let mut diagnostic_event = sample_event();
        diagnostic_event.event_type = EventType::Diagnostic;
        let card = TeamsClient::build_card(&diagnostic_event, &[], &[]);
        assert_eq!(card["body"][0]["color"], "Warning");

        let mut system_event = sample_event();
        system_event.event_type = EventType::System;
        let card = TeamsClient::build_card(&system_event, &[], &[]);
        assert_eq!(card["body"][0]["color"], "Attention");
    }

    #[test]
    fn test_build_card_with_mentions() {
        let event = sample_event();
        let mentions = vec![TeamsMention {
            aad_object_id: "aad-123".to_string(),
            display_name: "Jane Doe".to_string(),
        }];
        let card = TeamsClient::build_card(&event, &mentions, &[]);

        let entities = card["msteams"]["entities"]
            .as_array()
            .expect("entities must be an array");
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0]["mentioned"]["id"], "aad-123");
        assert_eq!(entities[0]["type"], "mention");
    }

    #[test]
    fn test_build_card_without_mentions_has_no_entities_block() {
        let event = sample_event();
        let card = TeamsClient::build_card(&event, &[], &[]);
        assert!(card.get("msteams").is_none());
    }

    #[test]
    fn test_build_card_with_actions() {
        let event = sample_event();
        let actions = vec![TeamsAction {
            title: "View Contract".to_string(),
            url: "https://example.com/contract/CONTRACT123".to_string(),
        }];
        let card = TeamsClient::build_card(&event, &[], &actions);

        let card_actions = card["actions"].as_array().expect("actions must be an array");
        assert_eq!(card_actions.len(), 1);
        assert_eq!(card_actions[0]["type"], "Action.OpenUrl");
        assert_eq!(card_actions[0]["title"], "View Contract");
    }
}
