use reqwest::Client;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info};
use uuid::Uuid;

use crate::notification_channel::NotificationChannel;
use crate::notification_delivery::NotificationError;
use crate::{metrics, models::SorobanEvent, retry_policy::RetryPolicy};

#[derive(Debug, Serialize, Deserialize)]
pub struct TwilioConfig {
    pub account_sid: String,
    pub auth_token: SecretString,
    pub from_number: String,
    pub to_numbers: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TwilioResponse {
    sid: String,
    status: String,
    error_message: Option<String>,
}

pub struct SmsNotifier {
    client: Client,
    config: TwilioConfig,
    contract_filter: Vec<String>,
    retry_policy: RetryPolicy,
    pool: sqlx::PgPool,
}

impl SmsNotifier {
    pub fn new(
        config: TwilioConfig,
        contract_filter: Vec<String>,
        retry_policy: RetryPolicy,
        pool: sqlx::PgPool,
    ) -> Result<Self, crate::error::InitError> {
        let client = crate::net::outbound::build_http_client(
            Client::builder().timeout(Duration::from_secs(30)),
            "SMS",
        )?;

        Ok(Self {
            client,
            config,
            contract_filter,
            retry_policy,
            pool,
        })
    }

    fn is_critical_event(&self, event: &SorobanEvent) -> bool {
        // For now, consider all contract events as potentially critical.
        event.event_type == "contract"
    }

    fn format_sms_message(&self, event: &SorobanEvent) -> String {
        let short_contract = event.contract_id.get(..8).unwrap_or(&event.contract_id);
        let base_message = format!(
            "Soroban Alert: {} event on contract {} at ledger {}",
            event.event_type, short_contract, event.ledger
        );

        // Truncate to 140 chars to leave room for the link.
        let truncated = match base_message.char_indices().nth(137) {
            Some((idx, _)) if base_message.chars().count() > 140 => {
                format!("{}...", &base_message[..idx])
            }
            _ => base_message,
        };

        let event_ref = event
            .id
            .map(|id| id.to_string())
            .unwrap_or_else(|| event.tx_hash.clone());
        format!("{} View: https://soroban-pulse.com/events/{}", truncated, event_ref)
    }

    /// Send one SMS, persisting its status in `sms_notifications`. Returns the
    /// Twilio message SID on success.
    async fn send_sms(&self, phone_number: &str, message: &str, event: &SorobanEvent) -> Result<String, String> {
        let notification_id = Uuid::new_v4();

        if let Err(e) = sqlx::query(
            "INSERT INTO sms_notifications (id, phone_number, message, status) VALUES ($1, $2, $3, 'pending')"
        )
        .bind(notification_id)
        .bind(phone_number)
        .bind(message)
        .execute(&self.pool)
        .await
        {
            error!(error = %e, "Failed to insert SMS notification record");
            return Err(format!("failed to record SMS notification: {e}"));
        }

        let result = self
            .retry_policy
            .execute_with_retry(|attempt| self.send_twilio_sms(&self.client, &self.config, phone_number, message, attempt))
            .await;

        match &result {
            Ok(twilio_sid) => {
                info!(
                    phone_number = %phone_number,
                    twilio_sid = %twilio_sid,
                    contract_id = %event.contract_id,
                    "SMS sent successfully"
                );

                if let Err(e) = sqlx::query(
                    "UPDATE sms_notifications SET twilio_sid = $1, status = 'sent', sent_at = NOW() WHERE id = $2"
                )
                .bind(twilio_sid)
                .bind(notification_id)
                .execute(&self.pool)
                .await
                {
                    error!(error = %e, "Failed to update SMS notification status");
                }

                metrics::increment_counter("soroban_pulse_sms_notifications_total", &[("status", "success")]);
            }
            Err(error_msg) => {
                error!(
                    phone_number = %phone_number,
                    error = %error_msg,
                    contract_id = %event.contract_id,
                    "SMS delivery failed after all retries"
                );

                if let Err(e) = sqlx::query(
                    "UPDATE sms_notifications SET status = 'failed', error_message = $1 WHERE id = $2"
                )
                .bind(error_msg)
                .bind(notification_id)
                .execute(&self.pool)
                .await
                {
                    error!(error = %e, "Failed to update SMS notification error status");
                }

                metrics::increment_counter("soroban_pulse_sms_notifications_total", &[("status", "failure")]);
            }
        }

        result
    }

    async fn send_twilio_sms(
        &self,
        client: &Client,
        config: &TwilioConfig,
        to: &str,
        message: &str,
        attempt: u32,
    ) -> Result<String, String> {
        let url = format!(
            "https://api.twilio.com/2010-04-01/Accounts/{}/Messages.json",
            config.account_sid
        );

        let form_data = [
            ("From", config.from_number.as_str()),
            ("To", to),
            ("Body", message),
        ];

        let response = client
            .post(&url)
            .basic_auth(&config.account_sid, Some(config.auth_token.expose_secret()))
            .form(&form_data)
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if response.status().is_success() {
            let twilio_response: TwilioResponse = response
                .json()
                .await
                .map_err(|e| format!("Failed to parse Twilio response: {}", e))?;

            if let Some(error_msg) = twilio_response.error_message {
                return Err(format!("Twilio API error: {}", error_msg));
            }

            Ok(twilio_response.sid)
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(format!("Twilio API returned {}: {}", status, body))
        }
    }
}

/// Issue #994: adapt `SmsNotifier` to the unified notification delivery
/// framework so SMS shares retry/error-handling/metrics with email and push
/// instead of each channel reimplementing them independently.
#[async_trait::async_trait]
impl crate::notification_delivery::DeliveryChannel for SmsNotifier {
    fn channel_name(&self) -> &'static str {
        "sms"
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy.clone()
    }

    async fn deliver(
        &self,
        target: &str,
        _subject: &str,
        body: &str,
    ) -> Result<
        crate::notification_delivery::DeliveryOutcome,
        crate::notification_delivery::NotificationError,
    > {
        match self
            .send_twilio_sms(&self.client, &self.config, target, body, 1)
            .await
        {
            Ok(_twilio_sid) => Ok(crate::notification_delivery::DeliveryOutcome::Delivered),
            Err(msg) => {
                // Twilio error code 21211 ("invalid 'To' phone number") is the
                // clearest permanent-failure signal available from this
                // client's string-based errors; anything else is treated as
                // retryable. See docs/notification-architecture.md follow-up
                // notes for a proper Twilio error-code mapping.
                if msg.contains("21211") {
                    Err(crate::notification_delivery::NotificationError::InvalidTarget(msg))
                } else {
                    Err(crate::notification_delivery::NotificationError::Transient(msg))
                }
            }
        }
    }
}

/// Issue #1245: SMS participates in the unified [`NotificationChannel`]
/// dispatch path. One event fans out to every configured recipient.
#[async_trait::async_trait]
impl NotificationChannel for SmsNotifier {
    fn channel_name(&self) -> &'static str {
        "sms"
    }

    fn accepts(&self, event: &SorobanEvent) -> bool {
        (self.contract_filter.is_empty() || self.contract_filter.contains(&event.contract_id))
            && self.is_critical_event(event)
    }

    /// Returns the comma-separated Twilio SIDs of the messages sent. Fails
    /// only if no recipient could be reached.
    async fn send_event_notification(&self, event: &SorobanEvent) -> Result<String, NotificationError> {
        let message = self.format_sms_message(event);
        let mut sids = Vec::new();
        let mut last_error = None;
        for phone_number in &self.config.to_numbers {
            match self.send_sms(phone_number, &message, event).await {
                Ok(sid) => sids.push(sid),
                Err(e) => last_error = Some(e),
            }
        }
        match last_error {
            Some(e) if sids.is_empty() => Err(NotificationError::Transient(e)),
            _ => Ok(sids.join(",")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notifier(contract_filter: Vec<String>) -> SmsNotifier {
        let config = TwilioConfig {
            account_sid: "test".to_string(),
            auth_token: SecretString::new("test".to_string()),
            from_number: "+1234567890".to_string(),
            to_numbers: vec!["+0987654321".to_string()],
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://test")
            .unwrap();
        SmsNotifier::new(config, contract_filter, RetryPolicy::sms_default(), pool).unwrap()
    }

    fn event(contract_id: &str, event_type: &str) -> SorobanEvent {
        SorobanEvent {
            id: Some(Uuid::new_v4()),
            contract_id: contract_id.to_string(),
            event_type: event_type.to_string(),
            tx_hash: "test_hash".to_string(),
            ledger: 12345,
            ..SorobanEvent::default()
        }
    }

    #[tokio::test]
    async fn test_format_sms_message() {
        let message = notifier(vec![]).format_sms_message(&event("CABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890", "contract"));
        assert!(message.len() <= 160);
        assert!(message.contains("CABCDEFG")); // Truncated contract ID
        assert!(message.contains("12345")); // Ledger number
        assert!(message.contains("https://soroban-pulse.com/events/"));
    }

    #[tokio::test]
    async fn format_sms_message_handles_short_contract_ids() {
        let message = notifier(vec![]).format_sms_message(&event("C1", "contract"));
        assert!(message.contains("contract C1 at ledger"));
    }

    #[tokio::test]
    async fn test_is_critical_event() {
        let n = notifier(vec![]);
        assert!(n.is_critical_event(&event("test", "contract")));
        assert!(!n.is_critical_event(&event("test", "system")));
    }

    #[tokio::test]
    async fn channel_accepts_respects_contract_filter_and_criticality() {
        let n = notifier(vec!["CWATCHED".to_string()]);
        assert_eq!(NotificationChannel::channel_name(&n), "sms");
        assert!(n.accepts(&event("CWATCHED", "contract")));
        assert!(!n.accepts(&event("COTHER", "contract")));
        assert!(!n.accepts(&event("CWATCHED", "system")));
    }
}
