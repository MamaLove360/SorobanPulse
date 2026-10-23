//! AWS SQS publisher integration.
//!
//! Enable with the `sqs` feature and configure `SQS_QUEUE_URL`. Messages are
//! sent with the signed SQS Query API so no additional lockfile entries are required.

use crate::models::SorobanEvent;
use async_trait::async_trait;
use tracing::error;

#[async_trait]
pub trait SqsPublisher: Send + Sync {
    async fn publish_batch(&self, events: &[SorobanEvent]) -> Result<(), String>;

    async fn publish(&self, event: &SorobanEvent) -> Result<(), String> {
        self.publish_batch(std::slice::from_ref(event)).await
    }
}

#[cfg(feature = "sqs")]
pub mod aws {
    use super::*;

    const MAX_SQS_BATCH_SIZE: usize = 10;

    pub struct AwsSqsPublisher {
        client: reqwest::Client,
        queue_url: String,
        region: String,
        dlq_url: Option<String>,
        batch_size: usize,
    }

    impl AwsSqsPublisher {
        pub async fn from_env(
            queue_url: String,
            region: String,
            dlq_url: Option<String>,
            batch_size: usize,
        ) -> Self {
            Self {
                client: reqwest::Client::new(),
                queue_url,
                region,
                dlq_url,
                batch_size: batch_size.clamp(1, MAX_SQS_BATCH_SIZE),
            }
        }

        fn signed_headers(
            &self,
            url: &str,
            body: &str,
        ) -> Result<Vec<(String, String)>, String> {
            let access_key = std::env::var("AWS_ACCESS_KEY_ID")
                .map_err(|_| "AWS_ACCESS_KEY_ID is required for SQS publishing".to_string())?;
            let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
                .map_err(|_| "AWS_SECRET_ACCESS_KEY is required for SQS publishing".to_string())?;
            let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
            let parsed = reqwest::Url::parse(url).map_err(|e| e.to_string())?;
            let host = parsed
                .host_str()
                .ok_or_else(|| "SQS_QUEUE_URL must include a host".to_string())?;
            let now = chrono::Utc::now();
            let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
            let date = now.format("%Y%m%d").to_string();
            let payload_hash = sha256_hex(body.as_bytes());
            let canonical_uri = if parsed.path().is_empty() {
                "/"
            } else {
                parsed.path()
            };
            let mut canonical_headers = format!(
                "content-type:application/x-www-form-urlencoded\n\
                 host:{host}\n\
                 x-amz-date:{amz_date}\n"
            );
            let mut signed_headers = "content-type;host;x-amz-date".to_string();
            if let Some(token) = &session_token {
                canonical_headers.push_str(&format!("x-amz-security-token:{token}\n"));
                signed_headers.push_str(";x-amz-security-token");
            }
            let canonical_request = format!(
                "POST\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
            );
            let scope = format!("{date}/{}/sqs/aws4_request", self.region);
            let string_to_sign = format!(
                "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
                sha256_hex(canonical_request.as_bytes())
            );
            let signing_key = signing_key(&secret_key, &date, &self.region);
            let signature = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());
            let authorization = format!(
                "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, \
                 SignedHeaders={signed_headers}, Signature={signature}"
            );
            let mut headers = vec![
                ("Authorization".to_string(), authorization),
                ("Content-Type".to_string(), "application/x-www-form-urlencoded".to_string()),
                ("X-Amz-Date".to_string(), amz_date),
            ];
            if let Some(token) = session_token {
                headers.push(("X-Amz-Security-Token".to_string(), token));
            }
            Ok(headers)
        }

        async fn post_form(
            &self,
            url: &str,
            form: &[(String, String)],
        ) -> Result<reqwest::Response, String> {
            let body = serde_urlencoded(form);
            let mut req = self.client.post(url).body(body.clone());
            for (name, value) in self.signed_headers(url, &body)? {
                req = req.header(name, value);
            }
            req.send().await.map_err(|e| e.to_string())
        }

        async fn send_to_dlq(&self, event: &SorobanEvent, reason: &str) {
            let Some(dlq_url) = &self.dlq_url else {
                return;
            };
            let Ok(body) = serde_json::to_string(&serde_json::json!({
                "event": event,
                "failure_reason": reason,
            })) else {
                return;
            };
            let form = vec![
                ("Action".to_string(), "SendMessage".to_string()),
                ("Version".to_string(), "2012-11-05".to_string()),
                ("MessageBody".to_string(), body),
            ];
            if let Err(e) = self.post_form(dlq_url, &form).await {
                error!(error = %e, "failed to write SQS message to DLQ");
            }
        }
    }

    #[async_trait]
    impl SqsPublisher for AwsSqsPublisher {
        async fn publish_batch(&self, events: &[SorobanEvent]) -> Result<(), String> {
            for chunk in events.chunks(self.batch_size) {
                let mut form = vec![
                    ("Action".to_string(), "SendMessageBatch".to_string()),
                    ("Version".to_string(), "2012-11-05".to_string()),
                ];
                for (idx, event) in chunk.iter().enumerate() {
                    let entry_no = idx + 1;
                    form.push((
                        format!("SendMessageBatchRequestEntry.{entry_no}.Id"),
                        format!("event-{idx}"),
                    ));
                    form.push((
                        format!("SendMessageBatchRequestEntry.{entry_no}.MessageBody"),
                        serde_json::to_string(event)
                            .map_err(|e| format!("failed to serialize SQS event: {e}"))?,
                    ));
                }

                let response = self
                    .post_form(&self.queue_url, &form)
                    .await
                    .map_err(|e| format!("SQS batch publish failed: {e}"))?;
                if !response.status().is_success() {
                    let reason = format!("SQS returned {}", response.status());
                    for event in chunk {
                        self.send_to_dlq(event, &reason).await;
                    }
                    return Err(reason);
                }
            }
            Ok(())
        }
    }
}

#[cfg(feature = "sqs")]
fn serde_urlencoded(form: &[(String, String)]) -> String {
    form.iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                url::form_urlencoded::byte_serialize(key.as_bytes()).collect::<String>(),
                url::form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>()
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(feature = "sqs")]
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(feature = "sqs")]
fn hmac_sha256(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    use hmac::Mac;
    let mut mac = crate::hmac_util::hmac_sha256(key);
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(feature = "sqs")]
fn hmac_sha256_hex(key: &[u8], bytes: &[u8]) -> String {
    hex::encode(hmac_sha256(key, bytes))
}

#[cfg(feature = "sqs")]
fn signing_key(secret: &str, date: &str, region: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"sqs");
    hmac_sha256(&k_service, b"aws4_request")
}

pub async fn publish_event(publisher: &dyn SqsPublisher, event: &SorobanEvent) {
    if let Err(e) = publisher.publish(event).await {
        error!(error = %e, "failed to publish event to SQS");
    }
}
