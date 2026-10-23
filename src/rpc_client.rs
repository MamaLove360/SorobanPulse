use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use crate::models::{GetEventsResult, LatestLedgerResult, RpcResponse};

#[async_trait]
pub trait RpcClient: Send + Sync {
    async fn get_latest_ledger(&self, url: &str)
        -> Result<RpcResponse<LatestLedgerResult>, String>;
    async fn get_events(
        &self,
        url: &str,
        params: Value,
    ) -> Result<RpcResponse<GetEventsResult>, String>;
}

pub struct HttpRpcClient {
    client: Client,
}

/// Retries applied to transient RPC failures (transport errors, 429 and 5xx).
const MAX_RETRIES: u32 = 3;
/// Base delay for exponential backoff between retries.
const RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

impl HttpRpcClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// POST a JSON-RPC request, retrying transient failures with exponential
    /// backoff so a flaky endpoint does not surface as an indexer error.
    async fn post_with_retry<T: DeserializeOwned>(&self, url: &str, body: &Value) -> Result<T, String> {
        let mut attempt = 0;
        loop {
            match self.try_post(url, body).await {
                Ok(v) => return Ok(v),
                Err((err, retryable)) if retryable && attempt < MAX_RETRIES => {
                    crate::metrics::record_rpc_error();
                    let delay = RETRY_BASE_DELAY * 2u32.pow(attempt);
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_retries = MAX_RETRIES,
                        delay_ms = delay.as_millis() as u64,
                        error = %err,
                        "Transient RPC error, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err((err, _)) => return Err(err),
            }
        }
    }

    /// Single request attempt. The returned flag marks the error as retryable.
    async fn try_post<T: DeserializeOwned>(&self, url: &str, body: &Value) -> Result<T, (String, bool)> {
        let resp = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|e| (e.to_string(), e.is_timeout() || e.is_connect()))?;

        let status = resp.status();
        if is_retryable_status(status) {
            return Err((format!("RPC returned HTTP {status}"), true));
        }

        resp.json().await.map_err(|e| (e.to_string(), false))
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

#[async_trait]
impl RpcClient for HttpRpcClient {
    async fn get_latest_ledger(
        &self,
        url: &str,
    ) -> Result<RpcResponse<LatestLedgerResult>, String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestLedger"
        });

        self.post_with_retry(url, &body).await
    }

    async fn get_events(
        &self,
        url: &str,
        params: Value,
    ) -> Result<RpcResponse<GetEventsResult>, String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getEvents",
            "params": params
        });

        self.post_with_retry(url, &body).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_only_transient_statuses() {
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_status(StatusCode::OK));
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(StatusCode::NOT_FOUND));
    }
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    pub struct MockRpcClient {
        responses: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    }

    impl MockRpcClient {
        pub fn new() -> Self {
            Self {
                responses: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub fn with_responses(responses: HashMap<String, serde_json::Value>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses)),
            }
        }

        pub fn set_response(&self, key: &str, response: serde_json::Value) {
            let mut responses = self.responses.lock().unwrap();
            responses.insert(key.to_string(), response);
        }
    }

    #[async_trait]
    impl RpcClient for MockRpcClient {
        async fn get_latest_ledger(
            &self,
            _url: &str,
        ) -> Result<RpcResponse<LatestLedgerResult>, String> {
            let responses = self.responses.lock().unwrap();
            if let Some(response) = responses.get("getLatestLedger") {
                serde_json::from_value(response.clone())
                    .map_err(|e| format!("Failed to deserialize mock response: {}", e))
            } else {
                Err("No mock response set for getLatestLedger".to_string())
            }
        }

        async fn get_events(
            &self,
            _url: &str,
            _params: Value,
        ) -> Result<RpcResponse<GetEventsResult>, String> {
            let responses = self.responses.lock().unwrap();
            if let Some(response) = responses.get("getEvents") {
                serde_json::from_value(response.clone())
                    .map_err(|e| format!("Failed to deserialize mock response: {}", e))
            } else {
                Err("No mock response set for getEvents".to_string())
            }
        }
    }
}
