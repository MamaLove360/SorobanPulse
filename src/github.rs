use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{metrics, models::SorobanEvent};

/// GitHub OAuth token configuration
#[derive(Debug, Clone)]
pub struct GitHubOAuthConfig {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
}

/// GitHub API client for issue creation and PR comments
pub struct GitHubClient {
    client: Client,
    access_token: String,
    repository: String,
    owner: String,
}

impl GitHubClient {
    pub fn new(access_token: String, owner: String, repository: String) -> Result<Self, crate::error::InitError> {
        let client = crate::net::outbound::build_http_client(
            Client::builder().timeout(Duration::from_secs(10)),
            "github",
        )?;

        Ok(Self {
            client,
            access_token,
            repository,
            owner,
        })
    }

    /// Create a GitHub issue for an event
    pub async fn create_issue(
        &self,
        event: &SorobanEvent,
        issue_title: Option<&str>,
        issue_body_template: Option<&str>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let title = issue_title.unwrap_or(&format!(
            "Soroban Event: {} on {}",
            event.event_type, event.contract_id
        ));

        let body = issue_body_template.unwrap_or(&format!(
            "## Event Details\n\n\
            - **Event Type**: {}\n\
            - **Contract ID**: {}\n\
            - **Transaction Hash**: {}\n\
            - **Ledger**: {}\n\
            - **Timestamp**: {}\n\
            - **Event Data**: ```json\n{}\n```",
            event.event_type,
            event.contract_id,
            event.tx_hash,
            event.ledger,
            event.timestamp,
            serde_json::to_string_pretty(&event.event_data).unwrap_or_default()
        ));

        let payload = json!({
            "title": title,
            "body": body,
            "labels": ["soroban-event", event.event_type.to_string().to_lowercase()]
        });

        let url = format!(
            "https://api.github.com/repos/{}/{}/issues",
            self.owner, self.repository
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("token {}", self.access_token))
            .header("Accept", "application/vnd.github.v3+json")
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;
            let issue_number = body
                .get("number")
                .and_then(|v| v.as_u64())
                .map(|n| n.to_string())
                .ok_or("No issue number in response")?;

            info!(
                owner = %self.owner,
                repository = %self.repository,
                issue_number = %issue_number,
                contract_id = %event.contract_id,
                "GitHub issue created"
            );

            Ok(issue_number)
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(
                status = %response.status(),
                body = %error_body,
                "Failed to create GitHub issue"
            );
            Err(format!("GitHub API error: {}", error_body).into())
        }
    }

    /// Add a comment to a GitHub issue
    pub async fn add_issue_comment(
        &self,
        issue_number: &str,
        comment: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let payload = json!({
            "body": comment
        });

        let url = format!(
            "https://api.github.com/repos/{}/{}/issues/{}/comments",
            self.owner, self.repository, issue_number
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("token {}", self.access_token))
            .header("Accept", "application/vnd.github.v3+json")
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;
            let comment_id = body
                .get("id")
                .and_then(|v| v.as_u64())
                .map(|n| n.to_string())
                .ok_or("No comment ID in response")?;

            info!(
                owner = %self.owner,
                repository = %self.repository,
                issue_number = %issue_number,
                comment_id = %comment_id,
                "GitHub issue comment added"
            );

            Ok(comment_id)
        } else {
            let error_body = response.text().await.unwrap_or_default();
            error!(
                status = %response.status(),
                body = %error_body,
                issue_number = %issue_number,
                "Failed to add GitHub issue comment"
            );
            Err(format!("GitHub API error: {}", error_body).into())
        }
    }

    /// Add a comment to a GitHub PR
    pub async fn add_pr_comment(
        &self,
        pr_number: &str,
        comment: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.add_issue_comment(pr_number, comment).await
    }

    /// Get OAuth access token from authorization code
    pub async fn exchange_code_for_token(
        config: &GitHubOAuthConfig,
        code: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        let payload = json!({
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "code": code,
            "redirect_uri": config.redirect_uri
        });

        let response = client
            .post("https://github.com/login/oauth/access_token")
            .header("Accept", "application/json")
            .json(&payload)
            .send()
            .await?;

        let body: Value = response.json().await?;
        let access_token = body
            .get("access_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or("No access token in response")?;

        info!("GitHub OAuth token exchanged successfully");
        Ok(access_token)
    }

    /// Send event to GitHub with retry logic
    pub async fn send_with_retry(
        &self,
        event: &SorobanEvent,
        max_retries: u32,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut backoff_ms = 1000u64;

        for attempt in 1..=max_retries {
            match self.create_issue(event, None, None).await {
                Ok(issue_num) => {
                    info!(attempt = attempt, issue_number = %issue_num, "GitHub issue created successfully");
                    return Ok(issue_num);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        attempt = attempt,
                        "GitHub API request failed"
                    );
                }
            }

            if attempt < max_retries {
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2;
            }
        }

        metrics::record_github_failure();
        Err("GitHub delivery failed after retries".into())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_github_client_creation() {
        let client = GitHubClient::new(
            "test-token".to_string(),
            "test-owner".to_string(),
            "test-repo".to_string(),
        )
        .unwrap();

        assert_eq!(client.access_token, "test-token");
        assert_eq!(client.owner, "test-owner");
        assert_eq!(client.repository, "test-repo");
    }

    #[test]
    fn test_github_oauth_config_creation() {
        let config = GitHubOAuthConfig {
            client_id: "test-client-id".to_string(),
            client_secret: "test-client-secret".to_string(),
            redirect_uri: "http://localhost:3000/callback".to_string(),
        };

        assert_eq!(config.client_id, "test-client-id");
        assert_eq!(config.client_secret, "test-client-secret");
    }
}
