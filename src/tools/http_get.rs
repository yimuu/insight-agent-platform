//! Restricted HTTP GET tool.

use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use reqwest::{redirect::Policy, StatusCode};
use serde_json::{json, Value};

use crate::{
    error::AppError,
    tools::registry::{Tool, ToolContext},
};

#[derive(Debug, Clone)]
pub struct HttpGetTool {
    client: reqwest::Client,
    max_bytes: usize,
    allowlist: Option<BTreeSet<String>>,
}

impl HttpGetTool {
    pub fn new(timeout: Duration, max_bytes: usize) -> Result<Self, AppError> {
        Self::new_with_optional_allowlist(timeout, max_bytes, None)
    }

    pub fn new_with_allowlist(
        timeout: Duration,
        max_bytes: usize,
        allowlist: Vec<String>,
    ) -> Result<Self, AppError> {
        Self::new_with_optional_allowlist(timeout, max_bytes, Some(allowlist))
    }

    fn new_with_optional_allowlist(
        timeout: Duration,
        max_bytes: usize,
        allowlist: Option<Vec<String>>,
    ) -> Result<Self, AppError> {
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .timeout(timeout)
            .build()
            .map_err(|err| AppError::Run(format!("failed to build http_get client: {err}")))?;

        let allowlist = allowlist.map(|hosts| {
            hosts
                .into_iter()
                .map(|host| host.trim().to_ascii_lowercase())
                .collect()
        });

        Ok(Self {
            client,
            max_bytes,
            allowlist,
        })
    }

    fn is_allowed_host(&self, parsed: &reqwest::Url) -> bool {
        match (&self.allowlist, parsed.host_str()) {
            (None, Some(_)) => true,
            (Some(allowlist), Some(host)) => allowlist.contains(&host.to_ascii_lowercase()),
            (_, None) => false,
        }
    }

    fn classify_request_error(err: &reqwest::Error) -> &'static str {
        if err.is_timeout() {
            "timeout"
        } else if err.is_connect() {
            "connection error"
        } else if err.is_request() {
            "request error"
        } else {
            "transport error"
        }
    }

    fn validate_response_status(status: StatusCode) -> Result<(), AppError> {
        if status.is_redirection() {
            return Err(AppError::Run("http_get redirect blocked".to_string()));
        }

        Ok(())
    }
}

impl Default for HttpGetTool {
    fn default() -> Self {
        Self::new(Duration::from_secs(10), 256 * 1024).expect("http_get client should build")
    }
}

#[async_trait]
impl Tool for HttpGetTool {
    fn name(&self) -> &'static str {
        "http_get"
    }

    async fn call(&self, args: Value, _ctx: ToolContext) -> Result<Value, AppError> {
        let url = args
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::Run("http_get requires string arg 'url'".to_string()))?;
        let parsed =
            reqwest::Url::parse(url).map_err(|err| AppError::Run(format!("invalid url: {err}")))?;

        if parsed.scheme() != "https" {
            return Err(AppError::Run("http_get only allows https URLs".to_string()));
        }
        if !self.is_allowed_host(&parsed) {
            return Err(AppError::Run(
                "http_get host is not in the allowlist".to_string(),
            ));
        }

        let response = self.client.get(parsed).send().await.map_err(|err| {
            AppError::Run(format!(
                "http_get request failed ({})",
                Self::classify_request_error(&err)
            ))
        })?;
        Self::validate_response_status(response.status())?;
        let status = response.status().as_u16();

        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|err| {
                AppError::Run(format!(
                    "http_get read failed ({})",
                    Self::classify_request_error(&err)
                ))
            })?;
            if body.len() + chunk.len() > self.max_bytes {
                return Err(AppError::Run("http_get response too large".to_string()));
            }
            body.extend_from_slice(&chunk);
        }

        Ok(json!({
            "status": status,
            "body": String::from_utf8_lossy(&body),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::StatusCode;
    use serde_json::json;

    use super::HttpGetTool;
    use crate::tools::registry::{Tool, ToolContext};

    #[tokio::test]
    async fn rejects_non_https_urls() {
        let tool = HttpGetTool::default();

        let error = tool
            .call(
                json!({"url":"http://example.com"}),
                ToolContext {
                    run_id: "run_test".to_string(),
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "run error: http_get only allows https URLs"
        );
    }

    #[tokio::test]
    async fn allowlist_permits_allowed_host() {
        let tool = HttpGetTool::new_with_allowlist(
            Duration::from_millis(50),
            1024,
            vec!["allowed.example".to_string()],
        )
        .unwrap();

        let error = tool
            .call(
                json!({"url":"https://allowed.example/path"}),
                ToolContext {
                    run_id: "run_test".to_string(),
                },
            )
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .starts_with("run error: http_get request failed"));
    }

    #[tokio::test]
    async fn allowlist_rejects_disallowed_host_before_request() {
        let tool = HttpGetTool::new_with_allowlist(
            Duration::from_secs(1),
            1024,
            vec!["allowed.example".to_string()],
        )
        .unwrap();

        let error = tool
            .call(
                json!({"url":"https://blocked.example/path"}),
                ToolContext {
                    run_id: "run_test".to_string(),
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "run error: http_get host is not in the allowlist"
        );
    }

    #[tokio::test]
    async fn request_failure_error_is_sanitized() {
        let tool = HttpGetTool::default();
        let secret_url = "https://user:pass@127.0.0.1:1/private?token=secret-token";

        let error = tool
            .call(
                json!({"url": secret_url}),
                ToolContext {
                    run_id: "run_test".to_string(),
                },
            )
            .await
            .unwrap_err();

        let message = error.to_string();
        assert!(message.starts_with("run error: http_get request failed"));
        assert!(!message.contains("secret-token"));
        assert!(!message.contains("user:pass"));
        assert!(!message.contains("127.0.0.1:1"));
        assert!(!message.contains("/private"));
    }

    #[test]
    fn redirect_status_is_rejected_with_sanitized_error() {
        let error = HttpGetTool::validate_response_status(StatusCode::FOUND)
            .unwrap_err()
            .to_string();

        assert_eq!(error, "run error: http_get redirect blocked");
    }
}
