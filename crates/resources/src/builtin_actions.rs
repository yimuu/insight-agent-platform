use std::{collections::BTreeSet, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use chrono_tz::Tz;
use futures::StreamExt;
use reqwest::{redirect::Policy, Url};
use serde_json::{json, Value};
use tokio::time::sleep;

use insight_engine::{author::CompileError, execution::RunError};

use super::actions::{
    Action, ActionCapability, ActionContext, ActionDescriptor, ActionRegistry, CancellationClass,
    EffectClass, IdempotencyClass,
};

#[derive(Debug, Clone, Copy)]
pub struct CurrentTimeAction;

#[async_trait]
impl Action for CurrentTimeAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "current_time",
            version: "1.0.0",
            input_schema: json!({
                "type":"object",
                "properties":{"timezone":{"type":"string"}},
                "additionalProperties":false
            }),
            output_schema: json!({
                "type":"object",
                "required":["timezone", "iso8601"],
                "properties":{
                    "timezone":{"type":"string"},
                    "iso8601":{"type":"string"}
                },
                "additionalProperties":false
            }),
            effect: EffectClass::ReadOnly,
            idempotency: IdempotencyClass::NonIdempotent,
            cancellation: CancellationClass::NotSupported,
            required_capabilities: BTreeSet::from([ActionCapability::new("clock")]),
        }
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        let timezone = input
            .get("timezone")
            .and_then(Value::as_str)
            .unwrap_or("UTC");
        let timezone_value: Tz = timezone.parse().map_err(|_| {
            RunError::operation("ACTION_INPUT_INVALID", "timezone is not recognized")
        })?;
        let now = Utc::now().with_timezone(&timezone_value);
        Ok(json!({
            "timezone": timezone,
            "iso8601": now.to_rfc3339(),
        }))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TextMetricsAction;

#[async_trait]
impl Action for TextMetricsAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "example.text_metrics",
            version: "1.0.0",
            input_schema: json!({
                "type":"object",
                "required":["text"],
                "properties":{"text":{"type":"string"}},
                "additionalProperties":false
            }),
            output_schema: json!({
                "type":"object",
                "required":["characters", "words", "lines"],
                "properties":{
                    "characters":{"type":"integer", "minimum":0},
                    "words":{"type":"integer", "minimum":0},
                    "lines":{"type":"integer", "minimum":0}
                },
                "additionalProperties":false
            }),
            effect: EffectClass::Pure,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::NotSupported,
            required_capabilities: BTreeSet::new(),
        }
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        let text = input.get("text").and_then(Value::as_str).ok_or_else(|| {
            RunError::operation("ACTION_INPUT_INVALID", "text metrics requires text")
        })?;
        Ok(json!({
            "characters": text.chars().count(),
            "words": text.split_whitespace().count(),
            "lines": text.lines().count(),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct RestrictedHttpGetAction {
    client: reqwest::Client,
    max_bytes: usize,
    allowlist: BTreeSet<String>,
}

impl RestrictedHttpGetAction {
    pub fn new(
        timeout: Duration,
        max_bytes: usize,
        allowlist: Vec<String>,
    ) -> Result<Self, CompileError> {
        if timeout.is_zero() || max_bytes == 0 {
            return Err(CompileError::new(
                "ACTION_CONFIG_INVALID",
                "HTTP action timeout and max_bytes must be greater than zero",
            ));
        }
        let allowlist = allowlist
            .into_iter()
            .map(|host| host.trim().to_ascii_lowercase())
            .filter(|host| !host.is_empty())
            .collect::<BTreeSet<_>>();
        if allowlist.is_empty() {
            return Err(CompileError::new(
                "ACTION_CONFIG_INVALID",
                "HTTP action allowlist must not be empty",
            ));
        }
        let client = reqwest::Client::builder()
            .tls_backend_rustls()
            .redirect(Policy::none())
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .map_err(|_| {
                CompileError::new(
                    "ACTION_CONFIG_INVALID",
                    "failed to build restricted HTTP client",
                )
            })?;
        Ok(Self {
            client,
            max_bytes,
            allowlist,
        })
    }

    fn validate_url(&self, value: &str) -> Result<Url, RunError> {
        let parsed = Url::parse(value)
            .map_err(|_| RunError::operation("ACTION_HTTP_BLOCKED", "HTTP URL is invalid"))?;
        if parsed.scheme() != "https" {
            return Err(RunError::operation(
                "ACTION_HTTP_BLOCKED",
                "HTTP action requires HTTPS",
            ));
        }
        let allowed = parsed
            .host_str()
            .map(|host| self.allowlist.contains(&host.to_ascii_lowercase()))
            .unwrap_or(false);
        if !allowed {
            return Err(RunError::operation(
                "ACTION_HTTP_BLOCKED",
                "HTTP host is not allowed",
            ));
        }
        Ok(parsed)
    }
}

#[async_trait]
impl Action for RestrictedHttpGetAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "http_get",
            version: "1.0.0",
            input_schema: json!({
                "type":"object",
                "required":["url"],
                "properties":{"url":{"type":"string"}},
                "additionalProperties":false
            }),
            output_schema: json!({
                "type":"object",
                "required":["status", "body"],
                "properties":{
                    "status":{"type":"integer", "minimum":100, "maximum":599},
                    "body":{"type":"string"}
                },
                "additionalProperties":false
            }),
            effect: EffectClass::ReadOnly,
            idempotency: IdempotencyClass::Idempotent,
            cancellation: CancellationClass::Cooperative,
            required_capabilities: BTreeSet::from([ActionCapability::new("network.https")]),
        }
    }

    async fn call(&self, input: Value, context: ActionContext) -> Result<Value, RunError> {
        let url = input.get("url").and_then(Value::as_str).ok_or_else(|| {
            RunError::operation("ACTION_INPUT_INVALID", "HTTP action requires a URL")
        })?;
        let parsed = self.validate_url(url)?;
        let send = self.client.get(parsed).send();
        tokio::pin!(send);
        let response = tokio::select! {
            result = &mut send => result.map_err(sanitized_http_error)?,
            _ = context.control.stopped() => return Err(stopped_error(&context)),
            _ = sleep(context.control.remaining()) => return Err(RunError::operation_timeout()),
        };
        if response.status().is_redirection() {
            return Err(RunError::operation(
                "ACTION_HTTP_BLOCKED",
                "HTTP redirects are blocked",
            ));
        }
        let status = response.status().as_u16();
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        loop {
            let chunk = tokio::select! {
                chunk = stream.next() => chunk,
                _ = context.control.stopped() => return Err(stopped_error(&context)),
                _ = sleep(context.control.remaining()) => return Err(RunError::operation_timeout()),
            };
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = chunk.map_err(sanitized_http_error)?;
            if body.len().saturating_add(chunk.len()) > self.max_bytes {
                return Err(RunError::operation(
                    "ACTION_HTTP_TOO_LARGE",
                    "HTTP response exceeded the configured size limit",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(json!({
            "status": status,
            "body": String::from_utf8_lossy(&body),
        }))
    }
}

pub fn builtin_action_registry(
    enabled: &[String],
    http_get: Option<RestrictedHttpGetAction>,
) -> Result<ActionRegistry, CompileError> {
    let mut registry = ActionRegistry::default();
    for name in enabled {
        match name.as_str() {
            "current_time" => registry.register(CurrentTimeAction)?,
            "http_get" => registry.register(http_get.clone().ok_or_else(|| {
                CompileError::new(
                    "ACTION_CONFIG_INVALID",
                    "http_get is enabled but is not configured",
                )
            })?)?,
            "example.text_metrics" => registry.register(TextMetricsAction)?,
            _ => {
                return Err(CompileError::new(
                    "ACTION_NOT_FOUND",
                    format!("built-in action '{name}' is not available"),
                ));
            }
        }
    }
    Ok(registry)
}

fn stopped_error(context: &ActionContext) -> RunError {
    context
        .control
        .stop_reason()
        .map(RunError::stopped)
        .unwrap_or_else(|| RunError::operation("RUN_STOPPED", "run stopped"))
}

fn sanitized_http_error(error: reqwest::Error) -> RunError {
    let kind = if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connection"
    } else if error.is_request() {
        "request"
    } else {
        "transport"
    };
    RunError::operation("ACTION_HTTP_FAILED", format!("HTTP action failed ({kind})"))
}
