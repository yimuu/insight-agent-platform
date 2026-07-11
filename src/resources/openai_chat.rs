use std::{collections::BTreeSet, collections::VecDeque, fmt, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, StreamExt};
use jsonschema::JSONSchema;
use reqwest::{redirect::Policy, Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{dsl::CompileError, runtime::RunError};

use super::models::{
    ChatChunk, ChatMessage, ChatModel, ChatRequest, ChatStream, ModelCapability,
    DEFAULT_MAX_ACCUMULATED_TEXT_BYTES,
};

pub const DEFAULT_MAX_UPSTREAM_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_MAX_BUFFERED_LINE_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MAX_EVENT_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MAX_CHUNK_TEXT_BYTES: usize = 256 * 1024;
pub const DEFAULT_MAX_USAGE_JSON_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenAiChatLimits {
    pub max_upstream_bytes: usize,
    pub max_buffered_line_bytes: usize,
    pub max_event_payload_bytes: usize,
    pub max_chunk_text_bytes: usize,
    pub max_usage_json_bytes: usize,
    pub max_accumulated_text_bytes: usize,
}

impl Default for OpenAiChatLimits {
    fn default() -> Self {
        Self {
            max_upstream_bytes: DEFAULT_MAX_UPSTREAM_BYTES,
            max_buffered_line_bytes: DEFAULT_MAX_BUFFERED_LINE_BYTES,
            max_event_payload_bytes: DEFAULT_MAX_EVENT_PAYLOAD_BYTES,
            max_chunk_text_bytes: DEFAULT_MAX_CHUNK_TEXT_BYTES,
            max_usage_json_bytes: DEFAULT_MAX_USAGE_JSON_BYTES,
            max_accumulated_text_bytes: DEFAULT_MAX_ACCUMULATED_TEXT_BYTES,
        }
    }
}

impl OpenAiChatLimits {
    pub fn validate(self) -> Result<Self, CompileError> {
        if [
            self.max_upstream_bytes,
            self.max_buffered_line_bytes,
            self.max_event_payload_bytes,
            self.max_chunk_text_bytes,
            self.max_usage_json_bytes,
            self.max_accumulated_text_bytes,
        ]
        .contains(&0)
        {
            return Err(CompileError::new(
                "MODEL_CONFIG_INVALID",
                "OpenAI response limits must be greater than zero",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone)]
pub struct OpenAiChatModel {
    client: Client,
    api_key: Option<String>,
    endpoint: Url,
    model: String,
    capabilities: BTreeSet<ModelCapability>,
    parameter_validator: std::sync::Arc<JSONSchema>,
    limits: OpenAiChatLimits,
}

impl OpenAiChatModel {
    pub fn new(
        api_key: Option<String>,
        base_url: String,
        model: String,
        capabilities: BTreeSet<ModelCapability>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, CompileError> {
        Self::new_with_limits(
            api_key,
            base_url,
            model,
            capabilities,
            connect_timeout,
            request_timeout,
            OpenAiChatLimits::default(),
        )
    }

    pub fn new_with_limits(
        api_key: Option<String>,
        base_url: String,
        model: String,
        capabilities: BTreeSet<ModelCapability>,
        connect_timeout: Duration,
        request_timeout: Duration,
        limits: OpenAiChatLimits,
    ) -> Result<Self, CompileError> {
        if model.trim().is_empty() || connect_timeout.is_zero() || request_timeout.is_zero() {
            return Err(CompileError::new(
                "MODEL_CONFIG_INVALID",
                "OpenAI model and timeouts must be non-empty",
            ));
        }
        let limits = limits.validate()?;
        let mut endpoint = Url::parse(&base_url)
            .map_err(|_| CompileError::new("MODEL_CONFIG_INVALID", "OpenAI base URL is invalid"))?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            return Err(CompileError::new(
                "MODEL_CONFIG_INVALID",
                "OpenAI base URL must use HTTP or HTTPS and include a host",
            ));
        }
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        let path = format!("{}/chat/completions", endpoint.path().trim_end_matches('/'));
        endpoint.set_path(&path);
        let client = Client::builder()
            .redirect(Policy::none())
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .map_err(|_| {
                CompileError::new("MODEL_CONFIG_INVALID", "failed to build OpenAI HTTP client")
            })?;
        let parameter_validator = JSONSchema::compile(&parameter_schema()).map_err(|_| {
            CompileError::new(
                "MODEL_CONFIG_INVALID",
                "failed to compile OpenAI parameter schema",
            )
        })?;
        Ok(Self {
            client,
            api_key,
            endpoint,
            model,
            capabilities,
            parameter_validator: std::sync::Arc::new(parameter_validator),
            limits,
        })
    }
}

impl fmt::Debug for OpenAiChatModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiChatModel")
            .field("endpoint_origin", &endpoint_origin(&self.endpoint))
            .field("model", &self.model)
            .field("capabilities", &self.capabilities)
            .field("api_key", &"REDACTED")
            .field("client", &"REDACTED")
            .finish()
    }
}

#[async_trait]
impl ChatModel for OpenAiChatModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        self.capabilities.clone()
    }

    fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError> {
        if self.parameter_validator.is_valid(parameters) {
            Ok(())
        } else {
            Err(CompileError::new(
                "MODEL_PARAMETERS_INVALID",
                "OpenAI parameters do not match the allowed schema",
            ))
        }
    }

    fn max_accumulated_text_bytes(&self) -> usize {
        self.limits.max_accumulated_text_bytes
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        self.validate_parameters(&request.parameters)
            .map_err(|error| RunError::new(error.code(), error.to_string()))?;
        let parameters = request.parameters.as_object().cloned().ok_or_else(|| {
            RunError::new(
                "MODEL_PARAMETERS_INVALID",
                "model parameters must be an object",
            )
        })?;
        let messages_count = request.messages.len();
        let image_parts_count = request
            .messages
            .iter()
            .map(|message| message.image_urls().len())
            .sum::<usize>();
        tracing::info!(
            model = self.model,
            messages_count,
            image_parts_count,
            "sending OpenAI-compatible chat request"
        );
        let body = OpenAiRequest {
            model: &self.model,
            messages: &request.messages,
            stream: true,
            parameters,
        };
        let request = self.client.post(self.endpoint.clone()).json(&body);
        let request = match &self.api_key {
            Some(api_key) => request.bearer_auth(api_key),
            None => request,
        };
        let response = request.send().await.map_err(|error| {
            RunError::new(
                "UPSTREAM_TRANSPORT",
                format!(
                    "chat provider request failed ({})",
                    classify_request_error(&error)
                ),
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(RunError::new(
                "UPSTREAM_STATUS",
                format!("chat provider returned HTTP {}", status.as_u16()),
            ));
        }

        let stream = stream::try_unfold(
            StreamState {
                bytes: Box::pin(response.bytes_stream()),
                decoder: SseDecoder::default(),
                pending: VecDeque::new(),
            },
            |mut state| async move {
                loop {
                    if let Some(chunk) = state.pending.pop_front() {
                        return Ok(Some((chunk, state)));
                    }
                    match state.bytes.next().await {
                        Some(Ok(bytes)) => {
                            state.pending.extend(state.decoder.push(&bytes)?);
                        }
                        Some(Err(error)) => {
                            return Err(RunError::new(
                                "UPSTREAM_STREAM",
                                format!(
                                    "chat provider stream failed ({})",
                                    classify_request_error(&error)
                                ),
                            ));
                        }
                        None => {
                            state.pending.extend(state.decoder.finish()?);
                            if let Some(chunk) = state.pending.pop_front() {
                                return Ok(Some((chunk, state)));
                            }
                            return Ok(None);
                        }
                    }
                }
            },
        );
        Ok(Box::pin(stream))
    }
}

#[derive(Serialize)]
struct OpenAiRequest<'a> {
    model: &'a str,
    messages: &'a [ChatMessage],
    stream: bool,
    #[serde(flatten)]
    parameters: Map<String, Value>,
}

struct StreamState {
    bytes: std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    decoder: SseDecoder,
    pending: VecDeque<ChatChunk>,
}

#[derive(Default)]
struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<ChatChunk>, RunError> {
        self.buffer.extend_from_slice(bytes);
        self.drain_complete_lines()
    }

    fn finish(&mut self) -> Result<Vec<ChatChunk>, RunError> {
        let mut chunks = self.drain_complete_lines()?;
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            chunks.extend(parse_sse_line(trim_carriage_return(&line))?);
        }
        Ok(chunks)
    }

    fn drain_complete_lines(&mut self) -> Result<Vec<ChatChunk>, RunError> {
        let mut chunks = Vec::new();
        while let Some(index) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = self.buffer.drain(..=index).collect::<Vec<_>>();
            line.pop();
            chunks.extend(parse_sse_line(trim_carriage_return(&line))?);
        }
        Ok(chunks)
    }
}

fn trim_carriage_return(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn parse_sse_line(line: &[u8]) -> Result<Vec<ChatChunk>, RunError> {
    if line.is_empty() {
        return Ok(Vec::new());
    }
    let line = std::str::from_utf8(line)
        .map_err(|_| RunError::new("UPSTREAM_STREAM_INVALID", "invalid UTF-8 in chat stream"))?;
    let Some(payload) = line.strip_prefix("data:") else {
        return Ok(Vec::new());
    };
    let payload = payload.trim_start();
    if payload == "[DONE]" {
        return Ok(Vec::new());
    }
    let parsed: OpenAiChunk = serde_json::from_str(payload).map_err(|_| {
        RunError::new(
            "UPSTREAM_STREAM_INVALID",
            "invalid chat provider stream payload",
        )
    })?;
    let choice_count = parsed.choices.len();
    let mut chunks = parsed
        .choices
        .into_iter()
        .enumerate()
        .filter_map(|(index, choice)| {
            let text = choice.delta.content.unwrap_or_default();
            let usage = (index + 1 == choice_count)
                .then(|| parsed.usage.clone())
                .flatten();
            (!text.is_empty() || choice.finish_reason.is_some() || usage.is_some()).then_some(
                ChatChunk {
                    text,
                    finish_reason: choice.finish_reason,
                    usage,
                },
            )
        })
        .collect::<Vec<_>>();
    if chunks.is_empty() {
        if let Some(usage) = parsed.usage {
            chunks.push(ChatChunk {
                text: String::new(),
                finish_reason: None,
                usage: Some(usage),
            });
        }
    }
    Ok(chunks)
}

#[derive(Deserialize)]
struct OpenAiChunk {
    #[serde(default)]
    choices: Vec<OpenAiChoice>,
    usage: Option<Value>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    delta: OpenAiDelta,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiDelta {
    content: Option<String>,
}

fn parameter_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "temperature":{"type":"number", "minimum":0, "maximum":2},
            "max_tokens":{"type":"integer", "minimum":1},
            "top_p":{"type":"number", "minimum":0, "maximum":1},
            "frequency_penalty":{"type":"number", "minimum":-2, "maximum":2},
            "presence_penalty":{"type":"number", "minimum":-2, "maximum":2},
            "stop":{
                "oneOf":[
                    {"type":"string"},
                    {"type":"array", "items":{"type":"string"}, "minItems":1}
                ]
            }
        },
        "additionalProperties":false
    })
}

fn endpoint_origin(endpoint: &Url) -> String {
    match (endpoint.host_str(), endpoint.port()) {
        (Some(host), Some(port)) => format!("{}://{host}:{port}", endpoint.scheme()),
        (Some(host), None) => format!("{}://{host}", endpoint.scheme()),
        (None, _) => "REDACTED".to_string(),
    }
}

fn classify_request_error(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connection"
    } else if error.is_request() {
        "request"
    } else {
        "transport"
    }
}
