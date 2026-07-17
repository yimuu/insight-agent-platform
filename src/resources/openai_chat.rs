use std::{
    collections::{BTreeSet, VecDeque},
    fmt,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, StreamExt};
use reqwest::{redirect::Policy, Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{
    dsl::CompileError,
    observability::{elapsed_ms, json_size_bytes},
    runtime::RunError,
    schema::{compile_schema, JsonSchemaValidator},
};

use super::models::{
    model_response_too_large, serialized_json_within_limit, validate_chat_request, ChatChunk,
    ChatContent, ChatContentPart, ChatMessage, ChatModel, ChatRequest, ChatResponseFormat,
    ChatRole, ChatStream, ModelCapability, DEFAULT_MAX_ACCUMULATED_TEXT_BYTES,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiTransportPolicy {
    HttpsOnly,
    AllowLoopbackHttp,
    AllowTrustedPrivateHttp,
}

#[derive(Clone)]
pub struct OpenAiChatModel {
    client: Client,
    api_key: Option<String>,
    endpoint: Url,
    model: String,
    capabilities: BTreeSet<ModelCapability>,
    parameter_validator: std::sync::Arc<JsonSchemaValidator>,
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
        Self::new_with_limits_and_transport_policy(
            api_key,
            base_url,
            model,
            capabilities,
            connect_timeout,
            request_timeout,
            limits,
            OpenAiTransportPolicy::HttpsOnly,
        )
    }

    // Intentional: mirrors `new_with_limits` and adds an explicit transport policy
    // so plaintext HTTP opt-in remains visible at the call site.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_limits_and_transport_policy(
        api_key: Option<String>,
        base_url: String,
        model: String,
        capabilities: BTreeSet<ModelCapability>,
        connect_timeout: Duration,
        request_timeout: Duration,
        limits: OpenAiChatLimits,
        transport_policy: OpenAiTransportPolicy,
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
        validate_endpoint_transport(&endpoint, &base_url, transport_policy)?;
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        let path = format!("{}/chat/completions", endpoint.path().trim_end_matches('/'));
        endpoint.set_path(&path);
        let client = Client::builder()
            .tls_backend_rustls()
            .redirect(Policy::none())
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .map_err(|_| {
                CompileError::new("MODEL_CONFIG_INVALID", "failed to build OpenAI HTTP client")
            })?;
        let parameter_validator = compile_schema(&parameter_schema()).map_err(|_| {
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

    fn request_body_within_limit(&self, request: &ChatRequest, max_bytes: usize) -> bool {
        let Some(parameters) = request.parameters.as_object().cloned() else {
            return false;
        };
        let Some(wire_messages) = openai_wire_messages(request) else {
            return false;
        };
        let messages = wire_messages
            .iter()
            .map(OpenAiMessage::from)
            .collect::<Vec<_>>();
        let response_format = openai_response_format(request.response_format.as_ref());
        serialized_json_within_limit(
            &OpenAiRequest {
                model: &self.model,
                messages: &messages,
                stream: true,
                response_format,
                parameters,
            },
            max_bytes,
        )
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        validate_chat_request(&request)?;
        if request
            .messages
            .iter()
            .any(|message| !message.image_urls().is_empty())
            && !self.capabilities.contains(&ModelCapability::Vision)
        {
            return Err(RunError::operation(
                "VNEXT_LLM_VISION_REQUIRED",
                "chat provider request requires a vision-capable model",
            ));
        }
        if let Some(response_format) = &request.response_format {
            if !self
                .capabilities
                .contains(&response_format.required_capability())
            {
                return Err(RunError::operation(
                    "VNEXT_LLM_STRUCTURED_OUTPUT_REQUIRED",
                    "chat provider request requires the selected structured-output capability",
                ));
            }
        }
        self.validate_parameters(&request.parameters)
            .map_err(|error| RunError::operation(error.code(), error.to_string()))?;
        let parameters = request.parameters.as_object().cloned().ok_or_else(|| {
            RunError::operation(
                "MODEL_PARAMETERS_INVALID",
                "model parameters must be an object",
            )
        })?;
        let parameters_keys_count = parameters.len();
        let wire_messages = openai_wire_messages(&request).ok_or_else(|| {
            RunError::operation(
                "VNEXT_LLM_RESPONSE_CONFIG_INVALID",
                "chat provider structured response format is invalid",
            )
        })?;
        let messages_count = wire_messages.len();
        let image_parts_count = request
            .messages
            .iter()
            .map(|message| message.image_urls().len())
            .sum::<usize>();
        tracing::info!(
            event_name = "openai.request",
            model = self.model.as_str(),
            messages_count,
            image_parts_count,
            parameters_keys_count,
            "sending OpenAI-compatible chat request"
        );
        let messages = wire_messages
            .iter()
            .map(OpenAiMessage::from)
            .collect::<Vec<_>>();
        let response_format = openai_response_format(request.response_format.as_ref());
        let body = OpenAiRequest {
            model: &self.model,
            messages: &messages,
            stream: true,
            response_format,
            parameters,
        };
        let request = self.client.post(self.endpoint.clone()).json(&body);
        let request = match &self.api_key {
            Some(api_key) => request.bearer_auth(api_key),
            None => request,
        };
        let response = request.send().await.map_err(|error| {
            RunError::operation(
                "UPSTREAM_TRANSPORT",
                format!(
                    "chat provider request failed ({})",
                    classify_request_error(&error)
                ),
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(RunError::operation(
                "UPSTREAM_STATUS",
                format!("chat provider returned HTTP {}", status.as_u16()),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.limits.max_upstream_bytes as u64)
        {
            return Err(model_response_too_large());
        }

        let stream = stream::try_unfold(
            StreamState {
                bytes: Some(Box::pin(response.bytes_stream())),
                decoder: SseDecoder::new(self.limits),
                pending: VecDeque::new(),
                upstream_bytes: 0,
                limits: self.limits,
                model: self.model.clone(),
                started: Instant::now(),
                chunks_count: 0,
                usage_bytes: 0,
                clean_eof: false,
            },
            |mut state| async move {
                loop {
                    if let Some(chunk) = state.pop_pending_chunk() {
                        return Ok(Some((chunk, state)));
                    }
                    if state.decoder.is_complete() {
                        state.log_response_metadata();
                        return Ok(None);
                    }
                    if state.clean_eof {
                        return Err(incomplete_stream());
                    }
                    let next_bytes = state
                        .bytes
                        .as_mut()
                        .expect("response body must exist before stream completion")
                        .next()
                        .await;
                    match next_bytes {
                        Some(Ok(bytes)) => {
                            if state.upstream_bytes.saturating_add(bytes.len())
                                > state.limits.max_upstream_bytes
                            {
                                return Err(model_response_too_large());
                            }
                            state.upstream_bytes += bytes.len();
                            state.pending.extend(state.decoder.push(&bytes)?);
                            if state.decoder.is_complete() {
                                state.bytes.take();
                            }
                        }
                        Some(Err(error)) => {
                            return Err(RunError::operation(
                                "UPSTREAM_STREAM",
                                format!(
                                    "chat provider stream failed ({})",
                                    classify_request_error(&error)
                                ),
                            ));
                        }
                        None => {
                            state.bytes.take();
                            state.clean_eof = true;
                            state.pending.extend(state.decoder.finish()?);
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
    messages: &'a [OpenAiMessage<'a>],
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<OpenAiResponseFormat<'a>>,
    #[serde(flatten)]
    parameters: Map<String, Value>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OpenAiResponseFormat<'a> {
    JsonObject,
    JsonSchema { json_schema: OpenAiJsonSchema<'a> },
}

#[derive(Serialize)]
struct OpenAiJsonSchema<'a> {
    name: &'a str,
    strict: bool,
    schema: &'a Value,
}

fn openai_response_format(
    response_format: Option<&ChatResponseFormat>,
) -> Option<OpenAiResponseFormat<'_>> {
    response_format.map(|format| match format {
        ChatResponseFormat::JsonObject { .. } => OpenAiResponseFormat::JsonObject,
        ChatResponseFormat::JsonSchema { name, schema } => OpenAiResponseFormat::JsonSchema {
            json_schema: OpenAiJsonSchema {
                name,
                strict: true,
                schema,
            },
        },
    })
}

fn openai_wire_messages(request: &ChatRequest) -> Option<Vec<ChatMessage>> {
    let mut messages = request.messages.clone();
    let Some(ChatResponseFormat::JsonObject { schema, .. }) = &request.response_format else {
        return Some(messages);
    };
    let schema = serde_json::to_string(schema).ok()?;
    let instruction =
        format!("\n\nReturn only a valid JSON object matching this JSON Schema:\n{schema}");
    let final_message = messages.last_mut()?;
    if final_message.role != ChatRole::User {
        return None;
    }
    match &mut final_message.content {
        ChatContent::Text(text) => text.push_str(&instruction),
        ChatContent::Parts(parts) => parts.push(ChatContentPart::Text { text: instruction }),
    }
    Some(messages)
}

#[derive(Serialize)]
struct OpenAiMessage<'a> {
    role: ChatRole,
    content: OpenAiContent<'a>,
}

impl<'a> From<&'a ChatMessage> for OpenAiMessage<'a> {
    fn from(message: &'a ChatMessage) -> Self {
        let content = match &message.content {
            ChatContent::Text(text) => OpenAiContent::Text(text),
            ChatContent::Parts(parts) => OpenAiContent::Parts(
                parts
                    .iter()
                    .map(|part| match part {
                        ChatContentPart::Text { text } => OpenAiContentPart::Text { text },
                        ChatContentPart::Image { image } => OpenAiContentPart::ImageUrl {
                            image_url: OpenAiImageUrl { url: image },
                        },
                    })
                    .collect(),
            ),
        };
        Self {
            role: message.role,
            content,
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum OpenAiContent<'a> {
    Text(&'a str),
    Parts(Vec<OpenAiContentPart<'a>>),
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OpenAiContentPart<'a> {
    Text { text: &'a str },
    ImageUrl { image_url: OpenAiImageUrl<'a> },
}

#[derive(Serialize)]
struct OpenAiImageUrl<'a> {
    url: &'a str,
}

type ResponseByteStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

struct StreamState {
    bytes: Option<ResponseByteStream>,
    decoder: SseDecoder,
    pending: VecDeque<ChatChunk>,
    upstream_bytes: usize,
    limits: OpenAiChatLimits,
    model: String,
    started: Instant,
    chunks_count: usize,
    usage_bytes: usize,
    clean_eof: bool,
}

impl StreamState {
    fn pop_pending_chunk(&mut self) -> Option<ChatChunk> {
        let chunk = self.pending.pop_front()?;
        self.chunks_count += 1;
        self.usage_bytes = self
            .usage_bytes
            .saturating_add(chunk.usage.as_ref().map_or(0, json_size_bytes));
        Some(chunk)
    }

    fn log_response_metadata(&self) {
        tracing::info!(
            event_name = "openai.response",
            model = self.model.as_str(),
            upstream_bytes = self.upstream_bytes,
            chunks_count = self.chunks_count,
            usage_bytes = self.usage_bytes,
            elapsed_ms = elapsed_ms(self.started),
            "OpenAI-compatible chat response metadata"
        );
    }
}

struct SseDecoder {
    buffer: Vec<u8>,
    limits: OpenAiChatLimits,
    complete: bool,
}

impl SseDecoder {
    fn new(limits: OpenAiChatLimits) -> Self {
        Self {
            buffer: Vec::new(),
            limits,
            complete: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<Vec<ChatChunk>, RunError> {
        if self.complete {
            return Ok(Vec::new());
        }
        let mut chunks = Vec::new();
        for segment in bytes.split_inclusive(|byte| *byte == b'\n') {
            let includes_lf = segment.last() == Some(&b'\n');
            let projected_line_len = self
                .buffer
                .len()
                .saturating_add(segment.len())
                .saturating_sub(usize::from(includes_lf));
            if projected_line_len > self.limits.max_buffered_line_bytes {
                return Err(model_response_too_large());
            }
            self.buffer.extend_from_slice(segment);
            chunks.extend(self.drain_complete_lines()?);
            if self.complete {
                break;
            }
        }
        Ok(chunks)
    }

    fn finish(&mut self) -> Result<Vec<ChatChunk>, RunError> {
        if self.complete {
            return Ok(Vec::new());
        }
        let mut chunks = self.drain_complete_lines()?;
        if !self.complete && !self.buffer.is_empty() {
            if self.buffer.len() > self.limits.max_buffered_line_bytes {
                return Err(model_response_too_large());
            }
            let line = std::mem::take(&mut self.buffer);
            chunks.extend(self.parse_line(trim_carriage_return(&line))?);
        }
        Ok(chunks)
    }

    fn is_complete(&self) -> bool {
        self.complete
    }

    fn drain_complete_lines(&mut self) -> Result<Vec<ChatChunk>, RunError> {
        let mut chunks = Vec::new();
        while !self.complete {
            let Some(index) = self.buffer.iter().position(|byte| *byte == b'\n') else {
                break;
            };
            if index > self.limits.max_buffered_line_bytes {
                return Err(model_response_too_large());
            }
            let mut line = self.buffer.drain(..=index).collect::<Vec<_>>();
            line.pop();
            chunks.extend(self.parse_line(trim_carriage_return(&line))?);
        }
        if self.complete {
            self.buffer.clear();
        }
        Ok(chunks)
    }

    fn parse_line(&mut self, line: &[u8]) -> Result<Vec<ChatChunk>, RunError> {
        match parse_sse_line(line, self.limits)? {
            ParsedSseLine::Chunks(chunks) => Ok(chunks),
            ParsedSseLine::Done => {
                self.complete = true;
                Ok(Vec::new())
            }
        }
    }
}

fn trim_carriage_return(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

enum ParsedSseLine {
    Chunks(Vec<ChatChunk>),
    Done,
}

fn parse_sse_line(line: &[u8], limits: OpenAiChatLimits) -> Result<ParsedSseLine, RunError> {
    if line.is_empty() {
        return Ok(ParsedSseLine::Chunks(Vec::new()));
    }
    let Some(payload) = line.strip_prefix(b"data:") else {
        return Ok(ParsedSseLine::Chunks(Vec::new()));
    };
    let payload = trim_ascii_space(payload);
    if payload.len() > limits.max_event_payload_bytes {
        return Err(model_response_too_large());
    }
    if payload == b"[DONE]" {
        return Ok(ParsedSseLine::Done);
    }
    let payload = std::str::from_utf8(payload).map_err(|_| {
        RunError::operation("UPSTREAM_STREAM_INVALID", "invalid UTF-8 in chat stream")
    })?;
    let parsed: OpenAiChunk = serde_json::from_str(payload).map_err(|_| {
        RunError::operation(
            "UPSTREAM_STREAM_INVALID",
            "invalid chat provider stream payload",
        )
    })?;
    let choice_count = parsed.choices.len();
    let mut chunks = Vec::new();
    for (index, choice) in parsed.choices.into_iter().enumerate() {
        let text = choice.delta.content.unwrap_or_default();
        if text.len() > limits.max_chunk_text_bytes {
            return Err(model_response_too_large());
        }
        let usage = (index + 1 == choice_count)
            .then(|| parsed.usage.clone())
            .flatten();
        if let Some(usage) = &usage {
            let bytes = serde_json::to_vec(usage).map_err(|_| {
                RunError::operation(
                    "UPSTREAM_STREAM_INVALID",
                    "invalid chat provider stream payload",
                )
            })?;
            if bytes.len() > limits.max_usage_json_bytes {
                return Err(model_response_too_large());
            }
        }
        if !text.is_empty() || choice.finish_reason.is_some() || usage.is_some() {
            chunks.push(ChatChunk {
                text,
                finish_reason: choice.finish_reason,
                usage,
            });
        }
    }
    if chunks.is_empty() {
        if let Some(usage) = parsed.usage {
            let bytes = serde_json::to_vec(&usage).map_err(|_| {
                RunError::operation(
                    "UPSTREAM_STREAM_INVALID",
                    "invalid chat provider stream payload",
                )
            })?;
            if bytes.len() > limits.max_usage_json_bytes {
                return Err(model_response_too_large());
            }
            chunks.push(ChatChunk {
                text: String::new(),
                finish_reason: None,
                usage: Some(usage),
            });
        }
    }
    Ok(ParsedSseLine::Chunks(chunks))
}

fn incomplete_stream() -> RunError {
    RunError::operation(
        "UPSTREAM_STREAM_INCOMPLETE",
        "chat provider stream ended without completion evidence",
    )
}

fn trim_ascii_space(bytes: &[u8]) -> &[u8] {
    let first_non_space = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    &bytes[first_non_space..]
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

fn validate_endpoint_transport(
    endpoint: &Url,
    base_url: &str,
    policy: OpenAiTransportPolicy,
) -> Result<(), CompileError> {
    if endpoint.host_str().is_none() {
        return Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI base URL must include a host",
        ));
    }
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI base URL must not include username or password",
        ));
    }
    match endpoint.scheme() {
        "https" => Ok(()),
        "http" => validate_plaintext_http(base_url, policy),
        _ => Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI base URL must use HTTP or HTTPS and include a host",
        )),
    }
}

fn validate_plaintext_http(
    base_url: &str,
    policy: OpenAiTransportPolicy,
) -> Result<(), CompileError> {
    match policy {
        OpenAiTransportPolicy::HttpsOnly => Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI base URL must use HTTPS unless plaintext HTTP is explicitly allowed",
        )),
        OpenAiTransportPolicy::AllowLoopbackHttp if has_exact_raw_loopback_host(base_url) => Ok(()),
        OpenAiTransportPolicy::AllowLoopbackHttp => Err(CompileError::new(
            "MODEL_CONFIG_INVALID",
            "OpenAI loopback HTTP is restricted to localhost, 127.0.0.1, or [::1]",
        )),
        OpenAiTransportPolicy::AllowTrustedPrivateHttp => Ok(()),
    }
}

fn has_exact_raw_loopback_host(base_url: &str) -> bool {
    matches!(
        raw_authority_host(base_url),
        Some("localhost" | "127.0.0.1" | "[::1]")
    )
}

fn raw_authority_host(base_url: &str) -> Option<&str> {
    let (_, after_scheme) = base_url.split_once("://")?;
    let authority_end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..authority_end];
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host_port)| host_port);

    if authority.starts_with('[') {
        let host_end = authority.find(']')?;
        return Some(&authority[..=host_end]);
    }

    let host_end = authority.find(':').unwrap_or(authority.len());
    Some(&authority[..host_end])
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
