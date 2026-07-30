use std::{
    collections::{BTreeSet, VecDeque},
    fmt,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, StreamExt};
use reqwest::{redirect::Policy, Client, Response, StatusCode, Url};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Map, Value};

use insight_engine::{
    author::CompileError,
    execution::RunError,
    schema::{compile_schema, JsonSchemaValidator},
};

use crate::observability::{elapsed_ms, json_size_bytes};

use super::models::{
    model_response_too_large, serialized_json_within_limit, validate_chat_request, ChatChunk,
    ChatContent, ChatContentPart, ChatEvent, ChatEventStream, ChatFinishReason,
    ChatInputTokensDetails, ChatMessage, ChatModel, ChatOutputTokensDetails, ChatRequest,
    ChatRequestMode, ChatResponse, ChatResponseFormat, ChatRole, ChatStream, ChatToolCall,
    ChatToolCallDelta, ChatToolChoice, ChatToolDefinition, ChatUsage, ModelCapability,
    ModelRequestCapability, DEFAULT_MAX_ACCUMULATED_TEXT_BYTES,
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

impl OpenAiTransportPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HttpsOnly => "https_only",
            Self::AllowLoopbackHttp => "allow_loopback_http",
            Self::AllowTrustedPrivateHttp => "allow_trusted_private_http",
        }
    }
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

struct PreparedOpenAiRequest {
    wire_messages: Vec<ChatMessage>,
    parameters: Map<String, Value>,
    response_format: Option<ChatResponseFormat>,
    tools: Vec<ChatToolDefinition>,
    tool_choice: ChatToolChoice,
    messages_count: usize,
    image_parts_count: usize,
    parameters_keys_count: usize,
}

impl OpenAiChatModel {
    fn prepare_request(&self, request: &ChatRequest) -> Result<PreparedOpenAiRequest, RunError> {
        validate_chat_request(request)?;
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
        let wire_messages = request.messages.clone();
        Ok(PreparedOpenAiRequest {
            messages_count: wire_messages.len(),
            image_parts_count: request
                .messages
                .iter()
                .map(|message| message.image_urls().len())
                .sum(),
            parameters_keys_count: parameters.len(),
            wire_messages,
            parameters,
            response_format: request.response_format.clone(),
            tools: request.tools.clone(),
            tool_choice: request.tool_choice.clone(),
        })
    }

    fn body_within_limit(
        &self,
        request: &ChatRequest,
        mode: ChatRequestMode,
        max_bytes: usize,
    ) -> bool {
        let Ok(prepared) = self.prepare_request(request) else {
            return false;
        };
        let messages = prepared
            .wire_messages
            .iter()
            .map(OpenAiMessage::from)
            .collect::<Vec<_>>();
        let response_format = openai_response_format(prepared.response_format.as_ref());
        let tools = prepared
            .tools
            .iter()
            .map(OpenAiTool::from)
            .collect::<Vec<_>>();
        let tool_choice = openai_tool_choice(&prepared.tools, &prepared.tool_choice);
        serialized_json_within_limit(
            &OpenAiRequest {
                model: &self.model,
                messages: &messages,
                stream: mode == ChatRequestMode::Streaming,
                stream_options: (mode == ChatRequestMode::Streaming).then_some(
                    OpenAiStreamOptions {
                        include_usage: true,
                    },
                ),
                response_format,
                tools: &tools,
                tool_choice,
                parameters: prepared.parameters,
            },
            max_bytes,
        )
    }

    async fn send_request(
        &self,
        request: &ChatRequest,
        mode: ChatRequestMode,
    ) -> Result<Response, RunError> {
        let prepared = self.prepare_request(request)?;
        tracing::info!(
            event_name = "openai.request",
            provider_origin = endpoint_origin(&self.endpoint),
            model = self.model.as_str(),
            request_mode = match mode {
                ChatRequestMode::Complete => "complete",
                ChatRequestMode::Streaming => "streaming",
            },
            messages_count = prepared.messages_count,
            image_parts_count = prepared.image_parts_count,
            tools_count = prepared.tools.len(),
            parameters_keys_count = prepared.parameters_keys_count,
            "sending OpenAI-compatible chat request"
        );
        let messages = prepared
            .wire_messages
            .iter()
            .map(OpenAiMessage::from)
            .collect::<Vec<_>>();
        let response_format = openai_response_format(prepared.response_format.as_ref());
        let tools = prepared
            .tools
            .iter()
            .map(OpenAiTool::from)
            .collect::<Vec<_>>();
        let tool_choice = openai_tool_choice(&prepared.tools, &prepared.tool_choice);
        let body = OpenAiRequest {
            model: &self.model,
            messages: &messages,
            stream: mode == ChatRequestMode::Streaming,
            stream_options: (mode == ChatRequestMode::Streaming).then_some(OpenAiStreamOptions {
                include_usage: true,
            }),
            response_format,
            tools: &tools,
            tool_choice,
            parameters: prepared.parameters,
        };
        let request = self.client.post(self.endpoint.clone()).json(&body);
        let request = match &self.api_key {
            Some(api_key) => request.bearer_auth(api_key),
            None => request,
        };
        let response = request.send().await.map_err(|error| {
            let transport_kind = classify_request_error(&error);
            let failure_code = classify_request_failure_code(&error);
            tracing::warn!(
                event_name = "openai.request_failed",
                provider_origin = endpoint_origin(&self.endpoint),
                model = self.model.as_str(),
                request_mode = match mode {
                    ChatRequestMode::Complete => "complete",
                    ChatRequestMode::Streaming => "streaming",
                },
                failure_code,
                transport_kind,
                "OpenAI-compatible chat request failed"
            );
            RunError::operation(
                failure_code,
                format!("chat provider request failed ({transport_kind})"),
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            let failure_code = classify_status_failure_code(status);
            tracing::warn!(
                event_name = "openai.request_failed",
                provider_origin = endpoint_origin(&self.endpoint),
                model = self.model.as_str(),
                request_mode = match mode {
                    ChatRequestMode::Complete => "complete",
                    ChatRequestMode::Streaming => "streaming",
                },
                failure_code,
                http_status = status.as_u16(),
                "OpenAI-compatible chat request failed"
            );
            return Err(RunError::operation(
                failure_code,
                format!("chat provider returned HTTP {}", status.as_u16()),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.limits.max_upstream_bytes as u64)
        {
            return Err(model_response_too_large());
        }
        Ok(response)
    }
}

#[async_trait]
impl ChatModel for OpenAiChatModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        self.capabilities.clone()
    }

    fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
        BTreeSet::from([
            ModelRequestCapability::Complete,
            ModelRequestCapability::Streaming,
        ])
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
        self.body_within_limit(request, ChatRequestMode::Streaming, max_bytes)
    }

    fn request_body_within_limit_for_mode(
        &self,
        request: &ChatRequest,
        mode: ChatRequestMode,
        max_bytes: usize,
    ) -> bool {
        self.body_within_limit(request, mode, max_bytes)
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, RunError> {
        let started = Instant::now();
        let response = self
            .send_request(&request, ChatRequestMode::Complete)
            .await?;
        let bytes = read_bounded_complete_body(response, self.limits.max_upstream_bytes).await?;
        let parsed = parse_complete_response(&bytes, self.limits)?;
        tracing::info!(
            event_name = "openai.response",
            model = self.model.as_str(),
            request_mode = "complete",
            upstream_bytes = bytes.len(),
            usage_bytes = parsed.usage.as_ref().map_or(0, normalized_usage_size_bytes),
            elapsed_ms = elapsed_ms(started),
            "OpenAI-compatible chat response metadata"
        );
        Ok(parsed)
    }

    async fn stream_chat_events(&self, request: ChatRequest) -> Result<ChatEventStream, RunError> {
        let response = self
            .send_request(&request, ChatRequestMode::Streaming)
            .await?;

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

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        let stream = self.stream_chat_events(request).await?;
        Ok(Box::pin(stream.map(|event| {
            let event = event?;
            Ok(ChatChunk {
                text: event.text_delta,
                finish_reason: event.finish_reason.map(|reason| reason.as_str().to_owned()),
                usage: event.usage.map(|usage| {
                    serde_json::to_value(usage)
                        .expect("the closed normalized usage type is always serializable")
                }),
            })
        })))
    }
}

#[derive(Serialize)]
struct OpenAiRequest<'a> {
    model: &'a str,
    messages: &'a [OpenAiMessage<'a>],
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<OpenAiStreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<OpenAiResponseFormat<'a>>,
    #[serde(skip_serializing_if = "slice_is_empty")]
    tools: &'a [OpenAiTool<'a>],
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<OpenAiToolChoice<'a>>,
    #[serde(flatten)]
    parameters: Map<String, Value>,
}

fn slice_is_empty<T>(values: &[T]) -> bool {
    values.is_empty()
}

#[derive(Serialize)]
struct OpenAiStreamOptions {
    include_usage: bool,
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

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OpenAiTool<'a> {
    Function {
        function: OpenAiFunctionDefinition<'a>,
    },
}

#[derive(Serialize)]
struct OpenAiFunctionDefinition<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
    parameters: &'a Value,
}

impl<'a> From<&'a ChatToolDefinition> for OpenAiTool<'a> {
    fn from(tool: &'a ChatToolDefinition) -> Self {
        Self::Function {
            function: OpenAiFunctionDefinition {
                name: &tool.name,
                description: tool.description.as_deref(),
                parameters: &tool.input_schema,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum OpenAiToolChoice<'a> {
    Mode(&'static str),
    Named(OpenAiNamedToolChoice<'a>),
}

#[derive(Serialize)]
struct OpenAiNamedToolChoice<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    function: OpenAiNamedFunction<'a>,
}

#[derive(Serialize)]
struct OpenAiNamedFunction<'a> {
    name: &'a str,
}

fn openai_tool_choice<'a>(
    tools: &'a [ChatToolDefinition],
    choice: &'a ChatToolChoice,
) -> Option<OpenAiToolChoice<'a>> {
    if tools.is_empty() {
        return None;
    }
    Some(match choice {
        ChatToolChoice::Auto => OpenAiToolChoice::Mode("auto"),
        ChatToolChoice::Required => OpenAiToolChoice::Mode("required"),
        ChatToolChoice::Named(name) => OpenAiToolChoice::Named(OpenAiNamedToolChoice {
            kind: "function",
            function: OpenAiNamedFunction { name },
        }),
    })
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

#[derive(Serialize)]
#[serde(untagged)]
enum OpenAiMessage<'a> {
    Author(OpenAiAuthorMessage<'a>),
    Assistant(OpenAiAssistantMessage<'a>),
    Tool(OpenAiToolResultMessage<'a>),
}

#[derive(Serialize)]
struct OpenAiAuthorMessage<'a> {
    role: ChatRole,
    content: OpenAiContent<'a>,
}

#[derive(Serialize)]
struct OpenAiAssistantMessage<'a> {
    role: ChatRole,
    content: Option<OpenAiContent<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tool_calls: Vec<OpenAiAssistantToolCall<'a>>,
}

#[derive(Serialize)]
struct OpenAiAssistantToolCall<'a> {
    id: &'a str,
    #[serde(rename = "type")]
    kind: &'static str,
    function: OpenAiAssistantFunctionCall<'a>,
}

#[derive(Serialize)]
struct OpenAiAssistantFunctionCall<'a> {
    name: &'a str,
    arguments: &'a str,
}

#[derive(Serialize)]
struct OpenAiToolResultMessage<'a> {
    role: ChatRole,
    tool_call_id: &'a str,
    content: &'a str,
}

impl<'a> From<&'a ChatMessage> for OpenAiMessage<'a> {
    fn from(message: &'a ChatMessage) -> Self {
        match message {
            ChatMessage::System { content } => Self::Author(OpenAiAuthorMessage {
                role: ChatRole::System,
                content: openai_content(content),
            }),
            ChatMessage::User { content } => Self::Author(OpenAiAuthorMessage {
                role: ChatRole::User,
                content: openai_content(content),
            }),
            ChatMessage::Assistant {
                content,
                tool_calls,
            } => Self::Assistant(OpenAiAssistantMessage {
                role: ChatRole::Assistant,
                content: content.as_ref().map(openai_content),
                tool_calls: tool_calls
                    .iter()
                    .map(|call| OpenAiAssistantToolCall {
                        id: &call.id,
                        kind: "function",
                        function: OpenAiAssistantFunctionCall {
                            name: &call.name,
                            arguments: &call.arguments,
                        },
                    })
                    .collect(),
            }),
            ChatMessage::Tool {
                tool_call_id,
                content,
            } => Self::Tool(OpenAiToolResultMessage {
                role: ChatRole::Tool,
                tool_call_id,
                content,
            }),
        }
    }
}

fn openai_content(content: &ChatContent) -> OpenAiContent<'_> {
    match content {
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
    pending: VecDeque<ChatEvent>,
    upstream_bytes: usize,
    limits: OpenAiChatLimits,
    model: String,
    started: Instant,
    chunks_count: usize,
    usage_bytes: usize,
    clean_eof: bool,
}

impl StreamState {
    fn pop_pending_chunk(&mut self) -> Option<ChatEvent> {
        let event = self.pending.pop_front()?;
        self.chunks_count += 1;
        self.usage_bytes = self
            .usage_bytes
            .saturating_add(event.usage.as_ref().map_or(0, normalized_usage_size_bytes));
        Some(event)
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

    fn push(&mut self, bytes: &[u8]) -> Result<Vec<ChatEvent>, RunError> {
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

    fn finish(&mut self) -> Result<Vec<ChatEvent>, RunError> {
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

    fn drain_complete_lines(&mut self) -> Result<Vec<ChatEvent>, RunError> {
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

    fn parse_line(&mut self, line: &[u8]) -> Result<Vec<ChatEvent>, RunError> {
        match parse_sse_line(line, self.limits)? {
            ParsedSseLine::Events(events) => Ok(events),
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
    Events(Vec<ChatEvent>),
    Done,
}

fn parse_sse_line(line: &[u8], limits: OpenAiChatLimits) -> Result<ParsedSseLine, RunError> {
    if line.is_empty() {
        return Ok(ParsedSseLine::Events(Vec::new()));
    }
    let Some(payload) = line.strip_prefix(b"data:") else {
        return Ok(ParsedSseLine::Events(Vec::new()));
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
    Ok(ParsedSseLine::Events(openai_chunk_events(parsed, limits)?))
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
    #[serde(default, deserialize_with = "deserialize_null_default")]
    choices: Vec<OpenAiStreamChoice>,
    usage: Option<Value>,
}

#[derive(Deserialize)]
struct OpenAiStreamChoice {
    delta: OpenAiDelta,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiDelta {
    content: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    tool_calls: Vec<OpenAiToolCallDelta>,
}

#[derive(Deserialize)]
struct OpenAiToolCallDelta {
    index: u32,
    id: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    function: Option<OpenAiFunctionDelta>,
}

#[derive(Deserialize)]
struct OpenAiFunctionDelta {
    name: Option<String>,
    arguments: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiCompleteResponse {
    choices: Vec<OpenAiCompleteChoice>,
    usage: Option<Value>,
}

#[derive(Deserialize)]
struct OpenAiCompleteChoice {
    message: OpenAiCompleteMessage,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiCompleteMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    tool_calls: Vec<OpenAiCompleteToolCall>,
}

#[derive(Deserialize)]
struct OpenAiCompleteToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: OpenAiCompleteFunction,
}

#[derive(Deserialize)]
struct OpenAiCompleteFunction {
    name: String,
    arguments: String,
}

fn openai_chunk_events(
    parsed: OpenAiChunk,
    limits: OpenAiChatLimits,
) -> Result<Vec<ChatEvent>, RunError> {
    if parsed.choices.len() > 1 {
        return Err(invalid_stream_payload());
    }
    let usage = normalize_openai_usage(parsed.usage, limits, invalid_stream_payload)?;
    let Some(choice) = parsed.choices.into_iter().next() else {
        return Ok(usage
            .map(|usage| {
                vec![ChatEvent {
                    usage: Some(usage),
                    ..ChatEvent::default()
                }]
            })
            .unwrap_or_default());
    };
    let text_delta = choice.delta.content.unwrap_or_default();
    if text_delta.len() > limits.max_chunk_text_bytes {
        return Err(model_response_too_large());
    }
    let mut tool_call_deltas = Vec::with_capacity(choice.delta.tool_calls.len());
    for call in choice.delta.tool_calls {
        let kind = normalize_optional_stream_identity(call.kind);
        let id = normalize_optional_stream_identity(call.id);
        if kind.as_deref().is_some_and(|kind| kind != "function") {
            return Err(invalid_stream_payload());
        }
        let function = call.function.unwrap_or(OpenAiFunctionDelta {
            name: None,
            arguments: None,
        });
        let name = normalize_optional_stream_identity(function.name);
        let arguments_delta = function.arguments.unwrap_or_default();
        if arguments_delta.len() > limits.max_chunk_text_bytes {
            return Err(model_response_too_large());
        }
        tool_call_deltas.push(ChatToolCallDelta {
            index: call.index,
            id,
            name,
            arguments_delta,
        });
    }
    let event = ChatEvent {
        text_delta,
        tool_call_deltas,
        finish_reason: choice
            .finish_reason
            .as_deref()
            .map(|reason| ChatFinishReason::from_provider(Some(reason))),
        usage,
    };
    Ok((!event.is_empty()).then_some(event).into_iter().collect())
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Option::<T>::deserialize(deserializer).map(Option::unwrap_or_default)
}

fn normalize_optional_stream_identity(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

async fn read_bounded_complete_body(
    response: Response,
    max_bytes: usize,
) -> Result<Vec<u8>, RunError> {
    let mut body = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|error| {
            RunError::operation(
                "UPSTREAM_RESPONSE",
                format!(
                    "chat provider response failed ({})",
                    classify_request_error(&error)
                ),
            )
        })?;
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(model_response_too_large());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn parse_complete_response(
    bytes: &[u8],
    limits: OpenAiChatLimits,
) -> Result<ChatResponse, RunError> {
    let parsed: OpenAiCompleteResponse =
        serde_json::from_slice(bytes).map_err(|_| invalid_complete_response())?;
    if parsed.choices.len() != 1 {
        return Err(invalid_complete_response());
    }
    let choice = parsed
        .choices
        .into_iter()
        .next()
        .expect("the exact choice count was checked");
    let text = choice.message.content.unwrap_or_default();
    if text.len() > limits.max_accumulated_text_bytes {
        return Err(model_response_too_large());
    }
    let mut tool_calls = Vec::with_capacity(choice.message.tool_calls.len());
    for (index, call) in choice.message.tool_calls.into_iter().enumerate() {
        if call.kind != "function"
            || call.id.is_empty()
            || call.function.name.is_empty()
            || call.function.arguments.len() > limits.max_accumulated_text_bytes
        {
            return Err(invalid_complete_response());
        }
        tool_calls.push(ChatToolCall {
            index: u32::try_from(index).map_err(|_| invalid_complete_response())?,
            id: call.id,
            name: call.function.name,
            arguments: call.function.arguments,
        });
    }
    Ok(ChatResponse {
        text,
        tool_calls,
        finish_reason: ChatFinishReason::from_provider(choice.finish_reason.as_deref()),
        usage: normalize_openai_usage(parsed.usage, limits, invalid_complete_response)?,
    })
}

fn normalize_openai_usage(
    usage: Option<Value>,
    limits: OpenAiChatLimits,
    invalid: fn() -> RunError,
) -> Result<Option<ChatUsage>, RunError> {
    let Some(usage) = usage else {
        return Ok(None);
    };
    let bytes = serde_json::to_vec(&usage).map_err(|_| invalid())?;
    if bytes.len() > limits.max_usage_json_bytes {
        return Err(model_response_too_large());
    }
    let object = usage.as_object().ok_or_else(invalid)?;
    let optional_count = |name: &str| -> Result<Option<u64>, RunError> {
        object
            .get(name)
            .filter(|value| !value.is_null())
            .map(|value| value.as_u64().ok_or_else(invalid))
            .transpose()
    };
    let input_tokens_details =
        normalize_input_details(object.get("prompt_tokens_details"), invalid)?;
    let output_tokens_details =
        normalize_output_details(object.get("completion_tokens_details"), invalid)?;
    Ok(Some(ChatUsage {
        input_tokens: optional_count("prompt_tokens")?,
        input_tokens_details,
        output_tokens: optional_count("completion_tokens")?,
        output_tokens_details,
        total_tokens: optional_count("total_tokens")?,
    }))
}

fn normalize_input_details(
    details: Option<&Value>,
    invalid: fn() -> RunError,
) -> Result<Option<ChatInputTokensDetails>, RunError> {
    let Some(details) = details.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let object = details.as_object().ok_or_else(invalid)?;
    Ok(Some(ChatInputTokensDetails {
        cached_tokens: object
            .get("cached_tokens")
            .filter(|value| !value.is_null())
            .map(|value| value.as_u64().ok_or_else(invalid))
            .transpose()?,
    }))
}

fn normalize_output_details(
    details: Option<&Value>,
    invalid: fn() -> RunError,
) -> Result<Option<ChatOutputTokensDetails>, RunError> {
    let Some(details) = details.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let object = details.as_object().ok_or_else(invalid)?;
    Ok(Some(ChatOutputTokensDetails {
        reasoning_tokens: object
            .get("reasoning_tokens")
            .filter(|value| !value.is_null())
            .map(|value| value.as_u64().ok_or_else(invalid))
            .transpose()?,
    }))
}

fn normalized_usage_size_bytes(usage: &ChatUsage) -> usize {
    json_size_bytes(usage)
}

fn invalid_stream_payload() -> RunError {
    RunError::operation(
        "UPSTREAM_STREAM_INVALID",
        "invalid chat provider stream payload",
    )
}

fn invalid_complete_response() -> RunError {
    RunError::operation(
        "UPSTREAM_RESPONSE_INVALID",
        "invalid chat provider response payload",
    )
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
            "enable_thinking":{"type":"boolean"},
            "parallel_tool_calls":{"type":"boolean"},
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

fn classify_request_failure_code(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "UPSTREAM_TIMEOUT"
    } else if error.is_connect() {
        "UPSTREAM_CONNECTION"
    } else if error.is_request() {
        "UPSTREAM_REQUEST"
    } else {
        "UPSTREAM_TRANSPORT"
    }
}

fn classify_status_failure_code(status: StatusCode) -> &'static str {
    match status.as_u16() {
        401 => "UPSTREAM_AUTHENTICATION",
        403 => "UPSTREAM_PERMISSION",
        408 | 504 => "UPSTREAM_TIMEOUT",
        429 => "UPSTREAM_RATE_LIMIT",
        500..=599 => "UPSTREAM_UNAVAILABLE",
        300..=499 => "UPSTREAM_REQUEST_REJECTED",
        _ => "UPSTREAM_STATUS",
    }
}
