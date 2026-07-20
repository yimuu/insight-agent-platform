use std::{
    collections::BTreeMap, collections::BTreeSet, fmt, fmt::Write as _, io::Write, pin::Pin,
    sync::Arc,
};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{dsl::CompileError, runtime::RunError};

use super::image::validate_image_url;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ModelCapability {
    JsonObjectOutput,
    JsonSchemaOutput,
    Vision,
}

impl ModelCapability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JsonObjectOutput => "json_object_output",
            Self::JsonSchemaOutput => "json_schema_output",
            Self::Vision => "vision",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ModelRequestCapability {
    Complete,
    Streaming,
}

impl ModelRequestCapability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete_request",
            Self::Streaming => "streaming_request",
        }
    }
}

pub(crate) fn select_structured_output_capability(
    capabilities: &BTreeSet<ModelCapability>,
    json_object_compatible: bool,
) -> Option<ModelCapability> {
    if capabilities.contains(&ModelCapability::JsonSchemaOutput) {
        Some(ModelCapability::JsonSchemaOutput)
    } else if json_object_compatible && capabilities.contains(&ModelCapability::JsonObjectOutput) {
        Some(ModelCapability::JsonObjectOutput)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChatRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ChatContent {
    Text(String),
    Parts(Vec<ChatContentPart>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatContentPart {
    Text { text: String },
    Image { image: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChatMessage {
    System {
        content: ChatContent,
    },
    User {
        content: ChatContent,
    },
    Assistant {
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<ChatContent>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<ChatToolCall>,
    },
    Tool {
        tool_call_id: String,
        content: String,
    },
}

impl ChatMessage {
    pub fn from_text(role: ChatRole, content: impl Into<String>) -> Self {
        Self::from_content(role, ChatContent::Text(content.into()))
    }

    pub fn from_content(role: ChatRole, content: ChatContent) -> Self {
        match role {
            ChatRole::System => Self::System { content },
            ChatRole::User => Self::User { content },
            ChatRole::Assistant => Self::Assistant {
                content: Some(content),
                tool_calls: Vec::new(),
            },
            ChatRole::Tool => {
                panic!("tool messages require an explicit tool_call_id")
            }
        }
    }

    pub fn assistant_tool_calls(
        content: Option<ChatContent>,
        tool_calls: Vec<ChatToolCall>,
    ) -> Self {
        Self::Assistant {
            content,
            tool_calls,
        }
    }

    pub fn tool_result(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self::Tool {
            tool_call_id: tool_call_id.into(),
            content: content.into(),
        }
    }

    pub const fn role(&self) -> ChatRole {
        match self {
            Self::System { .. } => ChatRole::System,
            Self::User { .. } => ChatRole::User,
            Self::Assistant { .. } => ChatRole::Assistant,
            Self::Tool { .. } => ChatRole::Tool,
        }
    }

    pub fn content(&self) -> Option<&ChatContent> {
        match self {
            Self::System { content } | Self::User { content } => Some(content),
            Self::Assistant { content, .. } => content.as_ref(),
            Self::Tool { .. } => None,
        }
    }

    pub fn tool_calls(&self) -> &[ChatToolCall] {
        match self {
            Self::Assistant { tool_calls, .. } => tool_calls,
            Self::System { .. } | Self::User { .. } | Self::Tool { .. } => &[],
        }
    }

    pub fn tool_call_id(&self) -> Option<&str> {
        match self {
            Self::Tool { tool_call_id, .. } => Some(tool_call_id),
            Self::System { .. } | Self::User { .. } | Self::Assistant { .. } => None,
        }
    }

    pub fn text_content(&self) -> Option<&str> {
        self.text()
    }

    pub fn text(&self) -> Option<&str> {
        match self {
            Self::Tool { content, .. } => Some(content),
            Self::System { content }
            | Self::User { content }
            | Self::Assistant {
                content: Some(content),
                ..
            } => match content {
                ChatContent::Text(text) => Some(text),
                ChatContent::Parts(parts) => parts.iter().find_map(|part| match part {
                    ChatContentPart::Text { text } => Some(text.as_str()),
                    ChatContentPart::Image { .. } => None,
                }),
            },
            Self::Assistant { content: None, .. } => None,
        }
    }

    pub fn image_urls(&self) -> Vec<&str> {
        let Some(content) = self.content() else {
            return Vec::new();
        };
        match content {
            ChatContent::Text(_) => Vec::new(),
            ChatContent::Parts(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    ChatContentPart::Text { .. } => None,
                    ChatContentPart::Image { image } => Some(image.as_str()),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatResponseFormat {
    JsonObject { name: String, schema: Value },
    JsonSchema { name: String, schema: Value },
}

impl ChatResponseFormat {
    pub fn required_capability(&self) -> ModelCapability {
        match self {
            Self::JsonObject { .. } => ModelCapability::JsonObjectOutput,
            Self::JsonSchema { .. } => ModelCapability::JsonSchemaOutput,
        }
    }

    fn contract(&self) -> (&str, &Value) {
        match self {
            Self::JsonObject { name, schema } | Self::JsonSchema { name, schema } => (name, schema),
        }
    }
}

/// One provider-neutral function tool made visible to a model call.
///
/// The schema is the frozen, model-visible input contract. Server-injected
/// fields must not be included here; they are added only after a returned call
/// has been validated by the durable execution layer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ChatToolDefinition {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: Value,
}

/// Closed provider-neutral tool-selection contract.
///
/// `Named` is validated against the request's tool whitelist before any
/// provider request is sent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ChatToolChoice {
    #[default]
    Auto,
    Required,
    Named(String),
}

#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub messages: Vec<ChatMessage>,
    pub parameters: Value,
    pub response_format: Option<ChatResponseFormat>,
    pub tools: Vec<ChatToolDefinition>,
    pub tool_choice: ChatToolChoice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatRequestMode {
    Complete,
    Streaming,
}

#[derive(Serialize)]
struct ProviderNeutralRequest<'a> {
    messages: &'a [ChatMessage],
    parameters: &'a Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<&'a ChatResponseFormat>,
    #[serde(skip_serializing_if = "slice_is_empty")]
    tools: &'a [ChatToolDefinition],
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<&'a ChatToolChoice>,
}

fn slice_is_empty<T>(values: &[T]) -> bool {
    values.is_empty()
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChatChunk {
    pub text: String,
    pub finish_reason: Option<String>,
    pub usage: Option<Value>,
}

pub type ChatStream = Pin<Box<dyn Stream<Item = Result<ChatChunk, RunError>> + Send>>;

/// Provider-independent completion reason for one model call.
///
/// `Invalid` deliberately covers both an absent provider reason and an
/// unrecognised value. Transport completion (for example SSE `[DONE]`) is a
/// separate concern and must never manufacture a successful `Stop` reason.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChatFinishReason {
    Stop,
    ToolCalls,
    Length,
    ContentFilter,
    Invalid,
}

impl ChatFinishReason {
    pub fn from_provider(value: Option<&str>) -> Self {
        match value {
            Some("stop") => Self::Stop,
            Some("tool_calls") => Self::ToolCalls,
            Some("length") => Self::Length,
            Some("content_filter") => Self::ContentFilter,
            Some(_) | None => Self::Invalid,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::ToolCalls => "tool_calls",
            Self::Length => "length",
            Self::ContentFilter => "content_filter",
            Self::Invalid => "invalid",
        }
    }
}

/// One function-call argument fragment emitted by a streaming provider.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChatToolCallDelta {
    pub index: u32,
    pub id: Option<String>,
    pub name: Option<String>,
    pub arguments_delta: String,
}

/// One complete function call emitted by a non-streaming provider response.
///
/// `arguments` remains the exact complete JSON string. Parsing and validating
/// it against the frozen tool schema belongs to the durable execution layer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChatToolCall {
    pub index: u32,
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChatInputTokensDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChatOutputTokensDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
}

/// OpenAI-Responses-compatible token telemetry.
///
/// Every field is optional on purpose. Provider omissions remain omissions;
/// the adapter never invents zero values or derives a missing total.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChatUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens_details: Option<ChatInputTokensDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens_details: Option<ChatOutputTokensDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
}

/// One typed provider streaming event. It mirrors one provider payload so a
/// finish reason and usage reported together remain atomically observable.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChatEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text_delta: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_call_deltas: Vec<ChatToolCallDelta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<ChatFinishReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<ChatUsage>,
}

impl ChatEvent {
    pub fn is_empty(&self) -> bool {
        self.text_delta.is_empty()
            && self.tool_call_deltas.is_empty()
            && self.finish_reason.is_none()
            && self.usage.is_none()
    }
}

pub type ChatEventStream = Pin<Box<dyn Stream<Item = Result<ChatEvent, RunError>> + Send>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChatResponse {
    pub text: String,
    pub tool_calls: Vec<ChatToolCall>,
    pub finish_reason: ChatFinishReason,
    pub usage: Option<ChatUsage>,
}

pub const DEFAULT_MAX_ACCUMULATED_TEXT_BYTES: usize = 1024 * 1024;
pub const MODEL_RESPONSE_TOO_LARGE_CODE: &str = "MODEL_RESPONSE_TOO_LARGE";
pub const MODEL_RESPONSE_TOO_LARGE_MESSAGE: &str =
    "chat provider response exceeded the configured size limit";
const LLM_CONTENT_INVALID: &str = "VNEXT_LLM_CONTENT_INVALID";
const LLM_MESSAGE_ORDER_INVALID: &str = "VNEXT_LLM_MESSAGE_ORDER_INVALID";
const LLM_RESPONSE_FORMAT_INVALID: &str = "VNEXT_LLM_RESPONSE_CONFIG_INVALID";
const LLM_TOOL_CONFIG_INVALID: &str = "VNEXT_LLM_TOOL_CONFIG_INVALID";

pub fn model_response_too_large() -> RunError {
    RunError::operation(
        MODEL_RESPONSE_TOO_LARGE_CODE,
        MODEL_RESPONSE_TOO_LARGE_MESSAGE,
    )
}

pub(crate) fn validate_chat_request(request: &ChatRequest) -> Result<(), RunError> {
    if request.messages.is_empty() {
        return Err(invalid_chat_request(
            LLM_CONTENT_INVALID,
            "chat provider request has no messages",
        ));
    }
    validate_chat_tools(request)?;
    validate_chat_messages(request)?;

    if let Some(response_format) = &request.response_format {
        let (name, schema) = response_format.contract();
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            || !schema.is_object()
            || crate::schema::compile_schema_2020(schema).is_err()
        {
            return Err(invalid_chat_request(
                LLM_RESPONSE_FORMAT_INVALID,
                "chat provider structured response format is invalid",
            ));
        }
        if matches!(response_format, ChatResponseFormat::JsonObject { .. })
            && !schema_requires_object_root(schema)
        {
            return Err(invalid_chat_request(
                LLM_RESPONSE_FORMAT_INVALID,
                "chat provider json_object response requires an object-root schema",
            ));
        }
    }
    Ok(())
}

enum ChatMessagePosition {
    SystemPrefix,
    AfterUser,
    AwaitingToolResults(BTreeSet<String>),
    AfterToolResults,
    AfterAssistant,
    Invalid,
}

fn validate_chat_messages(request: &ChatRequest) -> Result<(), RunError> {
    if !matches!(
        request
            .messages
            .iter()
            .find(|message| !matches!(message, ChatMessage::System { .. })),
        Some(ChatMessage::User { .. })
    ) {
        return Err(invalid_chat_request(
            LLM_MESSAGE_ORDER_INVALID,
            "chat provider request must end with a user message",
        ));
    }
    let mut position = ChatMessagePosition::SystemPrefix;
    for message in &request.messages {
        validate_chat_message_content(message, request)?;
        position = match (
            std::mem::replace(&mut position, ChatMessagePosition::Invalid),
            message,
        ) {
            (ChatMessagePosition::SystemPrefix, ChatMessage::System { .. }) => {
                ChatMessagePosition::SystemPrefix
            }
            (
                ChatMessagePosition::SystemPrefix
                | ChatMessagePosition::AfterUser
                | ChatMessagePosition::AfterToolResults
                | ChatMessagePosition::AfterAssistant,
                ChatMessage::User { .. },
            ) => ChatMessagePosition::AfterUser,
            (
                ChatMessagePosition::AfterUser
                | ChatMessagePosition::AfterToolResults
                | ChatMessagePosition::AfterAssistant,
                ChatMessage::Assistant { tool_calls, .. },
            ) if tool_calls.is_empty() => ChatMessagePosition::AfterAssistant,
            (
                ChatMessagePosition::AfterUser | ChatMessagePosition::AfterToolResults,
                ChatMessage::Assistant { tool_calls, .. },
            ) => ChatMessagePosition::AwaitingToolResults(
                tool_calls.iter().map(|call| call.id.clone()).collect(),
            ),
            (
                ChatMessagePosition::AwaitingToolResults(mut remaining),
                ChatMessage::Tool { tool_call_id, .. },
            ) => {
                if !remaining.remove(tool_call_id) {
                    return Err(invalid_chat_request(
                        LLM_MESSAGE_ORDER_INVALID,
                        "chat provider tool result does not match the pending tool-call batch",
                    ));
                }
                if remaining.is_empty() {
                    ChatMessagePosition::AfterToolResults
                } else {
                    ChatMessagePosition::AwaitingToolResults(remaining)
                }
            }
            _ => {
                return Err(invalid_chat_request(
                    LLM_MESSAGE_ORDER_INVALID,
                    "chat provider messages do not form a complete author/tool continuation",
                ))
            }
        };
    }

    match position {
        ChatMessagePosition::AfterUser | ChatMessagePosition::AfterToolResults => Ok(()),
        ChatMessagePosition::AwaitingToolResults(_) => Err(invalid_chat_request(
            LLM_MESSAGE_ORDER_INVALID,
            "chat provider request must end with a user message or a complete tool-result batch",
        )),
        ChatMessagePosition::SystemPrefix
        | ChatMessagePosition::AfterAssistant
        | ChatMessagePosition::Invalid => Err(invalid_chat_request(
            LLM_MESSAGE_ORDER_INVALID,
            "chat provider request must end with a user message",
        )),
    }
}

fn validate_chat_message_content(
    message: &ChatMessage,
    request: &ChatRequest,
) -> Result<(), RunError> {
    match message {
        ChatMessage::System { content } => validate_chat_content(content, false),
        ChatMessage::User { content } => validate_chat_content(content, true),
        ChatMessage::Assistant {
            content,
            tool_calls,
        } => {
            if content.is_none() && tool_calls.is_empty() {
                return Err(invalid_chat_request(
                    LLM_CONTENT_INVALID,
                    "chat provider assistant message has neither content nor tool calls",
                ));
            }
            if let Some(content) = content {
                validate_chat_content(content, false)?;
            }
            validate_assistant_tool_calls(tool_calls, request)
        }
        ChatMessage::Tool {
            tool_call_id,
            content,
        } => {
            if !valid_call_id(tool_call_id) || content.trim().is_empty() {
                return Err(invalid_chat_request(
                    LLM_CONTENT_INVALID,
                    "chat provider tool result is invalid",
                ));
            }
            Ok(())
        }
    }
}

fn validate_chat_content(content: &ChatContent, allow_images: bool) -> Result<(), RunError> {
    match content {
        ChatContent::Text(text) if text.trim().is_empty() => Err(invalid_chat_request(
            LLM_CONTENT_INVALID,
            "chat provider request contains empty text",
        )),
        ChatContent::Text(_) => Ok(()),
        ChatContent::Parts(parts) if parts.is_empty() => Err(invalid_chat_request(
            LLM_CONTENT_INVALID,
            "chat provider request contains an empty content list",
        )),
        ChatContent::Parts(parts) => {
            for part in parts {
                match part {
                    ChatContentPart::Text { text } if text.trim().is_empty() => {
                        return Err(invalid_chat_request(
                            LLM_CONTENT_INVALID,
                            "chat provider request contains an empty text part",
                        ));
                    }
                    ChatContentPart::Text { .. } => {}
                    ChatContentPart::Image { image }
                        if !allow_images || validate_image_url(image).is_err() =>
                    {
                        return Err(invalid_chat_request(
                            LLM_CONTENT_INVALID,
                            "chat provider request contains an invalid image part",
                        ));
                    }
                    ChatContentPart::Image { .. } => {}
                }
            }
            Ok(())
        }
    }
}

fn validate_assistant_tool_calls(
    tool_calls: &[ChatToolCall],
    request: &ChatRequest,
) -> Result<(), RunError> {
    let mut ids = BTreeSet::new();
    for (expected_index, call) in tool_calls.iter().enumerate() {
        let arguments = serde_json::from_str::<Value>(&call.arguments).map_err(|_| {
            invalid_chat_request(
                LLM_TOOL_CONFIG_INVALID,
                "chat provider assistant tool call is invalid",
            )
        })?;
        let tool = request
            .tools
            .iter()
            .find(|tool| tool.name == call.name)
            .ok_or_else(|| {
                invalid_chat_request(
                    LLM_TOOL_CONFIG_INVALID,
                    "chat provider assistant tool call is outside the declared whitelist",
                )
            })?;
        let validator = crate::schema::compile_schema_2020(&tool.input_schema).map_err(|_| {
            invalid_chat_request(
                LLM_TOOL_CONFIG_INVALID,
                "chat provider tool definition is invalid",
            )
        })?;
        if call.index != u32::try_from(expected_index).unwrap_or(u32::MAX)
            || !valid_call_id(&call.id)
            || !ids.insert(call.id.as_str())
            || !valid_provider_name(&call.name)
            || !arguments.is_object()
            || !validator.is_valid(&arguments)
        {
            return Err(invalid_chat_request(
                LLM_TOOL_CONFIG_INVALID,
                "chat provider assistant tool call is invalid",
            ));
        }
    }
    Ok(())
}

fn valid_call_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.chars().any(|character| character.is_control())
}

fn validate_chat_tools(request: &ChatRequest) -> Result<(), RunError> {
    if request.parameters.as_object().is_some_and(|parameters| {
        parameters.contains_key("tools") || parameters.contains_key("tool_choice")
    }) {
        return Err(invalid_chat_request(
            LLM_TOOL_CONFIG_INVALID,
            "chat tools and tool_choice must not be supplied through model parameters",
        ));
    }

    let mut names = BTreeSet::new();
    for tool in &request.tools {
        if !valid_provider_name(&tool.name)
            || !names.insert(tool.name.as_str())
            || tool
                .description
                .as_deref()
                .is_some_and(|description| description.trim().is_empty())
            || !tool.input_schema.is_object()
            || crate::schema::compile_schema_2020(&tool.input_schema).is_err()
            || !schema_requires_object_root(&tool.input_schema)
        {
            return Err(invalid_chat_request(
                LLM_TOOL_CONFIG_INVALID,
                "chat provider tool definition is invalid",
            ));
        }
    }

    match &request.tool_choice {
        ChatToolChoice::Auto => Ok(()),
        ChatToolChoice::Required if !request.tools.is_empty() => Ok(()),
        ChatToolChoice::Named(name)
            if valid_provider_name(name) && names.contains(name.as_str()) =>
        {
            Ok(())
        }
        ChatToolChoice::Required | ChatToolChoice::Named(_) => Err(invalid_chat_request(
            LLM_TOOL_CONFIG_INVALID,
            "chat provider tool_choice is outside the declared tool whitelist",
        )),
    }
}

fn valid_provider_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn schema_requires_object_root(schema: &Value) -> bool {
    schema_node_requires_object(schema, schema, &mut BTreeSet::new())
}

fn schema_node_requires_object(
    document: &Value,
    schema: &Value,
    visited_refs: &mut BTreeSet<String>,
) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    if let Some(schema_type) = object.get("type") {
        return match schema_type {
            Value::String(schema_type) => schema_type == "object",
            Value::Array(types) => {
                types.len() == 1 && types.first().and_then(Value::as_str) == Some("object")
            }
            _ => false,
        };
    }
    if object.get("const").is_some_and(Value::is_object) {
        return true;
    }
    if let Some(values) = object.get("enum").and_then(Value::as_array) {
        return !values.is_empty() && values.iter().all(Value::is_object);
    }
    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
        let Some(pointer) = reference.strip_prefix('#') else {
            return false;
        };
        if !visited_refs.insert(reference.to_string()) {
            return false;
        }
        let result = document
            .pointer(pointer)
            .is_some_and(|target| schema_node_requires_object(document, target, visited_refs));
        visited_refs.remove(reference);
        return result;
    }
    if let Some(all_of) = object.get("allOf").and_then(Value::as_array) {
        return all_of
            .iter()
            .any(|branch| schema_node_requires_object(document, branch, visited_refs));
    }
    for keyword in ["anyOf", "oneOf"] {
        if let Some(branches) = object.get(keyword).and_then(Value::as_array) {
            return !branches.is_empty()
                && branches
                    .iter()
                    .all(|branch| schema_node_requires_object(document, branch, visited_refs));
        }
    }
    false
}

fn invalid_chat_request(code: &'static str, message: &'static str) -> RunError {
    RunError::operation(code, message)
}

#[async_trait]
pub trait ChatModel: Send + Sync + fmt::Debug {
    fn capabilities(&self) -> BTreeSet<ModelCapability>;
    fn request_capabilities(&self) -> BTreeSet<ModelRequestCapability> {
        BTreeSet::from([ModelRequestCapability::Streaming])
    }
    fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError>;
    fn max_accumulated_text_bytes(&self) -> usize {
        DEFAULT_MAX_ACCUMULATED_TEXT_BYTES
    }
    /// Verifies the exact request representation emitted by this adapter.
    ///
    /// Adapters with a provider-specific envelope must override this method;
    /// the default is suitable only for models whose wire body is the
    /// provider-neutral messages/parameters pair.
    fn request_body_within_limit(&self, request: &ChatRequest, max_bytes: usize) -> bool {
        serialized_json_within_limit(
            &ProviderNeutralRequest {
                messages: &request.messages,
                parameters: &request.parameters,
                response_format: request.response_format.as_ref(),
                tools: &request.tools,
                tool_choice: (!request.tools.is_empty()).then_some(&request.tool_choice),
            },
            max_bytes,
        )
    }

    /// Verifies the exact request representation for one provider request
    /// mode. The legacy method above remains the streaming compatibility
    /// surface until all callers carry the mode explicitly.
    fn request_body_within_limit_for_mode(
        &self,
        request: &ChatRequest,
        _mode: ChatRequestMode,
        max_bytes: usize,
    ) -> bool {
        self.request_body_within_limit(request, max_bytes)
    }

    /// Executes a genuine provider complete-response request when overridden
    /// by an adapter. The default exists only as a compatibility bridge for
    /// test and third-party models that currently implement `stream_chat`.
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, RunError> {
        let mut stream = self.stream_chat(request).await?;
        let mut text = String::new();
        let mut finish_reason = ChatFinishReason::Invalid;
        let mut usage = None;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            text.push_str(&chunk.text);
            if let Some(reason) = chunk.finish_reason.as_deref() {
                finish_reason = ChatFinishReason::from_provider(Some(reason));
            }
            if let Some(value) = chunk.usage {
                usage = Some(normalize_legacy_usage(value)?);
            }
        }
        Ok(ChatResponse {
            text,
            tool_calls: Vec::new(),
            finish_reason,
            usage,
        })
    }

    /// Typed streaming surface. Existing model implementations inherit a
    /// lossless text/finish/usage projection; adapters with function calling
    /// override it to expose typed tool-call deltas as well.
    async fn stream_chat_events(&self, request: ChatRequest) -> Result<ChatEventStream, RunError> {
        let stream = self.stream_chat(request).await?;
        Ok(Box::pin(stream.map(|chunk| {
            let chunk = chunk?;
            Ok(ChatEvent {
                text_delta: chunk.text,
                tool_call_deltas: Vec::new(),
                finish_reason: chunk
                    .finish_reason
                    .as_deref()
                    .map(|value| ChatFinishReason::from_provider(Some(value))),
                usage: chunk.usage.map(normalize_legacy_usage).transpose()?,
            })
        })))
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError>;
}

fn normalize_legacy_usage(value: Value) -> Result<ChatUsage, RunError> {
    let object = value.as_object().ok_or_else(invalid_usage)?;
    let token = |responses_name: &str, chat_name: &str| -> Result<Option<u64>, RunError> {
        object
            .get(responses_name)
            .or_else(|| object.get(chat_name))
            .map(|value| value.as_u64().ok_or_else(invalid_usage))
            .transpose()
    };
    let input_details = object
        .get("input_tokens_details")
        .or_else(|| object.get("prompt_tokens_details"))
        .map(|value| {
            let value = value.as_object().ok_or_else(invalid_usage)?;
            Ok(ChatInputTokensDetails {
                cached_tokens: value
                    .get("cached_tokens")
                    .map(|value| value.as_u64().ok_or_else(invalid_usage))
                    .transpose()?,
            })
        })
        .transpose()?;
    let output_details = object
        .get("output_tokens_details")
        .or_else(|| object.get("completion_tokens_details"))
        .map(|value| {
            let value = value.as_object().ok_or_else(invalid_usage)?;
            Ok(ChatOutputTokensDetails {
                reasoning_tokens: value
                    .get("reasoning_tokens")
                    .map(|value| value.as_u64().ok_or_else(invalid_usage))
                    .transpose()?,
            })
        })
        .transpose()?;
    Ok(ChatUsage {
        input_tokens: token("input_tokens", "prompt_tokens")?,
        input_tokens_details: input_details,
        output_tokens: token("output_tokens", "completion_tokens")?,
        output_tokens_details: output_details,
        total_tokens: token("total_tokens", "total_tokens")?,
    })
}

fn invalid_usage() -> RunError {
    RunError::operation(
        "UPSTREAM_USAGE_INVALID",
        "chat provider returned invalid token usage",
    )
}

pub(crate) fn serialized_json_within_limit(value: &impl Serialize, max_bytes: usize) -> bool {
    serde_json::to_writer(LimitWriter::new(max_bytes), value).is_ok()
}

struct LimitWriter {
    written: usize,
    max_bytes: usize,
}

impl LimitWriter {
    fn new(max_bytes: usize) -> Self {
        Self {
            written: 0,
            max_bytes,
        }
    }
}

impl Write for LimitWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() > self.max_bytes.saturating_sub(self.written) {
            return Err(std::io::Error::other(
                "provider request exceeds configured limit",
            ));
        }
        self.written += buffer.len();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Non-secret immutable identity for one model alias binding. Secrets are
/// deliberately absent; rotating a secret value does not rewrite a published
/// Deployment Revision, while changing provider/model/adapter policy does.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelDeploymentIdentity {
    worker_version: String,
    binding_hash: String,
    evidence: Value,
}

impl ModelDeploymentIdentity {
    pub fn new(worker_version: impl Into<String>, evidence: Value) -> Result<Self, CompileError> {
        let worker_version = worker_version.into();
        if worker_version.is_empty()
            || worker_version.len() > 256
            || worker_version
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
            || !evidence.is_object()
        {
            return Err(CompileError::new(
                "MODEL_DEPLOYMENT_IDENTITY_INVALID",
                "model deployment identity must contain a bounded worker version and object evidence",
            ));
        }
        let canonical = serde_jcs::to_vec(&evidence).map_err(|_| {
            CompileError::new(
                "MODEL_DEPLOYMENT_IDENTITY_INVALID",
                "model deployment evidence cannot be canonicalized",
            )
        })?;
        let digest = Sha256::digest(canonical);
        let mut binding_hash = String::with_capacity("sha256:".len() + digest.len() * 2);
        binding_hash.push_str("sha256:");
        for byte in digest {
            write!(&mut binding_hash, "{byte:02x}")
                .expect("writing a digest into String cannot fail");
        }
        Ok(Self {
            worker_version,
            binding_hash,
            evidence,
        })
    }

    pub fn worker_version(&self) -> &str {
        &self.worker_version
    }

    pub fn binding_hash(&self) -> &str {
        &self.binding_hash
    }

    pub fn evidence(&self) -> &Value {
        &self.evidence
    }
}

#[derive(Clone)]
struct RegisteredModel {
    model: Arc<dyn ChatModel>,
    deployment: ModelDeploymentIdentity,
}

#[derive(Clone, Default)]
pub struct ModelRegistry {
    models: BTreeMap<String, RegisteredModel>,
}

impl ModelRegistry {
    pub fn register<M>(&mut self, alias: impl Into<String>, model: M) -> Result<(), CompileError>
    where
        M: ChatModel + 'static,
    {
        let alias = alias.into();
        let capabilities = model
            .capabilities()
            .into_iter()
            .map(ModelCapability::as_str)
            .collect::<Vec<_>>();
        let request_capabilities = model
            .request_capabilities()
            .into_iter()
            .map(ModelRequestCapability::as_str)
            .collect::<Vec<_>>();
        let deployment = ModelDeploymentIdentity::new(
            "legacy-model-registration-v1",
            serde_json::json!({
                "adapter_type": std::any::type_name::<M>(),
                "alias": alias,
                "capabilities": capabilities,
                "request_capabilities": request_capabilities,
                "registration": "legacy_explicit_test_surface",
            }),
        )?;
        self.register_versioned(alias, deployment, model)
    }

    pub fn register_versioned<M>(
        &mut self,
        alias: impl Into<String>,
        deployment: ModelDeploymentIdentity,
        model: M,
    ) -> Result<(), CompileError>
    where
        M: ChatModel + 'static,
    {
        let alias = alias.into();
        if alias.trim().is_empty() {
            return Err(CompileError::new(
                "MODEL_ALIAS_INVALID",
                "model alias must not be empty",
            ));
        }
        if self.models.contains_key(&alias) {
            return Err(CompileError::new(
                "DUPLICATE_MODEL",
                format!("model alias '{alias}' is already registered"),
            ));
        }
        self.models.insert(
            alias,
            RegisteredModel {
                model: Arc::new(model),
                deployment,
            },
        );
        Ok(())
    }

    pub fn resolve(&self, alias: &str) -> Result<Arc<dyn ChatModel>, CompileError> {
        self.models
            .get(alias)
            .map(|entry| entry.model.clone())
            .ok_or_else(|| {
                CompileError::new(
                    "MODEL_NOT_FOUND",
                    format!("model alias '{alias}' is not registered"),
                )
            })
    }

    pub fn deployment_identity(
        &self,
        alias: &str,
    ) -> Result<&ModelDeploymentIdentity, CompileError> {
        self.models
            .get(alias)
            .map(|entry| &entry.deployment)
            .ok_or_else(|| {
                CompileError::new(
                    "MODEL_NOT_FOUND",
                    format!("model alias '{alias}' is not registered"),
                )
            })
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.models.keys().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(messages: Vec<ChatMessage>) -> ChatRequest {
        ChatRequest {
            messages,
            parameters: serde_json::json!({}),
            response_format: None,
            tools: Vec::new(),
            tool_choice: ChatToolChoice::Auto,
        }
    }

    #[test]
    fn chat_validation_accepts_spliced_history_before_the_current_user_turn() {
        for history in [
            vec![ChatMessage::from_text(ChatRole::User, "prior user context")],
            vec![
                ChatMessage::from_text(ChatRole::User, "prior user context"),
                ChatMessage::from_text(ChatRole::Assistant, "prior assistant context"),
            ],
            vec![
                ChatMessage::from_text(ChatRole::User, "prior user context"),
                ChatMessage::from_text(ChatRole::Assistant, "first assistant context"),
                ChatMessage::from_text(ChatRole::Assistant, "second assistant context"),
            ],
        ] {
            let mut messages = vec![ChatMessage::from_text(ChatRole::System, "system policy")];
            messages.extend(history);
            messages.push(ChatMessage::from_text(ChatRole::User, "current question"));
            validate_chat_request(&request(messages)).unwrap();
        }
    }

    #[test]
    fn chat_validation_rejects_assistant_first_even_when_a_user_message_follows() {
        let error = validate_chat_request(&request(vec![
            ChatMessage::from_text(ChatRole::System, "system policy"),
            ChatMessage::from_text(ChatRole::Assistant, "assistant greeting"),
            ChatMessage::from_text(ChatRole::User, "current question"),
        ]))
        .unwrap_err();
        assert_eq!(error.code(), LLM_MESSAGE_ORDER_INVALID);
    }
}
