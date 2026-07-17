use std::{collections::BTreeMap, collections::BTreeSet, fmt, io::Write, pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{dsl::CompileError, runtime::RunError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ModelCapability {
    Vision,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChatRole {
    System,
    User,
    Assistant,
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
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: ChatContent,
}

impl ChatMessage {
    pub fn from_text(role: ChatRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: ChatContent::Text(content.into()),
        }
    }

    pub fn text_content(&self) -> Option<&str> {
        self.text()
    }

    pub fn text(&self) -> Option<&str> {
        match &self.content {
            ChatContent::Text(text) => Some(text),
            ChatContent::Parts(parts) => parts.iter().find_map(|part| match part {
                ChatContentPart::Text { text } => Some(text.as_str()),
                ChatContentPart::Image { .. } => None,
            }),
        }
    }

    pub fn image_urls(&self) -> Vec<&str> {
        match &self.content {
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

#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub messages: Vec<ChatMessage>,
    pub parameters: Value,
}

#[derive(Serialize)]
struct ProviderNeutralRequest<'a> {
    messages: &'a [ChatMessage],
    parameters: &'a Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChatChunk {
    pub text: String,
    pub finish_reason: Option<String>,
    pub usage: Option<Value>,
}

pub type ChatStream = Pin<Box<dyn Stream<Item = Result<ChatChunk, RunError>> + Send>>;

pub const DEFAULT_MAX_ACCUMULATED_TEXT_BYTES: usize = 1024 * 1024;
pub const MODEL_RESPONSE_TOO_LARGE_CODE: &str = "MODEL_RESPONSE_TOO_LARGE";
pub const MODEL_RESPONSE_TOO_LARGE_MESSAGE: &str =
    "chat provider response exceeded the configured size limit";
const LLM_CONTENT_INVALID: &str = "VNEXT_LLM_CONTENT_INVALID";
const LLM_MESSAGE_ORDER_INVALID: &str = "VNEXT_LLM_MESSAGE_ORDER_INVALID";

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

    let mut system_prefix = true;
    for message in &request.messages {
        if message.role == ChatRole::System {
            if !system_prefix {
                return Err(invalid_chat_request(
                    LLM_MESSAGE_ORDER_INVALID,
                    "chat provider request has a system message outside its prefix",
                ));
            }
        } else {
            system_prefix = false;
        }

        match &message.content {
            ChatContent::Text(text) if text.trim().is_empty() => {
                return Err(invalid_chat_request(
                    LLM_CONTENT_INVALID,
                    "chat provider request contains empty text",
                ));
            }
            ChatContent::Text(_) => {}
            ChatContent::Parts(parts) if parts.is_empty() => {
                return Err(invalid_chat_request(
                    LLM_CONTENT_INVALID,
                    "chat provider request contains an empty content list",
                ));
            }
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
                            if message.role != ChatRole::User || image.trim().is_empty() =>
                        {
                            return Err(invalid_chat_request(
                                LLM_CONTENT_INVALID,
                                "chat provider request contains an invalid image part",
                            ));
                        }
                        ChatContentPart::Image { .. } => {}
                    }
                }
            }
        }
    }

    if request.messages.last().map(|message| message.role) != Some(ChatRole::User) {
        return Err(invalid_chat_request(
            LLM_MESSAGE_ORDER_INVALID,
            "chat provider request must end with a user message",
        ));
    }
    Ok(())
}

fn invalid_chat_request(code: &'static str, message: &'static str) -> RunError {
    RunError::operation(code, message)
}

#[async_trait]
pub trait ChatModel: Send + Sync + fmt::Debug {
    fn capabilities(&self) -> BTreeSet<ModelCapability>;
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
            },
            max_bytes,
        )
    }
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError>;
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

#[derive(Clone, Default)]
pub struct ModelRegistry {
    models: BTreeMap<String, Arc<dyn ChatModel>>,
}

impl ModelRegistry {
    pub fn register<M>(&mut self, alias: impl Into<String>, model: M) -> Result<(), CompileError>
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
        self.models.insert(alias, Arc::new(model));
        Ok(())
    }

    pub fn resolve(&self, alias: &str) -> Result<Arc<dyn ChatModel>, CompileError> {
        self.models.get(alias).cloned().ok_or_else(|| {
            CompileError::new(
                "MODEL_NOT_FOUND",
                format!("model alias '{alias}' is not registered"),
            )
        })
    }
}
