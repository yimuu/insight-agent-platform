use std::{collections::BTreeMap, collections::BTreeSet, fmt, pin::Pin, sync::Arc};

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
    ImageUrl { image_url: ImageUrl },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ImageUrl {
    pub url: String,
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
                ChatContentPart::ImageUrl { .. } => None,
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
                    ChatContentPart::ImageUrl { image_url } => Some(image_url.url.as_str()),
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

pub fn model_response_too_large() -> RunError {
    RunError::new(
        MODEL_RESPONSE_TOO_LARGE_CODE,
        MODEL_RESPONSE_TOO_LARGE_MESSAGE,
    )
}

#[async_trait]
pub trait ChatModel: Send + Sync + fmt::Debug {
    fn capabilities(&self) -> BTreeSet<ModelCapability>;
    fn validate_parameters(&self, parameters: &Value) -> Result<(), CompileError>;
    fn max_accumulated_text_bytes(&self) -> usize {
        DEFAULT_MAX_ACCUMULATED_TEXT_BYTES
    }
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError>;
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
