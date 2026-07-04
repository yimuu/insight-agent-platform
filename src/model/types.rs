use std::pin::Pin;

use async_trait::async_trait;
use futures::{stream, Stream};
use serde::{Deserialize, Serialize};

use crate::{error::AppError, model::providers::ModelType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: ChatContent,
}

impl ChatMessage {
    pub fn text(role: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: ChatContent::Text(content.into()),
        }
    }

    pub fn multimodal(role: impl Into<String>, parts: Vec<ChatContentPart>) -> Self {
        Self {
            role: role.into(),
            content: ChatContent::Parts(parts),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChatContent {
    Text(String),
    Parts(Vec<ChatContentPart>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatContentPart {
    Text { text: String },
    ImageUrl { image_url: ImageUrl },
}

impl ChatContentPart {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn image_url(url: impl Into<String>) -> Self {
        Self::ImageUrl {
            image_url: ImageUrl { url: url.into() },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageUrl {
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub provider: String,
    pub model_type: ModelType,
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
}

pub type ChatStream = Pin<Box<dyn Stream<Item = Result<String, AppError>> + Send>>;

#[async_trait]
pub trait ModelClient: Clone + Send + Sync + 'static {
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, AppError>;
}

#[derive(Debug, Clone)]
pub struct FakeModelClient {
    chunks: Vec<String>,
}

impl FakeModelClient {
    pub fn new(chunks: Vec<&str>) -> Self {
        Self {
            chunks: chunks.into_iter().map(str::to_string).collect(),
        }
    }
}

#[async_trait]
impl ModelClient for FakeModelClient {
    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, AppError> {
        let chunks = self.chunks.clone();
        Ok(Box::pin(stream::iter(chunks.into_iter().map(Ok))))
    }
}
