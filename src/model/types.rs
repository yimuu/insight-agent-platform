use std::pin::Pin;

use async_trait::async_trait;
use futures::{stream, Stream};
use serde::{Deserialize, Serialize};

use crate::error::AppError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
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
