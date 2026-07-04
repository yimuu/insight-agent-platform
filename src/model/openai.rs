use std::collections::VecDeque;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{
    error::AppError,
    model::types::{ChatMessage, ChatRequest, ChatStream, ModelClient},
};

#[derive(Debug, Clone)]
pub struct OpenAiModelClient {
    client: Client,
    api_key: String,
    base_url: String,
    default_model: String,
}

impl OpenAiModelClient {
    pub fn new(api_key: String, base_url: String, default_model: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            default_model,
        }
    }
}

#[async_trait]
impl ModelClient for OpenAiModelClient {
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, AppError> {
        let body = OpenAiRequest {
            model: if request.model.is_empty() {
                self.default_model.clone()
            } else {
                request.model
            },
            messages: request.messages,
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            stream: true,
        };

        let response = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|err| AppError::Upstream(format!("model request failed: {err}")))?;

        let status = response.status();
        if !status.is_success() {
            return Err(AppError::Upstream(format!(
                "model returned status {status}"
            )));
        }

        let stream = stream::try_unfold(
            StreamState {
                bytes: Box::pin(response.bytes_stream()),
                buffer: String::new(),
                pending: VecDeque::new(),
            },
            |mut state| async move {
                loop {
                    if let Some(delta) = state.pending.pop_front() {
                        return Ok(Some((delta, state)));
                    }

                    match state.bytes.next().await {
                        Some(Ok(bytes)) => {
                            state.buffer.push_str(&String::from_utf8_lossy(&bytes));
                            drain_sse_buffer(&mut state.buffer, &mut state.pending);
                        }
                        Some(Err(err)) => {
                            return Err(AppError::Upstream(format!(
                                "model stream failed: {err}"
                            )));
                        }
                        None => {
                            drain_sse_tail(&mut state.buffer, &mut state.pending);
                            if let Some(delta) = state.pending.pop_front() {
                                return Ok(Some((delta, state)));
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

struct StreamState {
    bytes: ChatByteStream,
    buffer: String,
    pending: VecDeque<String>,
}

type ChatByteStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

#[derive(Debug, Serialize)]
struct OpenAiRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: Option<f32>,
    max_tokens: Option<u32>,
    stream: bool,
}

#[derive(Debug, Deserialize)]
struct OpenAiChunk {
    choices: Vec<OpenAiChoice>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChoice {
    delta: OpenAiDelta,
}

#[derive(Debug, Deserialize)]
struct OpenAiDelta {
    content: Option<String>,
}

fn drain_sse_buffer(buffer: &mut String, pending: &mut VecDeque<String>) {
    while let Some(newline_index) = buffer.find('\n') {
        let mut line: String = buffer.drain(..=newline_index).collect();
        if line.ends_with('\n') {
            line.pop();
        }
        if line.ends_with('\r') {
            line.pop();
        }
        parse_sse_line(&line, pending);
    }
}

fn drain_sse_tail(buffer: &mut String, pending: &mut VecDeque<String>) {
    if buffer.is_empty() {
        return;
    }

    let line = std::mem::take(buffer);
    parse_sse_line(line.trim_end_matches('\r'), pending);
}

fn parse_sse_line(line: &str, pending: &mut VecDeque<String>) {
    let Some(payload) = line.strip_prefix("data: ") else {
        return;
    };
    if payload == "[DONE]" {
        return;
    }
    let Ok(chunk) = serde_json::from_str::<OpenAiChunk>(payload) else {
        return;
    };
    pending.extend(
        chunk
            .choices
            .into_iter()
            .filter_map(|choice| choice.delta.content)
            .filter(|content| !content.is_empty()),
    );
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use serde_json::Value;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::OpenAiModelClient;
    use crate::model::types::{ChatMessage, ChatRequest, ModelClient};

    #[tokio::test]
    async fn stream_chat_uses_default_model_for_empty_request_and_streams_content() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 8192];
            let read = socket.read(&mut buffer).await.unwrap();
            let request = String::from_utf8(buffer[..read].to_vec()).unwrap();
            let (_, body) = request.split_once("\r\n\r\n").unwrap();
            let json: Value = serde_json::from_str(body).unwrap();
            assert_eq!(json["model"], "fallback-model");
            assert_eq!(json["stream"], true);
            assert_eq!(json["messages"][0]["role"], "user");
            assert_eq!(json["messages"][0]["content"], "Hi");

            socket
                .write_all(
                    concat!(
                        "HTTP/1.1 200 OK\r\n",
                        "content-type: text/event-stream\r\n",
                        "connection: close\r\n",
                        "\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket
                .write_all(b"data: {\"choices\":[{\"delta\":{}}]}\n")
                .await
                .unwrap();
            socket
                .write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n")
                .await
                .unwrap();
            socket
                .write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\ndata: [DONE]\n\n")
                .await
                .unwrap();
        });

        let client = OpenAiModelClient::new(
            "secret-key".to_string(),
            format!("http://{address}"),
            "fallback-model".to_string(),
        );
        let request = ChatRequest {
            model: String::new(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: "Hi".to_string(),
            }],
            temperature: Some(0.1),
            max_tokens: Some(32),
        };

        let mut stream = client.stream_chat(request).await.unwrap();
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next().await {
            chunks.push(chunk.unwrap());
        }

        assert_eq!(chunks, vec!["Hel".to_string(), "lo".to_string()]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stream_chat_sanitizes_error_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 2048];
            let _ = socket.read(&mut buffer).await.unwrap();

            let response = concat!(
                "HTTP/1.1 401 Unauthorized\r\n",
                "content-type: application/json\r\n",
                "content-length: 25\r\n",
                "connection: close\r\n",
                "\r\n",
                "{\"error\":\"invalid key\"}"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let api_key = "secret-key".to_string();
        let client = OpenAiModelClient::new(
            api_key.clone(),
            format!("http://{address}"),
            "fallback-model".to_string(),
        );
        let request = ChatRequest {
            model: "gpt-test".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: "Hi".to_string(),
            }],
            temperature: None,
            max_tokens: None,
        };

        let error = match client.stream_chat(request).await {
            Ok(_) => panic!("expected upstream error"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(message.contains("401"));
        assert!(!message.contains(&api_key));

        server.await.unwrap();
    }
}
