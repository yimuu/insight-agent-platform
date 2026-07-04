use std::fmt;
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

#[derive(Clone)]
pub struct OpenAiModelClient {
    client: Client,
    api_key: String,
    base_url: String,
    default_model: String,
}

impl fmt::Debug for OpenAiModelClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiModelClient")
            .field("base_url", &self.base_url)
            .field("default_model", &self.default_model)
            .field("api_key", &"REDACTED")
            .field("client", &"REDACTED")
            .finish()
    }
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
                decoder: SseDecoder::default(),
                pending: VecDeque::new(),
            },
            |mut state| async move {
                loop {
                    if let Some(delta) = state.pending.pop_front() {
                        return Ok(Some((delta, state)));
                    }

                    match state.bytes.next().await {
                        Some(Ok(bytes)) => {
                            let deltas = state.decoder.push(&bytes)?;
                            state.pending.extend(deltas);
                        }
                        Some(Err(err)) => {
                            return Err(AppError::Upstream(format!("model stream failed: {err}")));
                        }
                        None => {
                            let deltas = state.decoder.finish()?;
                            state.pending.extend(deltas);
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
    decoder: SseDecoder,
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

#[derive(Default)]
struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>, AppError> {
        self.buffer.extend_from_slice(bytes);
        self.drain_complete_lines()
    }

    fn finish(&mut self) -> Result<Vec<String>, AppError> {
        let mut deltas = self.drain_complete_lines()?;
        if self.buffer.is_empty() {
            return Ok(deltas);
        }

        let line = std::mem::take(&mut self.buffer);
        if !line.is_empty() {
            deltas.extend(parse_sse_line_bytes(trim_trailing_carriage_return(&line))?);
        }
        Ok(deltas)
    }

    fn drain_complete_lines(&mut self) -> Result<Vec<String>, AppError> {
        let mut deltas = Vec::new();
        while let Some(newline_index) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = self.buffer.drain(..=newline_index).collect::<Vec<_>>();
            line.pop();
            deltas.extend(parse_sse_line_bytes(trim_trailing_carriage_return(&line))?);
        }
        Ok(deltas)
    }
}

fn trim_trailing_carriage_return(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn parse_sse_line_bytes(line: &[u8]) -> Result<Vec<String>, AppError> {
    if line.is_empty() {
        return Ok(Vec::new());
    }

    let line = String::from_utf8(line.to_vec())
        .map_err(|_| AppError::Upstream("invalid utf-8 in model stream".to_string()))?;
    let Some(payload) = line.strip_prefix("data:") else {
        return Ok(Vec::new());
    };
    let payload = payload.trim_start();
    if payload == "[DONE]" {
        return Ok(Vec::new());
    }
    let chunk = serde_json::from_str::<OpenAiChunk>(payload)
        .map_err(|_| AppError::Upstream("invalid model stream payload".to_string()))?;
    Ok(chunk
        .choices
        .into_iter()
        .filter_map(|choice| choice.delta.content)
        .filter(|content| !content.is_empty())
        .collect())
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use serde_json::Value;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{OpenAiModelClient, SseDecoder};
    use crate::model::types::{ChatMessage, ChatRequest, ModelClient};

    #[test]
    fn openai_model_client_debug_does_not_leak_api_key() {
        let api_key = "secret-key-12345".to_string();
        let client = OpenAiModelClient::new(
            api_key.clone(),
            "https://api.openai.com".to_string(),
            "gpt-4o-mini".to_string(),
        );

        let debug_output = format!("{client:?}");

        assert!(!debug_output.contains(&api_key));
        assert!(debug_output.contains("api_key"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn sse_decoder_handles_split_multibyte_utf8_across_chunks() {
        let mut decoder = SseDecoder::default();

        let first = decoder.push(&b"data: {\"choices\":[{\"delta\":{\"content\":\"H"[..]);
        assert!(first.unwrap().is_empty());

        let second = decoder.push(&[0xC3]);
        assert!(second.unwrap().is_empty());

        let third = decoder
            .push(&[
                0xA9, b'l', b'l', b'o', b'"', b'}', b'}', b']', b'}', b'\n', b'\n',
            ])
            .unwrap();

        assert_eq!(third, vec!["H\u{e9}llo".to_string()]);
    }

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
                .write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"H")
                .await
                .unwrap();
            socket.write_all(&[0xC3]).await.unwrap();
            socket.write_all(&[0xA9]).await.unwrap();
            socket.write_all(b"llo\"}}]}\n\n").await.unwrap();
            socket.write_all(b"data: [DONE]\n\n").await.unwrap();
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

        assert_eq!(chunks, vec!["H\u{e9}llo".to_string()]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stream_chat_returns_error_for_malformed_data_json() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 2048];
            let _ = socket.read(&mut buffer).await.unwrap();

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
                .write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
                .await
                .unwrap();
            socket.write_all(b"data: {not json}\n\n").await.unwrap();
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
            temperature: None,
            max_tokens: None,
        };

        let mut stream = client.stream_chat(request).await.unwrap();
        let first = stream.next().await.unwrap();
        let error = match first {
            Ok(token) => {
                assert_eq!(token, "ok");
                match stream.next().await.unwrap() {
                    Ok(_) => panic!("expected malformed JSON error"),
                    Err(error) => error,
                }
            }
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(message.contains("invalid model stream payload"));
        assert!(!message.contains("secret-key"));

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
