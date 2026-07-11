use std::{collections::BTreeSet, sync::Arc, time::Duration};

use futures::StreamExt;
use insight_agent_platform::{
    resources::{
        actions::{Action, ActionContext, ActionRegistry},
        builtin_actions::{
            builtin_action_registry, CurrentTimeAction, RestrictedHttpGetAction, TextMetricsAction,
        },
        models::{
            ChatContent, ChatContentPart, ChatMessage, ChatModel, ChatRequest, ChatRole, ImageUrl,
            ModelCapability,
        },
        openai_chat::{OpenAiChatLimits, OpenAiChatModel},
    },
    runtime::{stop_pair, ExecutionControl, RunError},
};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};

fn control() -> ExecutionControl {
    let (_, stop) = stop_pair();
    ExecutionControl::new(stop, Duration::from_secs(2), |_| async { Ok(()) })
}

fn action_context() -> ActionContext {
    ActionContext::new("run_test", "action_test", control())
}

fn model(base_url: String, api_key: Option<String>) -> OpenAiChatModel {
    OpenAiChatModel::new(
        api_key,
        base_url,
        "fallback-model".to_string(),
        BTreeSet::from([ModelCapability::Vision]),
        Duration::from_secs(1),
        Duration::from_secs(2),
    )
    .unwrap()
}

fn model_with_limits(
    base_url: String,
    api_key: Option<String>,
    limits: OpenAiChatLimits,
) -> OpenAiChatModel {
    OpenAiChatModel::new_with_limits(
        api_key,
        base_url,
        "fallback-model".to_string(),
        BTreeSet::from([ModelCapability::Vision]),
        Duration::from_secs(1),
        Duration::from_secs(2),
        limits,
    )
    .unwrap()
}

fn default_chat_request() -> ChatRequest {
    ChatRequest {
        messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
        parameters: json!({}),
    }
}

async fn next_stream_error(model: OpenAiChatModel) -> RunError {
    let mut stream = model.stream_chat(default_chat_request()).await.unwrap();
    stream
        .next()
        .await
        .expect("stream should yield an error")
        .expect_err("limit violation must be an error")
}

fn assert_too_large(error: &RunError, forbidden: &[&str]) {
    assert_eq!(error.code(), "MODEL_RESPONSE_TOO_LARGE");
    assert_eq!(
        error.message(),
        "chat provider response exceeded the configured size limit"
    );
    let rendered = format!("{error:?} {error}");
    for value in forbidden {
        assert!(
            !rendered.contains(value),
            "limit error leaked forbidden value {value}: {rendered}"
        );
    }
}

#[tokio::test]
async fn openai_adapter_serializes_formal_messages_and_allowed_parameters() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request_json(&mut socket).await;
        assert_eq!(request["model"], "fallback-model");
        assert_eq!(request["stream"], true);
        assert_eq!(request["temperature"], 0.2);
        assert_eq!(request["max_tokens"], 64);
        assert_eq!(request["messages"][0]["role"], "system");
        assert_eq!(request["messages"][0]["content"], "Be concise.");
        assert_eq!(request["messages"][1]["content"][0]["type"], "text");
        assert_eq!(
            request["messages"][1]["content"][1]["image_url"]["url"],
            "https://example.test/report.png"
        );
        write_sse_headers(&mut socket).await;
        socket
            .write_all(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}],\"usage\":null}\n\n",
            )
            .await
            .unwrap();
        socket
            .write_all(
                b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":1}}\n\n",
            )
            .await
            .unwrap();
        socket.write_all(b"data: [DONE]\n\n").await.unwrap();
    });
    let model = model(format!("http://{address}"), Some("secret-key".to_string()));
    let request = ChatRequest {
        messages: vec![
            ChatMessage::from_text(ChatRole::System, "Be concise."),
            ChatMessage {
                role: ChatRole::User,
                content: ChatContent::Parts(vec![
                    ChatContentPart::Text {
                        text: "Interpret this.".to_string(),
                    },
                    ChatContentPart::ImageUrl {
                        image_url: ImageUrl {
                            url: "https://example.test/report.png".to_string(),
                        },
                    },
                ]),
            },
        ],
        parameters: json!({"temperature":0.2, "max_tokens":64}),
    };

    let mut stream = model.stream_chat(request).await.unwrap();
    let first = stream.next().await.unwrap().unwrap();
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(first.text, "Hi");
    assert_eq!(first.finish_reason, None);
    assert_eq!(second.text, "");
    assert_eq!(second.finish_reason.as_deref(), Some("stop"));
    assert_eq!(second.usage, Some(json!({"completion_tokens":1})));
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn openai_stream_accepts_exact_configured_response_limits() {
    let payload =
        r#"{"choices":[{"delta":{"content":"abc"},"finish_reason":"stop"}],"usage":{"u":"xy"}}"#;
    let data_line = format!("data: {payload}\n");
    let done_line = "data: [DONE]\n";
    let body = format!("{data_line}\n{done_line}\n");
    let usage = json!({"u":"xy"});
    let limits = OpenAiChatLimits {
        max_upstream_bytes: body.len(),
        max_buffered_line_bytes: data_line.trim_end_matches('\n').len(),
        max_event_payload_bytes: payload.len(),
        max_chunk_text_bytes: "abc".len(),
        max_usage_json_bytes: serde_json::to_vec(&usage).unwrap().len(),
        ..OpenAiChatLimits::default()
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(body.as_bytes()).await.unwrap();
    });

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let mut stream = model.stream_chat(default_chat_request()).await.unwrap();
    let chunk = stream.next().await.unwrap().unwrap();
    assert_eq!(chunk.text, "abc");
    assert_eq!(chunk.finish_reason.as_deref(), Some("stop"));
    assert_eq!(chunk.usage, Some(usage));
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn openai_stream_rejects_total_upstream_bytes_without_echoing_body() {
    let body_secret = "upstream-body-secret";
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{body_secret}\"}},\"finish_reason\":null}}]}}\n\n"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let limits = OpenAiChatLimits {
        max_upstream_bytes: body.len() - 1,
        ..OpenAiChatLimits::default()
    };

    let model = model_with_limits(
        format!("http://{address}/v1?token=url-secret"),
        Some("api-key-secret".to_string()),
        limits,
    );
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[body_secret, "url-secret", "api-key-secret"]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_stream_rejects_no_lf_buffer_growth() {
    let line_secret = "line-buffer-secret";
    let body = format!("data: {line_secret}");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let limits = OpenAiChatLimits {
        max_buffered_line_bytes: body.len() - 1,
        ..OpenAiChatLimits::default()
    };

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[line_secret]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_stream_rejects_oversized_event_payload_without_parsing_secret() {
    let payload_secret = "event-payload-secret";
    let payload = format!(
        "{{\"choices\":[{{\"delta\":{{\"content\":\"{payload_secret}\"}},\"finish_reason\":null}}]}}"
    );
    let body = format!("data: {payload}\n\n");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let limits = OpenAiChatLimits {
        max_event_payload_bytes: payload.len() - 1,
        ..OpenAiChatLimits::default()
    };

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[payload_secret]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_stream_rejects_oversized_chunk_text_without_echoing_text() {
    let text_secret = "chunk-text-secret";
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text_secret}\"}},\"finish_reason\":null}}]}}\n\n"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let limits = OpenAiChatLimits {
        max_chunk_text_bytes: text_secret.len() - 1,
        ..OpenAiChatLimits::default()
    };

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[text_secret]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_stream_rejects_oversized_usage_json_without_echoing_usage() {
    let usage_secret = "usage-json-secret";
    let usage = json!({"detail": usage_secret});
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{}},\"finish_reason\":\"stop\"}}],\"usage\":{usage}}}\n\n"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(response_body.as_bytes()).await.unwrap();
    });
    let limits = OpenAiChatLimits {
        max_usage_json_bytes: serde_json::to_vec(&usage).unwrap().len() - 1,
        ..OpenAiChatLimits::default()
    };

    let model = model_with_limits(format!("http://{address}"), None, limits);
    let error = next_stream_error(model).await;

    assert_too_large(&error, &[usage_secret]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_limit_error_drops_the_in_flight_http_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let closed = Arc::new(Notify::new());
    let server_closed = Arc::clone(&closed);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket
            .write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"too-large\"},\"finish_reason\":null}]}\n\n")
            .await
            .unwrap();
        let mut byte = [0_u8; 1];
        if socket.read(&mut byte).await.unwrap_or(0) == 0 {
            server_closed.notify_one();
        }
    });
    let limits = OpenAiChatLimits {
        max_chunk_text_bytes: "too-large".len() - 1,
        ..OpenAiChatLimits::default()
    };
    let model = model_with_limits(format!("http://{address}"), None, limits);
    let mut stream = model.stream_chat(default_chat_request()).await.unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_too_large(&error, &["too-large"]);

    drop(stream);

    tokio::time::timeout(Duration::from_secs(1), closed.notified())
        .await
        .unwrap();
    server.await.unwrap();
}

#[test]
fn openai_adapter_rejects_unknown_or_out_of_range_parameters() {
    let model = model("https://api.example.test/v1".to_string(), None);
    for parameters in [
        json!({"model":"injected"}),
        json!({"stream":false}),
        json!({"temperature":3}),
        json!({"max_tokens":0}),
        json!({"unknown":true}),
    ] {
        let error = model.validate_parameters(&parameters).unwrap_err();
        assert_eq!(error.code(), "MODEL_PARAMETERS_INVALID");
    }
    model
        .validate_parameters(&json!({
            "temperature":0,
            "max_tokens":1,
            "top_p":1,
            "frequency_penalty":-2,
            "presence_penalty":2,
            "stop":["END"]
        }))
        .unwrap();
}

#[tokio::test]
async fn openai_sse_decoder_handles_fragmented_multibyte_utf8() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket
            .write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"H")
            .await
            .unwrap();
        socket.write_all(&[0xC3]).await.unwrap();
        socket.write_all(&[0xA9]).await.unwrap();
        socket
            .write_all(b"llo\"},\"finish_reason\":null}]}\n\n")
            .await
            .unwrap();
        socket.write_all(b"data: [DONE]\n\n").await.unwrap();
    });
    let model = model(format!("http://{address}"), None);
    let mut stream = model
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
            parameters: json!({}),
        })
        .await
        .unwrap();

    assert_eq!(stream.next().await.unwrap().unwrap().text, "Héllo");
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn openai_errors_and_debug_never_expose_api_key_or_response_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        let body = "{\"error\":\"body-secret\"}";
        socket
            .write_all(
                format!(
                    "HTTP/1.1 401 Unauthorized\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });
    let api_key = "api-key-secret".to_string();
    let model = model(
        format!("http://{address}/v1?token=url-secret"),
        Some(api_key.clone()),
    );
    let debug = format!("{model:?}");
    assert!(!debug.contains(&api_key));
    assert!(!debug.contains("url-secret"));

    let error = match model
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
            parameters: json!({}),
        })
        .await
    {
        Ok(_) => panic!("expected upstream status failure"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "UPSTREAM_STATUS");
    let message = error.to_string();
    assert!(message.contains("401"));
    assert!(!message.contains(&api_key));
    assert!(!message.contains("body-secret"));
    assert!(!message.contains("url-secret"));
    server.await.unwrap();
}

#[tokio::test]
async fn dropping_openai_stream_closes_the_in_flight_http_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let closed = Arc::new(Notify::new());
    let server_closed = Arc::clone(&closed);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket
            .write_all(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"first\"},\"finish_reason\":null}]}\n\n",
            )
            .await
            .unwrap();
        let mut byte = [0_u8; 1];
        if socket.read(&mut byte).await.unwrap_or(0) == 0 {
            server_closed.notify_one();
        }
    });
    let model = model(format!("http://{address}"), None);
    let mut stream = model
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
            parameters: json!({}),
        })
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().text, "first");

    drop(stream);

    tokio::time::timeout(Duration::from_secs(1), closed.notified())
        .await
        .unwrap();
    server.await.unwrap();
}

#[test]
fn builtin_action_descriptors_define_strict_contracts() {
    let current_time = CurrentTimeAction.descriptor();
    assert_eq!(current_time.name, "current_time");
    assert!(!current_time.idempotent);
    assert!(!current_time.streams_content);
    assert_eq!(current_time.input_schema["additionalProperties"], false);

    let http = RestrictedHttpGetAction::new(
        Duration::from_secs(1),
        1024,
        vec!["example.test".to_string()],
    )
    .unwrap()
    .descriptor();
    assert_eq!(http.name, "http_get");
    assert!(http.idempotent);
    assert!(!http.streams_content);
    assert_eq!(http.output_schema["required"], json!(["status", "body"]));

    let metrics = TextMetricsAction.descriptor();
    assert_eq!(metrics.name, "example.text_metrics");
    assert!(metrics.idempotent);
    assert_eq!(
        metrics.output_schema["required"],
        json!(["characters", "words", "lines"])
    );
}

#[tokio::test]
async fn current_time_and_text_metrics_actions_return_schema_valid_outputs() {
    let mut registry = ActionRegistry::default();
    registry.register(CurrentTimeAction).unwrap();
    registry.register(TextMetricsAction).unwrap();

    let time = registry
        .resolve("current_time")
        .unwrap()
        .call(json!({"timezone":"Asia/Shanghai"}), action_context())
        .await
        .unwrap();
    assert_eq!(time["timezone"], "Asia/Shanghai");
    assert!(time["iso8601"].as_str().unwrap().contains("+08:00"));

    let metrics = registry
        .resolve("example.text_metrics")
        .unwrap()
        .call(json!({"text":"hello 世界\nsecond line"}), action_context())
        .await
        .unwrap();
    assert_eq!(metrics, json!({"characters":20, "words":4, "lines":2}));
}

#[tokio::test]
async fn restricted_http_action_blocks_non_https_and_non_allowlisted_urls_safely() {
    let action = RestrictedHttpGetAction::new(
        Duration::from_millis(100),
        1024,
        vec!["allowed.example".to_string()],
    )
    .unwrap();
    let non_https = action
        .call(
            json!({"url":"http://allowed.example/private"}),
            action_context(),
        )
        .await
        .unwrap_err();
    assert_eq!(non_https.code(), "ACTION_HTTP_BLOCKED");
    let secret_url = "https://user:pass@blocked.example/private?token=secret";
    let blocked = action
        .call(json!({"url":secret_url}), action_context())
        .await
        .unwrap_err();
    assert_eq!(blocked.code(), "ACTION_HTTP_BLOCKED");
    assert!(!blocked.to_string().contains("secret"));
    assert!(!blocked.to_string().contains("user:pass"));
}

#[test]
fn builtin_registry_registers_only_explicitly_enabled_actions() {
    let http = RestrictedHttpGetAction::new(
        Duration::from_secs(1),
        1024,
        vec!["example.test".to_string()],
    )
    .unwrap();
    let registry = builtin_action_registry(
        &[
            "current_time".to_string(),
            "example.text_metrics".to_string(),
        ],
        Some(http),
    )
    .unwrap();

    assert_eq!(
        registry.names().collect::<Vec<_>>(),
        vec!["current_time", "example.text_metrics"]
    );
    assert!(registry.resolve("http_get").is_err());
}

async fn read_request_json(socket: &mut tokio::net::TcpStream) -> Value {
    let mut buffer = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 2048];
        let read = socket.read(&mut chunk).await.unwrap();
        assert!(read > 0);
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = buffer.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = String::from_utf8_lossy(&buffer[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(str::trim)
                .and_then(|value| value.parse::<usize>().ok())
        })
        .unwrap();
    while buffer.len() - header_end < content_length {
        let mut chunk = [0_u8; 2048];
        let read = socket.read(&mut chunk).await.unwrap();
        buffer.extend_from_slice(&chunk[..read]);
    }
    serde_json::from_slice(&buffer[header_end..header_end + content_length]).unwrap()
}

async fn write_sse_headers(socket: &mut tokio::net::TcpStream) {
    socket
        .write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n",
        )
        .await
        .unwrap();
}
