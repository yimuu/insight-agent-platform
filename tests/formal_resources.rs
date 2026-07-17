use std::{collections::BTreeSet, sync::Arc, time::Duration};

use futures::StreamExt;
use insight_agent_platform::{
    resources::{
        actions::{Action, ActionContext, ActionRegistry},
        builtin_actions::{
            builtin_action_registry, CurrentTimeAction, RestrictedHttpGetAction, TextMetricsAction,
        },
        models::{
            ChatContent, ChatContentPart, ChatMessage, ChatModel, ChatRequest, ChatResponseFormat,
            ChatRole, ChatStream, ModelCapability,
        },
        openai_chat::{OpenAiChatLimits, OpenAiChatModel, OpenAiTransportPolicy},
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
    ExecutionControl::new(stop, Duration::from_secs(2))
}

fn action_context() -> ActionContext {
    ActionContext::for_operation("run_test", "action_test", 1, control())
}

fn model(base_url: String, api_key: Option<String>) -> OpenAiChatModel {
    loopback_model(base_url, api_key)
}

fn model_with_limits(
    base_url: String,
    api_key: Option<String>,
    limits: OpenAiChatLimits,
) -> OpenAiChatModel {
    loopback_model_with_limits(base_url, api_key, limits)
}

fn loopback_model(base_url: String, api_key: Option<String>) -> OpenAiChatModel {
    loopback_model_with_limits(base_url, api_key, OpenAiChatLimits::default())
}

fn loopback_model_with_limits(
    base_url: String,
    api_key: Option<String>,
    limits: OpenAiChatLimits,
) -> OpenAiChatModel {
    OpenAiChatModel::new_with_limits_and_transport_policy(
        api_key,
        base_url,
        "fallback-model".to_string(),
        BTreeSet::from([ModelCapability::JsonSchemaOutput, ModelCapability::Vision]),
        Duration::from_secs(1),
        Duration::from_secs(2),
        limits,
        OpenAiTransportPolicy::AllowLoopbackHttp,
    )
    .unwrap()
}

fn loopback_json_object_model(base_url: String) -> OpenAiChatModel {
    OpenAiChatModel::new_with_limits_and_transport_policy(
        None,
        base_url,
        "fallback-model".to_string(),
        BTreeSet::from([ModelCapability::JsonObjectOutput]),
        Duration::from_secs(1),
        Duration::from_secs(2),
        OpenAiChatLimits::default(),
        OpenAiTransportPolicy::AllowLoopbackHttp,
    )
    .unwrap()
}

fn trusted_private_model(base_url: String, api_key: Option<String>) -> OpenAiChatModel {
    OpenAiChatModel::new_with_limits_and_transport_policy(
        api_key,
        base_url,
        "fallback-model".to_string(),
        BTreeSet::from([ModelCapability::JsonSchemaOutput, ModelCapability::Vision]),
        Duration::from_secs(1),
        Duration::from_secs(2),
        OpenAiChatLimits::default(),
        OpenAiTransportPolicy::AllowTrustedPrivateHttp,
    )
    .unwrap()
}

fn default_chat_request() -> ChatRequest {
    ChatRequest {
        messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
        parameters: json!({}),
        response_format: None,
    }
}

#[test]
fn openai_request_budget_matches_the_exact_provider_wire_envelope() {
    let model = loopback_model("http://127.0.0.1:9".to_string(), None);
    let request = ChatRequest {
        messages: vec![ChatMessage {
            role: ChatRole::User,
            content: ChatContent::Parts(vec![
                ChatContentPart::Text {
                    text: "Interpret this.".to_string(),
                },
                ChatContentPart::Image {
                    image: "https://example.test/report.png".to_string(),
                },
            ]),
        }],
        parameters: json!({"temperature":0.2, "max_tokens":64}),
        response_format: Some(ChatResponseFormat::JsonSchema {
            name: "response".to_string(),
            schema: json!({"type":"object", "additionalProperties":false}),
        }),
    };
    let expected_wire_bytes = serde_json::to_vec(&json!({
        "model": "fallback-model",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "Interpret this."},
                {
                    "type": "image_url",
                    "image_url": {"url": "https://example.test/report.png"}
                }
            ]
        }],
        "stream": true,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "response",
                "strict": true,
                "schema": {"type":"object", "additionalProperties":false}
            }
        },
        "temperature": 0.2,
        "max_tokens": 64
    }))
    .unwrap()
    .len();

    assert!(model.request_body_within_limit(&request, expected_wire_bytes));
    assert!(!model.request_body_within_limit(&request, expected_wire_bytes - 1));
}

#[test]
fn json_object_request_budget_counts_the_injected_schema_instruction() {
    let model = loopback_json_object_model("http://127.0.0.1:9".to_string());
    let schema = json!({
        "type":"object",
        "required":["answer"],
        "properties":{"answer":{"type":"string"}},
        "additionalProperties":false
    });
    let instruction = format!(
        "\n\nReturn only a valid JSON object matching this JSON Schema:\n{}",
        serde_json::to_string(&schema).unwrap()
    );
    let request = ChatRequest {
        messages: vec![ChatMessage::from_text(ChatRole::User, "Analyze this.")],
        parameters: json!({}),
        response_format: Some(ChatResponseFormat::JsonObject {
            name: "response".to_string(),
            schema,
        }),
    };
    let expected_wire_bytes = serde_json::to_vec(&json!({
        "model":"fallback-model",
        "messages":[{"role":"user", "content":format!("Analyze this.{instruction}")}],
        "stream":true,
        "response_format":{"type":"json_object"}
    }))
    .unwrap()
    .len();

    assert!(model.request_body_within_limit(&request, expected_wire_bytes));
    assert!(!model.request_body_within_limit(&request, expected_wire_bytes - 1));
}

#[tokio::test]
async fn direct_openai_boundary_rejects_invalid_provider_neutral_messages() {
    let model = loopback_model("http://127.0.0.1:9".to_string(), None);
    let result = model
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage::from_text(ChatRole::Assistant, "prefill")],
            parameters: json!({}),
            response_format: None,
        })
        .await;
    let error = match result {
        Ok(_) => panic!("an assistant-prefill request must fail before network I/O"),
        Err(error) => error,
    };

    assert_eq!(error.code(), "VNEXT_LLM_MESSAGE_ORDER_INVALID");
    assert_eq!(
        error.message(),
        "chat provider request must end with a user message"
    );
}

#[tokio::test]
async fn direct_openai_boundary_rechecks_vision_capability() {
    let model = OpenAiChatModel::new_with_limits_and_transport_policy(
        None,
        "http://127.0.0.1:9".to_string(),
        "text-only-model".to_string(),
        BTreeSet::new(),
        Duration::from_secs(1),
        Duration::from_secs(2),
        OpenAiChatLimits::default(),
        OpenAiTransportPolicy::AllowLoopbackHttp,
    )
    .unwrap();
    let result = model
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage {
                role: ChatRole::User,
                content: ChatContent::Parts(vec![ChatContentPart::Image {
                    image: "https://example.test/report.png".to_string(),
                }]),
            }],
            parameters: json!({}),
            response_format: None,
        })
        .await;
    let error = match result {
        Ok(_) => panic!("a non-Vision adapter must reject image content before network I/O"),
        Err(error) => error,
    };

    assert_eq!(error.code(), "VNEXT_LLM_VISION_REQUIRED");
}

#[tokio::test]
async fn direct_openai_boundary_rechecks_image_url_contracts() {
    let model = loopback_model("http://127.0.0.1:9".to_string(), None);
    for image in ["image://bad", "data:image/png;base64,***"] {
        let result = model
            .stream_chat(ChatRequest {
                messages: vec![ChatMessage {
                    role: ChatRole::User,
                    content: ChatContent::Parts(vec![ChatContentPart::Image {
                        image: image.to_string(),
                    }]),
                }],
                parameters: json!({}),
                response_format: None,
            })
            .await;
        let error = match result {
            Ok(_) => panic!("an invalid image URL must fail before network I/O"),
            Err(error) => error,
        };
        assert_eq!(error.code(), "VNEXT_LLM_CONTENT_INVALID");
    }
}

#[tokio::test]
async fn direct_openai_boundary_rechecks_structured_output_capability() {
    let model = OpenAiChatModel::new_with_limits_and_transport_policy(
        None,
        "http://127.0.0.1:9".to_string(),
        "text-only-model".to_string(),
        BTreeSet::new(),
        Duration::from_secs(1),
        Duration::from_secs(2),
        OpenAiChatLimits::default(),
        OpenAiTransportPolicy::AllowLoopbackHttp,
    )
    .unwrap();
    let result = model
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
            parameters: json!({}),
            response_format: Some(ChatResponseFormat::JsonSchema {
                name: "response".to_string(),
                schema: json!({"type":"object", "additionalProperties":false}),
            }),
        })
        .await;
    let error = match result {
        Ok(_) => panic!("a text-only adapter must reject structured output before network I/O"),
        Err(error) => error,
    };

    assert_eq!(error.code(), "VNEXT_LLM_STRUCTURED_OUTPUT_REQUIRED");

    let object_only = loopback_json_object_model("http://127.0.0.1:9".to_string());
    let error = match object_only
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
            parameters: json!({}),
            response_format: Some(ChatResponseFormat::JsonSchema {
                name: "response".to_string(),
                schema: json!({"type":"object", "additionalProperties":false}),
            }),
        })
        .await
    {
        Ok(_) => panic!("json_object_output must not satisfy a json_schema request"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "VNEXT_LLM_STRUCTURED_OUTPUT_REQUIRED");

    let error = match object_only
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
            parameters: json!({}),
            response_format: Some(ChatResponseFormat::JsonObject {
                name: "response".to_string(),
                schema: json!({"type":"array", "items":{"type":"string"}}),
            }),
        })
        .await
    {
        Ok(_) => panic!("json_object mode must reject a non-object schema before network I/O"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "VNEXT_LLM_RESPONSE_CONFIG_INVALID");
}

async fn clean_eof_stream(body: Vec<u8>) -> (ChatStream, tokio::task::JoinHandle<()>) {
    clean_eof_stream_with_config(body, None, "").await
}

async fn clean_eof_stream_with_config(
    body: Vec<u8>,
    api_key: Option<String>,
    endpoint_suffix: &str,
) -> (ChatStream, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket.write_all(&body).await.unwrap();
    });
    let stream = model(format!("http://{address}{endpoint_suffix}"), api_key)
        .stream_chat(default_chat_request())
        .await
        .unwrap();
    (stream, server)
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

fn assert_incomplete(error: &RunError, forbidden: &[&str]) {
    assert_eq!(error.code(), "UPSTREAM_STREAM_INCOMPLETE");
    assert_eq!(
        error.message(),
        "chat provider stream ended without completion evidence"
    );
    let rendered = format!("{error:?} {error}");
    for value in forbidden {
        assert!(
            !rendered.contains(value),
            "incomplete-stream error leaked forbidden value {value}: {rendered}"
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
        assert_eq!(request["response_format"]["type"], "json_schema");
        assert_eq!(
            request["response_format"]["json_schema"]["name"],
            "response"
        );
        assert_eq!(request["response_format"]["json_schema"]["strict"], true);
        assert_eq!(
            request["response_format"]["json_schema"]["schema"],
            json!({"type":"object", "additionalProperties":false})
        );
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
                    ChatContentPart::Image {
                        image: "https://example.test/report.png".to_string(),
                    },
                ]),
            },
        ],
        parameters: json!({"temperature":0.2, "max_tokens":64}),
        response_format: Some(ChatResponseFormat::JsonSchema {
            name: "response".to_string(),
            schema: json!({"type":"object", "additionalProperties":false}),
        }),
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
async fn openai_adapter_serializes_json_object_mode_and_injects_schema_instruction() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let schema = json!({
        "$ref":"#/$defs/Answer",
        "$defs":{
            "Answer":{
                "type":"object",
                "required":["answer"],
                "properties":{"answer":{"type":"string"}},
                "additionalProperties":false
            }
        }
    });
    let expected_schema = schema.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request_json(&mut socket).await;
        assert_eq!(request["response_format"], json!({"type":"json_object"}));
        let content = request["messages"][0]["content"].as_str().unwrap();
        assert!(content.starts_with("Analyze this."));
        assert!(content.contains("valid JSON object"));
        assert!(content.contains("JSON Schema"));
        assert!(content.contains(&serde_json::to_string(&expected_schema).unwrap()));
        write_sse_headers(&mut socket).await;
        socket.write_all(b"data: [DONE]\n\n").await.unwrap();
    });
    let request = ChatRequest {
        messages: vec![ChatMessage::from_text(ChatRole::User, "Analyze this.")],
        parameters: json!({}),
        response_format: Some(ChatResponseFormat::JsonObject {
            name: "response".to_string(),
            schema,
        }),
    };

    let mut stream = loopback_json_object_model(format!("http://{address}"))
        .stream_chat(request)
        .await
        .unwrap();
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
async fn openai_done_marker_completes_and_closes_an_open_transport() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        write_sse_headers(&mut socket).await;
        socket
            .write_all(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"complete\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\ndata: malformed-after-done-secret\n\n",
            )
            .await
            .unwrap();

        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
            .await
            .expect("the client must close the response body after [DONE]")
            .unwrap();
        assert_eq!(read, 0, "the client sent unexpected bytes after [DONE]");
    });
    let model = model(format!("http://{address}"), None);
    let mut stream = model.stream_chat(default_chat_request()).await.unwrap();

    let chunk = stream.next().await.unwrap().unwrap();
    assert_eq!(chunk.text, "complete");
    assert_eq!(chunk.finish_reason, None);
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("provider must observe body close before the caller polls again")
        .unwrap();
    let completed = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("[DONE] must complete without waiting for transport EOF");
    assert!(completed.is_none());
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn openai_finish_reason_without_done_is_incomplete_at_clean_eof() {
    let body = b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
    let (mut stream, server) = clean_eof_stream(body.to_vec()).await;

    let chunk = stream.next().await.unwrap().unwrap();
    assert_eq!(chunk.text, "");
    assert_eq!(chunk.finish_reason.as_deref(), Some("stop"));
    let error = stream
        .next()
        .await
        .expect("clean EOF without [DONE] must yield an error")
        .expect_err("finish_reason is not protocol completion evidence");
    assert_incomplete(&error, &[]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_finish_and_usage_without_done_are_incomplete_at_clean_eof() {
    let response_secret = "finish-usage-response-secret";
    let usage_secret = "finish-usage-metadata-secret";
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{response_secret}\"}},\"finish_reason\":\"stop\"}}]}}\n\n\
         data: {{\"choices\":[],\"usage\":{{\"detail\":\"{usage_secret}\"}}}}\n\n"
    );
    let (mut stream, server) = clean_eof_stream(body.into_bytes()).await;

    let terminal = stream.next().await.unwrap().unwrap();
    assert_eq!(terminal.text, response_secret);
    assert_eq!(terminal.finish_reason.as_deref(), Some("stop"));
    let usage = stream.next().await.unwrap().unwrap();
    assert_eq!(usage.text, "");
    assert_eq!(usage.finish_reason, None);
    assert_eq!(usage.usage, Some(json!({"detail":usage_secret})));
    let error = stream
        .next()
        .await
        .expect("clean EOF without [DONE] must yield an error")
        .expect_err("finish_reason plus usage is not protocol completion evidence");
    assert_incomplete(&error, &[response_secret, usage_secret]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_content_only_clean_eof_is_incomplete() {
    let response_secret = "content-only-response-secret";
    let api_key_secret = "incomplete-api-key-secret";
    let query_secret = "incomplete-query-secret";
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{response_secret}\"}},\"finish_reason\":null}}]}}\n\n"
    );
    let endpoint_suffix = format!("/v1?token={query_secret}");
    let (mut stream, server) = clean_eof_stream_with_config(
        body.into_bytes(),
        Some(api_key_secret.to_string()),
        &endpoint_suffix,
    )
    .await;

    assert_eq!(stream.next().await.unwrap().unwrap().text, response_secret);
    let error = stream
        .next()
        .await
        .expect("content-only EOF must yield an error")
        .expect_err("content is not protocol completion evidence");
    assert_incomplete(&error, &[response_secret, api_key_secret, query_secret]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_empty_clean_eof_is_incomplete() {
    let (mut stream, server) = clean_eof_stream(Vec::new()).await;

    let error = stream
        .next()
        .await
        .expect("empty EOF must yield an error")
        .expect_err("an empty response is not protocol completion evidence");
    assert_incomplete(&error, &[]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_comment_only_clean_eof_is_incomplete() {
    let (mut stream, server) = clean_eof_stream(b": keepalive\n\n".to_vec()).await;

    let error = stream
        .next()
        .await
        .expect("comment-only EOF must yield an error")
        .expect_err("an SSE comment is not protocol completion evidence");
    assert_incomplete(&error, &[]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_usage_only_clean_eof_is_incomplete() {
    let usage_secret = "usage-only-metadata-secret";
    let body = format!("data: {{\"choices\":[],\"usage\":{{\"detail\":\"{usage_secret}\"}}}}\n\n");
    let (mut stream, server) = clean_eof_stream(body.into_bytes()).await;

    let usage = stream.next().await.unwrap().unwrap();
    assert_eq!(usage.text, "");
    assert_eq!(usage.finish_reason, None);
    assert_eq!(usage.usage, Some(json!({"detail":usage_secret})));
    let error = stream
        .next()
        .await
        .expect("usage-only EOF must yield an error")
        .expect_err("usage is not protocol completion evidence");
    assert_incomplete(&error, &[usage_secret]);
    server.await.unwrap();
}

#[tokio::test]
async fn openai_done_without_a_trailing_newline_completes_at_clean_eof() {
    let (mut stream, server) = clean_eof_stream(b"data: [DONE]".to_vec()).await;

    assert!(stream.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn openai_final_malformed_json_stays_invalid_and_body_free() {
    let payload_secret = "malformed-final-json-secret";
    let body = format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{payload_secret}");
    let (mut stream, server) = clean_eof_stream(body.into_bytes()).await;

    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(error.code(), "UPSTREAM_STREAM_INVALID");
    assert_eq!(error.message(), "invalid chat provider stream payload");
    assert!(!format!("{error:?} {error}").contains(payload_secret));
    server.await.unwrap();
}

#[tokio::test]
async fn openai_final_truncated_utf8_stays_invalid_and_body_free() {
    let payload_secret = "truncated-final-utf8-secret";
    let mut body =
        format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{payload_secret}").into_bytes();
    body.extend_from_slice(&[0xE2, 0x82]);
    let (mut stream, server) = clean_eof_stream(body).await;

    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(error.code(), "UPSTREAM_STREAM_INVALID");
    assert_eq!(error.message(), "invalid UTF-8 in chat stream");
    assert!(!format!("{error:?} {error}").contains(payload_secret));
    server.await.unwrap();
}

#[tokio::test]
async fn openai_content_length_truncation_after_finish_reason_stays_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let close_transport = Arc::new(Notify::new());
    let server_close_transport = Arc::clone(&close_transport);
    let response_secret = "terminal-before-transport-error-secret";
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{response_secret}\"}},\"finish_reason\":\"stop\"}}]}}\n\n"
    );
    let declared_length = body.len() + 1;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {declared_length}\r\nconnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
        server_close_transport.notified().await;
    });
    let model = model(format!("http://{address}"), None);
    let mut stream = model.stream_chat(default_chat_request()).await.unwrap();

    let terminal = stream.next().await.unwrap().unwrap();
    assert_eq!(terminal.text, response_secret);
    assert_eq!(terminal.finish_reason.as_deref(), Some("stop"));
    close_transport.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("the truncated Content-Length body must terminate")
        .expect("the truncated Content-Length body must yield an error")
        .expect_err("transport truncation must not become clean EOF success");
    assert_eq!(error.code(), "UPSTREAM_STREAM");
    assert_eq!(error.message(), "chat provider stream failed (transport)");
    assert!(!format!("{error:?} {error}").contains(response_secret));
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
async fn openai_stream_rejects_oversized_content_length_without_echoing_body() {
    let body_secret = "content-length-body-secret";
    let body = format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{body_secret}\"}},\"finish_reason\":null}}]}}\n\n"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response_body = body.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request_json(&mut socket).await;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    response_body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let _ = socket.write_all(response_body.as_bytes()).await;
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
    let error = match model.stream_chat(default_chat_request()).await {
        Ok(_) => panic!("oversized content-length must fail before returning a stream"),
        Err(error) => error,
    };

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

#[test]
fn openai_transport_policy_rejects_plaintext_http_by_default() {
    for constructor in ["new", "new_with_limits"] {
        let error = if constructor == "new" {
            OpenAiChatModel::new(
                Some("api-key-secret".to_string()),
                "http://model-service.internal/v1?token=url-secret".to_string(),
                "fallback-model".to_string(),
                BTreeSet::new(),
                Duration::from_secs(1),
                Duration::from_secs(2),
            )
            .unwrap_err()
        } else {
            OpenAiChatModel::new_with_limits(
                Some("api-key-secret".to_string()),
                "http://model-service.internal/v1?token=url-secret".to_string(),
                "fallback-model".to_string(),
                BTreeSet::new(),
                Duration::from_secs(1),
                Duration::from_secs(2),
                OpenAiChatLimits::default(),
            )
            .unwrap_err()
        };
        assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("api-key-secret"));
        assert!(!rendered.contains("url-secret"));
        assert!(!rendered.contains("model-service.internal"));
    }
}

#[test]
fn openai_transport_policy_allows_only_explicit_plaintext_scopes() {
    for base_url in [
        "http://127.0.0.1:8080/v1",
        "http://localhost:8080/v1",
        "http://[::1]:8080/v1",
    ] {
        loopback_model(base_url.to_string(), None);
    }

    let non_loopback = OpenAiChatModel::new_with_limits_and_transport_policy(
        None,
        "http://10.0.0.10:8080/v1".to_string(),
        "fallback-model".to_string(),
        BTreeSet::new(),
        Duration::from_secs(1),
        Duration::from_secs(2),
        OpenAiChatLimits::default(),
        OpenAiTransportPolicy::AllowLoopbackHttp,
    )
    .unwrap_err();
    assert_eq!(non_loopback.code(), "MODEL_CONFIG_INVALID");

    trusted_private_model("http://10.0.0.10:8080/v1".to_string(), None);
    trusted_private_model(
        "http://model.default.svc.cluster.local:8080/v1".to_string(),
        None,
    );
}

#[test]
fn openai_transport_policy_rejects_non_exact_loopback_aliases() {
    for base_url in [
        "http://127.1:8080/v1",
        "http://127.000.000.001:8080/v1",
        "http://2130706433:8080/v1",
        "http://[0:0:0:0:0:0:0:1]:8080/v1",
    ] {
        let error = OpenAiChatModel::new_with_limits_and_transport_policy(
            None,
            base_url.to_string(),
            "fallback-model".to_string(),
            BTreeSet::new(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            OpenAiChatLimits::default(),
            OpenAiTransportPolicy::AllowLoopbackHttp,
        )
        .unwrap_err();
        assert_eq!(error.code(), "MODEL_CONFIG_INVALID", "{base_url}");
    }
}

#[test]
fn openai_transport_policy_rejects_url_userinfo_for_every_policy() {
    for (base_url, policy) in [
        (
            "https://user:pass@models.example.test/v1",
            OpenAiTransportPolicy::HttpsOnly,
        ),
        (
            "http://user:pass@127.0.0.1:8080/v1",
            OpenAiTransportPolicy::AllowLoopbackHttp,
        ),
        (
            "http://user:pass@model.internal:8080/v1",
            OpenAiTransportPolicy::AllowTrustedPrivateHttp,
        ),
    ] {
        let error = OpenAiChatModel::new_with_limits_and_transport_policy(
            Some("api-key-secret".to_string()),
            base_url.to_string(),
            "fallback-model".to_string(),
            BTreeSet::new(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            OpenAiChatLimits::default(),
            policy,
        )
        .unwrap_err();
        assert_eq!(error.code(), "MODEL_CONFIG_INVALID");
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("user:pass"));
        assert!(!rendered.contains("api-key-secret"));
    }
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
            response_format: None,
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
            response_format: None,
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
async fn openai_client_does_not_follow_redirects_or_leak_authorization_to_location() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let second_request_seen = Arc::new(Notify::new());
    let server_second_request_seen = Arc::clone(&second_request_seen);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = Vec::new();
        loop {
            let mut chunk = [0_u8; 2048];
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            buffer.extend_from_slice(&chunk[..read]);
            if buffer.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8_lossy(&buffer);
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: bearer api-key-secret"));
        socket
            .write_all(
                b"HTTP/1.1 302 Found\r\nlocation: /redirected\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )
            .await
            .unwrap();

        match tokio::time::timeout(Duration::from_millis(100), listener.accept()).await {
            Ok(Ok((_socket, _))) => server_second_request_seen.notify_one(),
            Ok(Err(error)) => panic!("unexpected accept error: {error}"),
            Err(_) => {}
        }
    });

    let model = loopback_model(
        format!("http://{address}/v1"),
        Some("api-key-secret".to_string()),
    );
    let error = match model
        .stream_chat(ChatRequest {
            messages: vec![ChatMessage::from_text(ChatRole::User, "Hi")],
            parameters: json!({}),
            response_format: None,
        })
        .await
    {
        Ok(_) => panic!("expected upstream redirect status failure"),
        Err(error) => error,
    };

    assert_eq!(error.code(), "UPSTREAM_STATUS");
    assert!(error.to_string().contains("302"));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), second_request_seen.notified())
            .await
            .is_err()
    );
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
            response_format: None,
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
    assert_eq!(current_time.id, "current_time");
    assert!(!current_time.idempotency.is_idempotent());
    assert_eq!(current_time.input_schema["additionalProperties"], false);

    let http = RestrictedHttpGetAction::new(
        Duration::from_secs(1),
        1024,
        vec!["example.test".to_string()],
    )
    .unwrap()
    .descriptor();
    assert_eq!(http.id, "http_get");
    assert!(http.idempotency.is_idempotent());
    assert_eq!(http.output_schema["required"], json!(["status", "body"]));

    let metrics = TextMetricsAction.descriptor();
    assert_eq!(metrics.id, "example.text_metrics");
    assert!(metrics.idempotency.is_idempotent());
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
