use axum::{
    body::{to_bytes, Body},
    http::{header::HeaderName, Request, StatusCode},
};
use serde_json::{json, Value};
use tower::ServiceExt;

use insight_agent_platform::{
    agent::{
        config::{AgentConfig, InputConfig, ModelConfig, StepConfig, StepKind},
        loader::LoadedAgent,
        registry::AgentRegistry,
    },
    api::{
        routes::{build_router, AppState, RuntimeAuth},
        sse::encode_event,
    },
    engine::event::RunEvent,
    engine::runner::RunEngine,
    error::AppError,
    handlers::default_code_registry,
    history::store::RunHistoryStore,
    model::types::FakeModelClient,
    tools::registry::ToolRegistry,
};

fn prompt_input_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": { "type": "string" }
        }
    })
}

fn prompt_agent() -> LoadedAgent {
    LoadedAgent {
        root: std::path::PathBuf::from("agents/test"),
        prompts: Default::default(),
        config: AgentConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: "Test agent".to_string(),
            model: ModelConfig {
                provider: "openai_compatible".to_string(),
                model_type: Default::default(),
                model: Some("fake".to_string()),
                temperature: None,
                max_tokens: None,
                options: serde_json::Value::Null,
            },
            input: InputConfig {
                schema: prompt_input_schema(),
            },
            prompts: Default::default(),
            steps: vec![StepConfig {
                id: "hello".to_string(),
                kind: StepKind::Prompt,
                prompt_ref: None,
                prompt: Some("Hello {{ input.name }}".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                image_input: None,
                stream: false,
                tool: None,
                handler: None,
                inputs: serde_json::Value::Null,
                args: serde_json::Value::Null,
                cases: Vec::new(),
                default: None,
                end: false,
            }],
        },
    }
}

fn failing_tool_agent() -> LoadedAgent {
    LoadedAgent {
        root: std::path::PathBuf::from("agents/failing"),
        prompts: Default::default(),
        config: AgentConfig {
            id: "broken".to_string(),
            name: "Broken".to_string(),
            description: "Broken agent".to_string(),
            model: ModelConfig {
                provider: "openai_compatible".to_string(),
                model_type: Default::default(),
                model: Some("fake".to_string()),
                temperature: None,
                max_tokens: None,
                options: serde_json::Value::Null,
            },
            input: InputConfig {
                schema: json!({"type":"object"}),
            },
            prompts: Default::default(),
            steps: vec![StepConfig {
                id: "missing_tool".to_string(),
                kind: StepKind::Tool,
                prompt_ref: None,
                prompt: None,
                system_prompt_ref: None,
                system_prompt: None,
                image_input: None,
                stream: false,
                tool: Some("not_registered".to_string()),
                handler: None,
                inputs: serde_json::Value::Null,
                args: json!({}),
                cases: Vec::new(),
                default: None,
                end: false,
            }],
        },
    }
}

fn llm_agent() -> LoadedAgent {
    LoadedAgent {
        root: std::path::PathBuf::from("agents/llm"),
        prompts: Default::default(),
        config: AgentConfig {
            id: "llm".to_string(),
            name: "LLM".to_string(),
            description: "LLM agent".to_string(),
            model: ModelConfig {
                provider: "openai_compatible".to_string(),
                model_type: Default::default(),
                model: Some("fake".to_string()),
                temperature: None,
                max_tokens: None,
                options: serde_json::Value::Null,
            },
            input: InputConfig {
                schema: json!({"type":"object"}),
            },
            prompts: Default::default(),
            steps: vec![StepConfig {
                id: "answer".to_string(),
                kind: StepKind::Llm,
                prompt_ref: None,
                prompt: Some("Answer".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                image_input: None,
                stream: true,
                tool: None,
                handler: None,
                inputs: serde_json::Value::Null,
                args: serde_json::Value::Null,
                cases: Vec::new(),
                default: None,
                end: false,
            }],
        },
    }
}

async fn app() -> axum::Router {
    app_with_encoder(encode_event).await
}

async fn app_with_auth(auth: RuntimeAuth) -> axum::Router {
    app_with_encoder_and_auth(encode_event, auth).await
}

async fn app_with_encoder(
    event_encoder: fn(RunEvent) -> Result<axum::response::sse::Event, AppError>,
) -> axum::Router {
    app_with_encoder_and_auth(event_encoder, RuntimeAuth::disabled()).await
}

async fn app_with_encoder_and_auth(
    event_encoder: fn(RunEvent) -> Result<axum::response::sse::Event, AppError>,
    auth: RuntimeAuth,
) -> axum::Router {
    let registry =
        AgentRegistry::new(vec![prompt_agent(), failing_tool_agent(), llm_agent()]).unwrap();
    let engine = RunEngine::new(
        FakeModelClient::new(vec!["Hel", "lo"]),
        ToolRegistry::default(),
    )
    .with_code_handlers(default_code_registry())
    .with_history_store(RunHistoryStore::sqlite_in_memory().await.unwrap());
    build_router(AppState {
        registry,
        engine,
        event_encoder,
        auth,
    })
}

fn code_node_demo_agent() -> LoadedAgent {
    insight_agent_platform::agent::loader::load_agents("agents")
        .unwrap()
        .into_iter()
        .find(|agent| agent.config.id == "code_node_demo")
        .unwrap()
}

async fn app_with_code_node_demo() -> axum::Router {
    let registry = AgentRegistry::new(vec![code_node_demo_agent()]).unwrap();
    let engine = RunEngine::new(FakeModelClient::new(vec![]), ToolRegistry::default())
        .with_code_handlers(default_code_registry())
        .with_history_store(RunHistoryStore::sqlite_in_memory().await.unwrap());
    build_router(AppState {
        registry,
        engine,
        event_encoder: encode_event,
        auth: RuntimeAuth::disabled(),
    })
}

#[derive(Debug)]
struct SseFrame {
    event: String,
    data: Value,
}

fn parse_sse_frames(body: &str) -> Vec<SseFrame> {
    body.split("\n\n")
        .filter_map(|chunk| {
            let trimmed = chunk.trim();
            if trimmed.is_empty() {
                return None;
            }

            let mut event = None;
            let mut data = None;
            for line in trimmed.lines() {
                if let Some(value) = line.strip_prefix("event: ") {
                    event = Some(value.to_string());
                } else if let Some(value) = line.strip_prefix("data: ") {
                    data = Some(serde_json::from_str::<Value>(value).unwrap());
                }
            }

            Some(SseFrame {
                event: event.expect("SSE frame should include event"),
                data: data.expect("SSE frame should include data"),
            })
        })
        .collect()
}

#[tokio::test]
async fn lists_agents_without_prompt_contents() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .uri("/v1/agents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["code"], 0);
    assert_eq!(payload["message"], "ok");
    let agents = &payload["data"];
    let test_agent = agents
        .as_array()
        .unwrap()
        .iter()
        .find(|agent| agent["id"] == "test")
        .expect("expected test agent in list");
    assert_eq!(test_agent["name"], "Test");
    assert_eq!(test_agent["description"], "Test agent");
    assert_eq!(test_agent["input_schema"], prompt_input_schema());
    assert!(test_agent.get("steps").is_none());
    assert!(test_agent.get("prompts").is_none());
    assert!(!String::from_utf8_lossy(&body).contains("Hello {{ input.name }}"));
}

#[tokio::test]
async fn health_does_not_require_internal_auth() {
    let response = app_with_auth(RuntimeAuth::bearer_token("secret"))
        .await
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn v1_routes_reject_missing_internal_auth() {
    let response = app_with_auth(RuntimeAuth::bearer_token("secret"))
        .await
        .oneshot(
            Request::builder()
                .uri("/v1/agents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["message"],
        "missing or invalid internal authorization"
    );
}

#[tokio::test]
async fn v1_routes_accept_internal_bearer_token() {
    let response = app_with_auth(RuntimeAuth::bearer_token("secret"))
        .await
        .oneshot(
            Request::builder()
                .uri("/v1/agents")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn v1_stream_route_rejects_missing_internal_auth() {
    let response = app_with_auth(RuntimeAuth::bearer_token("secret"))
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"Ada"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
}

#[tokio::test]
async fn gets_agent_without_prompt_contents() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .uri("/v1/agents/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["code"], 0);
    assert_eq!(payload["message"], "ok");
    let agent = &payload["data"];
    assert_eq!(agent["id"], "test");
    assert_eq!(agent["input_schema"], prompt_input_schema());
    assert!(agent.get("steps").is_none());
    assert!(agent.get("prompts").is_none());
    assert!(!String::from_utf8_lossy(&body).contains("Hello {{ input.name }}"));
}

#[tokio::test]
async fn disabled_agent_is_not_visible_or_callable() {
    let app = app().await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/agents")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert!(!payload["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|agent| agent["id"] == "disabled"));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/agents/disabled")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/agents/disabled/runs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/disabled/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"Ada"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn streams_agent_run_as_sse() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"Ada"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let frames = parse_sse_frames(&text);

    assert!(frames.len() >= 4);
    assert_eq!(frames[0].event, "run_started");
    assert_eq!(frames[0].data["code"], 0);
    assert_eq!(frames[0].data["message"], "ok");
    assert_eq!(frames[0].data["data"]["event"], "run_started");
    assert_eq!(frames[0].data["data"]["agent_id"], "test");
    assert_eq!(frames[0].data["data"]["content"], "");
    assert!(frames[0].data["data"]["result"].is_null());

    let completed = frames
        .iter()
        .find(|frame| frame.event == "step_completed")
        .expect("expected step_completed frame");
    assert_eq!(completed.data["code"], 0);
    assert_eq!(completed.data["data"]["event"], "step_completed");
    assert_eq!(completed.data["data"]["step_id"], "hello");
    assert_eq!(completed.data["data"]["content"], "");
    assert!(completed.data["data"]["result"].is_null());

    let run_completed = frames
        .iter()
        .find(|frame| frame.event == "run_completed")
        .expect("expected run_completed frame");
    assert_eq!(run_completed.data["code"], 0);
    assert_eq!(run_completed.data["data"]["event"], "run_completed");
    assert_eq!(run_completed.data["data"]["content"], "");
    assert!(run_completed.data["data"]["result"].is_null());
}

#[tokio::test]
async fn streams_agent_run_with_request_id() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .header("x-request-id", "req_test_123")
                .body(Body::from(r#"{"name":"Ada"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(HeaderName::from_static("x-request-id"))
            .unwrap(),
        "req_test_123"
    );

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let frames = parse_sse_frames(&text);

    assert!(frames
        .iter()
        .all(|frame| frame.data["data"]["request_id"] == "req_test_123"));
}

#[tokio::test]
async fn streams_agent_run_generates_request_id() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"Ada"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let request_id = response
        .headers()
        .get(HeaderName::from_static("x-request-id"))
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(request_id.starts_with("req_"));

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let frames = parse_sse_frames(&text);

    assert!(frames
        .iter()
        .all(|frame| frame.data["data"]["request_id"] == request_id));
}

#[tokio::test]
async fn records_run_history_and_step_outputs() {
    let app = app().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .header("x-request-id", "req_history_001")
                .header("x-caller-service", "web-backend")
                .header("x-tenant-id", "tenant_123")
                .header("x-user-id", "user_456")
                .body(Body::from(r#"{"name":"Ada"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let frames = parse_sse_frames(&text);
    let run_id = frames[0].data["data"]["run_id"].as_str().unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/runs/{run_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["code"], 0);
    assert_eq!(payload["data"]["run_id"], run_id);
    assert_eq!(payload["data"]["request_id"], "req_history_001");
    assert_eq!(payload["data"]["agent_id"], "test");
    assert_eq!(payload["data"]["caller_service"], "web-backend");
    assert_eq!(payload["data"]["tenant_id"], "tenant_123");
    assert_eq!(payload["data"]["user_id"], "user_456");
    assert_eq!(payload["data"]["status"], "completed");
    assert_eq!(payload["data"]["input_summary"]["keys"], json!(["name"]));
    assert_eq!(
        payload["data"]["step_outputs"]["hello"]["text"],
        "Hello Ada"
    );
    assert!(payload["data"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["event"] == "step_completed" && event["step_id"] == "hello"));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/agents/test/runs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["code"], 0);
    assert_eq!(payload["data"][0]["run_id"], run_id);
    assert_eq!(payload["data"][0]["request_id"], "req_history_001");
    assert_eq!(payload["data"][0]["caller_service"], "web-backend");
    assert_eq!(payload["data"][0]["tenant_id"], "tenant_123");
    assert_eq!(payload["data"][0]["user_id"], "user_456");
    assert_eq!(payload["data"][0]["status"], "completed");
}

#[tokio::test]
async fn filters_run_history_by_request_context() {
    let app = app().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .header("x-request-id", "req_filter_001")
                .header("x-caller-service", "web-backend")
                .header("x-tenant-id", "tenant_123")
                .header("x-user-id", "user_a")
                .body(Body::from(r#"{"name":"Ada"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let first_frames = parse_sse_frames(&String::from_utf8(body.to_vec()).unwrap());
    let first_run_id = first_frames[0].data["data"]["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .header("x-request-id", "req_filter_002")
                .header("x-caller-service", "batch-worker")
                .header("x-tenant-id", "tenant_999")
                .header("x-user-id", "user_b")
                .body(Body::from(r#"{"name":"Grace"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let second_frames = parse_sse_frames(&String::from_utf8(body.to_vec()).unwrap());
    let second_run_id = second_frames[0].data["data"]["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/agents/test/runs?request_id=req_filter_001")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(payload["data"][0]["run_id"], first_run_id);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/agents/test/runs?user_id=user_b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(payload["data"][0]["run_id"], second_run_id);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/runs?tenant_id=tenant_123&caller_service=web-backend")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["data"].as_array().unwrap().len(), 1);
    assert_eq!(payload["data"][0]["run_id"], first_run_id);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/runs?request_id=missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert!(payload["data"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn streams_code_node_demo_agent() {
    let response = app_with_code_node_demo()
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/code_node_demo/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"text":"hello rust world"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let frames = parse_sse_frames(&text);
    let deltas = frames
        .iter()
        .filter(|frame| frame.event == "token_delta")
        .map(|frame| frame.data["data"]["content"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        deltas,
        vec![
            "Analyzing text metrics".to_string(),
            "\n\nText metrics:\n- characters: 16\n- words: 3\n- empty: false\n".to_string(),
        ]
    );
}

#[tokio::test]
async fn records_failed_run_history() {
    let app = app().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/broken/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let frames = parse_sse_frames(&text);
    let run_id = frames[0].data["data"]["run_id"].as_str().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/runs/{run_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["data"]["status"], "failed");
    assert_eq!(
        payload["data"]["error_message"],
        "run error: tool 'not_registered' is not registered"
    );
    assert!(payload["data"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["event"] == "error" && event["step_id"] == "missing_tool"));
}

#[tokio::test]
async fn streams_token_delta_content_as_direct_text() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/llm/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let frames = parse_sse_frames(&text);
    let deltas = frames
        .iter()
        .filter(|frame| frame.event == "token_delta")
        .map(|frame| frame.data["data"]["content"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();

    assert_eq!(deltas, vec!["Hel", "lo"]);
}

#[tokio::test]
async fn invalid_input_returns_400_before_sse() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#""not-object""#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_ne!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["code"], 10000);
    assert!(payload["message"]
        .as_str()
        .unwrap()
        .contains("input validation failed"));
    assert!(payload["data"].is_null());
}

#[tokio::test]
async fn wrapped_input_body_returns_400_before_sse() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"input":{"name":"Ada"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_ne!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["code"], 10000);
    assert!(payload["message"]
        .as_str()
        .unwrap()
        .contains("input validation failed"));
    assert!(payload["data"].is_null());
}

#[tokio::test]
async fn missing_content_type_returns_400_before_sse() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .body(Body::from(r#"{"name":"Ada"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_ne!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["code"], 10000);
    assert!(payload["message"]
        .as_str()
        .unwrap()
        .contains("request body must be application/json"));
    assert!(payload["data"].is_null());
}

#[tokio::test]
async fn streams_runtime_error_as_sse_error_without_run_completed() {
    let response = app()
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/broken/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let frames = parse_sse_frames(&text);

    assert_eq!(frames[0].event, "run_started");
    assert_eq!(frames[1].event, "step_started");

    let error_frame = frames
        .iter()
        .find(|frame| frame.event == "error")
        .expect("expected error frame");
    assert_eq!(error_frame.data["code"], 20000);
    assert_eq!(
        error_frame.data["message"],
        "run error: tool 'not_registered' is not registered"
    );
    assert_eq!(error_frame.data["data"]["event"], "error");
    assert_eq!(error_frame.data["data"]["agent_id"], "broken");
    assert_eq!(error_frame.data["data"]["step_id"], "missing_tool");
    assert_eq!(error_frame.data["data"]["content"], "");
    assert!(error_frame.data["data"]["result"].is_null());

    assert!(!frames.iter().any(|frame| frame.event == "run_completed"));
}

fn always_fail_encoding(_event: RunEvent) -> Result<axum::response::sse::Event, AppError> {
    Err(AppError::Run(
        "failed to encode sse event: synthetic failure".to_string(),
    ))
}

#[tokio::test]
async fn sanitizes_sse_encoding_failures_without_panicking() {
    let response = app_with_encoder(always_fail_encoding)
        .await
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/test/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"Ada"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let frames = parse_sse_frames(&text);

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].event, "error");
    assert_eq!(frames[0].data["code"], 20000);
    assert_eq!(frames[0].data["message"], "stream encoding failed");
    assert_eq!(frames[0].data["data"]["event"], "error");
    assert_eq!(frames[0].data["data"]["content"], "");
    assert!(frames[0].data["data"]["result"].is_null());
    assert!(!frames.iter().any(|frame| frame.event == "run_completed"));
}
