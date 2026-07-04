use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
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
        routes::{build_router, AppState},
        sse::encode_event,
    },
    engine::event::RunEvent,
    engine::runner::RunEngine,
    error::AppError,
    model::types::FakeModelClient,
    tools::registry::ToolRegistry,
};

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
                id: "hello".to_string(),
                kind: StepKind::Prompt,
                prompt_ref: None,
                prompt: Some("Hello {{ input.name }}".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                stream: false,
                tool: None,
                args: serde_json::Value::Null,
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
                stream: false,
                tool: Some("not_registered".to_string()),
                args: json!({}),
            }],
        },
    }
}

fn app() -> axum::Router {
    app_with_encoder(encode_event)
}

fn app_with_encoder(
    event_encoder: fn(RunEvent) -> Result<axum::response::sse::Event, AppError>,
) -> axum::Router {
    let registry = AgentRegistry::new(vec![prompt_agent(), failing_tool_agent()]).unwrap();
    let engine = RunEngine::new(FakeModelClient::new(vec![]), ToolRegistry::default());
    build_router(AppState {
        registry,
        engine,
        event_encoder,
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
    let agents: Value = serde_json::from_slice(&body).unwrap();
    let test_agent = agents
        .as_array()
        .unwrap()
        .iter()
        .find(|agent| agent["id"] == "test")
        .expect("expected test agent in list");
    assert_eq!(test_agent["name"], "Test");
    assert_eq!(test_agent["description"], "Test agent");
    assert_eq!(test_agent["input_schema"], json!({"type":"object"}));
    assert!(test_agent.get("steps").is_none());
    assert!(test_agent.get("prompts").is_none());
    assert!(!String::from_utf8_lossy(&body).contains("Hello {{ input.name }}"));
}

#[tokio::test]
async fn gets_agent_without_prompt_contents() {
    let response = app()
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
    let agent: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(agent["id"], "test");
    assert_eq!(agent["input_schema"], json!({"type":"object"}));
    assert!(agent.get("steps").is_none());
    assert!(agent.get("prompts").is_none());
    assert!(!String::from_utf8_lossy(&body).contains("Hello {{ input.name }}"));
}

#[tokio::test]
async fn streams_agent_run_as_sse() {
    let response = app()
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
    assert_eq!(frames[0].data["kind"], "run_started");
    assert_eq!(frames[0].data["agent_id"], "test");

    let completed = frames
        .iter()
        .find(|frame| frame.event == "step_completed")
        .expect("expected step_completed frame");
    assert_eq!(completed.data["kind"], "step_completed");
    assert_eq!(completed.data["step_id"], "hello");
    assert_eq!(completed.data["payload"]["output"], "Hello Ada");

    let run_completed = frames
        .iter()
        .find(|frame| frame.event == "run_completed")
        .expect("expected run_completed frame");
    assert_eq!(run_completed.data["kind"], "run_completed");
    assert_eq!(run_completed.data["payload"]["output"], "Hello Ada");
}

#[tokio::test]
async fn streams_runtime_error_as_sse_error_without_run_completed() {
    let response = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/agents/broken/runs/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"input":{}}"#))
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
    assert_eq!(error_frame.data["kind"], "error");
    assert_eq!(error_frame.data["agent_id"], "broken");
    assert_eq!(error_frame.data["step_id"], "missing_tool");
    assert_eq!(
        error_frame.data["payload"]["message"],
        "run error: tool 'not_registered' is not registered"
    );

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
    assert_eq!(frames[0].data["kind"], "error");
    assert_eq!(
        frames[0].data["payload"]["message"],
        "stream encoding failed"
    );
    assert!(!frames.iter().any(|frame| frame.event == "run_completed"));
}
