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
    api::routes::{build_router, AppState},
    engine::runner::RunEngine,
    model::types::FakeModelClient,
    tools::registry::ToolRegistry,
};

fn app() -> axum::Router {
    let agent = LoadedAgent {
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
    };
    let registry = AgentRegistry::new(vec![agent]).unwrap();
    let engine = RunEngine::new(FakeModelClient::new(vec![]), ToolRegistry::default());
    build_router(AppState { registry, engine })
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
    assert_eq!(agents[0]["id"], "test");
    assert_eq!(agents[0]["name"], "Test");
    assert_eq!(agents[0]["description"], "Test agent");
    assert_eq!(agents[0]["input_schema"], json!({"type":"object"}));
    assert!(agents[0].get("steps").is_none());
    assert!(agents[0].get("prompts").is_none());
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
    assert!(text.contains("event: run_started"));
    assert!(text.contains("event: step_completed"));
    assert!(text.contains("event: run_completed"));
    assert!(text.contains("\"output\":\"Hello Ada\""));
}
