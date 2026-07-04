use futures::StreamExt;
use serde_json::json;

use insight_agent_platform::{
    agent::{
        config::{AgentConfig, InputConfig, ModelConfig, StepConfig, StepKind},
        loader::LoadedAgent,
    },
    engine::{event::RunEventKind, runner::RunEngine},
    model::types::FakeModelClient,
    tools::registry::ToolRegistry,
};

#[tokio::test]
async fn prompt_step_renders_and_completes_run() {
    let agent = LoadedAgent {
        root: std::path::PathBuf::from("agents/test"),
        prompts: Default::default(),
        config: AgentConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: String::new(),
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

    let engine = RunEngine::new(FakeModelClient::new(vec![]), ToolRegistry::default());
    let events: Vec<_> = engine.run(agent, json!({"name":"Ada"})).collect().await;

    assert!(events
        .iter()
        .any(|event| event.kind == RunEventKind::RunStarted));
    assert!(events
        .iter()
        .any(|event| event.kind == RunEventKind::StepStarted));
    assert!(events
        .iter()
        .any(|event| event.kind == RunEventKind::StepCompleted));
    let completed = events
        .iter()
        .find(|event| event.kind == RunEventKind::RunCompleted)
        .unwrap();
    assert_eq!(completed.payload["output"], "Hello Ada");
}
