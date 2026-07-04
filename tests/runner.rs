use std::{pin::Pin, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};
use serde_json::json;
use tokio::{sync::Notify, time::timeout};

use insight_agent_platform::{
    agent::{
        config::{AgentConfig, InputConfig, ModelConfig, StepConfig, StepKind},
        loader::LoadedAgent,
    },
    engine::{event::RunEventKind, runner::RunEngine},
    error::AppError,
    model::types::{ChatRequest, ChatStream, FakeModelClient, ModelClient},
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

#[derive(Clone)]
struct BlockingModelClient {
    ready: Arc<Notify>,
    chunks: Vec<String>,
}

impl BlockingModelClient {
    fn new(chunks: Vec<&str>) -> Self {
        Self {
            ready: Arc::new(Notify::new()),
            chunks: chunks.into_iter().map(str::to_string).collect(),
        }
    }

    fn release(&self) {
        self.ready.notify_waiters();
    }
}

#[async_trait]
impl ModelClient for BlockingModelClient {
    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, AppError> {
        self.ready.notified().await;
        let chunks = self.chunks.clone();
        let stream: Pin<Box<dyn Stream<Item = Result<String, AppError>> + Send>> =
            Box::pin(stream::iter(chunks.into_iter().map(Ok)));
        Ok(stream)
    }
}

#[tokio::test]
async fn run_stream_yields_early_events_before_blocked_llm_finishes() {
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
            steps: vec![
                StepConfig {
                    id: "hello".to_string(),
                    kind: StepKind::Prompt,
                    prompt_ref: None,
                    prompt: Some("Hello {{ input.name }}".to_string()),
                    system_prompt_ref: None,
                    system_prompt: None,
                    stream: false,
                    tool: None,
                    args: serde_json::Value::Null,
                },
                StepConfig {
                    id: "answer".to_string(),
                    kind: StepKind::Llm,
                    prompt_ref: None,
                    prompt: Some("Respond to {{ steps.hello.output }}".to_string()),
                    system_prompt_ref: None,
                    system_prompt: None,
                    stream: true,
                    tool: None,
                    args: serde_json::Value::Null,
                },
            ],
        },
    };

    let model = BlockingModelClient::new(vec!["chunk-1", "chunk-2"]);
    let engine = RunEngine::new(model.clone(), ToolRegistry::default());
    let mut events = Box::pin(engine.run(agent, json!({"name":"Ada"})));

    assert_eq!(
        timeout(Duration::from_millis(100), events.next())
            .await
            .unwrap()
            .unwrap()
            .kind,
        RunEventKind::RunStarted
    );
    assert_eq!(
        timeout(Duration::from_millis(100), events.next())
            .await
            .unwrap()
            .unwrap()
            .kind,
        RunEventKind::StepStarted
    );
    assert_eq!(
        timeout(Duration::from_millis(100), events.next())
            .await
            .unwrap()
            .unwrap()
            .kind,
        RunEventKind::StepCompleted
    );
    let llm_started = timeout(Duration::from_millis(100), events.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(llm_started.kind, RunEventKind::StepStarted);
    assert_eq!(llm_started.step_id.as_deref(), Some("answer"));

    assert!(timeout(Duration::from_millis(50), events.next())
        .await
        .is_err());

    model.release();

    let token = timeout(Duration::from_millis(100), events.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(token.kind, RunEventKind::TokenDelta);
    assert_eq!(token.payload["delta"], "chunk-1");

    let rest: Vec<_> = events.collect().await;
    assert!(rest
        .iter()
        .any(|event| event.kind == RunEventKind::RunCompleted));
}
