use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};
use serde_json::{json, Value};
use tokio::{sync::Notify, time::timeout};

use insight_agent_platform::{
    agent::{
        config::{AgentConfig, InputConfig, ModelConfig, StepConfig, StepKind},
        loader::LoadedAgent,
    },
    engine::{event::RunEventKind, runner::RunEngine},
    error::AppError,
    model::types::{ChatRequest, ChatStream, FakeModelClient, ModelClient},
    tools::{
        current_time::CurrentTimeTool,
        registry::{default_tool_registry, Tool, ToolContext, ToolRegistry},
    },
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

#[tokio::test]
async fn tool_step_emits_tool_events_and_stores_output() {
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
                id: "now".to_string(),
                kind: StepKind::Tool,
                prompt_ref: None,
                prompt: None,
                system_prompt_ref: None,
                system_prompt: None,
                stream: false,
                tool: Some("current_time".to_string()),
                args: json!({"timezone":"Asia/Shanghai"}),
            }],
        },
    };

    let mut tools = ToolRegistry::default();
    tools.register(CurrentTimeTool);
    let engine = RunEngine::new(FakeModelClient::new(vec![]), tools);
    let events: Vec<_> = engine.run(agent, json!({})).collect().await;

    assert!(events
        .iter()
        .any(|event| event.kind == RunEventKind::ToolCallStarted));
    assert!(events
        .iter()
        .any(|event| event.kind == RunEventKind::ToolCallCompleted));
    let completed = events
        .iter()
        .find(|event| event.kind == RunEventKind::RunCompleted)
        .unwrap();
    assert_eq!(completed.payload["output"]["timezone"], "Asia/Shanghai");
    assert!(completed.payload["output"]["iso8601"]
        .as_str()
        .unwrap()
        .contains('+'));
}

#[test]
fn default_tool_registry_registers_built_in_tools() {
    let registry = default_tool_registry();

    assert!(registry.get("current_time").is_some());
    assert!(registry.get("http_get").is_some());
}

#[derive(Clone, Copy)]
struct FailingTool;

#[async_trait]
impl Tool for FailingTool {
    fn name(&self) -> &'static str {
        "failing_tool"
    }

    async fn call(&self, _args: Value, _ctx: ToolContext) -> Result<Value, AppError> {
        Err(AppError::Run("tool failed deliberately".to_string()))
    }
}

#[tokio::test]
async fn tool_step_error_emits_error_event_and_stops_run() {
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
                id: "broken".to_string(),
                kind: StepKind::Tool,
                prompt_ref: None,
                prompt: None,
                system_prompt_ref: None,
                system_prompt: None,
                stream: false,
                tool: Some("failing_tool".to_string()),
                args: json!({}),
            }],
        },
    };

    let mut tools = ToolRegistry::default();
    tools.register(FailingTool);
    let engine = RunEngine::new(FakeModelClient::new(vec![]), tools);
    let events: Vec<_> = engine.run(agent, json!({})).collect().await;

    assert!(events
        .iter()
        .any(|event| event.kind == RunEventKind::ToolCallStarted));
    assert!(!events
        .iter()
        .any(|event| event.kind == RunEventKind::ToolCallCompleted));
    let error_event = events
        .iter()
        .find(|event| event.kind == RunEventKind::Error)
        .unwrap();
    assert_eq!(
        error_event.payload["message"],
        "run error: tool failed deliberately"
    );
    assert!(!events
        .iter()
        .any(|event| event.kind == RunEventKind::RunCompleted));
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

#[derive(Clone)]
struct RecordingModelClient {
    requests: Arc<std::sync::Mutex<Vec<ChatRequest>>>,
    calls: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CancellableBlockingModelClient {
    started: Arc<Notify>,
    release: Arc<Notify>,
    cancelled: Arc<Notify>,
    started_count: Arc<AtomicUsize>,
    cancelled_count: Arc<AtomicUsize>,
}

impl CancellableBlockingModelClient {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            cancelled: Arc::new(Notify::new()),
            started_count: Arc::new(AtomicUsize::new(0)),
            cancelled_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn wait_until_started(&self) {
        loop {
            if self.started_count.load(Ordering::SeqCst) > 0 {
                return;
            }
            self.started.notified().await;
        }
    }

    fn cancelled_count(&self) -> usize {
        self.cancelled_count.load(Ordering::SeqCst)
    }

    async fn wait_until_cancelled(&self) {
        loop {
            if self.cancelled_count() > 0 {
                return;
            }
            self.cancelled.notified().await;
        }
    }
}

struct CancelGuard {
    notify: Arc<Notify>,
    cancelled_count: Arc<AtomicUsize>,
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.cancelled_count.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

impl RecordingModelClient {
    fn new() -> Self {
        Self {
            requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn take_requests(&self) -> Vec<ChatRequest> {
        let mut requests = self.requests.lock().unwrap();
        std::mem::take(&mut *requests)
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ModelClient for RecordingModelClient {
    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, AppError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        Ok(Box::pin(stream::iter(vec![Ok(String::from("ok"))])))
    }
}

#[async_trait]
impl ModelClient for CancellableBlockingModelClient {
    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, AppError> {
        self.started_count.fetch_add(1, Ordering::SeqCst);
        self.started.notify_waiters();
        let _guard = CancelGuard {
            notify: self.cancelled.clone(),
            cancelled_count: self.cancelled_count.clone(),
        };
        self.release.notified().await;
        Ok(Box::pin(stream::iter(vec![Ok(String::from("ok"))])))
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

#[tokio::test]
async fn llm_step_passes_empty_model_when_agent_model_is_absent() {
    let agent = LoadedAgent {
        root: std::path::PathBuf::from("agents/test"),
        prompts: Default::default(),
        config: AgentConfig {
            id: "test".to_string(),
            name: "Test".to_string(),
            description: String::new(),
            model: ModelConfig {
                provider: "openai_compatible".to_string(),
                model: None,
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
                prompt: Some("Hello {{ input.name }}".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                stream: true,
                tool: None,
                args: serde_json::Value::Null,
            }],
        },
    };

    let model = RecordingModelClient::new();
    let engine = RunEngine::new(model.clone(), ToolRegistry::default());
    let events: Vec<_> = engine.run(agent, json!({"name":"Ada"})).collect().await;

    assert!(events
        .iter()
        .any(|event| event.kind == RunEventKind::RunCompleted));
    let requests = model.take_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, "");
}

#[tokio::test]
async fn llm_step_streams_token_delta_events_and_final_output() {
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
                temperature: Some(0.2),
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
                prompt: Some("Answer {{ input.question }}".to_string()),
                system_prompt_ref: None,
                system_prompt: Some("You are concise.".to_string()),
                stream: true,
                tool: None,
                args: serde_json::Value::Null,
            }],
        },
    };

    let engine = RunEngine::new(
        FakeModelClient::new(vec!["Hel", "lo"]),
        ToolRegistry::default(),
    );
    let events: Vec<_> = engine.run(agent, json!({"question":"Q"})).collect().await;

    let deltas: Vec<_> = events
        .iter()
        .filter(|event| event.kind == RunEventKind::TokenDelta)
        .map(|event| event.payload["delta"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(deltas, vec!["Hel", "lo"]);

    let completed = events
        .iter()
        .find(|event| event.kind == RunEventKind::RunCompleted)
        .unwrap();
    assert_eq!(completed.payload["output"], "Hello");
}

#[tokio::test]
async fn dropping_stream_stops_run_before_llm_work_starts() {
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
                id: "answer".to_string(),
                kind: StepKind::Llm,
                prompt_ref: None,
                prompt: Some("Hello {{ input.name }}".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                stream: true,
                tool: None,
                args: serde_json::Value::Null,
            }],
        },
    };

    let model = RecordingModelClient::new();
    let engine = RunEngine::new(model.clone(), ToolRegistry::default());
    let events = engine.run(agent, json!({"name":"Ada"}));
    drop(events);

    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(model.call_count(), 0);
}

#[tokio::test]
async fn dropping_stream_cancels_in_flight_model_request() {
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
                id: "answer".to_string(),
                kind: StepKind::Llm,
                prompt_ref: None,
                prompt: Some("Hello {{ input.name }}".to_string()),
                system_prompt_ref: None,
                system_prompt: None,
                stream: true,
                tool: None,
                args: serde_json::Value::Null,
            }],
        },
    };

    let model = CancellableBlockingModelClient::new();
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
    let step_started = timeout(Duration::from_millis(100), events.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(step_started.kind, RunEventKind::StepStarted);

    timeout(Duration::from_millis(100), model.wait_until_started())
        .await
        .unwrap();

    drop(events);

    timeout(Duration::from_millis(100), model.wait_until_cancelled())
        .await
        .unwrap();
    assert_eq!(model.cancelled_count(), 1);
}
