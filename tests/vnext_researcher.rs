use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    catalog::AgentCatalog,
    dsl::{vnext::compiler::WorkflowCompiler, CompileError},
    events::hub::{EventHub, EventHubConfig},
    history::{
        repository::RunRepository,
        sqlite::SqliteRunRepository,
        types::{RunLifecycle, RunRecord},
    },
    resources::{
        actions::{
            Action, ActionCapability, ActionContext, ActionDescriptor, ActionRegistry,
            CancellationClass, EffectClass, IdempotencyClass,
        },
        models::{
            ChatChunk, ChatContent, ChatContentPart, ChatModel, ChatRequest, ChatRole, ChatStream,
            ModelCapability, ModelRegistry,
        },
    },
    runtime::{RequestMetadata, RunError, RunService, RunServiceConfig},
};
use serde_json::{json, Value};

const QUESTION: &str = "Rust 中如何设计可靠的结构化并发？";
const PLAN_RESULT: &str = "PRIVATE_PLAN_SENTINEL";
const FINAL_RESULT: &str = "FINAL_ANSWER_SENTINEL";
const FIXED_TIME: &str = "2030-01-02T03:04:05+08:00";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordedCall {
    PlannerLlm,
    CurrentTimeAction,
    FinalLlm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedModelRequest {
    roles: Vec<ChatRole>,
    text: String,
}

#[derive(Debug, Default)]
struct ResearcherTracker {
    calls: Mutex<Vec<RecordedCall>>,
    model_requests: Mutex<Vec<CapturedModelRequest>>,
    action_inputs: Mutex<Vec<Value>>,
}

impl ResearcherTracker {
    fn record_model(&self, request: &ChatRequest) -> Result<usize, RunError> {
        let captured = capture_request(request);
        let mut requests = self.model_requests.lock().unwrap();
        let call_index = requests.len();
        let call = match call_index {
            0 => RecordedCall::PlannerLlm,
            1 => RecordedCall::FinalLlm,
            _ => {
                return Err(RunError::operation(
                    "TEST_RESEARCHER_MODEL_CALL_UNEXPECTED",
                    "researcher fake model received an unexpected call",
                ))
            }
        };
        requests.push(captured);
        self.calls.lock().unwrap().push(call);
        Ok(call_index)
    }

    fn record_action(&self, input: Value) {
        self.action_inputs.lock().unwrap().push(input);
        self.calls
            .lock()
            .unwrap()
            .push(RecordedCall::CurrentTimeAction);
    }

    fn calls(&self) -> Vec<RecordedCall> {
        self.calls.lock().unwrap().clone()
    }

    fn model_requests(&self) -> Vec<CapturedModelRequest> {
        self.model_requests.lock().unwrap().clone()
    }

    fn action_inputs(&self) -> Vec<Value> {
        self.action_inputs.lock().unwrap().clone()
    }
}

#[derive(Debug)]
struct RecordingResearchModel {
    tracker: Arc<ResearcherTracker>,
}

#[async_trait]
impl ChatModel for RecordingResearchModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn validate_parameters(&self, _parameters: &Value) -> Result<(), CompileError> {
        Ok(())
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        let call_index = self.tracker.record_model(&request)?;
        let response = match call_index {
            0 => PLAN_RESULT,
            1 => FINAL_RESULT,
            _ => unreachable!("record_model rejects unexpected calls"),
        };
        Ok(Box::pin(stream::iter([Ok(ChatChunk {
            text: response.to_string(),
            finish_reason: Some("stop".to_string()),
            usage: Some(json!({"total_tokens": 1})),
        })])))
    }
}

#[derive(Debug)]
struct RecordingCurrentTimeAction {
    tracker: Arc<ResearcherTracker>,
}

#[async_trait]
impl Action for RecordingCurrentTimeAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            id: "current_time",
            version: "1.0.0",
            input_schema: json!({
                "type": "object",
                "properties": {"timezone": {"type": "string"}},
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "required": ["timezone", "iso8601"],
                "properties": {
                    "timezone": {"type": "string"},
                    "iso8601": {"type": "string"}
                },
                "additionalProperties": false
            }),
            effect: EffectClass::ReadOnly,
            idempotency: IdempotencyClass::NonIdempotent,
            cancellation: CancellationClass::NotSupported,
            required_capabilities: BTreeSet::from([ActionCapability::new("clock")]),
        }
    }

    async fn call(&self, input: Value, _context: ActionContext) -> Result<Value, RunError> {
        self.tracker.record_action(input);
        Ok(json!({
            "timezone": "Asia/Shanghai",
            "iso8601": FIXED_TIME,
        }))
    }
}

#[tokio::test]
async fn researcher_executes_llm_action_llm_and_consumes_the_typed_action_output() {
    let tracker = Arc::new(ResearcherTracker::default());
    let mut models = ModelRegistry::default();
    models
        .register(
            "general_chat",
            RecordingResearchModel {
                tracker: Arc::clone(&tracker),
            },
        )
        .unwrap();
    let mut actions = ActionRegistry::default();
    actions
        .register(RecordingCurrentTimeAction {
            tracker: Arc::clone(&tracker),
        })
        .unwrap();

    let compiler = WorkflowCompiler::new(models, actions);
    let agent_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("agents/researcher");
    let workflow = Arc::new(compiler.compile_dir(&agent_dir).unwrap());
    let agents = AgentCatalog::new(vec![workflow]).unwrap();

    let repository = Arc::new(SqliteRunRepository::in_memory().await.unwrap());
    let repository_trait: Arc<dyn RunRepository> = repository;
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 32,
            journal_capacity: 64,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let service = RunService::new(
        agents,
        repository_trait,
        events,
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_concurrent_operations: 2,
            max_concurrent_operations_per_run: 2,
            operation_timeout: Duration::from_secs(2),
            operation_cancel_grace_period: Duration::from_millis(100),
            max_template_output_bytes: 64 * 1024,
            run_timeout: Duration::from_secs(3),
        },
    )
    .unwrap();

    let created = service
        .create_detached(
            "researcher",
            json!({"question": QUESTION}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let record = wait_for_terminal(&service, &created.run_id).await;
    service.shutdown(Duration::from_secs(1)).await.unwrap();

    let RunLifecycle::Completed { output } = &record.lifecycle else {
        panic!("expected researcher workflow to complete")
    };
    assert_eq!(output.content, None);
    assert_eq!(output.data, json!({"answer": FINAL_RESULT}));

    assert_eq!(
        tracker.calls(),
        vec![
            RecordedCall::PlannerLlm,
            RecordedCall::CurrentTimeAction,
            RecordedCall::FinalLlm,
        ],
        "the canonical workflow must execute LLM -> Action -> LLM"
    );

    let action_inputs = tracker.action_inputs();
    assert_eq!(action_inputs.len(), 1, "Action must execute exactly once");
    assert_eq!(action_inputs[0], json!({"timezone": "Asia/Shanghai"}));

    let requests = tracker.model_requests();
    assert_eq!(
        requests.len(),
        2,
        "model provider must be called exactly twice"
    );
    assert_eq!(requests[0].roles, vec![ChatRole::System, ChatRole::User]);
    assert_eq!(requests[1].roles, vec![ChatRole::System, ChatRole::User]);
    assert!(requests[0].text.contains(QUESTION));
    assert!(!requests[0].text.contains(PLAN_RESULT));
    assert!(!requests[0].text.contains(FIXED_TIME));

    assert!(requests[1].text.contains(QUESTION));
    assert!(
        requests[1].text.contains(PLAN_RESULT),
        "the final LLM must consume the first LLM's typed text output"
    );
    assert!(
        requests[1].text.contains(FIXED_TIME)
            && requests[1].text.contains("\"timezone\":\"Asia/Shanghai\""),
        "the final LLM must consume the typed current_time Action object"
    );
}

async fn wait_for_terminal(service: &RunService, run_id: &str) -> RunRecord {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = service.get_run(run_id).await.unwrap();
            if record.status().is_terminal() {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("researcher workflow did not reach a durable terminal")
}

fn capture_request(request: &ChatRequest) -> CapturedModelRequest {
    let mut text = Vec::new();
    for message in &request.messages {
        match &message.content {
            ChatContent::Text(value) => text.push(value.clone()),
            ChatContent::Parts(parts) => {
                for part in parts {
                    if let ChatContentPart::Text { text: value } = part {
                        text.push(value.clone());
                    }
                }
            }
        }
    }
    CapturedModelRequest {
        roles: request
            .messages
            .iter()
            .map(|message| message.role)
            .collect(),
        text: text.join("\n"),
    }
}
