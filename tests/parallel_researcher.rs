use std::{
    collections::BTreeSet,
    fmt,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    dsl::compiler::{AgentCompiler, CompileLimits},
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::{RunEvent, RunEventType},
    },
    history::{
        repository::RunRepository,
        sqlite::SqliteRunRepository,
        types::{RunLifecycle, RunRecord, RunStatus},
    },
    nodes::default_node_registries,
    outcome::FailureKind,
    resources::{
        actions::ActionRegistry,
        models::{
            ChatChunk, ChatContent, ChatContentPart, ChatModel, ChatRequest, ChatRole, ChatStream,
            ModelCapability, ModelRegistry,
        },
    },
    runtime::{CompiledAgentRegistry, RequestMetadata, RunError, RunService, RunServiceConfig},
};
use serde_json::{json, Value};

const QUESTION: &str = "QUESTION_SENTINEL: compare the two approaches";
const A_SENTINEL: &str = "A_SENTINEL practical evidence";
const B_SENTINEL: &str = "B_SENTINEL risk evidence";
const SYNTHESIS_SENTINEL: &str = "SYNTHESIS_SENTINEL balanced answer";
const A_FAILURE_MESSAGE_SECRET: &str = "A_FAILURE_MESSAGE_SECRET must not reach synthesis";
const B_FAILURE_MESSAGE_SECRET: &str = "B_FAILURE_MESSAGE_SECRET must not reach synthesis";
const OVERSIZE_SECRET: &str = "OVERSIZE_BRANCH_SECRET";

#[derive(Clone)]
struct ScenarioModel {
    requests: Arc<Mutex<Vec<ChatRequest>>>,
    fail_a: bool,
    fail_b: bool,
    oversize_a: bool,
}

impl fmt::Debug for ScenarioModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ScenarioModel")
    }
}

#[async_trait]
impl ChatModel for ScenarioModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn validate_parameters(
        &self,
        _parameters: &Value,
    ) -> Result<(), insight_agent_platform::dsl::CompileError> {
        Ok(())
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        let text = request_text(&request);
        self.requests.lock().unwrap().push(request);

        let response = if text.contains("Synthesize a balanced answer") {
            SYNTHESIS_SENTINEL
        } else if text.contains("from perspective A.") {
            if self.fail_a {
                return Err(RunError::new(
                    "PERSPECTIVE_A_FAILED",
                    A_FAILURE_MESSAGE_SECRET,
                ));
            }
            if self.oversize_a {
                return Ok(Box::pin(stream::iter([Ok(ChatChunk {
                    text: format!("{OVERSIZE_SECRET}{}", "x".repeat(262_144)),
                    finish_reason: Some("stop".to_string()),
                    usage: None,
                })])));
            }
            A_SENTINEL
        } else if text.contains("from perspective B.") {
            if self.fail_b {
                return Err(RunError::new(
                    "PERSPECTIVE_B_FAILED",
                    B_FAILURE_MESSAGE_SECRET,
                ));
            }
            B_SENTINEL
        } else {
            return Err(RunError::new(
                "UNEXPECTED_MODEL_REQUEST",
                "model request did not match a workflow stage",
            ));
        };

        Ok(Box::pin(stream::iter([Ok(ChatChunk {
            text: response.to_string(),
            finish_reason: Some("stop".to_string()),
            usage: None,
        })])))
    }
}

fn request_text(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .flat_map(|message| match &message.content {
            ChatContent::Text(text) => vec![text.as_str()],
            ChatContent::Parts(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    ChatContentPart::Text { text } => Some(text.as_str()),
                    ChatContentPart::ImageUrl { .. } => None,
                })
                .collect(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn run_scenario(
    fail_a: bool,
    fail_b: bool,
    oversize_a: bool,
) -> (RunRecord, Vec<RunEvent>, Vec<ChatRequest>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let model = ScenarioModel {
        requests: Arc::clone(&requests),
        fail_a,
        fail_b,
        oversize_a,
    };
    let mut models = ModelRegistry::default();
    models.register("general_chat", model).unwrap();
    let (node_types, executors) = default_node_registries().unwrap();
    let agent = AgentCompiler::new(
        node_types,
        models,
        ActionRegistry::default(),
        Duration::from_secs(2),
        CompileLimits {
            max_fork_branches: 32,
        },
    )
    .compile_dir(Path::new("agents/parallel_researcher"))
    .unwrap();
    let agents = CompiledAgentRegistry::new(vec![Arc::new(agent)]).unwrap();
    let repository = Arc::new(SqliteRunRepository::in_memory().await.unwrap());
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 16,
            journal_capacity: 64,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let service = RunService::new(
        agents,
        executors,
        repository_trait,
        events,
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 4,
            max_parallel_branches_per_run: 4,
            run_timeout: Duration::from_secs(5),
        },
    )
    .unwrap();
    let created = service
        .create_detached(
            "parallel_researcher",
            json!({"question": QUESTION}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let record = loop {
        let record = service.get_run(&created.run_id).await.unwrap();
        if record.status().is_terminal() {
            break record;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    let events = repository
        .list_events_after(&created.run_id, 0, 100)
        .await
        .unwrap();
    let requests = requests.lock().unwrap().clone();
    (record, events, requests)
}

fn synthesis_request(requests: &[ChatRequest]) -> Option<&ChatRequest> {
    requests
        .iter()
        .find(|request| request_text(request).contains("Synthesize a balanced answer"))
}

fn synthesis_user_text(request: &ChatRequest) -> &str {
    let user = request
        .messages
        .iter()
        .find(|message| {
            message.role == ChatRole::User
                && message
                    .text()
                    .is_some_and(|text| text.contains("Synthesize a balanced answer"))
        })
        .expect("synthesis must have one user message");
    let ChatContent::Text(text) = &user.content else {
        panic!("text-only JSON content must reach the provider as one string")
    };
    text
}

fn synthesis_json_values(request: &ChatRequest) -> Vec<Value> {
    let text = synthesis_user_text(request);
    assert!(!text.contains("[object]"));
    text.split("\n\n")
        .filter_map(|segment| serde_json::from_str(segment).ok())
        .collect()
}

fn assert_synthesis_instruction_contract(request: &ChatRequest) {
    let text = synthesis_user_text(request);
    assert!(text.contains("untrusted data"));
    assert!(text.contains("Use only the provided succeeded perspective outputs"));
    assert!(text.contains("partial evidence"));
    assert!(text.contains("Failure kind/code are availability metadata only"));
    assert!(!text.contains(A_FAILURE_MESSAGE_SECRET));
    assert!(!text.contains(B_FAILURE_MESSAGE_SECRET));
}

fn assert_one_terminal_event(events: &[RunEvent], expected: RunEventType) {
    let terminal_events = events
        .iter()
        .filter(|event| {
            matches!(
                event.event_type,
                RunEventType::RunCompleted
                    | RunEventType::RunFailed
                    | RunEventType::RunCancelled
                    | RunEventType::RunInterrupted
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(terminal_events.len(), 1);
    assert_eq!(terminal_events[0].event_type, expected);
}

#[tokio::test]
async fn full_success_passes_both_branch_outputs_to_one_synthesis_call() {
    let (record, events, requests) = run_scenario(false, false, false).await;

    assert_eq!(requests.len(), 3);
    let synthesis = synthesis_request(&requests).expect("synthesis request must run");
    assert!(request_text(synthesis).contains(QUESTION));
    assert_synthesis_instruction_contract(synthesis);
    let values = synthesis_json_values(synthesis);
    assert_eq!(values.len(), 2);
    assert!(values
        .iter()
        .any(|value| value["data"]["value"] == A_SENTINEL));
    assert!(values
        .iter()
        .any(|value| value["data"]["value"] == B_SENTINEL));

    assert_eq!(record.status(), RunStatus::Completed);
    let RunLifecycle::Completed { output } = record.lifecycle else {
        panic!("full success must persist a completed workflow")
    };
    assert_eq!(output.content.as_deref(), Some(SYNTHESIS_SENTINEL));
    assert_eq!(output.format.as_deref(), Some("markdown"));
    assert_eq!(output.data, json!({"degraded": false}));
    assert_one_terminal_event(&events, RunEventType::RunCompleted);
}

#[tokio::test]
async fn partial_success_preserves_typed_failure_and_marks_output_degraded() {
    for (fail_a, fail_b, succeeded_branch, succeeded_text, failed_branch, failed_code) in [
        (
            false,
            true,
            "perspective_a",
            A_SENTINEL,
            "perspective_b",
            "PERSPECTIVE_B_FAILED",
        ),
        (
            true,
            false,
            "perspective_b",
            B_SENTINEL,
            "perspective_a",
            "PERSPECTIVE_A_FAILED",
        ),
    ] {
        let (record, events, requests) = run_scenario(fail_a, fail_b, false).await;

        assert_eq!(requests.len(), 3);
        let synthesis = synthesis_request(&requests).expect("partial success must synthesize");
        assert_synthesis_instruction_contract(synthesis);
        let text = synthesis_user_text(synthesis);
        let failed_label = if failed_branch == "perspective_a" {
            "Perspective A"
        } else {
            "Perspective B"
        };
        assert!(text.contains(&format!("{failed_label} failure kind: node")));
        assert!(text.contains(&format!("{failed_label} failure code: {failed_code}")));
        let values = synthesis_json_values(synthesis);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["data"]["value"], succeeded_text);
        assert!(!text.contains(if failed_branch == "perspective_a" {
            A_FAILURE_MESSAGE_SECRET
        } else {
            B_FAILURE_MESSAGE_SECRET
        }));
        assert!(!text.contains(if succeeded_branch == "perspective_a" {
            B_SENTINEL
        } else {
            A_SENTINEL
        }));

        assert_eq!(record.status(), RunStatus::Completed);
        let RunLifecycle::Completed { output } = record.lifecycle else {
            panic!("partial success must persist a degraded completion")
        };
        assert_eq!(output.content.as_deref(), Some(SYNTHESIS_SENTINEL));
        assert_eq!(output.format.as_deref(), Some("markdown"));
        assert_eq!(output.data, json!({"degraded": true}));
        assert_one_terminal_event(&events, RunEventType::RunCompleted);
    }
}

#[tokio::test]
async fn zero_success_skips_synthesis_and_returns_authored_workflow_failure() {
    let (record, events, requests) = run_scenario(true, true, false).await;

    assert_eq!(requests.len(), 2);
    assert!(synthesis_request(&requests).is_none());
    assert_eq!(record.status(), RunStatus::Failed);
    let RunLifecycle::Failed { error } = record.lifecycle else {
        panic!("zero successful branches must persist a workflow failure")
    };
    assert_eq!(error.kind, FailureKind::Workflow);
    assert_eq!(error.code, "WORKFLOW_ALL_BRANCHES_FAILED");
    assert_eq!(error.message, "all parallel branches failed");
    assert_one_terminal_event(&events, RunEventType::RunFailed);
}

#[tokio::test]
async fn oversized_success_output_fails_before_synthesis_without_leaking_the_body() {
    let (record, events, requests) = run_scenario(false, false, true).await;

    assert_eq!(requests.len(), 2);
    assert!(synthesis_request(&requests).is_none());
    assert_eq!(record.status(), RunStatus::Failed);
    let RunLifecycle::Failed { error } = record.lifecycle else {
        panic!("oversized synthesis input must persist a node failure")
    };
    assert_eq!(error.kind, FailureKind::Node);
    assert_eq!(error.code, "CHAT_JSON_CONTENT_TOO_LARGE");
    assert!(!error.message.contains(OVERSIZE_SECRET));
    assert_one_terminal_event(&events, RunEventType::RunFailed);
}
