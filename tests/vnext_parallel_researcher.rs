use std::{
    collections::BTreeSet,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use futures::{stream, Stream};
use insight_agent_platform::{
    catalog::AgentCatalog,
    dsl::{vnext::compiler::WorkflowCompiler, CompileError},
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::{RunEvent, RunEventType},
    },
    history::{
        repository::RunRepository,
        sqlite::SqliteRunRepository,
        types::{NewRun, RunAttachment, RunLifecycle, RunRecord},
    },
    outcome::TerminalOutcome,
    resources::{
        actions::ActionRegistry,
        models::{
            ChatChunk, ChatContent, ChatContentPart, ChatModel, ChatRequest, ChatStream,
            ModelCapability, ModelRegistry,
        },
    },
    runtime::{
        scope_scheduler::{ScopeScheduler, ScopeSchedulerConfig},
        stop_pair, RequestMetadata, RunError, RunExecutionResult, RunMetadata, RunService,
        RunServiceConfig, StopController, StopReason, StopSignal,
    },
};
use serde_json::json;
use tokio::sync::{Notify, Semaphore};

static RUN_SEQUENCE: AtomicUsize = AtomicUsize::new(1);

#[derive(Debug, Clone, Copy)]
enum ResponseMode {
    Success(&'static str),
    Fail(&'static str),
    Pending,
}

#[derive(Debug, Clone, Copy)]
struct Scenario {
    technical: ResponseMode,
    risk: ResponseMode,
    synthesis: ResponseMode,
}

#[derive(Debug, Default)]
struct ModelTracker {
    started: AtomicUsize,
    active_streams: AtomicUsize,
    pending_dropped: AtomicUsize,
    changed: Notify,
    requests: Mutex<Vec<String>>,
}

impl ModelTracker {
    async fn wait_started(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.started.load(Ordering::SeqCst) < expected {
                self.changed.notified().await;
            }
        })
        .await
        .expect("expected model calls were not admitted");
    }

    fn record(&self, request: &ChatRequest) -> String {
        let rendered = render_request(request);
        self.requests.lock().unwrap().push(rendered.clone());
        self.started.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
        rendered
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    fn stream_started(&self) {
        self.active_streams.fetch_add(1, Ordering::SeqCst);
    }

    fn stream_finished(&self) {
        let previous = self.active_streams.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0, "model stream accounting underflowed");
        self.changed.notify_waiters();
    }
}

#[derive(Debug)]
struct ScenarioModel {
    scenario: Scenario,
    tracker: Arc<ModelTracker>,
}

#[async_trait]
impl ChatModel for ScenarioModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn validate_parameters(&self, _parameters: &serde_json::Value) -> Result<(), CompileError> {
        Ok(())
    }

    async fn stream_chat(&self, request: ChatRequest) -> Result<ChatStream, RunError> {
        let rendered = self.tracker.record(&request);
        let is_synthesis = rendered.contains("supplied successful perspective values");
        let mode = if rendered.contains("Act as the technical-feasibility analyst") {
            self.scenario.technical
        } else if rendered.contains("Act as the risk-and-compliance analyst") {
            self.scenario.risk
        } else if is_synthesis {
            self.scenario.synthesis
        } else {
            return Err(RunError::operation(
                "TEST_REQUEST_UNCLASSIFIED",
                "test model could not classify the authored prompt",
            ));
        };

        match mode {
            ResponseMode::Success(text) => {
                self.tracker.stream_started();
                let text = if is_synthesis {
                    json!({
                        "content": text,
                        "degraded": rendered.contains("<failure>"),
                    })
                    .to_string()
                } else {
                    text.to_string()
                };
                Ok(Box::pin(SuccessfulChatStream {
                    tracker: Arc::clone(&self.tracker),
                    inner: stream::iter([Ok(ChatChunk {
                        text,
                        finish_reason: Some("stop".to_string()),
                        usage: Some(json!({"total_tokens": 1})),
                    })]),
                }))
            }
            ResponseMode::Fail(code) => Err(RunError::operation(
                code,
                "PRIVATE_MODEL_DIAGNOSTIC_MUST_NOT_REACH_SYNTHESIS",
            )),
            ResponseMode::Pending => {
                self.tracker.stream_started();
                Ok(Box::pin(PendingChatStream {
                    tracker: Arc::clone(&self.tracker),
                }))
            }
        }
    }
}

struct SuccessfulChatStream {
    tracker: Arc<ModelTracker>,
    inner: stream::Iter<std::array::IntoIter<Result<ChatChunk, RunError>, 1>>,
}

impl Stream for SuccessfulChatStream {
    type Item = Result<ChatChunk, RunError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(context)
    }
}

impl Drop for SuccessfulChatStream {
    fn drop(&mut self) {
        self.tracker.stream_finished();
    }
}

struct PendingChatStream {
    tracker: Arc<ModelTracker>,
}

impl Stream for PendingChatStream {
    type Item = Result<ChatChunk, RunError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Drop for PendingChatStream {
    fn drop(&mut self) {
        self.tracker.pending_dropped.fetch_add(1, Ordering::SeqCst);
        self.tracker.stream_finished();
    }
}

struct RuntimeFixture {
    scheduler: ScopeScheduler,
    metadata: RunMetadata,
    controller: StopController,
    stop: StopSignal,
    tracker: Arc<ModelTracker>,
}

fn compiled_workflow(
    scenario: Scenario,
    tracker: Arc<ModelTracker>,
) -> Arc<insight_agent_platform::dsl::vnext::compiler::CompiledWorkflow> {
    let mut models = ModelRegistry::default();
    models
        .register("general_chat", ScenarioModel { scenario, tracker })
        .unwrap();
    let compiler = WorkflowCompiler::new(models, ActionRegistry::default());
    let agent_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("agents/parallel_researcher");
    Arc::new(compiler.compile_dir(&agent_dir).unwrap())
}

async fn fixture(scenario: Scenario) -> RuntimeFixture {
    let tracker = Arc::new(ModelTracker::default());
    let workflow = compiled_workflow(scenario, Arc::clone(&tracker));

    let sequence = RUN_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let run_id = format!("parallel-researcher-{sequence}");
    let metadata = RunMetadata {
        run_id: run_id.clone(),
        request_id: format!("request-{sequence}"),
        agent_id: workflow.ir.metadata.id.as_str().to_string(),
        agent_version: workflow.version_hash.clone(),
        started_at: Utc::now(),
        execution_deadline: tokio::time::Instant::now() + Duration::from_secs(3),
    };
    let repository = Arc::new(SqliteRunRepository::in_memory().await.unwrap());
    repository
        .create_run(NewRun {
            run_id,
            request_id: metadata.request_id.clone(),
            agent_id: metadata.agent_id.clone(),
            agent_version: metadata.agent_version.clone(),
            attachment: RunAttachment::Attached,
            created_at: metadata.started_at,
            input_summary: json!({"keys": ["question"]}),
        })
        .await
        .unwrap();
    let events = EventHub::new(
        repository,
        EventHubConfig {
            subscriber_capacity: 32,
            journal_capacity: 64,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let scheduler = ScopeScheduler::new(
        workflow,
        Arc::new(Semaphore::new(4)),
        events,
        ScopeSchedulerConfig {
            max_concurrent_operations_per_run: 4,
            operation_timeout: Duration::from_secs(2),
            operation_cancel_grace_period: Duration::from_millis(100),
            max_template_output_bytes: 64 * 1024,
        },
    );
    let (controller, stop) = stop_pair();

    RuntimeFixture {
        scheduler,
        metadata,
        controller,
        stop,
        tracker,
    }
}

struct ServiceFixture {
    service: RunService,
    repository: Arc<SqliteRunRepository>,
    tracker: Arc<ModelTracker>,
}

async fn service_fixture(scenario: Scenario) -> ServiceFixture {
    let tracker = Arc::new(ModelTracker::default());
    let workflow = compiled_workflow(scenario, Arc::clone(&tracker));
    let repository = Arc::new(SqliteRunRepository::in_memory().await.unwrap());
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 32,
            journal_capacity: 64,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let agents = AgentCatalog::new(vec![workflow]).unwrap();
    let service = RunService::new(
        agents,
        repository_trait,
        events,
        RunServiceConfig {
            max_concurrent_runs: 2,
            max_concurrent_operations: 4,
            max_concurrent_operations_per_run: 4,
            operation_timeout: Duration::from_secs(2),
            operation_cancel_grace_period: Duration::from_millis(100),
            max_template_output_bytes: 64 * 1024,
            run_timeout: Duration::from_secs(3),
        },
    )
    .unwrap();
    ServiceFixture {
        service,
        repository,
        tracker,
    }
}

async fn run_through_service(scenario: Scenario) -> (RunRecord, Vec<RunEvent>, Arc<ModelTracker>) {
    let fixture = service_fixture(scenario).await;
    let created = fixture
        .service
        .create_detached(
            "parallel_researcher",
            json!({"question": "Should we ship this architecture?"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();
    let record = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = fixture.service.get_run(&created.run_id).await.unwrap();
            if record.status().is_terminal() {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("service-backed parallel run did not reach a durable terminal");
    let events = fixture
        .repository
        .list_events_after(&record.run_id, 0, 100)
        .await
        .unwrap();
    assert_eq!(
        fixture.tracker.active_streams.load(Ordering::SeqCst),
        0,
        "durable terminal must not leave a model child alive"
    );
    fixture
        .service
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
    (record, events, fixture.tracker)
}

async fn run(scenario: Scenario) -> (RunExecutionResult, Arc<ModelTracker>) {
    let fixture = fixture(scenario).await;
    let result = fixture
        .scheduler
        .run(
            fixture.metadata,
            json!({"question": "Should we ship this architecture?"}),
            fixture.stop,
        )
        .await
        .unwrap();
    (result, fixture.tracker)
}

#[tokio::test]
async fn full_success_uses_distinct_perspectives_and_complete_synthesis() {
    let (result, tracker) = run(Scenario {
        technical: ResponseMode::Success("technical evidence"),
        risk: ResponseMode::Success("risk evidence"),
        synthesis: ResponseMode::Success("balanced synthesis"),
    })
    .await;

    let RunExecutionResult::Ended(TerminalOutcome::Success { output }) = result else {
        panic!("expected successful workflow return")
    };
    assert_eq!(output.content.as_deref(), Some("balanced synthesis"));
    assert_eq!(output.data, json!({"degraded": false}));
    let requests = tracker.requests();
    assert_eq!(requests.len(), 3);
    assert!(requests
        .iter()
        .any(|request| request.contains("technical-feasibility analyst")));
    assert!(requests
        .iter()
        .any(|request| request.contains("risk-and-compliance analyst")));
    let synthesis = synthesis_request(&requests);
    assert!(synthesis.contains("technical evidence"));
    assert!(synthesis.contains("risk evidence"));
    assert!(!synthesis.contains("<failure>"));
    assert!(!synthesis.contains("\"status\":\"ok\""));
}

#[tokio::test]
async fn each_partial_success_uses_only_success_values_and_closed_failure_metadata() {
    for (scenario, successful_value, failed_branch, failure_code) in [
        (
            Scenario {
                technical: ResponseMode::Fail("PRIVATE_TECHNICAL_FAILURE"),
                risk: ResponseMode::Success("risk-only evidence"),
                synthesis: ResponseMode::Success("risk-only synthesis"),
            },
            "risk-only evidence",
            "technical",
            "PRIVATE_TECHNICAL_FAILURE",
        ),
        (
            Scenario {
                technical: ResponseMode::Success("technical-only evidence"),
                risk: ResponseMode::Fail("PRIVATE_RISK_FAILURE"),
                synthesis: ResponseMode::Success("technical-only synthesis"),
            },
            "technical-only evidence",
            "risk",
            "PRIVATE_RISK_FAILURE",
        ),
    ] {
        let (result, tracker) = run(scenario).await;
        let RunExecutionResult::Ended(TerminalOutcome::Success { output }) = result else {
            panic!("expected degraded workflow return")
        };
        assert_eq!(output.data, json!({"degraded": true}));
        let requests = tracker.requests();
        assert_eq!(requests.len(), 3);
        let synthesis = synthesis_request(&requests);
        assert!(synthesis.contains(successful_value));
        assert!(synthesis.contains("<failure>"));
        assert!(synthesis.contains(&format!("branch: {failed_branch}")));
        assert!(synthesis.contains(&format!("code: {failure_code}")));
        assert!(synthesis.contains("category: operation"));
        assert!(synthesis.contains("retryable: false"));
        assert!(synthesis.contains("origin:"));
        assert!(!synthesis.contains("PRIVATE_MODEL_DIAGNOSTIC"));
        assert!(!synthesis.contains("\"status\":\"error\""));
        assert!(!synthesis.contains("\"status\":\"ok\""));
    }
}

#[tokio::test]
async fn production_service_commits_one_private_terminal_for_full_and_partial_success() {
    let scenarios = [
        (
            Scenario {
                technical: ResponseMode::Success("FULL_TECHNICAL_INTERMEDIATE"),
                risk: ResponseMode::Success("FULL_RISK_INTERMEDIATE"),
                synthesis: ResponseMode::Success("service full synthesis"),
            },
            false,
            "service full synthesis",
            ["FULL_TECHNICAL_INTERMEDIATE", "FULL_RISK_INTERMEDIATE"],
        ),
        (
            Scenario {
                technical: ResponseMode::Fail("PRIVATE_TECHNICAL_FAILURE"),
                risk: ResponseMode::Success("PARTIAL_RISK_INTERMEDIATE"),
                synthesis: ResponseMode::Success("service partial synthesis"),
            },
            true,
            "service partial synthesis",
            [
                "PRIVATE_MODEL_DIAGNOSTIC_MUST_NOT_REACH_SYNTHESIS",
                "PARTIAL_RISK_INTERMEDIATE",
            ],
        ),
    ];

    for (scenario, degraded, expected_content, forbidden_intermediates) in scenarios {
        let (record, events, tracker) = run_through_service(scenario).await;
        let RunLifecycle::Completed { output } = &record.lifecycle else {
            panic!("expected completed service-backed run")
        };
        assert_eq!(output.content.as_deref(), Some(expected_content));
        assert_eq!(output.data, json!({"degraded": degraded}));
        assert_eq!(tracker.requests().len(), 3);
        assert_durable_parallel_contract(&record, &events, &forbidden_intermediates);
    }
}

#[tokio::test]
async fn zero_success_raises_declared_workflow_error_without_synthesis() {
    let (result, tracker) = run(Scenario {
        technical: ResponseMode::Fail("PRIVATE_TECHNICAL_FAILURE"),
        risk: ResponseMode::Fail("PRIVATE_RISK_FAILURE"),
        synthesis: ResponseMode::Success("must not execute"),
    })
    .await;

    let RunExecutionResult::Ended(TerminalOutcome::Failure { error }) = result else {
        panic!("expected declared workflow failure")
    };
    assert_eq!(error.code, "WORKFLOW_ALL_BRANCHES_FAILED");
    assert_eq!(error.message, "No analysis perspective was available.");
    assert_eq!(tracker.requests().len(), 2);
}

#[tokio::test]
async fn llm_response_contract_rejects_empty_synthesis() {
    let (result, _) = run(Scenario {
        technical: ResponseMode::Success("technical evidence"),
        risk: ResponseMode::Success("risk evidence"),
        synthesis: ResponseMode::Success(""),
    })
    .await;

    let RunExecutionResult::Failed(error) = result else {
        panic!("expected LLM response contract failure")
    };
    assert_eq!(error.code(), "VNEXT_LLM_RESPONSE_CONTRACT_INVALID");
}

#[tokio::test]
async fn cancellation_stops_and_drains_both_parallel_model_streams() {
    let fixture = fixture(Scenario {
        technical: ResponseMode::Pending,
        risk: ResponseMode::Pending,
        synthesis: ResponseMode::Success("must not execute"),
    })
    .await;
    let tracker = Arc::clone(&fixture.tracker);
    let control = async {
        tracker.wait_started(2).await;
        assert!(fixture.controller.request(StopReason::Cancelled));
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            fixture.scheduler.run(
                fixture.metadata,
                json!({"question": "Should we cancel?"}),
                fixture.stop,
            ),
            control,
        )
    })
    .await
    .expect("cancelled workflow must drain promptly");

    let RunExecutionResult::Stopped(error) = result.unwrap() else {
        panic!("expected external cancellation")
    };
    assert_eq!(error.code(), "RUN_CANCELLED");
    assert_eq!(tracker.pending_dropped.load(Ordering::SeqCst), 2);
    assert_eq!(tracker.requests().len(), 2);
}

fn synthesis_request(requests: &[String]) -> &str {
    requests
        .iter()
        .find(|request| request.contains("supplied successful perspective values"))
        .map(String::as_str)
        .expect("synthesis request was not observed")
}

fn assert_durable_parallel_contract(
    record: &RunRecord,
    events: &[RunEvent],
    forbidden_intermediates: &[&str],
) {
    let terminal_count = events
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
        .count();
    assert_eq!(terminal_count, 1, "each Run owns one durable terminal");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == RunEventType::RunCompleted)
            .count(),
        1
    );

    for event in events.iter().filter(|event| {
        matches!(
            event.event_type,
            RunEventType::OperationStarted
                | RunEventType::OperationCompleted
                | RunEventType::OperationFailed
        )
    }) {
        let data = event.data.as_object().unwrap();
        assert!(!data.contains_key("content"));
        assert!(!data.contains_key("output"));
        assert!(!data.contains_key("value"));
    }

    let durable = format!(
        "{}{}",
        serde_json::to_string(record).unwrap(),
        serde_json::to_string(events).unwrap()
    );
    assert!(!durable.contains("operation.content_delta"));
    for intermediate in forbidden_intermediates {
        assert!(
            !durable.contains(intermediate),
            "intermediate operation value entered durable history"
        );
    }
}

fn render_request(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .flat_map(|message| match &message.content {
            ChatContent::Text(text) => vec![text.as_str()],
            ChatContent::Parts(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    ChatContentPart::Text { text } => Some(text.as_str()),
                    ChatContentPart::Image { .. } => None,
                })
                .collect(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}
