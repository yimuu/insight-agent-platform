use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use handlebars::Handlebars;
use insight_agent_platform::{
    dsl::{
        compiled::{
            CompiledAgent, CompiledNode, ExecutionPlan, NodeCompilation, NodeControl, NodeOutcome,
            NodeTransition, RunOutput,
        },
        compiler::CompileContext,
        CompileError, EmitPolicy,
    },
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::{RunEvent, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{NewRun, NodeOutputRecord, RunAttachment, RunRecord, RunStatus, TerminalUpdate},
    },
    nodes::registry::{NodeExecutor, NodeExecutorRegistry, NodeType},
    runtime::{
        stop_pair, ExecutionControl, ExecutionLimiter, RunContext, RunCoordinator, RunError,
        RunState, StopReason,
    },
};
use jsonschema::JSONSchema;
use serde_json::{json, Value};
use tokio::sync::{Mutex, Notify, Semaphore};

const RUN_ID: &str = "run_coordinator";

enum Behavior {
    Next {
        output: Value,
        require_output: Option<(String, Value)>,
    },
    Goto {
        target: String,
        output: Value,
    },
    Complete(RunOutput),
    Fail(RunError),
    Delay(Duration),
    WaitForStop(Arc<Notify>),
}

struct SyntheticNode;

impl NodeType for SyntheticNode {
    fn kind(&self) -> &'static str {
        "test.synthetic"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        Err(CompileError::new(
            "TEST_ONLY",
            "synthetic nodes are constructed directly",
        ))
    }
}

#[async_trait]
impl NodeExecutor for SyntheticNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        match node.body::<Behavior>()? {
            Behavior::Next {
                output,
                require_output,
            } => {
                if let Some((required_node, expected)) = require_output {
                    if context.node_output(required_node) != Some(expected) {
                        return Err(RunError::new(
                            "PREDECESSOR_NOT_VISIBLE",
                            "predecessor output was not visible",
                        ));
                    }
                }
                Ok(NodeOutcome {
                    output: output.clone(),
                    transition: NodeTransition::Next,
                })
            }
            Behavior::Goto { target, output } => Ok(NodeOutcome {
                output: output.clone(),
                transition: NodeTransition::Goto(target.clone()),
            }),
            Behavior::Complete(output) => Ok(NodeOutcome {
                output: json!({"terminal":true}),
                transition: NodeTransition::Complete(output.clone()),
            }),
            Behavior::Fail(error) => Err(error.clone()),
            Behavior::Delay(duration) => {
                tokio::time::sleep(*duration).await;
                Ok(NodeOutcome {
                    output: json!({"late":true}),
                    transition: NodeTransition::Next,
                })
            }
            Behavior::WaitForStop(started) => {
                started.notify_one();
                control.stopped().await;
                Err(RunError::stopped(control.stop_reason().unwrap()))
            }
        }
    }
}

#[derive(Default)]
struct MemoryRepository {
    status: Mutex<Option<RunStatus>>,
    events: Mutex<Vec<RunEvent>>,
    outputs: Mutex<Vec<NodeOutputRecord>>,
    terminal_updates: Mutex<Vec<TerminalUpdate>>,
    operations: Mutex<Vec<String>>,
    fail_next_append: AtomicBool,
}

#[async_trait]
impl RunRepository for MemoryRepository {
    async fn create_run(&self, _run: NewRun) -> Result<(), HistoryError> {
        *self.status.lock().await = Some(RunStatus::Created);
        self.operations.lock().await.push("create".to_string());
        Ok(())
    }

    async fn mark_running(
        &self,
        _run_id: &str,
        _started_at: DateTime<Utc>,
    ) -> Result<(), HistoryError> {
        *self.status.lock().await = Some(RunStatus::Running);
        self.operations.lock().await.push("running".to_string());
        Ok(())
    }

    async fn append_events(&self, events: &[RunEvent]) -> Result<(), HistoryError> {
        if self.fail_next_append.swap(false, Ordering::SeqCst) {
            return Err(HistoryError::new(
                "SYNTHETIC_APPEND_FAILURE",
                "synthetic append failure",
            ));
        }
        let mut stored = self.events.lock().await;
        let mut operations = self.operations.lock().await;
        for event in events {
            operations.push(format!(
                "event:{}:{}",
                event.event_type.as_str(),
                event.node_id.as_deref().unwrap_or("-")
            ));
            stored.push(event.clone());
        }
        Ok(())
    }

    async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), HistoryError> {
        self.operations
            .lock()
            .await
            .push(format!("output:{}", output.node_id));
        self.outputs.lock().await.push(output);
        Ok(())
    }

    async fn finish_run(
        &self,
        update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<bool, HistoryError> {
        let mut status = self.status.lock().await;
        if status.is_some_and(RunStatus::is_terminal) {
            return Ok(false);
        }
        *status = Some(update.status);
        self.terminal_updates.lock().await.push(update);
        self.operations
            .lock()
            .await
            .push(format!("event:{}:-", event.event_type.as_str()));
        self.events.lock().await.push(event);
        Ok(true)
    }

    async fn recover_run(
        &self,
        update: TerminalUpdate,
        mut terminal: RunEvent,
    ) -> Result<RunEvent, HistoryError> {
        terminal.seq = self
            .events
            .lock()
            .await
            .last()
            .map_or(1, |event| event.seq + 1);
        self.finish_run(update, terminal.clone()).await?;
        Ok(terminal)
    }

    async fn get_run(&self, _run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        Ok(None)
    }

    async fn list_events_after(
        &self,
        _run_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError> {
        Ok(self
            .events
            .lock()
            .await
            .iter()
            .filter(|event| event.seq > after_seq)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn mark_incomplete_interrupted(&self, _at: DateTime<Utc>) -> Result<u64, HistoryError> {
        Ok(0)
    }
}

fn node(id: &str, next: Option<&str>, timeout: Duration, behavior: Behavior) -> CompiledNode {
    CompiledNode {
        id: id.to_string(),
        kind: "test.synthetic".to_string(),
        next: next.map(str::to_string),
        emit: EmitPolicy::None,
        timeout,
        body: Arc::new(behavior),
        edges: next.into_iter().map(str::to_string).collect(),
        references: BTreeSet::new(),
        terminal: next.is_none(),
        control: NodeControl::Ordinary,
    }
}

fn agent(nodes: Vec<CompiledNode>, entry: &str) -> Arc<CompiledAgent> {
    let nodes = nodes
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect::<BTreeMap<_, _>>();
    Arc::new(CompiledAgent {
        id: "coordinator-agent".to_string(),
        name: "Coordinator Agent".to_string(),
        description: String::new(),
        version_hash: "sha256:coordinator".to_string(),
        input_schema: Arc::new(JSONSchema::compile(&json!({"type":"object"})).unwrap()),
        entry: entry.to_string(),
        execution_plan: ExecutionPlan::sequential(entry, nodes.keys().cloned()),
        nodes,
        templates: Arc::new(Handlebars::new()),
    })
}

fn new_run() -> NewRun {
    NewRun {
        run_id: RUN_ID.to_string(),
        request_id: "req_coordinator".to_string(),
        agent_id: "coordinator-agent".to_string(),
        agent_version: "sha256:coordinator".to_string(),
        attachment: RunAttachment::Detached,
        created_at: Utc::now(),
        input_summary: json!({"keys":[], "serialized_bytes":2}),
    }
}

fn coordinator(
    agent: Arc<CompiledAgent>,
    repository: Arc<MemoryRepository>,
    register_executor: bool,
) -> RunCoordinator {
    let mut executors = NodeExecutorRegistry::default();
    if register_executor {
        executors.register(SyntheticNode).unwrap();
    }
    let repository_trait: Arc<dyn RunRepository> = repository;
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            ring_capacity: 32,
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    RunCoordinator::new(
        agent,
        executors,
        events,
        repository_trait,
        ExecutionLimiter::new(Arc::new(Semaphore::new(32)), Arc::new(Semaphore::new(8))),
    )
}

fn event_types(events: &[RunEvent]) -> Vec<RunEventType> {
    events.iter().map(|event| event.event_type).collect()
}

#[tokio::test]
async fn run_state_allows_only_created_running_and_one_terminal_transition() {
    let state = Arc::new(RunState::new());
    assert_eq!(state.status().await, RunStatus::Created);
    state.start().await.unwrap();
    assert_eq!(state.status().await, RunStatus::Running);

    let completed = {
        let state = Arc::clone(&state);
        tokio::spawn(async move { state.try_terminal(RunStatus::Completed).await })
    };
    let failed = {
        let state = Arc::clone(&state);
        tokio::spawn(async move { state.try_terminal(RunStatus::Failed).await })
    };
    let wins = [
        completed.await.unwrap().unwrap(),
        failed.await.unwrap().unwrap(),
    ];
    assert_eq!(wins.into_iter().filter(|won| *won).count(), 1);
    assert!(state.status().await.is_terminal());
    assert!(!state.try_terminal(RunStatus::Cancelled).await.unwrap());
}

#[tokio::test]
async fn coordinator_executes_next_goto_and_complete_with_persistence_barriers() {
    let repository = Arc::new(MemoryRepository::default());
    let final_output = RunOutput {
        content: Some("done".to_string()),
        format: Some("text".to_string()),
        data: json!({"done":true}),
    };
    let agent = agent(
        vec![
            node(
                "prepare",
                Some("route"),
                Duration::from_secs(1),
                Behavior::Next {
                    output: json!({"value":42}),
                    require_output: None,
                },
            ),
            node(
                "route",
                None,
                Duration::from_secs(1),
                Behavior::Goto {
                    target: "answer".to_string(),
                    output: json!({"next":"answer"}),
                },
            ),
            node(
                "answer",
                Some("result"),
                Duration::from_secs(1),
                Behavior::Next {
                    output: json!({"checked":true}),
                    require_output: Some(("prepare".to_string(), json!({"value":42}))),
                },
            ),
            node(
                "result",
                None,
                Duration::from_secs(1),
                Behavior::Complete(final_output.clone()),
            ),
        ],
        "prepare",
    );
    let coordinator = coordinator(agent, Arc::clone(&repository), true);
    let (_, stop) = stop_pair();

    let status = coordinator
        .execute(new_run(), json!({}), stop)
        .await
        .unwrap();

    assert_eq!(status, RunStatus::Completed);
    assert_eq!(*repository.status.lock().await, Some(RunStatus::Completed));
    assert_eq!(repository.terminal_updates.lock().await.len(), 1);
    assert_eq!(
        repository.terminal_updates.lock().await[0].output,
        Some(final_output)
    );
    assert_eq!(repository.outputs.lock().await.len(), 4);
    assert_eq!(
        event_types(&repository.events.lock().await),
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::NodeCompleted,
            RunEventType::NodeStarted,
            RunEventType::NodeCompleted,
            RunEventType::NodeStarted,
            RunEventType::NodeCompleted,
            RunEventType::NodeStarted,
            RunEventType::NodeCompleted,
            RunEventType::RunCompleted,
        ]
    );
    let operations = repository.operations.lock().await;
    for node_id in ["prepare", "route", "answer", "result"] {
        let output = operations
            .iter()
            .position(|operation| operation == &format!("output:{node_id}"))
            .unwrap();
        let completed = operations
            .iter()
            .position(|operation| operation == &format!("event:node.completed:{node_id}"))
            .unwrap();
        assert!(output < completed);
    }
}

#[tokio::test]
async fn node_failure_emits_node_failed_then_run_failed() {
    let repository = Arc::new(MemoryRepository::default());
    let agent = agent(
        vec![node(
            "answer",
            None,
            Duration::from_secs(1),
            Behavior::Fail(RunError::new("UPSTREAM_FAILURE", "model failed")),
        )],
        "answer",
    );
    let coordinator = coordinator(agent, Arc::clone(&repository), true);
    let (_, stop) = stop_pair();

    assert_eq!(
        coordinator
            .execute(new_run(), json!({}), stop)
            .await
            .unwrap(),
        RunStatus::Failed
    );
    let events = repository.events.lock().await;
    assert_eq!(
        event_types(&events),
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::NodeFailed,
            RunEventType::RunFailed,
        ]
    );
    assert_eq!(events[3].code, "UPSTREAM_FAILURE");
    assert_eq!(events[4].code, "UPSTREAM_FAILURE");
}

#[tokio::test]
async fn node_failure_codes_cannot_impersonate_typed_stop_reasons() {
    for code in ["RUN_CANCELLED", "RUN_INTERRUPTED", "RUN_TIMEOUT"] {
        let repository = Arc::new(MemoryRepository::default());
        let agent = agent(
            vec![node(
                "answer",
                None,
                Duration::from_secs(1),
                Behavior::Fail(RunError::new(code, "node-local collision")),
            )],
            "answer",
        );
        let coordinator = coordinator(agent, Arc::clone(&repository), true);
        let (_, stop) = stop_pair();

        assert_eq!(
            coordinator
                .execute(new_run(), json!({}), stop)
                .await
                .unwrap(),
            RunStatus::Failed,
            "node error code {code} changed terminal semantics"
        );
        let events = repository.events.lock().await;
        assert_eq!(events[3].event_type, RunEventType::NodeFailed);
        assert_eq!(events[4].event_type, RunEventType::RunFailed);
        assert_eq!(events[4].code, code);
    }
}

async fn assert_external_stop_status(
    reason: StopReason,
    expected_status: RunStatus,
    expected_event: RunEventType,
) {
    let repository = Arc::new(MemoryRepository::default());
    let started = Arc::new(Notify::new());
    let agent = agent(
        vec![node(
            "waiting",
            None,
            Duration::from_secs(5),
            Behavior::WaitForStop(Arc::clone(&started)),
        )],
        "waiting",
    );
    let coordinator = coordinator(agent, Arc::clone(&repository), true);
    let (controller, stop) = stop_pair();
    let execution = coordinator.execute(new_run(), json!({}), stop);
    let request_stop = async {
        started.notified().await;
        controller.request(reason)
    };
    let (result, requested) = tokio::join!(execution, request_stop);

    assert!(requested);
    assert_eq!(result.unwrap(), expected_status);
    let events = repository.events.lock().await;
    assert_eq!(events[3].event_type, RunEventType::NodeFailed);
    assert_eq!(events[4].event_type, expected_event);
    assert_eq!(events[3].code, RunError::stopped(reason).code());
    assert_eq!(events[4].code, RunError::stopped(reason).code());
}

#[tokio::test]
async fn typed_external_stop_reasons_keep_their_terminal_statuses() {
    for (reason, status, event_type) in [
        (
            StopReason::Cancelled,
            RunStatus::Cancelled,
            RunEventType::RunCancelled,
        ),
        (
            StopReason::Interrupted,
            RunStatus::Interrupted,
            RunEventType::RunInterrupted,
        ),
        (
            StopReason::TimedOut,
            RunStatus::Failed,
            RunEventType::RunFailed,
        ),
    ] {
        assert_external_stop_status(reason, status, event_type).await;
    }
}

#[tokio::test]
async fn coordinator_enforces_node_timeout_even_if_executor_ignores_control() {
    let repository = Arc::new(MemoryRepository::default());
    let agent = agent(
        vec![node(
            "slow",
            None,
            Duration::from_millis(10),
            Behavior::Delay(Duration::from_secs(5)),
        )],
        "slow",
    );
    let coordinator = coordinator(agent, Arc::clone(&repository), true);
    let (_, stop) = stop_pair();

    assert_eq!(
        coordinator
            .execute(new_run(), json!({}), stop)
            .await
            .unwrap(),
        RunStatus::Failed
    );
    let events = repository.events.lock().await;
    assert_eq!(events[3].event_type, RunEventType::NodeFailed);
    assert_eq!(events[3].code, "NODE_TIMEOUT");
    assert_eq!(events[4].event_type, RunEventType::RunFailed);
}

#[tokio::test]
async fn explicit_cancellation_stops_in_flight_node_and_wins_one_terminal_state() {
    let repository = Arc::new(MemoryRepository::default());
    let started = Arc::new(Notify::new());
    let agent = agent(
        vec![node(
            "waiting",
            None,
            Duration::from_secs(5),
            Behavior::WaitForStop(Arc::clone(&started)),
        )],
        "waiting",
    );
    let coordinator = coordinator(agent, Arc::clone(&repository), true);
    let (controller, stop) = stop_pair();

    let execution = coordinator.execute(new_run(), json!({}), stop);
    let cancellation = async {
        started.notified().await;
        controller.request(StopReason::Cancelled);
    };
    let (result, ()) = tokio::join!(execution, cancellation);

    assert_eq!(result.unwrap(), RunStatus::Cancelled);
    assert_eq!(repository.terminal_updates.lock().await.len(), 1);
    let events = repository.events.lock().await;
    assert_eq!(
        event_types(&events),
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::NodeFailed,
            RunEventType::RunCancelled,
        ]
    );
    assert_eq!(events[3].code, "RUN_CANCELLED");
    assert_eq!(events[4].code, "RUN_CANCELLED");
}

#[tokio::test]
async fn missing_executor_is_an_infrastructure_run_failure() {
    let repository = Arc::new(MemoryRepository::default());
    let mut missing = node(
        "missing",
        None,
        Duration::from_secs(1),
        Behavior::Next {
            output: json!({}),
            require_output: None,
        },
    );
    missing.kind = "company.not_registered".to_string();
    let coordinator = coordinator(
        agent(vec![missing], "missing"),
        Arc::clone(&repository),
        false,
    );
    let (_, stop) = stop_pair();

    assert_eq!(
        coordinator
            .execute(new_run(), json!({}), stop)
            .await
            .unwrap(),
        RunStatus::Failed
    );
    let events = repository.events.lock().await;
    assert_eq!(
        event_types(&events),
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::RunFailed,
        ]
    );
    assert_eq!(events[3].code, "INFRASTRUCTURE_FAILURE");
}

#[tokio::test]
async fn journal_worker_failure_recovers_one_durable_failed_terminal() {
    let repository = Arc::new(MemoryRepository::default());
    repository.fail_next_append.store(true, Ordering::SeqCst);
    let final_output = RunOutput {
        content: Some("must not complete".to_string()),
        format: Some("text".to_string()),
        data: json!({}),
    };
    let coordinator = coordinator(
        agent(
            vec![node(
                "result",
                None,
                Duration::from_secs(1),
                Behavior::Complete(final_output),
            )],
            "result",
        ),
        Arc::clone(&repository),
        true,
    );
    let (_, stop) = stop_pair();

    assert_eq!(
        coordinator
            .execute(new_run(), json!({}), stop)
            .await
            .unwrap(),
        RunStatus::Failed
    );
    assert_eq!(*repository.status.lock().await, Some(RunStatus::Failed));
    let events = repository.events.lock().await;
    assert_eq!(events.last().unwrap().event_type, RunEventType::RunFailed);
    assert_eq!(events.last().unwrap().code, "INFRASTRUCTURE_FAILURE");
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    assert_eq!(repository.terminal_updates.lock().await.len(), 1);
}
