use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use handlebars::Handlebars;
use insight_agent_platform::{
    dsl::{
        compiled::{
            BranchPlan, CompiledAgent, CompiledNode, ControlEdge, ExecutionPlan, ForkPlan,
            JoinPolicy, NodeCompilation, NodeControl, NodeOutcome, NodeRegion, NodeTransition,
        },
        compiler::CompileContext,
        CompileError, EmitPolicy,
    },
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::{RunEvent, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository, TerminalProposal, TerminalSequence},
        types::{
            NewRun, NodeOutputRecord, RunAttachment, RunLifecycle, RunRecord, RunStatus,
            RunTerminal, TerminalUpdate,
        },
    },
    nodes::registry::{NodeExecutor, NodeExecutorRegistry, NodeType},
    outcome::{FailureKind, RunFailure, RunOutput, TerminalOutcome, WorkflowError},
    runtime::{
        stop_pair, ExecutionControl, ExecutionLimiter, RunContext, RunCoordinator, RunError,
        RunState, StopReason,
    },
    schema::compile_schema,
};
use serde_json::{json, Value};
use tokio::sync::{Mutex, Notify, Semaphore};
use tracing::{
    field::{Field, Visit},
    Event as TracingEvent, Level, Subscriber,
};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    Layer, Registry,
};

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
    EndFailure(WorkflowError),
    Fail(RunError),
    Delay(Duration),
    WaitForStop(Arc<Notify>),
    ReturnedStopAfterRuntimeStop {
        returned: StopReason,
        started: Arc<Notify>,
    },
    TrackedWait {
        active: Arc<AtomicUsize>,
        started: Arc<Notify>,
        observed_stop: Arc<AtomicUsize>,
    },
    ActivateFork,
    NextAfter(Arc<Notify>),
    GotoAfter {
        started: Arc<Notify>,
        target: String,
    },
    InfrastructureAfter(Arc<Notify>),
    PanicAfter(Arc<Notify>),
}

struct SyntheticNode;

struct ActiveExecution(Arc<AtomicUsize>);

impl Drop for ActiveExecution {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

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
                transition: NodeTransition::End(TerminalOutcome::Success {
                    output: output.clone(),
                }),
            }),
            Behavior::EndFailure(error) => Ok(NodeOutcome {
                output: json!({"terminal":true}),
                transition: NodeTransition::End(TerminalOutcome::Failure {
                    error: error.clone(),
                }),
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
            Behavior::ReturnedStopAfterRuntimeStop { returned, started } => {
                started.notify_one();
                control.stopped().await;
                Err(RunError::stopped(*returned))
            }
            Behavior::TrackedWait {
                active,
                started,
                observed_stop,
            } => {
                active.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveExecution(Arc::clone(active));
                started.notify_one();
                control.stopped().await;
                observed_stop.fetch_add(1, Ordering::SeqCst);
                Err(RunError::stopped(control.stop_reason().unwrap()))
            }
            Behavior::ActivateFork => Ok(NodeOutcome {
                output: json!({}),
                transition: NodeTransition::ActivateFork,
            }),
            Behavior::NextAfter(started) => {
                started.notified().await;
                Ok(NodeOutcome {
                    output: json!({}),
                    transition: NodeTransition::Next,
                })
            }
            Behavior::GotoAfter { started, target } => {
                started.notified().await;
                Ok(NodeOutcome {
                    output: json!({}),
                    transition: NodeTransition::Goto(target.clone()),
                })
            }
            Behavior::InfrastructureAfter(started) => {
                started.notified().await;
                Err(RunError::infrastructure(
                    "SYNTHETIC_INFRASTRUCTURE",
                    "synthetic infrastructure failure",
                ))
            }
            Behavior::PanicAfter(started) => {
                started.notified().await;
                panic!("synthetic executor panic")
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RecordedEvent {
    level: Level,
    fields: BTreeMap<String, String>,
}

impl RecordedEvent {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

#[derive(Clone)]
struct RecordingLayer {
    events: Arc<StdMutex<Vec<RecordedEvent>>>,
}

struct FieldRecorder<'a> {
    fields: &'a mut BTreeMap<String, String>,
}

impl Visit for FieldRecorder<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for RecordingLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &TracingEvent<'_>, _context: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut FieldRecorder {
            fields: &mut fields,
        });
        self.events.lock().unwrap().push(RecordedEvent {
            level: *event.metadata().level(),
            fields,
        });
    }
}

fn recorded_info_logs(
    events: &Arc<StdMutex<Vec<RecordedEvent>>>,
    event_name: &str,
) -> Vec<RecordedEvent> {
    events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.level == Level::INFO)
        .filter(|event| event.field("event_name") == Some(event_name))
        .cloned()
        .collect()
}

#[derive(Default)]
struct MemoryRepository {
    run: Mutex<Option<NewRun>>,
    started_at: Mutex<Option<DateTime<Utc>>>,
    status: Mutex<Option<RunStatus>>,
    events: Mutex<Vec<RunEvent>>,
    outputs: Mutex<Vec<NodeOutputRecord>>,
    terminal_updates: Mutex<Vec<TerminalUpdate>>,
    operations: Mutex<Vec<String>>,
    fail_next_append: AtomicBool,
    fail_append_for: Mutex<Option<(String, String)>>,
    fail_output_for: Mutex<Option<String>>,
    terminal_race: Mutex<Option<TerminalUpdate>>,
}

#[async_trait]
impl RunRepository for MemoryRepository {
    async fn check_health(&self) -> Result<(), HistoryError> {
        Ok(())
    }

    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
        *self.run.lock().await = Some(run);
        *self.status.lock().await = Some(RunStatus::Created);
        self.operations.lock().await.push("create".to_string());
        Ok(())
    }

    async fn mark_running(
        &self,
        _run_id: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), HistoryError> {
        *self.started_at.lock().await = Some(started_at);
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
        if let Some((event_type, node_id)) = self.fail_append_for.lock().await.as_ref() {
            if events.iter().any(|event| {
                event.event_type.as_str() == event_type
                    && event.node_id.as_deref() == Some(node_id.as_str())
            }) {
                return Err(HistoryError::new(
                    "SYNTHETIC_APPEND_FAILURE",
                    "synthetic append failure",
                ));
            }
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
        if self.fail_output_for.lock().await.as_deref() == Some(output.node_id.as_str()) {
            return Err(HistoryError::new(
                "SYNTHETIC_OUTPUT_FAILURE",
                "synthetic node-output failure",
            ));
        }
        self.operations
            .lock()
            .await
            .push(format!("output:{}", output.node_id));
        self.outputs.lock().await.push(output);
        Ok(())
    }

    async fn commit_terminal(
        &self,
        proposal: TerminalProposal,
        sequence: TerminalSequence,
    ) -> Result<RunEvent, HistoryError> {
        let mut status = self.status.lock().await;
        if status.is_some_and(RunStatus::is_terminal) {
            return self.events.lock().await.last().cloned().ok_or_else(|| {
                HistoryError::new(
                    "HISTORY_TERMINAL_EVENT_MISSING",
                    "terminal run has no terminal event",
                )
            });
        }
        let next_seq = self
            .events
            .lock()
            .await
            .last()
            .map_or(1, |event| event.seq + 1);
        if matches!(sequence, TerminalSequence::Expected(expected) if expected != next_seq) {
            return Err(HistoryError::new(
                "HISTORY_EVENT_INVALID",
                "expected terminal sequence does not match durable next sequence",
            ));
        }
        if let Some(race_update) = self.terminal_race.lock().await.take() {
            *status = Some(race_update.status());
            self.terminal_updates.lock().await.push(race_update.clone());
            let race_proposal = TerminalProposal::new(proposal.scope().clone(), race_update)?;
            let terminal_event = race_proposal.event_at(next_seq);
            self.operations
                .lock()
                .await
                .push(format!("event:{}:-", terminal_event.event_type.as_str()));
            self.events.lock().await.push(terminal_event.clone());
            return Ok(terminal_event);
        }
        let terminal_event = proposal.event_at(next_seq);
        let update = proposal.update().clone();
        *status = Some(update.status());
        self.terminal_updates.lock().await.push(update);
        self.operations
            .lock()
            .await
            .push(format!("event:{}:-", terminal_event.event_type.as_str()));
        self.events.lock().await.push(terminal_event.clone());
        Ok(terminal_event)
    }

    async fn get_run(&self, _run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        let Some(run) = self.run.lock().await.clone() else {
            return Ok(None);
        };
        let Some(status) = *self.status.lock().await else {
            return Ok(None);
        };
        let started_at = *self.started_at.lock().await;
        let terminal = self.terminal_updates.lock().await.last().cloned();
        let updated_at = terminal
            .as_ref()
            .map_or(run.created_at, |update| update.ended_at);
        Ok(Some(RunRecord {
            run_id: run.run_id,
            request_id: run.request_id,
            agent_id: run.agent_id,
            agent_version: run.agent_version,
            attachment: run.attachment,
            lifecycle: terminal.as_ref().map_or_else(
                || match status {
                    RunStatus::Created => RunLifecycle::Created,
                    RunStatus::Running => RunLifecycle::Running,
                    _ => panic!("terminal status is missing its typed terminal update"),
                },
                |update| lifecycle_from_terminal(&update.terminal),
            ),
            started_at,
            ended_at: terminal.as_ref().map(|update| update.ended_at),
            updated_at,
            input_summary: run.input_summary,
        }))
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

fn lifecycle_from_terminal(terminal: &RunTerminal) -> RunLifecycle {
    match terminal {
        RunTerminal::Completed { output } => RunLifecycle::Completed {
            output: output.clone(),
        },
        RunTerminal::Failed { error } => RunLifecycle::Failed {
            error: error.clone(),
        },
        RunTerminal::Cancelled { error } => RunLifecycle::Cancelled {
            error: error.clone(),
        },
        RunTerminal::Interrupted { error } => RunLifecycle::Interrupted {
            error: error.clone(),
        },
    }
}

fn node(id: &str, next: Option<&str>, timeout: Duration, behavior: Behavior) -> CompiledNode {
    let control = match &behavior {
        Behavior::Complete(_) => NodeControl::End {
            outcome: insight_agent_platform::outcome::EndOutcomeKind::Success,
        },
        Behavior::EndFailure(_) => NodeControl::End {
            outcome: insight_agent_platform::outcome::EndOutcomeKind::Failure,
        },
        _ => NodeControl::Ordinary,
    };
    CompiledNode {
        id: id.to_string(),
        kind: "test.synthetic".to_string(),
        next: next.map(str::to_string),
        emit: EmitPolicy::None,
        timeout,
        body: Arc::new(behavior),
        edges: next
            .into_iter()
            .map(|target| ControlEdge::Direct {
                target: target.to_string(),
            })
            .collect(),
        references: BTreeSet::new(),
        control,
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
        input_schema: Arc::new(compile_schema(&json!({"type":"object"})).unwrap()),
        entry: entry.to_string(),
        execution_plan: ExecutionPlan::sequential(entry, nodes.keys().cloned()),
        nodes,
        templates: Arc::new(Handlebars::new()),
    })
}

fn parallel_agent(
    first: Behavior,
    second: Behavior,
    missing_second_executor: bool,
) -> Arc<CompiledAgent> {
    let first_node = node("work_a", Some("end_a"), Duration::from_secs(30), first);
    let mut second_node = node("work_b", Some("end_b"), Duration::from_secs(30), second);
    if missing_second_executor {
        second_node.kind = "company.not_registered".to_string();
    }
    let nodes = vec![
        node(
            "fanout",
            None,
            Duration::from_secs(30),
            Behavior::ActivateFork,
        ),
        first_node,
        second_node,
        branch_end_node("end_a"),
        branch_end_node("end_b"),
        node(
            "collect",
            None,
            Duration::from_secs(30),
            Behavior::Complete(RunOutput {
                content: None,
                format: None,
                data: json!({}),
            }),
        ),
    ]
    .into_iter()
    .map(|node| (node.id.clone(), node))
    .collect::<BTreeMap<_, _>>();
    let plan = ForkPlan {
        fork_id: "fanout".to_string(),
        join_id: "collect".to_string(),
        branches: BTreeMap::from([
            (
                "source_a".to_string(),
                BranchPlan {
                    branch_id: "source_a".to_string(),
                    entry: "work_a".to_string(),
                    nodes: BTreeSet::from(["work_a".to_string(), "end_a".to_string()]),
                },
            ),
            (
                "source_b".to_string(),
                BranchPlan {
                    branch_id: "source_b".to_string(),
                    entry: "work_b".to_string(),
                    nodes: BTreeSet::from(["work_b".to_string(), "end_b".to_string()]),
                },
            ),
        ]),
        policy: JoinPolicy::AllSettled,
    };
    Arc::new(CompiledAgent {
        id: "coordinator-agent".to_string(),
        name: "Coordinator Agent".to_string(),
        description: String::new(),
        version_hash: "sha256:coordinator".to_string(),
        input_schema: Arc::new(compile_schema(&json!({"type":"object"})).unwrap()),
        entry: "fanout".to_string(),
        execution_plan: ExecutionPlan {
            entry: "fanout".to_string(),
            forks: BTreeMap::from([("fanout".to_string(), plan)]),
            node_regions: BTreeMap::from([
                ("fanout".to_string(), NodeRegion::Linear),
                (
                    "work_a".to_string(),
                    NodeRegion::Branch {
                        fork_id: "fanout".to_string(),
                        branch_id: "source_a".to_string(),
                    },
                ),
                (
                    "work_b".to_string(),
                    NodeRegion::Branch {
                        fork_id: "fanout".to_string(),
                        branch_id: "source_b".to_string(),
                    },
                ),
                (
                    "end_a".to_string(),
                    NodeRegion::Branch {
                        fork_id: "fanout".to_string(),
                        branch_id: "source_a".to_string(),
                    },
                ),
                (
                    "end_b".to_string(),
                    NodeRegion::Branch {
                        fork_id: "fanout".to_string(),
                        branch_id: "source_b".to_string(),
                    },
                ),
                (
                    "collect".to_string(),
                    NodeRegion::Join {
                        fork_id: "fanout".to_string(),
                    },
                ),
            ]),
        },
        nodes,
        templates: Arc::new(Handlebars::new()),
    })
}

fn branch_end_node(id: &str) -> CompiledNode {
    let mut end = node(
        id,
        None,
        Duration::from_secs(30),
        Behavior::Complete(RunOutput {
            content: None,
            format: None,
            data: json!({"branch":id}),
        }),
    );
    end.control = NodeControl::BranchEnd {
        outcome: insight_agent_platform::outcome::EndOutcomeKind::Success,
    };
    end
}

fn branch_failure_end_node(id: &str) -> CompiledNode {
    let mut end = node(
        id,
        None,
        Duration::from_secs(30),
        Behavior::EndFailure(WorkflowError {
            code: "WORKFLOW_REJECTED".to_string(),
            message: "workflow rejected".to_string(),
        }),
    );
    end.control = NodeControl::BranchEnd {
        outcome: insight_agent_platform::outcome::EndOutcomeKind::Failure,
    };
    end
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
async fn coordinator_owns_run_lifecycle_around_scheduler_execution() {
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
        repository.terminal_updates.lock().await[0].terminal,
        RunTerminal::Completed {
            output: final_output
        }
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
async fn authored_main_failure_completes_its_node_then_fails_the_run_as_workflow() {
    let repository = Arc::new(MemoryRepository::default());
    let agent = agent(
        vec![node(
            "reject",
            None,
            Duration::from_secs(1),
            Behavior::EndFailure(WorkflowError {
                code: "WORKFLOW_REJECTED".to_string(),
                message: "workflow rejected".to_string(),
            }),
        )],
        "reject",
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

    let events = repository.list_events_after(RUN_ID, 0, 32).await.unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "run.created",
            "run.started",
            "node.started",
            "node.completed",
            "run.failed",
        ]
    );
    assert_eq!(events[3].node_id.as_deref(), Some("reject"));
    assert_eq!(events[3].code, "OK");
    assert_eq!(events[4].data, json!({"kind":"workflow"}));
    assert!(!events
        .iter()
        .any(|event| event.event_type == RunEventType::NodeFailed));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event.event_type,
                RunEventType::RunCompleted
                    | RunEventType::RunFailed
                    | RunEventType::RunCancelled
                    | RunEventType::RunInterrupted
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn authored_branch_failure_is_terminal_before_the_join_starts() {
    let repository = Arc::new(MemoryRepository::default());
    let next = || Behavior::Next {
        output: json!({}),
        require_output: None,
    };
    let mut agent = parallel_agent(next(), next(), false);
    Arc::get_mut(&mut agent)
        .unwrap()
        .nodes
        .insert("end_b".to_string(), branch_failure_end_node("end_b"));
    let coordinator = coordinator(agent, Arc::clone(&repository), true);
    let (_, stop) = stop_pair();

    assert_eq!(
        coordinator
            .execute(new_run(), json!({}), stop)
            .await
            .unwrap(),
        RunStatus::Completed
    );
    let events = repository.events.lock().await.clone();
    let position = |event_type: RunEventType, node_id: Option<&str>| {
        events
            .iter()
            .position(|event| event.event_type == event_type && event.node_id.as_deref() == node_id)
            .unwrap()
    };
    let end_started = position(RunEventType::NodeStarted, Some("end_b"));
    let end_completed = position(RunEventType::NodeCompleted, Some("end_b"));
    let branch_failed = events
        .iter()
        .position(|event| {
            event.event_type == RunEventType::BranchFailed && event.data["branch_id"] == "source_b"
        })
        .unwrap();
    let collect_started = position(RunEventType::NodeStarted, Some("collect"));
    assert!(end_started < end_completed);
    assert!(end_completed < branch_failed);
    assert!(branch_failed < collect_started);
    assert_eq!(events[branch_failed].data["kind"], "workflow");
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(
                    event.event_type,
                    RunEventType::BranchCompleted | RunEventType::BranchFailed
                ) && event.data["branch_id"] == "source_b"
            })
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event.event_type,
                RunEventType::RunCompleted
                    | RunEventType::RunFailed
                    | RunEventType::RunCancelled
                    | RunEventType::RunInterrupted
            ))
            .count(),
        1
    );
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
    assert_eq!(events[4].data, json!({"kind":"node"}));
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
async fn coordinator_uses_shared_stop_reason_when_executor_returns_mismatched_stop() {
    for (shared, returned, expected_status, expected_event, expected_code) in [
        (
            StopReason::Interrupted,
            StopReason::Cancelled,
            RunStatus::Interrupted,
            RunEventType::RunInterrupted,
            "RUN_INTERRUPTED",
        ),
        (
            StopReason::Cancelled,
            StopReason::Interrupted,
            RunStatus::Cancelled,
            RunEventType::RunCancelled,
            "RUN_CANCELLED",
        ),
        (
            StopReason::TimedOut,
            StopReason::Cancelled,
            RunStatus::Failed,
            RunEventType::RunFailed,
            "RUN_TIMEOUT",
        ),
    ] {
        let repository = Arc::new(MemoryRepository::default());
        let started = Arc::new(Notify::new());
        let agent = agent(
            vec![node(
                "mismatch",
                None,
                Duration::from_secs(5),
                Behavior::ReturnedStopAfterRuntimeStop {
                    returned,
                    started: Arc::clone(&started),
                },
            )],
            "mismatch",
        );
        let coordinator = coordinator(agent, Arc::clone(&repository), true);
        let (controller, stop) = stop_pair();
        let execution = coordinator.execute(new_run(), json!({}), stop);
        let request_stop = async {
            started.notified().await;
            controller.request(shared)
        };
        let (result, requested) = tokio::join!(execution, request_stop);

        assert!(requested);
        assert_eq!(result.unwrap(), expected_status);
        let events = repository.events.lock().await;
        assert_eq!(events[3].event_type, RunEventType::NodeFailed);
        assert_eq!(events[3].code, expected_code);
        assert_eq!(events[4].event_type, expected_event);
        assert_eq!(events[4].code, expected_code);
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
    assert_eq!(events[4].data, json!({"kind":"timeout"}));
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

#[tokio::test(flavor = "current_thread")]
async fn run_finished_log_uses_durable_failed_terminal_when_completion_loses_race() {
    let recorded = Arc::new(StdMutex::new(Vec::new()));
    let subscriber = Registry::default().with(RecordingLayer {
        events: Arc::clone(&recorded),
    });
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let repository = Arc::new(MemoryRepository::default());
    *repository.terminal_race.lock().await = Some(TerminalUpdate::new(
        RUN_ID,
        Utc::now(),
        RunTerminal::Failed {
            error: RunFailure {
                kind: FailureKind::Infrastructure,
                code: "DURABLE_FAILURE".to_string(),
                message: "durable failure won terminal race".to_string(),
            },
        },
    ));
    let attempted_output = RunOutput {
        content: Some("attempted success output".to_string()),
        format: Some("text".to_string()),
        data: json!({"ok": true}),
    };
    let race_coordinator = coordinator(
        agent(
            vec![node(
                "result",
                None,
                Duration::from_secs(1),
                Behavior::Complete(attempted_output),
            )],
            "result",
        ),
        Arc::clone(&repository),
        true,
    );
    let (_, stop) = stop_pair();

    assert_eq!(
        race_coordinator
            .execute(new_run(), json!({}), stop)
            .await
            .unwrap(),
        RunStatus::Failed
    );

    let finished = recorded_info_logs(&recorded, "run.finished")
        .into_iter()
        .filter(|event| event.field("error_code") == Some("DURABLE_FAILURE"))
        .collect::<Vec<_>>();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].field("status"), Some("failed"));
    assert_eq!(finished[0].field("output_bytes"), Some("0"));
    assert_eq!(finished[0].field("error_code"), Some("DURABLE_FAILURE"));

    let repository = Arc::new(MemoryRepository::default());
    repository.fail_next_append.store(true, Ordering::SeqCst);
    *repository.terminal_race.lock().await = Some(TerminalUpdate::new(
        RUN_ID,
        Utc::now(),
        RunTerminal::Failed {
            error: RunFailure {
                kind: FailureKind::Workflow,
                code: "DURABLE_WORKFLOW_FAILURE".into(),
                message: "durable workflow failure won recovery".into(),
            },
        },
    ));
    let recovery_coordinator = coordinator(
        agent(
            vec![node(
                "result",
                None,
                Duration::from_secs(1),
                Behavior::Complete(RunOutput {
                    content: Some("uncommitted output".into()),
                    format: Some("text".into()),
                    data: json!({}),
                }),
            )],
            "result",
        ),
        Arc::clone(&repository),
        true,
    );
    let (_, stop) = stop_pair();
    assert_eq!(
        recovery_coordinator
            .execute(new_run(), json!({}), stop)
            .await
            .unwrap(),
        RunStatus::Failed
    );
    let finished = recorded_info_logs(&recorded, "run.finished")
        .into_iter()
        .find(|event| event.field("error_code") == Some("DURABLE_WORKFLOW_FAILURE"))
        .expect("durable workflow run.finished log");
    assert_eq!(finished.field("failure_kind"), Some("workflow"));

    let repository = Arc::new(MemoryRepository::default());
    repository.fail_next_append.store(true, Ordering::SeqCst);
    let requested_recovery_coordinator = coordinator(
        agent(
            vec![node(
                "result",
                None,
                Duration::from_secs(1),
                Behavior::Complete(RunOutput {
                    content: Some("unreached output".into()),
                    format: Some("text".into()),
                    data: json!({}),
                }),
            )],
            "result",
        ),
        Arc::clone(&repository),
        true,
    );
    let (_, stop) = stop_pair();
    assert_eq!(
        requested_recovery_coordinator
            .execute(new_run(), json!({}), stop)
            .await
            .unwrap(),
        RunStatus::Failed
    );
    let updates = repository.terminal_updates.lock().await;
    let RunTerminal::Failed { error } = &updates
        .last()
        .expect("requested recovery terminal update")
        .terminal
    else {
        panic!("requested recovery must persist a failed terminal")
    };
    let terminal_code = error.code.clone();
    let terminal_kind = error.kind.as_str();
    drop(updates);
    let finished = recorded_info_logs(&recorded, "run.finished")
        .into_iter()
        .find(|event| event.field("error_code") == Some(terminal_code.as_str()))
        .expect("requested recovery run.finished log");
    assert_eq!(terminal_code, "INFRASTRUCTURE_FAILURE");
    assert_eq!(terminal_kind, "infrastructure");
    assert_eq!(finished.field("failure_kind"), Some(terminal_kind));
}

#[tokio::test]
async fn global_external_stop_preserves_reason_after_all_branches_drain() {
    for (reason, expected_status, expected_code) in [
        (StopReason::Cancelled, RunStatus::Cancelled, "RUN_CANCELLED"),
        (StopReason::TimedOut, RunStatus::Failed, "RUN_TIMEOUT"),
    ] {
        let repository = Arc::new(MemoryRepository::default());
        let active = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let observed_stop = Arc::new(AtomicUsize::new(0));
        let wait = || Behavior::TrackedWait {
            active: Arc::clone(&active),
            started: Arc::clone(&started),
            observed_stop: Arc::clone(&observed_stop),
        };
        let coordinator = coordinator(
            parallel_agent(wait(), wait(), false),
            Arc::clone(&repository),
            true,
        );
        let (controller, stop) = stop_pair();
        let execution = coordinator.execute(new_run(), json!({}), stop);
        let request = async {
            while active.load(Ordering::SeqCst) != 2 {
                started.notified().await;
            }
            assert!(controller.request(reason));
        };
        let (result, ()) = tokio::join!(execution, request);

        assert_eq!(result.unwrap(), expected_status);
        assert_eq!(observed_stop.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        let updates = repository.terminal_updates.lock().await;
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].terminal.error_code(), Some(expected_code));
        drop(updates);
        assert!(!repository.events.lock().await.iter().any(|event| {
            event.node_id.as_deref() == Some("collect")
                || matches!(
                    event.event_type,
                    RunEventType::BranchCompleted | RunEventType::BranchFailed
                )
        }));
        assert!(!repository
            .outputs
            .lock()
            .await
            .iter()
            .any(|output| output.node_id == "collect"));
    }
}

#[derive(Clone, Copy, Debug)]
enum GlobalFailureCase {
    Journal,
    NodeOutput,
    MissingExecutor,
    Panic,
    DuplicateActivation,
    Infrastructure,
}

#[tokio::test]
async fn global_failures_recover_exactly_one_durable_infrastructure_terminal() {
    for failure in [
        GlobalFailureCase::Journal,
        GlobalFailureCase::NodeOutput,
        GlobalFailureCase::MissingExecutor,
        GlobalFailureCase::Panic,
        GlobalFailureCase::DuplicateActivation,
        GlobalFailureCase::Infrastructure,
    ] {
        let repository = Arc::new(MemoryRepository::default());
        let active = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let waiting = Behavior::TrackedWait {
            active: Arc::clone(&active),
            started: Arc::clone(&started),
            observed_stop: Arc::new(AtomicUsize::new(0)),
        };
        let failing = match failure {
            GlobalFailureCase::Panic => Behavior::PanicAfter(Arc::clone(&started)),
            GlobalFailureCase::DuplicateActivation => Behavior::GotoAfter {
                started: Arc::clone(&started),
                target: "work_b".to_string(),
            },
            GlobalFailureCase::Infrastructure => {
                Behavior::InfrastructureAfter(Arc::clone(&started))
            }
            _ => Behavior::NextAfter(Arc::clone(&started)),
        };
        match failure {
            GlobalFailureCase::Journal => {
                *repository.fail_append_for.lock().await =
                    Some(("node.completed".to_string(), "work_b".to_string()));
            }
            GlobalFailureCase::NodeOutput => {
                *repository.fail_output_for.lock().await = Some("work_b".to_string());
            }
            _ => {}
        }
        let coordinator = coordinator(
            parallel_agent(
                waiting,
                failing,
                matches!(failure, GlobalFailureCase::MissingExecutor),
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
            RunStatus::Failed,
            "{failure:?}"
        );
        assert_eq!(active.load(Ordering::SeqCst), 0, "{failure:?}");
        assert_eq!(*repository.status.lock().await, Some(RunStatus::Failed));
        let updates = repository.terminal_updates.lock().await;
        assert_eq!(updates.len(), 1, "{failure:?}");
        assert_eq!(
            updates[0].terminal.error_code(),
            Some("INFRASTRUCTURE_FAILURE"),
            "{failure:?}"
        );
        drop(updates);
        let events = repository.events.lock().await;
        assert_eq!(events.last().unwrap().event_type, RunEventType::RunFailed);
        assert_eq!(events.last().unwrap().code, "INFRASTRUCTURE_FAILURE");
        assert!(
            !events.iter().any(|event| {
                event.node_id.as_deref() == Some("collect")
                    || matches!(
                        event.event_type,
                        RunEventType::BranchCompleted | RunEventType::BranchFailed
                    )
            }),
            "{failure:?}"
        );
        drop(events);
        assert!(!repository
            .outputs
            .lock()
            .await
            .iter()
            .any(|output| output.node_id == "collect"));
    }
}
