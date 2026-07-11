use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
            BranchPlan, CompiledAgent, CompiledNode, ExecutionPlan, ForkPlan, JoinPolicy,
            NodeCompilation, NodeControl, NodeOutcome, NodeTransition, RunOutput,
        },
        compiler::CompileContext,
        CompileError, EmitPolicy,
    },
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::RunEvent,
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{NewRun, NodeOutputRecord, RunRecord, TerminalUpdate},
    },
    nodes::registry::{NodeExecutor, NodeExecutorRegistry, NodeType},
    runtime::{
        execute_node, stop_pair, ExecutionControl, ExecutionLimiter, NodeExecutionFailure,
        RunContext, RunError, RunErrorKind, RunMetadata, Scheduler, SchedulerResult, StopReason,
    },
};
use jsonschema::JSONSchema;
use serde_json::{json, Value};
use tokio::sync::{Mutex, Notify, Semaphore};

struct BlockingExecutor {
    in_flight: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct ContentExecutor;

enum SchedulerBehavior {
    Next {
        output: Value,
        require_output: Option<(String, Value)>,
        executions: Arc<AtomicUsize>,
    },
    Goto {
        target: String,
        output: Value,
        executions: Arc<AtomicUsize>,
    },
    Complete {
        output: RunOutput,
        executions: Arc<AtomicUsize>,
    },
    ActivateFork {
        executions: Arc<AtomicUsize>,
    },
}

struct SchedulerExecutor;

impl NodeType for SchedulerExecutor {
    fn kind(&self) -> &'static str {
        "test.scheduler"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        Err(CompileError::new(
            "TEST_ONLY",
            "scheduler test nodes are constructed directly",
        ))
    }
}

#[async_trait]
impl NodeExecutor for SchedulerExecutor {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        _control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        match node.body::<SchedulerBehavior>()? {
            SchedulerBehavior::Next {
                output,
                require_output,
                executions,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                if let Some((predecessor, expected)) = require_output {
                    if context.node_output(predecessor) != Some(expected) {
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
            SchedulerBehavior::Goto {
                target,
                output,
                executions,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(NodeOutcome {
                    output: output.clone(),
                    transition: NodeTransition::Goto(target.clone()),
                })
            }
            SchedulerBehavior::Complete { output, executions } => {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(NodeOutcome {
                    output: json!({"terminal":true}),
                    transition: NodeTransition::Complete(output.clone()),
                })
            }
            SchedulerBehavior::ActivateFork { executions } => {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(NodeOutcome {
                    output: json!({"fork":true}),
                    transition: NodeTransition::ActivateFork,
                })
            }
        }
    }
}

impl NodeType for ContentExecutor {
    fn kind(&self) -> &'static str {
        "test.content"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        Err(CompileError::new(
            "TEST_ONLY",
            "scheduler test nodes are constructed directly",
        ))
    }
}

#[async_trait]
impl NodeExecutor for ContentExecutor {
    async fn execute(
        &self,
        _node: &CompiledNode,
        _context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        control.emit_content("delta").await?;
        Ok(NodeOutcome {
            output: json!({"ok":true}),
            transition: NodeTransition::Next,
        })
    }
}

impl NodeType for BlockingExecutor {
    fn kind(&self) -> &'static str {
        "test.blocking"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        Err(CompileError::new(
            "TEST_ONLY",
            "scheduler test nodes are constructed directly",
        ))
    }
}

#[async_trait]
impl NodeExecutor for BlockingExecutor {
    async fn execute(
        &self,
        _node: &CompiledNode,
        _context: &RunContext,
        _control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(current, Ordering::SeqCst);
        self.started.notify_one();
        self.release.notified().await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(NodeOutcome {
            output: json!({"ok":true}),
            transition: NodeTransition::Next,
        })
    }
}

#[derive(Default)]
struct SchedulerRepository {
    events: Mutex<Vec<RunEvent>>,
    outputs: Mutex<Vec<NodeOutputRecord>>,
    operations: Mutex<Vec<String>>,
    output_entered: Option<Arc<Notify>>,
    output_release: Option<Arc<Notify>>,
    completed_entered: Option<Arc<Notify>>,
    completed_release: Option<Arc<Notify>>,
    fail_content_append: AtomicBool,
}

#[async_trait]
impl RunRepository for SchedulerRepository {
    async fn create_run(&self, _run: NewRun) -> Result<(), HistoryError> {
        Ok(())
    }

    async fn mark_running(
        &self,
        _run_id: &str,
        _started_at: DateTime<Utc>,
    ) -> Result<(), HistoryError> {
        Ok(())
    }

    async fn append_events(&self, events: &[RunEvent]) -> Result<(), HistoryError> {
        if events
            .iter()
            .any(|event| event.event_type.as_str() == "content.delta")
            && self.fail_content_append.swap(false, Ordering::SeqCst)
        {
            return Err(HistoryError::new(
                "SYNTHETIC_CONTENT_APPEND_FAILURE",
                "synthetic content append failure",
            ));
        }
        let blocks_completion = events
            .iter()
            .any(|event| event.event_type.as_str() == "node.completed");
        self.operations
            .lock()
            .await
            .extend(events.iter().map(|event| {
                format!(
                    "event:{}:{}",
                    event.event_type.as_str(),
                    event.node_id.as_deref().unwrap_or("-")
                )
            }));
        self.events.lock().await.extend_from_slice(events);
        if blocks_completion {
            if let Some(entered) = &self.completed_entered {
                entered.notify_one();
            }
            if let Some(release) = &self.completed_release {
                release.notified().await;
            }
        }
        Ok(())
    }

    async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), HistoryError> {
        self.operations
            .lock()
            .await
            .push(format!("output:{}", output.node_id));
        self.outputs.lock().await.push(output);
        if let Some(entered) = &self.output_entered {
            entered.notify_one();
        }
        if let Some(release) = &self.output_release {
            release.notified().await;
        }
        Ok(())
    }

    async fn finish_run(
        &self,
        _update: TerminalUpdate,
        _event: RunEvent,
    ) -> Result<bool, HistoryError> {
        Ok(true)
    }

    async fn recover_run(
        &self,
        _update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<RunEvent, HistoryError> {
        Ok(event)
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

fn node(id: &str) -> CompiledNode {
    CompiledNode {
        id: id.to_string(),
        kind: "test.blocking".to_string(),
        next: None,
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(30),
        body: Arc::new(()),
        edges: Vec::new(),
        references: BTreeSet::new(),
        terminal: false,
        control: NodeControl::Ordinary,
    }
}

fn content_node(id: &str) -> CompiledNode {
    CompiledNode {
        kind: "test.content".to_string(),
        emit: EmitPolicy::Content,
        ..node(id)
    }
}

fn scheduler_node(id: &str, next: Option<&str>, behavior: SchedulerBehavior) -> CompiledNode {
    CompiledNode {
        id: id.to_string(),
        kind: "test.scheduler".to_string(),
        next: next.map(str::to_string),
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: Arc::new(behavior),
        edges: next.into_iter().map(str::to_string).collect(),
        references: BTreeSet::new(),
        terminal: next.is_none(),
        control: NodeControl::Ordinary,
    }
}

fn scheduler_agent(nodes: Vec<CompiledNode>, entry: &str) -> Arc<CompiledAgent> {
    let nodes = nodes
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect::<BTreeMap<_, _>>();
    Arc::new(CompiledAgent {
        id: "scheduler-agent".to_string(),
        name: "Scheduler Agent".to_string(),
        description: String::new(),
        version_hash: "sha256:scheduler".to_string(),
        input_schema: Arc::new(JSONSchema::compile(&json!({"type":"object"})).unwrap()),
        entry: entry.to_string(),
        execution_plan: ExecutionPlan::sequential(entry, nodes.keys().cloned()),
        nodes,
        templates: Arc::new(Handlebars::new()),
    })
}

fn scheduler(agent: Arc<CompiledAgent>, repository: Arc<SchedulerRepository>) -> Scheduler {
    let mut executors = NodeExecutorRegistry::default();
    executors.register(SchedulerExecutor).unwrap();
    Scheduler::new(
        agent,
        executors,
        event_hub(repository),
        ExecutionLimiter::new(Arc::new(Semaphore::new(4)), Arc::new(Semaphore::new(4))),
    )
}

fn context(run_id: &str) -> RunContext {
    RunContext::new(
        RunMetadata {
            run_id: run_id.to_string(),
            request_id: format!("req_{run_id}"),
            agent_id: "scheduler-agent".to_string(),
            agent_version: "sha256:scheduler".to_string(),
            started_at: Utc::now(),
        },
        json!({}),
    )
}

fn event_hub(repository: Arc<SchedulerRepository>) -> EventHub {
    let repository: Arc<dyn RunRepository> = repository;
    EventHub::new(
        repository,
        EventHubConfig {
            ring_capacity: 32,
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    )
}

#[tokio::test]
async fn sequential_scheduler_preserves_path_context_output_and_node_event_order() {
    let prepare_runs = Arc::new(AtomicUsize::new(0));
    let route_runs = Arc::new(AtomicUsize::new(0));
    let answer_runs = Arc::new(AtomicUsize::new(0));
    let result_runs = Arc::new(AtomicUsize::new(0));
    let final_output = RunOutput {
        content: Some("done".to_string()),
        format: Some("text".to_string()),
        data: json!({"done":true}),
    };
    let agent = scheduler_agent(
        vec![
            scheduler_node(
                "prepare",
                Some("route"),
                SchedulerBehavior::Next {
                    output: json!({"value":42}),
                    require_output: None,
                    executions: Arc::clone(&prepare_runs),
                },
            ),
            scheduler_node(
                "route",
                None,
                SchedulerBehavior::Goto {
                    target: "answer".to_string(),
                    output: json!({"next":"answer"}),
                    executions: Arc::clone(&route_runs),
                },
            ),
            scheduler_node(
                "answer",
                Some("result"),
                SchedulerBehavior::Next {
                    output: json!({"checked":true}),
                    require_output: Some(("prepare".to_string(), json!({"value":42}))),
                    executions: Arc::clone(&answer_runs),
                },
            ),
            scheduler_node(
                "result",
                None,
                SchedulerBehavior::Complete {
                    output: final_output.clone(),
                    executions: Arc::clone(&result_runs),
                },
            ),
        ],
        "prepare",
    );
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = scheduler(agent, Arc::clone(&repository));
    let (_, stop) = stop_pair();

    assert_eq!(
        scheduler
            .run(context("run_sequential"), stop)
            .await
            .unwrap(),
        SchedulerResult::Completed(final_output)
    );
    assert_eq!(
        [
            prepare_runs.load(Ordering::SeqCst),
            route_runs.load(Ordering::SeqCst),
            answer_runs.load(Ordering::SeqCst),
            result_runs.load(Ordering::SeqCst),
        ],
        [1, 1, 1, 1]
    );
    assert_eq!(
        repository
            .events
            .lock()
            .await
            .iter()
            .map(|event| (event.event_type.as_str(), event.node_id.as_deref().unwrap()))
            .collect::<Vec<_>>(),
        vec![
            ("node.started", "prepare"),
            ("node.completed", "prepare"),
            ("node.started", "route"),
            ("node.completed", "route"),
            ("node.started", "answer"),
            ("node.completed", "answer"),
            ("node.started", "result"),
            ("node.completed", "result"),
        ]
    );
}

#[tokio::test]
async fn sequential_scheduler_goto_never_executes_unselected_path() {
    let route_runs = Arc::new(AtomicUsize::new(0));
    let selected_runs = Arc::new(AtomicUsize::new(0));
    let unselected_runs = Arc::new(AtomicUsize::new(0));
    let output = RunOutput {
        content: None,
        format: None,
        data: json!({"selected":true}),
    };
    let agent = scheduler_agent(
        vec![
            scheduler_node(
                "route",
                Some("unselected"),
                SchedulerBehavior::Goto {
                    target: "selected".to_string(),
                    output: json!({"choice":"selected"}),
                    executions: Arc::clone(&route_runs),
                },
            ),
            scheduler_node(
                "unselected",
                None,
                SchedulerBehavior::Complete {
                    output: RunOutput {
                        content: None,
                        format: None,
                        data: json!({"selected":false}),
                    },
                    executions: Arc::clone(&unselected_runs),
                },
            ),
            scheduler_node(
                "selected",
                None,
                SchedulerBehavior::Complete {
                    output: output.clone(),
                    executions: Arc::clone(&selected_runs),
                },
            ),
        ],
        "route",
    );
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = scheduler(agent, repository);
    let (_, stop) = stop_pair();

    assert_eq!(
        scheduler.run(context("run_goto"), stop).await.unwrap(),
        SchedulerResult::Completed(output)
    );
    assert_eq!(route_runs.load(Ordering::SeqCst), 1);
    assert_eq!(selected_runs.load(Ordering::SeqCst), 1);
    assert_eq!(unselected_runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn sequential_scheduler_rejects_duplicate_activation_as_infrastructure() {
    let executions = Arc::new(AtomicUsize::new(0));
    let agent = scheduler_agent(
        vec![scheduler_node(
            "route",
            None,
            SchedulerBehavior::Goto {
                target: "route".to_string(),
                output: json!({}),
                executions: Arc::clone(&executions),
            },
        )],
        "route",
    );
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = scheduler(agent, repository);
    let (_, stop) = stop_pair();

    let error = scheduler
        .run(context("run_duplicate"), stop)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "SCHEDULER_INVARIANT_VIOLATION");
    assert_eq!(error.kind(), RunErrorKind::Infrastructure);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sequential_scheduler_rejects_missing_activation_target_as_infrastructure() {
    let executions = Arc::new(AtomicUsize::new(0));
    let agent = scheduler_agent(
        vec![scheduler_node(
            "route",
            None,
            SchedulerBehavior::Goto {
                target: "missing".to_string(),
                output: json!({}),
                executions: Arc::clone(&executions),
            },
        )],
        "route",
    );
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = scheduler(agent, repository);
    let (_, stop) = stop_pair();

    let error = scheduler
        .run(context("run_missing"), stop)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "SCHEDULER_INVARIANT_VIOLATION");
    assert_eq!(error.kind(), RunErrorKind::Infrastructure);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sequential_scheduler_returns_typed_unsupported_error_for_valid_fork() {
    let executions = Arc::new(AtomicUsize::new(0));
    let mut fork = scheduler_node(
        "fanout",
        None,
        SchedulerBehavior::ActivateFork {
            executions: Arc::clone(&executions),
        },
    );
    fork.control = NodeControl::Fork {
        branches: BTreeMap::from([
            ("left".to_string(), "left_entry".to_string()),
            ("right".to_string(), "right_entry".to_string()),
        ]),
        join: "collect".to_string(),
    };
    let mut agent = scheduler_agent(vec![fork], "fanout");
    Arc::get_mut(&mut agent)
        .unwrap()
        .execution_plan
        .forks
        .insert(
            "fanout".to_string(),
            ForkPlan {
                fork_id: "fanout".to_string(),
                join_id: "collect".to_string(),
                branches: BTreeMap::from([
                    (
                        "left".to_string(),
                        BranchPlan {
                            branch_id: "left".to_string(),
                            entry: "left_entry".to_string(),
                            nodes: BTreeSet::new(),
                        },
                    ),
                    (
                        "right".to_string(),
                        BranchPlan {
                            branch_id: "right".to_string(),
                            entry: "right_entry".to_string(),
                            nodes: BTreeSet::new(),
                        },
                    ),
                ]),
                policy: JoinPolicy::AllSettled,
            },
        );
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = scheduler(agent, repository);
    let (_, stop) = stop_pair();

    let error = scheduler.run(context("run_fork"), stop).await.unwrap_err();
    assert_eq!(error.code(), "SCHEDULER_FORK_UNSUPPORTED");
    assert_eq!(error.kind(), RunErrorKind::Infrastructure);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
}

async fn assert_execution_limit(per_run: usize, global: usize, expected_started: usize) {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut executors = NodeExecutorRegistry::default();
    executors
        .register(BlockingExecutor {
            in_flight: Arc::clone(&in_flight),
            maximum: Arc::clone(&maximum),
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        })
        .unwrap();

    let repository = Arc::new(SchedulerRepository::default());
    let events = event_hub(repository);
    let limiter = ExecutionLimiter::new(
        Arc::new(Semaphore::new(global)),
        Arc::new(Semaphore::new(per_run)),
    );
    let (_, stop) = stop_pair();
    let mut executions = Vec::new();
    for index in 0..3 {
        executions.push(tokio::spawn(execute_node(
            node(&format!("node_{index}")),
            context("run_scheduler"),
            executors.clone(),
            events.clone(),
            stop.clone(),
            limiter.clone(),
        )));
    }

    for _ in 0..expected_started {
        started.notified().await;
    }
    assert_eq!(in_flight.load(Ordering::SeqCst), expected_started);

    for _ in expected_started..3 {
        release.notify_one();
        started.notified().await;
    }
    release.notify_waiters();
    for execution in executions {
        execution.await.unwrap().unwrap();
    }
    assert!(maximum.load(Ordering::SeqCst) <= per_run);
    assert!(maximum.load(Ordering::SeqCst) <= global);
}

#[tokio::test]
async fn execution_respects_per_run_limit_before_global_capacity() {
    assert_execution_limit(1, 2, 1).await;
}

#[tokio::test]
async fn execution_respects_global_limit_across_wider_run_capacity() {
    assert_execution_limit(3, 2, 2).await;
}

#[tokio::test]
async fn execution_holds_both_permits_through_output_and_completion_persistence() {
    let started = Arc::new(Notify::new());
    let executor_release = Arc::new(Notify::new());
    let output_entered = Arc::new(Notify::new());
    let output_release = Arc::new(Notify::new());
    let completed_entered = Arc::new(Notify::new());
    let completed_release = Arc::new(Notify::new());
    let repository = Arc::new(SchedulerRepository {
        output_entered: Some(Arc::clone(&output_entered)),
        output_release: Some(Arc::clone(&output_release)),
        completed_entered: Some(Arc::clone(&completed_entered)),
        completed_release: Some(Arc::clone(&completed_release)),
        ..SchedulerRepository::default()
    });
    let events = event_hub(Arc::clone(&repository));
    let mut executors = NodeExecutorRegistry::default();
    executors
        .register(BlockingExecutor {
            in_flight: Arc::new(AtomicUsize::new(0)),
            maximum: Arc::new(AtomicUsize::new(0)),
            started: Arc::clone(&started),
            release: Arc::clone(&executor_release),
        })
        .unwrap();
    let global = Arc::new(Semaphore::new(1));
    let per_run = Arc::new(Semaphore::new(1));
    let limiter = ExecutionLimiter::new(Arc::clone(&global), Arc::clone(&per_run));
    let (_, stop) = stop_pair();

    let execution = tokio::spawn(execute_node(
        node("durable"),
        context("run_durable"),
        executors,
        events,
        stop,
        limiter,
    ));
    started.notified().await;
    executor_release.notify_one();

    output_entered.notified().await;
    assert_eq!(global.available_permits(), 0);
    assert_eq!(per_run.available_permits(), 0);
    output_release.notify_one();

    completed_entered.notified().await;
    assert_eq!(global.available_permits(), 0);
    assert_eq!(per_run.available_permits(), 0);
    completed_release.notify_one();
    execution.await.unwrap().unwrap();

    let operations = repository.operations.lock().await;
    let output = operations
        .iter()
        .position(|operation| operation == "output:durable")
        .unwrap();
    let completed = operations
        .iter()
        .position(|operation| operation == "event:node.completed:durable")
        .unwrap();
    assert!(output < completed);
}

#[tokio::test]
async fn execution_stop_at_either_permit_wait_emits_no_node_event() {
    for (global_capacity, per_run_capacity) in [(1, 0), (0, 1)] {
        let repository = Arc::new(SchedulerRepository::default());
        let events = event_hub(Arc::clone(&repository));
        let started = Arc::new(Notify::new());
        let in_flight = Arc::new(AtomicUsize::new(0));
        let mut executors = NodeExecutorRegistry::default();
        executors
            .register(BlockingExecutor {
                in_flight: Arc::clone(&in_flight),
                maximum: Arc::new(AtomicUsize::new(0)),
                started: Arc::clone(&started),
                release: Arc::new(Notify::new()),
            })
            .unwrap();
        let global = Arc::new(Semaphore::new(global_capacity));
        let per_run = Arc::new(Semaphore::new(per_run_capacity));
        let limiter = ExecutionLimiter::new(Arc::clone(&global), Arc::clone(&per_run));
        let (controller, stop) = stop_pair();
        let execution = tokio::spawn(execute_node(
            node("queued"),
            context("run_queued"),
            executors,
            events,
            stop,
            limiter,
        ));

        if per_run_capacity > 0 {
            while per_run.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        }
        controller.request(StopReason::Cancelled);
        assert!(matches!(
            execution.await.unwrap(),
            Err(NodeExecutionFailure::Stop { node_id, error })
                if node_id == "queued" && error.code() == "RUN_CANCELLED"
        ));
        assert!(repository.events.lock().await.is_empty());
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
        assert_eq!(global.available_permits(), global_capacity);
        assert_eq!(per_run.available_permits(), per_run_capacity);
    }
}

#[tokio::test]
async fn execution_content_journal_failure_remains_infrastructure_by_origin() {
    let repository = Arc::new(SchedulerRepository {
        fail_content_append: AtomicBool::new(true),
        ..SchedulerRepository::default()
    });
    let events = event_hub(Arc::clone(&repository));
    let mut executors = NodeExecutorRegistry::default();
    executors.register(ContentExecutor).unwrap();
    let limiter = ExecutionLimiter::new(Arc::new(Semaphore::new(1)), Arc::new(Semaphore::new(1)));
    let (_, stop) = stop_pair();

    assert!(matches!(
        execute_node(
            content_node("content"),
            context("run_content"),
            executors,
            events,
            stop,
            limiter,
        )
        .await,
        Err(NodeExecutionFailure::Infrastructure(error))
            if error.code() == "SYNTHETIC_CONTENT_APPEND_FAILURE"
                && error.kind() == RunErrorKind::Infrastructure
    ));
    let events = repository.events.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type.as_str(), "node.started");
}

#[test]
fn execution_errors_are_classified_by_source() {
    let node = RunError::new("UPSTREAM_FAILURE", "failed");
    assert!(matches!(node.kind(), RunErrorKind::Node));
    assert_eq!(node.stop_reason(), None);
    assert!(matches!(
        RunError::infrastructure("EVENT_APPEND_FAILED", "failed").kind(),
        RunErrorKind::Infrastructure
    ));
    let stop = RunError::stopped(StopReason::Cancelled);
    assert!(matches!(stop.kind(), RunErrorKind::Stop));
    assert_eq!(stop.stop_reason(), Some(StopReason::Cancelled));
}
