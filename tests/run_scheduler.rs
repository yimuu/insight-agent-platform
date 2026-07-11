use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_agent_platform::{
    dsl::{
        compiled::{CompiledNode, NodeCompilation, NodeControl, NodeOutcome, NodeTransition},
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
        RunContext, RunError, RunErrorKind, RunMetadata, StopReason,
    },
};
use serde_json::{json, Value};
use tokio::sync::{Mutex, Notify, Semaphore};

struct BlockingExecutor {
    in_flight: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct ContentExecutor;

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
    assert!(matches!(
        RunError::new("UPSTREAM_FAILURE", "failed").kind(),
        RunErrorKind::Node
    ));
    assert!(matches!(
        RunError::infrastructure("EVENT_APPEND_FAILED", "failed").kind(),
        RunErrorKind::Infrastructure
    ));
    assert!(matches!(
        RunError::stopped(StopReason::Cancelled).kind(),
        RunErrorKind::Stop
    ));
}
