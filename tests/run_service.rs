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
            BranchPlan, CompiledAgent, CompiledNode, ControlEdge, ExecutionPlan, ForkPlan,
            JoinPolicy, NodeCompilation, NodeControl, NodeOutcome, NodeRegion, NodeTransition,
        },
        compiler::CompileContext,
        CompileError, EmitPolicy,
    },
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::{RunEvent, RunEventScope, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{NewRun, NodeOutputRecord, RunRecord, RunStatus, TerminalUpdate},
    },
    nodes::registry::{NodeExecutor, NodeExecutorRegistry, NodeType},
    outcome::{RunOutput, TerminalOutcome},
    runtime::{
        CompiledAgentRegistry, ExecutionControl, RequestMetadata, RunContext, RunError, RunService,
        RunServiceConfig, ServiceError,
    },
    schema::compile_schema,
};
use serde_json::{json, Value};
use tokio::sync::{watch, Mutex, Notify};

enum ServiceBehavior {
    Complete,
    Block,
    TrackedBlock {
        active: Arc<AtomicUsize>,
        started: Arc<tokio::sync::Notify>,
        stopped: Arc<AtomicUsize>,
    },
    ActivateFork,
}

struct ServiceNode;

struct ActiveExecution(Arc<AtomicUsize>);

impl Drop for ActiveExecution {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl NodeType for ServiceNode {
    fn kind(&self) -> &'static str {
        "test.service"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        Err(CompileError::new(
            "TEST_ONLY",
            "service test nodes are constructed directly",
        ))
    }
}

#[async_trait]
impl NodeExecutor for ServiceNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        _context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        match node.body::<ServiceBehavior>()? {
            ServiceBehavior::Complete => Ok(NodeOutcome {
                output: json!({"complete":true}),
                transition: NodeTransition::End(TerminalOutcome::Success {
                    output: RunOutput {
                        content: Some("done".to_string()),
                        format: Some("text".to_string()),
                        data: json!({"done":true}),
                    },
                }),
            }),
            ServiceBehavior::Block => {
                control.stopped().await;
                Err(RunError::stopped(control.stop_reason().unwrap()))
            }
            ServiceBehavior::TrackedBlock {
                active,
                started,
                stopped,
            } => {
                active.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveExecution(Arc::clone(active));
                started.notify_one();
                control.stopped().await;
                stopped.fetch_add(1, Ordering::SeqCst);
                Err(RunError::stopped(control.stop_reason().unwrap()))
            }
            ServiceBehavior::ActivateFork => Ok(NodeOutcome {
                output: json!({}),
                transition: NodeTransition::ActivateFork,
            }),
        }
    }
}

fn agent(id: &str, behavior: ServiceBehavior) -> Arc<CompiledAgent> {
    let node = CompiledNode {
        id: "work".to_string(),
        kind: "test.service".to_string(),
        next: None,
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(3600),
        body: Arc::new(behavior),
        edges: Vec::new(),
        references: BTreeSet::new(),
        control: NodeControl::Ordinary,
    };
    let nodes = BTreeMap::from([("work".to_string(), node)]);
    Arc::new(CompiledAgent {
        id: id.to_string(),
        name: id.to_string(),
        description: String::new(),
        version_hash: format!("sha256:{id}"),
        input_schema: Arc::new(compile_schema(&json!({"type":"object"})).unwrap()),
        entry: "work".to_string(),
        execution_plan: ExecutionPlan::sequential("work", nodes.keys().cloned()),
        nodes,
        templates: Arc::new(Handlebars::new()),
    })
}

fn parallel_blocking_agent(
    active: Arc<AtomicUsize>,
    started: Arc<tokio::sync::Notify>,
    stopped: Arc<AtomicUsize>,
) -> Arc<CompiledAgent> {
    let tracked = || ServiceBehavior::TrackedBlock {
        active: Arc::clone(&active),
        started: Arc::clone(&started),
        stopped: Arc::clone(&stopped),
    };
    let make_node = |id: &str, next: Option<&str>, behavior: ServiceBehavior| CompiledNode {
        id: id.to_string(),
        kind: "test.service".to_string(),
        next: next.map(str::to_string),
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(3600),
        body: Arc::new(behavior),
        edges: next
            .into_iter()
            .map(|target| ControlEdge::Direct {
                target: target.to_string(),
            })
            .collect(),
        references: BTreeSet::new(),
        control: if id.starts_with("end_") {
            NodeControl::End {
                outcome: insight_agent_platform::outcome::EndOutcomeKind::Success,
            }
        } else {
            NodeControl::Ordinary
        },
    };
    let nodes = vec![
        make_node("fanout", None, ServiceBehavior::ActivateFork),
        make_node("work_a", Some("end_a"), tracked()),
        make_node("work_b", Some("end_b"), tracked()),
        make_node("end_a", None, ServiceBehavior::Complete),
        make_node("end_b", None, ServiceBehavior::Complete),
        make_node("collect", None, ServiceBehavior::Complete),
    ]
    .into_iter()
    .map(|node| (node.id.clone(), node))
    .collect::<BTreeMap<_, _>>();
    let fork = ForkPlan {
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
        id: "parallel-blocking".to_string(),
        name: "parallel-blocking".to_string(),
        description: String::new(),
        version_hash: "sha256:parallel-blocking".to_string(),
        input_schema: Arc::new(compile_schema(&json!({"type":"object"})).unwrap()),
        entry: "fanout".to_string(),
        execution_plan: ExecutionPlan {
            entry: "fanout".to_string(),
            forks: BTreeMap::from([("fanout".to_string(), fork)]),
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

#[derive(Clone, Default)]
struct RepositoryHooks {
    create_run: Option<Arc<RepositoryGate>>,
    get_run: Option<Arc<RepositoryGate>>,
    recover_run: Option<Arc<RecoveryGate>>,
}

struct RepositoryGate {
    entered: watch::Sender<bool>,
    release: Notify,
    used: AtomicBool,
}

impl RepositoryGate {
    fn new() -> Arc<Self> {
        let (entered, _) = watch::channel(false);
        Arc::new(Self {
            entered,
            release: Notify::new(),
            used: AtomicBool::new(false),
        })
    }

    async fn block_once(&self) {
        if !self.used.swap(true, Ordering::SeqCst) {
            let _ = self.entered.send(true);
            self.release.notified().await;
        }
    }

    async fn wait_entered(&self) {
        let mut entered = self.entered.subscribe();
        while !*entered.borrow() {
            entered.changed().await.unwrap();
        }
    }

    fn release(&self) {
        self.release.notify_waiters();
    }
}

struct RecoveryGate {
    calls: AtomicUsize,
    entered: watch::Sender<usize>,
    release: Notify,
    released: AtomicBool,
}

impl RecoveryGate {
    fn new() -> Arc<Self> {
        let (entered, _) = watch::channel(0);
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            entered,
            release: Notify::new(),
            released: AtomicBool::new(false),
        })
    }

    async fn block_until_released(&self) {
        let calls = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.entered.send(calls);
        if !self.released.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
    }

    async fn wait_calls(&self, expected: usize) {
        let mut entered = self.entered.subscribe();
        while *entered.borrow() < expected {
            entered.changed().await.unwrap();
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

struct CountingRepository {
    records: Mutex<BTreeMap<String, RunRecord>>,
    events: Mutex<BTreeMap<String, Vec<RunEvent>>>,
    outputs: Mutex<Vec<NodeOutputRecord>>,
    creates: AtomicUsize,
    event_history_reads: AtomicUsize,
    fail_appends: AtomicBool,
    hooks: RepositoryHooks,
}

#[async_trait]
impl RunRepository for CountingRepository {
    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
        if let Some(gate) = self.hooks.create_run.as_ref() {
            gate.block_once().await;
        }
        self.creates.fetch_add(1, Ordering::SeqCst);
        self.records.lock().await.insert(
            run.run_id.clone(),
            RunRecord {
                run_id: run.run_id,
                request_id: run.request_id,
                agent_id: run.agent_id,
                agent_version: run.agent_version,
                attachment: run.attachment,
                status: RunStatus::Created,
                started_at: None,
                ended_at: None,
                updated_at: run.created_at,
                input_summary: run.input_summary,
                output: None,
                error_code: None,
                error_message: None,
            },
        );
        Ok(())
    }

    async fn mark_running(
        &self,
        run_id: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), HistoryError> {
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(run_id)
            .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
        record.status = RunStatus::Running;
        record.started_at = Some(started_at);
        record.updated_at = started_at;
        Ok(())
    }

    async fn append_events(&self, events: &[RunEvent]) -> Result<(), HistoryError> {
        if self.fail_appends.load(Ordering::SeqCst) {
            return Err(HistoryError::new(
                "SYNTHETIC_APPEND_FAILURE",
                "synthetic append failure",
            ));
        }
        let mut stored = self.events.lock().await;
        for event in events {
            stored
                .entry(event.run_id.clone())
                .or_default()
                .push(event.clone());
        }
        Ok(())
    }

    async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), HistoryError> {
        self.outputs.lock().await.push(output);
        Ok(())
    }

    async fn finish_run(
        &self,
        update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<bool, HistoryError> {
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(&update.run_id)
            .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
        if record.status.is_terminal() {
            return Ok(false);
        }
        record.status = update.status;
        record.ended_at = Some(update.ended_at);
        record.updated_at = update.ended_at;
        record.output = update.output;
        record.error_code = update.error_code;
        record.error_message = update.error_message;
        drop(records);
        self.events
            .lock()
            .await
            .entry(event.run_id.clone())
            .or_default()
            .push(event);
        Ok(true)
    }

    async fn recover_run(
        &self,
        update: TerminalUpdate,
        mut terminal: RunEvent,
    ) -> Result<RunEvent, HistoryError> {
        if let Some(gate) = self.hooks.recover_run.as_ref() {
            gate.block_until_released().await;
        }
        terminal.seq = self
            .events
            .lock()
            .await
            .get(&update.run_id)
            .and_then(|events| events.last())
            .map_or(1, |event| event.seq + 1);
        self.finish_run(update, terminal.clone()).await?;
        Ok(terminal)
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        if let Some(gate) = self.hooks.get_run.as_ref() {
            gate.block_once().await;
        }
        Ok(self.records.lock().await.get(run_id).cloned())
    }

    async fn list_events_after(
        &self,
        run_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError> {
        self.event_history_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .events
            .lock()
            .await
            .get(run_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|event| event.seq > after_seq)
            .take(limit)
            .collect())
    }

    async fn mark_incomplete_interrupted(&self, at: DateTime<Utc>) -> Result<u64, HistoryError> {
        let mut records = self.records.lock().await;
        let mut interrupted = Vec::new();
        for record in records.values_mut() {
            if matches!(record.status, RunStatus::Created | RunStatus::Running) {
                record.status = RunStatus::Interrupted;
                record.ended_at = Some(at);
                record.updated_at = at;
                record.error_code = Some("RUN_INTERRUPTED".to_string());
                record.error_message =
                    Some("run interrupted during startup reconciliation".to_string());
                interrupted.push((
                    record.run_id.clone(),
                    record.request_id.clone(),
                    record.agent_id.clone(),
                    record.agent_version.clone(),
                ));
            }
        }
        drop(records);
        let mut events = self.events.lock().await;
        for (run_id, request_id, agent_id, agent_version) in &interrupted {
            let run_events = events.entry(run_id.clone()).or_default();
            let seq = run_events.last().map_or(1, |event| event.seq + 1);
            run_events.push(RunEvent::error_at(
                RunEventType::RunInterrupted,
                seq,
                RunEventScope::for_run(request_id, run_id, agent_id, agent_version),
                at,
                "RUN_INTERRUPTED",
                "run interrupted during startup reconciliation",
                json!({}),
            ));
        }
        Ok(interrupted.len() as u64)
    }
}

async fn service_with_config(
    config: RunServiceConfig,
) -> Result<(RunService, Arc<CountingRepository>), ServiceError> {
    service_with_agents(
        config,
        vec![
            agent("blocking", ServiceBehavior::Block),
            agent("fast", ServiceBehavior::Complete),
        ],
    )
    .await
}

async fn service_with_agents(
    config: RunServiceConfig,
    agents: Vec<Arc<CompiledAgent>>,
) -> Result<(RunService, Arc<CountingRepository>), ServiceError> {
    service_with_repository_hooks(config, agents, RepositoryHooks::default()).await
}

async fn service_with_repository_hooks(
    config: RunServiceConfig,
    agents: Vec<Arc<CompiledAgent>>,
    hooks: RepositoryHooks,
) -> Result<(RunService, Arc<CountingRepository>), ServiceError> {
    service_with_repository_hooks_and_event_timeout(config, agents, hooks, Duration::from_secs(1))
        .await
}

async fn service_with_repository_hooks_and_event_timeout(
    config: RunServiceConfig,
    agents: Vec<Arc<CompiledAgent>>,
    hooks: RepositoryHooks,
    operation_timeout: Duration,
) -> Result<(RunService, Arc<CountingRepository>), ServiceError> {
    let repository = Arc::new(CountingRepository {
        records: Mutex::new(BTreeMap::new()),
        events: Mutex::new(BTreeMap::new()),
        outputs: Mutex::new(Vec::new()),
        creates: AtomicUsize::new(0),
        event_history_reads: AtomicUsize::new(0),
        fail_appends: AtomicBool::new(false),
        hooks,
    });
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout,
        },
    );
    let agents = CompiledAgentRegistry::new(agents).unwrap();
    let mut executors = NodeExecutorRegistry::default();
    executors.register(ServiceNode).unwrap();
    let service = RunService::new(agents, executors, repository_trait, events, config)?;
    Ok((service, repository))
}

async fn service(max_concurrent_runs: usize) -> (RunService, Arc<CountingRepository>) {
    service_with_config(RunServiceConfig {
        max_concurrent_runs,
        max_parallel_node_executions: 32,
        max_parallel_branches_per_run: 8,
        run_timeout: Duration::from_secs(3600),
    })
    .await
    .unwrap()
}

async fn wait_for_status(service: &RunService, run_id: &str, expected: RunStatus) -> RunRecord {
    let mut last_status = None;
    for _ in 0..200 {
        let record = service.get_run(run_id).await.unwrap();
        last_status = Some(record.status);
        if record.status == expected {
            return record;
        }
        tokio::task::yield_now().await;
    }
    panic!("run {run_id} did not reach {expected:?}; last status: {last_status:?}")
}

async fn run_event_types(repository: &CountingRepository, run_id: &str) -> Vec<RunEventType> {
    repository
        .events
        .lock()
        .await
        .get(run_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|event| event.event_type)
        .collect()
}

#[tokio::test]
async fn zero_parallel_capacities_are_rejected() {
    for config in [
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 0,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(1),
        },
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 0,
            run_timeout: Duration::from_secs(1),
        },
    ] {
        let error = service_with_config(config).await.err().unwrap();
        assert_eq!(error.code(), "RUN_SERVICE_CONFIG_INVALID");
    }
}

#[tokio::test]
async fn attached_run_disconnect_cancels_immediately() {
    let (service, _) = service(2).await;
    let attached = service
        .create_attached("blocking", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let run_id = attached.run_id.clone();
    wait_for_status(&service, &run_id, RunStatus::Running).await;
    drop(attached.subscription);
    let cancelled = tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_status(&service, &run_id, RunStatus::Cancelled),
    )
    .await
    .expect("Attached cancellation must not wait for a reconnect grace period");
    assert_eq!(cancelled.status, RunStatus::Cancelled);
}

#[tokio::test]
async fn attached_disconnect_immediately_stops_and_drains_all_active_branches() {
    let active = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let stopped = Arc::new(AtomicUsize::new(0));
    let parallel = parallel_blocking_agent(
        Arc::clone(&active),
        Arc::clone(&started),
        Arc::clone(&stopped),
    );
    let (service, repository) = service_with_agents(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 4,
            max_parallel_branches_per_run: 4,
            run_timeout: Duration::from_secs(3600),
        },
        vec![parallel],
    )
    .await
    .unwrap();
    let attached = service
        .create_attached("parallel-blocking", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let run_id = attached.run_id.clone();
    while active.load(Ordering::SeqCst) != 2 {
        started.notified().await;
    }
    drop(attached.subscription);
    tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_status(&service, &run_id, RunStatus::Cancelled),
    )
    .await
    .expect("parallel Attached cancellation must drain promptly");
    assert_eq!(stopped.load(Ordering::SeqCst), 2);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    let events = repository.events.lock().await;
    let run_events = &events[&run_id];
    assert_eq!(
        run_events
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
    assert!(!run_events
        .iter()
        .any(|event| event.node_id.as_deref() == Some("collect")));
    drop(events);
    assert!(!repository
        .outputs
        .lock()
        .await
        .iter()
        .any(|output| output.run_id == run_id && output.node_id == "collect"));
}

#[tokio::test]
async fn attached_terminal_drop_never_reads_history_or_rewrites_completion() {
    let (service, repository) = service(2).await;
    let attached = service
        .create_attached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let run_id = attached.run_id.clone();
    let mut subscription = attached.subscription;

    loop {
        let event = subscription.recv().await.unwrap();
        if matches!(event.event_type, RunEventType::RunCompleted) {
            break;
        }
    }
    drop(subscription);
    tokio::task::yield_now().await;

    assert_eq!(
        service.get_run(&run_id).await.unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(repository.event_history_reads.load(Ordering::SeqCst), 0);
    let events = repository.events.lock().await;
    let terminal = events[&run_id]
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
    assert_eq!(terminal.len(), 1);
    assert_eq!(terminal[0].event_type, RunEventType::RunCompleted);
}

#[tokio::test]
async fn detached_run_completes_without_any_subscriber() {
    let (service, _) = service(2).await;
    let created = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();

    let completed = wait_for_status(&service, &created.run_id, RunStatus::Completed).await;
    assert_eq!(completed.output.unwrap().content.as_deref(), Some("done"));
}

#[tokio::test]
async fn permanent_journal_failure_rejects_later_runs_and_marks_service_unhealthy() {
    let (service, repository) = service(2).await;
    repository.fail_appends.store(true, Ordering::SeqCst);
    let first = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(&service, &first.run_id, RunStatus::Failed).await;
    assert!(!service.is_healthy());

    let error = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "RUN_SERVICE_UNAVAILABLE");
    assert_eq!(repository.creates.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_waits_for_background_recovery_owner() {
    let recover_gate = RecoveryGate::new();
    let (service, repository) = service_with_repository_hooks_and_event_timeout(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: None,
            get_run: None,
            recover_run: Some(Arc::clone(&recover_gate)),
        },
        Duration::from_millis(20),
    )
    .await
    .unwrap();
    repository.fail_appends.store(true, Ordering::SeqCst);

    let created = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(250), recover_gate.wait_calls(2))
        .await
        .expect("foreground timeout must hand off to a background recovery owner");
    assert!(!service.is_healthy());

    let error = service
        .shutdown(Duration::from_millis(40))
        .await
        .unwrap_err();
    assert_eq!(error.code(), "SHUTDOWN_TIMEOUT");

    recover_gate.release();
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    let recovered = service.get_run(&created.run_id).await.unwrap();
    assert_eq!(recovered.status, RunStatus::Failed);
    assert_eq!(
        recovered.error_code.as_deref(),
        Some("INFRASTRUCTURE_FAILURE")
    );
}

#[tokio::test]
async fn recovery_handoff_releases_active_ownership_but_keeps_service_unhealthy() {
    let recover_gate = RecoveryGate::new();
    let (service, repository) = service_with_repository_hooks_and_event_timeout(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: None,
            get_run: None,
            recover_run: Some(Arc::clone(&recover_gate)),
        },
        Duration::from_millis(20),
    )
    .await
    .unwrap();
    repository.fail_appends.store(true, Ordering::SeqCst);

    let created = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(250), recover_gate.wait_calls(2))
        .await
        .expect("foreground timeout must hand off to a background recovery owner");

    let error = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "RUN_SERVICE_UNAVAILABLE");

    recover_gate.release();
    service.shutdown(Duration::from_secs(1)).await.unwrap();
    assert_eq!(
        service.get_run(&created.run_id).await.unwrap().status,
        RunStatus::Failed
    );
}

#[tokio::test]
async fn cancellation_is_idempotent_and_does_not_rewrite_completed_runs() {
    let (service, _) = service(3).await;
    let running = service
        .create_detached("blocking", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(&service, &running.run_id, RunStatus::Running).await;

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), service.cancel(&running.run_id))
            .await
            .expect("cancellation completion notification must not be lost")
            .unwrap()
            .status,
        RunStatus::Cancelled
    );
    assert_eq!(
        service.cancel(&running.run_id).await.unwrap().status,
        RunStatus::Cancelled
    );

    let fast = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(&service, &fast.run_id, RunStatus::Completed).await;
    assert_eq!(
        service.cancel(&fast.run_id).await.unwrap().status,
        RunStatus::Completed
    );
}

#[tokio::test]
async fn capacity_is_rejected_before_a_second_run_is_inserted() {
    let (service, repository) = service(1).await;
    let first = service
        .create_detached("blocking", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(&service, &first.run_id, RunStatus::Running).await;

    let error = service
        .create_detached("blocking", json!({}), RequestMetadata::default())
        .await
        .unwrap_err();

    assert_eq!(error.code(), "RUN_CAPACITY_EXCEEDED");
    assert_eq!(repository.creates.load(Ordering::SeqCst), 1);
    service.cancel(&first.run_id).await.unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_detached_run_blocked_in_create_run() {
    let create_gate = RepositoryGate::new();
    let (service, repository) = service_with_repository_hooks(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: Some(Arc::clone(&create_gate)),
            get_run: None,
            recover_run: None,
        },
    )
    .await
    .unwrap();

    let creator_service = service.clone();
    let create_task = tokio::spawn(async move {
        creator_service
            .create_detached("fast", json!({}), RequestMetadata::default())
            .await
    });
    create_gate.wait_entered().await;

    let shutdown_service = service.clone();
    let mut shutdown_task =
        tokio::spawn(async move { shutdown_service.shutdown(Duration::from_secs(1)).await });
    let shutdown_finished_while_blocked = tokio::select! {
        result = &mut shutdown_task => {
            result.unwrap().unwrap();
            true
        }
        _ = tokio::time::sleep(Duration::from_millis(25)) => false,
    };
    create_gate.release();
    let created = create_task.await.unwrap().unwrap();
    if !shutdown_finished_while_blocked {
        shutdown_task.await.unwrap().unwrap();
    }

    assert!(
        !shutdown_finished_while_blocked,
        "shutdown completed while create_run was blocked"
    );
    let interrupted = service.get_run(&created.run_id).await.unwrap();
    assert_eq!(interrupted.status, RunStatus::Interrupted);
    assert_eq!(interrupted.error_code.as_deref(), Some("RUN_INTERRUPTED"));
    assert_eq!(repository.creates.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_after_durable_create_finalizes_before_detached_launch() {
    let get_gate = RepositoryGate::new();
    let (service, repository) = service_with_repository_hooks(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: None,
            get_run: Some(Arc::clone(&get_gate)),
            recover_run: None,
        },
    )
    .await
    .unwrap();

    let creator_service = service.clone();
    let create_task = tokio::spawn(async move {
        creator_service
            .create_detached("fast", json!({}), RequestMetadata::default())
            .await
    });
    get_gate.wait_entered().await;

    let shutdown_service = service.clone();
    let mut shutdown_task =
        tokio::spawn(async move { shutdown_service.shutdown(Duration::from_secs(1)).await });
    tokio::select! {
        result = &mut shutdown_task => panic!("shutdown completed while get_run was blocked: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
    }

    get_gate.release();
    let created = create_task.await.unwrap().unwrap();
    shutdown_task.await.unwrap().unwrap();
    let interrupted = wait_for_status(&service, &created.run_id, RunStatus::Interrupted).await;

    assert_eq!(interrupted.started_at, None);
    assert_eq!(interrupted.error_code.as_deref(), Some("RUN_INTERRUPTED"));
    assert_eq!(
        run_event_types(&repository, &created.run_id).await,
        vec![RunEventType::RunCreated, RunEventType::RunInterrupted]
    );
}

#[tokio::test]
async fn dropped_detached_create_future_releases_capacity_after_durable_create() {
    let get_gate = RepositoryGate::new();
    let (service, repository) = service_with_repository_hooks(
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(3600),
        },
        vec![agent("fast", ServiceBehavior::Complete)],
        RepositoryHooks {
            create_run: None,
            get_run: Some(Arc::clone(&get_gate)),
            recover_run: None,
        },
    )
    .await
    .unwrap();

    let creator_service = service.clone();
    let create_task = tokio::spawn(async move {
        creator_service
            .create_detached("fast", json!({}), RequestMetadata::default())
            .await
    });
    get_gate.wait_entered().await;
    create_task.abort();
    let _ = create_task.await;
    get_gate.release();

    let run_id = loop {
        if let Some(run_id) = repository.records.lock().await.keys().next().cloned() {
            break run_id;
        }
        tokio::task::yield_now().await;
    };
    wait_for_status(&service, &run_id, RunStatus::Interrupted).await;

    let next = service
        .create_detached("fast", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(&service, &next.run_id, RunStatus::Completed).await;
}

#[tokio::test]
async fn startup_reconciliation_and_shutdown_use_distinct_terminal_reasons() {
    let (service, repository) = service(4).await;
    let stale_created = NewRun {
        run_id: "stale_created".to_string(),
        request_id: "req_stale_created".to_string(),
        agent_id: "blocking".to_string(),
        agent_version: "sha256:blocking".to_string(),
        attachment: insight_agent_platform::history::types::RunAttachment::Detached,
        created_at: Utc::now(),
        input_summary: json!({"keys":[], "serialized_bytes":2}),
    };
    repository.create_run(stale_created).await.unwrap();
    assert_eq!(service.reconcile_startup().await.unwrap(), 1);
    assert_eq!(
        repository
            .get_run("stale_created")
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Interrupted
    );

    let attached = service
        .create_attached("blocking", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let attached_id = attached.run_id.clone();
    let detached = service
        .create_detached("blocking", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    wait_for_status(&service, &attached_id, RunStatus::Running).await;
    wait_for_status(&service, &detached.run_id, RunStatus::Running).await;

    tokio::time::timeout(
        Duration::from_secs(2),
        service.shutdown(Duration::from_secs(1)),
    )
    .await
    .expect("shutdown completion notification must not be lost")
    .unwrap();

    assert_eq!(
        service.get_run(&attached_id).await.unwrap().status,
        RunStatus::Cancelled
    );
    assert_eq!(
        service.get_run(&detached.run_id).await.unwrap().status,
        RunStatus::Interrupted
    );
}
