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
            CompiledAgent, CompiledNode, NodeCompilation, NodeOutcome, NodeTransition, RunOutput,
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
    runtime::{
        CompiledAgentRegistry, ExecutionControl, RequestMetadata, RunContext, RunError, RunService,
        RunServiceConfig, ServiceError,
    },
};
use jsonschema::JSONSchema;
use serde_json::{json, Value};
use tokio::sync::Mutex;

const GRACE: Duration = Duration::from_secs(10);

enum ServiceBehavior {
    Complete,
    Block,
}

struct ServiceNode;

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
                transition: NodeTransition::Complete(RunOutput {
                    content: Some("done".to_string()),
                    format: Some("text".to_string()),
                    data: json!({"done":true}),
                }),
            }),
            ServiceBehavior::Block => {
                control.stopped().await;
                Err(RunError::stopped(control.stop_reason().unwrap()))
            }
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
        terminal: true,
    };
    Arc::new(CompiledAgent {
        id: id.to_string(),
        name: id.to_string(),
        description: String::new(),
        version_hash: format!("sha256:{id}"),
        input_schema: Arc::new(JSONSchema::compile(&json!({"type":"object"})).unwrap()),
        entry: "work".to_string(),
        nodes: BTreeMap::from([("work".to_string(), node)]),
        templates: Arc::new(Handlebars::new()),
    })
}

struct CountingRepository {
    records: Mutex<BTreeMap<String, RunRecord>>,
    events: Mutex<BTreeMap<String, Vec<RunEvent>>>,
    outputs: Mutex<Vec<NodeOutputRecord>>,
    creates: AtomicUsize,
    fail_appends: AtomicBool,
}

#[async_trait]
impl RunRepository for CountingRepository {
    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
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
        Ok(self.records.lock().await.get(run_id).cloned())
    }

    async fn list_events_after(
        &self,
        run_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError> {
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
    let repository = Arc::new(CountingRepository {
        records: Mutex::new(BTreeMap::new()),
        events: Mutex::new(BTreeMap::new()),
        outputs: Mutex::new(Vec::new()),
        creates: AtomicUsize::new(0),
        fail_appends: AtomicBool::new(false),
    });
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
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
    let agents = CompiledAgentRegistry::new(vec![
        agent("blocking", ServiceBehavior::Block),
        agent("fast", ServiceBehavior::Complete),
    ])
    .unwrap();
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
        attached_reconnect_grace: GRACE,
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

#[tokio::test]
async fn zero_parallel_capacities_are_rejected() {
    for config in [
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 0,
            max_parallel_branches_per_run: 8,
            run_timeout: Duration::from_secs(1),
            attached_reconnect_grace: Duration::from_secs(1),
        },
        RunServiceConfig {
            max_concurrent_runs: 1,
            max_parallel_node_executions: 32,
            max_parallel_branches_per_run: 0,
            run_timeout: Duration::from_secs(1),
            attached_reconnect_grace: Duration::from_secs(1),
        },
    ] {
        let error = service_with_config(config).await.err().unwrap();
        assert_eq!(error.code(), "RUN_SERVICE_CONFIG_INVALID");
    }
}

#[tokio::test]
async fn attached_run_disconnect_uses_reconnect_grace_then_cancels() {
    let (service, _) = service(2).await;
    let attached = service
        .create_attached("blocking", json!({}), RequestMetadata::default())
        .await
        .unwrap();
    let run_id = attached.run_id.clone();
    wait_for_status(&service, &run_id, RunStatus::Running).await;
    tokio::time::pause();

    drop(attached.subscription);
    tokio::time::advance(Duration::ZERO).await;
    tokio::time::advance(GRACE - Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        service.get_run(&run_id).await.unwrap().status,
        RunStatus::Running
    );

    let reconnected = service.subscribe(&run_id, 0).await.unwrap();
    tokio::time::advance(GRACE + Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        service.get_run(&run_id).await.unwrap().status,
        RunStatus::Running
    );

    drop(reconnected);
    tokio::time::advance(Duration::ZERO).await;
    tokio::time::advance(GRACE + Duration::from_millis(1)).await;
    wait_for_status(&service, &run_id, RunStatus::Cancelled).await;
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
