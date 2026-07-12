use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_agent_platform::{
    dsl::{
        compiled::{
            CompiledAgent, CompiledNode, NextPolicy, NodeCompilation, NodeControl,
            NodeEnvelopeRules, NodeOutcome, NodeTransition,
        },
        compiler::{AgentCompiler, CompileContext, CompileLimits},
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
    nodes::{
        default_node_registries,
        registry::{NodeExecutor, NodeExecutorRegistry, NodeType, NodeTypeRegistry},
    },
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{
        stop_pair, CompiledAgentRegistry, ExecutionControl, RequestMetadata, RunContext, RunError,
        RunMetadata, RunService, RunServiceConfig,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConstantConfig {
    value: Value,
    #[serde(default)]
    references: Vec<String>,
}

#[derive(Debug)]
struct ConstantBody {
    value: Value,
}

#[derive(Debug, Clone, Copy)]
struct ConstantNode;

#[derive(Debug)]
struct WrongBodyConfig;

#[derive(Debug, Clone, Copy)]
struct WrongBodyExecutor;

impl NodeType for ConstantNode {
    fn kind(&self) -> &'static str {
        "test.constant"
    }

    fn compile(
        &self,
        _node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: ConstantConfig = serde_json::from_value(config)
            .map_err(|error| CompileError::new("NODE_CONFIG_INVALID", error.to_string()))?;
        Ok(NodeCompilation {
            body: Arc::new(ConstantBody {
                value: config.value,
            }),
            edges: Vec::new(),
            references: config.references.into_iter().collect(),
            terminal: false,
            control: NodeControl::Ordinary,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit: false,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for ConstantNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        _context: &RunContext,
        _control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        let config = node.body::<ConstantBody>()?;
        Ok(NodeOutcome {
            output: json!({"value": config.value.clone()}),
            transition: NodeTransition::Next,
        })
    }
}

impl NodeType for WrongBodyExecutor {
    fn kind(&self) -> &'static str {
        "test.constant"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        Err(CompileError::new(
            "TEST_ONLY",
            "wrong-body executor is runtime-only",
        ))
    }
}

#[async_trait]
impl NodeExecutor for WrongBodyExecutor {
    async fn execute(
        &self,
        node: &CompiledNode,
        _context: &RunContext,
        _control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        let _wrong = node.body::<WrongBodyConfig>()?;
        Ok(NodeOutcome {
            output: json!({}),
            transition: NodeTransition::Next,
        })
    }
}

fn test_context() -> RunContext {
    RunContext::new(
        RunMetadata {
            run_id: "run_test".to_string(),
            request_id: "req_test".to_string(),
            agent_id: "agent_test".to_string(),
            agent_version: "sha256:test".to_string(),
            started_at: Utc::now(),
        },
        json!({"question":"hello"}),
    )
}

fn test_control() -> ExecutionControl {
    let (_, signal) = stop_pair();
    ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) })
}

#[tokio::test]
async fn registered_node_compiles_and_executes_without_core_changes() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let mut types = NodeTypeRegistry::default();
    let mut executors = NodeExecutorRegistry::default();
    types.register(ConstantNode).unwrap();
    executors.register(ConstantNode).unwrap();

    let compilation = types
        .resolve("test.constant")
        .unwrap()
        .compile("constant", json!({"value":42}), &mut compile_context)
        .unwrap();
    assert_eq!(compilation.edges, Vec::<String>::new());
    assert_eq!(compilation.references, BTreeSet::new());
    assert!(!compilation.terminal);
    assert_eq!(compilation.envelope.next, NextPolicy::Required);
    assert_eq!(compilation.control, NodeControl::Ordinary);
    assert_eq!(NodeTransition::ActivateFork, NodeTransition::ActivateFork);

    let node = CompiledNode {
        id: "constant".to_string(),
        kind: "test.constant".to_string(),
        next: Some("result".to_string()),
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: compilation.body,
        edges: vec!["result".to_string()],
        references: compilation.references,
        terminal: compilation.terminal,
        control: NodeControl::Ordinary,
    };
    let outcome = executors
        .resolve("test.constant")
        .unwrap()
        .execute(&node, &test_context(), &test_control())
        .await
        .unwrap();

    assert_eq!(outcome.output, json!({"value":42}));
    assert_eq!(outcome.transition, NodeTransition::Next);
}

#[test]
fn node_registries_reject_duplicate_kinds() {
    let mut types = NodeTypeRegistry::default();
    types.register(ConstantNode).unwrap();
    assert_eq!(
        types.register(ConstantNode).unwrap_err().code(),
        "DUPLICATE_NODE_TYPE"
    );

    let mut executors = NodeExecutorRegistry::default();
    executors.register(ConstantNode).unwrap();
    assert_eq!(
        executors.register(ConstantNode).unwrap_err().code(),
        "DUPLICATE_NODE_EXECUTOR"
    );
}

#[test]
fn compiled_node_rejects_wrong_body_type() {
    let node = CompiledNode {
        id: "constant".to_string(),
        kind: "test.constant".to_string(),
        next: Some("result".to_string()),
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: Arc::new("wrong body".to_string()),
        edges: vec!["result".to_string()],
        references: BTreeSet::new(),
        terminal: false,
        control: NodeControl::Ordinary,
    };

    assert_eq!(
        node.body::<ConstantBody>().unwrap_err().code(),
        "NODE_BODY_TYPE_MISMATCH"
    );
}

#[test]
fn run_context_exposes_only_formal_template_roots() {
    let mut context = test_context();
    context.set_node_output("prior", json!({"text":"done"}));
    let data = context.template_data();

    assert_eq!(data["input"]["question"], "hello");
    assert_eq!(data["run"]["id"], "run_test");
    assert_eq!(data["nodes"]["prior"]["output"]["text"], "done");
    assert_eq!(data.as_object().unwrap().len(), 3);
}

#[derive(Default)]
struct ExtensionRepository {
    records: Mutex<BTreeMap<String, RunRecord>>,
    events: Mutex<BTreeMap<String, Vec<RunEvent>>>,
    outputs: Mutex<Vec<NodeOutputRecord>>,
}

impl ExtensionRepository {
    async fn events_for(&self, run_id: &str) -> Vec<RunEvent> {
        self.events
            .lock()
            .await
            .get(run_id)
            .cloned()
            .unwrap_or_default()
    }

    async fn outputs_for(&self, run_id: &str) -> Vec<NodeOutputRecord> {
        self.outputs
            .lock()
            .await
            .iter()
            .filter(|output| output.run_id == run_id)
            .cloned()
            .collect()
    }
}

#[async_trait]
impl RunRepository for ExtensionRepository {
    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
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

fn extension_compiler() -> AgentCompiler {
    let (mut node_types, _) = default_node_registries().unwrap();
    node_types.register(ConstantNode).unwrap();
    AgentCompiler::new(
        node_types,
        ModelRegistry::default(),
        ActionRegistry::default(),
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 32,
        },
    )
}

fn write_agent(yaml: &str) -> (TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("agent");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("agent.yaml"), yaml).unwrap();
    (temp, root)
}

fn compile_extension_agent(yaml: &str) -> Arc<CompiledAgent> {
    let (_temp, root) = write_agent(yaml);
    Arc::new(extension_compiler().compile_dir(&root).unwrap())
}

fn assert_extension_compile_error(yaml: &str, code: &'static str) {
    let (_temp, root) = write_agent(yaml);
    let error = extension_compiler().compile_dir(&root).unwrap_err();
    assert_eq!(error.code(), code, "unexpected error: {error}");
}

fn extension_success_yaml() -> &'static str {
    r#"
version: 1
id: extension_agent
name: Extension Agent
input:
  schema:
    type: object
entry: constant
nodes:
  constant:
    type: test.constant
    next: result
    config:
      value: 42
  result:
    type: core.output
    config:
      content:
        template: "value={{ nodes.constant.output.value }}"
      format: text
      data:
        value: "{{ nodes.constant.output.value }}"
"#
}

fn default_extension_executors() -> NodeExecutorRegistry {
    let (_, mut executors) = default_node_registries().unwrap();
    executors.register(ConstantNode).unwrap();
    executors
}

fn executors_without_constant() -> NodeExecutorRegistry {
    let (_, executors) = default_node_registries().unwrap();
    executors
}

fn executors_with_wrong_body() -> NodeExecutorRegistry {
    let (_, mut executors) = default_node_registries().unwrap();
    executors.register(WrongBodyExecutor).unwrap();
    executors
}

fn run_service_config() -> RunServiceConfig {
    RunServiceConfig {
        max_concurrent_runs: 4,
        max_parallel_node_executions: 8,
        max_parallel_branches_per_run: 4,
        run_timeout: Duration::from_secs(30),
    }
}

fn service_for(
    agent: Arc<CompiledAgent>,
    executors: NodeExecutorRegistry,
) -> (RunService, Arc<ExtensionRepository>) {
    let repository = Arc::new(ExtensionRepository::default());
    let repository_trait: Arc<dyn RunRepository> = repository.clone();
    let events = EventHub::new(
        Arc::clone(&repository_trait),
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    );
    let agents = CompiledAgentRegistry::new(vec![agent]).unwrap();
    let service = RunService::new(
        agents,
        executors,
        repository_trait,
        events,
        run_service_config(),
    )
    .unwrap();
    (service, repository)
}

async fn wait_for_status(service: &RunService, run_id: &str, expected: RunStatus) -> RunRecord {
    let mut last = None;
    for _ in 0..200 {
        let record = service.get_run(run_id).await.unwrap();
        last = Some(record.status);
        if record.status == expected {
            return record;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("run {run_id} did not reach {expected:?}; last status: {last:?}");
}

async fn event_types(repository: &ExtensionRepository, run_id: &str) -> Vec<RunEventType> {
    repository
        .events_for(run_id)
        .await
        .into_iter()
        .map(|event| event.event_type)
        .collect()
}

#[tokio::test]
async fn custom_node_runs_through_compiler_service_events_repository_and_terminal() {
    let agent = compile_extension_agent(extension_success_yaml());
    let (service, repository) = service_for(agent, default_extension_executors());

    let created = service
        .create_detached(
            "extension_agent",
            json!({"question":"hello"}),
            RequestMetadata::default(),
        )
        .await
        .unwrap();

    let completed = wait_for_status(&service, &created.run_id, RunStatus::Completed).await;
    let output = completed.output.unwrap();
    assert_eq!(output.content.as_deref(), Some("value=42"));
    assert_eq!(output.format.as_deref(), Some("text"));
    assert_eq!(output.data, json!({"value":"42"}));

    let outputs = repository.outputs_for(&created.run_id).await;
    assert!(outputs
        .iter()
        .any(|output| { output.node_id == "constant" && output.output == json!({"value":42}) }));

    assert_eq!(
        event_types(&repository, &created.run_id).await,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::NodeCompleted,
            RunEventType::NodeStarted,
            RunEventType::NodeCompleted,
            RunEventType::RunCompleted,
        ]
    );
    let events = repository.events_for(&created.run_id).await;
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    assert!(events.iter().any(|event| {
        event.event_type == RunEventType::NodeStarted
            && event.node_id.as_deref() == Some("constant")
            && event.data == json!({"type":"test.constant"})
    }));
}

#[tokio::test]
async fn custom_node_missing_executor_terminalizes_as_infrastructure_failure() {
    let agent = compile_extension_agent(extension_success_yaml());
    let (service, repository) = service_for(agent, executors_without_constant());

    let created = service
        .create_detached("extension_agent", json!({}), RequestMetadata::default())
        .await
        .unwrap();

    let failed = wait_for_status(&service, &created.run_id, RunStatus::Failed).await;
    assert_eq!(failed.error_code.as_deref(), Some("INFRASTRUCTURE_FAILURE"));
    assert_eq!(
        event_types(&repository, &created.run_id).await,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::RunFailed,
        ]
    );
}

#[tokio::test]
async fn custom_node_body_mismatch_terminalizes_as_node_failure() {
    let agent = compile_extension_agent(extension_success_yaml());
    let (service, repository) = service_for(agent, executors_with_wrong_body());

    let created = service
        .create_detached("extension_agent", json!({}), RequestMetadata::default())
        .await
        .unwrap();

    let failed = wait_for_status(&service, &created.run_id, RunStatus::Failed).await;
    assert_eq!(
        failed.error_code.as_deref(),
        Some("NODE_BODY_TYPE_MISMATCH")
    );
    assert_eq!(
        event_types(&repository, &created.run_id).await,
        vec![
            RunEventType::RunCreated,
            RunEventType::RunStarted,
            RunEventType::NodeStarted,
            RunEventType::NodeFailed,
            RunEventType::RunFailed,
        ]
    );
    assert!(repository
        .outputs_for(&created.run_id)
        .await
        .into_iter()
        .all(|output| output.node_id != "constant"));
}

#[test]
fn custom_node_required_next_is_enforced_by_agent_compiler() {
    assert_extension_compile_error(
        r#"
version: 1
id: extension_agent
name: Extension Agent
input:
  schema: {type: object}
entry: constant
nodes:
  constant:
    type: test.constant
    config:
      value: 42
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "NODE_NEXT_REQUIRED",
    );
}

#[test]
fn custom_node_references_use_shared_graph_validation() {
    assert_extension_compile_error(
        r#"
version: 1
id: extension_agent
name: Extension Agent
input:
  schema: {type: object}
entry: constant
nodes:
  constant:
    type: test.constant
    next: result
    config:
      value: 42
      references: [result]
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "INVALID_NODE_REFERENCE",
    );
}

#[test]
fn custom_node_content_emit_requires_envelope_support() {
    assert_extension_compile_error(
        r#"
version: 1
id: extension_agent
name: Extension Agent
input:
  schema: {type: object}
entry: constant
nodes:
  constant:
    type: test.constant
    emit: content
    next: result
    config:
      value: 42
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "NODE_EMIT_UNSUPPORTED",
    );
}
