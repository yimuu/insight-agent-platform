use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
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
            CompiledAgent, CompiledNode, ControlEdge, ExecutionPlan, NodeCompilation, NodeControl,
            NodeOutcome, NodeTransition,
        },
        compiler::CompileContext,
        compiler::{AgentCompiler, CompileLimits},
        CompileError, EmitPolicy,
    },
    events::{
        hub::{EventHub, EventHubConfig},
        protocol::{RunEvent, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{NewRun, NodeOutputRecord, RunRecord, TerminalUpdate},
    },
    nodes::{
        default_node_registries,
        registry::{NodeExecutor, NodeExecutorRegistry, NodeType},
    },
    outcome::{RunOutput, TerminalOutcome},
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{
        execute_node, stop_pair, ExecutionControl, ExecutionLimiter, NodeExecutionFailure,
        RunContext, RunError, RunErrorKind, RunMetadata, Scheduler, SchedulerResult, StopReason,
    },
    schema::compile_schema,
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
    BarrierNext {
        output: Value,
        content: Option<&'static str>,
        executions: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
        started: Arc<Notify>,
        release_open: Arc<AtomicBool>,
        release: Arc<Notify>,
    },
    Fail {
        code: &'static str,
        message: &'static str,
        executions: Arc<AtomicUsize>,
    },
    Infrastructure {
        executions: Arc<AtomicUsize>,
    },
    Stop {
        reason: StopReason,
        executions: Arc<AtomicUsize>,
    },
    ReturnedStopAfterRuntimeStop {
        returned: StopReason,
        executions: Arc<AtomicUsize>,
        started: Arc<Notify>,
    },
    WaitForever {
        executions: Arc<AtomicUsize>,
    },
    WaitForStop {
        executions: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        started: Arc<Notify>,
        stopped: Arc<AtomicUsize>,
    },
    NextAfter {
        executions: Arc<AtomicUsize>,
        sibling_started: Arc<Notify>,
        output: Value,
    },
    GotoAfter {
        executions: Arc<AtomicUsize>,
        sibling_started: Arc<Notify>,
        target: String,
    },
    InfrastructureAfter {
        executions: Arc<AtomicUsize>,
        sibling_started: Arc<Notify>,
    },
    PanicAfter {
        executions: Arc<AtomicUsize>,
        sibling_started: Arc<Notify>,
    },
    CompleteAfter {
        predecessor: String,
        expected: Value,
        output: RunOutput,
        executions: Arc<AtomicUsize>,
    },
    IsolatedNext {
        predecessor: String,
        expected: Value,
        absent: String,
        output: Value,
        executions: Arc<AtomicUsize>,
    },
}

struct SchedulerExecutor;

struct ActiveExecution(Arc<AtomicUsize>);

impl Drop for ActiveExecution {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

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
        control: &ExecutionControl,
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
                    transition: NodeTransition::End(TerminalOutcome::Success {
                        output: output.clone(),
                    }),
                })
            }
            SchedulerBehavior::BarrierNext {
                output,
                content,
                executions,
                in_flight,
                maximum,
                started,
                release_open,
                release,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(current, Ordering::SeqCst);
                if let Some(content) = content {
                    control.emit_content(*content).await?;
                }
                let notified = release.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if !release_open.load(Ordering::SeqCst) {
                    started.notify_one();
                    notified.await;
                }
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(NodeOutcome {
                    output: output.clone(),
                    transition: NodeTransition::Next,
                })
            }
            SchedulerBehavior::IsolatedNext {
                predecessor,
                expected,
                absent,
                output,
                executions,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                assert!(context.branch_results().is_none());
                assert_eq!(context.node_output(predecessor), Some(expected));
                assert!(context.node_output(absent).is_none());
                Ok(NodeOutcome {
                    output: output.clone(),
                    transition: NodeTransition::Next,
                })
            }
            SchedulerBehavior::Fail {
                code,
                message,
                executions,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                Err(RunError::new(code, *message))
            }
            SchedulerBehavior::Infrastructure { executions } => {
                executions.fetch_add(1, Ordering::SeqCst);
                Err(RunError::infrastructure(
                    "SYNTHETIC_INFRASTRUCTURE",
                    "synthetic infrastructure failure",
                ))
            }
            SchedulerBehavior::Stop { reason, executions } => {
                executions.fetch_add(1, Ordering::SeqCst);
                Err(RunError::stopped(*reason))
            }
            SchedulerBehavior::ReturnedStopAfterRuntimeStop {
                returned,
                executions,
                started,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                control.stopped().await;
                Err(RunError::stopped(*returned))
            }
            SchedulerBehavior::WaitForever { executions } => {
                executions.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
            SchedulerBehavior::WaitForStop {
                executions,
                active,
                started,
                stopped,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                active.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveExecution(Arc::clone(active));
                started.notify_one();
                control.stopped().await;
                stopped.fetch_add(1, Ordering::SeqCst);
                Err(RunError::stopped(control.stop_reason().unwrap()))
            }
            SchedulerBehavior::NextAfter {
                executions,
                sibling_started,
                output,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                sibling_started.notified().await;
                Ok(NodeOutcome {
                    output: output.clone(),
                    transition: NodeTransition::Next,
                })
            }
            SchedulerBehavior::GotoAfter {
                executions,
                sibling_started,
                target,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                sibling_started.notified().await;
                Ok(NodeOutcome {
                    output: json!({}),
                    transition: NodeTransition::Goto(target.clone()),
                })
            }
            SchedulerBehavior::InfrastructureAfter {
                executions,
                sibling_started,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                sibling_started.notified().await;
                Err(RunError::infrastructure(
                    "SYNTHETIC_INFRASTRUCTURE",
                    "synthetic infrastructure failure",
                ))
            }
            SchedulerBehavior::PanicAfter {
                executions,
                sibling_started,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                sibling_started.notified().await;
                panic!("synthetic executor panic")
            }
            SchedulerBehavior::CompleteAfter {
                predecessor,
                expected,
                output,
                executions,
            } => {
                executions.fetch_add(1, Ordering::SeqCst);
                assert!(context.branch_results().is_none());
                assert_eq!(context.node_output(predecessor), Some(expected));
                Ok(NodeOutcome {
                    output: json!({"terminal":true}),
                    transition: NodeTransition::End(TerminalOutcome::Success {
                        output: output.clone(),
                    }),
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
    blocked_output_node: Option<String>,
    completed_entered: Option<Arc<Notify>>,
    completed_release: Option<Arc<Notify>>,
    fail_content_append: AtomicBool,
    fail_append_for: Mutex<Option<(String, String)>>,
    fail_output_for: Mutex<Option<String>>,
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
        let blocks_output = self
            .blocked_output_node
            .as_deref()
            .map_or(true, |node_id| node_id == output.node_id);
        self.outputs.lock().await.push(output);
        if blocks_output {
            if let Some(entered) = &self.output_entered {
                entered.notify_one();
            }
            if let Some(release) = &self.output_release {
                release.notified().await;
            }
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
        edges: next
            .into_iter()
            .map(|target| ControlEdge::Direct {
                target: target.to_string(),
            })
            .collect(),
        references: BTreeSet::new(),
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
        input_schema: Arc::new(compile_schema(&json!({"type":"object"})).unwrap()),
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

fn parallel_scheduler(
    agent: Arc<CompiledAgent>,
    repository: Arc<SchedulerRepository>,
    per_run: usize,
) -> Scheduler {
    let (_, mut executors) = default_node_registries().unwrap();
    executors.register(SchedulerExecutor).unwrap();
    Scheduler::new(
        agent,
        executors,
        event_hub(repository),
        ExecutionLimiter::new(
            Arc::new(Semaphore::new(32)),
            Arc::new(Semaphore::new(per_run)),
        ),
    )
}

fn compile_parallel_agent(yaml: &str) -> CompiledAgent {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("agent");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("agent.yaml"), yaml).unwrap();
    let (types, _) = default_node_registries().unwrap();
    AgentCompiler::new(
        types,
        ModelRegistry::default(),
        ActionRegistry::default(),
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 32,
        },
    )
    .compile_dir(Path::new(&root))
    .unwrap()
}

fn replace_behavior(agent: &mut CompiledAgent, node_id: &str, behavior: SchedulerBehavior) {
    let node = agent.nodes.get_mut(node_id).unwrap();
    node.kind = "test.scheduler".to_string();
    node.body = Arc::new(behavior);
}

fn two_branch_yaml() -> &'static str {
    r#"
version: 1
id: scheduler-agent
name: Scheduler Agent
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {source_a: search_a, source_b: search_b}
      join: collect
  search_a:
    type: core.template
    next: summarize_a
    config: {value: search-a}
  summarize_a:
    type: core.template
    next: end_a
    config: {value: {text: summary-a}}
  end_a:
    type: core.end
    config: {outcome: success, data: {text: "{{ nodes.summarize_a.output.text }}"}}
  search_b:
    type: core.template
    next: summarize_b
    config: {value: search-b}
  summarize_b:
    type: core.template
    next: end_b
    config: {value: {text: summary-b}}
  end_b:
    type: core.end
    config: {outcome: success, data: {text: "{{ nodes.summarize_b.output.text }}"}}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.end
    config: {outcome: success, data: {done: true}}
"#
}

fn structured_end_yaml() -> &'static str {
    include_str!("fixtures/structured_end_agent.yaml")
}

struct StructuredEndFixture {
    result: SchedulerResult,
    join_output: Value,
    events: Vec<RunEvent>,
    operations: Vec<String>,
}

async fn run_structured_end_agent() -> StructuredEndFixture {
    run_structured_agent(
        compile_parallel_agent(structured_end_yaml()),
        "run_structured_end",
    )
    .await
}

async fn run_structured_agent(agent: CompiledAgent, run_id: &str) -> StructuredEndFixture {
    let context = context_with_agent_input(&agent, run_id, json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();
    let result = scheduler.run(context, stop).await.unwrap();
    let join_output = repository
        .outputs
        .lock()
        .await
        .iter()
        .find(|output| output.node_id == "collect")
        .expect("collect output")
        .output
        .clone();
    let events = repository.events.lock().await.clone();
    let operations = repository.operations.lock().await.clone();
    StructuredEndFixture {
        result,
        join_output,
        events,
        operations,
    }
}

#[tokio::test]
async fn explicit_branch_ends_settle_then_activate_join() {
    let fixture = run_structured_end_agent().await;
    assert!(matches!(
        fixture.result,
        SchedulerResult::Ended(TerminalOutcome::Success { .. })
    ));
    assert_eq!(fixture.join_output["summary"]["total"], 2);
    assert_eq!(fixture.join_output["summary"]["succeeded"], 1);
    assert_eq!(fixture.join_output["summary"]["failed"], 1);
    assert_eq!(fixture.join_output["summary"]["failures"]["workflow"], 1);
    assert!(fixture.events.iter().any(|event| {
        event.event_type == RunEventType::NodeCompleted && event.node_id.as_deref() == Some("end_b")
    }));
    assert!(!fixture.events.iter().any(|event| {
        event.event_type == RunEventType::NodeFailed && event.node_id.as_deref() == Some("end_b")
    }));
    let output_position = fixture
        .operations
        .iter()
        .position(|operation| operation == "output:end_b")
        .unwrap();
    let node_completed_position = fixture
        .operations
        .iter()
        .position(|operation| operation == "event:node.completed:end_b")
        .unwrap();
    let branch_failed_position = fixture
        .operations
        .iter()
        .position(|operation| operation == "event:branch.failed:-")
        .unwrap();
    assert!(output_position < node_completed_position);
    assert!(node_completed_position < branch_failed_position);
}

#[tokio::test]
async fn contained_node_error_is_counted_by_typed_origin() {
    let mut agent = compile_parallel_agent(structured_end_yaml());
    replace_behavior(
        &mut agent,
        "make_b",
        SchedulerBehavior::Fail {
            code: "NODE_TIMEOUT_LOOKALIKE",
            message: "ordinary node failure",
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );

    let fixture = run_structured_agent(agent, "run_structured_node_error").await;

    assert_eq!(fixture.join_output["summary"]["failures"]["node"], 1);
    assert_eq!(fixture.join_output["summary"]["failures"]["timeout"], 0);
    assert_eq!(
        fixture.join_output["branches"]["b"]["error"]["kind"],
        "node"
    );
}

#[tokio::test]
async fn contained_node_timeout_is_counted_by_typed_origin() {
    let mut agent = compile_parallel_agent(structured_end_yaml());
    replace_behavior(
        &mut agent,
        "make_b",
        SchedulerBehavior::WaitForever {
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );
    agent.nodes.get_mut("make_b").unwrap().timeout = Duration::from_millis(1);

    let fixture = run_structured_agent(agent, "run_structured_timeout").await;

    assert_eq!(fixture.join_output["summary"]["failures"]["node"], 0);
    assert_eq!(fixture.join_output["summary"]["failures"]["timeout"], 1);
    assert_eq!(
        fixture.join_output["branches"]["b"]["error"]["kind"],
        "timeout"
    );
}

#[tokio::test]
async fn all_workflow_failed_branches_still_run_join_then_main_failure_end() {
    let yaml = structured_end_yaml()
        .replace(
            "    config: {outcome: success, data: {value: \"{{ nodes.make_a.output }}\"}}",
            "    config: {outcome: failure, code: WORKFLOW_A_REJECTED, message: branch a rejected}",
        )
        .replace("    next: result\n    config: {mode: all_settled}", "    next: decide\n    config: {mode: all_settled}")
        .replace(
            "  result:\n    type: core.end\n    config: {outcome: success, data: {summary: \"{{ nodes.collect.output.summary }}\"}}",
            "  decide:\n    type: core.condition\n    config:\n      cases: [{when: \"nodes.collect.output.summary.succeeded == 0\", next: failed}]\n      default: result\n  failed:\n    type: core.end\n    config: {outcome: failure, code: WORKFLOW_ALL_BRANCHES_FAILED, message: all branches failed}\n  result:\n    type: core.end\n    config: {outcome: success, data: {summary: \"{{ nodes.collect.output.summary }}\"}}",
        );

    let fixture =
        run_structured_agent(compile_parallel_agent(&yaml), "run_all_workflow_failed").await;

    assert!(matches!(
        fixture.result,
        SchedulerResult::Ended(TerminalOutcome::Failure { ref error })
            if error.code == "WORKFLOW_ALL_BRANCHES_FAILED"
    ));
    assert_eq!(fixture.join_output["summary"]["succeeded"], 0);
    assert_eq!(fixture.join_output["summary"]["failures"]["workflow"], 2);
    let join_output_position = fixture
        .operations
        .iter()
        .position(|operation| operation == "output:collect")
        .unwrap();
    let failure_end_output_position = fixture
        .operations
        .iter()
        .position(|operation| operation == "output:failed")
        .unwrap();
    assert!(join_output_position < failure_end_output_position);
}

#[tokio::test]
async fn all_node_failed_branches_still_run_join() {
    let mut agent = compile_parallel_agent(structured_end_yaml());
    for (node_id, code) in [("make_a", "NODE_A_FAILED"), ("make_b", "NODE_B_FAILED")] {
        replace_behavior(
            &mut agent,
            node_id,
            SchedulerBehavior::Fail {
                code,
                message: "node failed",
                executions: Arc::new(AtomicUsize::new(0)),
            },
        );
    }

    let fixture = run_structured_agent(agent, "run_all_node_failed").await;

    assert!(matches!(
        fixture.result,
        SchedulerResult::Ended(TerminalOutcome::Success { .. })
    ));
    assert_eq!(fixture.join_output["summary"]["succeeded"], 0);
    assert_eq!(fixture.join_output["summary"]["failures"]["node"], 2);
    assert!(fixture
        .operations
        .iter()
        .any(|operation| operation == "output:result"));
}

#[tokio::test]
async fn partial_success_can_end_as_explicit_degraded_success() {
    let yaml = structured_end_yaml().replace(
        "    config: {outcome: success, data: {summary: \"{{ nodes.collect.output.summary }}\"}}",
        "    config:\n      outcome: success\n      data:\n        summary:\n          total: \"{{ nodes.collect.output.summary.total }}\"\n          succeeded: \"{{ nodes.collect.output.summary.succeeded }}\"\n          failed: \"{{ nodes.collect.output.summary.failed }}\"\n          workflow: \"{{ nodes.collect.output.summary.failures.workflow }}\"",
    );
    let fixture = run_structured_agent(compile_parallel_agent(&yaml), "run_partial_degraded").await;

    let SchedulerResult::Ended(TerminalOutcome::Success { output }) = fixture.result else {
        panic!("partial success must follow the authored success End")
    };
    assert_eq!(
        output.data["summary"],
        json!({"total":"2", "succeeded":"1", "failed":"1", "workflow":"1"})
    );
}

fn select_yaml() -> &'static str {
    r#"
version: 1
id: scheduler-select
name: Scheduler Select
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases: [{when: "input.kind == 'medical'", next: medical}]
      default: general
  medical:
    type: core.template
    next: selected
    config: {value: {text: medical-answer}}
  general:
    type: core.template
    next: selected
    config: {value: {text: general-answer}}
  selected:
    type: core.select
    next: result
    config: {sources: [medical, general]}
  result:
    type: core.end
    config:
      outcome: success
      data:
        source: "{{ nodes.selected.output.source_node_id }}"
        answer: "{{ nodes.selected.output.value.text }}"
"#
}

fn branch_select_yaml() -> &'static str {
    r#"
version: 1
id: scheduler-branch-select
name: Scheduler Branch Select
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {choice: route, fixed: fixed}
      join: collect
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: left}]
      default: right
  left:
    type: core.template
    next: branch_select
    config: {value: left}
  right:
    type: core.template
    next: branch_select
    config: {value: right}
  branch_select:
    type: core.select
    next: end_choice
    config: {sources: [left, right]}
  end_choice:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.branch_select.output }}"}}
  fixed:
    type: core.template
    next: end_fixed
    config: {value: fixed}
  end_fixed:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.fixed.output }}"}}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.end
    config: {outcome: success, data: {done: true}}
"#
}

fn context_with_input(run_id: &str, input: Value) -> RunContext {
    RunContext::new(
        RunMetadata {
            run_id: run_id.to_string(),
            request_id: format!("req_{run_id}"),
            agent_id: "scheduler-agent".to_string(),
            agent_version: "sha256:scheduler".to_string(),
            started_at: Utc::now(),
        },
        input,
    )
}

fn context(run_id: &str) -> RunContext {
    context_with_input(run_id, json!({}))
}

fn context_with_agent_input(agent: &CompiledAgent, run_id: &str, input: Value) -> RunContext {
    context_with_input(run_id, input).with_templates(Arc::clone(&agent.templates))
}

fn event_hub(repository: Arc<SchedulerRepository>) -> EventHub {
    let repository: Arc<dyn RunRepository> = repository;
    EventHub::new(
        repository,
        EventHubConfig {
            subscriber_capacity: 8,
            journal_capacity: 32,
            journal_batch_size: 8,
            operation_timeout: Duration::from_secs(1),
        },
    )
}

async fn assert_no_branch_result_or_join_output(repository: &SchedulerRepository, join_id: &str) {
    assert!(!repository.events.lock().await.iter().any(|event| {
        matches!(
            event.event_type,
            RunEventType::BranchCompleted | RunEventType::BranchFailed
        )
    }));
    assert!(!repository
        .outputs
        .lock()
        .await
        .iter()
        .any(|output| output.node_id == join_id));
}

#[tokio::test]
async fn select_scheduler_runs_only_the_chosen_path_and_persists_stable_output() {
    for (kind, selected, unselected, answer) in [
        ("medical", "medical", "general", "medical-answer"),
        ("general", "general", "medical", "general-answer"),
    ] {
        let agent = compile_parallel_agent(select_yaml());
        let context =
            context_with_agent_input(&agent, &format!("run_select_{kind}"), json!({"kind":kind}));
        let repository = Arc::new(SchedulerRepository::default());
        let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
        let (_, stop) = stop_pair();

        assert_eq!(
            scheduler.run(context, stop).await.unwrap(),
            SchedulerResult::Ended(TerminalOutcome::Success {
                output: RunOutput {
                    content: None,
                    format: None,
                    data: json!({"source":selected, "answer":answer}),
                },
            })
        );

        let outputs = repository.outputs.lock().await.clone();
        assert_eq!(
            outputs
                .iter()
                .find(|output| output.node_id == "selected")
                .unwrap()
                .output,
            json!({
                "source_node_id": selected,
                "value": {"text": answer},
            })
        );
        assert!(outputs.iter().any(|output| output.node_id == selected));
        assert!(!outputs.iter().any(|output| output.node_id == unselected));

        let events = repository.events.lock().await.clone();
        assert!(events.iter().any(|event| {
            event.event_type.as_str() == "node.completed"
                && event.node_id.as_deref() == Some("selected")
        }));
        assert!(!events.iter().any(|event| {
            event.event_type.as_str() == "node.started"
                && event.node_id.as_deref() == Some(unselected)
        }));
        let operations = repository.operations.lock().await.clone();
        let output_position = operations
            .iter()
            .position(|operation| operation == "output:selected")
            .unwrap();
        let completed_position = operations
            .iter()
            .position(|operation| operation == "event:node.completed:selected")
            .unwrap();
        assert!(output_position < completed_position);
    }
}

#[tokio::test]
async fn select_missing_source_outside_a_fork_fails_the_run() {
    let mut agent = compile_parallel_agent(select_yaml());
    agent.nodes.get_mut("selected").unwrap().body =
        Arc::new(BTreeSet::from(["never_completed".to_string()]));
    let context = context_with_agent_input(&agent, "run_select_missing", json!({"kind":"medical"}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();

    let SchedulerResult::Failed(error) = scheduler.run(context, stop).await.unwrap() else {
        panic!("missing Select source must fail the main Run");
    };
    assert_eq!(error.code(), "SELECT_SOURCE_MISSING");
    assert!(repository.events.lock().await.iter().any(|event| {
        event.event_type.as_str() == "node.failed" && event.node_id.as_deref() == Some("selected")
    }));
}

#[tokio::test]
async fn select_missing_source_inside_a_fork_settles_only_that_branch() {
    let mut agent = compile_parallel_agent(branch_select_yaml());
    agent.nodes.get_mut("branch_select").unwrap().body =
        Arc::new(BTreeSet::from(["never_completed".to_string()]));
    let context = context_with_agent_input(&agent, "run_branch_select_missing", json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();

    assert_eq!(
        scheduler.run(context, stop).await.unwrap(),
        SchedulerResult::Ended(TerminalOutcome::Success {
            output: RunOutput {
                content: None,
                format: None,
                data: json!({"done":true}),
            },
        })
    );
    assert_eq!(
        repository
            .outputs
            .lock()
            .await
            .iter()
            .find(|output| output.node_id == "collect")
            .unwrap()
            .output["summary"],
        json!({
            "total":2,
            "succeeded":1,
            "failed":1,
            "failures":{"workflow":0,"node":1,"timeout":0}
        })
    );
    let events = repository.events.lock().await.clone();
    assert!(events.iter().any(|event| {
        event.event_type.as_str() == "node.failed"
            && event.node_id.as_deref() == Some("branch_select")
            && event.code == "SELECT_SOURCE_MISSING"
    }));
    assert!(events.iter().any(|event| {
        event.event_type.as_str() == "branch.failed" && event.data["branch_id"] == "choice"
    }));
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
        SchedulerResult::Ended(TerminalOutcome::Success {
            output: final_output
        })
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
        SchedulerResult::Ended(TerminalOutcome::Success { output })
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
    assert_eq!(error.code(), "INFRASTRUCTURE_FAILURE");
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
    assert_eq!(error.code(), "INFRASTRUCTURE_FAILURE");
    assert_eq!(error.kind(), RunErrorKind::Infrastructure);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parallel_scheduler_overlaps_compiled_branches_and_waits_before_join() {
    let mut agent = compile_parallel_agent(two_branch_yaml());
    let in_flight = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release_open = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Notify::new());
    let counts = [
        "search_a",
        "summarize_a",
        "search_b",
        "summarize_b",
        "result",
    ]
    .into_iter()
    .map(|id| (id, Arc::new(AtomicUsize::new(0))))
    .collect::<BTreeMap<_, _>>();
    for (id, output) in [("search_a", "search a"), ("search_b", "search b")] {
        replace_behavior(
            &mut agent,
            id,
            SchedulerBehavior::BarrierNext {
                output: json!({"text":output}),
                content: Some(output),
                executions: Arc::clone(&counts[id]),
                in_flight: Arc::clone(&in_flight),
                maximum: Arc::clone(&maximum),
                started: Arc::clone(&started),
                release_open: Arc::clone(&release_open),
                release: Arc::clone(&release),
            },
        );
        agent.nodes.get_mut(id).unwrap().emit = EmitPolicy::Content;
    }
    for (id, predecessor, absent, output) in [
        ("summarize_a", "search_a", "search_b", "result a"),
        ("summarize_b", "search_b", "search_a", "result b"),
    ] {
        replace_behavior(
            &mut agent,
            id,
            SchedulerBehavior::IsolatedNext {
                predecessor: predecessor.to_string(),
                expected: json!({"text":format!("search {}", &predecessor[7..])}),
                absent: absent.to_string(),
                output: json!({"text":output}),
                executions: Arc::clone(&counts[id]),
            },
        );
    }
    let expected_join = json!({
        "branches": {
            "source_a": {"status":"succeeded", "terminal_node_id":"end_a", "output":{"data":{"text":"result a"}}},
            "source_b": {"status":"succeeded", "terminal_node_id":"end_b", "output":{"data":{"text":"result b"}}}
        },
        "summary":{"total":2,"succeeded":2,"failed":0,"failures":{"workflow":0,"node":0,"timeout":0}}
    });
    let final_output = RunOutput {
        content: None,
        format: None,
        data: json!({"done":true}),
    };
    replace_behavior(
        &mut agent,
        "result",
        SchedulerBehavior::CompleteAfter {
            predecessor: "collect".to_string(),
            expected: expected_join.clone(),
            output: final_output.clone(),
            executions: Arc::clone(&counts["result"]),
        },
    );
    let run_context = context_with_agent_input(&agent, "run_parallel", json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();
    let mut execution = tokio::spawn(async move { scheduler.run(run_context, stop).await });

    while in_flight.load(Ordering::SeqCst) < 2 {
        let notified = started.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if in_flight.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::select! {
            _ = &mut notified => {}
            result = &mut execution => panic!("scheduler ended before branch barrier: {result:?}"),
        }
    }
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    release_open.store(true, Ordering::SeqCst);
    release.notify_waiters();

    assert_eq!(
        execution.await.unwrap().unwrap(),
        SchedulerResult::Ended(TerminalOutcome::Success {
            output: final_output
        })
    );
    for id in [
        "search_a",
        "summarize_a",
        "search_b",
        "summarize_b",
        "result",
    ] {
        assert_eq!(counts[id].load(Ordering::SeqCst), 1, "{id} execution count");
    }
    let events = repository.events.lock().await.clone();
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    let position = |kind: &str, id: &str| {
        events
            .iter()
            .position(|event| {
                event.event_type.as_str() == kind
                    && (event.node_id.as_deref() == Some(id) || event.data["branch_id"] == id)
            })
            .unwrap()
    };
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type.as_str() == "branch.started")
            .map(|event| event.data["branch_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["source_a", "source_b"]
    );
    let fanout_completed = position("node.completed", "fanout");
    let collect_started = position("node.started", "collect");
    for (branch_id, terminal_node_id) in [("source_a", "end_a"), ("source_b", "end_b")] {
        let branch_started = position("branch.started", branch_id);
        let node_terminal = position("node.completed", terminal_node_id);
        let branch_terminal = position("branch.completed", branch_id);
        assert!(fanout_completed < branch_started);
        assert!(branch_started < node_terminal);
        assert!(node_terminal < branch_terminal);
        assert!(branch_terminal < collect_started);
    }
    assert!(position("node.started", "collect") < position("node.completed", "collect"));
    let sibling_content = events
        .iter()
        .filter(|event| event.event_type.as_str() == "content.delta")
        .map(|event| event.node_id.as_deref().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(sibling_content, BTreeSet::from(["search_a", "search_b"]));
}

#[tokio::test]
async fn parallel_scheduler_limits_ten_compiled_branches_to_three_and_drains_all() {
    let branches = (0..10)
        .map(|i| format!("branch_{i}: work_{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let workers = (0..10).map(|i| format!("  work_{i}:\n    type: core.template\n    next: end_{i}\n    config: {{value: {i}}}\n  end_{i}:\n    type: core.end\n    config: {{outcome: success, data: {{value: \"{{{{ nodes.work_{i}.output }}}}\"}}}}"))
        .collect::<Vec<_>>().join("\n");
    let yaml = format!(
        r#"
version: 1
id: scheduler-agent
name: Scheduler Agent
input:
  schema: {{type: object}}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {{{branches}}}
      join: collect
{workers}
  collect:
    type: core.join
    next: result
    config: {{mode: all_settled}}
  result:
    type: core.end
    config: {{outcome: success, data: {{done: true}}}}
"#
    );
    let mut agent = compile_parallel_agent(&yaml);
    let in_flight = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release_open = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Notify::new());
    let counts = (0..10)
        .map(|i| (format!("work_{i}"), Arc::new(AtomicUsize::new(0))))
        .collect::<BTreeMap<_, _>>();
    for (id, executions) in &counts {
        replace_behavior(
            &mut agent,
            id,
            SchedulerBehavior::BarrierNext {
                output: json!({"id":id}),
                content: None,
                executions: Arc::clone(executions),
                in_flight: Arc::clone(&in_flight),
                maximum: Arc::clone(&maximum),
                started: Arc::clone(&started),
                release_open: Arc::clone(&release_open),
                release: Arc::clone(&release),
            },
        );
    }
    let run_context = context_with_agent_input(&agent, "run_parallel_ten", json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), repository, 3);
    let (_, stop) = stop_pair();
    let mut execution = tokio::spawn(async move { scheduler.run(run_context, stop).await });
    while in_flight.load(Ordering::SeqCst) < 3 {
        let notified = started.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if in_flight.load(Ordering::SeqCst) >= 3 {
            break;
        }
        tokio::select! {
            _ = &mut notified => {}
            result = &mut execution => panic!("scheduler ended before branch barrier: {result:?}"),
        }
    }
    assert_eq!(maximum.load(Ordering::SeqCst), 3);
    release_open.store(true, Ordering::SeqCst);
    release.notify_waiters();
    assert!(matches!(
        execution.await.unwrap().unwrap(),
        SchedulerResult::Ended(TerminalOutcome::Success { .. })
    ));
    assert_eq!(maximum.load(Ordering::SeqCst), 3);
    assert!(counts
        .values()
        .all(|count| count.load(Ordering::SeqCst) == 1));
}

#[tokio::test]
async fn parallel_branch_goto_executes_only_selected_successor() {
    let yaml = two_branch_yaml()
        .replace("type: core.template\n    next: summarize_a\n    config: {value: search-a}",
            "type: core.condition\n    config:\n      cases: [{when: \"true\", next: summarize_a}]\n      default: unused_a")
        .replace("  summarize_a:", "  unused_a:\n    type: core.template\n    next: unused_end_a\n    config: {value: unused}\n  unused_end_a:\n    type: core.end\n    config: {outcome: success, data: {value: \"{{ nodes.unused_a.output }}\"}}\n  summarize_a:");
    let mut agent = compile_parallel_agent(&yaml);
    let route = Arc::new(AtomicUsize::new(0));
    let selected = Arc::new(AtomicUsize::new(0));
    let unselected = Arc::new(AtomicUsize::new(0));
    replace_behavior(
        &mut agent,
        "search_a",
        SchedulerBehavior::Goto {
            target: "summarize_a".to_string(),
            output: json!({}),
            executions: Arc::clone(&route),
        },
    );
    replace_behavior(
        &mut agent,
        "summarize_a",
        SchedulerBehavior::Next {
            output: json!({"selected":true}),
            require_output: None,
            executions: Arc::clone(&selected),
        },
    );
    replace_behavior(
        &mut agent,
        "unused_a",
        SchedulerBehavior::Next {
            output: json!({"selected":false}),
            require_output: None,
            executions: Arc::clone(&unselected),
        },
    );
    let run_context = context_with_agent_input(&agent, "run_parallel_goto", json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), repository, 4);
    let (_, stop) = stop_pair();
    assert!(matches!(
        scheduler.run(run_context, stop).await.unwrap(),
        SchedulerResult::Ended(TerminalOutcome::Success { .. })
    ));
    assert_eq!(route.load(Ordering::SeqCst), 1);
    assert_eq!(selected.load(Ordering::SeqCst), 1);
    assert_eq!(unselected.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn parallel_scheduler_settles_partial_failure_and_runs_join() {
    let mut agent = compile_parallel_agent(two_branch_yaml());
    let summarize_b = Arc::new(AtomicUsize::new(0));
    replace_behavior(
        &mut agent,
        "search_a",
        SchedulerBehavior::Next {
            output: json!({"text":"search a"}),
            require_output: None,
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );
    replace_behavior(
        &mut agent,
        "search_b",
        SchedulerBehavior::Fail {
            code: "UPSTREAM_FAILURE",
            message: "upstream service failed",
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );
    replace_behavior(
        &mut agent,
        "summarize_a",
        SchedulerBehavior::Next {
            output: json!({"text":"result a"}),
            require_output: None,
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );
    replace_behavior(
        &mut agent,
        "summarize_b",
        SchedulerBehavior::Next {
            output: json!({"text":"result b"}),
            require_output: None,
            executions: Arc::clone(&summarize_b),
        },
    );
    let expected = json!({
        "branches": {
            "source_a": {"status":"succeeded", "terminal_node_id":"end_a", "output":{"data":{"text":"result a"}}},
            "source_b": {"status":"failed", "terminal_node_id":"search_b", "error":{"kind":"node", "code":"UPSTREAM_FAILURE", "message":"upstream service failed"}}
        },
        "summary":{"total":2,"succeeded":1,"failed":1,"failures":{"workflow":0,"node":1,"timeout":0}}
    });
    replace_behavior(
        &mut agent,
        "result",
        SchedulerBehavior::CompleteAfter {
            predecessor: "collect".to_string(),
            expected: expected.clone(),
            output: RunOutput {
                content: None,
                format: None,
                data: json!({"done":true}),
            },
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );
    let run_context = context_with_agent_input(&agent, "run_parallel_partial", json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();
    assert!(matches!(
        scheduler.run(run_context, stop).await.unwrap(),
        SchedulerResult::Ended(TerminalOutcome::Success { .. })
    ));
    assert_eq!(summarize_b.load(Ordering::SeqCst), 0);
    assert_eq!(
        repository
            .outputs
            .lock()
            .await
            .iter()
            .find(|output| output.node_id == "collect")
            .unwrap()
            .output,
        expected
    );
    let events = repository.events.lock().await.clone();
    let position = |kind: &str, node_or_branch: &str| {
        events
            .iter()
            .position(|event| {
                event.event_type.as_str() == kind
                    && (event.node_id.as_deref() == Some(node_or_branch)
                        || event.data["branch_id"] == node_or_branch)
            })
            .unwrap()
    };
    assert!(position("branch.started", "source_b") < position("node.failed", "search_b"));
    assert!(position("node.failed", "search_b") < position("branch.failed", "source_b"));
    assert!(position("branch.failed", "source_b") < position("node.started", "collect"));
}

#[tokio::test]
async fn parallel_scheduler_runs_join_and_post_join_when_all_branches_fail() {
    let mut agent = compile_parallel_agent(two_branch_yaml());
    let result_runs = Arc::new(AtomicUsize::new(0));
    for (id, code) in [("search_a", "FAILED_A"), ("search_b", "FAILED_B")] {
        replace_behavior(
            &mut agent,
            id,
            SchedulerBehavior::Fail {
                code,
                message: "source failed",
                executions: Arc::new(AtomicUsize::new(0)),
            },
        );
    }
    replace_behavior(
        &mut agent,
        "result",
        SchedulerBehavior::Complete {
            output: RunOutput {
                content: None,
                format: None,
                data: json!({"done":true}),
            },
            executions: Arc::clone(&result_runs),
        },
    );
    let run_context = context_with_agent_input(&agent, "run_parallel_all_failed", json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();
    assert!(matches!(
        scheduler.run(run_context, stop).await.unwrap(),
        SchedulerResult::Ended(TerminalOutcome::Success { .. })
    ));
    assert_eq!(result_runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        repository
            .outputs
            .lock()
            .await
            .iter()
            .find(|output| output.node_id == "collect")
            .unwrap()
            .output["summary"],
        json!({"total":2,"succeeded":0,"failed":2,"failures":{"workflow":0,"node":2,"timeout":0}})
    );
}

#[tokio::test]
async fn parallel_scheduler_runs_sequential_compiled_fork_regions() {
    let mut agent = compile_parallel_agent(
        r#"
version: 1
id: scheduler-agent
name: Sequential Forks
input:
  schema: {type: object}
entry: fork_a
nodes:
  fork_a:
    type: core.fork
    config:
      branches: {a1: a1, a2: a2}
      join: join_a
  a1:
    type: core.template
    next: end_a1
    config: {value: a1}
  end_a1:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.a1.output }}"}}
  a2:
    type: core.template
    next: end_a2
    config: {value: a2}
  end_a2:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.a2.output }}"}}
  join_a:
    type: core.join
    next: fork_b
    config: {mode: all_settled}
  fork_b:
    type: core.fork
    config:
      branches: {b1: b1, b2: b2}
      join: join_b
  b1:
    type: core.template
    next: end_b1
    config: {value: b1}
  end_b1:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.b1.output }}"}}
  b2:
    type: core.template
    next: end_b2
    config: {value: b2}
  end_b2:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.b2.output }}"}}
  join_b:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.end
    config: {outcome: success, data: {done: true}}
"#,
    );
    let counts = ["a1", "a2", "b1", "b2", "result"]
        .into_iter()
        .map(|node_id| (node_id, Arc::new(AtomicUsize::new(0))))
        .collect::<BTreeMap<_, _>>();
    for node_id in ["a1", "a2", "b1", "b2"] {
        replace_behavior(
            &mut agent,
            node_id,
            SchedulerBehavior::Next {
                output: json!({"node":node_id}),
                require_output: None,
                executions: Arc::clone(&counts[node_id]),
            },
        );
    }
    let output = RunOutput {
        content: None,
        format: None,
        data: json!({"done":true}),
    };
    replace_behavior(
        &mut agent,
        "result",
        SchedulerBehavior::Complete {
            output: output.clone(),
            executions: Arc::clone(&counts["result"]),
        },
    );
    let run_context = context_with_agent_input(&agent, "run_sequential_forks", json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();

    assert_eq!(
        scheduler.run(run_context, stop).await.unwrap(),
        SchedulerResult::Ended(TerminalOutcome::Success { output })
    );
    assert!(counts
        .values()
        .all(|executions| executions.load(Ordering::SeqCst) == 1));
    assert_eq!(
        repository
            .events
            .lock()
            .await
            .iter()
            .filter(|event| {
                event.event_type.as_str() == "node.started"
                    && matches!(event.node_id.as_deref(), Some("join_a" | "join_b"))
            })
            .map(|event| event.node_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["join_a", "join_b"]
    );
}

#[tokio::test]
async fn parallel_scheduler_isolates_node_timeout_to_its_branch() {
    let mut agent = compile_parallel_agent(two_branch_yaml());
    let summarize_b = Arc::new(AtomicUsize::new(0));
    replace_behavior(
        &mut agent,
        "search_b",
        SchedulerBehavior::WaitForever {
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );
    agent.nodes.get_mut("search_b").unwrap().timeout = Duration::from_millis(1);
    replace_behavior(
        &mut agent,
        "summarize_b",
        SchedulerBehavior::Next {
            output: json!({}),
            require_output: None,
            executions: Arc::clone(&summarize_b),
        },
    );
    let run_context = context_with_agent_input(&agent, "run_parallel_timeout", json!({}));
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();
    assert!(matches!(
        scheduler.run(run_context, stop).await.unwrap(),
        SchedulerResult::Ended(TerminalOutcome::Success { .. })
    ));
    assert_eq!(summarize_b.load(Ordering::SeqCst), 0);
    let collect = repository
        .outputs
        .lock()
        .await
        .iter()
        .find(|output| output.node_id == "collect")
        .unwrap()
        .output
        .clone();
    assert_eq!(
        collect["branches"]["source_b"]["error"],
        json!({"kind":"timeout","code":"NODE_TIMEOUT","message":"node execution timed out"})
    );
    assert_eq!(collect["branches"]["source_a"]["status"], "succeeded");
    assert_eq!(collect["summary"]["succeeded"], 1);
    assert_eq!(collect["summary"]["failures"]["timeout"], 1);
}

#[tokio::test]
async fn parallel_scheduler_never_captures_stop_as_a_branch_result() {
    let mut agent = compile_parallel_agent(two_branch_yaml());
    replace_behavior(
        &mut agent,
        "search_a",
        SchedulerBehavior::Stop {
            reason: StopReason::Interrupted,
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();

    let error = scheduler
        .run(context("run_parallel_stop"), stop)
        .await
        .unwrap_err();

    assert_eq!(error.code(), "INFRASTRUCTURE_FAILURE");
    assert_eq!(error.kind(), RunErrorKind::Infrastructure);
    assert!(!repository.events.lock().await.iter().any(|event| {
        event.event_type.as_str() == "branch.failed" && event.data["branch_id"] == "source_a"
    }));
    assert_no_branch_result_or_join_output(&repository, "collect").await;
}

#[tokio::test]
async fn unbacked_executor_stop_is_infrastructure_failure() {
    let executions = Arc::new(AtomicUsize::new(0));
    let agent = scheduler_agent(
        vec![scheduler_node(
            "self_stop",
            None,
            SchedulerBehavior::Stop {
                reason: StopReason::Interrupted,
                executions: Arc::clone(&executions),
            },
        )],
        "self_stop",
    );
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = scheduler(agent, Arc::clone(&repository));
    let (_, stop) = stop_pair();

    let error = scheduler
        .run(context("run_unbacked_stop"), stop)
        .await
        .unwrap_err();

    assert_eq!(error.code(), "INFRASTRUCTURE_FAILURE");
    assert_eq!(error.kind(), RunErrorKind::Infrastructure);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    let events = repository.events.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type.as_str(), "node.started");
}

#[tokio::test]
async fn unbacked_executor_stop_cancels_parallel_siblings_and_releases_permits() {
    let mut agent = compile_parallel_agent(two_branch_yaml());
    let stopper_runs = Arc::new(AtomicUsize::new(0));
    let blocked_runs = Arc::new(AtomicUsize::new(0));
    let successors = Arc::new(AtomicUsize::new(0));
    replace_behavior(
        &mut agent,
        "search_a",
        SchedulerBehavior::Stop {
            reason: StopReason::Interrupted,
            executions: Arc::clone(&stopper_runs),
        },
    );
    replace_behavior(
        &mut agent,
        "search_b",
        SchedulerBehavior::WaitForever {
            executions: Arc::clone(&blocked_runs),
        },
    );
    for node_id in ["summarize_a", "summarize_b", "collect", "result"] {
        replace_behavior(
            &mut agent,
            node_id,
            SchedulerBehavior::Next {
                output: json!({}),
                require_output: None,
                executions: Arc::clone(&successors),
            },
        );
    }
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 2);
    let (_, stop) = stop_pair();

    let error = tokio::time::timeout(
        Duration::from_secs(1),
        scheduler.run(context("run_unbacked_parallel_stop"), stop),
    )
    .await
    .expect("unbacked stopped return must cancel sibling wrappers")
    .unwrap_err();

    assert_eq!(error.code(), "INFRASTRUCTURE_FAILURE");
    assert_eq!(stopper_runs.load(Ordering::SeqCst), 1);
    assert_eq!(blocked_runs.load(Ordering::SeqCst), 1);
    assert_eq!(successors.load(Ordering::SeqCst), 0);
    assert!(!repository.events.lock().await.iter().any(|event| {
        matches!(
            event.event_type.as_str(),
            "node.failed" | "branch.completed" | "branch.failed"
        )
    }));
    assert_no_branch_result_or_join_output(&repository, "collect").await;
}

#[tokio::test]
async fn parallel_scheduler_never_captures_infrastructure_as_a_branch_result() {
    let mut agent = compile_parallel_agent(two_branch_yaml());
    replace_behavior(
        &mut agent,
        "search_a",
        SchedulerBehavior::Infrastructure {
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );
    let repository = Arc::new(SchedulerRepository::default());
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
    let (_, stop) = stop_pair();

    let error = scheduler
        .run(context("run_parallel_infrastructure"), stop)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "INFRASTRUCTURE_FAILURE");
    assert_eq!(error.kind(), RunErrorKind::Infrastructure);
    assert!(!repository.events.lock().await.iter().any(|event| {
        event.event_type.as_str() == "branch.failed" && event.data["branch_id"] == "source_a"
    }));
    assert_no_branch_result_or_join_output(&repository, "collect").await;
}

#[tokio::test]
async fn global_external_stop_is_observed_by_all_branches_and_drained_before_return() {
    for reason in [StopReason::Cancelled, StopReason::TimedOut] {
        let mut agent = compile_parallel_agent(two_branch_yaml());
        let active = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let stopped = Arc::new(AtomicUsize::new(0));
        let successors = Arc::new(AtomicUsize::new(0));
        for node_id in ["search_a", "search_b"] {
            replace_behavior(
                &mut agent,
                node_id,
                SchedulerBehavior::WaitForStop {
                    executions: Arc::new(AtomicUsize::new(0)),
                    active: Arc::clone(&active),
                    started: Arc::clone(&started),
                    stopped: Arc::clone(&stopped),
                },
            );
        }
        for node_id in ["summarize_a", "summarize_b"] {
            replace_behavior(
                &mut agent,
                node_id,
                SchedulerBehavior::Next {
                    output: json!({}),
                    require_output: None,
                    executions: Arc::clone(&successors),
                },
            );
        }
        let repository = Arc::new(SchedulerRepository::default());
        let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
        let (controller, stop) = stop_pair();
        let execution = tokio::spawn(async move {
            scheduler
                .run(context("run_global_external_stop"), stop)
                .await
        });

        while active.load(Ordering::SeqCst) != 2 {
            started.notified().await;
        }
        assert!(controller.request(reason));

        assert_eq!(
            execution.await.unwrap().unwrap(),
            SchedulerResult::Stopped(RunError::stopped(reason))
        );
        assert_eq!(stopped.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(successors.load(Ordering::SeqCst), 0);
        assert!(!repository.events.lock().await.iter().any(|event| {
            matches!(
                event.event_type.as_str(),
                "branch.completed" | "branch.failed" | "node.started"
            ) && matches!(
                event.node_id.as_deref(),
                Some("summarize_a" | "summarize_b" | "collect")
            )
        }));
        assert_no_branch_result_or_join_output(&repository, "collect").await;
    }
}

#[tokio::test]
async fn shared_stop_reason_overrides_executor_returned_reason() {
    for (shared, returned, expected) in [
        (
            StopReason::Interrupted,
            StopReason::Cancelled,
            RunError::stopped(StopReason::Interrupted),
        ),
        (
            StopReason::Cancelled,
            StopReason::Interrupted,
            RunError::stopped(StopReason::Cancelled),
        ),
        (
            StopReason::TimedOut,
            StopReason::Cancelled,
            RunError::stopped(StopReason::TimedOut),
        ),
    ] {
        let started = Arc::new(Notify::new());
        let executions = Arc::new(AtomicUsize::new(0));
        let agent = scheduler_agent(
            vec![scheduler_node(
                "mismatch",
                None,
                SchedulerBehavior::ReturnedStopAfterRuntimeStop {
                    returned,
                    executions: Arc::clone(&executions),
                    started: Arc::clone(&started),
                },
            )],
            "mismatch",
        );
        let repository = Arc::new(SchedulerRepository::default());
        let scheduler = scheduler(agent, Arc::clone(&repository));
        let (controller, stop) = stop_pair();
        let execution = tokio::spawn(async move {
            scheduler
                .run(context("run_shared_stop_override"), stop)
                .await
        });

        started.notified().await;
        assert!(controller.request(shared));

        assert_eq!(
            execution.await.unwrap().unwrap(),
            SchedulerResult::Stopped(expected.clone())
        );
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        let events = repository.events.lock().await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].event_type.as_str(), "node.failed");
        assert_eq!(events[1].code, expected.code());
    }
}

#[tokio::test]
async fn global_external_stop_drains_journal_stage_without_private_task_abort() {
    let mut agent = compile_parallel_agent(two_branch_yaml());
    replace_behavior(
        &mut agent,
        "search_a",
        SchedulerBehavior::Next {
            output: json!({}),
            require_output: None,
            executions: Arc::new(AtomicUsize::new(0)),
        },
    );
    replace_behavior(
        &mut agent,
        "search_b",
        SchedulerBehavior::WaitForStop {
            executions: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(Notify::new()),
            stopped: Arc::new(AtomicUsize::new(0)),
        },
    );
    let output_entered = Arc::new(Notify::new());
    let output_release = Arc::new(Notify::new());
    let repository = Arc::new(SchedulerRepository {
        output_entered: Some(Arc::clone(&output_entered)),
        output_release: Some(Arc::clone(&output_release)),
        blocked_output_node: Some("search_a".to_string()),
        ..SchedulerRepository::default()
    });
    let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 1);
    let (controller, stop) = stop_pair();
    let mut execution = tokio::spawn(async move {
        scheduler
            .run(context("run_global_external_journal"), stop)
            .await
    });

    output_entered.notified().await;
    tokio::time::pause();
    assert!(controller.request(StopReason::Cancelled));
    let completed_while_output_was_blocked =
        tokio::time::timeout(Duration::from_millis(20), &mut execution)
            .await
            .is_ok();
    output_release.notify_one();
    assert!(
        !completed_while_output_was_blocked,
        "external stop must drain cancellation-safe journal work instead of privately aborting it"
    );

    assert_eq!(
        execution.await.unwrap().unwrap(),
        SchedulerResult::Stopped(RunError::stopped(StopReason::Cancelled))
    );
    assert_no_branch_result_or_join_output(&repository, "collect").await;
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
async fn global_infrastructure_failures_cancel_siblings_and_never_start_join() {
    for failure in [
        GlobalFailureCase::Journal,
        GlobalFailureCase::NodeOutput,
        GlobalFailureCase::MissingExecutor,
        GlobalFailureCase::Panic,
        GlobalFailureCase::DuplicateActivation,
        GlobalFailureCase::Infrastructure,
    ] {
        let mut agent = compile_parallel_agent(two_branch_yaml());
        let active = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let waiting_executions = Arc::new(AtomicUsize::new(0));
        replace_behavior(
            &mut agent,
            "search_a",
            SchedulerBehavior::WaitForStop {
                executions: Arc::clone(&waiting_executions),
                active: Arc::clone(&active),
                started: Arc::clone(&started),
                stopped: Arc::new(AtomicUsize::new(0)),
            },
        );
        let failure_executions = Arc::new(AtomicUsize::new(0));
        let behavior = match failure {
            GlobalFailureCase::Panic => SchedulerBehavior::PanicAfter {
                executions: Arc::clone(&failure_executions),
                sibling_started: Arc::clone(&started),
            },
            GlobalFailureCase::Infrastructure => SchedulerBehavior::InfrastructureAfter {
                executions: Arc::clone(&failure_executions),
                sibling_started: Arc::clone(&started),
            },
            GlobalFailureCase::DuplicateActivation => SchedulerBehavior::GotoAfter {
                target: "search_b".to_string(),
                executions: Arc::clone(&failure_executions),
                sibling_started: Arc::clone(&started),
            },
            _ => SchedulerBehavior::NextAfter {
                executions: Arc::clone(&failure_executions),
                sibling_started: Arc::clone(&started),
                output: json!({}),
            },
        };
        replace_behavior(&mut agent, "search_b", behavior);
        if matches!(failure, GlobalFailureCase::MissingExecutor) {
            agent.nodes.get_mut("search_b").unwrap().kind = "company.not_registered".to_string();
        }
        let repository = Arc::new(SchedulerRepository::default());
        match failure {
            GlobalFailureCase::Journal => {
                *repository.fail_append_for.lock().await =
                    Some(("node.completed".to_string(), "search_b".to_string()));
            }
            GlobalFailureCase::NodeOutput => {
                *repository.fail_output_for.lock().await = Some("search_b".to_string());
            }
            _ => {}
        }
        let scheduler = parallel_scheduler(Arc::new(agent), Arc::clone(&repository), 4);
        let (_, stop) = stop_pair();

        let error = scheduler
            .run(context("run_global_infrastructure"), stop)
            .await
            .expect_err(&format!("{failure:?} must fail the run globally"));

        assert_eq!(error.code(), "INFRASTRUCTURE_FAILURE", "{failure:?}");
        assert_eq!(error.kind(), RunErrorKind::Infrastructure, "{failure:?}");
        assert_eq!(waiting_executions.load(Ordering::SeqCst), 1, "{failure:?}");
        assert_eq!(active.load(Ordering::SeqCst), 0, "{failure:?}");
        if !matches!(failure, GlobalFailureCase::MissingExecutor) {
            assert_eq!(failure_executions.load(Ordering::SeqCst), 1, "{failure:?}");
        }
        let events = repository.events.lock().await;
        assert!(
            !events.iter().any(|event| {
                event.node_id.as_deref() == Some("collect")
                    || matches!(
                        event.event_type.as_str(),
                        "branch.completed" | "branch.failed"
                    )
            }),
            "{failure:?}"
        );
        drop(events);
        assert_no_branch_result_or_join_output(&repository, "collect").await;
    }
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
    let timeout = RunError::timeout();
    assert!(matches!(timeout.kind(), RunErrorKind::Timeout));
    assert_eq!(timeout.code(), "NODE_TIMEOUT");
    assert!(matches!(
        RunError::infrastructure("EVENT_APPEND_FAILED", "failed").kind(),
        RunErrorKind::Infrastructure
    ));
    let stop = RunError::stopped(StopReason::Cancelled);
    assert!(matches!(stop.kind(), RunErrorKind::Stop));
    assert_eq!(stop.stop_reason(), Some(StopReason::Cancelled));
}
