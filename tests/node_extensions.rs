use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use insight_agent_platform::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules, NodeOutcome,
            NodeTransition,
        },
        compiler::CompileContext,
        EmitPolicy,
    },
    nodes::registry::{NodeExecutor, NodeExecutorRegistry, NodeType, NodeTypeRegistry},
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{stop_pair, ExecutionControl, RunContext, RunError, RunMetadata},
};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConstantConfig {
    value: i64,
}

#[derive(Debug, Clone, Copy)]
struct ConstantNode;

impl NodeType for ConstantNode {
    fn kind(&self) -> &'static str {
        "test.constant"
    }

    fn compile(
        &self,
        _node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, insight_agent_platform::dsl::CompileError> {
        let config: ConstantConfig = serde_json::from_value(config).map_err(|error| {
            insight_agent_platform::dsl::CompileError::new("NODE_CONFIG_INVALID", error.to_string())
        })?;
        Ok(NodeCompilation {
            body: Arc::new(config),
            edges: Vec::new(),
            references: BTreeSet::new(),
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
        let config = node.body::<ConstantConfig>()?;
        Ok(NodeOutcome {
            output: json!({"value": config.value}),
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
        node.body::<ConstantConfig>().unwrap_err().code(),
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
