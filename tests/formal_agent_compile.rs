use std::{collections::BTreeSet, fmt, time::Duration};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    dsl::{
        compiled::NodeRegion,
        compiler::{AgentCompiler, CompileLimits},
    },
    nodes::default_node_registries,
    resources::{
        actions::{Action, ActionContext, ActionDescriptor, ActionRegistry},
        models::{ChatChunk, ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
    },
    runtime::RunError,
};
use serde_json::{json, Value};
use tempfile::tempdir;

#[derive(Debug)]
struct FakeModel;

#[async_trait]
impl ChatModel for FakeModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        BTreeSet::new()
    }

    fn validate_parameters(
        &self,
        parameters: &Value,
    ) -> Result<(), insight_agent_platform::dsl::CompileError> {
        if parameters.is_object() {
            Ok(())
        } else {
            Err(insight_agent_platform::dsl::CompileError::new(
                "MODEL_PARAMETERS_INVALID",
                "parameters must be an object",
            ))
        }
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::iter(vec![Ok(ChatChunk {
            text: "general".to_string(),
            finish_reason: Some("stop".to_string()),
            usage: None,
        })])))
    }
}

struct ClassifyAction;

impl fmt::Debug for ClassifyAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClassifyAction")
    }
}

#[async_trait]
impl Action for ClassifyAction {
    fn descriptor(&self) -> ActionDescriptor {
        ActionDescriptor {
            name: "classify",
            input_schema: json!({"type":"object"}),
            output_schema: json!({
                "type":"object",
                "required":["kind"],
                "properties":{"kind":{"type":"string"}}
            }),
            idempotent: true,
            streams_content: false,
        }
    }

    async fn call(&self, _input: Value, _context: ActionContext) -> Result<Value, RunError> {
        Ok(json!({"kind":"general"}))
    }
}

#[test]
fn complete_formal_agent_with_all_core_nodes_compiles() {
    let directory = tempdir().unwrap();
    std::fs::write(
        directory.path().join("agent.yaml"),
        r#"
version: 1
id: complete-formal-agent
name: Complete Formal Agent
description: Exercises every formal V1 core node type.
input:
  schema:
    type: object
    required: [question]
    properties:
      question: {type: string}
prompts:
  system: prompts/system.md
entry: prepare
nodes:
  prepare:
    type: core.template
    next: route
    config:
      value: "{{ input.question }}"
  route:
    type: core.condition
    config:
      cases:
        - when: "input.question == 'medical'"
          next: answer
      default: classify
  answer:
    type: core.chat
    next: selected
    config:
      model: primary
      messages:
        - role: system
          content: "You are concise."
        - role: user
          content: "{{ nodes.prepare.output }}"
      parameters: {}
  classify:
    type: core.action
    next: selected
    config:
      action: classify
      input:
        text: "{{ nodes.prepare.output }}"
  selected:
    type: core.select
    next: fanout
    config:
      sources: [answer, classify]
  fanout:
    type: core.fork
    config:
      branches: {a: end_a, b: end_b}
      join: collect
  end_a:
    type: core.branch_end
    config:
      outcome: success
      data: {selected: "{{ nodes.selected.output.value }}"}
  end_b:
    type: core.branch_end
    config:
      outcome: success
      data: {selected: "{{ nodes.selected.output.value }}"}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.end
    config:
      outcome: success
      content:
        template: "{{ input.question }}"
      format: text
      data:
        branches: "{{ nodes.collect.output.summary }}"
"#,
    )
    .unwrap();
    std::fs::create_dir(directory.path().join("prompts")).unwrap();
    std::fs::write(
        directory.path().join("prompts/system.md"),
        "Unused declared prompt is still part of the content hash.",
    )
    .unwrap();

    let mut models = ModelRegistry::default();
    models.register("primary", FakeModel).unwrap();
    let mut actions = ActionRegistry::default();
    actions.register(ClassifyAction).unwrap();
    let (types, _) = default_node_registries().unwrap();
    let compiler = AgentCompiler::new(
        types,
        models,
        actions,
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 32,
        },
    );

    let agent = compiler.compile_dir(directory.path()).unwrap();

    assert_eq!(agent.nodes.len(), 10);
    assert!(agent.nodes.contains_key("prepare"));
    assert!(agent.nodes.contains_key("answer"));
    assert!(agent.nodes.contains_key("classify"));
    assert!(agent.nodes.contains_key("route"));
    assert!(agent.nodes.contains_key("selected"));
    assert!(agent.nodes.contains_key("fanout"));
    assert!(agent.nodes.contains_key("end_a"));
    assert!(agent.nodes.contains_key("end_b"));
    assert!(agent.nodes.contains_key("collect"));
    assert!(agent.nodes.contains_key("result"));
    assert_eq!(
        agent
            .nodes
            .values()
            .map(|node| node.kind.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "core.action",
            "core.branch_end",
            "core.chat",
            "core.condition",
            "core.end",
            "core.fork",
            "core.join",
            "core.select",
            "core.template",
        ])
    );
    assert_eq!(agent.execution_plan.forks.len(), 1);
    assert_eq!(
        agent.execution_plan.node_regions["prepare"],
        NodeRegion::Linear
    );
    assert_eq!(
        agent.execution_plan.node_regions["end_a"],
        NodeRegion::Branch {
            fork_id: "fanout".to_string(),
            branch_id: "a".to_string(),
        }
    );
    assert_eq!(
        agent.execution_plan.node_regions["collect"],
        NodeRegion::Join {
            fork_id: "fanout".to_string(),
        }
    );
    assert!(agent.nodes["answer"].references.contains("prepare"));
    assert!(agent.nodes["result"].references.contains("collect"));
    assert!(agent.version_hash.starts_with("sha256:"));
}

#[test]
fn select_output_compiles_for_all_builtin_consumers() {
    let directory = tempdir().unwrap();
    std::fs::write(
        directory.path().join("agent.yaml"),
        r#"
version: 1
id: select-consumers
name: Select Consumers
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: medical}]
      default: general
  medical:
    type: core.template
    next: selected
    config: {value: {text: medical}}
  general:
    type: core.template
    next: selected
    config: {value: {text: general}}
  selected:
    type: core.select
    next: render
    config: {sources: [medical, general]}
  render:
    type: core.template
    next: classify
    config:
      value: "{{ nodes.selected.output.value.text }}"
  classify:
    type: core.action
    next: answer
    config:
      action: classify
      input:
        text: "{{ nodes.selected.output.value.text }}"
  answer:
    type: core.chat
    next: result
    config:
      model: primary
      messages:
        - role: user
          content: "{{ nodes.selected.output.value.text }}"
      parameters: {}
  result:
    type: core.end
    config:
      outcome: success
      data:
        source: "{{ nodes.selected.output.source_node_id }}"
        rendered: "{{ nodes.render.output }}"
        kind: "{{ nodes.classify.output.kind }}"
        answer: "{{ nodes.answer.output.text }}"
"#,
    )
    .unwrap();

    let mut models = ModelRegistry::default();
    models.register("primary", FakeModel).unwrap();
    let mut actions = ActionRegistry::default();
    actions.register(ClassifyAction).unwrap();
    let (types, _) = default_node_registries().unwrap();
    let compiler = AgentCompiler::new(
        types,
        models,
        actions,
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 8,
        },
    );

    let agent = compiler.compile_dir(directory.path()).unwrap();
    assert_eq!(
        agent.nodes["render"].references,
        ["selected".to_string()].into_iter().collect()
    );
    assert_eq!(
        agent.nodes["classify"].references,
        ["selected".to_string()].into_iter().collect()
    );
    assert_eq!(
        agent.nodes["answer"].references,
        ["selected".to_string()].into_iter().collect()
    );
}
