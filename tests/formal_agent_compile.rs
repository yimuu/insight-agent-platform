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
    next: answer
    config:
      value:
        question: "{{ input.question }}"
  answer:
    type: core.chat
    next: classify
    config:
      model: primary
      messages:
        - role: system
          content: "You are concise."
        - role: user
          content: "{{ nodes.prepare.output.question }}"
      parameters: {}
  classify:
    type: core.action
    next: route
    config:
      action: classify
      input:
        text: "{{ nodes.answer.output.text }}"
  route:
    type: core.condition
    config:
      cases:
        - when: "nodes.classify.output.kind == 'medical'"
          next: medical
      default: general
  medical:
    type: core.template
    next: result
    config:
      value: medical
  general:
    type: core.template
    next: result
    config:
      value: general
  result:
    type: core.output
    config:
      content:
        template: "{{ input.question }}"
      format: text
      data:
        source: complete-formal-agent
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

    assert_eq!(agent.nodes.len(), 7);
    assert!(agent.nodes.contains_key("prepare"));
    assert!(agent.nodes.contains_key("answer"));
    assert!(agent.nodes.contains_key("classify"));
    assert!(agent.nodes.contains_key("route"));
    assert!(agent.nodes.contains_key("medical"));
    assert!(agent.nodes.contains_key("general"));
    assert!(agent.nodes.contains_key("result"));
    assert!(agent.execution_plan.forks.is_empty());
    assert!(agent
        .execution_plan
        .node_regions
        .values()
        .all(|region| region == &NodeRegion::Linear));
    assert!(agent.version_hash.starts_with("sha256:"));
}
