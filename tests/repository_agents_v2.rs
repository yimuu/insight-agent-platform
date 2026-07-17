use std::collections::{BTreeSet, HashSet};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    catalog::compile_enabled_agents,
    dsl::{
        vnext::{compiler::WorkflowCompiler, ir::OperationKind, plan::CallPlan},
        CompileError,
    },
    resources::{
        builtin_actions::builtin_action_registry,
        models::{ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
    },
    runtime::RunError,
};

#[derive(Debug)]
struct CompileOnlyModel {
    capabilities: BTreeSet<ModelCapability>,
}

#[async_trait]
impl ChatModel for CompileOnlyModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        self.capabilities.clone()
    }

    fn validate_parameters(&self, _parameters: &serde_json::Value) -> Result<(), CompileError> {
        Ok(())
    }

    async fn stream_chat(&self, _request: ChatRequest) -> Result<ChatStream, RunError> {
        Ok(Box::pin(stream::empty()))
    }
}

#[test]
fn every_checked_in_agent_compiles_through_the_production_v2_catalog() {
    let enabled = BTreeSet::from([
        "action_demo".to_string(),
        "medical_report_interpreter".to_string(),
        "parallel_researcher".to_string(),
        "researcher".to_string(),
        "workflow_failure_demo".to_string(),
    ]);
    for structured_output in [
        ModelCapability::JsonSchemaOutput,
        ModelCapability::JsonObjectOutput,
    ] {
        let mut models = ModelRegistry::default();
        let general_capabilities = BTreeSet::from([structured_output]);
        let mut vision_capabilities = general_capabilities.clone();
        vision_capabilities.insert(ModelCapability::Vision);
        models
            .register(
                "general_chat",
                CompileOnlyModel {
                    capabilities: general_capabilities,
                },
            )
            .unwrap();
        models
            .register(
                "vision_chat",
                CompileOnlyModel {
                    capabilities: vision_capabilities,
                },
            )
            .unwrap();
        let actions = builtin_action_registry(
            &[
                "current_time".to_string(),
                "example.text_metrics".to_string(),
            ],
            None,
        )
        .unwrap();
        let compiler = WorkflowCompiler::new(models, actions);
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("agents");

        let catalog = compile_enabled_agents(&root, &enabled, &compiler).unwrap();
        let compiled = catalog.ids().collect::<HashSet<_>>();
        let expected = enabled.iter().map(String::as_str).collect::<HashSet<_>>();

        assert_eq!(compiled, expected);
        assert!(catalog
            .list()
            .all(|workflow| workflow.version_hash.starts_with("sha256:")));
    }
}

const STRUCTURED_RESPONSE_SOURCE: &str = r#"
api_version: insight.agent/v2
kind: agent
metadata:
  id: structured_response
  name: Structured Response
types:
  Answer:
    fields:
      text: string
inputs: {}
output: Answer
workflow:
  steps:
    - id: answer
      type: llm
      model: general_chat
      messages:
        - role: user
          content:
            - text: Answer as requested.
      response: Answer
  result:
    return: $answer
"#;

fn compiler_with_capabilities(capabilities: BTreeSet<ModelCapability>) -> WorkflowCompiler {
    let mut models = ModelRegistry::default();
    models
        .register("general_chat", CompileOnlyModel { capabilities })
        .unwrap();
    WorkflowCompiler::new(models, Default::default())
}

#[test]
fn json_object_output_is_limited_to_object_roots_and_schema_mode_wins_when_available() {
    let object_only =
        compiler_with_capabilities(BTreeSet::from([ModelCapability::JsonObjectOutput]));
    object_only
        .compile_source(std::path::Path::new("."), STRUCTURED_RESPONSE_SOURCE)
        .expect("an object response must compile with json_object_output");

    let array_source = STRUCTURED_RESPONSE_SOURCE
        .replace("output: Answer", "output: Answer[]")
        .replace("response: Answer", "response: Answer[]");
    let error = object_only
        .compile_source(std::path::Path::new("."), &array_source)
        .expect_err("an array response must require json_schema_output");
    assert_eq!(error.code(), "VNEXT_LLM_STRUCTURED_OUTPUT_REQUIRED");

    let both = compiler_with_capabilities(BTreeSet::from([
        ModelCapability::JsonObjectOutput,
        ModelCapability::JsonSchemaOutput,
    ]));
    let compiled = both
        .compile_source(std::path::Path::new("."), STRUCTURED_RESPONSE_SOURCE)
        .unwrap();
    let plan = compiled
        .ir
        .root
        .operations
        .iter()
        .find_map(|operation| match &operation.kind {
            OperationKind::Call(call) => match &call.plan {
                CallPlan::Llm(plan) => Some(plan),
                CallPlan::Action(_) => None,
            },
            _ => None,
        })
        .expect("fixture must contain one LLM plan");
    assert_eq!(
        plan.capabilities,
        BTreeSet::from([ModelCapability::JsonSchemaOutput])
    );
}
