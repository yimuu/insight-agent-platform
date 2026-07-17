use std::collections::{BTreeSet, HashSet};

use async_trait::async_trait;
use futures::stream;
use insight_agent_platform::{
    catalog::compile_enabled_agents,
    dsl::{vnext::compiler::WorkflowCompiler, CompileError},
    resources::{
        builtin_actions::builtin_action_registry,
        models::{ChatModel, ChatRequest, ChatStream, ModelCapability, ModelRegistry},
    },
    runtime::RunError,
};

#[derive(Debug)]
struct CompileOnlyModel {
    vision: bool,
}

#[async_trait]
impl ChatModel for CompileOnlyModel {
    fn capabilities(&self) -> BTreeSet<ModelCapability> {
        if self.vision {
            BTreeSet::from([ModelCapability::Vision])
        } else {
            BTreeSet::new()
        }
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
    let mut models = ModelRegistry::default();
    models
        .register("general_chat", CompileOnlyModel { vision: false })
        .unwrap();
    models
        .register("vision_chat", CompileOnlyModel { vision: true })
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
    let enabled = BTreeSet::from([
        "action_demo".to_string(),
        "medical_report_interpreter".to_string(),
        "parallel_researcher".to_string(),
        "researcher".to_string(),
        "workflow_failure_demo".to_string(),
    ]);
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("agents");

    let catalog = compile_enabled_agents(&root, &enabled, &compiler).unwrap();
    let compiled = catalog.ids().collect::<HashSet<_>>();
    let expected = enabled.iter().map(String::as_str).collect::<HashSet<_>>();

    assert_eq!(compiled, expected);
    assert!(catalog
        .list()
        .all(|workflow| workflow.version_hash.starts_with("sha256:")));
}
