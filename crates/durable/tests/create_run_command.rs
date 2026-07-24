use insight_dsl::{compile_source, CompileOptions};
use insight_durable::{CreateRunCommand, VersionedPlan};
use insight_engine::{
    repository::REPOSITORY_CONFIGURATION_INVALID, DefinitionRevisionId, DeploymentRevisionId, RunId,
};
use serde_json::json;

fn normalized_input_plan() -> (insight_engine::Plan, VersionedPlan) {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
  messages: {type: "Message[]", default: []}
  image_url: {type: string, optional: true}
output: string
workflow:
  steps:
    - return: fixed
"#;
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("normalized_repository_input").unwrap(),
            "normalized-repository-input.yaml",
            source,
        ),
    )
    .unwrap();
    let versioned = VersionedPlan::from_verified_plan(
        "definition_normalized_input",
        "agent_normalized_input",
        "Normalized input",
        DeploymentRevisionId::new("deployment_normalized_input").unwrap(),
        "expression-3.0.0",
        json!({"author": "structured"}),
        &plan,
        json!({}),
        json!([]),
        json!([]),
    )
    .unwrap();
    (plan, versioned)
}

#[test]
fn create_run_command_accepts_only_plan_normalized_input() {
    let (plan, versioned) = normalized_input_plan();

    let raw = json!({"question": "safe"});
    let missing_default = CreateRunCommand::new(
        RunId::new("run_missing_frozen_default").unwrap(),
        &versioned,
        raw.clone(),
    )
    .unwrap_err();
    assert_eq!(missing_default.code(), REPOSITORY_CONFIGURATION_INVALID);

    let normalized = plan.metadata().input_contract().normalize(raw).unwrap();
    assert_eq!(normalized, json!({"messages": [], "question": "safe"}));
    assert!(CreateRunCommand::new(
        RunId::new("run_normalized_input").unwrap(),
        &versioned,
        normalized,
    )
    .is_ok());
    let explicit_null = CreateRunCommand::new(
        RunId::new("run_explicit_null_optional").unwrap(),
        &versioned,
        json!({"messages": [], "question": "safe", "image_url": null}),
    )
    .unwrap_err();
    assert_eq!(explicit_null.code(), REPOSITORY_CONFIGURATION_INVALID);
}
