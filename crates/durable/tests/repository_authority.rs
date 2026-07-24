use insight_dsl::{compile_source, CompileOptions};
use insight_durable::VersionedPlan;
use insight_engine::{
    repository::REPOSITORY_CONFIGURATION_INVALID, DefinitionRevisionId, DeploymentRevisionId,
    SemanticHash,
};
use serde_json::json;

const PLAN_SOURCE: &str = r#"api_version: insight.agent/v1
kind: agent
inputs:
  question: string
output: string
workflow:
  steps:
    - return: fixed
"#;

fn verified_plan(label: &str) -> insight_engine::Plan {
    compile_source(
        PLAN_SOURCE,
        CompileOptions::new(
            DefinitionRevisionId::new(format!("definition_revision_{label}")).unwrap(),
            format!("{label}.yaml"),
            PLAN_SOURCE,
        ),
    )
    .unwrap()
}

/// Makes a deliberately non-authoritative in-memory `Plan` without relying on
/// its private field order. The public accessor locates the semantic hash
/// within this exclusively borrowed value; the shared borrow ends before the
/// slot is replaced. This is test-only evidence that downstream authority
/// boundaries re-run `Plan::verify` rather than trusting the Rust type alone.
fn forge_in_memory_semantic_hash(plan: &mut insight_engine::Plan) {
    let plan_start = std::ptr::from_mut(plan).cast::<u8>();
    let hash_address = {
        let hash = plan.semantic_hash();
        std::ptr::from_ref(hash).cast::<u8>()
    };
    let hash_offset = unsafe { hash_address.offset_from(plan_start.cast_const()) };
    let hash_slot = unsafe { plan_start.offset(hash_offset).cast::<SemanticHash>() };
    let forged = SemanticHash::parse(format!("sha256:{}", "0".repeat(64))).unwrap();
    assert_ne!(plan.semantic_hash(), &forged);
    let original = unsafe { std::ptr::replace(hash_slot, forged) };
    drop(original);
}

#[test]
fn repository_authority_hashes_are_verified_and_content_derived() {
    let plan = verified_plan("derived_authority");
    let first = VersionedPlan::from_verified_plan(
        "definition_derived_authority",
        "agent_derived_authority",
        "Derived authority",
        DeploymentRevisionId::new("deployment_revision_derived_authority").unwrap(),
        "expression-3.0.0",
        json!({"author": "structured"}),
        &plan,
        json!({"return": "descriptor-v1"}),
        serde_json::from_str(r#"{"temperature":0,"model":"fixed"}"#).unwrap(),
        serde_json::from_str(r#"{"version":1,"implementation":"worker"}"#).unwrap(),
    )
    .unwrap();
    let reordered = VersionedPlan::from_verified_plan(
        "definition_derived_authority",
        "agent_derived_authority",
        "Derived authority",
        DeploymentRevisionId::new("deployment_revision_derived_authority").unwrap(),
        "expression-3.0.0",
        json!({"author": "structured"}),
        &plan,
        json!({"return": "descriptor-v1"}),
        serde_json::from_str(r#"{"model":"fixed","temperature":0}"#).unwrap(),
        serde_json::from_str(r#"{"implementation":"worker","version":1}"#).unwrap(),
    )
    .unwrap();
    let changed = VersionedPlan::from_verified_plan(
        "definition_derived_authority",
        "agent_derived_authority",
        "Derived authority",
        DeploymentRevisionId::new("deployment_revision_derived_authority").unwrap(),
        "expression-3.0.0",
        json!({"author": "structured"}),
        &plan,
        json!({"return": "descriptor-v1"}),
        json!({"model": "fixed", "temperature": 1}),
        json!({"implementation": "worker", "version": 1}),
    )
    .unwrap();

    assert_eq!(first.plan_hash().as_str(), plan.semantic_hash().as_str());
    assert_eq!(first.plan_hash(), reordered.plan_hash());
    assert_eq!(first.binding_hash(), reordered.binding_hash());
    assert_ne!(first.binding_hash(), changed.binding_hash());

    let mut forged_plan = plan;
    forge_in_memory_semantic_hash(&mut forged_plan);
    assert!(forged_plan.verify().is_err());
    assert_eq!(
        VersionedPlan::from_verified_plan(
            "definition_forged_authority",
            "agent_forged_authority",
            "Forged authority",
            DeploymentRevisionId::new("deployment_revision_forged_authority").unwrap(),
            "expression-3.0.0",
            json!({"author": "structured"}),
            &forged_plan,
            json!({"return": "descriptor-v1"}),
            json!({"model": "fixed"}),
            json!({"worker": "worker-v1"}),
        )
        .unwrap_err()
        .code(),
        REPOSITORY_CONFIGURATION_INVALID,
        "from_verified_plan must call Plan::verify even for an in-memory Plan value"
    );
}
