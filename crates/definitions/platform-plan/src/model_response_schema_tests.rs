use super::*;
use insight_platform_contracts::checked_in_hard_limit_profile;
use serde_json::json;

fn plan() -> (RuntimePlan, PlanNodeKey, ClosedJsonSchema) {
    let schema = ClosedJsonSchema::build(json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema", "type":"object",
        "additionalProperties":false, "required":["draft"],
        "properties":{"draft":{"type":"string","minLength":0,"maxLength":256,"x-platform-max-bytes":1024}}
    }))
    .unwrap();
    let model = PlanNodeKey::new("model".into()).unwrap();
    let done = PlanNodeKey::new("done".into()).unwrap();
    let output = ExactDataPortRef::NodeOutput {
        producer_node_id: model.clone(),
        port_id: DataPortKey::new("draft".into()).unwrap(),
        schema_digest: schema.canonical_digest.clone(),
    };
    let value_schema = ClosedValueSchema::try_from(schema.clone()).unwrap();
    (
        RuntimePlan {
            plan_version: 6,
            interface_contract_digest: schema.canonical_digest.clone(),
            entry_node_id: model.clone(),
            dependency_slots: BTreeMap::from([(
                "model".into(),
                RuntimeDependencySlot {
                    kind: RuntimeDependencyKind::Model,
                    requirement_digest: schema.canonical_digest.clone(),
                },
            )]),
            schema_documents: BTreeMap::from([(schema.canonical_digest.clone(), value_schema)]),
            nodes: BTreeMap::from([
                (
                    model.clone(),
                    RuntimeNode::ModelLoop {
                        model_slot_id: "model".into(),
                        skill_slot_ids: vec![],
                        capability_slot_ids: vec![],
                        input: ExactDataPortRef::RunInput {
                            schema_digest: schema.canonical_digest.clone(),
                        },
                        model_route: None,
                        output: output.clone(),
                        maximum_rounds: 1,
                        maximum_capability_calls: 0,
                        maximum_parallel_calls_per_round: 0,
                        token_budget: 10240,
                        resume: done.clone(),
                    },
                ),
                (done, RuntimeNode::Return { value: output }),
            ]),
        },
        model,
        schema,
    )
}

#[test]
fn exact_model_output_document_is_required_and_validated() {
    let (plan, key, expected) = plan();
    let limits = PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
    plan.validate(limits).unwrap();
    assert_eq!(plan.model_response_schema(&key).unwrap(), expected);
    assert!(plan
        .model_response_schema(&PlanNodeKey::new("done".into()).unwrap())
        .is_err());
    assert!(plan
        .model_response_schema(&PlanNodeKey::new("absent".into()).unwrap())
        .is_err());
    for corruption in 0..3 {
        let mut bad = plan.clone();
        let schema = bad
            .schema_documents
            .get_mut(&expected.canonical_digest)
            .unwrap();
        match corruption {
            0 => schema.schema["required"] = json!([]),
            1 => schema.canonical_digest = format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            _ => {
                bad.schema_documents.clear();
            }
        }
        assert!(bad.model_response_schema(&key).is_err());
        assert!(bad.validate(limits).is_err());
    }
}

#[test]
fn model_outputs_require_objects_while_other_value_schemas_remain_internal() {
    let (mut plan, key, _) = plan();
    let scalar=ClosedValueSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"integer","minimum":0,"maximum":9})).unwrap();
    plan.schema_documents
        .insert(scalar.canonical_digest.clone(), scalar.clone());
    let limits = PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
    plan.validate(limits).unwrap();
    let RuntimeNode::ModelLoop { output, .. } = plan.nodes.get_mut(&key).unwrap() else {
        panic!()
    };
    *output = ExactDataPortRef::NodeOutput {
        producer_node_id: key.clone(),
        port_id: DataPortKey::new("draft".into()).unwrap(),
        schema_digest: scalar.canonical_digest,
    };
    assert!(plan.model_response_schema(&key).is_err());
    assert!(plan.validate(limits).is_err());
}

#[test]
fn previous_model_output_semantics_cannot_claim_current_plans() {
    let old = serde_json::json!({"contract":"insight.platform/program-semantic-identity","identity_version":1,
        "profile":"insight.platform/program-interpreter/ir-v6/semantics-v2","ir_abi_version":6,
        "frozen_schema_documents_abi":1,"internal_value_schema_profile":insight_platform_contracts::CLOSED_VALUE_SCHEMA_PROFILE_ID,
        "human_task_typed_eligibility_abi":1,"human_task_frozen_response_schema_abi":1,"orchestration_job_payload_abi":2,
        "external_leaf_success_same_node_structural_resume_abi":1,"bounded_run_convergence_and_parent_control_abi":1,
        "child_budget_admission_anchor_abi":1,"model_loop_zero_tool_budget_abi":1});
    let capability = insight_platform_contracts::WorkerExecutionCapability::Program {
        program_semantic_identity: canonical_digest(&old).unwrap().parse().unwrap(),
        ir_abi_version: 6,
    };
    let (_, _, schema) = plan();
    let required = execution::program_execution_requirement(schema.canonical_digest, 6).unwrap();
    assert!(!capability.supports(&required));
    assert!(execution::program_execution_capability(6)
        .unwrap()
        .supports(&required));
}
