use super::*;
use insight_platform_contracts::{checked_in_hard_limit_profile, ClosedJsonValue};
use serde_json::json;

fn comparison_plan(literal: i64) -> (RuntimePlan, PlanNodeKey, TypedExpressionProgram) {
    let integer = ClosedValueSchema::build(json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema", "type":"integer", "minimum":0, "maximum":10,
    })).unwrap();
    let boolean = ClosedValueSchema::build(json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema", "type":"boolean",
    }))
    .unwrap();
    let compute = PlanNodeKey::new("compute".into()).unwrap();
    let done = PlanNodeKey::new("done".into()).unwrap();
    let expression = TypedExpressionProgram::build(
        vec![],
        vec![
            TypedInstruction::Literal {
                value: ClosedJsonValue::build(integer.canonical_digest.clone(), json!(literal))
                    .unwrap(),
            },
            TypedInstruction::Literal {
                value: ClosedJsonValue::build(integer.canonical_digest.clone(), json!(0)).unwrap(),
            },
            TypedInstruction::Greater,
        ],
        boolean.canonical_digest.clone(),
        ExpressionLimits::ABSOLUTE,
    )
    .unwrap();
    let output = ExactDataPortRef::NodeOutput {
        producer_node_id: compute.clone(),
        port_id: DataPortKey::new("positive".into()).unwrap(),
        schema_digest: boolean.canonical_digest.clone(),
    };
    let plan = RuntimePlan {
        plan_version: 6,
        interface_contract_digest: boolean.canonical_digest.clone(),
        entry_node_id: compute.clone(),
        dependency_slots: BTreeMap::new(),
        schema_documents: [
            (integer.canonical_digest.clone(), integer),
            (boolean.canonical_digest.clone(), boolean),
        ]
        .into(),
        nodes: [
            (
                compute.clone(),
                RuntimeNode::Compute {
                    assignments: vec![PortAssignment {
                        output_port: output.clone(),
                        expression: expression.clone(),
                    }],
                    next: done.clone(),
                },
            ),
            (done, RuntimeNode::Return { value: output }),
        ]
        .into(),
    };
    (plan, compute, expression)
}

#[test]
fn intermediate_literal_cannot_evade_its_frozen_constraints_with_valid_boolean_output() {
    let (plan, node, expression) = comparison_plan(11);
    // Default portable Plan validation stays identical between WASM and native authoring.
    plan.validate(PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap())
        .unwrap();
    let output = expression
        .evaluate(&BTreeMap::new(), ExpressionLimits::ABSOLUTE)
        .unwrap();
    assert_eq!(output.value, json!(true));
    plan.validate_value_instance(&output.schema_digest, &output.value)
        .unwrap();
    assert_eq!(
        plan.validate_node_literal_instances(&node),
        Err(PlanError::InvalidPlan)
    );
}

#[test]
fn bounded_literal_endpoints_execute_and_unknown_node_is_rejected() {
    for literal in [0, 10] {
        let (plan, node, _) = comparison_plan(literal);
        plan.validate_node_literal_instances(&node).unwrap();
        assert_eq!(
            plan.validate_node_literal_instances(&PlanNodeKey::new("unknown".into()).unwrap()),
            Err(PlanError::UnknownPlanNodeReference)
        );
    }
}
