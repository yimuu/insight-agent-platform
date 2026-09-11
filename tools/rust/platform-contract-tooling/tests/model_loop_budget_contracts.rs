use insight_platform_agent_compiler::evaluation_schema::evaluation_plan_request_schema;
use insight_platform_contract_tooling::machine::repository_root_from_manifest;
use serde_json::{json, Value};

#[test]
fn tool_budget_projections_accept_only_paired_zero_or_positive_values() {
    let profile: Value = serde_json::from_slice(
        &std::fs::read(
            repository_root_from_manifest()
                .join("contracts/platform-v1/schemas/agent-authoring-profile-v1.schema.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let evaluation = evaluation_plan_request_schema();
    for schema in [
        &profile["$defs"]["ModelLoopLimits"],
        &evaluation["$defs"]["CompilerProfile"]["properties"]["model_loop"],
    ] {
        let validator = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(schema)
            .unwrap();
        for (total, parallel, accepted) in [
            (0, 0, true),
            (1, 1, true),
            (8, 2, true),
            (0, 1, false),
            (1, 0, false),
            (-1, 0, false),
            (0, -1, false),
        ] {
            let value = json!({"maximum_rounds":1,"maximum_capability_calls":total,"maximum_parallel_calls_per_round":parallel,"token_budget":10240});
            assert_eq!(validator.is_valid(&value), accepted, "{value}");
            for field in ["maximum_rounds", "token_budget"] {
                let mut invalid = value.clone();
                invalid[field] = json!(0);
                assert!(!validator.is_valid(&invalid));
            }
        }
    }
    // Independent ChildBudget still has a strictly positive call allowance.
    assert_eq!(
        evaluation["$defs"]["ChildBudget"]["properties"]["maximum_capability_calls"]["minimum"],
        1
    );
}
