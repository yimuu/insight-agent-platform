use super::*;
use insight_platform_contracts::checked_in_hard_limit_profile;

fn model_node(calls: u32, parallel: u16, skill: bool, capability: bool) -> RuntimeNode {
    let key = PlanNodeKey::new("model".into()).unwrap();
    let schema: Sha256Digest = format!("sha256:{}", "a".repeat(64)).parse().unwrap();
    RuntimeNode::ModelLoop {
        model_slot_id: "model".into(),
        skill_slot_ids: if skill { vec!["skill".into()] } else { vec![] },
        capability_slot_ids: if capability {
            vec!["capability".into()]
        } else {
            vec![]
        },
        input: ExactDataPortRef::RunInput {
            schema_digest: schema.clone(),
        },
        model_route: None,
        output: ExactDataPortRef::NodeOutput {
            producer_node_id: key,
            port_id: DataPortKey::new("response".into()).unwrap(),
            schema_digest: schema,
        },
        maximum_rounds: 1,
        maximum_capability_calls: calls,
        maximum_parallel_calls_per_round: parallel,
        token_budget: 10240,
        resume: PlanNodeKey::new("done".into()).unwrap(),
    }
}

#[test]
fn model_loop_tool_budget_respects_selected_slots() {
    let key = PlanNodeKey::new("model".into()).unwrap();
    let limits = PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
    for (calls, parallel, skill, capability, accepted) in [
        (0, 0, false, false, true),
        (0, 1, false, false, false),
        (1, 0, false, false, false),
        (0, 0, true, false, false),
        (0, 0, false, true, false),
        (0, 0, true, true, false),
        (1, 1, false, false, true),
        (8, 2, true, true, true),
        (1, 2, false, false, false),
    ] {
        assert_eq!(
            validate_node(
                &key,
                &model_node(calls, parallel, skill, capability),
                limits
            )
            .is_ok(),
            accepted,
            "calls={calls} parallel={parallel} skill={skill} capability={capability}"
        );
    }
    for zero_rounds in [true, false] {
        let mut node = model_node(0, 0, false, false);
        if let RuntimeNode::ModelLoop {
            maximum_rounds,
            token_budget,
            ..
        } = &mut node
        {
            if zero_rounds {
                *maximum_rounds = 0;
            } else {
                *token_budget = 0;
            }
        }
        assert!(validate_node(&key, &node, limits).is_err());
    }
}
