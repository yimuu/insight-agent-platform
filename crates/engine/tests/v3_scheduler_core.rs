use std::collections::BTreeMap;

use insight_engine as engine;
use insight_engine::{plan::*, scheduler::*, DefinitionRevisionId, NodeId, RunId};
use serde_json::json;

fn node_id(value: &str) -> NodeId {
    NodeId::new(value).unwrap()
}

fn control_port_id(value: &str) -> ControlPortId {
    ControlPortId::new(value).unwrap()
}

fn data_port_id(value: &str) -> DataPortId {
    DataPortId::new(value).unwrap()
}

fn control_edge_id(value: &str) -> ControlEdgeId {
    ControlEdgeId::new(value).unwrap()
}

fn data_binding_id(value: &str) -> DataBindingId {
    DataBindingId::new(value).unwrap()
}

fn phi_binding_id(value: &str) -> PhiBindingId {
    PhiBindingId::new(value).unwrap()
}

fn case_id(value: &str) -> BranchCaseId {
    BranchCaseId::new(value).unwrap()
}

fn port_name(value: &str) -> PortName {
    PortName::new(value).unwrap()
}

fn scope_id(value: &str) -> ScopeId {
    ScopeId::new(value).unwrap()
}

fn version(value: &str) -> VersionTag {
    VersionTag::new(value).unwrap()
}

fn safe_error_type() -> PlanType {
    PlanType::Object {
        properties: BTreeMap::from([
            (
                "code".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            ),
            (
                "kind".to_owned(),
                PlanProperty::new(PlanType::literal(json!("safe_error")).unwrap(), true).unwrap(),
            ),
            (
                "message".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            ),
        ]),
        additional_properties: None,
    }
}

fn descriptor(implementation: &str) -> LeafTaskDescriptor {
    LeafTaskDescriptor::new(implementation, version("1"), BTreeMap::new())
}

fn branch_plan_with_worker_policy(
    reverse_storage_order: bool,
    effect_policy: Option<&engine::WorkerEffectPolicy>,
) -> (Plan, DescriptorContractRegistry) {
    branch_plan_fixture(reverse_storage_order, effect_policy, false)
}

fn branch_plan_fixture(
    reverse_storage_order: bool,
    effect_policy: Option<&engine::WorkerEffectPolicy>,
    duplicate_llm_output: bool,
) -> (Plan, DescriptorContractRegistry) {
    let root = scope_id("root_scope");
    let branch = node_id("route");
    let merge = node_id("merge");
    let ret = node_id("return_result");

    let input_type = PlanType::Object {
        properties: BTreeMap::from([
            (
                "use_action".to_owned(),
                PlanProperty::new(PlanType::Boolean, true).unwrap(),
            ),
            (
                "use_llm".to_owned(),
                PlanProperty::new(PlanType::Boolean, true).unwrap(),
            ),
        ]),
        additional_properties: None,
    };
    let metadata = PlanMetadata::new(
        DefinitionRevisionId::new("scheduler_fixture_v1").unwrap(),
        version("compiler-1"),
        AuthorFormat::Programmatic,
        branch.clone(),
        PlanInputContract::new(input_type),
        PlanType::String,
        safe_error_type(),
    );

    let branch_llm_input = data_port_id("route_use_llm");
    let branch_action_input = data_port_id("route_use_action");
    let mut nodes = vec![Node::new(
        branch.clone(),
        root.clone(),
        NodeKind::Branch(BranchDescriptor {
            cases: vec![
                BranchCase::when(
                    case_id("llm"),
                    PureExpression::new(
                        ExpressionLanguage::Cel,
                        version(CEL_EXPRESSION_ENGINE_VERSION),
                        "use_llm && (use_action || !use_action)",
                        PlanType::Boolean,
                    )
                    .with_dependency("use_llm", branch_llm_input.clone())
                    .with_dependency("use_action", branch_action_input.clone()),
                    control_port_id("route_llm_out"),
                ),
                BranchCase::when(
                    case_id("action"),
                    PureExpression::new(
                        ExpressionLanguage::Cel,
                        version(CEL_EXPRESSION_ENGINE_VERSION),
                        "use_action",
                        PlanType::Boolean,
                    )
                    .with_dependency("use_action", branch_action_input.clone()),
                    control_port_id("route_action_out"),
                ),
                BranchCase::otherwise(case_id("else"), control_port_id("route_else_out")),
            ],
        }),
    )];
    let mut control_ports = vec![];
    let mut data_ports = vec![
        DataPort::new(
            branch_llm_input.clone(),
            branch.clone(),
            port_name("use_llm"),
            PortDirection::Input,
            PlanType::Boolean,
            true,
        ),
        DataPort::new(
            branch_action_input.clone(),
            branch.clone(),
            port_name("use_action"),
            PortDirection::Input,
            PlanType::Boolean,
            true,
        ),
    ];
    let mut control_edges = vec![];
    let mut data_bindings = vec![
        DataBinding::new(
            data_binding_id("bind_use_llm"),
            ValueSource::RunInput {
                path: vec!["use_llm".to_owned()],
            },
            branch_llm_input,
        ),
        DataBinding::new(
            data_binding_id("bind_use_action"),
            ValueSource::RunInput {
                path: vec!["use_action".to_owned()],
            },
            branch_action_input,
        ),
    ];
    let mut merge_arms = BTreeMap::new();
    let mut phi_sources = BTreeMap::new();

    for (case, kind) in [
        ("llm", LeafTaskKind::Llm),
        ("action", LeafTaskKind::Action),
        ("else", LeafTaskKind::Action),
    ] {
        let task = node_id(&format!("task_{case}"));
        let implementation = format!("fixture.{case}");
        let node_kind = match kind {
            LeafTaskKind::Llm => NodeKind::LlmTask(descriptor(&implementation)),
            LeafTaskKind::Action => NodeKind::ActionTask(descriptor(&implementation)),
            _ => unreachable!(),
        };
        nodes.push(Node::new(task.clone(), root.clone(), node_kind));

        let branch_out = control_port_id(&format!("route_{case}_out"));
        let task_in = control_port_id(&format!("task_{case}_in"));
        let task_out = control_port_id(&format!("task_{case}_out"));
        let merge_in = control_port_id(&format!("merge_{case}_in"));
        let task_value = data_port_id(&format!("task_{case}_value"));
        control_ports.extend([
            ControlPort::new(
                branch_out.clone(),
                branch.clone(),
                port_name(case),
                PortDirection::Output,
            ),
            ControlPort::new(
                task_in.clone(),
                task.clone(),
                port_name("in"),
                PortDirection::Input,
            ),
            ControlPort::new(
                task_out.clone(),
                task.clone(),
                port_name("out"),
                PortDirection::Output,
            ),
            ControlPort::new(
                merge_in.clone(),
                merge.clone(),
                port_name(case),
                PortDirection::Input,
            ),
        ]);
        data_ports.push(DataPort::new(
            task_value.clone(),
            task.clone(),
            port_name("value"),
            PortDirection::Output,
            PlanType::String,
            false,
        ));
        if duplicate_llm_output && kind == LeafTaskKind::Llm {
            data_ports.push(DataPort::new(
                data_port_id("task_llm_duplicate"),
                task,
                port_name("duplicate"),
                PortDirection::Output,
                PlanType::String,
                false,
            ));
        }
        control_edges.extend([
            ControlEdge::new(
                control_edge_id(&format!("edge_route_{case}")),
                branch_out,
                task_in,
            ),
            ControlEdge::new(
                control_edge_id(&format!("edge_{case}_merge")),
                task_out,
                merge_in.clone(),
            ),
        ]);
        merge_arms.insert(case_id(case), merge_in);
        phi_sources.insert(
            case_id(case),
            ValueSource::Port {
                port_id: task_value,
            },
        );
    }

    let merge_out = control_port_id("merge_out");
    let merge_value = data_port_id("merge_value");
    nodes.push(Node::new(
        merge.clone(),
        root.clone(),
        NodeKind::Merge(MergeDescriptor {
            branch_node_id: branch,
            arms: merge_arms,
            output_port: merge_out.clone(),
        }),
    ));
    control_ports.push(ControlPort::new(
        merge_out.clone(),
        merge.clone(),
        port_name("out"),
        PortDirection::Output,
    ));
    data_ports.push(DataPort::new(
        merge_value.clone(),
        merge.clone(),
        port_name("result"),
        PortDirection::Output,
        PlanType::String,
        false,
    ));

    let return_in = control_port_id("return_in");
    let return_value = data_port_id("return_value");
    nodes.push(Node::new(
        ret.clone(),
        root.clone(),
        NodeKind::Return(ReturnDescriptor {
            value_input: return_value.clone(),
        }),
    ));
    control_ports.push(ControlPort::new(
        return_in.clone(),
        ret.clone(),
        port_name("in"),
        PortDirection::Input,
    ));
    data_ports.push(DataPort::new(
        return_value.clone(),
        ret,
        port_name("value"),
        PortDirection::Input,
        PlanType::String,
        true,
    ));
    control_edges.push(ControlEdge::new(
        control_edge_id("edge_merge_return"),
        merge_out,
        return_in,
    ));
    data_bindings.push(DataBinding::from_port(
        data_binding_id("bind_return"),
        merge_value.clone(),
        return_value,
    ));

    let mut builder = PlanBuilder::new(metadata);
    builder.add_scope(ScopeMetadata::root(root));
    if reverse_storage_order {
        nodes.reverse();
        control_ports.reverse();
        data_ports.reverse();
        control_edges.reverse();
        data_bindings.reverse();
    }
    for node in nodes {
        builder.add_node(node);
    }
    for port in control_ports {
        builder.add_control_port(port);
    }
    for port in data_ports {
        builder.add_data_port(port);
    }
    for edge in control_edges {
        builder.add_control_edge(edge);
    }
    for binding in data_bindings {
        builder.add_data_binding(binding);
    }
    builder.add_phi_binding(PhiBinding::new(
        phi_binding_id("phi_merge"),
        merge,
        merge_value,
        phi_sources,
    ));
    let plan = builder.build().unwrap();

    let mut descriptors = DescriptorContractRegistry::new();
    for (name, kind) in [
        ("llm", LeafTaskKind::Llm),
        ("action", LeafTaskKind::Action),
        ("else", LeafTaskKind::Action),
    ] {
        let mut outputs = BTreeMap::from([(port_name("value"), PlanType::String)]);
        if duplicate_llm_output && kind == LeafTaskKind::Llm {
            outputs.insert(port_name("duplicate"), PlanType::String);
        }
        let mut worker = WorkerContract::new(kind, version("worker-1"), BTreeMap::new(), outputs);
        if let Some(effect_policy) = effect_policy {
            worker = worker.with_effect_policy(effect_policy.clone());
        }
        descriptors
            .register(DescriptorContract::new(
                format!("fixture.{name}"),
                version("1"),
                DescriptorConfigurationContract::empty(),
                worker,
            ))
            .unwrap();
    }
    (plan, descriptors)
}

fn branch_plan(reverse_storage_order: bool) -> (Plan, DescriptorContractRegistry) {
    branch_plan_with_worker_policy(reverse_storage_order, None)
}

fn action(decision: &SchedulerDecision) -> &PlannedSchedulerAction {
    decision.action().expect("expected scheduler action")
}

fn apply(action: &PlannedSchedulerAction, facts: &mut SchedulerFacts) {
    match action.intent().action() {
        SchedulerAction::AdmitActivation { activation_id, .. } => {
            facts.record_activation(activation_id.clone());
        }
        SchedulerAction::ConsumeToken { token_id, .. } => {
            facts.record_consumed_token(token_id.clone());
        }
        SchedulerAction::EmitToken { token_id, .. } => {
            facts.record_emitted_token(token_id.clone());
        }
        SchedulerAction::DispatchTask { task_id, .. } => {
            facts.record_dispatched_task(task_id.clone());
        }
        SchedulerAction::CommitNativeOutput {
            output: NativeOutput::Values { values },
            ..
        } => {
            for (port, value) in values {
                facts.record_value(port.clone(), value.clone());
            }
        }
        SchedulerAction::SelectBranchAndAdmit { selection } => {
            facts.record_occurrence_branch_selection(
                selection.occurrence().clone(),
                selection.branch_node_id().clone(),
                selection.case_id().clone(),
            );
            facts.record_emitted_token(selection.token_id().clone());
            facts.record_activation(selection.successor().activation_id().clone());
            facts.record_consumed_token(selection.token_id().clone());
        }
        SchedulerAction::CompleteRun { output, .. } => {
            facts.record_terminal(RunTerminalFact::Succeeded(output.clone()));
        }
        SchedulerAction::FailRun { error, .. } => {
            facts.record_terminal(RunTerminalFact::Failed(error.clone()));
        }
        action => panic!("unexpected advanced scheduler action in core fixture: {action:?}"),
    }
    facts.commit_checkpoint(action.intent().checkpoint_id().clone());
    facts.set_projection_version(facts.projection_version() + 1);
}

fn drive(input: serde_json::Value) -> (Vec<&'static str>, SchedulerFacts) {
    let (plan, descriptors) = branch_plan(false);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_scheduler_fixture").unwrap(),
        7,
        RuntimeValue::new(input).unwrap(),
    );
    let mut trace = Vec::new();

    for _ in 0..80 {
        match planner.plan(&facts).unwrap() {
            SchedulerDecision::Action(planned) => {
                let dispatched = match planned.intent().action() {
                    SchedulerAction::AdmitActivation { .. } => {
                        trace.push("admit");
                        None
                    }
                    SchedulerAction::ConsumeToken { .. } => {
                        trace.push("consume");
                        None
                    }
                    SchedulerAction::EmitToken { .. } => {
                        trace.push("emit");
                        None
                    }
                    SchedulerAction::DispatchTask {
                        task_id,
                        node_id,
                        task_kind,
                        ..
                    } => {
                        trace.push(match task_kind {
                            SchedulerTaskKind::Llm => "dispatch_llm",
                            SchedulerTaskKind::Action => "dispatch_action",
                            SchedulerTaskKind::Retrieval => "dispatch_retrieval",
                            SchedulerTaskKind::Http => "dispatch_http",
                            SchedulerTaskKind::Tool => "dispatch_tool",
                        });
                        Some((task_id.clone(), node_id.clone()))
                    }
                    SchedulerAction::CommitNativeOutput {
                        node_id, output, ..
                    } => {
                        assert!(matches!(output, NativeOutput::Values { .. }));
                        trace.push(match node_id.as_str() {
                            "merge" => "merge",
                            "return_result" => "return",
                            other => panic!("unexpected native-output node: {other}"),
                        });
                        None
                    }
                    SchedulerAction::SelectBranchAndAdmit { .. } => {
                        trace.push("branch");
                        None
                    }
                    SchedulerAction::CompleteRun { .. } => {
                        trace.push("complete");
                        None
                    }
                    SchedulerAction::FailRun { .. } => {
                        trace.push("fail");
                        None
                    }
                    action => {
                        panic!("unexpected advanced scheduler action in core fixture: {action:?}")
                    }
                };
                apply(&planned, &mut facts);
                if let Some((task_id, node_id)) = dispatched {
                    assert!(matches!(
                        planner.plan(&facts).unwrap(),
                        SchedulerDecision::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
                    ));
                    let case = node_id.as_str().strip_prefix("task_").unwrap();
                    facts.record_value(
                        data_port_id(&format!("task_{case}_value")),
                        RuntimeValue::new(json!(case)).unwrap(),
                    );
                    facts.record_completed_task(task_id);
                    facts.set_projection_version(facts.projection_version() + 1);
                }
            }
            SchedulerDecision::Quiescent(SchedulerQuiescence::RunSucceeded) => {
                return (trace, facts);
            }
            other => panic!("unexpected terminal decision: {other:?}"),
        }
    }
    panic!("scheduler did not reach Return")
}

#[test]
fn minimal_branch_llm_merge_return_flow_is_planned_one_closed_action_at_a_time() {
    let (trace, facts) = drive(json!({"use_llm": true, "use_action": true}));
    assert_eq!(
        trace,
        vec![
            "admit",
            "branch",
            "dispatch_llm",
            "emit",
            "admit",
            "consume",
            "merge",
            "emit",
            "admit",
            "consume",
            "return",
            "complete"
        ]
    );
    assert_eq!(
        facts.terminal(),
        Some(&RunTerminalFact::Succeeded(
            RuntimeValue::new(json!("llm")).unwrap()
        ))
    );
}

#[test]
fn branch_uses_declaration_order_then_action_case_then_else_case() {
    let (llm_trace, first_true) = drive(json!({"use_llm": true, "use_action": true}));
    let (action_trace, action) = drive(json!({"use_llm": false, "use_action": true}));
    let (fallback_trace, fallback) = drive(json!({"use_llm": false, "use_action": false}));
    assert!(llm_trace.contains(&"dispatch_llm"));
    assert!(action_trace.contains(&"dispatch_action"));
    assert!(fallback_trace.contains(&"dispatch_action"));
    assert_eq!(
        first_true.terminal(),
        Some(&RunTerminalFact::Succeeded(
            RuntimeValue::new(json!("llm")).unwrap()
        ))
    );
    assert_eq!(
        action.terminal(),
        Some(&RunTerminalFact::Succeeded(
            RuntimeValue::new(json!("action")).unwrap()
        ))
    );
    assert_eq!(
        fallback.terminal(),
        Some(&RunTerminalFact::Succeeded(
            RuntimeValue::new(json!("else")).unwrap()
        ))
    );
}

#[test]
fn repeated_projection_is_byte_equivalent_and_committed_checkpoint_is_not_replanned() {
    let (plan, descriptors) = branch_plan(false);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_replay").unwrap(),
        11,
        RuntimeValue::new(json!({"use_llm": true, "use_action": false})).unwrap(),
    );
    let first = planner.plan(&facts).unwrap();
    let replay = planner.plan(&facts).unwrap();
    assert_eq!(first, replay);
    assert_eq!(
        serde_jcs::to_vec(&first).unwrap(),
        serde_jcs::to_vec(&replay).unwrap()
    );

    let first_checkpoint = action(&first).intent().checkpoint_id().clone();
    apply(action(&first), &mut facts);
    let next = planner.plan(&facts).unwrap();
    assert_ne!(action(&next).intent().checkpoint_id(), &first_checkpoint);
    assert!(matches!(
        action(&next).intent().action(),
        SchedulerAction::SelectBranchAndAdmit { .. }
    ));
    assert_eq!(next, planner.plan(&facts).unwrap());
}

#[test]
fn missing_output_and_runtime_type_errors_fail_closed() {
    let (plan, descriptors) = branch_plan(false);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let planner = SchedulerPlanner::new(&linked);
    let bad_input = SchedulerFacts::new(
        RunId::new("run_bad_input").unwrap(),
        1,
        RuntimeValue::new(json!({"use_llm": null, "use_action": false})).unwrap(),
    );
    assert_eq!(
        planner.plan(&bad_input).unwrap_err().code(),
        SCHEDULER_VALUE_TYPE_MISMATCH
    );

    let mut facts = SchedulerFacts::new(
        RunId::new("run_missing_output").unwrap(),
        1,
        RuntimeValue::new(json!({"use_llm": true, "use_action": false})).unwrap(),
    );
    loop {
        let decision = planner.plan(&facts).unwrap();
        let planned = action(&decision);
        let dispatched = match planned.intent().action() {
            SchedulerAction::DispatchTask { task_id, .. } => Some(task_id.clone()),
            _ => None,
        };
        apply(planned, &mut facts);
        if let Some(task_id) = dispatched {
            facts.record_completed_task(task_id);
            break;
        }
    }
    assert_eq!(
        planner.plan(&facts).unwrap_err().code(),
        SCHEDULER_FACT_MISSING
    );
    facts.record_value(
        data_port_id("task_llm_value"),
        RuntimeValue::new(json!(42)).unwrap(),
    );
    assert_eq!(
        planner.plan(&facts).unwrap_err().code(),
        SCHEDULER_VALUE_TYPE_MISMATCH
    );
}

#[test]
fn every_runtime_identity_is_stable_boundary_safe_and_semantic_order_independent() {
    let (plan, descriptors) = branch_plan(false);
    let (reordered, reordered_descriptors) = branch_plan(true);
    assert_eq!(plan.semantic_hash(), reordered.semantic_hash());

    let run = RunId::new("run_identity").unwrap();
    let occurrence = LogicalOccurrence::entry().child("edge:one").unwrap();
    let ids = DeterministicIds::new(&run, plan.semantic_hash());
    let scope = ids
        .scope_instance(&scope_id("root_scope"), &LogicalOccurrence::root_scope())
        .unwrap();
    let node = node_id("task_llm");
    let activation = ids.activation(&node, &scope, &occurrence).unwrap();
    assert_eq!(
        activation,
        ids.activation(&node, &scope, &occurrence).unwrap()
    );
    assert_eq!(
        ids.control_token(&node, &scope, &occurrence, &control_port_id("task_llm_out"))
            .unwrap(),
        ids.control_token(&node, &scope, &occurrence, &control_port_id("task_llm_out"))
            .unwrap()
    );
    assert_eq!(
        ids.effect(&node, &scope, &occurrence).unwrap(),
        ids.effect(&node, &scope, &occurrence).unwrap()
    );
    assert_eq!(
        ids.task(&node, &scope, &occurrence),
        ids.task(&node, &scope, &occurrence)
    );
    assert_eq!(
        ids.timer(&node, &scope, &occurrence, "timeout").unwrap(),
        ids.timer(&node, &scope, &occurrence, "timeout").unwrap()
    );

    let ambiguous_a = LogicalOccurrence::entry()
        .child("a:b")
        .unwrap()
        .child("c")
        .unwrap();
    let ambiguous_b = LogicalOccurrence::entry()
        .child("a")
        .unwrap()
        .child("b:c")
        .unwrap();
    assert_ne!(
        ids.task(&node, &scope, &ambiguous_a),
        ids.task(&node, &scope, &ambiguous_b)
    );

    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let reordered_linked = LinkedPlan::link(&reordered, &reordered_descriptors, &subflows).unwrap();
    let facts = SchedulerFacts::new(
        run,
        3,
        RuntimeValue::new(json!({"use_llm": true, "use_action": false})).unwrap(),
    );
    assert_eq!(
        SchedulerPlanner::new(&linked).plan(&facts).unwrap(),
        SchedulerPlanner::new(&reordered_linked)
            .plan(&facts)
            .unwrap()
    );
}
