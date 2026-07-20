use std::collections::BTreeMap;

use engine::{plan::*, DefinitionRevisionId, NodeId, RunId};
use insight_agent_platform::{
    dsl::v3::{compile_source, CompileOptions},
    engine,
};
use serde_json::json;

#[path = "../src/engine/scheduler/mod.rs"]
#[allow(dead_code)]
mod scheduler;
use scheduler::*;

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

fn branch_plan_with_duplicate_llm_output() -> (Plan, DescriptorContractRegistry) {
    branch_plan_fixture(false, None, true)
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
fn scheduler_evaluates_the_versioned_lazy_match_expression() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {route: string}
output: string
workflow:
  steps:
    - return:
        match: $route
        cases: {image: vision}
        default: report
"#;
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("scheduler_match_fixture").unwrap(),
            "scheduler-match.yaml",
            source,
        ),
    )
    .unwrap();
    let descriptors = DescriptorContractRegistry::new();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_scheduler_match").unwrap(),
        0,
        RuntimeValue::new(json!({"route": "image"})).unwrap(),
    );
    for _ in 0..8 {
        match planner.plan(&facts).unwrap() {
            SchedulerDecision::Action(planned) => apply(&planned, &mut facts),
            SchedulerDecision::Quiescent(SchedulerQuiescence::RunSucceeded) => break,
            decision => panic!("unexpected Match scheduler decision: {decision:?}"),
        }
    }
    assert_eq!(
        facts.terminal(),
        Some(&RunTerminalFact::Succeeded(
            RuntimeValue::new(json!("vision")).unwrap()
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

#[derive(Clone)]
struct DurableFixtureExecutor {
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

struct SlowFixtureExecutor;

#[derive(Clone)]
struct DelayedArtifactStore {
    inner: insight_agent_platform::engine::LocalContentAddressedArtifactStore,
    put_delay: std::time::Duration,
    put_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl insight_agent_platform::engine::WorkerArtifactStore for DelayedArtifactStore {
    fn inline_threshold_bytes(&self) -> usize {
        self.inner.inline_threshold_bytes()
    }

    fn storage_locator(
        &self,
        artifact: &insight_agent_platform::engine::ArtifactRef,
    ) -> Result<
        insight_agent_platform::engine::repository::StorageLocator,
        insight_agent_platform::engine::repository::RepositoryError,
    > {
        self.inner.storage_locator(artifact)
    }

    async fn put_and_verify(
        &self,
        artifact: &insight_agent_platform::engine::ArtifactRef,
        bytes: &[u8],
    ) -> Result<
        (insight_agent_platform::engine::ContentHash, u64),
        insight_agent_platform::engine::repository::RepositoryError,
    > {
        self.put_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(self.put_delay).await;
        self.inner.put_and_verify(artifact, bytes).await
    }

    async fn delete(
        &self,
        artifact: &insight_agent_platform::engine::ArtifactRef,
        locator: &insight_agent_platform::engine::repository::StorageLocator,
    ) -> Result<(), insight_agent_platform::engine::repository::RepositoryError> {
        self.inner.delete(artifact, locator).await
    }
}

#[async_trait::async_trait]
impl insight_agent_platform::engine::LeafTaskExecutor for SlowFixtureExecutor {
    async fn execute(
        &self,
        _context: &insight_agent_platform::engine::WorkerExecutionContext,
        request: &insight_agent_platform::engine::TaskExecutionRequest,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<
        insight_agent_platform::engine::TaskExecutionResult,
        insight_agent_platform::engine::WorkerFailure,
    > {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let output = request.outputs().first().expect("fixture output");
        Ok(insight_agent_platform::engine::TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                insight_agent_platform::engine::RuntimeValue::new(json!("late")).unwrap(),
            )]),
            insight_agent_platform::engine::EffectEvidence::Committed,
        ))
    }
}

async fn create_active_sqlite_scheduler_run(
    repository: &insight_agent_platform::engine::repository::SqliteDurableRepository,
    control: &sqlx::SqlitePool,
    versioned: &insight_agent_platform::engine::repository::VersionedPlan,
    run_id: &RunId,
) -> insight_agent_platform::engine::repository::FencedSchedulerRunCommand {
    use insight_agent_platform::engine::{
        repository::{CreateRunCommand, DurableRepository},
        TransitionKey, TransitionOutcome,
    };

    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("durable.scheduler.worker.gate", &[run_id.as_str()]).unwrap(),
                CreateRunCommand::new(
                    run_id.clone(),
                    versioned,
                    json!({"use_llm": true, "use_action": true}),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let owner = format!("scheduler-{}", run_id.as_str());
    let token = format!("fence-{}", run_id.as_str());
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner=?,scheduler_fencing_token=?,
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(&owner)
    .bind(&token)
    .bind(run_id.as_str())
    .execute(control)
    .await
    .unwrap();
    insight_agent_platform::engine::repository::FencedSchedulerRunCommand::new(
        run_id.clone(),
        owner,
        1,
        token,
    )
    .unwrap()
}

async fn drive_sqlite_to_task(
    repository: &insight_agent_platform::engine::repository::SqliteDurableRepository,
    linked: &LinkedPlan<'_>,
    fence: &insight_agent_platform::engine::repository::FencedSchedulerRunCommand,
) {
    use insight_agent_platform::engine::repository::{
        drive_scheduler_until_quiescent, NoSchedulerCrash, SchedulerRecoveryOutcome,
    };
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, linked, fence, &NoSchedulerCrash, 64)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(
            insight_agent_platform::engine::SchedulerQuiescence::WaitingForTask { .. }
        )
    ));
}

#[async_trait::async_trait]
impl insight_agent_platform::engine::LeafTaskExecutor for DurableFixtureExecutor {
    async fn execute(
        &self,
        context: &insight_agent_platform::engine::WorkerExecutionContext,
        request: &insight_agent_platform::engine::TaskExecutionRequest,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<
        insight_agent_platform::engine::TaskExecutionResult,
        insight_agent_platform::engine::WorkerFailure,
    > {
        assert!(context.attempt_no().get() >= 1);
        assert!(!context.fencing_token().is_empty());
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let selected = request
            .implementation()
            .strip_prefix("fixture.")
            .expect("fixture implementation");
        Ok(insight_agent_platform::engine::TaskExecutionResult::new(
            request
                .outputs()
                .iter()
                .map(|output| {
                    (
                        output.port_id().clone(),
                        insight_agent_platform::engine::RuntimeValue::new(json!(selected)).unwrap(),
                    )
                })
                .collect(),
            insight_agent_platform::engine::EffectEvidence::Committed,
        ))
    }
}

#[tokio::test]
async fn sqlite_durable_scheduler_recovers_after_result_commit_without_reexecuting_effect() {
    use insight_agent_platform::engine::{
        repository::{
            consume_scheduler_task_once, drive_scheduler_once, drive_scheduler_until_quiescent,
            CreateRunCommand, DurableRepository, FailOnceSchedulerCrash, FencedSchedulerRunCommand,
            NoSchedulerCrash, PlanInstallOutcome, SchedulerCrashPoint, SchedulerDriveOutcome,
            SchedulerDurableRepository, SchedulerRecoveryOutcome, SchedulerWorkerPumpOutcome,
            SqliteDurableRepository, TerminalSchedulerWorkerFailurePolicy, VersionedPlan,
        },
        DeploymentRevisionId, RunLifecycle, TransitionKey, TransitionOutcome,
        WorkerExecutorRegistry,
    };

    let (plan, descriptors) = branch_plan(false);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = VersionedPlan::from_verified_plan(
        "durable-scheduler-fixture",
        "durable-scheduler-agent",
        "Durable scheduler fixture",
        DeploymentRevisionId::new("durable_scheduler_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "programmatic"}),
        &plan,
        json!({"fixture": "descriptor-v1"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("durable-scheduler.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_durable_scheduler_crash_recovery").unwrap();
    let created = repository
        .create_run(
            TransitionKey::derive("durable.scheduler.test", &["create"]).unwrap(),
            CreateRunCommand::new(
                run_id.clone(),
                &versioned,
                json!({"use_llm": true, "use_action": true}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(created, TransitionOutcome::Committed { .. }));
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&database)
        .foreign_keys(true);
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='scheduler-test',scheduler_fencing_token='scheduler-test-fence',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    control.close().await;
    let fence =
        FencedSchedulerRunCommand::new(run_id.clone(), "scheduler-test", 1, "scheduler-test-fence")
            .unwrap();

    let mut waiting = None;
    for step in 0..64 {
        match drive_scheduler_once(&repository, &linked, &fence, &NoSchedulerCrash)
            .await
            .unwrap_or_else(|error| panic!("initial durable drive step {step} failed: {error:?}"))
        {
            SchedulerDriveOutcome::Applied(_) => {}
            SchedulerDriveOutcome::Quiescent(value) => {
                waiting = Some(SchedulerRecoveryOutcome::Quiescent(value));
                break;
            }
            SchedulerDriveOutcome::Fenced => panic!("fixture scheduler was unexpectedly fenced"),
        }
    }
    let waiting = waiting.expect("fixture reached quiescence");
    assert!(matches!(
        waiting,
        SchedulerRecoveryOutcome::Quiescent(
            insight_agent_platform::engine::SchedulerQuiescence::WaitingForTask { .. }
        )
    ));

    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = WorkerExecutorRegistry::new();
    registry
        .register(
            insight_agent_platform::engine::SchedulerTaskKind::Llm,
            "fixture.llm",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            std::sync::Arc::new(DurableFixtureExecutor {
                calls: calls.clone(),
            }),
        )
        .unwrap();
    let crash = FailOnceSchedulerCrash::new(SchedulerCrashPoint::AfterResultCommit);
    let interrupted = consume_scheduler_task_once(
        &repository,
        &registry,
        &TerminalSchedulerWorkerFailurePolicy,
        "worker-test",
        60,
        64,
        tokio_util::sync::CancellationToken::new(),
        &crash,
    )
    .await
    .unwrap_err();
    assert_eq!(
        interrupted.code(),
        "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED"
    );
    assert!(crash.fired());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    assert_eq!(
        consume_scheduler_task_once(
            &repository,
            &registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "worker-recovery",
            60,
            64,
            tokio_util::sync::CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::AcknowledgedRecoveredResult
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let terminal =
        drive_scheduler_until_quiescent(&repository, &linked, &fence, &NoSchedulerCrash, 64)
            .await
            .unwrap();
    assert_eq!(
        terminal,
        SchedulerRecoveryOutcome::Quiescent(
            insight_agent_platform::engine::SchedulerQuiescence::RunSucceeded
        )
    );
    let run = repository.load_run(&run_id).await.unwrap().unwrap();
    assert_eq!(run.lifecycle(), RunLifecycle::Succeeded);

    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER execution_event_projection_ledger_immutable")
        .execute(&control)
        .await
        .unwrap();
    let (dispatch_transition, original_dispatch_payload) = sqlx::query_as::<_, (String, String)>(
        "SELECT c.transition_key,e.safe_payload
             FROM scheduler_checkpoints c
             JOIN execution_events e ON e.run_id=c.run_id AND e.event_id=c.event_id
             WHERE c.run_id=? AND c.checkpoint_kind='planned_action'
               AND json_extract(c.fact_payload,'$.action.kind')='dispatch_task'",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE execution_events SET safe_payload=json_set(safe_payload,'$.lease_epoch',999)
         WHERE run_id=? AND transition_key=?",
    )
    .bind(run_id.as_str())
    .bind(&dispatch_transition)
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .load_scheduler_facts(&run_id)
            .await
            .unwrap_err()
            .code(),
        "ENGINE_REPOSITORY_DATA_INVALID",
        "planned-action replay must reject an event payload that no longer matches its intent"
    );
    sqlx::query("UPDATE execution_events SET safe_payload=? WHERE run_id=? AND transition_key=?")
        .bind(original_dispatch_payload)
        .bind(run_id.as_str())
        .bind(dispatch_transition)
        .execute(&control)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE execution_events
         SET safe_payload=json_set(
             safe_payload,'$.output.content_hash',
             'sha256:0000000000000000000000000000000000000000000000000000000000000000')
         WHERE run_id=? AND transition_key=(
             SELECT transition_key FROM scheduler_checkpoints
             WHERE run_id=? AND checkpoint_kind='task_completed')",
    )
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .load_scheduler_facts(&run_id)
            .await
            .unwrap_err()
            .code(),
        "ENGINE_REPOSITORY_DATA_INVALID",
        "task completion replay must reject an event summary that differs from frozen outputs"
    );
}

#[tokio::test]
async fn sqlite_duplicate_large_worker_values_share_one_verified_artifact_and_survive_ack_crash() {
    use insight_agent_platform::engine::{
        repository::{
            consume_scheduler_task_once_with_artifact_store, ArtifactDurableRepository,
            DurableRepository, FailOnceSchedulerCrash, NoSchedulerCrash, OrphanSweepCommand,
            PlanInstallOutcome, SchedulerCrashPoint, SchedulerDurableRepository,
            SchedulerWorkerPumpOutcome, SqliteDurableRepository,
            TerminalSchedulerWorkerFailurePolicy, VersionedPlan,
        },
        DeploymentRevisionId, LocalContentAddressedArtifactStore, TransitionKey,
        WorkerExecutorRegistry,
    };

    let (plan, descriptors) = branch_plan_with_duplicate_llm_output();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = VersionedPlan::from_verified_plan(
        "durable-artifact-fixture",
        "durable-artifact-agent",
        "Durable artifact fixture",
        DeploymentRevisionId::new("durable_artifact_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "programmatic"}),
        &plan,
        json!({"fixture": "descriptor-v1"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("durable-artifact.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    let run_id = RunId::new("run_durable_scheduler_artifact").unwrap();
    let fence =
        create_active_sqlite_scheduler_run(&repository, &control, &versioned, &run_id).await;
    drive_sqlite_to_task(&repository, &linked, &fence).await;

    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = WorkerExecutorRegistry::new();
    registry
        .register(
            insight_agent_platform::engine::SchedulerTaskKind::Llm,
            "fixture.llm",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            std::sync::Arc::new(DurableFixtureExecutor {
                calls: calls.clone(),
            }),
        )
        .unwrap();
    let put_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let object_root = directory.path().join("objects");
    let store = DelayedArtifactStore {
        inner: LocalContentAddressedArtifactStore::open(object_root.clone(), 1)
            .await
            .unwrap(),
        put_delay: std::time::Duration::from_millis(1_500),
        put_calls: put_calls.clone(),
    };
    let crash = FailOnceSchedulerCrash::new(SchedulerCrashPoint::AfterResultCommit);
    assert_eq!(
        consume_scheduler_task_once_with_artifact_store(
            &repository,
            &registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "artifact-worker",
            3,
            1,
            tokio_util::sync::CancellationToken::new(),
            &store,
            &crash,
        )
        .await
        .unwrap_err()
        .code(),
        "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED"
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(put_calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let artifact = sqlx::query_as::<_, (String, String, String)>(
        "SELECT artifact_id,content_hash,artifact_state FROM artifacts WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(artifact.2, "referenced");
    let scheduler_refs = sqlx::query_as::<_, (String, Option<String>, String)>(
        "SELECT storage_kind,artifact_id,port_id FROM scheduler_values WHERE run_id=? ORDER BY port_id",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(scheduler_refs.len(), 2);
    assert!(scheduler_refs
        .iter()
        .all(|scheduler_ref| scheduler_ref.0 == "artifact"));
    assert!(scheduler_refs
        .iter()
        .all(|scheduler_ref| scheduler_ref.1.as_deref() == Some(artifact.0.as_str())));
    let hash = artifact.1.strip_prefix("sha256:").unwrap();
    let object_path = object_root.join(&hash[..2]).join(hash);
    assert!(object_path.is_file());

    let sweep = repository
        .sweep_orphan_artifacts(
            TransitionKey::derive("durable.artifact.test", &["referenced-sweep"]).unwrap(),
            OrphanSweepCommand::new(1, "artifact-test-sweeper", 30, 10).unwrap(),
        )
        .await
        .unwrap();
    assert!(sweep.committed_result().unwrap().claims().is_empty());

    assert_eq!(
        consume_scheduler_task_once_with_artifact_store(
            &repository,
            &registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "artifact-recovery-worker",
            3,
            1,
            tokio_util::sync::CancellationToken::new(),
            &store,
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::AcknowledgedRecoveredResult
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let output_port = DataPortId::new(scheduler_refs[0].2.clone()).unwrap();
    assert!(repository
        .load_scheduler_value(&run_id, &output_port)
        .await
        .unwrap()
        .is_some());

    let forged_runtime = serde_json::to_string(
        &RuntimeValue::new(json!({"forged": "sqlite-runtime-value"})).unwrap(),
    )
    .unwrap();
    sqlx::query("UPDATE scheduler_values SET runtime_value=? WHERE run_id=? AND port_id=?")
        .bind(forged_runtime)
        .bind(run_id.as_str())
        .bind(output_port.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(repository
        .load_scheduler_value(&run_id, &output_port)
        .await
        .is_err());
    assert!(repository.load_scheduler_facts(&run_id).await.is_err());
}

#[tokio::test]
async fn sqlite_worker_heartbeat_retry_timeout_and_zombie_fences_use_frozen_policy() {
    use insight_agent_platform::engine::{
        repository::{
            consume_scheduler_task_once, drive_scheduler_until_quiescent, DurableRepository,
            FrozenSchedulerWorkerFailurePolicy, NoSchedulerCrash, PlanInstallOutcome,
            SchedulerDurableRepository, SchedulerRecoveryOutcome, SchedulerTaskHeartbeatOutcome,
            SchedulerWorkerPumpOutcome, SqliteDurableRepository, VersionedPlan,
        },
        DeploymentRevisionId, EffectIdempotency, TransitionOutcome, WorkerCancellation,
        WorkerEffectClass, WorkerEffectPolicy, WorkerExecutorRegistry,
    };

    let policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::Mutating,
        EffectIdempotency::Idempotent,
        2,
        0,
        0,
        1_100,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let (plan, descriptors) = branch_plan_with_worker_policy(false, Some(&policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = VersionedPlan::from_verified_plan(
        "durable-worker-gate",
        "durable-worker-agent",
        "Durable worker gate",
        DeploymentRevisionId::new("durable_worker_gate_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "programmatic"}),
        &plan,
        json!({"fixture": "descriptor-v1"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("durable-worker-gate.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();

    let retry_run = RunId::new("run_sqlite_worker_retry_gate").unwrap();
    let retry_fence =
        create_active_sqlite_scheduler_run(&repository, &control, &versioned, &retry_run).await;
    drive_sqlite_to_task(&repository, &linked, &retry_fence).await;
    let claim = repository
        .claim_scheduler_tasks_with_run_limit("worker-heartbeat", 60, 1, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let start_receipt = match repository
        .mark_scheduler_task_started(&claim)
        .await
        .unwrap()
    {
        TransitionOutcome::Committed { result } => result,
        other => panic!("initial task start did not commit: {other:?}"),
    };
    let renewed = match repository
        .heartbeat_scheduler_task(&claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => renewed,
        SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(_) => {
            panic!("fresh heartbeat reached the operation deadline")
        }
        SchedulerTaskHeartbeatOutcome::LeaseLost => panic!("fresh heartbeat lost its lease"),
    };
    assert!(renewed.task_projection_version() > claim.task_projection_version());
    assert_eq!(
        repository
            .mark_scheduler_task_started(&claim)
            .await
            .unwrap(),
        TransitionOutcome::ExactReplay {
            authoritative: start_receipt,
        },
        "heartbeat projection changes must not hide the immutable start receipt"
    );
    sqlx::query(
        "UPDATE task_outbox SET claim_expires_at=datetime('now','-1 second') WHERE run_id=?;",
    )
    .bind(retry_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE node_attempts SET lease_expires_at=datetime('now','-1 second') WHERE run_id=?;",
    )
    .bind(retry_run.as_str())
    .execute(&control)
    .await
    .unwrap();

    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = WorkerExecutorRegistry::new();
    registry
        .register(
            insight_agent_platform::engine::SchedulerTaskKind::Llm,
            "fixture.llm",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            std::sync::Arc::new(DurableFixtureExecutor {
                calls: calls.clone(),
            }),
        )
        .unwrap();
    assert!(matches!(
        consume_scheduler_task_once(
            &repository,
            &registry,
            &FrozenSchedulerWorkerFailurePolicy,
            "worker-finalize-expired",
            60,
            1,
            tokio_util::sync::CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::Committed {
            acknowledged: false,
            ..
        }
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let attempts = sqlx::query_as::<_, (i64, i64, String)>(
        "SELECT attempt_no,lease_epoch,fencing_token FROM node_attempts
         WHERE run_id=? ORDER BY attempt_no",
    )
    .bind(retry_run.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(attempts.len(), 2);
    assert!(attempts[1].0 > attempts[0].0);
    assert!(attempts[1].1 > attempts[0].1);
    assert_ne!(attempts[1].2, attempts[0].2);
    let retried = consume_scheduler_task_once(
        &repository,
        &registry,
        &FrozenSchedulerWorkerFailurePolicy,
        "worker-retry-attempt",
        60,
        1,
        tokio_util::sync::CancellationToken::new(),
        &NoSchedulerCrash,
    )
    .await
    .unwrap();
    assert!(
        matches!(
            retried,
            SchedulerWorkerPumpOutcome::Committed {
                acknowledged: true,
                ..
            }
        ),
        "unexpected retry outcome: {retried:?}"
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(matches!(
        drive_scheduler_until_quiescent(&repository, &linked, &retry_fence, &NoSchedulerCrash, 64,)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(
            insight_agent_platform::engine::SchedulerQuiescence::RunSucceeded
        )
    ));

    let timeout_run = RunId::new("run_sqlite_worker_timeout_gate").unwrap();
    let timeout_fence =
        create_active_sqlite_scheduler_run(&repository, &control, &versioned, &timeout_run).await;
    drive_sqlite_to_task(&repository, &linked, &timeout_fence).await;
    let mut slow_registry = WorkerExecutorRegistry::new();
    slow_registry
        .register(
            insight_agent_platform::engine::SchedulerTaskKind::Llm,
            "fixture.llm",
            VersionTag::new("1").unwrap(),
            VersionTag::new("worker-1").unwrap(),
            std::sync::Arc::new(SlowFixtureExecutor),
        )
        .unwrap();
    assert!(matches!(
        consume_scheduler_task_once(
            &repository,
            &slow_registry,
            &FrozenSchedulerWorkerFailurePolicy,
            "worker-timeout",
            60,
            1,
            tokio_util::sync::CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::Committed {
            acknowledged: false,
            ..
        }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=? AND attempt_no=1",
        )
        .bind(timeout_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timed_out"
    );

    let materialization_timeout_run =
        RunId::new("run_sqlite_worker_materialization_timeout_gate").unwrap();
    let materialization_timeout_fence = create_active_sqlite_scheduler_run(
        &repository,
        &control,
        &versioned,
        &materialization_timeout_run,
    )
    .await;
    drive_sqlite_to_task(&repository, &linked, &materialization_timeout_fence).await;
    let materialization_puts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let materialization_store = DelayedArtifactStore {
        inner: insight_agent_platform::engine::LocalContentAddressedArtifactStore::open(
            directory.path().join("materialization-timeout-objects"),
            1,
        )
        .await
        .unwrap(),
        put_delay: std::time::Duration::from_secs(2),
        put_calls: materialization_puts.clone(),
    };
    assert!(matches!(
        insight_agent_platform::engine::repository::consume_scheduler_task_once_with_artifact_store(
            &repository,
            &registry,
            &FrozenSchedulerWorkerFailurePolicy,
            "worker-materialization-timeout",
            3,
            1,
            tokio_util::sync::CancellationToken::new(),
            &materialization_store,
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::Committed {
            acknowledged: false,
            ..
        }
    ));
    assert_eq!(
        materialization_puts.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=? AND attempt_no=1",
        )
        .bind(materialization_timeout_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timed_out"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scheduler_values WHERE run_id=?")
            .bind(materialization_timeout_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
        "deadline during materialization must not publish a value reference"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT artifact_state FROM artifacts WHERE run_id=?")
            .bind(materialization_timeout_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        "staged",
        "the uncommitted artifact remains an explicitly collectable orphan"
    );
    assert_eq!(
        consume_scheduler_task_once(
            &repository,
            &registry,
            &FrozenSchedulerWorkerFailurePolicy,
            "worker-materialization-timeout-recovery",
            3,
            1,
            tokio_util::sync::CancellationToken::new(),
            &NoSchedulerCrash,
        )
        .await
        .unwrap(),
        SchedulerWorkerPumpOutcome::NoTask
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn sqlite_retry_backoff_uses_database_time_and_rejects_corrupted_lineage() {
    use insight_agent_platform::engine::{
        repository::{
            DurableRepository, PlanInstallOutcome, SchedulerDurableRepository,
            SchedulerTaskCommitOutcome, SchedulerTaskHeartbeatOutcome, SchedulerTaskOutcome,
            SchedulerWorkerFailurePolicy, SqliteDurableRepository, VersionedPlan,
        },
        DeploymentRevisionId, EffectIdempotency, TransitionOutcome, WorkerCancellation,
        WorkerEffectClass, WorkerEffectPolicy, WorkerFailure, WorkerFailureClass,
    };

    let policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::ReadOnly,
        EffectIdempotency::Idempotent,
        2,
        1_000,
        1_000,
        10_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let (plan, descriptors) = branch_plan_with_worker_policy(false, Some(&policy));
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = VersionedPlan::from_verified_plan(
        "durable-retry-backoff-gate",
        "durable-retry-backoff-agent",
        "Durable retry backoff gate",
        DeploymentRevisionId::new("durable_retry_backoff_gate_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "programmatic"}),
        &plan,
        json!({"fixture": "descriptor-v1"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("durable-retry-backoff-gate.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();

    let run_id = RunId::new("run_sqlite_retry_backoff_lineage_gate").unwrap();
    let fence =
        create_active_sqlite_scheduler_run(&repository, &control, &versioned, &run_id).await;
    drive_sqlite_to_task(&repository, &linked, &fence).await;
    let initial_claim = repository
        .claim_scheduler_tasks("worker-retry-lineage-initial", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&initial_claim)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let initial_claim = match repository
        .heartbeat_scheduler_task(&initial_claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => renewed,
        other => panic!("fresh retry claim did not renew: {other:?}"),
    };
    let failure = WorkerFailure::new(
        WorkerFailureClass::InfrastructureFailure,
        "TRANSIENT_RETRY_BACKOFF",
        true,
    )
    .unwrap();
    let retry = SchedulerTaskOutcome::Failed(
        insight_agent_platform::engine::repository::FrozenSchedulerWorkerFailurePolicy
            .freeze(&initial_claim, &failure)
            .unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&initial_claim, &retry)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));

    let (available_at, last_error_code, baseline_projection_version, baseline_publish_attempts) =
        sqlx::query_as::<_, (String, String, i64, i64)>(
            "SELECT available_at,last_error_code,projection_version,publish_attempts
             FROM task_outbox WHERE run_id=? AND task_state='pending'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(last_error_code, "TRANSIENT_RETRY_BACKOFF");
    assert!(repository
        .claim_scheduler_tasks("worker-retry-lineage-too-early", 60, 1)
        .await
        .unwrap()
        .is_empty());

    sqlx::query("UPDATE task_outbox SET available_at=datetime('now','-1 second') WHERE run_id=?")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(repository
        .claim_scheduler_tasks("worker-retry-lineage-forged-time", 60, 1)
        .await
        .is_err());
    sqlx::query("UPDATE task_outbox SET available_at=? WHERE run_id=?")
        .bind(&available_at)
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    sqlx::query("UPDATE task_outbox SET last_error_code='FORGED_RETRY_ERROR' WHERE run_id=?")
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(repository
        .claim_scheduler_tasks("worker-retry-lineage-forged-error", 60, 1)
        .await
        .is_err());
    sqlx::query("UPDATE task_outbox SET last_error_code=? WHERE run_id=?")
        .bind(&last_error_code)
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();

    let (retry_transition, retry_payload) = sqlx::query_as::<_, (String, String)>(
        "SELECT transition_key,fact_payload FROM scheduler_checkpoints
         WHERE run_id=? AND checkpoint_kind='task_retry_scheduled'",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let retry_payload_json: serde_json::Value = serde_json::from_str(&retry_payload).unwrap();
    assert_eq!(
        retry_payload_json["next_envelope"]["attempt_no"],
        json!(2),
        "the retry checkpoint must literally bind the complete next envelope",
    );
    sqlx::query(
        "UPDATE scheduler_checkpoints
         SET fact_payload=json_set(fact_payload,'$.next_envelope.attempt_no',999)
         WHERE run_id=? AND checkpoint_kind='task_retry_scheduled'",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(repository
        .claim_scheduler_tasks("worker-retry-lineage-forged-checkpoint", 60, 1)
        .await
        .is_err());
    sqlx::query(
        "UPDATE scheduler_checkpoints SET fact_payload=?
         WHERE run_id=? AND checkpoint_kind='task_retry_scheduled'",
    )
    .bind(&retry_payload)
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();

    let original_event_kind = sqlx::query_scalar::<_, String>(
        "SELECT kind FROM execution_events WHERE run_id=? AND transition_key=?",
    )
    .bind(run_id.as_str())
    .bind(&retry_transition)
    .fetch_one(&control)
    .await
    .unwrap();
    assert!(
        sqlx::query(
            "UPDATE execution_events SET kind='forged.retry.event'
         WHERE run_id=? AND transition_key=?",
        )
        .bind(run_id.as_str())
        .bind(&retry_transition)
        .execute(&control)
        .await
        .is_err(),
        "the immutable retry event authority must reject tampering before claim"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT kind FROM execution_events WHERE run_id=? AND transition_key=?",
        )
        .bind(run_id.as_str())
        .bind(&retry_transition)
        .fetch_one(&control)
        .await
        .unwrap(),
        original_event_kind,
    );

    assert_eq!(
        sqlx::query_as::<_, (String, i64, i64)>(
            "SELECT task_state,projection_version,publish_attempts
             FROM task_outbox WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        (
            "pending".to_owned(),
            baseline_projection_version,
            baseline_publish_attempts,
        ),
        "every forged claim transaction must roll back completely",
    );
    let retried = repository
        .claim_scheduler_tasks("worker-retry-lineage-authorized", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(retried.envelope().attempt_no().get(), 2);
    assert_eq!(retried.envelope().lease_epoch().get(), 2);
    assert_ne!(
        retried.envelope().fencing_token(),
        initial_claim.envelope().fencing_token()
    );
}
