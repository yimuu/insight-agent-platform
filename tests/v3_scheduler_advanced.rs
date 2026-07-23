use std::collections::BTreeMap;

use insight_agent_platform::{
    dsl::v3::{compile_source, CompileOptions},
    engine::{
        plan::{
            DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
            DescriptorFieldContract, DescriptorValue, DescriptorValueSchema, LeafTaskKind,
            NodeKind, Plan, PlanBuilder, PlanIndex, PlanInputContract, PlanProperty, PlanType,
            Policy, PolicyId, PolicyKind, RetryPolicy, SubflowContractRegistry,
            SubflowInterfaceContract, TimeoutPolicy, VersionTag, WorkerContract,
            WorkerInputPortContract,
        },
        scheduler::*,
        ActivationId, ContentHash, DefinitionRevisionId, DeploymentRevisionId,
        ExecutionRevisionPin, NodeId, RunId, SignalId, TerminationReason, TimerId,
        WorkerFailureClass,
    },
};
use serde_json::{json, Value};

fn version(value: &str) -> VersionTag {
    VersionTag::new(value).unwrap()
}

fn compile(source: &str) -> Plan {
    compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new("scheduler_advanced_fixture").unwrap(),
            "scheduler-advanced.yaml",
            source,
        ),
    )
    .unwrap()
}

fn linked<'a>(
    plan: &'a Plan,
    subflows: &SubflowContractRegistry,
) -> insight_agent_platform::engine::plan::LinkedPlan<'a> {
    let index = PlanIndex::new(plan).unwrap();
    let mut registry = DescriptorContractRegistry::new();
    for node in plan.nodes() {
        let (kind, descriptor) = match node.kind() {
            NodeKind::LlmTask(value) => (LeafTaskKind::Llm, value),
            NodeKind::ActionTask(value) => (LeafTaskKind::Action, value),
            NodeKind::HttpTask(value) => (LeafTaskKind::Http, value),
            NodeKind::ToolTask(value) => (LeafTaskKind::Tool, value),
            _ => continue,
        };
        let public_fields = descriptor
            .public_configuration
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    DescriptorFieldContract::required(descriptor_schema(value)),
                )
            })
            .collect();
        let inputs = index
            .data_inputs(node.id())
            .iter()
            .map(|id| {
                let port = index.data_port(id).unwrap();
                (
                    port.name().clone(),
                    WorkerInputPortContract::new(port.value_type().clone(), port.required()),
                )
            })
            .collect();
        let outputs = index
            .data_outputs(node.id())
            .iter()
            .map(|id| {
                let port = index.data_port(id).unwrap();
                (port.name().clone(), port.value_type().clone())
            })
            .collect();
        registry
            .register(DescriptorContract::new(
                descriptor.implementation.clone(),
                descriptor.descriptor_version.clone(),
                DescriptorConfigurationContract::closed(public_fields, BTreeMap::new()),
                WorkerContract::new(kind, version("worker-1"), inputs, outputs),
            ))
            .unwrap();
    }
    insight_agent_platform::engine::plan::LinkedPlan::link(plan, &registry, subflows).unwrap()
}

fn descriptor_schema(value: &DescriptorValue) -> DescriptorValueSchema {
    match value {
        DescriptorValue::Null => DescriptorValueSchema::Null,
        DescriptorValue::Boolean(_) => DescriptorValueSchema::Boolean,
        DescriptorValue::Integer(_) => DescriptorValueSchema::Integer,
        DescriptorValue::Number(_) => DescriptorValueSchema::Number,
        DescriptorValue::String(_) => DescriptorValueSchema::String,
        DescriptorValue::Array(values) => DescriptorValueSchema::Array(Box::new(
            values
                .first()
                .map(descriptor_schema)
                .unwrap_or(DescriptorValueSchema::Any),
        )),
        DescriptorValue::Object(values) => DescriptorValueSchema::Object(
            values
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        DescriptorFieldContract::required(descriptor_schema(value)),
                    )
                })
                .collect(),
        ),
    }
}

fn apply(planned: &PlannedSchedulerAction, facts: &mut SchedulerFacts) {
    match planned.intent().action() {
        SchedulerAction::FailRunPlanning { failure } => {
            facts.record_terminal(RunTerminalFact::FailedPlanning(*failure))
        }
        SchedulerAction::AdmitActivation { activation_id, .. } => {
            facts.record_activation(activation_id.clone())
        }
        SchedulerAction::ConsumeToken { token_id, .. } => {
            facts.record_consumed_token(token_id.clone())
        }
        SchedulerAction::EmitToken { token_id, .. } => facts.record_emitted_token(token_id.clone()),
        SchedulerAction::DispatchTask { task_id, .. } => {
            facts.record_dispatched_task(task_id.clone())
        }
        SchedulerAction::CommitNativeOutput {
            occurrence,
            output: NativeOutput::Values { values },
            ..
        } => {
            for (port, value) in values {
                facts.record_occurrence_value(occurrence.clone(), port.clone(), value.clone());
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
        SchedulerAction::OpenFork { admission } => {
            facts.record_fork_group(admission.group().clone());
            for leg in admission.legs() {
                facts.record_fork_leg(leg.leg().clone());
            }
        }
        SchedulerAction::SettleForkLeg { leg, outcome } => {
            facts.settle_fork_leg(leg.key().clone(), outcome.clone())
        }
        SchedulerAction::CompleteFork { group_id, .. } => facts.complete_fork(group_id.clone()),
        SchedulerAction::RequestScopeCancellation {
            scope_instance_id, ..
        } => facts.request_scope_cancellation(scope_instance_id.clone()),
        SchedulerAction::OpenMap { map } => facts.record_map_instance(map.clone()),
        SchedulerAction::SpawnMapItem {
            item,
            item_port,
            item_value,
            ..
        } => facts.record_map_item(item.clone(), item_port.clone(), item_value.clone()),
        SchedulerAction::SettleMapItem { item, outcome } => {
            facts.settle_map_item(item.key().clone(), outcome.clone())
        }
        SchedulerAction::CompleteMap { map_activation_id } => {
            facts.complete_map(map_activation_id.clone())
        }
        SchedulerAction::OpenLoop { loop_instance } => {
            facts.record_loop_instance(loop_instance.clone())
        }
        SchedulerAction::StartLoopIteration {
            iteration,
            state_port,
            ..
        } => facts.record_loop_iteration(iteration.clone(), state_port.clone()),
        SchedulerAction::AdvanceLoop { iteration, state } => facts
            .advance_loop(iteration.key().loop_activation_id(), state.clone())
            .unwrap(),
        SchedulerAction::SettleLoopIteration { iteration, outcome } => {
            facts.settle_loop_iteration(iteration.key().clone(), outcome.clone())
        }
        SchedulerAction::CompleteLoop {
            loop_activation_id,
            state,
            ..
        } => facts
            .complete_loop(loop_activation_id, state.clone())
            .unwrap(),
        SchedulerAction::RegisterWait { registration } => facts.register_wait(registration.clone()),
        SchedulerAction::CommitOccurrenceValues {
            occurrence, values, ..
        } => {
            for (port, value) in values {
                facts.record_occurrence_value(occurrence.clone(), port.clone(), value.clone());
            }
        }
        SchedulerAction::StartSubflow { invocation, .. } => {
            facts.record_subflow(invocation.clone())
        }
        SchedulerAction::RequestChildRunCancellation { child_run_id } => {
            facts.request_child_cancellation(child_run_id.clone())
        }
        SchedulerAction::SettleSubflow {
            invocation,
            outcome,
        } => {
            if let SubflowOutcomeFact::Succeeded { outputs } = outcome {
                for (port, value) in outputs {
                    facts.record_occurrence_value(
                        invocation.occurrence().clone(),
                        port.clone(),
                        value.clone(),
                    );
                }
            }
            facts.settle_subflow(invocation.child_run_id().clone(), outcome.clone());
        }
        SchedulerAction::OpenErrorBoundary { boundary }
        | SchedulerAction::TransitionErrorBoundary { boundary } => {
            facts.record_boundary(boundary.clone())
        }
        SchedulerAction::CompleteRun { output, .. } => {
            facts.record_terminal(RunTerminalFact::Succeeded(output.clone()))
        }
        SchedulerAction::FailRun { error, .. } => {
            facts.record_terminal(RunTerminalFact::Failed(error.clone()))
        }
        SchedulerAction::FailRunInternal { failure, .. } => {
            facts.record_terminal(RunTerminalFact::FailedInternal(failure.clone()))
        }
        SchedulerAction::CancelRun { reason, .. } => facts.record_terminal(match reason {
            TerminationReason::Failure => RunTerminalFact::FailedInternal(
                TaskFailureFact::new(
                    WorkerFailureClass::InfrastructureFailure,
                    "RUN_TERMINATED_FAILURE",
                    None,
                )
                .unwrap(),
            ),
            TerminationReason::Cancelled => RunTerminalFact::Cancelled,
            TerminationReason::TimedOut => RunTerminalFact::TimedOut,
            TerminationReason::Interrupted => RunTerminalFact::Interrupted,
        }),
    }
    facts.commit_checkpoint(planned.intent().checkpoint_id().clone());
    facts.set_projection_version(facts.projection_version() + 1);
}

fn success_for(action: &SchedulerAction, value: Value) -> Option<TaskOutcomeFact> {
    let SchedulerAction::DispatchTask { outputs, .. } = action else {
        return None;
    };
    Some(TaskOutcomeFact::Succeeded {
        outputs: outputs
            .iter()
            .map(|output| {
                (
                    output.port_id().clone(),
                    RuntimeValue::new(value.clone()).unwrap(),
                )
            })
            .collect(),
    })
}

fn drive_with(
    planner: &SchedulerPlanner<'_, '_>,
    facts: &mut SchedulerFacts,
    mut task_outcome: impl FnMut(&SchedulerAction) -> Option<TaskOutcomeFact>,
) -> SchedulerQuiescence {
    for _ in 0..500 {
        match planner.plan(facts).unwrap() {
            SchedulerDecision::Action(planned) => {
                let dispatched = match planned.intent().action() {
                    SchedulerAction::DispatchTask { task_id, .. } => {
                        task_outcome(planned.intent().action())
                            .map(|outcome| (task_id.clone(), outcome))
                    }
                    _ => None,
                };
                apply(&planned, facts);
                if let Some((task, outcome)) = dispatched {
                    facts.record_task_outcome(task, outcome);
                    facts.set_projection_version(facts.projection_version() + 1);
                }
            }
            SchedulerDecision::Quiescent(value) => return value,
        }
    }
    panic!("advanced scheduler did not quiesce")
}

#[test]
fn verified_leaf_retry_and_timeout_policies_are_frozen_into_dispatch() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: answer
      type: action
      call: fixture.policy_answer
      inputs: {question: $question}
      response: string
    - return: $answer
"#;
    let compiled = compile(source);
    let task = compiled
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::ActionTask(_)))
        .unwrap()
        .id()
        .clone();
    let retry_id = PolicyId::new("answer_retry").unwrap();
    let timeout_id = PolicyId::new("answer_timeout").unwrap();
    let mut source_map = compiled.source_map().clone();
    let task_span = compiled.source_map().node(&task).unwrap().clone();
    source_map.insert_policy(retry_id.clone(), task_span.clone());
    source_map.insert_policy(timeout_id.clone(), task_span);
    let mut builder = PlanBuilder::from_verified_plan(&compiled).unwrap();
    builder
        .set_source_map(source_map)
        .add_policy(Policy::new(
            retry_id,
            task.clone(),
            PolicyKind::Retry(RetryPolicy {
                max_attempts: 4,
                initial_backoff_ms: 25,
                max_backoff_ms: 250,
            }),
        ))
        .add_policy(Policy::new(
            timeout_id,
            task,
            PolicyKind::Timeout(TimeoutPolicy { timeout_ms: 750 }),
        ));
    let plan = builder.build().unwrap();
    let subflows = SubflowContractRegistry::new();
    let linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_leaf_policy_freeze").unwrap(),
        0,
        RuntimeValue::new(json!({"question": "q"})).unwrap(),
    );

    for _ in 0..8 {
        let SchedulerDecision::Action(planned) = planner.plan(&facts).unwrap() else {
            panic!("leaf policy fixture quiesced before dispatch")
        };
        if let SchedulerAction::DispatchTask { effect_policy, .. } = planned.intent().action() {
            assert_eq!(effect_policy.max_attempts(), 4);
            assert_eq!(effect_policy.initial_backoff_ms(), 25);
            assert_eq!(effect_policy.max_backoff_ms(), 250);
            assert_eq!(effect_policy.timeout_ms(), 750);
            return;
        }
        apply(&planned, &mut facts);
    }
    panic!("leaf policy fixture did not dispatch")
}

#[test]
fn fork_join_collect_preserves_declaration_order_and_all_settled_safe_errors() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: any
workflow:
  steps:
    - id: analyses
      settle: all_settled
      parallel:
        slow:
          - id: slow_task
            type: action
            call: fixture.slow
            response: string
          - yield: $slow_task
        risky:
          - id: risky_task
            type: action
            call: fixture.risky
            response: string
          - yield: $risky_task
    - return: $analyses
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked_plan = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked_plan);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_parallel_advanced").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    let SchedulerDecision::Action(admit_fork) = planner.plan(&facts).unwrap() else {
        panic!("Fork entry must first admit its owner activation")
    };
    assert!(matches!(
        admit_fork.intent().action(),
        SchedulerAction::AdmitActivation { .. }
    ));
    apply(&admit_fork, &mut facts);
    let SchedulerDecision::Action(open_fork) = planner.plan(&facts).unwrap() else {
        panic!("Fork admission must be a closed action")
    };
    let SchedulerAction::OpenFork { admission } = open_fork.intent().action() else {
        panic!("Fork must atomically admit its complete ordered group")
    };
    assert_eq!(admission.legs().len(), 2);
    assert!(admission
        .group()
        .members()
        .iter()
        .zip(admission.legs())
        .all(|(member, leg)| member == leg.leg().key().leg_id()));
    assert_eq!(
        planner.plan(&facts).unwrap(),
        SchedulerDecision::Action(open_fork.clone())
    );
    apply(&open_fork, &mut facts);
    let safe_error = RuntimeValue::new(json!({
        "kind": "safe_error",
        "code": "RISK",
        "message": "rejected"
    }))
    .unwrap();
    let quiescence = drive_with(&planner, &mut facts, |action| {
        let SchedulerAction::DispatchTask { implementation, .. } = action else {
            return None;
        };
        if implementation == "fixture.risky" {
            Some(TaskOutcomeFact::Failed {
                failure: TaskFailureFact::new(
                    WorkerFailureClass::SafeBusinessFailure,
                    "RISK",
                    Some(safe_error.clone()),
                )
                .unwrap(),
            })
        } else {
            success_for(action, json!("slow-result"))
        }
    });
    assert_eq!(quiescence, SchedulerQuiescence::RunSucceeded);
    let RunTerminalFact::Succeeded(value) = facts.terminal().unwrap() else {
        panic!("expected success")
    };
    assert_eq!(
        value.value(),
        &json!({
            "slow": {"kind": "ok", "value": "slow-result"},
            "risky": {"kind": "error", "error": safe_error.value()}
        })
    );
}

#[test]
fn all_success_infrastructure_failure_cancels_and_drains_before_internal_failure() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: any
workflow:
  steps:
    - id: work
      settle: all_success
      parallel:
        failing:
          - id: fail_task
            type: action
            call: fixture.infrastructure_failure
            response: string
          - yield: $fail_task
        sibling:
          - id: sibling_task
            type: action
            call: fixture.sibling
            response: string
          - yield: $sibling_task
    - return: $work
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked_plan = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked_plan);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_parallel_drain").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    let mut saw_cancel = false;
    let mut saw_drain = false;
    for _ in 0..300 {
        match planner.plan(&facts).unwrap() {
            SchedulerDecision::Action(planned) => {
                let outcome = match planned.intent().action() {
                    SchedulerAction::DispatchTask {
                        task_id,
                        implementation,
                        ..
                    } if implementation == "fixture.infrastructure_failure" => Some((
                        task_id.clone(),
                        TaskOutcomeFact::Failed {
                            failure: TaskFailureFact::new(
                                WorkerFailureClass::InfrastructureFailure,
                                "PROVIDER_UNAVAILABLE",
                                None,
                            )
                            .unwrap(),
                        },
                    )),
                    SchedulerAction::RequestScopeCancellation { .. } => {
                        saw_cancel = true;
                        None
                    }
                    _ => None,
                };
                apply(&planned, &mut facts);
                if let Some((task, outcome)) = outcome {
                    facts.record_task_outcome(task, outcome);
                }
            }
            SchedulerDecision::Quiescent(SchedulerQuiescence::WaitingForDrain { .. }) => {
                saw_drain = true;
                let cancelled = TaskFailureFact::new(
                    WorkerFailureClass::ControlTermination,
                    "SIBLING_CANCELLED",
                    None,
                )
                .unwrap();
                let unsettled = facts
                    .fork_legs()
                    .keys()
                    .filter(|key| !facts.fork_settlements().contains_key(*key))
                    .cloned()
                    .collect::<Vec<_>>();
                for key in unsettled {
                    facts.settle_fork_leg(
                        key,
                        StructuralOutcomeFact::Failed {
                            failure: cancelled.clone(),
                        },
                    );
                }
            }
            SchedulerDecision::Quiescent(SchedulerQuiescence::RunFailed) => break,
            decision => panic!("unexpected all_success decision: {decision:?}"),
        }
    }
    assert!(saw_cancel && saw_drain);
    let RunTerminalFact::FailedInternal(failure) = facts.terminal().unwrap() else {
        panic!("fatal parallel failure must remain internal")
    };
    assert_eq!(failure.class(), WorkerFailureClass::InfrastructureFailure);
}

#[test]
fn nested_parallel_groups_complete_without_recursive_scheduler_state() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: any
workflow:
  steps:
    - id: outer
      settle: all_success
      parallel:
        nested:
          - id: inner
            settle: all_success
            parallel:
              left:
                - id: left_task
                  type: action
                  call: fixture.left
                  response: string
                - yield: $left_task
              right:
                - id: right_task
                  type: action
                  call: fixture.right
                  response: string
                - yield: $right_task
          - yield: $inner
        plain:
          - id: plain_task
            type: action
            call: fixture.plain
            response: string
          - yield: $plain_task
    - return: $outer
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked_plan = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked_plan);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_nested_parallel").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    assert_eq!(
        drive_with(&planner, &mut facts, |action| success_for(
            action,
            json!("ok")
        )),
        SchedulerQuiescence::RunSucceeded
    );
    assert_eq!(facts.fork_groups().len(), 2);
}

#[test]
fn keyed_map_persists_input_order_supports_empty_and_rejects_runtime_duplicates() {
    let source = r#"api_version: insight.agent/v3
kind: agent
types:
  Item:
    fields: {id: string, text: string}
inputs:
  items: Item[]
output: string[]
workflow:
  steps:
    - id: rendered
      map:
        items: $items
        key: id
        max_concurrency: 2
        steps:
          - id: render
            type: action
            call: fixture.render
            inputs: {item: $item}
            response: string
          - yield: $render
    - return: $rendered
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked_plan = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked_plan);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_map_ordered").unwrap(),
        0,
        RuntimeValue::new(json!({
            "items": [
                {"id": "b", "text": "second"},
                {"id": "a", "text": "first"}
            ]
        }))
        .unwrap(),
    );
    let done = drive_with(&planner, &mut facts, |action| {
        let SchedulerAction::DispatchTask { inputs, .. } = action else {
            return None;
        };
        let text = inputs.first().unwrap().value().value()["text"]
            .as_str()
            .unwrap();
        success_for(action, json!(text))
    });
    assert_eq!(done, SchedulerQuiescence::RunSucceeded);
    let RunTerminalFact::Succeeded(value) = facts.terminal().unwrap() else {
        panic!("expected success")
    };
    assert_eq!(value.value(), &json!(["second", "first"]));

    let mut empty = SchedulerFacts::new(
        RunId::new("run_map_empty").unwrap(),
        0,
        RuntimeValue::new(json!({"items": []})).unwrap(),
    );
    let index = PlanIndex::new(&plan).unwrap();
    let mut saw_terminalizing_collect_output = false;
    for _ in 0..100 {
        match planner.plan(&empty).unwrap() {
            SchedulerDecision::Action(planned) => {
                match planned.intent().action() {
                    SchedulerAction::CommitNativeOutput { node_id, .. }
                        if matches!(index.node(node_id).unwrap().kind(), NodeKind::Collect(_)) =>
                    {
                        saw_terminalizing_collect_output = true;
                    }
                    SchedulerAction::CommitOccurrenceValues { node_id, .. }
                        if matches!(index.node(node_id).unwrap().kind(), NodeKind::Collect(_)) =>
                    {
                        panic!("Collect output must terminalize its native activation")
                    }
                    _ => {}
                }
                apply(&planned, &mut empty);
            }
            SchedulerDecision::Quiescent(SchedulerQuiescence::RunSucceeded) => break,
            decision => panic!("unexpected empty Map decision: {decision:?}"),
        }
    }
    assert!(saw_terminalizing_collect_output);
    let RunTerminalFact::Succeeded(value) = empty.terminal().unwrap() else {
        panic!("expected empty success")
    };
    assert_eq!(value.value(), &json!([]));

    let duplicate = SchedulerFacts::new(
        RunId::new("run_map_duplicate").unwrap(),
        0,
        RuntimeValue::new(json!({
            "items": [
                {"id": "same", "text": "one"},
                {"id": "same", "text": "two"}
            ]
        }))
        .unwrap(),
    );
    let mut duplicate = duplicate;
    loop {
        match planner.plan(&duplicate) {
            Ok(SchedulerDecision::Action(action)) => apply(&action, &mut duplicate),
            Err(error) => {
                assert_eq!(error.code(), SCHEDULER_DYNAMIC_KEY_DUPLICATE);
                break;
            }
            decision => panic!("unexpected duplicate-key decision: {decision:?}"),
        }
    }
}

#[test]
fn loop_creates_new_iteration_occurrences_and_fails_closed_at_its_budget() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {seed: string}
output: string
workflow:
  steps:
    - id: reasoning
      loop:
        initial: $seed
        as: state
        until: false
        max_iterations: 2
        steps:
          - id: next_state
            type: tool
            tool: fixture.next
            arguments: {state: $state}
            response: string
          - continue: $next_state
    - return: $reasoning
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_loop_budget").unwrap(),
        0,
        RuntimeValue::new(json!({"seed": "s0"})).unwrap(),
    );
    let done = drive_with(&planner, &mut facts, |action| {
        let SchedulerAction::DispatchTask { inputs, .. } = action else {
            return None;
        };
        let state = inputs.first().unwrap().value().value().as_str().unwrap();
        success_for(action, json!(format!("{state}+")))
    });
    assert_eq!(done, SchedulerQuiescence::RunFailed);
    assert_eq!(facts.loop_iterations().len(), 2);
    let mut occurrences = facts
        .loop_iterations()
        .values()
        .map(|value| value.occurrence().clone())
        .collect::<Vec<_>>();
    occurrences.dedup();
    assert_eq!(occurrences.len(), 2);
    let RunTerminalFact::FailedInternal(failure) = facts.terminal().unwrap() else {
        panic!("expected internal loop budget failure")
    };
    assert_eq!(failure.code(), "LOOP_MAX_ITERATIONS");
}

#[test]
fn agent_loop_preserves_distinct_durable_turn_identity() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {seed: string}
output: string
workflow:
  steps:
    - id: reasoning
      agent_loop:
        initial: $seed
        as: state
        until: false
        max_iterations: 2
        steps:
          - id: next_state
            type: tool
            tool: fixture.next
            arguments: {state: $state}
            response: string
          - continue: $next_state
    - return: $reasoning
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_agent_loop_turns").unwrap(),
        0,
        RuntimeValue::new(json!({"seed": "s0"})).unwrap(),
    );
    let done = drive_with(&planner, &mut facts, |action| {
        let SchedulerAction::DispatchTask { inputs, .. } = action else {
            return None;
        };
        let state = inputs.first().unwrap().value().value().as_str().unwrap();
        success_for(action, json!(format!("{state}+")))
    });
    assert_eq!(done, SchedulerQuiescence::RunFailed);
    assert_eq!(facts.loop_iterations().len(), 2);
    assert!(facts.loop_iterations().values().all(|turn| {
        turn.flavor() == insight_agent_platform::engine::plan::LoopFlavor::Agent
            && turn
                .occurrence()
                .segments()
                .iter()
                .any(|segment| segment.starts_with("agent_loop_turn:"))
    }));
    assert_eq!(
        facts
            .loop_iterations()
            .values()
            .map(LoopIterationFact::scope_instance_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
    let turn = facts.loop_iterations().values().next().unwrap();
    let mut wrong_flavor = serde_json::to_value(turn).unwrap();
    wrong_flavor["flavor"] = json!("workflow");
    assert!(serde_json::from_value::<LoopIterationFact>(wrong_flavor).is_err());
    let mut unknown = serde_json::to_value(turn).unwrap();
    unknown["future_field"] = json!(true);
    assert!(serde_json::from_value::<LoopIterationFact>(unknown).is_err());
}

#[test]
fn durable_wait_resolution_is_first_winner_and_timer_does_not_skip_signal_contract() {
    let source = r#"api_version: insight.agent/v3
kind: agent
types:
  Approval:
    fields:
      decision:
        type: string
        enum: [approved, rejected]
inputs: {}
output: string
workflow:
  steps:
    - id: approval
      human_task:
        signal: review
        request: Review this item
        response: Approval
    - return: $approval.decision
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_wait_first_winner").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    let wait = drive_with(&planner, &mut facts, |_| None);
    let SchedulerQuiescence::WaitingForWait { wait_id, .. } = wait else {
        panic!("expected durable wait")
    };
    let signal = facts
        .waits()
        .get(&wait_id)
        .unwrap()
        .signal_id()
        .cloned()
        .unwrap();
    assert!(facts
        .resolve_wait_first_winner(
            wait_id.clone(),
            WaitResolutionFact::new(
                WaitSubjectFact::Signal {
                    signal_id: signal.clone(),
                },
                Some(RuntimeValue::new(json!({"decision": "approved"})).unwrap()),
            )
            .unwrap(),
        )
        .unwrap());
    assert!(!facts
        .resolve_wait_first_winner(
            wait_id,
            WaitResolutionFact::new(
                WaitSubjectFact::Signal { signal_id: signal },
                Some(RuntimeValue::new(json!({"decision": "rejected"})).unwrap()),
            )
            .unwrap(),
        )
        .unwrap());
    assert_eq!(
        drive_with(&planner, &mut facts, |_| None),
        SchedulerQuiescence::RunSucceeded
    );
}

#[test]
fn signal_and_timeout_share_one_durable_first_winner_slot() {
    let wait_id = SchedulerWaitId::parse(format!("wait_{}", "a".repeat(64))).unwrap();
    let signal_id = SignalId::new("signal_review").unwrap();
    let timer_id = TimerId::new("timer_review_deadline").unwrap();
    let registration = WaitRegistrationFact::new(
        wait_id.clone(),
        ActivationId::new("activation_review").unwrap(),
        NodeId::new("review").unwrap(),
        LogicalOccurrence::entry(),
        Some("review".to_owned()),
        Some(signal_id.clone()),
        Some(timer_id.clone()),
        Some(100),
        Some(PlanType::String),
    )
    .unwrap();
    let mut facts = SchedulerFacts::new(
        RunId::new("run_signal_timeout_race").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    facts.register_wait(registration);
    assert!(facts
        .resolve_wait_first_winner(
            wait_id.clone(),
            WaitResolutionFact::new(
                WaitSubjectFact::Signal { signal_id },
                Some(RuntimeValue::new(json!("approved")).unwrap()),
            )
            .unwrap(),
        )
        .unwrap());
    assert!(!facts
        .resolve_wait_first_winner(
            wait_id,
            WaitResolutionFact::new(WaitSubjectFact::Timer { timer_id }, None).unwrap(),
        )
        .unwrap());
}

#[test]
fn error_boundary_catches_only_safe_business_failure_and_always_runs_finalizer() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - id: protected_call
          type: action
          call: fixture.may_fail
          inputs: {question: $question}
          response: string
      catch:
        safe_business_failure:
          as: failure
          steps:
            - id: recover
              type: action
              call: fixture.recover
              inputs: {failure: $failure}
              response: string
      finally:
        - id: audit
          type: action
          call: fixture.audit
          inputs: {question: $question}
          response: string
    - return: $question
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked);
    let safe = RuntimeValue::new(json!({
        "kind": "safe_error",
        "code": "INVALID",
        "message": "invalid"
    }))
    .unwrap();
    let mut facts = SchedulerFacts::new(
        RunId::new("run_boundary_safe").unwrap(),
        0,
        RuntimeValue::new(json!({"question": "q"})).unwrap(),
    );
    let mut dispatched = Vec::new();
    let done = drive_with(&planner, &mut facts, |action| {
        let SchedulerAction::DispatchTask { implementation, .. } = action else {
            return None;
        };
        dispatched.push(implementation.clone());
        if implementation == "fixture.may_fail" {
            Some(TaskOutcomeFact::Failed {
                failure: TaskFailureFact::new(
                    WorkerFailureClass::SafeBusinessFailure,
                    "INVALID",
                    Some(safe.clone()),
                )
                .unwrap(),
            })
        } else {
            success_for(action, json!(implementation))
        }
    });
    assert_eq!(done, SchedulerQuiescence::RunSucceeded);
    assert_eq!(
        dispatched,
        vec![
            "fixture.may_fail".to_owned(),
            "fixture.recover".to_owned(),
            "fixture.audit".to_owned()
        ]
    );

    let mut infrastructure = SchedulerFacts::new(
        RunId::new("run_boundary_infrastructure").unwrap(),
        0,
        RuntimeValue::new(json!({"question": "q"})).unwrap(),
    );
    let mut infrastructure_dispatches = Vec::new();
    let done = drive_with(&planner, &mut infrastructure, |action| {
        let SchedulerAction::DispatchTask { implementation, .. } = action else {
            return None;
        };
        infrastructure_dispatches.push(implementation.clone());
        Some(TaskOutcomeFact::Failed {
            failure: TaskFailureFact::new(
                WorkerFailureClass::InfrastructureFailure,
                "PROVIDER_DOWN",
                None,
            )
            .unwrap(),
        })
    });
    assert_eq!(done, SchedulerQuiescence::RunFailed);
    assert_eq!(
        infrastructure_dispatches,
        vec!["fixture.may_fail".to_owned(), "fixture.audit".to_owned()]
    );
}

#[test]
fn finalizer_failure_deterministically_overrides_the_pending_failure() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - id: protected_call
          type: action
          call: fixture.may_fail
          inputs: {question: $question}
          response: string
      catch:
        safe_business_failure:
          as: failure
          steps:
            - id: recover
              type: action
              call: fixture.recover
              inputs: {failure: $failure}
              response: string
      finally:
        - id: audit
          type: action
          call: fixture.audit
          inputs: {question: $question}
          response: string
    - return: $question
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_finalizer_failure_precedence").unwrap(),
        0,
        RuntimeValue::new(json!({"question": "q"})).unwrap(),
    );
    let mut dispatches = Vec::new();
    let done = drive_with(&planner, &mut facts, |action| {
        let SchedulerAction::DispatchTask { implementation, .. } = action else {
            return None;
        };
        dispatches.push(implementation.clone());
        let code = if implementation == "fixture.audit" {
            "AUDIT_STORE_DOWN"
        } else {
            "PROVIDER_DOWN"
        };
        Some(TaskOutcomeFact::Failed {
            failure: TaskFailureFact::new(WorkerFailureClass::InfrastructureFailure, code, None)
                .unwrap(),
        })
    });
    assert_eq!(done, SchedulerQuiescence::RunFailed);
    assert_eq!(
        dispatches,
        vec!["fixture.may_fail".to_owned(), "fixture.audit".to_owned()]
    );
    assert!(matches!(
        facts.terminal(),
        Some(RunTerminalFact::FailedInternal(failure))
            if failure.code() == "AUDIT_STORE_DOWN"
    ));
}

#[test]
fn authored_raise_is_caught_but_authored_return_skips_catch_and_runs_finally() {
    let raise_source = r#"api_version: insight.agent/v3
kind: agent
errors:
  rejected:
    category: workflow
    code: REJECTED
    public_message: rejected
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - raise: rejected
      catch:
        safe_business_failure:
          as: failure
          steps:
            - id: recover
              type: action
              call: fixture.recover
              response: string
      finally:
        - id: audit
          type: action
          call: fixture.audit
          response: string
    - return: caught
"#;
    let plan = compile(raise_source);
    let subflows = SubflowContractRegistry::new();
    let raise_linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&raise_linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_authored_raise_catch").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    let mut dispatches = Vec::new();
    let mut caught_code = None;
    let done = loop {
        match planner.plan(&facts).unwrap() {
            SchedulerDecision::Action(planned) => {
                if let SchedulerAction::TransitionErrorBoundary { boundary } =
                    planned.intent().action()
                {
                    if boundary.phase() == ErrorBoundaryPhase::Handler {
                        caught_code = boundary
                            .safe_error()
                            .and_then(|error| error.value()["code"].as_str())
                            .map(str::to_owned);
                    }
                }
                let dispatched = match planned.intent().action() {
                    SchedulerAction::DispatchTask {
                        task_id,
                        implementation,
                        ..
                    } => {
                        dispatches.push(implementation.clone());
                        success_for(planned.intent().action(), json!(implementation))
                            .map(|outcome| (task_id.clone(), outcome))
                    }
                    _ => None,
                };
                apply(&planned, &mut facts);
                if let Some((task, outcome)) = dispatched {
                    facts.record_task_outcome(task, outcome);
                    facts.set_projection_version(facts.projection_version() + 1);
                }
            }
            SchedulerDecision::Quiescent(value) => break value,
        }
    };
    assert_eq!(done, SchedulerQuiescence::RunSucceeded);
    assert_eq!(caught_code.as_deref(), Some("REJECTED"));
    assert_eq!(
        facts.terminal(),
        Some(&RunTerminalFact::Succeeded(
            RuntimeValue::new(json!("caught")).unwrap()
        ))
    );
    assert_eq!(dispatches, vec!["fixture.recover", "fixture.audit"]);

    let return_source = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - return: protected-result
      catch:
        safe_business_failure:
          as: failure
          steps:
            - return: catch-must-not-run
      finally:
        - id: audit
          type: action
          call: fixture.audit
          response: string
"#;
    let plan = compile(return_source);
    let return_linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&return_linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_authored_try_return").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    let mut dispatches = Vec::new();
    let done = drive_with(&planner, &mut facts, |action| {
        let SchedulerAction::DispatchTask { implementation, .. } = action else {
            return None;
        };
        dispatches.push(implementation.clone());
        success_for(action, json!(implementation))
    });
    assert_eq!(done, SchedulerQuiescence::RunSucceeded);
    assert_eq!(
        facts.terminal(),
        Some(&RunTerminalFact::Succeeded(
            RuntimeValue::new(json!("protected-result")).unwrap()
        ))
    );
    assert_eq!(dispatches, vec!["fixture.audit"]);
}

#[test]
fn dynamic_raise_rejects_invalid_safe_error_before_native_completion_is_persisted() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs:
  code: string
output: string
workflow:
  steps:
    - raise:
        kind: safe_error
        code: $code
        message: rejected
"#;
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_invalid_dynamic_raise").unwrap(),
        0,
        RuntimeValue::new(json!({"code": "not_a_public_code"})).unwrap(),
    );

    for _ in 0..10 {
        match planner.plan(&facts) {
            Ok(SchedulerDecision::Action(planned)) => {
                assert!(!matches!(
                    planned.intent().action(),
                    SchedulerAction::CommitNativeOutput { .. } | SchedulerAction::FailRun { .. }
                ));
                apply(&planned, &mut facts);
            }
            Err(error) => {
                assert_eq!(error.code(), SCHEDULER_VALUE_TYPE_MISMATCH);
                return;
            }
            Ok(SchedulerDecision::Quiescent(value)) => {
                panic!("invalid dynamic Raise unexpectedly quiesced: {value:?}")
            }
        }
    }
    panic!("invalid dynamic Raise was not rejected")
}

#[test]
fn catch_return_runs_finally_and_finalizer_raise_overrides_pending_return() {
    let catch_return = r#"api_version: insight.agent/v3
kind: agent
errors:
  rejected:
    category: workflow
    code: REJECTED
    public_message: rejected
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - raise: rejected
      catch:
        safe_business_failure:
          as: failure
          steps:
            - return: recovered-result
      finally:
        - id: audit
          type: action
          call: fixture.audit
          response: string
"#;
    let plan = compile(catch_return);
    let subflows = SubflowContractRegistry::new();
    let catch_linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&catch_linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_authored_catch_return").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    let mut dispatches = Vec::new();
    let done = drive_with(&planner, &mut facts, |action| {
        let SchedulerAction::DispatchTask { implementation, .. } = action else {
            return None;
        };
        dispatches.push(implementation.clone());
        success_for(action, json!(implementation))
    });
    assert_eq!(done, SchedulerQuiescence::RunSucceeded);
    assert_eq!(
        facts.terminal(),
        Some(&RunTerminalFact::Succeeded(
            RuntimeValue::new(json!("recovered-result")).unwrap()
        ))
    );
    assert_eq!(dispatches, vec!["fixture.audit"]);

    let finalizer_raise = r#"api_version: insight.agent/v3
kind: agent
errors:
  finalizer_rejected:
    category: workflow
    code: FINALIZER_REJECTED
    public_message: finalizer rejected
inputs: {}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - return: pending-result
      catch:
        safe_business_failure:
          as: failure
          steps:
            - return: catch-must-not-run
      finally:
        - raise: finalizer_rejected
"#;
    let plan = compile(finalizer_raise);
    let finalizer_linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&finalizer_linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_authored_finalizer_raise").unwrap(),
        0,
        RuntimeValue::new(json!({})).unwrap(),
    );
    let done = drive_with(&planner, &mut facts, |_| None);
    assert_eq!(done, SchedulerQuiescence::RunFailed);
    assert!(matches!(
        facts.terminal(),
        Some(RunTerminalFact::Failed(error))
            if error.value()["code"] == json!("FINALIZER_REJECTED")
    ));
}

#[test]
fn control_termination_runs_finalizer_and_replays_the_first_winner_terminal() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - id: protected_call
          type: action
          call: fixture.long_running
          inputs: {question: $question}
          response: string
      catch:
        safe_business_failure:
          as: failure
          steps:
            - id: recover
              type: action
              call: fixture.recover
              inputs: {failure: $failure}
              response: string
      finally:
        - id: audit
          type: action
          call: fixture.audit
          inputs: {question: $question}
          response: string
    - return: $question
"#;
    for (suffix, reason, expected) in [
        (
            "cancel",
            TerminationReason::Cancelled,
            SchedulerQuiescence::RunCancelled,
        ),
        (
            "timeout",
            TerminationReason::TimedOut,
            SchedulerQuiescence::RunFailed,
        ),
        (
            "interrupt",
            TerminationReason::Interrupted,
            SchedulerQuiescence::RunFailed,
        ),
    ] {
        let plan = compile(source);
        let subflows = SubflowContractRegistry::new();
        let linked = linked(&plan, &subflows);
        let planner = SchedulerPlanner::new(&linked);
        let mut facts = SchedulerFacts::new(
            RunId::new(format!("run_finalizer_{suffix}")).unwrap(),
            0,
            RuntimeValue::new(json!({"question": "q"})).unwrap(),
        );

        loop {
            let SchedulerDecision::Action(action) = planner.plan(&facts).unwrap() else {
                panic!("protected task must be dispatched before termination")
            };
            let protected_dispatched = matches!(
                action.intent().action(),
                SchedulerAction::DispatchTask { implementation, .. }
                    if implementation == "fixture.long_running"
            );
            apply(&action, &mut facts);
            if protected_dispatched {
                break;
            }
        }
        facts.request_run_termination(reason);
        facts.set_projection_version(facts.projection_version() + 1);

        let mut post_intent_dispatches = Vec::new();
        let done = drive_with(&planner, &mut facts, |action| {
            let SchedulerAction::DispatchTask { implementation, .. } = action else {
                return None;
            };
            post_intent_dispatches.push(implementation.clone());
            success_for(action, json!("audited"))
        });
        assert_eq!(done, expected);
        assert_eq!(post_intent_dispatches, vec!["fixture.audit".to_owned()]);
        assert_eq!(
            planner.plan(&facts).unwrap(),
            SchedulerDecision::Quiescent(expected)
        );
        match reason {
            TerminationReason::Cancelled => {
                assert_eq!(facts.terminal(), Some(&RunTerminalFact::Cancelled));
            }
            TerminationReason::TimedOut => {
                assert_eq!(facts.terminal(), Some(&RunTerminalFact::TimedOut));
            }
            TerminationReason::Interrupted => {
                assert_eq!(facts.terminal(), Some(&RunTerminalFact::Interrupted));
            }
            TerminationReason::Failure => unreachable!(),
        }
    }
}

#[test]
fn subflow_has_a_deterministic_child_identity_and_child_cancel_isolated_from_siblings() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: child
      type: call
      definition_revision: child_revision
      interface_version: child-v1
      input: {question: $question}
      response: string
    - return: $child
"#;
    let plan = compile(source);
    let index = PlanIndex::new(&plan).unwrap();
    let call = plan
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::SubflowCall(_)))
        .unwrap();
    let NodeKind::SubflowCall(descriptor) = call.kind() else {
        unreachable!()
    };
    let input_contract = PlanInputContract::new(PlanType::Object {
        properties: descriptor
            .inputs
            .iter()
            .map(|(name, id)| {
                let port = index.data_port(id).unwrap();
                (
                    name.as_str().to_owned(),
                    PlanProperty::new(port.value_type().clone(), port.required()).unwrap(),
                )
            })
            .collect(),
        additional_properties: None,
    });
    let output_port = index.data_outputs(call.id())[0].clone();
    let output = index.data_port(&output_port).unwrap();
    let mut subflows = SubflowContractRegistry::new();
    subflows
        .register(SubflowInterfaceContract::new(
            ExecutionRevisionPin::new(
                DefinitionRevisionId::new("child_revision").unwrap(),
                DeploymentRevisionId::new("child_deployment").unwrap(),
                ContentHash::from_bytes(b"child-plan"),
                ContentHash::from_bytes(b"child-binding"),
            ),
            version("child-v1"),
            input_contract,
            BTreeMap::from([(output.name().clone(), output.value_type().clone())]),
            plan.metadata().error_type().clone(),
        ))
        .unwrap();
    let linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked);
    let mut facts = SchedulerFacts::new(
        RunId::new("run_subflow_parent").unwrap(),
        0,
        RuntimeValue::new(json!({"question": "q"})).unwrap(),
    );
    let wait = drive_with(&planner, &mut facts, |_| None);
    let SchedulerQuiescence::WaitingForChildRun { child_run_id, .. } = wait else {
        panic!("expected child wait")
    };
    assert_eq!(facts.subflows().len(), 1);
    facts.observe_subflow_outcome(
        child_run_id,
        SubflowOutcomeFact::Succeeded {
            outputs: BTreeMap::from([(
                output_port,
                RuntimeValue::new(json!("child answer")).unwrap(),
            )]),
        },
    );
    assert_eq!(
        drive_with(&planner, &mut facts, |_| None),
        SchedulerQuiescence::RunSucceeded
    );
}

#[derive(Clone)]
struct DurableAdvancedExecutor;

#[async_trait::async_trait]
impl insight_agent_platform::engine::LeafTaskExecutor for DurableAdvancedExecutor {
    async fn execute(
        &self,
        _context: &insight_agent_platform::engine::WorkerExecutionContext,
        request: &insight_agent_platform::engine::TaskExecutionRequest,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<
        insight_agent_platform::engine::TaskExecutionResult,
        insight_agent_platform::engine::WorkerFailure,
    > {
        if request.implementation() == "fixture.may_fail" {
            return Err(
                insight_agent_platform::engine::WorkerFailure::safe_business(
                    "FIXTURE_REJECTED",
                    false,
                    RuntimeValue::new(json!({
                        "kind": "safe_error",
                        "code": "FIXTURE_REJECTED",
                        "message": "fixture rejected"
                    }))
                    .unwrap(),
                )
                .unwrap(),
            );
        }
        let value = if request.implementation() == "fixture.render" {
            request
                .inputs()
                .first()
                .and_then(|input| input.value().value().get("text"))
                .cloned()
                .expect("render input text")
        } else {
            json!(request.implementation())
        };
        Ok(insight_agent_platform::engine::TaskExecutionResult::new(
            request
                .outputs()
                .iter()
                .map(|output| {
                    (
                        output.port_id().clone(),
                        RuntimeValue::new(value.clone()).unwrap(),
                    )
                })
                .collect(),
            insight_agent_platform::engine::EffectEvidence::Committed,
        ))
    }
}

#[tokio::test]
async fn sqlite_durable_map_and_fork_recover_from_only_committed_projection() {
    use insight_agent_platform::engine::{
        plan::LinkedPlan,
        repository::{
            consume_scheduler_task_once, drive_scheduler_once, CreateRunCommand, DurableRepository,
            FencedSchedulerRunCommand, NoSchedulerCrash, PlanInstallOutcome, SchedulerDriveOutcome,
            SchedulerDurableRepository, SqliteDurableRepository,
            TerminalSchedulerWorkerFailurePolicy, VersionedPlan,
        },
        LeafTaskExecutor, TransitionKey, TransitionOutcome, WorkerExecutorRegistry,
    };
    use std::sync::Arc;

    let source = r#"api_version: insight.agent/v3
kind: agent
types:
  Item:
    fields: {id: string, text: string}
inputs:
  items: Item[]
output: any
workflow:
  steps:
    - id: routed
      if: size(items) > 0
      then:
        - id: primary_path
          type: action
          call: fixture.primary
          response: string
        - yield: $primary_path
      else:
        - id: fallback_path
          type: action
          call: fixture.fallback
          response: string
        - yield: $fallback_path
    - id: guarded
      try:
        - id: protected_call
          type: action
          call: fixture.may_fail
          response: string
      catch:
        safe_business_failure:
          as: failure
          steps:
            - id: recovered
              type: action
              call: fixture.recover
              inputs: {failure: $failure}
              response: string
      finally:
        - id: audited
          type: action
          call: fixture.audit
          response: string
    - id: rendered
      map:
        items: $items
        key: id
        max_concurrency: 2
        steps:
          - id: render
            type: action
            call: fixture.render
            inputs: {item: $item}
            response: string
          - yield: $render
    - id: analyses
      settle: all_success
      parallel:
        left:
          - id: left_task
            type: action
            call: fixture.left
            response: string
          - yield: $left_task
        right:
          - id: right_task
            type: action
            call: fixture.right
            response: string
          - yield: $right_task
    - return: $analyses
"#;
    let plan = compile(source);
    let index = PlanIndex::new(&plan).unwrap();
    let mut descriptors = DescriptorContractRegistry::new();
    let mut workers = Vec::new();
    for node in plan.nodes() {
        let (kind, descriptor) = match node.kind() {
            NodeKind::LlmTask(value) => (LeafTaskKind::Llm, value),
            NodeKind::ActionTask(value) => (LeafTaskKind::Action, value),
            NodeKind::HttpTask(value) => (LeafTaskKind::Http, value),
            NodeKind::ToolTask(value) => (LeafTaskKind::Tool, value),
            _ => continue,
        };
        let inputs = index
            .data_inputs(node.id())
            .iter()
            .map(|id| {
                let port = index.data_port(id).unwrap();
                (
                    port.name().clone(),
                    WorkerInputPortContract::new(port.value_type().clone(), port.required()),
                )
            })
            .collect();
        let outputs = index
            .data_outputs(node.id())
            .iter()
            .map(|id| {
                let port = index.data_port(id).unwrap();
                (port.name().clone(), port.value_type().clone())
            })
            .collect();
        let public_fields = descriptor
            .public_configuration
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    DescriptorFieldContract::required(descriptor_schema(value)),
                )
            })
            .collect();
        descriptors
            .register(DescriptorContract::new(
                descriptor.implementation.clone(),
                descriptor.descriptor_version.clone(),
                DescriptorConfigurationContract::closed(public_fields, BTreeMap::new()),
                WorkerContract::new(kind, version("worker-1"), inputs, outputs),
            ))
            .unwrap();
        workers.push((
            match kind {
                LeafTaskKind::Llm => SchedulerTaskKind::Llm,
                LeafTaskKind::Action => SchedulerTaskKind::Action,
                LeafTaskKind::Retrieval => SchedulerTaskKind::Retrieval,
                LeafTaskKind::Http => SchedulerTaskKind::Http,
                LeafTaskKind::Tool => SchedulerTaskKind::Tool,
            },
            descriptor.implementation.clone(),
            descriptor.descriptor_version.clone(),
        ));
    }
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let versioned = VersionedPlan::from_verified_plan(
        "durable-advanced",
        "durable-advanced-agent",
        "Durable advanced",
        DeploymentRevisionId::new("durable_advanced_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "dsl-v3"}),
        &plan,
        json!({"fixture": "advanced"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("advanced.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_durable_advanced_map_fork").unwrap();
    assert!(matches!(
        repository
            .create_run(
                TransitionKey::derive("durable.advanced", &["create"]).unwrap(),
                CreateRunCommand::new(
                    run_id.clone(),
                    &versioned,
                    json!({
                        "items": [
                            {"id": "b", "text": "second"},
                            {"id": "a", "text": "first"}
                        ]
                    }),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let control = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET lifecycle='active',scheduler_lease_epoch=1,
            scheduler_lease_owner='advanced-test',scheduler_fencing_token='advanced-fence',
            scheduler_lease_expires_at=datetime('now','+1 hour'),
            scheduler_heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    let fence =
        FencedSchedulerRunCommand::new(run_id.clone(), "advanced-test", 1, "advanced-fence")
            .unwrap();
    let mut registry = WorkerExecutorRegistry::new();
    let executor: Arc<dyn LeafTaskExecutor> = Arc::new(DurableAdvancedExecutor);
    for (kind, implementation, descriptor_version) in workers {
        registry
            .register(
                kind,
                implementation,
                descriptor_version,
                version("worker-1"),
                executor.clone(),
            )
            .unwrap();
    }
    let mut terminal = false;
    for step in 0..512 {
        match drive_scheduler_once(&repository, &linked, &fence, &NoSchedulerCrash).await {
            Ok(SchedulerDriveOutcome::Applied(_)) => {}
            Ok(SchedulerDriveOutcome::Quiescent(SchedulerQuiescence::RunSucceeded)) => {
                terminal = true;
                break;
            }
            Ok(SchedulerDriveOutcome::Quiescent(
                SchedulerQuiescence::WaitingForTask { .. }
                | SchedulerQuiescence::WaitingForChildren { .. },
            )) => {
                consume_scheduler_task_once(
                    &repository,
                    &registry,
                    &TerminalSchedulerWorkerFailurePolicy,
                    "advanced-worker",
                    60,
                    64,
                    tokio_util::sync::CancellationToken::new(),
                    &NoSchedulerCrash,
                )
                .await
                .unwrap();
            }
            Ok(other) => panic!("unexpected durable advanced quiescence: {other:?}"),
            Err(error) => {
                let facts = repository.load_scheduler_facts(&run_id).await.unwrap();
                let decision = SchedulerPlanner::new(&linked).plan(&facts);
                panic!(
                    "durable advanced step {step} failed: {error:?}; next decision={decision:?}"
                );
            }
        }
    }
    assert!(terminal, "durable advanced workflow did not terminate");
    let lifecycle: String =
        sqlx::query_scalar("SELECT lifecycle FROM workflow_runs WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap();
    assert_eq!(lifecycle, "succeeded");
    let root_scope = sqlx::query(
        "SELECT lifecycle,admission_state,admitted_children,settled_children
         FROM scope_instances WHERE run_id=? AND is_root=1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(
        sqlx::Row::get::<String, _>(&root_scope, "lifecycle"),
        "settled"
    );
    assert_eq!(
        sqlx::Row::get::<String, _>(&root_scope, "admission_state"),
        "closed"
    );
    assert_eq!(
        sqlx::Row::get::<i64, _>(&root_scope, "admitted_children"),
        4,
        "the root owns exactly two Map items and two Fork legs"
    );
    assert_eq!(
        sqlx::Row::get::<i64, _>(&root_scope, "settled_children"),
        4,
        "each admitted dynamic child must settle exactly once"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_checkpoints
             WHERE run_id=? AND checkpoint_kind='planned_action'
               AND fact_payload LIKE '%\"kind\":\"select_branch_and_admit\"%'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
        "Branch selection, token correlation, successor admission and consumption share one checkpoint"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM control_tokens
             WHERE run_id=? AND branch_activation_id IS NOT NULL
               AND selected_branch_port_id IS NOT NULL
               AND json_array_length(provenance_frames)=1
               AND json_extract(provenance_frames,'$[0].kind')='branch'
               AND token_state='consumed'
               AND consumed_by_activation_id IS NOT NULL
               AND consumed_by_transition_key=emitted_by_transition_key",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
        "the selected Branch token retains its non-null correlation frame after atomic consumption"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_checkpoints
             WHERE run_id=? AND checkpoint_kind='planned_action'
               AND fact_payload LIKE '%\"kind\":\"open_fork\"%'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
        "the complete ordered Fork admission is one checkpoint"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT expected_legs,admitted_legs,settled_legs FROM fork_groups WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        (2, 2, 2)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scope_instances
             WHERE run_id=? AND scope_kind='parallel_leg'
               AND admitted_children=0 AND settled_children=0
               AND lifecycle='settled' AND admission_state='closed'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2,
        "Fork leg scopes start 0/0 and settle without fabricating child counts"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM fork_groups g
             JOIN node_activations a ON a.run_id=g.run_id
                AND a.activation_id=g.fork_activation_id
             WHERE g.run_id=? AND a.lifecycle='succeeded'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
        "closing a Fork settles its controller activation as well as its Join"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations
             WHERE run_id=? AND node_id IN ('primary_path','protected_call','recovered','audited')
               AND scope_instance_id=(
                 SELECT scope_instance_id FROM scope_instances WHERE run_id=? AND is_root=1
               )",
        )
        .bind(run_id.as_str())
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        4,
        "if arms and try protected/handler blocks inherit their runtime-owning root scope"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scope_instances WHERE run_id=? AND is_root=0",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        4,
        "only the two Map items and two Fork legs materialize runtime scopes"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM node_activations WHERE run_id=?
             AND lifecycle NOT IN ('succeeded','failed','cancelled','timed_out')",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
        "a terminal Run cannot retain nonterminal control activations"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scope_instances WHERE run_id=? AND scope_kind='map_item'
               AND lifecycle='settled'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM fork_legs WHERE run_id=? AND leg_state='settled'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM join_arrivals WHERE run_id=? AND settlement_class='succeeded'
               AND value_payload_id IS NOT NULL AND value_hash IS NOT NULL",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2
    );
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scheduler_occurrence_values WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap()
            >= 4
    );
    control.close().await;
}

#[test]
fn scheduler_action_wire_rederives_the_intent_hash_and_closes_its_schema() {
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
    let plan = compile(source);
    let subflows = SubflowContractRegistry::new();
    let linked = linked(&plan, &subflows);
    let planner = SchedulerPlanner::new(&linked);
    let facts = SchedulerFacts::new(
        RunId::new("run_closed_scheduler_action").unwrap(),
        0,
        RuntimeValue::new(json!({"route": "image"})).unwrap(),
    );
    let SchedulerDecision::Action(planned) = planner.plan(&facts).unwrap() else {
        panic!("expected one inert scheduler action")
    };

    let encoded = serde_json::to_value(planned.as_ref()).unwrap();
    assert_eq!(
        serde_json::from_value::<PlannedSchedulerAction>(encoded.clone()).unwrap(),
        *planned
    );

    let mut tampered = encoded.clone();
    tampered["intent"]["run_id"] = json!("run_tampered");
    assert!(serde_json::from_value::<PlannedSchedulerAction>(tampered).is_err());

    let mut wrong_version = encoded.clone();
    wrong_version["intent"]["schema_version"] = json!(999);
    assert!(serde_json::from_value::<PlannedSchedulerAction>(wrong_version).is_err());

    let mut open_wire = encoded;
    open_wire["unknown_future_field"] = json!(true);
    assert!(serde_json::from_value::<PlannedSchedulerAction>(open_wire).is_err());
}
