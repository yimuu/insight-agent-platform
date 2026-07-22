#[allow(unused_imports)]
pub(crate) use insight_durable::recovery_repository::*;
pub use insight_durable::{
    BeginMigrationCommand, ContinueAsNewCommand, FinalizeMigrationCommand, ForkRunCommand,
    MigrationIntentReceipt, MigrationMappingCompatibility, RecoveryDurableRepository,
    RecoveryEventReceipt, RecoveryRevisionSpec, RecoveryRunReceipt, RedriveRunCommand,
};

#[cfg(test)]
pub(crate) mod dynamic_scope_test_contract {
    use insight_dsl::v3::{compile_source, CompileOptions};
    use insight_engine::internal::{
        scope_instance_for_occurrence, scope_instance_for_runtime_scope,
    };
    use insight_engine::{
        plan::{NodeKind, Plan, PlanIndex, ScopeKind},
        scheduler::LogicalOccurrence,
        DefinitionRevisionId, RunId, ScopeInstanceId,
    };

    fn compile(source: &str, revision: &str) -> Plan {
        compile_source(
            source,
            CompileOptions::new(
                DefinitionRevisionId::new(revision).unwrap(),
                format!("{revision}.yaml"),
                source,
            ),
        )
        .unwrap()
    }

    fn assert_node_scope_is_stable_and_target_run_specific(
        plan: &Plan,
        node: &insight_engine::plan::Node,
        occurrence: &LogicalOccurrence,
    ) {
        let index = PlanIndex::new(plan).unwrap();
        let source_run = RunId::new("run_dynamic_scope_source").unwrap();
        let target_run = RunId::new("run_dynamic_scope_target").unwrap();
        let source = scope_instance_for_occurrence(&index, &source_run, node, occurrence).unwrap();
        let target = scope_instance_for_occurrence(&index, &target_run, node, occurrence).unwrap();
        let replay = scope_instance_for_occurrence(&index, &target_run, node, occurrence).unwrap();
        assert_ne!(source, ScopeInstanceId::root());
        assert_ne!(source, target, "scope identity must be target-Run specific");
        assert_eq!(target, replay, "stable occurrence must replay exactly");
    }

    pub(crate) fn assert_map_item_scope_rederivation() {
        let source = r#"api_version: insight.agent/v3
kind: agent
inputs:
  items: string[]
output: string[]
workflow:
  steps:
    - id: rendered
      map:
        items: $items
        as: item
        steps:
          - id: render_item
            type: action
            call: fixture.render
            inputs: {item: $item}
            response: string
          - yield: $render_item
    - return: $rendered
"#;
        let plan = compile(source, "dynamic_scope_map_revision");
        let index = PlanIndex::new(&plan).unwrap();
        let node = plan
            .nodes()
            .iter()
            .find(|node| {
                matches!(
                    index.scope(node.scope_id()).map(|scope| scope.kind()),
                    Some(ScopeKind::MapBody { .. })
                ) && matches!(node.kind(), NodeKind::ActionTask(_))
            })
            .unwrap();
        let owner = LogicalOccurrence::entry()
            .child("map_item:stable-business-key")
            .unwrap();
        let occurrence = owner.child("edge:map-body-entry").unwrap();
        assert_node_scope_is_stable_and_target_run_specific(&plan, node, &occurrence);
        let deeper = occurrence.child("node:render-item").unwrap();
        assert_eq!(
            scope_instance_for_occurrence(
                &index,
                &RunId::new("run_dynamic_scope_target").unwrap(),
                node,
                &occurrence,
            )
            .unwrap(),
            scope_instance_for_occurrence(
                &index,
                &RunId::new("run_dynamic_scope_target").unwrap(),
                node,
                &deeper,
            )
            .unwrap(),
            "Map item scope is keyed by the stable item occurrence, not traversal suffixes"
        );
    }

    pub(crate) fn assert_loop_iteration_scope_rederivation() {
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
            type: action
            call: fixture.next
            inputs: {state: $state}
            response: string
          - continue: $next_state
    - return: $reasoning
"#;
        let plan = compile(source, "dynamic_scope_loop_revision");
        let index = PlanIndex::new(&plan).unwrap();
        let node = plan
            .nodes()
            .iter()
            .find(|node| {
                matches!(
                    index.scope(node.scope_id()).map(|scope| scope.kind()),
                    Some(ScopeKind::LoopBody { .. })
                ) && matches!(node.kind(), NodeKind::ActionTask(_))
            })
            .unwrap();
        let occurrence = LogicalOccurrence::entry()
            .child("loop_iteration:7")
            .unwrap()
            .child("edge:loop-body-entry")
            .unwrap();
        assert_node_scope_is_stable_and_target_run_specific(&plan, node, &occurrence);
    }

    pub(crate) fn assert_subflow_invocation_scope_rederivation() {
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
        let plan = compile(source, "dynamic_scope_subflow_revision");
        let index = PlanIndex::new(&plan).unwrap();
        let descriptor = plan
            .nodes()
            .iter()
            .find_map(|node| match node.kind() {
                NodeKind::SubflowCall(descriptor) => Some(descriptor),
                _ => None,
            })
            .unwrap();
        let occurrence = LogicalOccurrence::entry()
            .child("edge:subflow-call")
            .unwrap();
        let source_run = RunId::new("run_subflow_scope_source").unwrap();
        let target_run = RunId::new("run_subflow_scope_target").unwrap();
        let source = scope_instance_for_runtime_scope(
            &index,
            &source_run,
            &descriptor.invocation_scope_id,
            &occurrence,
        )
        .unwrap();
        let target = scope_instance_for_runtime_scope(
            &index,
            &target_run,
            &descriptor.invocation_scope_id,
            &occurrence,
        )
        .unwrap();
        let replay = scope_instance_for_runtime_scope(
            &index,
            &target_run,
            &descriptor.invocation_scope_id,
            &occurrence,
        )
        .unwrap();
        assert_ne!(source, ScopeInstanceId::root());
        assert_ne!(source, target);
        assert_eq!(target, replay);
    }
}
