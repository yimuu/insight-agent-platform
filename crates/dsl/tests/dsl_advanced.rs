use insight_dsl::{compile_source, CompileOptions, INVALID_CONTROL_FLOW};
use insight_engine::{
    plan::{CatchFailureKind, CollectSource, LoopFlavor, NodeKind, PlanIndex, PlanType, ScopeKind},
    DefinitionRevisionId,
};

fn options(source: &str) -> CompileOptions {
    CompileOptions::new(
        DefinitionRevisionId::new("dsl_advanced_revision").unwrap(),
        "advanced.yaml",
        source,
    )
}

#[test]
fn keyed_map_lowers_empty_typed_input_body_scope_and_typed_collect() {
    let source = r#"api_version: insight.agent/v1
kind: agent
types:
  Item:
    fields:
      id: string
      text: string
inputs:
  items: Item[]
output: string[]
workflow:
  steps:
    - id: rendered
      map:
        items: $items
        key: id
        as: item
        max_concurrency: 4
        steps:
          - id: render_item
            type: action
            call: fixture.render
            inputs: {item: $item}
            response: string
          - yield: $render_item
    - return: $rendered
"#;
    let plan = compile_source(source, options(source)).unwrap();
    plan.verify().unwrap();
    let map = plan
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::Map(value) => Some((node, value)),
            _ => None,
        })
        .unwrap();
    assert_eq!(map.1.max_concurrency, Some(4));
    assert!(plan.scopes().iter().any(|scope| {
        matches!(scope.kind(), ScopeKind::MapBody { map_node_id } if map_node_id == map.0.id())
    }));
    assert!(plan.nodes().iter().any(|node| matches!(
        node.kind(),
        NodeKind::Collect(value)
            if matches!(
                &value.source,
                CollectSource::DynamicMap { key_field: Some(key_field), .. } if key_field == "id"
            )
    )));
}

#[test]
fn map_without_a_business_key_uses_canonical_ordinal_identity() {
    let source = r#"api_version: insight.agent/v1
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
    let plan = compile_source(source, options(source)).unwrap();
    plan.verify().unwrap();
    assert!(plan.nodes().iter().any(|node| matches!(
        node.kind(),
        NodeKind::Collect(value)
            if matches!(
                &value.source,
                CollectSource::DynamicMap { key_field: None, .. }
            )
    )));
}

#[test]
fn static_duplicate_map_keys_are_rejected_before_execution() {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {}
output: string[]
workflow:
  steps:
    - id: rendered
      map:
        items:
          - {id: duplicate, text: a}
          - {id: duplicate, text: b}
        key: id
        steps:
          - id: render_item
            type: action
            call: fixture.render
            inputs: {item: $item}
            response: string
          - yield: $render_item
    - return: $rendered
"#;
    let error = compile_source(source, options(source)).unwrap_err();
    assert_eq!(error.code(), INVALID_CONTROL_FLOW);
    assert!(error.message().contains("duplicate key"));
}

#[test]
fn bounded_loop_has_dynamic_body_scope_typed_state_and_continue_contract() {
    let source = loop_source("loop", "yield");
    let plan = compile_source(&source, options(&source)).unwrap();
    plan.verify().unwrap();
    let loop_node = plan
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::Loop(value) => Some((node, value)),
            _ => None,
        })
        .unwrap();
    assert_eq!(loop_node.1.max_iterations, Some(3));
    assert_eq!(loop_node.1.flavor, LoopFlavor::Workflow);
    assert!(plan.scopes().iter().any(|scope| {
        matches!(scope.kind(), ScopeKind::LoopBody { loop_node_id } if loop_node_id == loop_node.0.id())
    }));
    assert!(plan.nodes().iter().any(|node| matches!(
        node.kind(),
        NodeKind::Collect(value)
            if matches!(&value.source, CollectSource::Loop { break_input: None, .. })
    )));
}

#[test]
fn loop_break_is_explicit_and_loop_control_is_lexically_scoped() {
    let source = loop_source("loop", "break");
    let plan = compile_source(&source, options(&source)).unwrap();
    plan.verify().unwrap();
    assert!(plan.nodes().iter().any(|node| matches!(
        node.kind(),
        NodeKind::Collect(value)
            if matches!(&value.source, CollectSource::Loop { break_input: Some(_), .. })
    )));

    let outside = r#"api_version: insight.agent/v1
kind: agent
inputs: {value: string}
output: string
workflow:
  steps:
    - continue: $value
"#;
    let error = compile_source(outside, options(outside)).unwrap_err();
    assert_eq!(error.code(), INVALID_CONTROL_FLOW);

    let unbounded = loop_source("loop", "yield").replace("        max_iterations: 3\n", "");
    let error = compile_source(&unbounded, options(&unbounded)).unwrap_err();
    assert_eq!(error.code(), INVALID_CONTROL_FLOW);
}

#[test]
fn subflow_call_freezes_revision_interface_and_typed_ports() {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: child
      type: call
      definition_revision: child_definition_sha256_abc
      interface_version: medical-v2
      timeout_ms: 42000
      input: {question: $question}
      response: string
    - return: $child
"#;
    let plan = compile_source(source, options(source)).unwrap();
    plan.verify().unwrap();
    let (call, descriptor) = plan
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::SubflowCall(value) => Some((node, value)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        descriptor.definition_revision_id.as_str(),
        "child_definition_sha256_abc"
    );
    assert_eq!(descriptor.interface_version.as_str(), "medical-v2");
    assert_eq!(descriptor.timeout_ms, 42_000);
    let invocation_scope = plan
        .scopes()
        .iter()
        .find(|scope| scope.id() == &descriptor.invocation_scope_id)
        .unwrap();
    assert_eq!(
        plan.scopes()
            .iter()
            .filter(|scope| matches!(
                scope.kind(),
                ScopeKind::Subflow { call_node_id } if call_node_id == call.id()
            ))
            .count(),
        1
    );
    assert_eq!(invocation_scope.parent(), Some(call.scope_id()));
    assert_eq!(invocation_scope.owner_node(), Some(call.id()));
    assert!(matches!(
        invocation_scope.kind(),
        ScopeKind::Subflow { call_node_id } if call_node_id == call.id()
    ));
    assert_eq!(
        descriptor
            .inputs
            .keys()
            .map(|name| name.as_str())
            .collect::<Vec<_>>(),
        vec!["question"]
    );
    let index = PlanIndex::new(&plan).unwrap();
    assert_eq!(
        index
            .data_inputs(call.id())
            .iter()
            .map(|id| index.data_port(id).unwrap().name().as_str())
            .collect::<Vec<_>>(),
        vec!["question"]
    );
    let invalid = source.replace("timeout_ms: 42000", "timeout_ms: 0");
    assert_eq!(
        compile_source(&invalid, options(&invalid))
            .unwrap_err()
            .code(),
        insight_dsl::INVALID_STEP
    );
}

#[test]
fn try_catches_only_safe_business_failure_and_finally_is_a_durable_child_path() {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: guarded
      try:
        - id: protected_call
          type: action
          call: fixture.may_fail_safely
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
    let plan = compile_source(source, options(source)).unwrap();
    plan.verify().unwrap();
    let boundary = plan
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::ErrorBoundary(value) => Some(value),
            _ => None,
        })
        .unwrap();
    assert_eq!(boundary.catch_kind, CatchFailureKind::SafeBusinessFailure);
    let finalizer_scope_id = boundary
        .finalizer_scope_id
        .as_ref()
        .expect("authored finally lowers to a durable finalizer scope");
    assert!(boundary.finalizer_output.is_some());
    assert!(boundary.finalizer_completed_input.is_some());
    let finalizer_scope = plan
        .scopes()
        .iter()
        .find(|scope| scope.id() == finalizer_scope_id)
        .unwrap();
    assert!(matches!(
        finalizer_scope.kind(),
        ScopeKind::ErrorFinalizer { .. }
    ));
    assert!(plan
        .nodes()
        .iter()
        .any(|node| node.id().as_str() == "audit" && node.scope_id() == finalizer_scope_id));

    let forbidden = source.replace("safe_business_failure:", "control_termination:");
    assert!(compile_source(&forbidden, options(&forbidden)).is_err());
}

#[test]
fn human_task_is_a_first_class_typed_durable_work_item() {
    let source = r#"api_version: insight.agent/v1
kind: agent
types:
  Approval:
    fields:
      decision: {type: string, enum: [approved, rejected]}
      comment: string
inputs: {}
output: Approval
workflow:
  steps:
    - id: review
      human_task:
        signal: medical_review
        request: Review the medical report
        response: Approval
    - return: $review
"#;
    let plan = compile_source(source, options(source)).unwrap();
    plan.verify().unwrap();
    assert!(plan.nodes().iter().any(|node| matches!(
        node.kind(),
        NodeKind::HumanTask(value)
            if value.completion_signal == "medical_review"
                && value.response_type == *plan.metadata().output_type()
    )));
}

#[test]
fn human_task_accepts_a_general_typed_form_response_and_request_context() {
    let source = r#"api_version: insight.agent/v1
kind: agent
inputs: {document_id: string}
output: string
workflow:
  steps:
    - id: review
      human_task:
        signal: document_review
        request: {document_id: $document_id, instruction: Summarize the document}
        response: string
        assignees: [alice]
    - return: $review
"#;
    let plan = compile_source(source, options(source)).unwrap();
    plan.verify().unwrap();
    let descriptor = plan
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::HumanTask(descriptor) => Some(descriptor),
            _ => None,
        })
        .unwrap();
    assert_eq!(descriptor.response_type, PlanType::String);
    assert!(matches!(descriptor.request_type, PlanType::Object { .. }));
    assert_eq!(descriptor.assignees, ["alice"]);
}

#[test]
fn agent_loop_is_losslessly_lowered_to_a_bounded_loop_plus_tool_leaf() {
    let source = loop_source("agent_loop", "yield");
    let plan = compile_source(&source, options(&source)).unwrap();
    plan.verify().unwrap();
    assert!(plan.nodes().iter().any(|node| matches!(
        node.kind(),
        NodeKind::Loop(value)
            if value.max_iterations == Some(3) && value.flavor == LoopFlavor::Agent
    )));
    assert!(plan
        .nodes()
        .iter()
        .any(|node| matches!(node.kind(), NodeKind::ToolTask(_))));
}

fn loop_source(kind: &str, terminator: &str) -> String {
    format!(
        r#"api_version: insight.agent/v1
kind: agent
inputs: {{seed: string}}
output: string
workflow:
  steps:
    - id: reasoning
      {kind}:
        initial: $seed
        as: state
        until: false
        max_iterations: 3
        steps:
          - id: next_state
            type: tool
            tool: fixture.next_state
            arguments: {{state: $state}}
            response: string
          - {terminator}: $next_state
    - return: $reasoning
"#
    )
}
