use std::collections::BTreeMap;

use insight_agent_platform::{
    dsl::v3::{
        compile_source,
        graph::{
            ActivationTrace, GraphAuthorDocument, GraphDocumentId, GraphSemanticEdit,
            GraphSemanticEditBatch, NodeView, TraceActivationState, TraceOverlay, ViewDocument,
            Viewport, GRAPH_DOCUMENT_INVALID, GRAPH_EDIT_CONFLICT, GRAPH_EDIT_KIND_MISMATCH,
            GRAPH_IRREDUCIBLE, GRAPH_PLAN_INVALID,
        },
        parse, validate, CompileOptions,
    },
    engine::{
        plan::{
            AuthorFormat, BranchCase, BranchCaseId, BranchDescriptor, BudgetPolicy, ControlEdge,
            ControlEdgeId, ControlPort, ControlPortId, DataBinding, DataBindingId, DataPort,
            DataPortId, ExpressionLanguage, MergeDescriptor, Node, NodeKind, PlanBuilder,
            PlanDiagnosticTarget, PlanInputContract, PlanMetadata, PlanProperty, PlanType, Policy,
            PolicyId, PolicyKind, PortDirection, PortName, PureExpression, RetryPolicy,
            ReturnDescriptor, ScopeId, ScopeMetadata, SourceMap, TimeoutPolicy, ValueSource,
            VersionTag, CEL_EXPRESSION_ENGINE_VERSION, PLAN_MERGE_INVALID, PLAN_POLICY_INVALID,
        },
        ActivationId, DefinitionRevisionId, NodeId, RunId,
    },
};

fn options(source_id: &str, source: &str) -> CompileOptions {
    CompileOptions::new(
        DefinitionRevisionId::new(format!("graph_revision_{source_id}")).unwrap(),
        source_id,
        source,
    )
}

fn structured_graph(source: &str) -> GraphAuthorDocument {
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("medical_flow_graph").unwrap(),
        source,
        options("graph_fixture.yaml", source),
    )
    .unwrap()
}

fn graph_for_revision(source: &str, revision: &str) -> GraphAuthorDocument {
    GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("semantic_edit_graph").unwrap(),
        source,
        CompileOptions::new(
            DefinitionRevisionId::new(revision).unwrap(),
            "semantic-edit.yaml",
            source,
        ),
    )
    .unwrap()
}

fn topology_edits(from: &GraphAuthorDocument, to: &GraphAuthorDocument) -> Vec<GraphSemanticEdit> {
    let mut edits = Vec::new();
    for value in from.nodes() {
        if !to.nodes().iter().any(|other| other.id() == value.id()) {
            edits.push(GraphSemanticEdit::DeleteNode {
                node_id: value.id().clone(),
            });
        }
    }
    for value in from.ports().control() {
        if !to
            .ports()
            .control()
            .iter()
            .any(|other| other.id() == value.id())
        {
            edits.push(GraphSemanticEdit::DeleteControlPort {
                port_id: value.id().clone(),
            });
        }
    }
    for value in from.ports().data() {
        if !to
            .ports()
            .data()
            .iter()
            .any(|other| other.id() == value.id())
        {
            edits.push(GraphSemanticEdit::DeleteDataPort {
                port_id: value.id().clone(),
            });
        }
    }
    for value in from.edges().control() {
        if !to
            .edges()
            .control()
            .iter()
            .any(|other| other.id() == value.id())
        {
            edits.push(GraphSemanticEdit::DeleteControlEdge {
                edge_id: value.id().clone(),
            });
        }
    }
    for value in from.bindings().data() {
        if !to
            .bindings()
            .data()
            .iter()
            .any(|other| other.id() == value.id())
        {
            edits.push(GraphSemanticEdit::DeleteDataBinding {
                binding_id: value.id().clone(),
            });
        }
    }
    for value in from.bindings().phi() {
        if !to
            .bindings()
            .phi()
            .iter()
            .any(|other| other.id() == value.id())
        {
            edits.push(GraphSemanticEdit::DeletePhiBinding {
                binding_id: value.id().clone(),
            });
        }
    }
    for value in from.scopes() {
        if !to.scopes().iter().any(|other| other.id() == value.id()) {
            edits.push(GraphSemanticEdit::DeleteScope {
                scope_id: value.id().clone(),
            });
        }
    }
    for value in from.policies() {
        if !to.policies().iter().any(|other| other.id() == value.id()) {
            edits.push(GraphSemanticEdit::DeletePolicy {
                policy_id: value.id().clone(),
            });
        }
    }
    edits.extend(
        to.nodes()
            .iter()
            .cloned()
            .map(|node| GraphSemanticEdit::UpsertNode { node }),
    );
    edits.extend(
        to.ports()
            .control()
            .iter()
            .cloned()
            .map(|port| GraphSemanticEdit::UpsertControlPort { port }),
    );
    edits.extend(
        to.ports()
            .data()
            .iter()
            .cloned()
            .map(|port| GraphSemanticEdit::UpsertDataPort { port }),
    );
    edits.extend(
        to.edges()
            .control()
            .iter()
            .cloned()
            .map(|edge| GraphSemanticEdit::UpsertControlEdge { edge }),
    );
    edits.extend(
        to.bindings()
            .data()
            .iter()
            .cloned()
            .map(|binding| GraphSemanticEdit::UpsertDataBinding { binding }),
    );
    edits.extend(
        to.bindings()
            .phi()
            .iter()
            .cloned()
            .map(|binding| GraphSemanticEdit::UpsertPhiBinding { binding }),
    );
    edits.extend(
        to.scopes()
            .iter()
            .cloned()
            .map(|scope| GraphSemanticEdit::UpsertScope { scope }),
    );
    edits.extend(
        to.policies()
            .iter()
            .cloned()
            .map(|policy| GraphSemanticEdit::UpsertPolicy { policy }),
    );
    edits
}

#[test]
fn layout_only_changes_cannot_change_graph_semantic_hash() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let graph = structured_graph(source);
    let before = graph.semantic_hash().clone();
    let node_id = graph.plan().nodes()[0].id().clone();

    let mut view = ViewDocument::new(graph.document_id().clone());
    let mut node_view = NodeView::at(120.5, -42.25);
    node_view.collapsed = true;
    node_view.color = Some("#6a5acd".to_owned());
    node_view.annotation = Some("only presentation state".to_owned());
    view.set_node(node_id, node_view).unwrap();
    view.set_viewport(Viewport {
        origin: insight_agent_platform::dsl::v3::graph::CanvasPoint::new(45.0, 80.0),
        zoom: 1.75,
    })
    .unwrap();
    view.validate_against(&graph).unwrap();

    let encoded_view = serde_json::to_vec(&view).unwrap();
    let decoded_view = ViewDocument::decode_json(&encoded_view).unwrap();
    decoded_view.validate_against(&graph).unwrap();
    assert_eq!(&before, graph.semantic_hash());

    let encoded_graph = graph.encode_json().unwrap();
    let decoded_graph = GraphAuthorDocument::decode_json(&encoded_graph).unwrap();
    assert_eq!(decoded_graph.semantic_hash(), &before);
}

#[test]
fn graph_wire_contains_explicit_parts_and_never_serializes_plan_or_hash() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let graph = structured_graph(source);
    let encoded = graph.encode_json().unwrap();
    let wire: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    let object = wire.as_object().unwrap();

    for required in [
        "metadata",
        "nodes",
        "ports",
        "edges",
        "bindings",
        "scopes",
        "policies",
        "source_map",
    ] {
        assert!(
            object.contains_key(required),
            "missing graph part {required}"
        );
    }
    assert!(!object.contains_key("plan"));
    assert!(!object.contains_key("semantic_hash"));
    assert!(!String::from_utf8(encoded)
        .unwrap()
        .contains("semantic_hash"));
}

#[test]
fn authoritative_graph_decode_rejects_unknown_duplicate_and_plan_shaped_fields() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let graph = structured_graph(source);
    let encoded = graph.encode_json().unwrap();
    let mut wire: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    wire["semantic_hash"] = serde_json::Value::String(format!("sha256:{}", "0".repeat(64)));

    let error = GraphAuthorDocument::decode_json(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
    assert_eq!(error.code(), GRAPH_DOCUMENT_INVALID);

    let mut wire: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    wire["plan"] = serde_json::json!({"semantic_hash": format!("sha256:{}", "0".repeat(64))});
    let error = GraphAuthorDocument::decode_json(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
    assert_eq!(error.code(), GRAPH_DOCUMENT_INVALID);

    let encoded = String::from_utf8(encoded).unwrap();
    let duplicate = encoded.replacen('{', "{\"schema_version\":2,", 1);
    let error = GraphAuthorDocument::decode_json(duplicate.as_bytes()).unwrap_err();
    assert_eq!(error.code(), GRAPH_DOCUMENT_INVALID);
    assert!(error.message().contains("duplicate JSON object member"));

    let nested_duplicate = encoded.replacen(
        "\"ports\":{\"control\":",
        "\"ports\":{\"control\":[],\"control\":",
        1,
    );
    assert_ne!(nested_duplicate, encoded);
    let error = GraphAuthorDocument::decode_json(nested_duplicate.as_bytes()).unwrap_err();
    assert_eq!(error.code(), GRAPH_DOCUMENT_INVALID);
    assert!(error.message().contains("duplicate JSON object member"));

    let mut nested_unknown: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    nested_unknown["ports"]["unknown"] = serde_json::json!([]);
    assert_eq!(
        GraphAuthorDocument::decode_json(&serde_json::to_vec(&nested_unknown).unwrap())
            .unwrap_err()
            .code(),
        GRAPH_DOCUMENT_INVALID
    );

    let mut missing_required: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    missing_required.as_object_mut().unwrap().remove("bindings");
    assert_eq!(
        GraphAuthorDocument::decode_json(&serde_json::to_vec(&missing_required).unwrap())
            .unwrap_err()
            .code(),
        GRAPH_DOCUMENT_INVALID
    );
}

#[test]
fn graph_plan_diagnostics_target_exact_canvas_nodes_ports_and_edges() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let graph = structured_graph(source);
    let encoded = graph.encode_json().unwrap();
    let canonical: serde_json::Value = serde_json::from_slice(&encoded).unwrap();

    let node_id = graph.nodes()[0].id().clone();
    let expected_node_span = graph.source_map().node(&node_id).unwrap().clone();
    let mut invalid_node = canonical.clone();
    let node = invalid_node["nodes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|node| node["id"] == node_id.as_str())
        .unwrap();
    node["scope_id"] = serde_json::json!("missing_canvas_scope");
    let error =
        GraphAuthorDocument::decode_json(&serde_json::to_vec(&invalid_node).unwrap()).unwrap_err();
    assert_eq!(error.code(), GRAPH_PLAN_INVALID);
    assert_eq!(
        error.target(),
        Some(&PlanDiagnosticTarget::Node {
            node_id: node_id.clone()
        })
    );
    assert_eq!(error.plan_source_span(), Some(&expected_node_span));

    let control_port = graph.ports().control()[0].clone();
    let expected_port_span = graph
        .source_map()
        .control_port(control_port.id())
        .unwrap()
        .clone();
    let mut invalid_port = canonical.clone();
    let port = invalid_port["ports"]["control"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|port| port["id"] == control_port.id().as_str())
        .unwrap();
    port["owner"] = serde_json::json!("missing_canvas_node");
    let error =
        GraphAuthorDocument::decode_json(&serde_json::to_vec(&invalid_port).unwrap()).unwrap_err();
    assert_eq!(error.code(), GRAPH_PLAN_INVALID);
    assert_eq!(
        error.target().unwrap().port_id(),
        Some(control_port.id().as_str())
    );
    assert_eq!(
        error.target().unwrap().node_id().unwrap().as_str(),
        "missing_canvas_node"
    );
    assert_eq!(error.plan_source_span(), Some(&expected_port_span));

    let edge = graph.edges().control()[0].clone();
    let expected_edge_span = graph.source_map().control_edge(edge.id()).unwrap().clone();
    let mut invalid_edge = canonical;
    let edge_wire = invalid_edge["edges"]["control"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|candidate| candidate["id"] == edge.id().as_str())
        .unwrap();
    edge_wire["to"] = serde_json::json!("missing_canvas_port");
    let error =
        GraphAuthorDocument::decode_json(&serde_json::to_vec(&invalid_edge).unwrap()).unwrap_err();
    assert_eq!(error.code(), GRAPH_PLAN_INVALID);
    assert_eq!(error.target().unwrap().edge_id(), Some(edge.id()));
    assert_eq!(error.plan_source_span(), Some(&expected_edge_span));
}

#[test]
fn structured_graph_conversion_preserves_structured_parse_diagnostics() {
    let source = "api_version: insight.agent/v3\nkind: agent\nkind: agent\ninputs: {}\noutput: string\nworkflow: {steps: []}\n";
    let error = GraphAuthorDocument::from_structured_source(
        GraphDocumentId::new("invalid_structured_graph").unwrap(),
        source,
        options("invalid-structured.yaml", source),
    )
    .unwrap_err();
    assert_eq!(
        error.code(),
        insight_agent_platform::dsl::v3::graph::GRAPH_STRUCTURED_COMPILE_FAILED
    );
    assert_eq!(error.source_path().unwrap().to_string(), "$.kind");
    assert!(error.structured_source_span().is_some());
}

#[test]
fn authoritative_graph_decode_recompiles_and_rejects_invalid_tampering() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let graph = structured_graph(source);
    let expected = graph.semantic_hash().clone();
    let mut wire: serde_json::Value =
        serde_json::from_slice(&graph.encode_json().unwrap()).unwrap();
    wire["nodes"][0]["scope_id"] = serde_json::Value::String("missing_scope".to_owned());

    let error = GraphAuthorDocument::decode_json(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
    assert_eq!(error.code(), GRAPH_PLAN_INVALID);

    let decoded = GraphAuthorDocument::decode_json(&graph.encode_json().unwrap()).unwrap();
    assert_eq!(decoded.semantic_hash(), &expected);
}

#[test]
fn structured_core_sources_round_trip_with_identical_plan_hashes() {
    const WAIT: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: pause
      wait: {duration_ms: 10}
    - id: reply
      wait: {signal: reply, response: string}
    - return: $reply
"#;
    let sources = [
        ("linear.yaml", include_str!("fixtures/v3/linear.yaml")),
        ("if.yaml", include_str!("fixtures/v3/if.yaml")),
        ("parallel.yaml", include_str!("fixtures/v3/parallel.yaml")),
        ("wait.yaml", WAIT),
    ];

    for (source_id, source) in sources {
        let expected_plan = compile_source(source, options(source_id, source)).unwrap();
        let expected_document = validate(parse(source).unwrap()).unwrap();
        let graph = GraphAuthorDocument::from_structured_source(
            GraphDocumentId::new(format!("graph_{source_id}")).unwrap(),
            source,
            options(source_id, source),
        )
        .unwrap();
        assert_eq!(graph.semantic_hash(), expected_plan.semantic_hash());
        assert_eq!(
            graph
                .nodes()
                .iter()
                .map(|node| node.id())
                .collect::<Vec<_>>(),
            expected_plan
                .nodes()
                .iter()
                .map(|node| node.id())
                .collect::<Vec<_>>()
        );

        // Prove that the conversion contract survives publication; the graph
        // does not rely on an in-memory copy of the AST.
        let graph = GraphAuthorDocument::decode_json(&graph.encode_json().unwrap()).unwrap();
        let reduced = graph.to_structured().unwrap();
        assert_eq!(reduced.source(), source);
        assert_eq!(reduced.document(), &expected_document);

        let recompiled =
            compile_source(reduced.source(), options(source_id, reduced.source())).unwrap();
        assert_eq!(recompiled.semantic_hash(), graph.semantic_hash());
    }
}

#[test]
fn native_linear_if_parallel_map_and_loop_graphs_reduce_without_a_source_certificate() {
    const MAP: &str = r#"api_version: insight.agent/v3
kind: agent
types:
  Item:
    fields:
      id: string
      value: string
inputs:
  items: Item[]
output: string[]
workflow:
  steps:
    - id: copied
      map:
        items: $items
        key: id
        as: entry
        max_concurrency: 2
        steps:
          - id: render_item
            type: action
            call: fixture.render
            inputs: {item: $entry}
            response: string
          - yield: $render_item
    - return: $copied
"#;
    const LOOP: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {seed: string}
output: string
workflow:
  steps:
    - id: final_state
      loop:
        initial: $seed
        as: current
        until: false
        max_iterations: 2
        steps:
          - id: next_state
            type: tool
            tool: fixture.next_state
            arguments: {state: $current}
            response: string
          - continue: $next_state
    - return: $final_state
"#;
    let sources = [
        ("native_linear", include_str!("fixtures/v3/linear.yaml")),
        ("native_if", include_str!("fixtures/v3/if.yaml")),
        ("native_parallel", include_str!("fixtures/v3/parallel.yaml")),
        ("native_map", MAP),
        ("native_loop", LOOP),
    ];

    for (id, source) in sources {
        let plan = compile_source(source, options(&format!("{id}.yaml"), source)).unwrap();
        let expected = plan.semantic_hash().clone();
        let graph = GraphAuthorDocument::from_verified_plan(
            GraphDocumentId::new(format!("{id}_graph")).unwrap(),
            plan,
        )
        .unwrap();
        let graph = GraphAuthorDocument::decode_json(&graph.encode_json().unwrap()).unwrap();

        let reduced = graph
            .to_structured()
            .unwrap_or_else(|error| panic!("{id} failed: {error}"));
        let recompiled = compile_source(
            reduced.source(),
            options(&format!("{id}_reduced.json"), reduced.source()),
        )
        .unwrap();
        assert_eq!(recompiled.semantic_hash(), &expected, "{id}");
        assert_eq!(graph.semantic_hash(), &expected, "{id}");
    }
}

#[test]
fn native_llm_graph_reduces_descriptor_v2_without_losing_publication_contracts() {
    const SOURCE: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: answer
      type: llm
      model: general_chat
      messages:
        - role: user
          content: [{text: hello}]
      stream: false
      publish: true
      tools: [lookup]
      tool_choice: required
      tool_limits: {max_rounds: 2, max_calls: 5}
      response: string
    - return: $answer
"#;
    let plan = compile_source(SOURCE, options("native_llm.yaml", SOURCE)).unwrap();
    let expected_hash = plan.semantic_hash().clone();
    let graph = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("native_llm_graph").unwrap(),
        plan,
    )
    .unwrap();
    let graph = GraphAuthorDocument::decode_json(&graph.encode_json().unwrap()).unwrap();

    let reduced = graph.to_structured().unwrap();
    let insight_agent_platform::dsl::v3::ast::Step::Leaf(leaf) = &reduced.document().steps[0]
    else {
        panic!("expected reduced LLM leaf");
    };
    let llm = leaf.llm.as_ref().unwrap();
    assert!(!llm.stream);
    assert!(llm.publish);
    assert_eq!(llm.tools, ["lookup"]);
    assert_eq!(
        llm.tool_choice,
        insight_agent_platform::dsl::v3::ast::LlmToolChoice::Required
    );
    assert_eq!(
        (llm.tool_limits.max_rounds, llm.tool_limits.max_calls),
        (2, 5)
    );

    let recompiled = compile_source(
        reduced.source(),
        options("native_llm_reduced.json", reduced.source()),
    )
    .unwrap();
    assert_eq!(recompiled.semantic_hash(), &expected_hash);

    let changed = SOURCE.replace("stream: false", "stream: true");
    let changed = compile_source(&changed, options("native_llm_changed.yaml", &changed)).unwrap();
    assert_ne!(changed.semantic_hash(), &expected_hash);
}

#[test]
fn native_graph_input_contract_round_trip_preserves_presence_defaults_and_hash() {
    const SOURCE: &str = r#"api_version: insight.agent/v3
kind: agent
inputs:
  required_text: string
  optional_note: {type: string, optional: true}
  language: {type: string, default: en}
output: string
workflow:
  steps:
    - return: $required_text
"#;
    let plan = compile_source(SOURCE, options("native_input_contract.yaml", SOURCE)).unwrap();
    let PlanType::Object { properties, .. } = plan.metadata().input_contract().accepted_type()
    else {
        panic!("fixture input must be an object");
    };
    assert!(properties["required_text"].required);
    assert!(!properties["optional_note"].required);
    assert!(!properties["language"].required);
    assert_eq!(
        plan.metadata().input_contract().defaults()["language"],
        serde_json::json!("en")
    );

    let expected_hash = plan.semantic_hash().clone();
    let expected_contract = plan.metadata().input_contract().clone();
    let graph = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("native_input_contract_graph").unwrap(),
        plan,
    )
    .unwrap();
    let decoded = GraphAuthorDocument::decode_json(&graph.encode_json().unwrap()).unwrap();
    decoded.validate().unwrap();
    assert_eq!(decoded.semantic_hash(), &expected_hash);
    assert_eq!(decoded.metadata().input_contract(), &expected_contract);

    let reduced = decoded.to_structured().unwrap();
    assert!(reduced.document().inputs["required_text"].default.is_none());
    assert!(!reduced.document().inputs["required_text"].optional);
    assert!(reduced.document().inputs["optional_note"].optional);
    assert!(reduced.document().inputs["optional_note"].default.is_none());
    assert!(!reduced.document().inputs["language"].optional);
    assert_eq!(
        reduced.document().inputs["language"].default,
        Some(serde_json::json!("en"))
    );
    let recompiled = compile_source(
        reduced.source(),
        options("native_input_contract_reduced.json", reduced.source()),
    )
    .unwrap();
    assert_eq!(recompiled.metadata().input_contract(), &expected_contract);
    assert_eq!(recompiled.semantic_hash(), &expected_hash);

    let changed = SOURCE.replace("default: en", "default: fr");
    let changed = compile_source(
        &changed,
        options("native_input_contract_changed.yaml", &changed),
    )
    .unwrap();
    assert_ne!(changed.semantic_hash(), &expected_hash);
}

#[test]
fn native_nullable_required_input_is_not_inferred_to_be_optional() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let plan = compile_source(source, options("native_nullable_input.yaml", source)).unwrap();
    let PlanType::Object {
        mut properties,
        additional_properties,
    } = plan.metadata().input_contract().accepted_type().clone()
    else {
        panic!("fixture input must be an object");
    };
    properties.insert(
        "nullable_note".to_owned(),
        PlanProperty::new(
            PlanType::union([PlanType::String, PlanType::Null]).unwrap(),
            true,
        )
        .unwrap(),
    );
    let metadata = PlanMetadata::new(
        plan.metadata().definition_revision_id().clone(),
        plan.metadata().compiler_version().clone(),
        AuthorFormat::Programmatic,
        plan.metadata().entry_node_id().clone(),
        PlanInputContract::new(PlanType::Object {
            properties,
            additional_properties,
        })
        .with_defaults(plan.metadata().input_contract().defaults().clone()),
        plan.metadata().output_type().clone(),
        plan.metadata().error_type().clone(),
    );
    let mut builder = PlanBuilder::new(metadata);
    for node in plan.nodes() {
        builder.add_node(node.clone());
    }
    for port in plan.control_ports() {
        builder.add_control_port(port.clone());
    }
    for port in plan.data_ports() {
        builder.add_data_port(port.clone());
    }
    for edge in plan.control_edges() {
        builder.add_control_edge(edge.clone());
    }
    for binding in plan.data_bindings() {
        builder.add_data_binding(binding.clone());
    }
    for binding in plan.phi_bindings() {
        builder.add_phi_binding(binding.clone());
    }
    for scope in plan.scopes() {
        builder.add_scope(scope.clone());
    }
    for policy in plan.policies() {
        builder.add_policy(policy.clone());
    }
    builder.set_source_map(SourceMap::new());
    let graph = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("native_nullable_input_graph").unwrap(),
        builder.build().unwrap(),
    )
    .unwrap();
    graph.validate().unwrap();

    let diagnostic = graph.to_structured().unwrap_err();
    assert_eq!(diagnostic.code(), GRAPH_IRREDUCIBLE);
    assert!(diagnostic
        .message()
        .contains("has no lossless structured type spelling"));
    assert!(!diagnostic.message().contains("optional"));
}

#[test]
fn native_linear_graph_with_policy_remains_graph_with_a_stable_diagnostic() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let plan = compile_source(source, options("native_policy.yaml", source)).unwrap();
    let task = plan
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::ActionTask(_)))
        .unwrap()
        .id()
        .clone();
    let policy_id = PolicyId::new("native_retry_policy").unwrap();
    let mut source_map = plan.source_map().clone();
    source_map.insert_policy(
        policy_id.clone(),
        plan.source_map().node(&task).unwrap().clone(),
    );
    let mut builder = PlanBuilder::from_verified_plan(&plan).unwrap();
    builder.set_source_map(source_map).add_policy(Policy::new(
        policy_id,
        task,
        PolicyKind::Retry(RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
        }),
    ));
    let graph = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("native_policy_graph").unwrap(),
        builder.build().unwrap(),
    )
    .unwrap();
    graph.validate().unwrap();
    assert_eq!(graph.policies().len(), 1);

    let expected_diagnostic = "graph is not structurally reducible (authored policy syntax cannot yet represent this Plan); retain graph authoring mode";
    let diagnostic = graph.to_structured().unwrap_err();
    assert_eq!(diagnostic.code(), GRAPH_IRREDUCIBLE);
    assert_eq!(diagnostic.message(), expected_diagnostic);

    let semantic_hash = graph.semantic_hash().clone();
    let policies = graph.policies().to_vec();
    let decoded = GraphAuthorDocument::decode_json(&graph.encode_json().unwrap()).unwrap();
    decoded.validate().unwrap();
    assert_eq!(decoded.semantic_hash(), &semantic_hash);
    assert_eq!(decoded.policies(), policies);
    let decoded_diagnostic = decoded.to_structured().unwrap_err();
    assert_eq!(decoded_diagnostic.code(), GRAPH_IRREDUCIBLE);
    assert_eq!(decoded_diagnostic.message(), expected_diagnostic);
}

#[test]
fn graph_policy_edits_reject_unexecutable_contracts_before_publication() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let mut graph = structured_graph(source);
    let task = graph
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::ActionTask(_)))
        .unwrap()
        .id()
        .clone();
    let original = graph.clone();
    let error = graph
        .apply_semantic_edits([GraphSemanticEdit::UpsertPolicy {
            policy: Policy::new(
                PolicyId::new("unexecutable_budget").unwrap(),
                task,
                PolicyKind::Budget(BudgetPolicy {
                    max_tokens: Some(1_000),
                    max_cost_microunits: None,
                }),
            ),
        }])
        .unwrap_err();
    assert_eq!(error.code(), GRAPH_PLAN_INVALID);
    assert!(error.message().contains(PLAN_POLICY_INVALID));
    assert!(error
        .message()
        .contains("budget enforcement has no durable runtime contract"));
    assert_eq!(graph, original, "a rejected policy edit must be atomic");

    const MAP: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {items: "string[]"}
output: "string[]"
workflow:
  steps:
    - id: copied
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
    - return: $copied
"#;
    const LOOP: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {seed: string}
output: string
workflow:
  steps:
    - id: result
      loop:
        initial: $seed
        as: state
        until: false
        max_iterations: 1
        steps:
          - id: next_state
            type: action
            call: fixture.next
            inputs: {state: $state}
            response: string
          - continue: $next_state
    - return: $result
"#;
    const SUBFLOW: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {question: string}
output: string
workflow:
  steps:
    - id: child
      type: call
      definition_revision: child_revision_v1
      interface_version: child-v1
      input: {question: $question}
      response: string
    - return: $child
"#;
    const TIMER: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: pause
      wait: {duration_ms: 1}
    - return: done
"#;

    for (case_name, source, node_kind) in [
        ("map", MAP, "map"),
        ("loop", LOOP, "loop"),
        ("subflow", SUBFLOW, "subflow_call"),
        ("timer", TIMER, "timer"),
    ] {
        let mut graph = GraphAuthorDocument::from_structured_source(
            GraphDocumentId::new(format!("unexecutable_{case_name}_timeout")).unwrap(),
            source,
            options(&format!("{case_name}.yaml"), source),
        )
        .unwrap();
        let target = graph
            .nodes()
            .iter()
            .find(|node| node.kind().name() == node_kind)
            .unwrap()
            .id()
            .clone();
        let original = graph.clone();
        let error = graph
            .apply_semantic_edits([GraphSemanticEdit::UpsertPolicy {
                policy: Policy::new(
                    PolicyId::new(format!("{case_name}_timeout")).unwrap(),
                    target,
                    PolicyKind::Timeout(TimeoutPolicy { timeout_ms: 1 }),
                ),
            }])
            .unwrap_err();
        assert_eq!(error.code(), GRAPH_PLAN_INVALID, "case={case_name}");
        assert!(error.message().contains(PLAN_POLICY_INVALID));
        assert!(
            error
                .message()
                .contains("structural timeout enforcement has no durable runtime contract"),
            "case={case_name}, error={error}"
        );
        assert_eq!(
            graph, original,
            "case={case_name}: rejected edit must not become publishable"
        );
    }
}

#[test]
fn crossing_branch_regions_are_rejected_at_the_verified_graph_boundary() {
    let root = ScopeId::new("crossing_root").unwrap();
    let route_a = NodeId::new("route_a").unwrap();
    let route_b = NodeId::new("route_b").unwrap();
    let merge_a = NodeId::new("merge_a").unwrap();
    let merge_b = NodeId::new("merge_b").unwrap();
    let finish = NodeId::new("finish").unwrap();

    let cp = |value: &str| ControlPortId::new(value).unwrap();
    let case = |value: &str| BranchCaseId::new(value).unwrap();
    let name = |value: &str| PortName::new(value).unwrap();
    let condition = || {
        PureExpression::new(
            ExpressionLanguage::Cel,
            VersionTag::new(CEL_EXPRESSION_ENGINE_VERSION).unwrap(),
            "true",
            PlanType::Boolean,
        )
    };
    let a_then = cp("a_then");
    let a_else = cp("a_else");
    let b_in = cp("b_in");
    let b_then = cp("b_then");
    let b_else = cp("b_else");
    let ma_then = cp("ma_then");
    let ma_else = cp("ma_else");
    let ma_out = cp("ma_out");
    let mb_then = cp("mb_then");
    let mb_else = cp("mb_else");
    let mb_out = cp("mb_out");
    let finish_in = cp("finish_in");
    let finish_value = DataPortId::new("finish_value").unwrap();

    let safe_error = PlanType::Object {
        properties: BTreeMap::from([
            (
                "code".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            ),
            (
                "kind".to_owned(),
                PlanProperty::new(
                    PlanType::literal(serde_json::json!("safe_error")).unwrap(),
                    true,
                )
                .unwrap(),
            ),
            (
                "message".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            ),
        ]),
        additional_properties: None,
    };
    let metadata = PlanMetadata::new(
        DefinitionRevisionId::new("crossing_revision").unwrap(),
        VersionTag::new("graph-test-1").unwrap(),
        AuthorFormat::Programmatic,
        route_a.clone(),
        PlanInputContract::new(PlanType::Object {
            properties: BTreeMap::new(),
            additional_properties: None,
        }),
        PlanType::String,
        safe_error,
    );
    let mut builder = PlanBuilder::new(metadata);
    builder.add_scope(ScopeMetadata::root(root.clone()));
    builder.add_node(Node::new(
        route_a.clone(),
        root.clone(),
        NodeKind::Branch(BranchDescriptor {
            cases: vec![
                BranchCase::when(case("then"), condition(), a_then.clone()),
                BranchCase::otherwise(case("else"), a_else.clone()),
            ],
        }),
    ));
    builder.add_node(Node::new(
        route_b.clone(),
        root.clone(),
        NodeKind::Branch(BranchDescriptor {
            cases: vec![
                BranchCase::when(case("then"), condition(), b_then.clone()),
                BranchCase::otherwise(case("else"), b_else.clone()),
            ],
        }),
    ));
    builder.add_node(Node::new(
        merge_a.clone(),
        root.clone(),
        NodeKind::Merge(MergeDescriptor {
            branch_node_id: route_a.clone(),
            arms: BTreeMap::from([
                (case("then"), ma_then.clone()),
                (case("else"), ma_else.clone()),
            ]),
            output_port: ma_out.clone(),
        }),
    ));
    builder.add_node(Node::new(
        merge_b.clone(),
        root.clone(),
        NodeKind::Merge(MergeDescriptor {
            branch_node_id: route_b.clone(),
            arms: BTreeMap::from([
                (case("then"), mb_then.clone()),
                (case("else"), mb_else.clone()),
            ]),
            output_port: mb_out.clone(),
        }),
    ));
    builder.add_node(Node::new(
        finish.clone(),
        root,
        NodeKind::Return(ReturnDescriptor {
            value_input: finish_value.clone(),
        }),
    ));
    for (id, owner, port_name, direction) in [
        (
            a_then.clone(),
            route_a.clone(),
            "then",
            PortDirection::Output,
        ),
        (a_else.clone(), route_a, "else", PortDirection::Output),
        (b_in.clone(), route_b.clone(), "in", PortDirection::Input),
        (
            b_then.clone(),
            route_b.clone(),
            "then",
            PortDirection::Output,
        ),
        (b_else.clone(), route_b, "else", PortDirection::Output),
        (
            ma_then.clone(),
            merge_a.clone(),
            "then",
            PortDirection::Input,
        ),
        (
            ma_else.clone(),
            merge_a.clone(),
            "else",
            PortDirection::Input,
        ),
        (ma_out.clone(), merge_a, "out", PortDirection::Output),
        (
            mb_then.clone(),
            merge_b.clone(),
            "then",
            PortDirection::Input,
        ),
        (
            mb_else.clone(),
            merge_b.clone(),
            "else",
            PortDirection::Input,
        ),
        (mb_out.clone(), merge_b, "out", PortDirection::Output),
        (
            finish_in.clone(),
            finish.clone(),
            "in",
            PortDirection::Input,
        ),
    ] {
        builder.add_control_port(ControlPort::new(id, owner, name(port_name), direction));
    }
    builder.add_data_port(DataPort::new(
        finish_value.clone(),
        finish,
        name("value"),
        PortDirection::Input,
        PlanType::String,
        true,
    ));
    for (id, from, to) in [
        ("edge_a_then_b", a_then, b_in),
        ("edge_a_else_ma", a_else, ma_else),
        ("edge_b_then_ma", b_then, ma_then),
        ("edge_ma_mb", ma_out, mb_then),
        ("edge_b_else_mb", b_else, mb_else),
        ("edge_mb_finish", mb_out, finish_in),
    ] {
        builder.add_control_edge(ControlEdge::new(ControlEdgeId::new(id).unwrap(), from, to));
    }
    builder.add_data_binding(DataBinding::new(
        DataBindingId::new("bind_finish").unwrap(),
        ValueSource::Literal {
            value: serde_json::json!("done"),
        },
        finish_value,
    ));
    builder.set_source_map(SourceMap::new());
    let error = builder.build().unwrap_err();
    assert_eq!(error.code(), PLAN_MERGE_INVALID);
}

#[test]
fn valid_graphs_without_a_lossless_structured_inverse_remain_graphs() {
    let root = ScopeId::new("native_root").unwrap();
    let finish = NodeId::new("native_finish").unwrap();
    let finish_value = DataPortId::new("native_finish_value").unwrap();
    let safe_error = PlanType::Object {
        properties: BTreeMap::from([
            (
                "code".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            ),
            (
                "kind".to_owned(),
                PlanProperty::new(
                    PlanType::literal(serde_json::json!("safe_error")).unwrap(),
                    true,
                )
                .unwrap(),
            ),
            (
                "message".to_owned(),
                PlanProperty::new(PlanType::String, true).unwrap(),
            ),
        ]),
        additional_properties: None,
    };
    let metadata = PlanMetadata::new(
        DefinitionRevisionId::new("native_irreducible_revision").unwrap(),
        VersionTag::new("graph-test-1").unwrap(),
        AuthorFormat::Programmatic,
        finish.clone(),
        PlanInputContract::new(PlanType::Object {
            properties: BTreeMap::new(),
            additional_properties: None,
        }),
        PlanType::String,
        safe_error,
    );
    let mut builder = PlanBuilder::new(metadata);
    builder
        .add_scope(ScopeMetadata::root(root.clone()))
        .add_node(Node::new(
            finish.clone(),
            root,
            NodeKind::Return(ReturnDescriptor {
                value_input: finish_value.clone(),
            }),
        ))
        .add_data_port(DataPort::new(
            finish_value.clone(),
            finish,
            PortName::new("value").unwrap(),
            PortDirection::Input,
            PlanType::String,
            true,
        ))
        .add_data_binding(DataBinding::new(
            DataBindingId::new("native_finish_binding").unwrap(),
            ValueSource::Literal {
                value: serde_json::json!("done"),
            },
            finish_value,
        ))
        .set_source_map(SourceMap::new());
    let plan = builder.build().unwrap();
    let graph = GraphAuthorDocument::from_verified_plan(
        GraphDocumentId::new("native_irreducible_graph").unwrap(),
        plan,
    )
    .unwrap();
    let node_span = graph.source_map().node(graph.nodes()[0].id()).unwrap();
    let port_span = graph
        .source_map()
        .data_port(graph.ports().data()[0].id())
        .unwrap();
    assert_ne!(node_span, port_span);
    assert!(node_span.start.line > 1);
    assert!(port_span.start.line > node_span.start.line);
    assert_ne!(
        (node_span.start.line, node_span.start.column),
        (1, 1),
        "native Canvas elements must not share a virtual 1:1 point"
    );

    let diagnostic = graph.to_structured().unwrap_err();
    assert_eq!(diagnostic.code(), GRAPH_IRREDUCIBLE);
    assert!(diagnostic.message().contains("retain graph authoring mode"));
}

#[test]
fn trace_overlay_links_stable_run_activation_and_node_ids_without_semantic_effect() {
    let source = include_str!("fixtures/v3/parallel.yaml");
    let graph = structured_graph(source);
    let before = graph.semantic_hash().clone();
    let node_id = graph.plan().nodes()[0].id().clone();

    let mut trace = TraceOverlay::new(
        graph.document_id().clone(),
        RunId::new("run_graph_trace_1").unwrap(),
    );
    trace
        .add_activation(ActivationTrace::new(
            ActivationId::new("activation_graph_trace_1").unwrap(),
            node_id,
            Some(1),
            TraceActivationState::Running,
        ))
        .unwrap();
    trace.validate_against(&graph).unwrap();
    assert_eq!(&before, graph.semantic_hash());

    let encoded = trace.encode_json().unwrap();
    let decoded = TraceOverlay::decode_json(&encoded).unwrap();
    decoded.validate_against(&graph).unwrap();
    assert_eq!(decoded.run_id().as_str(), "run_graph_trace_1");
}

#[test]
fn branch_canvas_edits_are_atomic_verified_and_preserve_stable_node_identity() {
    let source = include_str!("fixtures/v3/if.yaml");
    let mut graph = structured_graph(source);
    let (branch_id, mut descriptor) = graph
        .plan()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::Branch(descriptor) => Some((node.id().clone(), descriptor.clone())),
            _ => None,
        })
        .unwrap();
    assert!(descriptor.cases.len() >= 2);
    let before = graph.semantic_hash().clone();
    descriptor.cases.swap(0, 1);
    graph
        .apply_semantic_edits([GraphSemanticEdit::Branch {
            node_id: branch_id.clone(),
            descriptor,
        }])
        .unwrap();
    graph.plan().verify().unwrap();
    assert_ne!(graph.semantic_hash(), &before);
    assert_eq!(graph.source_map().documents().len(), 1);
    assert!(graph
        .source_map()
        .documents()
        .keys()
        .all(|source_id| source_id.as_str().starts_with("graph:")));
    assert!(graph
        .plan()
        .nodes()
        .iter()
        .any(|node| node.id() == &branch_id));

    let authoritative = graph.clone();
    let (descriptor, non_branch_id) = {
        let descriptor = graph
            .plan()
            .nodes()
            .iter()
            .find_map(|node| match node.kind() {
                NodeKind::Branch(value) => Some(value.clone()),
                _ => None,
            })
            .unwrap();
        let non_branch_id = graph
            .plan()
            .nodes()
            .iter()
            .find(|node| !matches!(node.kind(), NodeKind::Branch(_)))
            .unwrap()
            .id()
            .clone();
        (descriptor, non_branch_id)
    };
    let error = graph
        .apply_semantic_edits([GraphSemanticEdit::Branch {
            node_id: non_branch_id,
            descriptor,
        }])
        .unwrap_err();
    assert_eq!(error.code(), GRAPH_EDIT_KIND_MISMATCH);
    assert_eq!(graph, authoritative, "failed edit must be all-or-nothing");
}

#[test]
fn semantic_edit_wire_is_closed_and_grouped_topology_upsert_delete_is_atomic() {
    const DIRECT: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {primary: boolean, secondary: boolean, question: string}
output: string
workflow:
  steps:
    - id: route
      type: action
      call: fixture.direct
      inputs: {question: $question}
      response: string
    - return: $route
"#;
    const BRANCHED: &str = include_str!("fixtures/v3/if.yaml");

    let mut graph = graph_for_revision(DIRECT, "semantic_edit_revision_1");
    let branched = graph_for_revision(BRANCHED, "semantic_edit_revision_2");
    assert_eq!(
        graph.metadata().entry_node_id(),
        branched.metadata().entry_node_id(),
        "the transaction deliberately preserves the closed metadata contract"
    );
    let original = graph.clone();
    let batch = GraphSemanticEditBatch::new(
        graph.semantic_hash().clone(),
        DefinitionRevisionId::new("semantic_edit_revision_2").unwrap(),
        topology_edits(&graph, &branched),
    )
    .unwrap();
    let encoded = batch.encode_json().unwrap();
    assert_eq!(
        GraphSemanticEditBatch::decode_json(&encoded).unwrap(),
        batch
    );
    let mut unknown: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    unknown["edits"][0]["undeclared"] = serde_json::json!(true);
    assert!(GraphSemanticEditBatch::decode_json(&serde_json::to_vec(&unknown).unwrap()).is_err());
    let duplicate = String::from_utf8(encoded).unwrap().replacen(
        "\"schema_version\":1",
        "\"schema_version\":1,\"schema_version\":1",
        1,
    );
    assert!(GraphSemanticEditBatch::decode_json(duplicate.as_bytes()).is_err());

    graph.apply_semantic_edit_batch(batch).unwrap();
    assert_eq!(graph.semantic_hash(), branched.semantic_hash());
    assert_eq!(graph.nodes(), branched.nodes());
    assert_eq!(graph.ports(), branched.ports());
    assert_eq!(graph.edges(), branched.edges());
    assert_eq!(graph.bindings(), branched.bindings());
    assert_eq!(graph.scopes(), branched.scopes());

    let stale = GraphSemanticEditBatch::new(
        original.semantic_hash().clone(),
        DefinitionRevisionId::new("semantic_edit_revision_stale").unwrap(),
        vec![GraphSemanticEdit::UpsertNode {
            node: graph.nodes()[0].clone(),
        }],
    )
    .unwrap();
    let authoritative = graph.clone();
    let error = graph.apply_semantic_edit_batch(stale).unwrap_err();
    assert_eq!(error.code(), GRAPH_EDIT_CONFLICT);
    assert_eq!(graph, authoritative);

    let direct_v3 = graph_for_revision(DIRECT, "semantic_edit_revision_3");
    let remove_branch = GraphSemanticEditBatch::new(
        graph.semantic_hash().clone(),
        DefinitionRevisionId::new("semantic_edit_revision_3").unwrap(),
        topology_edits(&graph, &direct_v3),
    )
    .unwrap();
    graph.apply_semantic_edit_batch(remove_branch).unwrap();
    assert_eq!(graph.semantic_hash(), direct_v3.semantic_hash());
    assert_eq!(graph.nodes(), direct_v3.nodes());

    let before_invalid = graph.clone();
    let invalid = GraphSemanticEditBatch::new(
        graph.semantic_hash().clone(),
        DefinitionRevisionId::new("semantic_edit_revision_invalid").unwrap(),
        vec![GraphSemanticEdit::DeleteNode {
            node_id: graph.metadata().entry_node_id().clone(),
        }],
    )
    .unwrap();
    assert!(graph.apply_semantic_edit_batch(invalid).is_err());
    assert_eq!(
        graph, before_invalid,
        "invalid batch must roll back every edit"
    );
}

#[test]
fn parallel_map_and_loop_canvas_edit_surfaces_reverify_the_complete_plan() {
    let mut parallel = structured_graph(include_str!("fixtures/v3/parallel.yaml"));
    let (fork_node_id, mut fork) = parallel
        .plan()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::Fork(value) => Some((node.id().clone(), value.clone())),
            _ => None,
        })
        .unwrap();
    let (join_node_id, join) = parallel
        .plan()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::Join(value) => Some((node.id().clone(), value.clone())),
            _ => None,
        })
        .unwrap();
    let parallel_hash = parallel.semantic_hash().clone();
    fork.legs.swap(0, 1);
    parallel
        .apply_semantic_edits([GraphSemanticEdit::Parallel {
            fork_node_id,
            fork,
            join_node_id,
            join,
        }])
        .unwrap();
    assert_ne!(parallel.semantic_hash(), &parallel_hash);

    const MAP: &str = r#"api_version: insight.agent/v3
kind: agent
types:
  Item:
    fields:
      id: string
      value: string
inputs:
  items: Item[]
output: string[]
workflow:
  steps:
    - id: copied
      map:
        items: $items
        key: id
        as: item
        max_concurrency: 2
        steps:
          - id: render_item
            type: action
            call: fixture.render
            inputs: {item: $item}
            response: string
          - yield: $render_item
    - return: $copied
"#;
    let mut map = structured_graph(MAP);
    let (map_node_id, mut map_descriptor) = map
        .plan()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::Map(value) => Some((node.id().clone(), value.clone())),
            _ => None,
        })
        .unwrap();
    let map_hash = map.semantic_hash().clone();
    map_descriptor.max_concurrency = Some(
        map_descriptor
            .max_concurrency
            .unwrap_or(1)
            .saturating_add(1),
    );
    map.apply_semantic_edits([GraphSemanticEdit::Map {
        node_id: map_node_id,
        descriptor: map_descriptor,
    }])
    .unwrap();
    map.plan().verify().unwrap();
    assert_ne!(map.semantic_hash(), &map_hash);

    const LOOP: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {seed: string}
output: string
workflow:
  steps:
    - id: final_state
      loop:
        initial: $seed
        as: state
        until: false
        max_iterations: 2
        steps:
          - id: next_state
            type: tool
            tool: fixture.next_state
            arguments: {state: $state}
            response: string
          - break: $next_state
    - return: $final_state
"#;
    let mut loop_graph = structured_graph(LOOP);
    let (loop_node_id, mut loop_descriptor) = loop_graph
        .plan()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::Loop(value) => Some((node.id().clone(), value.clone())),
            _ => None,
        })
        .unwrap();
    let loop_hash = loop_graph.semantic_hash().clone();
    loop_descriptor.max_iterations = Some(
        loop_descriptor
            .max_iterations
            .unwrap_or(1)
            .saturating_add(1),
    );
    loop_graph
        .apply_semantic_edits([GraphSemanticEdit::Loop {
            node_id: loop_node_id,
            descriptor: loop_descriptor,
        }])
        .unwrap();
    loop_graph.plan().verify().unwrap();
    assert_ne!(loop_graph.semantic_hash(), &loop_hash);
}
