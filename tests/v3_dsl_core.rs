use insight_agent_platform::{
    dsl::v3::{compile_source, validate, CompileOptions, INVALID_STEP},
    engine::{
        plan::{
            AuthorFormat, DescriptorValue, ExpressionLanguage, NodeKind, PlanBuilder, PlanMetadata,
            SourceMap, ValueSource, VersionTag,
        },
        DefinitionRevisionId,
    },
};

fn options(source: &str) -> CompileOptions {
    CompileOptions::new(
        DefinitionRevisionId::new("dsl_v3_fixture_revision").unwrap(),
        "fixture.yaml",
        source,
    )
}

#[test]
fn compiles_linear_leaf_and_return_to_verified_plan() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let plan = compile_source(source, options(source)).unwrap();
    let NodeKind::ActionTask(descriptor) = plan.nodes()[0].kind() else {
        panic!("expected action task");
    };
    assert_eq!(descriptor.descriptor_version.as_str(), "1");
    assert!(plan
        .nodes()
        .iter()
        .any(|node| matches!(node.kind(), NodeKind::Return(_))));
    assert_eq!(
        plan.source_map()
            .node(plan.nodes()[0].id())
            .unwrap()
            .source_id
            .as_str(),
        "fixture.yaml"
    );
}

#[test]
fn lowers_ordered_if_elif_else_to_branch_merge_and_typed_phi() {
    let source = include_str!("fixtures/v3/if.yaml");
    let plan = compile_source(source, options(source)).unwrap();
    let branch = plan
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::Branch(value) => Some(value),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        branch
            .cases
            .iter()
            .map(|case| case.case_id.as_str())
            .collect::<Vec<_>>(),
        ["then", "secondary", "else"]
    );
    assert_eq!(plan.phi_bindings().len(), 1);
    assert!(plan
        .nodes()
        .iter()
        .any(|node| matches!(node.kind(), NodeKind::Merge(_))));
}

#[test]
fn lowers_parallel_to_flat_fork_join_collect() {
    let source = include_str!("fixtures/v3/parallel.yaml");
    let plan = compile_source(source, options(source)).unwrap();
    assert_eq!(
        plan.nodes()
            .iter()
            .filter(|node| matches!(node.kind(), NodeKind::Fork(_)))
            .count(),
        1
    );
    assert!(plan
        .nodes()
        .iter()
        .any(|node| matches!(node.kind(), NodeKind::Join(_))));
    assert!(plan
        .nodes()
        .iter()
        .any(|node| matches!(node.kind(), NodeKind::Collect(_))));
}

#[test]
fn parallel_all_settled_freezes_typed_result_envelopes() {
    let source = include_str!("fixtures/v3/parallel.yaml")
        .replace("settle: all_success", "settle: all_settled");
    let plan = compile_source(&source, options(&source)).unwrap();
    let collect = plan
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), NodeKind::Collect(_)))
        .unwrap();
    let result = plan
        .data_ports()
        .iter()
        .find(|port| port.owner() == collect.id() && port.name().as_str() == "result")
        .unwrap();
    let encoded = serde_json::to_string(result.value_type()).unwrap();
    assert!(encoded.contains("\"ok\""));
    assert!(encoded.contains("\"error\""));
}

#[test]
fn literal_match_folds_and_dynamic_match_lowers_to_a_typed_pure_expression() {
    let literal = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - return:
        match: image
        cases: {image: vision}
        default: report
"#;
    let plan = compile_source(literal, options(literal)).unwrap();
    assert_eq!(plan.nodes().len(), 1);
    assert!(matches!(plan.nodes()[0].kind(), NodeKind::Return(_)));

    let dynamic = literal
        .replace("inputs: {}", "inputs: {route: string}")
        .replace("match: image", "match: $route");
    let plan = compile_source(&dynamic, options(&dynamic)).unwrap();
    assert!(plan.data_bindings().iter().any(|binding| matches!(
        binding.source(),
        ValueSource::Expression { expression }
            if expression.language == ExpressionLanguage::Match
                && expression.result_type.is_assignable_to(
                    &insight_agent_platform::engine::plan::PlanType::String
                )
    )));
}

#[test]
fn typed_cel_conditions_support_logic_comparison_and_size_with_explicit_dependencies() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs:
  ready: boolean
  messages: "Message[]"
output: string
workflow:
  steps:
    - id: route
      if: ready && size(messages) > 0
      then:
        - return: yes
      else:
        - return: no
"#;
    let plan = compile_source(source, options(source)).unwrap();
    let condition = plan
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            NodeKind::Branch(branch) => branch.cases[0].condition.as_ref(),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        condition.dependencies.keys().cloned().collect::<Vec<_>>(),
        ["messages", "ready"]
    );
    assert_eq!(
        condition.result_type,
        insight_agent_platform::engine::plan::PlanType::Boolean
    );
}

#[test]
fn wait_signal_timer_and_declared_raise_lower_to_lifecycle_nodes() {
    let wait = r#"api_version: insight.agent/v3
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
    let plan = compile_source(wait, options(wait)).unwrap();
    assert!(plan
        .nodes()
        .iter()
        .any(|node| matches!(node.kind(), NodeKind::Timer(_))));
    assert!(plan
        .nodes()
        .iter()
        .any(|node| matches!(node.kind(), NodeKind::WaitSignal(_))));

    let raised = r#"api_version: insight.agent/v3
kind: agent
errors:
  rejected:
    category: workflow
    code: REQUEST_REJECTED
    public_message: request rejected
inputs: {}
output: string
workflow:
  steps:
    - raise: rejected
"#;
    let plan = compile_source(raised, options(raised)).unwrap();
    assert!(matches!(plan.nodes()[0].kind(), NodeKind::Raise(_)));
}

#[test]
fn normalized_hash_matches_an_equivalent_programmatic_plan() {
    let source = include_str!("fixtures/v3/linear.yaml");
    let compiled = compile_source(source, options(source)).unwrap();
    let metadata = PlanMetadata::new(
        DefinitionRevisionId::new("manual_equivalent").unwrap(),
        VersionTag::new("manual-1").unwrap(),
        AuthorFormat::Programmatic,
        compiled.metadata().entry_node_id().clone(),
        compiled.metadata().input_contract().clone(),
        compiled.metadata().output_type().clone(),
        compiled.metadata().error_type().clone(),
    );
    let mut builder = PlanBuilder::new(metadata);
    for value in compiled.nodes().iter().rev().cloned() {
        builder.add_node(value);
    }
    for value in compiled.control_ports().iter().rev().cloned() {
        builder.add_control_port(value);
    }
    for value in compiled.data_ports().iter().rev().cloned() {
        builder.add_data_port(value);
    }
    for value in compiled.control_edges().iter().rev().cloned() {
        builder.add_control_edge(value);
    }
    for value in compiled.data_bindings().iter().rev().cloned() {
        builder.add_data_binding(value);
    }
    for value in compiled.phi_bindings().iter().rev().cloned() {
        builder.add_phi_binding(value);
    }
    for value in compiled.scopes().iter().rev().cloned() {
        builder.add_scope(value);
    }
    builder.set_source_map(SourceMap::new());
    let manual = builder.build().unwrap();
    assert_eq!(compiled.semantic_hash(), manual.semantic_hash());
}

#[test]
fn preserves_prompt_message_splice_content_and_template_contracts() {
    let source = r#"api_version: insight.agent/v3
kind: agent
metadata: {id: chat, name: Chat}
prompts:
  system: {inline: You are safe.}
inputs:
  messages: {type: "Message[]", default: []}
  question: string
  image_url: {type: string, optional: true}
output: string
workflow:
  steps:
    - id: answer
      type: llm
      model: general_chat
      messages:
        - role: system
          content:
            - text: system
        - $messages
        - role: user
          content:
            - text: "Question: {{ question }}"
            - image_url: $image_url
      response: string
    - return: $answer
"#;
    let document = validate(insight_agent_platform::dsl::v3::parse(source).unwrap()).unwrap();
    let insight_agent_platform::dsl::v3::ast::Step::Leaf(leaf) = &document.steps[0] else {
        panic!("expected llm leaf");
    };
    let llm = leaf.llm.as_ref().unwrap();
    assert!(matches!(
        llm.messages[0],
        insight_agent_platform::dsl::v3::ast::MessageExpr::Message { .. }
    ));
    assert!(matches!(
        llm.messages[1],
        insight_agent_platform::dsl::v3::ast::MessageExpr::Splice(_)
    ));
    let plan = insight_agent_platform::dsl::v3::compile(document, options(source)).unwrap();
    let NodeKind::LlmTask(descriptor) = plan.nodes()[0].kind() else {
        panic!("expected LLM task");
    };
    let DescriptorValue::Object(bindings) = descriptor
        .public_configuration
        .get("runtime_bindings")
        .unwrap()
    else {
        panic!("runtime bindings must be an object");
    };
    let DescriptorValue::String(image_port) = bindings.get("image_url").unwrap() else {
        panic!("image binding must name a data port");
    };
    let image_port = plan
        .data_ports()
        .iter()
        .find(|port| port.id().as_str() == image_port)
        .unwrap();
    assert!(!image_port.required());
    assert!(matches!(
        plan.data_bindings()
            .iter()
            .find(|binding| binding.to() == image_port.id())
            .unwrap()
            .source(),
        ValueSource::OptionalRunInput { path } if path == &["image_url".to_owned()]
    ));
    assert_eq!(
        descriptor
            .public_configuration
            .get("optional_runtime_bindings"),
        Some(&DescriptorValue::Array(vec![DescriptorValue::String(
            "image_url".to_owned()
        )]))
    );
    assert!(plan.data_bindings().len() >= 4);
}

#[test]
fn llm_execution_and_publication_contracts_are_normalized_into_descriptor_v2() {
    let source = r#"api_version: insight.agent/v3
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
      tools: [lookup, summarize]
      tool_choice: summarize
      tool_limits: {max_rounds: 3}
      response: string
    - return: $answer
"#;
    let document = validate(insight_agent_platform::dsl::v3::parse(source).unwrap()).unwrap();
    let insight_agent_platform::dsl::v3::ast::Step::Leaf(leaf) = &document.steps[0] else {
        panic!("expected llm leaf");
    };
    let llm = leaf.llm.as_ref().unwrap();
    assert!(!llm.stream);
    assert!(llm.publish);
    assert_eq!(llm.tools, ["lookup", "summarize"]);
    assert_eq!(
        llm.tool_choice,
        insight_agent_platform::dsl::v3::ast::LlmToolChoice::Tool("summarize".to_owned())
    );
    assert_eq!(
        (llm.tool_limits.max_rounds, llm.tool_limits.max_calls),
        (3, 32)
    );

    let plan = insight_agent_platform::dsl::v3::compile(document, options(source)).unwrap();
    let NodeKind::LlmTask(descriptor) = plan.nodes()[0].kind() else {
        panic!("expected LLM task");
    };
    assert_eq!(descriptor.descriptor_version.as_str(), "2");
    assert_eq!(
        descriptor.public_configuration.get("stream"),
        Some(&DescriptorValue::Boolean(false))
    );
    assert_eq!(
        descriptor.public_configuration.get("publish"),
        Some(&DescriptorValue::Boolean(true))
    );
    assert_eq!(
        descriptor.public_configuration.get("tools"),
        Some(&DescriptorValue::Array(vec![
            DescriptorValue::String("lookup".to_owned()),
            DescriptorValue::String("summarize".to_owned()),
        ]))
    );
    assert_eq!(
        descriptor.public_configuration.get("tool_choice"),
        Some(&DescriptorValue::String("summarize".to_owned()))
    );
    assert_eq!(
        descriptor.public_configuration.get("tool_limits"),
        Some(&DescriptorValue::Object(std::collections::BTreeMap::from(
            [
                ("max_calls".to_owned(), DescriptorValue::Integer(32)),
                ("max_rounds".to_owned(), DescriptorValue::Integer(3)),
            ]
        )))
    );
}

#[test]
fn llm_defaults_are_explicit_plan_semantics() {
    let source = r#"api_version: insight.agent/v3
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
      response: string
    - return: $answer
"#;
    let plan = compile_source(source, options(source)).unwrap();
    let NodeKind::LlmTask(descriptor) = plan.nodes()[0].kind() else {
        panic!("expected LLM task");
    };
    assert_eq!(descriptor.descriptor_version.as_str(), "2");
    assert_eq!(
        descriptor.public_configuration.get("stream"),
        Some(&DescriptorValue::Boolean(true))
    );
    assert_eq!(
        descriptor.public_configuration.get("publish"),
        Some(&DescriptorValue::Boolean(false))
    );
    assert_eq!(
        descriptor.public_configuration.get("tools"),
        Some(&DescriptorValue::Array(Vec::new()))
    );
    assert_eq!(
        descriptor.public_configuration.get("tool_choice"),
        Some(&DescriptorValue::String("auto".to_owned()))
    );
    assert_eq!(
        descriptor.public_configuration.get("tool_limits"),
        Some(&DescriptorValue::Object(std::collections::BTreeMap::from(
            [
                ("max_calls".to_owned(), DescriptorValue::Integer(32)),
                ("max_rounds".to_owned(), DescriptorValue::Integer(8)),
            ]
        )))
    );
}

#[test]
fn invalid_llm_control_contracts_fail_in_author_validation() {
    let base = r#"api_version: insight.agent/v3
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
      CONTROL
      response: string
    - return: $answer
"#;
    for control in [
        "parameters: {stream: true}",
        "stream: streaming",
        "publish: 1",
        "tools: lookup",
        "tools: [lookup, lookup]",
        "tools: [lookup]\n      tool_choice: missing",
        "tool_choice: required",
        "tool_limits: {max_rounds: 0}",
        "tool_limits: {max_calls: -1}",
        "tool_limits: {max_rounds: 1, future: 2}",
    ] {
        let source = base.replace("CONTROL", control);
        let error = validate(insight_agent_platform::dsl::v3::parse(&source).unwrap()).unwrap_err();
        assert_eq!(error.code(), INVALID_STEP, "control={control}");
    }
}

#[test]
fn constrained_defaults_are_validated_after_type_lowering() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs:
  language: {type: string, min_length: 1, default: ""}
output: string
workflow:
  steps:
    - return: fixed
"#;
    let error = compile_source(source, options(source)).unwrap_err();
    assert_eq!(error.code(), insight_agent_platform::dsl::v3::INVALID_TYPE);
    assert!(error.message().contains("fully constrained type"));
}

#[test]
fn validated_ast_recursively_preserves_natural_values_templates_and_error_refs() {
    let source = r#"api_version: insight.agent/v3
kind: agent
errors:
  rejected: {category: workflow, code: REJECTED, public_message: rejected}
inputs: {answer: string}
output: any
workflow:
  steps:
    - return:
        answer: $answer
        lines: [fixed, "{{ answer }}"]
"#;
    let document = validate(insight_agent_platform::dsl::v3::parse(source).unwrap()).unwrap();
    let insight_agent_platform::dsl::v3::ast::Step::Return(value) = &document.steps[0] else {
        panic!("expected return");
    };
    let insight_agent_platform::dsl::v3::ast::ValueExpr::Object(fields) = value else {
        panic!("natural mapping must remain a typed Object AST");
    };
    assert!(matches!(
        fields["answer"],
        insight_agent_platform::dsl::v3::ast::ValueExpr::Reference(_)
    ));
    assert!(matches!(
        fields["lines"],
        insight_agent_platform::dsl::v3::ast::ValueExpr::Array(_)
    ));

    let raised = source.replace(
        "- return:\n        answer: $answer\n        lines: [fixed, \"{{ answer }}\"]",
        "- raise: rejected",
    );
    let document = validate(insight_agent_platform::dsl::v3::parse(&raised).unwrap()).unwrap();
    assert!(matches!(
        document.steps[0],
        insight_agent_platform::dsl::v3::ast::Step::Raise(
            insight_agent_platform::dsl::v3::ast::ValueExpr::ErrorRef(_)
        )
    ));
}

#[test]
fn author_constraints_are_recursively_lowered_to_refined_plan_types() {
    let source = r#"api_version: insight.agent/v3
kind: agent
inputs:
  question: {type: string, min_length: 1}
output: string
workflow:
  steps:
    - return: $question
"#;
    let document = validate(insight_agent_platform::dsl::v3::parse(source).unwrap()).unwrap();
    assert_eq!(
        document.inputs["question"].value_type.constraints[&Vec::<String>::new()].min_length,
        Some(1)
    );
    let plan = compile_source(source, options(source)).unwrap();
    let insight_agent_platform::engine::plan::PlanType::Object { properties, .. } =
        plan.metadata().input_contract().accepted_type()
    else {
        panic!("workflow input must be an object");
    };
    assert_eq!(
        properties["question"].value_type.string_constraints(),
        Some((1, None, None, None))
    );
}

#[test]
fn nested_array_and_string_constraints_survive_the_plan_boundary() {
    let source = r#"api_version: insight.agent/v3
kind: agent
types:
  Code:
    type: string
    min_length: 1
    max_length: 1
    pattern: "^[AB]$"
    enum: [A, B]
inputs:
  values: {type: "Code[]", min_items: 1, max_items: 2}
output: string
workflow:
  steps:
    - return: done
"#;
    let plan = compile_source(source, options(source)).unwrap();
    let insight_agent_platform::engine::plan::PlanType::Object { properties, .. } =
        plan.metadata().input_contract().accepted_type()
    else {
        panic!("workflow input must be an object");
    };
    let (items, minimum, maximum) = properties["values"].value_type.array_constraints().unwrap();
    assert_eq!((minimum, maximum), (1, Some(2)));
    let (minimum, maximum, pattern, values) = items.string_constraints().unwrap();
    assert_eq!((minimum, maximum, pattern), (1, Some(1), Some("^[AB]$")));
    assert_eq!(
        values.unwrap(),
        [serde_json::json!("A"), serde_json::json!("B")]
    );
}

#[test]
fn legacy_control_and_child_result_are_not_in_the_positive_surface() {
    for source in [
        include_str!("fixtures/v3/negative-switch.yaml"),
        include_str!("fixtures/v3/negative-core.yaml"),
        include_str!("fixtures/v3/negative-child-result.yaml"),
    ] {
        let raw = insight_agent_platform::dsl::v3::parse(source).unwrap();
        assert_eq!(validate(raw).unwrap_err().code(), INVALID_STEP);
    }
}

#[test]
fn checked_in_agents_and_markdown_prompts_compile_through_v3() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("agents");
    for agent in [
        "action_demo",
        "medical_report_interpreter",
        "parallel_researcher",
        "researcher",
        "workflow_failure_demo",
    ] {
        let directory = root.join(agent);
        let source = std::fs::read_to_string(directory.join("agent.yaml")).unwrap();
        let mut options = CompileOptions::new(
            DefinitionRevisionId::new(format!("checked_in_{agent}")).unwrap(),
            format!("agents/{agent}/agent.yaml"),
            &source,
        );
        let prompts = directory.join("prompts");
        if prompts.exists() {
            let mut files = std::fs::read_dir(prompts)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>();
            files.sort();
            for path in files {
                if path.extension().and_then(|value| value.to_str()) == Some("md") {
                    let name = path.file_name().unwrap().to_string_lossy();
                    options = options.with_prompt_file(
                        format!("prompts/{name}"),
                        std::fs::read_to_string(&path).unwrap(),
                    );
                }
            }
        }
        compile_source(&source, options).unwrap_or_else(|error| {
            panic!("checked-in agent '{agent}' failed v3 compile: {error}")
        });
    }
}
