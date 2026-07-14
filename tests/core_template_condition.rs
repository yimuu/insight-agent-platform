use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use insight_agent_platform::{
    dsl::{
        compiled::{
            CompiledNode, ControlEdge, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules,
            NodeOutcome, NodeTransition,
        },
        compiler::{AgentCompiler, CompileContext, CompileLimits},
        EmitPolicy,
    },
    nodes::{
        condition::ConditionNode,
        registry::{NodeExecutor, NodeType, NodeTypeRegistry},
        template::TemplateNode,
    },
    outcome::EndOutcomeKind,
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{stop_pair, ExecutionControl, RunContext, RunMetadata},
};
use serde_json::{json, Value};
use tempfile::tempdir;

#[derive(Debug, Clone, Copy)]
struct TestEnd;

impl NodeType for TestEnd {
    fn kind(&self) -> &'static str {
        "core.end"
    }

    fn compile(
        &self,
        _node_id: &str,
        _config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, insight_agent_platform::dsl::CompileError> {
        Ok(NodeCompilation {
            body: Arc::new(()),
            edges: Vec::new(),
            references: BTreeSet::new(),
            control: NodeControl::End {
                outcome: EndOutcomeKind::Success,
            },
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Forbidden,
                allows_content_emit: false,
            },
        })
    }
}

fn run_context(input: Value) -> RunContext {
    RunContext::new(
        RunMetadata {
            run_id: "run_test".to_string(),
            request_id: "req_test".to_string(),
            agent_id: "agent_test".to_string(),
            agent_version: "sha256:test".to_string(),
            started_at: Utc::now(),
        },
        input,
    )
}

fn compiled_node(
    id: &str,
    kind: &str,
    emit: EmitPolicy,
    compilation: NodeCompilation,
) -> CompiledNode {
    CompiledNode {
        id: id.to_string(),
        kind: kind.to_string(),
        next: Some("done".to_string()),
        emit,
        timeout: Duration::from_secs(1),
        body: compilation.body,
        edges: compilation.edges,
        references: compilation.references,
        control: NodeControl::Ordinary,
    }
}

#[tokio::test]
async fn template_recursively_renders_json_without_html_escaping() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = TemplateNode
        .compile(
            "prepare",
            json!({
                "value": {
                    "text": "{{ input.text }}",
                    "nested": ["prefix {{ input.text }}", 2, true]
                }
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("prepare", "core.template", EmitPolicy::None, compilation);
    let context = run_context(json!({"text":"A&B"}))
        .with_templates(Arc::new(compile_context.into_templates()));
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    let outcome = TemplateNode
        .execute(&node, &context, &control)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        NodeOutcome {
            output: json!({
                "text": "A&B",
                "nested": ["prefix A&B", 2, true]
            }),
            transition: NodeTransition::Next,
        }
    );
}

#[tokio::test]
async fn string_template_emits_its_complete_rendered_content() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = TemplateNode
        .compile(
            "answer",
            json!({"value":"{{ input.text }}"}),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("answer", "core.template", EmitPolicy::Content, compilation);
    let context = run_context(json!({"text":"A&B"}))
        .with_templates(Arc::new(compile_context.into_templates()));
    let emitted = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&emitted);
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), move |content| {
        let captured = Arc::clone(&captured);
        async move {
            captured.lock().unwrap().push(content);
            Ok(())
        }
    });

    let outcome = TemplateNode
        .execute(&node, &context, &control)
        .await
        .unwrap();

    assert_eq!(outcome.output, json!("A&B"));
    assert_eq!(*emitted.lock().unwrap(), vec!["A&B"]);
}

#[test]
fn template_reference_extraction_ignores_inert_handlebars_syntax() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);

    let compilation = TemplateNode
        .compile(
            "render",
            json!({
                "value": {
                    "plain": "literal nodes.future.output.text",
                    "comment": "{{!-- nodes.future.output.text --}}visible",
                    "escaped": "\\{{ nodes.future.output.text }}",
                    "real": "{{ nodes.prepare.output.text }}",
                    "helper": "{{#if nodes.ready.output.flag}}{{ nodes.prepare.output.text }}{{/if}}"
                }
            }),
            &mut compile_context,
        )
        .unwrap();

    assert_eq!(
        compilation.references,
        BTreeSet::from(["prepare".to_string(), "ready".to_string()])
    );
}

#[test]
fn template_reference_extraction_rejects_non_canonical_nodes_paths() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);

    let error = TemplateNode
        .compile(
            "render",
            json!({"value": "{{ nodes.prepare }}"}),
            &mut compile_context,
        )
        .err()
        .expect("non-canonical nodes path must fail compilation");

    assert_eq!(error.code(), "TEMPLATE_REFERENCE_INVALID");
}

#[test]
fn condition_compiles_ordered_typed_edges() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let condition = ConditionNode
        .compile(
            "route",
            json!({
                "cases": [
                    {"when":"input.kind == \"a\"", "next":"a"},
                    {"when":"input.kind == \"b\"", "next":"b"}
                ],
                "default": "fallback"
            }),
            &mut compile_context,
        )
        .unwrap();

    assert_eq!(
        condition.edges,
        vec![
            ControlEdge::Conditional { target: "a".into() },
            ControlEdge::Conditional { target: "b".into() },
            ControlEdge::Conditional {
                target: "fallback".into(),
            },
        ]
    );
}

#[tokio::test]
async fn condition_selects_the_first_matching_case_then_falls_back_to_default() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ConditionNode
        .compile(
            "route",
            json!({
                "cases": [
                    {"when":"input.kind == \"medical\"", "next":"medical_answer"},
                    {"when":"true", "next":"fallback_case"}
                ],
                "default": "general_answer"
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("route", "core.condition", EmitPolicy::None, compilation);
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    let matched = ConditionNode
        .execute(&node, &run_context(json!({"kind":"medical"})), &control)
        .await
        .unwrap();
    let second = ConditionNode
        .execute(&node, &run_context(json!({"kind":"unknown"})), &control)
        .await
        .unwrap();

    assert_eq!(
        matched,
        NodeOutcome {
            output: json!({"matched_case":0, "next":"medical_answer"}),
            transition: NodeTransition::Goto("medical_answer".to_string()),
        }
    );
    assert_eq!(
        second,
        NodeOutcome {
            output: json!({"matched_case":1, "next":"fallback_case"}),
            transition: NodeTransition::Goto("fallback_case".to_string()),
        }
    );
}

#[tokio::test]
async fn condition_uses_default_when_no_case_matches() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ConditionNode
        .compile(
            "route",
            json!({
                "cases": [{"when":"input.kind == \"medical\"", "next":"medical_answer"}],
                "default": "general_answer"
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("route", "core.condition", EmitPolicy::None, compilation);
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    let outcome = ConditionNode
        .execute(&node, &run_context(json!({"kind":"unknown"})), &control)
        .await
        .unwrap();

    assert_eq!(
        outcome.output,
        json!({"matched_case":null, "next":"general_answer"})
    );
    assert_eq!(
        outcome.transition,
        NodeTransition::Goto("general_answer".to_string())
    );
}

#[tokio::test]
async fn condition_preserves_json_value_corpus() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ConditionNode
        .compile(
            "route",
            json!({
                "cases": [{
                    "when": "input.enabled && input.name == \"alpha\" && input.count == 3 && input.score > 1.5 && input.tags[1] == \"two\" && input.meta.region == \"apac\" && input.nullable == null",
                    "next": "done"
                }],
                "default": "fallback"
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("route", "core.condition", EmitPolicy::None, compilation);
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    let outcome = ConditionNode
        .execute(
            &node,
            &run_context(json!({
                "enabled": true,
                "name": "alpha",
                "count": 3,
                "score": 2.25,
                "tags": ["one", "two"],
                "meta": {"region": "apac"},
                "nullable": null
            })),
            &control,
        )
        .await
        .unwrap();

    assert_eq!(outcome.transition, NodeTransition::Goto("done".to_string()));
}

#[tokio::test]
async fn condition_rejects_non_bool_results_at_runtime() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = ConditionNode
        .compile(
            "route",
            json!({
                "cases": [{"when": "input.kind", "next": "done"}],
                "default": "fallback"
            }),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("route", "core.condition", EmitPolicy::None, compilation);
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    let error = ConditionNode
        .execute(&node, &run_context(json!({"kind": "alpha"})), &control)
        .await
        .unwrap_err();

    assert_eq!(error.code(), "CONDITION_RESULT_NOT_BOOL");
}

#[test]
fn condition_rejects_invalid_or_incomplete_configuration_at_compile_time() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);

    let invalid_expression = ConditionNode
        .compile(
            "route",
            json!({"cases":[{"when":"input.", "next":"done"}], "default":"done"}),
            &mut context,
        )
        .err()
        .expect("invalid CEL must fail compilation");
    assert_eq!(invalid_expression.code(), "CONDITION_EXPRESSION_INVALID");

    let no_cases = ConditionNode
        .compile("route", json!({"cases":[], "default":"done"}), &mut context)
        .err()
        .expect("an empty case list must fail compilation");
    assert_eq!(no_cases.code(), "CONDITION_CASES_REQUIRED");

    let no_default = ConditionNode
        .compile(
            "route",
            json!({"cases":[{"when":"true", "next":"done"}]}),
            &mut context,
        )
        .err()
        .expect("a missing default must fail compilation");
    assert_eq!(no_default.code(), "NODE_CONFIG_INVALID");
}

#[test]
fn condition_reference_extraction_ignores_cel_string_literals() {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);

    let compilation = ConditionNode
        .compile(
            "route",
            json!({
                "cases": [{
                    "when": "nodes.prepare.output.kind == 'ready' && 'nodes.future.output' == 'nodes.future.output' && size(nodes.prepare.output.items) >= 0",
                    "next": "done"
                }],
                "default": "done"
            }),
            &mut context,
        )
        .unwrap();

    assert_eq!(
        compilation.references,
        BTreeSet::from(["prepare".to_string()])
    );
}

#[test]
fn condition_rejects_non_canonical_nodes_access() {
    let cases = [
        "nodes[\"prepare\"].output == true",
        "nodes[id].output == true",
        "nodes.prepare[\"output\"] == true",
        "nodes.prepare == true",
        "nodes == {}",
    ];

    for expression in cases {
        let models = ModelRegistry::default();
        let actions = ActionRegistry::default();
        let mut context = CompileContext::new(&models, &actions);
        let error = ConditionNode
            .compile(
                "route",
                json!({
                    "cases": [{"when": expression, "next": "done"}],
                    "default": "done"
                }),
                &mut context,
            )
            .err()
            .expect("non-canonical nodes access must fail compilation");

        assert_eq!(
            error.code(),
            "CONDITION_REFERENCE_INVALID",
            "{expression} should be rejected"
        );
    }
}

#[test]
fn compiler_enforces_condition_and_template_envelope_rules() {
    let common = r#"
version: 1
id: envelope-test
name: Envelope Test
input:
  schema: {type: object}
entry: start
nodes:
"#;

    let condition_with_next = format!(
        "{common}  start:\n    type: core.condition\n    next: done\n    config:\n      cases:\n        - when: 'true'\n          next: done\n      default: done\n  done:\n    type: core.end\n    config: {{outcome: success, data: {{}}}}\n"
    );
    assert_compile_error(&condition_with_next, "NODE_NEXT_FORBIDDEN");

    let object_content = format!(
        "{common}  start:\n    type: core.template\n    next: done\n    emit: content\n    config:\n      value:\n        answer: '{{{{ input.answer }}}}'\n  done:\n    type: core.end\n    config: {{outcome: success, data: {{}}}}\n"
    );
    assert_compile_error(&object_content, "NODE_EMIT_UNSUPPORTED");
}

fn assert_compile_error(yaml: &str, expected_code: &str) {
    let directory = tempdir().unwrap();
    std::fs::write(directory.path().join("agent.yaml"), yaml).unwrap();
    let mut types = NodeTypeRegistry::default();
    types.register(TemplateNode).unwrap();
    types.register(ConditionNode).unwrap();
    types.register(TestEnd).unwrap();
    let compiler = AgentCompiler::new(
        types,
        ModelRegistry::default(),
        ActionRegistry::default(),
        Duration::from_secs(1),
        CompileLimits {
            max_fork_branches: 32,
        },
    );

    let error = compiler.compile_dir(directory.path()).unwrap_err();
    assert_eq!(error.code(), expected_code, "{error}");
}
