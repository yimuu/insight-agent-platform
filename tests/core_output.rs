use std::{sync::Arc, time::Duration};

use insight_agent_platform::{
    dsl::{
        compiled::{CompiledNode, NodeControl, NodeOutcome, NodeTransition, RunOutput},
        compiler::{AgentCompiler, CompileContext, CompileLimits},
        EmitPolicy,
    },
    nodes::{
        default_node_registries,
        output::OutputNode,
        registry::{NodeExecutor, NodeType},
    },
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{stop_pair, ExecutionControl, RunContext, RunMetadata},
};
use serde_json::{json, Value};
use tempfile::tempdir;

fn context(input: Value) -> RunContext {
    RunContext::new(
        RunMetadata {
            run_id: "run_test".to_string(),
            request_id: "req_test".to_string(),
            agent_id: "agent_test".to_string(),
            agent_version: "sha256:test".to_string(),
            started_at: chrono::Utc::now(),
        },
        input,
    )
}

async fn execute_output(config: Value, input: Value) -> RunOutput {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = OutputNode
        .compile("result", config, &mut compile_context)
        .unwrap();
    let node = CompiledNode {
        id: "result".to_string(),
        kind: "core.output".to_string(),
        next: None,
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: compilation.body,
        edges: compilation.edges,
        references: compilation.references,
        terminal: compilation.terminal,
        control: NodeControl::Ordinary,
    };
    let context = context(input).with_templates(Arc::new(compile_context.into_templates()));
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    let outcome = OutputNode.execute(&node, &context, &control).await.unwrap();
    let NodeOutcome {
        transition: NodeTransition::Complete(output),
        output: diagnostic_output,
    } = outcome
    else {
        panic!("output node must complete the run")
    };
    assert_eq!(
        diagnostic_output,
        json!({
            "content": output.content,
            "format": output.format,
            "data": output.data,
        })
    );
    output
}

#[tokio::test]
async fn output_supports_content_only_data_only_and_combined_results() {
    let content_only = execute_output(
        json!({
            "content":{"template":"{{ input.answer }}"},
            "format":"text"
        }),
        json!({"answer":"plain"}),
    )
    .await;
    assert_eq!(content_only.content.as_deref(), Some("plain"));
    assert_eq!(content_only.format.as_deref(), Some("text"));
    assert_eq!(content_only.data, Value::Null);

    let data_only = execute_output(
        json!({"data":{"answer":"{{ input.answer }}", "count":2}}),
        json!({"answer":"structured"}),
    )
    .await;
    assert_eq!(data_only.content, None);
    assert_eq!(data_only.format, None);
    assert_eq!(data_only.data, json!({"answer":"structured", "count":2}));

    let combined = execute_output(
        json!({
            "content":{"template":"{{ input.answer }}"},
            "format":"markdown",
            "data":{"answer":"{{ input.answer }}"}
        }),
        json!({"answer":"answer"}),
    )
    .await;
    assert_eq!(combined.content.as_deref(), Some("answer"));
    assert_eq!(combined.format.as_deref(), Some("markdown"));
    assert_eq!(combined.data, json!({"answer":"answer"}));
}

#[test]
fn output_rejects_invalid_result_combinations() {
    assert_output_compile_error(json!({}), "OUTPUT_VALUE_REQUIRED");
    assert_output_compile_error(
        json!({"content":{"template":"answer"}}),
        "OUTPUT_FORMAT_REQUIRED",
    );
    assert_output_compile_error(
        json!({"content":{"template":"answer"}, "format":"html"}),
        "NODE_CONFIG_INVALID",
    );
    assert_output_compile_error(
        json!({"format":"text", "data":{"ok":true}}),
        "OUTPUT_FORMAT_WITHOUT_CONTENT",
    );
}

fn assert_output_compile_error(config: Value, expected_code: &str) {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    let error = OutputNode
        .compile("result", config, &mut context)
        .err()
        .expect("output compilation must fail");
    assert_eq!(error.code(), expected_code, "{error}");
}

#[test]
fn agent_compiler_rejects_next_and_content_emit_on_output() {
    let common = r#"
version: 1
id: output-envelope
name: Output Envelope
input:
  schema: {type: object}
entry: result
nodes:
  result:
    type: core.output
"#;
    assert_agent_compile_error(
        &format!("{common}    next: result\n    config:\n      data: {{ok: true}}\n"),
        "NODE_NEXT_FORBIDDEN",
    );
    assert_agent_compile_error(
        &format!(
            "{common}    emit: content\n    config:\n      content: {{template: answer}}\n      format: text\n"
        ),
        "NODE_EMIT_UNSUPPORTED",
    );
}

fn assert_agent_compile_error(yaml: &str, expected_code: &str) {
    let directory = tempdir().unwrap();
    std::fs::write(directory.path().join("agent.yaml"), yaml).unwrap();
    let (types, _) = default_node_registries().unwrap();
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

#[test]
fn default_registries_contain_exactly_the_five_formal_core_nodes() {
    let (types, executors) = default_node_registries().unwrap();
    let expected = vec![
        "core.action",
        "core.chat",
        "core.condition",
        "core.output",
        "core.template",
    ];

    assert_eq!(types.kinds(), expected);
    assert_eq!(executors.kinds(), expected);
}
