use std::{sync::Arc, time::Duration};

use insight_agent_platform::{
    dsl::{
        compiled::{CompiledNode, NodeControl, NodeOutcome, NodeTransition},
        compiler::CompileContext,
        EmitPolicy,
    },
    nodes::{
        default_node_registries,
        end::EndNode,
        registry::{NodeExecutor, NodeType},
    },
    outcome::{RunOutput, TerminalOutcome, WorkflowError},
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{stop_pair, ExecutionControl, RunContext, RunError, RunMetadata},
};
use serde_json::{json, Value};

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

async fn execute_end(config: Value, input: Value) -> Result<NodeOutcome, RunError> {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = EndNode
        .compile("result", config, &mut compile_context)
        .unwrap();
    let node = CompiledNode {
        id: "result".to_string(),
        kind: "core.end".to_string(),
        next: None,
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: compilation.body,
        edges: compilation.edges,
        references: compilation.references,
        control: NodeControl::End {
            outcome: match compilation.control {
                NodeControl::End { outcome } => outcome,
                _ => panic!("end compilation must declare end control"),
            },
        },
    };
    let context = context(input).with_templates(Arc::new(compile_context.into_templates()));
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    EndNode.execute(&node, &context, &control).await
}

#[tokio::test]
async fn end_supports_content_only_success() {
    let outcome = execute_end(
        json!({
            "outcome":"success",
            "content":{"template":"{{ input.answer }}"},
            "format":"text"
        }),
        json!({"answer":"plain"}),
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.transition,
        NodeTransition::End(TerminalOutcome::Success {
            output: RunOutput {
                content: Some("plain".into()),
                format: Some("text".into()),
                data: Value::Null,
            },
        })
    );
}

#[tokio::test]
async fn end_supports_recursive_data_only_success() {
    let outcome = execute_end(
        json!({
            "outcome":"success",
            "data":{"answer":"{{ input.answer }}", "count":2}
        }),
        json!({"answer":"structured"}),
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.transition,
        NodeTransition::End(TerminalOutcome::Success {
            output: RunOutput {
                content: None,
                format: None,
                data: json!({"answer":"structured", "count":2}),
            },
        })
    );
}

#[tokio::test]
async fn end_success_returns_a_typed_terminal_outcome() {
    let outcome = execute_end(
        json!({
            "outcome":"success",
            "content":{"template":"{{ input.answer }}"},
            "format":"text",
            "data":{"answer":"{{ input.answer }}"}
        }),
        json!({"answer":"done"}),
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.transition,
        NodeTransition::End(TerminalOutcome::Success {
            output: RunOutput {
                content: Some("done".into()),
                format: Some("text".into()),
                data: json!({"answer":"done"}),
            },
        })
    );
    assert_eq!(
        outcome.output,
        json!({
            "outcome":"success",
            "output":{"content":"done","format":"text","data":{"answer":"done"}}
        })
    );
}

#[tokio::test]
async fn end_failure_is_a_successfully_executed_workflow_outcome() {
    let outcome = execute_end(
        json!({
            "outcome":"failure",
            "code":"WORKFLOW_ALL_BRANCHES_FAILED",
            "message":"all parallel branches failed"
        }),
        json!({"secret":"must-not-be-rendered"}),
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.transition,
        NodeTransition::End(TerminalOutcome::Failure {
            error: WorkflowError {
                code: "WORKFLOW_ALL_BRANCHES_FAILED".into(),
                message: "all parallel branches failed".into(),
            },
        })
    );
    assert_eq!(outcome.output["outcome"], "failure");
    assert_eq!(outcome.output["error"]["kind"], "workflow");
}

#[test]
fn end_rejects_mixed_invalid_and_dynamic_failure_contracts() {
    assert_end_compile_error(json!({"outcome":"success"}), "END_VALUE_REQUIRED");
    assert_end_compile_error(
        json!({"outcome":"success","content":{"template":"answer"}}),
        "END_FORMAT_REQUIRED",
    );
    assert_end_compile_error(
        json!({"outcome":"success","format":"text","data":{"ok":true}}),
        "END_FORMAT_WITHOUT_CONTENT",
    );
    assert_end_compile_error(
        json!({"outcome":"failure","code":"RUN_TIMEOUT","message":"x"}),
        "END_FAILURE_CODE_INVALID",
    );
    assert_end_compile_error(
        json!({"outcome":"failure","code":"WORKFLOW_X","message":"line 1\nline 2"}),
        "END_FAILURE_MESSAGE_INVALID",
    );
    for message in [
        "   ".to_string(),
        "bad\u{0000}message".to_string(),
        "x".repeat(257),
    ] {
        assert_end_compile_error(
            json!({"outcome":"failure","code":"WORKFLOW_X","message":message}),
            "END_FAILURE_MESSAGE_INVALID",
        );
    }
    assert_end_compile_error(
        json!({
            "outcome":"failure",
            "code":"WORKFLOW_X",
            "message":"{{ input.reason }}"
        }),
        "END_FAILURE_MESSAGE_INVALID",
    );
    assert_end_compile_error(
        json!({
            "outcome":"failure",
            "code":format!("WORKFLOW_{}", "X".repeat(56)),
            "message":"x"
        }),
        "END_FAILURE_CODE_INVALID",
    );
    assert_end_compile_error(
        json!({
            "outcome":"failure",
            "code":"WORKFLOW_X",
            "message":"x",
            "data":{"not":"allowed"}
        }),
        "NODE_CONFIG_INVALID",
    );
}

fn assert_end_compile_error(config: Value, expected_code: &str) {
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    let error = EndNode
        .compile("result", config, &mut context)
        .err()
        .expect("end compilation must fail");
    assert_eq!(error.code(), expected_code, "{error}");
}

#[test]
fn default_registries_contain_all_formal_core_nodes() {
    let (types, executors) = default_node_registries().unwrap();
    let expected = vec![
        "core.action",
        "core.chat",
        "core.condition",
        "core.end",
        "core.fork",
        "core.join",
        "core.select",
        "core.template",
    ];

    assert_eq!(types.kinds(), expected);
    assert_eq!(executors.kinds(), expected);
}
