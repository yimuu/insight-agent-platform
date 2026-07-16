use std::{sync::Arc, time::Duration};

use insight_agent_platform::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeControl, NodeOutcome, NodeTransition,
        },
        compiler::CompileContext,
        EmitPolicy,
    },
    nodes::{
        default_node_registries,
        end::{BranchEndNode, EndNode},
        registry::{NodeExecutor, NodeType},
    },
    outcome::{EndOutcomeKind, RunOutput, TerminalOutcome, WorkflowError},
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
    execute_terminal(EndNode, config, input).await
}

async fn execute_terminal<T>(
    terminal: T,
    config: Value,
    input: Value,
) -> Result<NodeOutcome, RunError>
where
    T: NodeType + NodeExecutor,
{
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = terminal
        .compile("result", config, &mut compile_context)
        .unwrap();
    let node = CompiledNode {
        id: "result".to_string(),
        kind: terminal.kind().to_string(),
        next: None,
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: compilation.body,
        edges: compilation.edges,
        references: compilation.references,
        control: compilation.control,
    };
    let context = context(input).with_templates(Arc::new(compile_context.into_templates()));
    let (_, signal) = stop_pair();
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });

    terminal.execute(&node, &context, &control).await
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

#[tokio::test]
async fn end_and_branch_end_share_success_execution_contract() {
    let config = json!({
        "outcome":"success",
        "content":{"template":"{{ input.answer }}"},
        "format":"markdown",
        "data":{"answer":"{{ input.answer }}"}
    });
    let input = json!({"answer":"shared"});

    let run_end = execute_terminal(EndNode, config.clone(), input.clone())
        .await
        .unwrap();
    let branch_end = execute_terminal(BranchEndNode, config, input)
        .await
        .unwrap();

    assert_eq!(branch_end, run_end);
}

#[tokio::test]
async fn end_and_branch_end_share_failure_execution_contract() {
    let config = json!({
        "outcome":"failure",
        "code":"WORKFLOW_BRANCH_REJECTED",
        "message":"branch rejected"
    });

    let run_end = execute_terminal(EndNode, config.clone(), json!({}))
        .await
        .unwrap();
    let branch_end = execute_terminal(BranchEndNode, config, json!({}))
        .await
        .unwrap();

    assert_eq!(branch_end, run_end);
}

#[test]
fn end_and_branch_end_compile_distinct_controls_with_equal_envelopes() {
    let run_end = compile_terminal(EndNode, json!({"outcome":"success","data":{"ok":true}}));
    let branch_end = compile_terminal(
        BranchEndNode,
        json!({"outcome":"success","data":{"ok":true}}),
    );

    assert_eq!(
        run_end.control,
        NodeControl::End {
            outcome: EndOutcomeKind::Success
        }
    );
    assert_eq!(
        branch_end.control,
        NodeControl::BranchEnd {
            outcome: EndOutcomeKind::Success
        }
    );
    assert_eq!(run_end.envelope, branch_end.envelope);
    assert_eq!(run_end.envelope.next, NextPolicy::Forbidden);
    assert!(!run_end.envelope.allows_content_emit);
    assert!(run_end.edges.is_empty());
    assert!(branch_end.edges.is_empty());
}

#[test]
fn terminal_config_errors_name_the_authored_node_kind() {
    let config = json!({"outcome":"success","data":{},"unexpected":true});
    let run_error = terminal_compile_error(EndNode, config.clone());
    let branch_error = terminal_compile_error(BranchEndNode, config);

    assert_eq!(run_error.code(), "NODE_CONFIG_INVALID");
    assert_eq!(branch_error.code(), "NODE_CONFIG_INVALID");
    assert!(run_error.to_string().contains("invalid core.end config"));
    assert!(branch_error
        .to_string()
        .contains("invalid core.branch_end config"));
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
    let error = terminal_compile_error(EndNode, config);
    assert_eq!(error.code(), expected_code, "{error}");
}

fn compile_terminal<T>(terminal: T, config: Value) -> NodeCompilation
where
    T: NodeType,
{
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    terminal.compile("result", config, &mut context).unwrap()
}

fn terminal_compile_error<T>(
    terminal: T,
    config: Value,
) -> insight_agent_platform::dsl::CompileError
where
    T: NodeType,
{
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    terminal
        .compile("result", config, &mut context)
        .err()
        .expect("terminal compilation must fail")
}

#[test]
fn default_registries_contain_all_formal_core_nodes() {
    let (types, executors) = default_node_registries().unwrap();
    let expected = vec![
        "core.action",
        "core.branch_end",
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
