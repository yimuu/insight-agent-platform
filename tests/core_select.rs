use std::{collections::BTreeSet, time::Duration};

use chrono::Utc;
use insight_agent_platform::{
    dsl::{
        compiled::{CompiledNode, NextPolicy, NodeControl, NodeOutcome, NodeTransition},
        compiler::CompileContext,
        EmitPolicy,
    },
    nodes::default_node_registries,
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{stop_pair, ExecutionControl, RunContext, RunError, RunMetadata, StopReason},
};
use serde_json::{json, Value};

fn compile_select(
    node_id: &str,
    config: Value,
) -> Result<
    insight_agent_platform::dsl::compiled::NodeCompilation,
    insight_agent_platform::dsl::CompileError,
> {
    let (types, _) = default_node_registries().unwrap();
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    types
        .resolve("core.select")?
        .compile(node_id, config, &mut context)
}

fn compiled_node(
    compilation: insight_agent_platform::dsl::compiled::NodeCompilation,
) -> CompiledNode {
    CompiledNode {
        id: "selected".to_string(),
        kind: "core.select".to_string(),
        next: Some("result".to_string()),
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: compilation.body,
        edges: compilation.edges,
        references: compilation.references,
        terminal: compilation.terminal,
        control: compilation.control,
    }
}

fn context(outputs: impl IntoIterator<Item = (&'static str, Value)>) -> RunContext {
    let mut context = RunContext::new(
        RunMetadata {
            run_id: "run_select".to_string(),
            request_id: "req_select".to_string(),
            agent_id: "select_agent".to_string(),
            agent_version: "sha256:select".to_string(),
            started_at: Utc::now(),
        },
        json!({}),
    );
    for (node_id, output) in outputs {
        context.set_node_output(node_id, output);
    }
    context
}

fn control() -> ExecutionControl {
    let (_, signal) = stop_pair();
    ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) })
}

async fn execute(
    outputs: impl IntoIterator<Item = (&'static str, Value)>,
) -> Result<NodeOutcome, RunError> {
    let compilation =
        compile_select("selected", json!({"sources":["medical", "general"]})).unwrap();
    let node = compiled_node(compilation);
    let (_, executors) = default_node_registries().unwrap();
    executors
        .resolve("core.select")
        .unwrap()
        .execute(&node, &context(outputs), &control())
        .await
}

#[test]
fn select_compiles_to_a_typed_ordinary_successor_contract() {
    let compilation =
        compile_select("selected", json!({"sources":["medical", "general"]})).unwrap();

    assert_eq!(compilation.envelope.next, NextPolicy::Required);
    assert!(!compilation.envelope.allows_content_emit);
    assert!(compilation.edges.is_empty());
    assert!(compilation.references.is_empty());
    assert!(!compilation.terminal);
    assert_eq!(
        compilation.control,
        NodeControl::Select {
            sources: BTreeSet::from(["general".to_string(), "medical".to_string()]),
        }
    );
}

#[test]
fn select_rejects_invalid_local_contracts_with_stable_codes() {
    let cases = [
        (json!({}), "NODE_CONFIG_INVALID"),
        (json!({"sources":[], "extra":true}), "NODE_CONFIG_INVALID"),
        (json!({"sources":[]}), "SELECT_SOURCE_COUNT_INVALID"),
        (
            json!({"sources":["medical"]}),
            "SELECT_SOURCE_COUNT_INVALID",
        ),
        (
            json!({"sources":["medical", "medical"]}),
            "SELECT_SOURCE_DUPLICATE",
        ),
        (
            json!({"sources":["medical", "bad-id"]}),
            "SELECT_SOURCE_ID_INVALID",
        ),
        (
            json!({"sources":["selected", "medical"]}),
            "SELECT_SOURCE_ID_INVALID",
        ),
    ];

    for (config, expected) in cases {
        let error = compile_select("selected", config)
            .err()
            .expect("invalid Select config must fail");
        assert_eq!(error.code(), expected, "unexpected error: {error}");
    }
}

#[tokio::test]
async fn select_returns_the_only_visible_source_without_coercion() {
    assert_eq!(
        execute([("medical", json!({"text":"answer"}))])
            .await
            .unwrap(),
        NodeOutcome {
            output: json!({
                "source_node_id": "medical",
                "value": {"text":"answer"},
            }),
            transition: NodeTransition::Next,
        }
    );
}

#[tokio::test]
async fn select_treats_an_executed_json_null_as_present() {
    assert_eq!(
        execute([("general", Value::Null)]).await.unwrap().output,
        json!({"source_node_id":"general", "value":null})
    );
}

#[tokio::test]
async fn select_rejects_zero_and_multiple_visible_sources_without_output_bodies() {
    let missing = execute([]).await.unwrap_err();
    assert_eq!(missing.code(), "SELECT_SOURCE_MISSING");
    assert_eq!(
        missing.message(),
        "select node 'selected' has no completed source"
    );

    let ambiguous = execute([
        ("medical", json!({"secret":"medical-secret"})),
        ("general", json!({"secret":"general-secret"})),
    ])
    .await
    .unwrap_err();
    assert_eq!(ambiguous.code(), "SELECT_SOURCE_AMBIGUOUS");
    assert_eq!(
        ambiguous.message(),
        "select node 'selected' has multiple completed sources: general, medical"
    );
    assert!(!ambiguous.message().contains("medical-secret"));
    assert!(!ambiguous.message().contains("general-secret"));
}

#[tokio::test]
async fn select_preserves_the_authoritative_stop_reason() {
    let compilation =
        compile_select("selected", json!({"sources":["medical", "general"]})).unwrap();
    let node = compiled_node(compilation);
    let (controller, signal) = stop_pair();
    assert!(controller.request(StopReason::Cancelled));
    let control = ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) });
    let (_, executors) = default_node_registries().unwrap();

    let error = executors
        .resolve("core.select")
        .unwrap()
        .execute(&node, &context([]), &control)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "RUN_CANCELLED");
    assert_eq!(error.stop_reason(), Some(StopReason::Cancelled));
}
