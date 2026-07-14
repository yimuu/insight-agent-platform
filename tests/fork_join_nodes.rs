use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use chrono::Utc;
use insight_agent_platform::{
    dsl::{
        compiled::{
            CompiledNode, ControlEdge, JoinPolicy, NextPolicy, NodeCompilation, NodeControl,
            NodeTransition,
        },
        compiler::CompileContext,
        EmitPolicy,
    },
    nodes::default_node_registries,
    resources::{actions::ActionRegistry, models::ModelRegistry},
    runtime::{stop_pair, BranchError, BranchResult, ExecutionControl, RunContext, RunMetadata},
};
use serde_json::{json, Value};

fn test_context() -> RunContext {
    RunContext::new(
        RunMetadata {
            run_id: "run_test".to_string(),
            request_id: "req_test".to_string(),
            agent_id: "agent_test".to_string(),
            agent_version: "sha256:test".to_string(),
            started_at: Utc::now(),
        },
        json!({"question":"hello"}),
    )
}

fn test_control() -> ExecutionControl {
    let (_, signal) = stop_pair();
    ExecutionControl::new(signal, Duration::from_secs(1), |_| async { Ok(()) })
}

fn compiled_node(
    id: &str,
    kind: &str,
    next: Option<&str>,
    compilation: NodeCompilation,
) -> CompiledNode {
    CompiledNode {
        id: id.to_string(),
        kind: kind.to_string(),
        next: next.map(str::to_string),
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: compilation.body,
        edges: compilation.edges,
        references: compilation.references,
        control: compilation.control,
    }
}

fn branch_results() -> BTreeMap<String, BranchResult> {
    BTreeMap::from([
        (
            "source_a".to_string(),
            BranchResult::Succeeded {
                terminal_node_id: "summarize_a".to_string(),
                output: json!({"text":"result a"}),
            },
        ),
        (
            "source_b".to_string(),
            BranchResult::Failed {
                terminal_node_id: "search_b".to_string(),
                error: BranchError {
                    code: "UPSTREAM_FAILURE".to_string(),
                    message: "upstream service failed".to_string(),
                },
            },
        ),
    ])
}

#[test]
fn fork_and_join_compile_to_typed_controls() {
    let (types, _) = default_node_registries().unwrap();
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);

    let fork = types
        .resolve("core.fork")
        .unwrap()
        .compile(
            "fanout",
            json!({
                "branches": {"source_b": "search_b", "source_a": "search_a"},
                "join": "collect"
            }),
            &mut context,
        )
        .unwrap();
    assert_eq!(fork.envelope.next, NextPolicy::Forbidden);
    assert!(!fork.envelope.allows_content_emit);
    assert_eq!(
        fork.edges,
        vec![
            ControlEdge::ForkBranch {
                branch_id: "source_a".into(),
                target: "search_a".into(),
            },
            ControlEdge::ForkBranch {
                branch_id: "source_b".into(),
                target: "search_b".into(),
            },
            ControlEdge::ForkContinuation {
                target: "collect".into(),
            },
        ]
    );
    assert_eq!(fork.references, BTreeSet::new());
    assert_eq!(
        fork.control,
        NodeControl::Fork {
            branches: BTreeMap::from([
                ("source_a".into(), "search_a".into()),
                ("source_b".into(), "search_b".into()),
            ]),
            join: "collect".into(),
        }
    );

    let join = types
        .resolve("core.join")
        .unwrap()
        .compile("collect", json!({"mode":"all_settled"}), &mut context)
        .unwrap();
    assert_eq!(join.envelope.next, NextPolicy::Required);
    assert!(!join.envelope.allows_content_emit);
    assert_eq!(join.edges, Vec::<ControlEdge>::new());
    assert_eq!(join.references, BTreeSet::new());
    assert_eq!(
        join.control,
        NodeControl::Join {
            policy: JoinPolicy::AllSettled
        }
    );
}

#[tokio::test]
async fn fork_executor_activates_sorted_branches() {
    let (types, executors) = default_node_registries().unwrap();
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);
    let compilation = types
        .resolve("core.fork")
        .unwrap()
        .compile(
            "fanout",
            json!({
                "branches": {"source_b": "search_b", "source_a": "search_a"},
                "join": "collect"
            }),
            &mut context,
        )
        .unwrap();
    let node = compiled_node("fanout", "core.fork", None, compilation);

    let outcome = executors
        .resolve("core.fork")
        .unwrap()
        .execute(&node, &test_context(), &test_control())
        .await
        .unwrap();

    assert_eq!(
        outcome.output,
        json!({"branches":["source_a", "source_b"], "join":"collect"})
    );
    assert_eq!(outcome.transition, NodeTransition::ActivateFork);
}

#[tokio::test]
async fn join_serializes_the_stable_all_settled_envelope() {
    let outcome = execute_join(branch_results()).await.unwrap();

    assert_eq!(outcome.transition, NodeTransition::Next);
    assert_eq!(
        outcome.output,
        json!({
            "branches": {
                "source_a": {
                    "status": "succeeded",
                    "terminal_node_id": "summarize_a",
                    "output": {"text": "result a"}
                },
                "source_b": {
                    "status": "failed",
                    "terminal_node_id": "search_b",
                    "error": {
                        "code": "UPSTREAM_FAILURE",
                        "message": "upstream service failed"
                    }
                }
            },
            "summary": {"total": 2, "succeeded": 1, "failed": 1}
        })
    );
    let branch_keys = outcome.output["branches"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(branch_keys, vec!["source_a", "source_b"]);
}

#[tokio::test]
async fn join_advances_when_all_branches_failed() {
    let results = BTreeMap::from([
        (
            "source_a".to_string(),
            BranchResult::Failed {
                terminal_node_id: "search_a".to_string(),
                error: BranchError {
                    code: "FAILED_A".to_string(),
                    message: "source a failed".to_string(),
                },
            },
        ),
        (
            "source_b".to_string(),
            BranchResult::Failed {
                terminal_node_id: "search_b".to_string(),
                error: BranchError {
                    code: "FAILED_B".to_string(),
                    message: "source b failed".to_string(),
                },
            },
        ),
    ]);

    let outcome = execute_join(results).await.unwrap();

    assert_eq!(outcome.transition, NodeTransition::Next);
    assert_eq!(
        outcome.output["summary"],
        json!({"total": 2, "succeeded": 0, "failed": 2})
    );
}

#[tokio::test]
async fn join_rejects_execution_without_scheduler_results() {
    let error = execute_compiled_join(&test_context()).await.unwrap_err();

    assert_eq!(error.code(), "JOIN_RESULTS_MISSING");
    assert_eq!(error.message(), "join node requires settled branch results");
}

#[test]
fn fork_rejects_invalid_branch_contracts() {
    let invalid = [
        json!({"branches":{"only":"target"}, "join":"collect"}),
        json!({"branches":{"9source":"a", "source_b":"b"}, "join":"collect"}),
        json!({"branches":{"source.a":"a", "source_b":"b"}, "join":"collect"}),
        json!({"branches":{"source a":"a", "source_b":"b"}, "join":"collect"}),
        json!({"branches":{"source_a":"", "source_b":"b"}, "join":"collect"}),
        json!({"branches":{"source_a":"   ", "source_b":"b"}, "join":"collect"}),
        json!({"branches":{"source_a":"a", "source_b":"b"}}),
        json!({"branches":{"source_a":"a", "source_b":"b"}, "join":""}),
        json!({"branches":{"source_a":"a", "source_b":"b"}, "join":"collect", "extra":true}),
    ];

    for config in invalid {
        assert_compile_error("core.fork", config);
    }
}

#[test]
fn join_rejects_unknown_modes_and_config_fields() {
    assert_compile_error("core.join", json!({"mode":"fail_fast"}));
    assert_compile_error("core.join", json!({"mode":"all_settled", "extra":true}));
}

#[test]
fn branch_contexts_freeze_visible_outputs_and_isolate_siblings() {
    let mut main = test_context();
    main.set_node_output("prepare", json!({"query":"rust"}));
    let mut source_a = main.fork_branch();
    let mut source_b = main.fork_branch();
    source_a.set_node_output("search_a", json!({"text":"a"}));
    source_b.set_node_output("search_b", json!({"text":"b"}));

    assert!(source_a.node_output("search_b").is_none());
    assert!(source_b.node_output("search_a").is_none());
    assert_eq!(
        source_a.node_output("prepare"),
        Some(&json!({"query":"rust"}))
    );
    assert_eq!(
        source_a.template_data()["nodes"]["search_a"]["output"]["text"],
        "a"
    );
    assert!(source_a.template_data()["nodes"].get("search_b").is_none());

    main.set_node_output("late", json!({"visible":false}));
    assert!(source_a.node_output("late").is_none());
    assert!(source_b.node_output("late").is_none());
}

#[test]
fn join_context_freezes_outputs_and_exposes_immutable_results() {
    let mut main = test_context();
    main.set_node_output("prepare", json!({"query":"rust"}));
    let join = main.with_join_results(branch_results());
    main.set_node_output("late", json!({"visible":false}));

    assert_eq!(join.node_output("prepare"), Some(&json!({"query":"rust"})));
    assert!(join.node_output("late").is_none());
    assert_eq!(join.branch_results(), Some(&branch_results()));
    assert!(test_context().branch_results().is_none());
}

async fn execute_join(
    results: BTreeMap<String, BranchResult>,
) -> Result<
    insight_agent_platform::dsl::compiled::NodeOutcome,
    insight_agent_platform::runtime::RunError,
> {
    execute_compiled_join(&test_context().with_join_results(results)).await
}

async fn execute_compiled_join(
    context: &RunContext,
) -> Result<
    insight_agent_platform::dsl::compiled::NodeOutcome,
    insight_agent_platform::runtime::RunError,
> {
    let (types, executors) = default_node_registries().unwrap();
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut compile_context = CompileContext::new(&models, &actions);
    let compilation = types
        .resolve("core.join")
        .unwrap()
        .compile(
            "collect",
            json!({"mode":"all_settled"}),
            &mut compile_context,
        )
        .unwrap();
    let node = compiled_node("collect", "core.join", Some("result"), compilation);

    executors
        .resolve("core.join")
        .unwrap()
        .execute(&node, context, &test_control())
        .await
}

fn assert_compile_error(kind: &str, config: Value) {
    let (types, _) = default_node_registries().unwrap();
    let models = ModelRegistry::default();
    let actions = ActionRegistry::default();
    let mut context = CompileContext::new(&models, &actions);

    assert!(
        types
            .resolve(kind)
            .unwrap()
            .compile("invalid", config, &mut context)
            .is_err(),
        "{kind} accepted an invalid config"
    );
}
