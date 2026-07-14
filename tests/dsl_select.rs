use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::Arc,
    time::Duration,
};

use insight_agent_platform::{
    dsl::{
        compiled::{CompiledNode, ControlEdge, ExecutionPlan, NodeControl, NodeRegion},
        compiler::{AgentCompiler, CompileLimits},
        graph::validate_graph,
        EmitPolicy,
    },
    nodes::default_node_registries,
    resources::{actions::ActionRegistry, models::ModelRegistry},
};
use tempfile::TempDir;

fn compiler() -> AgentCompiler {
    let (types, _) = default_node_registries().unwrap();
    AgentCompiler::new(
        types,
        ModelRegistry::default(),
        ActionRegistry::default(),
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 8,
        },
    )
}

fn write_agent(yaml: &str) -> (TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("agent");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("agent.yaml"), yaml).unwrap();
    (temp, root)
}

fn compile(yaml: &str) -> insight_agent_platform::dsl::compiled::CompiledAgent {
    let (_temp, root) = write_agent(yaml);
    compiler().compile_dir(Path::new(&root)).unwrap()
}

fn assert_compile_error(yaml: &str, expected: &'static str) {
    let (_temp, root) = write_agent(yaml);
    let error = compiler().compile_dir(Path::new(&root)).unwrap_err();
    assert_eq!(error.code(), expected, "unexpected error: {error}");
}

fn topology_node(id: &str, control: NodeControl, edges: Vec<ControlEdge>) -> CompiledNode {
    CompiledNode {
        id: id.to_string(),
        kind: "test.topology".to_string(),
        next: None,
        emit: EmitPolicy::None,
        timeout: Duration::from_secs(1),
        body: Arc::new(()),
        edges,
        references: BTreeSet::new(),
        control,
    }
}

fn select_yaml() -> &'static str {
    r#"
version: 1
id: select-agent
name: Select Agent
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: medical}]
      default: general
  medical:
    type: core.template
    next: selected
    config: {value: {text: medical}}
  general:
    type: core.template
    next: selected
    config: {value: {text: general}}
  selected:
    type: core.select
    next: result
    config: {sources: [medical, general]}
  result:
    type: core.end
    config:
      outcome: success
      data:
        source: "{{ nodes.selected.output.source_node_id }}"
        text: "{{ nodes.selected.output.value.text }}"
"#
}

#[test]
fn compiles_condition_convergence_and_dominating_select_references() {
    let agent = compile(select_yaml());

    assert_eq!(
        agent.nodes["selected"].control,
        NodeControl::Select {
            sources: ["general".to_string(), "medical".to_string()]
                .into_iter()
                .collect(),
        }
    );
    assert_eq!(
        agent.execution_plan.node_regions["selected"],
        NodeRegion::Linear
    );
    assert_eq!(
        agent.nodes["result"].references,
        ["selected".to_string()].into_iter().collect()
    );
}

#[test]
fn fork_continuation_is_not_a_select_predecessor_candidate() {
    let nodes = BTreeMap::from([
        (
            "route".to_string(),
            topology_node(
                "route",
                NodeControl::Ordinary,
                vec![
                    ControlEdge::Direct { target: "a".into() },
                    ControlEdge::Direct { target: "b".into() },
                    ControlEdge::Direct {
                        target: "fanout".into(),
                    },
                ],
            ),
        ),
        (
            "a".to_string(),
            topology_node(
                "a",
                NodeControl::Ordinary,
                vec![ControlEdge::Direct {
                    target: "selected".into(),
                }],
            ),
        ),
        (
            "b".to_string(),
            topology_node(
                "b",
                NodeControl::Ordinary,
                vec![ControlEdge::Direct {
                    target: "selected".into(),
                }],
            ),
        ),
        (
            "fanout".to_string(),
            topology_node(
                "fanout",
                NodeControl::Ordinary,
                vec![ControlEdge::ForkContinuation {
                    target: "selected".into(),
                }],
            ),
        ),
        (
            "selected".to_string(),
            topology_node(
                "selected",
                NodeControl::Select {
                    sources: BTreeSet::from(["a".to_string(), "b".to_string()]),
                },
                vec![ControlEdge::Direct {
                    target: "result".into(),
                }],
            ),
        ),
        (
            "result".to_string(),
            topology_node(
                "result",
                NodeControl::End {
                    outcome: insight_agent_platform::outcome::EndOutcomeKind::Success,
                },
                Vec::new(),
            ),
        ),
    ]);
    let plan = ExecutionPlan::sequential("route", nodes.keys().cloned());

    validate_graph("route", &nodes, &plan).unwrap();
}

#[test]
fn compiles_multi_way_condition_convergence() {
    let agent = compile(
        r#"
version: 1
id: multi-select
name: Multi Select
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases:
        - {when: "input.kind == 'a'", next: a}
        - {when: "input.kind == 'b'", next: b}
      default: c
  a:
    type: core.template
    next: selected
    config: {value: a}
  b:
    type: core.template
    next: selected
    config: {value: b}
  c:
    type: core.template
    next: selected
    config: {value: c}
  selected:
    type: core.select
    next: result
    config: {sources: [a, b, c]}
  result:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.selected.output.value }}"}}
"#,
    );

    assert_eq!(
        agent.nodes["selected"].control,
        NodeControl::Select {
            sources: ["a".to_string(), "b".to_string(), "c".to_string()]
                .into_iter()
                .collect(),
        }
    );
}

#[test]
fn select_requires_next_and_rejects_content_emit() {
    assert_compile_error(
        &select_yaml().replace("    next: result\n", ""),
        "END_REQUIRED",
    );
    assert_compile_error(
        &select_yaml().replace(
            "    type: core.select\n    next: result",
            "    type: core.select\n    next: result\n    emit: content",
        ),
        "NODE_EMIT_UNSUPPORTED",
    );
}

#[test]
fn source_order_changes_the_agent_hash_without_changing_control_semantics() {
    let authored = compile(select_yaml());
    let reversed = compile(
        &select_yaml().replace("sources: [medical, general]", "sources: [general, medical]"),
    );

    assert_ne!(authored.version_hash, reversed.version_hash);
    assert_eq!(
        authored.nodes["selected"].control,
        reversed.nodes["selected"].control
    );
}

#[test]
fn rejects_missing_and_mismatched_sources_with_select_codes() {
    assert_compile_error(
        &select_yaml().replace("[medical, general]", "[medical, missing]"),
        "SELECT_SOURCE_NOT_FOUND",
    );
    assert_compile_error(
        &select_yaml().replace("[medical, general]", "[medical, route]"),
        "SELECT_PREDECESSOR_MISMATCH",
    );
}

#[test]
fn rejects_all_predecessors_plus_an_existing_non_predecessor() {
    assert_compile_error(
        &select_yaml().replace("[medical, general]", "[medical, general, route]"),
        "SELECT_PREDECESSOR_MISMATCH",
    );
}

#[test]
fn rejects_two_of_three_direct_predecessors() {
    assert_compile_error(
        r#"
version: 1
id: incomplete-multi-select
name: Incomplete Multi Select
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases:
        - {when: "input.kind == 'a'", next: a}
        - {when: "input.kind == 'b'", next: b}
      default: c
  a:
    type: core.template
    next: selected
    config: {value: a}
  b:
    type: core.template
    next: selected
    config: {value: b}
  c:
    type: core.template
    next: selected
    config: {value: c}
  selected:
    type: core.select
    next: result
    config: {sources: [a, b]}
  result:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.selected.output.value }}"}}
"#,
        "SELECT_PREDECESSOR_MISMATCH",
    );
}

#[test]
fn rejects_sources_connected_by_a_path() {
    assert_compile_error(
        r#"
version: 1
id: sequential-sources
name: Sequential Sources
input:
  schema: {type: object}
entry: first
nodes:
  first:
    type: core.condition
    config:
      cases: [{when: "true", next: second}]
      default: selected
  second:
    type: core.template
    next: selected
    config: {value: second}
  selected:
    type: core.select
    next: result
    config: {sources: [first, second]}
  result:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.selected.output.value }}"}}
"#,
        "SELECT_SOURCES_NOT_EXCLUSIVE",
    );
}

#[test]
fn compiles_select_inside_one_fork_branch() {
    let agent = compile(
        r#"
version: 1
id: branch-local-select
name: Branch Local Select
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {choice: route, fixed: fixed}
      join: collect
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: left}]
      default: right
  left:
    type: core.template
    next: branch_select
    config: {value: left}
  right:
    type: core.template
    next: branch_select
    config: {value: right}
  branch_select:
    type: core.select
    next: end_choice
    config: {sources: [left, right]}
  end_choice:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.branch_select.output }}"}}
  fixed:
    type: core.template
    next: end_fixed
    config: {value: fixed}
  end_fixed:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.fixed.output }}"}}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.end
    config: {outcome: success, data: {ok: true}}
"#,
    );

    assert_eq!(
        agent.execution_plan.node_regions["branch_select"],
        NodeRegion::Branch {
            fork_id: "fanout".to_string(),
            branch_id: "choice".to_string(),
        }
    );
}

#[test]
fn existing_fork_validation_rejects_sibling_branch_convergence_first() {
    assert_compile_error(
        r#"
version: 1
id: sibling-select
name: Sibling Select
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {a: a, b: b}
      join: collect
  a:
    type: core.template
    next: selected
    config: {value: a}
  b:
    type: core.template
    next: selected
    config: {value: b}
  selected:
    type: core.select
    next: end_selected
    config: {sources: [a, b]}
  end_selected:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.selected.output }}"}}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.end
    config: {outcome: success, data: {ok: true}}
"#,
        "BRANCH_CROSS_REGION_EDGE",
    );
}

#[test]
fn rejects_join_and_linear_sources_with_different_regions() {
    assert_compile_error(
        r#"
version: 1
id: mixed-region-select
name: Mixed Region Select
input:
  schema: {type: object}
entry: route
nodes:
  route:
    type: core.condition
    config:
      cases: [{when: "true", next: fanout}]
      default: outside
  fanout:
    type: core.fork
    config:
      branches: {a: a, b: b}
      join: collect
  a:
    type: core.template
    next: end_a
    config: {value: a}
  end_a:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.a.output }}"}}
  b:
    type: core.template
    next: end_b
    config: {value: b}
  end_b:
    type: core.end
    config: {outcome: success, data: {value: "{{ nodes.b.output }}"}}
  collect:
    type: core.join
    next: selected
    config: {mode: all_settled}
  outside:
    type: core.template
    next: selected
    config: {value: outside}
  selected:
    type: core.select
    next: result
    config: {sources: [collect, outside]}
  result:
    type: core.end
    config: {outcome: success, data: {ok: true}}
"#,
        "SELECT_REGION_INVALID",
    );
}

#[test]
fn downstream_nodes_cannot_bypass_select_dominance() {
    assert_compile_error(
        &select_yaml().replace(
            "nodes.selected.output.value.text",
            "nodes.medical.output.text",
        ),
        "INVALID_NODE_REFERENCE",
    );
}
