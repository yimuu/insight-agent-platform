use std::{collections::BTreeSet, fs, path::Path, time::Duration};

use insight_agent_platform::{
    dsl::{
        compiled::{JoinPolicy, NodeRegion},
        compiler::{AgentCompiler, CompileLimits},
    },
    nodes::default_node_registries,
    resources::{actions::ActionRegistry, models::ModelRegistry},
};
use tempfile::TempDir;

fn compiler() -> AgentCompiler {
    let (node_types, _) = default_node_registries().unwrap();
    AgentCompiler::new(
        node_types,
        ModelRegistry::default(),
        ActionRegistry::default(),
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 32,
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

fn assert_compile_error(yaml: &str, expected_code: &'static str) {
    let (_temp, root) = write_agent(yaml);
    let error = compiler().compile_dir(Path::new(&root)).unwrap_err();
    assert_eq!(error.code(), expected_code, "unexpected error: {error}");
}

fn parallel_yaml() -> &'static str {
    r#"
version: 1
id: parallel-agent
name: Parallel Agent
input:
  schema: {type: object}
entry: prepare
nodes:
  prepare:
    type: core.template
    next: fanout
    config:
      value: prepared
  fanout:
    type: core.fork
    config:
      branches:
        source_a: search_a
        source_b: search_b
      join: collect
  search_a:
    type: core.template
    next: summarize_a
    config:
      value: result-a
  summarize_a:
    type: core.template
    next: collect
    config:
      value: summary-a
  search_b:
    type: core.condition
    config:
      cases:
        - when: "true"
          next: summarize_b
      default: collect
  summarize_b:
    type: core.template
    next: collect
    config:
      value: summary-b
  collect:
    type: core.join
    next: result
    config:
      mode: all_settled
  result:
    type: core.output
    config:
      data: {ok: true}
"#
}

fn parallel_yaml_with_outside_edge(target: &str) -> String {
    parallel_yaml()
        .replace(
            "  prepare:\n    type: core.template\n    next: fanout\n    config:\n      value: prepared",
            "  prepare:\n    type: core.condition\n    config:\n      cases:\n        - when: \"true\"\n          next: fanout\n      default: outside",
        )
        .replace(
            "  fanout:",
            &format!(
                "  outside:\n    type: core.template\n    next: {target}\n    config:\n      value: outside\n  fanout:"
            ),
        )
}

#[test]
fn compiles_immutable_parallel_regions() {
    let agent = compile(parallel_yaml());

    let fork = &agent.execution_plan.forks["fanout"];
    assert_eq!(fork.join_id, "collect");
    assert_eq!(fork.policy, JoinPolicy::AllSettled);
    assert_eq!(fork.branches["source_a"].entry, "search_a");
    assert_eq!(
        fork.branches["source_a"].nodes,
        BTreeSet::from(["search_a".to_string(), "summarize_a".to_string()])
    );
    assert_eq!(
        agent.execution_plan.node_regions["search_b"],
        NodeRegion::Branch {
            fork_id: "fanout".to_string(),
            branch_id: "source_b".to_string(),
        }
    );
    assert_eq!(
        agent.execution_plan.node_regions["collect"],
        NodeRegion::Join {
            fork_id: "fanout".to_string(),
        }
    );
}

#[test]
fn sequential_agent_has_only_linear_regions() {
    let agent = compile(
        r#"
version: 1
id: sequential-agent
name: Sequential Agent
input:
  schema: {type: object}
entry: prepare
nodes:
  prepare:
    type: core.template
    next: result
    config:
      value: prepared
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
    );

    assert!(agent.execution_plan.forks.is_empty());
    assert!(agent
        .execution_plan
        .node_regions
        .values()
        .all(|region| region == &NodeRegion::Linear));
}

#[test]
fn rejects_fork_whose_join_has_the_wrong_kind() {
    assert_compile_error(
        &parallel_yaml().replace(
            "  collect:\n    type: core.join\n    next: result\n    config:\n      mode: all_settled",
            "  collect:\n    type: core.template\n    next: result\n    config:\n      value: collected",
        ),
        "FORK_JOIN_KIND_INVALID",
    );
}

#[test]
fn rejects_fork_whose_declared_join_is_absent() {
    assert_compile_error(
        &parallel_yaml().replace("join: collect", "join: missing"),
        "FORK_JOIN_NOT_FOUND",
    );
}

#[test]
fn rejects_branch_that_escapes_to_output() {
    assert_compile_error(
        &parallel_yaml().replace(
            "  summarize_a:\n    type: core.template\n    next: collect",
            "  summarize_a:\n    type: core.template\n    next: result",
        ),
        "BRANCH_PATH_MISSING_JOIN",
    );
}

#[test]
fn rejects_overlapping_branch_entries() {
    assert_compile_error(
        r#"
version: 1
id: overlapping-entries
name: Overlapping Entries
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {source_a: shared, source_b: shared}
      join: collect
  shared:
    type: core.template
    next: collect
    config: {value: shared}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "BRANCH_REGION_OVERLAP",
    );
}

#[test]
fn rejects_branch_edge_into_sibling_region() {
    assert_compile_error(
        &parallel_yaml().replace(
            "  summarize_a:\n    type: core.template\n    next: collect",
            "  summarize_a:\n    type: core.template\n    next: search_b",
        ),
        "BRANCH_CROSS_REGION_EDGE",
    );
}

#[test]
fn rejects_linear_edge_into_branch_entry() {
    assert_compile_error(
        &parallel_yaml_with_outside_edge("search_a"),
        "BRANCH_CROSS_REGION_EDGE",
    );
}

#[test]
fn rejects_linear_edge_into_branch_interior() {
    assert_compile_error(
        &parallel_yaml_with_outside_edge("summarize_a"),
        "BRANCH_CROSS_REGION_EDGE",
    );
}

#[test]
fn rejects_direct_fork_to_join_bypass() {
    assert_compile_error(
        r#"
version: 1
id: direct-bypass
name: Direct Bypass
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {bypass: collect, work: search}
      join: collect
  search:
    type: core.template
    next: collect
    config: {value: found}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "JOIN_PREDECESSOR_INVALID",
    );
}

#[test]
fn rejects_nested_fork_before_outer_join() {
    assert_compile_error(
        r#"
version: 1
id: nested-fork
name: Nested Fork
input:
  schema: {type: object}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches: {nested: nested_fork, plain: plain}
      join: outer_join
  nested_fork:
    type: core.fork
    config:
      branches: {x: x, y: y}
      join: nested_join
  x:
    type: core.template
    next: nested_join
    config: {value: x}
  y:
    type: core.template
    next: nested_join
    config: {value: y}
  nested_join:
    type: core.join
    next: outer_join
    config: {mode: all_settled}
  plain:
    type: core.template
    next: outer_join
    config: {value: plain}
  outer_join:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "BRANCH_NESTED_FORK",
    );
}

#[test]
fn rejects_condition_path_that_bypasses_join() {
    assert_compile_error(
        &parallel_yaml().replace("      default: collect", "      default: result"),
        "BRANCH_PATH_MISSING_JOIN",
    );
}

#[test]
fn rejects_outside_predecessor_entering_join() {
    assert_compile_error(
        r#"
version: 1
id: outside-predecessor
name: Outside Predecessor
input:
  schema: {type: object}
entry: prepare
nodes:
  prepare:
    type: core.condition
    config:
      cases:
        - when: "true"
          next: fanout
      default: outside
  outside:
    type: core.template
    next: collect
    config: {value: outside}
  fanout:
    type: core.fork
    config:
      branches: {source_a: a, source_b: b}
      join: collect
  a:
    type: core.template
    next: collect
    config: {value: a}
  b:
    type: core.template
    next: collect
    config: {value: b}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "JOIN_PREDECESSOR_INVALID",
    );
}

#[test]
fn rejects_join_claimed_by_two_forks() {
    assert_compile_error(
        r#"
version: 1
id: shared-join
name: Shared Join
input:
  schema: {type: object}
entry: prepare
nodes:
  prepare:
    type: core.condition
    config:
      cases:
        - when: "true"
          next: fork_a
      default: fork_b
  fork_a:
    type: core.fork
    config:
      branches: {a1: a1, a2: a2}
      join: collect
  a1:
    type: core.template
    next: collect
    config: {value: a1}
  a2:
    type: core.template
    next: collect
    config: {value: a2}
  fork_b:
    type: core.fork
    config:
      branches: {b1: b1, b2: b2}
      join: collect
  b1:
    type: core.template
    next: collect
    config: {value: b1}
  b2:
    type: core.template
    next: collect
    config: {value: b2}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "JOIN_PAIRING_INVALID",
    );
}

#[test]
fn rejects_unclaimed_join() {
    assert_compile_error(
        r#"
version: 1
id: unclaimed-join
name: Unclaimed Join
input:
  schema: {type: object}
entry: prepare
nodes:
  prepare:
    type: core.template
    next: collect
    config: {value: prepared}
  collect:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
        "JOIN_PAIRING_INVALID",
    );
}

#[test]
fn compiles_sequential_fork_regions() {
    let agent = compile(
        r#"
version: 1
id: sequential-forks
name: Sequential Forks
input:
  schema: {type: object}
entry: fork_a
nodes:
  fork_a:
    type: core.fork
    config:
      branches: {a1: a1, a2: a2}
      join: join_a
  a1:
    type: core.template
    next: join_a
    config: {value: a1}
  a2:
    type: core.template
    next: join_a
    config: {value: a2}
  join_a:
    type: core.join
    next: fork_b
    config: {mode: all_settled}
  fork_b:
    type: core.fork
    config:
      branches: {b1: b1, b2: b2}
      join: join_b
  b1:
    type: core.template
    next: join_b
    config: {value: b1}
  b2:
    type: core.template
    next: join_b
    config: {value: b2}
  join_b:
    type: core.join
    next: result
    config: {mode: all_settled}
  result:
    type: core.output
    config:
      data: {ok: true}
"#,
    );

    assert_eq!(
        agent.execution_plan.forks.keys().collect::<Vec<_>>(),
        vec!["fork_a", "fork_b"]
    );
    assert_eq!(
        agent.execution_plan.node_regions["a1"],
        NodeRegion::Branch {
            fork_id: "fork_a".to_string(),
            branch_id: "a1".to_string(),
        }
    );
    assert_eq!(
        agent.execution_plan.node_regions["b1"],
        NodeRegion::Branch {
            fork_id: "fork_b".to_string(),
            branch_id: "b1".to_string(),
        }
    );
}

#[test]
fn rejects_fork_over_configured_branch_limit() {
    let branches = (0..33)
        .map(|index| format!("        b{index:02}: n{index:02}"))
        .collect::<Vec<_>>()
        .join("\n");
    let branch_nodes = (0..33)
        .map(|index| {
            format!(
                "  n{index:02}:\n    type: core.template\n    next: collect\n    config: {{value: {index}}}"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let yaml = format!(
        r#"
version: 1
id: too-many-branches
name: Too Many Branches
input:
  schema: {{type: object}}
entry: fanout
nodes:
  fanout:
    type: core.fork
    config:
      branches:
{branches}
      join: collect
{branch_nodes}
  collect:
    type: core.join
    next: result
    config: {{mode: all_settled}}
  result:
    type: core.output
    config:
      data: {{ok: true}}
"#
    );

    assert_compile_error(&yaml, "FORK_BRANCH_LIMIT_EXCEEDED");
}
