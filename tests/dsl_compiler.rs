use std::{collections::BTreeSet, fs, path::Path, sync::Arc, time::Duration};

use insight_agent_platform::{
    dsl::{
        compiled::{
            ControlEdge, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules, NodeRegion,
        },
        compiler::{AgentCompiler, CompileContext, CompileLimits},
        CompileError,
    },
    nodes::{
        default_node_registries,
        registry::{NodeType, NodeTypeRegistry},
    },
    outcome::EndOutcomeKind,
    resources::{actions::ActionRegistry, models::ModelRegistry},
};
use serde::Deserialize;
use serde_json::Value;
use tempfile::TempDir;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExtensionConfig {
    #[serde(default)]
    prompt_ref: Option<String>,
    #[serde(default)]
    references: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct StepNode;

impl NodeType for StepNode {
    fn kind(&self) -> &'static str {
        "test.step"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: ExtensionConfig = serde_json::from_value(config)
            .map_err(|error| CompileError::new("NODE_CONFIG_INVALID", error.to_string()))?;
        let mut references = config.references.into_iter().collect::<BTreeSet<_>>();
        if let Some(prompt_ref) = config.prompt_ref {
            references.extend(
                context
                    .compile_prompt_ref(node_id, "prompt", &prompt_ref)?
                    .references,
            );
        }
        Ok(NodeCompilation {
            body: Arc::new(()),
            edges: Vec::new(),
            references,
            control: NodeControl::Ordinary,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit: false,
            },
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BranchConfig {
    targets: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct BranchNode;

impl NodeType for BranchNode {
    fn kind(&self) -> &'static str {
        "test.branch"
    }

    fn compile(
        &self,
        _node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: BranchConfig = serde_json::from_value(config)
            .map_err(|error| CompileError::new("NODE_CONFIG_INVALID", error.to_string()))?;
        Ok(NodeCompilation {
            body: Arc::new(()),
            edges: config
                .targets
                .into_iter()
                .map(|target| ControlEdge::Direct { target })
                .collect(),
            references: BTreeSet::new(),
            control: NodeControl::Ordinary,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Forbidden,
                allows_content_emit: false,
            },
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct TerminalNode {
    kind: &'static str,
}

impl NodeType for TerminalNode {
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn compile(
        &self,
        _node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let expected_config = if self.kind == "core.end" {
            serde_json::json!({"outcome":"success", "data":{}})
        } else {
            serde_json::json!({})
        };
        if config != expected_config {
            return Err(CompileError::new(
                "NODE_CONFIG_INVALID",
                "terminal config must be empty",
            ));
        }
        Ok(NodeCompilation {
            body: Arc::new(()),
            edges: Vec::new(),
            references: BTreeSet::new(),
            control: if self.kind == "core.end" {
                NodeControl::End {
                    outcome: EndOutcomeKind::Success,
                }
            } else {
                NodeControl::Ordinary
            },
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Forbidden,
                allows_content_emit: false,
            },
        })
    }
}

fn compiler() -> AgentCompiler {
    let mut node_types = NodeTypeRegistry::default();
    node_types.register(StepNode).unwrap();
    node_types.register(BranchNode).unwrap();
    node_types
        .register(TerminalNode { kind: "core.end" })
        .unwrap();
    node_types
        .register(TerminalNode {
            kind: "test.terminal",
        })
        .unwrap();
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

#[test]
fn compiler_exposes_configured_limits() {
    assert_eq!(
        compiler().limits(),
        CompileLimits {
            max_fork_branches: 32,
        }
    );
}

fn write_agent(yaml: &str, prompt: &str) -> (TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("agent");
    fs::create_dir_all(root.join("prompts")).unwrap();
    fs::write(root.join("agent.yaml"), yaml).unwrap();
    fs::write(root.join("prompts/system.md"), prompt).unwrap();
    (temp, root)
}

fn valid_yaml() -> &'static str {
    r#"
version: 1
id: test_agent
name: Test Agent
input:
  schema:
    type: object
    required: [question]
prompts:
  system: prompts/system.md
entry: first
nodes:
  first:
    type: test.step
    next: result
    config:
      prompt_ref: system
  result:
    type: core.end
    config: {outcome: success, data: {}}
"#
}

const VALID_AGENT_VERSION_HASH: &str =
    "sha256:7e5748bbd8ba60e127a43351670d2498faf85db5e8e537ac62801d35d254848c";

fn assert_compile_error(yaml: &str, code: &'static str) {
    let (_temp, root) = write_agent(yaml, "Hello {{ input.question }}");
    let error = compiler().compile_dir(&root).unwrap_err();
    assert_eq!(error.code(), code, "unexpected error: {error}");
}

fn assert_core_compile_error(yaml: &str, code: &'static str) {
    let (_temp, root) = write_agent(yaml, "unused");
    let (types, _) = default_node_registries().unwrap();
    let compiler = AgentCompiler::new(
        types,
        ModelRegistry::default(),
        ActionRegistry::default(),
        Duration::from_secs(30),
        CompileLimits {
            max_fork_branches: 32,
        },
    );
    let error = compiler.compile_dir(&root).unwrap_err();
    assert_eq!(error.code(), code, "unexpected error: {error}");
}

#[test]
fn rejects_next_and_content_emit_on_end() {
    assert_core_compile_error(
        r#"
version: 1
id: end_next
name: End Next
input:
  schema: {type: object}
entry: result
nodes:
  result:
    type: core.end
    next: result
    config:
      outcome: success
      data: {ok: true}
"#,
        "NODE_NEXT_FORBIDDEN",
    );
    assert_core_compile_error(
        r#"
version: 1
id: end_emit
name: End Emit
input:
  schema: {type: object}
entry: result
nodes:
  result:
    type: core.end
    emit: content
    config:
      outcome: success
      content: {template: answer}
      format: text
"#,
        "NODE_EMIT_UNSUPPORTED",
    );
}

#[test]
fn compiles_valid_graph_and_hashes_prompt_contents() {
    let (temp, root) = write_agent(valid_yaml(), "Hello {{ input.question }}");
    let first = compiler().compile_dir(&root).unwrap();
    let second = compiler().compile_dir(&root).unwrap();
    assert_eq!(
        first.version_hash, VALID_AGENT_VERSION_HASH,
        "Agent hash changed before the sha2 migration"
    );
    assert_eq!(
        second.version_hash, VALID_AGENT_VERSION_HASH,
        "Agent hash is not stable across repeated compiles"
    );
    assert_eq!(
        first.nodes["first"].edges,
        vec![ControlEdge::Direct {
            target: "result".into(),
        }]
    );
    assert!(first.execution_plan.forks.is_empty());
    assert!(first
        .execution_plan
        .node_regions
        .values()
        .all(|region| region == &NodeRegion::Linear));

    fs::write(
        root.join("prompts/system.md"),
        "Changed {{ input.question }}",
    )
    .unwrap();
    let changed = compiler().compile_dir(&root).unwrap();
    assert_ne!(first.version_hash, changed.version_hash);
    drop(temp);
}

#[test]
fn rejects_missing_entry_and_edges() {
    assert_compile_error(
        &valid_yaml().replace("entry: first", "entry: missing"),
        "ENTRY_NOT_FOUND",
    );
    assert_compile_error(
        &valid_yaml().replace("next: result", "next: missing"),
        "NODE_EDGE_NOT_FOUND",
    );
}

#[test]
fn rejects_node_ids_outside_canonical_identifier_grammar() {
    let invalid = valid_yaml().replace("  first:", "  first-node:");
    assert_compile_error(&invalid, "NODE_ID_INVALID");
}

#[test]
fn rejects_cycles_and_unreachable_nodes() {
    let cycle = valid_yaml()
        .replace("next: result", "next: second")
        .replace(
            "  result:\n    type: core.end\n    config: {outcome: success, data: {}}",
            "  second:\n    type: test.step\n    next: first\n    config: {}",
        );
    assert_compile_error(&cycle, "GRAPH_CYCLE");

    let unreachable = format!(
        "{}\n  orphan:\n    type: core.end\n    config: {{outcome: success, data: {{}}}}\n",
        valid_yaml()
    );
    assert_compile_error(&unreachable, "NODE_UNREACHABLE");
}

#[test]
fn rejects_non_end_dead_end_and_invalid_predecessor_reference() {
    let non_end = r#"
version: 1
id: test_agent
name: Test Agent
input:
  schema: {type: object}
entry: end
nodes:
  end:
    type: test.terminal
    config: {}
"#;
    assert_compile_error(non_end, "END_REQUIRED");

    let future_reference = valid_yaml()
        .replace("next: result", "next: second")
        .replace(
            "      prompt_ref: system",
            "      prompt_ref: system\n      references: [second]",
        )
        .replace(
            "  result:\n    type: core.end",
            "  second:\n    type: test.step\n    next: result\n    config: {}\n  result:\n    type: core.end",
        );
    assert_compile_error(&future_reference, "INVALID_NODE_REFERENCE");

    let self_reference = valid_yaml().replace(
        "      prompt_ref: system",
        "      prompt_ref: system\n      references: [first]",
    );
    assert_compile_error(&self_reference, "INVALID_NODE_REFERENCE");

    let missing_reference = valid_yaml().replace(
        "      prompt_ref: system",
        "      prompt_ref: system\n      references: [missing]",
    );
    assert_compile_error(&missing_reference, "INVALID_NODE_REFERENCE");
}

#[test]
fn rejects_prompt_paths_outside_agent_and_invalid_templates() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("agent");
    fs::create_dir_all(&root).unwrap();
    fs::write(temp.path().join("outside.md"), "outside").unwrap();
    fs::write(
        root.join("agent.yaml"),
        valid_yaml().replace("prompts/system.md", "../outside.md"),
    )
    .unwrap();
    assert_eq!(
        compiler().compile_dir(&root).unwrap_err().code(),
        "PROMPT_PATH_ESCAPE"
    );

    let (_temp, root) = write_agent(valid_yaml(), "{{#if input.question}}");
    assert_eq!(
        compiler().compile_dir(&root).unwrap_err().code(),
        "TEMPLATE_INVALID"
    );
}

#[test]
fn accepts_references_to_dominating_predecessors() {
    let yaml = valid_yaml()
        .replace("next: result", "next: second")
        .replace(
            "  result:\n    type: core.end",
            "  second:\n    type: test.step\n    next: result\n    config:\n      references: [first]\n  result:\n    type: core.end",
        );
    let (_temp, root) = write_agent(&yaml, "Hello");
    assert!(compiler().compile_dir(Path::new(&root)).is_ok());
}

#[test]
fn compiler_rejects_non_draft7_input_schema_uri() {
    let yaml = r#"
version: 1
id: test_agent
name: Test Agent
input:
  schema:
    $schema: https://json-schema.org/draft/2020-12/schema
    type: object
prompts: {}
entry: done
nodes:
  done:
    type: test.terminal
    config: {}
"#;

    let (_temp, root) = write_agent(yaml, "");
    let error = compiler().compile_dir(&root).unwrap_err();

    assert_eq!(error.code(), "INPUT_SCHEMA_INVALID");
    assert!(error.to_string().contains("unsupported JSON Schema draft"));
}

#[test]
fn compiler_rejects_external_input_schema_ref() {
    let yaml = r#"
version: 1
id: test_agent
name: Test Agent
input:
  schema:
    $ref: https://example.invalid/schema.json
prompts: {}
entry: done
nodes:
  done:
    type: test.terminal
    config: {}
"#;

    let (_temp, root) = write_agent(yaml, "");
    let error = compiler().compile_dir(&root).unwrap_err();

    assert_eq!(error.code(), "INPUT_SCHEMA_INVALID");
    assert!(error
        .to_string()
        .contains("external JSON Schema references are not supported"));
}
