use std::{collections::BTreeSet, fs, path::Path, sync::Arc, time::Duration};

use insight_agent_platform::{
    dsl::{
        compiled::{NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules, NodeRegion},
        compiler::{AgentCompiler, CompileContext, CompileLimits},
        CompileError,
    },
    nodes::registry::{NodeType, NodeTypeRegistry},
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
            terminal: false,
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
            edges: config.targets,
            references: BTreeSet::new(),
            terminal: false,
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
        if config != serde_json::json!({}) {
            return Err(CompileError::new(
                "NODE_CONFIG_INVALID",
                "terminal config must be empty",
            ));
        }
        Ok(NodeCompilation {
            body: Arc::new(()),
            edges: Vec::new(),
            references: BTreeSet::new(),
            terminal: true,
            control: NodeControl::Ordinary,
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
        .register(TerminalNode {
            kind: "core.output",
        })
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
    type: core.output
    config: {}
"#
}

fn assert_compile_error(yaml: &str, code: &'static str) {
    let (_temp, root) = write_agent(yaml, "Hello {{ input.question }}");
    let error = compiler().compile_dir(&root).unwrap_err();
    assert_eq!(error.code(), code, "unexpected error: {error}");
}

#[test]
fn compiles_valid_graph_and_hashes_prompt_contents() {
    let (temp, root) = write_agent(valid_yaml(), "Hello {{ input.question }}");
    let first = compiler().compile_dir(&root).unwrap();
    let second = compiler().compile_dir(&root).unwrap();
    assert_eq!(first.version_hash, second.version_hash);
    assert!(first.version_hash.starts_with("sha256:"));
    assert_eq!(first.nodes["first"].edges, vec!["result"]);
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
fn rejects_cycles_and_unreachable_nodes() {
    let cycle = valid_yaml()
        .replace("next: result", "next: second")
        .replace(
            "  result:\n    type: core.output\n    config: {}",
            "  second:\n    type: test.step\n    next: first\n    config: {}",
        );
    assert_compile_error(&cycle, "GRAPH_CYCLE");

    let unreachable = format!(
        "{}\n  orphan:\n    type: core.output\n    config: {{}}\n",
        valid_yaml()
    );
    assert_compile_error(&unreachable, "NODE_UNREACHABLE");
}

#[test]
fn rejects_non_output_terminal_and_invalid_predecessor_reference() {
    let non_output = r#"
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
    assert_compile_error(non_output, "OUTPUT_REQUIRED");

    let future_reference = valid_yaml()
        .replace("next: result", "next: second")
        .replace(
            "      prompt_ref: system",
            "      prompt_ref: system\n      references: [second]",
        )
        .replace(
            "  result:\n    type: core.output",
            "  second:\n    type: test.step\n    next: result\n    config: {}\n  result:\n    type: core.output",
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
            "  result:\n    type: core.output",
            "  second:\n    type: test.step\n    next: result\n    config:\n      references: [first]\n  result:\n    type: core.output",
        );
    let (_temp, root) = write_agent(&yaml, "Hello");
    assert!(compiler().compile_dir(Path::new(&root)).is_ok());
}
