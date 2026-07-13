use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    dsl::{
        compiled::{
            CompiledNode, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules, NodeOutcome,
            NodeTransition,
        },
        compiler::CompileContext,
        references::is_dsl_identifier,
        CompileError,
    },
    nodes::registry::{NodeExecutor, NodeType},
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectConfig {
    sources: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct SelectNode;

impl NodeType for SelectNode {
    fn kind(&self) -> &'static str {
        "core.select"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: SelectConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!("invalid core.select config for node '{node_id}': {error}"),
            )
        })?;
        if config.sources.len() < 2 {
            return Err(CompileError::new(
                "SELECT_SOURCE_COUNT_INVALID",
                format!("select node '{node_id}' must define at least two sources"),
            ));
        }

        let mut sources = BTreeSet::new();
        for source in config.sources {
            if source == node_id || !is_dsl_identifier(&source) {
                return Err(CompileError::new(
                    "SELECT_SOURCE_ID_INVALID",
                    format!("select node '{node_id}' has invalid source ID '{source}'"),
                ));
            }
            if !sources.insert(source.clone()) {
                return Err(CompileError::new(
                    "SELECT_SOURCE_DUPLICATE",
                    format!("select node '{node_id}' declares source '{source}' more than once"),
                ));
            }
        }

        Ok(NodeCompilation {
            body: Arc::new(sources.clone()),
            edges: Vec::new(),
            references: BTreeSet::new(),
            terminal: false,
            control: NodeControl::Select { sources },
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit: false,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for SelectNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        if let Some(reason) = control.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        let sources = node.body::<BTreeSet<String>>()?;
        let visible = sources
            .iter()
            .filter_map(|source| context.node_output(source).map(|value| (source, value)))
            .collect::<Vec<_>>();

        match visible.as_slice() {
            [(source, value)] => Ok(NodeOutcome {
                output: json!({"source_node_id": source, "value": value}),
                transition: NodeTransition::Next,
            }),
            [] => Err(RunError::new(
                "SELECT_SOURCE_MISSING",
                format!("select node '{}' has no completed source", node.id),
            )),
            values => {
                let source_ids = values
                    .iter()
                    .map(|(source, _)| source.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(RunError::new(
                    "SELECT_SOURCE_AMBIGUOUS",
                    format!(
                        "select node '{}' has multiple completed sources: {source_ids}",
                        node.id
                    ),
                ))
            }
        }
    }
}
