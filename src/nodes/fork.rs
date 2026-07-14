use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    dsl::{
        compiled::{
            CompiledNode, ControlEdge, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules,
            NodeOutcome, NodeTransition,
        },
        compiler::CompileContext,
        references::is_dsl_identifier,
        CompileError,
    },
    nodes::registry::{NodeExecutor, NodeType},
    runtime::{ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForkConfig {
    branches: BTreeMap<String, String>,
    join: String,
}

#[derive(Debug, Clone, Copy)]
pub struct ForkNode;

impl NodeType for ForkNode {
    fn kind(&self) -> &'static str {
        "core.fork"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: ForkConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!("invalid core.fork config for node '{node_id}': {error}"),
            )
        })?;
        if config.branches.len() < 2 {
            return Err(CompileError::new(
                "FORK_BRANCH_COUNT_INVALID",
                format!("fork node '{node_id}' must define at least two branches"),
            ));
        }
        for (branch_id, target) in &config.branches {
            if !is_dsl_identifier(branch_id) {
                return Err(CompileError::new(
                    "FORK_BRANCH_ID_INVALID",
                    format!("fork node '{node_id}' has invalid branch ID '{branch_id}'"),
                ));
            }
            if target.trim().is_empty() {
                return Err(CompileError::new(
                    "FORK_BRANCH_TARGET_INVALID",
                    format!("fork node '{node_id}' branch '{branch_id}' has an empty target"),
                ));
            }
        }
        if config.join.trim().is_empty() {
            return Err(CompileError::new(
                "FORK_JOIN_INVALID",
                format!("fork node '{node_id}' join must not be empty"),
            ));
        }

        let mut edges = config
            .branches
            .iter()
            .map(|(branch_id, target)| ControlEdge::ForkBranch {
                branch_id: branch_id.clone(),
                target: target.clone(),
            })
            .collect::<Vec<_>>();
        edges.push(ControlEdge::ForkContinuation {
            target: config.join.clone(),
        });
        let control = NodeControl::Fork {
            branches: config.branches.clone(),
            join: config.join.clone(),
        };
        Ok(NodeCompilation {
            body: Arc::new(config),
            edges,
            references: BTreeSet::new(),
            control,
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Forbidden,
                allows_content_emit: false,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for ForkNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        _context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        if let Some(reason) = control.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        let body = node.body::<ForkConfig>()?;
        Ok(NodeOutcome {
            output: json!({
                "branches": body.branches.keys().collect::<Vec<_>>(),
                "join": body.join,
            }),
            transition: NodeTransition::ActivateFork,
        })
    }
}
