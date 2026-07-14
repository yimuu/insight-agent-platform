use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    dsl::{
        compiled::{
            CompiledNode, JoinPolicy, NextPolicy, NodeCompilation, NodeControl, NodeEnvelopeRules,
            NodeOutcome, NodeTransition,
        },
        compiler::CompileContext,
        CompileError,
    },
    nodes::registry::{NodeExecutor, NodeType},
    runtime::{BranchFailureKind, BranchResult, ExecutionControl, RunContext, RunError},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinConfig {
    mode: JoinPolicy,
}

#[derive(Debug, Clone, Copy)]
pub struct JoinNode;

impl NodeType for JoinNode {
    fn kind(&self) -> &'static str {
        "core.join"
    }

    fn compile(
        &self,
        node_id: &str,
        config: Value,
        _context: &mut CompileContext<'_>,
    ) -> Result<NodeCompilation, CompileError> {
        let config: JoinConfig = serde_json::from_value(config).map_err(|error| {
            CompileError::new(
                "NODE_CONFIG_INVALID",
                format!("invalid core.join config for node '{node_id}': {error}"),
            )
        })?;
        let policy = config.mode;
        Ok(NodeCompilation {
            body: Arc::new(policy),
            edges: Vec::new(),
            references: BTreeSet::new(),
            control: NodeControl::Join { policy },
            envelope: NodeEnvelopeRules {
                next: NextPolicy::Required,
                allows_content_emit: false,
            },
        })
    }
}

#[async_trait]
impl NodeExecutor for JoinNode {
    async fn execute(
        &self,
        node: &CompiledNode,
        context: &RunContext,
        control: &ExecutionControl,
    ) -> Result<NodeOutcome, RunError> {
        if let Some(reason) = control.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        let _policy = node.body::<JoinPolicy>()?;
        let results = context.branch_results().ok_or_else(|| {
            RunError::new(
                "JOIN_RESULTS_MISSING",
                "join node requires settled branch results",
            )
        })?;
        let succeeded = results
            .values()
            .filter(|result| matches!(result, BranchResult::Succeeded { .. }))
            .count();
        let failed = results.len() - succeeded;
        let mut workflow = 0;
        let mut node = 0;
        let mut timeout = 0;
        for result in results.values() {
            if let BranchResult::Failed { error, .. } = result {
                match error.kind {
                    BranchFailureKind::Workflow => workflow += 1,
                    BranchFailureKind::Node => node += 1,
                    BranchFailureKind::Timeout => timeout += 1,
                }
            }
        }
        if failed != workflow + node + timeout {
            return Err(RunError::infrastructure(
                "JOIN_RESULT_INVALID",
                "join branch failure taxonomy is inconsistent",
            ));
        }

        Ok(NodeOutcome {
            output: json!({
                "branches": results,
                "summary": {
                    "total": results.len(),
                    "succeeded": succeeded,
                    "failed": failed,
                    "failures": {
                        "workflow": workflow,
                        "node": node,
                        "timeout": timeout,
                    },
                },
            }),
            transition: NodeTransition::Next,
        })
    }
}
