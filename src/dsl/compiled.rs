use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::Duration,
};

use handlebars::Handlebars;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    outcome::{EndOutcomeKind, TerminalOutcome},
    runtime::RunError,
    schema::JsonSchemaValidator,
};

use super::EmitPolicy;

pub type CompiledBody = Arc<dyn Any + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextPolicy {
    Required,
    Forbidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeEnvelopeRules {
    pub next: NextPolicy,
    pub allows_content_emit: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinPolicy {
    AllSettled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeControl {
    Ordinary,
    Fork {
        branches: BTreeMap<String, String>,
        join: String,
    },
    Join {
        policy: JoinPolicy,
    },
    Select {
        sources: BTreeSet<String>,
    },
    End {
        outcome: EndOutcomeKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRegion {
    Linear,
    Branch { fork_id: String, branch_id: String },
    Join { fork_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchPlan {
    pub branch_id: String,
    pub entry: String,
    pub nodes: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkPlan {
    pub fork_id: String,
    pub join_id: String,
    pub branches: BTreeMap<String, BranchPlan>,
    pub policy: JoinPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub entry: String,
    pub forks: BTreeMap<String, ForkPlan>,
    pub node_regions: BTreeMap<String, NodeRegion>,
}

impl ExecutionPlan {
    pub fn sequential(
        entry: impl Into<String>,
        node_ids: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            entry: entry.into(),
            forks: BTreeMap::new(),
            node_regions: node_ids
                .into_iter()
                .map(|node_id| (node_id, NodeRegion::Linear))
                .collect(),
        }
    }
}

pub struct NodeCompilation {
    pub body: CompiledBody,
    pub edges: Vec<String>,
    pub references: BTreeSet<String>,
    pub control: NodeControl,
    pub envelope: NodeEnvelopeRules,
}

#[derive(Clone)]
pub struct CompiledNode {
    pub id: String,
    pub kind: String,
    pub next: Option<String>,
    pub emit: EmitPolicy,
    pub timeout: Duration,
    pub body: CompiledBody,
    pub edges: Vec<String>,
    pub references: BTreeSet<String>,
    pub control: NodeControl,
}

impl CompiledNode {
    pub fn body<T: Any>(&self) -> Result<&T, RunError> {
        self.body.downcast_ref::<T>().ok_or_else(|| {
            RunError::new(
                "NODE_BODY_TYPE_MISMATCH",
                format!(
                    "compiled body for node '{}' does not match executor '{}'",
                    self.id, self.kind
                ),
            )
        })
    }
}

#[derive(Clone)]
pub struct CompiledAgent {
    pub id: String,
    pub name: String,
    pub description: String,
    pub version_hash: String,
    pub input_schema: Arc<JsonSchemaValidator>,
    pub entry: String,
    pub nodes: BTreeMap<String, CompiledNode>,
    pub execution_plan: ExecutionPlan,
    pub templates: Arc<Handlebars<'static>>,
}

impl fmt::Debug for CompiledAgent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledAgent")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("description", &self.description)
            .field("version_hash", &self.version_hash)
            .field("entry", &self.entry)
            .field("node_ids", &self.nodes.keys().collect::<Vec<_>>())
            .field(
                "fork_ids",
                &self.execution_plan.forks.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeTransition {
    Next,
    Goto(String),
    ActivateFork,
    End(TerminalOutcome),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeOutcome {
    pub output: Value,
    pub transition: NodeTransition,
}
