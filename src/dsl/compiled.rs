use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use handlebars::Handlebars;
use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::runtime::RunError;

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

pub struct NodeCompilation {
    pub body: CompiledBody,
    pub edges: Vec<String>,
    pub references: BTreeSet<String>,
    pub terminal: bool,
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
    pub terminal: bool,
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
    pub input_schema: Arc<JSONSchema>,
    pub entry: String,
    pub nodes: BTreeMap<String, CompiledNode>,
    pub templates: Arc<Handlebars<'static>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    pub data: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeTransition {
    Next,
    Goto(String),
    Complete(RunOutput),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeOutcome {
    pub output: Value,
    pub transition: NodeTransition,
}
