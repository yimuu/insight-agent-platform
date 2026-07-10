use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use handlebars::Handlebars;
use serde_json::{json, Value};

use super::BranchResult;

#[derive(Debug, Clone)]
pub struct RunMetadata {
    pub run_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub agent_version: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RunContext {
    metadata: RunMetadata,
    input: Value,
    base_node_outputs: Arc<BTreeMap<String, Value>>,
    local_node_outputs: BTreeMap<String, Value>,
    join_results: Option<Arc<BTreeMap<String, BranchResult>>>,
    templates: Arc<Handlebars<'static>>,
}

impl RunContext {
    pub fn new(metadata: RunMetadata, input: Value) -> Self {
        Self {
            metadata,
            input,
            base_node_outputs: Arc::new(BTreeMap::new()),
            local_node_outputs: BTreeMap::new(),
            join_results: None,
            templates: Arc::new(Handlebars::new()),
        }
    }

    pub fn with_templates(mut self, templates: Arc<Handlebars<'static>>) -> Self {
        self.templates = templates;
        self
    }

    pub fn metadata(&self) -> &RunMetadata {
        &self.metadata
    }

    pub fn input(&self) -> &Value {
        &self.input
    }

    pub fn node_output(&self, node_id: &str) -> Option<&Value> {
        self.local_node_outputs
            .get(node_id)
            .or_else(|| self.base_node_outputs.get(node_id))
    }

    pub fn templates(&self) -> &Handlebars<'static> {
        &self.templates
    }

    pub fn set_node_output(&mut self, node_id: impl Into<String>, output: Value) {
        self.local_node_outputs.insert(node_id.into(), output);
    }

    pub fn fork_branch(&self) -> Self {
        Self {
            metadata: self.metadata.clone(),
            input: self.input.clone(),
            base_node_outputs: Arc::new(self.visible_node_outputs()),
            local_node_outputs: BTreeMap::new(),
            join_results: None,
            templates: Arc::clone(&self.templates),
        }
    }

    pub fn with_join_results(&self, results: BTreeMap<String, BranchResult>) -> Self {
        Self {
            metadata: self.metadata.clone(),
            input: self.input.clone(),
            base_node_outputs: Arc::new(self.visible_node_outputs()),
            local_node_outputs: BTreeMap::new(),
            join_results: Some(Arc::new(results)),
            templates: Arc::clone(&self.templates),
        }
    }

    pub fn branch_results(&self) -> Option<&BTreeMap<String, BranchResult>> {
        self.join_results.as_deref()
    }

    pub fn template_data(&self) -> Value {
        let nodes = self
            .visible_node_outputs()
            .iter()
            .map(|(node_id, output)| (node_id.clone(), json!({"output": output})))
            .collect::<serde_json::Map<_, _>>();

        json!({
            "input": self.input,
            "run": {
                "id": self.metadata.run_id,
                "request_id": self.metadata.request_id,
                "agent_id": self.metadata.agent_id,
                "agent_version": self.metadata.agent_version,
                "started_at": self.metadata.started_at,
            },
            "nodes": nodes,
        })
    }

    fn visible_node_outputs(&self) -> BTreeMap<String, Value> {
        let mut outputs = self.base_node_outputs.as_ref().clone();
        outputs.extend(self.local_node_outputs.clone());
        outputs
    }
}
