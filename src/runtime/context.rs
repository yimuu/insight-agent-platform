use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use handlebars::Handlebars;
use serde_json::{json, Value};

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
    node_outputs: BTreeMap<String, Value>,
    templates: Arc<Handlebars<'static>>,
}

impl RunContext {
    pub fn new(metadata: RunMetadata, input: Value) -> Self {
        Self {
            metadata,
            input,
            node_outputs: BTreeMap::new(),
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
        self.node_outputs.get(node_id)
    }

    pub fn templates(&self) -> &Handlebars<'static> {
        &self.templates
    }

    pub fn set_node_output(&mut self, node_id: impl Into<String>, output: Value) {
        self.node_outputs.insert(node_id.into(), output);
    }

    pub fn template_data(&self) -> Value {
        let nodes = self
            .node_outputs
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
}
