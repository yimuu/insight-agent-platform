use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use crate::request_context::RequestContext;

#[derive(Debug, Clone)]
pub struct RunContext {
    pub request: RequestContext,
    pub run_id: String,
    pub agent_id: String,
    pub started_at: DateTime<Utc>,
    pub input: Value,
    pub step_outputs: BTreeMap<String, Value>,
}

impl RunContext {
    pub fn template_data(&self) -> Value {
        let steps = self
            .step_outputs
            .iter()
            .map(|(id, output)| (id.clone(), json!({ "output": output })))
            .collect::<serde_json::Map<_, _>>();

        json!({
            "run": {
                "request_id": self.request.request_id,
                "id": self.run_id,
                "agent_id": self.agent_id,
                "started_at": self.started_at,
            },
            "request": self.request,
            "input": self.input,
            "steps": steps,
        })
    }

    pub fn set_step_output(&mut self, step_id: &str, output: Value) {
        self.step_outputs
            .insert(step_id.to_string(), normalize_step_output(output));
    }
}

fn normalize_step_output(output: Value) -> Value {
    match output {
        Value::String(text) => json!({ "text": text }),
        Value::Object(object) => Value::Object(object),
        other => json!({ "value": other }),
    }
}
