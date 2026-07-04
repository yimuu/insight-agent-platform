use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct RunContext {
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
                "id": self.run_id,
                "agent_id": self.agent_id,
                "started_at": self.started_at,
            },
            "input": self.input,
            "steps": steps,
        })
    }

    pub fn set_step_output(&mut self, step_id: &str, output: Value) {
        self.step_outputs.insert(step_id.to_string(), output);
    }
}
