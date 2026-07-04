use std::collections::BTreeMap;

use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct PromptStore {
    prompts: BTreeMap<String, String>,
}

impl PromptStore {
    pub fn new(prompts: BTreeMap<String, String>) -> Self {
        Self { prompts }
    }

    pub fn resolve_ref(&self, name: &str) -> Result<&str, AppError> {
        self.prompts
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| AppError::Config(format!("unknown prompt ref '{name}'")))
    }
}
