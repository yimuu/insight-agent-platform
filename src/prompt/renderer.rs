use handlebars::{Handlebars, RenderErrorReason};
use serde_json::Value;

use crate::error::AppError;

#[derive(Debug, Clone)]
pub struct PromptRenderer {
    handlebars: Handlebars<'static>,
}

impl PromptRenderer {
    pub fn new() -> Self {
        let mut handlebars = Handlebars::new();
        handlebars.set_strict_mode(true);
        Self { handlebars }
    }

    pub fn render(&self, template: &str, data: &Value) -> Result<String, AppError> {
        self.handlebars
            .render_template(template, data)
            .map_err(|err| match err.reason() {
                RenderErrorReason::MissingVariable(_) => {
                    AppError::Run(format!("prompt render error: {err}"))
                }
                _ => AppError::Run(format!("prompt render error: {err}")),
            })
    }
}

impl Default for PromptRenderer {
    fn default() -> Self {
        Self::new()
    }
}
