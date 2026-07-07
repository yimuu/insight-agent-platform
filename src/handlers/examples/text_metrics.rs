use async_trait::async_trait;
use serde_json::{json, Value};

use crate::{
    code::registry::{CodeContext, CodeHandler},
    error::AppError,
};

#[derive(Clone, Copy)]
pub struct TextMetricsHandler;

#[async_trait]
impl CodeHandler for TextMetricsHandler {
    fn name(&self) -> &'static str {
        "example.text_metrics"
    }

    async fn call(&self, input: Value, ctx: CodeContext) -> Result<Value, AppError> {
        ctx.emit_text("Analyzing text metrics").await?;
        let text = input
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| AppError::Run("example.text_metrics requires input.text".to_string()))?;
        Ok(json!({
            "characters": text.chars().count(),
            "words": text.split_whitespace().count(),
            "empty": text.trim().is_empty(),
        }))
    }
}
