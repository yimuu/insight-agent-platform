//! Current time tool.

use async_trait::async_trait;
use chrono::Utc;
use chrono_tz::Tz;
use serde_json::{json, Value};

use crate::{
    error::AppError,
    tools::registry::{Tool, ToolContext},
};

#[derive(Debug, Clone, Copy)]
pub struct CurrentTimeTool;

#[async_trait]
impl Tool for CurrentTimeTool {
    fn name(&self) -> &'static str {
        "current_time"
    }

    async fn call(&self, args: Value, _ctx: ToolContext) -> Result<Value, AppError> {
        let timezone = args
            .get("timezone")
            .and_then(Value::as_str)
            .unwrap_or("UTC");
        let tz: Tz = timezone
            .parse()
            .map_err(|_| AppError::Run(format!("invalid timezone '{timezone}'")))?;
        let now = Utc::now().with_timezone(&tz);

        Ok(json!({
            "timezone": timezone,
            "iso8601": now.to_rfc3339(),
        }))
    }
}
