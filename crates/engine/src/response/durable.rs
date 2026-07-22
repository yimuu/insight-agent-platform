//! Durable response snapshot contracts shared by repository and API layers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{repository::RepositoryError, ContentHash};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseUsageStatus {
    Complete,
    Partial,
    Unavailable,
}

impl ResponseUsageStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Unavailable => "unavailable",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "complete" => Ok(Self::Complete),
            "partial" => Ok(Self::Partial),
            "unavailable" => Ok(Self::Unavailable),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseTerminalKind {
    #[serde(rename = "response.completed")]
    Completed,
    #[serde(rename = "response.failed")]
    Failed,
    #[serde(rename = "workflow.response.timed_out")]
    TimedOut,
    #[serde(rename = "workflow.response.cancelled")]
    Cancelled,
    #[serde(rename = "workflow.response.interrupted")]
    Interrupted,
}

impl ResponseTerminalKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "response.completed",
            Self::Failed => "response.failed",
            Self::TimedOut => "workflow.response.timed_out",
            Self::Cancelled => "workflow.response.cancelled",
            Self::Interrupted => "workflow.response.interrupted",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "response.completed" => Ok(Self::Completed),
            "response.failed" => Ok(Self::Failed),
            "workflow.response.timed_out" => Ok(Self::TimedOut),
            "workflow.response.cancelled" => Ok(Self::Cancelled),
            "workflow.response.interrupted" => Ok(Self::Interrupted),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

/// Durable terminal calibration authority. Connection-local event type and
/// sequence numbers are deliberately absent and are assigned by the SSE
/// dispatcher when this snapshot is delivered.
#[derive(Debug, Clone, PartialEq)]
pub struct DurableResponseSnapshot {
    response_id: String,
    terminal_kind: ResponseTerminalKind,
    response: Value,
    workflow: Value,
    public_item_manifest: Value,
    usage: Option<Value>,
    usage_status: ResponseUsageStatus,
    snapshot_hash: ContentHash,
}

impl DurableResponseSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        response_id: String,
        terminal_kind: ResponseTerminalKind,
        response: Value,
        workflow: Value,
        public_item_manifest: Value,
        usage: Option<Value>,
        usage_status: ResponseUsageStatus,
        snapshot_hash: ContentHash,
    ) -> Result<Self, RepositoryError> {
        if response_id.is_empty()
            || !response.is_object()
            || !workflow.is_object()
            || !public_item_manifest.is_array()
            || usage.as_ref().is_some_and(|value| !value.is_object())
            || (usage_status == ResponseUsageStatus::Complete) != usage.is_some()
        {
            return Err(RepositoryError::invalid_data());
        }
        let hash_projection = serde_json::json!({
            "response_id": response_id,
            "terminal_kind": terminal_kind.as_str(),
            "response": response,
            "workflow": workflow,
            "public_item_manifest": public_item_manifest,
            "usage": usage,
            "usage_status": usage_status.as_str(),
        });
        let canonical =
            serde_jcs::to_vec(&hash_projection).map_err(|_| RepositoryError::canonicalization())?;
        let computed_hash = ContentHash::from_bytes(&canonical);
        if computed_hash != snapshot_hash {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            response_id,
            terminal_kind,
            response,
            workflow,
            public_item_manifest,
            usage,
            usage_status,
            snapshot_hash,
        })
    }

    pub fn response_id(&self) -> &str {
        &self.response_id
    }

    pub fn terminal_kind(&self) -> ResponseTerminalKind {
        self.terminal_kind
    }

    pub fn response(&self) -> &Value {
        &self.response
    }

    pub fn workflow(&self) -> &Value {
        &self.workflow
    }

    pub fn public_item_manifest(&self) -> &Value {
        &self.public_item_manifest
    }

    pub fn usage(&self) -> Option<&Value> {
        self.usage.as_ref()
    }

    pub fn usage_status(&self) -> ResponseUsageStatus {
        self.usage_status
    }

    pub fn snapshot_hash(&self) -> &ContentHash {
        &self.snapshot_hash
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn durable_response_snapshot_recomputes_its_complete_hash_authority() {
        let response_id = "resp_run_snapshot".to_owned();
        let response = json!({
            "id": response_id,
            "object": "response",
            "status": "completed",
            "output": [],
            "usage": {
                "input_tokens": 1,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": 2,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 3
            },
            "error": null
        });
        let workflow = json!({
            "run_id": "run_snapshot",
            "result": "ok",
            "tool_results": [],
            "retrievals": [],
            "usage_status": "complete"
        });
        let manifest = json!([]);
        let usage = response.get("usage").cloned();
        let projection = json!({
            "response_id": response_id,
            "terminal_kind": ResponseTerminalKind::Completed.as_str(),
            "response": response,
            "workflow": workflow,
            "public_item_manifest": manifest,
            "usage": usage,
            "usage_status": ResponseUsageStatus::Complete.as_str(),
        });
        let hash = ContentHash::from_bytes(&serde_jcs::to_vec(&projection).unwrap());

        DurableResponseSnapshot::new(
            response_id.clone(),
            ResponseTerminalKind::Completed,
            response.clone(),
            workflow.clone(),
            manifest.clone(),
            usage.clone(),
            ResponseUsageStatus::Complete,
            hash.clone(),
        )
        .unwrap();

        let mut tampered_response = response;
        tampered_response["status"] = json!("failed");
        assert!(DurableResponseSnapshot::new(
            response_id,
            ResponseTerminalKind::Completed,
            tampered_response,
            workflow,
            manifest,
            usage,
            ResponseUsageStatus::Complete,
            hash,
        )
        .is_err());
    }

    #[test]
    fn durable_response_snapshot_parse_helpers_are_closed() {
        assert_eq!(
            ResponseTerminalKind::parse("workflow.response.interrupted").unwrap(),
            ResponseTerminalKind::Interrupted
        );
        assert!(ResponseTerminalKind::parse("response.future").is_err());
        assert_eq!(
            ResponseUsageStatus::parse("unavailable").unwrap(),
            ResponseUsageStatus::Unavailable
        );
        assert!(ResponseUsageStatus::parse("unknown").is_err());
    }
}
