//! Durable `run-stream/v1` snapshot contracts shared by repository and API layers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{repository::RepositoryError, ContentHash};

use super::{
    validate_completed_run, validate_failed_run, validate_stopped_run, RunCompletedSnapshot,
    RunFailedSnapshot, RunStatus, RunStoppedSnapshot, RUN_STREAM_PROTOCOL_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunUsageStatus {
    Complete,
    Partial,
    Unavailable,
}

impl RunUsageStatus {
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
pub enum RunTerminalKind {
    #[serde(rename = "run.lifecycle.completed")]
    Completed,
    #[serde(rename = "run.lifecycle.failed")]
    Failed,
    #[serde(rename = "run.lifecycle.timed_out")]
    TimedOut,
    #[serde(rename = "run.lifecycle.cancelled")]
    Cancelled,
    #[serde(rename = "run.lifecycle.interrupted")]
    Interrupted,
}

impl RunTerminalKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "run.lifecycle.completed",
            Self::Failed => "run.lifecycle.failed",
            Self::TimedOut => "run.lifecycle.timed_out",
            Self::Cancelled => "run.lifecycle.cancelled",
            Self::Interrupted => "run.lifecycle.interrupted",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "run.lifecycle.completed" => Ok(Self::Completed),
            "run.lifecycle.failed" => Ok(Self::Failed),
            "run.lifecycle.timed_out" => Ok(Self::TimedOut),
            "run.lifecycle.cancelled" => Ok(Self::Cancelled),
            "run.lifecycle.interrupted" => Ok(Self::Interrupted),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

/// Durable terminal calibration authority. Connection-local event type and
/// sequence numbers are deliberately absent and are assigned by the SSE
/// dispatcher when this snapshot is delivered.
#[derive(Debug, Clone, PartialEq)]
pub struct DurableRunStreamSnapshot {
    run_id: String,
    terminal_kind: RunTerminalKind,
    run: Value,
    public_item_manifest: Value,
    snapshot_hash: ContentHash,
}

impl DurableRunStreamSnapshot {
    pub(super) fn new(
        run_id: String,
        terminal_kind: RunTerminalKind,
        run: Value,
        public_item_manifest: Value,
        snapshot_hash: ContentHash,
    ) -> Result<Self, RepositoryError> {
        if run_id.is_empty() || !run.is_object() || !public_item_manifest.is_array() {
            return Err(RepositoryError::invalid_data());
        }

        validate_terminal_run(&run_id, terminal_kind, &run)?;

        let hash_projection = serde_json::json!({
            "protocol": RUN_STREAM_PROTOCOL_VERSION,
            "run_id": run_id,
            "terminal_kind": terminal_kind.as_str(),
            "run": run,
            "public_item_manifest": public_item_manifest,
        });
        let canonical =
            serde_jcs::to_vec(&hash_projection).map_err(|_| RepositoryError::canonicalization())?;
        let computed_hash = ContentHash::from_bytes(&canonical);
        if computed_hash != snapshot_hash {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            run_id,
            terminal_kind,
            run,
            public_item_manifest,
            snapshot_hash,
        })
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn terminal_kind(&self) -> RunTerminalKind {
        self.terminal_kind
    }

    pub fn run(&self) -> &Value {
        &self.run
    }

    pub fn public_item_manifest(&self) -> &Value {
        &self.public_item_manifest
    }

    pub fn snapshot_hash(&self) -> &ContentHash {
        &self.snapshot_hash
    }
}

fn validate_terminal_run(
    run_id: &str,
    terminal_kind: RunTerminalKind,
    run: &Value,
) -> Result<(), RepositoryError> {
    let (id, status) = match terminal_kind {
        RunTerminalKind::Completed => {
            let snapshot = serde_json::from_value::<RunCompletedSnapshot>(run.clone())
                .map_err(|_| RepositoryError::invalid_data())?;
            validate_completed_run(&snapshot).map_err(|_| RepositoryError::invalid_data())?;
            require_exact_run_payload(run, &snapshot)?;
            (snapshot.id, snapshot.status)
        }
        RunTerminalKind::Failed | RunTerminalKind::TimedOut => {
            let snapshot = serde_json::from_value::<RunFailedSnapshot>(run.clone())
                .map_err(|_| RepositoryError::invalid_data())?;
            let expected_status = match terminal_kind {
                RunTerminalKind::Failed => RunStatus::Failed,
                RunTerminalKind::TimedOut => RunStatus::TimedOut,
                _ => unreachable!(),
            };
            validate_failed_run(&snapshot, expected_status)
                .map_err(|_| RepositoryError::invalid_data())?;
            require_exact_run_payload(run, &snapshot)?;
            (snapshot.id, snapshot.status)
        }
        RunTerminalKind::Cancelled | RunTerminalKind::Interrupted => {
            let snapshot = serde_json::from_value::<RunStoppedSnapshot>(run.clone())
                .map_err(|_| RepositoryError::invalid_data())?;
            let expected_status = match terminal_kind {
                RunTerminalKind::Cancelled => RunStatus::Cancelled,
                RunTerminalKind::Interrupted => RunStatus::Interrupted,
                _ => unreachable!(),
            };
            validate_stopped_run(&snapshot, expected_status)
                .map_err(|_| RepositoryError::invalid_data())?;
            require_exact_run_payload(run, &snapshot)?;
            (snapshot.id, snapshot.status)
        }
    };
    let expected_status = match terminal_kind {
        RunTerminalKind::Completed => RunStatus::Completed,
        RunTerminalKind::Failed => RunStatus::Failed,
        RunTerminalKind::TimedOut => RunStatus::TimedOut,
        RunTerminalKind::Cancelled => RunStatus::Cancelled,
        RunTerminalKind::Interrupted => RunStatus::Interrupted,
    };
    if id != run_id || status != expected_status {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

fn require_exact_run_payload<T: Serialize>(
    original: &Value,
    typed: &T,
) -> Result<(), RepositoryError> {
    let projected = serde_json::to_value(typed).map_err(|_| RepositoryError::canonicalization())?;
    if &projected != original {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn durable_run_stream_snapshot_recomputes_its_complete_hash_authority() {
        let run_id = "run_snapshot".to_owned();
        let run = json!({
            "id": run_id,
            "object": "run",
            "status": "completed",
            "output": [],
            "result": "ok",
            "tool_results": [],
            "retrievals": [],
            "interactions": [],
            "usage": {
                "input_tokens": 1,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": 2,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 3
            },
            "usage_status": "complete"
        });
        let manifest = json!([]);
        let projection = json!({
            "protocol": RUN_STREAM_PROTOCOL_VERSION,
            "run_id": run_id,
            "terminal_kind": RunTerminalKind::Completed.as_str(),
            "run": run,
            "public_item_manifest": manifest,
        });
        let hash = ContentHash::from_bytes(&serde_jcs::to_vec(&projection).unwrap());

        DurableRunStreamSnapshot::new(
            run_id.clone(),
            RunTerminalKind::Completed,
            run.clone(),
            manifest.clone(),
            hash.clone(),
        )
        .unwrap();

        for required_field in ["usage", "tool_results"] {
            let mut incomplete = run.clone();
            incomplete.as_object_mut().unwrap().remove(required_field);
            let incomplete_projection = json!({
                "protocol": RUN_STREAM_PROTOCOL_VERSION,
                "run_id": run_id,
                "terminal_kind": RunTerminalKind::Completed.as_str(),
                "run": incomplete,
                "public_item_manifest": manifest,
            });
            let incomplete_hash =
                ContentHash::from_bytes(&serde_jcs::to_vec(&incomplete_projection).unwrap());
            assert!(DurableRunStreamSnapshot::new(
                run_id.clone(),
                RunTerminalKind::Completed,
                incomplete,
                manifest.clone(),
                incomplete_hash,
            )
            .is_err());
        }

        let legacy_projection = json!({
            "protocol": "response-stream/v1",
            "run_id": run_id,
            "terminal_kind": RunTerminalKind::Completed.as_str(),
            "run": run,
            "public_item_manifest": manifest,
        });
        let legacy_hash = ContentHash::from_bytes(&serde_jcs::to_vec(&legacy_projection).unwrap());
        assert!(DurableRunStreamSnapshot::new(
            run_id.clone(),
            RunTerminalKind::Completed,
            run.clone(),
            manifest.clone(),
            legacy_hash,
        )
        .is_err());

        let mut tampered_run = run;
        tampered_run["status"] = json!("failed");
        assert!(DurableRunStreamSnapshot::new(
            run_id,
            RunTerminalKind::Completed,
            tampered_run,
            manifest,
            hash,
        )
        .is_err());
    }

    #[test]
    fn durable_run_stream_snapshot_parse_helpers_are_closed() {
        assert_eq!(
            RunTerminalKind::parse("run.lifecycle.interrupted").unwrap(),
            RunTerminalKind::Interrupted
        );
        assert!(RunTerminalKind::parse("response.future").is_err());
        assert_eq!(
            RunUsageStatus::parse("unavailable").unwrap(),
            RunUsageStatus::Unavailable
        );
        assert!(RunUsageStatus::parse("unknown").is_err());
    }
}
