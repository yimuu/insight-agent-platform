//! Public wire models owned by the v1 HTTP API.

use insight_engine::history::types::RunRecord;
use insight_runtime::{RecoveryOperation, RecoveryReusePolicy, RecoveryRunResult};
use serde::{ser::Error as _, Serialize, Serializer};

pub use insight_engine::PersistenceMode;
pub use insight_runtime::terminal_only::{RecoveryCapability, RunPersistenceCapability};

pub(crate) const fn run_persistence_capability_for_mode(
    mode: PersistenceMode,
) -> RunPersistenceCapability {
    match mode {
        PersistenceMode::Full => RunPersistenceCapability::FULL,
        PersistenceMode::TerminalOnly => RunPersistenceCapability::TERMINAL_ONLY,
    }
}

/// Public Run projection with its failure and replay semantics made explicit.
#[derive(Debug, Clone)]
pub struct RunDto {
    pub run: RunRecord,
    pub capability: RunPersistenceCapability,
}

impl Serialize for RunDto {
    fn serialize<__S>(&self, serializer: __S) -> Result<__S::Ok, __S::Error>
    where
        __S: Serializer,
    {
        let mut run = serde_json::to_value(&self.run)
            .map_err(|error| __S::Error::custom(error.to_string()))?;
        let object = run
            .as_object_mut()
            .ok_or_else(|| __S::Error::custom("Run record must serialize as an object"))?;
        object.remove("response_id");
        let capability = serde_json::to_value(self.capability)
            .map_err(|error| __S::Error::custom(error.to_string()))?;
        let capability = capability
            .as_object()
            .ok_or_else(|| __S::Error::custom("Run capability must serialize as an object"))?;
        object.extend(capability.clone());
        run.serialize(serializer)
    }
}

impl RunDto {
    pub fn new(run: RunRecord, capability: RunPersistenceCapability) -> Self {
        Self { run, capability }
    }

    pub fn full(run: RunRecord) -> Self {
        Self::new(run, RunPersistenceCapability::FULL)
    }

    pub fn terminal_only(run: RunRecord) -> Self {
        Self::new(run, RunPersistenceCapability::TERMINAL_ONLY)
    }
}

/// Recovery response whose source and target carry the same explicit Run
/// capability fields as ordinary create/get/control responses.
#[derive(Debug, Clone, Serialize)]
pub struct RecoveryRunDto {
    pub operation: RecoveryOperation,
    pub request_id: String,
    pub reuse_policy: RecoveryReusePolicy,
    pub candidates_created: u32,
    pub source: RunDto,
    pub target: RunDto,
}

impl From<RecoveryRunResult> for RecoveryRunDto {
    fn from(result: RecoveryRunResult) -> Self {
        Self {
            operation: result.operation,
            request_id: result.request_id,
            reuse_policy: result.reuse_policy,
            candidates_created: result.candidates_created,
            // Recovery operations are unavailable to terminal-only Runs, so a
            // successful recovery result necessarily belongs to the full
            // persistence engine.
            source: RunDto::full(result.source),
            target: RunDto::full(result.target),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use insight_engine::{
        history::types::{RunAttachment, RunLifecycle, RunRecord},
        outcome::RunOutput,
    };
    use serde_json::json;

    use super::{RunDto, RunPersistenceCapability};

    fn completed_run() -> RunRecord {
        RunRecord {
            run_id: "run_terminal".to_owned(),
            response_id: "resp_terminal".to_owned(),
            projection_version: 0,
            request_id: "request_terminal".to_owned(),
            agent_id: "agent".to_owned(),
            agent_version: "deployment".to_owned(),
            attachment: RunAttachment::Detached,
            lifecycle: RunLifecycle::Completed {
                output: RunOutput {
                    content: None,
                    format: None,
                    data: json!({"answer": 42}),
                },
            },
            started_at: None,
            ended_at: None,
            updated_at: Utc::now(),
            input_summary: json!({"kind": "object"}),
        }
    }

    #[test]
    fn terminal_only_run_dto_exposes_failure_semantics() {
        let value = serde_json::to_value(RunDto::terminal_only(completed_run())).unwrap();
        assert_eq!(value["persistence_mode"], "terminal_only");
        assert_eq!(value["recovery_capability"], "none");
        assert_eq!(value["event_replay"], false);
        assert_eq!(value["run_id"], "run_terminal");
        assert!(value.get("response_id").is_none());
        assert_eq!(value["status"], "completed");
    }

    #[test]
    fn full_conversation_run_dto_exposes_restart_only_recovery() {
        let value = serde_json::to_value(RunDto::new(
            completed_run(),
            RunPersistenceCapability::FULL_CONVERSATION,
        ))
        .unwrap();
        assert_eq!(value["persistence_mode"], "full");
        assert_eq!(value["recovery_capability"], "restart_only");
        assert_eq!(value["event_replay"], true);
        assert_eq!(value["run_id"], "run_terminal");
        assert_eq!(value["status"], "completed");
    }
}
