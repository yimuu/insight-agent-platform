use std::{error::Error, fmt};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::events::protocol::{RunEvent, RunEventType};

use super::types::{NewRun, NodeOutputRecord, RunRecord, TerminalUpdate};

pub struct HistoryError {
    code: &'static str,
    message: String,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl HistoryError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source<E>(code: &'static str, message: impl Into<String>, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            code,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Debug for HistoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistoryError")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for HistoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for HistoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[async_trait]
pub trait RunRepository: Send + Sync {
    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError>;

    async fn mark_running(
        &self,
        run_id: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), HistoryError>;

    async fn append_events(&self, events: &[RunEvent]) -> Result<(), HistoryError>;

    async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), HistoryError>;

    async fn finish_run(
        &self,
        update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<bool, HistoryError>;

    /// Atomically locks the run, derives the next sequence from durable state,
    /// inserts the terminal event, and transitions the run. If the run is
    /// already terminal, returns its authoritative stored terminal event.
    async fn recover_run(
        &self,
        update: TerminalUpdate,
        terminal: RunEvent,
    ) -> Result<RunEvent, HistoryError>;

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError>;

    async fn list_events_after(
        &self,
        run_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError>;

    async fn mark_incomplete_interrupted(&self, at: DateTime<Utc>) -> Result<u64, HistoryError>;
}

pub(crate) fn validate_recovery_event(
    update: &TerminalUpdate,
    terminal: &RunEvent,
) -> Result<(), HistoryError> {
    let expected_type = match update.status() {
        super::types::RunStatus::Completed => RunEventType::RunCompleted,
        super::types::RunStatus::Failed => RunEventType::RunFailed,
        super::types::RunStatus::Cancelled => RunEventType::RunCancelled,
        super::types::RunStatus::Interrupted => RunEventType::RunInterrupted,
        _ => {
            return Err(HistoryError::new(
                "HISTORY_RECOVERY_INVALID",
                "recovery update must be terminal",
            ));
        }
    };
    if terminal.event_type != expected_type
        || terminal.node_id.is_some()
        || terminal.run_id != update.run_id
    {
        return Err(HistoryError::new(
            "HISTORY_RECOVERY_INVALID",
            "recovery terminal event does not match its update",
        ));
    }
    Ok(())
}
