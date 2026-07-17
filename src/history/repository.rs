use std::{error::Error, fmt};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use crate::{
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    outcome::RunOutput,
};

use super::types::{NewRun, RunRecord, RunTerminal, TerminalUpdate};

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

#[derive(Debug, Clone, PartialEq)]
pub struct TerminalProposal {
    scope: RunEventScope,
    update: TerminalUpdate,
}

impl TerminalProposal {
    pub fn new(scope: RunEventScope, update: TerminalUpdate) -> Result<Self, HistoryError> {
        if scope.run_id != update.run_id {
            return Err(HistoryError::new(
                "HISTORY_EVENT_INVALID",
                "terminal event scope does not match its typed update",
            ));
        }
        Ok(Self { scope, update })
    }

    pub fn scope(&self) -> &RunEventScope {
        &self.scope
    }

    pub fn update(&self) -> &TerminalUpdate {
        &self.update
    }

    pub fn run_id(&self) -> &str {
        &self.update.run_id
    }

    pub fn into_parts(self) -> (RunEventScope, TerminalUpdate) {
        (self.scope, self.update)
    }

    pub fn event_at(&self, seq: u64) -> RunEvent {
        match &self.update.terminal {
            RunTerminal::Completed { output } => RunEvent::ok_at(
                RunEventType::RunCompleted,
                seq,
                self.scope.clone(),
                self.update.ended_at,
                completed_data(output),
            ),
            RunTerminal::Failed { error } => RunEvent::error_at(
                RunEventType::RunFailed,
                seq,
                self.scope.clone(),
                self.update.ended_at,
                error.code.clone(),
                error.message.clone(),
                json!({"kind": error.kind}),
            ),
            RunTerminal::Cancelled { error } => RunEvent::error_at(
                RunEventType::RunCancelled,
                seq,
                self.scope.clone(),
                self.update.ended_at,
                error.code.clone(),
                error.message.clone(),
                json!({}),
            ),
            RunTerminal::Interrupted { error } => RunEvent::error_at(
                RunEventType::RunInterrupted,
                seq,
                self.scope.clone(),
                self.update.ended_at,
                error.code.clone(),
                error.message.clone(),
                json!({}),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalSequence {
    Expected(u64),
    NextDurable,
}

fn completed_data(output: &RunOutput) -> Value {
    let mut data = serde_json::Map::new();
    data.insert("data".to_string(), output.data.clone());
    if let Some(content) = &output.content {
        data.insert("content".to_string(), Value::String(content.clone()));
    }
    if let Some(format) = &output.format {
        data.insert("format".to_string(), Value::String(format.clone()));
    }
    Value::Object(data)
}

#[async_trait]
pub trait RunRepository: Send + Sync {
    async fn check_health(&self) -> Result<(), HistoryError>;

    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError>;

    async fn mark_running(
        &self,
        run_id: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), HistoryError>;

    async fn append_events(&self, events: &[RunEvent]) -> Result<(), HistoryError>;

    /// Atomically commits the proposed Run terminal and its event, or returns the
    /// exact validated durable terminal event when another proposal already won.
    /// Concurrent losers never overwrite the winner; callers distinguish requested
    /// from authoritative resolution by comparing the complete returned event with
    /// the candidate projected from the proposal at the intended sequence.
    async fn commit_terminal(
        &self,
        proposal: TerminalProposal,
        sequence: TerminalSequence,
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
