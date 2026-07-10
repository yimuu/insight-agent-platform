use std::{error::Error, fmt};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::events::protocol::RunEvent;

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

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError>;

    async fn list_events_after(
        &self,
        run_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError>;

    async fn mark_incomplete_interrupted(&self, at: DateTime<Utc>) -> Result<u64, HistoryError>;
}
