use chrono::{DateTime, Utc};
use tokio::time::Instant;

/// Immutable identity and timing metadata shared by one workflow Run.
#[derive(Debug, Clone)]
pub struct RunMetadata {
    pub run_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub agent_version: String,
    pub started_at: DateTime<Utc>,
    /// Monotonic execution deadline. This runtime-only value is never exposed
    /// through the authored `run` object or a public event.
    pub execution_deadline: Instant,
}
