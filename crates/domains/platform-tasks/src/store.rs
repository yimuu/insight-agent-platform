//! Durable Task read projection shared by ports and storage implementations.
use crate::{TaskAction, TaskKind, TaskQueryPurpose, TaskState};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{TraceIdentityV1, TypedPayload};

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRecord {
    pub tenant_id: String,
    pub task_id: String,
    pub trace: TraceIdentityV1,
    pub task_kind: TaskKind,
    pub owner_kind: String,
    pub owner_id: String,
    pub run_id: Option<String>,
    pub node_id: Option<String>,
    pub invocation_id: Option<String>,
    pub state: TaskState,
    pub generation: i64,
    pub version: i64,
    pub response_schema_digest: Option<String>,
    pub payload: TypedPayload,
    pub response_value_id: Option<String>,
    pub deadline: DateTime<Utc>,
    pub responded_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct TaskAccessRecord {
    pub task: TaskRecord,
    pub allowed_actions: Vec<TaskAction>,
}

/// Bounded direct-authority inbox scan. Eligibility remains a pure domain rule.
#[derive(Debug, Clone)]
pub struct TaskInboxQuery {
    pub tenant_id: insight_platform_contracts::ResourceId,
    pub principal_id: insight_platform_contracts::ResourceId,
    pub principal_kind: insight_platform_contracts::PrincipalKind,
    pub purpose: TaskQueryPurpose,
    pub state: Option<TaskState>,
    pub kind: Option<TaskKind>,
    pub run_id: Option<insight_platform_contracts::ResourceId>,
    pub snapshot_at: Option<DateTime<Utc>>,
    pub boundary: Option<(DateTime<Utc>, insight_platform_contracts::ResourceId)>,
    pub page_size: u16,
}
#[derive(Debug, Clone)]
pub struct TaskInboxPage {
    pub snapshot_at: DateTime<Utc>,
    pub records: Vec<TaskAccessRecord>,
    /// The last examined row, even when no row in this page was eligible.
    pub next_scanned_boundary: Option<(DateTime<Utc>, insight_platform_contracts::ResourceId)>,
}
pub const TASK_INBOX_MAX_SCAN: i64 = 200;

/// Current controller versions returned only after Task eligibility authorization.
#[derive(Debug, Clone)]
pub struct CapabilityTaskControl {
    pub task: TaskRecord,
    pub invocation_version: u64,
    pub job_version: u64,
}
