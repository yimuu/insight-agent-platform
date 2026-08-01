//! Durable MCP management control-plane contracts.
//!
//! The repository owns lifecycle, CAS, idempotency, immutable publication,
//! discovery leases, and disable fences. Network discovery and policy
//! validation deliberately happen outside database transactions.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RepositoryError;

pub const MCP_MANAGEMENT_MAX_REQUEST_ID_BYTES: usize = 256;
pub const MCP_MANAGEMENT_MAX_OPERATOR_ID_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpManagedServerState {
    Draft,
    Active,
    Disabled,
    Retired,
}

impl McpManagedServerState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Disabled => "disabled",
            Self::Retired => "retired",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpDiscoveryStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl McpDiscoveryStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpManagedServer {
    pub server_id: String,
    pub display_name: String,
    pub state: McpManagedServerState,
    pub server_version: u64,
    pub draft_version: u64,
    pub active_revision_id: Option<String>,
    pub disable_fence: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpStoredDraft {
    pub server_id: String,
    pub draft_version: u64,
    pub discovery_input_hash: String,
    pub document: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSignedManifest {
    pub manifest_id: String,
    pub server_id: String,
    pub format: String,
    pub key_id: String,
    pub payload: String,
    pub signature: String,
    pub content_hash: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryFailure {
    pub code: String,
    pub stage: String,
    pub retryable: bool,
    pub correlation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryOperation {
    pub discovery_id: String,
    pub server_id: String,
    pub source_draft_version: u64,
    pub discovery_input_hash: String,
    pub status: McpDiscoveryStatus,
    pub cancel_requested: bool,
    pub attempts: u32,
    pub claimed_by: Option<String>,
    pub claim_token: Option<String>,
    pub claim_expires_at: Option<DateTime<Utc>>,
    pub failure: Option<McpDiscoveryFailure>,
    pub stale: bool,
    pub stale_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoverySnapshot {
    pub discovery_id: String,
    pub server_id: String,
    pub source_draft_version: u64,
    pub discovery_input_hash: String,
    pub catalog_fingerprint: String,
    pub document: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpValidationReport {
    pub validation_id: String,
    pub server_id: String,
    pub draft_version: u64,
    pub discovery_id: String,
    pub report_hash: String,
    pub valid: bool,
    pub document: Value,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerRevision {
    pub revision_id: String,
    pub server_id: String,
    pub revision_number: u64,
    pub source_draft_version: u64,
    pub discovery_id: String,
    pub validation_id: String,
    pub catalog_fingerprint: String,
    pub revision_hash: String,
    pub document: Value,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpManagementPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpMutationReceipt {
    pub replayed: bool,
    pub status: u16,
    pub response: Value,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpManagementConflict {
    NotFound,
    ForbiddenState,
    PreconditionFailed,
    IdempotencyKeyReused,
    Referenced,
    DiscoveryStale,
    CandidateMismatch,
    ValidationFailed,
    CapacityExceeded,
    FenceLost,
}

#[derive(Debug)]
pub enum McpManagementWriteError {
    Conflict(McpManagementConflict),
    Repository(RepositoryError),
}

impl From<RepositoryError> for McpManagementWriteError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value)
    }
}

impl std::fmt::Display for McpManagementWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(_) => formatter.write_str("MCP management state conflict"),
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for McpManagementWriteError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpMutationMetadata {
    pub operator_id: String,
    pub method: String,
    pub canonical_path: String,
    pub request_id: String,
    pub request_hash: String,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMcpServerCommand {
    pub metadata: McpMutationMetadata,
    pub server_id: String,
    pub display_name: String,
    pub draft_document: Value,
    pub discovery_input_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplaceMcpDraftCommand {
    pub metadata: McpMutationMetadata,
    pub server_id: String,
    pub expected_draft_version: u64,
    pub draft_document: Value,
    pub discovery_input_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteMcpServerCommand {
    pub metadata: McpMutationMetadata,
    pub server_id: String,
    pub expected_server_version: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMcpManifestCommand {
    pub metadata: McpMutationMetadata,
    pub manifest: McpSignedManifest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMcpDiscoveryCommand {
    pub metadata: McpMutationMetadata,
    pub discovery_id: String,
    pub server_id: String,
    pub expected_draft_version: u64,
    pub discovery_input_hash: String,
    pub max_pending_discoveries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelMcpDiscoveryCommand {
    pub metadata: McpMutationMetadata,
    pub server_id: String,
    pub discovery_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMcpValidationCommand {
    pub metadata: McpMutationMetadata,
    pub report: McpValidationReport,
    pub expected_draft_version: u64,
    pub expected_discovery_input_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishMcpRevisionCommand {
    pub metadata: McpMutationMetadata,
    pub revision_id: String,
    pub server_id: String,
    pub expected_draft_version: u64,
    pub discovery_id: String,
    pub validation_id: String,
    pub revision_hash: String,
    pub document: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateMcpRevisionCommand {
    pub metadata: McpMutationMetadata,
    pub server_id: String,
    pub revision_id: String,
    pub expected_server_version: u64,
    pub readiness_hash: String,
    pub readiness_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisableMcpServerCommand {
    pub metadata: McpMutationMetadata,
    pub server_id: String,
    pub expected_server_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireMcpServerCommand {
    pub metadata: McpMutationMetadata,
    pub server_id: String,
    pub expected_server_version: u64,
    pub reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimMcpDiscoveriesCommand {
    pub worker_id: String,
    pub now: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryClaim {
    pub operation: McpDiscoveryOperation,
    pub claim_token: String,
    pub draft_document: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompleteMcpDiscoveryResult {
    Succeeded {
        catalog_fingerprint: String,
        snapshot_document: Value,
    },
    Failed(McpDiscoveryFailure),
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteMcpDiscoveryCommand {
    pub discovery_id: String,
    pub claim_token: String,
    pub now: DateTime<Utc>,
    pub result: CompleteMcpDiscoveryResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkMcpDiscoveryStaleCommand {
    pub server_id: String,
    pub reason_code: String,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMcpManagementRejectionCommand {
    pub actor_id: String,
    pub request_id: String,
    pub server_id: Option<String>,
    pub subject_id: String,
    pub result_code: String,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpManagementRuntimeStats {
    pub pending_discoveries: u64,
    pub running_discoveries: u64,
    pub oldest_open_discovery_at: Option<DateTime<Utc>>,
    pub active_servers: u64,
    pub disabled_servers: u64,
    pub stale_servers: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerFence {
    pub server_id: String,
    pub state: McpManagedServerState,
    pub active_revision_id: Option<String>,
    pub disable_fence: u64,
}

#[async_trait]
pub trait McpManagementDurableRepository: Send + Sync {
    /// Records a recognized Operator request rejected before a successful
    /// durable mutation. The implementation stores only bounded identity,
    /// hashes and stable result metadata; request/response bodies are absent.
    async fn record_mcp_management_rejection(
        &self,
        command: RecordMcpManagementRejectionCommand,
    ) -> Result<(), RepositoryError>;

    async fn load_mcp_management_runtime_stats(
        &self,
    ) -> Result<McpManagementRuntimeStats, RepositoryError>;

    async fn replay_mcp_mutation(
        &self,
        metadata: &McpMutationMetadata,
    ) -> Result<Option<McpMutationReceipt>, McpManagementWriteError>;

    async fn create_mcp_server(
        &self,
        command: CreateMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn replace_mcp_draft(
        &self,
        command: ReplaceMcpDraftCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn delete_mcp_server(
        &self,
        command: DeleteMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn get_mcp_server(
        &self,
        server_id: &str,
    ) -> Result<Option<McpManagedServer>, RepositoryError>;

    async fn get_mcp_draft(
        &self,
        server_id: &str,
    ) -> Result<Option<McpStoredDraft>, RepositoryError>;

    async fn list_mcp_servers(
        &self,
        state: Option<McpManagedServerState>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpManagedServer>, RepositoryError>;

    async fn create_mcp_manifest(
        &self,
        command: CreateMcpManifestCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn get_mcp_manifest(
        &self,
        server_id: &str,
        manifest_id: &str,
    ) -> Result<Option<McpSignedManifest>, RepositoryError>;

    async fn list_mcp_manifests(
        &self,
        server_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpSignedManifest>, RepositoryError>;

    async fn create_mcp_discovery(
        &self,
        command: CreateMcpDiscoveryCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn cancel_mcp_discovery(
        &self,
        command: CancelMcpDiscoveryCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn get_mcp_discovery(
        &self,
        server_id: &str,
        discovery_id: &str,
    ) -> Result<Option<McpDiscoveryOperation>, RepositoryError>;

    async fn get_mcp_discovery_snapshot(
        &self,
        server_id: &str,
        discovery_id: &str,
    ) -> Result<Option<McpDiscoverySnapshot>, RepositoryError>;

    async fn list_mcp_discoveries(
        &self,
        server_id: &str,
        status: Option<McpDiscoveryStatus>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpDiscoveryOperation>, RepositoryError>;

    async fn claim_mcp_discoveries(
        &self,
        command: ClaimMcpDiscoveriesCommand,
    ) -> Result<Vec<McpDiscoveryClaim>, RepositoryError>;

    async fn complete_mcp_discovery(
        &self,
        command: CompleteMcpDiscoveryCommand,
    ) -> Result<(), McpManagementWriteError>;

    async fn mark_mcp_discovery_stale(
        &self,
        command: MarkMcpDiscoveryStaleCommand,
    ) -> Result<u64, RepositoryError>;

    async fn cleanup_terminal_mcp_discoveries(
        &self,
        finished_before: DateTime<Utc>,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError>;

    async fn create_mcp_validation(
        &self,
        command: CreateMcpValidationCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn get_mcp_validation(
        &self,
        server_id: &str,
        validation_id: &str,
    ) -> Result<Option<McpValidationReport>, RepositoryError>;

    async fn publish_mcp_revision(
        &self,
        command: PublishMcpRevisionCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn activate_mcp_revision(
        &self,
        command: ActivateMcpRevisionCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn disable_mcp_server(
        &self,
        command: DisableMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn retire_mcp_server(
        &self,
        command: RetireMcpServerCommand,
    ) -> Result<McpMutationReceipt, McpManagementWriteError>;

    async fn get_mcp_revision(
        &self,
        server_id: &str,
        revision_id: &str,
    ) -> Result<Option<McpServerRevision>, RepositoryError>;

    async fn list_mcp_revisions(
        &self,
        server_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<McpManagementPage<McpServerRevision>, RepositoryError>;

    async fn load_active_mcp_revisions(
        &self,
    ) -> Result<Vec<(McpManagedServer, McpServerRevision)>, RepositoryError>;

    async fn load_mcp_server_fence(
        &self,
        server_id: &str,
    ) -> Result<Option<McpServerFence>, RepositoryError>;
}
