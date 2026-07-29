//! Lightweight terminal-only Run and Conversation storage contracts.
//!
//! These ports are intentionally separate from `DurableRepository`: a
//! terminal-only runtime has no event, checkpoint, claim, or replay authority
//! to call accidentally.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_engine::{
    repository::RepositoryError, run_stream::RunToolResult, ContentHash, DefinitionRevisionId,
    DeploymentRevisionId, PersistenceMode, RunId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const TERMINAL_RUN_OWNER_LEASE_LOST: &str = "TERMINAL_RUN_OWNER_LEASE_LOST";
pub const TERMINAL_RUN_NOT_FOUND: &str = "TERMINAL_RUN_NOT_FOUND";
pub const CONVERSATION_NOT_FOUND: &str = "CONVERSATION_NOT_FOUND";
pub const CONVERSATION_ARCHIVED: &str = "CONVERSATION_ARCHIVED";
pub const CONVERSATION_OWNERSHIP_MISMATCH: &str = "CONVERSATION_OWNERSHIP_MISMATCH";
pub const TERMINAL_SCOPED_ARTIFACT_MEDIA_TYPE: &str =
    "application/vnd.insight.terminal-object.v1+json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOwner {
    pub instance_id: String,
    pub owner_epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeInstanceLease {
    pub owner: RuntimeOwner,
    pub endpoint: String,
    pub lease_expires_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
}

pub type RegisterRuntimeInstance = RuntimeInstanceLease;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatRuntimeInstance {
    pub owner: RuntimeOwner,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerLeaseQuery {
    pub owner: RuntimeOwner,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OwnerLeaseStatus {
    Active { lease: RuntimeInstanceLease },
    Expired,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OwnerLeaseHeartbeat {
    Renewed { lease: RuntimeInstanceLease },
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionConversation {
    pub conversation_id: String,
    pub user_message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewTerminalRunAdmission {
    pub run_id: RunId,
    pub tenant_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub definition_revision_id: DefinitionRevisionId,
    pub deployment_revision_id: DeploymentRevisionId,
    pub conversation: Option<AdmissionConversation>,
    pub input_ref: Option<String>,
    pub input_hash: ContentHash,
    pub selected_context_hash: Option<ContentHash>,
    pub owner: RuntimeOwner,
    pub accepted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalRunAdmission {
    pub run_id: RunId,
    pub tenant_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub definition_revision_id: DefinitionRevisionId,
    pub deployment_revision_id: DeploymentRevisionId,
    pub conversation: Option<AdmissionConversation>,
    pub input_ref: Option<String>,
    pub input_hash: ContentHash,
    pub selected_context_hash: Option<ContentHash>,
    pub owner: RuntimeOwner,
    pub accepted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionOutcome {
    pub admission: TerminalRunAdmission,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalState {
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl TerminalState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }

    fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "timed_out" => Ok(Self::TimedOut),
            _ => Err(invalid_data()),
        }
    }
}

#[doc(hidden)]
pub fn terminal_state_as_str(value: TerminalState) -> &'static str {
    value.as_str()
}

#[doc(hidden)]
pub fn parse_terminal_state(value: &str) -> Result<TerminalState, RepositoryError> {
    TerminalState::parse(value)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewTerminalRunResult {
    pub run_id: RunId,
    pub owner: RuntimeOwner,
    pub terminal_state: TerminalState,
    pub response_id: String,
    pub output_ref: Option<String>,
    pub output_hash: Option<ContentHash>,
    pub error_code: Option<String>,
    pub usage_json: Option<Value>,
    pub tool_results: Vec<RunToolResult>,
    pub started_at: DateTime<Utc>,
    pub terminal_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TerminalRunResult {
    pub run_id: RunId,
    pub terminal_state: TerminalState,
    pub response_id: String,
    pub output_ref: Option<String>,
    pub output_hash: Option<ContentHash>,
    pub error_code: Option<String>,
    pub usage_json: Option<Value>,
    pub tool_results: Vec<RunToolResult>,
    pub started_at: DateTime<Utc>,
    pub terminal_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TerminalCommitOutcome {
    pub result: TerminalRunResult,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalRunDerivedState {
    Active,
    Interrupted,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl From<TerminalState> for TerminalRunDerivedState {
    fn from(value: TerminalState) -> Self {
        match value {
            TerminalState::Succeeded => Self::Succeeded,
            TerminalState::Failed => Self::Failed,
            TerminalState::Cancelled => Self::Cancelled,
            TerminalState::TimedOut => Self::TimedOut,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalRunQuery {
    pub tenant_id: String,
    pub run_id: RunId,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalRunRequestQuery {
    pub tenant_id: String,
    pub request_id: String,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TerminalRunView {
    pub admission: TerminalRunAdmission,
    pub result: Option<TerminalRunResult>,
    pub state: TerminalRunDerivedState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundedRetention {
    pub before: DateTime<Utc>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewConversation {
    pub conversation_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub agent_id: String,
    pub persistence_mode: PersistenceMode,
    pub deployment_revision_id: DeploymentRevisionId,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conversation {
    pub conversation_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub agent_id: String,
    #[serde(skip_serializing)]
    pub persistence_mode: PersistenceMode,
    #[serde(skip_serializing)]
    pub deployment_revision_id: DeploymentRevisionId,
    pub created_at: DateTime<Utc>,
    pub archived_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateConversationOutcome {
    pub conversation: Conversation,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationQuery {
    pub conversation_id: String,
    pub tenant_id: String,
    pub user_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationRole {
    User,
    Assistant,
}

impl ConversationRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }

    fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "user" => Ok(Self::User),
            "assistant" => Ok(Self::Assistant),
            _ => Err(invalid_data()),
        }
    }
}

#[doc(hidden)]
pub fn conversation_role_as_str(value: ConversationRole) -> &'static str {
    value.as_str()
}

#[doc(hidden)]
pub fn parse_conversation_role(value: &str) -> Result<ConversationRole, RepositoryError> {
    ConversationRole::parse(value)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "storage", content = "value", rename_all = "snake_case")]
pub enum ConversationContent {
    Inline(Value),
    Ref(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewConversationMessage {
    pub message_id: String,
    pub content: ConversationContent,
    pub content_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationMessage {
    pub message_id: String,
    pub conversation_id: String,
    pub message_order: i64,
    pub role: ConversationRole,
    pub run_id: Option<RunId>,
    pub content: ConversationContent,
    pub content_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewConversationTurn {
    pub user_id: String,
    pub message: NewConversationMessage,
    pub admission: NewTerminalRunAdmission,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationTurnOutcome {
    pub admission: TerminalRunAdmission,
    pub user_message: ConversationMessage,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullConversationTurnQuery {
    pub tenant_id: String,
    pub request_id: String,
    pub conversation_id: String,
    pub user_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FullConversationTurn {
    pub run_id: RunId,
    pub user_message: ConversationMessage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitConversationTurn {
    pub result: NewTerminalRunResult,
    pub assistant_message: NewConversationMessage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationTerminalCommitOutcome {
    pub result: TerminalRunResult,
    pub assistant_message: ConversationMessage,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageCursor {
    pub message_order: i64,
    pub message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessagePageQuery {
    pub conversation: ConversationQuery,
    pub before: Option<MessageCursor>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationMessagePage {
    /// Newest-first messages. Reverse this vector when assembling a prompt.
    pub messages: Vec<ConversationMessage>,
    pub next_cursor: Option<MessageCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ArchiveOutcome {
    Archived {
        conversation: Conversation,
        changed: bool,
    },
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedConversationContent {
    pub content_refs: Vec<String>,
    pub summary_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PrivacyDeleteOutcome {
    Deleted { content: DeletedConversationContent },
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewConversationSummary {
    pub conversation: ConversationQuery,
    pub through_message_order: i64,
    pub summary_ref: String,
    pub summary_hash: ContentHash,
    pub model_revision: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationSummary {
    pub conversation_id: String,
    pub through_message_order: i64,
    pub summary_ref: String,
    pub summary_hash: ContentHash,
    pub model_revision: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryOutcome {
    pub summary: ConversationSummary,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimConversationSummaryJob {
    pub conversation: ConversationQuery,
    pub claim_token: String,
    pub claimed_by: String,
    pub claim_expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseConversationSummaryJob {
    pub conversation_id: String,
    pub claim_token: String,
    pub claimed_by: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationContextQuery {
    pub conversation: ConversationQuery,
    pub recent_message_limit: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationContext {
    pub summary: Option<ConversationSummary>,
    /// Chronological messages strictly after the selected summary boundary.
    pub messages: Vec<ConversationMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionDeleteOutcome {
    pub deleted: u64,
    pub input_refs: Vec<String>,
    pub output_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationRetentionOutcome {
    pub deleted: u64,
    pub content_refs: Vec<String>,
    pub summary_refs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentDeletionSourceKind {
    TerminalRunRetention,
    ConversationPrivacy,
    ConversationRetention,
}

impl ContentDeletionSourceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::TerminalRunRetention => "terminal_run_retention",
            Self::ConversationPrivacy => "conversation_privacy",
            Self::ConversationRetention => "conversation_retention",
        }
    }

    fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "terminal_run_retention" => Ok(Self::TerminalRunRetention),
            "conversation_privacy" => Ok(Self::ConversationPrivacy),
            "conversation_retention" => Ok(Self::ConversationRetention),
            _ => Err(invalid_data()),
        }
    }
}

#[doc(hidden)]
pub fn content_deletion_source_kind_as_str(value: ContentDeletionSourceKind) -> &'static str {
    value.as_str()
}

#[doc(hidden)]
pub fn parse_content_deletion_source_kind(
    value: &str,
) -> Result<ContentDeletionSourceKind, RepositoryError> {
    ContentDeletionSourceKind::parse(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalContentDeletionJob {
    pub deletion_job_id: String,
    pub tenant_id: String,
    pub content_ref: String,
    pub content_hash: ContentHash,
    pub source_kind: ContentDeletionSourceKind,
    pub source_id: String,
    pub attempts: u64,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimContentDeletionJobs {
    pub claimed_by: String,
    pub observed_at: DateTime<Utc>,
    pub claim_expires_at: DateTime<Utc>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalContentDeletionClaim {
    pub job: TerminalContentDeletionJob,
    pub claim_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckContentDeletionJob {
    pub deletion_job_id: String,
    pub claim_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalArtifactSourceKind {
    RunOutput,
    UserMessage,
    AssistantMessage,
    ConversationSummary,
}

impl TerminalArtifactSourceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RunOutput => "run_output",
            Self::UserMessage => "user_message",
            Self::AssistantMessage => "assistant_message",
            Self::ConversationSummary => "conversation_summary",
        }
    }

    fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "run_output" => Ok(Self::RunOutput),
            "user_message" => Ok(Self::UserMessage),
            "assistant_message" => Ok(Self::AssistantMessage),
            "conversation_summary" => Ok(Self::ConversationSummary),
            _ => Err(invalid_data()),
        }
    }
}

#[doc(hidden)]
pub fn terminal_artifact_source_kind_as_str(value: TerminalArtifactSourceKind) -> &'static str {
    value.as_str()
}

#[doc(hidden)]
pub fn parse_terminal_artifact_source_kind(
    value: &str,
) -> Result<TerminalArtifactSourceKind, RepositoryError> {
    TerminalArtifactSourceKind::parse(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewTerminalArtifactStage {
    pub tenant_id: String,
    pub content_ref: String,
    pub content_hash: ContentHash,
    pub source_kind: TerminalArtifactSourceKind,
    pub source_id: String,
    /// Earliest time at which a crashed producer may be collected.
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalArtifactStage {
    pub staging_id: String,
    pub tenant_id: String,
    pub content_ref: String,
    pub content_hash: ContentHash,
    pub source_kind: TerminalArtifactSourceKind,
    pub source_id: String,
    pub attempts: u64,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageTerminalArtifactOutcome {
    pub stage: TerminalArtifactStage,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimTerminalArtifactStages {
    pub claimed_by: String,
    pub observed_at: DateTime<Utc>,
    pub claim_expires_at: DateTime<Utc>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalArtifactStageClaim {
    pub stage: TerminalArtifactStage,
    pub claim_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveTerminalArtifactStage {
    pub staging_id: String,
    pub claim_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalArtifactStageDisposition {
    /// A durable terminal or full-runtime authority now references the bytes.
    /// The staging row was removed atomically and the object must be kept.
    Authoritative,
    /// No authority references the object. The exact claim remains fenced
    /// while the caller performs an idempotent external delete.
    DeleteOrphan,
    /// The claim no longer exists or its token was superseded.
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckTerminalArtifactStage {
    pub staging_id: String,
    pub claim_token: String,
}

pub fn terminal_artifact_staging_id(
    tenant_id: &str,
    source_kind: TerminalArtifactSourceKind,
    source_id: &str,
) -> String {
    let identity = format!(
        "{}:{tenant_id}:{}:{source_id}",
        tenant_id.len(),
        source_kind.as_str()
    );
    format!(
        "terminal_stage_{}",
        ContentHash::from_bytes(identity.as_bytes())
            .as_str()
            .trim_start_matches("sha256:")
    )
}

#[async_trait]
pub trait TerminalRunStore: Send + Sync {
    async fn register_runtime_instance(
        &self,
        command: RegisterRuntimeInstance,
    ) -> Result<RuntimeInstanceLease, RepositoryError>;

    async fn heartbeat_runtime_instance(
        &self,
        command: HeartbeatRuntimeInstance,
    ) -> Result<OwnerLeaseHeartbeat, RepositoryError>;

    async fn check_runtime_owner(
        &self,
        query: OwnerLeaseQuery,
    ) -> Result<OwnerLeaseStatus, RepositoryError>;

    async fn unregister_runtime_instance(
        &self,
        owner: RuntimeOwner,
    ) -> Result<bool, RepositoryError>;

    async fn admit_terminal_run(
        &self,
        command: NewTerminalRunAdmission,
    ) -> Result<AdmissionOutcome, RepositoryError>;

    async fn get_terminal_run(
        &self,
        query: TerminalRunQuery,
    ) -> Result<Option<TerminalRunView>, RepositoryError>;

    async fn get_terminal_run_by_request(
        &self,
        query: TerminalRunRequestQuery,
    ) -> Result<Option<TerminalRunView>, RepositoryError>;

    async fn commit_terminal_result(
        &self,
        command: NewTerminalRunResult,
    ) -> Result<TerminalCommitOutcome, RepositoryError>;

    async fn delete_terminal_runs_before(
        &self,
        retention: BoundedRetention,
    ) -> Result<RetentionDeleteOutcome, RepositoryError>;
}

#[async_trait]
pub trait TerminalContentDeletionStore: Send + Sync {
    /// Claims pending or lease-expired jobs in a bounded batch.
    async fn claim_content_deletion_jobs(
        &self,
        command: ClaimContentDeletionJobs,
    ) -> Result<Vec<TerminalContentDeletionClaim>, RepositoryError>;

    /// Acknowledges an exact claim after object deletion succeeds.
    async fn ack_content_deletion_job(
        &self,
        command: AckContentDeletionJob,
    ) -> Result<bool, RepositoryError>;
}

#[async_trait]
pub trait TerminalArtifactStagingStore: Send + Sync {
    /// Registers durable ownership before the external object is written.
    async fn stage_terminal_artifact(
        &self,
        command: NewTerminalArtifactStage,
    ) -> Result<StageTerminalArtifactOutcome, RepositoryError>;

    /// Claims expired producer intents in a bounded, retryable batch.
    async fn claim_terminal_artifact_stages(
        &self,
        command: ClaimTerminalArtifactStages,
    ) -> Result<Vec<TerminalArtifactStageClaim>, RepositoryError>;

    /// Rechecks every durable reference authority under the exact claim. An
    /// authoritative stage is removed in this transaction; an orphan remains
    /// claimed and fenced until external deletion is acknowledged.
    async fn resolve_terminal_artifact_stage(
        &self,
        command: ResolveTerminalArtifactStage,
    ) -> Result<TerminalArtifactStageDisposition, RepositoryError>;

    async fn ack_terminal_artifact_stage(
        &self,
        command: AckTerminalArtifactStage,
    ) -> Result<bool, RepositoryError>;
}

#[async_trait]
pub trait ConversationStore: Send + Sync {
    async fn create_conversation(
        &self,
        command: NewConversation,
    ) -> Result<CreateConversationOutcome, RepositoryError>;

    async fn get_conversation(
        &self,
        query: ConversationQuery,
    ) -> Result<Option<Conversation>, RepositoryError>;

    async fn create_conversation_turn(
        &self,
        command: NewConversationTurn,
    ) -> Result<ConversationTurnOutcome, RepositoryError>;

    /// Loads an already-admitted terminal-only Conversation turn without
    /// consuming active-Run capacity.
    async fn get_terminal_conversation_turn(
        &self,
        query: FullConversationTurnQuery,
    ) -> Result<Option<ConversationTurnOutcome>, RepositoryError>;

    /// Loads an idempotent full-persistence Conversation turn by its
    /// tenant-scoped request identity.
    async fn get_full_conversation_turn(
        &self,
        query: FullConversationTurnQuery,
    ) -> Result<Option<FullConversationTurn>, RepositoryError>;

    async fn full_conversation_run_tenant(
        &self,
        run_id: &RunId,
    ) -> Result<Option<String>, RepositoryError>;

    async fn full_conversation_run_user(
        &self,
        run_id: &RunId,
    ) -> Result<Option<String>, RepositoryError>;

    /// Returns the owning Conversation for a full-persistence Run (or one of
    /// its recovery descendants). The binding deliberately survives privacy
    /// deletion so callers can acquire the Conversation mutation fence before
    /// deciding whether a response must be redacted.
    async fn full_conversation_run_conversation_id(
        &self,
        run_id: &RunId,
    ) -> Result<Option<String>, RepositoryError>;

    async fn full_conversation_run_is_deleted(
        &self,
        run_id: &RunId,
    ) -> Result<Option<bool>, RepositoryError>;

    /// Returns every durable full-persistence Run in the Conversation,
    /// including recovery descendants whose input may retain Conversation
    /// context. Privacy deletion uses this set to quiesce execution before
    /// removing messages.
    async fn list_full_conversation_run_ids(
        &self,
        query: ConversationQuery,
    ) -> Result<Vec<RunId>, RepositoryError>;

    async fn commit_conversation_turn(
        &self,
        command: CommitConversationTurn,
    ) -> Result<ConversationTerminalCommitOutcome, RepositoryError>;

    async fn page_conversation_messages(
        &self,
        query: MessagePageQuery,
    ) -> Result<Option<ConversationMessagePage>, RepositoryError>;

    async fn archive_conversation(
        &self,
        query: ConversationQuery,
        archived_at: DateTime<Utc>,
    ) -> Result<ArchiveOutcome, RepositoryError>;

    async fn delete_conversation(
        &self,
        query: ConversationQuery,
    ) -> Result<PrivacyDeleteOutcome, RepositoryError>;

    async fn put_conversation_summary(
        &self,
        summary: NewConversationSummary,
    ) -> Result<SummaryOutcome, RepositoryError>;

    async fn try_claim_conversation_summary_job(
        &self,
        command: ClaimConversationSummaryJob,
    ) -> Result<bool, RepositoryError>;

    async fn release_conversation_summary_job(
        &self,
        command: ReleaseConversationSummaryJob,
    ) -> Result<bool, RepositoryError>;

    async fn latest_conversation_summary(
        &self,
        query: ConversationQuery,
    ) -> Result<Option<ConversationSummary>, RepositoryError>;

    async fn load_conversation_context(
        &self,
        query: ConversationContextQuery,
    ) -> Result<Option<ConversationContext>, RepositoryError>;

    /// Deletes a bounded batch selected by the Conversation's independent
    /// creation-time retention cutoff. Archived and unarchived Conversations
    /// are equally eligible.
    async fn delete_conversations_before(
        &self,
        retention: BoundedRetention,
    ) -> Result<ConversationRetentionOutcome, RepositoryError>;

    /// Compatibility alias retained for callers compiled against the initial
    /// terminal-only port. The retention boundary is `created_at`, not
    /// `archived_at`; callers should prefer [`Self::delete_conversations_before`].
    async fn delete_archived_conversations_before(
        &self,
        retention: BoundedRetention,
    ) -> Result<ConversationRetentionOutcome, RepositoryError> {
        self.delete_conversations_before(retention).await
    }
}

pub(crate) fn invalid_data() -> RepositoryError {
    insight_engine::repository::adapter::invalid_data()
}
