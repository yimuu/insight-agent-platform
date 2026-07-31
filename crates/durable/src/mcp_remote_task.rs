//! Durable authority for MCP Tasks created by remote servers.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_engine::TransitionOutcome;
use serde::{Deserialize, Serialize};

use super::{
    McpInteractionPrincipal, McpSecretCiphertext, RepositoryError, RepositoryErrorExt as _,
};

const MAX_LABEL_BYTES: usize = 8 * 1024;
const MAX_POLL_INTERVAL_MS: u64 = 24 * 60 * 60 * 1_000;
const MAX_CLAIM_LIMIT: u32 = 1_024;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct McpRemoteTaskId(String);

impl McpRemoteTaskId {
    pub fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        validate_label(&value, 256)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpRemoteTaskStatus {
    Working,
    InputRequired,
    Completed,
    Failed,
    Cancelled,
    Expired,
}

impl McpRemoteTaskStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Expired
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpRemoteTask {
    task_id: McpRemoteTaskId,
    principal: McpInteractionPrincipal,
    run_id: String,
    operation_id: String,
    logical_request_key: String,
    server_id: String,
    binding_hash: String,
    protocol_version: String,
    capability_id: String,
    status: McpRemoteTaskStatus,
    version: u64,
    remote_created_at: DateTime<Utc>,
    remote_updated_at: DateTime<Utc>,
    ttl_deadline: DateTime<Utc>,
    poll_interval_ms: u64,
    next_poll_at: Option<DateTime<Utc>>,
    lease_owner: Option<String>,
    lease_epoch: u64,
    lease_expires_at: Option<DateTime<Utc>>,
    terminal_receipt_hash: Option<String>,
    terminal_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl McpRemoteTask {
    pub fn task_id(&self) -> &McpRemoteTaskId {
        &self.task_id
    }
    pub fn principal(&self) -> &McpInteractionPrincipal {
        &self.principal
    }
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }
    pub fn logical_request_key(&self) -> &str {
        &self.logical_request_key
    }
    pub fn server_id(&self) -> &str {
        &self.server_id
    }
    pub fn binding_hash(&self) -> &str {
        &self.binding_hash
    }
    pub fn protocol_version(&self) -> &str {
        &self.protocol_version
    }
    pub fn capability_id(&self) -> &str {
        &self.capability_id
    }
    pub fn status(&self) -> McpRemoteTaskStatus {
        self.status
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn remote_created_at(&self) -> DateTime<Utc> {
        self.remote_created_at
    }
    pub fn remote_updated_at(&self) -> DateTime<Utc> {
        self.remote_updated_at
    }
    pub fn ttl_deadline(&self) -> DateTime<Utc> {
        self.ttl_deadline
    }
    pub fn poll_interval_ms(&self) -> u64 {
        self.poll_interval_ms
    }
    pub fn next_poll_at(&self) -> Option<DateTime<Utc>> {
        self.next_poll_at
    }
    pub fn lease_owner(&self) -> Option<&str> {
        self.lease_owner.as_deref()
    }
    pub fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }
    pub fn lease_expires_at(&self) -> Option<DateTime<Utc>> {
        self.lease_expires_at
    }
    pub fn terminal_receipt_hash(&self) -> Option<&str> {
        self.terminal_receipt_hash.as_deref()
    }
    pub fn terminal_at(&self) -> Option<DateTime<Utc>> {
        self.terminal_at
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CreateMcpRemoteTaskCommand {
    task: McpRemoteTask,
    remote_task_id: McpSecretCiphertext,
    remote_task_id_hash: String,
    initial_payload: McpSecretCiphertext,
    initial_payload_hash: String,
}

impl CreateMcpRemoteTaskCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: McpRemoteTaskId,
        principal: McpInteractionPrincipal,
        run_id: impl Into<String>,
        operation_id: impl Into<String>,
        logical_request_key: impl Into<String>,
        server_id: impl Into<String>,
        binding_hash: impl Into<String>,
        protocol_version: impl Into<String>,
        capability_id: impl Into<String>,
        remote_task_id: McpSecretCiphertext,
        remote_task_id_hash: impl Into<String>,
        initial_payload: McpSecretCiphertext,
        initial_payload_hash: impl Into<String>,
        remote_created_at: DateTime<Utc>,
        remote_updated_at: DateTime<Utc>,
        ttl_deadline: DateTime<Utc>,
        poll_interval_ms: u64,
        now: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let run_id = run_id.into();
        let operation_id = operation_id.into();
        let logical_request_key = logical_request_key.into();
        let server_id = server_id.into();
        let binding_hash = binding_hash.into();
        let protocol_version = protocol_version.into();
        let capability_id = capability_id.into();
        for value in [
            &run_id,
            &operation_id,
            &logical_request_key,
            &server_id,
            &protocol_version,
            &capability_id,
        ] {
            validate_label(value, MAX_LABEL_BYTES)?;
        }
        validate_hash(&binding_hash)?;
        let remote_task_id_hash = remote_task_id_hash.into();
        let initial_payload_hash = initial_payload_hash.into();
        validate_hash(&remote_task_id_hash)?;
        validate_hash(&initial_payload_hash)?;
        if remote_created_at > remote_updated_at
            || ttl_deadline <= now
            || poll_interval_ms == 0
            || poll_interval_ms > MAX_POLL_INTERVAL_MS
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            task: McpRemoteTask {
                task_id,
                principal,
                run_id,
                operation_id,
                logical_request_key,
                server_id,
                binding_hash,
                protocol_version,
                capability_id,
                status: McpRemoteTaskStatus::Working,
                version: 1,
                remote_created_at,
                remote_updated_at,
                ttl_deadline,
                poll_interval_ms,
                next_poll_at: Some(now),
                lease_owner: None,
                lease_epoch: 0,
                lease_expires_at: None,
                terminal_receipt_hash: None,
                terminal_at: None,
                created_at: now,
                updated_at: now,
            },
            remote_task_id,
            remote_task_id_hash,
            initial_payload,
            initial_payload_hash,
        })
    }

    pub fn task(&self) -> &McpRemoteTask {
        &self.task
    }
    pub fn remote_task_id(&self) -> &McpSecretCiphertext {
        &self.remote_task_id
    }
    pub fn remote_task_id_hash(&self) -> &str {
        &self.remote_task_id_hash
    }
    pub fn initial_payload(&self) -> &McpSecretCiphertext {
        &self.initial_payload
    }
    pub fn initial_payload_hash(&self) -> &str {
        &self.initial_payload_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRemoteTaskSecret {
    pub remote_task_id: McpSecretCiphertext,
    pub remote_task_id_hash: String,
    pub latest_payload: McpSecretCiphertext,
    pub latest_payload_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpRemoteTaskPollClaim {
    pub task: McpRemoteTask,
    pub secret: McpRemoteTaskSecret,
    pub owner: String,
    pub lease_epoch: u64,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimMcpRemoteTasksCommand {
    owner: String,
    now: DateTime<Utc>,
    lease_expires_at: DateTime<Utc>,
    limit: u32,
}

impl ClaimMcpRemoteTasksCommand {
    pub fn new(
        owner: impl Into<String>,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Self, RepositoryError> {
        let owner = owner.into();
        validate_label(&owner, 256)?;
        if lease_expires_at <= now || limit == 0 || limit > MAX_CLAIM_LIMIT {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            owner,
            now,
            lease_expires_at,
            limit,
        })
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }
    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }
    pub fn lease_expires_at(&self) -> DateTime<Utc> {
        self.lease_expires_at
    }
    pub fn limit(&self) -> u32 {
        self.limit
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ObserveMcpRemoteTaskCommand {
    task_id: McpRemoteTaskId,
    request_id: String,
    owner: String,
    lease_epoch: u64,
    expected_version: u64,
    status: McpRemoteTaskStatus,
    remote_updated_at: DateTime<Utc>,
    poll_interval_ms: u64,
    next_poll_at: Option<DateTime<Utc>>,
    payload: McpSecretCiphertext,
    payload_hash: String,
    terminal_receipt_hash: Option<String>,
    observed_at: DateTime<Utc>,
}

impl ObserveMcpRemoteTaskCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: McpRemoteTaskId,
        request_id: impl Into<String>,
        owner: impl Into<String>,
        lease_epoch: u64,
        expected_version: u64,
        status: McpRemoteTaskStatus,
        remote_updated_at: DateTime<Utc>,
        poll_interval_ms: u64,
        next_poll_at: Option<DateTime<Utc>>,
        payload: McpSecretCiphertext,
        payload_hash: impl Into<String>,
        terminal_receipt_hash: Option<String>,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        let owner = owner.into();
        validate_label(&request_id, 256)?;
        validate_label(&owner, 256)?;
        let payload_hash = payload_hash.into();
        validate_hash(&payload_hash)?;
        if lease_epoch == 0
            || expected_version == 0
            || poll_interval_ms == 0
            || poll_interval_ms > MAX_POLL_INTERVAL_MS
            || status.is_terminal() != terminal_receipt_hash.is_some()
            || status.is_terminal() == next_poll_at.is_some()
            || terminal_receipt_hash
                .as_ref()
                .is_some_and(|hash| validate_hash(hash).is_err())
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            task_id,
            request_id,
            owner,
            lease_epoch,
            expected_version,
            status,
            remote_updated_at,
            poll_interval_ms,
            next_poll_at,
            payload,
            payload_hash,
            terminal_receipt_hash,
            observed_at,
        })
    }

    pub fn task_id(&self) -> &McpRemoteTaskId {
        &self.task_id
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn owner(&self) -> &str {
        &self.owner
    }
    pub fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }
    pub fn expected_version(&self) -> u64 {
        self.expected_version
    }
    pub fn status(&self) -> McpRemoteTaskStatus {
        self.status
    }
    pub fn remote_updated_at(&self) -> DateTime<Utc> {
        self.remote_updated_at
    }
    pub fn poll_interval_ms(&self) -> u64 {
        self.poll_interval_ms
    }
    pub fn next_poll_at(&self) -> Option<DateTime<Utc>> {
        self.next_poll_at
    }
    pub fn payload(&self) -> &McpSecretCiphertext {
        &self.payload
    }
    pub fn payload_hash(&self) -> &str {
        &self.payload_hash
    }
    pub fn terminal_receipt_hash(&self) -> Option<&str> {
        self.terminal_receipt_hash.as_deref()
    }
    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FinalizeMcpRemoteTaskCommand {
    task_id: McpRemoteTaskId,
    request_id: String,
    status: McpRemoteTaskStatus,
    payload: McpSecretCiphertext,
    payload_hash: String,
    terminal_receipt_hash: String,
    finalized_at: DateTime<Utc>,
}

impl FinalizeMcpRemoteTaskCommand {
    pub fn new(
        task_id: McpRemoteTaskId,
        request_id: impl Into<String>,
        status: McpRemoteTaskStatus,
        payload: McpSecretCiphertext,
        payload_hash: impl Into<String>,
        terminal_receipt_hash: impl Into<String>,
        finalized_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        validate_label(&request_id, 256)?;
        let payload_hash = payload_hash.into();
        let terminal_receipt_hash = terminal_receipt_hash.into();
        validate_hash(&payload_hash)?;
        validate_hash(&terminal_receipt_hash)?;
        if !matches!(
            status,
            McpRemoteTaskStatus::Cancelled | McpRemoteTaskStatus::Expired
        ) {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            task_id,
            request_id,
            status,
            payload,
            payload_hash,
            terminal_receipt_hash,
            finalized_at,
        })
    }

    pub fn task_id(&self) -> &McpRemoteTaskId {
        &self.task_id
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn status(&self) -> McpRemoteTaskStatus {
        self.status
    }
    pub fn payload(&self) -> &McpSecretCiphertext {
        &self.payload
    }
    pub fn payload_hash(&self) -> &str {
        &self.payload_hash
    }
    pub fn terminal_receipt_hash(&self) -> &str {
        &self.terminal_receipt_hash
    }
    pub fn finalized_at(&self) -> DateTime<Utc> {
        self.finalized_at
    }
}

#[async_trait]
pub trait McpRemoteTaskDurableRepository: Send + Sync {
    async fn create_mcp_remote_task(
        &self,
        command: CreateMcpRemoteTaskCommand,
    ) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError>;

    async fn load_mcp_remote_task(
        &self,
        task_id: &McpRemoteTaskId,
    ) -> Result<Option<McpRemoteTask>, RepositoryError>;

    async fn load_mcp_remote_task_secret(
        &self,
        task_id: &McpRemoteTaskId,
    ) -> Result<Option<McpRemoteTaskSecret>, RepositoryError>;

    async fn claim_mcp_remote_tasks(
        &self,
        command: ClaimMcpRemoteTasksCommand,
    ) -> Result<Vec<McpRemoteTaskPollClaim>, RepositoryError>;

    async fn claim_mcp_remote_task(
        &self,
        task_id: &McpRemoteTaskId,
        command: ClaimMcpRemoteTasksCommand,
    ) -> Result<Option<McpRemoteTaskPollClaim>, RepositoryError>;

    async fn observe_mcp_remote_task(
        &self,
        command: ObserveMcpRemoteTaskCommand,
    ) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError>;

    async fn finalize_mcp_remote_task(
        &self,
        command: FinalizeMcpRemoteTaskCommand,
    ) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError>;
}

#[doc(hidden)]
pub mod adapter {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    pub fn task_from_storage(
        task_id: McpRemoteTaskId,
        principal: McpInteractionPrincipal,
        run_id: String,
        operation_id: String,
        logical_request_key: String,
        server_id: String,
        binding_hash: String,
        protocol_version: String,
        capability_id: String,
        status: McpRemoteTaskStatus,
        version: u64,
        remote_created_at: DateTime<Utc>,
        remote_updated_at: DateTime<Utc>,
        ttl_deadline: DateTime<Utc>,
        poll_interval_ms: u64,
        next_poll_at: Option<DateTime<Utc>>,
        lease_owner: Option<String>,
        lease_epoch: u64,
        lease_expires_at: Option<DateTime<Utc>>,
        terminal_receipt_hash: Option<String>,
        terminal_at: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> McpRemoteTask {
        McpRemoteTask {
            task_id,
            principal,
            run_id,
            operation_id,
            logical_request_key,
            server_id,
            binding_hash,
            protocol_version,
            capability_id,
            status,
            version,
            remote_created_at,
            remote_updated_at,
            ttl_deadline,
            poll_interval_ms,
            next_poll_at,
            lease_owner,
            lease_epoch,
            lease_expires_at,
            terminal_receipt_hash,
            terminal_at,
            created_at,
            updated_at,
        }
    }
}

fn validate_label(value: &str, max: usize) -> Result<(), RepositoryError> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        Err(RepositoryError::invalid_data())
    } else {
        Ok(())
    }
}

fn validate_hash(value: &str) -> Result<(), RepositoryError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(RepositoryError::invalid_data())
    }
}
