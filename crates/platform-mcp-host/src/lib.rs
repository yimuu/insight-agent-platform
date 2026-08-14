//! Protocol-safe MCP Host boundary for Platform v1.
//!
//! Durable Invocation/Job/Task authority remains outside this crate. The Host consumes only exact
//! MCP Deployment, Server Revision, Protocol Policy, authorization generation, Discovery Snapshot
//! and operation envelopes. It performs no registry discovery, stores no plaintext credential and
//! never starts a managed-stdio process.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use insight_platform_contracts::{
    canonical_digest, CapabilityIdempotencyKind, ClosedJsonValue, Effect, ExactDeploymentRef,
    ExactSecretBindingRef, InteractionSchemaDocument, McpAuthorizationPrincipalKind,
    McpDeploymentClosure, McpDiscoverySnapshot, McpExperimentalFeature, McpProtocolPolicyDocument,
    McpServerExecutionContract, McpTransportKind, PrincipalKind, PublishedMcpMethod, ResourceId,
    ResourceKind, SecretResolutionPolicy, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, panic::AssertUnwindSafe, sync::Arc, time::Duration};

mod authorization;
mod discovery;
mod notification;
mod oauth;
mod oauth_callback;
mod oauth_cleanup;
mod oauth_start;
mod oauth_state;
mod session;
mod subscription;
mod subscription_worker;
mod transport;

pub use authorization::*;
pub use discovery::*;
pub use notification::*;
pub use oauth::*;
pub use oauth_callback::*;
pub use oauth_cleanup::*;
pub use oauth_start::*;
pub use oauth_state::*;
pub use session::*;
pub use subscription::*;
pub use subscription_worker::*;
pub use transport::*;

pub const MAX_MCP_AUTHORIZATION_SCOPES: usize = 64;
pub const MAX_MCP_SCOPE_BYTES: usize = 256;
pub const MAX_MCP_SAFE_MESSAGE_BYTES: usize = 512;
pub const MAX_MCP_OPAQUE_STATE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMcpAuthorizationContext {
    pub tenant_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub principal_kind: McpAuthorizationPrincipalKind,
    pub principal_id: ResourceId,
    pub principal_identity_kind: PrincipalKind,
    pub principal_binding_generation: u64,
    pub audience_identity_digest: Sha256Digest,
    pub granted_scopes: Vec<String>,
    pub token_secret_binding: ExactSecretBindingRef,
    pub generation: u64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpAuthorizationContext {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub principal_kind: McpAuthorizationPrincipalKind,
    pub principal_id: ResourceId,
    pub principal_identity_kind: PrincipalKind,
    pub principal_binding_generation: u64,
    pub audience_identity_digest: Sha256Digest,
    pub granted_scopes: Vec<String>,
    pub scope_digest: Sha256Digest,
    pub token_secret_binding: ExactSecretBindingRef,
    pub generation: u64,
    pub expires_at: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl McpAuthorizationContext {
    pub fn build(mut input: NewMcpAuthorizationContext) -> Result<Self, McpHostError> {
        input.granted_scopes.sort();
        let scope_digest = scope_digest(&input.granted_scopes)?;
        let mut context = Self {
            schema_version: 1,
            tenant_id: input.tenant_id,
            authorization_binding_id: input.authorization_binding_id,
            mcp_deployment: input.mcp_deployment,
            principal_kind: input.principal_kind,
            principal_id: input.principal_id,
            principal_identity_kind: input.principal_identity_kind,
            principal_binding_generation: input.principal_binding_generation,
            audience_identity_digest: input.audience_identity_digest,
            granted_scopes: input.granted_scopes,
            scope_digest,
            token_secret_binding: input.token_secret_binding,
            generation: input.generation,
            expires_at: input.expires_at,
            canonical_digest: placeholder_digest()?,
        };
        context.validate_at(Utc::now())?;
        context.canonical_digest = digest_without_field(&context, "canonical_digest")?;
        Ok(context)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.principal_id.kind() != ResourceKind::Principal
            || self.principal_binding_generation == 0
            || self.token_secret_binding.validate().is_err()
            || !matches!(
                &self.token_secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
            || self.generation == 0
            || self.expires_at <= now
            || self.granted_scopes.len() > MAX_MCP_AUTHORIZATION_SCOPES
            || !self.granted_scopes.windows(2).all(|pair| pair[0] < pair[1])
            || self.granted_scopes.iter().any(|scope| !valid_scope(scope))
            || scope_digest(&self.granted_scopes)? != self.scope_digest
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        match self.principal_kind {
            McpAuthorizationPrincipalKind::PerUser
                if self.principal_identity_kind == PrincipalKind::ServiceIdentity =>
            {
                Err(McpHostError::InvalidAuthorization)
            }
            McpAuthorizationPrincipalKind::ServiceIdentity
                if self.principal_identity_kind != PrincipalKind::ServiceIdentity =>
            {
                Err(McpHostError::InvalidAuthorization)
            }
            _ => Ok(()),
        }
    }

    pub fn validate_canonical_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.validate_at(now)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMcpHostExecutionContract {
    pub deployment: ExactDeploymentRef,
    pub deployment_closure: McpDeploymentClosure,
    pub server: McpServerExecutionContract,
    pub protocol_profile: McpProtocolPolicyDocument,
    pub authorization: McpAuthorizationContext,
    pub discovery: McpDiscoverySnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpHostExecutionContract {
    pub schema_version: u32,
    pub deployment: ExactDeploymentRef,
    pub deployment_closure: McpDeploymentClosure,
    pub server: McpServerExecutionContract,
    pub protocol_profile: McpProtocolPolicyDocument,
    pub authorization: McpAuthorizationContext,
    pub discovery: McpDiscoverySnapshot,
    pub canonical_digest: Sha256Digest,
}

/// Exact authority lookup used by execution adapters.
///
/// Every field is copied from the durable Invocation admission snapshot. Implementations must
/// resolve only these identities and generations; they must never follow a current Registry head
/// or choose a replacement authorization binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpExecutionContractQuery {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub discovery_snapshot_id: ResourceId,
    pub discovery_snapshot_digest: Sha256Digest,
    pub authorization_binding_id: ResourceId,
    pub authorization_generation: u64,
    pub authorization_context_digest: Sha256Digest,
    pub principal_id: ResourceId,
}

impl McpExecutionContractQuery {
    pub fn validate(&self) -> Result<(), McpExecutionContractResolutionError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.discovery_snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.authorization_generation == 0
            || self.principal_id.kind() != ResourceKind::Principal
        {
            return Err(McpExecutionContractResolutionError::InvalidQuery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpExecutionContractResolutionError {
    InvalidQuery,
    NotFoundOrChanged,
    AuthorityUnavailable,
}

impl fmt::Display for McpExecutionContractResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidQuery => "MCP execution contract query is invalid",
            Self::NotFoundOrChanged => "exact MCP execution binding was not found or changed",
            Self::AuthorityUnavailable => "MCP execution contract authority is unavailable",
        })
    }
}

impl Error for McpExecutionContractResolutionError {}

#[async_trait]
pub trait McpExecutionContractResolver: Send + Sync {
    async fn resolve_mcp_execution_contract(
        &self,
        query: &McpExecutionContractQuery,
    ) -> Result<McpHostExecutionContract, McpExecutionContractResolutionError>;
}

impl McpHostExecutionContract {
    pub fn build(input: NewMcpHostExecutionContract) -> Result<Self, McpHostError> {
        let mut contract = Self {
            schema_version: 1,
            deployment: input.deployment,
            deployment_closure: input.deployment_closure,
            server: input.server,
            protocol_profile: input.protocol_profile,
            authorization: input.authorization,
            discovery: input.discovery,
            canonical_digest: placeholder_digest()?,
        };
        contract.validate_at(Utc::now())?;
        contract.canonical_digest = digest_without_field(&contract, "canonical_digest")?;
        Ok(contract)
    }

    pub fn transport_kind(&self) -> McpTransportKind {
        self.server.transport
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.deployment.resource_kind != ResourceKind::McpDeployment
            || self.deployment.validate().is_err()
            || self.deployment_closure.validate().is_err()
            || self.server.validate().is_err()
            || self.protocol_profile.validate().is_err()
            || self.discovery.validate().is_err()
            || self.deployment_closure.server_revision != self.server.revision
            || self.deployment_closure.protocol_policy != self.server.protocol_policy
            || self.deployment_closure.transport.kind() != self.server.transport
            || self.authorization.mcp_deployment != self.deployment
            || self.authorization.audience_identity_digest
                != self.deployment_closure.server_identity_digest
            || self.discovery.mcp_deployment != self.deployment
            || self.discovery.server_revision != self.server.revision
            || self.discovery.protocol_profile != self.server.protocol_policy
            || self.discovery.authorization_context_digest != self.authorization.canonical_digest
            || self.discovery.observed_at > now
            || self.discovery.expires_at <= now
            || !insight_platform_contracts::exact_secret_binding_purposes_match(
                &self.deployment_closure.secret_bindings,
                &self.server.deployment_credential_requirements,
            )
            || self.deployment_closure.auth_policy.is_none()
            || self.server.authorization_credential_purpose.as_ref()
                != Some(&self.authorization.token_secret_binding.purpose)
            || self.protocol_profile.canonical_digest().ok().as_ref()
                != Some(&self.server.protocol_policy.semantic_digest)
            || !self
                .protocol_profile
                .offered_versions
                .contains(&self.discovery.negotiated_version)
            || !negotiated_capabilities_allowed(self)
            || !transport_features_allowed(self)
        {
            return Err(McpHostError::InvalidExecutionContract);
        }
        self.authorization.validate_canonical_at(now)
    }

    pub fn validate_canonical_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.validate_at(now)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidExecutionContract);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpLogicalOperationRequest {
    pub schema_version: u32,
    pub mcp_operation_id: ResourceId,
    pub tenant_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub physical_attempt: u32,
    pub snapshot_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub method: PublishedMcpMethod,
    pub params: ClosedJsonValue,
    pub task_requested: bool,
    pub continuation: Option<McpOperationContinuation>,
    pub idempotency_key_digest: Sha256Digest,
    pub idempotency: CapabilityIdempotencyKind,
    pub effect: Effect,
    pub deadline: DateTime<Utc>,
}

impl McpLogicalOperationRequest {
    /// Binds the physical Sandbox claim only after the sole Sandbox Job has been leased.
    pub fn bind_physical(
        &self,
        worker_process_generation_id: ResourceId,
        lease_generation: u64,
    ) -> McpOperationRequest {
        McpOperationRequest {
            schema_version: self.schema_version,
            mcp_operation_id: self.mcp_operation_id.clone(),
            tenant_id: self.tenant_id.clone(),
            invocation_id: self.invocation_id.clone(),
            job_id: self.job_id.clone(),
            worker_process_generation_id,
            lease_generation,
            physical_attempt: self.physical_attempt,
            snapshot_id: self.snapshot_id.clone(),
            authorization_binding_id: self.authorization_binding_id.clone(),
            method: self.method,
            params: self.params.clone(),
            task_requested: self.task_requested,
            continuation: self.continuation.clone(),
            idempotency_key_digest: self.idempotency_key_digest.clone(),
            idempotency: self.idempotency,
            effect: self.effect,
            deadline: self.deadline,
        }
    }

    pub fn validate_for(
        &self,
        contract: &McpHostExecutionContract,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        validate_logical_operation(self, contract, now)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOperationRequest {
    pub schema_version: u32,
    pub mcp_operation_id: ResourceId,
    pub tenant_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub physical_attempt: u32,
    pub snapshot_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub method: PublishedMcpMethod,
    pub params: ClosedJsonValue,
    pub task_requested: bool,
    pub continuation: Option<McpOperationContinuation>,
    pub idempotency_key_digest: Sha256Digest,
    pub idempotency: CapabilityIdempotencyKind,
    pub effect: Effect,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOperationContinuation {
    pub encrypted_state: EncryptedMcpState,
    pub external_identity_digest: Sha256Digest,
    pub poll_count: u32,
    pub elicitation_response: Option<McpElicitationResponse>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpElicitationAction {
    Accept,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpElicitationResponse {
    pub action: McpElicitationAction,
    pub content: Option<ClosedJsonValue>,
}

impl McpElicitationResponse {
    fn validate(&self) -> Result<(), McpHostError> {
        if self.content.is_some() != (self.action == McpElicitationAction::Accept)
            || self
                .content
                .as_ref()
                .is_some_and(|content| content.validate().is_err())
        {
            return Err(McpHostError::InvalidOperation);
        }
        Ok(())
    }
}

impl McpOperationRequest {
    pub fn logical(&self) -> McpLogicalOperationRequest {
        McpLogicalOperationRequest {
            schema_version: self.schema_version,
            mcp_operation_id: self.mcp_operation_id.clone(),
            tenant_id: self.tenant_id.clone(),
            invocation_id: self.invocation_id.clone(),
            job_id: self.job_id.clone(),
            physical_attempt: self.physical_attempt,
            snapshot_id: self.snapshot_id.clone(),
            authorization_binding_id: self.authorization_binding_id.clone(),
            method: self.method,
            params: self.params.clone(),
            task_requested: self.task_requested,
            continuation: self.continuation.clone(),
            idempotency_key_digest: self.idempotency_key_digest.clone(),
            idempotency: self.idempotency,
            effect: self.effect,
            deadline: self.deadline,
        }
    }

    pub fn validate_for(
        &self,
        contract: &McpHostExecutionContract,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
        {
            return Err(McpHostError::InvalidOperation);
        }
        self.logical().validate_for(contract, now)
    }

    fn safe_to_retry_after_unknown(&self) -> bool {
        match self.effect {
            Effect::Pure | Effect::ReadOnly => true,
            Effect::IdempotentWrite => matches!(
                self.idempotency,
                CapabilityIdempotencyKind::Intrinsic | CapabilityIdempotencyKind::CallerKey
            ),
            Effect::NonIdempotentWrite | Effect::Irreversible => false,
        }
    }
}

fn validate_logical_operation(
    request: &McpLogicalOperationRequest,
    contract: &McpHostExecutionContract,
    now: DateTime<Utc>,
) -> Result<(), McpHostError> {
    if request.schema_version != 1
        || request.mcp_operation_id.kind() != ResourceKind::McpOperation
        || request.tenant_id.kind() != ResourceKind::Tenant
        || request.tenant_id != contract.authorization.tenant_id
        || request.invocation_id.kind() != ResourceKind::CapabilityInvocation
        || request.job_id.kind() != ResourceKind::Job
        || request.physical_attempt == 0
        || request.snapshot_id != contract.discovery.snapshot_id
        || request.authorization_binding_id != contract.authorization.authorization_binding_id
        || request.deadline <= now
        || request.deadline > contract.authorization.expires_at
        || request.deadline > contract.discovery.expires_at
        || request.params.validate().is_err()
        || (request.task_requested && request.continuation.is_some())
        || (request.task_requested
            && (request.method != PublishedMcpMethod::ToolsCall
                || !contract.discovery.negotiated_capabilities.tasks_tools_call))
        || request.continuation.as_ref().is_some_and(|continuation| {
            request.method != PublishedMcpMethod::ToolsCall
                || !contract.discovery.negotiated_capabilities.tasks_tools_call
                || continuation.poll_count == 0
                || continuation.encrypted_state.validate().is_err()
                || continuation
                    .elicitation_response
                    .as_ref()
                    .is_some_and(|response| response.validate().is_err())
                || (continuation.elicitation_response.is_some()
                    && (!contract.discovery.negotiated_capabilities.elicitation
                        || !contract
                            .protocol_profile
                            .client_capabilities
                            .tasks_elicitation_create))
        })
        || !contract
            .protocol_profile
            .method_limits
            .contains_key(&request.method)
        || !method_was_negotiated(request.method, &contract.discovery)
        || !effect_allowed_for_method(request.effect, request.method)
    {
        return Err(McpHostError::InvalidOperation);
    }
    let bytes =
        serde_json::to_vec(&request.params.value).map_err(|_| McpHostError::Canonicalization)?;
    let method_limits = contract
        .protocol_profile
        .method_limits
        .get(&request.method)
        .ok_or(McpHostError::InvalidOperation)?;
    let maximum_request_bytes = method_limits
        .maximum_request_bytes
        .min(contract.server.limits.maximum_message_bytes);
    if bytes.len()
        > usize::try_from(maximum_request_bytes).map_err(|_| McpHostError::InvalidOperation)?
    {
        return Err(McpHostError::InvalidOperation);
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptedMcpState {
    pub scheme: String,
    pub ciphertext: Vec<u8>,
    pub key_id: String,
    pub key_reference_digest: Sha256Digest,
    pub plaintext_digest: Sha256Digest,
}

impl fmt::Debug for EncryptedMcpState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptedMcpState")
            .field("scheme", &self.scheme)
            .field("ciphertext", &"[REDACTED]")
            .field("key_id", &"[REDACTED]")
            .field("plaintext_digest", &"[REDACTED]")
            .finish()
    }
}

impl EncryptedMcpState {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.ciphertext.is_empty()
            || self.ciphertext.len() > MAX_MCP_OPAQUE_STATE_BYTES
            || !valid_code(&self.scheme)
            || self.key_id.is_empty()
            || self.key_id.len() > 255
            || self.key_id.chars().any(char::is_control)
        {
            return Err(McpHostError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeMcpFailure {
    pub safe_code: String,
    pub safe_message: String,
    pub evidence_digest: Sha256Digest,
}

impl SafeMcpFailure {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if !valid_code(&self.safe_code)
            || self.safe_message.is_empty()
            || self.safe_message.len() > MAX_MCP_SAFE_MESSAGE_BYTES
            || self.safe_message.chars().any(char::is_control)
        {
            return Err(McpHostError::InvalidOutcome);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum McpTransportFailure {
    RejectedBeforeDispatch(SafeMcpFailure),
    RetryableBeforeDispatch(SafeMcpFailure),
    Permanent(SafeMcpFailure),
    ReauthorizationRequired {
        challenge_digest: Sha256Digest,
    },
    PostDispatchUncertain {
        failure: SafeMcpFailure,
        external_identity_digest: Sha256Digest,
    },
}

impl McpTransportFailure {
    pub fn validate_wire_shape(&self) -> Result<(), McpHostError> {
        match self {
            Self::RejectedBeforeDispatch(failure)
            | Self::RetryableBeforeDispatch(failure)
            | Self::Permanent(failure) => failure.validate(),
            Self::ReauthorizationRequired { .. } => Ok(()),
            Self::PostDispatchUncertain { failure, .. } => failure.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum McpOperationOutcome {
    Completed {
        result: ClosedJsonValue,
        evidence_digest: Sha256Digest,
    },
    RemoteTask {
        encrypted_state: EncryptedMcpState,
        external_identity_digest: Sha256Digest,
        next_poll_at: DateTime<Utc>,
    },
    InputRequired {
        encrypted_state: EncryptedMcpState,
        external_identity_digest: Sha256Digest,
        safe_prompt_key: String,
        response_schema: InteractionSchemaDocument,
        response_schema_digest: Sha256Digest,
        deadline: DateTime<Utc>,
    },
    ReauthorizationRequired {
        challenge_digest: Sha256Digest,
    },
    RetryableFailure(SafeMcpFailure),
    PermanentFailure(SafeMcpFailure),
    Uncertain {
        observation_digest: Sha256Digest,
        external_identity_digest: Sha256Digest,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpRemoteTaskCancelOutcome {
    Accepted,
}

impl McpOperationOutcome {
    /// Validates the shape and request-bound limits of an outcome crossing the Egress RPC trust
    /// boundary. Effect-specific authorization remains with `validate_for`, which owns the full
    /// immutable Host execution contract.
    pub fn validate_streamable_wire_shape(
        &self,
        request: &McpStreamableHttpRequest,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        match self {
            Self::Completed { result, .. } => {
                result
                    .validate()
                    .map_err(|_| McpHostError::InvalidOutcome)?;
                if serde_json::to_vec(&result.value)
                    .map_err(|_| McpHostError::Canonicalization)?
                    .len()
                    > request.maximum_response_bytes as usize
                {
                    return Err(McpHostError::InvalidOutcome);
                }
            }
            Self::RemoteTask {
                encrypted_state,
                next_poll_at,
                ..
            } => {
                encrypted_state.validate()?;
                let limits = request.task_limits.ok_or(McpHostError::InvalidOutcome)?;
                let wait = (*next_poll_at - now).num_milliseconds();
                if request.method != PublishedMcpMethod::ToolsCall
                    || (!request.task_requested && request.continuation.is_none())
                    || wait
                        < i64::try_from(limits.minimum_poll_milliseconds)
                            .map_err(|_| McpHostError::InvalidOutcome)?
                    || wait
                        > i64::try_from(limits.maximum_poll_milliseconds)
                            .map_err(|_| McpHostError::InvalidOutcome)?
                    || *next_poll_at >= request.deadline
                {
                    return Err(McpHostError::InvalidOutcome);
                }
            }
            Self::InputRequired {
                encrypted_state,
                safe_prompt_key,
                response_schema,
                response_schema_digest,
                deadline,
                ..
            } => {
                encrypted_state.validate()?;
                if request.method != PublishedMcpMethod::ToolsCall
                    || request.continuation.is_none()
                    || request.task_limits.is_none()
                    || !request.negotiated_capabilities.elicitation
                    || !request.client_capabilities.elicitation_form
                    || !request.client_capabilities.tasks_elicitation_create
                    || !valid_code(safe_prompt_key)
                    || response_schema.validate().is_err()
                    || &response_schema.canonical_digest != response_schema_digest
                    || *deadline <= now
                    || *deadline > request.deadline
                {
                    return Err(McpHostError::InvalidOutcome);
                }
            }
            Self::RetryableFailure(failure) | Self::PermanentFailure(failure) => {
                failure.validate()?;
            }
            Self::ReauthorizationRequired { .. } | Self::Uncertain { .. } => {}
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        request: &McpOperationRequest,
        contract: &McpHostExecutionContract,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        match self {
            Self::Completed { result, .. } => {
                result
                    .validate()
                    .map_err(|_| McpHostError::InvalidOutcome)?;
                let bytes = serde_json::to_vec(&result.value)
                    .map_err(|_| McpHostError::Canonicalization)?;
                let method_limits = contract
                    .protocol_profile
                    .method_limits
                    .get(&request.method)
                    .ok_or(McpHostError::InvalidOutcome)?;
                let maximum_response_bytes = method_limits
                    .maximum_response_bytes
                    .min(contract.server.limits.maximum_response_bytes);
                if bytes.len()
                    > usize::try_from(maximum_response_bytes)
                        .map_err(|_| McpHostError::InvalidOutcome)?
                {
                    return Err(McpHostError::InvalidOutcome);
                }
            }
            Self::RemoteTask {
                encrypted_state,
                next_poll_at,
                ..
            } => {
                encrypted_state.validate()?;
                let limits = contract
                    .protocol_profile
                    .method_limits
                    .get(&PublishedMcpMethod::TasksGet)
                    .ok_or(McpHostError::InvalidOutcome)?;
                let wait = (*next_poll_at - now).num_milliseconds();
                if request.method != PublishedMcpMethod::ToolsCall
                    || !tasks_enabled(contract)
                    || !contract.discovery.negotiated_capabilities.tasks_tools_call
                    || (!request.task_requested && request.continuation.is_none())
                    || wait
                        < i64::try_from(limits.minimum_poll_milliseconds)
                            .map_err(|_| McpHostError::InvalidOutcome)?
                    || wait
                        > i64::try_from(limits.maximum_poll_milliseconds)
                            .map_err(|_| McpHostError::InvalidOutcome)?
                    || *next_poll_at >= request.deadline
                {
                    return Err(McpHostError::InvalidOutcome);
                }
            }
            Self::InputRequired {
                encrypted_state,
                safe_prompt_key,
                response_schema,
                response_schema_digest,
                deadline,
                ..
            } => {
                encrypted_state.validate()?;
                if request.method != PublishedMcpMethod::ToolsCall
                    || request.continuation.is_none()
                    || !tasks_enabled(contract)
                    || !contract.discovery.negotiated_capabilities.elicitation
                    || !contract
                        .protocol_profile
                        .client_capabilities
                        .elicitation_form
                    || !contract
                        .protocol_profile
                        .client_capabilities
                        .tasks_elicitation_create
                    || !valid_code(safe_prompt_key)
                    || response_schema.validate().is_err()
                    || &response_schema.canonical_digest != response_schema_digest
                    || *deadline <= now
                    || *deadline > request.deadline
                {
                    return Err(McpHostError::InvalidOutcome);
                }
            }
            Self::RetryableFailure(failure) | Self::PermanentFailure(failure) => {
                failure.validate()?;
            }
            Self::Uncertain { .. }
                if request.effect.risk_rank() <= Effect::ReadOnly.risk_rank() =>
            {
                return Err(McpHostError::InvalidOutcome);
            }
            Self::ReauthorizationRequired { .. } | Self::Uncertain { .. } => {}
        }
        Ok(())
    }
}

#[async_trait]
pub trait McpHostTransport: Send + Sync {
    fn kind(&self) -> McpTransportKind;

    async fn execute(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure>;

    async fn cancel_remote_task(
        &self,
        _contract: &McpHostExecutionContract,
        _request: &McpOperationRequest,
        _deadline: DateTime<Utc>,
    ) -> Result<McpRemoteTaskCancelOutcome, McpTransportFailure> {
        Err(McpTransportFailure::Permanent(SafeMcpFailure {
            safe_code: "mcp_remote_task_cancel_unsupported".to_owned(),
            safe_message: "MCP transport does not support remote Task cancellation".to_owned(),
            evidence_digest: static_digest("mcp_remote_task_cancel_unsupported"),
        }))
    }
}

pub struct McpHostService {
    transport: Arc<dyn McpHostTransport>,
}

#[async_trait]
pub trait McpHostClient: Send + Sync {
    async fn execute(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpHostError>;

    async fn cancel_remote_task(
        &self,
        _contract: &McpHostExecutionContract,
        _request: &McpOperationRequest,
        _deadline: DateTime<Utc>,
    ) -> Result<McpRemoteTaskCancelOutcome, McpHostError> {
        Err(McpHostError::InvalidOperation)
    }
}

#[async_trait]
impl McpHostClient for McpHostService {
    async fn execute(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpHostError> {
        Self::execute(self, contract, request).await
    }

    async fn cancel_remote_task(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
        deadline: DateTime<Utc>,
    ) -> Result<McpRemoteTaskCancelOutcome, McpHostError> {
        Self::cancel_remote_task(self, contract, request, deadline).await
    }
}

impl McpHostService {
    pub fn new(transport: Arc<dyn McpHostTransport>) -> Self {
        Self { transport }
    }

    pub async fn execute(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
    ) -> Result<McpOperationOutcome, McpHostError> {
        let now = Utc::now();
        contract.validate_canonical_at(now)?;
        request.validate_for(contract, now)?;
        if self.transport.kind() != contract.transport_kind() {
            return Err(McpHostError::WrongTransport);
        }
        let remaining = u64::try_from((request.deadline - now).num_milliseconds())
            .map_err(|_| McpHostError::InvalidOperation)?;
        let timeout = remaining.min(contract.server.limits.total_timeout_milliseconds);
        let future = AssertUnwindSafe(self.transport.execute(contract, request)).catch_unwind();
        let mut outcome = match tokio::time::timeout(Duration::from_millis(timeout), future).await {
            Ok(Ok(Ok(outcome))) => outcome,
            Ok(Ok(Err(failure))) => map_transport_failure(request, contract, failure)?,
            Ok(Err(_)) => unknown_transport_outcome(request, contract, "mcp_transport_panic"),
            Err(_) => unknown_transport_outcome(request, contract, "mcp_transport_timeout"),
        };
        let validation_now = Utc::now();
        normalize_remote_task_poll(&mut outcome, request, contract, validation_now)?;
        outcome.validate_for(request, contract, validation_now)?;
        Ok(outcome)
    }

    pub async fn cancel_remote_task(
        &self,
        contract: &McpHostExecutionContract,
        request: &McpOperationRequest,
        deadline: DateTime<Utc>,
    ) -> Result<McpRemoteTaskCancelOutcome, McpHostError> {
        let now = Utc::now();
        contract.validate_canonical_at(now)?;
        let mut validation_request = request.clone();
        validation_request.deadline = deadline;
        validation_request.validate_for(contract, now)?;
        if request.continuation.is_none()
            || request.task_requested
            || !contract.discovery.negotiated_capabilities.tasks_cancel
            || self.transport.kind() != contract.transport_kind()
        {
            return Err(McpHostError::InvalidOperation);
        }
        let remaining = u64::try_from((deadline - now).num_milliseconds())
            .map_err(|_| McpHostError::InvalidOperation)?;
        let timeout = remaining.min(contract.server.limits.total_timeout_milliseconds);
        let future = AssertUnwindSafe(
            self.transport
                .cancel_remote_task(contract, request, deadline),
        )
        .catch_unwind();
        match tokio::time::timeout(Duration::from_millis(timeout), future).await {
            Ok(Ok(Ok(outcome))) => Ok(outcome),
            Ok(Ok(Err(failure))) => {
                failure.validate_wire_shape()?;
                Err(McpHostError::InvalidOutcome)
            }
            Ok(Err(_)) | Err(_) => Err(McpHostError::InvalidOutcome),
        }
    }
}

fn normalize_remote_task_poll(
    outcome: &mut McpOperationOutcome,
    request: &McpOperationRequest,
    contract: &McpHostExecutionContract,
    now: DateTime<Utc>,
) -> Result<(), McpHostError> {
    let McpOperationOutcome::RemoteTask { next_poll_at, .. } = outcome else {
        return Ok(());
    };
    let limits = contract
        .protocol_profile
        .method_limits
        .get(&PublishedMcpMethod::TasksGet)
        .ok_or(McpHostError::InvalidOutcome)?;
    let current_wait = (*next_poll_at - now).num_milliseconds();
    let maximum_wait = i64::try_from(limits.maximum_poll_milliseconds)
        .map_err(|_| McpHostError::InvalidOutcome)?;
    if current_wait <= 0 || current_wait > maximum_wait {
        return Err(McpHostError::InvalidOutcome);
    }
    let minimum_poll = chrono::Duration::milliseconds(
        i64::try_from(limits.minimum_poll_milliseconds)
            .map_err(|_| McpHostError::InvalidOutcome)?,
    );
    let minimum_next_poll = now
        .checked_add_signed(minimum_poll)
        .ok_or(McpHostError::InvalidOutcome)?;
    if *next_poll_at < minimum_next_poll {
        *next_poll_at = minimum_next_poll;
    }
    if *next_poll_at >= request.deadline {
        return Err(McpHostError::InvalidOutcome);
    }
    Ok(())
}

fn map_transport_failure(
    request: &McpOperationRequest,
    contract: &McpHostExecutionContract,
    failure: McpTransportFailure,
) -> Result<McpOperationOutcome, McpHostError> {
    failure.validate_wire_shape()?;
    Ok(match failure {
        McpTransportFailure::RetryableBeforeDispatch(_)
        | McpTransportFailure::PostDispatchUncertain { .. }
            if request.continuation.is_some() =>
        {
            defer_existing_task(request, contract)?
        }
        McpTransportFailure::RejectedBeforeDispatch(failure)
        | McpTransportFailure::Permanent(failure) => McpOperationOutcome::PermanentFailure(failure),
        McpTransportFailure::RetryableBeforeDispatch(failure) => {
            McpOperationOutcome::RetryableFailure(failure)
        }
        McpTransportFailure::ReauthorizationRequired { challenge_digest } => {
            McpOperationOutcome::ReauthorizationRequired { challenge_digest }
        }
        McpTransportFailure::PostDispatchUncertain {
            failure,
            external_identity_digest: _,
        } if request.safe_to_retry_after_unknown() => {
            McpOperationOutcome::RetryableFailure(failure)
        }
        McpTransportFailure::PostDispatchUncertain {
            failure,
            external_identity_digest,
        } => McpOperationOutcome::Uncertain {
            observation_digest: failure.evidence_digest,
            external_identity_digest,
        },
    })
}

fn unknown_transport_outcome(
    request: &McpOperationRequest,
    contract: &McpHostExecutionContract,
    domain: &str,
) -> McpOperationOutcome {
    if request.continuation.is_some() {
        return defer_existing_task(request, contract).unwrap_or_else(|_| {
            McpOperationOutcome::Uncertain {
                observation_digest: static_digest(domain),
                external_identity_digest: request
                    .continuation
                    .as_ref()
                    .expect("continuation checked")
                    .external_identity_digest
                    .clone(),
            }
        });
    }
    let failure = SafeMcpFailure {
        safe_code: domain.to_owned(),
        safe_message: "MCP transport completion could not be observed".to_owned(),
        evidence_digest: static_digest(domain),
    };
    if request.safe_to_retry_after_unknown() {
        McpOperationOutcome::RetryableFailure(failure)
    } else {
        McpOperationOutcome::Uncertain {
            observation_digest: failure.evidence_digest,
            external_identity_digest: request.idempotency_key_digest.clone(),
        }
    }
}

fn defer_existing_task(
    request: &McpOperationRequest,
    contract: &McpHostExecutionContract,
) -> Result<McpOperationOutcome, McpHostError> {
    let continuation = request
        .continuation
        .as_ref()
        .ok_or(McpHostError::InvalidOperation)?;
    let limits = contract
        .protocol_profile
        .method_limits
        .get(&PublishedMcpMethod::TasksGet)
        .ok_or(McpHostError::InvalidOperation)?;
    let delay = limits.minimum_poll_milliseconds.max(1);
    let next_poll_at = Utc::now()
        .checked_add_signed(chrono::Duration::milliseconds(
            i64::try_from(delay).map_err(|_| McpHostError::InvalidOperation)?,
        ))
        .ok_or(McpHostError::InvalidOperation)?;
    if next_poll_at >= request.deadline {
        return Err(McpHostError::InvalidOutcome);
    }
    Ok(McpOperationOutcome::RemoteTask {
        encrypted_state: continuation.encrypted_state.clone(),
        external_identity_digest: continuation.external_identity_digest.clone(),
        next_poll_at,
    })
}

fn negotiated_capabilities_allowed(contract: &McpHostExecutionContract) -> bool {
    let negotiated = &contract.discovery.negotiated_capabilities;
    let server = &contract.protocol_profile.allowed_server_capabilities;
    let client = &contract.protocol_profile.client_capabilities;
    (!negotiated.tools || server.tools)
        && (!negotiated.resources || server.resources)
        && (!negotiated.prompts || server.prompts)
        && (!negotiated.logging || server.logging)
        && (!negotiated.tasks || tasks_enabled(contract))
        && (!negotiated.tasks_list || tasks_enabled(contract))
        && (!negotiated.tasks_cancel || tasks_enabled(contract))
        && (!negotiated.tasks_tools_call || tasks_enabled(contract))
        && (negotiated.tasks
            || !(negotiated.tasks_list || negotiated.tasks_cancel || negotiated.tasks_tools_call))
        && (!negotiated.subscriptions || server.subscriptions)
        && (!negotiated.elicitation || client.elicitation_form || client.elicitation_url)
        && (!negotiated.sampling || client.sampling)
        && (!negotiated.roots || client.roots)
}

fn transport_features_allowed(contract: &McpHostExecutionContract) -> bool {
    let features = &contract.protocol_profile.transport_features;
    match contract.server.transport {
        McpTransportKind::StreamableHttp => {
            !features.managed_stdio
                && (features.streamable_http_get || features.streamable_http_sse)
        }
        McpTransportKind::ManagedStdio => {
            features.managed_stdio && !features.streamable_http_get && !features.streamable_http_sse
        }
    }
}

fn tasks_enabled(contract: &McpHostExecutionContract) -> bool {
    contract
        .protocol_profile
        .experimental_features
        .contains(&McpExperimentalFeature::Tasks)
        && contract.protocol_profile.allowed_server_capabilities.tasks
        && contract.discovery.negotiated_capabilities.tasks
}

fn method_was_negotiated(method: PublishedMcpMethod, snapshot: &McpDiscoverySnapshot) -> bool {
    match method {
        PublishedMcpMethod::ToolsCall => snapshot.negotiated_capabilities.tools,
        PublishedMcpMethod::ResourcesRead => snapshot.negotiated_capabilities.resources,
        PublishedMcpMethod::PromptsGet => snapshot.negotiated_capabilities.prompts,
        PublishedMcpMethod::TasksGet | PublishedMcpMethod::TasksResult => {
            snapshot.negotiated_capabilities.tasks
        }
        PublishedMcpMethod::TasksCancel => snapshot.negotiated_capabilities.tasks_cancel,
    }
}

fn effect_allowed_for_method(effect: Effect, method: PublishedMcpMethod) -> bool {
    match method {
        PublishedMcpMethod::ToolsCall => true,
        PublishedMcpMethod::ResourcesRead
        | PublishedMcpMethod::PromptsGet
        | PublishedMcpMethod::TasksGet
        | PublishedMcpMethod::TasksResult => effect.risk_rank() <= Effect::ReadOnly.risk_rank(),
        PublishedMcpMethod::TasksCancel => {
            effect.risk_rank() <= Effect::IdempotentWrite.risk_rank()
        }
    }
}

fn valid_scope(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MCP_SCOPE_BYTES
        && !value.chars().any(char::is_control)
        && value.trim() == value
        && !value.contains(' ')
}

fn valid_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

fn scope_digest(scopes: &[String]) -> Result<Sha256Digest, McpHostError> {
    digest(&serde_json::json!({
        "schema_version": 1,
        "scopes": scopes,
    }))
}

fn digest<T: Serialize>(value: &T) -> Result<Sha256Digest, McpHostError> {
    let value = serde_json::to_value(value).map_err(|_| McpHostError::Canonicalization)?;
    canonical_digest(&value)
        .map_err(|_| McpHostError::Canonicalization)?
        .parse()
        .map_err(|_| McpHostError::Canonicalization)
}

fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, McpHostError> {
    let mut value = serde_json::to_value(value).map_err(|_| McpHostError::Canonicalization)?;
    value
        .as_object_mut()
        .ok_or(McpHostError::Canonicalization)?
        .remove(field)
        .ok_or(McpHostError::Canonicalization)?;
    digest(&value)
}

fn placeholder_digest() -> Result<Sha256Digest, McpHostError> {
    digest(&serde_json::json!({"empty": true}))
}

fn static_digest(domain: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({"domain": domain, "schema_version": 1}))
        .expect("static MCP evidence is canonical")
        .parse()
        .expect("canonical digest is SHA-256")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpHostError {
    InvalidAuthorization,
    InvalidDiscovery,
    InvalidExecutionContract,
    InvalidOperation,
    InvalidOutcome,
    InvalidSession,
    InvalidSubscription,
    WrongTransport,
    Canonicalization,
}

impl fmt::Display for McpHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidAuthorization => "MCP authorization context is invalid",
            Self::InvalidDiscovery => "MCP Discovery Snapshot authority is invalid",
            Self::InvalidExecutionContract => "MCP execution contract is invalid",
            Self::InvalidOperation => "MCP operation request is invalid",
            Self::InvalidOutcome => "MCP operation outcome is invalid",
            Self::InvalidSession => "MCP session contract is invalid",
            Self::InvalidSubscription => "MCP subscription contract is invalid",
            Self::WrongTransport => "MCP transport does not match the exact Deployment",
            Self::Canonicalization => "MCP value cannot be canonicalized",
        })
    }
}

impl Error for McpHostError {}

#[cfg(test)]
mod tests;
