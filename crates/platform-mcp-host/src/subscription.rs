use super::{
    digest, digest_without_field, placeholder_digest, EncryptedMcpState, McpHostError,
    McpHostExecutionContract, McpSessionBindingKey, McpSessionRecord,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    CommandAudit, ExactDeploymentRef, JobState, McpAuthorizationPrincipalKind, McpSessionState,
    McpTransportKind, PrincipalKind, ResourceId, ResourceKind, Sha256Digest, TraceIdentityV1,
};
use insight_platform_jobs::JobFence;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use url::Url;

pub const MAX_MCP_SUBSCRIPTION_LOGICAL_KEY_BYTES: usize = 255;
pub const MAX_MCP_RESOURCE_URI_BYTES: usize = 4_096;
pub const MAX_MCP_NOTIFICATION_BYTES: u32 = 4 * 1_024 * 1_024;
pub const MAX_MCP_SUBSCRIPTION_RECONCILE_SCAN: u16 = 256;
pub const MIN_MCP_SUBSCRIPTION_RECONCILE_IDLE_MILLISECONDS: u64 = 60_000;
pub const MAX_MCP_SUBSCRIPTION_RECONCILE_IDLE_MILLISECONDS: u64 = 86_400_000;
const MAX_MCP_NOTIFICATION_CLOCK_SKEW_SECONDS: i64 = 60;

/// Immutable identity shared by one logical MCP subscription generation and its sole physical
/// Managed stdio Sandbox session Job.
///
/// The logical subscription payload and the Sandbox Job payload both retain this exact document.
/// The physical Job ID and typed Sandbox owner ID share one UUID, while `logical_job_id` remains
/// the distinct `work_class=mcp` recovery/notification authority.
#[cfg(any())]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionIdentity {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub logical_job_id: ResourceId,
    pub admitted_subscription_version: u64,
    pub admitted_logical_job_version: u64,
    pub session_generation: u64,
    pub sandbox_job_id: ResourceId,
    pub physical_job_id: ResourceId,
    pub subscription_binding_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

#[cfg(any())]
impl ManagedMcpSandboxSessionIdentity {
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        binding: &McpResourceSubscriptionBinding,
        admitted_subscription_version: u64,
        admitted_logical_job_version: u64,
        session_generation: u64,
        sandbox_job_id: ResourceId,
        physical_job_id: ResourceId,
    ) -> Result<Self, McpHostError> {
        binding.validate_canonical()?;
        let mut identity = Self {
            schema_version: 1,
            tenant_id: binding.tenant_id.clone(),
            subscription_id: binding.subscription_id.clone(),
            logical_job_id: binding.job_id.clone(),
            admitted_subscription_version,
            admitted_logical_job_version,
            session_generation,
            sandbox_job_id,
            physical_job_id,
            subscription_binding_digest: binding.canonical_digest.clone(),
            canonical_digest: placeholder_digest()?,
        };
        identity.validate_for_binding(binding)?;
        identity.canonical_digest = digest_without_field(&identity, "canonical_digest")?;
        Ok(identity)
    }

    pub fn validate_canonical_for_binding(
        &self,
        binding: &McpResourceSubscriptionBinding,
    ) -> Result<(), McpHostError> {
        self.validate_for_binding(binding)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    fn validate_for_binding(
        &self,
        binding: &McpResourceSubscriptionBinding,
    ) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.tenant_id != binding.tenant_id
            || self.subscription_id != binding.subscription_id
            || self.logical_job_id != binding.job_id
            || self.admitted_subscription_version == 0
            || self.admitted_logical_job_version == 0
            || self.session_generation == 0
            || self.sandbox_job_id.kind() != ResourceKind::Job
            || self.physical_job_id.kind() != ResourceKind::Job
            || self.sandbox_job_id.uuid() != self.physical_job_id.uuid()
            || self.subscription_binding_digest != binding.canonical_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

/// Logical-side pointer to the immutable physical request. The request digest is intentionally
/// outside `ManagedMcpSandboxSessionIdentity`: the physical request embeds the identity, so
/// including its own digest there would create a circular canonicalization dependency.
#[cfg(any())]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpSandboxSessionLink {
    pub schema_version: u32,
    pub identity: ManagedMcpSandboxSessionIdentity,
    pub sandbox_request_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

#[cfg(any())]
impl ManagedMcpSandboxSessionLink {
    pub fn build(
        identity: ManagedMcpSandboxSessionIdentity,
        sandbox_request_digest: Sha256Digest,
        binding: &McpResourceSubscriptionBinding,
    ) -> Result<Self, McpHostError> {
        let mut link = Self {
            schema_version: 1,
            identity,
            sandbox_request_digest,
            canonical_digest: placeholder_digest()?,
        };
        link.validate_for(binding)?;
        link.canonical_digest = digest_without_field(&link, "canonical_digest")?;
        Ok(link)
    }

    pub fn validate_canonical_for(
        &self,
        binding: &McpResourceSubscriptionBinding,
    ) -> Result<(), McpHostError> {
        self.validate_for(binding)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    fn validate_for(&self, binding: &McpResourceSubscriptionBinding) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self
                .identity
                .validate_canonical_for_binding(binding)
                .is_err()
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpSubscriptionState {
    Pending,
    Active,
    RebuildRequired,
    Closing,
    Closed,
    Failed,
}

impl McpSubscriptionState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::RebuildRequired => "rebuild_required",
            Self::Closing => "closing",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Failed)
    }
}

impl fmt::Display for McpSubscriptionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for McpSubscriptionState {
    type Err = McpHostError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "active" => Ok(Self::Active),
            "rebuild_required" => Ok(Self::RebuildRequired),
            "closing" => Ok(Self::Closing),
            "closed" => Ok(Self::Closed),
            "failed" => Ok(Self::Failed),
            _ => Err(McpHostError::InvalidSubscription),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpNotificationClass {
    ResourceUpdated,
    ResourceListChanged,
    ToolListChanged,
    PromptListChanged,
}

impl McpNotificationClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResourceUpdated => "resource_updated",
            Self::ResourceListChanged => "resource_list_changed",
            Self::ToolListChanged => "tool_list_changed",
            Self::PromptListChanged => "prompt_list_changed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMcpResourceSubscriptionBinding {
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub context_deployment: ExactDeploymentRef,
    pub resource_uri: String,
}

/// Immutable durable identity of one published MCP Resource subscription.
///
/// The URI is an exact untrusted resource locator. It is never used as an endpoint or credential,
/// and notification/event projections carry only `resource_uri_digest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpResourceSubscriptionBinding {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub discovery_snapshot_id: ResourceId,
    pub discovery_snapshot_digest: Sha256Digest,
    pub protocol_profile: insight_platform_contracts::ExactVersionRef,
    pub authorization_binding_id: ResourceId,
    pub authorization_generation: u64,
    pub authorization_context_digest: Sha256Digest,
    pub scope_digest: Sha256Digest,
    pub principal_kind: McpAuthorizationPrincipalKind,
    pub principal_id: ResourceId,
    pub principal_identity_kind: PrincipalKind,
    pub principal_binding_generation: u64,
    pub server_identity_digest: Sha256Digest,
    pub transport_kind: McpTransportKind,
    pub transport_binding_digest: Sha256Digest,
    pub context_deployment: ExactDeploymentRef,
    pub resource_uri: String,
    pub resource_uri_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

impl McpResourceSubscriptionBinding {
    pub fn build(
        input: NewMcpResourceSubscriptionBinding,
        contract: &McpHostExecutionContract,
        now: DateTime<Utc>,
    ) -> Result<Self, McpHostError> {
        contract.validate_canonical_at(now)?;
        if !contract
            .protocol_profile
            .allowed_server_capabilities
            .resources
            || !contract
                .protocol_profile
                .allowed_server_capabilities
                .subscriptions
            || !contract.discovery.negotiated_capabilities.resources
            || !contract.discovery.negotiated_capabilities.subscriptions
        {
            return Err(McpHostError::InvalidSubscription);
        }
        validate_canonical_resource_uri(&input.resource_uri)?;
        let resource_uri_digest = digest(&input.resource_uri)?;
        let mut binding = Self {
            schema_version: 1,
            tenant_id: contract.authorization.tenant_id.clone(),
            subscription_id: input.subscription_id,
            job_id: input.job_id,
            mcp_deployment: contract.deployment.clone(),
            discovery_snapshot_id: contract.discovery.snapshot_id.clone(),
            discovery_snapshot_digest: contract.discovery.canonical_digest.clone(),
            protocol_profile: contract.server.protocol_policy.clone(),
            authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
            authorization_generation: contract.authorization.generation,
            authorization_context_digest: contract.authorization.canonical_digest.clone(),
            scope_digest: contract.authorization.scope_digest.clone(),
            principal_kind: contract.authorization.principal_kind,
            principal_id: contract.authorization.principal_id.clone(),
            principal_identity_kind: contract.authorization.principal_identity_kind,
            principal_binding_generation: contract.authorization.principal_binding_generation,
            server_identity_digest: contract.deployment_closure.server_identity_digest.clone(),
            transport_kind: contract.transport_kind(),
            transport_binding_digest: digest(&contract.deployment_closure.transport)?,
            context_deployment: input.context_deployment,
            resource_uri: input.resource_uri,
            resource_uri_digest,
            canonical_digest: placeholder_digest()?,
        };
        binding.validate_shape()?;
        binding.canonical_digest = digest_without_field(&binding, "canonical_digest")?;
        Ok(binding)
    }

    pub fn validate_canonical(&self) -> Result<(), McpHostError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn validate_for_execution_contract_at(
        &self,
        contract: &McpHostExecutionContract,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        self.validate_canonical()?;
        contract.validate_canonical_at(now)?;
        if self.tenant_id != contract.authorization.tenant_id
            || self.mcp_deployment != contract.deployment
            || self.discovery_snapshot_id != contract.discovery.snapshot_id
            || self.discovery_snapshot_digest != contract.discovery.canonical_digest
            || self.protocol_profile != contract.server.protocol_policy
            || self.authorization_binding_id != contract.authorization.authorization_binding_id
            || self.authorization_generation != contract.authorization.generation
            || self.authorization_context_digest != contract.authorization.canonical_digest
            || self.scope_digest != contract.authorization.scope_digest
            || self.principal_kind != contract.authorization.principal_kind
            || self.principal_id != contract.authorization.principal_id
            || self.principal_identity_kind != contract.authorization.principal_identity_kind
            || self.principal_binding_generation
                != contract.authorization.principal_binding_generation
            || self.server_identity_digest != contract.deployment_closure.server_identity_digest
            || self.transport_kind != contract.transport_kind()
            || self.transport_binding_digest != digest(&contract.deployment_closure.transport)?
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), McpHostError> {
        validate_canonical_resource_uri(&self.resource_uri)?;
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.discovery_snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
            || self.protocol_profile.resource_kind != ResourceKind::PolicyRevision
            || self.protocol_profile.validate().is_err()
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.authorization_generation == 0
            || self.principal_id.kind() != ResourceKind::Principal
            || self.principal_binding_generation == 0
            || self.context_deployment.resource_kind != ResourceKind::ContextDeployment
            || self.context_deployment.validate().is_err()
            || digest(&self.resource_uri)? != self.resource_uri_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        match self.principal_kind {
            McpAuthorizationPrincipalKind::PerUser
                if self.principal_identity_kind == PrincipalKind::ServiceIdentity =>
            {
                Err(McpHostError::InvalidSubscription)
            }
            McpAuthorizationPrincipalKind::ServiceIdentity
                if self.principal_identity_kind != PrincipalKind::ServiceIdentity =>
            {
                Err(McpHostError::InvalidSubscription)
            }
            _ => Ok(()),
        }
    }

    fn matches_session_key(&self, key: &McpSessionBindingKey) -> bool {
        key.tenant_id == self.tenant_id
            && key.deployment == self.mcp_deployment
            && key.protocol_profile == self.protocol_profile
            && key.authorization_binding_id == self.authorization_binding_id
            && key.authorization_generation == self.authorization_generation
            && key.principal_kind == self.principal_kind
            && key.principal_id == self.principal_id
            && key.principal_identity_kind == self.principal_identity_kind
            && key.principal_binding_generation == self.principal_binding_generation
            && key.scope_digest == self.scope_digest
            && key.server_identity_digest == self.server_identity_digest
            && key.transport_kind == self.transport_kind
            && key.transport_binding_digest == self.transport_binding_digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpPendingInvalidation {
    pub class: McpNotificationClass,
    pub session_generation: u64,
    pub event_generation: u64,
    pub event_key_digest: Sha256Digest,
    pub resource_uri_digest: Option<Sha256Digest>,
    pub body_digest: Sha256Digest,
    pub received_at: DateTime<Utc>,
}

impl McpPendingInvalidation {
    fn validate_for(
        &self,
        _binding: &McpResourceSubscriptionBinding,
        session: &McpSessionRecord,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        if self.session_generation == 0
            || self.session_generation != session.generation
            || self.event_generation == 0
            || self.received_at > now + Duration::seconds(MAX_MCP_NOTIFICATION_CLOCK_SKEW_SECONDS)
        {
            return Err(McpHostError::InvalidSubscription);
        }
        match self.class {
            McpNotificationClass::ResourceUpdated if self.resource_uri_digest.is_some() => Ok(()),
            McpNotificationClass::ResourceUpdated => Err(McpHostError::InvalidSubscription),
            McpNotificationClass::ResourceListChanged
            | McpNotificationClass::ToolListChanged
            | McpNotificationClass::PromptListChanged
                if self.resource_uri_digest.is_none() =>
            {
                Ok(())
            }
            McpNotificationClass::ResourceListChanged
            | McpNotificationClass::ToolListChanged
            | McpNotificationClass::PromptListChanged => Err(McpHostError::InvalidSubscription),
        }
    }
}

/// Bounded payload stored in the shared `invocations` row.
///
/// It contains no notification body, raw event key, plaintext session header or credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSubscriptionPayload {
    pub schema_version: u32,
    pub binding: McpResourceSubscriptionBinding,
    pub session: McpSessionRecord,
    #[cfg(any())]
    pub managed_sandbox_session: Option<ManagedMcpSandboxSessionLink>,
    pub last_notification_session_generation: u64,
    pub last_notification_event_generation: u64,
    pub pending_invalidation: Option<McpPendingInvalidation>,
    pub full_reconcile_required: bool,
    pub canonical_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpNotificationApplyDisposition {
    Wake,
    Coalesced,
    Stale,
}

impl McpNotificationApplyDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wake => "wake",
            Self::Coalesced => "coalesced",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpNotificationCommitOutcome {
    pub record: McpSubscriptionRecord,
    pub disposition: McpNotificationApplyDisposition,
    pub replayed: bool,
}

impl McpSubscriptionPayload {
    pub fn pending(
        binding: McpResourceSubscriptionBinding,
        session: McpSessionRecord,
    ) -> Result<Self, McpHostError> {
        let mut payload = Self {
            schema_version: 1,
            binding,
            session,
            #[cfg(any())]
            managed_sandbox_session: None,
            last_notification_session_generation: 0,
            last_notification_event_generation: 0,
            pending_invalidation: None,
            full_reconcile_required: false,
            canonical_digest: placeholder_digest()?,
        };
        payload.validate_at(Utc::now())?;
        payload.canonical_digest = digest_without_field(&payload, "canonical_digest")?;
        Ok(payload)
    }

    pub fn validate_canonical_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.validate_at(now)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.binding.validate_canonical()?;
        self.session.validate_canonical_at(now)?;
        if self.schema_version != 1
            || !self.binding.matches_session_key(&self.session.binding_key)
            || (self.last_notification_session_generation == 0)
                != (self.last_notification_event_generation == 0)
            || self.last_notification_session_generation > self.session.generation
            || (self.full_reconcile_required
                && matches!(
                    self.session.state,
                    McpSessionState::ReauthRequired
                        | McpSessionState::Draining
                        | McpSessionState::Closed
                        | McpSessionState::Failed
                ))
        {
            return Err(McpHostError::InvalidSubscription);
        }
        if let Some(invalidation) = &self.pending_invalidation {
            invalidation.validate_for(&self.binding, &self.session, now)?;
            if invalidation.session_generation != self.last_notification_session_generation
                || invalidation.event_generation != self.last_notification_event_generation
            {
                return Err(McpHostError::InvalidSubscription);
            }
        }
        Ok(())
    }

    pub fn transition_session(
        &self,
        expected_session_version: u64,
        target: McpSessionState,
        encrypted_opaque_session: Option<EncryptedMcpState>,
        expires_at: Option<DateTime<Utc>>,
        maximum_session_milliseconds: u64,
        now: DateTime<Utc>,
    ) -> Result<(Self, McpSubscriptionState), McpHostError> {
        self.validate_canonical_at(now)?;
        if self.binding.transport_kind != McpTransportKind::StreamableHttp {
            return Err(McpHostError::InvalidSubscription);
        }
        if maximum_session_milliseconds == 0
            || expires_at.is_some_and(|expiry| {
                expiry
                    > now
                        + Duration::milliseconds(
                            i64::try_from(maximum_session_milliseconds).unwrap_or(i64::MAX),
                        )
            })
        {
            return Err(McpHostError::InvalidSubscription);
        }
        let session = self.session.transition(
            expected_session_version,
            target,
            encrypted_opaque_session,
            expires_at,
            now,
        )?;
        if matches!(target, McpSessionState::Ready | McpSessionState::Degraded)
            && session.expires_at.is_none_or(|expiry| expiry <= now)
        {
            return Err(McpHostError::InvalidSubscription);
        }
        let generation_changed = session.generation != self.session.generation;
        let mut next = Self {
            schema_version: 1,
            binding: self.binding.clone(),
            session,
            #[cfg(any())]
            managed_sandbox_session: None,
            last_notification_session_generation: if generation_changed {
                0
            } else {
                self.last_notification_session_generation
            },
            last_notification_event_generation: if generation_changed {
                0
            } else {
                self.last_notification_event_generation
            },
            pending_invalidation: if generation_changed {
                None
            } else {
                self.pending_invalidation.clone()
            },
            full_reconcile_required: if matches!(
                target,
                McpSessionState::ReauthRequired
                    | McpSessionState::Draining
                    | McpSessionState::Closed
                    | McpSessionState::Failed
            ) {
                false
            } else {
                self.full_reconcile_required
            },
            canonical_digest: placeholder_digest()?,
        };
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok((next, subscription_state_for_session(target)))
    }

    /// Atomically binds the next Managed stdio session generation to its sole physical Sandbox
    /// Job. The caller must persist this payload in the same transaction that creates that Job and
    /// parks/releases the logical MCP Job lease.
    #[cfg(any())]
    pub fn schedule_managed_sandbox_session(
        &self,
        expected_session_version: u64,
        link: ManagedMcpSandboxSessionLink,
        now: DateTime<Utc>,
    ) -> Result<(Self, McpSubscriptionState), McpHostError> {
        self.validate_canonical_at(now)?;
        if self.binding.transport_kind != McpTransportKind::ManagedStdio
            || self.session.state != McpSessionState::Disconnected
            || self.managed_sandbox_session.is_some()
            || link.validate_canonical_for(&self.binding).is_err()
            || link.identity.session_generation
                != self
                    .session
                    .generation
                    .checked_add(1)
                    .ok_or(McpHostError::InvalidSubscription)?
        {
            return Err(McpHostError::InvalidSubscription);
        }
        let session = self.session.transition(
            expected_session_version,
            McpSessionState::Connecting,
            None,
            None,
            now,
        )?;
        let mut next = Self {
            schema_version: 1,
            binding: self.binding.clone(),
            session,
            managed_sandbox_session: Some(link),
            last_notification_session_generation: 0,
            last_notification_event_generation: 0,
            pending_invalidation: None,
            full_reconcile_required: self.full_reconcile_required,
            canonical_digest: placeholder_digest()?,
        };
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok((next, McpSubscriptionState::Pending))
    }

    /// Advances a Managed stdio session only when the physical Sandbox identity remains exact.
    /// Sandbox lifecycle code uses this for `Connecting -> Initializing -> Ready` and subsequent
    /// degradation/terminal observations; the generic MCP transport path cannot call it.
    #[cfg(any())]
    pub fn transition_managed_sandbox_session(
        &self,
        expected_session_version: u64,
        identity: &ManagedMcpSandboxSessionIdentity,
        target: McpSessionState,
        ready: Option<(EncryptedMcpState, DateTime<Utc>)>,
        maximum_session_milliseconds: u64,
        now: DateTime<Utc>,
    ) -> Result<(Self, McpSubscriptionState), McpHostError> {
        self.validate_canonical_at(now)?;
        let link = self
            .managed_sandbox_session
            .as_ref()
            .ok_or(McpHostError::InvalidSubscription)?;
        if self.binding.transport_kind != McpTransportKind::ManagedStdio
            || &link.identity != identity
            || link
                .identity
                .validate_canonical_for_binding(&self.binding)
                .is_err()
            || maximum_session_milliseconds == 0
            || ready.as_ref().is_some_and(|(_, expiry)| {
                *expiry
                    > now
                        + Duration::milliseconds(
                            i64::try_from(maximum_session_milliseconds).unwrap_or(i64::MAX),
                        )
            })
        {
            return Err(McpHostError::InvalidSubscription);
        }
        let (encrypted_opaque_session, expires_at) = ready.unzip();
        let session = self.session.transition(
            expected_session_version,
            target,
            encrypted_opaque_session,
            expires_at,
            now,
        )?;
        if matches!(target, McpSessionState::Ready | McpSessionState::Degraded)
            && session.expires_at.is_none_or(|expiry| expiry <= now)
        {
            return Err(McpHostError::InvalidSubscription);
        }
        let mut next = Self {
            session,
            canonical_digest: placeholder_digest()?,
            ..self.clone()
        };
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok((next, subscription_state_for_session(target)))
    }

    pub fn apply_notification(
        &self,
        notification: &McpNotificationCommit,
        now: DateTime<Utc>,
    ) -> Result<(Self, McpNotificationApplyDisposition), McpHostError> {
        self.validate_canonical_at(now)?;
        notification.validate_at(now)?;
        if notification.subscription_id != self.binding.subscription_id
            || notification.tenant_id != self.binding.tenant_id
            || notification.authorization_generation != self.binding.authorization_generation
            || notification.session_generation != self.session.generation
            || !matches!(
                self.session.state,
                McpSessionState::Ready | McpSessionState::Degraded
            )
            || self.session.expires_at.is_none_or(|expiry| expiry <= now)
            || (notification.class != McpNotificationClass::ResourceUpdated
                && notification.resource_uri_digest.is_some())
            || (self.last_notification_session_generation == notification.session_generation
                && notification.event_generation <= self.last_notification_event_generation)
        {
            return Ok((self.clone(), McpNotificationApplyDisposition::Stale));
        }
        let disposition = if self.pending_invalidation.is_some() {
            McpNotificationApplyDisposition::Coalesced
        } else {
            McpNotificationApplyDisposition::Wake
        };
        let mut next = Self {
            schema_version: 1,
            binding: self.binding.clone(),
            session: self.session.clone(),
            #[cfg(any())]
            managed_sandbox_session: self.managed_sandbox_session.clone(),
            last_notification_session_generation: notification.session_generation,
            last_notification_event_generation: notification.event_generation,
            pending_invalidation: Some(McpPendingInvalidation {
                class: notification.class,
                session_generation: notification.session_generation,
                event_generation: notification.event_generation,
                event_key_digest: notification.event_key_digest.clone(),
                resource_uri_digest: notification.resource_uri_digest.clone(),
                body_digest: notification.body_digest.clone(),
                received_at: notification.received_at,
            }),
            full_reconcile_required: self.full_reconcile_required,
            canonical_digest: placeholder_digest()?,
        };
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok((next, disposition))
    }

    pub fn acknowledge_invalidation(
        &self,
        expected_session_generation: u64,
        expected_event_generation: u64,
        now: DateTime<Utc>,
    ) -> Result<Self, McpHostError> {
        self.validate_canonical_at(now)?;
        let pending = self
            .pending_invalidation
            .as_ref()
            .ok_or(McpHostError::InvalidSubscription)?;
        if pending.session_generation != expected_session_generation
            || pending.event_generation != expected_event_generation
        {
            return Err(McpHostError::InvalidSubscription);
        }
        let mut next = Self {
            pending_invalidation: None,
            canonical_digest: placeholder_digest()?,
            ..self.clone()
        };
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok(next)
    }

    pub fn rebuild_after_session_loss(
        &self,
        expected_session_version: u64,
        now: DateTime<Utc>,
    ) -> Result<(Self, McpSubscriptionState), McpHostError> {
        self.validate_canonical_at(now)?;
        if self.binding.transport_kind != McpTransportKind::StreamableHttp {
            return Err(McpHostError::InvalidSubscription);
        }
        let session = self
            .session
            .rebuild_after_loss(expected_session_version, now)?;
        let mut next = Self {
            schema_version: 1,
            binding: self.binding.clone(),
            session,
            #[cfg(any())]
            managed_sandbox_session: None,
            last_notification_session_generation: 0,
            last_notification_event_generation: 0,
            pending_invalidation: None,
            full_reconcile_required: true,
            canonical_digest: placeholder_digest()?,
        };
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok((next, McpSubscriptionState::Pending))
    }

    /// Clears the sole physical Managed stdio generation after that exact Sandbox Job has been
    /// durably terminalized. The next admission must create a new physical identity and complete
    /// a full reconcile before the subscription may return to its steady waiting state.
    #[cfg(any())]
    pub fn rebuild_managed_sandbox_session_after_loss(
        &self,
        expected_session_version: u64,
        identity: &ManagedMcpSandboxSessionIdentity,
        now: DateTime<Utc>,
    ) -> Result<(Self, McpSubscriptionState), McpHostError> {
        self.validate_canonical_at(now)?;
        let link = self
            .managed_sandbox_session
            .as_ref()
            .ok_or(McpHostError::InvalidSubscription)?;
        if self.binding.transport_kind != McpTransportKind::ManagedStdio
            || &link.identity != identity
            || identity.session_generation != self.session.generation
        {
            return Err(McpHostError::InvalidSubscription);
        }
        let session = self
            .session
            .rebuild_after_loss(expected_session_version, now)?;
        let mut next = Self {
            schema_version: 1,
            binding: self.binding.clone(),
            session,
            #[cfg(any())]
            managed_sandbox_session: None,
            last_notification_session_generation: 0,
            last_notification_event_generation: 0,
            pending_invalidation: None,
            full_reconcile_required: true,
            canonical_digest: placeholder_digest()?,
        };
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok((next, McpSubscriptionState::Pending))
    }

    pub fn acknowledge_full_reconcile(
        &self,
        expected_session_generation: u64,
        now: DateTime<Utc>,
    ) -> Result<Self, McpHostError> {
        self.validate_canonical_at(now)?;
        if self.session.generation != expected_session_generation
            || !matches!(
                self.session.state,
                McpSessionState::Ready | McpSessionState::Degraded
            )
            || self.pending_invalidation.is_some()
        {
            return Err(McpHostError::InvalidSubscription);
        }
        let mut next = Self {
            full_reconcile_required: false,
            canonical_digest: placeholder_digest()?,
            ..self.clone()
        };
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok(next)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSubscriptionJobPayload {
    pub schema_version: u32,
    pub subscription_id: ResourceId,
    pub binding_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

impl McpSubscriptionJobPayload {
    pub fn build(binding: &McpResourceSubscriptionBinding) -> Result<Self, McpHostError> {
        binding.validate_canonical()?;
        let mut payload = Self {
            schema_version: 1,
            subscription_id: binding.subscription_id.clone(),
            binding_digest: binding.canonical_digest.clone(),
            canonical_digest: placeholder_digest()?,
        };
        payload.canonical_digest = digest_without_field(&payload, "canonical_digest")?;
        payload.validate_for(&binding.subscription_id)?;
        Ok(payload)
    }

    pub fn validate_for(&self, owner_id: &ResourceId) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.subscription_id != *owner_id
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSubscriptionRecord {
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub logical_key: String,
    pub state: McpSubscriptionState,
    pub version: u64,
    pub payload: McpSubscriptionPayload,
    pub deadline: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
}

impl McpSubscriptionRecord {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.payload.validate_canonical_at(now)?;
        if self.tenant_id != self.payload.binding.tenant_id
            || self.subscription_id != self.payload.binding.subscription_id
            || self.job_id != self.payload.binding.job_id
            || !valid_logical_key(&self.logical_key)
            || self.version == 0
            || self.deadline < self.created_at
            || self.updated_at < self.created_at
            || self.state.is_terminal() != self.terminal_at.is_some()
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CreateMcpResourceSubscription {
    pub audit: CommandAudit,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub logical_key: String,
    pub execution: super::McpExecutionContractQuery,
    pub context_deployment: ExactDeploymentRef,
    pub resource_uri: String,
    pub attempt_limit: u16,
    pub deadline: DateTime<Utc>,
}

impl CreateMcpResourceSubscription {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit
            .validate_at(now)
            .map_err(|_| McpHostError::InvalidSubscription)?;
        self.execution
            .validate()
            .map_err(|_| McpHostError::InvalidSubscription)?;
        validate_canonical_resource_uri(&self.resource_uri)?;
        if self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || !valid_logical_key(&self.logical_key)
            || self.execution.tenant_id != self.audit.tenant_id
            || self.execution.principal_id != self.audit.principal_id
            || self.context_deployment.resource_kind != ResourceKind::ContextDeployment
            || self.context_deployment.validate().is_err()
            || self.attempt_limit == 0
            || self.attempt_limit > 8
            || self.deadline <= now
            || self.request_digest()? != self.audit.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "attempt_limit": self.attempt_limit,
            "context_deployment": self.context_deployment,
            "deadline": self.deadline,
            "execution": self.execution,
            "job_id": self.job_id,
            "logical_key": self.logical_key,
            "principal_id": self.audit.principal_id,
            "principal_kind": self.audit.principal_kind,
            "resource_uri": self.resource_uri,
            "schema_version": 1,
            "subscription_id": self.subscription_id,
            "tenant_id": self.audit.tenant_id,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSubscriptionWorkerAudit {
    pub trace: TraceIdentityV1,
    pub tenant_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl McpSubscriptionWorkerAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        if self.trace.validate().is_err()
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SaveMcpSubscriptionSession {
    pub audit: McpSubscriptionWorkerAudit,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub expected_subscription_version: u64,
    pub expected_session_version: u64,
    pub target: McpSessionState,
    pub encrypted_opaque_session: Option<EncryptedMcpState>,
    pub expires_at: Option<DateTime<Utc>>,
    pub phase_evidence_digest: Option<Sha256Digest>,
}

impl SaveMcpSubscriptionSession {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        if self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_subscription_version == 0
            || self.expected_session_version == 0
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.audit.worker_process_generation_id != self.fence.worker_process_generation_id
            || matches!(
                self.target,
                McpSessionState::Ready
                    | McpSessionState::Degraded
                    | McpSessionState::ReauthRequired
                    | McpSessionState::Failed
            ) != self.phase_evidence_digest.is_some()
            || self.request_digest()? != self.audit.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "encrypted_opaque_session_digest": self.encrypted_opaque_session.as_ref().map(digest).transpose()?,
            "expected_session_version": self.expected_session_version,
            "expected_subscription_version": self.expected_subscription_version,
            "expires_at": self.expires_at,
            "fence": {
                "expected_version": self.fence.expected_version,
                "lease_generation": self.fence.lease_generation,
                "token_digest": self.fence.token_digest,
                "worker_process_generation_id": self.fence.worker_process_generation_id,
            },
            "job_id": self.job_id,
            "phase_evidence_digest": self.phase_evidence_digest,
            "schema_version": 1,
            "subscription_id": self.subscription_id,
            "target": self.target,
            "tenant_id": self.audit.tenant_id,
            "worker_process_generation_id": self.audit.worker_process_generation_id,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpNotificationAudit {
    pub tenant_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub receipt_expires_at: DateTime<Utc>,
}

impl McpNotificationAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpNotificationCommit {
    pub audit: McpNotificationAudit,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub authorization_generation: u64,
    pub session_generation: u64,
    pub event_key_digest: Sha256Digest,
    pub event_generation: u64,
    pub class: McpNotificationClass,
    pub resource_uri_digest: Option<Sha256Digest>,
    pub body_digest: Sha256Digest,
    pub wire_bytes: u32,
    pub received_at: DateTime<Utc>,
}

impl McpNotificationCommit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.tenant_id != self.audit.tenant_id
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.authorization_generation == 0
            || self.session_generation == 0
            || self.event_generation == 0
            || self.wire_bytes == 0
            || self.wire_bytes > MAX_MCP_NOTIFICATION_BYTES
            || self.received_at > now + Duration::seconds(MAX_MCP_NOTIFICATION_CLOCK_SKEW_SECONDS)
            || (self.class == McpNotificationClass::ResourceUpdated)
                != self.resource_uri_digest.is_some()
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "authorization_generation": self.authorization_generation,
            "body_digest": self.body_digest,
            "class": self.class,
            "event_generation": self.event_generation,
            "event_key_digest": self.event_key_digest,
            "resource_uri_digest": self.resource_uri_digest,
            "schema_version": 1,
            "session_generation": self.session_generation,
            "subscription_id": self.subscription_id,
            "tenant_id": self.tenant_id,
            "wire_bytes": self.wire_bytes,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct CompleteMcpSubscriptionRefresh {
    pub audit: McpSubscriptionWorkerAudit,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub expected_subscription_version: u64,
    pub expected_session_generation: u64,
    pub expected_event_generation: u64,
    pub refresh_evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone)]
pub struct CompleteMcpSubscriptionReconcile {
    pub audit: McpSubscriptionWorkerAudit,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub expected_subscription_version: u64,
    pub expected_session_generation: u64,
    pub reconcile_evidence_digest: Sha256Digest,
}

impl CompleteMcpSubscriptionReconcile {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        if self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_subscription_version == 0
            || self.expected_session_generation == 0
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.audit.worker_process_generation_id != self.fence.worker_process_generation_id
            || self.request_digest()? != self.audit.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "expected_session_generation": self.expected_session_generation,
            "expected_subscription_version": self.expected_subscription_version,
            "fence": {
                "expected_version": self.fence.expected_version,
                "lease_generation": self.fence.lease_generation,
                "token_digest": self.fence.token_digest,
                "worker_process_generation_id": self.fence.worker_process_generation_id,
            },
            "job_id": self.job_id,
            "reconcile_evidence_digest": self.reconcile_evidence_digest,
            "schema_version": 1,
            "subscription_id": self.subscription_id,
            "tenant_id": self.audit.tenant_id,
            "worker_process_generation_id": self.audit.worker_process_generation_id,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSubscriptionReconcileScan {
    pub tenant_id: ResourceId,
    pub limit: u16,
    pub minimum_idle_milliseconds: u64,
}

impl McpSubscriptionReconcileScan {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.limit == 0
            || self.limit > MAX_MCP_SUBSCRIPTION_RECONCILE_SCAN
            || self.minimum_idle_milliseconds < MIN_MCP_SUBSCRIPTION_RECONCILE_IDLE_MILLISECONDS
            || self.minimum_idle_milliseconds > MAX_MCP_SUBSCRIPTION_RECONCILE_IDLE_MILLISECONDS
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DueMcpSubscriptionReconcile {
    pub trace: TraceIdentityV1,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub subscription_version: u64,
    pub job_version: u64,
    pub session_generation: u64,
    pub not_updated_after: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}

impl DueMcpSubscriptionReconcile {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.trace.validate().is_err()
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.subscription_version == 0
            || self.job_version == 0
            || self.session_generation == 0
            || self.not_updated_after >= self.observed_at
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct WakeMcpSubscriptionReconcile {
    pub audit: McpSubscriptionWorkerAudit,
    pub candidate: DueMcpSubscriptionReconcile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpSubscriptionRecoveryCause {
    ExpiredLease,
    ExpiredSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSubscriptionRecoveryScan {
    pub tenant_id: ResourceId,
    pub limit: u16,
}

impl McpSubscriptionRecoveryScan {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.limit == 0
            || self.limit > MAX_MCP_SUBSCRIPTION_RECONCILE_SCAN
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DueMcpSubscriptionRecovery {
    pub trace: TraceIdentityV1,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub subscription_version: u64,
    pub session_version: u64,
    pub session_generation: u64,
    pub job_version: u64,
    pub observed_job_state: JobState,
    pub observed_lease_generation: Option<u64>,
    pub observed_lease_expires_at: Option<DateTime<Utc>>,
    pub observed_session_expires_at: Option<DateTime<Utc>>,
    pub cause: McpSubscriptionRecoveryCause,
    pub observed_at: DateTime<Utc>,
}

impl DueMcpSubscriptionRecovery {
    pub fn validate(&self) -> Result<(), McpHostError> {
        let common_invalid = self.trace.validate().is_err()
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.subscription_version == 0
            || self.session_version == 0
            || self.job_version == 0;
        let cause_invalid = match self.cause {
            McpSubscriptionRecoveryCause::ExpiredLease => {
                !matches!(
                    self.observed_job_state,
                    JobState::Leased | JobState::Running
                ) || self
                    .observed_lease_generation
                    .is_none_or(|value| value == 0)
                    || self
                        .observed_lease_expires_at
                        .is_none_or(|expiry| expiry > self.observed_at)
            }
            McpSubscriptionRecoveryCause::ExpiredSession => {
                self.observed_job_state != JobState::Waiting
                    || self.observed_lease_generation.is_some()
                    || self.observed_lease_expires_at.is_some()
                    || self.session_generation == 0
                    || self
                        .observed_session_expires_at
                        .is_none_or(|expiry| expiry > self.observed_at)
            }
        };
        if common_invalid || cause_invalid {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RecoverDueMcpSubscription {
    pub audit: McpSubscriptionWorkerAudit,
    pub candidate: DueMcpSubscriptionRecovery,
}

impl RecoverDueMcpSubscription {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        self.candidate.validate()?;
        if self.audit.tenant_id != self.candidate.tenant_id
            || self.candidate.observed_at
                > now + Duration::seconds(MAX_MCP_NOTIFICATION_CLOCK_SKEW_SECONDS)
            || self.request_digest()? != self.audit.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "candidate": self.candidate,
            "schema_version": 1,
            "worker_process_generation_id": self.audit.worker_process_generation_id,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct ReportMcpSubscriptionSessionLoss {
    pub audit: McpSubscriptionWorkerAudit,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub expected_subscription_version: u64,
    pub expected_session_version: u64,
    pub expected_session_generation: u64,
    pub reported_at: DateTime<Utc>,
    pub session_loss_evidence_digest: Sha256Digest,
}

/// Asynchronous transport termination signal. Unlike the worker recovery command it does not
/// carry caller-observed aggregate versions: PostgreSQL must lock the current subscription and
/// atomically compare the exact authorization/session generation before scheduling a rebuild.
#[derive(Debug, Clone)]
pub struct ReportMcpSubscriptionTransportTermination {
    pub audit: McpSubscriptionWorkerAudit,
    pub subscription_id: ResourceId,
    pub expected_authorization_generation: u64,
    pub expected_session_generation: u64,
    pub reported_at: DateTime<Utc>,
    pub session_loss_evidence_digest: Sha256Digest,
}

impl ReportMcpSubscriptionTransportTermination {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        if self.subscription_id.kind() != ResourceKind::McpOperation
            || self.expected_authorization_generation == 0
            || self.expected_session_generation == 0
            || self.reported_at > now + Duration::seconds(MAX_MCP_NOTIFICATION_CLOCK_SKEW_SECONDS)
            || self.request_digest()? != self.audit.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "expected_authorization_generation": self.expected_authorization_generation,
            "expected_session_generation": self.expected_session_generation,
            "reported_at": self.reported_at,
            "schema_version": 1,
            "session_loss_evidence_digest": self.session_loss_evidence_digest,
            "subscription_id": self.subscription_id,
            "tenant_id": self.audit.tenant_id,
            "worker_process_generation_id": self.audit.worker_process_generation_id,
        }))
    }
}

impl ReportMcpSubscriptionSessionLoss {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        if self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_subscription_version == 0
            || self.expected_session_version == 0
            || self.expected_session_generation == 0
            || self.reported_at > now + Duration::seconds(MAX_MCP_NOTIFICATION_CLOCK_SKEW_SECONDS)
            || self.request_digest()? != self.audit.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "expected_session_generation": self.expected_session_generation,
            "expected_session_version": self.expected_session_version,
            "expected_subscription_version": self.expected_subscription_version,
            "job_id": self.job_id,
            "reported_at": self.reported_at,
            "schema_version": 1,
            "session_loss_evidence_digest": self.session_loss_evidence_digest,
            "subscription_id": self.subscription_id,
            "tenant_id": self.audit.tenant_id,
            "worker_process_generation_id": self.audit.worker_process_generation_id,
        }))
    }
}

impl WakeMcpSubscriptionReconcile {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        self.candidate.validate()?;
        if self.audit.tenant_id != self.candidate.tenant_id
            || self.candidate.observed_at
                > now + Duration::seconds(MAX_MCP_NOTIFICATION_CLOCK_SKEW_SECONDS)
            || self.request_digest()? != self.audit.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "job_id": self.candidate.job_id,
            "job_version": self.candidate.job_version,
            "not_updated_after": self.candidate.not_updated_after,
            "observed_at": self.candidate.observed_at,
            "schema_version": 1,
            "session_generation": self.candidate.session_generation,
            "subscription_id": self.candidate.subscription_id,
            "subscription_version": self.candidate.subscription_version,
            "tenant_id": self.candidate.tenant_id,
            "worker_process_generation_id": self.audit.worker_process_generation_id,
        }))
    }
}

impl CompleteMcpSubscriptionRefresh {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        if self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_subscription_version == 0
            || self.expected_session_generation == 0
            || self.expected_event_generation == 0
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.audit.worker_process_generation_id != self.fence.worker_process_generation_id
            || self.request_digest()? != self.audit.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    pub fn request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "expected_event_generation": self.expected_event_generation,
            "expected_session_generation": self.expected_session_generation,
            "refresh_evidence_digest": self.refresh_evidence_digest,
            "expected_subscription_version": self.expected_subscription_version,
            "fence": {
                "expected_version": self.fence.expected_version,
                "lease_generation": self.fence.lease_generation,
                "token_digest": self.fence.token_digest,
                "worker_process_generation_id": self.fence.worker_process_generation_id,
            },
            "job_id": self.job_id,
            "schema_version": 1,
            "subscription_id": self.subscription_id,
            "tenant_id": self.audit.tenant_id,
            "worker_process_generation_id": self.audit.worker_process_generation_id,
        }))
    }
}

fn subscription_state_for_session(state: McpSessionState) -> McpSubscriptionState {
    match state {
        McpSessionState::Disconnected
        | McpSessionState::Connecting
        | McpSessionState::Initializing => McpSubscriptionState::Pending,
        McpSessionState::Ready | McpSessionState::Degraded => McpSubscriptionState::Active,
        McpSessionState::ReauthRequired => McpSubscriptionState::RebuildRequired,
        McpSessionState::Draining => McpSubscriptionState::Closing,
        McpSessionState::Closed => McpSubscriptionState::Closed,
        McpSessionState::Failed => McpSubscriptionState::Failed,
    }
}

fn valid_logical_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MCP_SUBSCRIPTION_LOGICAL_KEY_BYTES
        && !value.chars().any(char::is_control)
}

fn validate_canonical_resource_uri(value: &str) -> Result<(), McpHostError> {
    if value.is_empty()
        || value.len() > MAX_MCP_RESOURCE_URI_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(McpHostError::InvalidSubscription);
    }
    let parsed = Url::parse(value).map_err(|_| McpHostError::InvalidSubscription)?;
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || parsed.to_string() != value
    {
        return Err(McpHostError::InvalidSubscription);
    }
    Ok(())
}

pub(crate) fn canonical_mcp_resource_uri_digest(value: &str) -> Result<Sha256Digest, McpHostError> {
    validate_canonical_resource_uri(value)?;
    digest(&value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{ExactVersionRef, McpTransportKind};

    #[test]
    fn subscription_state_wire_is_closed() {
        for (state, wire) in [
            (McpSubscriptionState::Pending, "pending"),
            (McpSubscriptionState::Active, "active"),
            (McpSubscriptionState::RebuildRequired, "rebuild_required"),
            (McpSubscriptionState::Closing, "closing"),
            (McpSubscriptionState::Closed, "closed"),
            (McpSubscriptionState::Failed, "failed"),
        ] {
            assert_eq!(state.to_string(), wire);
            assert_eq!(wire.parse::<McpSubscriptionState>().unwrap(), state);
        }
        assert!("ready".parse::<McpSubscriptionState>().is_err());
    }

    #[test]
    fn resource_uri_requires_one_canonical_credential_free_identity() {
        assert!(validate_canonical_resource_uri("mcp://catalog.example/items/42").is_ok());
        assert!(validate_canonical_resource_uri("mcp://user@catalog.example/items/42").is_err());
        assert!(validate_canonical_resource_uri("mcp://catalog.example/items/../42").is_err());
        assert!(validate_canonical_resource_uri("mcp://catalog.example/items/42#secret").is_err());
    }

    #[test]
    fn notification_request_digest_excludes_delivery_identity_and_receive_time() {
        let now = Utc::now();
        let build = |suffix: &str, received_at: DateTime<Utc>| McpNotificationCommit {
            audit: McpNotificationAudit {
                tenant_id: id("ten", "90"),
                receipt_id: id("rcp", suffix),
                event_id: id("evt", suffix),
                outbox_id: id("obx", suffix),
                receipt_expires_at: now + Duration::minutes(5),
            },
            tenant_id: id("ten", "90"),
            subscription_id: id("mop", "91"),
            authorization_generation: 2,
            session_generation: 3,
            event_key_digest: sha('a'),
            event_generation: 4,
            class: McpNotificationClass::ResourceListChanged,
            resource_uri_digest: None,
            body_digest: sha('b'),
            wire_bytes: 128,
            received_at,
        };
        assert_eq!(
            build("92", now).request_digest().unwrap(),
            build("93", now + Duration::seconds(1))
                .request_digest()
                .unwrap()
        );
    }

    #[test]
    fn session_generation_fences_and_notifications_coalesce_until_refresh() {
        let now = Utc::now();
        let (payload, uri_digest) = payload_fixture(now);
        let (payload, state) = payload
            .transition_session(1, McpSessionState::Connecting, None, None, 60_000, now)
            .unwrap();
        assert_eq!(state, McpSubscriptionState::Pending);
        let (payload, _) = payload
            .transition_session(2, McpSessionState::Initializing, None, None, 60_000, now)
            .unwrap();
        let secret_canary = b"opaque-session-canary".to_vec();
        let encrypted = EncryptedMcpState {
            scheme: "aes256_gcm_v1".to_owned(),
            ciphertext: secret_canary.clone(),
            key_id: "session-key-canary".to_owned(),
            key_reference_digest: sha('1'),
            plaintext_digest: sha('e'),
        };
        let (payload, state) = payload
            .transition_session(
                3,
                McpSessionState::Ready,
                Some(encrypted.clone()),
                Some(now + Duration::seconds(30)),
                60_000,
                now,
            )
            .unwrap();
        assert_eq!(state, McpSubscriptionState::Active);
        let debug = format!("{encrypted:?}");
        assert!(!debug.contains("opaque-session-canary"));
        assert!(!debug.contains("session-key-canary"));
        assert!(!debug.contains(sha('e').as_str()));

        let first = notification(
            now,
            1,
            McpNotificationClass::ResourceUpdated,
            Some(uri_digest),
        );
        let (payload, disposition) = payload.apply_notification(&first, now).unwrap();
        assert_eq!(disposition, McpNotificationApplyDisposition::Wake);
        let second = notification(now, 2, McpNotificationClass::ResourceListChanged, None);
        let (payload, disposition) = payload.apply_notification(&second, now).unwrap();
        assert_eq!(disposition, McpNotificationApplyDisposition::Coalesced);
        assert_eq!(
            payload
                .pending_invalidation
                .as_ref()
                .unwrap()
                .event_generation,
            2
        );
        let stale = notification(now, 1, McpNotificationClass::ResourceListChanged, None);
        let (same, disposition) = payload.apply_notification(&stale, now).unwrap();
        assert_eq!(disposition, McpNotificationApplyDisposition::Stale);
        assert_eq!(same, payload);

        let payload = payload.acknowledge_invalidation(1, 2, now).unwrap();
        assert!(payload.pending_invalidation.is_none());
        assert_eq!(payload.last_notification_event_generation, 2);

        let (payload, _) = payload
            .transition_session(
                4,
                McpSessionState::ReauthRequired,
                Some(encrypted),
                Some(now + Duration::seconds(30)),
                60_000,
                now,
            )
            .unwrap();
        let (payload, state) = payload
            .transition_session(5, McpSessionState::Connecting, None, None, 60_000, now)
            .unwrap();
        assert_eq!(state, McpSubscriptionState::Pending);
        assert_eq!(payload.session.generation, 2);
        assert_eq!(payload.last_notification_session_generation, 0);
        assert_eq!(payload.last_notification_event_generation, 0);
        assert!(payload.pending_invalidation.is_none());
    }

    #[test]
    fn session_loss_discards_opaque_state_and_requires_full_reconcile() {
        let now = Utc::now();
        let (payload, _) = ready_payload_fixture(now);
        let prior_generation = payload.session.generation;
        let prior_version = payload.session.version;
        let (rebuilding, state) = payload
            .rebuild_after_session_loss(prior_version, now)
            .unwrap();
        assert_eq!(state, McpSubscriptionState::Pending);
        assert_eq!(rebuilding.session.state, McpSessionState::Disconnected);
        assert_eq!(rebuilding.session.generation, prior_generation);
        assert_eq!(rebuilding.session.version, prior_version + 1);
        assert!(rebuilding.session.encrypted_opaque_session.is_none());
        assert!(rebuilding.session.expires_at.is_none());
        assert!(rebuilding.full_reconcile_required);

        let (connecting, _) = rebuilding
            .transition_session(
                rebuilding.session.version,
                McpSessionState::Connecting,
                None,
                None,
                60_000,
                now,
            )
            .unwrap();
        assert_eq!(connecting.session.generation, prior_generation + 1);
        assert!(connecting.full_reconcile_required);
        assert!(connecting
            .acknowledge_full_reconcile(connecting.session.generation, now)
            .is_err());
    }

    #[cfg(any())]
    #[test]
    fn managed_subscription_generation_has_one_exact_physical_sandbox_job() {
        let now = Utc::now();
        let payload = managed_payload_fixture(now);
        assert!(payload
            .transition_session(1, McpSessionState::Connecting, None, None, 60_000, now)
            .is_err());

        let identity = ManagedMcpSandboxSessionIdentity::build(
            &payload.binding,
            7,
            11,
            1,
            id("job", "a0"),
            id("job", "a0"),
        )
        .unwrap();
        let link =
            ManagedMcpSandboxSessionLink::build(identity.clone(), sha('9'), &payload.binding)
                .unwrap();
        let (connecting, state) = payload
            .schedule_managed_sandbox_session(1, link, now)
            .unwrap();
        assert_eq!(state, McpSubscriptionState::Pending);
        assert_eq!(connecting.session.generation, 1);
        assert_eq!(
            connecting
                .managed_sandbox_session
                .as_ref()
                .unwrap()
                .identity,
            identity
        );

        let (initializing, _) = connecting
            .transition_managed_sandbox_session(
                2,
                &identity,
                McpSessionState::Initializing,
                None,
                60_000,
                now,
            )
            .unwrap();
        let encrypted = EncryptedMcpState {
            scheme: "aes256_gcm_v1".to_owned(),
            ciphertext: vec![1, 2, 3],
            key_id: "key-1".to_owned(),
            key_reference_digest: sha('1'),
            plaintext_digest: sha('e'),
        };
        let (ready, state) = initializing
            .transition_managed_sandbox_session(
                3,
                &identity,
                McpSessionState::Ready,
                Some((encrypted, now + Duration::seconds(30))),
                60_000,
                now,
            )
            .unwrap();
        assert_eq!(state, McpSubscriptionState::Active);
        assert!(ready.validate_canonical_at(now).is_ok());
        assert!(ready
            .rebuild_after_session_loss(ready.session.version, now)
            .is_err());
        let (rebuilding, state) = ready
            .rebuild_managed_sandbox_session_after_loss(ready.session.version, &identity, now)
            .unwrap();
        assert_eq!(state, McpSubscriptionState::Pending);
        assert_eq!(rebuilding.session.state, McpSessionState::Disconnected);
        assert_eq!(rebuilding.session.generation, identity.session_generation);
        assert!(rebuilding.session.encrypted_opaque_session.is_none());
        assert!(rebuilding.managed_sandbox_session.is_none());
        assert!(rebuilding.full_reconcile_required);

        let wrong_job = ManagedMcpSandboxSessionIdentity::build(
            &payload.binding,
            7,
            11,
            1,
            id("job", "a1"),
            id("job", "a2"),
        );
        assert!(wrong_job.is_err());
    }

    #[test]
    fn sub_resource_update_invalidates_exact_root_but_wrong_session_is_stale() {
        let now = Utc::now();
        let (payload, uri_digest) = ready_payload_fixture(now);
        let sub_resource = notification(
            now,
            1,
            McpNotificationClass::ResourceUpdated,
            Some(sha('f')),
        );
        let (updated, disposition) = payload.apply_notification(&sub_resource, now).unwrap();
        assert_eq!(disposition, McpNotificationApplyDisposition::Wake);
        assert_eq!(
            updated
                .pending_invalidation
                .as_ref()
                .unwrap()
                .resource_uri_digest,
            Some(sha('f'))
        );

        let mut wrong_session = notification(
            now,
            1,
            McpNotificationClass::ResourceUpdated,
            Some(uri_digest),
        );
        wrong_session.session_generation = 2;
        let (same, disposition) = payload.apply_notification(&wrong_session, now).unwrap();
        assert_eq!(disposition, McpNotificationApplyDisposition::Stale);
        assert_eq!(same, payload);
    }

    #[test]
    fn worker_request_digest_binds_the_exact_fence_and_encrypted_state() {
        let now = Utc::now();
        let (payload, _) = payload_fixture(now);
        let mut command = SaveMcpSubscriptionSession {
            audit: McpSubscriptionWorkerAudit {
                trace: TraceIdentityV1::generate(),
                tenant_id: payload.binding.tenant_id.clone(),
                worker_process_generation_id: id("wrk", "ab"),
                receipt_id: id("rcp", "ac"),
                event_id: id("evt", "ad"),
                outbox_id: id("obx", "ae"),
                idempotency_key_digest: sha('a'),
                request_digest: sha('b'),
                receipt_expires_at: now + Duration::minutes(5),
            },
            subscription_id: payload.binding.subscription_id.clone(),
            job_id: payload.binding.job_id.clone(),
            fence: JobFence {
                expected_version: 4,
                worker_process_generation_id: id("wrk", "ab"),
                lease_generation: 2,
                token_digest: sha('c'),
            },
            expected_subscription_version: 3,
            expected_session_version: payload.session.version,
            target: McpSessionState::Connecting,
            encrypted_opaque_session: None,
            expires_at: None,
            phase_evidence_digest: None,
        };
        command.audit.request_digest = command.request_digest().unwrap();
        command.validate_at(now).unwrap();

        command.encrypted_opaque_session = Some(EncryptedMcpState {
            scheme: "aes256_gcm_v1".to_owned(),
            ciphertext: vec![1, 2, 3],
            key_id: "key".to_owned(),
            key_reference_digest: sha('1'),
            plaintext_digest: sha('d'),
        });
        assert!(command.validate_at(now).is_err());
    }

    #[test]
    fn subscription_binding_cannot_drift_from_transport_session_identity() {
        let now = Utc::now();
        let (payload, _) = payload_fixture(now);
        let mut binding = payload.binding.clone();
        binding.transport_binding_digest = sha('f');
        binding.canonical_digest = digest_without_field(&binding, "canonical_digest").unwrap();
        assert!(McpSubscriptionPayload::pending(binding, payload.session).is_err());
    }

    fn ready_payload_fixture(now: DateTime<Utc>) -> (McpSubscriptionPayload, Sha256Digest) {
        let (payload, uri_digest) = payload_fixture(now);
        let (payload, _) = payload
            .transition_session(1, McpSessionState::Connecting, None, None, 60_000, now)
            .unwrap();
        let (payload, _) = payload
            .transition_session(2, McpSessionState::Initializing, None, None, 60_000, now)
            .unwrap();
        let (payload, _) = payload
            .transition_session(
                3,
                McpSessionState::Ready,
                Some(EncryptedMcpState {
                    scheme: "aes256_gcm_v1".to_owned(),
                    ciphertext: vec![1, 2, 3],
                    key_id: "key-1".to_owned(),
                    key_reference_digest: sha('1'),
                    plaintext_digest: sha('e'),
                }),
                Some(now + Duration::seconds(30)),
                60_000,
                now,
            )
            .unwrap();
        (payload, uri_digest)
    }

    fn payload_fixture(now: DateTime<Utc>) -> (McpSubscriptionPayload, Sha256Digest) {
        let deployment = ExactDeploymentRef::new(id("mcdep", "94"), sha('1')).unwrap();
        let profile = ExactVersionRef::new(id("prev", "95"), sha('2')).unwrap();
        let uri = "mcp://catalog.example/items/42".to_owned();
        let uri_digest = digest(&uri).unwrap();
        let session_key = McpSessionBindingKey {
            schema_version: 1,
            tenant_id: id("ten", "90"),
            deployment: deployment.clone(),
            protocol_profile: profile.clone(),
            authorization_binding_id: id("mab", "96"),
            authorization_generation: 2,
            principal_kind: McpAuthorizationPrincipalKind::PerUser,
            principal_id: id("prn", "97"),
            principal_identity_kind: PrincipalKind::AgentRunner,
            principal_binding_generation: 3,
            scope_digest: sha('3'),
            server_identity_digest: sha('4'),
            transport_kind: McpTransportKind::StreamableHttp,
            transport_binding_digest: sha('5'),
            canonical_digest: placeholder_digest().unwrap(),
        };
        let mut session_key = session_key;
        session_key.canonical_digest =
            digest_without_field(&session_key, "canonical_digest").unwrap();
        let session = McpSessionRecord::disconnected(session_key).unwrap();
        let binding = McpResourceSubscriptionBinding {
            schema_version: 1,
            tenant_id: id("ten", "90"),
            subscription_id: id("mop", "91"),
            job_id: id("job", "92"),
            mcp_deployment: deployment,
            discovery_snapshot_id: id("mdsc", "98"),
            discovery_snapshot_digest: sha('6'),
            protocol_profile: profile,
            authorization_binding_id: id("mab", "96"),
            authorization_generation: 2,
            authorization_context_digest: sha('7'),
            scope_digest: sha('3'),
            principal_kind: McpAuthorizationPrincipalKind::PerUser,
            principal_id: id("prn", "97"),
            principal_identity_kind: PrincipalKind::AgentRunner,
            principal_binding_generation: 3,
            server_identity_digest: sha('4'),
            transport_kind: McpTransportKind::StreamableHttp,
            transport_binding_digest: sha('5'),
            context_deployment: ExactDeploymentRef::new(id("xdep", "99"), sha('8')).unwrap(),
            resource_uri: uri,
            resource_uri_digest: uri_digest.clone(),
            canonical_digest: placeholder_digest().unwrap(),
        };
        let mut binding = binding;
        binding.canonical_digest = digest_without_field(&binding, "canonical_digest").unwrap();
        let payload = McpSubscriptionPayload::pending(binding, session).unwrap();
        payload.validate_canonical_at(now).unwrap();
        (payload, uri_digest)
    }

    #[cfg(any())]
    fn managed_payload_fixture(now: DateTime<Utc>) -> McpSubscriptionPayload {
        let (payload, _) = payload_fixture(now);
        let mut binding = payload.binding;
        binding.transport_kind = McpTransportKind::ManagedStdio;
        binding.transport_binding_digest = sha('9');
        binding.canonical_digest = digest_without_field(&binding, "canonical_digest").unwrap();
        let mut key = payload.session.binding_key;
        key.transport_kind = McpTransportKind::ManagedStdio;
        key.transport_binding_digest = sha('9');
        key.canonical_digest = digest_without_field(&key, "canonical_digest").unwrap();
        let session = McpSessionRecord::disconnected(key).unwrap();
        McpSubscriptionPayload::pending(binding, session).unwrap()
    }

    fn notification(
        now: DateTime<Utc>,
        event_generation: u64,
        class: McpNotificationClass,
        resource_uri_digest: Option<Sha256Digest>,
    ) -> McpNotificationCommit {
        McpNotificationCommit {
            audit: McpNotificationAudit {
                tenant_id: id("ten", "90"),
                receipt_id: id("rcp", &format!("a{event_generation}")),
                event_id: id("evt", &format!("a{event_generation}")),
                outbox_id: id("obx", &format!("a{event_generation}")),
                receipt_expires_at: now + Duration::minutes(5),
            },
            tenant_id: id("ten", "90"),
            subscription_id: id("mop", "91"),
            authorization_generation: 2,
            session_generation: 1,
            event_key_digest: sha(char::from_digit(event_generation as u32, 16).unwrap()),
            event_generation,
            class,
            resource_uri_digest,
            body_digest: sha('b'),
            wire_bytes: 128,
            received_at: now,
        }
    }

    fn id(prefix: &str, tail: &str) -> ResourceId {
        format!("{prefix}_0198f1c8-32e4-75e1-a9e8-d95ca0f600{tail}")
            .parse()
            .unwrap()
    }

    fn sha(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }
}
