use super::{
    digest, digest_without_field, placeholder_digest, EncryptedMcpState, McpHostError,
    McpHostExecutionContract,
};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ExactDeploymentRef, ExactVersionRef, McpAuthorizationPrincipalKind, McpSessionState,
    McpTransportKind, PrincipalKind, ResourceId, ResourceKind, Sha256Digest,
};
use serde::{Deserialize, Serialize};

/// Complete isolation key for a reconstructable MCP transport session.
///
/// It deliberately contains no session ID or credential. Any change in principal, authorization
/// generation, scope, protocol, Deployment or transport closure produces a different key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSessionBindingKey {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub deployment: ExactDeploymentRef,
    pub protocol_profile: ExactVersionRef,
    pub authorization_binding_id: ResourceId,
    pub authorization_generation: u64,
    pub principal_kind: McpAuthorizationPrincipalKind,
    pub principal_id: ResourceId,
    pub principal_identity_kind: PrincipalKind,
    pub principal_binding_generation: u64,
    pub scope_digest: Sha256Digest,
    pub server_identity_digest: Sha256Digest,
    pub transport_kind: McpTransportKind,
    pub transport_binding_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

impl McpSessionBindingKey {
    pub fn build(contract: &McpHostExecutionContract) -> Result<Self, McpHostError> {
        let mut key = Self {
            schema_version: 1,
            tenant_id: contract.authorization.tenant_id.clone(),
            deployment: contract.deployment.clone(),
            protocol_profile: contract.server.protocol_policy.clone(),
            authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
            authorization_generation: contract.authorization.generation,
            principal_kind: contract.authorization.principal_kind,
            principal_id: contract.authorization.principal_id.clone(),
            principal_identity_kind: contract.authorization.principal_identity_kind,
            principal_binding_generation: contract.authorization.principal_binding_generation,
            scope_digest: contract.authorization.scope_digest.clone(),
            server_identity_digest: contract.deployment_closure.server_identity_digest.clone(),
            transport_kind: contract.transport_kind(),
            transport_binding_digest: digest(&contract.deployment_closure.transport)?,
            canonical_digest: placeholder_digest()?,
        };
        key.validate_for(contract)?;
        key.canonical_digest = digest_without_field(&key, "canonical_digest")?;
        Ok(key)
    }

    pub fn validate_for(&self, contract: &McpHostExecutionContract) -> Result<(), McpHostError> {
        self.validate_shape()?;
        if self.tenant_id != contract.authorization.tenant_id
            || self.deployment != contract.deployment
            || self.protocol_profile != contract.server.protocol_policy
            || self.authorization_binding_id != contract.authorization.authorization_binding_id
            || self.authorization_generation != contract.authorization.generation
            || self.principal_kind != contract.authorization.principal_kind
            || self.principal_id != contract.authorization.principal_id
            || self.principal_identity_kind != contract.authorization.principal_identity_kind
            || self.principal_binding_generation
                != contract.authorization.principal_binding_generation
            || self.scope_digest != contract.authorization.scope_digest
            || self.server_identity_digest != contract.deployment_closure.server_identity_digest
            || self.transport_kind != contract.transport_kind()
            || self.transport_binding_digest != digest(&contract.deployment_closure.transport)?
        {
            return Err(McpHostError::InvalidSession);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.deployment.resource_kind != ResourceKind::McpDeployment
            || self.deployment.validate().is_err()
            || self.protocol_profile.resource_kind != ResourceKind::PolicyRevision
            || self.protocol_profile.validate().is_err()
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.authorization_generation == 0
            || self.principal_id.kind() != ResourceKind::Principal
            || self.principal_binding_generation == 0
        {
            return Err(McpHostError::InvalidSession);
        }
        Ok(())
    }

    pub fn validate_canonical(&self) -> Result<(), McpHostError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidSession);
        }
        Ok(())
    }

    pub fn validate_canonical_for(
        &self,
        contract: &McpHostExecutionContract,
    ) -> Result<(), McpHostError> {
        self.validate_for(contract)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidSession);
        }
        Ok(())
    }
}

/// Reconstructable session observation stored inside a bounded shared Job/Invocation payload.
///
/// The record is not a durable execution authority: losing it can only cause a new connection. The
/// encrypted opaque value may contain a protocol session identifier but never plaintext tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSessionRecord {
    pub schema_version: u32,
    pub binding_key: McpSessionBindingKey,
    pub state: McpSessionState,
    pub generation: u64,
    pub version: u64,
    pub encrypted_opaque_session: Option<EncryptedMcpState>,
    pub expires_at: Option<DateTime<Utc>>,
    pub canonical_digest: Sha256Digest,
}

impl McpSessionRecord {
    pub fn disconnected(binding_key: McpSessionBindingKey) -> Result<Self, McpHostError> {
        let mut session = Self {
            schema_version: 1,
            binding_key,
            state: McpSessionState::Disconnected,
            generation: 0,
            version: 1,
            encrypted_opaque_session: None,
            expires_at: None,
            canonical_digest: placeholder_digest()?,
        };
        session.validate_at(Utc::now())?;
        session.canonical_digest = digest_without_field(&session, "canonical_digest")?;
        Ok(session)
    }

    pub fn transition(
        &self,
        expected_version: u64,
        target: McpSessionState,
        encrypted_opaque_session: Option<EncryptedMcpState>,
        expires_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<Self, McpHostError> {
        self.validate_canonical_at(now)?;
        if self.version != expected_version || !self.state.can_transition_to(target) {
            return Err(McpHostError::InvalidSession);
        }
        let mut next = Self {
            schema_version: 1,
            binding_key: self.binding_key.clone(),
            state: target,
            generation: self.generation,
            version: self
                .version
                .checked_add(1)
                .ok_or(McpHostError::InvalidSession)?,
            encrypted_opaque_session,
            expires_at,
            canonical_digest: placeholder_digest()?,
        };
        if target == McpSessionState::Connecting {
            next.generation = self
                .generation
                .checked_add(1)
                .ok_or(McpHostError::InvalidSession)?;
        }
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok(next)
    }

    /// Discards transport-affine state after a Host/session loss. This is a recovery reset, not a
    /// normal protocol transition: the next connection must enter `Connecting`, which advances
    /// the session generation before any remote state can be trusted again.
    pub fn rebuild_after_loss(
        &self,
        expected_version: u64,
        now: DateTime<Utc>,
    ) -> Result<Self, McpHostError> {
        self.validate_canonical_at(now)?;
        if self.version != expected_version
            || !matches!(
                self.state,
                McpSessionState::Connecting
                    | McpSessionState::Initializing
                    | McpSessionState::Ready
                    | McpSessionState::Degraded
            )
        {
            return Err(McpHostError::InvalidSession);
        }
        let mut next = Self {
            schema_version: 1,
            binding_key: self.binding_key.clone(),
            state: McpSessionState::Disconnected,
            generation: self.generation,
            version: self
                .version
                .checked_add(1)
                .ok_or(McpHostError::InvalidSession)?,
            encrypted_opaque_session: None,
            expires_at: None,
            canonical_digest: placeholder_digest()?,
        };
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok(next)
    }

    pub fn validate_at(&self, _now: DateTime<Utc>) -> Result<(), McpHostError> {
        if self.binding_key.validate_canonical().is_err()
            || self.schema_version != 1
            || self.version == 0
            || (self.generation == 0 && self.state != McpSessionState::Disconnected)
        {
            return Err(McpHostError::InvalidSession);
        }
        if let Some(opaque) = &self.encrypted_opaque_session {
            opaque
                .validate()
                .map_err(|_| McpHostError::InvalidSession)?;
        }
        let carries_session = matches!(
            self.state,
            McpSessionState::Ready
                | McpSessionState::Degraded
                | McpSessionState::ReauthRequired
                | McpSessionState::Draining
        );
        if carries_session != self.encrypted_opaque_session.is_some()
            || carries_session != self.expires_at.is_some()
        {
            return Err(McpHostError::InvalidSession);
        }
        Ok(())
    }

    pub fn validate_canonical_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.validate_at(now)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidSession);
        }
        Ok(())
    }
}
