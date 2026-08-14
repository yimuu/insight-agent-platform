use super::{
    digest_without_field, placeholder_digest, scope_digest, valid_scope, McpAuthorizationContext,
    McpHostError, NewMcpAuthorizationContext, MAX_MCP_AUTHORIZATION_SCOPES,
};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    CommandAudit, ExactDeploymentRef, ExactSecretBindingRef, McpAuthorizationPrincipalKind,
    McpAuthorizationState, PrincipalKind, ResourceId, ResourceKind, SecretResolutionPolicy,
    Sha256Digest,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMcpAuthorizationBinding {
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
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpAuthorizationReplacement {
    pub principal_binding_generation: u64,
    pub granted_scopes: Vec<String>,
    pub token_secret_binding: ExactSecretBindingRef,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CreateMcpAuthorizationBinding {
    pub audit: CommandAudit,
    pub input: NewMcpAuthorizationBinding,
}

impl CreateMcpAuthorizationBinding {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit
            .validate_at(now)
            .map_err(|_| McpHostError::InvalidAuthorization)?;
        if self.input.tenant_id != self.audit.tenant_id {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TransitionMcpAuthorizationBinding {
    pub audit: CommandAudit,
    pub authorization_binding_id: ResourceId,
    pub expected_version: u64,
    pub target: McpAuthorizationState,
}

impl TransitionMcpAuthorizationBinding {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit
            .validate_at(now)
            .map_err(|_| McpHostError::InvalidAuthorization)?;
        if self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.expected_version == 0
            || self.target == McpAuthorizationState::Active
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReactivateMcpAuthorizationBinding {
    pub audit: CommandAudit,
    pub authorization_binding_id: ResourceId,
    pub expected_version: u64,
    pub replacement: McpAuthorizationReplacement,
}

impl ReactivateMcpAuthorizationBinding {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit
            .validate_at(now)
            .map_err(|_| McpHostError::InvalidAuthorization)?;
        if self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.expected_version == 0
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }
}

/// Current authorization authority persisted in the shared `Resource` aggregate.
///
/// Tokens remain in Secret Manager. This payload contains only exact non-sensitive binding
/// metadata, optimistic version and generation fences.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpAuthorizationBindingRecord {
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
    pub state: McpAuthorizationState,
    pub generation: u64,
    pub version: u64,
    pub expires_at: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl McpAuthorizationBindingRecord {
    pub fn create(
        mut input: NewMcpAuthorizationBinding,
        now: DateTime<Utc>,
    ) -> Result<Self, McpHostError> {
        input.granted_scopes.sort();
        let mut record = Self {
            schema_version: 1,
            tenant_id: input.tenant_id,
            authorization_binding_id: input.authorization_binding_id,
            mcp_deployment: input.mcp_deployment,
            principal_kind: input.principal_kind,
            principal_id: input.principal_id,
            principal_identity_kind: input.principal_identity_kind,
            principal_binding_generation: input.principal_binding_generation,
            audience_identity_digest: input.audience_identity_digest,
            scope_digest: scope_digest(&input.granted_scopes)?,
            granted_scopes: input.granted_scopes,
            token_secret_binding: input.token_secret_binding,
            state: McpAuthorizationState::Active,
            generation: 1,
            version: 1,
            expires_at: input.expires_at,
            canonical_digest: placeholder_digest()?,
        };
        record.validate_at(now)?;
        record.canonical_digest = digest_without_field(&record, "canonical_digest")?;
        Ok(record)
    }

    pub fn transition(
        &self,
        expected_version: u64,
        target: McpAuthorizationState,
        now: DateTime<Utc>,
    ) -> Result<Self, McpHostError> {
        self.validate_canonical_shape()?;
        if self.version != expected_version
            || target == McpAuthorizationState::Active
            || !self.state.can_transition_to(target)
            || (target == McpAuthorizationState::Expired && self.expires_at > now)
            || (self.state == McpAuthorizationState::Active
                && self.expires_at <= now
                && target != McpAuthorizationState::Expired)
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        let mut next = self.clone();
        next.state = target;
        next.version = next
            .version
            .checked_add(1)
            .ok_or(McpHostError::InvalidAuthorization)?;
        next.canonical_digest = placeholder_digest()?;
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok(next)
    }

    pub fn reactivate(
        &self,
        expected_version: u64,
        mut replacement: McpAuthorizationReplacement,
        now: DateTime<Utc>,
    ) -> Result<Self, McpHostError> {
        self.validate_canonical_at(now)?;
        replacement.granted_scopes.sort();
        if self.version != expected_version
            || self.state != McpAuthorizationState::ReauthRequired
            || !self.state.can_transition_to(McpAuthorizationState::Active)
            || replacement.principal_binding_generation < self.principal_binding_generation
            || replacement.token_secret_binding.validate().is_err()
            || !matches!(
                &replacement.token_secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
            || replacement.token_secret_binding.secret_binding_id
                != self.token_secret_binding.secret_binding_id
            || replacement.token_secret_binding.provider_id != self.token_secret_binding.provider_id
            || replacement.token_secret_binding.purpose != self.token_secret_binding.purpose
            || replacement.token_secret_binding.binding_generation
                <= self.token_secret_binding.binding_generation
            || replacement.expires_at <= now
            || replacement.granted_scopes.len() > MAX_MCP_AUTHORIZATION_SCOPES
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        let mut next = self.clone();
        next.principal_binding_generation = replacement.principal_binding_generation;
        next.scope_digest = scope_digest(&replacement.granted_scopes)?;
        next.granted_scopes = replacement.granted_scopes;
        next.token_secret_binding = replacement.token_secret_binding;
        next.expires_at = replacement.expires_at;
        next.state = McpAuthorizationState::Active;
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(McpHostError::InvalidAuthorization)?;
        next.version = next
            .version
            .checked_add(1)
            .ok_or(McpHostError::InvalidAuthorization)?;
        next.canonical_digest = placeholder_digest()?;
        next.validate_at(now)?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok(next)
    }

    pub fn execution_context(
        &self,
        now: DateTime<Utc>,
    ) -> Result<McpAuthorizationContext, McpHostError> {
        self.validate_canonical_at(now)?;
        if self.state != McpAuthorizationState::Active {
            return Err(McpHostError::InvalidAuthorization);
        }
        McpAuthorizationContext::build(NewMcpAuthorizationContext {
            tenant_id: self.tenant_id.clone(),
            authorization_binding_id: self.authorization_binding_id.clone(),
            mcp_deployment: self.mcp_deployment.clone(),
            principal_kind: self.principal_kind,
            principal_id: self.principal_id.clone(),
            principal_identity_kind: self.principal_identity_kind,
            principal_binding_generation: self.principal_binding_generation,
            audience_identity_digest: self.audience_identity_digest.clone(),
            granted_scopes: self.granted_scopes.clone(),
            token_secret_binding: self.token_secret_binding.clone(),
            generation: self.generation,
            expires_at: self.expires_at,
        })
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.validate_shape()?;
        if (self.state == McpAuthorizationState::Active && self.expires_at <= now)
            || (self.state == McpAuthorizationState::Expired && self.expires_at > now)
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.principal_id.kind() != ResourceKind::Principal
            || self.principal_binding_generation == 0
            || self.granted_scopes.len() > MAX_MCP_AUTHORIZATION_SCOPES
            || !self.granted_scopes.windows(2).all(|pair| pair[0] < pair[1])
            || self.granted_scopes.iter().any(|scope| !valid_scope(scope))
            || scope_digest(&self.granted_scopes)? != self.scope_digest
            || self.token_secret_binding.validate().is_err()
            || !matches!(
                &self.token_secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
            || self.generation == 0
            || self.version == 0
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
        self.validate_canonical_shape()
    }

    pub fn validate_canonical(&self) -> Result<(), McpHostError> {
        self.validate_canonical_shape()
    }

    fn validate_canonical_shape(&self) -> Result<(), McpHostError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }
}
