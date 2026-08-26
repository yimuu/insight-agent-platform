use super::{digest_without_field, placeholder_digest, valid_code, valid_scope, McpHostError};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    CommandAudit, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef, McpOAuthTaskBinding,
    PrincipalKind, PrincipalSnapshot, ResourceId, ResourceKind, SecretPurpose,
    SecretResolutionPolicy, Sha256Digest, TraceIdentityV1,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MCP_OAUTH_PKCE_SECRET_PURPOSE: &str = "mcp.oauth.pkce";
pub const MAX_MCP_OAUTH_EXPIRY_BATCH: u16 = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthReauthorizationFence {
    pub authorization_generation: u64,
    pub authorization_version: u64,
}

impl McpOAuthReauthorizationFence {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.authorization_generation == 0 || self.authorization_version == 0 {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }
}

/// Principal command that starts one durable Authorization Code + PKCE interaction.
///
/// Raw state, nonce and verifier values are deliberately absent. The verifier is held behind an
/// exact, pinned SecretBinding and only digests are persisted in the shared Task payload.
#[derive(Debug, Clone)]
pub struct BeginMcpOAuthAuthorization {
    pub audit: CommandAudit,
    pub task_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub expected_principal_binding_generation: u64,
    pub requested_scopes: Vec<String>,
    pub state_digest: Sha256Digest,
    pub nonce_digest: Sha256Digest,
    pub callback_binding_digest: Sha256Digest,
    pub pkce_secret_binding: ExactSecretBindingRef,
    pub reauthorization: Option<McpOAuthReauthorizationFence>,
    pub safe_prompt_key: String,
    pub deadline: DateTime<Utc>,
}

impl BeginMcpOAuthAuthorization {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit
            .validate_at(now)
            .map_err(|_| McpHostError::InvalidAuthorization)?;
        if self.task_id.kind() != ResourceKind::Interaction
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.audit.principal_kind == PrincipalKind::ServiceIdentity
            || self.expected_principal_binding_generation == 0
            || self.requested_scopes.is_empty()
            || self.requested_scopes.len() > super::MAX_MCP_AUTHORIZATION_SCOPES
            || !self
                .requested_scopes
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self
                .requested_scopes
                .iter()
                .any(|scope| !valid_scope(scope))
            || self.pkce_secret_binding.validate().is_err()
            || self.pkce_secret_binding.purpose.as_str() != MCP_OAUTH_PKCE_SECRET_PURPOSE
            || !matches!(
                &self.pkce_secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
            || self
                .reauthorization
                .as_ref()
                .is_some_and(|fence| fence.validate().is_err())
            || !valid_code(&self.safe_prompt_key)
            || self.deadline <= now
            || self.audit.receipt_expires_at < self.deadline
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }

    pub fn task_binding(
        &self,
        principal: &PrincipalSnapshot,
        audience_identity_digest: Sha256Digest,
        token_credential_purpose: SecretPurpose,
        auth_policy: ExactVersionRef,
    ) -> Result<McpOAuthTaskBinding, McpHostError> {
        self.validate_at(Utc::now())?;
        principal
            .validate()
            .map_err(|_| McpHostError::InvalidAuthorization)?;
        if principal.tenant_id != self.audit.tenant_id
            || principal.principal_id != self.audit.principal_id
            || principal.principal_kind != self.audit.principal_kind
            || principal.binding_generation != self.expected_principal_binding_generation
            || token_credential_purpose.as_str() == MCP_OAUTH_PKCE_SECRET_PURPOSE
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        let (expected_authorization_generation, expected_authorization_version) =
            self.reauthorization.map_or((None, None), |fence| {
                (
                    Some(fence.authorization_generation),
                    Some(fence.authorization_version),
                )
            });
        let binding = McpOAuthTaskBinding {
            authorization_binding_id: self.authorization_binding_id.clone(),
            mcp_deployment: self.mcp_deployment.clone(),
            auth_policy,
            principal_binding_generation: self.expected_principal_binding_generation,
            audience_identity_digest,
            requested_scopes: self.requested_scopes.clone(),
            token_credential_purpose,
            state_digest: self.state_digest.clone(),
            nonce_digest: self.nonce_digest.clone(),
            callback_binding_digest: self.callback_binding_digest.clone(),
            pkce_secret_binding: Box::new(self.pkce_secret_binding.clone()),
            expected_authorization_generation,
            expected_authorization_version,
        };
        binding
            .validate()
            .map_err(|_| McpHostError::InvalidAuthorization)?;
        Ok(binding)
    }
}

/// Non-secret result of an OAuth broker exchange.
///
/// The access/refresh token values are already written to Secret Manager. Only the exact binding
/// metadata and verification evidence cross into the Host/repository boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthAuthorizedGrant {
    pub schema_version: u32,
    pub granted_scopes: Vec<String>,
    pub token_secret_binding: ExactSecretBindingRef,
    pub audience_identity_digest: Sha256Digest,
    pub issuer_identity_digest: Sha256Digest,
    pub subject_identity_digest: Sha256Digest,
    pub exchange_evidence_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl McpOAuthAuthorizedGrant {
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        mut granted_scopes: Vec<String>,
        token_secret_binding: ExactSecretBindingRef,
        audience_identity_digest: Sha256Digest,
        issuer_identity_digest: Sha256Digest,
        subject_identity_digest: Sha256Digest,
        exchange_evidence_digest: Sha256Digest,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, McpHostError> {
        granted_scopes.sort();
        let mut grant = Self {
            schema_version: 1,
            granted_scopes,
            token_secret_binding,
            audience_identity_digest,
            issuer_identity_digest,
            subject_identity_digest,
            exchange_evidence_digest,
            expires_at,
            canonical_digest: placeholder_digest()?,
        };
        grant.validate_shape_at(Utc::now())?;
        grant.canonical_digest = digest_without_field(&grant, "canonical_digest")?;
        Ok(grant)
    }

    pub fn validate_for_binding(
        &self,
        binding: &McpOAuthTaskBinding,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        self.validate_shape_at(now)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidAuthorization);
        }
        binding
            .validate()
            .map_err(|_| McpHostError::InvalidAuthorization)?;
        if self.audience_identity_digest != binding.audience_identity_digest
            || self.token_secret_binding.purpose != binding.token_credential_purpose
            || !self
                .granted_scopes
                .iter()
                .all(|scope| binding.requested_scopes.binary_search(scope).is_ok())
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }

    fn validate_shape_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.granted_scopes.is_empty()
            || self.granted_scopes.len() > super::MAX_MCP_AUTHORIZATION_SCOPES
            || !self.granted_scopes.windows(2).all(|pair| pair[0] < pair[1])
            || self.granted_scopes.iter().any(|scope| !valid_scope(scope))
            || self.token_secret_binding.validate().is_err()
            || !matches!(
                &self.token_secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
            || self.token_secret_binding.purpose.as_str() == MCP_OAUTH_PKCE_SECRET_PURPOSE
            || self.expires_at <= now
        {
            return Err(McpHostError::InvalidAuthorization);
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
pub enum McpOAuthCallbackResolution {
    Authorized(Box<McpOAuthAuthorizedGrant>),
    Declined {
        safe_reason_code: String,
        evidence_digest: Sha256Digest,
    },
}

impl McpOAuthCallbackResolution {
    pub fn validate_for_binding(
        &self,
        binding: &McpOAuthTaskBinding,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        match self {
            Self::Authorized(grant) => grant.validate_for_binding(binding, now),
            Self::Declined {
                safe_reason_code, ..
            } if valid_code(safe_reason_code) => Ok(()),
            Self::Declined { .. } => Err(McpHostError::InvalidAuthorization),
        }
    }
}

/// Identity supplied only after the callback ingress authenticated and decoded the fixed redirect.
/// It contains no raw state, authorization code, token, cookie or caller-controlled URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthCallbackAudit {
    pub trace: TraceIdentityV1,
    pub tenant_id: ResourceId,
    pub callback_ingress_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub callback_binding_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl McpOAuthCallbackAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        if self.trace.validate().is_err()
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.callback_ingress_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CompleteMcpOAuthCallback {
    pub audit: McpOAuthCallbackAudit,
    pub task_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub expected_task_generation: u64,
    pub expected_task_version: u64,
    pub state_digest: Sha256Digest,
    pub resolution: McpOAuthCallbackResolution,
}

impl CompleteMcpOAuthCallback {
    pub fn validate_for_binding(
        &self,
        tenant_id: &ResourceId,
        task_id: &ResourceId,
        binding: &McpOAuthTaskBinding,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        binding
            .validate()
            .map_err(|_| McpHostError::InvalidAuthorization)?;
        if self.task_id.kind() != ResourceKind::Interaction
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.audit.tenant_id != *tenant_id
            || self.task_id != *task_id
            || self.authorization_binding_id != binding.authorization_binding_id
            || self.expected_task_generation == 0
            || self.expected_task_version == 0
            || self.state_digest != binding.state_digest
            || self.audit.callback_binding_digest != binding.callback_binding_digest
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        self.resolution.validate_for_binding(binding, now)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthExpirySlot {
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}

impl McpOAuthExpirySlot {
    fn validate(&self) -> Result<(), McpHostError> {
        if self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        Ok(())
    }
}

/// Bounded safety scan for expired OAuth Tasks. PostgreSQL time and row locks decide winners.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriveExpiredMcpOAuthTasks {
    pub tenant_id: ResourceId,
    pub scheduler_generation_id: ResourceId,
    pub limit: u16,
    pub slots: Vec<McpOAuthExpirySlot>,
}

impl DriveExpiredMcpOAuthTasks {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.scheduler_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.limit == 0
            || self.limit > MAX_MCP_OAUTH_EXPIRY_BATCH
            || self.slots.len() != usize::from(self.limit)
        {
            return Err(McpHostError::InvalidAuthorization);
        }
        let mut identities = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            if !identities.insert(slot.event_id.to_string())
                || !identities.insert(slot.outbox_id.to_string())
            {
                return Err(McpHostError::InvalidAuthorization);
            }
        }
        Ok(())
    }
}
