//! Pure tenant membership and SecretBinding commands and transaction ports.
//!
//! The crate contains no storage, transport, provider, or wall-clock implementation. Application
//! services inject time and own the outer transaction; adapters implement the persistence ports.

#![allow(async_fn_in_trait)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, CommandAudit, CommandOutcome, ExactDeploymentRef, ExactSecretBindingRef,
    PermissionSet, PrincipalKind, ResourceId, ResourceKind, SecretBindingPayload,
    SecretBindingState, SecretPurpose, SecretResolutionPolicy, Sha256Digest,
};
use std::{error::Error, fmt};

#[derive(Clone, PartialEq, Eq)]
pub struct EncryptedOpaqueReference(Vec<u8>);

impl EncryptedOpaqueReference {
    pub fn new(ciphertext: Vec<u8>) -> Result<Self, SecurityCommandError> {
        if ciphertext.is_empty() || ciphertext.len() > 16_384 {
            return Err(SecurityCommandError::InvalidOpaqueReference);
        }
        Ok(Self(ciphertext))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for EncryptedOpaqueReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncryptedOpaqueReference")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// Trusted resolution projection used only by the Secret Broker boundary.
///
/// Management/query projections deliberately exclude these fields. The ciphertext is an
/// envelope-encrypted external provider reference, never the Secret value itself.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretBindingResolutionRecord {
    pub tenant_id: ResourceId,
    pub secret_binding_id: ResourceId,
    pub purpose: SecretPurpose,
    pub provider_id: ResourceId,
    pub state: SecretBindingState,
    pub generation: u64,
    pub encrypted_reference: EncryptedOpaqueReference,
    pub key_id: String,
    pub reference_digest: Sha256Digest,
    pub payload: SecretBindingPayload,
}

impl SecretBindingResolutionRecord {
    pub fn validate(&self) -> Result<(), SecretBindingResolutionError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.secret_binding_id.kind() != ResourceKind::SecretBinding
            || self.provider_id.kind() != ResourceKind::SecretProvider
            || self.generation == 0
            || self.key_id.is_empty()
            || self.key_id.len() > 255
            || self.payload.provider_id != self.provider_id
            || self.payload.validate().is_err()
        {
            return Err(SecretBindingResolutionError::InvalidEvidence);
        }
        Ok(())
    }
}

impl fmt::Debug for SecretBindingResolutionRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretBindingResolutionRecord")
            .field("tenant_id", &self.tenant_id)
            .field("secret_binding_id", &self.secret_binding_id)
            .field("purpose", &self.purpose)
            .field("provider_id", &self.provider_id)
            .field("state", &self.state)
            .field("generation", &self.generation)
            .field("encrypted_reference", &self.encrypted_reference)
            .field("key_id", &"[redacted]")
            .field("reference_digest", &self.reference_digest)
            .field("payload", &self.payload)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretBindingResolutionError {
    Unavailable,
    NotFound,
    InvalidEvidence,
}

/// Read-only authority port available only to the trusted Secret Broker composition.
#[async_trait]
pub trait SecretBindingResolutionAuthority: Send + Sync {
    async fn load_for_resolution(
        &self,
        tenant_id: &ResourceId,
        secret_binding_id: &ResourceId,
    ) -> Result<SecretBindingResolutionRecord, SecretBindingResolutionError>;
}

/// Trusted command used after an external Secret Manager `prepare-or-load` winner exists.
///
/// The external entry and PostgreSQL cannot share a transaction. The preparation digest is the
/// stable idempotency owner: retries re-register the same semantic external version through the
/// normal Receipt/Event/Outbox path. Ciphertext and KMS key rotation are deliberately excluded
/// from the semantic request digest; the first committed encrypted representation remains owner.
#[derive(Debug, Clone)]
pub struct RegisterPreparedSecretBinding {
    pub audit: CommandAudit,
    pub preparation_digest: Sha256Digest,
    pub secret_binding_id: ResourceId,
    pub purpose: SecretPurpose,
    pub provider_id: ResourceId,
    pub encrypted_reference: EncryptedOpaqueReference,
    pub key_id: String,
    pub reference_digest: Sha256Digest,
    pub opaque_version_identity_digest: Sha256Digest,
    pub provider_storage_evidence_digest: Sha256Digest,
}

impl RegisterPreparedSecretBinding {
    pub fn semantic_request_digest(&self) -> Result<Sha256Digest, SecurityCommandError> {
        canonical_digest(&serde_json::json!({
            "domain": "prepared_secret_binding_registration_v1",
            "opaque_version_identity_digest": self.opaque_version_identity_digest,
            "preparation_digest": self.preparation_digest,
            "provider_id": self.provider_id,
            "provider_storage_evidence_digest": self.provider_storage_evidence_digest,
            "purpose": self.purpose,
            "reference_digest": self.reference_digest,
            "schema_version": 1,
            "secret_binding_id": self.secret_binding_id,
            "tenant_id": self.audit.tenant_id,
        }))
        .map_err(|failure| SecurityCommandError::Contract(failure.to_string()))?
        .parse()
        .map_err(|_| SecurityCommandError::InvalidSecretBinding)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
        validate_audit(&self.audit, now)?;
        if self.audit.principal_kind != PrincipalKind::ServiceIdentity
            || self.secret_binding_id.kind() != ResourceKind::SecretBinding
            || self.provider_id.kind() != ResourceKind::SecretProvider
            || self.key_id.is_empty()
            || self.key_id.len() > 255
            || self.audit.idempotency_key_digest != self.preparation_digest
            || self.semantic_request_digest()? != self.audit.request_digest
        {
            return Err(SecurityCommandError::InvalidSecretBinding);
        }
        SecretBindingPayload {
            provider_id: self.provider_id.clone(),
            resolution_policy: SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: self.opaque_version_identity_digest.clone(),
            },
        }
        .validate()
        .map_err(|failure| SecurityCommandError::Contract(failure.to_string()))
    }

    pub fn exact_binding(&self) -> Result<ExactSecretBindingRef, SecurityCommandError> {
        ExactSecretBindingRef::build(
            self.secret_binding_id.clone(),
            1,
            self.provider_id.clone(),
            self.purpose.clone(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: self.opaque_version_identity_digest.clone(),
            },
        )
        .map_err(|failure| SecurityCommandError::Contract(failure.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedSecretBindingRegistrationDisposition {
    Applied,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSecretBindingRegistrationOutcome {
    pub disposition: PreparedSecretBindingRegistrationDisposition,
    pub exact_binding: ExactSecretBindingRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedSecretBindingRegistrationError {
    Rejected,
    TemporarilyUnavailable,
}

/// PostgreSQL-backed authority available only to the trusted Secret Broker composition.
#[async_trait]
pub trait PreparedSecretBindingAuthority: Send + Sync {
    async fn register_prepared(
        &self,
        command: RegisterPreparedSecretBinding,
    ) -> Result<PreparedSecretBindingRegistrationOutcome, PreparedSecretBindingRegistrationError>;
}

#[derive(Debug, Clone)]
pub struct BindTenantPrincipal {
    pub audit: CommandAudit,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub permissions: PermissionSet,
}

impl BindTenantPrincipal {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
        validate_audit(&self.audit, now)?;
        validate_tenant_principal(&self.principal_id, self.principal_kind)
    }
}

#[derive(Debug, Clone)]
pub struct UpdateTenantPrincipalPermissions {
    pub audit: CommandAudit,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub expected_generation: i64,
    pub expected_version: i64,
    pub permissions: PermissionSet,
}

impl UpdateTenantPrincipalPermissions {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
        validate_audit(&self.audit, now)?;
        validate_tenant_principal(&self.principal_id, self.principal_kind)?;
        validate_fence(self.expected_generation, self.expected_version)
    }
}

#[derive(Debug, Clone)]
pub struct RevokeTenantPrincipal {
    pub audit: CommandAudit,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub expected_generation: i64,
    pub expected_version: i64,
}

#[derive(Debug, Clone)]
pub struct BindTenantSchedulingPolicy {
    pub audit: CommandAudit,
    pub expected_tenant_version: i64,
    pub policy: ExactDeploymentRef,
}

#[derive(Debug, Clone)]
pub struct BindTenantArtifactPolicies {
    pub audit: CommandAudit,
    pub expected_tenant_version: i64,
    pub retention_policy: ExactDeploymentRef,
    pub artifact_io_policy: ExactDeploymentRef,
}

impl BindTenantArtifactPolicies {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
        validate_audit(&self.audit, now)?;
        for policy in [&self.retention_policy, &self.artifact_io_policy] {
            policy
                .validate()
                .map_err(|failure| SecurityCommandError::Contract(failure.to_string()))?;
            if policy.resource_kind != ResourceKind::PolicyDeployment {
                return Err(SecurityCommandError::InvalidTenantPolicy);
            }
        }
        if self.expected_tenant_version <= 0
            || self.retention_policy.deployment_id == self.artifact_io_policy.deployment_id
        {
            return Err(SecurityCommandError::InvalidTenantPolicy);
        }
        Ok(())
    }
}

impl BindTenantSchedulingPolicy {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
        validate_audit(&self.audit, now)?;
        self.policy
            .validate()
            .map_err(|failure| SecurityCommandError::Contract(failure.to_string()))?;
        if self.expected_tenant_version <= 0
            || self.policy.resource_kind != ResourceKind::PolicyDeployment
        {
            return Err(SecurityCommandError::InvalidTenantPolicy);
        }
        Ok(())
    }
}

impl RevokeTenantPrincipal {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
        validate_audit(&self.audit, now)?;
        validate_tenant_principal(&self.principal_id, self.principal_kind)?;
        validate_fence(self.expected_generation, self.expected_version)
    }
}

#[derive(Debug, Clone)]
pub struct CreateSecretBinding {
    pub audit: CommandAudit,
    pub secret_binding_id: ResourceId,
    pub purpose: SecretPurpose,
    pub encrypted_reference: EncryptedOpaqueReference,
    pub key_id: String,
    pub reference_digest: Sha256Digest,
    pub payload: SecretBindingPayload,
}

impl CreateSecretBinding {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
        validate_audit(&self.audit, now)?;
        if self.secret_binding_id.kind() != ResourceKind::SecretBinding
            || self.key_id.is_empty()
            || self.key_id.len() > 255
        {
            return Err(SecurityCommandError::InvalidSecretBinding);
        }
        self.payload
            .validate()
            .map_err(|failure| SecurityCommandError::Contract(failure.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct RotateSecretBinding {
    pub audit: CommandAudit,
    pub secret_binding_id: ResourceId,
    pub expected_generation: i64,
    pub expected_version: i64,
    pub encrypted_reference: EncryptedOpaqueReference,
    pub key_id: String,
    pub reference_digest: Sha256Digest,
    pub payload: SecretBindingPayload,
    pub provider_evidence_digest: Sha256Digest,
}

impl RotateSecretBinding {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
        validate_audit(&self.audit, now)?;
        if self.secret_binding_id.kind() != ResourceKind::SecretBinding
            || self.key_id.is_empty()
            || self.key_id.len() > 255
        {
            return Err(SecurityCommandError::InvalidSecretBinding);
        }
        validate_fence(self.expected_generation, self.expected_version)?;
        self.payload
            .validate()
            .map_err(|failure| SecurityCommandError::Contract(failure.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct RevokeSecretBinding {
    pub audit: CommandAudit,
    pub secret_binding_id: ResourceId,
    pub expected_generation: i64,
    pub expected_version: i64,
}

impl RevokeSecretBinding {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
        validate_audit(&self.audit, now)?;
        if self.secret_binding_id.kind() != ResourceKind::SecretBinding {
            return Err(SecurityCommandError::InvalidSecretBinding);
        }
        validate_fence(self.expected_generation, self.expected_version)
    }
}

/// One caller-owned security transaction. Mutation methods must not commit the outer transaction.
pub trait SecurityTransaction {
    type Error;
    type TenantRecord;
    type TenantPrincipalRecord;
    type SecretBindingRecord;

    async fn bind_tenant_principal(
        &mut self,
        command: BindTenantPrincipal,
    ) -> Result<CommandOutcome<Self::TenantPrincipalRecord>, Self::Error>;
    async fn update_tenant_principal_permissions(
        &mut self,
        command: UpdateTenantPrincipalPermissions,
    ) -> Result<CommandOutcome<Self::TenantPrincipalRecord>, Self::Error>;
    async fn revoke_tenant_principal(
        &mut self,
        command: RevokeTenantPrincipal,
    ) -> Result<CommandOutcome<Self::TenantPrincipalRecord>, Self::Error>;
    async fn bind_tenant_scheduling_policy(
        &mut self,
        command: BindTenantSchedulingPolicy,
    ) -> Result<CommandOutcome<Self::TenantRecord>, Self::Error>;
    async fn bind_tenant_artifact_policies(
        &mut self,
        command: BindTenantArtifactPolicies,
    ) -> Result<CommandOutcome<Self::TenantRecord>, Self::Error>;
    async fn create_secret_binding(
        &mut self,
        command: CreateSecretBinding,
    ) -> Result<CommandOutcome<Self::SecretBindingRecord>, Self::Error>;
    async fn rotate_secret_binding(
        &mut self,
        command: RotateSecretBinding,
    ) -> Result<CommandOutcome<Self::SecretBindingRecord>, Self::Error>;
    async fn revoke_secret_binding(
        &mut self,
        command: RevokeSecretBinding,
    ) -> Result<CommandOutcome<Self::SecretBindingRecord>, Self::Error>;
    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}

pub trait SecurityStore {
    type Error;
    type Transaction<'a>: SecurityTransaction<Error = Self::Error>
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityCommandError {
    InvalidAudit,
    InvalidTenantPrincipal,
    InvalidTenantPolicy,
    InvalidFence,
    InvalidOpaqueReference,
    InvalidSecretBinding,
    Contract(String),
}

impl fmt::Display for SecurityCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAudit => {
                formatter.write_str("command audit identity or expiry is invalid")
            }
            Self::InvalidTenantPrincipal => {
                formatter.write_str("tenant principal identity or kind is invalid")
            }
            Self::InvalidTenantPolicy => {
                formatter.write_str("tenant scheduling policy binding is invalid")
            }
            Self::InvalidFence => {
                formatter.write_str("expected generation and version must be positive")
            }
            Self::InvalidOpaqueReference => {
                formatter.write_str("encrypted opaque reference is empty or unbounded")
            }
            Self::InvalidSecretBinding => {
                formatter.write_str("secret binding identity or metadata is invalid")
            }
            Self::Contract(message) => write!(formatter, "security contract failed: {message}"),
        }
    }
}

impl Error for SecurityCommandError {}

fn validate_audit(audit: &CommandAudit, now: DateTime<Utc>) -> Result<(), SecurityCommandError> {
    audit
        .validate_at(now)
        .map_err(|_| SecurityCommandError::InvalidAudit)
}

fn validate_tenant_principal(
    principal_id: &ResourceId,
    principal_kind: PrincipalKind,
) -> Result<(), SecurityCommandError> {
    if principal_id.kind() != ResourceKind::Principal
        || principal_kind == PrincipalKind::InstallationOperator
    {
        return Err(SecurityCommandError::InvalidTenantPrincipal);
    }
    Ok(())
}

fn validate_fence(generation: i64, version: i64) -> Result<(), SecurityCommandError> {
    if generation <= 0 || version <= 0 {
        return Err(SecurityCommandError::InvalidFence);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn digest(marker: char) -> Sha256Digest {
        format!("sha256:{}", marker.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn exact_policy(suffix: &str, marker: char) -> ExactDeploymentRef {
        ExactDeploymentRef::new(
            format!("pdep_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")
                .parse()
                .unwrap(),
            digest(marker),
        )
        .unwrap()
    }

    fn audit() -> CommandAudit {
        CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: "ten_0198f1c3-8f49-7c3e-b1f3-773c28367b80".parse().unwrap(),
            principal_id: "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b81".parse().unwrap(),
            principal_kind: PrincipalKind::TenantAdmin,
            receipt_id: "rcp_0198f1c3-8f49-7c3e-b1f3-773c28367b82".parse().unwrap(),
            event_id: "evt_0198f1c3-8f49-7c3e-b1f3-773c28367b83".parse().unwrap(),
            outbox_id: "obx_0198f1c3-8f49-7c3e-b1f3-773c28367b84".parse().unwrap(),
            idempotency_key_digest: digest('d'),
            request_digest: digest('e'),
            receipt_expires_at: Utc::now() + Duration::minutes(5),
        }
    }

    #[test]
    fn opaque_reference_debug_is_redacted() {
        let reference = EncryptedOpaqueReference::new(b"secret-canary".to_vec()).unwrap();
        let rendered = format!("{reference:?}");
        assert!(!rendered.contains("secret-canary"));
        assert!(rendered.contains("byte_length"));
    }

    #[test]
    fn artifact_policy_binding_requires_two_distinct_exact_policy_deployments() {
        let retention = exact_policy("7b85", 'a');
        let artifact_io = exact_policy("7b86", 'b');
        BindTenantArtifactPolicies {
            audit: audit(),
            expected_tenant_version: 1,
            retention_policy: retention.clone(),
            artifact_io_policy: artifact_io,
        }
        .validate_at(Utc::now())
        .unwrap();

        assert_eq!(
            BindTenantArtifactPolicies {
                audit: audit(),
                expected_tenant_version: 1,
                retention_policy: retention.clone(),
                artifact_io_policy: retention,
            }
            .validate_at(Utc::now()),
            Err(SecurityCommandError::InvalidTenantPolicy)
        );
    }
}
