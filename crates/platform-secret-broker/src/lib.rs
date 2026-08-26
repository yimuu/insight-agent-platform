//! Trusted SecretBinding resolver composition for the Egress Broker role.
//!
//! This crate owns no durable state and exposes no public management API. It combines the current
//! PostgreSQL binding authority, an envelope-reference unsealer and one process-installed external
//! Secret Provider. Secret values remain non-clone, redacted and zeroed on drop.

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    CommandAudit, ExactSecretBindingRef, PrincipalKind, ResourceId, ResourceKind,
    SecretBindingState, SecretPurpose, SecretResolutionPolicy, Sha256Digest,
};
use insight_platform_egress::{
    ExactSecretVersionDeleteDisposition, ExactSecretVersionDeleteError, ExactSecretVersionDeleter,
    McpOAuthTokenPreparation, McpOAuthTokenSet, McpOAuthTokenStore, McpOAuthTokenStoreError,
    McpOAuthTransientSecretStore, McpOAuthTransientSecretStoreError,
    NewMcpOAuthTransientSecretBundle, ResolvedSecretMaterial, SecretMaterialResolutionError,
    SecretMaterialResolver, StoredMcpOAuthTokenSecret, StoredMcpOAuthTransientSecretBundle,
    VerifiedMcpOAuthToken, MAX_SECRET_MATERIAL_BYTES_HARD,
};
use insight_platform_security::{
    EncryptedOpaqueReference, PreparedSecretBindingAuthority,
    PreparedSecretBindingRegistrationError, RegisterPreparedSecretBinding,
    SecretBindingResolutionAuthority, SecretBindingResolutionError, SecretBindingResolutionRecord,
};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, fmt, str::FromStr, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::timeout};
use uuid::Uuid;

mod aws;

pub use aws::{
    AwsSecretProviderCatalog, AwsSecretProviderCatalogConfig, AwsSecretProviderConfig,
    AwsSecretProviderConfigError, AwsSecretProviderReadinessError,
};

pub const MAX_INSTALLED_SECRET_PROVIDERS: usize = 64;
pub const MAX_SECRET_RESOLUTION_IN_FLIGHT_HARD: usize = 4_096;
pub const MAX_OPAQUE_SECRET_REFERENCE_BYTES: usize = 16_384;
pub const MAX_SECRET_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_PREPARED_SECRET_TTL: ChronoDuration = ChronoDuration::hours(24);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretBrokerCapacitySnapshot {
    pub maximum_in_flight: usize,
    pub available: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretBrokerLimits {
    pub maximum_in_flight: usize,
    pub maximum_material_bytes: usize,
    pub resolution_timeout: Duration,
}

impl SecretBrokerLimits {
    pub fn validate(self) -> Result<(), SecretBrokerConfigurationError> {
        if self.maximum_in_flight == 0
            || self.maximum_in_flight > MAX_SECRET_RESOLUTION_IN_FLIGHT_HARD
            || self.maximum_material_bytes == 0
            || self.maximum_material_bytes > MAX_SECRET_MATERIAL_BYTES_HARD
            || self.resolution_timeout.is_zero()
            || self.resolution_timeout > MAX_SECRET_RESOLUTION_TIMEOUT
        {
            return Err(SecretBrokerConfigurationError::InvalidLimits);
        }
        Ok(())
    }
}

impl Default for SecretBrokerLimits {
    fn default() -> Self {
        Self {
            maximum_in_flight: 128,
            maximum_material_bytes: 8_192,
            resolution_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretBrokerConfigurationError {
    InvalidLimits,
    InvalidProvider,
    DuplicateProvider,
    ProviderCatalogTooLarge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretExternalDependency {
    Kms,
    Secret,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretExternalDependencyOutcome {
    Success,
    Failure,
}

/// Process-installed observer for actual external SDK calls. Implementations receive no provider
/// identity, endpoint, tenant, binding, error text or secret material.
pub trait SecretExternalDependencyObserver: Send + Sync {
    fn observe(
        &self,
        dependency: SecretExternalDependency,
        outcome: SecretExternalDependencyOutcome,
    );
}

#[derive(Debug)]
struct NoopSecretExternalDependencyObserver;

impl SecretExternalDependencyObserver for NoopSecretExternalDependencyObserver {
    fn observe(
        &self,
        _dependency: SecretExternalDependency,
        _outcome: SecretExternalDependencyOutcome,
    ) {
    }
}

/// Non-clone decrypted reference to an object owned by the external Secret Provider.
pub struct OpaqueSecretReference(Vec<u8>);

impl OpaqueSecretReference {
    pub fn new(mut bytes: Vec<u8>) -> Result<Self, SecretReferenceUnsealError> {
        if bytes.is_empty() || bytes.len() > MAX_OPAQUE_SECRET_REFERENCE_BYTES {
            bytes.fill(0);
            return Err(SecretReferenceUnsealError::InvalidEvidence);
        }
        Ok(Self(bytes))
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for OpaqueSecretReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpaqueSecretReference")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for OpaqueSecretReference {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretReferenceUnsealError {
    Unavailable,
    Rejected,
    InvalidEvidence,
}

/// KMS/AEAD port. The implementation must bind ciphertext to tenant, Binding, generation and key.
#[async_trait]
pub trait SecretReferenceUnsealer: Send + Sync {
    async fn unseal(
        &self,
        binding: &SecretBindingResolutionRecord,
    ) -> Result<OpaqueSecretReference, SecretReferenceUnsealError>;
}

/// External provider output. Material is zeroed and cannot be cloned or formatted.
pub struct ProviderSecretMaterial {
    pub opaque_version_identity_digest: Sha256Digest,
    material: Vec<u8>,
}

impl ProviderSecretMaterial {
    pub fn new(
        opaque_version_identity_digest: Sha256Digest,
        mut material: Vec<u8>,
    ) -> Result<Self, SecretProviderResolveError> {
        if material.is_empty() || material.len() > MAX_SECRET_MATERIAL_BYTES_HARD {
            material.fill(0);
            return Err(SecretProviderResolveError::InvalidEvidence);
        }
        Ok(Self {
            opaque_version_identity_digest,
            material,
        })
    }

    fn into_material(mut self) -> Vec<u8> {
        std::mem::take(&mut self.material)
    }
}

impl fmt::Debug for ProviderSecretMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderSecretMaterial")
            .field(
                "opaque_version_identity_digest",
                &self.opaque_version_identity_digest,
            )
            .field("byte_length", &self.material.len())
            .finish_non_exhaustive()
    }
}

impl Drop for ProviderSecretMaterial {
    fn drop(&mut self) {
        self.material.fill(0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretProviderResolveError {
    Unavailable,
    NotFound,
    Rejected,
    InvalidEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretProviderDeleteDisposition {
    Deleted,
    AlreadyAbsent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretProviderDeleteError {
    Unavailable,
    Rejected,
    OutcomeUncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretProviderPrepareError {
    Unavailable,
    Rejected,
    WriteUncertain,
}

/// Non-secret provider evidence for one prepared external secret version. The opaque reference is
/// immediately envelope-encrypted before PostgreSQL registration and is zeroed on drop.
pub struct ProviderPreparedSecretVersion {
    pub secret_binding_id: ResourceId,
    pub provider_id: ResourceId,
    pub opaque_reference: OpaqueSecretReference,
    pub opaque_version_identity_digest: Sha256Digest,
    pub storage_evidence_digest: Sha256Digest,
}

impl fmt::Debug for ProviderPreparedSecretVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderPreparedSecretVersion")
            .field("secret_binding_id", &self.secret_binding_id)
            .field("provider_id", &self.provider_id)
            .field("opaque_reference", &self.opaque_reference)
            .field(
                "opaque_version_identity_digest",
                &self.opaque_version_identity_digest,
            )
            .field("storage_evidence_digest", &self.storage_evidence_digest)
            .finish()
    }
}

#[derive(Debug)]
pub struct ProviderStoredMcpOAuthTransientSecretBundle {
    pub stored: StoredMcpOAuthTransientSecretBundle,
    pub prepared_secret: ProviderPreparedSecretVersion,
}

#[derive(Debug)]
pub struct ProviderStoredMcpOAuthTokenSecret {
    pub stored: StoredMcpOAuthTokenSecret,
    pub prepared_secret: ProviderPreparedSecretVersion,
}

pub struct SealedSecretReference {
    pub encrypted_reference: EncryptedOpaqueReference,
    pub key_id: String,
    pub reference_digest: Sha256Digest,
}

impl fmt::Debug for SealedSecretReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SealedSecretReference")
            .field("encrypted_reference", &self.encrypted_reference)
            .field("key_id", &"[redacted]")
            .field("reference_digest", &self.reference_digest)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretReferenceSealError {
    Unavailable,
    Rejected,
    InvalidEvidence,
}

/// KMS/AEAD sealing port. Associated data must include every supplied identity and generation.
#[async_trait]
pub trait SecretReferenceSealer: Send + Sync {
    async fn seal(
        &self,
        tenant_id: &ResourceId,
        secret_binding_id: &ResourceId,
        provider_id: &ResourceId,
        binding_generation: u64,
        reference: &OpaqueSecretReference,
    ) -> Result<SealedSecretReference, SecretReferenceSealError>;
}

/// CandidateManifest-installed provider adapter. It receives only a trusted decrypted reference.
#[async_trait]
pub trait InstalledSecretProvider: Send + Sync {
    fn provider_id(&self) -> &ResourceId;

    async fn resolve(
        &self,
        tenant_id: &ResourceId,
        reference: &OpaqueSecretReference,
        policy: &SecretResolutionPolicy,
    ) -> Result<ProviderSecretMaterial, SecretProviderResolveError>;

    /// Deletes only the version identified by the frozen policy. Providers must never interpret
    /// this as "delete current" or "delete logical secret".
    async fn delete_exact(
        &self,
        tenant_id: &ResourceId,
        reference: &OpaqueSecretReference,
        policy: &SecretResolutionPolicy,
    ) -> Result<SecretProviderDeleteDisposition, SecretProviderDeleteError>;

    async fn prepare_or_load_mcp_oauth_transient(
        &self,
        _candidate: NewMcpOAuthTransientSecretBundle,
    ) -> Result<ProviderStoredMcpOAuthTransientSecretBundle, SecretProviderPrepareError> {
        Err(SecretProviderPrepareError::Rejected)
    }

    async fn load_prepared_mcp_oauth_token(
        &self,
        _preparation: &McpOAuthTokenPreparation,
    ) -> Result<Option<ProviderStoredMcpOAuthTokenSecret>, SecretProviderPrepareError> {
        Err(SecretProviderPrepareError::Rejected)
    }

    async fn prepare_or_load_mcp_oauth_token(
        &self,
        _preparation: &McpOAuthTokenPreparation,
        _tokens: &McpOAuthTokenSet,
        _verified: &VerifiedMcpOAuthToken,
    ) -> Result<ProviderStoredMcpOAuthTokenSecret, SecretProviderPrepareError> {
        Err(SecretProviderPrepareError::Rejected)
    }
}

#[derive(Clone)]
pub struct InstalledSecretProviderCatalog {
    providers: BTreeMap<ResourceId, Arc<dyn InstalledSecretProvider>>,
}

impl InstalledSecretProviderCatalog {
    pub fn new(
        providers: Vec<Arc<dyn InstalledSecretProvider>>,
    ) -> Result<Self, SecretBrokerConfigurationError> {
        if providers.is_empty() || providers.len() > MAX_INSTALLED_SECRET_PROVIDERS {
            return Err(SecretBrokerConfigurationError::ProviderCatalogTooLarge);
        }
        let mut installed = BTreeMap::new();
        for provider in providers {
            let provider_id = provider.provider_id().clone();
            if provider_id.kind() != ResourceKind::SecretProvider {
                return Err(SecretBrokerConfigurationError::InvalidProvider);
            }
            if installed.insert(provider_id, provider).is_some() {
                return Err(SecretBrokerConfigurationError::DuplicateProvider);
            }
        }
        Ok(Self {
            providers: installed,
        })
    }

    fn get(&self, provider_id: &ResourceId) -> Option<Arc<dyn InstalledSecretProvider>> {
        self.providers.get(provider_id).cloned()
    }
}

/// Production resolver composition installed in the Egress Broker role.
pub struct BrokeredSecretMaterialResolver {
    authority: Arc<dyn SecretBindingResolutionAuthority>,
    unsealer: Arc<dyn SecretReferenceUnsealer>,
    providers: InstalledSecretProviderCatalog,
    limits: SecretBrokerLimits,
    in_flight: Arc<Semaphore>,
}

/// OAuth write composition. External provider state is the preparation winner; PostgreSQL
/// registration is retried with the same digest until the existing SecretBinding is observable.
pub struct BrokeredMcpOAuthSecretStore {
    registration: Arc<dyn PreparedSecretBindingAuthority>,
    sealer: Arc<dyn SecretReferenceSealer>,
    providers: InstalledSecretProviderCatalog,
    service_principal_id: ResourceId,
    limits: SecretBrokerLimits,
    in_flight: Arc<Semaphore>,
}

struct ProviderSecretRegistration<'a> {
    now: DateTime<Utc>,
    tenant_id: &'a ResourceId,
    preparation_digest: &'a Sha256Digest,
    purpose: &'a SecretPurpose,
    expires_at: DateTime<Utc>,
    expected_binding: &'a ExactSecretBindingRef,
    prepared: &'a ProviderPreparedSecretVersion,
}

impl BrokeredMcpOAuthSecretStore {
    pub fn new(
        registration: Arc<dyn PreparedSecretBindingAuthority>,
        sealer: Arc<dyn SecretReferenceSealer>,
        providers: InstalledSecretProviderCatalog,
        service_principal_id: ResourceId,
        limits: SecretBrokerLimits,
    ) -> Result<Self, SecretBrokerConfigurationError> {
        limits.validate()?;
        if service_principal_id.kind() != ResourceKind::Principal {
            return Err(SecretBrokerConfigurationError::InvalidProvider);
        }
        Ok(Self {
            registration,
            sealer,
            providers,
            service_principal_id,
            limits,
            in_flight: Arc::new(Semaphore::new(limits.maximum_in_flight)),
        })
    }

    pub fn capacity_snapshot(&self) -> SecretBrokerCapacitySnapshot {
        SecretBrokerCapacitySnapshot {
            maximum_in_flight: self.limits.maximum_in_flight,
            available: self.in_flight.available_permits(),
        }
    }

    async fn register_provider_secret(
        &self,
        registration: ProviderSecretRegistration<'_>,
    ) -> Result<(), McpOAuthTransientSecretStoreError> {
        let ProviderSecretRegistration {
            now,
            tenant_id,
            preparation_digest,
            purpose,
            expires_at,
            expected_binding,
            prepared,
        } = registration;
        validate_provider_prepared_secret(tenant_id, purpose, expected_binding, prepared)?;
        let sealed = self
            .sealer
            .seal(
                tenant_id,
                &prepared.secret_binding_id,
                &prepared.provider_id,
                1,
                &prepared.opaque_reference,
            )
            .await
            .map_err(map_seal_error)?;
        if sealed.reference_digest != digest(prepared.opaque_reference.expose()) {
            return Err(McpOAuthTransientSecretStoreError::Rejected);
        }
        let placeholder = preparation_digest.clone();
        let mut command = RegisterPreparedSecretBinding {
            audit: CommandAudit {
                trace: insight_platform_contracts::TraceIdentityV1::generate(),
                tenant_id: tenant_id.clone(),
                principal_id: self.service_principal_id.clone(),
                principal_kind: PrincipalKind::ServiceIdentity,
                receipt_id: new_resource_id(ResourceKind::Receipt)?,
                event_id: new_resource_id(ResourceKind::Event)?,
                outbox_id: new_resource_id(ResourceKind::OutboxEvent)?,
                idempotency_key_digest: preparation_digest.clone(),
                request_digest: placeholder,
                receipt_expires_at: registration_receipt_expiry(now, expires_at)?,
            },
            preparation_digest: preparation_digest.clone(),
            secret_binding_id: prepared.secret_binding_id.clone(),
            purpose: purpose.clone(),
            provider_id: prepared.provider_id.clone(),
            encrypted_reference: sealed.encrypted_reference,
            key_id: sealed.key_id,
            reference_digest: sealed.reference_digest,
            opaque_version_identity_digest: prepared.opaque_version_identity_digest.clone(),
            provider_storage_evidence_digest: prepared.storage_evidence_digest.clone(),
        };
        command.audit.request_digest = command
            .semantic_request_digest()
            .map_err(|_| McpOAuthTransientSecretStoreError::Rejected)?;
        let outcome = self
            .registration
            .register_prepared(command)
            .await
            .map_err(map_registration_error)?;
        if outcome.exact_binding != *expected_binding {
            return Err(McpOAuthTransientSecretStoreError::Rejected);
        }
        Ok(())
    }

    async fn prepare_transient_inner(
        &self,
        candidate: NewMcpOAuthTransientSecretBundle,
        now: DateTime<Utc>,
    ) -> Result<StoredMcpOAuthTransientSecretBundle, McpOAuthTransientSecretStoreError> {
        let tenant_id = candidate.tenant_id.clone();
        let task_id = candidate.task_id.clone();
        let authorization_binding_id = candidate.authorization_binding_id.clone();
        let mcp_deployment = candidate.mcp_deployment.clone();
        let provider_id = candidate.pkce_secret_provider_id.clone();
        let preparation_digest = candidate.preparation_digest.clone();
        let callback_binding_digest = candidate.callback_binding_digest.clone();
        let expires_at = candidate.expires_at;
        let provider = self
            .providers
            .get(&provider_id)
            .ok_or(McpOAuthTransientSecretStoreError::Rejected)?;
        let prepared = provider
            .prepare_or_load_mcp_oauth_transient(candidate)
            .await
            .map_err(map_provider_prepare_error)?;
        prepared.stored.validate()?;
        if prepared.stored.tenant_id != tenant_id
            || prepared.stored.task_id != task_id
            || prepared.stored.authorization_binding_id != authorization_binding_id
            || prepared.stored.mcp_deployment != mcp_deployment
            || prepared.stored.pkce_secret_provider_id != provider_id
            || prepared.stored.pkce_secret_binding.provider_id != provider_id
            || prepared.stored.preparation_digest != preparation_digest
            || prepared.stored.callback_binding_digest != callback_binding_digest
            || prepared.stored.expires_at != expires_at
            || prepared.stored.storage_evidence_digest
                != prepared.prepared_secret.storage_evidence_digest
        {
            return Err(McpOAuthTransientSecretStoreError::Rejected);
        }
        self.register_provider_secret(ProviderSecretRegistration {
            now,
            tenant_id: &tenant_id,
            preparation_digest: &preparation_digest,
            purpose: &prepared.stored.pkce_secret_binding.purpose,
            expires_at,
            expected_binding: &prepared.stored.pkce_secret_binding,
            prepared: &prepared.prepared_secret,
        })
        .await?;
        Ok(prepared.stored)
    }

    async fn register_token_result(
        &self,
        preparation: &McpOAuthTokenPreparation,
        prepared: ProviderStoredMcpOAuthTokenSecret,
        now: DateTime<Utc>,
    ) -> Result<StoredMcpOAuthTokenSecret, McpOAuthTokenStoreError> {
        prepared.stored.validate_for_preparation(preparation, now)?;
        if prepared.stored.preparation_digest != preparation.preparation_digest
            || prepared.stored.token_secret_binding.provider_id
                != preparation.token_secret_provider_id
            || prepared.stored.storage_evidence_digest
                != prepared.prepared_secret.storage_evidence_digest
        {
            return Err(McpOAuthTokenStoreError::Rejected);
        }
        self.register_provider_secret(ProviderSecretRegistration {
            now,
            tenant_id: &preparation.tenant_id,
            preparation_digest: &preparation.preparation_digest,
            purpose: &preparation.token_credential_purpose,
            expires_at: preparation.expires_at,
            expected_binding: &prepared.stored.token_secret_binding,
            prepared: &prepared.prepared_secret,
        })
        .await
        .map_err(map_transient_store_error_to_token_store)?;
        Ok(prepared.stored)
    }

    async fn load_token_inner(
        &self,
        preparation: &McpOAuthTokenPreparation,
        now: DateTime<Utc>,
    ) -> Result<Option<StoredMcpOAuthTokenSecret>, McpOAuthTokenStoreError> {
        preparation.validate_at(now)?;
        let provider = self
            .providers
            .get(&preparation.token_secret_provider_id)
            .ok_or(McpOAuthTokenStoreError::Rejected)?;
        match provider
            .load_prepared_mcp_oauth_token(preparation)
            .await
            .map_err(map_provider_prepare_error_to_token_store)?
        {
            Some(prepared) => self
                .register_token_result(preparation, prepared, now)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn store_token_inner(
        &self,
        preparation: &McpOAuthTokenPreparation,
        tokens: &McpOAuthTokenSet,
        verified: &VerifiedMcpOAuthToken,
        now: DateTime<Utc>,
    ) -> Result<StoredMcpOAuthTokenSecret, McpOAuthTokenStoreError> {
        preparation.validate_at(now)?;
        let provider = self
            .providers
            .get(&preparation.token_secret_provider_id)
            .ok_or(McpOAuthTokenStoreError::Rejected)?;
        let prepared = provider
            .prepare_or_load_mcp_oauth_token(preparation, tokens, verified)
            .await
            .map_err(map_provider_prepare_error_to_token_store)?;
        self.register_token_result(preparation, prepared, now).await
    }
}

#[async_trait]
impl McpOAuthTransientSecretStore for BrokeredMcpOAuthSecretStore {
    async fn prepare_or_load(
        &self,
        candidate: NewMcpOAuthTransientSecretBundle,
    ) -> Result<StoredMcpOAuthTransientSecretBundle, McpOAuthTransientSecretStoreError> {
        candidate.validate()?;
        let now = Utc::now();
        let permit = self
            .in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| McpOAuthTransientSecretStoreError::TemporarilyUnavailable)?;
        let result = timeout(
            self.limits.resolution_timeout,
            self.prepare_transient_inner(candidate, now),
        )
        .await
        .map_err(|_| McpOAuthTransientSecretStoreError::TemporarilyUnavailable)?;
        drop(permit);
        result
    }
}

#[async_trait]
impl McpOAuthTokenStore for BrokeredMcpOAuthSecretStore {
    async fn load_prepared(
        &self,
        preparation: &McpOAuthTokenPreparation,
        now: DateTime<Utc>,
    ) -> Result<Option<StoredMcpOAuthTokenSecret>, McpOAuthTokenStoreError> {
        let permit = self
            .in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| McpOAuthTokenStoreError::TemporarilyUnavailable)?;
        let result = timeout(
            self.limits.resolution_timeout,
            self.load_token_inner(preparation, now),
        )
        .await
        .map_err(|_| McpOAuthTokenStoreError::TemporarilyUnavailable)?;
        drop(permit);
        result
    }

    async fn store_prepared(
        &self,
        preparation: &McpOAuthTokenPreparation,
        tokens: &McpOAuthTokenSet,
        verified: &VerifiedMcpOAuthToken,
        now: DateTime<Utc>,
    ) -> Result<StoredMcpOAuthTokenSecret, McpOAuthTokenStoreError> {
        let permit = self
            .in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| McpOAuthTokenStoreError::TemporarilyUnavailable)?;
        let result = timeout(
            self.limits.resolution_timeout,
            self.store_token_inner(preparation, tokens, verified, now),
        )
        .await
        .map_err(|_| McpOAuthTokenStoreError::TemporarilyUnavailable)?;
        drop(permit);
        result
    }
}

impl BrokeredSecretMaterialResolver {
    pub fn new(
        authority: Arc<dyn SecretBindingResolutionAuthority>,
        unsealer: Arc<dyn SecretReferenceUnsealer>,
        providers: InstalledSecretProviderCatalog,
        limits: SecretBrokerLimits,
    ) -> Result<Self, SecretBrokerConfigurationError> {
        limits.validate()?;
        Ok(Self {
            authority,
            unsealer,
            providers,
            limits,
            in_flight: Arc::new(Semaphore::new(limits.maximum_in_flight)),
        })
    }

    pub fn capacity_snapshot(&self) -> SecretBrokerCapacitySnapshot {
        SecretBrokerCapacitySnapshot {
            maximum_in_flight: self.limits.maximum_in_flight,
            available: self.in_flight.available_permits(),
        }
    }

    async fn resolve_inner(
        &self,
        tenant_id: &ResourceId,
        exact: &ExactSecretBindingRef,
    ) -> Result<ResolvedSecretMaterial, SecretMaterialResolutionError> {
        let current = self
            .authority
            .load_for_resolution(tenant_id, &exact.secret_binding_id)
            .await
            .map_err(map_authority_error)?;
        validate_current_binding(tenant_id, exact, &current)?;

        let provider = self
            .providers
            .get(&current.provider_id)
            .ok_or(SecretMaterialResolutionError::InvalidEvidence)?;
        let reference = self
            .unsealer
            .unseal(&current)
            .await
            .map_err(map_unseal_error)?;
        if digest(reference.expose()) != current.reference_digest {
            return Err(SecretMaterialResolutionError::InvalidEvidence);
        }
        let resolved = provider
            .resolve(tenant_id, &reference, &exact.resolution_policy)
            .await
            .map_err(map_provider_error)?;
        if resolved.material.is_empty()
            || resolved.material.len() > self.limits.maximum_material_bytes
            || matches!(
                &exact.resolution_policy,
                SecretResolutionPolicy::Pinned {
                    opaque_version_identity_digest
                } if opaque_version_identity_digest != &resolved.opaque_version_identity_digest
            )
        {
            return Err(SecretMaterialResolutionError::InvalidEvidence);
        }
        let version_digest = resolved.opaque_version_identity_digest.clone();
        ResolvedSecretMaterial::new(
            current.secret_binding_id,
            current.provider_id,
            current.purpose,
            current.generation,
            version_digest,
            resolved.into_material(),
        )
        .map_err(|_| SecretMaterialResolutionError::InvalidEvidence)
    }

    async fn delete_exact_inner(
        &self,
        tenant_id: &ResourceId,
        exact: &ExactSecretBindingRef,
    ) -> Result<ExactSecretVersionDeleteDisposition, ExactSecretVersionDeleteError> {
        let current = self
            .authority
            .load_for_resolution(tenant_id, &exact.secret_binding_id)
            .await
            .map_err(map_delete_authority_error)?;
        validate_current_binding(tenant_id, exact, &current)
            .map_err(map_delete_validation_error)?;
        if !matches!(
            exact.resolution_policy,
            SecretResolutionPolicy::Pinned { .. }
        ) {
            return Err(ExactSecretVersionDeleteError::Rejected);
        }
        let provider = self
            .providers
            .get(&current.provider_id)
            .ok_or(ExactSecretVersionDeleteError::Rejected)?;
        let reference = self
            .unsealer
            .unseal(&current)
            .await
            .map_err(map_delete_unseal_error)?;
        if digest(reference.expose()) != current.reference_digest {
            return Err(ExactSecretVersionDeleteError::Rejected);
        }
        provider
            .delete_exact(tenant_id, &reference, &exact.resolution_policy)
            .await
            .map(|disposition| match disposition {
                SecretProviderDeleteDisposition::Deleted => {
                    ExactSecretVersionDeleteDisposition::Deleted
                }
                SecretProviderDeleteDisposition::AlreadyAbsent => {
                    ExactSecretVersionDeleteDisposition::AlreadyAbsent
                }
            })
            .map_err(|failure| match failure {
                SecretProviderDeleteError::Unavailable => {
                    ExactSecretVersionDeleteError::TemporarilyUnavailable
                }
                SecretProviderDeleteError::Rejected => ExactSecretVersionDeleteError::Rejected,
                SecretProviderDeleteError::OutcomeUncertain => {
                    ExactSecretVersionDeleteError::OutcomeUncertain
                }
            })
    }
}

#[async_trait]
impl SecretMaterialResolver for BrokeredSecretMaterialResolver {
    async fn resolve(
        &self,
        tenant_id: &ResourceId,
        binding: &ExactSecretBindingRef,
    ) -> Result<ResolvedSecretMaterial, SecretMaterialResolutionError> {
        if tenant_id.kind() != ResourceKind::Tenant || binding.validate().is_err() {
            return Err(SecretMaterialResolutionError::InvalidEvidence);
        }
        let permit = self
            .in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| SecretMaterialResolutionError::Unavailable)?;
        let result = timeout(
            self.limits.resolution_timeout,
            self.resolve_inner(tenant_id, binding),
        )
        .await
        .map_err(|_| SecretMaterialResolutionError::Unavailable)?;
        drop(permit);
        result
    }
}

#[async_trait]
impl ExactSecretVersionDeleter for BrokeredSecretMaterialResolver {
    async fn delete_exact_version(
        &self,
        tenant_id: &ResourceId,
        binding: &ExactSecretBindingRef,
    ) -> Result<ExactSecretVersionDeleteDisposition, ExactSecretVersionDeleteError> {
        if tenant_id.kind() != ResourceKind::Tenant || binding.validate().is_err() {
            return Err(ExactSecretVersionDeleteError::Rejected);
        }
        let permit = self
            .in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| ExactSecretVersionDeleteError::TemporarilyUnavailable)?;
        let result = timeout(
            self.limits.resolution_timeout,
            self.delete_exact_inner(tenant_id, binding),
        )
        .await
        .map_err(|_| ExactSecretVersionDeleteError::TemporarilyUnavailable)?;
        drop(permit);
        result
    }
}

fn validate_current_binding(
    tenant_id: &ResourceId,
    exact: &ExactSecretBindingRef,
    current: &SecretBindingResolutionRecord,
) -> Result<(), SecretMaterialResolutionError> {
    current
        .validate()
        .map_err(|_| SecretMaterialResolutionError::InvalidEvidence)?;
    if current.tenant_id != *tenant_id
        || current.secret_binding_id != exact.secret_binding_id
        || current.provider_id != exact.provider_id
        || current.purpose != exact.purpose
        || current.payload.provider_id != exact.provider_id
        || current.payload.resolution_policy != exact.resolution_policy
    {
        return Err(SecretMaterialResolutionError::InvalidEvidence);
    }
    if current.state != SecretBindingState::Active {
        return Err(SecretMaterialResolutionError::Revoked);
    }
    if !exact.permits_resolved_generation(
        &current.secret_binding_id,
        &current.purpose,
        current.generation,
    ) {
        return Err(SecretMaterialResolutionError::InvalidEvidence);
    }
    Ok(())
}

fn map_authority_error(error: SecretBindingResolutionError) -> SecretMaterialResolutionError {
    match error {
        SecretBindingResolutionError::Unavailable => SecretMaterialResolutionError::Unavailable,
        SecretBindingResolutionError::NotFound => SecretMaterialResolutionError::NotFound,
        SecretBindingResolutionError::InvalidEvidence => {
            SecretMaterialResolutionError::InvalidEvidence
        }
    }
}

fn map_unseal_error(error: SecretReferenceUnsealError) -> SecretMaterialResolutionError {
    match error {
        SecretReferenceUnsealError::Unavailable => SecretMaterialResolutionError::Unavailable,
        SecretReferenceUnsealError::Rejected | SecretReferenceUnsealError::InvalidEvidence => {
            SecretMaterialResolutionError::InvalidEvidence
        }
    }
}

fn map_provider_error(error: SecretProviderResolveError) -> SecretMaterialResolutionError {
    match error {
        SecretProviderResolveError::Unavailable => SecretMaterialResolutionError::Unavailable,
        SecretProviderResolveError::NotFound => SecretMaterialResolutionError::NotFound,
        SecretProviderResolveError::Rejected | SecretProviderResolveError::InvalidEvidence => {
            SecretMaterialResolutionError::InvalidEvidence
        }
    }
}

fn map_delete_authority_error(
    error: SecretBindingResolutionError,
) -> ExactSecretVersionDeleteError {
    match error {
        SecretBindingResolutionError::Unavailable => {
            ExactSecretVersionDeleteError::TemporarilyUnavailable
        }
        SecretBindingResolutionError::NotFound | SecretBindingResolutionError::InvalidEvidence => {
            ExactSecretVersionDeleteError::Rejected
        }
    }
}

fn map_delete_validation_error(
    error: SecretMaterialResolutionError,
) -> ExactSecretVersionDeleteError {
    match error {
        SecretMaterialResolutionError::Unavailable => {
            ExactSecretVersionDeleteError::TemporarilyUnavailable
        }
        SecretMaterialResolutionError::NotFound
        | SecretMaterialResolutionError::Revoked
        | SecretMaterialResolutionError::InvalidEvidence => ExactSecretVersionDeleteError::Rejected,
    }
}

fn map_delete_unseal_error(error: SecretReferenceUnsealError) -> ExactSecretVersionDeleteError {
    match error {
        SecretReferenceUnsealError::Unavailable => {
            ExactSecretVersionDeleteError::TemporarilyUnavailable
        }
        SecretReferenceUnsealError::Rejected | SecretReferenceUnsealError::InvalidEvidence => {
            ExactSecretVersionDeleteError::Rejected
        }
    }
}

fn validate_provider_prepared_secret(
    tenant_id: &ResourceId,
    purpose: &SecretPurpose,
    exact: &ExactSecretBindingRef,
    prepared: &ProviderPreparedSecretVersion,
) -> Result<(), McpOAuthTransientSecretStoreError> {
    if tenant_id.kind() != ResourceKind::Tenant
        || exact.validate().is_err()
        || exact.secret_binding_id != prepared.secret_binding_id
        || exact.provider_id != prepared.provider_id
        || exact.purpose != *purpose
        || exact.binding_generation != 1
        || !matches!(
            &exact.resolution_policy,
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest
            } if opaque_version_identity_digest == &prepared.opaque_version_identity_digest
        )
        || prepared.opaque_reference.expose().is_empty()
    {
        return Err(McpOAuthTransientSecretStoreError::Rejected);
    }
    Ok(())
}

fn map_provider_prepare_error(
    failure: SecretProviderPrepareError,
) -> McpOAuthTransientSecretStoreError {
    match failure {
        SecretProviderPrepareError::Unavailable => {
            McpOAuthTransientSecretStoreError::TemporarilyUnavailable
        }
        SecretProviderPrepareError::Rejected => McpOAuthTransientSecretStoreError::Rejected,
        SecretProviderPrepareError::WriteUncertain => {
            McpOAuthTransientSecretStoreError::WriteUncertain
        }
    }
}

fn map_provider_prepare_error_to_token_store(
    failure: SecretProviderPrepareError,
) -> McpOAuthTokenStoreError {
    match failure {
        SecretProviderPrepareError::Unavailable => McpOAuthTokenStoreError::TemporarilyUnavailable,
        SecretProviderPrepareError::Rejected => McpOAuthTokenStoreError::Rejected,
        SecretProviderPrepareError::WriteUncertain => McpOAuthTokenStoreError::WriteUncertain,
    }
}

fn map_seal_error(failure: SecretReferenceSealError) -> McpOAuthTransientSecretStoreError {
    match failure {
        SecretReferenceSealError::Unavailable => {
            McpOAuthTransientSecretStoreError::TemporarilyUnavailable
        }
        SecretReferenceSealError::Rejected | SecretReferenceSealError::InvalidEvidence => {
            McpOAuthTransientSecretStoreError::Rejected
        }
    }
}

fn map_registration_error(
    failure: PreparedSecretBindingRegistrationError,
) -> McpOAuthTransientSecretStoreError {
    match failure {
        PreparedSecretBindingRegistrationError::Rejected => {
            McpOAuthTransientSecretStoreError::Rejected
        }
        PreparedSecretBindingRegistrationError::TemporarilyUnavailable => {
            McpOAuthTransientSecretStoreError::TemporarilyUnavailable
        }
    }
}

fn map_transient_store_error_to_token_store(
    failure: McpOAuthTransientSecretStoreError,
) -> McpOAuthTokenStoreError {
    match failure {
        McpOAuthTransientSecretStoreError::Rejected => McpOAuthTokenStoreError::Rejected,
        McpOAuthTransientSecretStoreError::TemporarilyUnavailable => {
            McpOAuthTokenStoreError::TemporarilyUnavailable
        }
        McpOAuthTransientSecretStoreError::WriteUncertain => {
            McpOAuthTokenStoreError::WriteUncertain
        }
    }
}

fn registration_receipt_expiry(
    now: DateTime<Utc>,
    prepared_expires_at: DateTime<Utc>,
) -> Result<DateTime<Utc>, McpOAuthTransientSecretStoreError> {
    if prepared_expires_at <= now || prepared_expires_at - now > MAX_PREPARED_SECRET_TTL {
        return Err(McpOAuthTransientSecretStoreError::Rejected);
    }
    prepared_expires_at
        .checked_add_signed(ChronoDuration::hours(1))
        .ok_or(McpOAuthTransientSecretStoreError::Rejected)
}

fn new_resource_id(kind: ResourceKind) -> Result<ResourceId, McpOAuthTransientSecretStoreError> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7())
        .map_err(|_| McpOAuthTransientSecretStoreError::TemporarilyUnavailable)
}

fn digest(bytes: &[u8]) -> Sha256Digest {
    let hash = Sha256::digest(bytes);
    let hex = hash
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Sha256Digest::from_str(&format!("sha256:{hex}"))
        .expect("SHA-256 output is always a canonical digest")
}

#[cfg(test)]
mod tests;
