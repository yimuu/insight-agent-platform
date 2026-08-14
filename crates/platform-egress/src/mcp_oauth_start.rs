use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ExactDeploymentRef, ExactSecretBindingRef, ResourceId, SecretResolutionPolicy, Sha256Digest,
};
use insight_platform_mcp_host::{
    AuthenticatedMcpOAuthState, McpOAuthAuthorizationPreparationBroker,
    McpOAuthAuthorizationPreparationError, McpOAuthAuthorizationPreparationRequest,
    McpOAuthCallbackError, McpOAuthStateIssuer, PreparedMcpOAuthAuthorization,
    SensitiveMcpOAuthNonce, SensitiveOAuthValue, MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use ring::{
    digest::{digest, SHA256},
    rand::{SecureRandom, SystemRandom},
};
use std::{error::Error, fmt, sync::Arc};
use tokio::sync::Semaphore;

pub const MCP_OAUTH_PKCE_VERIFIER_ENTROPY_BYTES: usize = 32;
pub const MCP_OAUTH_NONCE_ENTROPY_BYTES: usize = 32;
pub const MAX_MCP_OAUTH_PREPARATION_IN_FLIGHT_HARD: usize = 512;
pub const MAX_MCP_OAUTH_PKCE_VERIFIER_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpOAuthAuthorizationPreparationLimits {
    pub maximum_in_flight: usize,
}

impl Default for McpOAuthAuthorizationPreparationLimits {
    fn default() -> Self {
        Self {
            maximum_in_flight: 64,
        }
    }
}

impl McpOAuthAuthorizationPreparationLimits {
    fn validate(self) -> Result<(), McpOAuthAuthorizationPreparationConfigurationError> {
        if self.maximum_in_flight == 0
            || self.maximum_in_flight > MAX_MCP_OAUTH_PREPARATION_IN_FLIGHT_HARD
        {
            return Err(McpOAuthAuthorizationPreparationConfigurationError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthAuthorizationPreparationConfigurationError {
    InvalidLimits,
}

impl fmt::Display for McpOAuthAuthorizationPreparationConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP OAuth authorization preparation limits are invalid")
    }
}

impl Error for McpOAuthAuthorizationPreparationConfigurationError {}

/// Raw PKCE verifier. It is accepted only by the Secret Manager preparation port and is zeroed.
pub struct SensitiveMcpOAuthPkceVerifier(Vec<u8>);

impl SensitiveMcpOAuthPkceVerifier {
    pub fn new(mut value: Vec<u8>) -> Result<Self, McpOAuthTransientSecretStoreError> {
        if !(43..=MAX_MCP_OAUTH_PKCE_VERIFIER_BYTES).contains(&value.len())
            || !value.iter().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_' | b'~')
            })
        {
            value.fill(0);
            return Err(McpOAuthTransientSecretStoreError::Rejected);
        }
        Ok(Self(value))
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SensitiveMcpOAuthPkceVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveMcpOAuthPkceVerifier")
            .field("byte_length", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl Drop for SensitiveMcpOAuthPkceVerifier {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Candidate prepared entry. The store owns idempotency by `preparation_digest` and must either
/// create this entry or load the exact existing entry. Its exact SecretBinding resolves only to
/// `pkce_verifier`; state and nonce are transient metadata returned for URL reconstruction.
pub struct NewMcpOAuthTransientSecretBundle {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub pkce_secret_provider_id: ResourceId,
    pub preparation_digest: Sha256Digest,
    pub callback_binding_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub state: SensitiveOAuthValue,
    pub nonce: SensitiveMcpOAuthNonce,
    pub pkce_verifier: SensitiveMcpOAuthPkceVerifier,
}

impl NewMcpOAuthTransientSecretBundle {
    pub fn validate(&self) -> Result<(), McpOAuthTransientSecretStoreError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != insight_platform_contracts::ResourceKind::Tenant
            || self.task_id.kind() != insight_platform_contracts::ResourceKind::Interaction
            || self.authorization_binding_id.kind()
                != insight_platform_contracts::ResourceKind::McpAuthorizationBinding
            || self.mcp_deployment.resource_kind
                != insight_platform_contracts::ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.pkce_secret_provider_id.kind()
                != insight_platform_contracts::ResourceKind::SecretProvider
            || self.expires_at <= Utc::now()
            || self.state.as_bytes().is_empty()
            || self.nonce.as_bytes().is_empty()
            || self.pkce_verifier.expose().is_empty()
        {
            return Err(McpOAuthTransientSecretStoreError::Rejected);
        }
        Ok(())
    }
}

impl fmt::Debug for NewMcpOAuthTransientSecretBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewMcpOAuthTransientSecretBundle")
            .field("schema_version", &self.schema_version)
            .field("tenant_id", &self.tenant_id)
            .field("task_id", &self.task_id)
            .field("authorization_binding_id", &self.authorization_binding_id)
            .field("mcp_deployment", &self.mcp_deployment)
            .field("pkce_secret_provider_id", &self.pkce_secret_provider_id)
            .field("preparation_digest", &self.preparation_digest)
            .field("callback_binding_digest", &self.callback_binding_digest)
            .field("expires_at", &self.expires_at)
            .field("state", &self.state)
            .field("nonce", &self.nonce)
            .field("pkce_verifier", &self.pkce_verifier)
            .finish()
    }
}

pub struct StoredMcpOAuthTransientSecretBundle {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub authorization_binding_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub pkce_secret_provider_id: ResourceId,
    pub preparation_digest: Sha256Digest,
    pub callback_binding_digest: Sha256Digest,
    pub expires_at: DateTime<Utc>,
    pub state: SensitiveOAuthValue,
    pub nonce: SensitiveMcpOAuthNonce,
    pub pkce_verifier: SensitiveMcpOAuthPkceVerifier,
    pub pkce_secret_binding: ExactSecretBindingRef,
    pub storage_evidence_digest: Sha256Digest,
}

impl fmt::Debug for StoredMcpOAuthTransientSecretBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredMcpOAuthTransientSecretBundle")
            .field("schema_version", &self.schema_version)
            .field("tenant_id", &self.tenant_id)
            .field("task_id", &self.task_id)
            .field("authorization_binding_id", &self.authorization_binding_id)
            .field("mcp_deployment", &self.mcp_deployment)
            .field("pkce_secret_provider_id", &self.pkce_secret_provider_id)
            .field("preparation_digest", &self.preparation_digest)
            .field("callback_binding_digest", &self.callback_binding_digest)
            .field("expires_at", &self.expires_at)
            .field("state", &self.state)
            .field("nonce", &self.nonce)
            .field("pkce_verifier", &self.pkce_verifier)
            .field("pkce_secret_binding", &self.pkce_secret_binding)
            .field("storage_evidence_digest", &self.storage_evidence_digest)
            .finish()
    }
}

impl StoredMcpOAuthTransientSecretBundle {
    pub fn validate(&self) -> Result<(), McpOAuthTransientSecretStoreError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != insight_platform_contracts::ResourceKind::Tenant
            || self.task_id.kind() != insight_platform_contracts::ResourceKind::Interaction
            || self.authorization_binding_id.kind()
                != insight_platform_contracts::ResourceKind::McpAuthorizationBinding
            || self.mcp_deployment.resource_kind
                != insight_platform_contracts::ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.pkce_secret_provider_id.kind()
                != insight_platform_contracts::ResourceKind::SecretProvider
            || self.state.as_bytes().is_empty()
            || self.nonce.as_bytes().is_empty()
            || self.pkce_verifier.expose().is_empty()
            || self.pkce_secret_binding.validate().is_err()
            || self.pkce_secret_binding.provider_id != self.pkce_secret_provider_id
            || self.pkce_secret_binding.purpose.as_str() != MCP_OAUTH_PKCE_SECRET_PURPOSE
            || !matches!(
                &self.pkce_secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
        {
            return Err(McpOAuthTransientSecretStoreError::Rejected);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        request: &McpOAuthAuthorizationPreparationRequest,
    ) -> Result<(), McpOAuthTransientSecretStoreError> {
        self.validate()?;
        if self.tenant_id != request.tenant_id
            || self.task_id != request.task_id
            || self.authorization_binding_id != request.authorization_binding_id
            || self.mcp_deployment != request.mcp_deployment
            || self.pkce_secret_provider_id != request.pkce_secret_provider_id
            || self.preparation_digest != request.preparation_digest
            || self.callback_binding_digest != request.callback_binding_digest
            || self.expires_at != request.expires_at
        {
            return Err(McpOAuthTransientSecretStoreError::Rejected);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthTransientSecretStoreError {
    Rejected,
    TemporarilyUnavailable,
    WriteUncertain,
}

/// External Secret Manager port. An orphan entry caused by a failed PostgreSQL commit must expire
/// at `expires_at`; retrying the same digest must return byte-identical state, nonce and verifier.
#[async_trait]
pub trait McpOAuthTransientSecretStore: Send + Sync {
    async fn prepare_or_load(
        &self,
        candidate: NewMcpOAuthTransientSecretBundle,
    ) -> Result<StoredMcpOAuthTransientSecretBundle, McpOAuthTransientSecretStoreError>;
}

trait McpOAuthPreparationRandomSource: Send + Sync {
    fn fill(&self, destination: &mut [u8]) -> Result<(), ()>;
}

struct SystemMcpOAuthPreparationRandomSource(SystemRandom);

impl Default for SystemMcpOAuthPreparationRandomSource {
    fn default() -> Self {
        Self(SystemRandom::new())
    }
}

impl McpOAuthPreparationRandomSource for SystemMcpOAuthPreparationRandomSource {
    fn fill(&self, destination: &mut [u8]) -> Result<(), ()> {
        self.0.fill(destination).map_err(|_| ())
    }
}

pub struct BrokeredMcpOAuthAuthorizationPreparation {
    state_issuer: Arc<dyn McpOAuthStateIssuer>,
    store: Arc<dyn McpOAuthTransientSecretStore>,
    random: Arc<dyn McpOAuthPreparationRandomSource>,
    permits: Arc<Semaphore>,
}

impl BrokeredMcpOAuthAuthorizationPreparation {
    pub fn new(
        state_issuer: Arc<dyn McpOAuthStateIssuer>,
        store: Arc<dyn McpOAuthTransientSecretStore>,
        limits: McpOAuthAuthorizationPreparationLimits,
    ) -> Result<Self, McpOAuthAuthorizationPreparationConfigurationError> {
        Self::with_random(
            state_issuer,
            store,
            Arc::new(SystemMcpOAuthPreparationRandomSource::default()),
            limits,
        )
    }

    fn with_random(
        state_issuer: Arc<dyn McpOAuthStateIssuer>,
        store: Arc<dyn McpOAuthTransientSecretStore>,
        random: Arc<dyn McpOAuthPreparationRandomSource>,
        limits: McpOAuthAuthorizationPreparationLimits,
    ) -> Result<Self, McpOAuthAuthorizationPreparationConfigurationError> {
        limits.validate()?;
        Ok(Self {
            state_issuer,
            store,
            random,
            permits: Arc::new(Semaphore::new(limits.maximum_in_flight)),
        })
    }
}

#[async_trait]
impl McpOAuthAuthorizationPreparationBroker for BrokeredMcpOAuthAuthorizationPreparation {
    async fn prepare_or_load(
        &self,
        request: &McpOAuthAuthorizationPreparationRequest,
        now: DateTime<Utc>,
    ) -> Result<PreparedMcpOAuthAuthorization, McpOAuthAuthorizationPreparationError> {
        request
            .validate_at(now)
            .map_err(|_| McpOAuthAuthorizationPreparationError::Rejected)?;
        let _permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| McpOAuthAuthorizationPreparationError::TemporarilyUnavailable)?;
        let state = self
            .state_issuer
            .issue_state(
                &AuthenticatedMcpOAuthState {
                    tenant_id: request.tenant_id.clone(),
                    task_id: request.task_id.clone(),
                },
                now,
                request.expires_at,
            )
            .map_err(map_state_issue_error)?;
        let mut verifier_entropy = [0_u8; MCP_OAUTH_PKCE_VERIFIER_ENTROPY_BYTES];
        if self.random.fill(&mut verifier_entropy).is_err() {
            verifier_entropy.fill(0);
            return Err(McpOAuthAuthorizationPreparationError::TemporarilyUnavailable);
        }
        let verifier = URL_SAFE_NO_PAD.encode(verifier_entropy);
        verifier_entropy.fill(0);
        let pkce_verifier =
            SensitiveMcpOAuthPkceVerifier::new(verifier.into_bytes()).map_err(map_store_error)?;
        let mut nonce_entropy = [0_u8; MCP_OAUTH_NONCE_ENTROPY_BYTES];
        if self.random.fill(&mut nonce_entropy).is_err() {
            nonce_entropy.fill(0);
            return Err(McpOAuthAuthorizationPreparationError::TemporarilyUnavailable);
        }
        let nonce = URL_SAFE_NO_PAD.encode(nonce_entropy);
        nonce_entropy.fill(0);
        let nonce = SensitiveMcpOAuthNonce::new(nonce.into_bytes())
            .map_err(|_| McpOAuthAuthorizationPreparationError::Rejected)?;
        let stored = self
            .store
            .prepare_or_load(NewMcpOAuthTransientSecretBundle {
                schema_version: 1,
                tenant_id: request.tenant_id.clone(),
                task_id: request.task_id.clone(),
                authorization_binding_id: request.authorization_binding_id.clone(),
                mcp_deployment: request.mcp_deployment.clone(),
                pkce_secret_provider_id: request.pkce_secret_provider_id.clone(),
                preparation_digest: request.preparation_digest.clone(),
                callback_binding_digest: request.callback_binding_digest.clone(),
                expires_at: request.expires_at,
                state,
                nonce,
                pkce_verifier,
            })
            .await
            .map_err(map_store_error)?;
        stored.validate_for(request).map_err(map_store_error)?;
        let pkce_challenge = URL_SAFE_NO_PAD.encode(digest(&SHA256, stored.pkce_verifier.expose()));
        Ok(PreparedMcpOAuthAuthorization {
            preparation_digest: stored.preparation_digest,
            state: stored.state,
            nonce: stored.nonce,
            pkce_challenge,
            pkce_secret_binding: stored.pkce_secret_binding,
            storage_evidence_digest: stored.storage_evidence_digest,
        })
    }
}

fn map_state_issue_error(error: McpOAuthCallbackError) -> McpOAuthAuthorizationPreparationError {
    match error {
        McpOAuthCallbackError::Rejected(_) => McpOAuthAuthorizationPreparationError::Rejected,
        McpOAuthCallbackError::TemporarilyUnavailable(_) => {
            McpOAuthAuthorizationPreparationError::TemporarilyUnavailable
        }
        McpOAuthCallbackError::CommitUncertain(_) => {
            McpOAuthAuthorizationPreparationError::WriteUncertain
        }
    }
}

fn map_store_error(
    error: McpOAuthTransientSecretStoreError,
) -> McpOAuthAuthorizationPreparationError {
    match error {
        McpOAuthTransientSecretStoreError::Rejected => {
            McpOAuthAuthorizationPreparationError::Rejected
        }
        McpOAuthTransientSecretStoreError::TemporarilyUnavailable => {
            McpOAuthAuthorizationPreparationError::TemporarilyUnavailable
        }
        McpOAuthTransientSecretStoreError::WriteUncertain => {
            McpOAuthAuthorizationPreparationError::WriteUncertain
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use insight_platform_contracts::{ExactDeploymentRef, SecretPurpose, SecretResolutionPolicy};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn sha(character: char) -> Sha256Digest {
        let hexadecimal = char::from_digit((character as u32) % 16, 16).unwrap();
        format!("sha256:{}", hexadecimal.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn deployment() -> ExactDeploymentRef {
        ExactDeploymentRef::new(id("mcdep_0198f1c3-8f49-7c3e-b1f3-773c28367bb0"), sha('d')).unwrap()
    }

    fn binding() -> ExactSecretBindingRef {
        ExactSecretBindingRef::build(
            id("sbd_0198f1c3-8f49-7c3e-b1f3-773c28367bb1"),
            2,
            id("spr_0198f1c3-8f49-7c3e-b1f3-773c28367bb2"),
            MCP_OAUTH_PKCE_SECRET_PURPOSE
                .parse::<SecretPurpose>()
                .unwrap(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: sha('v'),
            },
        )
        .unwrap()
    }

    fn request(now: DateTime<Utc>) -> McpOAuthAuthorizationPreparationRequest {
        McpOAuthAuthorizationPreparationRequest {
            schema_version: 1,
            tenant_id: id("ten_0198f1c3-8f49-7c3e-b1f3-773c28367bb3"),
            task_id: id("int_0198f1c3-8f49-7c3e-b1f3-773c28367bb4"),
            authorization_binding_id: id("mab_0198f1c3-8f49-7c3e-b1f3-773c28367bb5"),
            mcp_deployment: deployment(),
            pkce_secret_provider_id: binding().provider_id,
            preparation_digest: sha('p'),
            callback_binding_digest: sha('c'),
            expires_at: now + Duration::minutes(10),
        }
    }

    struct FixedStateIssuer;

    impl McpOAuthStateIssuer for FixedStateIssuer {
        fn issue_state(
            &self,
            _identity: &AuthenticatedMcpOAuthState,
            _issued_at: DateTime<Utc>,
            _expires_at: DateTime<Utc>,
        ) -> Result<SensitiveOAuthValue, McpOAuthCallbackError> {
            SensitiveOAuthValue::from_decoded(
                b"sealed-state".to_vec(),
                insight_platform_mcp_host::MAX_MCP_OAUTH_STATE_BYTES,
            )
        }
    }

    struct FixedRandom {
        calls: AtomicUsize,
    }

    impl McpOAuthPreparationRandomSource for FixedRandom {
        fn fill(&self, destination: &mut [u8]) -> Result<(), ()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            destination.fill((call + 1) as u8);
            Ok(())
        }
    }

    struct EchoStore {
        calls: AtomicUsize,
        first: Mutex<Option<RememberedValues>>,
        wrong_digest: bool,
    }

    struct RememberedValues {
        state: Vec<u8>,
        nonce: Vec<u8>,
        verifier: Vec<u8>,
    }

    #[async_trait]
    impl McpOAuthTransientSecretStore for EchoStore {
        async fn prepare_or_load(
            &self,
            candidate: NewMcpOAuthTransientSecretBundle,
        ) -> Result<StoredMcpOAuthTransientSecretBundle, McpOAuthTransientSecretStoreError>
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut first = self.first.lock().unwrap();
            let values = first.get_or_insert_with(|| RememberedValues {
                state: candidate.state.as_bytes().to_vec(),
                nonce: candidate.nonce.as_bytes().to_vec(),
                verifier: candidate.pkce_verifier.expose().to_vec(),
            });
            Ok(StoredMcpOAuthTransientSecretBundle {
                schema_version: candidate.schema_version,
                tenant_id: candidate.tenant_id,
                task_id: candidate.task_id,
                authorization_binding_id: candidate.authorization_binding_id,
                mcp_deployment: candidate.mcp_deployment,
                pkce_secret_provider_id: candidate.pkce_secret_provider_id,
                preparation_digest: if self.wrong_digest {
                    sha('x')
                } else {
                    candidate.preparation_digest
                },
                callback_binding_digest: candidate.callback_binding_digest,
                expires_at: candidate.expires_at,
                state: SensitiveOAuthValue::from_decoded(
                    values.state.clone(),
                    insight_platform_mcp_host::MAX_MCP_OAUTH_STATE_BYTES,
                )
                .unwrap(),
                nonce: SensitiveMcpOAuthNonce::new(values.nonce.clone()).unwrap(),
                pkce_verifier: SensitiveMcpOAuthPkceVerifier::new(values.verifier.clone()).unwrap(),
                pkce_secret_binding: binding(),
                storage_evidence_digest: sha('s'),
            })
        }
    }

    fn broker(
        store: Arc<EchoStore>,
        random: Arc<FixedRandom>,
        maximum_in_flight: usize,
    ) -> BrokeredMcpOAuthAuthorizationPreparation {
        BrokeredMcpOAuthAuthorizationPreparation::with_random(
            Arc::new(FixedStateIssuer),
            store,
            random,
            McpOAuthAuthorizationPreparationLimits { maximum_in_flight },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn preparation_is_idempotent_and_challenge_uses_stored_verifier() {
        let now = Utc::now();
        let store = Arc::new(EchoStore {
            calls: AtomicUsize::new(0),
            first: Mutex::new(None),
            wrong_digest: false,
        });
        let random = Arc::new(FixedRandom {
            calls: AtomicUsize::new(0),
        });
        let broker = broker(store.clone(), random, 2);
        let first = broker.prepare_or_load(&request(now), now).await.unwrap();
        let second = broker.prepare_or_load(&request(now), now).await.unwrap();
        assert_eq!(first.state.as_bytes(), second.state.as_bytes());
        assert_eq!(first.nonce.as_bytes(), second.nonce.as_bytes());
        assert_eq!(first.pkce_challenge, second.pkce_challenge);
        assert_eq!(first.pkce_secret_binding, binding());
        assert_eq!(store.calls.load(Ordering::SeqCst), 2);
        let debug = format!("{first:?}");
        assert!(!debug.contains("sealed-state"));
        assert!(!debug.contains(std::str::from_utf8(first.nonce.as_bytes()).unwrap()));
    }

    #[tokio::test]
    async fn mismatched_store_identity_is_rejected() {
        let now = Utc::now();
        let store = Arc::new(EchoStore {
            calls: AtomicUsize::new(0),
            first: Mutex::new(None),
            wrong_digest: true,
        });
        let broker = broker(
            store,
            Arc::new(FixedRandom {
                calls: AtomicUsize::new(0),
            }),
            1,
        );
        assert!(matches!(
            broker.prepare_or_load(&request(now), now).await,
            Err(McpOAuthAuthorizationPreparationError::Rejected)
        ));
    }

    #[tokio::test]
    async fn saturated_bulkhead_rejects_before_random_or_store() {
        let now = Utc::now();
        let store = Arc::new(EchoStore {
            calls: AtomicUsize::new(0),
            first: Mutex::new(None),
            wrong_digest: false,
        });
        let random = Arc::new(FixedRandom {
            calls: AtomicUsize::new(0),
        });
        let broker = broker(store.clone(), random.clone(), 1);
        let _occupied = broker.permits.clone().try_acquire_owned().unwrap();
        assert!(matches!(
            broker.prepare_or_load(&request(now), now).await,
            Err(McpOAuthAuthorizationPreparationError::TemporarilyUnavailable)
        ));
        assert_eq!(random.calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    }
}
