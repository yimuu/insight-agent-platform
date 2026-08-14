use super::*;
use insight_platform_contracts::{
    canonical_digest, ExactDeploymentRef, ExactSecretBindingRef, SecretBindingPayload,
    SecretPurpose, SecretResolutionPolicy,
};
use insight_platform_mcp_host::{
    SensitiveMcpOAuthNonce, SensitiveOAuthValue, MAX_MCP_OAUTH_STATE_BYTES,
    MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use insight_platform_security::{
    EncryptedOpaqueReference, PreparedSecretBindingRegistrationDisposition,
    PreparedSecretBindingRegistrationOutcome, SecretBindingResolutionAuthority,
    SecretBindingResolutionRecord,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

fn id(kind: &str, suffix: u16) -> ResourceId {
    format!("{kind}_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix:04x}")
        .parse()
        .unwrap()
}

fn sha(label: &[u8]) -> Sha256Digest {
    digest(label)
}

fn purpose() -> SecretPurpose {
    "provider.api_key".parse().unwrap()
}

fn policy(version: &[u8]) -> SecretResolutionPolicy {
    SecretResolutionPolicy::Pinned {
        opaque_version_identity_digest: sha(version),
    }
}

fn exact(
    binding_id: ResourceId,
    provider_id: ResourceId,
    generation: u64,
    policy: SecretResolutionPolicy,
) -> ExactSecretBindingRef {
    ExactSecretBindingRef::build(binding_id, generation, provider_id, purpose(), policy).unwrap()
}

#[derive(Clone)]
struct FixtureAuthority {
    record: SecretBindingResolutionRecord,
}

#[async_trait]
impl SecretBindingResolutionAuthority for FixtureAuthority {
    async fn load_for_resolution(
        &self,
        tenant_id: &ResourceId,
        secret_binding_id: &ResourceId,
    ) -> Result<SecretBindingResolutionRecord, SecretBindingResolutionError> {
        if tenant_id != &self.record.tenant_id
            || secret_binding_id != &self.record.secret_binding_id
        {
            return Err(SecretBindingResolutionError::NotFound);
        }
        Ok(self.record.clone())
    }
}

struct FixtureUnsealer {
    reference: Vec<u8>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl SecretReferenceUnsealer for FixtureUnsealer {
    async fn unseal(
        &self,
        _binding: &SecretBindingResolutionRecord,
    ) -> Result<OpaqueSecretReference, SecretReferenceUnsealError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        OpaqueSecretReference::new(self.reference.clone())
    }
}

struct FixtureProvider {
    provider_id: ResourceId,
    expected_reference: Vec<u8>,
    version: Sha256Digest,
    material: Vec<u8>,
    delay: Duration,
    calls: Arc<AtomicUsize>,
    delete_disposition: SecretProviderDeleteDisposition,
    delete_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl InstalledSecretProvider for FixtureProvider {
    fn provider_id(&self) -> &ResourceId {
        &self.provider_id
    }

    async fn resolve(
        &self,
        tenant_id: &ResourceId,
        reference: &OpaqueSecretReference,
        _policy: &SecretResolutionPolicy,
    ) -> Result<ProviderSecretMaterial, SecretProviderResolveError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if tenant_id.kind() != ResourceKind::Tenant || reference.expose() != self.expected_reference
        {
            return Err(SecretProviderResolveError::Rejected);
        }
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        ProviderSecretMaterial::new(self.version.clone(), self.material.clone())
    }

    async fn delete_exact(
        &self,
        tenant_id: &ResourceId,
        reference: &OpaqueSecretReference,
        policy: &SecretResolutionPolicy,
    ) -> Result<SecretProviderDeleteDisposition, SecretProviderDeleteError> {
        self.delete_calls.fetch_add(1, Ordering::SeqCst);
        if tenant_id.kind() != ResourceKind::Tenant
            || reference.expose() != self.expected_reference
            || !matches!(policy, SecretResolutionPolicy::Pinned { .. })
        {
            return Err(SecretProviderDeleteError::Rejected);
        }
        Ok(self.delete_disposition)
    }
}

struct Fixture {
    tenant_id: ResourceId,
    binding_id: ResourceId,
    provider_id: ResourceId,
    reference: Vec<u8>,
    version: Vec<u8>,
    unseal_calls: Arc<AtomicUsize>,
    provider_calls: Arc<AtomicUsize>,
    delete_calls: Arc<AtomicUsize>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            tenant_id: id("ten", 1),
            binding_id: id("sbd", 2),
            provider_id: id("spr", 3),
            reference: b"vault:path/tenant/key#version=7".to_vec(),
            version: b"provider-version-7".to_vec(),
            unseal_calls: Arc::new(AtomicUsize::new(0)),
            provider_calls: Arc::new(AtomicUsize::new(0)),
            delete_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn record(
        &self,
        generation: u64,
        state: SecretBindingState,
        resolution_policy: SecretResolutionPolicy,
    ) -> SecretBindingResolutionRecord {
        SecretBindingResolutionRecord {
            tenant_id: self.tenant_id.clone(),
            secret_binding_id: self.binding_id.clone(),
            purpose: purpose(),
            provider_id: self.provider_id.clone(),
            state,
            generation,
            encrypted_reference: EncryptedOpaqueReference::new(b"ciphertext".to_vec()).unwrap(),
            key_id: "kms://tenant-key/7".to_owned(),
            reference_digest: sha(&self.reference),
            payload: SecretBindingPayload {
                provider_id: self.provider_id.clone(),
                resolution_policy,
            },
        }
    }

    fn resolver(
        &self,
        record: SecretBindingResolutionRecord,
        unsealed_reference: Vec<u8>,
        resolved_version: Sha256Digest,
        delay: Duration,
        limits: SecretBrokerLimits,
    ) -> BrokeredSecretMaterialResolver {
        let provider: Arc<dyn InstalledSecretProvider> = Arc::new(FixtureProvider {
            provider_id: self.provider_id.clone(),
            expected_reference: self.reference.clone(),
            version: resolved_version,
            material: b"canary-secret-material".to_vec(),
            delay,
            calls: self.provider_calls.clone(),
            delete_disposition: SecretProviderDeleteDisposition::Deleted,
            delete_calls: self.delete_calls.clone(),
        });
        BrokeredSecretMaterialResolver::new(
            Arc::new(FixtureAuthority { record }),
            Arc::new(FixtureUnsealer {
                reference: unsealed_reference,
                calls: self.unseal_calls.clone(),
            }),
            InstalledSecretProviderCatalog::new(vec![provider]).unwrap(),
            limits,
        )
        .unwrap()
    }
}

#[tokio::test]
async fn resolves_active_exact_binding_without_exposing_material_in_debug() {
    let fixture = Fixture::new();
    let pinned = policy(&fixture.version);
    let exact = exact(
        fixture.binding_id.clone(),
        fixture.provider_id.clone(),
        1,
        pinned.clone(),
    );
    let resolver = fixture.resolver(
        fixture.record(1, SecretBindingState::Active, pinned),
        fixture.reference.clone(),
        sha(&fixture.version),
        Duration::ZERO,
        SecretBrokerLimits::default(),
    );

    let resolved = resolver.resolve(&fixture.tenant_id, &exact).await.unwrap();
    assert_eq!(resolved.secret_binding_id, fixture.binding_id);
    assert_eq!(resolved.provider_id, fixture.provider_id);
    assert_eq!(resolved.binding_generation, 1);
    assert_eq!(
        resolved.opaque_version_identity_digest,
        sha(&fixture.version)
    );
    assert!(!format!("{resolved:?}").contains("canary-secret-material"));
    assert_eq!(fixture.unseal_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.provider_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn revoked_and_pinned_generation_drift_fail_before_unseal() {
    for (generation, state, expected) in [
        (
            1,
            SecretBindingState::Revoked,
            SecretMaterialResolutionError::Revoked,
        ),
        (
            2,
            SecretBindingState::Active,
            SecretMaterialResolutionError::InvalidEvidence,
        ),
    ] {
        let fixture = Fixture::new();
        let pinned = policy(&fixture.version);
        let exact = exact(
            fixture.binding_id.clone(),
            fixture.provider_id.clone(),
            1,
            pinned.clone(),
        );
        let resolver = fixture.resolver(
            fixture.record(generation, state, pinned),
            fixture.reference.clone(),
            sha(&fixture.version),
            Duration::ZERO,
            SecretBrokerLimits::default(),
        );
        assert_eq!(
            resolver
                .resolve(&fixture.tenant_id, &exact)
                .await
                .unwrap_err(),
            expected
        );
        assert_eq!(fixture.unseal_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.provider_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn tampered_reference_and_provider_version_evidence_fail_closed() {
    let fixture = Fixture::new();
    let pinned = policy(&fixture.version);
    let exact = exact(
        fixture.binding_id.clone(),
        fixture.provider_id.clone(),
        1,
        pinned.clone(),
    );
    let bad_reference = fixture.resolver(
        fixture.record(1, SecretBindingState::Active, pinned.clone()),
        b"vault:path/attacker".to_vec(),
        sha(&fixture.version),
        Duration::ZERO,
        SecretBrokerLimits::default(),
    );
    assert_eq!(
        bad_reference
            .resolve(&fixture.tenant_id, &exact)
            .await
            .unwrap_err(),
        SecretMaterialResolutionError::InvalidEvidence
    );
    assert_eq!(fixture.provider_calls.load(Ordering::SeqCst), 0);

    let bad_version = fixture.resolver(
        fixture.record(1, SecretBindingState::Active, pinned),
        fixture.reference.clone(),
        sha(b"wrong-provider-version"),
        Duration::ZERO,
        SecretBrokerLimits::default(),
    );
    assert_eq!(
        bad_version
            .resolve(&fixture.tenant_id, &exact)
            .await
            .unwrap_err(),
        SecretMaterialResolutionError::InvalidEvidence
    );
}

#[tokio::test]
async fn follow_rotation_accepts_newer_generation_and_preserves_actual_version_evidence() {
    let fixture = Fixture::new();
    let follow = SecretResolutionPolicy::FollowProviderRotation {
        rotation_policy_revision_id: id("prev", 4),
    };
    let exact = exact(
        fixture.binding_id.clone(),
        fixture.provider_id.clone(),
        1,
        follow.clone(),
    );
    let actual_version = sha(b"provider-version-8");
    let resolver = fixture.resolver(
        fixture.record(2, SecretBindingState::Active, follow),
        fixture.reference.clone(),
        actual_version.clone(),
        Duration::ZERO,
        SecretBrokerLimits::default(),
    );

    let resolved = resolver.resolve(&fixture.tenant_id, &exact).await.unwrap();
    assert_eq!(resolved.binding_generation, 2);
    assert_eq!(resolved.opaque_version_identity_digest, actual_version);
}

#[tokio::test]
async fn timeout_is_unavailable_and_catalog_rejects_duplicate_provider() {
    let fixture = Fixture::new();
    let pinned = policy(&fixture.version);
    let exact = exact(
        fixture.binding_id.clone(),
        fixture.provider_id.clone(),
        1,
        pinned.clone(),
    );
    let resolver = fixture.resolver(
        fixture.record(1, SecretBindingState::Active, pinned),
        fixture.reference.clone(),
        sha(&fixture.version),
        Duration::from_millis(20),
        SecretBrokerLimits {
            resolution_timeout: Duration::from_millis(1),
            ..SecretBrokerLimits::default()
        },
    );
    assert_eq!(
        resolver
            .resolve(&fixture.tenant_id, &exact)
            .await
            .unwrap_err(),
        SecretMaterialResolutionError::Unavailable
    );

    let provider = || -> Arc<dyn InstalledSecretProvider> {
        Arc::new(FixtureProvider {
            provider_id: fixture.provider_id.clone(),
            expected_reference: fixture.reference.clone(),
            version: sha(&fixture.version),
            material: b"material".to_vec(),
            delay: Duration::ZERO,
            calls: fixture.provider_calls.clone(),
            delete_disposition: SecretProviderDeleteDisposition::Deleted,
            delete_calls: fixture.delete_calls.clone(),
        })
    };
    assert!(matches!(
        InstalledSecretProviderCatalog::new(vec![provider(), provider()]),
        Err(SecretBrokerConfigurationError::DuplicateProvider)
    ));
}

#[tokio::test]
async fn exact_delete_rechecks_current_gate_reference_and_pinned_version() {
    let fixture = Fixture::new();
    let pinned = policy(&fixture.version);
    let pinned_exact = exact(
        fixture.binding_id.clone(),
        fixture.provider_id.clone(),
        1,
        pinned.clone(),
    );
    let resolver = fixture.resolver(
        fixture.record(1, SecretBindingState::Active, pinned),
        fixture.reference.clone(),
        sha(&fixture.version),
        Duration::ZERO,
        SecretBrokerLimits::default(),
    );
    assert_eq!(
        resolver
            .delete_exact_version(&fixture.tenant_id, &pinned_exact)
            .await
            .unwrap(),
        ExactSecretVersionDeleteDisposition::Deleted
    );
    assert_eq!(fixture.delete_calls.load(Ordering::SeqCst), 1);

    let revoked_fixture = Fixture::new();
    let pinned = policy(&revoked_fixture.version);
    let revoked_exact = exact(
        revoked_fixture.binding_id.clone(),
        revoked_fixture.provider_id.clone(),
        1,
        pinned.clone(),
    );
    let revoked = revoked_fixture.resolver(
        revoked_fixture.record(1, SecretBindingState::Revoked, pinned),
        revoked_fixture.reference.clone(),
        sha(&revoked_fixture.version),
        Duration::ZERO,
        SecretBrokerLimits::default(),
    );
    assert_eq!(
        revoked
            .delete_exact_version(&revoked_fixture.tenant_id, &revoked_exact)
            .await
            .unwrap_err(),
        ExactSecretVersionDeleteError::Rejected
    );
    assert_eq!(revoked_fixture.delete_calls.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct PreparedFixtureAuthority {
    calls: Arc<AtomicUsize>,
    failures_remaining: Arc<AtomicUsize>,
    winner: Arc<Mutex<Option<(Sha256Digest, ExactSecretBindingRef)>>>,
}

#[async_trait]
impl PreparedSecretBindingAuthority for PreparedFixtureAuthority {
    async fn register_prepared(
        &self,
        command: RegisterPreparedSecretBinding,
    ) -> Result<PreparedSecretBindingRegistrationOutcome, PreparedSecretBindingRegistrationError>
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self
            .failures_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_sub(1)
            })
            .is_ok()
        {
            return Err(PreparedSecretBindingRegistrationError::TemporarilyUnavailable);
        }
        command
            .validate_at(Utc::now())
            .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?;
        let exact_binding = command
            .exact_binding()
            .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?;
        let mut winner = self.winner.lock().unwrap();
        match winner.as_ref() {
            Some((digest, exact))
                if digest == &command.preparation_digest && exact == &exact_binding =>
            {
                Ok(PreparedSecretBindingRegistrationOutcome {
                    disposition: PreparedSecretBindingRegistrationDisposition::Replayed,
                    exact_binding,
                })
            }
            Some(_) => Err(PreparedSecretBindingRegistrationError::Rejected),
            None => {
                *winner = Some((command.preparation_digest, exact_binding.clone()));
                Ok(PreparedSecretBindingRegistrationOutcome {
                    disposition: PreparedSecretBindingRegistrationDisposition::Applied,
                    exact_binding,
                })
            }
        }
    }
}

struct PreparedFixtureSealer {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl SecretReferenceSealer for PreparedFixtureSealer {
    async fn seal(
        &self,
        tenant_id: &ResourceId,
        secret_binding_id: &ResourceId,
        provider_id: &ResourceId,
        binding_generation: u64,
        reference: &OpaqueSecretReference,
    ) -> Result<SealedSecretReference, SecretReferenceSealError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if tenant_id.kind() != ResourceKind::Tenant
            || secret_binding_id.kind() != ResourceKind::SecretBinding
            || provider_id.kind() != ResourceKind::SecretProvider
            || binding_generation != 1
        {
            return Err(SecretReferenceSealError::Rejected);
        }
        Ok(SealedSecretReference {
            encrypted_reference: EncryptedOpaqueReference::new(b"sealed-reference".to_vec())
                .unwrap(),
            key_id: "kms://fixture/key/1".to_owned(),
            reference_digest: sha(reference.expose()),
        })
    }
}

struct PreparedFixtureProvider {
    provider_id: ResourceId,
    binding_id: ResourceId,
    reference: Vec<u8>,
    version: Sha256Digest,
    calls: Arc<AtomicUsize>,
    winner: Mutex<Option<PreparedTransientSnapshot>>,
    drift_provider: bool,
}

#[derive(Clone)]
struct PreparedTransientSnapshot {
    tenant_id: ResourceId,
    task_id: ResourceId,
    authorization_binding_id: ResourceId,
    mcp_deployment: ExactDeploymentRef,
    preparation_digest: Sha256Digest,
    callback_binding_digest: Sha256Digest,
    expires_at: DateTime<Utc>,
    state: Vec<u8>,
    nonce: Vec<u8>,
    verifier: Vec<u8>,
}

#[derive(Clone)]
struct PreparedTokenSnapshot {
    verified: VerifiedMcpOAuthToken,
}

struct PreparedTokenFixtureProvider {
    provider_id: ResourceId,
    binding_id: ResourceId,
    reference: Vec<u8>,
    version: Sha256Digest,
    load_calls: Arc<AtomicUsize>,
    store_calls: Arc<AtomicUsize>,
    winner: Mutex<Option<PreparedTokenSnapshot>>,
}

impl PreparedTokenFixtureProvider {
    fn stored(
        &self,
        preparation: &McpOAuthTokenPreparation,
        snapshot: PreparedTokenSnapshot,
    ) -> ProviderStoredMcpOAuthTokenSecret {
        let exact = ExactSecretBindingRef::build(
            self.binding_id.clone(),
            1,
            self.provider_id.clone(),
            preparation.token_credential_purpose.clone(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: self.version.clone(),
            },
        )
        .unwrap();
        ProviderStoredMcpOAuthTokenSecret {
            stored: StoredMcpOAuthTokenSecret {
                schema_version: 1,
                preparation_digest: preparation.preparation_digest.clone(),
                token_secret_binding: exact,
                granted_scopes: snapshot.verified.granted_scopes,
                audience_identity_digest: snapshot.verified.audience_identity_digest,
                issuer_identity_digest: snapshot.verified.issuer_identity_digest,
                subject_identity_digest: snapshot.verified.subject_identity_digest,
                verification_evidence_digest: snapshot.verified.verification_evidence_digest,
                expires_at: snapshot.verified.expires_at,
                storage_evidence_digest: sha(b"token-storage-evidence"),
            },
            prepared_secret: ProviderPreparedSecretVersion {
                secret_binding_id: self.binding_id.clone(),
                provider_id: self.provider_id.clone(),
                opaque_reference: OpaqueSecretReference::new(self.reference.clone()).unwrap(),
                opaque_version_identity_digest: self.version.clone(),
                storage_evidence_digest: sha(b"token-storage-evidence"),
            },
        }
    }
}

#[async_trait]
impl InstalledSecretProvider for PreparedTokenFixtureProvider {
    fn provider_id(&self) -> &ResourceId {
        &self.provider_id
    }

    async fn resolve(
        &self,
        _tenant_id: &ResourceId,
        _reference: &OpaqueSecretReference,
        _policy: &SecretResolutionPolicy,
    ) -> Result<ProviderSecretMaterial, SecretProviderResolveError> {
        Err(SecretProviderResolveError::Rejected)
    }

    async fn delete_exact(
        &self,
        _tenant_id: &ResourceId,
        _reference: &OpaqueSecretReference,
        _policy: &SecretResolutionPolicy,
    ) -> Result<SecretProviderDeleteDisposition, SecretProviderDeleteError> {
        Err(SecretProviderDeleteError::Rejected)
    }

    async fn load_prepared_mcp_oauth_token(
        &self,
        preparation: &McpOAuthTokenPreparation,
    ) -> Result<Option<ProviderStoredMcpOAuthTokenSecret>, SecretProviderPrepareError> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .winner
            .lock()
            .unwrap()
            .clone()
            .map(|snapshot| self.stored(preparation, snapshot)))
    }

    async fn prepare_or_load_mcp_oauth_token(
        &self,
        preparation: &McpOAuthTokenPreparation,
        _tokens: &McpOAuthTokenSet,
        verified: &VerifiedMcpOAuthToken,
    ) -> Result<ProviderStoredMcpOAuthTokenSecret, SecretProviderPrepareError> {
        self.store_calls.fetch_add(1, Ordering::SeqCst);
        let snapshot = self
            .winner
            .lock()
            .unwrap()
            .get_or_insert_with(|| PreparedTokenSnapshot {
                verified: verified.clone(),
            })
            .clone();
        Ok(self.stored(preparation, snapshot))
    }
}

fn token_preparation(now: DateTime<Utc>, provider_id: ResourceId) -> McpOAuthTokenPreparation {
    let tenant_id = id("ten", 0x940);
    let task_id = id("int", 0x941);
    let authorization_binding_id = id("mab", 0x942);
    let mcp_deployment =
        ExactDeploymentRef::new(id("mcdep", 0x943), sha(b"token-deployment")).unwrap();
    let state_digest = sha(b"state");
    let authorization_code_digest = sha(b"authorization-code");
    let token_credential_purpose: SecretPurpose = "mcp.oauth.token".parse().unwrap();
    let requested_scopes = vec!["openid".to_owned(), "profile".to_owned()];
    let audience_identity_digest = sha(b"audience");
    let issuer_identity_digest = sha(b"issuer");
    let expires_at = now + ChronoDuration::minutes(10);
    let preparation_digest: Sha256Digest = canonical_digest(&serde_json::json!({
        "authorization_binding_id": authorization_binding_id,
        "authorization_code_digest": authorization_code_digest,
        "domain": "mcp_oauth_token_preparation_v1",
        "expires_at": expires_at,
        "mcp_deployment": mcp_deployment,
        "schema_version": 1,
        "state_digest": state_digest,
        "task_generation": 1,
        "task_id": task_id,
        "task_version": 1,
        "tenant_id": tenant_id,
        "token_credential_purpose": token_credential_purpose,
        "token_secret_provider_id": provider_id,
        "requested_scopes": requested_scopes,
        "audience_identity_digest": audience_identity_digest,
        "issuer_identity_digest": issuer_identity_digest,
    }))
    .unwrap()
    .parse()
    .unwrap();
    McpOAuthTokenPreparation {
        schema_version: 1,
        tenant_id,
        task_id,
        task_generation: 1,
        task_version: 1,
        authorization_binding_id,
        mcp_deployment,
        state_digest,
        authorization_code_digest,
        token_credential_purpose,
        token_secret_provider_id: provider_id,
        requested_scopes,
        audience_identity_digest,
        issuer_identity_digest,
        expires_at,
        preparation_digest,
    }
}

impl PreparedFixtureProvider {
    fn stored(
        &self,
        snapshot: PreparedTransientSnapshot,
    ) -> ProviderStoredMcpOAuthTransientSecretBundle {
        let provider_id = if self.drift_provider {
            id("spr", 0x901)
        } else {
            self.provider_id.clone()
        };
        let exact = ExactSecretBindingRef::build(
            self.binding_id.clone(),
            1,
            provider_id.clone(),
            MCP_OAUTH_PKCE_SECRET_PURPOSE.parse().unwrap(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: self.version.clone(),
            },
        )
        .unwrap();
        ProviderStoredMcpOAuthTransientSecretBundle {
            stored: StoredMcpOAuthTransientSecretBundle {
                schema_version: 1,
                tenant_id: snapshot.tenant_id,
                task_id: snapshot.task_id,
                authorization_binding_id: snapshot.authorization_binding_id,
                mcp_deployment: snapshot.mcp_deployment,
                pkce_secret_provider_id: self.provider_id.clone(),
                preparation_digest: snapshot.preparation_digest,
                callback_binding_digest: snapshot.callback_binding_digest,
                expires_at: snapshot.expires_at,
                state: SensitiveOAuthValue::from_decoded(snapshot.state, MAX_MCP_OAUTH_STATE_BYTES)
                    .unwrap(),
                nonce: SensitiveMcpOAuthNonce::new(snapshot.nonce).unwrap(),
                pkce_verifier: insight_platform_egress::SensitiveMcpOAuthPkceVerifier::new(
                    snapshot.verifier,
                )
                .unwrap(),
                pkce_secret_binding: exact,
                storage_evidence_digest: sha(b"provider-storage-evidence"),
            },
            prepared_secret: ProviderPreparedSecretVersion {
                secret_binding_id: self.binding_id.clone(),
                provider_id,
                opaque_reference: OpaqueSecretReference::new(self.reference.clone()).unwrap(),
                opaque_version_identity_digest: self.version.clone(),
                storage_evidence_digest: sha(b"provider-storage-evidence"),
            },
        }
    }
}

#[async_trait]
impl InstalledSecretProvider for PreparedFixtureProvider {
    fn provider_id(&self) -> &ResourceId {
        &self.provider_id
    }

    async fn resolve(
        &self,
        _tenant_id: &ResourceId,
        _reference: &OpaqueSecretReference,
        _policy: &SecretResolutionPolicy,
    ) -> Result<ProviderSecretMaterial, SecretProviderResolveError> {
        Err(SecretProviderResolveError::Rejected)
    }

    async fn delete_exact(
        &self,
        _tenant_id: &ResourceId,
        _reference: &OpaqueSecretReference,
        _policy: &SecretResolutionPolicy,
    ) -> Result<SecretProviderDeleteDisposition, SecretProviderDeleteError> {
        Err(SecretProviderDeleteError::Rejected)
    }

    async fn prepare_or_load_mcp_oauth_transient(
        &self,
        candidate: NewMcpOAuthTransientSecretBundle,
    ) -> Result<ProviderStoredMcpOAuthTransientSecretBundle, SecretProviderPrepareError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut winner = self.winner.lock().unwrap();
        let snapshot = winner
            .get_or_insert_with(|| PreparedTransientSnapshot {
                tenant_id: candidate.tenant_id,
                task_id: candidate.task_id,
                authorization_binding_id: candidate.authorization_binding_id,
                mcp_deployment: candidate.mcp_deployment,
                preparation_digest: candidate.preparation_digest,
                callback_binding_digest: candidate.callback_binding_digest,
                expires_at: candidate.expires_at,
                state: candidate.state.as_bytes().to_vec(),
                nonce: candidate.nonce.as_bytes().to_vec(),
                verifier: candidate.pkce_verifier.expose().to_vec(),
            })
            .clone();
        Ok(self.stored(snapshot))
    }
}

fn prepared_candidate(
    now: DateTime<Utc>,
    provider_id: ResourceId,
) -> NewMcpOAuthTransientSecretBundle {
    NewMcpOAuthTransientSecretBundle {
        schema_version: 1,
        tenant_id: id("ten", 0x910),
        task_id: id("int", 0x911),
        authorization_binding_id: id("mab", 0x912),
        mcp_deployment: ExactDeploymentRef::new(id("mcdep", 0x913), sha(b"deployment")).unwrap(),
        pkce_secret_provider_id: provider_id,
        preparation_digest: sha(b"preparation"),
        callback_binding_digest: sha(b"callback"),
        expires_at: now + ChronoDuration::minutes(10),
        state: SensitiveOAuthValue::from_decoded(
            b"opaque-prepared-state".to_vec(),
            MAX_MCP_OAUTH_STATE_BYTES,
        )
        .unwrap(),
        nonce: SensitiveMcpOAuthNonce::new(b"n".repeat(43)).unwrap(),
        pkce_verifier: insight_platform_egress::SensitiveMcpOAuthPkceVerifier::new(b"v".repeat(43))
            .unwrap(),
    }
}

#[tokio::test]
async fn prepared_external_winner_replays_and_repairs_database_registration() {
    let now = Utc::now();
    let provider_id = id("spr", 0x920);
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let registration_calls = Arc::new(AtomicUsize::new(0));
    let sealer_calls = Arc::new(AtomicUsize::new(0));
    let authority = Arc::new(PreparedFixtureAuthority {
        calls: registration_calls.clone(),
        failures_remaining: Arc::new(AtomicUsize::new(1)),
        winner: Arc::new(Mutex::new(None)),
    });
    let provider: Arc<dyn InstalledSecretProvider> = Arc::new(PreparedFixtureProvider {
        provider_id: provider_id.clone(),
        binding_id: id("sbd", 0x921),
        reference: b"provider://oauth/pkce/version/1".to_vec(),
        version: sha(b"version-1"),
        calls: provider_calls.clone(),
        winner: Mutex::new(None),
        drift_provider: false,
    });
    let store = BrokeredMcpOAuthSecretStore::new(
        authority,
        Arc::new(PreparedFixtureSealer {
            calls: sealer_calls.clone(),
        }),
        InstalledSecretProviderCatalog::new(vec![provider]).unwrap(),
        id("prn", 0x922),
        SecretBrokerLimits::default(),
    )
    .unwrap();

    assert_eq!(
        store
            .prepare_or_load(prepared_candidate(now, provider_id.clone()))
            .await
            .unwrap_err(),
        McpOAuthTransientSecretStoreError::TemporarilyUnavailable
    );
    let repaired = store
        .prepare_or_load(prepared_candidate(now, provider_id))
        .await
        .unwrap();
    assert_eq!(
        repaired.pkce_secret_binding.secret_binding_id,
        id("sbd", 0x921)
    );
    assert_eq!(provider_calls.load(Ordering::SeqCst), 2);
    assert_eq!(sealer_calls.load(Ordering::SeqCst), 2);
    assert_eq!(registration_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn prepared_provider_drift_is_rejected_before_seal_or_database_registration() {
    let now = Utc::now();
    let provider_id = id("spr", 0x930);
    let registration_calls = Arc::new(AtomicUsize::new(0));
    let sealer_calls = Arc::new(AtomicUsize::new(0));
    let provider: Arc<dyn InstalledSecretProvider> = Arc::new(PreparedFixtureProvider {
        provider_id: provider_id.clone(),
        binding_id: id("sbd", 0x931),
        reference: b"provider://oauth/pkce/version/1".to_vec(),
        version: sha(b"version-1"),
        calls: Arc::new(AtomicUsize::new(0)),
        winner: Mutex::new(None),
        drift_provider: true,
    });
    let store = BrokeredMcpOAuthSecretStore::new(
        Arc::new(PreparedFixtureAuthority {
            calls: registration_calls.clone(),
            failures_remaining: Arc::new(AtomicUsize::new(0)),
            winner: Arc::new(Mutex::new(None)),
        }),
        Arc::new(PreparedFixtureSealer {
            calls: sealer_calls.clone(),
        }),
        InstalledSecretProviderCatalog::new(vec![provider]).unwrap(),
        id("prn", 0x932),
        SecretBrokerLimits::default(),
    )
    .unwrap();

    assert_eq!(
        store
            .prepare_or_load(prepared_candidate(now, provider_id))
            .await
            .unwrap_err(),
        McpOAuthTransientSecretStoreError::Rejected
    );
    assert_eq!(sealer_calls.load(Ordering::SeqCst), 0);
    assert_eq!(registration_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn prepared_token_winner_load_repairs_database_without_reusing_authorization_code() {
    let now = Utc::now();
    let provider_id = id("spr", 0x950);
    let load_calls = Arc::new(AtomicUsize::new(0));
    let store_calls = Arc::new(AtomicUsize::new(0));
    let registration_calls = Arc::new(AtomicUsize::new(0));
    let sealer_calls = Arc::new(AtomicUsize::new(0));
    let provider: Arc<dyn InstalledSecretProvider> = Arc::new(PreparedTokenFixtureProvider {
        provider_id: provider_id.clone(),
        binding_id: id("sbd", 0x951),
        reference: b"provider://oauth/token/version/1".to_vec(),
        version: sha(b"token-version-1"),
        load_calls: load_calls.clone(),
        store_calls: store_calls.clone(),
        winner: Mutex::new(None),
    });
    let store = BrokeredMcpOAuthSecretStore::new(
        Arc::new(PreparedFixtureAuthority {
            calls: registration_calls.clone(),
            failures_remaining: Arc::new(AtomicUsize::new(1)),
            winner: Arc::new(Mutex::new(None)),
        }),
        Arc::new(PreparedFixtureSealer {
            calls: sealer_calls.clone(),
        }),
        InstalledSecretProviderCatalog::new(vec![provider]).unwrap(),
        id("prn", 0x952),
        SecretBrokerLimits::default(),
    )
    .unwrap();
    let preparation = token_preparation(now, provider_id);
    let tokens: McpOAuthTokenSet = serde_json::from_value(serde_json::json!({
        "access_token": "access-token-canary",
        "refresh_token": "refresh-token-canary",
        "token_type": "Bearer",
        "expires_in": 600,
        "scope": "openid profile"
    }))
    .unwrap();
    let verified = VerifiedMcpOAuthToken {
        granted_scopes: vec!["openid".to_owned(), "profile".to_owned()],
        audience_identity_digest: preparation.audience_identity_digest.clone(),
        issuer_identity_digest: preparation.issuer_identity_digest.clone(),
        subject_identity_digest: sha(b"subject"),
        verification_evidence_digest: sha(b"verification"),
        expires_at: now + ChronoDuration::minutes(5),
        nonce_verified: true,
    };

    assert_eq!(
        store
            .store_prepared(&preparation, &tokens, &verified, now)
            .await
            .unwrap_err(),
        McpOAuthTokenStoreError::TemporarilyUnavailable
    );
    let repaired = store
        .load_prepared(&preparation, now)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        repaired.token_secret_binding.secret_binding_id,
        id("sbd", 0x951)
    );
    assert_eq!(store_calls.load(Ordering::SeqCst), 1);
    assert_eq!(load_calls.load(Ordering::SeqCst), 1);
    assert_eq!(sealer_calls.load(Ordering::SeqCst), 2);
    assert_eq!(registration_calls.load(Ordering::SeqCst), 2);
    assert!(!format!("{tokens:?}").contains("access-token-canary"));
}
