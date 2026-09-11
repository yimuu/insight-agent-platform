use super::*;
use insight_platform_contracts::{
    ModelCredentialImportAuthorizationV1, ModelCredentialImportError,
    ModelCredentialImportIdentityV1, ModelCredentialImportPermitV1, SensitiveModelApiKey,
};
use insight_platform_security::{ModelCredentialImportAuthority, ModelCredentialImporter};
struct Authorization {
    rejected: bool,
    lifetime_ms: i64,
}
#[async_trait]
impl ModelCredentialImportAuthority for Authorization {
    async fn authorize_model_credential_import(
        &self,
        request: &ModelCredentialImportAuthorizationV1,
    ) -> Result<ModelCredentialImportPermitV1, ModelCredentialImportError> {
        if self.rejected {
            return Err(ModelCredentialImportError::Rejected);
        }
        Ok(ModelCredentialImportPermitV1 {
            schema_version: 1,
            request_digest: request.canonical_digest()?,
            valid_until: (Utc::now() + ChronoDuration::milliseconds(self.lifetime_ms))
                .min(request.deadline),
        })
    }
}
struct ImportProvider {
    provider_id: ResourceId,
    calls: Arc<AtomicUsize>,
    delay: Duration,
    winner: Mutex<Option<(ModelCredentialImportIdentityV1, Vec<u8>)>>,
}
#[async_trait]
impl InstalledSecretProvider for ImportProvider {
    fn provider_id(&self) -> &ResourceId {
        &self.provider_id
    }
    async fn resolve(
        &self,
        _tenant: &ResourceId,
        _reference: &OpaqueSecretReference,
        _policy: &SecretResolutionPolicy,
    ) -> Result<ProviderSecretMaterial, SecretProviderResolveError> {
        Err(SecretProviderResolveError::Rejected)
    }
    async fn delete_exact(
        &self,
        _tenant: &ResourceId,
        _reference: &OpaqueSecretReference,
        _policy: &SecretResolutionPolicy,
    ) -> Result<SecretProviderDeleteDisposition, SecretProviderDeleteError> {
        Err(SecretProviderDeleteError::Rejected)
    }
    async fn prepare_or_load_model_credential(
        &self,
        request: &ModelCredentialImportAuthorizationV1,
        permit: &ModelCredentialImportPermitV1,
        key: &SensitiveModelApiKey,
    ) -> Result<ProviderPreparedSecretVersion, SecretProviderPrepareError> {
        assert!(permit.validate_for(request, Utc::now()));
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let mut winner = self.winner.lock().unwrap();
        if let Some((identity, stored)) = winner.as_ref() {
            if identity != &request.identity || stored != key.expose() {
                return Err(SecretProviderPrepareError::Rejected);
            }
        } else {
            *winner = Some((request.identity.clone(), key.expose().to_vec()));
        }
        Ok(ProviderPreparedSecretVersion {
            secret_binding_id: request.identity.secret_binding_id().unwrap(),
            provider_id: self.provider_id.clone(),
            opaque_reference: OpaqueSecretReference::new(
                b"fixture/exact/prepared/version".to_vec(),
            )
            .unwrap(),
            opaque_version_identity_digest: sha(b"fixed-import-version"),
            storage_evidence_digest: sha(b"fixed-storage-evidence"),
        })
    }
}
fn import_request() -> ModelCredentialImportAuthorizationV1 {
    ModelCredentialImportAuthorizationV1 {
        schema_version: 1,
        deadline: Utc::now() + ChronoDuration::seconds(25),
        identity: ModelCredentialImportIdentityV1 {
            schema_version: 1,
            operation_id: "a8376371-3d45-4ef6-8c8c-eb1a895fa99c".parse().unwrap(),
            tenant_id: id("ten", 0x991),
            principal_id: id("prn", 0x992),
            principal_kind: PrincipalKind::TenantAdmin,
            provider_id: id("spr", 0x993),
            purpose: "model_api_key".parse().unwrap(),
        },
    }
}
fn key(value: &[u8]) -> SensitiveModelApiKey {
    SensitiveModelApiKey::new(value.to_vec()).unwrap()
}
#[tokio::test]
async fn credential_import_authorization_and_expiry_precede_provider_and_kms() {
    for (rejected, lifetime, delay, expected_provider) in
        [(true, 25000, 0, 0), (false, -1, 0, 0), (false, 5, 25, 1)]
    {
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let seal_calls = Arc::new(AtomicUsize::new(0));
        let registration_calls = Arc::new(AtomicUsize::new(0));
        let request = import_request();
        let store = BrokeredPreparedSecretStore::new(
            Arc::new(PreparedFixtureAuthority {
                calls: registration_calls.clone(),
                failures_remaining: Arc::new(AtomicUsize::new(0)),
                winner: Arc::new(Mutex::new(None)),
            }),
            Arc::new(PreparedFixtureSealer {
                calls: seal_calls.clone(),
            }),
            InstalledSecretProviderCatalog::new(vec![Arc::new(ImportProvider {
                provider_id: request.identity.provider_id.clone(),
                calls: provider_calls.clone(),
                delay: Duration::from_millis(delay),
                winner: Mutex::new(None),
            })])
            .unwrap(),
            id("prn", 0x994),
            SecretBrokerLimits::default(),
        )
        .unwrap()
        .with_model_credential_authority(Arc::new(Authorization {
            rejected,
            lifetime_ms: lifetime,
        }));
        assert_eq!(
            store
                .import_model_credential(request, key(b"credential-canary"))
                .await
                .unwrap_err(),
            ModelCredentialImportError::Rejected
        );
        assert_eq!(provider_calls.load(Ordering::SeqCst), expected_provider);
        assert_eq!(seal_calls.load(Ordering::SeqCst), 0);
        assert_eq!(registration_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.capacity_snapshot().available,
            store.capacity_snapshot().maximum_in_flight
        );
    }
}
#[tokio::test]
async fn credential_import_unknown_commit_retries_same_identity_and_rejects_new_input() {
    let request = import_request();
    let calls = Arc::new(AtomicUsize::new(0));
    let seal_calls = Arc::new(AtomicUsize::new(0));
    let registration_calls = Arc::new(AtomicUsize::new(0));
    let store = BrokeredPreparedSecretStore::new(
        Arc::new(PreparedFixtureAuthority {
            calls: registration_calls.clone(),
            failures_remaining: Arc::new(AtomicUsize::new(1)),
            winner: Arc::new(Mutex::new(None)),
        }),
        Arc::new(PreparedFixtureSealer {
            calls: seal_calls.clone(),
        }),
        InstalledSecretProviderCatalog::new(vec![Arc::new(ImportProvider {
            provider_id: request.identity.provider_id.clone(),
            calls: calls.clone(),
            delay: Duration::ZERO,
            winner: Mutex::new(None),
        })])
        .unwrap(),
        id("prn", 0x994),
        SecretBrokerLimits::default(),
    )
    .unwrap()
    .with_model_credential_authority(Arc::new(Authorization {
        rejected: false,
        lifetime_ms: 25000,
    }));
    assert_eq!(
        store
            .import_model_credential(request.clone(), key(b"credential-canary"))
            .await
            .unwrap_err(),
        ModelCredentialImportError::OutcomeUnknown
    );
    let (left, right) = tokio::join!(
        store.import_model_credential(request.clone(), key(b"credential-canary")),
        store.import_model_credential(request.clone(), key(b"credential-canary"))
    );
    assert_eq!(left.unwrap(), right.unwrap());
    let before = seal_calls.load(Ordering::SeqCst);
    assert_eq!(
        store
            .import_model_credential(request, key(b"different-input"))
            .await
            .unwrap_err(),
        ModelCredentialImportError::Rejected
    );
    assert_eq!(seal_calls.load(Ordering::SeqCst), before);
    assert_eq!(registration_calls.load(Ordering::SeqCst), 3);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}
