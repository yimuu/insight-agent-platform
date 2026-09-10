//! Opt-in physical AWS-compatible boundary proof. Security is a fixture here; the separate real
//! PostgreSQL target proves current authorization, transaction/replay and restricted SQL grants.
use super::*;
use crate::{BrokeredPreparedSecretStore, SecretBrokerLimits};
use insight_platform_contracts::{
    ModelCredentialImportAuthorizationV1, ModelCredentialImportError as Failure,
    ModelCredentialImportIdentityV1, ModelCredentialImportPermitV1, PrincipalKind,
    SecretBindingPayload, SecretBindingState, SensitiveModelApiKey,
};
use insight_platform_security::{
    ModelCredentialImportAuthority, ModelCredentialImporter, PreparedSecretBindingAuthority,
    PreparedSecretBindingRegistrationDisposition, PreparedSecretBindingRegistrationError,
    PreparedSecretBindingRegistrationOutcome, RegisterPreparedSecretBinding,
};
use std::{io::Read, sync::Mutex};

#[derive(Default)]
struct Authority {
    record: Mutex<Option<SecretBindingResolutionRecord>>,
}
#[async_trait]
impl ModelCredentialImportAuthority for Authority {
    async fn authorize_model_credential_import(
        &self,
        request: &ModelCredentialImportAuthorizationV1,
    ) -> Result<ModelCredentialImportPermitV1, Failure> {
        if !request.validate_at(Utc::now()) {
            return Err(Failure::Rejected);
        }
        Ok(ModelCredentialImportPermitV1 {
            schema_version: 1,
            request_digest: request.canonical_digest()?,
            valid_until: request.deadline,
        })
    }
}
#[async_trait]
impl PreparedSecretBindingAuthority for Authority {
    async fn register_prepared(
        &self,
        command: RegisterPreparedSecretBinding,
    ) -> Result<PreparedSecretBindingRegistrationOutcome, PreparedSecretBindingRegistrationError>
    {
        command
            .validate_at(Utc::now())
            .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?;
        let binding = command
            .exact_binding()
            .map_err(|_| PreparedSecretBindingRegistrationError::Rejected)?;
        let mut record = self.record.lock().unwrap();
        let disposition = if let Some(current) = record.as_ref() {
            if current.secret_binding_id != command.secret_binding_id
                || current.reference_digest != command.reference_digest
                || current.payload.resolution_policy != binding.resolution_policy
            {
                return Err(PreparedSecretBindingRegistrationError::Rejected);
            }
            PreparedSecretBindingRegistrationDisposition::Replayed
        } else {
            *record = Some(SecretBindingResolutionRecord {
                tenant_id: command.audit.tenant_id,
                secret_binding_id: command.secret_binding_id,
                purpose: command.purpose,
                provider_id: command.provider_id.clone(),
                state: SecretBindingState::Active,
                generation: 1,
                encrypted_reference: command.encrypted_reference,
                key_id: command.key_id,
                reference_digest: command.reference_digest,
                payload: SecretBindingPayload {
                    provider_id: command.provider_id,
                    resolution_policy: binding.resolution_policy.clone(),
                },
            });
            PreparedSecretBindingRegistrationDisposition::Applied
        };
        Ok(PreparedSecretBindingRegistrationOutcome {
            disposition,
            exact_binding: binding,
        })
    }
}
fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
}
fn authorization(
    identity: ModelCredentialImportIdentityV1,
) -> ModelCredentialImportAuthorizationV1 {
    ModelCredentialImportAuthorizationV1 {
        schema_version: 1,
        identity,
        deadline: Utc::now() + chrono::Duration::seconds(25),
    }
}
fn key() -> SensitiveModelApiKey {
    SensitiveModelApiKey::new(b"synthetic-physical-import-proof-key".to_vec()).unwrap()
}

#[tokio::test]
#[ignore = "requires a dedicated HTTPS Secret Manager/KMS fixture catalog and standard test SDK credentials"]
async fn actual_model_credential_prepare_seal_resolve_and_exact_replay() {
    let path = std::env::var("PLATFORM_TEST_SECRET_CATALOG_PATH")
        .expect("dedicated test catalog path is required");
    assert!(std::path::Path::new(&path).is_absolute());
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .expect("test catalog is readable")
        .take(1_048_577)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 1_048_576);
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: 1_048_576,
            max_depth: 8,
            max_properties_per_object: 32,
            max_items_per_array: 1,
            max_string_bytes: 4096,
        },
    )
    .expect("strict test catalog");
    let config: AwsSecretProviderCatalogConfig =
        serde_json::from_value(value).expect("closed test catalog");
    assert_eq!(
        config.providers.len(),
        1,
        "fixture must own exactly one provider"
    );
    let provider_id = config.providers[0].provider_id.clone();
    let installed = AwsSecretProviderCatalog::install(config)
        .await
        .expect("physical catalog installs");
    installed
        .check_readiness()
        .await
        .expect("physical fixture is ready");
    let (sealer, unsealer, providers) = installed.into_components();
    let provider = providers.get(&provider_id).unwrap();
    let identity = ModelCredentialImportIdentityV1 {
        schema_version: 1,
        operation_id: Uuid::new_v4().to_string().parse().unwrap(),
        tenant_id: fresh(ResourceKind::Tenant),
        principal_id: fresh(ResourceKind::Principal),
        principal_kind: PrincipalKind::TenantAdmin,
        provider_id: provider_id.clone(),
        purpose: "model_api_key".parse().unwrap(),
    };
    let authority = Arc::new(Authority::default());
    let request = authorization(identity.clone());
    let permit = authority
        .authorize_model_credential_import(&request)
        .await
        .unwrap();
    // Keep the first actual external winner's opaque reference for exact cleanup, even if a later
    // assertion panics. This setup also proves physical CreateSecret, then the store proves readback.
    let prepared = provider
        .prepare_or_load_model_credential(&request, &permit, &key())
        .await
        .expect("physical preparation");
    let expected = exact_binding(
        prepared.secret_binding_id.clone(),
        provider_id,
        identity.purpose.clone(),
        prepared.opaque_version_identity_digest.clone(),
    )
    .unwrap();
    let store = Arc::new(
        BrokeredPreparedSecretStore::new(
            authority.clone(),
            sealer,
            providers,
            fresh(ResourceKind::Principal),
            SecretBrokerLimits {
                resolution_timeout: Duration::from_secs(25),
                ..SecretBrokerLimits::default()
            },
        )
        .unwrap()
        .with_model_credential_authority(authority.clone()),
    );
    let verification = tokio::spawn({
        let identity = identity.clone();
        let expected = expected.clone();
        let provider = provider.clone();
        async move {
            let (left, right) = tokio::join!(
                store.import_model_credential(authorization(identity.clone()), key()),
                store.import_model_credential(authorization(identity.clone()), key())
            );
            assert_eq!(left.unwrap(), expected);
            assert_eq!(right.unwrap(), expected);
            assert_eq!(
                store
                    .import_model_credential(
                        authorization(identity.clone()),
                        SensitiveModelApiKey::new(b"different-key".to_vec()).unwrap()
                    )
                    .await
                    .unwrap_err(),
                Failure::Rejected
            );
            let record = authority.record.lock().unwrap().clone().unwrap();
            let reference = unsealer.unseal(&record).await.expect("actual KMS unseal");
            let mut material = provider
                .resolve(&identity.tenant_id, &reference, &expected.resolution_policy)
                .await
                .expect("actual Secret Manager resolve")
                .into_material();
            let correct = material == key().expose();
            material.fill(0);
            assert!(correct, "resolved material must match the synthetic input");
            assert!(!format!("{record:?}").contains("synthetic-physical-import-proof-key"));
        }
    })
    .await;
    let cleanup = provider
        .delete_exact(
            &identity.tenant_id,
            &prepared.opaque_reference,
            &expected.resolution_policy,
        )
        .await;
    assert!(
        matches!(
            cleanup,
            Ok(crate::SecretProviderDeleteDisposition::Deleted
                | crate::SecretProviderDeleteDisposition::AlreadyAbsent)
        ),
        "exact physical cleanup must succeed"
    );
    assert!(
        verification.is_ok(),
        "physical import verification failed (cleanup completed)"
    );
}
