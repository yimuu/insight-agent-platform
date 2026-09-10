//! Explicit physical qualification; no PostgreSQL or current-authorization claim is made here.
//! The controller owns a dedicated Bao instance and destroys its volumes after this test.
use super::*;
use insight_platform_contracts::{
    parse_strict_json, JsonLimits, ModelCredentialImportIdentityV1, SecretBindingPayload,
};
use std::{io::Read, sync::Mutex};

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
}

fn config() -> OpenBaoSecretProviderConfigV1 {
    let path = std::env::var("PLATFORM_TEST_OPENBAO_SECRET_CATALOG_PATH")
        .expect("dedicated private OpenBao catalog path is required");
    assert!(std::path::Path::new(&path).is_absolute());
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .unwrap()
        .take(1_048_577)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 1_048_576);
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: 1_048_576,
            max_depth: 10,
            max_properties_per_object: 32,
            max_items_per_array: 8,
            max_string_bytes: 4096,
        },
    )
    .expect("strict physical catalog");
    let mut config: SecretProviderCatalogConfigV2 =
        serde_json::from_value(value).expect("closed physical catalog");
    config.validate().unwrap();
    assert_eq!(
        config.providers.len(),
        1,
        "exactly one dedicated fixture provider"
    );
    let SecretProviderConfig::OpenBaoKvV2(config) = config.providers.remove(0) else {
        panic!("OpenBao physical fixture required");
    };
    *config
}

type Cleanup = Arc<Mutex<Vec<(ResourceId, Arc<ProviderPreparedSecretVersion>)>>>;

fn remember(
    cleanup: &Cleanup,
    tenant: &ResourceId,
    prepared: ProviderPreparedSecretVersion,
) -> Arc<ProviderPreparedSecretVersion> {
    let prepared = Arc::new(prepared);
    cleanup
        .lock()
        .unwrap()
        .push((tenant.clone(), Arc::clone(&prepared)));
    prepared
}

async fn roundtrip(
    provider: &OpenBaoProvider,
    tenant: &ResourceId,
    prepared: &ProviderPreparedSecretVersion,
    purpose: SecretPurpose,
    expected: &[u8],
) {
    let sealed = provider
        .seal(
            tenant,
            &prepared.secret_binding_id,
            &prepared.provider_id,
            1,
            &prepared.opaque_reference,
        )
        .await
        .expect("actual Transit seal");
    let policy = SecretResolutionPolicy::Pinned {
        opaque_version_identity_digest: prepared.opaque_version_identity_digest.clone(),
    };
    let mut record = SecretBindingResolutionRecord {
        tenant_id: tenant.clone(),
        secret_binding_id: prepared.secret_binding_id.clone(),
        purpose,
        provider_id: prepared.provider_id.clone(),
        state: SecretBindingState::Active,
        generation: 1,
        encrypted_reference: sealed.encrypted_reference,
        key_id: sealed.key_id,
        reference_digest: sealed.reference_digest,
        payload: SecretBindingPayload {
            provider_id: prepared.provider_id.clone(),
            resolution_policy: policy.clone(),
        },
    };
    let reference = provider
        .unseal(&record)
        .await
        .expect("actual Transit unseal");
    let mut material = provider
        .resolve(tenant, &reference, &policy)
        .await
        .expect("actual exact KV resolve")
        .into_material();
    let matches = material == expected;
    material.fill(0);
    assert!(matches, "physical material differs from synthetic input");
    record.generation = 2;
    assert!(
        provider.unseal(&record).await.is_err(),
        "AEAD must reject changed generation"
    );
    record.generation = 1;
    record.tenant_id = fresh(ResourceKind::Tenant);
    assert!(
        provider.unseal(&record).await.is_err(),
        "AEAD must reject another tenant"
    );
}

#[tokio::test]
#[ignore = "requires an exclusive, persistent, HTTPS/mTLS OpenBao fixture and explicit typed catalog"]
async fn actual_model_and_mcp_versions_seal_resolve_replay_and_destroy_exactly() {
    let config = config();
    let provider = Arc::new(
        OpenBaoProvider::install(
            config.clone(),
            Arc::new(NoopSecretExternalDependencyObserver),
        )
        .unwrap(),
    );
    provider
        .check_readiness()
        .await
        .expect("actual provider readiness");
    let cleanup: Cleanup = Arc::new(Mutex::new(Vec::new()));
    let verification = tokio::spawn({
        let provider = Arc::clone(&provider);
        let cleanup = Arc::clone(&cleanup);
        let config = config.clone();
        async move {
            let request = ModelCredentialImportAuthorizationV1 {
                schema_version: 1,
                deadline: Utc::now() + ChronoDuration::seconds(25),
                identity: ModelCredentialImportIdentityV1 {
                    schema_version: 1, operation_id: Uuid::new_v4().to_string().parse().unwrap(),
                    tenant_id: fresh(ResourceKind::Tenant), principal_id: fresh(ResourceKind::Principal),
                    principal_kind: PrincipalKind::TenantAdmin, provider_id: config.provider_id.clone(), purpose: "model_api_key".parse().unwrap(),
                },
            };
            let permit = ModelCredentialImportPermitV1 { schema_version: 1, request_digest: request.canonical_digest().unwrap(), valid_until: request.deadline };
            let key = SensitiveModelApiKey::new(b"synthetic-openbao-model-key".to_vec()).unwrap();
            let (left, right) = tokio::join!(provider.prepare_or_load_model_credential(&request, &permit, &key), provider.prepare_or_load_model_credential(&request, &permit, &key));
            let left = remember(&cleanup, &request.identity.tenant_id, left.expect("actual CAS0 winner"));
            let right = right.expect("actual concurrent exact winner readback");
            assert_eq!(left.secret_binding_id, right.secret_binding_id);
            assert_eq!(left.opaque_version_identity_digest, right.opaque_version_identity_digest);
            assert!(left.opaque_reference.expose() == right.opaque_reference.expose());
            let wrong = SensitiveModelApiKey::new(b"different-synthetic-key".to_vec()).unwrap();
            assert!(matches!(provider.prepare_or_load_model_credential(&request, &permit, &wrong).await, Err(SecretProviderPrepareError::Rejected)));
            roundtrip(&provider, &request.identity.tenant_id, &left, request.identity.purpose.clone(), key.expose()).await;
            let reference = OpenBaoOpaqueSecretReferenceV1::decode(&left.opaque_reference, &config, &request.identity.tenant_id).unwrap();
            let path = BaoSecretPath::parse(&reference.relative_path).unwrap();
            let metadata = provider.client.metadata(&config.kv, &path, provider.deadline()).await.unwrap();
            assert_eq!(metadata.current_version, 1);
            assert_eq!(metadata.versions.len(), 1);

            let now = Utc::now();
            let tenant = fresh(ResourceKind::Tenant);
            let preparation_digest = digest(Uuid::new_v4().as_bytes());
            let candidate = || {
                let mut candidate = crate::tests::prepared_candidate(now, config.provider_id.clone());
                candidate.tenant_id = tenant.clone();
                candidate.preparation_digest = preparation_digest.clone();
                candidate
            };
            let pkce = provider.prepare_or_load_mcp_oauth_transient(candidate()).await.expect("actual PKCE preparation");
            let pkce_prepared = remember(&cleanup, &tenant, pkce.prepared_secret);
            let replay = provider.prepare_or_load_mcp_oauth_transient(candidate()).await.expect("exact PKCE recovery");
            assert_eq!(pkce.stored.pkce_secret_binding, replay.stored.pkce_secret_binding);
            assert!(pkce.stored.state.as_bytes() == replay.stored.state.as_bytes());
            roundtrip(&provider, &tenant, &pkce_prepared, MCP_OAUTH_PKCE_SECRET_PURPOSE.parse().unwrap(), pkce.stored.pkce_verifier.expose()).await;

            let preparation = crate::tests::token_preparation(Utc::now(), config.provider_id.clone());
            assert!(provider.load_prepared_mcp_oauth_token(&preparation).await.unwrap().is_none());
            let tokens: McpOAuthTokenSet = serde_json::from_value(serde_json::json!({
                "access_token":"synthetic-openbao-access-token", "refresh_token":"synthetic-openbao-refresh-token", "token_type":"Bearer", "expires_in":600,"scope":"openid profile"
            })).unwrap();
            let verified = VerifiedMcpOAuthToken { granted_scopes: preparation.requested_scopes.clone(), audience_identity_digest: preparation.audience_identity_digest.clone(), issuer_identity_digest: preparation.issuer_identity_digest.clone(), subject_identity_digest: digest(b"subject"), verification_evidence_digest: digest(b"verification"), expires_at: Utc::now() + ChronoDuration::minutes(5), nonce_verified: true };
            let token = provider.prepare_or_load_mcp_oauth_token(&preparation, &tokens, &verified).await.expect("actual OAuth token prepare");
            let token_prepared = remember(&cleanup, &preparation.tenant_id, token.prepared_secret);
            let replay = provider.load_prepared_mcp_oauth_token(&preparation).await.unwrap().expect("exact OAuth token recovery without external code exchange");
            assert_eq!(token.stored, replay.stored);
            roundtrip(&provider, &preparation.tenant_id, &token_prepared, preparation.token_credential_purpose, tokens.access_token.expose()).await;
        }
    }).await;
    // Always attempt every owned exact version even if a verification assertion failed.
    let entries = std::mem::take(&mut *cleanup.lock().unwrap());
    let mut failures = 0;
    for (tenant, prepared) in entries {
        let policy = SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: prepared.opaque_version_identity_digest.clone(),
        };
        if provider
            .delete_exact(&tenant, &prepared.opaque_reference, &policy)
            .await
            != Ok(SecretProviderDeleteDisposition::Deleted)
        {
            failures += 1;
            continue;
        }
        if provider
            .delete_exact(&tenant, &prepared.opaque_reference, &policy)
            .await
            != Ok(SecretProviderDeleteDisposition::AlreadyAbsent)
        {
            failures += 1;
        }
        if provider
            .resolve(&tenant, &prepared.opaque_reference, &policy)
            .await
            .is_ok()
        {
            failures += 1;
        }
        let reference =
            OpenBaoOpaqueSecretReferenceV1::decode(&prepared.opaque_reference, &config, &tenant)
                .unwrap();
        let path = BaoSecretPath::parse(&reference.relative_path).unwrap();
        // CAS0 cannot resurrect a destroyed version because metadata was retained.
        let recreate = provider
            .client
            .create_only(
                &config.kv,
                &path,
                b"{\"synthetic\":true}",
                provider.deadline(),
            )
            .await;
        if recreate.is_ok() {
            failures += 1;
        }
    }
    assert_eq!(failures, 0, "exact fixture cleanup/tombstone proof failed");
    assert!(
        verification.is_ok(),
        "physical boundary verification failed; exact cleanup attempted"
    );
}
