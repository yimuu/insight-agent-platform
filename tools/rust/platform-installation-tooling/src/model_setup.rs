//! Frozen model Policy declaration and physical S3 verification, before the PostgreSQL owner writes.
use insight_platform_artifact_broker::{
    ArtifactProviderCatalog, ArtifactProviderCatalogConfigV2, ArtifactUploadProviderError,
    S3ArtifactUploadRequest, StagedArtifactObject,
};
use insight_platform_contracts::*;
use insight_platform_deployment_contracts::installation::{
    InstallationError as Error, InstallationIdentityV1, InstallationInputV1,
    InstallationModelPolicyArtifactInputsV1,
};
use insight_platform_deployment_tooling::private_state::InstallationDirectory;
use insight_platform_registry::model_policy_bootstrap::{
    build_model_policy_bootstrap, ModelPolicyBootstrapMaterial,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

pub const SEED_FILE: &str = "model-policy-seed.json";
pub const MATERIAL_FILE: &str = "model-policy-material.json";
const JOURNAL_FILE: &str = "model-policy-object.json";
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Provision,
    Verify,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Requested,
    Staged,
    Verified,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    input_digest: Sha256Digest,
    identity_digest: Sha256Digest,
    seed_digest: Sha256Digest,
    staging_credentials_digest: Sha256Digest,
    phase: Phase,
    material: Option<ModelPolicyArtifactMaterialV1>,
    readback_digest: Option<Sha256Digest>,
}
pub struct ModelSetup {
    pub seed: ModelPolicyBootstrapSeedV1,
    pub built: ModelPolicyBootstrapMaterial,
    pub material: ModelPolicyArtifactMaterialV1,
}
fn invalid() -> Error {
    Error::ConfigurationDrift
}
fn physical_error(error: ArtifactUploadProviderError, may_have_written: bool) -> Error {
    match error {
        ArtifactUploadProviderError::InvalidRequest
        | ArtifactUploadProviderError::InvalidEvidence
        | ArtifactUploadProviderError::TooLarge => Error::ConfigurationDrift,
        ArtifactUploadProviderError::StorageUnavailable
        | ArtifactUploadProviderError::KmsUnavailable
            if may_have_written =>
        {
            Error::ExternalOutcomeUnknown
        }
        ArtifactUploadProviderError::StorageUnavailable
        | ArtifactUploadProviderError::KmsUnavailable => Error::PrerequisiteUnavailable,
    }
}

async fn observe_verified_bytes<T, F, Fut>(
    mode: Mode,
    phase: Phase,
    mut read: F,
) -> Result<T, ArtifactUploadProviderError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, ArtifactUploadProviderError>>,
{
    if mode != Mode::Verify || phase != Phase::Verified {
        return read().await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(ArtifactUploadProviderError::StorageUnavailable);
        }
        // This closure only decrypts the frozen locator and reads the exact object generation.
        // It never stages bytes. Listening does not prove a restarted volume is readable yet.
        let result = tokio::time::timeout_at(deadline, read())
            .await
            .map_err(|_| ArtifactUploadProviderError::StorageUnavailable)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(ArtifactUploadProviderError::StorageUnavailable);
        }
        match result {
            Err(ArtifactUploadProviderError::StorageUnavailable) => {
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + Duration::from_secs(1)),
                )
                .await;
            }
            result => return result,
        }
    }
}

fn fresh(kind: ResourceKind) -> Result<ResourceId, Error> {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).map_err(|_| Error::InvalidInput)
}
fn canonical(value: &impl Serialize) -> Result<Sha256Digest, Error> {
    canonical_digest(&serde_json::to_value(value).map_err(|_| invalid())?)
        .map_err(|_| invalid())?
        .parse()
        .map_err(|_| invalid())
}
fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_json::from_value(
        parse_strict_json(
            bytes,
            JsonLimits {
                max_bytes: MAX_MODEL_POLICY_MATERIAL_BYTES,
                max_depth: 12,
                max_properties_per_object: 32,
                max_items_per_array: 16384,
                max_string_bytes: 2048,
            },
        )
        .map_err(|_| invalid())?,
    )
    .map_err(|_| invalid())
}
fn save(private: &InstallationDirectory, journal: &Journal) -> Result<(), Error> {
    private.replace(
        JOURNAL_FILE,
        &serde_json::to_vec(journal).map_err(|_| invalid())?,
    )
}

pub fn prepare_seed(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    private: &InstallationDirectory,
    artifact: &InstallationModelPolicyArtifactInputsV1,
    minimum_retention_seconds: u64,
    mode: Mode,
) -> Result<ModelPolicyBootstrapSeedV1, Error> {
    artifact.validate_for(input, identity)?;
    let seed: ModelPolicyBootstrapSeedV1 =
        match private.read(SEED_FILE, MAX_MODEL_POLICY_DECLARATION_BYTES)? {
            Some(bytes) => parse(&bytes)?,
            None if mode == Mode::Provision => {
                let mut policies = Vec::new();
                for role in ModelBootstrapPolicyRole::ALL {
                    policies.push(ModelBootstrapPolicyIdentityV1 {
                        role,
                        resource_id: fresh(ResourceKind::Policy)?,
                        revision_id: fresh(ResourceKind::PolicyRevision)?,
                        deployment_id: fresh(ResourceKind::PolicyDeployment)?,
                        artifact_reference_id: fresh(ResourceKind::ArtifactLink)?,
                    });
                }
                let seconds = i64::try_from(minimum_retention_seconds.max(86400))
                    .map_err(|_| Error::InvalidInput)?;
                let retain_until = chrono::Utc::now()
                    .checked_add_signed(chrono::Duration::seconds(seconds))
                    .ok_or(Error::InvalidInput)?;
                let seed = ModelPolicyBootstrapSeedV1 {
                    schema_version: 1,
                    tenant_id: identity.session.tenant_id.clone(),
                    installation_principal_id: identity.bootstrap.installation.principal_id.clone(),
                    created_by: identity.bootstrap.administrator.principal_id.clone(),
                    request_id: fresh(ResourceKind::ServerRequest)?,
                    environment: input.environment_class.clone(),
                    authoring_artifact_id: fresh(ResourceKind::Artifact)?,
                    authoring_blob_id: fresh(ResourceKind::InternalBlob)?,
                    model_quota_account_id: fresh(ResourceKind::QuotaAccount)?,
                    encryption_domain_id: artifact.encryption_domain_id.clone(),
                    retention_policy: artifact.retention_policy.clone(),
                    retain_until: UtcTimestamp::from_datetime(retain_until),
                    policies: policies.try_into().map_err(|_| Error::InvalidInput)?,
                };
                seed.validate().map_err(|_| invalid())?;
                private.write_immutable(
                    SEED_FILE,
                    &serde_json::to_vec(&seed).map_err(|_| invalid())?,
                )?;
                seed
            }
            None => return Err(Error::Incomplete),
        };
    seed.validate().map_err(|_| invalid())?;
    if seed.tenant_id != identity.session.tenant_id
        || seed.installation_principal_id != identity.bootstrap.installation.principal_id
        || seed.created_by != identity.bootstrap.administrator.principal_id
        || seed.environment != input.environment_class
        || seed.encryption_domain_id != artifact.encryption_domain_id
        || seed.retention_policy != artifact.retention_policy
    {
        return Err(Error::IdentityDrift);
    }
    Ok(seed)
}

fn staging_environment(
    private: &InstallationDirectory,
    identity: &InstallationIdentityV1,
    read: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<Sha256Digest, Error> {
    use insight_platform_deployment_tooling::s3_profile::{S3IdentityRole, S3RoleCredentials};
    use sha2::{Digest as _, Sha256};
    let path = private.path(S3IdentityRole::ArtifactGateway.credential_filename())?;
    for (name, expected) in [
        ("AWS_SHARED_CREDENTIALS_FILE", path.as_os_str()),
        ("AWS_PROFILE", std::ffi::OsStr::new("default")),
        ("AWS_CONFIG_FILE", std::ffi::OsStr::new("/dev/null")),
        ("AWS_EC2_METADATA_DISABLED", std::ffi::OsStr::new("true")),
    ] {
        if read(name).as_deref() != Some(expected) {
            return Err(Error::CredentialInvalid);
        }
    }
    for name in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_DEFAULT_PROFILE",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_CA_BUNDLE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_ROLE_SESSION_NAME",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        if read(name).is_some() {
            return Err(Error::CredentialInvalid);
        }
    }
    crate::s3_setup::tls_environment(
        private,
        identity,
        read("SSL_CERT_FILE").as_deref(),
        read("SSL_CERT_DIR").as_deref(),
    )?;
    let bytes = zeroize::Zeroizing::new(
        private
            .read(S3IdentityRole::ArtifactGateway.credential_filename(), 256)?
            .ok_or(Error::Incomplete)?,
    );
    let _credential = S3RoleCredentials::decode(&bytes)?;
    format!(
        "sha256:{}",
        Sha256::digest(bytes.as_slice())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
    .parse()
    .map_err(|_| Error::CredentialInvalid)
}

pub async fn ensure_model_object(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    private: &InstallationDirectory,
    seed: ModelPolicyBootstrapSeedV1,
    provider_config: serde_json::Value,
    mode: Mode,
) -> Result<ModelSetup, Error> {
    let staging_credentials_digest =
        staging_environment(private, identity, |name| std::env::var_os(name))?;
    let built = build_model_policy_bootstrap(&seed).map_err(|_| invalid())?;
    let seed_digest = seed.canonical_digest().map_err(|_| invalid())?;
    let mut journal: Journal = match private.read(JOURNAL_FILE, MAX_MODEL_POLICY_MATERIAL_BYTES)? {
        Some(bytes) => parse(&bytes)?,
        None if mode == Mode::Provision => {
            let value = Journal {
                schema_version: 1,
                input_digest: input.digest()?,
                identity_digest: identity.digest()?,
                seed_digest: seed_digest.clone(),
                staging_credentials_digest: staging_credentials_digest.clone(),
                phase: Phase::Requested,
                material: None,
                readback_digest: None,
            };
            save(private, &value)?;
            value
        }
        None => return Err(Error::Incomplete),
    };
    if journal.schema_version != 1
        || journal.input_digest != input.digest()?
        || journal.identity_digest != identity.digest()?
        || journal.seed_digest != seed_digest
        || journal.staging_credentials_digest != staging_credentials_digest
        || (journal.phase == Phase::Requested) != (journal.material.is_none())
        || (journal.phase == Phase::Verified) != (journal.readback_digest.is_some())
    {
        return Err(Error::IdentityDrift);
    }
    if mode == Mode::Verify && journal.phase != Phase::Verified {
        return Err(Error::Incomplete);
    }
    let provider: ArtifactProviderCatalogConfigV2 =
        serde_json::from_value(provider_config).map_err(|_| invalid())?;
    let upload = ArtifactProviderCatalog::install(provider)
        .await
        .map_err(|_| invalid())?
        .into_gateway_provider();
    let request = S3ArtifactUploadRequest {
        tenant_id: &seed.tenant_id,
        artifact_id: &seed.authoring_artifact_id,
        blob_id: &seed.authoring_blob_id,
        encryption_domain_id: &seed.encryption_domain_id,
        expected_size_bytes: built.declaration_bytes.len() as u64,
        declared_media_type: Some("application/json"),
        expires_at: SystemTime::now() + Duration::from_secs(300),
    };
    if journal.phase == Phase::Requested {
        // stage_bytes uses the frozen object key and conditional create. An interrupted PUT can
        // only resolve to the same generation/content; it cannot overwrite or allocate a new key.
        let staged = upload
            .stage_bytes(
                request,
                &built.content_digest,
                built.declaration_bytes.clone(),
            )
            .await
            .map_err(|error| physical_error(error, true))?;
        let material = ModelPolicyArtifactMaterialV1 {
            schema_version: 1,
            seed_digest: seed_digest.clone(),
            content_digest: built.content_digest.clone(),
            size_bytes: staged.observed_size_bytes,
            storage_backend: staged.storage_backend,
            storage_binding_digest: staged.storage_binding_digest,
            object_reference_ciphertext: staged.object_reference_ciphertext,
            object_generation: staged.object_generation,
            key_id: staged.key_id,
            backend_evidence_digest: staged.backend_evidence_digest,
        };
        material
            .validate_for(
                &seed,
                &built.content_digest,
                built.declaration_bytes.len() as u64,
            )
            .map_err(|_| invalid())?;
        journal.material = Some(material);
        journal.phase = Phase::Staged;
        save(private, &journal)?;
    }
    let material = journal.material.as_ref().ok_or(Error::Incomplete)?;
    material
        .validate_for(
            &seed,
            &built.content_digest,
            built.declaration_bytes.len() as u64,
        )
        .map_err(|_| invalid())?;
    let staged = StagedArtifactObject {
        storage_backend: material.storage_backend.clone(),
        storage_binding_digest: material.storage_binding_digest.clone(),
        object_reference_ciphertext: material.object_reference_ciphertext.clone(),
        object_generation: material.object_generation.clone(),
        key_id: material.key_id.clone(),
        observed_size_bytes: material.size_bytes,
        backend_evidence_digest: material.backend_evidence_digest.clone(),
    };
    let observed = observe_verified_bytes(mode, journal.phase, || {
        upload.verify_staged_bytes(request, &staged, &built.content_digest)
    })
    .await
    .map_err(|error| physical_error(error, false))?;
    if journal
        .readback_digest
        .as_ref()
        .is_some_and(|digest| digest != &observed.backend_evidence_digest)
    {
        return Err(invalid());
    }
    if journal.phase != Phase::Verified {
        journal.phase = Phase::Verified;
        journal.readback_digest = Some(observed.backend_evidence_digest);
        save(private, &journal)?;
    }
    let material = journal.material.ok_or(Error::Incomplete)?;
    match private.read(MATERIAL_FILE, MAX_MODEL_POLICY_MATERIAL_BYTES)? {
        Some(bytes)
            if canonical(
                &ModelPolicyArtifactMaterialV1::decode(&bytes).map_err(|_| invalid())?,
            )? == canonical(&material)? => {}
        None if mode == Mode::Provision => private.write_immutable(
            MATERIAL_FILE,
            &serde_json::to_vec(&material).map_err(|_| invalid())?,
        )?,
        _ => return Err(invalid()),
    }
    Ok(ModelSetup {
        seed,
        built,
        material,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_deployment_tooling::{
        installation::{compose_input, PreparedInstallation},
        model_profile,
        worker_profile::WorkerBuilds,
    };

    #[tokio::test(start_paused = true)]
    async fn verified_read_observes_unavailable_until_current_evidence_succeeds() {
        use std::cell::Cell;
        let calls = Cell::new(0);
        let started = tokio::time::Instant::now();
        let result = observe_verified_bytes(Mode::Verify, Phase::Verified, || {
            calls.set(calls.get() + 1);
            std::future::ready(if calls.get() < 3 {
                Err(ArtifactUploadProviderError::StorageUnavailable)
            } else {
                Ok("current exact evidence")
            })
        })
        .await;
        assert_eq!(result, Ok("current exact evidence"));
        assert_eq!(calls.get(), 3);
        assert_eq!(started.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn first_write_and_incomplete_phases_never_repeat_readback() {
        use std::cell::Cell;
        for (mode, phase) in [
            (Mode::Provision, Phase::Requested),
            (Mode::Provision, Phase::Staged),
            (Mode::Provision, Phase::Verified),
            (Mode::Verify, Phase::Requested),
            (Mode::Verify, Phase::Staged),
        ] {
            let calls = Cell::new(0);
            let started = tokio::time::Instant::now();
            let result = observe_verified_bytes(mode, phase, || {
                calls.set(calls.get() + 1);
                std::future::ready(Err::<(), _>(
                    ArtifactUploadProviderError::StorageUnavailable,
                ))
            })
            .await;
            assert_eq!(result, Err(ArtifactUploadProviderError::StorageUnavailable));
            assert_eq!(calls.get(), 1);
            assert_eq!(started.elapsed(), Duration::ZERO);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn verified_read_retains_nonretryable_error_and_classification() {
        use std::cell::Cell;
        for (error, expected) in [
            (
                ArtifactUploadProviderError::InvalidRequest,
                Error::ConfigurationDrift,
            ),
            (
                ArtifactUploadProviderError::InvalidEvidence,
                Error::ConfigurationDrift,
            ),
            (
                ArtifactUploadProviderError::TooLarge,
                Error::ConfigurationDrift,
            ),
            (
                ArtifactUploadProviderError::KmsUnavailable,
                Error::PrerequisiteUnavailable,
            ),
        ] {
            let calls = Cell::new(0);
            let started = tokio::time::Instant::now();
            let result = observe_verified_bytes(Mode::Verify, Phase::Verified, || {
                calls.set(calls.get() + 1);
                std::future::ready(Err::<(), _>(error))
            })
            .await;
            assert_eq!(result, Err(error));
            assert_eq!(physical_error(result.unwrap_err(), false), expected);
            assert_eq!(calls.get(), 1);
            assert_eq!(started.elapsed(), Duration::ZERO);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn verified_read_repeated_failure_has_one_deadline() {
        use std::cell::Cell;
        let calls = Cell::new(0);
        let started = tokio::time::Instant::now();
        let result = observe_verified_bytes(Mode::Verify, Phase::Verified, || {
            calls.set(calls.get() + 1);
            std::future::ready(Err::<(), _>(
                ArtifactUploadProviderError::StorageUnavailable,
            ))
        })
        .await;
        assert_eq!(
            physical_error(result.unwrap_err(), false),
            Error::PrerequisiteUnavailable
        );
        assert_eq!(calls.get(), 30);
        assert_eq!(started.elapsed(), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn verified_read_deadline_bounds_the_complete_call_and_late_success() {
        use std::cell::Cell;
        for delay in [Duration::from_secs(30), Duration::from_secs(60)] {
            let calls = Cell::new(0);
            let started = tokio::time::Instant::now();
            let result = observe_verified_bytes(Mode::Verify, Phase::Verified, || {
                calls.set(calls.get() + 1);
                async move {
                    tokio::time::sleep(delay).await;
                    Ok(())
                }
            })
            .await;
            assert_eq!(result, Err(ArtifactUploadProviderError::StorageUnavailable));
            assert_eq!(calls.get(), 1);
            assert_eq!(started.elapsed(), Duration::from_secs(30));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn verified_read_second_call_uses_remaining_deadline() {
        use std::cell::Cell;
        let calls = Cell::new(0);
        let started = tokio::time::Instant::now();
        let result = observe_verified_bytes(Mode::Verify, Phase::Verified, || {
            calls.set(calls.get() + 1);
            let current = calls.get();
            async move {
                tokio::time::sleep(Duration::from_secs(20)).await;
                if current == 1 {
                    Err(ArtifactUploadProviderError::StorageUnavailable)
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(result, Err(ArtifactUploadProviderError::StorageUnavailable));
        assert_eq!(calls.get(), 2);
        assert_eq!(started.elapsed(), Duration::from_secs(30));
    }

    #[test]
    fn seed_freezes_actual_policy_refs_and_catalog_uses_the_same_built_worker_manifest() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let input = compose_input(
            "model-seed-test",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        let prepared = PreparedInstallation::prepare(&input, &root.join("private")).unwrap();
        let artifact = InstallationModelPolicyArtifactInputsV1 {
            schema_version: 1,
            input_digest: input.digest().unwrap(),
            identity_digest: prepared.identity().digest().unwrap(),
            retention_policy: ExactVersionRef::new(
                fresh(ResourceKind::PolicyRevision).unwrap(),
                format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            )
            .unwrap(),
            encryption_domain_id: prepared.identity().artifact_encryption_domain_id.clone(),
            storage_binding_digest: format!("sha256:{}", "c".repeat(64)).parse().unwrap(),
        };
        assert!(matches!(
            prepare_seed(
                &input,
                prepared.identity(),
                prepared.directory(),
                &artifact,
                3600,
                Mode::Verify
            ),
            Err(Error::Incomplete)
        ));
        assert!(prepared
            .directory()
            .read(SEED_FILE, MAX_MODEL_POLICY_DECLARATION_BYTES)
            .unwrap()
            .is_none());
        let seed = prepare_seed(
            &input,
            prepared.identity(),
            prepared.directory(),
            &artifact,
            3600,
            Mode::Provision,
        )
        .unwrap();
        assert_eq!(
            seed,
            prepare_seed(
                &input,
                prepared.identity(),
                prepared.directory(),
                &artifact,
                3600,
                Mode::Verify
            )
            .unwrap()
        );
        assert_eq!(seed.retention_policy, artifact.retention_policy);
        assert_eq!(
            seed.model_quota_account_id.kind(),
            ResourceKind::QuotaAccount
        );
        let bytes = std::fs::read(prepared.directory().path(SEED_FILE).unwrap()).unwrap();
        let mut wrong = artifact.clone();
        wrong.retention_policy = ExactVersionRef::new(
            fresh(ResourceKind::PolicyRevision).unwrap(),
            wrong.retention_policy.semantic_digest,
        )
        .unwrap();
        assert!(prepare_seed(
            &input,
            prepared.identity(),
            prepared.directory(),
            &wrong,
            3600,
            Mode::Provision
        )
        .is_err());
        assert_eq!(
            bytes,
            std::fs::read(prepared.directory().path(SEED_FILE).unwrap()).unwrap()
        );
        let binaries = root.join("binaries");
        std::fs::create_dir(&binaries).unwrap();
        std::fs::write(
            binaries.join("platform-model-worker"),
            b"actual selected executable bytes",
        )
        .unwrap();
        let builds=WorkerBuilds::read_processes(&binaries,&[insight_platform_deployment_contracts::installation::InstallationProcess::ModelWorker]).unwrap();
        let built = build_model_policy_bootstrap(&seed).unwrap();
        let catalog =
            model_profile::installation_catalog(&input, prepared.identity(), &built, &builds)
                .unwrap()
                .unwrap();
        assert_eq!(
            catalog.adapters[0].worker_manifest_digest,
            canonical(
                &insight_platform_deployment_tooling::worker_profile::model_manifest(&builds)
                    .unwrap()
            )
            .unwrap()
        );
        assert_eq!(
            catalog.adapters[0].adapter_contract_digest,
            ModelProviderWireProtocol::OpenAiResponses.adapter_contract_digest()
        );
        assert_eq!(
            catalog.policies.network,
            built
                .policy(ModelBootstrapPolicyRole::Network)
                .exact
                .revision
        );
        assert_eq!(
            catalog.secret_provider_id,
            prepared.identity().secret_provider_id
        );
        assert_eq!(catalog.public_egress().protocols.len(), 2);
        // A completed setup may not invent a new quota identity when persisted input is damaged.
        // Both recovery and read-only verification reject before any provider or PG command.
        let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for replacement in [
            None,
            Some(serde_json::Value::Null),
            Some(serde_json::json!(fresh(ResourceKind::Artifact).unwrap())),
        ] {
            let mut damaged = original.clone();
            match replacement {
                None => {
                    damaged
                        .as_object_mut()
                        .unwrap()
                        .remove("model_quota_account_id");
                }
                Some(value) => {
                    damaged["model_quota_account_id"] = value;
                }
            }
            let encoded = serde_json::to_vec(&damaged).unwrap();
            prepared.directory().replace(SEED_FILE, &encoded).unwrap();
            for mode in [Mode::Provision, Mode::Verify] {
                assert!(prepare_seed(
                    &input,
                    prepared.identity(),
                    prepared.directory(),
                    &artifact,
                    3600,
                    mode
                )
                .is_err());
                assert_eq!(
                    prepared
                        .directory()
                        .read(SEED_FILE, MAX_MODEL_POLICY_DECLARATION_BYTES)
                        .unwrap()
                        .unwrap(),
                    encoded
                );
            }
        }
        prepared.directory().replace(SEED_FILE, &bytes).unwrap();
    }

    #[test]
    fn staging_uses_exact_private_gateway_profile_and_trust_without_ambient_credentials() {
        use std::collections::BTreeMap;
        use std::ffi::OsString;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let input = compose_input(
            "staging-environment",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        let prepared = PreparedInstallation::prepare(&input, &root.join("private")).unwrap();
        let directory = prepared.directory();
        let values = BTreeMap::<&str, OsString>::from([
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                directory
                    .path("s3-artifact-gateway-credentials")
                    .unwrap()
                    .into_os_string(),
            ),
            ("AWS_PROFILE", "default".into()),
            ("AWS_CONFIG_FILE", "/dev/null".into()),
            ("AWS_EC2_METADATA_DISABLED", "true".into()),
            (
                "SSL_CERT_FILE",
                directory.path("ca.pem").unwrap().into_os_string(),
            ),
            ("SSL_CERT_DIR", "/etc/ssl/certs".into()),
        ]);
        let check = |values: &BTreeMap<&str, OsString>| {
            staging_environment(directory, prepared.identity(), |name| {
                values.get(name).cloned()
            })
        };
        let original = check(&values).unwrap();
        assert_eq!(check(&values).unwrap(), original);
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_ENDPOINT_URL_S3",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "HTTPS_PROXY",
            "AWS_REGION",
        ] {
            let mut changed = values.clone();
            changed.insert(name, "test".into());
            assert!(check(&changed).is_err());
        }
        for name in values.keys() {
            let mut changed = values.clone();
            changed.remove(name);
            assert!(check(&changed).is_err());
        }
        let mut wrong = values.clone();
        wrong.insert(
            "AWS_SHARED_CREDENTIALS_FILE",
            directory
                .path("s3-initializer-credentials")
                .unwrap()
                .into_os_string(),
        );
        assert!(check(&wrong).is_err());
        directory
            .replace(
                "s3-artifact-gateway-credentials",
                b"[default]\naws_access_key_id=test\naws_secret_access_key=test\n",
            )
            .unwrap();
        assert!(check(&values).is_err());
    }
}
