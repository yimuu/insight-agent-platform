//! Isolated provider persistence evidence. Never initializes or repairs a provider.
#![cfg(unix)]
use insight_platform_artifact_broker::{
    ArtifactProviderCatalog, ArtifactProviderCatalogConfigV2, S3ArtifactUploadRequest,
    StagedArtifactObject,
};
use insight_platform_contracts::{ResourceId, ResourceKind, Sha256Digest};
use insight_platform_deployment_contracts::{
    installation::InstallationInputV1,
    installation_provider::{InstallationProviderStateV1, OpenBaoInstallationRole},
};
use insight_platform_deployment_tooling::{installation::PreparedInstallation, openbao_profile};
use insight_platform_openbao::{BaoClient, BaoSecretPath};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    fs,
    io::Read as _,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    time::{Duration, SystemTime},
};
use tokio::time::Instant;

const BYTES: &[u8] = b"public isolated provider restart canary";
const JSON: &[u8] = br#"{"purpose":"isolated_provider_restart","schema_version":1}"#;
const AAD: &[u8] = br#"{"purpose":"isolated_provider_restart_reference","schema_version":1}"#;
const PROOF: &str = "qualification-recovery-proof.json";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proof {
    schema_version: u32,
    input_digest: Sha256Digest,
    identity_digest: Sha256Digest,
    ciphertext: String,
    tenant: ResourceId,
    artifact: ResourceId,
    blob: ResourceId,
    domain: ResourceId,
    staged: Stage,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stage {
    storage_backend: String,
    storage_binding_digest: Sha256Digest,
    object_reference_ciphertext: Vec<u8>,
    object_generation: String,
    key_id: String,
    observed_size_bytes: u64,
    backend_evidence_digest: Sha256Digest,
}
impl From<StagedArtifactObject> for Stage {
    fn from(value: StagedArtifactObject) -> Self {
        Self {
            storage_backend: value.storage_backend,
            storage_binding_digest: value.storage_binding_digest,
            object_reference_ciphertext: value.object_reference_ciphertext,
            object_generation: value.object_generation,
            key_id: value.key_id,
            observed_size_bytes: value.observed_size_bytes,
            backend_evidence_digest: value.backend_evidence_digest,
        }
    }
}
impl Stage {
    fn object(&self) -> StagedArtifactObject {
        StagedArtifactObject {
            storage_backend: self.storage_backend.clone(),
            storage_binding_digest: self.storage_binding_digest.clone(),
            object_reference_ciphertext: self.object_reference_ciphertext.clone(),
            object_generation: self.object_generation.clone(),
            key_id: self.key_id.clone(),
            observed_size_bytes: self.observed_size_bytes,
            backend_evidence_digest: self.backend_evidence_digest.clone(),
        }
    }
}
fn private_json<T: serde::de::DeserializeOwned>(path: &Path) -> T {
    let metadata = fs::symlink_metadata(path).unwrap();
    assert!(
        metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= 1_048_576
    );
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(metadata.nlink(), 1);
    let mut bytes = Vec::new();
    fs::File::open(path)
        .unwrap()
        .take(1_048_577)
        .read_to_end(&mut bytes)
        .unwrap();
    let value = insight_platform_contracts::parse_strict_json(
        &bytes,
        insight_platform_contracts::JsonLimits::CONTRACT_FIXTURE,
    )
    .unwrap();
    serde_json::from_value(value).unwrap()
}
fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn request(proof: &Proof) -> S3ArtifactUploadRequest<'_> {
    S3ArtifactUploadRequest {
        tenant_id: &proof.tenant,
        artifact_id: &proof.artifact,
        blob_id: &proof.blob,
        encryption_domain_id: &proof.domain,
        expected_size_bytes: BYTES.len() as u64,
        declared_media_type: Some("text/plain"),
        expires_at: SystemTime::now() + Duration::from_secs(60),
    }
}

#[tokio::test]
#[ignore = "requires explicitly owned real Bao and S3 fixtures; seed once, then read-only verify after recreation"]
async fn actual_provider_recreation_preserves_original_ciphertext_kv_and_object_version() {
    let root = std::env::var_os("INSIGHT_OPENBAO_FIXTURE_DIRECTORY")
        .expect("explicit provider fixture directory required");
    let root = Path::new(&root);
    let mode =
        std::env::var("INSIGHT_PROVIDER_RECOVERY_PHASE").expect("explicit seed or verify required");
    assert!(matches!(mode.as_str(), "seed" | "verify"));
    let input: InstallationInputV1 = private_json(&root.join("input.json"));
    let prepared = PreparedInstallation::open(&input, &root.join("private")).unwrap();
    let InstallationProviderStateV1::ProviderReady { evidence } =
        prepared.provider_state().unwrap().state
    else {
        panic!("ProviderReady required")
    };
    let client = BaoClient::install(
        openbao_profile::role_client(
            &evidence,
            OpenBaoInstallationRole::EgressBroker,
            &root.join("private"),
        )
        .unwrap(),
    )
    .unwrap();
    let path = BaoSecretPath::parse("prepared/qualification-recovery").unwrap();
    let catalog: ArtifactProviderCatalogConfigV2 =
        private_json(&root.join("artifact-catalog.json"));
    let catalog = ArtifactProviderCatalog::install(catalog).await.unwrap();
    catalog.check_readiness().await.unwrap();
    let (upload, _, _) = catalog.into_gateway_components();
    let hex = Sha256::digest(BYTES)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let digest: Sha256Digest = format!("sha256:{hex}").parse().unwrap();
    if mode == "seed" {
        assert!(prepared
            .directory()
            .read(PROOF, 1_048_576)
            .unwrap()
            .is_none());
        assert!(prepared
            .directory()
            .read("qualification-recovery-requested.json", 4096)
            .unwrap()
            .is_none());
        // A lost seed response leaves evidence for diagnosis, never an automatic second seed.
        prepared
            .directory()
            .write_immutable(
                "qualification-recovery-requested.json",
                b"{\"schema_version\":1}",
            )
            .unwrap();
        assert_eq!(
            client
                .create_only(&evidence.secrets, &path, JSON, deadline())
                .await
                .unwrap(),
            1
        );
        let ciphertext = client
            .encrypt(&evidence.secret_key, BYTES, AAD, deadline())
            .await
            .unwrap();
        let mut proof = Proof {
            schema_version: 1,
            input_digest: input.digest().unwrap(),
            identity_digest: prepared.identity().digest().unwrap(),
            ciphertext: String::from_utf8(ciphertext.into_bytes()).unwrap(),
            tenant: fresh(ResourceKind::Tenant),
            artifact: fresh(ResourceKind::Artifact),
            blob: fresh(ResourceKind::InternalBlob),
            domain: fresh(ResourceKind::EncryptionDomain),
            staged: Stage {
                storage_backend: String::new(),
                storage_binding_digest: digest.clone(),
                object_reference_ciphertext: Vec::new(),
                object_generation: String::new(),
                key_id: String::new(),
                observed_size_bytes: 0,
                backend_evidence_digest: digest.clone(),
            },
        };
        proof.staged = upload
            .stage_bytes(request(&proof), &digest, BYTES.to_vec())
            .await
            .unwrap()
            .into();
        prepared
            .directory()
            .write_immutable(PROOF, &serde_json::to_vec(&proof).unwrap())
            .unwrap();
    }
    let proof: Proof = serde_json::from_slice(
        &prepared
            .directory()
            .read(PROOF, 1_048_576)
            .unwrap()
            .expect("original committed proof required"),
    )
    .unwrap();
    assert_eq!(proof.schema_version, 1);
    assert_eq!(proof.input_digest, input.digest().unwrap());
    assert_eq!(proof.identity_digest, prepared.identity().digest().unwrap());
    let plaintext = client
        .decrypt(
            &evidence.secret_key,
            proof.ciphertext.as_bytes(),
            AAD,
            deadline(),
        )
        .await
        .unwrap();
    assert!(plaintext.as_bytes() == BYTES);
    assert!(client
        .decrypt(
            &evidence.secret_key,
            proof.ciphertext.as_bytes(),
            b"wrong-aad",
            deadline()
        )
        .await
        .is_err());
    let read = client
        .read_exact(&evidence.secrets, &path, 1, deadline())
        .await
        .unwrap();
    assert_eq!(read.version, 1);
    assert!(read.bytes.as_bytes() == JSON);
    let metadata = client
        .metadata(&evidence.secrets, &path, deadline())
        .await
        .unwrap();
    assert_eq!(metadata.current_version, 1);
    assert_eq!(metadata.versions.len(), 1);
    assert!(!metadata.versions[&1].destroyed && metadata.versions[&1].deletion_time.is_empty());
    let verified = upload
        .verify_staged_bytes(request(&proof), &proof.staged.object(), &digest)
        .await
        .unwrap();
    assert_eq!(verified.object_generation, proof.staged.object_generation);
    assert_eq!(verified.observed_size_bytes, BYTES.len() as u64);
}
