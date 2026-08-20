use super::*;
use chrono::Duration as ChronoDuration;
use insight_platform_artifacts::{
    ArtifactObjectReadAuthority, ArtifactObjectReadAuthorityError, AuthorizedArtifactObjectRead,
    EncryptedArtifactObjectReference,
};
use insight_platform_contracts::{ArtifactRef, DataClassification, ResourceId, ResourceKind};
use insight_platform_sandbox::{WasiArtifactReadPurpose, WasiArtifactReadRequest};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use uuid::Uuid;

fn id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

struct FixtureAuthority {
    tenant_id: ResourceId,
    blob_id: ResourceId,
    artifact: ArtifactRef,
    storage_binding_digest: Sha256Digest,
    encryption_domain_id: ResourceId,
    authorization_digest: Sha256Digest,
    drift_after_first: bool,
    calls: AtomicUsize,
}

impl FixtureAuthority {
    fn projection(&self, drift: bool) -> AuthorizedArtifactObjectRead {
        AuthorizedArtifactObjectRead {
            tenant_id: self.tenant_id.clone(),
            blob_id: self.blob_id.clone(),
            artifact: self.artifact.clone(),
            backend: "s3".to_owned(),
            storage_binding_digest: self.storage_binding_digest.clone(),
            encryption_domain_id: self.encryption_domain_id.clone(),
            key_id: "kms-key-1".to_owned(),
            object_reference_ciphertext: EncryptedArtifactObjectReference::new(
                b"ciphertext".to_vec(),
            )
            .unwrap(),
            object_generation: "version-1".to_owned(),
            authorization_digest: if drift {
                digest('f')
            } else {
                self.authorization_digest.clone()
            },
        }
    }
}

#[async_trait]
impl ArtifactObjectReadAuthority<WasiArtifactReadRequest> for FixtureAuthority {
    async fn authorize_object_read(
        &self,
        _request: &WasiArtifactReadRequest,
    ) -> Result<AuthorizedArtifactObjectRead, ArtifactObjectReadAuthorityError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.projection(self.drift_after_first && call > 0))
    }
}

struct FixtureUnsealer {
    plaintext: Mutex<Vec<u8>>,
}

#[async_trait]
impl ArtifactObjectReferenceUnsealer for FixtureUnsealer {
    async fn unseal(
        &self,
        _authorized: &AuthorizedArtifactObjectRead,
    ) -> Result<DecryptedArtifactObjectReference, ArtifactObjectReferenceUnsealError> {
        DecryptedArtifactObjectReference::new(self.plaintext.lock().unwrap().clone())
    }
}

struct FixtureStore {
    binding: Sha256Digest,
    metadata: Mutex<ArtifactObjectMetadata>,
    bytes: Mutex<Vec<u8>>,
}

#[async_trait]
impl InstalledArtifactObjectStore for FixtureStore {
    fn backend(&self) -> &str {
        "s3"
    }

    fn storage_binding_digest(&self) -> &Sha256Digest {
        &self.binding
    }

    async fn head_exact(
        &self,
        object_key: &str,
        object_generation: &str,
    ) -> Result<ArtifactObjectMetadata, ArtifactObjectStoreError> {
        assert_eq!(object_key, "opaque/object-1");
        assert_eq!(object_generation, "version-1");
        Ok(self.metadata.lock().unwrap().clone())
    }

    async fn read_exact(
        &self,
        object_key: &str,
        object_generation: &str,
        maximum_bytes: usize,
    ) -> Result<ArtifactObjectBytes, ArtifactObjectStoreError> {
        assert_eq!(object_key, "opaque/object-1");
        assert_eq!(object_generation, "version-1");
        ArtifactObjectBytes::new(
            self.metadata.lock().unwrap().clone(),
            self.bytes.lock().unwrap().clone(),
            maximum_bytes,
        )
    }
}

struct Fixture {
    broker: BrokeredSandboxArtifactBroker,
    request: WasiArtifactReadRequest,
    store: Arc<FixtureStore>,
}

fn fixture(
    bytes: &[u8],
    plaintext: Vec<u8>,
    drift_after_first: bool,
    metadata_generation: &str,
) -> Fixture {
    fixture_with_limits(
        bytes,
        plaintext,
        drift_after_first,
        metadata_generation,
        ArtifactBrokerLimits::default(),
    )
}

fn fixture_with_limits(
    bytes: &[u8],
    plaintext: Vec<u8>,
    drift_after_first: bool,
    metadata_generation: &str,
    limits: ArtifactBrokerLimits,
) -> Fixture {
    let tenant_id = id(ResourceKind::Tenant);
    let content_digest = sha256(bytes);
    let artifact = ArtifactRef::new(
        id(ResourceKind::Artifact),
        content_digest,
        u64::try_from(bytes.len()).unwrap(),
        "application/wasm",
        DataClassification::Internal,
        None,
    )
    .unwrap();
    let storage_binding_digest = digest('b');
    let authority = Arc::new(FixtureAuthority {
        tenant_id: tenant_id.clone(),
        blob_id: id(ResourceKind::InternalBlob),
        artifact: artifact.clone(),
        storage_binding_digest: storage_binding_digest.clone(),
        encryption_domain_id: id(ResourceKind::EncryptionDomain),
        authorization_digest: digest('c'),
        drift_after_first,
        calls: AtomicUsize::new(0),
    });
    let store = Arc::new(FixtureStore {
        binding: storage_binding_digest,
        metadata: Mutex::new(ArtifactObjectMetadata {
            object_generation: metadata_generation.to_owned(),
            byte_length: u64::try_from(bytes.len()).unwrap(),
        }),
        bytes: Mutex::new(bytes.to_vec()),
    });
    let broker = BrokeredSandboxArtifactBroker::new(
        authority,
        Arc::new(FixtureUnsealer {
            plaintext: Mutex::new(plaintext),
        }),
        InstalledArtifactObjectStoreCatalog::new(vec![store.clone()]).unwrap(),
        limits,
    )
    .unwrap();
    Fixture {
        broker,
        request: WasiArtifactReadRequest {
            tenant_id,
            sandbox_job_id: id(ResourceKind::Job),
            request_digest: digest('d'),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
            lease_generation: 1,
            artifact,
            purpose: WasiArtifactReadPurpose::RuntimeBundle,
            read_grant: None,
            maximum_bytes: 1024,
            deadline: Utc::now() + ChronoDuration::minutes(1),
        },
        store,
    }
}

fn locator(binding: &Sha256Digest, _generation: &str) -> Vec<u8> {
    serde_jcs::to_vec(&serde_json::json!({
        "backend": "s3",
        "object_key": "opaque/object-1",
        "schema_version": 1,
        "storage_binding_digest": binding,
    }))
    .unwrap()
}

#[tokio::test]
async fn exact_object_is_returned_only_after_second_authorization() {
    let bytes = b"wasm-module";
    let locator_bytes = locator(&digest('b'), "version-1");
    assert!(parse_locator(&locator_bytes).is_ok());
    let fixture = fixture(bytes, locator_bytes, false, "version-1");
    assert_eq!(
        WasiArtifactBroker::read_exact(&fixture.broker, fixture.request)
            .await
            .unwrap(),
        bytes
    );
}

#[tokio::test]
async fn audience_permit_is_held_until_the_response_lease_is_dropped() {
    let bytes = b"wasm-module";
    let fixture = fixture_with_limits(
        bytes,
        locator(&digest('b'), "version-1"),
        false,
        "version-1",
        ArtifactBrokerLimits {
            maximum_in_flight: 1,
            ..ArtifactBrokerLimits::default()
        },
    );
    let held = fixture
        .broker
        .read_wasi_for_response(fixture.request.clone())
        .await
        .unwrap();
    assert_eq!(held.as_bytes(), bytes);
    assert_eq!(
        fixture
            .broker
            .read_wasi_for_response(fixture.request.clone())
            .await
            .err(),
        Some(WasiArtifactBrokerError::Unavailable)
    );

    drop(held);
    assert_eq!(
        fixture
            .broker
            .read_wasi_for_response(fixture.request)
            .await
            .unwrap()
            .as_bytes(),
        bytes
    );
}

#[tokio::test]
async fn authoritative_generation_and_post_io_authority_drift_fail_closed() {
    let bytes = b"wasm-module";
    let wrong_generation = fixture(
        bytes,
        locator(&digest('b'), "version-1"),
        false,
        "version-2",
    );
    assert_eq!(
        WasiArtifactBroker::read_exact(&wrong_generation.broker, wrong_generation.request).await,
        Err(WasiArtifactBrokerError::Integrity)
    );

    let drift = fixture(bytes, locator(&digest('b'), "version-1"), true, "version-1");
    assert_eq!(
        WasiArtifactBroker::read_exact(&drift.broker, drift.request).await,
        Err(WasiArtifactBrokerError::Denied)
    );
}

#[tokio::test]
async fn noncanonical_locator_and_content_digest_mismatch_fail_closed() {
    let bytes = b"wasm-module";
    let noncanonical = fixture(
        bytes,
        format!(
            "{{ \"backend\":\"s3\",\"object_key\":\"opaque/object-1\",\"schema_version\":1,\"storage_binding_digest\":\"{}\"}}",
            digest('b')
        )
        .into_bytes(),
        false,
        "version-1",
    );
    assert_eq!(
        WasiArtifactBroker::read_exact(&noncanonical.broker, noncanonical.request).await,
        Err(WasiArtifactBrokerError::Integrity)
    );

    let corrupt = fixture(
        bytes,
        locator(&digest('b'), "version-1"),
        false,
        "version-1",
    );
    *corrupt.store.bytes.lock().unwrap() = b"wasm-modulf".to_vec();
    assert_eq!(
        WasiArtifactBroker::read_exact(&corrupt.broker, corrupt.request).await,
        Err(WasiArtifactBrokerError::Integrity)
    );
}
