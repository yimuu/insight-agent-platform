use super::*;
use chrono::Duration as ChronoDuration;
use insight_platform_artifacts::{
    ArtifactObjectReadAuthority, ArtifactObjectReadAuthorityError, AuthorizedArtifactObjectRead,
    EncryptedArtifactObjectReference,
};
use insight_platform_contracts::{ArtifactRef, DataClassification, ResourceId, ResourceKind};
use insight_platform_models::{
    ExactInvocationValueRef, InvocationValueStorage, JobFence, ModelArtifactBroker,
    ModelArtifactReadRequest,
};
use insight_platform_sandbox::{
    MicroVmArtifactBroker, MicroVmArtifactReadPurpose, MicroVmArtifactReadRequest,
    MicroVmSandboxWorkloadKind, WasiArtifactReadPurpose, WasiArtifactReadRequest,
};
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

#[async_trait]
impl ArtifactObjectReadAuthority<MicroVmArtifactReadRequest> for FixtureAuthority {
    async fn authorize_object_read(
        &self,
        _request: &MicroVmArtifactReadRequest,
    ) -> Result<AuthorizedArtifactObjectRead, ArtifactObjectReadAuthorityError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.projection(self.drift_after_first && call > 0))
    }
}

#[async_trait]
impl ArtifactObjectReadAuthority<ModelArtifactReadRequest> for FixtureAuthority {
    async fn authorize_object_read(
        &self,
        _request: &ModelArtifactReadRequest,
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
        authority.clone(),
        authority,
        Arc::new(FixtureUnsealer {
            plaintext: Mutex::new(plaintext),
        }),
        InstalledArtifactObjectStoreCatalog::new(vec![store.clone()]).unwrap(),
        ArtifactBrokerLimits::default(),
    )
    .unwrap();
    Fixture {
        broker,
        request: WasiArtifactReadRequest {
            tenant_id,
            sandbox_job_id: id(ResourceKind::SandboxJob),
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

fn locator(binding: &Sha256Digest, generation: &str) -> Vec<u8> {
    serde_jcs::to_vec(&serde_json::json!({
        "backend": "s3",
        "object_generation": generation,
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
async fn micro_vm_reads_share_the_exact_object_pipeline() {
    let bytes = b"microvm-runtime";
    let fixture = fixture(
        bytes,
        locator(&digest('b'), "version-1"),
        false,
        "version-1",
    );
    let request = MicroVmArtifactReadRequest {
        workload_kind: MicroVmSandboxWorkloadKind::CapabilityExecution,
        tenant_id: fixture.request.tenant_id.clone(),
        sandbox_job_id: fixture.request.sandbox_job_id.clone(),
        request_digest: fixture.request.request_digest.clone(),
        executor_worker_process_generation_id: fixture.request.worker_process_generation_id.clone(),
        provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
        sandbox_identity_digest: digest('e'),
        lease_generation: fixture.request.lease_generation,
        artifact: fixture.request.artifact.clone(),
        purpose: MicroVmArtifactReadPurpose::RuntimeBundle,
        read_grant: None,
        maximum_bytes: fixture.request.maximum_bytes,
        deadline: fixture.request.deadline,
    };

    assert_eq!(
        MicroVmArtifactBroker::read_exact(&fixture.broker, request)
            .await
            .unwrap(),
        bytes
    );
}

#[tokio::test]
async fn model_reads_share_the_exact_object_pipeline() {
    let value = serde_json::json!({"messages": []});
    let bytes = serde_jcs::to_vec(&value).unwrap();
    let tenant_id = id(ResourceKind::Tenant);
    let artifact = ArtifactRef::new(
        id(ResourceKind::Artifact),
        sha256(&bytes),
        u64::try_from(bytes.len()).unwrap(),
        "application/json",
        DataClassification::Internal,
        Some("model-request.json".to_owned()),
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
        drift_after_first: false,
        calls: AtomicUsize::new(0),
    });
    let store = Arc::new(FixtureStore {
        binding: storage_binding_digest.clone(),
        metadata: Mutex::new(ArtifactObjectMetadata {
            object_generation: "version-1".to_owned(),
            byte_length: u64::try_from(bytes.len()).unwrap(),
        }),
        bytes: Mutex::new(bytes.clone()),
    });
    let broker = BrokeredModelArtifactBroker::new(
        authority,
        Arc::new(FixtureUnsealer {
            plaintext: Mutex::new(locator(&storage_binding_digest, "version-1")),
        }),
        InstalledArtifactObjectStoreCatalog::new(vec![store]).unwrap(),
        ArtifactBrokerLimits::default(),
    )
    .unwrap();
    let exact = ExactInvocationValueRef {
        schema_version: 1,
        value_id: id(ResourceKind::RunValue),
        run_id: id(ResourceKind::Run),
        producing_node_id: Some(id(ResourceKind::NodeExecution)),
        value_kind: "model_request".to_owned(),
        classification: DataClassification::Internal,
        schema_digest: digest('d'),
        content_digest: artifact.content_digest().clone(),
        storage: InvocationValueStorage::Artifact {
            artifact: artifact.clone(),
        },
    };
    let request = ModelArtifactReadRequest {
        schema_version: 1,
        tenant_id,
        model_turn_id: id(ResourceKind::ModelTurn),
        job_id: id(ResourceKind::Job),
        exact,
        artifact_link_id: id(ResourceKind::ArtifactLink),
        fence: JobFence {
            expected_version: 2,
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
            lease_generation: 1,
            token_digest: digest('e'),
        },
        request_digest: digest('f'),
        maximum_bytes: bytes.len(),
        deadline: Utc::now() + ChronoDuration::minutes(1),
    };

    assert_eq!(
        ModelArtifactBroker::read_exact(&broker, request)
            .await
            .unwrap(),
        bytes
    );
}

#[tokio::test]
async fn locator_generation_and_post_io_authority_drift_fail_closed() {
    let bytes = b"wasm-module";
    let wrong_locator = fixture(
        bytes,
        locator(&digest('b'), "version-2"),
        false,
        "version-1",
    );
    assert_eq!(
        WasiArtifactBroker::read_exact(&wrong_locator.broker, wrong_locator.request).await,
        Err(WasiArtifactBrokerError::Integrity)
    );

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
            "{{ \"backend\":\"s3\",\"object_generation\":\"version-1\",\"object_key\":\"opaque/object-1\",\"schema_version\":1,\"storage_binding_digest\":\"{}\"}}",
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
