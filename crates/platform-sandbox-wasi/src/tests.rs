use super::*;
use chrono::Duration as ChronoDuration;
use insight_platform_contracts::{
    checked_in_hard_limit_profile, ArtifactRef, AuthoringPackage, CapabilityBackendBinding,
    CapabilityDeploymentClosure, CodeTrustClass, DataClassification, ExactDeploymentRef,
    ExactSandboxProfileBinding, ExactVersionRef, ResourceKind, SandboxArtifactIoPolicyDocument,
    SandboxCleanupPolicy, SandboxIsolationPolicyDocument, SandboxJobState,
    SandboxNetworkPolicyDocument, SandboxPackageResourceSpec, SandboxProfileResourceSpec,
    SandboxResourcePolicyDocument, SandboxRuntimeResourceSpec,
};
use insight_platform_sandbox::{
    SafeSandboxTraceContext, SandboxCommandLimits, SandboxExecutionPolicyClosure,
    SandboxProcessGenerationAbsenceEvidence, SandboxResourceEnvelope, ScopedSandboxCallback,
};
use std::sync::atomic::{AtomicUsize, Ordering};

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c8-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn sha(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn artifact(suffix: u16, bytes: &[u8], media_type: &str) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        sha256_digest(bytes),
        u64::try_from(bytes.len()).unwrap(),
        media_type,
        DataClassification::Internal,
        Some(format!("artifact-{suffix}.bin")),
    )
    .unwrap()
}

fn placeholder_artifact(suffix: u16, character: char) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        sha(character),
        16,
        "application/octet-stream",
        DataClassification::Internal,
        Some(format!("artifact-{suffix}.bin")),
    )
    .unwrap()
}

fn authoring(suffix: u16, character: char) -> AuthoringPackage {
    AuthoringPackage {
        artifact: placeholder_artifact(suffix, character),
        manifest_digest: sha(character),
    }
}

fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn deployment(kind: ResourceKind, suffix: u16, character: char) -> ExactDeploymentRef {
    ExactDeploymentRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn policy_closure() -> SandboxExecutionPolicyClosure {
    SandboxExecutionPolicyClosure {
        isolation: SandboxIsolationPolicyDocument {
            schema_version: 2,
            minimum_isolation: SandboxIsolationClass::Wasm,
            allowed_runtime_families: vec![SandboxRuntimeFamily::WasmWasi],
            allowed_trust_classes: vec![CodeTrustClass::BuiltIn],
            fresh_jail_per_job: true,
            deny_runtime_fallback: true,
            deny_host_devices: true,
        },
        resource: SandboxResourcePolicyDocument {
            schema_version: 3,
            maximum_cpu_millicores: 1_000,
            maximum_memory_mebibytes: 1_024,
            maximum_pids: 64,
            maximum_files: 256,
            maximum_io_bytes: 16_777_216,
            maximum_stdout_bytes: 1_048_576,
            maximum_stderr_bytes: 1_048_576,
            maximum_result_bytes: 2_097_152,
            maximum_artifact_output_bytes: 2_097_152,
            maximum_network_connections: 32,
            maximum_network_request_bytes: 1_048_576,
            maximum_network_response_bytes: 1_048_576,
            maximum_startup_milliseconds: 60_000,
            maximum_idle_milliseconds: 60_000,
            maximum_wall_milliseconds: 60_000,
            maximum_cleanup_milliseconds: 10_000,
            maximum_wasm_fuel: Some(10_000_000),
            maximum_wasm_memory_pages: Some(16_384),
            swap_disabled: true,
        },
        network: SandboxNetworkPolicyDocument {
            schema_version: 1,
            destinations: vec![],
            maximum_redirects: 0,
            maximum_dns_answers: 8,
            maximum_response_bytes: 1_048_576,
            require_https: true,
            require_tls12_or_newer: true,
            deny_private_addresses: true,
            deny_link_local_addresses: true,
            deny_metadata_addresses: true,
            deny_proxy_environment: true,
            deny_connect_tunnel: true,
            deny_listen: true,
            deny_udp: true,
        },
        artifact_io: SandboxArtifactIoPolicyDocument {
            schema_version: 1,
            allowed_input_media_types: vec![
                "application/json".to_owned(),
                "application/octet-stream".to_owned(),
            ],
            allowed_output_media_types: vec!["application/json".to_owned()],
            maximum_input_artifacts: 64,
            maximum_output_artifacts: 64,
            scanner_contract_digest: sha('8'),
            verification_evidence_ttl_milliseconds: 60_000,
            verification_retry_backoff_milliseconds: 1_000,
            write_storage_binding_digest: sha('9'),
            encryption_domain_id: id(ResourceKind::EncryptionDomain, 0x0f06),
            deny_symlink: true,
            deny_hardlink: true,
            deny_device: true,
            deny_fifo: true,
            deny_socket: true,
            deny_sparse_file: true,
            archive_expansion_disabled: true,
        },
        secret_resolution: None,
    }
}

fn success_module() -> Vec<u8> {
    let output = r#"{"schema_version":1,"value":{"answer":42}}"#;
    let packed = (128_u64 << 32) | u64::try_from(output.len()).unwrap();
    wat::parse_str(format!(
        r#"(module
          (memory (export "memory") 1 16)
          (global $heap (mut i32) (i32.const 4096))
          (func (export "insight_alloc") (param $length i32) (result i32)
            (local $pointer i32)
            global.get $heap
            local.set $pointer
            global.get $heap
            local.get $length
            i32.add
            global.set $heap
            local.get $pointer)
          (func (export "run") (param i32 i32) (result i64)
            i64.const {packed})
          (data (i32.const 128) "{escaped}"))"#,
        escaped = output.replace('"', "\\\"")
    ))
    .unwrap()
}

fn infinite_module() -> Vec<u8> {
    wat::parse_str(
        r#"(module
          (memory (export "memory") 1 16)
          (func (export "insight_alloc") (param i32) (result i32) i32.const 4096)
          (func (export "run") (param i32 i32) (result i64)
            (loop $forever br $forever)
            i64.const 0))"#,
    )
    .unwrap()
}

fn imported_module() -> Vec<u8> {
    wat::parse_str(
        r#"(module
          (import "wasi_snapshot_preview1" "clock_time_get"
            (func $clock (param i32 i64 i32) (result i32)))
          (memory (export "memory") 1 16)
          (func (export "insight_alloc") (param i32) (result i32) i32.const 4096)
          (func (export "run") (param i32 i32) (result i64) i64.const 0))"#,
    )
    .unwrap()
}

fn simd_module() -> Vec<u8> {
    wat::parse_str(
        r#"(module
          (memory (export "memory") 1 16)
          (func (export "insight_alloc") (param i32) (result i32) i32.const 4096)
          (func (export "run") (param i32 i32) (result i64)
            v128.const i32x4 0 0 0 0
            drop
            i64.const 0))"#,
    )
    .unwrap()
}

fn request(now: DateTime<Utc>, module: &[u8]) -> SandboxExecutionRequest {
    let runtime_revision = exact(ResourceKind::SandboxRuntimeRevision, 10, '1');
    let package_revision = exact(ResourceKind::SandboxPackageRevision, 11, '2');
    let profile_revision = exact(ResourceKind::SandboxProfileRevision, 12, '3');
    let profile_binding = ExactSandboxProfileBinding {
        deployment: deployment(ResourceKind::SandboxProfileDeployment, 18, '8'),
        revision: profile_revision.clone(),
    };
    let isolation_policy = exact(ResourceKind::PolicyRevision, 13, '4');
    let resource_policy = exact(ResourceKind::PolicyRevision, 14, '5');
    let network_policy = exact(ResourceKind::PolicyRevision, 15, '6');
    let artifact_io_policy = exact(ResourceKind::PolicyRevision, 16, '7');
    let runtime = SandboxRuntimeResourceSpec {
        authoring_package: authoring(1, '1'),
        contract_digest: sha('2'),
        dependency_versions: vec![],
        policy_versions: vec![],
        runtime_family: SandboxRuntimeFamily::WasmWasi,
        runtime_version: WASI_ABI_V1_RUNTIME_VERSION.to_owned(),
        image_or_module_digest: sha('3'),
        supported_isolation: vec![SandboxIsolationClass::Wasm],
        abi: SandboxAbiVersion::V1,
        builtin_modules_manifest_digest: sha('5'),
        sbom_artifact: placeholder_artifact(2, '6'),
        provenance_evidence: placeholder_artifact(3, '7'),
        semantic_digest: sha('8'),
    };
    let runtime_bundle_artifact = artifact(6, module, "application/wasm");
    let package = SandboxPackageResourceSpec {
        authoring_package: authoring(4, '9'),
        contract_digest: sha('a'),
        dependency_versions: vec![runtime_revision.clone()],
        policy_versions: vec![],
        source_artifact: placeholder_artifact(5, 'b'),
        source_digest: sha('b'),
        runtime_revision: runtime_revision.clone(),
        entrypoint_kind: SandboxEntrypointKind::WasmExport,
        entrypoint: WASI_ABI_V1_ENTRYPOINT.to_owned(),
        dependency_lock_digest: sha('c'),
        runtime_bundle_artifact,
        build_evidence: placeholder_artifact(7, 'e'),
        trust_class: CodeTrustClass::BuiltIn,
        package_digest: sha('f'),
    };
    let profile = SandboxProfileResourceSpec {
        authoring_package: authoring(8, '1'),
        contract_digest: sha('2'),
        dependency_versions: vec![],
        policy_versions: vec![
            isolation_policy.clone(),
            resource_policy.clone(),
            network_policy.clone(),
            artifact_io_policy.clone(),
        ],
        allowed_trust_classes: vec![CodeTrustClass::BuiltIn],
        allowed_runtime_families: vec![SandboxRuntimeFamily::WasmWasi],
        minimum_isolation: SandboxIsolationClass::Wasm,
        isolation_policy,
        resource_policy: resource_policy.clone(),
        network_policy: network_policy.clone(),
        artifact_io_policy: artifact_io_policy.clone(),
        secret_policy: None,
        cleanup: SandboxCleanupPolicy::SingleUseDestroy,
        max_job_duration_milliseconds: 60_000,
        semantic_digest: sha('3'),
    };
    let tenant_id = id(ResourceKind::Tenant, 20);
    let sandbox_job_id = id(ResourceKind::Job, 23);
    let invocation_id = id(ResourceKind::CapabilityInvocation, 22);
    let deadline = now + ChronoDuration::minutes(1);
    let callback = ScopedSandboxCallback {
        schema_version: 1,
        tenant_id: tenant_id.clone(),
        sandbox_job_id: sandbox_job_id.clone(),
        invocation_id: invocation_id.clone(),
        attempt_no: 1,
        audience_identity_digest: sha('4'),
        expires_at: deadline,
        binding_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let capability_deployment_closure = CapabilityDeploymentClosure {
        implementation: exact(ResourceKind::CapabilityImplementationRevision, 25, '8'),
        interface: exact(ResourceKind::CapabilityInterfaceRevision, 26, '9'),
        backend: CapabilityBackendBinding::Sandbox {
            runtime: runtime_revision.clone(),
            package: package_revision.clone(),
            profile: profile_binding.clone(),
            isolation: SandboxIsolationClass::Wasm,
            network_policy,
            resource_policy,
            artifact_io_policy,
            secret_policy: None,
        },
        secret_bindings: vec![],
        policies: vec![],
        conformance_evidence: placeholder_artifact(27, 'a'),
    };
    SandboxExecutionRequest {
        schema_version: 1,
        protocol_version: SandboxAbiVersion::V1,
        tenant_id,
        sandbox_job_id,
        invocation_id,
        job_id: id(ResourceKind::Job, 23),
        expected_invocation_version: 1,
        attempt_no: 1,
        retry_backoff_milliseconds: 100,
        lease_generation: 1,
        capability_deployment: deployment(ResourceKind::CapabilityDeployment, 24, '5'),
        execution_source: insight_platform_sandbox::SandboxExecutionSource::SandboxCapability {
            capability_deployment_closure: Box::new(capability_deployment_closure),
        },
        runtime_revision,
        runtime,
        package_revision,
        package,
        profile_binding,
        profile,
        policies: Box::new(policy_closure()),
        isolation_class: SandboxIsolationClass::Wasm,
        executor_worker_manifest_digest: sha('6'),
        isolation_backend_contract_digest: sha('7'),
        effect: Effect::Pure,
        classification: DataClassification::Internal,
        input_value_id: id(ResourceKind::RunValue, 38),
        input_schema_digest: sha('e'),
        input_ref: ValueRef::Inline {
            value: serde_json::json!({"question": "life"}),
        },
        output_value_id: id(ResourceKind::RunValue, 23),
        output_schema_digest: sha('f'),
        artifact_grants: vec![],
        secret_grants: vec![],
        network_mode: SandboxNetworkMode::None,
        resources: SandboxResourceEnvelope {
            cpu_millicores: 100,
            memory_mebibytes: 64,
            pids: 1,
            files: 1,
            io_bytes: 1_048_576,
            stdout_bytes: 1_024,
            stderr_bytes: 1_024,
            result_bytes: 1_048_576,
            artifact_output_bytes: 0,
            network_connections: 0,
            network_request_bytes: 0,
            network_response_bytes: 0,
            startup_milliseconds: 10_000,
            idle_milliseconds: 30_000,
            wall_milliseconds: 30_000,
            cleanup_milliseconds: 1_000,
            wasm_fuel: Some(1_000_000),
            wasm_memory_pages: Some(1_024),
        },
        deadline,
        callback,
        trace_context: SafeSandboxTraceContext {
            trace_id_digest: sha('8'),
            parent_span_id_digest: sha('9'),
        },
        request_digest: sha('0'),
    }
    .seal()
    .unwrap()
}

#[derive(Default)]
struct MemoryArtifactBroker {
    values: Mutex<BTreeMap<ResourceId, Vec<u8>>>,
    reads: AtomicUsize,
}

impl MemoryArtifactBroker {
    fn with_module(module: &[u8]) -> Self {
        Self {
            values: Mutex::new(BTreeMap::from([(
                id(ResourceKind::Artifact, 6),
                module.to_vec(),
            )])),
            reads: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl WasiArtifactBroker for MemoryArtifactBroker {
    async fn read_exact(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<Vec<u8>, WasiArtifactBrokerError> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        self.values
            .lock()
            .unwrap()
            .get(request.artifact.artifact_id())
            .cloned()
            .ok_or(WasiArtifactBrokerError::NotFound)
    }
}

struct ExactValueValidator;

#[async_trait]
impl WasiValueValidator for ExactValueValidator {
    async fn validate(
        &self,
        request: WasiValueValidationRequest,
    ) -> Result<Sha256Digest, WasiValueValidationError> {
        let valid = request.worker_process_generation_id
            == id(ResourceKind::WorkerProcessGeneration, 70)
            && request.lease_generation == 1
            && match request.direction {
                WasiValueDirection::Input => {
                    request.value == serde_json::json!({"question": "life"})
                }
                WasiValueDirection::Output => request.value == serde_json::json!({"answer": 42}),
            };
        if !valid {
            return Err(WasiValueValidationError::Invalid);
        }
        Ok(canonical_digest(&serde_json::json!({
            "direction": request.direction,
            "schema_digest": request.schema_digest,
            "value": request.value,
        }))
        .unwrap()
        .parse()
        .unwrap())
    }
}

#[derive(Default)]
struct RecordingGrantRevoker {
    calls: AtomicUsize,
}

struct RecordingProcessIsolation {
    calls: AtomicUsize,
    observed_at: DateTime<Utc>,
}

#[async_trait]
impl SandboxProcessGenerationIsolation for RecordingProcessIsolation {
    async fn prove_absent(
        &self,
        request: ProveSandboxProcessGenerationAbsent,
    ) -> Result<SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolationError>
    {
        self.calls.fetch_add(1, Ordering::AcqRel);
        SandboxProcessGenerationAbsenceEvidence {
            schema_version: 1,
            tenant_id: request.tenant_id,
            sandbox_job_id: request.sandbox_job_id,
            request_digest: request.request_digest.clone(),
            previous_worker_process_generation_id: request.previous_worker_process_generation_id,
            executor_identity_digest: request.executor_identity_digest,
            attestor_identity_digest: sha('f'),
            attestor_route: request.attestor_route,
            disposition:
                insight_platform_sandbox::SandboxProcessGenerationIsolationDisposition::ProcessAbsent,
            observed_at: self.observed_at,
            evidence_digest: evidence_digest(
                "test_process_generation_absent",
                &request.request_digest,
                1,
                1,
            ),
        }
        .seal()
    }
}

#[async_trait]
impl WasiGrantRevoker for RecordingGrantRevoker {
    async fn revoke_exact(
        &self,
        request: RevokeWasiSandboxGrants,
    ) -> Result<WasiGrantRevocationEvidence, WasiGrantRevocationError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(WasiGrantRevocationEvidence {
            evidence_digest: evidence_digest(
                "test_revoke",
                &request.request_digest,
                request.attempt_no,
                request.lease_generation,
            ),
        })
    }
}

struct FixedClock(DateTime<Utc>);

impl WasiExecutorClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

fn backend(
    now: DateTime<Utc>,
    module: &[u8],
) -> (
    Arc<WasmtimeSandboxExecutorBackend>,
    Arc<MemoryArtifactBroker>,
    Arc<RecordingGrantRevoker>,
    Arc<RecordingProcessIsolation>,
) {
    let artifacts = Arc::new(MemoryArtifactBroker::with_module(module));
    let revoker = Arc::new(RecordingGrantRevoker::default());
    let process_isolation = Arc::new(RecordingProcessIsolation {
        calls: AtomicUsize::new(0),
        observed_at: now,
    });
    let backend = Arc::new(
        WasmtimeSandboxExecutorBackend::new(
            WasiExecutorBackendConfig::production(
                InstalledSandboxBackendDescriptor {
                    backend_kind: SandboxIsolationBackendKind::Wasi,
                    isolation_class: SandboxIsolationClass::Wasm,
                    worker_manifest_digest: sha('6'),
                    backend_contract_digest: sha('7'),
                },
                id(ResourceKind::WorkerProcessGeneration, 70),
            ),
            artifacts.clone(),
            Arc::new(ExactValueValidator),
            revoker.clone(),
            process_isolation.clone(),
            Arc::new(FixedClock(now)),
        )
        .unwrap(),
    );
    (backend, artifacts, revoker, process_isolation)
}

#[tokio::test]
async fn exact_abi_executes_canonical_json_and_destroys_single_use_store() {
    let now = DateTime::parse_from_rfc3339("2026-08-13T09:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let module = success_module();
    let request = request(now, &module);
    request
        .validate_at(
            now,
            SandboxCommandLimits::from_profile(&checked_in_hard_limit_profile()).unwrap(),
        )
        .unwrap();
    let (backend, artifacts, revoker, _) = backend(now, &module);
    let prepared = backend.prepare(request.clone()).await.unwrap();
    let running = backend
        .start(request.clone(), prepared.clone())
        .await
        .unwrap();
    let collected = backend.collect(request.clone(), running).await.unwrap();
    let SandboxExecutionOutcome::Completed(output) = collected.outcome else {
        panic!("expected completed WASI output");
    };
    assert_eq!(
        output.value,
        ValueRef::Inline {
            value: serde_json::json!({"answer": 42})
        }
    );
    assert!(output.usage.wasm_fuel_consumed.unwrap() > 0);
    let cleanup = backend
        .destroy(DestroySandbox {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            sandbox_identity_digest: prepared.sandbox_identity_digest,
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            effect: request.effect,
            network_mode: request.network_mode,
        })
        .await
        .unwrap();
    assert_eq!(cleanup.disposition, SandboxCleanupDisposition::Destroyed);
    assert_eq!(artifacts.reads.load(Ordering::Acquire), 1);
    assert_eq!(revoker.calls.load(Ordering::Acquire), 1);
    assert!(backend.lookup(&request.request_digest).is_err());
}

#[tokio::test]
async fn io_budget_is_enforced_instead_of_clamped_into_a_success() {
    let now = Utc::now();
    let module = success_module();
    let mut request = request(now, &module);
    request.resources.io_bytes = 1;
    request = request.seal().unwrap();
    let (backend, _, _, _) = backend(now, &module);
    let prepared = backend.prepare(request.clone()).await.unwrap();
    let running = backend
        .start(request.clone(), prepared.clone())
        .await
        .unwrap();
    let collected = backend.collect(request.clone(), running).await.unwrap();
    let SandboxExecutionOutcome::Failed(failure) = collected.outcome else {
        panic!("expected bounded I/O failure");
    };
    assert_eq!(failure.safe_code, "sandbox_wasi_io_exhausted");
    assert_eq!(failure.resource_violation.as_deref(), Some("io_bytes"));
    assert_eq!(collected.usage.io_bytes, request.resources.io_bytes);
}

#[tokio::test]
async fn restarted_executor_requires_process_generation_absence_proof() {
    let now = Utc::now();
    let module = success_module();
    let request = request(now - ChronoDuration::minutes(2), &module);
    let previous_generation = id(ResourceKind::WorkerProcessGeneration, 71);
    let (backend, _, revoker, process_isolation) = backend(now, &module);
    let recovery = backend
        .recover_expired_lease(ExpiredSandboxLease {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            invocation_id: request.invocation_id.clone(),
            job_id: request.job_id.clone(),
            request: request.clone(),
            observed_job_version: 2,
            observed_lease_generation: request.lease_generation,
            previous_worker_process_generation_id: previous_generation,
            lease_expires_at: request.deadline,
            database_observed_at: now,
            physical_state: SandboxJobState::Running,
            executor_identity_digest: Some(request.executor_worker_manifest_digest.clone()),
            attestor_route: Some("https://10.0.0.7:9443".parse().unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(
        recovery.cleanup.disposition,
        SandboxCleanupDisposition::Destroyed
    );
    assert_eq!(process_isolation.calls.load(Ordering::Acquire), 1);
    assert_eq!(revoker.calls.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn module_with_any_import_is_rejected_before_start() {
    let now = Utc::now();
    let module = imported_module();
    let request = request(now, &module);
    let (backend, _, revoker, _) = backend(now, &module);
    let failure = backend.prepare(request).await.unwrap_err();
    assert_eq!(failure.stage, SandboxBackendFailureStage::Preparing);
    assert_eq!(failure.safe_code, "sandbox_wasi_module_invalid");
    assert!(!failure.execution_may_have_started);
    assert_eq!(revoker.calls.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn post_wasm1_instruction_is_rejected_before_start() {
    let now = Utc::now();
    let module = simd_module();
    let request = request(now, &module);
    let (backend, _, revoker, _) = backend(now, &module);
    let failure = backend.prepare(request).await.unwrap_err();
    assert_eq!(failure.safe_code, "sandbox_wasi_module_invalid");
    assert_eq!(revoker.calls.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn fuel_exhaustion_is_a_bounded_safe_failure() {
    let now = Utc::now();
    let module = infinite_module();
    let request = request(now, &module);
    let (backend, _, _, _) = backend(now, &module);
    let prepared = backend.prepare(request.clone()).await.unwrap();
    let running = backend
        .start(request.clone(), prepared.clone())
        .await
        .unwrap();
    let collected = backend.collect(request.clone(), running).await.unwrap();
    let SandboxExecutionOutcome::Failed(failure) = collected.outcome else {
        panic!("expected fuel failure");
    };
    assert_eq!(failure.safe_code, "sandbox_wasi_fuel_exhausted");
    assert_eq!(failure.resource_violation.as_deref(), Some("wasm_fuel"));
    assert_eq!(
        collected.usage.wasm_fuel_consumed,
        request.resources.wasm_fuel
    );
    backend
        .destroy(DestroySandbox {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            sandbox_identity_digest: prepared.sandbox_identity_digest,
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            effect: request.effect,
            network_mode: request.network_mode,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_abort_interrupts_running_guest_and_revokes_grants() {
    let now = Utc::now();
    let module = infinite_module();
    let mut request = request(now, &module);
    request.resources.wasm_fuel = Some(1_000_000_000);
    request = request.seal().unwrap();
    let (backend, _, revoker, _) = backend(now, &module);
    let prepared = backend.prepare(request.clone()).await.unwrap();
    let running = backend
        .start(request.clone(), prepared.clone())
        .await
        .unwrap();
    let collecting_backend = Arc::clone(&backend);
    let collecting_request = request.clone();
    let collecting = tokio::spawn(async move {
        collecting_backend
            .collect(collecting_request, running)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let abort = backend
        .abort(AbortSandboxExecution {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            reason: insight_platform_sandbox::SandboxStopReason::Cancelled,
            effect: request.effect,
            network_mode: request.network_mode,
            execution_deadline: request.deadline,
        })
        .await
        .unwrap();
    assert!(abort.termination.process_tree_terminated);
    assert!(abort.cleanup.ephemeral_storage_destroyed);
    assert_eq!(revoker.calls.load(Ordering::Acquire), 1);
    let late = tokio::time::timeout(std::time::Duration::from_secs(2), collecting)
        .await
        .expect("epoch interruption must stop the guest")
        .unwrap()
        .unwrap();
    let SandboxExecutionOutcome::Failed(failure) = late.outcome else {
        panic!("interrupted guest must not complete");
    };
    assert_eq!(failure.safe_code, "sandbox_wasi_guest_trapped");
}

#[test]
fn production_config_rejects_non_wasi_or_unbounded_capacity() {
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Gvisor,
        isolation_class: SandboxIsolationClass::SandboxedContainer,
        worker_manifest_digest: sha('6'),
        backend_contract_digest: sha('7'),
    };
    let artifacts = Arc::new(MemoryArtifactBroker::default());
    let result = WasmtimeSandboxExecutorBackend::new(
        WasiExecutorBackendConfig::production(
            descriptor,
            id(ResourceKind::WorkerProcessGeneration, 70),
        ),
        artifacts,
        Arc::new(ExactValueValidator),
        Arc::new(RecordingGrantRevoker::default()),
        Arc::new(RecordingProcessIsolation {
            calls: AtomicUsize::new(0),
            observed_at: Utc::now(),
        }),
        Arc::new(FixedClock(Utc::now())),
    );
    assert!(result.is_err());
}

#[test]
fn engine_feature_contract_is_supported() {
    if let Err(error) = new_engine() {
        panic!("WASM1 engine configuration failed: {error:?}");
    }
}

#[test]
fn request_rejects_wasm_pages_above_the_memory_envelope() {
    let now = Utc::now();
    let module = success_module();
    let mut request = request(now, &module);
    request.resources.wasm_memory_pages = Some(1_025);
    request = request.seal().unwrap();
    let error = request
        .validate_at(
            now,
            SandboxCommandLimits::from_profile(&checked_in_hard_limit_profile()).unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        error,
        insight_platform_sandbox::SandboxContractError::InvalidResourceEnvelope
    );
}
