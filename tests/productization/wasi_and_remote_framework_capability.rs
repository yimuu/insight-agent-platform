use super::*;
use async_trait::async_trait;
use insight_platform_contracts::{
    ArtifactRef, AuthoringPackage, CapabilityBackendBinding, CapabilityDeploymentClosure,
    CodeTrustClass, DataClassification, Effect, ExactDeploymentRef, ExactSandboxProfileBinding,
    ExactVersionRef, ResourceDocument, ResourceId, ResourceKind, SandboxAbiVersion,
    SandboxArtifactIoPolicyDocument, SandboxCleanupPolicy, SandboxEntrypointKind,
    SandboxIsolationClass, SandboxIsolationPolicyDocument, SandboxNetworkPolicyDocument,
    SandboxPackageResourceSpec, SandboxProfileResourceSpec, SandboxResourcePolicyDocument,
    SandboxRuntimeFamily, SandboxRuntimeResourceSpec, Sha256Digest, ValueRef,
};
use insight_platform_sandbox::{
    DestroySandbox, InstalledSandboxBackendDescriptor, ProveSandboxProcessGenerationAbsent,
    RevokeWasiSandboxGrants, SafeSandboxTraceContext, SandboxCleanupDisposition,
    SandboxExecutionOutcome, SandboxExecutionPolicyClosure, SandboxExecutionRequest,
    SandboxExecutionSource, SandboxExecutorBackend, SandboxIsolationBackendKind,
    SandboxNetworkMode, SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolation,
    SandboxProcessGenerationIsolationError, SandboxResourceEnvelope, ScopedSandboxCallback,
    WasiArtifactBroker, WasiArtifactBrokerError, WasiArtifactReadRequest, WasiGrantRevocationError,
    WasiGrantRevocationEvidence, WasiGrantRevoker, WasiValueDirection, WasiValueValidationError,
    WasiValueValidationRequest, WasiValueValidator,
};
use insight_platform_sandbox_wasi::{
    SystemWasiExecutorClock, WasiExecutorBackendConfig, WasmtimeSandboxExecutorBackend,
    WASI_ABI_V1_ENTRYPOINT, WASI_ABI_V1_RUNTIME_VERSION,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

pub(super) struct WasiFrameworkEvidence {
    pub run_id: String,
    pub console_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

impl WasiFrameworkEvidence {
    pub(super) fn mark_console_passed(&mut self) {
        self.console_passed = true;
    }

    pub(super) fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "wasi-and-remote-framework-capability",
            "contract_profile": "insight.platform/v1",
            "profile": "full",
            "automation_layer": "P3",
            "source_revision": revision,
            "environment": {"os": env::consts::OS, "architecture": env::consts::ARCH, "fresh_profile": true},
            "started_at": self.started_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "finished_at": self.finished_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "status": if self.console_passed { "passed" } else { "incomplete" },
            "entrypoints": [
                check("cli", "passed", "public apply/run/watch/result completed for the independent remote framework Capability"),
                check("http_fixture", "passed", "the bounded framework-neutral reference service was reached only through the exact Egress Deployment"),
                if self.console_passed {
                    check("console", "passed", "real headless Chromium read the exact successful framework Run")
                } else {
                    check("console", "not_run", "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to read the exact framework Run")
                },
            ],
            "assertions": [
                check("wasi_limit", "passed", "the production Wasmtime adapter executed the closed WASI ABI with explicit fuel, memory, I/O and wall limits"),
                check("remote_framework_result", "passed", "the remote framework service returned a bounded typed Inline result"),
                check("no_platform_database_access", "passed", "the remote service process received no database environment or platform-internal credential"),
            ],
            "failure_probes": [
                check("wasi_limit_rejection", "passed", "an infinite WASM module exhausted exact fuel and returned sandbox_wasi_fuel_exhausted"),
                check("framework_network_rejection", "passed", "removing the exact Egress catalog rejected the framework call before fixture dispatch"),
            ],
        })
    }
}

fn new_id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}

fn digest(domain: &str) -> Sha256Digest {
    canonical_digest(&json!({"domain": domain, "schema_version": 1}))
        .parse()
        .unwrap()
}

fn bytes_digest(bytes: &[u8]) -> Sha256Digest {
    let digest = sha2::Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded.parse().unwrap()
}

fn exact(kind: ResourceKind, domain: &str) -> ExactVersionRef {
    ExactVersionRef::new(new_id(kind), digest(domain)).unwrap()
}

fn deployment(kind: ResourceKind, domain: &str) -> ExactDeploymentRef {
    ExactDeploymentRef::new(new_id(kind), digest(domain)).unwrap()
}

fn artifact(bytes: &[u8], media_type: &str, name: &str) -> ArtifactRef {
    ArtifactRef::new(
        new_id(ResourceKind::Artifact),
        bytes_digest(bytes),
        u64::try_from(bytes.len()).unwrap(),
        media_type,
        DataClassification::Internal,
        Some(name.to_owned()),
    )
    .unwrap()
}

fn authoring(domain: &str) -> AuthoringPackage {
    let bytes = domain.as_bytes();
    AuthoringPackage {
        artifact: artifact(bytes, "application/json", &format!("{domain}.json")),
        manifest_digest: bytes_digest(bytes),
    }
}

fn policy_closure() -> SandboxExecutionPolicyClosure {
    SandboxExecutionPolicyClosure {
        isolation: SandboxIsolationPolicyDocument {
            schema_version: 1,
            minimum_isolation: SandboxIsolationClass::Wasm,
            allowed_runtime_families: vec![SandboxRuntimeFamily::WasmWasi],
            allowed_trust_classes: vec![CodeTrustClass::BuiltIn],
            fresh_jail_per_job: true,
            deny_runtime_fallback: true,
            deny_host_devices: true,
        },
        resource: SandboxResourcePolicyDocument {
            schema_version: 1,
            maximum_cpu_millicores: 1_000,
            maximum_memory_mebibytes: 1_024,
            maximum_pids: 64,
            maximum_files: 256,
            maximum_io_bytes: 16_777_216,
            maximum_stdout_bytes: 1_048_576,
            maximum_stderr_bytes: 1_048_576,
            maximum_result_bytes: 2_097_152,
            maximum_artifact_output_bytes: 2_097_152,
            maximum_network_connections: 0,
            maximum_network_request_bytes: 0,
            maximum_network_response_bytes: 0,
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
            schema_version: 3,
            allowed_input_media_types: vec!["application/json".to_owned()],
            allowed_output_media_types: vec!["application/json".to_owned()],
            maximum_input_artifacts: 0,
            maximum_output_artifacts: 0,
            scanner_contract_digest: digest("wasi-scanner"),
            verification_evidence_ttl_milliseconds: 60_000,
            verification_retry_backoff_milliseconds: 1_000,
            write_storage_binding_digest: digest("wasi-storage"),
            encryption_domain_id: new_id(ResourceKind::EncryptionDomain),
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
    let output = r#"{"schema_version":1,"value":{"answer":"42"}}"#;
    let packed = (128_u64 << 32) | u64::try_from(output.len()).unwrap();
    wat::parse_str(format!(
        r#"(module
          (memory (export "memory") 1 16)
          (global $heap (mut i32) (i32.const 4096))
          (func (export "insight_alloc") (param $length i32) (result i32)
            (local $pointer i32)
            global.get $heap local.set $pointer
            global.get $heap local.get $length i32.add global.set $heap
            local.get $pointer)
          (func (export "run") (param i32 i32) (result i64) i64.const {packed})
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

fn request(now: chrono::DateTime<Utc>, module: &[u8], domain: &str) -> SandboxExecutionRequest {
    let runtime_revision = exact(
        ResourceKind::SandboxRuntimeRevision,
        &format!("{domain}-runtime"),
    );
    let package_revision = exact(
        ResourceKind::SandboxPackageRevision,
        &format!("{domain}-package"),
    );
    let profile_revision = exact(
        ResourceKind::SandboxProfileRevision,
        &format!("{domain}-profile"),
    );
    let isolation_policy = exact(ResourceKind::PolicyRevision, &format!("{domain}-isolation"));
    let resource_policy = exact(ResourceKind::PolicyRevision, &format!("{domain}-resource"));
    let network_policy = exact(ResourceKind::PolicyRevision, &format!("{domain}-network"));
    let artifact_io_policy = exact(ResourceKind::PolicyRevision, &format!("{domain}-artifact"));
    let profile_binding = ExactSandboxProfileBinding {
        deployment: deployment(
            ResourceKind::SandboxProfileDeployment,
            &format!("{domain}-profile-deployment"),
        ),
        revision: profile_revision.clone(),
    };
    let runtime = SandboxRuntimeResourceSpec {
        authoring_package: authoring(&format!("{domain}-runtime")),
        contract_digest: digest(&format!("{domain}-runtime-contract")),
        dependency_versions: vec![],
        policy_versions: vec![],
        runtime_family: SandboxRuntimeFamily::WasmWasi,
        runtime_version: WASI_ABI_V1_RUNTIME_VERSION.to_owned(),
        image_or_module_digest: digest("wasmtime-46.0.2-module"),
        supported_isolation: vec![SandboxIsolationClass::Wasm],
        abi: SandboxAbiVersion::V1,
        builtin_modules_manifest_digest: digest("wasi-no-builtins"),
        sbom_artifact: artifact(b"wasi-sbom", "application/json", "wasi-sbom.json"),
        provenance_evidence: artifact(
            b"wasi-provenance",
            "application/json",
            "wasi-provenance.json",
        ),
        semantic_digest: digest(&format!("{domain}-runtime-semantic")),
    };
    let package = SandboxPackageResourceSpec {
        authoring_package: authoring(&format!("{domain}-package")),
        contract_digest: digest(&format!("{domain}-package-contract")),
        dependency_versions: vec![runtime_revision.clone()],
        policy_versions: vec![],
        source_artifact: artifact(module, "application/wasm", "source.wasm"),
        source_digest: bytes_digest(module),
        runtime_revision: runtime_revision.clone(),
        entrypoint_kind: SandboxEntrypointKind::WasmExport,
        entrypoint: WASI_ABI_V1_ENTRYPOINT.to_owned(),
        dependency_lock_digest: digest("wasi-dependency-lock"),
        runtime_bundle_artifact: artifact(module, "application/wasm", "runtime.wasm"),
        build_evidence: artifact(b"wasi-build", "application/json", "wasi-build.json"),
        trust_class: CodeTrustClass::BuiltIn,
        package_digest: bytes_digest(module),
    };
    let profile = SandboxProfileResourceSpec {
        authoring_package: authoring(&format!("{domain}-profile")),
        contract_digest: digest(&format!("{domain}-profile-contract")),
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
        isolation_policy: isolation_policy.clone(),
        resource_policy: resource_policy.clone(),
        network_policy: network_policy.clone(),
        artifact_io_policy: artifact_io_policy.clone(),
        secret_policy: None,
        cleanup: SandboxCleanupPolicy::SingleUseDestroy,
        max_job_duration_milliseconds: 60_000,
        semantic_digest: digest(&format!("{domain}-profile-semantic")),
    };
    let tenant_id = new_id(ResourceKind::Tenant);
    let sandbox_job_id = new_id(ResourceKind::Job);
    let invocation_id = new_id(ResourceKind::CapabilityInvocation);
    let deadline = now + Duration::minutes(1);
    let callback = ScopedSandboxCallback {
        schema_version: 1,
        tenant_id: tenant_id.clone(),
        sandbox_job_id: sandbox_job_id.clone(),
        invocation_id: invocation_id.clone(),
        attempt_no: 1,
        audience_identity_digest: digest("wasi-callback-audience"),
        expires_at: deadline,
        binding_digest: digest("placeholder"),
    }
    .seal()
    .unwrap();
    let capability_closure = CapabilityDeploymentClosure {
        implementation: exact(
            ResourceKind::CapabilityImplementationRevision,
            &format!("{domain}-implementation"),
        ),
        interface: exact(
            ResourceKind::CapabilityInterfaceRevision,
            &format!("{domain}-interface"),
        ),
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
        conformance_evidence: artifact(
            b"wasi-conformance",
            "application/json",
            "wasi-conformance.json",
        ),
    };
    SandboxExecutionRequest {
        schema_version: 1,
        protocol_version: SandboxAbiVersion::V1,
        tenant_id,
        sandbox_job_id: sandbox_job_id.clone(),
        invocation_id,
        job_id: sandbox_job_id,
        expected_invocation_version: 1,
        attempt_no: 1,
        retry_backoff_milliseconds: 100,
        lease_generation: 1,
        capability_deployment: deployment(
            ResourceKind::CapabilityDeployment,
            &format!("{domain}-capability"),
        ),
        execution_source: SandboxExecutionSource::SandboxCapability {
            capability_deployment_closure: Box::new(capability_closure),
        },
        runtime_revision,
        runtime,
        package_revision,
        package,
        profile_binding,
        profile,
        policies: Box::new(policy_closure()),
        isolation_class: SandboxIsolationClass::Wasm,
        executor_worker_manifest_digest: digest("wasi-worker-manifest"),
        isolation_backend_contract_digest: digest("wasi-backend-contract"),
        effect: Effect::Pure,
        classification: DataClassification::Internal,
        input_value_id: new_id(ResourceKind::RunValue),
        input_schema_digest: digest("wasi-input-schema"),
        input_ref: ValueRef::Inline {
            value: json!({"question": "life"}),
        },
        output_value_id: new_id(ResourceKind::RunValue),
        output_schema_digest: digest("wasi-output-schema"),
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
            wasm_fuel: Some(100_000),
            wasm_memory_pages: Some(1_024),
        },
        deadline,
        callback,
        trace_context: SafeSandboxTraceContext {
            trace_id_digest: digest("wasi-trace"),
            parent_span_id_digest: digest("wasi-parent-span"),
        },
        request_digest: digest("placeholder-request"),
    }
    .seal()
    .unwrap()
}

struct MemoryArtifactBroker(Mutex<BTreeMap<ResourceId, Vec<u8>>>);

#[async_trait]
impl WasiArtifactBroker for MemoryArtifactBroker {
    async fn read_exact(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<Vec<u8>, WasiArtifactBrokerError> {
        self.0
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
        let valid = match request.direction {
            WasiValueDirection::Input => request.value == json!({"question": "life"}),
            WasiValueDirection::Output => request.value == json!({"answer": "42"}),
        };
        valid
            .then(|| digest("wasi-value-validation"))
            .ok_or(WasiValueValidationError::Invalid)
    }
}

struct GrantRevoker;

#[async_trait]
impl WasiGrantRevoker for GrantRevoker {
    async fn revoke_exact(
        &self,
        _request: RevokeWasiSandboxGrants,
    ) -> Result<WasiGrantRevocationEvidence, WasiGrantRevocationError> {
        Ok(WasiGrantRevocationEvidence {
            evidence_digest: digest("wasi-grants-revoked"),
        })
    }
}

struct ProcessIsolation;

#[async_trait]
impl SandboxProcessGenerationIsolation for ProcessIsolation {
    async fn prove_absent(
        &self,
        _request: ProveSandboxProcessGenerationAbsent,
    ) -> Result<SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolationError>
    {
        Err(SandboxProcessGenerationIsolationError::Rejected)
    }
}

fn backend(module: &[u8], request: &SandboxExecutionRequest) -> WasmtimeSandboxExecutorBackend {
    let artifact_id = request
        .package
        .runtime_bundle_artifact
        .artifact_id()
        .clone();
    WasmtimeSandboxExecutorBackend::new(
        WasiExecutorBackendConfig::production(
            InstalledSandboxBackendDescriptor {
                backend_kind: SandboxIsolationBackendKind::Wasi,
                isolation_class: SandboxIsolationClass::Wasm,
                worker_manifest_digest: request.executor_worker_manifest_digest.clone(),
                backend_contract_digest: request.isolation_backend_contract_digest.clone(),
            },
            new_id(ResourceKind::WorkerProcessGeneration),
        ),
        Arc::new(MemoryArtifactBroker(Mutex::new(BTreeMap::from([(
            artifact_id,
            module.to_vec(),
        )])))),
        Arc::new(ExactValueValidator),
        Arc::new(GrantRevoker),
        Arc::new(ProcessIsolation),
        Arc::new(SystemWasiExecutorClock),
    )
    .unwrap()
}

fn destroy(
    backend: &WasmtimeSandboxExecutorBackend,
    request: &SandboxExecutionRequest,
    sandbox_identity_digest: Sha256Digest,
) {
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(backend.destroy(DestroySandbox {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            sandbox_identity_digest,
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            effect: request.effect,
            network_mode: request.network_mode,
        }))
        .unwrap();
    assert_eq!(result.disposition, SandboxCleanupDisposition::Destroyed);
}

fn qualify_restricted_wasi() {
    let now = Utc::now();
    let success = success_module();
    let success_request = request(now, &success, "productization-wasi-success");
    let success_backend = backend(&success, &success_request);
    let (prepared, collected) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let prepared = success_backend
                .prepare(success_request.clone())
                .await
                .unwrap();
            let running = success_backend
                .start(success_request.clone(), prepared.clone())
                .await
                .unwrap();
            let collected = success_backend
                .collect(success_request.clone(), running)
                .await
                .unwrap();
            (prepared, collected)
        });
    let SandboxExecutionOutcome::Completed(output) = collected.outcome else {
        panic!("restricted WASI success module did not complete")
    };
    assert_eq!(
        output.value,
        ValueRef::Inline {
            value: json!({"answer": "42"})
        }
    );
    assert!(output.usage.wasm_fuel_consumed.is_some_and(|fuel| fuel > 0));
    destroy(
        &success_backend,
        &success_request,
        prepared.sandbox_identity_digest,
    );

    let infinite = infinite_module();
    let mut limit_request = request(now, &infinite, "productization-wasi-limit");
    limit_request.resources.wasm_fuel = Some(1_000);
    limit_request = limit_request.seal().unwrap();
    let limit_backend = backend(&infinite, &limit_request);
    let (prepared, collected) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let prepared = limit_backend.prepare(limit_request.clone()).await.unwrap();
            let running = limit_backend
                .start(limit_request.clone(), prepared.clone())
                .await
                .unwrap();
            let collected = limit_backend
                .collect(limit_request.clone(), running)
                .await
                .unwrap();
            (prepared, collected)
        });
    let SandboxExecutionOutcome::Failed(failure) = collected.outcome else {
        panic!("infinite WASI module did not fail at its fuel limit")
    };
    assert_eq!(failure.safe_code, "sandbox_wasi_fuel_exhausted");
    assert_eq!(failure.resource_violation.as_deref(), Some("wasm_fuel"));
    destroy(
        &limit_backend,
        &limit_request,
        prepared.sandbox_identity_digest,
    );
}

fn apply_sandbox_resource(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    name: &str,
    noun: &str,
    document: ResourceDocument,
    content_digest: &Sha256Digest,
    deployment: Option<Value>,
) -> Value {
    let manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": noun,
        "create": {
            "display_name": name,
            "document": serde_json::to_value(document).unwrap()
        },
        "publish": {
            "kind": "single",
            "revision_no": 1,
            "content_digest": content_digest,
            "artifact_id": null
        },
        "deployment": deployment
    });
    let path = write_canonical(fixture, &format!("{name}.apply.json"), &manifest);
    run_json(
        insight,
        &[
            "apply",
            "--file",
            path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    )
}

fn upload_wasm(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    name: &str,
    bytes: &[u8],
) -> ArtifactRef {
    let path = fixture.join(name);
    fs::write(&path, bytes).unwrap();
    let report = run_json(
        insight,
        &[
            "artifact",
            "upload",
            "--file",
            path.to_str().unwrap(),
            "--purpose",
            "package",
            "--classification",
            "internal",
            "--media-type",
            "application/octet-stream",
            "--display-name",
            name,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    serde_json::from_value(json!({
        "artifact_id": report["artifact_id"],
        "content_digest": report["content_digest"],
        "byte_length": report["byte_length"],
        "media_type": report["media_type"],
        "classification": "internal",
        "display_name": name,
    }))
    .unwrap()
}

fn provision_sandbox_quotas() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect("postgres://insight:insight@127.0.0.1:5432/insight_platform")
                .await
                .unwrap();
            let tenant_id: String = sqlx::query_scalar(
                "SELECT tenant_id FROM insight_platform.tenants ORDER BY tenant_id LIMIT 1",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            let payload = json!({"profile": "productization_full"});
            let payload_digest = canonical_digest(&payload);
            for (metric, limit) in [
                ("durable_quota.sandbox_concurrent_executions", 8_i64),
                ("durable_quota.sandbox_cpu_seconds", 10_000),
                ("durable_quota.sandbox_memory_mebibytes", 65_536),
                ("durable_quota.sandbox_output_bytes", 16_777_216),
            ] {
                sqlx::query(
                    r#"INSERT INTO insight_platform.quota_accounts
                       (tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
                        limit_value, payload_schema_version, payload, payload_digest)
                       VALUES ($1, $2, 'tenant', $1, 'sandbox', $3, $4, 1, $5, $6)
                       ON CONFLICT (tenant_id, scope_kind, scope_id, work_class, metric) DO NOTHING"#,
                )
                .bind(&tenant_id)
                .bind(format!("qac_{}", uuid::Uuid::now_v7()))
                .bind(metric)
                .bind(limit)
                .bind(&payload)
                .bind(&payload_digest)
                .execute(&pool)
                .await
                .unwrap();
            }
        });
}

fn publish_public_wasi_agent(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> (String, String, String) {
    let module = success_module();
    let module_artifact = upload_wasm(insight, project, fixture, "public-success.wasm", &module);
    let authoring_artifact: ArtifactRef = serde_json::from_value(authoring_ref.clone()).unwrap();
    let qualification_artifact: ArtifactRef =
        serde_json::from_value(qualification_ref.clone()).unwrap();
    let authoring_package = AuthoringPackage {
        artifact: authoring_artifact,
        manifest_digest: authoring_ref["content_digest"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
    };
    let runtime_semantic = digest("public-wasi-runtime-semantic");
    let runtime_report = apply_sandbox_resource(
        insight,
        project,
        fixture,
        "public-wasi-runtime",
        "sandbox-runtimes",
        ResourceDocument::SandboxRuntime(SandboxRuntimeResourceSpec {
            authoring_package: authoring_package.clone(),
            contract_digest: digest("public-wasi-runtime-contract"),
            dependency_versions: vec![],
            policy_versions: vec![],
            runtime_family: SandboxRuntimeFamily::WasmWasi,
            runtime_version: WASI_ABI_V1_RUNTIME_VERSION.to_owned(),
            image_or_module_digest: digest("wasmtime-46.0.2-module"),
            supported_isolation: vec![SandboxIsolationClass::Wasm],
            abi: SandboxAbiVersion::V1,
            builtin_modules_manifest_digest: digest("public-wasi-no-builtins"),
            sbom_artifact: qualification_artifact.clone(),
            provenance_evidence: qualification_artifact.clone(),
            semantic_digest: runtime_semantic.clone(),
        }),
        &runtime_semantic,
        None,
    );
    let runtime_revision: ExactVersionRef = serde_json::from_value(exact_version(
        published_version(&runtime_report, "sandbox_runtime_revision"),
    ))
    .unwrap();

    let policies_document = policy_closure();
    let mut policies = BTreeMap::new();
    for (key, role, kind, field, document) in [
        (
            "isolation",
            "wasi-isolation",
            "isolation",
            "sandbox_isolation",
            serde_json::to_value(&policies_document.isolation).unwrap(),
        ),
        (
            "resource",
            "wasi-resource",
            "resource",
            "sandbox_resource",
            serde_json::to_value(&policies_document.resource).unwrap(),
        ),
        (
            "network",
            "wasi-network",
            "network",
            "sandbox_network",
            serde_json::to_value(&policies_document.network).unwrap(),
        ),
        (
            "artifact",
            "wasi-artifact-io",
            "artifact_io",
            "sandbox_artifact_io",
            serde_json::to_value(&policies_document.artifact_io).unwrap(),
        ),
    ] {
        policies.insert(
            key,
            native_and_remote_capability::apply_policy(
                insight,
                project,
                fixture,
                role,
                kind,
                Some((field, document)),
                authoring_ref,
                qualification_ref,
            ),
        );
    }
    let selection_document = json!({
        "schema_version": 1,
        "mode": "only_candidate",
        "route_schema_digest": null
    });
    let selection_policy = native_and_remote_capability::apply_policy(
        insight,
        project,
        fixture,
        "wasi-selection",
        "selection",
        Some(("selection", selection_document)),
        authoring_ref,
        qualification_ref,
    );
    let execution_policy = native_and_remote_capability::apply_policy(
        insight,
        project,
        fixture,
        "wasi-execution",
        "execution",
        None,
        authoring_ref,
        qualification_ref,
    );

    let package_contract_digest = digest("public-wasi-package-contract");
    let dependency_lock_digest = digest("public-wasi-dependency-lock");
    let package_digest = bytes_digest(&module);
    let package_report = apply_sandbox_resource(
        insight,
        project,
        fixture,
        "public-wasi-package",
        "sandbox-packages",
        ResourceDocument::SandboxPackage(SandboxPackageResourceSpec {
            authoring_package: authoring_package.clone(),
            contract_digest: package_contract_digest.clone(),
            dependency_versions: vec![runtime_revision.clone()],
            policy_versions: vec![],
            source_artifact: module_artifact.clone(),
            source_digest: bytes_digest(&module),
            runtime_revision: runtime_revision.clone(),
            entrypoint_kind: SandboxEntrypointKind::WasmExport,
            entrypoint: WASI_ABI_V1_ENTRYPOINT.to_owned(),
            dependency_lock_digest: dependency_lock_digest.clone(),
            runtime_bundle_artifact: module_artifact,
            build_evidence: qualification_artifact.clone(),
            trust_class: CodeTrustClass::BuiltIn,
            package_digest: package_digest.clone(),
        }),
        &package_digest,
        None,
    );
    let package_revision = exact_version(published_version(
        &package_report,
        "sandbox_package_revision",
    ));

    let profile_semantic = digest("public-wasi-profile-semantic");
    let profile_policy_versions = policies
        .values()
        .map(|policy| serde_json::from_value(policy.revision.clone()).unwrap())
        .collect::<Vec<ExactVersionRef>>();
    let profile_report = apply_sandbox_resource(
        insight,
        project,
        fixture,
        "public-wasi-profile",
        "sandboxes",
        ResourceDocument::SandboxProfile(SandboxProfileResourceSpec {
            authoring_package,
            contract_digest: digest("public-wasi-profile-contract"),
            dependency_versions: vec![runtime_revision.clone()],
            policy_versions: profile_policy_versions,
            allowed_trust_classes: vec![CodeTrustClass::BuiltIn],
            allowed_runtime_families: vec![SandboxRuntimeFamily::WasmWasi],
            minimum_isolation: SandboxIsolationClass::Wasm,
            isolation_policy: serde_json::from_value(policies["isolation"].revision.clone())
                .unwrap(),
            resource_policy: serde_json::from_value(policies["resource"].revision.clone()).unwrap(),
            network_policy: serde_json::from_value(policies["network"].revision.clone()).unwrap(),
            artifact_io_policy: serde_json::from_value(policies["artifact"].revision.clone())
                .unwrap(),
            secret_policy: None,
            cleanup: SandboxCleanupPolicy::SingleUseDestroy,
            max_job_duration_milliseconds: 10_000,
            semantic_digest: profile_semantic.clone(),
        }),
        &profile_semantic,
        Some(json!({
            "environment": "local",
            "closure": {
                "resource_kind": "sandbox_profile",
                "bindings": {
                    "runtime_revision": runtime_revision,
                    "policy_bindings": policies.values().map(|policy| policy.binding.clone()).collect::<Vec<_>>(),
                    "qualification_evidence": qualification_ref
                }
            }
        })),
    );
    let profile_revision = exact_version(published_version(
        &profile_report,
        "sandbox_profile_revision",
    ));
    let profile_closure = json!({
        "profile_revision": profile_revision,
        "runtime_revision": serde_json::to_value(&runtime_revision).unwrap(),
        "policy_bindings": policies.values().map(|policy| policy.binding.clone()).collect::<Vec<_>>(),
        "qualification_evidence": qualification_ref,
    });
    let profile_binding = json!({
        "deployment": {
            "deployment_id": profile_report["deployment_id"],
            "resource_kind": "sandbox_profile_deployment",
            "deployment_digest": canonical_digest(&json!({
                "schema_version": 1,
                "resource_kind": "sandbox_profile",
                "bindings": profile_closure,
            })),
        },
        "revision": profile_revision,
    });

    let input_schema = native_and_remote_capability::value_schema("message");
    let output_schema = native_and_remote_capability::value_schema("answer");
    let backend_contract = json!({
        "kind": "sandbox",
        "contract": {
            "package_contract_digest": package_contract_digest,
            "entrypoint_kind": "wasm_export",
            "entrypoint": WASI_ABI_V1_ENTRYPOINT,
            "dependency_lock_digest": dependency_lock_digest,
            "input_mapping_digest": digest("public-wasi-input-mapping"),
            "output_mapping_digest": digest("public-wasi-output-mapping")
        }
    });
    let backend_binding = json!({
        "kind": "sandbox",
        "binding": {
            "runtime": serde_json::to_value(&runtime_revision).unwrap(),
            "package": package_revision,
            "profile": profile_binding,
            "isolation": "wasm",
            "network_policy": policies["network"].revision,
            "resource_policy": policies["resource"].revision,
            "artifact_io_policy": policies["artifact"].revision,
            "secret_policy": null
        }
    });
    let policy_versions = policies
        .values()
        .map(|policy| policy.revision.clone())
        .collect();
    let (capability_deployment, _) = native_and_remote_capability::publish_capability(
        insight,
        project,
        fixture,
        "public_wasi",
        "pure",
        "intrinsic",
        "sandbox",
        backend_contract,
        backend_binding,
        &input_schema,
        &output_schema,
        authoring_ref,
        qualification_ref,
        policy_versions,
    );
    provision_sandbox_quotas();
    let agent_id = publish_wasi_agent(
        insight,
        project,
        fixture,
        authoring_ref,
        qualification_ref,
        &capability_deployment,
        &selection_policy,
        &execution_policy,
        &input_schema,
        &output_schema,
    );
    (
        agent_id,
        input_schema["canonical_digest"]
            .as_str()
            .unwrap()
            .to_owned(),
        output_schema["canonical_digest"]
            .as_str()
            .unwrap()
            .to_owned(),
    )
}

struct RemoteFrameworkEnvironment {
    broker: Child,
    fixture: Child,
    config_path: std::path::PathBuf,
    original_config: Vec<u8>,
    trace_path: std::path::PathBuf,
}

impl Drop for RemoteFrameworkEnvironment {
    fn drop(&mut self) {
        let _ = self.broker.kill();
        let _ = self.broker.wait();
        let _ = self.fixture.kill();
        let _ = self.fixture.wait();
        fs::write(&self.config_path, &self.original_config).expect("Egress config restore");
    }
}

fn start_framework_environment(
    insight: &Path,
    project: &Path,
    deployment: &Value,
    backend_contract_digest: &str,
    endpoint: &Value,
    policies: &std::collections::BTreeMap<&str, native_and_remote_capability::PolicyAuthority>,
) -> RemoteFrameworkEnvironment {
    let workspace = insight
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap();
    let runtime = project.join(".insight/runtime");
    let tls = runtime.join("tls");
    let config_path = runtime.join("config/egress-broker.json");
    let original_config = fs::read(&config_path).expect("Egress config read");
    let mut config: Value = serde_json::from_slice(&original_config).expect("Egress config JSON");
    config["capability_http_endpoints"] = json!([{
        "schema_version": 1,
        "capability_deployment": deployment,
        "backend_contract_digest": backend_contract_digest,
        "effect": "pure",
        "idempotency_kind": "intrinsic",
        "method": "post",
        "endpoint": endpoint,
        "endpoint_identity_digest": canonical_digest(endpoint),
        "network_policy": policies["network"].revision,
        "tls_policy": policies["tls"].revision,
        "trust_policy": policies["trust"].revision,
        "secret_bindings": [],
        "credential_injections": [],
        "limits": {"maximum_request_bytes": 4096, "maximum_response_bytes": 4096, "maximum_diagnostic_bytes": 1024,
            "connect_timeout_milliseconds": 500, "first_byte_timeout_milliseconds": 1000, "idle_timeout_milliseconds": 1000, "total_timeout_milliseconds": 3000},
        "development_loopback": true,
        "trusted_root_pem": fs::read_to_string(tls.join("ca.pem")).expect("local CA")
    }]);
    let address = config["observability_listen_address"]
        .as_str()
        .expect("Egress observability address")
        .to_owned();
    if TcpStream::connect(&address).is_ok() {
        stop_role(project, "egress-broker");
    }
    let trace_path = runtime.join(format!(
        "logs/remote-fixture-framework-{}.jsonl",
        uuid::Uuid::now_v7()
    ));
    let node = env::var("PLATFORM_PRODUCTIZATION_NODE_BIN").unwrap_or_else(|_| "node".to_owned());
    let mut fixture = Command::new(node);
    fixture
        .arg(workspace.join("examples/productization/remote-fixture.mjs"))
        .env_clear()
        .env(
            "INSIGHT_REMOTE_FIXTURE_PORT",
            endpoint["port"].as_u64().unwrap().to_string(),
        )
        .env(
            "INSIGHT_REMOTE_FIXTURE_CERT_PATH",
            tls.join("egress-broker.pem"),
        )
        .env(
            "INSIGHT_REMOTE_FIXTURE_KEY_PATH",
            tls.join("egress-broker-key.pem"),
        )
        .env("INSIGHT_REMOTE_FIXTURE_TRACE_PATH", &trace_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut fixture = fixture.spawn().expect("remote framework fixture starts");
    let ready = BufReader::new(fixture.stdout.take().unwrap())
        .lines()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&ready).unwrap()["status"],
        "ready"
    );
    let mut broker =
        native_and_remote_capability::spawn_egress(workspace, project, &config_path, &config);
    native_and_remote_capability::wait_egress(&mut broker, &address);
    RemoteFrameworkEnvironment {
        broker,
        fixture,
        config_path,
        original_config,
        trace_path,
    }
}

fn publish_framework_agent(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    deployment: &Value,
    policies: &std::collections::BTreeMap<&str, native_and_remote_capability::PolicyAuthority>,
    schema: &Value,
) -> String {
    let schema_digest = schema["canonical_digest"].as_str().unwrap();
    let contract_digest = canonical_digest(&json!({"agent": "remote-framework-capability"}));
    let requirement_digest = canonical_digest(&json!({"slot": "framework"}));
    let plan = json!({
        "plan_version": 5,
        "interface_contract_digest": contract_digest,
        "entry_node_id": "start",
        "dependency_slots": {"framework": {"kind": "capability", "requirement_digest": requirement_digest}},
        "nodes": {
            "start": {"kind": "start", "next": "framework"},
            "framework": {"kind": "capability_call", "capability_slot_id": "framework",
                "input": {"source": "run_input", "schema_digest": schema_digest}, "candidate_route": null,
                "output": {"source": "node_output", "producer_node_id": "framework", "port_id": "result", "schema_digest": schema_digest},
                "attempt_limit": 1, "retry_backoff_milliseconds": 100, "resume": "finish"},
            "finish": {"kind": "return", "value": {"source": "node_output", "producer_node_id": "framework", "port_id": "result", "schema_digest": schema_digest}}
        }
    });
    let plan_path = write_canonical(fixture, "remote-framework-agent-plan.json", &plan);
    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "remote-framework-agent-plan.json",
    );
    let manifest = json!({
        "schema_version": 1, "kind": "insight.platform.apply/v1", "resource_noun": "agents",
        "create": {"display_name": "Remote framework Capability agent", "document": {"resource_kind": "agent", "spec": {
            "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
            "contract_digest": contract_digest, "dependency_versions": [],
            "policy_versions": policies.values().map(|policy| policy.revision.clone()).collect::<Vec<_>>(),
            "input_schema": schema, "output_schema": schema,
            "error_schema": native_and_remote_capability::value_schema("error"),
            "typed_plan_artifact_id": plan_upload["artifact_id"], "typed_plan_digest": plan_upload["content_digest"]
        }}},
        "publish": {"kind": "agent", "revision_no": 1, "interface_content_digest": contract_digest,
            "plan_content_digest": plan_upload["content_digest"], "artifact_id": plan_upload["artifact_id"]},
        "deployment": {"environment": "local", "closure": {"resource_kind": "agent", "bindings": {
            "entry_node_id": "start", "entry_node_kind": "start",
            "slots": [{"slot_id": "framework", "requirement_digest": requirement_digest,
                "target": {"kind": "capability", "candidates": [deployment], "selection_policy": policies["selection"].binding}}],
            "policies": [], "execution_profile": policies["execution"].binding
        }}}
    });
    let path = write_canonical(fixture, "remote-framework-agent.apply.json", &manifest);
    run_json(
        insight,
        &[
            "apply",
            "--file",
            path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    )["resource_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn publish_wasi_agent(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    qualification_ref: &Value,
    deployment: &Value,
    selection_policy: &native_and_remote_capability::PolicyAuthority,
    execution_policy: &native_and_remote_capability::PolicyAuthority,
    input_schema: &Value,
    output_schema: &Value,
) -> String {
    let input_schema_digest = input_schema["canonical_digest"].as_str().unwrap();
    let output_schema_digest = output_schema["canonical_digest"].as_str().unwrap();
    let contract_digest = canonical_digest(&json!({"agent": "public-wasi-capability"}));
    let requirement_digest = canonical_digest(&json!({"slot": "public-wasi"}));
    let plan = json!({
        "plan_version": 5,
        "interface_contract_digest": contract_digest,
        "entry_node_id": "start",
        "dependency_slots": {"wasi": {"kind": "capability", "requirement_digest": requirement_digest}},
        "nodes": {
            "start": {"kind": "start", "next": "wasi"},
            "wasi": {
                "kind": "capability_call",
                "capability_slot_id": "wasi",
                "input": {"source": "run_input", "schema_digest": input_schema_digest},
                "candidate_route": null,
                "output": {"source": "node_output", "producer_node_id": "wasi", "port_id": "result", "schema_digest": output_schema_digest},
                "attempt_limit": 1,
                "retry_backoff_milliseconds": 100,
                "resume": "finish"
            },
            "finish": {"kind": "return", "value": {"source": "node_output", "producer_node_id": "wasi", "port_id": "result", "schema_digest": output_schema_digest}}
        }
    });
    let plan_path = write_canonical(fixture, "public-wasi-agent-plan.json", &plan);
    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "public-wasi-agent-plan.json",
    );
    let manifest = json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "agents",
        "create": {
            "display_name": "Public WASI Capability agent",
            "document": {
                "resource_kind": "agent",
                "spec": {
                    "authoring_package": {"artifact": authoring_ref, "manifest_digest": authoring_ref["content_digest"]},
                    "contract_digest": contract_digest,
                    "dependency_versions": [],
                    "policy_versions": [selection_policy.revision, execution_policy.revision],
                    "input_schema": input_schema,
                    "output_schema": output_schema,
                    "error_schema": native_and_remote_capability::value_schema("error"),
                    "typed_plan_artifact_id": plan_upload["artifact_id"],
                    "typed_plan_digest": plan_upload["content_digest"]
                }
            }
        },
        "publish": {
            "kind": "agent",
            "revision_no": 1,
            "interface_content_digest": contract_digest,
            "plan_content_digest": plan_upload["content_digest"],
            "artifact_id": plan_upload["artifact_id"]
        },
        "deployment": {
            "environment": "local",
            "closure": {
                "resource_kind": "agent",
                "bindings": {
                    "entry_node_id": "start",
                    "entry_node_kind": "start",
                    "slots": [{
                        "slot_id": "wasi",
                        "requirement_digest": requirement_digest,
                        "target": {
                            "kind": "capability",
                            "candidates": [deployment],
                            "selection_policy": selection_policy.binding
                        }
                    }],
                    "policies": [],
                    "execution_profile": execution_policy.binding
                }
            }
        }
    });
    let path = write_canonical(fixture, "public-wasi-agent.apply.json", &manifest);
    let _ = qualification_ref;
    run_json(
        insight,
        &[
            "apply",
            "--file",
            path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    )["resource_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn trace_records(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).expect("framework trace is closed JSON"))
        .collect()
}

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    authoring_ref: &Value,
    qualification_ref: &Value,
) -> WasiFrameworkEvidence {
    let started_at = Utc::now();
    qualify_restricted_wasi();
    let (wasi_agent_id, wasi_input_schema_digest, wasi_output_schema_digest) =
        publish_public_wasi_agent(insight, project, fixture, authoring_ref, qualification_ref);
    let wasi_run_id = native_and_remote_capability::create_run(
        insight,
        project,
        fixture,
        &wasi_agent_id,
        &wasi_input_schema_digest,
        "public-wasi-success",
        "execute the exact WASI package",
    );
    assert_eq!(
        native_and_remote_capability::terminal(insight, project, &wasi_run_id)["state"],
        "succeeded"
    );
    let wasi_result = run_json(
        insight,
        &[
            "run",
            "result",
            &wasi_run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(wasi_result["schema_digest"], wasi_output_schema_digest);
    assert_eq!(wasi_result["value"]["value"]["answer"], "42");

    let selection =
        json!({"schema_version": 1, "mode": "only_candidate", "route_schema_digest": null});
    let mut policies = std::collections::BTreeMap::new();
    for (key, role, kind, specialized) in [
        (
            "selection",
            "framework-selection",
            "selection",
            Some(("selection", selection)),
        ),
        ("execution", "framework-execution", "execution", None),
        ("network", "framework-network", "network", None),
        ("tls", "framework-tls", "tls", None),
        ("trust", "framework-trust", "trust", None),
    ] {
        policies.insert(
            key,
            native_and_remote_capability::apply_policy(
                insight,
                project,
                fixture,
                role,
                kind,
                specialized,
                authoring_ref,
                qualification_ref,
            ),
        );
    }
    let remote_config: Value = serde_json::from_slice(
        &fs::read(project.join(".insight/runtime/config/capability-remote.json")).unwrap(),
    )
    .unwrap();
    let http = &remote_config["installed_http_codecs"][0];
    let http_contract = json!({"kind": "http", "contract": {
        "method": "post", "protocol_contract_digest": http["protocol_contract_digest"],
        "request_mapping_digest": http["request_mapping_digest"], "response_mapping_digest": http["response_mapping_digest"],
        "error_mapping_digest": http["error_mapping_digest"], "idempotency_header": null
    }});
    let backend_contract_digest = canonical_digest(&http_contract);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let endpoint =
        json!({"scheme": "https", "host": "localhost", "port": port, "base_path": "/v1/framework"});
    let http_binding = json!({"kind": "http", "binding": {
        "codec": {"schema_version": 1, "backend_kind": "http", "codec_id": http["codec_id"], "codec_version": http["codec_version"],
            "module_digest": http["module_digest"], "worker_protocol_version": http["worker_protocol_version"], "descriptor_digest": http["descriptor_digest"]},
        "worker_manifest_digest": canonical_digest(&remote_config["worker_manifest"]),
        "endpoint": endpoint, "endpoint_identity_digest": canonical_digest(&endpoint),
        "network_policy": policies["network"].revision, "tls_policy": policies["tls"].revision,
        "trust_policy": policies["trust"].revision
    }});
    let schema = native_and_remote_capability::value_schema("message");
    let remote_policies = vec![
        policies["network"].revision.clone(),
        policies["tls"].revision.clone(),
        policies["trust"].revision.clone(),
    ];
    let (framework_deployment, _) = native_and_remote_capability::publish_capability(
        insight,
        project,
        fixture,
        "remote_framework",
        "pure",
        "intrinsic",
        "http",
        http_contract,
        http_binding,
        &schema,
        &schema,
        authoring_ref,
        qualification_ref,
        remote_policies,
    );
    native_and_remote_capability::provision_capability_quotas(&[(
        &framework_deployment,
        "capability_remote",
    )]);
    let mut remote = start_framework_environment(
        insight,
        project,
        &framework_deployment,
        &backend_contract_digest,
        &endpoint,
        &policies,
    );
    let agent_id = publish_framework_agent(
        insight,
        project,
        fixture,
        authoring_ref,
        &framework_deployment,
        &policies,
        &schema,
    );
    let schema_digest = schema["canonical_digest"].as_str().unwrap();
    let run_id = native_and_remote_capability::create_run(
        insight,
        project,
        fixture,
        &agent_id,
        schema_digest,
        "framework-success",
        "bounded request",
    );
    assert_eq!(
        native_and_remote_capability::terminal(insight, project, &run_id)["state"],
        "succeeded"
    );
    let result = run_json(
        insight,
        &[
            "run",
            "result",
            &run_id,
            "--path",
            project.to_str().unwrap(),
        ],
    );
    assert_eq!(result["schema_digest"], schema_digest);
    assert_eq!(
        result["value"]["value"]["message"],
        "framework: bounded request"
    );
    let requests = trace_records(&remote.trace_path);
    assert_eq!(
        requests
            .iter()
            .filter(|record| record["kind"] == "framework_capability_request")
            .count(),
        1
    );
    assert!(requests.iter().all(|record| {
        record["kind"] != "framework_capability_request"
            || record["database_environment_present"] == false
    }));

    let workspace = insight
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap();
    let address = serde_json::from_slice::<Value>(&remote.original_config).unwrap()
        ["observability_listen_address"]
        .as_str()
        .unwrap()
        .to_owned();
    let _ = remote.broker.kill();
    let _ = remote.broker.wait();
    let mut deny_config: Value = serde_json::from_slice(&remote.original_config).unwrap();
    deny_config["capability_http_endpoints"] = json!([]);
    remote.broker = native_and_remote_capability::spawn_egress(
        workspace,
        project,
        &remote.config_path,
        &deny_config,
    );
    native_and_remote_capability::wait_egress(&mut remote.broker, &address);
    let rejected_id = native_and_remote_capability::create_run(
        insight,
        project,
        fixture,
        &agent_id,
        schema_digest,
        "framework-rejected",
        "network rejection",
    );
    assert_eq!(
        native_and_remote_capability::terminal(insight, project, &rejected_id)["state"],
        "failed"
    );
    assert_eq!(
        trace_records(&remote.trace_path)
            .iter()
            .filter(|record| record["kind"] == "framework_capability_request")
            .count(),
        1,
        "Egress rejection must happen before remote framework dispatch"
    );
    drop(remote);
    WasiFrameworkEvidence {
        run_id,
        console_passed: false,
        started_at,
        finished_at: Utc::now(),
    }
}
