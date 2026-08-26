use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_artifact_broker::{
    ArtifactBrokerLimits, ArtifactObjectBytes, ArtifactObjectMetadata,
    ArtifactObjectReferenceUnsealError, ArtifactObjectReferenceUnsealer, ArtifactObjectStoreError,
    BrokeredSandboxArtifactBroker, DecryptedArtifactObjectReference, InstalledArtifactObjectStore,
    InstalledArtifactObjectStoreCatalog,
};
use insight_platform_artifacts::{ArtifactReferenceSnapshot, AuthorizedArtifactObjectRead};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ArtifactGrantOperation, ArtifactPurpose, ArtifactRef,
    ArtifactReferenceKind, ArtifactRetentionPolicy, AuthoringPackage, CapabilityArtifactContract,
    CapabilityBackendBinding, CapabilityBackendFeatures, CapabilityBackendKind,
    CapabilityCancellationKind, CapabilityDataFlowPolicy, CapabilityDeploymentClosure,
    CapabilityIdempotencyKind, CapabilityInterfaceLimits, CapabilityInterfaceResourceSpec,
    CapabilityProgressContract, CapabilityProgressDurability, CapabilityProgressMode,
    ClosedJsonSchema, CodeTrustClass, CommandAudit, CommandOutcome, DataClassification,
    DeploymentClosure, Effect, ExactDeploymentRef, ExactPolicyBinding, ExactSandboxProfileBinding,
    ExactVersionRef, InvocationState, JsonLimits, Permission, PermissionSet,
    PolicyDeploymentClosure, PolicyKind, PolicyResourceSpec, PrincipalBindingsPayload,
    PrincipalKind, PrincipalSnapshot, PublishedVersionPayload, RegistryResourceKind,
    ResourceDocument, ResourceId, ResourceKind, SandboxAbiVersion, SandboxArtifactIoPolicyDocument,
    SandboxCleanupPolicy, SandboxEntrypointKind, SandboxIsolationClass,
    SandboxIsolationPolicyDocument, SandboxNetworkPolicyDocument, SandboxPackageResourceSpec,
    SandboxProfileDeploymentClosure, SandboxProfileResourceSpec, SandboxResourcePolicyDocument,
    SandboxRuntimeFamily, SandboxRuntimeResourceSpec, Sha256Digest, TenantConfig,
    TenantPrincipalPayload, ValidationSummary, ValueRef,
};
use insight_platform_invocations::{
    CapabilityAdmissionSnapshot, CapabilityControlKind, CapabilityInvocationPayload,
    CapabilityInvocationRecord, ControlCapabilityInvocation, ExactInvocationValueRef,
    InvocationOrigin, InvocationPolicyDecisionBundle, InvocationSelectionEvidence, InvocationStore,
    InvocationTransaction, InvocationValueStorage,
};
use insight_platform_postgres::{
    repository::{
        NewPrincipal, NewTenant, NewTenantPrincipal, PgRepository, RepositoryError,
        SafetyScanShard, TypedPayload,
    },
    sandbox_repository::{ScanExpiredSandboxLeases, ScanPendingSandboxCapabilityOutcomes},
    verify_schema,
};
use insight_platform_sandbox::{
    AcceptSandboxExecution, ClaimSandboxJobs, CollectedSandbox, CommitSandboxPhase, DestroySandbox,
    ExecuteSandboxJob, HeartbeatSandboxExecution, InstalledSandboxBackendDescriptor,
    InstalledSandboxBackendRegistry, MergeSandboxCapabilityOutcome, PreparedSandbox,
    RecoverExpiredSandboxLease, RevokeWasiSandboxGrants, RunningSandbox, SafeSandboxTraceContext,
    SandboxBackendFailure, SandboxCleanupDisposition, SandboxCleanupEvidence,
    SandboxCompletedOutput, SandboxControlAuthority, SandboxControllerAudit,
    SandboxExecutionAuthority, SandboxExecutionOutcome, SandboxExecutionPolicyClosure,
    SandboxExecutorBackend, SandboxExecutorHost, SandboxExecutorWorker, SandboxGatewayAuthority,
    SandboxIsolationBackendKind, SandboxLeaseRecoveryAction, SandboxLeaseRecoveryAuthority,
    SandboxLeaseRecoveryDisposition, SandboxNetworkMode, SandboxOutcomeMergeAudit,
    SandboxPrestartControlOutcome, SandboxRecoveryAudit, SandboxResourceEnvelope,
    SandboxResourceUsage, SandboxStopReason, SandboxTerminationEvidence, SandboxUncertainty,
    SandboxWorkerAudit, SandboxWorkerAuditBundle, SandboxWorkerCommitIdentity, ScopedArtifactGrant,
    ScopedSandboxCallback, StopUnclaimedSandboxJob, TerminateSandbox, WasiArtifactBroker,
    WasiArtifactBrokerError, WasiArtifactReadPurpose, WasiArtifactReadRequest, WasiGrantRevoker,
    WasiValueDirection, WasiValueValidationError, WasiValueValidationRequest, WasiValueValidator,
};
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::Arc;

fn typed_payload<T: serde::Serialize>(value: &T) -> TypedPayload {
    TypedPayload::from_versioned(1, value, 1_048_576).unwrap()
}

fn seal_admission(mut admission: CapabilityAdmissionSnapshot) -> CapabilityAdmissionSnapshot {
    let mut value = serde_json::to_value(&admission).unwrap();
    value.as_object_mut().unwrap().remove("canonical_digest");
    admission.canonical_digest = canonical_digest(&value).unwrap().parse().unwrap();
    admission
}

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1d0-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn sha(character: char) -> insight_platform_contracts::Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn sha256_bytes(bytes: &[u8]) -> Sha256Digest {
    let digest = Sha256::digest(bytes);
    format!(
        "sha256:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
    .parse()
    .unwrap()
}

fn runtime_bundle_bytes() -> Vec<u8> {
    b"fixture-wasi-module-v1".to_vec()
}

fn artifact(suffix: u16, character: char) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        sha(character),
        16,
        "application/octet-stream",
        DataClassification::Internal,
        Some(format!("sandbox-fixture-{suffix}.bin")),
    )
    .unwrap()
}

fn authoring(suffix: u16, character: char) -> AuthoringPackage {
    AuthoringPackage {
        artifact: artifact(suffix, character),
        manifest_digest: sha(character),
    }
}

fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn policy_closure() -> SandboxExecutionPolicyClosure {
    SandboxExecutionPolicyClosure {
        isolation: SandboxIsolationPolicyDocument {
            schema_version: 1,
            minimum_isolation: SandboxIsolationClass::Wasm,
            allowed_runtime_families: vec![SandboxRuntimeFamily::WasmWasi],
            allowed_trust_classes: vec![CodeTrustClass::BuiltIn],
            deny_runtime_fallback: true,
            fresh_jail_per_job: true,
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

fn sandbox_policy_resource(
    suffix: u16,
    kind: PolicyKind,
    policies: &SandboxExecutionPolicyClosure,
) -> ResourceDocument {
    let mut spec = PolicyResourceSpec {
        authoring_package: authoring(suffix, '5'),
        contract_digest: sha('6'),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: kind,
        rules_digest: sha('0'),
        selection: None,
        scheduling: None,
        retention: None,
        model_safety: None,
        model_budget: None,
        model_public_projection: None,
        mcp_protocol: None,
        mcp_auth: None,
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        sandbox_secret_resolution: None,
    };
    match kind {
        PolicyKind::Isolation => {
            spec.rules_digest = policies.isolation.canonical_digest().unwrap();
            spec.sandbox_isolation = Some(policies.isolation.clone());
        }
        PolicyKind::Resource => {
            spec.rules_digest = policies.resource.canonical_digest().unwrap();
            spec.sandbox_resource = Some(policies.resource.clone());
        }
        PolicyKind::Network => {
            spec.rules_digest = policies.network.canonical_digest().unwrap();
            spec.sandbox_network = Some(policies.network.clone());
        }
        PolicyKind::ArtifactIo => {
            spec.rules_digest = policies.artifact_io.canonical_digest().unwrap();
            spec.sandbox_artifact_io = Some(policies.artifact_io.clone());
        }
        PolicyKind::SecretResolution => {
            let document = policies.secret_resolution.clone().unwrap();
            spec.rules_digest = document.canonical_digest().unwrap();
            spec.sandbox_secret_resolution = Some(document);
        }
        _ => unreachable!("fixture only publishes Sandbox execution policies"),
    }
    ResourceDocument::Policy(Box::new(spec))
}

fn validation() -> ValidationSummary {
    ValidationSummary {
        validator_digest: sha('a'),
        validated_draft_digest: sha('b'),
        dependency_closure_digest: sha('c'),
        security_evidence_digest: sha('d'),
        warnings: vec![],
    }
}

fn policy_deployment_closure(revision: ExactVersionRef) -> PolicyDeploymentClosure {
    PolicyDeploymentClosure {
        policy_revision: revision,
        applicability_digest: sha('7'),
        qualification_evidence: artifact(77, '8'),
    }
}

fn rebind_sandbox_input_grant(
    request: &mut insight_platform_sandbox::SandboxExecutionRequest,
    grant_suffix: u16,
) {
    let grant = request.artifact_grants.first_mut().unwrap();
    grant.grant_id = id(ResourceKind::ArtifactGrant, grant_suffix);
    grant.sandbox_job_id = request.sandbox_job_id.clone();
    grant.grant_digest = sha('0');
    *grant = grant.clone().seal().unwrap();
}

fn reidentify_accept_command(
    command: &mut AcceptSandboxExecution,
    base_suffix: u16,
    digest_character: char,
) {
    command.audit.receipt_id = id(ResourceKind::Receipt, base_suffix);
    command.audit.event_id = id(ResourceKind::Event, base_suffix + 1);
    command.audit.outbox_id = id(ResourceKind::OutboxEvent, base_suffix + 2);
    command.audit.idempotency_key_digest = sha(digest_character);
    command.audit.request_digest = command.request.request_digest.clone();
    command.usage_reservation_id = id(ResourceKind::UsageReservation, base_suffix + 3);
    command.quota_entry_ids = (base_suffix + 4..base_suffix + 8)
        .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
        .collect();
}

struct Fixture {
    tenant_id: ResourceId,
    principal_id: ResourceId,
    worker_id: ResourceId,
    command: AcceptSandboxExecution,
    capability_resource_id: ResourceId,
    capability_interface: ExactVersionRef,
    error_schema_digest: Sha256Digest,
    capability_closure: CapabilityDeploymentClosure,
    profile_closure: SandboxProfileDeploymentClosure,
    resources: Vec<(
        ResourceId,
        RegistryResourceKind,
        ExactVersionRef,
        ResourceDocument,
    )>,
}

fn fixture(now: DateTime<Utc>) -> Fixture {
    let tenant_id = id(ResourceKind::Tenant, 1);
    let principal_id = id(ResourceKind::Principal, 2);
    let runtime_revision = exact(ResourceKind::SandboxRuntimeRevision, 10, '1');
    let package_revision = exact(ResourceKind::SandboxPackageRevision, 11, '2');
    let profile_revision = exact(ResourceKind::SandboxProfileRevision, 12, '3');
    let isolation_policy = exact(ResourceKind::PolicyRevision, 13, '4');
    let resource_policy = exact(ResourceKind::PolicyRevision, 14, '5');
    let network_policy = exact(ResourceKind::PolicyRevision, 15, '6');
    let artifact_io_policy = exact(ResourceKind::PolicyRevision, 16, '7');
    let runtime = SandboxRuntimeResourceSpec {
        authoring_package: authoring(20, '1'),
        contract_digest: sha('2'),
        dependency_versions: vec![],
        policy_versions: vec![],
        runtime_family: SandboxRuntimeFamily::WasmWasi,
        runtime_version: "wasmtime-42.0.0".to_owned(),
        image_or_module_digest: sha('3'),
        supported_isolation: vec![SandboxIsolationClass::Wasm],
        abi: SandboxAbiVersion::V1,
        builtin_modules_manifest_digest: sha('5'),
        sbom_artifact: artifact(21, '6'),
        provenance_evidence: artifact(22, '7'),
        semantic_digest: sha('8'),
    };
    let package = SandboxPackageResourceSpec {
        authoring_package: authoring(23, '9'),
        contract_digest: sha('a'),
        dependency_versions: vec![runtime_revision.clone()],
        policy_versions: vec![],
        source_artifact: artifact(24, 'b'),
        source_digest: sha('b'),
        runtime_revision: runtime_revision.clone(),
        entrypoint_kind: SandboxEntrypointKind::WasmExport,
        entrypoint: "run".to_owned(),
        dependency_lock_digest: sha('c'),
        runtime_bundle_artifact: ArtifactRef::new(
            id(ResourceKind::Artifact, 25),
            sha256_bytes(&runtime_bundle_bytes()),
            u64::try_from(runtime_bundle_bytes().len()).unwrap(),
            "application/wasm",
            DataClassification::Internal,
            Some("fixture-wasi-module.wasm".to_owned()),
        )
        .unwrap(),
        build_evidence: artifact(26, 'e'),
        trust_class: CodeTrustClass::BuiltIn,
        package_digest: sha('f'),
    };
    let profile = SandboxProfileResourceSpec {
        authoring_package: authoring(27, '1'),
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
        isolation_policy: isolation_policy.clone(),
        resource_policy: resource_policy.clone(),
        network_policy: network_policy.clone(),
        artifact_io_policy: artifact_io_policy.clone(),
        secret_policy: None,
        cleanup: SandboxCleanupPolicy::SingleUseDestroy,
        max_job_duration_milliseconds: 60_000,
        semantic_digest: sha('3'),
    };
    let profile_closure = SandboxProfileDeploymentClosure {
        profile_revision: profile_revision.clone(),
        runtime_revision: runtime_revision.clone(),
        policy_bindings: [
            (&isolation_policy, 73),
            (&resource_policy, 74),
            (&network_policy, 75),
            (&artifact_io_policy, 76),
        ]
        .into_iter()
        .map(|(revision, suffix)| {
            let closure = policy_deployment_closure(revision.clone());
            let bindings = TypedPayload::new(1, &DeploymentClosure::Policy(closure)).unwrap();
            ExactPolicyBinding {
                deployment: ExactDeploymentRef::new(
                    id(ResourceKind::PolicyDeployment, suffix),
                    bindings.digest.parse().unwrap(),
                )
                .unwrap(),
                revision: revision.clone(),
            }
        })
        .collect(),
        qualification_evidence: artifact(77, '8'),
    };
    let profile_bindings = TypedPayload::new(
        1,
        &DeploymentClosure::SandboxProfile(profile_closure.clone()),
    )
    .unwrap();
    let profile_binding = ExactSandboxProfileBinding {
        deployment: ExactDeploymentRef::new(
            id(ResourceKind::SandboxProfileDeployment, 78),
            profile_bindings.digest.parse().unwrap(),
        )
        .unwrap(),
        revision: profile_revision.clone(),
    };
    let capability_resource_id = id(ResourceKind::CapabilityInterface, 30);
    let capability_interface = exact(ResourceKind::CapabilityInterfaceRevision, 31, '8');
    let capability_implementation = exact(ResourceKind::CapabilityImplementationRevision, 32, '9');
    let input_schema = ClosedJsonSchema::build(serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "question": {
                "description": "Bounded Sandbox fixture question.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 64,
                "x-platform-max-bytes": 256
            }
        },
        "required": ["question"],
        "additionalProperties": false
    }))
    .unwrap();
    let output_schema = ClosedJsonSchema::build(serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "answer": {
                "description": "Bounded Sandbox fixture answer.",
                "x-platform-classification": "internal",
                "type": "integer",
                "minimum": 0,
                "maximum": 100
            }
        },
        "required": ["answer"],
        "additionalProperties": false
    }))
    .unwrap();
    let error_schema = ClosedJsonSchema::build(serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "error": {
                "description": "Bounded Sandbox fixture error.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "x-platform-max-bytes": 512
            }
        },
        "required": ["error"],
        "additionalProperties": false
    }))
    .unwrap();
    let input_schema_digest = input_schema.canonical_digest.clone();
    let output_schema_digest = output_schema.canonical_digest.clone();
    let error_schema_digest = error_schema.canonical_digest.clone();
    let interface_document =
        ResourceDocument::CapabilityInterface(CapabilityInterfaceResourceSpec {
            authoring_package: authoring(28, '2'),
            contract_digest: sha('3'),
            dependency_versions: vec![],
            policy_versions: vec![],
            qualified_name: "fixture.sandbox".parse().unwrap(),
            input_schema,
            output_schema,
            error_schema,
            artifacts: CapabilityArtifactContract { ports: vec![] },
            data_policy: CapabilityDataFlowPolicy {
                maximum_input_classification: DataClassification::Restricted,
                maximum_output_classification: DataClassification::Restricted,
                allowed_regions: vec!["global".parse().unwrap()],
                declassification_policy: None,
            },
            execution_limits: CapabilityInterfaceLimits {
                maximum_input_bytes: 1_048_576,
                maximum_output_bytes: 1_048_576,
                maximum_artifacts: 0,
                maximum_execution_milliseconds: 60_000,
            },
            effect: Effect::Pure,
            idempotency: CapabilityIdempotencyKind::Intrinsic,
            cancellation: CapabilityCancellationKind::Confirmed,
            progress: CapabilityProgressContract {
                mode: CapabilityProgressMode::None,
                schema_digest: None,
                max_events: 0,
                max_bytes_per_event: 0,
                minimum_interval_milliseconds: 0,
                durability: CapabilityProgressDurability::None,
            },
        });
    let backend = CapabilityBackendBinding::Sandbox {
        runtime: runtime_revision.clone(),
        package: package_revision.clone(),
        profile: profile_binding.clone(),
        isolation: SandboxIsolationClass::Wasm,
        network_policy,
        resource_policy,
        artifact_io_policy,
        secret_policy: None,
    };
    let capability_closure = CapabilityDeploymentClosure {
        implementation: capability_implementation,
        interface: capability_interface.clone(),
        backend: backend.clone(),
        secret_bindings: vec![],
        policies: vec![],
        conformance_evidence: artifact(33, 'a'),
    };
    let bindings = TypedPayload::new(
        1,
        &DeploymentClosure::CapabilityInterface(capability_closure.clone()),
    )
    .unwrap();
    let capability_deployment = ExactDeploymentRef::new(
        id(ResourceKind::CapabilityDeployment, 34),
        bindings.digest.parse().unwrap(),
    )
    .unwrap();
    let sandbox_job_id = id(ResourceKind::Job, 42);
    let invocation_id = id(ResourceKind::CapabilityInvocation, 41);
    let deadline = now + Duration::minutes(1);
    let input_value = serde_json::json!({"question": "life"});
    let input_content_digest: Sha256Digest =
        canonical_digest(&input_value).unwrap().parse().unwrap();
    let input_artifact = ArtifactRef::new(
        id(ResourceKind::Artifact, 67),
        input_content_digest,
        u64::try_from(input_value.to_string().len()).unwrap(),
        "application/json",
        DataClassification::Internal,
        Some("sandbox-input.json".to_owned()),
    )
    .unwrap();
    let input_grant = ScopedArtifactGrant {
        schema_version: 1,
        grant_id: id(ResourceKind::ArtifactGrant, 68),
        tenant_id: tenant_id.clone(),
        sandbox_job_id: sandbox_job_id.clone(),
        operation: ArtifactGrantOperation::ReadWhole,
        port: "input".to_owned(),
        artifact: Some(input_artifact.clone()),
        staging_artifact_id: None,
        byte_range: None,
        maximum_bytes: 1_048_576,
        generation: 1,
        expires_at: deadline,
        grant_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let callback = ScopedSandboxCallback {
        schema_version: 1,
        tenant_id: tenant_id.clone(),
        sandbox_job_id: sandbox_job_id.clone(),
        invocation_id: invocation_id.clone(),
        attempt_no: 1,
        audience_identity_digest: sha('b'),
        expires_at: deadline,
        binding_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let request = insight_platform_sandbox::SandboxExecutionRequest {
        schema_version: 1,
        protocol_version: SandboxAbiVersion::V1,
        tenant_id: tenant_id.clone(),
        sandbox_job_id,
        invocation_id,
        job_id: id(ResourceKind::Job, 42),
        expected_invocation_version: 1,
        attempt_no: 1,
        retry_backoff_milliseconds: 100,
        lease_generation: 0,
        capability_deployment,
        execution_source: insight_platform_sandbox::SandboxExecutionSource::SandboxCapability {
            capability_deployment_closure: Box::new(capability_closure.clone()),
        },
        runtime_revision: runtime_revision.clone(),
        runtime: runtime.clone(),
        package_revision: package_revision.clone(),
        package: package.clone(),
        profile_binding,
        profile: profile.clone(),
        policies: Box::new(policy_closure()),
        isolation_class: SandboxIsolationClass::Wasm,
        executor_worker_manifest_digest: sha('c'),
        isolation_backend_contract_digest: sha('d'),
        effect: Effect::Pure,
        classification: DataClassification::Internal,
        input_value_id: id(ResourceKind::RunValue, 38),
        input_schema_digest,
        input_ref: ValueRef::Artifact {
            artifact: input_artifact,
        },
        output_value_id: id(ResourceKind::RunValue, 42),
        output_schema_digest,
        artifact_grants: vec![input_grant],
        secret_grants: vec![],
        network_mode: SandboxNetworkMode::None,
        resources: SandboxResourceEnvelope {
            cpu_millicores: 100,
            memory_mebibytes: 64,
            pids: 4,
            files: 16,
            io_bytes: 1_048_576,
            stdout_bytes: 1_024,
            stderr_bytes: 1_024,
            result_bytes: 1_048_576,
            artifact_output_bytes: 0,
            network_connections: 0,
            network_request_bytes: 0,
            network_response_bytes: 0,
            startup_milliseconds: 1_000,
            idle_milliseconds: 5_000,
            wall_milliseconds: 30_000,
            cleanup_milliseconds: 1_000,
            wasm_fuel: Some(1_000_000),
            wasm_memory_pages: Some(1_024),
        },
        deadline,
        callback,
        trace_context: SafeSandboxTraceContext {
            trace_id_digest: sha('e'),
            parent_span_id_digest: sha('f'),
        },
        request_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let request_policies = request.policies.clone();
    let isolation_policy_revision = request.profile.isolation_policy.clone();
    let resource_policy_revision = request.profile.resource_policy.clone();
    let network_policy_revision = request.profile.network_policy.clone();
    let artifact_io_policy_revision = request.profile.artifact_io_policy.clone();
    let audit = CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind: PrincipalKind::ServiceIdentity,
        receipt_id: id(ResourceKind::Receipt, 43),
        event_id: id(ResourceKind::Event, 44),
        outbox_id: id(ResourceKind::OutboxEvent, 45),
        idempotency_key_digest: sha('1'),
        request_digest: request.request_digest.clone(),
        receipt_expires_at: now + Duration::minutes(5),
    };
    Fixture {
        tenant_id,
        principal_id,
        worker_id: id(ResourceKind::WorkerProcessGeneration, 46),
        command: AcceptSandboxExecution {
            audit,
            request,
            usage_reservation_id: id(ResourceKind::UsageReservation, 47),
            quota_entry_ids: (48..52)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        },
        capability_resource_id,
        capability_interface,
        error_schema_digest,
        capability_closure,
        profile_closure,
        resources: vec![
            (
                id(ResourceKind::CapabilityInterface, 30),
                RegistryResourceKind::CapabilityInterface,
                exact(ResourceKind::CapabilityInterfaceRevision, 31, '8'),
                interface_document,
            ),
            (
                id(ResourceKind::SandboxRuntime, 53),
                RegistryResourceKind::SandboxRuntime,
                runtime_revision,
                ResourceDocument::SandboxRuntime(runtime),
            ),
            (
                id(ResourceKind::SandboxPackage, 54),
                RegistryResourceKind::SandboxPackage,
                package_revision,
                ResourceDocument::SandboxPackage(package),
            ),
            (
                id(ResourceKind::SandboxProfile, 55),
                RegistryResourceKind::SandboxProfile,
                profile_revision,
                ResourceDocument::SandboxProfile(profile),
            ),
            (
                id(ResourceKind::Policy, 69),
                RegistryResourceKind::Policy,
                isolation_policy_revision,
                sandbox_policy_resource(69, PolicyKind::Isolation, &request_policies),
            ),
            (
                id(ResourceKind::Policy, 70),
                RegistryResourceKind::Policy,
                resource_policy_revision,
                sandbox_policy_resource(70, PolicyKind::Resource, &request_policies),
            ),
            (
                id(ResourceKind::Policy, 71),
                RegistryResourceKind::Policy,
                network_policy_revision,
                sandbox_policy_resource(71, PolicyKind::Network, &request_policies),
            ),
            (
                id(ResourceKind::Policy, 72),
                RegistryResourceKind::Policy,
                artifact_io_policy_revision,
                sandbox_policy_resource(72, PolicyKind::ArtifactIo, &request_policies),
            ),
            (
                id(ResourceKind::Policy, 65),
                RegistryResourceKind::Policy,
                exact(ResourceKind::PolicyRevision, 66, '6'),
                ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                    authoring_package: authoring(64, '5'),
                    contract_digest: sha('6'),
                    dependency_versions: vec![],
                    policy_versions: vec![],
                    policy_kind: PolicyKind::Retention,
                    rules_digest: ArtifactRetentionPolicy {
                        version: 1,
                        minimum_retention_seconds: 60,
                        gc_grace_seconds: 60,
                        tombstone_retention_seconds: 3_600,
                        retain_provenance_sources: true,
                        delete_requires_approval: false,
                    }
                    .canonical_digest()
                    .unwrap(),
                    selection: None,
                    scheduling: None,
                    retention: Some(ArtifactRetentionPolicy {
                        version: 1,
                        minimum_retention_seconds: 60,
                        gc_grace_seconds: 60,
                        tombstone_retention_seconds: 3_600,
                        retain_provenance_sources: true,
                        delete_requires_approval: false,
                    }),
                    model_safety: None,
                    model_budget: None,
                    model_public_projection: None,
                    mcp_protocol: None,
                    mcp_auth: None,
                    sandbox_isolation: None,
                    sandbox_resource: None,
                    sandbox_network: None,
                    sandbox_artifact_io: None,
                    sandbox_secret_resolution: None,
                })),
            ),
        ],
    }
}

async fn insert_resource_version(
    pool: &PgPool,
    fixture: &Fixture,
    resource_id: &ResourceId,
    kind: RegistryResourceKind,
    exact: &ExactVersionRef,
    document: &ResourceDocument,
) {
    let resource_payload = TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, $3, 'active', 'enabled', $4, $5, $6)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(kind.as_str())
    .bind(resource_payload.schema_version)
    .bind(&resource_payload.value)
    .bind(&resource_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let published = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: document.clone(),
            validation: validation(),
        },
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resource_versions (
            tenant_id, resource_version_id, resource_id, resource_version_kind,
            revision_no, content_digest, payload_schema_version, payload,
            payload_digest, created_by
        ) VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(exact.revision_id.to_string())
    .bind(resource_id.to_string())
    .bind(exact.resource_kind.descriptor().name)
    .bind(exact.semantic_digest.to_string())
    .bind(published.schema_version)
    .bind(&published.value)
    .bind(&published.digest)
    .bind(fixture.principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_version_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(exact.revision_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn seed(pool: &PgPool, repository: &PgRepository, fixture: &Fixture) {
    let database_observed_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    repository
        .create_tenant(NewTenant {
            tenant_id: fixture.tenant_id.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    repository
        .create_principal(NewPrincipal {
            principal_id: fixture.principal_id.clone(),
            authentication_authority_digest: sha('2'),
            subject_digest: sha('3'),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: fixture.tenant_id.clone(),
            principal_id: fixture.principal_id.clone(),
            principal_kind: PrincipalKind::ServiceIdentity,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::SandboxExecute,
                    Permission::RuntimeControl,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();
    for (resource_id, kind, exact, document) in &fixture.resources {
        insert_resource_version(pool, fixture, resource_id, *kind, exact, document).await;
    }
    seed_sandbox_input_artifact(pool, fixture).await;

    for binding in &fixture.profile_closure.policy_bindings {
        let (resource_id, _, _, _) = fixture
            .resources
            .iter()
            .find(|(_, kind, exact, _)| {
                *kind == RegistryResourceKind::Policy && exact == &binding.revision
            })
            .unwrap();
        let policy_bindings = TypedPayload::new(
            1,
            &DeploymentClosure::Policy(policy_deployment_closure(binding.revision.clone())),
        )
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO insight_platform.deployments (
                tenant_id, deployment_id, resource_id, resource_version_id,
                environment, bindings_digest, payload_schema_version, bindings, created_by
            ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(binding.deployment.deployment_id.to_string())
        .bind(resource_id.to_string())
        .bind(binding.revision.revision_id.to_string())
        .bind(&policy_bindings.digest)
        .bind(policy_bindings.schema_version)
        .bind(&policy_bindings.value)
        .bind(fixture.principal_id.to_string())
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE insight_platform.resources SET active_version_id = NULL, active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(binding.deployment.deployment_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    }

    let profile_bindings = TypedPayload::new(
        1,
        &DeploymentClosure::SandboxProfile(fixture.profile_closure.clone()),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id,
            environment, bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(
        fixture
            .command
            .request
            .profile_binding
            .deployment
            .deployment_id
            .to_string(),
    )
    .bind(id(ResourceKind::SandboxProfile, 55).to_string())
    .bind(
        fixture
            .profile_closure
            .profile_revision
            .revision_id
            .to_string(),
    )
    .bind(&profile_bindings.digest)
    .bind(profile_bindings.schema_version)
    .bind(&profile_bindings.value)
    .bind(fixture.principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_version_id = NULL, active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::SandboxProfile, 55).to_string())
    .bind(
        fixture
            .command
            .request
            .profile_binding
            .deployment
            .deployment_id
            .to_string(),
    )
    .execute(pool)
    .await
    .unwrap();

    let bindings = TypedPayload::new(
        1,
        &DeploymentClosure::CapabilityInterface(fixture.capability_closure.clone()),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id,
            environment, bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(
        fixture
            .command
            .request
            .capability_deployment
            .deployment_id
            .to_string(),
    )
    .bind(fixture.capability_resource_id.to_string())
    .bind(fixture.capability_interface.revision_id.to_string())
    .bind(&bindings.digest)
    .bind(bindings.schema_version)
    .bind(&bindings.value)
    .bind(fixture.principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let agent_resource_id = id(ResourceKind::Agent, 80);
    let agent_version_id = id(ResourceKind::AgentPlanRevision, 81);
    let agent_deployment_id = id(ResourceKind::AgentDeployment, 82);
    let agent_resource_payload =
        TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'agent', 'active', 'enabled', $3, $4, $5)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(agent_resource_id.to_string())
    .bind(agent_resource_payload.schema_version)
    .bind(&agent_resource_payload.value)
    .bind(&agent_resource_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let agent_version_payload =
        TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resource_versions (
            tenant_id, resource_version_id, resource_id, resource_version_kind,
            revision_no, content_digest, payload_schema_version, payload,
            payload_digest, created_by
        ) VALUES ($1, $2, $3, 'agent_plan_revision', 1, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(agent_version_id.to_string())
    .bind(agent_resource_id.to_string())
    .bind(sha('1').to_string())
    .bind(agent_version_payload.schema_version)
    .bind(&agent_version_payload.value)
    .bind(&agent_version_payload.digest)
    .bind(fixture.principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let agent_deployment_payload =
        TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id,
            environment, bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(agent_deployment_id.to_string())
    .bind(agent_resource_id.to_string())
    .bind(agent_version_id.to_string())
    .bind(&agent_deployment_payload.digest)
    .bind(agent_deployment_payload.schema_version)
    .bind(&agent_deployment_payload.value)
    .bind(fixture.principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let run_id = id(ResourceKind::Run, 83);
    let node_id = id(ResourceKind::NodeExecution, 56);
    let run_payload = TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.runs (
            tenant_id, run_id, root_run_id, agent_deployment_id, principal_id,
            state, version, bindings_schema_version, bindings, bindings_digest,
            current_schema_version, current_payload, current_payload_digest,
            deadline, started_at, created_at, updated_at, trace_id
        ) VALUES ($1, $2, $2, $3, $4, 'running', 1, $5, $6, $7, $5, $6, $7,
                  $8, $9, $9, $9, '0123456789abcdef0123456789abcdef')
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(run_id.to_string())
    .bind(agent_deployment_id.to_string())
    .bind(fixture.principal_id.to_string())
    .bind(run_payload.schema_version)
    .bind(&run_payload.value)
    .bind(&run_payload.digest)
    .bind(fixture.command.request.deadline)
    .bind(database_observed_at)
    .execute(pool)
    .await
    .unwrap();
    let node_payload = TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, record_kind, scope_id, plan_node_key,
            activation_ordinal, logical_key, node_kind, state, generation, version,
            payload_schema_version, payload, payload_digest, deadline, started_at,
            created_at, updated_at
        ) VALUES ($1, $2, $3, 'node_execution', $2, 'sandbox', 1, 'sandbox',
                  'capability_call', 'running', 1, 1, $4, $5, $6, $7, $8, $8, $8)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(run_id.to_string())
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .bind(fixture.command.request.deadline)
    .bind(database_observed_at)
    .execute(pool)
    .await
    .unwrap();
    let ValueRef::Artifact {
        artifact: input_artifact,
    } = &fixture.command.request.input_ref
    else {
        panic!("Sandbox fixture input must be Artifact-backed");
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, value_kind, classification,
            schema_digest, content_digest, artifact_id
        ) VALUES ($1, $2, $3, 'capability_input', 'internal', $4, $5, $6)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.command.request.input_value_id.to_string())
    .bind(run_id.to_string())
    .bind(fixture.command.request.input_schema_digest.to_string())
    .bind(input_artifact.content_digest().to_string())
    .bind(input_artifact.artifact_id().to_string())
    .execute(pool)
    .await
    .unwrap();

    let principal = PrincipalSnapshot::build(
        fixture.tenant_id.clone(),
        fixture.principal_id.clone(),
        PrincipalKind::ServiceIdentity,
        PermissionSet::new(vec![Permission::SandboxExecute]).unwrap(),
        1,
        1,
        1,
    )
    .unwrap();
    let exact_input = ExactInvocationValueRef {
        schema_version: 1,
        value_id: fixture.command.request.input_value_id.clone(),
        run_id: run_id.clone(),
        producing_node_id: None,
        value_kind: "capability_input".to_owned(),
        classification: fixture.command.request.classification,
        schema_digest: fixture.command.request.input_schema_digest.clone(),
        content_digest: input_artifact.content_digest().clone(),
        storage: InvocationValueStorage::Artifact {
            artifact: input_artifact.clone(),
        },
    };
    let selection = InvocationSelectionEvidence::build(
        std::slice::from_ref(&fixture.command.request.capability_deployment),
        0,
        sha('6'),
    )
    .unwrap();
    let admission = seal_admission(CapabilityAdmissionSnapshot {
        schema_version: 1,
        origin_key: InvocationOrigin::PlanNode {
            node_execution_id: node_id.clone(),
        },
        slot_id: "sandbox".to_owned(),
        slot_binding_digest: sha('7'),
        run_bindings_digest: sha('8'),
        selection_policy: ExactPolicyBinding {
            deployment: ExactDeploymentRef::new(id(ResourceKind::PolicyDeployment, 85), sha('9'))
                .unwrap(),
            revision: exact(ResourceKind::PolicyRevision, 84, '9'),
        },
        selection_evidence: selection,
        deployment: fixture.command.request.capability_deployment.clone(),
        interface: fixture.capability_interface.clone(),
        capability_name: "fixture.sandbox".parse().unwrap(),
        implementation: fixture.capability_closure.implementation.clone(),
        backend_kind: CapabilityBackendKind::Sandbox,
        backend_contract_digest: sha('a'),
        mcp_runtime: None,
        input: exact_input,
        input_artifact_link_id: Some(id(ResourceKind::ArtifactLink, 69)),
        effect: fixture.command.request.effect,
        idempotency: CapabilityIdempotencyKind::Intrinsic,
        cancellation: CapabilityCancellationKind::Confirmed,
        progress: CapabilityProgressContract {
            mode: CapabilityProgressMode::None,
            schema_digest: None,
            max_events: 0,
            max_bytes_per_event: 0,
            minimum_interval_milliseconds: 0,
            durability: CapabilityProgressDurability::None,
        },
        implementation_features: CapabilityBackendFeatures {
            deferred: true,
            input_required: false,
            callback: false,
            poll: false,
            progress: false,
            cancellation: true,
            max_remote_state_bytes: 1_024,
            max_poll_count: 0,
        },
        input_schema_digest: fixture.command.request.input_schema_digest.clone(),
        output_schema_digest: fixture.command.request.output_schema_digest.clone(),
        error_schema_digest: fixture.error_schema_digest.clone(),
        artifact_contract: CapabilityArtifactContract { ports: vec![] },
        data_flow_policy: CapabilityDataFlowPolicy {
            maximum_input_classification: DataClassification::Restricted,
            maximum_output_classification: DataClassification::Restricted,
            allowed_regions: vec!["global".parse().unwrap()],
            declassification_policy: None,
        },
        interface_limits: CapabilityInterfaceLimits {
            maximum_input_bytes: 1_048_576,
            maximum_output_bytes: 1_048_576,
            maximum_artifacts: 0,
            maximum_execution_milliseconds: 60_000,
        },
        policies: InvocationPolicyDecisionBundle::build(vec![], None).unwrap(),
        principal,
        effect_key_digest: sha('c'),
        idempotency_key_digest: sha('d'),
        attempt_limit: 1,
        retry_backoff_milliseconds: fixture.command.request.retry_backoff_milliseconds,
        deadline: fixture.command.request.deadline,
        canonical_digest: sha('0'),
    });
    let invocation_observed_at = database_observed_at;
    let invocation = CapabilityInvocationRecord {
        tenant_id: fixture.tenant_id.clone(),
        invocation_id: fixture.command.request.invocation_id.clone(),
        trace: insight_platform_contracts::TraceIdentityV1::new(
            "0123456789abcdef0123456789abcdef"
                .parse::<insight_platform_contracts::TraceId>()
                .unwrap(),
        ),
        run_id: run_id.clone(),
        node_execution_id: node_id.clone(),
        owner_kind: ResourceKind::NodeExecution,
        owner_id: node_id,
        logical_key: format!("plan:{}", id(ResourceKind::NodeExecution, 56)),
        deployment_id: fixture
            .command
            .request
            .capability_deployment
            .deployment_id
            .clone(),
        input_value_id: fixture.command.request.input_value_id.clone(),
        output_value_id: None,
        effect_key_digest: admission.effect_key_digest.clone(),
        state: InvocationState::Ready,
        version: 1,
        payload: CapabilityInvocationPayload {
            schema_version: 1,
            admission,
            current_job_id: None,
            approval_task_id: None,
            input_task_id: None,
            detached_pending: None,
            result: None,
            failure: None,
            reconciliation: None,
        },
        deadline: fixture.command.request.deadline,
        retry_at: None,
        started_at: None,
        terminal_at: None,
        created_at: invocation_observed_at,
        updated_at: invocation_observed_at,
    };
    invocation.validate().unwrap();
    let invocation_payload = typed_payload(&invocation.payload);
    sqlx::query(
        r#"
        INSERT INTO insight_platform.invocations (
            tenant_id, invocation_id, invocation_kind, owner_kind, owner_id,
            logical_key, run_id, node_id, deployment_id, state, version,
            input_value_id, effect_key_digest, payload_schema_version, payload,
            payload_digest, deadline, started_at, created_at, updated_at, trace_id
        ) VALUES ($1, $2, 'capability', 'node_execution', $3, $4, $5, $3,
                  $6, 'ready', 1, $7, $8, $9, $10, $11, $12, NULL, $13, $13,
                  (SELECT trace_id FROM insight_platform.runs
                   WHERE tenant_id = $1 AND run_id = $5))
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.command.request.invocation_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(&invocation.logical_key)
    .bind(run_id.to_string())
    .bind(
        fixture
            .command
            .request
            .capability_deployment
            .deployment_id
            .to_string(),
    )
    .bind(invocation.input_value_id.to_string())
    .bind(invocation.effect_key_digest.to_string())
    .bind(invocation_payload.schema_version)
    .bind(&invocation_payload.value)
    .bind(&invocation_payload.digest)
    .bind(fixture.command.request.deadline)
    .bind(invocation_observed_at)
    .execute(pool)
    .await
    .unwrap();
    insert_sandbox_input_reference(
        pool,
        fixture,
        &fixture.command.request.invocation_id,
        &id(ResourceKind::ArtifactLink, 69),
        input_artifact,
    )
    .await;

    let metrics = [
        "durable_quota.sandbox_concurrent_executions",
        "durable_quota.sandbox_cpu_seconds",
        "durable_quota.sandbox_memory_mebibytes",
        "durable_quota.sandbox_output_bytes",
    ];
    for (ordinal, metric) in metrics.into_iter().enumerate() {
        let payload = TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_accounts (
                tenant_id, quota_account_id, scope_kind, scope_id, work_class,
                metric, limit_value, payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, 'tenant', $1, 'sandbox', $3, 100000000, $4, $5, $6)
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(id(ResourceKind::QuotaAccount, 60 + ordinal as u16).to_string())
        .bind(metric)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn insert_derived_sandbox_invocation(
    pool: &PgPool,
    fixture: &Fixture,
    request: &insight_platform_sandbox::SandboxExecutionRequest,
    node_id: ResourceId,
    activation_ordinal: i32,
) {
    let database_observed_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let run_id = id(ResourceKind::Run, 83);
    let node_payload = TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
    let plan_node_key = format!("sandbox-{activation_ordinal}");
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, record_kind, scope_id, plan_node_key,
            activation_ordinal, logical_key, node_kind, state, generation, version,
            payload_schema_version, payload, payload_digest, deadline, started_at,
            created_at, updated_at
        ) VALUES ($1, $2, $3, 'node_execution', $2, $4, $5, $4,
                  'capability_call', 'running', 1, 1, $6, $7, $8, $9, $10, $10, $10)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(run_id.to_string())
    .bind(&plan_node_key)
    .bind(activation_ordinal)
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .bind(request.deadline)
    .bind(database_observed_at)
    .execute(pool)
    .await
    .unwrap();

    let base_payload: serde_json::Value = sqlx::query_scalar(
        r#"
        SELECT payload FROM insight_platform.invocations
        WHERE tenant_id = $1 AND invocation_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.command.request.invocation_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let mut payload: CapabilityInvocationPayload = serde_json::from_value(base_payload).unwrap();
    payload.admission.origin_key = InvocationOrigin::PlanNode {
        node_execution_id: node_id.clone(),
    };
    let input_artifact_link_id = id(
        ResourceKind::ArtifactLink,
        340 + u16::try_from(activation_ordinal).unwrap(),
    );
    payload.admission.input_artifact_link_id = Some(input_artifact_link_id.clone());
    payload.admission = seal_admission(payload.admission);
    payload.current_job_id = None;
    let observed_at = database_observed_at;
    let invocation = CapabilityInvocationRecord {
        tenant_id: fixture.tenant_id.clone(),
        invocation_id: request.invocation_id.clone(),
        trace: insight_platform_contracts::TraceIdentityV1::new(
            "0123456789abcdef0123456789abcdef"
                .parse::<insight_platform_contracts::TraceId>()
                .unwrap(),
        ),
        run_id: run_id.clone(),
        node_execution_id: node_id.clone(),
        owner_kind: ResourceKind::NodeExecution,
        owner_id: node_id,
        logical_key: payload.admission.origin_key.logical_key(),
        deployment_id: request.capability_deployment.deployment_id.clone(),
        input_value_id: request.input_value_id.clone(),
        output_value_id: None,
        effect_key_digest: payload.admission.effect_key_digest.clone(),
        state: InvocationState::Ready,
        version: 1,
        payload,
        deadline: request.deadline,
        retry_at: None,
        started_at: None,
        terminal_at: None,
        created_at: observed_at,
        updated_at: observed_at,
    };
    invocation.validate().unwrap();
    let invocation_payload = typed_payload(&invocation.payload);
    sqlx::query(
        r#"
        INSERT INTO insight_platform.invocations (
            tenant_id, invocation_id, invocation_kind, owner_kind, owner_id,
            logical_key, run_id, node_id, deployment_id, state, version,
            input_value_id, effect_key_digest, payload_schema_version, payload,
            payload_digest, deadline, started_at, created_at, updated_at, trace_id
        ) VALUES ($1, $2, 'capability', 'node_execution', $3, $4, $5, $3,
                  $6, 'ready', 1, $7, $8, $9, $10, $11, $12, NULL, $13, $13,
                  (SELECT trace_id FROM insight_platform.runs
                   WHERE tenant_id = $1 AND run_id = $5))
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(request.invocation_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(&invocation.logical_key)
    .bind(run_id.to_string())
    .bind(request.capability_deployment.deployment_id.to_string())
    .bind(invocation.input_value_id.to_string())
    .bind(invocation.effect_key_digest.to_string())
    .bind(invocation_payload.schema_version)
    .bind(&invocation_payload.value)
    .bind(&invocation_payload.digest)
    .bind(request.deadline)
    .bind(observed_at)
    .execute(pool)
    .await
    .unwrap();
    let ValueRef::Artifact { artifact } = &request.input_ref else {
        panic!("derived Sandbox fixture input must be Artifact-backed");
    };
    insert_sandbox_input_reference(
        pool,
        fixture,
        &request.invocation_id,
        &input_artifact_link_id,
        artifact,
    )
    .await;
}

async fn seed_sandbox_input_artifact(pool: &PgPool, fixture: &Fixture) {
    let ValueRef::Artifact { artifact } = &fixture.command.request.input_ref else {
        panic!("Sandbox fixture input must be Artifact-backed");
    };
    let observed_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest, security_domain_digest,
            object_reference_ciphertext, object_generation, key_id, encryption_domain_id,
            content_digest, size_bytes, state, version, verified_at, created_at, updated_at
        ) VALUES (
            $1, $2, 's3', $3, $4, $5, 'fixture-generation-1', 'fixture-key-1', $6,
            $7, $8, 'verified', 1, $9, $9, $9
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::InternalBlob, 70).to_string())
    .bind(sha('1').to_string())
    .bind(sha('2').to_string())
    .bind(vec![1_u8, 2, 3])
    .bind(id(ResourceKind::EncryptionDomain, 71).to_string())
    .bind(artifact.content_digest().to_string())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(observed_at)
    .execute(pool)
    .await
    .unwrap();
    let metadata = TypedPayload::new(
        1,
        &serde_json::json!({"display_name": "sandbox-input.json"}),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, version, metadata_schema_version,
            metadata, metadata_digest, retention_policy_revision_id, retain_until,
            created_by, created_at, updated_at
        ) VALUES (
            $1, $2, $3, 'capability_input', 'internal', $4, $5,
            'application/json', 'application/json', 'ready', 1, $6, $7, $8,
            $9, $10, $11, $12, $12
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(id(ResourceKind::InternalBlob, 70).to_string())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(artifact.content_digest().to_string())
    .bind(metadata.schema_version)
    .bind(&metadata.value)
    .bind(&metadata.digest)
    .bind(id(ResourceKind::PolicyRevision, 66).to_string())
    .bind(observed_at + Duration::hours(1))
    .bind(fixture.principal_id.to_string())
    .bind(observed_at)
    .execute(pool)
    .await
    .unwrap();

    let runtime_bundle = &fixture.command.request.package.runtime_bundle_artifact;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest, security_domain_digest,
            object_reference_ciphertext, object_generation, key_id, encryption_domain_id,
            content_digest, size_bytes, state, version, verified_at, created_at, updated_at
        ) VALUES (
            $1, $2, 's3', $3, $4, $5, 'fixture-generation-2', 'fixture-key-2', $6,
            $7, $8, 'verified', 1, $9, $9, $9
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::InternalBlob, 72).to_string())
    .bind(sha('1').to_string())
    .bind(sha('3').to_string())
    .bind(vec![4_u8, 5, 6])
    .bind(id(ResourceKind::EncryptionDomain, 73).to_string())
    .bind(runtime_bundle.content_digest().to_string())
    .bind(i64::try_from(runtime_bundle.byte_length()).unwrap())
    .bind(observed_at)
    .execute(pool)
    .await
    .unwrap();
    let bundle_metadata = TypedPayload::new(
        1,
        &serde_json::json!({"display_name": "fixture-wasi-module.wasm"}),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, version, metadata_schema_version,
            metadata, metadata_digest, retention_policy_revision_id, retain_until,
            created_by, created_at, updated_at
        ) VALUES (
            $1, $2, $3, 'package', 'internal', $4, $5,
            'application/wasm', 'application/wasm', 'ready', 1, $6, $7, $8,
            $9, $10, $11, $12, $12
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(runtime_bundle.artifact_id().to_string())
    .bind(id(ResourceKind::InternalBlob, 72).to_string())
    .bind(i64::try_from(runtime_bundle.byte_length()).unwrap())
    .bind(runtime_bundle.content_digest().to_string())
    .bind(bundle_metadata.schema_version)
    .bind(&bundle_metadata.value)
    .bind(&bundle_metadata.digest)
    .bind(id(ResourceKind::PolicyRevision, 66).to_string())
    .bind(observed_at + Duration::hours(1))
    .bind(fixture.principal_id.to_string())
    .bind(observed_at)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_sandbox_input_reference(
    pool: &PgPool,
    fixture: &Fixture,
    invocation_id: &ResourceId,
    artifact_link_id: &ResourceId,
    artifact: &ArtifactRef,
) {
    let snapshot = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: artifact.artifact_id().clone(),
        owner_id: invocation_id.clone(),
        reference_kind: ArtifactReferenceKind::Input,
        purpose: ArtifactPurpose::CapabilityInput,
        created_by: fixture.principal_id.clone(),
    };
    let payload = typed_payload(&snapshot);
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest
        ) VALUES ($1, $2, 'reference', 'capability_invocation', $3, $4, $5,
                  'active', $6, $7, $8)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(artifact_link_id.to_string())
    .bind(invocation_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(snapshot.link_key_digest().unwrap().to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(pool)
    .await
    .unwrap();
}

fn usage() -> SandboxResourceUsage {
    SandboxResourceUsage {
        cpu_milliseconds: 1_500,
        peak_memory_mebibytes: 16,
        peak_pids: 1,
        files_created: 1,
        io_bytes: 1_024,
        stdout_bytes: 4,
        stderr_bytes: 0,
        result_bytes: 32,
        artifact_output_bytes: 0,
        network_connections: 0,
        network_request_bytes: 0,
        network_response_bytes: 0,
        wall_milliseconds: 25,
        wasm_fuel_consumed: Some(1_000),
    }
}

fn output(schema_digest: Sha256Digest) -> SandboxCompletedOutput {
    let value = serde_json::json!({"answer": 42});
    SandboxCompletedOutput {
        value_id: id(ResourceKind::RunValue, 42),
        classification: DataClassification::Internal,
        schema_digest,
        content_digest: canonical_digest(&value).unwrap().parse().unwrap(),
        value: ValueRef::Inline { value },
        artifact_link_ids: vec![],
        artifact_outputs: vec![],
        validation_evidence_digest: sha('5'),
        usage: usage(),
    }
}

struct FixtureArtifactUnsealer;

#[async_trait]
impl ArtifactObjectReferenceUnsealer for FixtureArtifactUnsealer {
    async fn unseal(
        &self,
        authorized: &AuthorizedArtifactObjectRead,
    ) -> Result<DecryptedArtifactObjectReference, ArtifactObjectReferenceUnsealError> {
        authorized
            .validate()
            .map_err(|_| ArtifactObjectReferenceUnsealError::InvalidEvidence)?;
        if authorized.backend != "s3" {
            return Err(ArtifactObjectReferenceUnsealError::InvalidEvidence);
        }
        let object_key = match (
            authorized.object_reference_ciphertext.as_bytes(),
            authorized.key_id.as_str(),
        ) {
            ([1_u8, 2, 3], "fixture-key-1") => "fixture/sandbox-input.json",
            ([4_u8, 5, 6], "fixture-key-2") => "fixture/runtime-bundle.wasm",
            _ => return Err(ArtifactObjectReferenceUnsealError::InvalidEvidence),
        };
        let plaintext = serde_jcs::to_vec(&serde_json::json!({
            "backend": "s3",
            "object_key": object_key,
            "schema_version": 1,
            "storage_binding_digest": authorized.storage_binding_digest,
        }))
        .map_err(|_| ArtifactObjectReferenceUnsealError::InvalidEvidence)?;
        DecryptedArtifactObjectReference::new(plaintext)
    }
}

struct FixtureArtifactStore {
    storage_binding_digest: Sha256Digest,
    input_bytes: Vec<u8>,
    runtime_bundle_bytes: Vec<u8>,
}

#[async_trait]
impl InstalledArtifactObjectStore for FixtureArtifactStore {
    fn backend(&self) -> &str {
        "s3"
    }

    fn storage_binding_digest(&self) -> &Sha256Digest {
        &self.storage_binding_digest
    }

    async fn head_exact(
        &self,
        object_key: &str,
        object_generation: &str,
    ) -> Result<ArtifactObjectMetadata, ArtifactObjectStoreError> {
        let bytes = match (object_key, object_generation) {
            ("fixture/sandbox-input.json", "fixture-generation-1") => &self.input_bytes,
            ("fixture/runtime-bundle.wasm", "fixture-generation-2") => &self.runtime_bundle_bytes,
            _ => return Err(ArtifactObjectStoreError::NotFound),
        };
        Ok(ArtifactObjectMetadata {
            object_generation: object_generation.to_owned(),
            byte_length: u64::try_from(bytes.len())
                .map_err(|_| ArtifactObjectStoreError::InvalidEvidence)?,
        })
    }

    async fn read_exact(
        &self,
        object_key: &str,
        object_generation: &str,
        maximum_bytes: usize,
    ) -> Result<ArtifactObjectBytes, ArtifactObjectStoreError> {
        let metadata = self.head_exact(object_key, object_generation).await?;
        let bytes = match object_key {
            "fixture/sandbox-input.json" => self.input_bytes.clone(),
            "fixture/runtime-bundle.wasm" => self.runtime_bundle_bytes.clone(),
            _ => return Err(ArtifactObjectStoreError::NotFound),
        };
        ArtifactObjectBytes::new(metadata, bytes, maximum_bytes)
    }
}

struct WasiBackend {
    descriptor: InstalledSandboxBackendDescriptor,
    artifact_broker: Arc<dyn WasiArtifactBroker>,
    validator: Arc<PgRepository>,
    grant_revoker: Arc<PgRepository>,
    worker_process_generation_id: ResourceId,
}

#[async_trait]
impl SandboxExecutorBackend for WasiBackend {
    fn descriptor(&self) -> InstalledSandboxBackendDescriptor {
        self.descriptor.clone()
    }

    async fn prepare(
        &self,
        request: insight_platform_sandbox::SandboxExecutionRequest,
    ) -> Result<PreparedSandbox, SandboxBackendFailure> {
        let runtime_bundle = self
            .artifact_broker
            .read_exact(WasiArtifactReadRequest {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                worker_process_generation_id: self.worker_process_generation_id.clone(),
                lease_generation: request.lease_generation,
                artifact: request.package.runtime_bundle_artifact.clone(),
                purpose: WasiArtifactReadPurpose::RuntimeBundle,
                read_grant: None,
                maximum_bytes: 1_048_576,
                deadline: request.deadline,
            })
            .await
            .map_err(|_| {
                test_backend_failure_with_code(
                    insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
                    "sandbox_fixture_runtime_bundle_read_failed",
                )
            })?;
        if runtime_bundle != runtime_bundle_bytes() {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
            ));
        }
        let ValueRef::Artifact { artifact } = &request.input_ref else {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
            ));
        };
        if request
            .artifact_grants
            .iter()
            .all(|grant| grant.artifact.as_ref() != Some(artifact))
        {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
            ));
        }
        let grant = request
            .artifact_grants
            .iter()
            .find(|grant| {
                grant.operation == ArtifactGrantOperation::ReadWhole
                    && grant.artifact.as_ref() == Some(artifact)
            })
            .cloned()
            .ok_or_else(|| {
                test_backend_failure(
                    insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
                )
            })?;
        let read = WasiArtifactReadRequest {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: self.worker_process_generation_id.clone(),
            lease_generation: request.lease_generation,
            artifact: artifact.clone(),
            purpose: WasiArtifactReadPurpose::InputValue,
            read_grant: Some(grant),
            maximum_bytes: 1_048_576,
            deadline: request.deadline,
        };
        let mut wrong_worker = read.clone();
        wrong_worker.worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 999);
        if !matches!(
            self.artifact_broker.read_exact(wrong_worker).await,
            Err(WasiArtifactBrokerError::Denied)
        ) {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
            ));
        }
        let mut wrong_purpose = read.clone();
        wrong_purpose.purpose = WasiArtifactReadPurpose::RuntimeBundle;
        wrong_purpose.read_grant = None;
        if !matches!(
            self.artifact_broker.read_exact(wrong_purpose).await,
            Err(WasiArtifactBrokerError::Denied)
        ) {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
            ));
        }
        let bytes = self.artifact_broker.read_exact(read).await.map_err(|_| {
            test_backend_failure_with_code(
                insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
                "sandbox_fixture_input_read_failed",
            )
        })?;
        let value = parse_strict_json(
            &bytes,
            JsonLimits {
                max_bytes: 1_048_576,
                max_depth: 16,
                max_properties_per_object: 64,
                max_items_per_array: 64,
                max_string_bytes: 262_144,
            },
        )
        .map_err(|_| {
            test_backend_failure(insight_platform_sandbox::SandboxBackendFailureStage::Preparing)
        })?;
        let mut validation = WasiValueValidationRequest {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 999),
            lease_generation: request.lease_generation,
            direction: WasiValueDirection::Input,
            schema_digest: request.input_schema_digest.clone(),
            classification: request.classification,
            value: value.clone(),
        };
        if !matches!(
            self.validator.validate(validation.clone()).await,
            Err(WasiValueValidationError::Invalid)
        ) {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
            ));
        }
        validation.worker_process_generation_id = self.worker_process_generation_id.clone();
        validation.value = serde_json::json!({"question": 42});
        if !matches!(
            self.validator.validate(validation.clone()).await,
            Err(WasiValueValidationError::Invalid)
        ) {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
            ));
        }
        validation.value = value;
        self.validator.validate(validation).await.map_err(|_| {
            test_backend_failure_with_code(
                insight_platform_sandbox::SandboxBackendFailureStage::Preparing,
                "sandbox_fixture_input_validation_failed",
            )
        })?;
        Ok(PreparedSandbox {
            provider_process_generation_id: None,
            sandbox_identity_digest: sha('6'),
            request_digest: request.request_digest,
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            prepare_evidence_digest: sha('7'),
        })
    }

    async fn start(
        &self,
        _request: insight_platform_sandbox::SandboxExecutionRequest,
        prepared: PreparedSandbox,
    ) -> Result<RunningSandbox, SandboxBackendFailure> {
        Ok(RunningSandbox {
            prepared,
            start_evidence_digest: sha('8'),
        })
    }

    async fn collect(
        &self,
        request: insight_platform_sandbox::SandboxExecutionRequest,
        running: RunningSandbox,
    ) -> Result<CollectedSandbox, SandboxBackendFailure> {
        let mut output = output(request.output_schema_digest.clone());
        let ValueRef::Inline { value } = &output.value else {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Collecting,
            ));
        };
        let mut validation = WasiValueValidationRequest {
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: self.worker_process_generation_id.clone(),
            lease_generation: request.lease_generation,
            direction: WasiValueDirection::Output,
            schema_digest: request.output_schema_digest.clone(),
            classification: request.classification,
            value: serde_json::json!({"answer": "forty-two"}),
        };
        if !matches!(
            self.validator.validate(validation.clone()).await,
            Err(WasiValueValidationError::Invalid)
        ) {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Collecting,
            ));
        }
        validation.value = value.clone();
        output.validation_evidence_digest =
            self.validator.validate(validation).await.map_err(|_| {
                test_backend_failure(
                    insight_platform_sandbox::SandboxBackendFailureStage::Collecting,
                )
            })?;
        Ok(CollectedSandbox {
            running,
            outcome: SandboxExecutionOutcome::Completed(Box::new(output)),
            usage: usage(),
            collection_evidence_digest: sha('9'),
        })
    }

    async fn terminate(
        &self,
        _command: TerminateSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure> {
        Ok(SandboxTerminationEvidence {
            evidence_digest: sha('a'),
            guest_acknowledged: true,
            process_tree_terminated: true,
            grants_revoked: true,
            external_effect_possible: false,
        })
    }

    async fn destroy(
        &self,
        command: DestroySandbox,
    ) -> Result<SandboxCleanupEvidence, SandboxBackendFailure> {
        let revoke = RevokeWasiSandboxGrants {
            tenant_id: command.tenant_id.clone(),
            sandbox_job_id: command.sandbox_job_id.clone(),
            request_digest: command.request_digest.clone(),
            worker_process_generation_id: self.worker_process_generation_id.clone(),
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
        };
        let first = WasiGrantRevoker::revoke_exact(self.grant_revoker.as_ref(), revoke.clone())
            .await
            .map_err(|_| {
                test_backend_failure(
                    insight_platform_sandbox::SandboxBackendFailureStage::Destroying,
                )
            })?;
        let replay = WasiGrantRevoker::revoke_exact(self.grant_revoker.as_ref(), revoke)
            .await
            .map_err(|_| {
                test_backend_failure(
                    insight_platform_sandbox::SandboxBackendFailureStage::Destroying,
                )
            })?;
        if first != replay {
            return Err(test_backend_failure(
                insight_platform_sandbox::SandboxBackendFailureStage::Destroying,
            ));
        }
        Ok(SandboxCleanupEvidence {
            disposition: SandboxCleanupDisposition::Destroyed,
            sandbox_identity_digest: command.sandbox_identity_digest,
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            observed_at: Utc::now(),
            evidence_digest: sha('b'),
        })
    }

    async fn abort(
        &self,
        command: insight_platform_sandbox::AbortSandboxExecution,
    ) -> Result<insight_platform_sandbox::SandboxAbortEvidence, SandboxBackendFailure> {
        Ok(insight_platform_sandbox::SandboxAbortEvidence {
            reason: command.reason,
            request_digest: command.request_digest,
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
            sandbox_identity_digest: sha('1'),
            termination: SandboxTerminationEvidence {
                evidence_digest: sha('a'),
                guest_acknowledged: true,
                process_tree_terminated: true,
                grants_revoked: true,
                external_effect_possible: false,
            },
            cleanup: SandboxCleanupEvidence {
                disposition: SandboxCleanupDisposition::Destroyed,
                sandbox_identity_digest: sha('1'),
                grants_revoked: true,
                ephemeral_storage_destroyed: true,
                observed_at: Utc::now(),
                evidence_digest: sha('b'),
            },
            evidence_digest: sha('2'),
        })
    }

    async fn recover_expired_lease(
        &self,
        expired: insight_platform_sandbox::ExpiredSandboxLease,
    ) -> Result<insight_platform_sandbox::SandboxLeaseRecoveryEvidence, SandboxBackendFailure> {
        let execution_may_have_started =
            expired.physical_state != insight_platform_contracts::SandboxJobState::Preparing;
        let external_effect_possible = execution_may_have_started
            && (expired.request.effect.risk_rank() >= Effect::IdempotentWrite.risk_rank()
                || expired.request.network_mode != SandboxNetworkMode::None);
        let sandbox_identity_digest = sha('1');
        insight_platform_sandbox::SandboxLeaseRecoveryEvidence {
            schema_version: 1,
            request_digest: expired.request.request_digest,
            attempt_no: expired.request.attempt_no,
            lease_generation: expired.observed_lease_generation,
            physical_state: expired.physical_state,
            executor_identity_digest: expired.executor_identity_digest.unwrap(),
            uncertainty: SandboxUncertainty {
                evidence_digest: sha('3'),
                sandbox_identity_digest: sandbox_identity_digest.clone(),
                execution_may_have_started,
                external_effect_possible,
                manual_reconciliation_required: external_effect_possible,
            },
            cleanup: SandboxCleanupEvidence {
                disposition: SandboxCleanupDisposition::Destroyed,
                sandbox_identity_digest,
                grants_revoked: true,
                ephemeral_storage_destroyed: true,
                observed_at: Utc::now(),
                evidence_digest: sha('4'),
            },
            evidence_digest: sha('0'),
        }
        .seal()
        .map_err(|_| SandboxBackendFailure {
            stage: insight_platform_sandbox::SandboxBackendFailureStage::Recovering,
            safe_code: "sandbox_recovery_evidence_invalid".to_owned(),
            safe_message: "Sandbox recovery evidence is invalid".to_owned(),
            retryability: insight_platform_contracts::Retryability::Never,
            evidence_digest: sha('5'),
            sandbox_identity_digest: None,
            execution_may_have_started,
            external_effect_possible,
        })
    }
}

fn test_backend_failure(
    stage: insight_platform_sandbox::SandboxBackendFailureStage,
) -> SandboxBackendFailure {
    test_backend_failure_with_code(stage, "sandbox_fixture_validation_failed")
}

fn test_backend_failure_with_code(
    stage: insight_platform_sandbox::SandboxBackendFailureStage,
    safe_code: &str,
) -> SandboxBackendFailure {
    SandboxBackendFailure {
        stage,
        safe_code: safe_code.to_owned(),
        safe_message: "Sandbox fixture value validation failed".to_owned(),
        retryability: insight_platform_contracts::Retryability::Never,
        evidence_digest: sha('f'),
        sandbox_identity_digest: None,
        execution_may_have_started: false,
        external_effect_possible: false,
    }
}

fn commit_identity(
    fixture: &Fixture,
    suffix: u16,
    character: char,
    expires_at: DateTime<Utc>,
) -> SandboxWorkerCommitIdentity {
    SandboxWorkerCommitIdentity {
        tenant_id: fixture.tenant_id.clone(),
        worker_process_generation_id: fixture.worker_id.clone(),
        receipt_id: id(ResourceKind::Receipt, suffix),
        event_id: id(ResourceKind::Event, suffix + 10),
        outbox_id: id(ResourceKind::OutboxEvent, suffix + 20),
        idempotency_key_digest: sha(character),
        receipt_expires_at: expires_at,
    }
}

fn audits(fixture: &Fixture, now: DateTime<Utc>) -> SandboxWorkerAuditBundle {
    let expires = now + Duration::minutes(5);
    SandboxWorkerAuditBundle {
        preparing: commit_identity(fixture, 80, '1', expires),
        starting: commit_identity(fixture, 81, '2', expires),
        running: commit_identity(fixture, 82, '3', expires),
        collecting: commit_identity(fixture, 83, '4', expires),
        cancelling: commit_identity(fixture, 84, '5', expires),
        terminal: commit_identity(fixture, 85, '6', expires),
    }
}

#[test]
fn sandbox_admission_and_every_executor_phase_are_one_fenced_postgres_authority() {
    std::thread::Builder::new()
        .name("phase4-sandbox-fixture".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(sandbox_fixture());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn sandbox_fixture() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let now = database_now;
    let fixture = fixture(now);
    seed(&pool, &repository, &fixture).await;

    sqlx::query(
        "UPDATE insight_platform.resources SET gate_state = 'suspended' WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::SandboxProfile, 55).to_string())
    .execute(&pool)
    .await
    .unwrap();
    let mut suspended_profile = fixture.command.clone();
    reidentify_accept_command(&mut suspended_profile, 820, '6');
    assert!(matches!(
        repository.accept_sandbox_execution(suspended_profile).await,
        Err(RepositoryError::Conflict("Sandbox Profile Deployment gate"))
    ));
    sqlx::query(
        "UPDATE insight_platform.resources SET gate_state = 'enabled' WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::SandboxProfile, 55).to_string())
    .execute(&pool)
    .await
    .unwrap();

    let policy_resource_id = fixture
        .resources
        .iter()
        .find(|(_, kind, exact, _)| {
            *kind == RegistryResourceKind::Policy
                && exact == &fixture.profile_closure.policy_bindings[0].revision
        })
        .map(|(resource_id, _, _, _)| resource_id)
        .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET gate_state = 'suspended' WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(policy_resource_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let mut suspended_policy = fixture.command.clone();
    reidentify_accept_command(&mut suspended_policy, 830, '7');
    assert!(matches!(
        repository.accept_sandbox_execution(suspended_policy).await,
        Err(RepositoryError::NotFound("active Policy Deployment"))
    ));
    sqlx::query(
        "UPDATE insight_platform.resources SET gate_state = 'enabled' WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(policy_resource_id.to_string())
    .execute(&pool)
    .await
    .unwrap();

    let mut drifted_closure = fixture.command.clone();
    let insight_platform_sandbox::SandboxExecutionSource::SandboxCapability {
        capability_deployment_closure: closure,
    } = &mut drifted_closure.request.execution_source;
    closure
        .policies
        .push(exact(ResourceKind::PolicyRevision, 299, '4'));
    drifted_closure.request.request_digest = sha('0');
    drifted_closure.request = drifted_closure.request.seal().unwrap();
    drifted_closure.audit.receipt_id = id(ResourceKind::Receipt, 290);
    drifted_closure.audit.event_id = id(ResourceKind::Event, 291);
    drifted_closure.audit.outbox_id = id(ResourceKind::OutboxEvent, 292);
    drifted_closure.audit.idempotency_key_digest = sha('4');
    drifted_closure.audit.request_digest = drifted_closure.request.request_digest.clone();
    drifted_closure.usage_reservation_id = id(ResourceKind::UsageReservation, 293);
    drifted_closure.quota_entry_ids = (294..298)
        .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
        .collect();
    assert!(matches!(
        repository.accept_sandbox_execution(drifted_closure).await,
        Err(RepositoryError::Conflict(
            "Sandbox exact Capability Deployment closure"
        ))
    ));

    let mut drifted_policy_document = fixture.command.clone();
    drifted_policy_document
        .request
        .policies
        .resource
        .maximum_cpu_millicores = 1_001;
    drifted_policy_document.request.request_digest = sha('0');
    drifted_policy_document.request = drifted_policy_document.request.seal().unwrap();
    drifted_policy_document.audit.receipt_id = id(ResourceKind::Receipt, 800);
    drifted_policy_document.audit.event_id = id(ResourceKind::Event, 801);
    drifted_policy_document.audit.outbox_id = id(ResourceKind::OutboxEvent, 802);
    drifted_policy_document.audit.idempotency_key_digest = sha('5');
    drifted_policy_document.audit.request_digest =
        drifted_policy_document.request.request_digest.clone();
    drifted_policy_document.usage_reservation_id = id(ResourceKind::UsageReservation, 803);
    drifted_policy_document.quota_entry_ids = (804..808)
        .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
        .collect();
    assert!(matches!(
        repository
            .accept_sandbox_execution(drifted_policy_document)
            .await,
        Err(RepositoryError::Conflict(
            "Sandbox frozen Policy document closure"
        ))
    ));

    for (ordinal, mutate) in [(0_u16, 0_u8), (1_u16, 1_u8), (2_u16, 2_u8), (3_u16, 3_u8)] {
        let mut rejected = fixture.command.clone();
        match mutate {
            0 => rejected.request.input_value_id = id(ResourceKind::RunValue, 301),
            1 => rejected.request.input_schema_digest = sha('2'),
            2 => rejected.request.output_schema_digest = sha('3'),
            3 => rejected.request.retry_backoff_milliseconds = 101,
            _ => unreachable!(),
        }
        rejected.request.request_digest = sha('0');
        rejected.request = rejected.request.seal().unwrap();
        rejected.audit.receipt_id = id(ResourceKind::Receipt, 300 + ordinal);
        rejected.audit.event_id = id(ResourceKind::Event, 303 + ordinal);
        rejected.audit.outbox_id = id(ResourceKind::OutboxEvent, 306 + ordinal);
        rejected.audit.idempotency_key_digest = sha(char::from(b'4' + ordinal as u8));
        rejected.audit.request_digest = rejected.request.request_digest.clone();
        rejected.usage_reservation_id = id(ResourceKind::UsageReservation, 309 + ordinal);
        rejected.quota_entry_ids = (0..4)
            .map(|offset| id(ResourceKind::QuotaLedgerEntry, 312 + ordinal * 4 + offset))
            .collect();
        assert!(matches!(
            repository.accept_sandbox_execution(rejected).await,
            Err(RepositoryError::Conflict(
                "Sandbox Capability Invocation value binding"
            ))
        ));
    }
    let rejected_side_effects: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.jobs WHERE tenant_id = $1),
          (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1),
          (SELECT COALESCE(sum(reserved_value), 0)::bigint
             FROM insight_platform.quota_accounts
            WHERE tenant_id = $1 AND work_class = 'sandbox')
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rejected_side_effects, (0, 0, 0));

    let mut prestart_request = fixture.command.request.clone();
    prestart_request.sandbox_job_id = id(ResourceKind::Job, 202);
    prestart_request.invocation_id = id(ResourceKind::CapabilityInvocation, 201);
    prestart_request.job_id = id(ResourceKind::Job, 202);
    prestart_request.output_value_id = id(ResourceKind::RunValue, 202);
    prestart_request.callback.sandbox_job_id = prestart_request.sandbox_job_id.clone();
    prestart_request.callback.invocation_id = prestart_request.invocation_id.clone();
    prestart_request.callback.binding_digest = sha('0');
    prestart_request.callback = prestart_request.callback.seal().unwrap();
    rebind_sandbox_input_grant(&mut prestart_request, 332);
    prestart_request.request_digest = sha('0');
    prestart_request = prestart_request.seal().unwrap();
    insert_derived_sandbox_invocation(
        &pool,
        &fixture,
        &prestart_request,
        id(ResourceKind::NodeExecution, 203),
        2,
    )
    .await;
    let prestart_command = AcceptSandboxExecution {
        audit: CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: fixture.tenant_id.clone(),
            principal_id: fixture.principal_id.clone(),
            principal_kind: PrincipalKind::ServiceIdentity,
            receipt_id: id(ResourceKind::Receipt, 204),
            event_id: id(ResourceKind::Event, 205),
            outbox_id: id(ResourceKind::OutboxEvent, 206),
            idempotency_key_digest: sha('7'),
            request_digest: prestart_request.request_digest.clone(),
            receipt_expires_at: now + Duration::minutes(5),
        },
        request: prestart_request.clone(),
        usage_reservation_id: id(ResourceKind::UsageReservation, 207),
        quota_entry_ids: (208..212)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
    };
    assert!(matches!(
        repository
            .accept_sandbox_execution(prestart_command)
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    let prestart_source_event_id = id(ResourceKind::Event, 214);
    let mut prestart_control_transaction = repository.begin().await.unwrap();
    let controlled = prestart_control_transaction
        .control_capability_invocation(ControlCapabilityInvocation {
            audit: CommandAudit {
                trace: insight_platform_contracts::TraceIdentityV1::generate(),
                tenant_id: fixture.tenant_id.clone(),
                principal_id: fixture.principal_id.clone(),
                principal_kind: PrincipalKind::ServiceIdentity,
                receipt_id: id(ResourceKind::Receipt, 213),
                event_id: prestart_source_event_id.clone(),
                outbox_id: id(ResourceKind::OutboxEvent, 215),
                idempotency_key_digest: sha('6'),
                request_digest: sha('5'),
                receipt_expires_at: now + Duration::minutes(5),
            },
            invocation_id: prestart_request.invocation_id.clone(),
            expected_invocation_version: 2,
            quota_entry_ids: vec![],
            kind: CapabilityControlKind::Cancel,
        })
        .await
        .unwrap();
    assert!(matches!(
        controlled,
        CommandOutcome::Applied(ref value)
            if value.invocation.state == InvocationState::Cancelling
                && value.invocation.version == 3
    ));
    prestart_control_transaction.commit().await.unwrap();
    let prestart_source_payload_digest: String = sqlx::query_scalar(
        "SELECT payload_digest FROM insight_platform.events WHERE tenant_id = $1 AND event_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(prestart_source_event_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut prestart_stop = StopUnclaimedSandboxJob {
        audit: SandboxControllerAudit {
            tenant_id: fixture.tenant_id.clone(),
            controller_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 216),
            source_event_id: prestart_source_event_id,
            source_invocation_version: 3,
            source_event_payload_digest: prestart_source_payload_digest.parse().unwrap(),
            receipt_id: id(ResourceKind::Receipt, 217),
            event_id: id(ResourceKind::Event, 218),
            outbox_id: id(ResourceKind::OutboxEvent, 219),
            idempotency_key_digest: sha('0'),
            request_digest: sha('0'),
            receipt_expires_at: now + Duration::minutes(5),
        },
        sandbox_job_id: prestart_request.sandbox_job_id.clone(),
        invocation_id: prestart_request.invocation_id.clone(),
        job_id: prestart_request.job_id.clone(),
        sandbox_request_digest: prestart_request.request_digest.clone(),
        reason: SandboxStopReason::Cancelled,
        quota_entry_ids: (220..224)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
    };
    prestart_stop.audit.idempotency_key_digest =
        prestart_stop.canonical_idempotency_key_digest().unwrap();
    prestart_stop.audit.request_digest = prestart_stop.canonical_request_digest().unwrap();
    let stopped = repository
        .stop_unclaimed_sandbox_job(prestart_stop.clone())
        .await
        .unwrap();
    assert!(matches!(
        stopped,
        SandboxPrestartControlOutcome::Applied(ref decision)
            if decision.payload.physical_state
                == insight_platform_contracts::SandboxJobState::Cancelled
                && decision.payload.executor_identity_digest.is_none()
                && decision.payload.outcome.is_none()
    ));
    let mut replay_stop = prestart_stop;
    replay_stop.audit.receipt_id = id(ResourceKind::Receipt, 225);
    replay_stop.audit.event_id = id(ResourceKind::Event, 226);
    replay_stop.audit.outbox_id = id(ResourceKind::OutboxEvent, 227);
    replay_stop.quota_entry_ids = (228..232)
        .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
        .collect();
    assert!(matches!(
        repository
            .stop_unclaimed_sandbox_job(replay_stop)
            .await
            .unwrap(),
        SandboxPrestartControlOutcome::Replayed(_)
    ));
    let prestart_reserved: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(reserved_value), 0)::bigint FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND work_class = 'sandbox'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(prestart_reserved, 0);

    let mut recovery_request = fixture.command.request.clone();
    recovery_request.sandbox_job_id = id(ResourceKind::Job, 242);
    recovery_request.invocation_id = id(ResourceKind::CapabilityInvocation, 241);
    recovery_request.job_id = id(ResourceKind::Job, 242);
    recovery_request.output_value_id = id(ResourceKind::RunValue, 242);
    recovery_request.callback.sandbox_job_id = recovery_request.sandbox_job_id.clone();
    recovery_request.callback.invocation_id = recovery_request.invocation_id.clone();
    recovery_request.callback.binding_digest = sha('0');
    recovery_request.callback = recovery_request.callback.seal().unwrap();
    rebind_sandbox_input_grant(&mut recovery_request, 333);
    recovery_request.request_digest = sha('0');
    recovery_request = recovery_request.seal().unwrap();
    insert_derived_sandbox_invocation(
        &pool,
        &fixture,
        &recovery_request,
        id(ResourceKind::NodeExecution, 243),
        3,
    )
    .await;
    let recovery_admission = AcceptSandboxExecution {
        audit: CommandAudit {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: fixture.tenant_id.clone(),
            principal_id: fixture.principal_id.clone(),
            principal_kind: PrincipalKind::ServiceIdentity,
            receipt_id: id(ResourceKind::Receipt, 244),
            event_id: id(ResourceKind::Event, 245),
            outbox_id: id(ResourceKind::OutboxEvent, 246),
            idempotency_key_digest: sha('8'),
            request_digest: recovery_request.request_digest.clone(),
            receipt_expires_at: now + Duration::minutes(5),
        },
        request: recovery_request.clone(),
        usage_reservation_id: id(ResourceKind::UsageReservation, 247),
        quota_entry_ids: (248..252)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
    };
    assert!(matches!(
        repository
            .accept_sandbox_execution(recovery_admission)
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    let recovery_claim = repository
        .claim_sandbox_jobs(ClaimSandboxJobs {
            worker_process_generation_id: fixture.worker_id.clone(),
            worker_manifest_digest: recovery_request.executor_worker_manifest_digest.clone(),
            isolation_backend_contract_digest: recovery_request
                .isolation_backend_contract_digest
                .clone(),
            executor_identity_digest: sha('d'),
            attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            limit: 1,
            lease_milliseconds: 500,
            lease_token_digests: vec![sha('e')],
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(recovery_claim.request.job_id, recovery_request.job_id);
    let mut preparing = CommitSandboxPhase {
        audit: SandboxWorkerAudit {
            tenant_id: fixture.tenant_id.clone(),
            worker_process_generation_id: fixture.worker_id.clone(),
            receipt_id: id(ResourceKind::Receipt, 252),
            event_id: id(ResourceKind::Event, 253),
            outbox_id: id(ResourceKind::OutboxEvent, 254),
            idempotency_key_digest: sha('9'),
            request_digest: sha('0'),
            receipt_expires_at: now + Duration::minutes(5),
        },
        sandbox_job_id: recovery_request.sandbox_job_id.clone(),
        job_id: recovery_request.job_id.clone(),
        fence: recovery_claim.fence.clone(),
        target: insight_platform_contracts::SandboxJobState::Preparing,
        executor_identity_digest: sha('a'),
        attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
        phase_evidence_digest: sha('b'),
        prepared: None,
    };
    preparing.audit.request_digest = preparing.canonical_request_digest().unwrap();
    assert!(matches!(
        repository.commit_sandbox_phase(preparing).await.unwrap(),
        CommandOutcome::Applied(_)
    ));
    tokio::time::sleep(std::time::Duration::from_millis(550)).await;
    let expired = repository
        .scan_expired_sandbox_leases(ScanExpiredSandboxLeases {
            shard: SafetyScanShard::whole(),
            after: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(expired.records.len(), 1);
    let expired = &expired.records[0];
    assert_eq!(expired.job_id, recovery_request.job_id);
    assert_eq!(
        expired.physical_state,
        insight_platform_contracts::SandboxJobState::Preparing
    );
    let recovered_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let sandbox_identity_digest = sha('c');
    let mut recovery = RecoverExpiredSandboxLease {
        audit: SandboxRecoveryAudit {
            tenant_id: fixture.tenant_id.clone(),
            recovery_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 255),
            receipt_id: id(ResourceKind::Receipt, 256),
            event_id: id(ResourceKind::Event, 257),
            outbox_id: id(ResourceKind::OutboxEvent, 258),
            idempotency_key_digest: sha('0'),
            request_digest: sha('0'),
            receipt_expires_at: recovered_at + Duration::minutes(5),
        },
        sandbox_job_id: expired.sandbox_job_id.clone(),
        invocation_id: expired.invocation_id.clone(),
        job_id: expired.job_id.clone(),
        sandbox_request_digest: expired.request.request_digest.clone(),
        observed_job_version: expired.observed_job_version,
        observed_lease_generation: expired.observed_lease_generation,
        previous_worker_process_generation_id: expired
            .previous_worker_process_generation_id
            .clone(),
        action: SandboxLeaseRecoveryAction::MarkLost {
            uncertainty: SandboxUncertainty {
                evidence_digest: sha('d'),
                sandbox_identity_digest: sandbox_identity_digest.clone(),
                execution_may_have_started: false,
                external_effect_possible: false,
                manual_reconciliation_required: false,
            },
            cleanup: SandboxCleanupEvidence {
                disposition: SandboxCleanupDisposition::NodeQuarantined,
                sandbox_identity_digest,
                grants_revoked: true,
                ephemeral_storage_destroyed: false,
                observed_at: recovered_at,
                evidence_digest: sha('e'),
            },
            recovery_evidence_digest: sha('f'),
        },
        quota_entry_ids: (259..263)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
    };
    recovery.audit.idempotency_key_digest = recovery.canonical_idempotency_key_digest().unwrap();
    recovery.audit.request_digest = recovery.canonical_request_digest().unwrap();
    let recovered = repository
        .recover_expired_sandbox_lease(recovery.clone())
        .await
        .unwrap();
    assert!(matches!(
        recovered,
        CommandOutcome::Applied(ref result)
            if result.disposition == SandboxLeaseRecoveryDisposition::Lost
    ));
    recovery.audit.receipt_id = id(ResourceKind::Receipt, 263);
    recovery.audit.event_id = id(ResourceKind::Event, 264);
    recovery.audit.outbox_id = id(ResourceKind::OutboxEvent, 265);
    recovery.quota_entry_ids = (266..270)
        .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
        .collect();
    assert!(matches!(
        repository
            .recover_expired_sandbox_lease(recovery)
            .await
            .unwrap(),
        CommandOutcome::Replayed(ref result)
            if result.disposition == SandboxLeaseRecoveryDisposition::Lost
    ));

    let accepted = repository
        .accept_sandbox_execution(fixture.command.clone())
        .await
        .unwrap();
    let accepted = match accepted {
        CommandOutcome::Applied(value) => value,
        CommandOutcome::Replayed(_) => panic!("fresh Sandbox admission replayed"),
    };
    assert_eq!(
        accepted.job.state,
        insight_platform_contracts::JobState::Ready
    );
    let wrong_manifest_claim = repository
        .claim_sandbox_jobs(ClaimSandboxJobs {
            worker_process_generation_id: fixture.worker_id.clone(),
            worker_manifest_digest: sha('1'),
            isolation_backend_contract_digest: fixture
                .command
                .request
                .isolation_backend_contract_digest
                .clone(),
            executor_identity_digest: sha('d'),
            attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            limit: 1,
            lease_milliseconds: 30_000_u64,
            lease_token_digests: vec![sha('a')],
        })
        .await
        .unwrap();
    assert!(wrong_manifest_claim.is_empty());
    let wrong_backend_claim = repository
        .claim_sandbox_jobs(ClaimSandboxJobs {
            worker_process_generation_id: fixture.worker_id.clone(),
            worker_manifest_digest: fixture
                .command
                .request
                .executor_worker_manifest_digest
                .clone(),
            isolation_backend_contract_digest: sha('2'),
            executor_identity_digest: sha('d'),
            attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            limit: 1,
            lease_milliseconds: 30_000_u64,
            lease_token_digests: vec![sha('b')],
        })
        .await
        .unwrap();
    assert!(wrong_backend_claim.is_empty());
    let claimed = repository
        .claim_sandbox_jobs(ClaimSandboxJobs {
            worker_process_generation_id: fixture.worker_id.clone(),
            worker_manifest_digest: fixture
                .command
                .request
                .executor_worker_manifest_digest
                .clone(),
            isolation_backend_contract_digest: fixture
                .command
                .request
                .isolation_backend_contract_digest
                .clone(),
            executor_identity_digest: sha('d'),
            attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            limit: 1,
            lease_milliseconds: 30_000_u64,
            lease_token_digests: vec![sha('c')],
        })
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let mut claimed = claimed[0].clone();
    assert_eq!(
        claimed.trace.trace_id.to_string(),
        "0123456789abcdef0123456789abcdef"
    );
    let heartbeat = repository
        .heartbeat_sandbox_execution(HeartbeatSandboxExecution {
            tenant_id: fixture.tenant_id.clone(),
            sandbox_job_id: claimed.request.sandbox_job_id.clone(),
            job_id: claimed.request.job_id.clone(),
            fence: claimed.fence.clone(),
            lease_milliseconds: 30_000,
        })
        .await
        .unwrap();
    assert_eq!(heartbeat.job.version, claimed.fence.expected_version + 1);
    assert_eq!(
        heartbeat
            .job
            .lease
            .as_ref()
            .unwrap()
            .worker_process_generation_id,
        fixture.worker_id
    );
    claimed.fence.expected_version = heartbeat.job.version;
    let leased_request = claimed.request.clone();

    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: leased_request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: leased_request.isolation_backend_contract_digest.clone(),
    };
    let repository = Arc::new(repository);
    let input_bytes = serde_jcs::to_vec(&serde_json::json!({"question": "life"})).unwrap();
    let store: Arc<dyn InstalledArtifactObjectStore> = Arc::new(FixtureArtifactStore {
        storage_binding_digest: sha('1'),
        input_bytes,
        runtime_bundle_bytes: runtime_bundle_bytes(),
    });
    let input_artifact = match &leased_request.input_ref {
        ValueRef::Artifact { artifact } => artifact.clone(),
        _ => panic!("Sandbox input must be Artifact-backed"),
    };
    let terminal_read = WasiArtifactReadRequest {
        tenant_id: leased_request.tenant_id.clone(),
        sandbox_job_id: leased_request.sandbox_job_id.clone(),
        request_digest: leased_request.request_digest.clone(),
        worker_process_generation_id: fixture.worker_id.clone(),
        lease_generation: leased_request.lease_generation,
        artifact: input_artifact,
        purpose: WasiArtifactReadPurpose::InputValue,
        read_grant: Some(leased_request.artifact_grants[0].clone()),
        maximum_bytes: 1_048_576,
        deadline: leased_request.deadline,
    };
    let artifact_read_authority = if let Ok(configured_role) =
        std::env::var("PLATFORM_ARTIFACT_DATA_READER_TEST_ROLE")
    {
        assert_eq!(
            configured_role,
            "platform_artifact_data_reader_qualification"
        );
        let restricted_pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|connection, _metadata| {
                Box::pin(async move {
                    sqlx::query("SET ROLE platform_artifact_data_reader_qualification")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .unwrap();
        verify_schema(&restricted_pool).await.unwrap();
        let forbidden_update = sqlx::query(
            "UPDATE insight_platform.jobs SET version = version WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(terminal_read.tenant_id.to_string())
        .bind(leased_request.job_id.to_string())
        .execute(&restricted_pool)
        .await
        .unwrap_err();
        assert_eq!(
            forbidden_update
                .as_database_error()
                .and_then(|failure| failure.code().map(|code| code.into_owned()))
                .as_deref(),
            Some("42501")
        );
        let forbidden_secret_read =
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM insight_platform.secret_bindings")
                .fetch_one(&restricted_pool)
                .await
                .unwrap_err();
        assert_eq!(
            forbidden_secret_read
                .as_database_error()
                .and_then(|failure| failure.code().map(|code| code.into_owned()))
                .as_deref(),
            Some("42501")
        );
        Arc::new(PgRepository::new(restricted_pool))
    } else {
        Arc::clone(&repository)
    };
    let artifact_broker = Arc::new(
        BrokeredSandboxArtifactBroker::new(
            Arc::clone(&artifact_read_authority)
                as Arc<
                    dyn insight_platform_artifacts::ArtifactObjectReadAuthority<
                        WasiArtifactReadRequest,
                    >,
                >,
            Arc::new(FixtureArtifactUnsealer),
            InstalledArtifactObjectStoreCatalog::new(vec![store]).unwrap(),
            ArtifactBrokerLimits::default(),
        )
        .unwrap(),
    );
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(WasiBackend {
            descriptor,
            artifact_broker: Arc::clone(&artifact_broker) as Arc<dyn WasiArtifactBroker>,
            validator: Arc::clone(&repository),
            grant_revoker: Arc::clone(&repository),
            worker_process_generation_id: fixture.worker_id.clone(),
        }))
        .unwrap();
    let worker = SandboxExecutorWorker::new(
        Arc::new(SandboxExecutorHost::new(
            registry,
            insight_platform_sandbox::SandboxCommandLimits::from_profile(
                &insight_platform_contracts::checked_in_hard_limit_profile(),
            )
            .unwrap(),
        )),
        Arc::clone(&repository),
        insight_platform_sandbox::SandboxCommandLimits::from_profile(
            &insight_platform_contracts::checked_in_hard_limit_profile(),
        )
        .unwrap(),
    );
    let terminal = worker
        .execute(ExecuteSandboxJob {
            request: leased_request,
            fence: claimed.fence.clone(),
            executor_identity_digest: sha('d'),
            attestor_route: "https://10.0.0.7:9443".parse().unwrap(),
            audits: audits(&fixture, now),
            usage_reservation_id: claimed.usage_reservation_id.clone(),
            quota_entry_ids: (90..94)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        })
        .await
        .unwrap();
    assert!(matches!(
        terminal,
        CommandOutcome::Applied(ref value)
            if value.payload.physical_state == insight_platform_contracts::SandboxJobState::Succeeded
    ));
    assert!(matches!(
        artifact_broker.read_exact(terminal_read).await,
        Err(WasiArtifactBrokerError::Denied)
    ));
    let grant_state: (String, i64, bool) = sqlx::query_as(
        r#"
        SELECT state, version, released_at IS NOT NULL
        FROM insight_platform.artifact_links
        WHERE tenant_id = $1 AND artifact_link_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(
        fixture.command.request.artifact_grants[0]
            .grant_id
            .to_string(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(grant_state, ("released".to_owned(), 2, true));
    let quota: Vec<(String, i64, i64)> = sqlx::query(
        r#"
        SELECT metric, reserved_value, used_value
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND work_class = 'sandbox'
        ORDER BY metric
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.get("metric"),
            row.get("reserved_value"),
            row.get("used_value"),
        )
    })
    .collect();
    assert!(quota.iter().all(|(_, reserved, _)| *reserved == 0));
    assert!(quota
        .iter()
        .any(|(metric, _, used)| { metric == "durable_quota.sandbox_cpu_seconds" && *used == 5 }));
    assert!(quota.iter().any(|(metric, _, used)| {
        metric == "durable_quota.sandbox_output_bytes" && *used == 1_050_660
    }));
    let pending = repository
        .scan_pending_sandbox_capability_outcomes(ScanPendingSandboxCapabilityOutcomes {
            shard: SafetyScanShard::whole(),
            after: None,
            limit: 16,
        })
        .await
        .unwrap();
    assert!(pending.exhausted);
    assert!(pending.next_cursor.is_none());
    let terminal_source = pending
        .records
        .iter()
        .find(|candidate| candidate.sandbox_job_id == fixture.command.request.sandbox_job_id)
        .unwrap();
    assert_eq!(terminal_source.tenant_id, fixture.tenant_id);
    assert_eq!(
        terminal_source.sandbox_job_id,
        fixture.command.request.sandbox_job_id
    );
    assert_eq!(
        terminal_source.invocation_id,
        fixture.command.request.invocation_id
    );
    assert_eq!(terminal_source.job_id, fixture.command.request.job_id);
    assert_eq!(terminal_source.expected_invocation_version, 2);
    let mut merge = MergeSandboxCapabilityOutcome {
        audit: SandboxOutcomeMergeAudit {
            tenant_id: fixture.tenant_id.clone(),
            controller_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 270),
            source_event_id: terminal_source.source_event_id.clone(),
            source_job_version: terminal_source.source_job_version,
            source_event_payload_digest: terminal_source.source_event_payload_digest.clone(),
            receipt_id: id(ResourceKind::Receipt, 271),
            event_id: id(ResourceKind::Event, 272),
            outbox_id: id(ResourceKind::OutboxEvent, 273),
            idempotency_key_digest: sha('0'),
            request_digest: sha('0'),
            receipt_expires_at: now + Duration::minutes(5),
        },
        sandbox_job_id: terminal_source.sandbox_job_id.clone(),
        invocation_id: terminal_source.invocation_id.clone(),
        job_id: terminal_source.job_id.clone(),
        sandbox_request_digest: terminal_source.sandbox_request_digest.clone(),
        expected_invocation_version: terminal_source.expected_invocation_version,
    };
    merge.audit.idempotency_key_digest = merge.canonical_idempotency_key_digest().unwrap();
    merge.audit.request_digest = merge.canonical_request_digest().unwrap();
    let merged = repository
        .merge_sandbox_capability_outcome(merge.clone())
        .await
        .unwrap();
    assert!(matches!(
        merged,
        CommandOutcome::Applied(ref invocation)
            if invocation.state == InvocationState::Succeeded
                && invocation.output_value_id.as_ref()
                    == Some(&fixture.command.request.output_value_id)
                && invocation.version == 3
    ));
    let authority_shape: (String, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT invocation.state, invocation.version,
               (SELECT count(*) FROM insight_platform.jobs AS job
                 WHERE job.tenant_id = invocation.tenant_id
                   AND job.invocation_id = invocation.invocation_id),
               (SELECT count(*) FROM insight_platform.run_values AS value
                 WHERE value.tenant_id = invocation.tenant_id
                   AND value.value_id = $3)
        FROM insight_platform.invocations AS invocation
        WHERE invocation.tenant_id = $1 AND invocation.invocation_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.command.request.invocation_id.to_string())
    .bind(fixture.command.request.output_value_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(authority_shape, ("succeeded".to_owned(), 3, 1, 1));
    assert!(matches!(
        repository
            .merge_sandbox_capability_outcome(merge)
            .await
            .unwrap(),
        CommandOutcome::Replayed(ref invocation)
            if invocation.state == InvocationState::Succeeded && invocation.version == 3
    ));
    let (receipt_count, event_count, outbox_count): (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1),
          (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1),
          (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((receipt_count, event_count, outbox_count), (13, 13, 13));

    let replay = repository
        .accept_sandbox_execution(fixture.command.clone())
        .await
        .unwrap();
    assert!(matches!(
        replay,
        CommandOutcome::Replayed(value)
            if value.payload.physical_state == insight_platform_contracts::SandboxJobState::Succeeded
    ));
}

#[test]
fn phase4_sandbox_fixture_compiles_without_postgres() {
    let now = Utc::now();
    let fixture = fixture(now);
    assert_eq!(
        fixture.command.request.isolation_class,
        SandboxIsolationClass::Wasm
    );
    assert_eq!(fixture.command.request.resources.cpu_millicores, 100);
}
