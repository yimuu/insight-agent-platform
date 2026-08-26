use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityAdapterResponse, InstalledMcpToolCodecDescriptor,
    InstalledMcpToolCodecRegistry, ManagedMcpToolDecodeContext, McpToolCapabilityCodec,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, AllowedMcpServerCapabilities, ArtifactRef,
    AuthoringPackage, CapabilityArtifactContract, CapabilityBackendBinding,
    CapabilityBackendContract, CapabilityBackendFeatures, CapabilityBackendKind,
    CapabilityBackendLimits, CapabilityCancellationKind, CapabilityDataFlowPolicy,
    CapabilityDeploymentClosure, CapabilityIdempotencyKind, CapabilityImplementationResourceSpec,
    CapabilityInterfaceLimits, CapabilityInterfaceResourceSpec, CapabilityProgressContract,
    CapabilityProgressDurability, CapabilityProgressMode, ClosedJsonSchema, ClosedJsonValue,
    CodeTrustClass, DataClassification, Effect, ExactDeploymentRef, ExactSandboxProfileBinding,
    ExactSecretBindingRef, ExactVersionRef, McpAuthorizationPrincipalKind, McpClientCapabilities,
    McpDeploymentClosure,
    McpMetadataPolicy, McpMethodLimits, McpNegotiatedCapabilities, McpProtocolPolicyDocument,
    McpServerExecutionContract, McpServerLimits, McpToolCapabilityContract, McpTransportBinding,
    McpTransportFeatures, McpTransportKind, PrincipalKind, PublishedMcpMethod, ResourceId,
    ResourceKind, SandboxAbiVersion, SandboxArtifactIoPolicyDocument, SandboxCleanupPolicy,
    SandboxEntrypointKind, SandboxIsolationClass, SandboxIsolationPolicyDocument,
    SandboxNetworkPolicyDocument, SandboxPackageResourceSpec, SandboxProfileResourceSpec,
    SandboxResourcePolicyDocument, SandboxRuntimeFamily, SandboxRuntimeResourceSpec,
    SandboxSecretResolutionPolicyDocument, SecretPurpose, SecretResolutionPolicy, Sha256Digest,
    ValueRef, MCP_PROTOCOL_BASELINE,
};
use insight_platform_invocations::{CapabilityOutputValue, DispatchOutcome};
use insight_platform_mcp_host::{
    McpAuthorizationContext, McpHostExecutionContract, McpLogicalOperationRequest,
    McpOperationOutcome, NewMcpAuthorizationContext, NewMcpHostExecutionContract,
};
use insight_platform_sandbox::{
    AbortSandboxExecution, DestroySandbox, ExpiredSandboxLease, InstalledSandboxBackendDescriptor,
    MicroVmIsolationProviderBackend, MicroVmProviderExecutionFence, SafeSandboxTraceContext,
    SandboxCleanupDisposition, SandboxCommandLimits, SandboxExecutionPolicyClosure,
    SandboxExecutionRequest, SandboxExecutionSource, SandboxIsolationBackendKind,
    SandboxNetworkMode, SandboxResourceEnvelope, SandboxResourceUsage, SandboxStopReason,
    ScopedSandboxCallback, ScopedSecretGrant, TerminateSandbox, SANDBOX_PROTOCOL_VERSION,
};
use insight_platform_sandbox_microvm::{
    read_guest_frame, write_guest_frame, DurableMicroVmLifecycle, FirecrackerApiClient,
    FirecrackerGuestChannel, FirecrackerInstallation, JailerLaunchPlan,
    ManagedMcpSandboxProtocolAdapter, MicroVmAbortProof, MicroVmAttemptKey, MicroVmCleanupProof,
    MicroVmGuestArtifactPurpose, MicroVmGuestCollection, MicroVmGuestCommand,
    MicroVmGuestCommandEnvelope, MicroVmGuestEvent, MicroVmGuestEventEnvelope, MicroVmHostFactory,
    MicroVmHostInstance, MicroVmLifecyclePort, MicroVmProviderClock, MicroVmProviderFailure,
    MicroVmRecoveryProof, MicroVmSandboxExecutorBackend, MicroVmTerminationProof, PreparedMicroVm,
    PreparedMicroVmHost, RunningMicroVm, FIRECRACKER_API_SOCKET_IN_JAIL,
    MICROVM_GUEST_PROTOCOL_VERSION,
};
use sha2::{Digest as _, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::BTreeMap, path::PathBuf, str::FromStr, sync::Arc, time::Duration as StdDuration,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixListener;

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1e8-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
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

fn content_sha(value: &[u8]) -> Sha256Digest {
    let digest = Sha256::digest(value);
    let hexadecimal = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{hexadecimal}").parse().unwrap()
}

fn artifact(suffix: u16, character: char) -> ArtifactRef {
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
        artifact: artifact(suffix, character),
        manifest_digest: sha(character),
    }
}

fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn deployment(kind: ResourceKind, suffix: u16, character: char) -> ExactDeploymentRef {
    ExactDeploymentRef::new(id(kind, suffix), sha(character)).unwrap()
}

fn limits() -> SandboxCommandLimits {
    SandboxCommandLimits::from_profile(&checked_in_hard_limit_profile()).unwrap()
}

fn policy_closure() -> SandboxExecutionPolicyClosure {
    SandboxExecutionPolicyClosure {
        isolation: SandboxIsolationPolicyDocument {
            schema_version: 2,
            minimum_isolation: SandboxIsolationClass::Wasm,
            allowed_runtime_families: vec![SandboxRuntimeFamily::WasmWasi],
            allowed_trust_classes: vec![CodeTrustClass::BuiltIn],
            require_hardware_virtualization_for_microvm: false,
            fresh_jail_per_job: true,
            fresh_guest_kernel_per_job: true,
            single_tenant_guest: true,
            single_use_guest: true,
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
            scanner_contract_digest: sha('8'),
            verification_evidence_ttl_milliseconds: 60_000,
            verification_retry_backoff_milliseconds: 1_000,
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

fn usage() -> SandboxResourceUsage {
    SandboxResourceUsage {
        cpu_milliseconds: 10,
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
        wasm_fuel_consumed: None,
    }
}

fn base_request(now: DateTime<Utc>) -> SandboxExecutionRequest {
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
        runtime_version: "wasmtime-42.0.0".to_owned(),
        image_or_module_digest: sha('3'),
        guest_kernel_digest: None,
        guest_agent_digest: sha('4'),
        supported_isolation: vec![SandboxIsolationClass::Wasm],
        abi: SandboxAbiVersion::V1,
        builtin_modules_manifest_digest: sha('5'),
        sbom_artifact: artifact(2, '6'),
        provenance_evidence: artifact(3, '7'),
        semantic_digest: sha('8'),
    };
    let package = SandboxPackageResourceSpec {
        authoring_package: authoring(4, '9'),
        contract_digest: sha('a'),
        dependency_versions: vec![runtime_revision.clone()],
        policy_versions: vec![],
        source_artifact: artifact(5, 'b'),
        source_digest: sha('b'),
        runtime_revision: runtime_revision.clone(),
        entrypoint_kind: SandboxEntrypointKind::WasmExport,
        entrypoint: "run".to_owned(),
        dependency_lock_digest: sha('c'),
        runtime_bundle_artifact: artifact(6, 'd'),
        build_evidence: artifact(7, 'e'),
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
    let deadline = now + Duration::minutes(1);
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
        conformance_evidence: artifact(27, 'a'),
    };
    SandboxExecutionRequest {
        schema_version: 1,
        protocol_version: SANDBOX_PROTOCOL_VERSION,
        tenant_id,
        sandbox_job_id,
        invocation_id,
        job_id: id(ResourceKind::Job, 23),
        expected_invocation_version: 1,
        attempt_no: 1,
        retry_backoff_milliseconds: 100,
        lease_generation: 0,
        capability_deployment: deployment(ResourceKind::CapabilityDeployment, 24, '5'),
        execution_source: SandboxExecutionSource::SandboxCapability {
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
            trace_id_digest: sha('8'),
            parent_span_id_digest: sha('9'),
        },
        request_digest: sha('0'),
    }
    .seal()
    .unwrap()
}

fn managed_protocol() -> McpProtocolPolicyDocument {
    McpProtocolPolicyDocument {
        schema_version: 1,
        offered_versions: vec![MCP_PROTOCOL_BASELINE.to_owned()],
        transport_features: McpTransportFeatures {
            streamable_http_get: false,
            streamable_http_sse: false,
            resumable_stream: false,
            session_affinity: true,
            managed_stdio: true,
        },
        client_capabilities: McpClientCapabilities {
            elicitation_form: false,
            elicitation_url: false,
            tasks_elicitation_create: false,
            sampling: false,
            roots: false,
        },
        allowed_server_capabilities: AllowedMcpServerCapabilities {
            tools: true,
            resources: false,
            prompts: false,
            logging: false,
            tasks: false,
            subscriptions: false,
        },
        experimental_features: vec![],
        method_limits: BTreeMap::from([(
            PublishedMcpMethod::ToolsCall,
            McpMethodLimits {
                maximum_request_bytes: 4_096,
                maximum_response_bytes: 4_096,
                maximum_metadata_entries: 16,
                maximum_progress_events: 8,
                maximum_pages: 4,
                minimum_poll_milliseconds: 10,
                maximum_poll_milliseconds: 1_000,
            },
        )]),
        metadata_policy: McpMetadataPolicy {
            maximum_server_name_bytes: 128,
            maximum_server_version_bytes: 64,
            maximum_instruction_bytes: 4_096,
            maximum_object_name_bytes: 128,
            maximum_description_bytes: 8_192,
            maximum_icon_bytes: 1_048_576,
        },
    }
}

fn managed_request(now: DateTime<Utc>) -> SandboxExecutionRequest {
    let mut request = base_request(now);
    let isolation_policy = request.profile.isolation_policy.clone();
    let resource_policy = request.profile.resource_policy.clone();
    let artifact_io_policy = request.profile.artifact_io_policy.clone();
    let secret_policy = exact(ResourceKind::PolicyRevision, 17, '8');
    request.runtime.runtime_family = SandboxRuntimeFamily::ManagedMcpServer;
    request.runtime.runtime_version = "mcp-runner-1.0.0".to_owned();
    request.runtime.guest_kernel_digest = Some(sha('9'));
    request.runtime.supported_isolation = vec![SandboxIsolationClass::MicroVm];
    request.package.entrypoint_kind = SandboxEntrypointKind::ManagedMcpServer;
    request.package.entrypoint = "bin/mcp-server".to_owned();
    request.profile.allowed_runtime_families = vec![SandboxRuntimeFamily::ManagedMcpServer];
    request.profile.minimum_isolation = SandboxIsolationClass::MicroVm;
    request.profile.secret_policy = Some(secret_policy.clone());
    request.profile.policy_versions.push(secret_policy);
    request.isolation_class = SandboxIsolationClass::MicroVm;
    request.resources.wasm_fuel = None;
    request.resources.wasm_memory_pages = None;

    let deployment = deployment(ResourceKind::McpDeployment, 40, '1');
    let server_revision = exact(ResourceKind::McpServerRevision, 41, '2');
    let protocol_profile = managed_protocol();
    let protocol_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 42),
        protocol_profile.canonical_digest().unwrap(),
    )
    .unwrap();
    let auth_policy = exact(ResourceKind::PolicyRevision, 44, '4');
    let server_identity_digest = sha('5');
    let token_purpose = SecretPurpose::from_str("mcp.oauth").unwrap();
    let token_binding = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 45),
        1,
        id(ResourceKind::SecretProvider, 46),
        token_purpose.clone(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: sha('6'),
        },
    )
    .unwrap();
    let deployment_closure = McpDeploymentClosure {
        server_revision: server_revision.clone(),
        server_identity_digest: server_identity_digest.clone(),
        transport: McpTransportBinding::ManagedStdio {
            package: request.package_revision.clone(),
            runtime: request.runtime_revision.clone(),
            profile: request.profile_binding.revision.clone(),
            isolation: SandboxIsolationClass::MicroVm,
            isolation_policy,
            resource_policy,
            artifact_io_policy,
        },
        protocol_policy: protocol_policy.clone(),
        trust_policy: exact(ResourceKind::PolicyRevision, 43, '3'),
        auth_policy: Some(auth_policy.clone()),
        secret_bindings: vec![],
        conformance_evidence: artifact(47, '7'),
    };
    let server = McpServerExecutionContract::build(
        server_revision.clone(),
        McpTransportKind::ManagedStdio,
        protocol_policy.clone(),
        vec![],
        Some(token_purpose),
        McpServerLimits {
            maximum_message_bytes: 8_192,
            maximum_response_bytes: 8_192,
            maximum_headers: 32,
            maximum_sse_event_bytes: 4_096,
            maximum_in_flight: 4,
            maximum_connections: 1,
            maximum_sessions: 1,
            maximum_session_milliseconds: 60_000,
            idle_timeout_milliseconds: 1_000,
            initialize_timeout_milliseconds: 1_000,
            request_timeout_milliseconds: 30_000,
            total_timeout_milliseconds: 30_000,
        },
    )
    .unwrap();
    let authorization = McpAuthorizationContext::build(NewMcpAuthorizationContext {
        tenant_id: request.tenant_id.clone(),
        authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 48),
        mcp_deployment: deployment.clone(),
        principal_kind: McpAuthorizationPrincipalKind::PerUser,
        principal_id: id(ResourceKind::Principal, 49),
        principal_identity_kind: PrincipalKind::AgentRunner,
        principal_binding_generation: 1,
        audience_identity_digest: server_identity_digest,
        granted_scopes: vec!["tools.call".to_owned()],
        token_secret_binding: token_binding.clone(),
        generation: 1,
        expires_at: now + Duration::minutes(5),
    })
    .unwrap();
    let discovery = insight_platform_contracts::McpDiscoverySnapshot::build(
        id(ResourceKind::McpDiscoverySnapshot, 50),
        deployment.clone(),
        server_revision,
        protocol_policy,
        authorization.canonical_digest.clone(),
        MCP_PROTOCOL_BASELINE.to_owned(),
        McpNegotiatedCapabilities {
            tools: true,
            resources: false,
            prompts: false,
            logging: false,
            tasks: false,
            tasks_list: false,
            tasks_cancel: false,
            tasks_tools_call: false,
            elicitation: false,
            sampling: false,
            roots: false,
            subscriptions: false,
        },
        artifact(51, '8'),
        now - Duration::seconds(1),
        now + Duration::minutes(4),
    )
    .unwrap();
    let contract = McpHostExecutionContract::build(NewMcpHostExecutionContract {
        deployment: deployment.clone(),
        deployment_closure,
        server,
        protocol_profile,
        authorization,
        discovery,
    })
    .unwrap();
    let operation = McpLogicalOperationRequest {
        schema_version: 1,
        mcp_operation_id: id(ResourceKind::McpOperation, 52),
        tenant_id: request.tenant_id.clone(),
        invocation_id: request.invocation_id.clone(),
        job_id: request.job_id.clone(),
        physical_attempt: request.attempt_no,
        snapshot_id: contract.discovery.snapshot_id.clone(),
        authorization_binding_id: contract.authorization.authorization_binding_id.clone(),
        method: PublishedMcpMethod::ToolsCall,
        params: ClosedJsonValue::build(sha('9'), serde_json::json!({"query": "hello"})).unwrap(),
        task_requested: false,
        continuation: None,
        idempotency_key_digest: sha('a'),
        idempotency: CapabilityIdempotencyKind::Intrinsic,
        effect: request.effect,
        deadline: request.deadline,
    };
    let input_schema = ClosedJsonSchema::build(serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"question": {"description": "input", "x-platform-classification": "internal", "type": "string", "minLength": 1, "maxLength": 64, "x-platform-max-bytes": 256}},
        "required": ["question"],
        "additionalProperties": false
    }))
    .unwrap();
    let output_schema = ClosedJsonSchema::build(serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"answer": {"description": "output", "x-platform-classification": "internal", "type": "integer", "minimum": 0, "maximum": 100}},
        "required": ["answer"],
        "additionalProperties": false
    }))
    .unwrap();
    let error_schema = ClosedJsonSchema::build(serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"error": {"description": "error", "x-platform-classification": "internal", "type": "string", "minLength": 1, "maxLength": 128, "x-platform-max-bytes": 512}},
        "required": ["error"],
        "additionalProperties": false
    }))
    .unwrap();
    request.input_schema_digest = input_schema.canonical_digest.clone();
    request.output_schema_digest = output_schema.canonical_digest.clone();
    let interface_revision = exact(ResourceKind::CapabilityInterfaceRevision, 55, 'c');
    let interface = CapabilityInterfaceResourceSpec {
        authoring_package: authoring(56, 'd'),
        contract_digest: sha('e'),
        dependency_versions: vec![],
        policy_versions: vec![],
        qualified_name: "fixture.managed_mcp".parse().unwrap(),
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
        effect: request.effect,
        idempotency: operation.idempotency,
        cancellation: CapabilityCancellationKind::Confirmed,
        progress: CapabilityProgressContract {
            mode: CapabilityProgressMode::None,
            schema_digest: None,
            max_events: 0,
            max_bytes_per_event: 0,
            minimum_interval_milliseconds: 0,
            durability: CapabilityProgressDurability::None,
        },
    };
    let backend_contract = CapabilityBackendContract::Mcp(McpToolCapabilityContract {
        remote_tool_name: "search".to_owned(),
        remote_input_schema_digest: operation.params.schema_digest.clone(),
        output_mapping_digest: sha('f'),
        protocol_profile: contract.server.protocol_policy.clone(),
        discovery_semantic_evidence_digest: contract.discovery.objects_digest.clone(),
        supports_task: false,
        supports_progress: false,
    });
    let implementation_revision = exact(ResourceKind::CapabilityImplementationRevision, 57, '1');
    let implementation = CapabilityImplementationResourceSpec {
        authoring_package: authoring(58, '2'),
        contract_digest: sha('3'),
        dependency_versions: vec![interface_revision.clone()],
        policy_versions: vec![],
        interface_revision: interface_revision.clone(),
        backend_kind: CapabilityBackendKind::Mcp,
        backend_contract_digest: backend_contract.canonical_digest().unwrap(),
        backend_contract,
        credential_requirements: vec![],
        backend_limits: CapabilityBackendLimits {
            maximum_request_bytes: 8_192,
            maximum_response_bytes: 8_192,
            maximum_diagnostic_bytes: 4_096,
            connect_timeout_milliseconds: 1_000,
            first_byte_timeout_milliseconds: 1_000,
            idle_timeout_milliseconds: 1_000,
            total_timeout_milliseconds: 30_000,
        },
        features: CapabilityBackendFeatures {
            deferred: false,
            input_required: false,
            callback: false,
            poll: false,
            progress: false,
            cancellation: true,
            max_remote_state_bytes: 0,
            max_poll_count: 0,
        },
    };
    let capability_closure = CapabilityDeploymentClosure {
        implementation: implementation_revision.clone(),
        interface: interface_revision.clone(),
        backend: CapabilityBackendBinding::Mcp {
            mcp_deployment: deployment,
            discovery_snapshot_id: contract.discovery.snapshot_id.clone(),
            discovery_snapshot_digest: contract.discovery.canonical_digest.clone(),
            authorization_policy: auth_policy,
        },
        secret_bindings: vec![],
        policies: vec![],
        conformance_evidence: artifact(59, '4'),
    };
    request.execution_source = SandboxExecutionSource::ManagedMcp {
        capability_deployment_closure: Box::new(capability_closure),
        capability_interface_revision: interface_revision,
        capability_interface: Box::new(interface),
        capability_implementation_revision: implementation_revision,
        capability_implementation: Box::new(implementation),
        mcp_contract: Box::new(contract),
        operation: Box::new(operation),
    };
    request.secret_grants = vec![ScopedSecretGrant {
        schema_version: 1,
        tenant_id: request.tenant_id.clone(),
        sandbox_job_id: request.sandbox_job_id.clone(),
        secret_binding: token_binding,
        resolved_binding_generation: 1,
        runtime_digest: request.runtime.image_or_module_digest.clone(),
        maximum_reads: 1,
        expires_at: request.deadline,
        grant_digest: sha('0'),
    }
    .seal()
    .unwrap()];
    request.policies.isolation.minimum_isolation = SandboxIsolationClass::MicroVm;
    request.policies.isolation.allowed_runtime_families =
        vec![SandboxRuntimeFamily::ManagedMcpServer];
    request
        .policies
        .isolation
        .require_hardware_virtualization_for_microvm = true;
    request.policies.resource.maximum_wasm_fuel = None;
    request.policies.resource.maximum_wasm_memory_pages = None;
    request.policies.secret_resolution = Some(SandboxSecretResolutionPolicyDocument {
        schema_version: 1,
        allowed_purposes: vec![request.secret_grants[0].secret_binding.purpose.clone()],
        maximum_reads_per_binding: 1,
        delivery_mode: insight_platform_contracts::SandboxSecretDeliveryMode::BrokeredMemory,
        deny_environment: true,
        deny_argv: true,
        deny_snapshot: true,
        scrub_after_read: true,
    });
    request.seal().unwrap()
}

#[tokio::test]
async fn guest_frames_are_canonical_and_exactly_fenced() {
    let request = base_request(Utc::now()).bind_lease_generation(1).unwrap();
    let command = MicroVmGuestCommandEnvelope::execute(request.clone()).unwrap();
    let (mut writer, mut reader) = tokio::io::duplex(1_048_576);
    let write = tokio::spawn(async move { write_guest_frame(&mut writer, &command).await });
    let decoded: MicroVmGuestCommandEnvelope = read_guest_frame(&mut reader).await.unwrap();
    write.await.unwrap().unwrap();
    decoded.validate().unwrap();

    let ready = MicroVmGuestEventEnvelope {
        protocol_version: MICROVM_GUEST_PROTOCOL_VERSION,
        sequence: 1,
        sandbox_job_id: request.sandbox_job_id.clone(),
        request_digest: request.request_digest.clone(),
        attempt_no: request.attempt_no,
        lease_generation: request.lease_generation,
        event: MicroVmGuestEvent::Ready {
            guest_agent_digest: request.runtime.guest_agent_digest.clone(),
            runtime_digest: request.runtime.image_or_module_digest.clone(),
        },
        envelope_digest: sha('0'),
    }
    .seal()
    .unwrap();
    ready.validate_for(&request, 1).unwrap();

    let mut stale = ready;
    stale.lease_generation += 1;
    assert!(stale.validate_for(&request, 1).is_err());
}

#[test]
fn guest_artifacts_are_chunked_canonical_and_delivered_before_execute() {
    let bytes = b"managed-mcp-runtime-bundle";
    let runtime_bundle = ArtifactRef::new(
        id(ResourceKind::Artifact, 90),
        content_sha(bytes),
        u64::try_from(bytes.len()).unwrap(),
        "application/octet-stream",
        DataClassification::Internal,
        Some("managed-mcp-runtime.bundle".to_owned()),
    )
    .unwrap();
    let mut request = managed_request(Utc::now())
        .bind_lease_generation(1)
        .unwrap();
    request.package.runtime_bundle_artifact = runtime_bundle.clone();
    request = request.seal().unwrap();

    let first = MicroVmGuestCommandEnvelope::materialize_artifact(
        &request,
        1,
        MicroVmGuestArtifactPurpose::RuntimeBundle,
        runtime_bundle.clone(),
        0,
        &bytes[..8],
        false,
    )
    .unwrap();
    let second = MicroVmGuestCommandEnvelope::materialize_artifact(
        &request,
        2,
        MicroVmGuestArtifactPurpose::RuntimeBundle,
        runtime_bundle.clone(),
        8,
        &bytes[8..],
        true,
    )
    .unwrap();
    let execute = MicroVmGuestCommandEnvelope::execute_at(request.clone(), 3).unwrap();
    let cancel =
        MicroVmGuestCommandEnvelope::cancel(&request, 4, SandboxStopReason::Cancelled).unwrap();
    for (expected_sequence, envelope) in [(1, &first), (2, &second), (3, &execute), (4, &cancel)] {
        assert_eq!(envelope.sequence, expected_sequence);
        envelope.validate().unwrap();
    }
    let MicroVmGuestCommand::MaterializeArtifact(chunk) = &second.command else {
        panic!("second command must carry the final runtime chunk")
    };
    assert_eq!(chunk.decoded_content().unwrap(), bytes[8..]);
    assert!(matches!(execute.command, MicroVmGuestCommand::Execute(_)));

    assert!(MicroVmGuestCommandEnvelope::materialize_artifact(
        &request,
        4,
        MicroVmGuestArtifactPurpose::RuntimeBundle,
        runtime_bundle,
        8,
        &bytes[8..],
        false,
    )
    .is_err());
}

#[tokio::test]
async fn guest_decoder_rejects_noncanonical_json() {
    let request = base_request(Utc::now()).bind_lease_generation(1).unwrap();
    let command = MicroVmGuestCommandEnvelope::execute(request).unwrap();
    let canonical = serde_jcs::to_vec(&command).unwrap();
    let mut noncanonical = Vec::with_capacity(canonical.len() + 1);
    noncanonical.push(b' ');
    noncanonical.extend(canonical);
    let (mut writer, mut reader) = tokio::io::duplex(1_048_576);
    writer
        .write_all(&u32::try_from(noncanonical.len()).unwrap().to_be_bytes())
        .await
        .unwrap();
    writer.write_all(&noncanonical).await.unwrap();
    drop(writer);
    assert!(
        read_guest_frame::<_, MicroVmGuestCommandEnvelope>(&mut reader)
            .await
            .is_err()
    );
}

#[test]
fn jailer_plan_is_single_use_and_resource_bounded() {
    let now = Utc::now();
    let request = managed_request(now).bind_lease_generation(1).unwrap();
    let directory = tempfile::tempdir_in("/private/tmp").unwrap();
    let installation = FirecrackerInstallation {
        version: "1.16.1".to_owned(),
        firecracker_path: PathBuf::from("/opt/firecracker/1.16.1/firecracker"),
        firecracker_digest: sha('1'),
        jailer_path: PathBuf::from("/opt/firecracker/1.16.1/jailer"),
        jailer_digest: sha('2'),
        chroot_base_directory: directory.path().to_path_buf(),
        parent_cgroup: "insight-sandbox".to_owned(),
    };
    let plan = JailerLaunchPlan::build(
        &installation,
        &request,
        "job-0198f1e8-32e4-75e1-a9e8-d95ca0f40017-a1-l1".to_owned(),
        200_001,
        200_001,
    )
    .unwrap();
    assert_eq!(plan.executable, installation.jailer_path);
    assert!(plan
        .arguments
        .windows(2)
        .any(|pair| pair == ["--new-pid-ns", "--"]));
    assert!(plan
        .arguments
        .iter()
        .any(|value| value == "memory.swap.max=0"));
    assert!(plan
        .arguments
        .windows(2)
        .any(|pair| pair == ["--api-sock", FIRECRACKER_API_SOCKET_IN_JAIL]));
    assert!(plan.api_socket.starts_with(&plan.jail_root));
    assert!(plan.vsock_path.starts_with(&plan.jail_root));
}

#[tokio::test]
async fn firecracker_api_keeps_configuration_and_start_as_separate_calls() {
    let request = managed_request(Utc::now())
        .bind_lease_generation(1)
        .unwrap();
    let directory = tempfile::tempdir_in("/private/tmp").unwrap();
    let socket = directory.path().join("firecracker.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let expected = [
            "/machine-config",
            "/boot-source",
            "/drives/rootfs",
            "/vsock",
            "/actions",
        ];
        for path in expected {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            stream.read_to_end(&mut bytes).await.unwrap();
            let request = String::from_utf8(bytes).unwrap();
            assert!(request.starts_with(&format!("PUT {path} HTTP/1.1\r\n")));
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        }
    });
    let client = FirecrackerApiClient::new(socket, StdDuration::from_secs(1)).unwrap();
    client.configure(&request, 9).await.unwrap();
    client.start().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn firecracker_vsock_handshake_preserves_the_exact_guest_fence() {
    let request = managed_request(Utc::now())
        .bind_lease_generation(1)
        .unwrap();
    let command = MicroVmGuestCommandEnvelope::execute(request.clone()).unwrap();
    let ready = MicroVmGuestEventEnvelope {
        protocol_version: MICROVM_GUEST_PROTOCOL_VERSION,
        sequence: 1,
        sandbox_job_id: request.sandbox_job_id.clone(),
        request_digest: request.request_digest.clone(),
        attempt_no: request.attempt_no,
        lease_generation: request.lease_generation,
        event: MicroVmGuestEvent::Ready {
            guest_agent_digest: request.runtime.guest_agent_digest.clone(),
            runtime_digest: request.runtime.image_or_module_digest.clone(),
        },
        envelope_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let directory = tempfile::tempdir_in("/private/tmp").unwrap();
    let socket = directory.path().join("guest.vsock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut handshake = [0_u8; 14];
        stream.read_exact(&mut handshake).await.unwrap();
        assert_eq!(&handshake, b"CONNECT 10000\n");
        stream.write_all(b"OK 10000\n").await.unwrap();
        let received: MicroVmGuestCommandEnvelope = read_guest_frame(&mut stream).await.unwrap();
        received.validate().unwrap();
        write_guest_frame(&mut stream, &ready).await.unwrap();
    });
    let mut channel = FirecrackerGuestChannel::connect(&socket, 10_000, StdDuration::from_secs(1))
        .await
        .unwrap();
    channel.write_command(&command).await.unwrap();
    let received = channel.read_event().await.unwrap();
    received.validate_for(&request, 1).unwrap();
    server.await.unwrap();
}

struct CountingHostInstance {
    identity: Sha256Digest,
    starts: AtomicUsize,
    collections: AtomicUsize,
    terminations: AtomicUsize,
    destroys: AtomicUsize,
}

#[async_trait]
impl MicroVmHostInstance for CountingHostInstance {
    fn sandbox_identity_digest(&self) -> Sha256Digest {
        self.identity.clone()
    }

    async fn start(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _request: &SandboxExecutionRequest,
    ) -> Result<RunningMicroVm, MicroVmProviderFailure> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(RunningMicroVm {
            sandbox_identity_digest: self.identity.clone(),
            start_evidence_digest: sha('b'),
        })
    }

    async fn collect(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _request: &SandboxExecutionRequest,
    ) -> Result<MicroVmGuestCollection, MicroVmProviderFailure> {
        self.collections.fetch_add(1, Ordering::SeqCst);
        Ok(MicroVmGuestCollection::ManagedMcp {
            outcome: Box::new(McpOperationOutcome::Completed {
                result: ClosedJsonValue::build(sha('d'), serde_json::json!({"answer": 42}))
                    .unwrap(),
                evidence_digest: sha('e'),
            }),
            usage: usage(),
            protocol_evidence_digest: sha('f'),
            collection_evidence_digest: sha('1'),
        })
    }

    async fn terminate(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _command: &TerminateSandbox,
    ) -> Result<MicroVmTerminationProof, MicroVmProviderFailure> {
        self.terminations.fetch_add(1, Ordering::SeqCst);
        Ok(MicroVmTerminationProof {
            sandbox_identity_digest: self.identity.clone(),
            evidence_digest: sha('2'),
            guest_acknowledged: true,
            process_tree_terminated: true,
            grants_revoked: true,
            external_effect_possible: false,
        })
    }

    async fn destroy(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _command: &DestroySandbox,
    ) -> Result<MicroVmCleanupProof, MicroVmProviderFailure> {
        self.destroys.fetch_add(1, Ordering::SeqCst);
        Ok(MicroVmCleanupProof {
            sandbox_identity_digest: self.identity.clone(),
            evidence_digest: sha('3'),
            disposition: SandboxCleanupDisposition::Destroyed,
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            observed_at: Utc::now(),
        })
    }

    async fn abort(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _command: &AbortSandboxExecution,
    ) -> Result<MicroVmAbortProof, MicroVmProviderFailure> {
        unreachable!()
    }

    async fn recover_expired(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _expired: &ExpiredSandboxLease,
    ) -> Result<MicroVmRecoveryProof, MicroVmProviderFailure> {
        unreachable!()
    }
}

struct CountingHostFactory {
    instance: Arc<CountingHostInstance>,
    expected_worker_process_generation_id: ResourceId,
    prepares: AtomicUsize,
    opens: AtomicUsize,
}

#[async_trait]
impl MicroVmHostFactory for CountingHostFactory {
    async fn prepare_exact(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _request: &SandboxExecutionRequest,
    ) -> Result<PreparedMicroVmHost, MicroVmProviderFailure> {
        self.prepares.fetch_add(1, Ordering::SeqCst);
        Ok(PreparedMicroVmHost {
            instance: self.instance.clone(),
            evidence: PreparedMicroVm {
                provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 61),
                sandbox_identity_digest: self.instance.identity.clone(),
                prepare_evidence_digest: sha('a'),
            },
        })
    }

    async fn open_exact(
        &self,
        key: &MicroVmAttemptKey,
        expected_sandbox_identity_digest: Option<&Sha256Digest>,
    ) -> Result<Option<PreparedMicroVmHost>, MicroVmProviderFailure> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        if key.executor_worker_process_generation_id != self.expected_worker_process_generation_id
            || expected_sandbox_identity_digest
                .is_some_and(|expected| expected != &self.instance.identity)
        {
            return Ok(None);
        }
        Ok(Some(PreparedMicroVmHost {
            instance: self.instance.clone(),
            evidence: PreparedMicroVm {
                provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 61),
                sandbox_identity_digest: self.instance.identity.clone(),
                prepare_evidence_digest: sha('a'),
            },
        }))
    }
}

#[tokio::test]
async fn durable_lifecycle_single_flights_retries_and_reopens_after_provider_restart() {
    let request = managed_request(Utc::now())
        .bind_lease_generation(1)
        .unwrap();
    let instance = Arc::new(CountingHostInstance {
        identity: sha('9'),
        starts: AtomicUsize::new(0),
        collections: AtomicUsize::new(0),
        terminations: AtomicUsize::new(0),
        destroys: AtomicUsize::new(0),
    });
    let factory = Arc::new(CountingHostFactory {
        instance: Arc::clone(&instance),
        expected_worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 60),
        prepares: AtomicUsize::new(0),
        opens: AtomicUsize::new(0),
    });
    let lifecycle = DurableMicroVmLifecycle::new(Arc::clone(&factory), 8).unwrap();
    let fence = MicroVmProviderExecutionFence {
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 60),
    };
    let prepared = lifecycle.prepare(&fence, &request).await.unwrap();
    assert_eq!(lifecycle.prepare(&fence, &request).await.unwrap(), prepared);
    assert_eq!(factory.prepares.load(Ordering::SeqCst), 1);

    let wrong_fence = MicroVmProviderExecutionFence {
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 61),
    };
    assert!(lifecycle
        .start(&wrong_fence, &request, &prepared)
        .await
        .is_err());
    assert_eq!(instance.starts.load(Ordering::SeqCst), 0);

    let running = lifecycle.start(&fence, &request, &prepared).await.unwrap();
    assert_eq!(
        lifecycle.start(&fence, &request, &prepared).await.unwrap(),
        running
    );
    assert_eq!(instance.starts.load(Ordering::SeqCst), 1);
    let collected = lifecycle.collect(&fence, &request, &running).await.unwrap();
    assert_eq!(
        lifecycle.collect(&fence, &request, &running).await.unwrap(),
        collected
    );
    assert_eq!(instance.collections.load(Ordering::SeqCst), 1);

    let restarted = DurableMicroVmLifecycle::new(Arc::clone(&factory), 8).unwrap();
    let command = TerminateSandbox {
        tenant_id: request.tenant_id.clone(),
        sandbox_job_id: request.sandbox_job_id.clone(),
        request_digest: request.request_digest.clone(),
        sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
        attempt_no: request.attempt_no,
        lease_generation: request.lease_generation,
        effect: request.effect,
        network_mode: request.network_mode,
        deadline: request.deadline,
    };
    let proof = restarted.terminate(&fence, &command).await.unwrap();
    assert!(proof.process_tree_terminated);
    assert_eq!(factory.opens.load(Ordering::SeqCst), 2);
    assert_eq!(instance.terminations.load(Ordering::SeqCst), 1);
    assert_eq!(restarted.terminate(&fence, &command).await.unwrap(), proof);
    assert_eq!(instance.terminations.load(Ordering::SeqCst), 1);

    let cleanup = restarted
        .destroy(
            &fence,
            &DestroySandbox {
                tenant_id: request.tenant_id.clone(),
                sandbox_job_id: request.sandbox_job_id.clone(),
                request_digest: request.request_digest.clone(),
                sandbox_identity_digest: prepared.sandbox_identity_digest,
                attempt_no: request.attempt_no,
                lease_generation: request.lease_generation,
                effect: request.effect,
                network_mode: request.network_mode,
            },
        )
        .await
        .unwrap();
    assert!(cleanup.ephemeral_storage_destroyed);
    assert_eq!(instance.destroys.load(Ordering::SeqCst), 1);
}

struct CompletedCodec {
    descriptor: InstalledMcpToolCodecDescriptor,
}

impl McpToolCapabilityCodec for CompletedCodec {
    fn descriptor(&self) -> InstalledMcpToolCodecDescriptor {
        self.descriptor.clone()
    }

    fn encode(
        &self,
        _request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
    ) -> Result<ClosedJsonValue, CapabilityAdapterFailure> {
        unreachable!("ordinary Capability Workers cannot encode Managed stdio")
    }

    fn decode(
        &self,
        _request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
        _outcome: McpOperationOutcome,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        unreachable!("ordinary Capability Workers cannot decode Managed stdio")
    }

    fn decode_managed_sandbox(
        &self,
        context: &ManagedMcpToolDecodeContext<'_>,
        outcome: McpOperationOutcome,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        assert!(matches!(outcome, McpOperationOutcome::Completed { .. }));
        let value = serde_json::json!({"answer": 42});
        Ok(CapabilityAdapterResponse {
            outcome: DispatchOutcome::Completed(CapabilityOutputValue {
                value_id: context.output_value_id.clone(),
                classification: context.classification,
                schema_digest: context.output_schema_digest.clone(),
                content_digest: canonical_digest(&value).unwrap().parse().unwrap(),
                value: ValueRef::Inline { value },
                artifact_link_id: None,
                validation_evidence_digest: sha('e'),
            }),
        })
    }
}

#[test]
fn managed_mcp_raw_exchange_is_mapped_only_by_the_microvm_provider_codec() {
    let now = Utc::now();
    let request = managed_request(now).bind_lease_generation(1).unwrap();
    request.validate_at(now, limits()).unwrap();
    let SandboxExecutionSource::ManagedMcp {
        capability_implementation,
        ..
    } = &request.execution_source
    else {
        unreachable!()
    };
    let CapabilityBackendContract::Mcp(tool) = &capability_implementation.backend_contract else {
        unreachable!()
    };
    let mut codecs = InstalledMcpToolCodecRegistry::default();
    codecs
        .install(Arc::new(CompletedCodec {
            descriptor: InstalledMcpToolCodecDescriptor::from(tool),
        }))
        .unwrap();
    let adapter = ManagedMcpSandboxProtocolAdapter::new(codecs, limits());
    let mapped = adapter
        .decode(
            &request,
            id(ResourceKind::WorkerProcessGeneration, 60),
            McpOperationOutcome::Completed {
                result: ClosedJsonValue::build(sha('d'), serde_json::json!({"answer": 42}))
                    .unwrap(),
                evidence_digest: sha('e'),
            },
            now + Duration::milliseconds(1),
            sha('f'),
            usage(),
        )
        .unwrap();
    assert!(matches!(mapped.outcome, DispatchOutcome::Completed(_)));
    assert_eq!(mapped.logical_poll_count, 0);
    assert_eq!(mapped.usage, usage());
}

#[test]
fn managed_mcp_completion_without_the_exact_codec_fails_closed() {
    let now = Utc::now();
    let request = managed_request(now).bind_lease_generation(1).unwrap();
    let adapter =
        ManagedMcpSandboxProtocolAdapter::new(InstalledMcpToolCodecRegistry::default(), limits());
    let failure = adapter
        .decode(
            &request,
            id(ResourceKind::WorkerProcessGeneration, 60),
            McpOperationOutcome::Completed {
                result: ClosedJsonValue::build(sha('d'), serde_json::json!({"answer": 42}))
                    .unwrap(),
                evidence_digest: sha('e'),
            },
            now + Duration::milliseconds(1),
            sha('f'),
            usage(),
        )
        .unwrap_err();
    assert_eq!(failure.safe_code, "protocol_codec_not_installed");
}

struct FixedClock(DateTime<Utc>);

impl MicroVmProviderClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

struct FixtureLifecycle {
    raw_outcome: McpOperationOutcome,
}

#[async_trait]
impl MicroVmLifecyclePort for FixtureLifecycle {
    async fn prepare(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _request: &SandboxExecutionRequest,
    ) -> Result<PreparedMicroVm, MicroVmProviderFailure> {
        Ok(PreparedMicroVm {
            provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 61),
            sandbox_identity_digest: sha('1'),
            prepare_evidence_digest: sha('2'),
        })
    }

    async fn start(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _request: &SandboxExecutionRequest,
        prepared: &PreparedMicroVm,
    ) -> Result<RunningMicroVm, MicroVmProviderFailure> {
        Ok(RunningMicroVm {
            sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
            start_evidence_digest: sha('3'),
        })
    }

    async fn collect(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _request: &SandboxExecutionRequest,
        _running: &RunningMicroVm,
    ) -> Result<MicroVmGuestCollection, MicroVmProviderFailure> {
        Ok(MicroVmGuestCollection::ManagedMcp {
            outcome: Box::new(self.raw_outcome.clone()),
            usage: usage(),
            protocol_evidence_digest: sha('4'),
            collection_evidence_digest: sha('5'),
        })
    }

    async fn terminate(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _command: &TerminateSandbox,
    ) -> Result<MicroVmTerminationProof, MicroVmProviderFailure> {
        unreachable!()
    }

    async fn destroy(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _command: &DestroySandbox,
    ) -> Result<MicroVmCleanupProof, MicroVmProviderFailure> {
        unreachable!()
    }

    async fn abort(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _command: &AbortSandboxExecution,
    ) -> Result<MicroVmAbortProof, MicroVmProviderFailure> {
        unreachable!()
    }

    async fn recover_expired(
        &self,
        _fence: &MicroVmProviderExecutionFence,
        _expired: &ExpiredSandboxLease,
    ) -> Result<MicroVmRecoveryProof, MicroVmProviderFailure> {
        unreachable!()
    }
}

#[tokio::test]
async fn provider_lifecycle_never_exposes_raw_managed_mcp_as_a_generic_sandbox_result() {
    let now = Utc::now();
    let request = managed_request(now).bind_lease_generation(1).unwrap();
    let SandboxExecutionSource::ManagedMcp {
        capability_implementation,
        ..
    } = &request.execution_source
    else {
        unreachable!()
    };
    let CapabilityBackendContract::Mcp(tool) = &capability_implementation.backend_contract else {
        unreachable!()
    };
    let mut codecs = InstalledMcpToolCodecRegistry::default();
    codecs
        .install(Arc::new(CompletedCodec {
            descriptor: InstalledMcpToolCodecDescriptor::from(tool),
        }))
        .unwrap();
    let worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 60);
    let backend = MicroVmSandboxExecutorBackend::new(
        InstalledSandboxBackendDescriptor {
            backend_kind: SandboxIsolationBackendKind::MicroVm,
            isolation_class: SandboxIsolationClass::MicroVm,
            worker_manifest_digest: request.executor_worker_manifest_digest.clone(),
            backend_contract_digest: request.isolation_backend_contract_digest.clone(),
        },
        limits(),
        Arc::new(FixtureLifecycle {
            raw_outcome: McpOperationOutcome::Completed {
                result: ClosedJsonValue::build(sha('d'), serde_json::json!({"answer": 42}))
                    .unwrap(),
                evidence_digest: sha('e'),
            },
        }),
        Arc::new(ManagedMcpSandboxProtocolAdapter::new(codecs, limits())),
        Arc::new(FixedClock(now + Duration::milliseconds(1))),
    )
    .unwrap();
    let fence = MicroVmProviderExecutionFence {
        worker_process_generation_id: worker_process_generation_id.clone(),
    };
    let prepared = backend
        .prepare(fence.clone(), request.clone())
        .await
        .unwrap();
    let running = backend
        .start(fence.clone(), request.clone(), prepared)
        .await
        .unwrap();
    let collected = backend.collect(fence, request, running).await.unwrap();
    let insight_platform_sandbox::SandboxExecutionOutcome::ManagedMcp(mapped) = collected.outcome
    else {
        panic!("Managed MCP must remain a tagged trusted Sandbox result")
    };
    assert_eq!(
        mapped.worker_process_generation_id,
        worker_process_generation_id
    );
    assert!(matches!(mapped.outcome, DispatchOutcome::Completed(_)));
    assert_eq!(mapped.usage, collected.usage);
}
