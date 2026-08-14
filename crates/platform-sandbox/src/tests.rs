use super::*;
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, AllowedMcpServerCapabilities,
    ArtifactGrantOperation, ArtifactRef, AuthoringPackage, CapabilityArtifactContract,
    CapabilityBackendBinding, CapabilityBackendContract, CapabilityBackendFeatures,
    CapabilityBackendKind, CapabilityBackendLimits, CapabilityCancellationKind,
    CapabilityDataFlowPolicy, CapabilityDeploymentClosure, CapabilityIdempotencyKind,
    CapabilityImplementationResourceSpec, CapabilityInterfaceLimits,
    CapabilityInterfaceResourceSpec, CapabilityProgressContract, CapabilityProgressDurability,
    CapabilityProgressMode, ClosedJsonSchema, ClosedJsonValue, CodeTrustClass, CommandAudit,
    CommandOutcome, DataClassification, Effect, ExactDeploymentRef, ExactSecretBindingRef,
    ExactVersionRef, JobState, McpAuthorizationPrincipalKind, McpClientCapabilities,
    McpDeploymentClosure, McpMetadataPolicy, McpMethodLimits, McpNegotiatedCapabilities,
    McpProtocolPolicyDocument, McpServerExecutionContract, McpServerLimits,
    McpToolCapabilityContract, McpTransportBinding, McpTransportFeatures, McpTransportKind,
    PrincipalKind, PublishedMcpMethod, ResourceId, ResourceKind, Retryability, SandboxAbiVersion,
    SandboxArtifactIoPolicyDocument, SandboxCleanupPolicy, SandboxEntrypointKind,
    SandboxIsolationClass, SandboxIsolationPolicyDocument, SandboxJobState,
    SandboxNetworkPolicyDocument, SandboxPackageResourceSpec, SandboxProfileResourceSpec,
    SandboxResourcePolicyDocument, SandboxRuntimeFamily, SandboxRuntimeResourceSpec,
    SandboxSecretDeliveryMode, SandboxSecretResolutionPolicyDocument, SecretPurpose,
    SecretResolutionPolicy, ValueRef, WorkClass, MCP_PROTOCOL_BASELINE,
};
use insight_platform_invocations::{CapabilityOutputValue, DispatchOutcome};
use insight_platform_jobs::{decide_claim, JobFence, LeasePolicy};
use insight_platform_mcp_host::{
    ManagedMcpSandboxSessionIdentity, McpAuthorizationContext, McpHostExecutionContract,
    McpLogicalOperationRequest, McpResourceSubscriptionBinding, McpSessionBindingKey,
    McpSessionRecord, McpSubscriptionPayload, McpSubscriptionRecord, McpSubscriptionState,
    McpSubscriptionWorkerAudit, NewMcpAuthorizationContext, NewMcpHostExecutionContract,
    NewMcpResourceSubscriptionBinding,
};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c8-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
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

fn attestor_route() -> NodeAttestorRoute {
    "https://10.0.0.7:9443".parse().unwrap()
}

#[test]
fn node_attestor_route_is_private_exact_and_closed_on_decode() {
    for valid in ["https://10.0.0.7:9443", "https://[fd00::7]:9443"] {
        let route: NodeAttestorRoute = valid.parse().unwrap();
        assert_eq!(
            serde_json::to_string(&route).unwrap(),
            format!("\"{valid}\"")
        );
        assert_eq!(
            serde_json::from_str::<NodeAttestorRoute>(&format!("\"{valid}\"")).unwrap(),
            route
        );
    }
    for invalid in [
        "http://10.0.0.7:9443",
        "https://8.8.8.8:9443",
        "https://attestor.platform.svc:9443",
        "https://10.0.0.7",
        "https://10.0.0.7:9443/path",
        "https://10.0.0.7:9443/",
    ] {
        assert!(
            invalid.parse::<NodeAttestorRoute>().is_err(),
            "accepted {invalid}"
        );
        assert!(serde_json::from_str::<NodeAttestorRoute>(&format!("\"{invalid}\"")).is_err());
    }
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

fn policy_closure(
    runtime_family: SandboxRuntimeFamily,
    trust_class: CodeTrustClass,
    isolation: SandboxIsolationClass,
    wasm: bool,
) -> SandboxExecutionPolicyClosure {
    SandboxExecutionPolicyClosure {
        isolation: SandboxIsolationPolicyDocument {
            schema_version: 1,
            minimum_isolation: isolation,
            allowed_runtime_families: vec![runtime_family],
            allowed_trust_classes: vec![trust_class],
            require_hardware_virtualization_for_microvm: isolation
                == SandboxIsolationClass::MicroVm,
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
            maximum_wasm_fuel: wasm.then_some(10_000_000),
            maximum_wasm_memory_pages: wasm.then_some(16_384),
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
        wasm_fuel_consumed: Some(1_000),
    }
}

fn completed_output() -> SandboxCompletedOutput {
    let value = serde_json::json!({"answer": 42});
    let content_digest = canonical_digest(&value).unwrap().parse().unwrap();
    SandboxCompletedOutput {
        value_id: id(ResourceKind::RunValue, 23),
        classification: DataClassification::Internal,
        schema_digest: sha('f'),
        content_digest,
        value: ValueRef::Inline { value },
        artifact_link_ids: vec![],
        artifact_outputs: vec![],
        validation_evidence_digest: sha('e'),
        usage: usage(),
    }
}

fn request_at(now: DateTime<Utc>) -> SandboxExecutionRequest {
    let runtime_revision = exact(ResourceKind::SandboxRuntimeRevision, 10, '1');
    let package_revision = exact(ResourceKind::SandboxPackageRevision, 11, '2');
    let profile_revision = exact(ResourceKind::SandboxProfileRevision, 12, '3');
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
    let sandbox_job_id = id(ResourceKind::SandboxJob, 23);
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
            profile: profile_revision.clone(),
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
        profile_revision,
        profile,
        policies: Box::new(policy_closure(
            SandboxRuntimeFamily::WasmWasi,
            CodeTrustClass::BuiltIn,
            SandboxIsolationClass::Wasm,
            true,
        )),
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

fn managed_mcp_protocol() -> McpProtocolPolicyDocument {
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

fn managed_mcp_request_at(now: DateTime<Utc>) -> SandboxExecutionRequest {
    let mut request = request_at(now);
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
    let protocol_profile = managed_mcp_protocol();
    let protocol_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 42),
        protocol_profile.canonical_digest().unwrap(),
    )
    .unwrap();
    let trust_policy = exact(ResourceKind::PolicyRevision, 43, '3');
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
            profile: request.profile_revision.clone(),
            isolation: SandboxIsolationClass::MicroVm,
            isolation_policy,
            resource_policy,
            artifact_io_policy,
        },
        protocol_policy: protocol_policy.clone(),
        trust_policy,
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
        expires_at: now + ChronoDuration::minutes(5),
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
        now - ChronoDuration::seconds(1),
        now + ChronoDuration::minutes(4),
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
        "properties": {
            "question": {
                "description": "Bounded managed MCP fixture input.",
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
                "description": "Bounded managed MCP fixture output.",
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
                "description": "Bounded managed MCP fixture error.",
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
    request.input_schema_digest = input_schema.canonical_digest.clone();
    request.output_schema_digest = output_schema.canonical_digest.clone();
    let capability_interface_revision = exact(ResourceKind::CapabilityInterfaceRevision, 55, 'c');
    let capability_interface = CapabilityInterfaceResourceSpec {
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
    let backend_contract_digest = backend_contract.canonical_digest().unwrap();
    let capability_implementation_revision =
        exact(ResourceKind::CapabilityImplementationRevision, 57, '1');
    let capability_implementation = CapabilityImplementationResourceSpec {
        authoring_package: authoring(58, '2'),
        contract_digest: sha('3'),
        dependency_versions: vec![capability_interface_revision.clone()],
        policy_versions: vec![],
        interface_revision: capability_interface_revision.clone(),
        backend_kind: CapabilityBackendKind::Mcp,
        backend_contract,
        backend_contract_digest,
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
    let capability_deployment_closure = CapabilityDeploymentClosure {
        implementation: capability_implementation_revision.clone(),
        interface: capability_interface_revision.clone(),
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
        capability_deployment_closure: Box::new(capability_deployment_closure),
        capability_interface_revision,
        capability_interface: Box::new(capability_interface),
        capability_implementation_revision,
        capability_implementation: Box::new(capability_implementation),
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
    request.policies = Box::new(policy_closure(
        SandboxRuntimeFamily::ManagedMcpServer,
        CodeTrustClass::BuiltIn,
        SandboxIsolationClass::MicroVm,
        false,
    ));
    request.policies.secret_resolution = Some(SandboxSecretResolutionPolicyDocument {
        schema_version: 1,
        allowed_purposes: vec![request.secret_grants[0].secret_binding.purpose.clone()],
        maximum_reads_per_binding: 1,
        delivery_mode: SandboxSecretDeliveryMode::BrokeredMemory,
        deny_environment: true,
        deny_argv: true,
        deny_snapshot: true,
        scrub_after_read: true,
    });
    request.seal().unwrap()
}

#[test]
fn managed_mcp_submission_requires_exact_microvm_source_and_token_grant() {
    let now = Utc::now();
    let request = managed_mcp_request_at(now);
    request.validate_submission_at(now, limits()).unwrap();

    let mut missing_token = request.clone();
    missing_token.secret_grants[0].secret_binding = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 53),
        1,
        id(ResourceKind::SecretProvider, 54),
        SecretPurpose::from_str("mcp.oauth").unwrap(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: sha('b'),
        },
    )
    .unwrap();
    missing_token.secret_grants[0] = missing_token.secret_grants[0].clone().seal().unwrap();
    missing_token = missing_token.seal().unwrap();
    assert_eq!(
        missing_token.validate_submission_at(now, limits()),
        Err(SandboxContractError::InvalidSecretGrant)
    );

    let mut drifted_capability = request.clone();
    let SandboxExecutionSource::ManagedMcp {
        capability_implementation,
        ..
    } = &mut drifted_capability.execution_source
    else {
        unreachable!();
    };
    capability_implementation.features.poll = true;
    drifted_capability = drifted_capability.seal().unwrap();
    assert!(drifted_capability
        .validate_submission_at(now, limits())
        .is_err());

    let mut downgraded = request;
    downgraded.isolation_class = SandboxIsolationClass::SandboxedContainer;
    downgraded = downgraded.seal().unwrap();
    assert!(downgraded.validate_submission_at(now, limits()).is_err());
}

#[test]
fn managed_subscription_admission_binds_two_jobs_to_one_session_generation() {
    let now = Utc::now();
    let (record, command) = managed_mcp_session_fixture(now);
    let accepted =
        decide_accept_managed_mcp_sandbox_session(&record, &command, now, limits()).unwrap();
    let link = accepted
        .logical_payload
        .managed_sandbox_session
        .as_ref()
        .unwrap();
    assert_eq!(accepted.logical_state, McpSubscriptionState::Pending);
    assert_eq!(link.identity, command.request.identity);
    assert_eq!(link.sandbox_request_digest, command.request.request_digest);
    assert_eq!(accepted.logical_payload.session.generation, 1);
    assert_eq!(accepted.physical_job.work_class, WorkClass::Sandbox);
    assert_eq!(
        accepted.physical_job.job_id.uuid(),
        accepted.physical_job.owner.owner_id.uuid()
    );
    accepted
        .physical_payload
        .validate_for(&accepted.physical_job, limits())
        .unwrap();

    let mut stale = command.clone();
    stale.request.identity.admitted_subscription_version += 1;
    stale.request.identity.canonical_digest =
        insight_platform_contracts::canonical_digest(&serde_json::json!({
            "admitted_logical_job_version": stale.request.identity.admitted_logical_job_version,
            "admitted_subscription_version": stale.request.identity.admitted_subscription_version,
            "logical_job_id": stale.request.identity.logical_job_id,
            "physical_job_id": stale.request.identity.physical_job_id,
            "sandbox_job_id": stale.request.identity.sandbox_job_id,
            "schema_version": stale.request.identity.schema_version,
            "session_generation": stale.request.identity.session_generation,
            "subscription_binding_digest": stale.request.identity.subscription_binding_digest,
            "subscription_id": stale.request.identity.subscription_id,
            "tenant_id": stale.request.identity.tenant_id,
        }))
        .unwrap()
        .parse()
        .unwrap();
    stale.request = stale.request.seal().unwrap();
    stale.audit.request_digest = stale.request.request_digest.clone();
    assert!(decide_accept_managed_mcp_sandbox_session(&record, &stale, now, limits()).is_err());
}

#[test]
fn managed_subscription_commits_physical_phases_and_both_jobs_before_ready() {
    let now = Utc::now();
    let (mut logical, admission) = managed_mcp_session_fixture(now);
    let accepted =
        decide_accept_managed_mcp_sandbox_session(&logical, &admission, now, limits()).unwrap();
    logical.version += 1;
    logical.payload = accepted.logical_payload;
    logical.state = accepted.logical_state;

    let worker = id(ResourceKind::WorkerProcessGeneration, 85);
    let leased = decide_claim(
        &accepted.physical_job,
        now,
        worker.clone(),
        sha('c'),
        LeasePolicy {
            requested_milliseconds: 60_000,
            hard_maximum_milliseconds: 60_000,
        },
    )
    .unwrap();
    let audit = |suffix: u16| SandboxWorkerAudit {
        tenant_id: logical.tenant_id.clone(),
        worker_process_generation_id: worker.clone(),
        receipt_id: id(ResourceKind::Receipt, suffix),
        event_id: id(ResourceKind::Event, suffix + 10),
        outbox_id: id(ResourceKind::OutboxEvent, suffix + 20),
        idempotency_key_digest: sha('d'),
        request_digest: sha('0'),
        receipt_expires_at: now + ChronoDuration::minutes(5),
    };
    let mut preparing = CommitManagedMcpSandboxSessionPhase {
        audit: audit(86),
        identity: admission.request.identity.clone(),
        fence: JobFence {
            expected_version: leased.version,
            worker_process_generation_id: worker.clone(),
            lease_generation: leased.lease_generation,
            token_digest: sha('c'),
        },
        target: SandboxJobState::Preparing,
        executor_identity_digest: sha('e'),
        attestor_route: attestor_route(),
        phase_evidence_digest: sha('f'),
        prepared_binding: None,
    };
    preparing.audit.request_digest = preparing.canonical_request_digest().unwrap();
    let preparing = decide_managed_mcp_sandbox_session_phase(
        &logical,
        &leased,
        &accepted.physical_payload,
        &preparing,
        now,
        limits(),
    )
    .unwrap();
    assert_eq!(
        preparing.physical_payload.physical_state,
        SandboxJobState::Preparing
    );
    assert_eq!(
        preparing.logical_payload.session.state,
        insight_platform_contracts::McpSessionState::Connecting
    );

    let prepared_binding = PreparedManagedMcpSandboxSession {
        schema_version: 1,
        identity: admission.request.identity.clone(),
        request_digest: admission.request.request_digest.clone(),
        worker_process_generation_id: worker.clone(),
        provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 88),
        lease_generation: preparing.physical_job.lease_generation,
        executor_identity_digest: sha('e'),
        sandbox_identity_digest: sha('2'),
        prepare_evidence_digest: sha('1'),
        canonical_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let mut starting = CommitManagedMcpSandboxSessionPhase {
        audit: audit(87),
        identity: admission.request.identity.clone(),
        fence: JobFence {
            expected_version: preparing.physical_job.version,
            worker_process_generation_id: worker.clone(),
            lease_generation: preparing.physical_job.lease_generation,
            token_digest: sha('c'),
        },
        target: SandboxJobState::Starting,
        executor_identity_digest: sha('e'),
        attestor_route: attestor_route(),
        phase_evidence_digest: prepared_binding.canonical_digest.clone(),
        prepared_binding: Some(prepared_binding.clone()),
    };
    starting.audit.request_digest = starting.canonical_request_digest().unwrap();
    let starting = decide_managed_mcp_sandbox_session_phase(
        &logical,
        &preparing.physical_job,
        &preparing.physical_payload,
        &starting,
        now,
        limits(),
    )
    .unwrap();
    logical.version += 1;
    logical.payload = starting.logical_payload.clone();
    logical.state = starting.logical_state;
    assert_eq!(
        logical.payload.session.state,
        insight_platform_contracts::McpSessionState::Initializing
    );
    assert_eq!(
        starting.physical_payload.prepared_binding.as_ref(),
        Some(&prepared_binding)
    );

    let ready_at = now + ChronoDuration::seconds(1);
    let ready_evidence = ManagedMcpSandboxSessionReadyEvidence {
        schema_version: 1,
        session_identity_digest: admission.request.identity.canonical_digest.clone(),
        sandbox_identity_digest: sha('2'),
        encrypted_opaque_session: insight_platform_mcp_host::EncryptedMcpState {
            scheme: "aes256_gcm_v1".to_owned(),
            ciphertext: vec![1, 2, 3],
            key_id: "managed-session-key".to_owned(),
            key_reference_digest: sha('3'),
            plaintext_digest: sha('4'),
        },
        established_at: ready_at,
        expires_at: ready_at + ChronoDuration::seconds(30),
        protocol_evidence_digest: sha('5'),
        canonical_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let mut ready = CommitManagedMcpSandboxSessionReady {
        audit: audit(88),
        identity: admission.request.identity.clone(),
        fence: JobFence {
            expected_version: starting.physical_job.version,
            worker_process_generation_id: worker,
            lease_generation: starting.physical_job.lease_generation,
            token_digest: sha('c'),
        },
        ready: ready_evidence,
        phase_evidence_digest: sha('6'),
    };
    ready.audit.request_digest = ready.canonical_request_digest().unwrap();
    let ready = decide_managed_mcp_sandbox_session_ready(
        &logical,
        &starting.physical_job,
        &starting.physical_payload,
        &ready,
        ready_at,
        limits(),
    )
    .unwrap();
    assert_eq!(ready.logical_state, McpSubscriptionState::Active);
    assert_eq!(
        ready.logical_payload.session.state,
        insight_platform_contracts::McpSessionState::Ready
    );
    assert_eq!(
        ready.physical_payload.physical_state,
        SandboxJobState::Running
    );
    assert!(ready.physical_payload.ready_binding.is_some());
    let physical_json = serde_json::to_string(&ready.physical_payload).unwrap();
    assert!(!physical_json.contains("managed-session-key"));
    assert!(!physical_json.contains("ciphertext"));
    ready
        .physical_payload
        .validate_for(&ready.physical_job, limits())
        .unwrap();

    let mut logical_ready = logical;
    logical_ready.version += 1;
    logical_ready.payload = ready.logical_payload.clone();
    logical_ready.state = ready.logical_state;
    let heartbeat_at = ready_at + ChronoDuration::seconds(2);
    let heartbeat = HeartbeatSandboxExecution {
        tenant_id: logical_ready.tenant_id.clone(),
        sandbox_job_id: admission.request.identity.sandbox_job_id.clone(),
        job_id: admission.request.identity.physical_job_id.clone(),
        fence: JobFence {
            expected_version: ready.physical_job.version,
            worker_process_generation_id: ready
                .physical_job
                .lease
                .as_ref()
                .unwrap()
                .worker_process_generation_id
                .clone(),
            lease_generation: ready.physical_job.lease_generation,
            token_digest: sha('c'),
        },
        lease_milliseconds: 60_000,
    };
    let renewed = decide_managed_mcp_sandbox_session_heartbeat(
        &logical_ready,
        &ready.physical_job,
        &ready.physical_payload,
        &heartbeat,
        heartbeat_at,
        limits(),
        60_000,
    )
    .unwrap();
    assert_eq!(renewed.logical_payload, logical_ready.payload);
    assert_eq!(renewed.logical_state, logical_ready.state);
    assert_eq!(renewed.physical_payload, ready.physical_payload);
    assert_eq!(renewed.physical_job.version, ready.physical_job.version + 1);
    assert_eq!(
        renewed.physical_job.lease.as_ref().unwrap().heartbeat_at,
        heartbeat_at
    );
    assert!(decide_managed_mcp_sandbox_session_heartbeat(
        &logical_ready,
        &renewed.physical_job,
        &renewed.physical_payload,
        &heartbeat,
        heartbeat_at,
        limits(),
        60_000,
    )
    .is_err());
}

#[derive(Debug)]
struct ManagedSessionFixtureError;

impl std::fmt::Display for ManagedSessionFixtureError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("managed session fixture failure")
    }
}

impl std::error::Error for ManagedSessionFixtureError {}

struct ManagedSessionAuthorityState {
    logical: McpSubscriptionRecord,
    physical_job: insight_platform_jobs::JobProjection,
    physical_payload: ManagedMcpSandboxSessionJobPayload,
}

struct RecordingManagedSessionAuthority {
    state: Mutex<ManagedSessionAuthorityState>,
    events: Arc<Mutex<Vec<&'static str>>>,
    fail_ready: bool,
    fail_heartbeat: bool,
}

#[async_trait]
impl ManagedMcpSandboxSessionExecutionAuthority for RecordingManagedSessionAuthority {
    type Error = ManagedSessionFixtureError;

    async fn commit_managed_mcp_sandbox_session_phase(
        &self,
        command: CommitManagedMcpSandboxSessionPhase,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
        self.events.lock().unwrap().push(match command.target {
            SandboxJobState::Preparing => "commit_preparing",
            SandboxJobState::Starting => "commit_starting",
            _ => return Err(ManagedSessionFixtureError),
        });
        let now = Utc::now();
        let mut state = self.state.lock().unwrap();
        let decision = decide_managed_mcp_sandbox_session_phase(
            &state.logical,
            &state.physical_job,
            &state.physical_payload,
            &command,
            now,
            limits(),
        )
        .map_err(|_| ManagedSessionFixtureError)?;
        if decision.logical_payload != state.logical.payload
            || decision.logical_state != state.logical.state
        {
            state.logical.version += 1;
            state.logical.payload = decision.logical_payload.clone();
            state.logical.state = decision.logical_state;
        }
        state.physical_job = decision.physical_job.clone();
        state.physical_payload = decision.physical_payload.clone();
        Ok(CommandOutcome::Applied(decision))
    }

    async fn commit_managed_mcp_sandbox_session_ready(
        &self,
        command: CommitManagedMcpSandboxSessionReady,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
        self.events.lock().unwrap().push(if self.fail_ready {
            "commit_ready_failed"
        } else {
            "commit_ready"
        });
        if self.fail_ready {
            return Err(ManagedSessionFixtureError);
        }
        let now = Utc::now();
        let mut state = self.state.lock().unwrap();
        let decision = decide_managed_mcp_sandbox_session_ready(
            &state.logical,
            &state.physical_job,
            &state.physical_payload,
            &command,
            now,
            limits(),
        )
        .map_err(|_| ManagedSessionFixtureError)?;
        state.logical.version += 1;
        state.logical.payload = decision.logical_payload.clone();
        state.logical.state = decision.logical_state;
        state.physical_job = decision.physical_job.clone();
        state.physical_payload = decision.physical_payload.clone();
        Ok(CommandOutcome::Applied(decision))
    }

    async fn heartbeat_managed_mcp_sandbox_session(
        &self,
        command: HeartbeatSandboxExecution,
    ) -> Result<ManagedMcpSandboxSessionPhaseDecision, Self::Error> {
        self.events.lock().unwrap().push(if self.fail_heartbeat {
            "heartbeat_failed"
        } else {
            "heartbeat"
        });
        if self.fail_heartbeat {
            return Err(ManagedSessionFixtureError);
        }
        let now = Utc::now();
        let mut state = self.state.lock().unwrap();
        let decision = decide_managed_mcp_sandbox_session_heartbeat(
            &state.logical,
            &state.physical_job,
            &state.physical_payload,
            &command,
            now,
            limits(),
            60_000,
        )
        .map_err(|_| ManagedSessionFixtureError)?;
        state.physical_job = decision.physical_job.clone();
        Ok(decision)
    }

    async fn commit_managed_mcp_sandbox_session_lost(
        &self,
        command: CommitManagedMcpSandboxSessionLost,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
        self.events.lock().unwrap().push("commit_lost");
        let now = Utc::now();
        let mut state = self.state.lock().unwrap();
        let decision = decide_managed_mcp_sandbox_session_lost(
            &state.logical,
            &state.physical_job,
            &state.physical_payload,
            &command,
            now,
            limits(),
        )
        .map_err(|_| ManagedSessionFixtureError)?;
        state.logical.version += 1;
        state.logical.payload = decision.logical_payload.clone();
        state.logical.state = decision.logical_state;
        state.physical_job = decision.physical_job.clone();
        state.physical_payload = decision.physical_payload.clone();
        Ok(CommandOutcome::Applied(decision))
    }
}

struct RecordingManagedSessionProvider {
    events: Arc<Mutex<Vec<&'static str>>>,
    prepare_delay: std::time::Duration,
}

#[async_trait]
impl ManagedMcpSandboxSessionProvider for RecordingManagedSessionProvider {
    type Error = ManagedSessionFixtureError;

    async fn prepare(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        executor_identity_digest: &insight_platform_contracts::Sha256Digest,
    ) -> Result<PreparedManagedMcpSandboxSession, Self::Error> {
        self.events.lock().unwrap().push("provider_prepare");
        tokio::time::sleep(self.prepare_delay).await;
        PreparedManagedMcpSandboxSession {
            schema_version: 1,
            identity: request.identity.clone(),
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: fence.worker_process_generation_id.clone(),
            provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 36),
            lease_generation: fence.lease_generation,
            executor_identity_digest: executor_identity_digest.clone(),
            sandbox_identity_digest: sha('2'),
            prepare_evidence_digest: sha('3'),
            canonical_digest: sha('0'),
        }
        .seal()
        .map_err(|_| ManagedSessionFixtureError)
    }

    async fn initialize(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        _fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
    ) -> Result<PreparedManagedMcpSandboxSessionActivation, Self::Error> {
        self.events.lock().unwrap().push("provider_initialize");
        let established_at = Utc::now();
        let ready = ManagedMcpSandboxSessionReadyEvidence {
            schema_version: 1,
            session_identity_digest: request.identity.canonical_digest.clone(),
            sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
            encrypted_opaque_session: insight_platform_mcp_host::EncryptedMcpState {
                scheme: "aes256_gcm_v1".to_owned(),
                ciphertext: vec![7, 8, 9],
                key_id: "fixture-session-key".to_owned(),
                key_reference_digest: sha('4'),
                plaintext_digest: sha('5'),
            },
            established_at,
            expires_at: established_at + ChronoDuration::seconds(30),
            protocol_evidence_digest: sha('6'),
            canonical_digest: sha('0'),
        }
        .seal()
        .map_err(|_| ManagedSessionFixtureError)?;
        PreparedManagedMcpSandboxSessionActivation {
            schema_version: 1,
            prepared: prepared.clone(),
            ready,
            activation_binding_digest: sha('7'),
            canonical_digest: sha('0'),
        }
        .seal()
        .map_err(|_| ManagedSessionFixtureError)
    }

    async fn activate(
        &self,
        _request: &ManagedMcpSandboxSessionRequest,
        _fence: &JobFence,
        activation: &PreparedManagedMcpSandboxSessionActivation,
    ) -> Result<ActivatedManagedMcpSandboxSession, Self::Error> {
        self.events.lock().unwrap().push("provider_activate");
        ActivatedManagedMcpSandboxSession {
            schema_version: 1,
            identity: activation.prepared.identity.clone(),
            request_digest: activation.prepared.request_digest.clone(),
            worker_process_generation_id: activation.prepared.worker_process_generation_id.clone(),
            lease_generation: activation.prepared.lease_generation,
            sandbox_identity_digest: activation.prepared.sandbox_identity_digest.clone(),
            ready_evidence_digest: activation.ready.canonical_digest.clone(),
            activated_at: Utc::now(),
            activation_evidence_digest: sha('8'),
            canonical_digest: sha('0'),
        }
        .seal()
        .map_err(|_| ManagedSessionFixtureError)
    }

    async fn observe_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        activated: &ActivatedManagedMcpSandboxSession,
    ) -> Result<ManagedMcpSandboxSessionLivenessEvidence, Self::Error> {
        self.events.lock().unwrap().push("provider_observe");
        ManagedMcpSandboxSessionLivenessEvidence {
            schema_version: 1,
            identity: request.identity.clone(),
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: fence.worker_process_generation_id.clone(),
            provider_process_generation_id: prepared.provider_process_generation_id.clone(),
            lease_generation: fence.lease_generation,
            executor_identity_digest: prepared.executor_identity_digest.clone(),
            sandbox_identity_digest: activated.sandbox_identity_digest.clone(),
            liveness: ManagedMcpSandboxSessionLiveness::Alive,
            observed_at: Utc::now(),
            evidence_digest: sha('0'),
        }
        .seal()
        .map_err(|_| ManagedSessionFixtureError)
    }

    async fn destroy_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        _fence: &JobFence,
        prepared: Option<&PreparedManagedMcpSandboxSession>,
    ) -> Result<ManagedMcpSandboxSessionCleanupOutcome, Self::Error> {
        self.events.lock().unwrap().push("provider_destroy");
        let prepared = prepared.ok_or(ManagedSessionFixtureError)?;
        let cleanup = SandboxCleanupEvidence {
            disposition: SandboxCleanupDisposition::Destroyed,
            sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            observed_at: Utc::now(),
            evidence_digest: sha('9'),
        };
        ManagedMcpSandboxSessionDestroyedEvidence {
            schema_version: 1,
            identity: request.identity.clone(),
            request_digest: request.request_digest.clone(),
            worker_process_generation_id: prepared.worker_process_generation_id.clone(),
            provider_process_generation_id: prepared.provider_process_generation_id.clone(),
            lease_generation: prepared.lease_generation,
            executor_identity_digest: prepared.executor_identity_digest.clone(),
            sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
            cleanup,
            evidence_digest: sha('0'),
        }
        .seal()
        .map(ManagedMcpSandboxSessionCleanupOutcome::Destroyed)
        .map_err(|_| ManagedSessionFixtureError)
    }
}

fn managed_session_worker_fixture(
    now: DateTime<Utc>,
    fail_ready: bool,
) -> (
    Arc<RecordingManagedSessionAuthority>,
    Arc<RecordingManagedSessionProvider>,
    ExecuteManagedMcpSandboxSession,
) {
    let (mut logical, admission) = managed_mcp_session_fixture(now);
    let accepted =
        decide_accept_managed_mcp_sandbox_session(&logical, &admission, now, limits()).unwrap();
    logical.version += 1;
    logical.payload = accepted.logical_payload;
    logical.state = accepted.logical_state;
    let worker = id(ResourceKind::WorkerProcessGeneration, 180);
    let leased = decide_claim(
        &accepted.physical_job,
        now,
        worker.clone(),
        sha('a'),
        LeasePolicy {
            requested_milliseconds: 60_000,
            hard_maximum_milliseconds: 60_000,
        },
    )
    .unwrap();
    let identity = |suffix: u16, digest: char| SandboxWorkerCommitIdentity {
        tenant_id: logical.tenant_id.clone(),
        worker_process_generation_id: worker.clone(),
        receipt_id: id(ResourceKind::Receipt, suffix),
        event_id: id(ResourceKind::Event, suffix + 10),
        outbox_id: id(ResourceKind::OutboxEvent, suffix + 20),
        idempotency_key_digest: sha(digest),
        receipt_expires_at: now + ChronoDuration::minutes(5),
    };
    let audits = ManagedMcpSandboxSessionWorkerAudits {
        preparing: identity(181, 'c'),
        starting: identity(182, 'd'),
        ready: identity(183, 'e'),
    };
    let events = Arc::new(Mutex::new(Vec::new()));
    let authority = Arc::new(RecordingManagedSessionAuthority {
        state: Mutex::new(ManagedSessionAuthorityState {
            logical,
            physical_job: leased.clone(),
            physical_payload: accepted.physical_payload,
        }),
        events: Arc::clone(&events),
        fail_ready,
        fail_heartbeat: false,
    });
    let provider = Arc::new(RecordingManagedSessionProvider {
        events,
        prepare_delay: std::time::Duration::ZERO,
    });
    let command = ExecuteManagedMcpSandboxSession {
        claimed: ClaimedManagedMcpSandboxSession {
            request: admission.request,
            fence: JobFence {
                expected_version: leased.version,
                worker_process_generation_id: worker,
                lease_generation: leased.lease_generation,
                token_digest: sha('a'),
            },
            usage_reservation_id: admission.usage_reservation_id,
        },
        executor_identity_digest: sha('b'),
        attestor_route: attestor_route(),
        audits,
    };
    (authority, provider, command)
}

#[tokio::test]
async fn managed_session_worker_activates_only_after_durable_ready_on_the_same_instance() {
    let now = Utc::now();
    let (authority, provider, command) = managed_session_worker_fixture(now, false);
    let events = Arc::clone(&provider.events);
    let worker = ManagedMcpSandboxSessionWorker::new(authority.clone(), provider, limits());
    let established = worker.establish(command).await.unwrap();
    assert_eq!(established.activated.sandbox_identity_digest, sha('2'));
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "commit_preparing",
            "provider_prepare",
            "commit_starting",
            "provider_initialize",
            "commit_ready",
            "provider_activate",
        ]
    );
    let state = authority.state.lock().unwrap();
    assert_eq!(state.logical.state, McpSubscriptionState::Active);
    assert_eq!(
        state.logical.payload.session.state,
        insight_platform_contracts::McpSessionState::Ready
    );
    assert_eq!(
        state.physical_payload.physical_state,
        SandboxJobState::Running
    );
    assert_eq!(
        state
            .physical_payload
            .ready_binding
            .as_ref()
            .unwrap()
            .sandbox_identity_digest,
        established.activated.sandbox_identity_digest
    );
}

#[tokio::test]
async fn managed_session_provider_observation_and_cleanup_are_exactly_fenced() {
    let now = Utc::now();
    let (authority, provider, command) = managed_session_worker_fixture(now, false);
    let worker = ManagedMcpSandboxSessionWorker::new(authority, provider.clone(), limits());
    let established = worker.establish(command).await.unwrap();
    let observation = provider
        .observe_exact(
            &established.request,
            &established.fence,
            &established.prepared,
            &established.activated,
        )
        .await
        .unwrap();
    observation
        .validate_for(
            &established.request,
            &established.fence,
            &established.prepared,
            &established.activated,
            Utc::now(),
        )
        .unwrap();
    assert_eq!(
        observation.liveness,
        ManagedMcpSandboxSessionLiveness::Alive
    );

    let cleanup = provider
        .destroy_exact(
            &established.request,
            &established.fence,
            Some(&established.prepared),
        )
        .await
        .unwrap();
    cleanup
        .validate_for(
            &established.request,
            &established.fence,
            Some(&established.prepared),
            Utc::now(),
        )
        .unwrap();
    let ManagedMcpSandboxSessionCleanupOutcome::Destroyed(destroyed) = cleanup else {
        panic!("a prepared session must produce destroyed evidence");
    };
    assert_eq!(
        destroyed.cleanup.sandbox_identity_digest,
        established.prepared.sandbox_identity_digest
    );

    let mut stale = established.fence;
    stale.lease_generation += 1;
    assert!(observation
        .validate_for(
            &established.request,
            &stale,
            &established.prepared,
            &established.activated,
            Utc::now(),
        )
        .is_err());
}

#[tokio::test]
async fn managed_session_worker_destroys_prepared_instance_when_ready_commit_fails() {
    let now = Utc::now();
    let (authority, provider, command) = managed_session_worker_fixture(now, true);
    let events = Arc::clone(&provider.events);
    let worker = ManagedMcpSandboxSessionWorker::new(authority, provider, limits());
    assert!(matches!(
        worker.establish(command).await,
        Err(ManagedMcpSandboxSessionWorkerError::Authority(_))
    ));
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "commit_preparing",
            "provider_prepare",
            "commit_starting",
            "provider_initialize",
            "commit_ready_failed",
            "provider_destroy",
        ]
    );
}

#[tokio::test]
async fn managed_session_worker_renews_the_latest_fence_while_provider_prepare_is_pending() {
    let now = Utc::now();
    let (authority, provider, command) = managed_session_worker_fixture(now, false);
    let provider = Arc::new(RecordingManagedSessionProvider {
        events: Arc::clone(&provider.events),
        prepare_delay: std::time::Duration::from_millis(35),
    });
    let heartbeat =
        SandboxHeartbeatConfig::bounded(std::time::Duration::from_millis(5), 100, 1_000).unwrap();
    let worker = ManagedMcpSandboxSessionWorker::with_heartbeat(
        authority.clone(),
        provider.clone(),
        limits(),
        heartbeat,
    );
    let mut established = worker.establish(command).await.unwrap();
    assert!(established.fence.expected_version > 5);
    assert!(provider.events.lock().unwrap().contains(&"heartbeat"));
    let prior_version = established.fence.expected_version;
    let decision = authority
        .heartbeat_managed_mcp_sandbox_session(HeartbeatSandboxExecution {
            tenant_id: established.request.identity.tenant_id.clone(),
            sandbox_job_id: established.request.identity.sandbox_job_id.clone(),
            job_id: established.request.identity.physical_job_id.clone(),
            fence: established.fence.clone(),
            lease_milliseconds: 100,
        })
        .await
        .unwrap();
    established
        .apply_running_heartbeat(&decision, limits())
        .unwrap();
    assert_eq!(established.fence.expected_version, prior_version + 1);
}

#[tokio::test]
async fn managed_session_worker_waits_for_prepare_then_destroys_on_heartbeat_failure() {
    let now = Utc::now();
    let (authority, provider, command) = managed_session_worker_fixture(now, false);
    let authority = Arc::new(RecordingManagedSessionAuthority {
        state: Mutex::new({
            let state = authority.state.lock().unwrap();
            ManagedSessionAuthorityState {
                logical: state.logical.clone(),
                physical_job: state.physical_job.clone(),
                physical_payload: state.physical_payload.clone(),
            }
        }),
        events: Arc::clone(&provider.events),
        fail_ready: false,
        fail_heartbeat: true,
    });
    let provider = Arc::new(RecordingManagedSessionProvider {
        events: Arc::clone(&provider.events),
        prepare_delay: std::time::Duration::from_millis(25),
    });
    let heartbeat =
        SandboxHeartbeatConfig::bounded(std::time::Duration::from_millis(5), 100, 1_000).unwrap();
    let worker = ManagedMcpSandboxSessionWorker::with_heartbeat(
        authority,
        provider.clone(),
        limits(),
        heartbeat,
    );
    assert!(matches!(
        worker.establish(command).await,
        Err(ManagedMcpSandboxSessionWorkerError::Authority(_))
    ));
    let events = provider.events.lock().unwrap();
    let failed = events
        .iter()
        .position(|event| *event == "heartbeat_failed")
        .unwrap();
    let destroyed = events
        .iter()
        .position(|event| *event == "provider_destroy")
        .unwrap();
    assert!(failed < destroyed);
}

#[tokio::test]
async fn managed_session_loss_terminalizes_the_physical_job_before_rebuild() {
    let now = Utc::now();
    let (authority, provider, command) = managed_session_worker_fixture(now, false);
    let worker = ManagedMcpSandboxSessionWorker::new(authority.clone(), provider, limits());
    let established = worker.establish(command).await.unwrap();
    let mut lost = CommitManagedMcpSandboxSessionLost {
        audit: SandboxWorkerAudit {
            tenant_id: established.request.identity.tenant_id.clone(),
            worker_process_generation_id: established.fence.worker_process_generation_id.clone(),
            receipt_id: id(ResourceKind::Receipt, 194),
            event_id: id(ResourceKind::Event, 195),
            outbox_id: id(ResourceKind::OutboxEvent, 196),
            idempotency_key_digest: sha('f'),
            request_digest: sha('0'),
            receipt_expires_at: now + ChronoDuration::minutes(5),
        },
        identity: established.request.identity.clone(),
        fence: established.fence.clone(),
        cleanup: SandboxCleanupEvidence {
            disposition: SandboxCleanupDisposition::Destroyed,
            sandbox_identity_digest: established.prepared.sandbox_identity_digest.clone(),
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            observed_at: Utc::now(),
            evidence_digest: sha('1'),
        },
        session_loss_evidence_digest: sha('2'),
        usage_reservation_id: established.usage_reservation_id.clone(),
        quota_entry_ids: (197..201)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
    };
    lost.audit.request_digest = lost.canonical_request_digest().unwrap();
    let decision = match authority
        .commit_managed_mcp_sandbox_session_lost(lost)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(decision) => decision,
        CommandOutcome::Replayed(_) => unreachable!(),
    };
    assert_eq!(decision.logical_state, McpSubscriptionState::Pending);
    assert_eq!(
        decision.logical_payload.session.state,
        insight_platform_contracts::McpSessionState::Disconnected
    );
    assert!(decision.logical_payload.managed_sandbox_session.is_none());
    assert!(decision.logical_payload.full_reconcile_required);
    assert_eq!(
        decision.physical_job.state,
        JobState::ReconciliationRequired
    );
    assert!(decision.physical_job.lease.is_none());
    assert_eq!(
        decision.physical_payload.physical_state,
        SandboxJobState::Lost
    );
    let cleanup = decision.physical_payload.cleanup.as_ref().unwrap();
    established
        .validate_lost_decision(&decision, cleanup, limits())
        .unwrap();
    assert_eq!(cleanup.disposition, SandboxCleanupDisposition::Destroyed);
    assert_eq!(
        cleanup.sandbox_identity_digest,
        established.prepared.sandbox_identity_digest
    );
    assert!(cleanup.grants_revoked && cleanup.ephemeral_storage_destroyed);
    assert_eq!(cleanup.evidence_digest, sha('1'));
}

#[test]
fn managed_session_opaque_claims_bind_both_process_generations_and_the_exact_vm() {
    let now = Utc::now();
    let (_, _, command) = managed_session_worker_fixture(now, false);
    let request = &command.claimed.request;
    let fence = &command.claimed.fence;
    let prepared = PreparedManagedMcpSandboxSession {
        schema_version: 1,
        identity: request.identity.clone(),
        request_digest: request.request_digest.clone(),
        worker_process_generation_id: fence.worker_process_generation_id.clone(),
        provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 190),
        lease_generation: fence.lease_generation,
        executor_identity_digest: command.executor_identity_digest.clone(),
        sandbox_identity_digest: sha('2'),
        prepare_evidence_digest: sha('3'),
        canonical_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let claims = ManagedMcpSandboxOpaqueSessionClaims {
        schema_version: 1,
        identity: request.identity.clone(),
        request_digest: request.request_digest.clone(),
        worker_process_generation_id: fence.worker_process_generation_id.clone(),
        provider_process_generation_id: prepared.provider_process_generation_id.clone(),
        lease_generation: fence.lease_generation,
        sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
        protocol_evidence_digest: sha('4'),
        established_at: now,
        expires_at: now + ChronoDuration::minutes(1),
    };
    claims.validate_for(request, fence, &prepared, now).unwrap();
    assert_eq!(
        claims.canonical_digest().unwrap(),
        canonical_digest(&serde_json::to_value(&claims).unwrap())
            .unwrap()
            .parse()
            .unwrap()
    );

    let mut drifted = claims;
    drifted.provider_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 191);
    assert!(drifted
        .validate_for(request, fence, &prepared, now)
        .is_err());
}

fn managed_mcp_session_fixture(
    now: DateTime<Utc>,
) -> (McpSubscriptionRecord, AcceptManagedMcpSandboxSession) {
    let operation_request = managed_mcp_request_at(now);
    let SandboxExecutionSource::ManagedMcp { mcp_contract, .. } =
        &operation_request.execution_source
    else {
        unreachable!();
    };
    let base = mcp_contract.as_ref();
    let mut protocol = base.protocol_profile.clone();
    protocol.allowed_server_capabilities.resources = true;
    protocol.allowed_server_capabilities.subscriptions = true;
    protocol.method_limits.insert(
        PublishedMcpMethod::ResourcesRead,
        *protocol
            .method_limits
            .get(&PublishedMcpMethod::ToolsCall)
            .unwrap(),
    );
    let protocol_ref = ExactVersionRef::new(
        base.server.protocol_policy.revision_id.clone(),
        protocol.canonical_digest().unwrap(),
    )
    .unwrap();
    let mut deployment_closure = base.deployment_closure.clone();
    deployment_closure.protocol_policy = protocol_ref.clone();
    let server = McpServerExecutionContract::build(
        base.server.revision.clone(),
        McpTransportKind::ManagedStdio,
        protocol_ref.clone(),
        base.server.deployment_credential_requirements.clone(),
        base.server.authorization_credential_purpose.clone(),
        base.server.limits,
    )
    .unwrap();
    let mut negotiated = base.discovery.negotiated_capabilities.clone();
    negotiated.resources = true;
    negotiated.subscriptions = true;
    let discovery = insight_platform_contracts::McpDiscoverySnapshot::build(
        base.discovery.snapshot_id.clone(),
        base.deployment.clone(),
        base.server.revision.clone(),
        protocol_ref,
        base.authorization.canonical_digest.clone(),
        base.discovery.negotiated_version.clone(),
        negotiated,
        base.discovery.objects_artifact.clone(),
        now - ChronoDuration::seconds(1),
        now + ChronoDuration::minutes(4),
    )
    .unwrap();
    let contract = McpHostExecutionContract::build(NewMcpHostExecutionContract {
        deployment: base.deployment.clone(),
        deployment_closure,
        server,
        protocol_profile: protocol,
        authorization: base.authorization.clone(),
        discovery,
    })
    .unwrap();
    let subscription_id = id(ResourceKind::McpOperation, 70);
    let logical_job_id = id(ResourceKind::Job, 71);
    let binding = McpResourceSubscriptionBinding::build(
        NewMcpResourceSubscriptionBinding {
            subscription_id: subscription_id.clone(),
            job_id: logical_job_id.clone(),
            context_deployment: deployment(ResourceKind::ContextDeployment, 72, '5'),
            resource_uri: "mcp://catalog.example/items/42".to_owned(),
        },
        &contract,
        now,
    )
    .unwrap();
    let session =
        McpSessionRecord::disconnected(McpSessionBindingKey::build(&contract).unwrap()).unwrap();
    let record = McpSubscriptionRecord {
        tenant_id: binding.tenant_id.clone(),
        subscription_id,
        job_id: logical_job_id,
        logical_key: "managed-resource-items-42".to_owned(),
        state: McpSubscriptionState::Pending,
        version: 1,
        payload: McpSubscriptionPayload::pending(binding.clone(), session).unwrap(),
        deadline: operation_request.deadline,
        created_at: now,
        updated_at: now,
        terminal_at: None,
    };
    let physical_job_id = id(ResourceKind::Job, 73);
    let sandbox_job_id =
        ResourceId::from_uuid_v7(ResourceKind::SandboxJob, physical_job_id.uuid()).unwrap();
    let identity = ManagedMcpSandboxSessionIdentity::build(
        &binding,
        record.version,
        7,
        1,
        sandbox_job_id.clone(),
        physical_job_id,
    )
    .unwrap();
    let runtime_artifact = operation_request.package.runtime_bundle_artifact.clone();
    let artifact_grant = ScopedArtifactGrant {
        schema_version: 1,
        grant_id: id(ResourceKind::ArtifactGrant, 74),
        tenant_id: identity.tenant_id.clone(),
        sandbox_job_id: sandbox_job_id.clone(),
        operation: ArtifactGrantOperation::ReadWhole,
        port: "runtime_bundle".to_owned(),
        artifact: Some(runtime_artifact.clone()),
        staging_artifact_id: None,
        byte_range: None,
        maximum_bytes: runtime_artifact.byte_length(),
        generation: 1,
        expires_at: operation_request.deadline,
        grant_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let mut secret_grants = operation_request.secret_grants.clone();
    for grant in &mut secret_grants {
        grant.sandbox_job_id = sandbox_job_id.clone();
        *grant = grant.clone().seal().unwrap();
    }
    let callback = ManagedMcpSandboxSessionCallback {
        schema_version: 1,
        session_identity_digest: identity.canonical_digest.clone(),
        audience_identity_digest: sha('8'),
        expires_at: operation_request.deadline,
        binding_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let request = ManagedMcpSandboxSessionRequest {
        schema_version: 1,
        protocol_version: SANDBOX_PROTOCOL_VERSION,
        identity,
        subscription_binding: binding,
        mcp_contract: Box::new(contract),
        runtime_revision: operation_request.runtime_revision,
        runtime: operation_request.runtime,
        package_revision: operation_request.package_revision,
        package: operation_request.package,
        profile_revision: operation_request.profile_revision,
        profile: operation_request.profile,
        policies: operation_request.policies,
        isolation_class: operation_request.isolation_class,
        executor_worker_manifest_digest: operation_request.executor_worker_manifest_digest,
        isolation_backend_contract_digest: operation_request.isolation_backend_contract_digest,
        classification: operation_request.classification,
        artifact_grants: vec![artifact_grant],
        secret_grants,
        network_mode: operation_request.network_mode,
        resources: operation_request.resources,
        deadline: operation_request.deadline,
        callback,
        request_digest: sha('0'),
    }
    .seal()
    .unwrap();
    let worker = id(ResourceKind::WorkerProcessGeneration, 75);
    let command = AcceptManagedMcpSandboxSession {
        audit: McpSubscriptionWorkerAudit {
            tenant_id: request.identity.tenant_id.clone(),
            worker_process_generation_id: worker.clone(),
            receipt_id: id(ResourceKind::Receipt, 76),
            event_id: id(ResourceKind::Event, 77),
            outbox_id: id(ResourceKind::OutboxEvent, 78),
            idempotency_key_digest: sha('a'),
            request_digest: request.request_digest.clone(),
            receipt_expires_at: now + ChronoDuration::minutes(5),
        },
        logical_fence: JobFence {
            expected_version: 7,
            worker_process_generation_id: worker,
            lease_generation: 3,
            token_digest: sha('b'),
        },
        request,
        usage_reservation_id: id(ResourceKind::UsageReservation, 79),
        quota_entry_ids: (80..84)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
    };
    (record, command)
}

#[test]
fn managed_mcp_accepts_only_trusted_mapped_protocol_outcomes() {
    let now = Utc::now();
    let request = managed_mcp_request_at(now)
        .bind_lease_generation(1)
        .unwrap();
    let value = serde_json::json!({"answer": 42});
    let content_digest = canonical_digest(&value).unwrap().parse().unwrap();
    let mut managed_usage = usage();
    managed_usage.wasm_fuel_consumed = None;
    let output = SandboxManagedMcpOutput {
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 59),
        logical_poll_count: 0,
        observed_at: now + ChronoDuration::milliseconds(1),
        outcome: DispatchOutcome::Completed(CapabilityOutputValue {
            value_id: request.output_value_id.clone(),
            classification: request.classification,
            schema_digest: request.output_schema_digest.clone(),
            content_digest,
            value: ValueRef::Inline { value },
            artifact_link_id: None,
            validation_evidence_digest: sha('4'),
        }),
        protocol_evidence_digest: sha('5'),
        usage: managed_usage,
    };
    SandboxExecutionOutcome::ManagedMcp(Box::new(output.clone()))
        .validate_for(&request, limits())
        .unwrap();
    assert_eq!(
        SandboxExecutionOutcome::Completed(Box::new(completed_output()))
            .validate_for(&request, limits()),
        Err(SandboxContractError::InvalidOutcome)
    );
    let mut wrong_generation = output;
    wrong_generation.logical_poll_count = 1;
    assert_eq!(
        SandboxExecutionOutcome::ManagedMcp(Box::new(wrong_generation))
            .validate_for(&request, limits()),
        Err(SandboxContractError::InvalidOutcome)
    );
}

fn audit(request: &SandboxExecutionRequest, now: DateTime<Utc>) -> CommandAudit {
    CommandAudit {
        tenant_id: request.tenant_id.clone(),
        principal_id: id(ResourceKind::Principal, 30),
        principal_kind: PrincipalKind::ServiceIdentity,
        receipt_id: id(ResourceKind::Receipt, 31),
        event_id: id(ResourceKind::Event, 32),
        outbox_id: id(ResourceKind::OutboxEvent, 33),
        idempotency_key_digest: sha('a'),
        request_digest: request.request_digest.clone(),
        receipt_expires_at: now + ChronoDuration::minutes(5),
    }
}

fn fence_for(job: &insight_platform_jobs::JobProjection, token: char) -> JobFence {
    JobFence {
        expected_version: job.version,
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 50),
        lease_generation: job.lease_generation,
        token_digest: sha(token),
    }
}

fn commit_identity(
    tenant_id: &ResourceId,
    worker_process_generation_id: &ResourceId,
    suffix: u16,
    digest_character: char,
    expires_at: DateTime<Utc>,
) -> SandboxWorkerCommitIdentity {
    SandboxWorkerCommitIdentity {
        tenant_id: tenant_id.clone(),
        worker_process_generation_id: worker_process_generation_id.clone(),
        receipt_id: id(ResourceKind::Receipt, suffix),
        event_id: id(ResourceKind::Event, suffix + 10),
        outbox_id: id(ResourceKind::OutboxEvent, suffix + 20),
        idempotency_key_digest: sha(digest_character),
        receipt_expires_at: expires_at,
    }
}

fn worker_audits(
    tenant_id: &ResourceId,
    worker_process_generation_id: &ResourceId,
    base: u16,
    expires_at: DateTime<Utc>,
) -> SandboxWorkerAuditBundle {
    SandboxWorkerAuditBundle {
        preparing: commit_identity(
            tenant_id,
            worker_process_generation_id,
            base,
            '1',
            expires_at,
        ),
        starting: commit_identity(
            tenant_id,
            worker_process_generation_id,
            base + 1,
            '2',
            expires_at,
        ),
        running: commit_identity(
            tenant_id,
            worker_process_generation_id,
            base + 2,
            '3',
            expires_at,
        ),
        collecting: commit_identity(
            tenant_id,
            worker_process_generation_id,
            base + 3,
            '4',
            expires_at,
        ),
        cancelling: commit_identity(
            tenant_id,
            worker_process_generation_id,
            base + 4,
            '5',
            expires_at,
        ),
        terminal: commit_identity(
            tenant_id,
            worker_process_generation_id,
            base + 5,
            '6',
            expires_at,
        ),
    }
}

fn stop_signal(
    request: &SandboxExecutionRequest,
    worker_process_generation_id: &ResourceId,
    reason: SandboxStopReason,
    event_suffix: u16,
) -> SandboxStopSignal {
    SandboxStopSignal {
        schema_version: 1,
        tenant_id: request.tenant_id.clone(),
        sandbox_job_id: request.sandbox_job_id.clone(),
        invocation_id: request.invocation_id.clone(),
        job_id: request.job_id.clone(),
        request_digest: request.request_digest.clone(),
        attempt_no: request.attempt_no,
        lease_generation: request.lease_generation,
        worker_process_generation_id: worker_process_generation_id.clone(),
        reason,
        source_event_id: id(ResourceKind::Event, event_suffix),
        source_invocation_version: 3,
        source_event_payload_digest: sha('9'),
        signal_digest: sha('0'),
    }
    .seal()
    .unwrap()
}

struct StaticSandboxControlSource {
    signal: SandboxStopSignal,
}

#[async_trait]
impl SandboxControlSignalSource for StaticSandboxControlSource {
    type Error = Infallible;

    async fn resolve_committed_control_event(
        &self,
        _query: ResolveSandboxControlEvent,
    ) -> Result<Vec<SandboxStopSignal>, Self::Error> {
        Ok(vec![self.signal.clone()])
    }

    async fn recover_committed_control_signals(
        &self,
        _query: RecoverSandboxControlSignals,
    ) -> Result<SandboxControlSignalPage, Self::Error> {
        Ok(SandboxControlSignalPage {
            signals: vec![self.signal.clone()],
            next_cursor: None,
            exhausted: true,
        })
    }
}

#[test]
fn isolation_is_derived_from_trust_effect_secret_network_and_classification() {
    assert_eq!(
        required_isolation(
            SandboxRuntimeFamily::WasmWasi,
            CodeTrustClass::BuiltIn,
            Effect::Pure,
            DataClassification::Internal,
            false,
            SandboxNetworkMode::None,
        ),
        SandboxIsolationClass::Wasm
    );
    for required in [
        required_isolation(
            SandboxRuntimeFamily::WasmWasi,
            CodeTrustClass::ModelGenerated,
            Effect::Pure,
            DataClassification::Internal,
            false,
            SandboxNetworkMode::None,
        ),
        required_isolation(
            SandboxRuntimeFamily::Python,
            CodeTrustClass::ReviewedPublished,
            Effect::Pure,
            DataClassification::Internal,
            true,
            SandboxNetworkMode::None,
        ),
        required_isolation(
            SandboxRuntimeFamily::NodeJs,
            CodeTrustClass::ReviewedPublished,
            Effect::IdempotentWrite,
            DataClassification::Internal,
            false,
            SandboxNetworkMode::BrokeredEgress,
        ),
        required_isolation(
            SandboxRuntimeFamily::ReviewedShell,
            CodeTrustClass::ReviewedPublished,
            Effect::Pure,
            DataClassification::Restricted,
            false,
            SandboxNetworkMode::None,
        ),
    ] {
        assert_eq!(required, SandboxIsolationClass::MicroVm);
    }
}

#[test]
fn range_grant_freezes_an_in_bounds_exact_byte_window() {
    let now = Utc::now();
    let request = request_at(now);
    let source = artifact(90, 'a');
    let valid = ScopedArtifactGrant {
        schema_version: 1,
        grant_id: id(ResourceKind::ArtifactGrant, 91),
        tenant_id: request.tenant_id.clone(),
        sandbox_job_id: request.sandbox_job_id.clone(),
        operation: ArtifactGrantOperation::ReadRange,
        port: "input".to_owned(),
        artifact: Some(source.clone()),
        staging_artifact_id: None,
        byte_range: Some(SandboxArtifactByteRange {
            offset: 8,
            length: 8,
        }),
        maximum_bytes: 8,
        generation: 1,
        expires_at: request.deadline,
        grant_digest: sha('0'),
    }
    .seal()
    .unwrap();
    valid
        .validate_for(
            &request.tenant_id,
            &request.sandbox_job_id,
            request.deadline,
            limits(),
        )
        .unwrap();

    let mut out_of_bounds = valid;
    out_of_bounds.byte_range = Some(SandboxArtifactByteRange {
        offset: 9,
        length: 8,
    });
    out_of_bounds = out_of_bounds.seal().unwrap();
    assert_eq!(
        out_of_bounds.validate_for(
            &request.tenant_id,
            &request.sandbox_job_id,
            request.deadline,
            limits(),
        ),
        Err(SandboxContractError::InvalidArtifactGrant)
    );
}

#[test]
fn artifact_backed_logical_input_requires_a_whole_read_grant() {
    let now = Utc::now();
    let mut request = request_at(now);
    let source = artifact(92, 'a');
    request.input_ref = ValueRef::Artifact {
        artifact: source.clone(),
    };
    request.artifact_grants = vec![ScopedArtifactGrant {
        schema_version: 1,
        grant_id: id(ResourceKind::ArtifactGrant, 93),
        tenant_id: request.tenant_id.clone(),
        sandbox_job_id: request.sandbox_job_id.clone(),
        operation: ArtifactGrantOperation::ReadRange,
        port: "input".to_owned(),
        artifact: Some(source),
        staging_artifact_id: None,
        byte_range: Some(SandboxArtifactByteRange {
            offset: 0,
            length: 16,
        }),
        maximum_bytes: 16,
        generation: 1,
        expires_at: request.deadline,
        grant_digest: sha('0'),
    }
    .seal()
    .unwrap()];
    request = request.seal().unwrap();

    assert_eq!(
        request.validate_submission_at(now, limits()),
        Err(SandboxContractError::InvalidInput)
    );
}

#[test]
fn sandbox_profile_policy_closure_cannot_drift_from_role_bindings() {
    let now = Utc::now();
    let mut request = request_at(now);
    request.profile.policy_versions.pop();
    request = request.seal().unwrap();
    assert_eq!(
        request.validate_submission_at(now, limits()),
        Err(SandboxContractError::InvalidResourceContract)
    );
}

#[test]
fn retry_backoff_is_closed_and_bounded_in_the_sandbox_request() {
    let now = Utc::now();
    for invalid in [0, 60_001] {
        let mut request = request_at(now);
        request.retry_backoff_milliseconds = invalid;
        request = request.seal().unwrap();
        assert_eq!(
            request.validate_submission_at(now, limits()),
            Err(SandboxContractError::InvalidExecutionRequest)
        );
    }
}

#[test]
fn cleanup_uses_its_bounded_grace_after_the_execution_deadline() {
    let deadline = Utc::now();
    let cleanup = SandboxCleanupEvidence {
        disposition: SandboxCleanupDisposition::Destroyed,
        sandbox_identity_digest: sha('a'),
        grants_revoked: true,
        ephemeral_storage_destroyed: true,
        observed_at: deadline + ChronoDuration::milliseconds(500),
        evidence_digest: sha('b'),
    };
    cleanup.validate(deadline, 1_000).unwrap();
    assert_eq!(
        cleanup.validate(deadline, 499),
        Err(SandboxContractError::InvalidOutcome)
    );
}

#[test]
fn committed_control_closes_an_unclaimed_job_without_fabricating_execution() {
    let now = Utc::now();
    for (reason, expected_state, expected_sequence) in [
        (
            SandboxStopReason::Cancelled,
            insight_platform_contracts::SandboxJobState::Cancelled,
            2,
        ),
        (
            SandboxStopReason::TimedOut,
            insight_platform_contracts::SandboxJobState::TimedOut,
            1,
        ),
    ] {
        let request = request_at(now);
        let accepted = decide_accept(
            &AcceptSandboxExecution {
                audit: audit(&request, now),
                request: request.clone(),
                usage_reservation_id: id(ResourceKind::UsageReservation, 140),
                quota_entry_ids: (141..145)
                    .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                    .collect(),
            },
            now,
            limits(),
        )
        .unwrap();
        let source_digest = sha('7');
        let terminal = decide_prestart_control(
            &accepted.job,
            &accepted.payload,
            reason,
            source_digest.clone(),
            limits(),
        )
        .unwrap();
        assert_eq!(terminal.payload.physical_state, expected_state);
        assert_eq!(terminal.payload.phase_sequence, expected_sequence);
        assert_eq!(
            terminal.payload.phase_evidence_digest,
            Some(source_digest.clone())
        );
        assert!(terminal.payload.executor_identity_digest.is_none());
        assert!(terminal.payload.outcome.is_none());
        assert!(terminal.payload.cleanup.is_none());
        terminal
            .payload
            .validate_for(&terminal.job, limits())
            .unwrap();

        let mut command = StopUnclaimedSandboxJob {
            audit: SandboxControllerAudit {
                tenant_id: request.tenant_id.clone(),
                controller_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 145),
                source_event_id: id(ResourceKind::Event, 146),
                source_invocation_version: 2,
                source_event_payload_digest: source_digest,
                receipt_id: id(ResourceKind::Receipt, 147),
                event_id: id(ResourceKind::Event, 148),
                outbox_id: id(ResourceKind::OutboxEvent, 149),
                idempotency_key_digest: sha('0'),
                request_digest: sha('0'),
                receipt_expires_at: now + ChronoDuration::minutes(5),
            },
            sandbox_job_id: request.sandbox_job_id.clone(),
            invocation_id: request.invocation_id.clone(),
            job_id: request.job_id.clone(),
            sandbox_request_digest: request.request_digest,
            reason,
            quota_entry_ids: (150..154)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        };
        command.audit.idempotency_key_digest = command.canonical_idempotency_key_digest().unwrap();
        command.audit.request_digest = command.canonical_request_digest().unwrap();
        command.validate_at(now).unwrap();
        let mut wrong = command;
        wrong.audit.source_invocation_version = 3;
        assert_eq!(
            wrong.validate_at(now),
            Err(SandboxControlError::InvalidControllerCommand)
        );
    }
}

#[tokio::test]
async fn expired_lease_recovery_requeues_only_unstarted_work_and_marks_preparing_lost() {
    let now = Utc::now();
    let request = request_at(now);
    let accepted = decide_accept(
        &AcceptSandboxExecution {
            audit: audit(&request, now),
            request: request.clone(),
            usage_reservation_id: id(ResourceKind::UsageReservation, 320),
            quota_entry_ids: (321..325)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        },
        now,
        limits(),
    )
    .unwrap();
    let worker = id(ResourceKind::WorkerProcessGeneration, 50);
    let first_claim = decide_claim(
        &accepted.job,
        now,
        worker.clone(),
        sha('b'),
        LeasePolicy {
            requested_milliseconds: 10,
            hard_maximum_milliseconds: 100,
        },
    )
    .unwrap();
    let first_expired_at = now + ChronoDuration::milliseconds(11);
    let scanned = ExpiredSandboxLease {
        tenant_id: request.tenant_id.clone(),
        sandbox_job_id: request.sandbox_job_id.clone(),
        invocation_id: request.invocation_id.clone(),
        job_id: request.job_id.clone(),
        request: request
            .clone()
            .bind_lease_generation(first_claim.lease_generation)
            .unwrap(),
        observed_job_version: first_claim.version,
        observed_lease_generation: first_claim.lease_generation,
        previous_worker_process_generation_id: worker.clone(),
        lease_expires_at: first_claim.lease.as_ref().unwrap().expires_at,
        database_observed_at: first_expired_at,
        physical_state: SandboxJobState::Accepted,
        executor_identity_digest: None,
        attestor_route: None,
    };
    scanned.validate(limits()).unwrap();
    let requeued = decide_expired_lease_recovery(
        &first_claim,
        &accepted.payload,
        first_claim.version,
        first_claim.lease_generation,
        &SandboxLeaseRecoveryAction::RequeueUnstarted {
            recovery_evidence_digest: sha('c'),
        },
        first_expired_at,
        limits(),
    )
    .unwrap();
    assert_eq!(
        requeued.job.state,
        insight_platform_contracts::JobState::Ready
    );
    assert!(requeued.job.lease.is_none());
    assert_eq!(requeued.payload.physical_state, SandboxJobState::Accepted);

    let second_claim = decide_claim(
        &requeued.job,
        first_expired_at,
        worker.clone(),
        sha('d'),
        LeasePolicy {
            requested_milliseconds: 10,
            hard_maximum_milliseconds: 100,
        },
    )
    .unwrap();
    let preparing = decide_begin_execution(
        &second_claim,
        &requeued.payload,
        &JobFence {
            expected_version: second_claim.version,
            worker_process_generation_id: worker.clone(),
            lease_generation: second_claim.lease_generation,
            token_digest: sha('d'),
        },
        WasiExecutorProcessBinding {
            executor_identity_digest: sha('e'),
            attestor_route: attestor_route(),
        },
        sha('f'),
        first_expired_at,
        limits(),
    )
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    let expired_preparing = ExpiredSandboxLease {
        tenant_id: request.tenant_id.clone(),
        sandbox_job_id: request.sandbox_job_id.clone(),
        invocation_id: request.invocation_id.clone(),
        job_id: request.job_id.clone(),
        request: request
            .clone()
            .bind_lease_generation(preparing.job.lease_generation)
            .unwrap(),
        observed_job_version: preparing.job.version,
        observed_lease_generation: preparing.job.lease_generation,
        previous_worker_process_generation_id: worker.clone(),
        lease_expires_at: preparing.job.lease.as_ref().unwrap().expires_at,
        database_observed_at: Utc::now(),
        physical_state: SandboxJobState::Preparing,
        executor_identity_digest: preparing.payload.executor_identity_digest.clone(),
        attestor_route: preparing.payload.attestor_route.clone(),
    };
    let calls = Arc::new(Mutex::new(Vec::new()));
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: expired_preparing
            .request
            .executor_worker_manifest_digest
            .clone(),
        backend_contract_digest: expired_preparing
            .request
            .isolation_backend_contract_digest
            .clone(),
    };
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(StaticWasiBackend {
            descriptor,
            calls: Some(Arc::clone(&calls)),
        }))
        .unwrap();
    let action = SandboxExecutorHost::new(registry, limits())
        .recover_expired_lease(expired_preparing)
        .await
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), vec!["recover_expired_lease"]);
    let lost_at = Utc::now();
    let lost = decide_expired_lease_recovery(
        &preparing.job,
        &preparing.payload,
        preparing.job.version,
        preparing.job.lease_generation,
        &action,
        lost_at,
        limits(),
    )
    .unwrap();
    assert_eq!(
        lost.job.state,
        insight_platform_contracts::JobState::ReconciliationRequired
    );
    assert_eq!(lost.payload.physical_state, SandboxJobState::Lost);
    assert!(matches!(
        lost.payload.outcome,
        Some(SandboxExecutionOutcome::Uncertain(_))
    ));
    lost.payload.validate_for(&lost.job, limits()).unwrap();

    let mut command = RecoverExpiredSandboxLease {
        audit: SandboxRecoveryAudit {
            tenant_id: request.tenant_id,
            recovery_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 326),
            receipt_id: id(ResourceKind::Receipt, 327),
            event_id: id(ResourceKind::Event, 328),
            outbox_id: id(ResourceKind::OutboxEvent, 329),
            idempotency_key_digest: sha('0'),
            request_digest: sha('0'),
            receipt_expires_at: lost_at + ChronoDuration::minutes(5),
        },
        sandbox_job_id: request.sandbox_job_id,
        invocation_id: request.invocation_id,
        job_id: request.job_id,
        sandbox_request_digest: request.request_digest,
        observed_job_version: preparing.job.version,
        observed_lease_generation: preparing.job.lease_generation,
        previous_worker_process_generation_id: worker,
        action,
        quota_entry_ids: (330..334)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
    };
    command.audit.idempotency_key_digest = command.canonical_idempotency_key_digest().unwrap();
    command.audit.request_digest = command.canonical_request_digest().unwrap();
    command.validate_at(lost_at).unwrap();
}

#[test]
fn expired_unstarted_lease_times_out_without_executor_or_cleanup_evidence() {
    let now = Utc::now();
    let request = request_at(now);
    let accepted = decide_accept(
        &AcceptSandboxExecution {
            audit: audit(&request, now),
            request,
            usage_reservation_id: id(ResourceKind::UsageReservation, 340),
            quota_entry_ids: (341..345)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        },
        now,
        limits(),
    )
    .unwrap();
    let leased = decide_claim(
        &accepted.job,
        now,
        id(ResourceKind::WorkerProcessGeneration, 346),
        sha('5'),
        LeasePolicy {
            requested_milliseconds: 60_000,
            hard_maximum_milliseconds: 60_000,
        },
    )
    .unwrap();
    let timed_out = decide_expired_lease_recovery(
        &leased,
        &accepted.payload,
        leased.version,
        leased.lease_generation,
        &SandboxLeaseRecoveryAction::TimeoutUnstarted {
            recovery_evidence_digest: sha('6'),
        },
        accepted.job.deadline,
        limits(),
    )
    .unwrap();
    assert_eq!(
        timed_out.job.state,
        insight_platform_contracts::JobState::TimedOut
    );
    assert_eq!(timed_out.payload.physical_state, SandboxJobState::TimedOut);
    assert!(timed_out.payload.executor_identity_digest.is_none());
    assert!(timed_out.payload.outcome.is_none());
    assert!(timed_out.payload.cleanup.is_none());
    timed_out
        .payload
        .validate_for(&timed_out.job, limits())
        .unwrap();
}

#[test]
fn timed_out_executor_commits_exact_fenced_cleanup_after_execution_deadline() {
    let now = Utc::now();
    let request = request_at(now);
    let accepted = decide_accept(
        &AcceptSandboxExecution {
            audit: audit(&request, now),
            request,
            usage_reservation_id: id(ResourceKind::UsageReservation, 134),
            quota_entry_ids: (135..139)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        },
        now,
        limits(),
    )
    .unwrap();
    let claimed = decide_claim(
        &accepted.job,
        now,
        id(ResourceKind::WorkerProcessGeneration, 50),
        sha('b'),
        LeasePolicy {
            requested_milliseconds: 30_000,
            hard_maximum_milliseconds: 60_000,
        },
    )
    .unwrap();
    let preparing = decide_begin_execution(
        &claimed,
        &accepted.payload,
        &fence_for(&claimed, 'b'),
        WasiExecutorProcessBinding {
            executor_identity_digest: sha('c'),
            attestor_route: attestor_route(),
        },
        sha('d'),
        now + ChronoDuration::milliseconds(1),
        limits(),
    )
    .unwrap();
    let database_now = accepted.payload.request.deadline + ChronoDuration::milliseconds(500);
    let terminal = decide_execution_outcome(
        &preparing.job,
        &preparing.payload,
        &fence_for(&preparing.job, 'b'),
        SandboxExecutionOutcome::TimedOut(SandboxTerminationEvidence {
            evidence_digest: sha('e'),
            guest_acknowledged: false,
            process_tree_terminated: true,
            grants_revoked: true,
            external_effect_possible: false,
        }),
        SandboxCleanupEvidence {
            disposition: SandboxCleanupDisposition::Destroyed,
            sandbox_identity_digest: sha('f'),
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            observed_at: database_now,
            evidence_digest: sha('1'),
        },
        sha('2'),
        database_now,
        limits(),
    )
    .unwrap();
    assert_eq!(
        terminal.job.state,
        insight_platform_contracts::JobState::TimedOut
    );
    assert_eq!(
        terminal.payload.physical_state,
        insight_platform_contracts::SandboxJobState::TimedOut
    );
}

#[test]
fn submission_and_physical_state_share_one_fenced_job_authority() {
    let now = Utc::now();
    let request = request_at(now);
    let accepted = decide_accept(
        &AcceptSandboxExecution {
            audit: audit(&request, now),
            request,
            usage_reservation_id: id(ResourceKind::UsageReservation, 34),
            quota_entry_ids: (35..39)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        },
        now,
        limits(),
    )
    .unwrap();
    let token = sha('b');
    let claimed = decide_claim(
        &accepted.job,
        now,
        id(ResourceKind::WorkerProcessGeneration, 50),
        token,
        LeasePolicy {
            requested_milliseconds: 30_000,
            hard_maximum_milliseconds: 60_000,
        },
    )
    .unwrap();
    let preparing = decide_begin_execution(
        &claimed,
        &accepted.payload,
        &fence_for(&claimed, 'b'),
        WasiExecutorProcessBinding {
            executor_identity_digest: sha('c'),
            attestor_route: attestor_route(),
        },
        sha('d'),
        now + ChronoDuration::milliseconds(1),
        limits(),
    )
    .unwrap();
    let starting = decide_advance_phase(
        &preparing.job,
        &preparing.payload,
        &fence_for(&preparing.job, 'b'),
        insight_platform_contracts::SandboxJobState::Starting,
        sha('e'),
        now + ChronoDuration::milliseconds(2),
        limits(),
    )
    .unwrap();
    assert_eq!(
        decide_advance_phase(
            &starting.job,
            &starting.payload,
            &fence_for(&preparing.job, 'b'),
            insight_platform_contracts::SandboxJobState::Running,
            sha('f'),
            now + ChronoDuration::milliseconds(3),
            limits(),
        ),
        Err(SandboxContractError::StaleFence)
    );
    let running = decide_advance_phase(
        &starting.job,
        &starting.payload,
        &fence_for(&starting.job, 'b'),
        insight_platform_contracts::SandboxJobState::Running,
        sha('1'),
        now + ChronoDuration::milliseconds(3),
        limits(),
    )
    .unwrap();
    let collecting = decide_advance_phase(
        &running.job,
        &running.payload,
        &fence_for(&running.job, 'b'),
        insight_platform_contracts::SandboxJobState::Collecting,
        sha('2'),
        now + ChronoDuration::milliseconds(4),
        limits(),
    )
    .unwrap();
    let terminal = decide_execution_outcome(
        &collecting.job,
        &collecting.payload,
        &fence_for(&collecting.job, 'b'),
        SandboxExecutionOutcome::Completed(Box::new(completed_output())),
        SandboxCleanupEvidence {
            disposition: SandboxCleanupDisposition::Destroyed,
            sandbox_identity_digest: sha('c'),
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            observed_at: now + ChronoDuration::milliseconds(5),
            evidence_digest: sha('3'),
        },
        sha('4'),
        now + ChronoDuration::milliseconds(5),
        limits(),
    )
    .unwrap();
    assert_eq!(
        terminal.job.state,
        insight_platform_contracts::JobState::Succeeded
    );
    assert_eq!(
        terminal.payload.physical_state,
        insight_platform_contracts::SandboxJobState::Succeeded
    );
}

#[test]
fn model_generated_code_cannot_keep_a_wasm_isolation_request() {
    let now = Utc::now();
    let mut request = request_at(now);
    request.package.trust_class = CodeTrustClass::ModelGenerated;
    request.profile.allowed_trust_classes =
        vec![CodeTrustClass::BuiltIn, CodeTrustClass::ModelGenerated];
    request = request.seal().unwrap();
    assert_eq!(
        request.validate_submission_at(now, limits()),
        Err(SandboxContractError::InvalidExecutionRequest)
    );
}

struct StaticWasiBackend {
    descriptor: InstalledSandboxBackendDescriptor,
    calls: Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl StaticWasiBackend {
    fn new(descriptor: InstalledSandboxBackendDescriptor) -> Self {
        Self {
            descriptor,
            calls: None,
        }
    }

    fn record(&self, call: &'static str) {
        if let Some(calls) = &self.calls {
            calls.lock().unwrap().push(call);
        }
    }
}

#[async_trait]
impl SandboxExecutorBackend for StaticWasiBackend {
    fn descriptor(&self) -> InstalledSandboxBackendDescriptor {
        self.descriptor.clone()
    }

    async fn prepare(
        &self,
        request: SandboxExecutionRequest,
    ) -> Result<PreparedSandbox, SandboxBackendFailure> {
        self.record("prepare");
        Ok(PreparedSandbox {
            sandbox_identity_digest: sha('a'),
            request_digest: request.request_digest,
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            prepare_evidence_digest: sha('b'),
        })
    }

    async fn start(
        &self,
        _request: SandboxExecutionRequest,
        prepared: PreparedSandbox,
    ) -> Result<RunningSandbox, SandboxBackendFailure> {
        self.record("start");
        Ok(RunningSandbox {
            prepared,
            start_evidence_digest: sha('c'),
        })
    }

    async fn collect(
        &self,
        _request: SandboxExecutionRequest,
        running: RunningSandbox,
    ) -> Result<CollectedSandbox, SandboxBackendFailure> {
        self.record("collect");
        Ok(CollectedSandbox {
            running,
            outcome: SandboxExecutionOutcome::Completed(Box::new(completed_output())),
            usage: usage(),
            collection_evidence_digest: sha('d'),
        })
    }

    async fn terminate(
        &self,
        _command: TerminateSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure> {
        self.record("terminate");
        Ok(SandboxTerminationEvidence {
            evidence_digest: sha('e'),
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
        self.record("destroy");
        Ok(SandboxCleanupEvidence {
            disposition: SandboxCleanupDisposition::Destroyed,
            sandbox_identity_digest: command.sandbox_identity_digest,
            grants_revoked: true,
            ephemeral_storage_destroyed: true,
            observed_at: Utc::now(),
            evidence_digest: sha('f'),
        })
    }

    async fn abort(
        &self,
        command: AbortSandboxExecution,
    ) -> Result<SandboxAbortEvidence, SandboxBackendFailure> {
        self.record("abort");
        Ok(SandboxAbortEvidence {
            reason: command.reason,
            request_digest: command.request_digest,
            attempt_no: command.attempt_no,
            lease_generation: command.lease_generation,
            sandbox_identity_digest: sha('a'),
            termination: SandboxTerminationEvidence {
                evidence_digest: sha('e'),
                guest_acknowledged: true,
                process_tree_terminated: true,
                grants_revoked: true,
                external_effect_possible: false,
            },
            cleanup: SandboxCleanupEvidence {
                disposition: SandboxCleanupDisposition::Destroyed,
                sandbox_identity_digest: sha('a'),
                grants_revoked: true,
                ephemeral_storage_destroyed: true,
                observed_at: Utc::now(),
                evidence_digest: sha('f'),
            },
            evidence_digest: sha('1'),
        })
    }

    async fn recover_expired_lease(
        &self,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryEvidence, SandboxBackendFailure> {
        self.record("recover_expired_lease");
        let execution_may_have_started = expired.physical_state != SandboxJobState::Preparing;
        let external_effect_possible = execution_may_have_started
            && (expired.request.effect.risk_rank() >= Effect::IdempotentWrite.risk_rank()
                || expired.request.network_mode != SandboxNetworkMode::None);
        let sandbox_identity_digest = sha('a');
        SandboxLeaseRecoveryEvidence {
            schema_version: 1,
            request_digest: expired.request.request_digest,
            attempt_no: expired.request.attempt_no,
            lease_generation: expired.observed_lease_generation,
            physical_state: expired.physical_state,
            executor_identity_digest: expired.executor_identity_digest.unwrap(),
            uncertainty: SandboxUncertainty {
                evidence_digest: sha('2'),
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
                evidence_digest: sha('3'),
            },
            evidence_digest: sha('0'),
        }
        .seal()
        .map_err(|_| SandboxBackendFailure {
            stage: SandboxBackendFailureStage::Recovering,
            safe_code: "sandbox_recovery_evidence_invalid".to_owned(),
            safe_message: "Sandbox recovery evidence is invalid".to_owned(),
            retryability: Retryability::Never,
            evidence_digest: sha('4'),
            sandbox_identity_digest: None,
            execution_may_have_started,
            external_effect_possible,
        })
    }
}

struct BlockingWasiBackend {
    inner: StaticWasiBackend,
    collect_started: Arc<Notify>,
}

#[async_trait]
impl SandboxExecutorBackend for BlockingWasiBackend {
    fn descriptor(&self) -> InstalledSandboxBackendDescriptor {
        self.inner.descriptor()
    }

    async fn prepare(
        &self,
        request: SandboxExecutionRequest,
    ) -> Result<PreparedSandbox, SandboxBackendFailure> {
        self.inner.prepare(request).await
    }

    async fn start(
        &self,
        request: SandboxExecutionRequest,
        prepared: PreparedSandbox,
    ) -> Result<RunningSandbox, SandboxBackendFailure> {
        self.inner.start(request, prepared).await
    }

    async fn collect(
        &self,
        _request: SandboxExecutionRequest,
        _running: RunningSandbox,
    ) -> Result<CollectedSandbox, SandboxBackendFailure> {
        self.inner.record("collect");
        self.collect_started.notify_waiters();
        std::future::pending().await
    }

    async fn terminate(
        &self,
        command: TerminateSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure> {
        self.inner.terminate(command).await
    }

    async fn destroy(
        &self,
        command: DestroySandbox,
    ) -> Result<SandboxCleanupEvidence, SandboxBackendFailure> {
        self.inner.destroy(command).await
    }

    async fn abort(
        &self,
        command: AbortSandboxExecution,
    ) -> Result<SandboxAbortEvidence, SandboxBackendFailure> {
        self.inner.abort(command).await
    }

    async fn recover_expired_lease(
        &self,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryEvidence, SandboxBackendFailure> {
        self.inner.recover_expired_lease(expired).await
    }
}

#[tokio::test]
async fn host_resolves_only_the_exact_wasi_backend_and_destroys_single_use_instance() {
    let now = Utc::now();
    let request = request_at(now).bind_lease_generation(1).unwrap();
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: request.isolation_backend_contract_digest.clone(),
    };
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(StaticWasiBackend::new(descriptor)))
        .unwrap();
    let host = SandboxExecutorHost::new(registry, limits());
    let execution = host.execute(request).await.unwrap();
    assert!(matches!(
        execution.outcome,
        SandboxExecutionOutcome::Completed(_)
    ));
    assert_eq!(
        execution.cleanup.disposition,
        SandboxCleanupDisposition::Destroyed
    );
}

#[test]
fn completed_output_is_bound_to_the_reserved_value_identity_and_schema() {
    let now = Utc::now();
    let request = request_at(now).bind_lease_generation(1).unwrap();
    let mut wrong_identity = completed_output();
    wrong_identity.value_id = id(ResourceKind::RunValue, 41);
    assert_eq!(
        SandboxExecutionOutcome::Completed(Box::new(wrong_identity))
            .validate_for(&request, limits()),
        Err(SandboxContractError::InvalidOutcome)
    );
    let mut wrong_schema = completed_output();
    wrong_schema.schema_digest = sha('1');
    assert_eq!(
        SandboxExecutionOutcome::Completed(Box::new(wrong_schema)).validate_for(&request, limits()),
        Err(SandboxContractError::InvalidOutcome)
    );

    let duplicate = artifact(42, '2');
    let mut duplicate_artifacts = completed_output();
    duplicate_artifacts.artifact_link_ids = vec![
        id(ResourceKind::ArtifactLink, 42),
        id(ResourceKind::ArtifactLink, 43),
    ];
    duplicate_artifacts.artifact_outputs = vec![duplicate.clone(), duplicate];
    assert_eq!(
        SandboxExecutionOutcome::Completed(Box::new(duplicate_artifacts))
            .validate_for(&request, limits()),
        Err(SandboxContractError::InvalidOutcome)
    );
}

#[tokio::test]
async fn cancellation_aborts_the_exact_execution_and_never_waits_for_guest_cooperation() {
    let now = Utc::now();
    let request = request_at(now).bind_lease_generation(1).unwrap();
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: request.isolation_backend_contract_digest.clone(),
    };
    let calls = Arc::new(Mutex::new(Vec::new()));
    let collect_started = Arc::new(Notify::new());
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(BlockingWasiBackend {
            inner: StaticWasiBackend {
                descriptor,
                calls: Some(Arc::clone(&calls)),
            },
            collect_started: Arc::clone(&collect_started),
        }))
        .unwrap();
    let host = Arc::new(SandboxExecutorHost::new(registry, limits()));
    let cancellation = CancellationToken::new();
    let started = collect_started.notified();
    let execution = tokio::spawn({
        let host = Arc::clone(&host);
        let cancellation = cancellation.clone();
        async move { host.execute_cancellable(request, cancellation).await }
    });
    started.await;
    cancellation.cancel();
    assert!(matches!(
        execution.await.unwrap(),
        Err(SandboxBackendHostError::ExecutionStopped(
            SandboxStopReason::Cancelled
        ))
    ));
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["prepare", "start", "collect", "abort"]
    );
}

#[tokio::test]
async fn wall_deadline_aborts_the_exact_execution_and_reports_timed_out() {
    let now = Utc::now();
    let mut request = request_at(now);
    request.resources.startup_milliseconds = 50;
    request.resources.idle_milliseconds = 50;
    request.resources.wall_milliseconds = 50;
    request = request.seal().unwrap().bind_lease_generation(1).unwrap();
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: request.isolation_backend_contract_digest.clone(),
    };
    let calls = Arc::new(Mutex::new(Vec::new()));
    let collect_started = Arc::new(Notify::new());
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(BlockingWasiBackend {
            inner: StaticWasiBackend {
                descriptor,
                calls: Some(Arc::clone(&calls)),
            },
            collect_started,
        }))
        .unwrap();
    let host = SandboxExecutorHost::new(registry, limits());
    assert!(matches!(
        host.execute(request).await,
        Err(SandboxBackendHostError::ExecutionStopped(
            SandboxStopReason::TimedOut
        ))
    ));
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["prepare", "start", "collect", "abort"]
    );
}

#[tokio::test]
async fn control_router_rejects_stale_identity_and_preserves_timeout_reason() {
    let now = Utc::now();
    let request = request_at(now).bind_lease_generation(7).unwrap();
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: request.isolation_backend_contract_digest.clone(),
    };
    let collect_started = Arc::new(Notify::new());
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(BlockingWasiBackend {
            inner: StaticWasiBackend {
                descriptor,
                calls: None,
            },
            collect_started: Arc::clone(&collect_started),
        }))
        .unwrap();
    let host = Arc::new(SandboxExecutorHost::new(registry, limits()));
    let router = SandboxExecutionControlRouter::new(1).unwrap();
    let worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 309);
    let registration = router
        .register(&request, &worker_process_generation_id)
        .unwrap();
    assert_eq!(router.active_count().unwrap(), 1);

    let mut stale = stop_signal(
        &request,
        &worker_process_generation_id,
        SandboxStopReason::TimedOut,
        310,
    );
    stale.lease_generation += 1;
    stale = stale.seal().unwrap();
    assert_eq!(
        router.deliver(&stale).unwrap(),
        SandboxControlDelivery::Stale
    );
    let mut wrong_worker = stop_signal(
        &request,
        &worker_process_generation_id,
        SandboxStopReason::TimedOut,
        312,
    );
    wrong_worker.worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 308);
    wrong_worker = wrong_worker.seal().unwrap();
    assert_eq!(
        router.deliver(&wrong_worker).unwrap(),
        SandboxControlDelivery::Stale
    );

    let started = collect_started.notified();
    let execution = tokio::spawn({
        let host = Arc::clone(&host);
        let request = request.clone();
        let stop = registration.stop_token();
        async move { host.execute_with_stop_token(request, stop).await }
    });
    started.await;
    let signal = stop_signal(
        &request,
        &worker_process_generation_id,
        SandboxStopReason::TimedOut,
        311,
    );
    let controller = SandboxControlController::new(
        Arc::new(StaticSandboxControlSource {
            signal: signal.clone(),
        }),
        Arc::new(router.clone()),
        4,
        4,
    )
    .unwrap();
    let (report, cursor, exhausted) = controller
        .recover(RecoverSandboxControlSignals {
            shard: SandboxControlScanShard::whole(),
            after: None,
            executor_worker_process_generation_id: worker_process_generation_id.clone(),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(report.delivered, 1);
    assert!(cursor.is_none());
    assert!(exhausted);
    let report = controller
        .dispatch_event(ResolveSandboxControlEvent {
            tenant_id: request.tenant_id.clone(),
            source_event_id: signal.source_event_id.clone(),
            executor_worker_process_generation_id: worker_process_generation_id,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(report.already_stopped, 1);
    assert!(matches!(
        execution.await.unwrap(),
        Err(SandboxBackendHostError::ExecutionStopped(
            SandboxStopReason::TimedOut
        ))
    ));
    drop(registration);
    assert_eq!(router.active_count().unwrap(), 0);
}

struct InMemorySandboxAuthority {
    current: Mutex<(
        insight_platform_jobs::JobProjection,
        SandboxExecutionJobPayload,
        Vec<insight_platform_contracts::SandboxJobState>,
    )>,
}

#[async_trait]
impl SandboxExecutionAuthority for InMemorySandboxAuthority {
    type Error = SandboxContractError;

    async fn commit_sandbox_phase(
        &self,
        command: CommitSandboxPhase,
    ) -> Result<insight_platform_contracts::CommandOutcome<SandboxPhaseDecision>, Self::Error> {
        command
            .validate_at(Utc::now())
            .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
        let mut current = self.current.lock().unwrap();
        let decision = if command.target == insight_platform_contracts::SandboxJobState::Preparing {
            decide_begin_execution(
                &current.0,
                &current.1,
                &command.fence,
                WasiExecutorProcessBinding {
                    executor_identity_digest: command.executor_identity_digest,
                    attestor_route: command.attestor_route,
                },
                command.phase_evidence_digest,
                Utc::now(),
                limits(),
            )?
        } else {
            decide_advance_phase(
                &current.0,
                &current.1,
                &command.fence,
                command.target,
                command.phase_evidence_digest,
                Utc::now(),
                limits(),
            )?
        };
        current.0 = decision.job.clone();
        current.1 = decision.payload.clone();
        current.2.push(command.target);
        Ok(insight_platform_contracts::CommandOutcome::Applied(
            decision,
        ))
    }

    async fn commit_sandbox_outcome(
        &self,
        command: CommitSandboxOutcome,
    ) -> Result<insight_platform_contracts::CommandOutcome<SandboxPhaseDecision>, Self::Error> {
        command
            .validate_at(Utc::now())
            .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
        let mut current = self.current.lock().unwrap();
        let terminal_state = command.outcome.physical_state();
        let decision = decide_execution_outcome(
            &current.0,
            &current.1,
            &command.fence,
            command.outcome,
            command.cleanup,
            command.phase_evidence_digest,
            Utc::now(),
            limits(),
        )
        .map_err(|_| SandboxContractError::StaleFence)?;
        current.0 = decision.job.clone();
        current.1 = decision.payload.clone();
        current.2.push(terminal_state);
        Ok(insight_platform_contracts::CommandOutcome::Applied(
            decision,
        ))
    }

    async fn heartbeat_sandbox_execution(
        &self,
        command: HeartbeatSandboxExecution,
    ) -> Result<SandboxPhaseDecision, Self::Error> {
        command
            .validate(120_000)
            .map_err(|_| SandboxContractError::InvalidWorkerCommand)?;
        let mut current = self.current.lock().unwrap();
        let next = insight_platform_jobs::decide_heartbeat(
            &current.0,
            &command.fence,
            Utc::now(),
            insight_platform_jobs::LeasePolicy {
                requested_milliseconds: command.lease_milliseconds,
                hard_maximum_milliseconds: 120_000,
            },
        )
        .map_err(|_| SandboxContractError::StaleFence)?;
        current.0 = next.clone();
        Ok(SandboxPhaseDecision {
            job: next,
            payload: current.1.clone(),
        })
    }
}

#[tokio::test]
async fn worker_commits_every_physical_phase_through_the_shared_job_authority() {
    let now = Utc::now();
    let request = request_at(now);
    let accepted = decide_accept(
        &AcceptSandboxExecution {
            audit: audit(&request, now),
            request,
            usage_reservation_id: id(ResourceKind::UsageReservation, 150),
            quota_entry_ids: (151..155)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        },
        now,
        limits(),
    )
    .unwrap();
    let worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 160);
    let token_digest = sha('b');
    let claimed = decide_claim(
        &accepted.job,
        now,
        worker_process_generation_id.clone(),
        token_digest.clone(),
        LeasePolicy {
            requested_milliseconds: 30_000,
            hard_maximum_milliseconds: 60_000,
        },
    )
    .unwrap();
    let fence = JobFence {
        expected_version: claimed.version,
        worker_process_generation_id: worker_process_generation_id.clone(),
        lease_generation: claimed.lease_generation,
        token_digest,
    };
    let leased_request = accepted
        .payload
        .request
        .as_ref()
        .clone()
        .bind_lease_generation(claimed.lease_generation)
        .unwrap();
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: leased_request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: leased_request.isolation_backend_contract_digest.clone(),
    };
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(StaticWasiBackend::new(descriptor)))
        .unwrap();
    let authority = Arc::new(InMemorySandboxAuthority {
        current: Mutex::new((claimed, accepted.payload, vec![])),
    });
    let worker = SandboxExecutorWorker::new(
        Arc::new(SandboxExecutorHost::new(registry, limits())),
        Arc::clone(&authority),
        limits(),
    );
    let expires_at = now + ChronoDuration::minutes(5);
    let audits = worker_audits(
        &leased_request.tenant_id,
        &worker_process_generation_id,
        170,
        expires_at,
    );
    let terminal = worker
        .execute(ExecuteSandboxJob {
            request: leased_request,
            fence,
            executor_identity_digest: sha('c'),
            attestor_route: attestor_route(),
            audits,
            usage_reservation_id: id(ResourceKind::UsageReservation, 150),
            quota_entry_ids: (151..155)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        })
        .await
        .unwrap();
    let terminal = match terminal {
        insight_platform_contracts::CommandOutcome::Applied(value)
        | insight_platform_contracts::CommandOutcome::Replayed(value) => value,
    };
    assert_eq!(
        terminal.payload.physical_state,
        insight_platform_contracts::SandboxJobState::Succeeded
    );
    assert_eq!(
        authority.current.lock().unwrap().2,
        vec![
            insight_platform_contracts::SandboxJobState::Preparing,
            insight_platform_contracts::SandboxJobState::Starting,
            insight_platform_contracts::SandboxJobState::Running,
            insight_platform_contracts::SandboxJobState::Collecting,
            insight_platform_contracts::SandboxJobState::Succeeded,
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_heartbeats_the_exact_fence_while_guest_is_running() {
    let now = Utc::now();
    let request = request_at(now);
    let accepted = decide_accept(
        &AcceptSandboxExecution {
            audit: audit(&request, now),
            request,
            usage_reservation_id: id(ResourceKind::UsageReservation, 350),
            quota_entry_ids: (351..355)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        },
        now,
        limits(),
    )
    .unwrap();
    let worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 360);
    let token_digest = sha('b');
    let claimed = decide_claim(
        &accepted.job,
        now,
        worker_process_generation_id.clone(),
        token_digest.clone(),
        LeasePolicy {
            requested_milliseconds: 1_000,
            hard_maximum_milliseconds: 2_000,
        },
    )
    .unwrap();
    let leased_request = accepted
        .payload
        .request
        .as_ref()
        .clone()
        .bind_lease_generation(claimed.lease_generation)
        .unwrap();
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: leased_request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: leased_request.isolation_backend_contract_digest.clone(),
    };
    let collect_started = Arc::new(Notify::new());
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(BlockingWasiBackend {
            inner: StaticWasiBackend::new(descriptor),
            collect_started: Arc::clone(&collect_started),
        }))
        .unwrap();
    let authority = Arc::new(InMemorySandboxAuthority {
        current: Mutex::new((claimed.clone(), accepted.payload, vec![])),
    });
    let worker = Arc::new(
        SandboxExecutorWorker::with_heartbeat(
            Arc::new(SandboxExecutorHost::new(registry, limits())),
            Arc::clone(&authority),
            limits(),
            SandboxHeartbeatConfig::bounded(std::time::Duration::from_millis(10), 1_000, 2_000)
                .unwrap(),
        )
        .unwrap(),
    );
    let worker_task = Arc::clone(&worker);
    let execution = tokio::spawn(async move {
        worker_task
            .execute(ExecuteSandboxJob {
                audits: worker_audits(
                    &leased_request.tenant_id,
                    &worker_process_generation_id,
                    370,
                    now + ChronoDuration::minutes(5),
                ),
                request: leased_request,
                fence: JobFence {
                    expected_version: claimed.version,
                    worker_process_generation_id,
                    lease_generation: claimed.lease_generation,
                    token_digest,
                },
                executor_identity_digest: sha('c'),
                attestor_route: attestor_route(),
                usage_reservation_id: id(ResourceKind::UsageReservation, 350),
                quota_entry_ids: (351..355)
                    .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                    .collect(),
            })
            .await
    });
    collect_started.notified().await;
    tokio::time::sleep(std::time::Duration::from_millis(35)).await;
    let (current, _, _) = &*authority.current.lock().unwrap();
    assert!(current.version >= claimed.version + 3);
    assert_eq!(
        current.lease.as_ref().unwrap().worker_process_generation_id,
        id(ResourceKind::WorkerProcessGeneration, 360)
    );
    execution.abort();
}

#[tokio::test]
async fn worker_persists_cancelled_only_after_abort_and_cleanup_evidence() {
    let now = Utc::now();
    let request = request_at(now);
    let accepted = decide_accept(
        &AcceptSandboxExecution {
            audit: audit(&request, now),
            request,
            usage_reservation_id: id(ResourceKind::UsageReservation, 250),
            quota_entry_ids: (251..255)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        },
        now,
        limits(),
    )
    .unwrap();
    let worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 260);
    let token_digest = sha('b');
    let claimed = decide_claim(
        &accepted.job,
        now,
        worker_process_generation_id.clone(),
        token_digest.clone(),
        LeasePolicy {
            requested_milliseconds: 30_000,
            hard_maximum_milliseconds: 60_000,
        },
    )
    .unwrap();
    let fence = JobFence {
        expected_version: claimed.version,
        worker_process_generation_id: worker_process_generation_id.clone(),
        lease_generation: claimed.lease_generation,
        token_digest,
    };
    let leased_request = accepted
        .payload
        .request
        .as_ref()
        .clone()
        .bind_lease_generation(claimed.lease_generation)
        .unwrap();
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: leased_request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: leased_request.isolation_backend_contract_digest.clone(),
    };
    let calls = Arc::new(Mutex::new(Vec::new()));
    let collect_started = Arc::new(Notify::new());
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(BlockingWasiBackend {
            inner: StaticWasiBackend {
                descriptor,
                calls: Some(Arc::clone(&calls)),
            },
            collect_started: Arc::clone(&collect_started),
        }))
        .unwrap();
    let authority = Arc::new(InMemorySandboxAuthority {
        current: Mutex::new((claimed, accepted.payload, vec![])),
    });
    let worker = Arc::new(SandboxExecutorWorker::new(
        Arc::new(SandboxExecutorHost::new(registry, limits())),
        Arc::clone(&authority),
        limits(),
    ));
    let command = ExecuteSandboxJob {
        request: leased_request.clone(),
        fence,
        executor_identity_digest: sha('c'),
        attestor_route: attestor_route(),
        audits: worker_audits(
            &authority.current.lock().unwrap().0.tenant_id,
            &worker_process_generation_id,
            270,
            now + ChronoDuration::minutes(5),
        ),
        usage_reservation_id: id(ResourceKind::UsageReservation, 250),
        quota_entry_ids: (251..255)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
    };
    let router = SandboxExecutionControlRouter::new(1).unwrap();
    let started = collect_started.notified();
    let execution = tokio::spawn({
        let worker = Arc::clone(&worker);
        let router = router.clone();
        async move { worker.execute_registered(command, &router).await }
    });
    started.await;
    let signal = stop_signal(
        &leased_request,
        &worker_process_generation_id,
        SandboxStopReason::Cancelled,
        290,
    );
    let controller = SandboxControlController::new(
        Arc::new(StaticSandboxControlSource {
            signal: signal.clone(),
        }),
        Arc::new(router),
        4,
        4,
    )
    .unwrap();
    let report = controller
        .dispatch_event(ResolveSandboxControlEvent {
            tenant_id: leased_request.tenant_id.clone(),
            source_event_id: signal.source_event_id,
            executor_worker_process_generation_id: worker_process_generation_id,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(report.delivered, 1);
    let terminal = execution.await.unwrap().unwrap();
    let terminal = match terminal {
        insight_platform_contracts::CommandOutcome::Applied(value)
        | insight_platform_contracts::CommandOutcome::Replayed(value) => value,
    };
    assert_eq!(
        terminal.payload.physical_state,
        insight_platform_contracts::SandboxJobState::Cancelled
    );
    assert_eq!(
        authority.current.lock().unwrap().2,
        vec![
            insight_platform_contracts::SandboxJobState::Preparing,
            insight_platform_contracts::SandboxJobState::Starting,
            insight_platform_contracts::SandboxJobState::Running,
            insight_platform_contracts::SandboxJobState::Collecting,
            insight_platform_contracts::SandboxJobState::Cancelling,
            insight_platform_contracts::SandboxJobState::Cancelled,
        ]
    );
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["prepare", "start", "collect", "abort"]
    );
}

struct RejectStartingAuthority {
    current: Mutex<(
        insight_platform_jobs::JobProjection,
        SandboxExecutionJobPayload,
    )>,
}

#[async_trait]
impl SandboxExecutionAuthority for RejectStartingAuthority {
    type Error = &'static str;

    async fn commit_sandbox_phase(
        &self,
        command: CommitSandboxPhase,
    ) -> Result<insight_platform_contracts::CommandOutcome<SandboxPhaseDecision>, Self::Error> {
        if command.target == insight_platform_contracts::SandboxJobState::Starting {
            return Err("authority unavailable");
        }
        let mut current = self.current.lock().unwrap();
        let decision = decide_begin_execution(
            &current.0,
            &current.1,
            &command.fence,
            WasiExecutorProcessBinding {
                executor_identity_digest: command.executor_identity_digest,
                attestor_route: command.attestor_route,
            },
            command.phase_evidence_digest,
            Utc::now(),
            limits(),
        )
        .map_err(|_| "decision failed")?;
        current.0 = decision.job.clone();
        current.1 = decision.payload.clone();
        Ok(insight_platform_contracts::CommandOutcome::Applied(
            decision,
        ))
    }

    async fn commit_sandbox_outcome(
        &self,
        _command: CommitSandboxOutcome,
    ) -> Result<insight_platform_contracts::CommandOutcome<SandboxPhaseDecision>, Self::Error> {
        Err("unexpected terminal commit")
    }

    async fn heartbeat_sandbox_execution(
        &self,
        _command: HeartbeatSandboxExecution,
    ) -> Result<SandboxPhaseDecision, Self::Error> {
        Err("heartbeat unavailable")
    }
}

#[tokio::test]
async fn durable_phase_failure_destroys_prepared_instance_before_user_code_starts() {
    let now = Utc::now();
    let request = request_at(now);
    let accepted = decide_accept(
        &AcceptSandboxExecution {
            audit: audit(&request, now),
            request,
            usage_reservation_id: id(ResourceKind::UsageReservation, 210),
            quota_entry_ids: (211..215)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        },
        now,
        limits(),
    )
    .unwrap();
    let worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 220);
    let token_digest = sha('a');
    let claimed = decide_claim(
        &accepted.job,
        now,
        worker_process_generation_id.clone(),
        token_digest.clone(),
        LeasePolicy {
            requested_milliseconds: 30_000,
            hard_maximum_milliseconds: 60_000,
        },
    )
    .unwrap();
    let leased_request = accepted
        .payload
        .request
        .as_ref()
        .clone()
        .bind_lease_generation(claimed.lease_generation)
        .unwrap();
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: leased_request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: leased_request.isolation_backend_contract_digest.clone(),
    };
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(StaticWasiBackend {
            descriptor,
            calls: Some(Arc::clone(&calls)),
        }))
        .unwrap();
    let authority = Arc::new(RejectStartingAuthority {
        current: Mutex::new((claimed.clone(), accepted.payload)),
    });
    let worker = SandboxExecutorWorker::new(
        Arc::new(SandboxExecutorHost::new(registry, limits())),
        authority,
        limits(),
    );
    let result = worker
        .execute(ExecuteSandboxJob {
            audits: worker_audits(
                &leased_request.tenant_id,
                &worker_process_generation_id,
                230,
                now + ChronoDuration::minutes(5),
            ),
            request: leased_request,
            fence: JobFence {
                expected_version: claimed.version,
                worker_process_generation_id,
                lease_generation: claimed.lease_generation,
                token_digest,
            },
            executor_identity_digest: sha('b'),
            attestor_route: attestor_route(),
            usage_reservation_id: id(ResourceKind::UsageReservation, 210),
            quota_entry_ids: (211..215)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
        })
        .await;
    assert!(matches!(
        result,
        Err(SandboxExecutorWorkerError::Authority(
            "authority unavailable"
        ))
    ));
    assert_eq!(*calls.lock().unwrap(), vec!["prepare", "destroy"]);
}

#[tokio::test]
async fn exact_backend_contract_mismatch_fails_before_prepare() {
    let now = Utc::now();
    let mut request = request_at(now).bind_lease_generation(1).unwrap();
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::Wasm,
        worker_manifest_digest: request.executor_worker_manifest_digest.clone(),
        backend_contract_digest: request.isolation_backend_contract_digest.clone(),
    };
    let mut registry = InstalledSandboxBackendRegistry::default();
    registry
        .install(Arc::new(StaticWasiBackend::new(descriptor)))
        .unwrap();
    request.isolation_backend_contract_digest = sha('0');
    request = request.seal().unwrap();
    let host = SandboxExecutorHost::new(registry, limits());
    assert_eq!(
        host.execute(request).await,
        Err(SandboxBackendHostError::BackendNotInstalled)
    );
}

#[test]
fn plain_oci_cannot_be_registered_as_sandboxed_container() {
    let descriptor = InstalledSandboxBackendDescriptor {
        backend_kind: SandboxIsolationBackendKind::Wasi,
        isolation_class: SandboxIsolationClass::SandboxedContainer,
        worker_manifest_digest: sha('1'),
        backend_contract_digest: sha('2'),
    };
    let mut registry = InstalledSandboxBackendRegistry::default();
    assert_eq!(
        registry.install(Arc::new(StaticWasiBackend::new(descriptor))),
        Err(SandboxBackendHostError::InvalidInstalledBackend)
    );
}

#[test]
fn backend_failure_with_possible_effect_is_never_safe_automatic_retry() {
    let failure = SandboxBackendFailure {
        stage: SandboxBackendFailureStage::Running,
        safe_code: "sandbox_connection_lost".to_owned(),
        safe_message: "Sandbox completion was not observed".to_owned(),
        retryability: Retryability::ReconcileBeforeRetry,
        evidence_digest: sha('1'),
        sandbox_identity_digest: Some(sha('2')),
        execution_may_have_started: true,
        external_effect_possible: true,
    };
    assert!(matches!(
        failure.into_execution_outcome(),
        SandboxExecutionOutcome::Uncertain(SandboxUncertainty {
            manual_reconciliation_required: true,
            ..
        })
    ));
}

#[test]
fn process_generation_absence_evidence_is_exactly_bound_and_tamper_evident() {
    let observed_at = Utc::now();
    let request = ProveWasiProcessGenerationAbsent {
        tenant_id: id(ResourceKind::Tenant, 401),
        sandbox_job_id: id(ResourceKind::SandboxJob, 402),
        request_digest: sha('1'),
        previous_worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 403),
        executor_identity_digest: sha('2'),
        attestor_route: attestor_route(),
    };
    let evidence = WasiProcessGenerationAbsenceEvidence {
        schema_version: 1,
        tenant_id: request.tenant_id.clone(),
        sandbox_job_id: request.sandbox_job_id.clone(),
        request_digest: request.request_digest.clone(),
        previous_worker_process_generation_id: request
            .previous_worker_process_generation_id
            .clone(),
        executor_identity_digest: request.executor_identity_digest.clone(),
        attestor_identity_digest: sha('3'),
        attestor_route: request.attestor_route.clone(),
        disposition: WasiProcessGenerationIsolationDisposition::ProcessAbsent,
        observed_at,
        evidence_digest: sha('0'),
    }
    .seal()
    .unwrap();

    evidence.validate_for(&request, observed_at).unwrap();

    let mut wrong_request = request.clone();
    wrong_request.executor_identity_digest = sha('4');
    assert_eq!(
        evidence.validate_for(&wrong_request, observed_at),
        Err(WasiProcessGenerationIsolationError::Rejected)
    );

    let mut tampered = evidence.clone();
    tampered.disposition = WasiProcessGenerationIsolationDisposition::NodeQuarantined;
    assert_eq!(
        tampered.validate_for(&request, observed_at),
        Err(WasiProcessGenerationIsolationError::Rejected)
    );
    assert_eq!(
        evidence.validate_for(&request, observed_at - ChronoDuration::milliseconds(1)),
        Err(WasiProcessGenerationIsolationError::Rejected)
    );
}

#[test]
fn executor_process_identity_is_attestor_sealed_and_generation_bound() {
    let observed_at = Utc::now();
    let attestor_identity_digest = sha('8');
    let request = RegisterWasiExecutorProcessGeneration {
        worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 410),
        worker_manifest_digest: sha('4'),
        isolation_backend_contract_digest: sha('5'),
    };
    let peer = WasiExecutorRegistrationPeer {
        host_process_id: 42,
        host_user_id: 1000,
        host_group_id: 1000,
    };
    peer.validate().unwrap();
    assert_eq!(
        WasiExecutorRegistrationPeer {
            host_process_id: 0,
            ..peer
        }
        .validate(),
        Err(WasiExecutorProcessRegistrationError::Rejected)
    );
    let evidence = WasiExecutorProcessIdentityEvidence {
        schema_version: 1,
        worker_process_generation_id: request.worker_process_generation_id.clone(),
        worker_manifest_digest: request.worker_manifest_digest.clone(),
        isolation_backend_contract_digest: request.isolation_backend_contract_digest.clone(),
        executor_instance_binding_digest: sha('6'),
        executor_identity_digest: sha('0'),
        attestor_identity_digest: attestor_identity_digest.clone(),
        attestor_route: attestor_route(),
        observed_at,
        evidence_digest: sha('0'),
    }
    .seal()
    .unwrap();

    evidence
        .validate_for(&request, &attestor_identity_digest, observed_at)
        .unwrap();
    assert_ne!(
        evidence.executor_identity_digest,
        request.worker_manifest_digest
    );

    let mut wrong_generation = request.clone();
    wrong_generation.worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 411);
    assert_eq!(
        evidence.validate_for(&wrong_generation, &attestor_identity_digest, observed_at),
        Err(WasiExecutorProcessRegistrationError::Rejected)
    );

    let mut tampered = evidence;
    tampered.executor_instance_binding_digest = sha('7');
    assert_eq!(
        tampered.validate_for(&request, &attestor_identity_digest, observed_at),
        Err(WasiExecutorProcessRegistrationError::Rejected)
    );
}
