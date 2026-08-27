use chrono::{DateTime, Duration, Utc};
use insight_platform_artifacts::ArtifactReferenceSnapshot;
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityAdapterRequest, CapabilityAdapterResponse,
    CapabilityAdapterWorker, CapabilityDispatcher, CapabilityTransportCancelOutcome,
    CapabilityTransportCancelRequest, GrpcNetworkTransport, GrpcTransportRequest,
    GrpcTransportResponse, HttpNetworkTransport, HttpTransportRequest, HttpTransportResponse,
    InstalledNativeAdapter, InstalledNativeRegistry, NativeCapabilityAdapter,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, AdministrativeGate, AgentDeploymentClosure,
    AgentResourceSpec, AllowedMcpServerCapabilities, ArtifactPurpose, ArtifactRef,
    ArtifactReferenceKind, AuthoringPackage, CandidateSelectionMode,
    CandidateSelectionPolicyDocument, CanonicalHttpEndpoint, CapabilityArtifactContract,
    CapabilityArtifactDirection, CapabilityArtifactPort, CapabilityBackendBinding,
    CapabilityBackendContract, CapabilityBackendFeatures, CapabilityBackendKind,
    CapabilityBackendLimits, CapabilityCancellationKind, CapabilityDataFlowPolicy,
    CapabilityDeploymentClosure, CapabilityEndpointScheme, CapabilityIdempotencyKind,
    CapabilityImplementationResourceSpec, CapabilityInterfaceLimits,
    CapabilityInterfaceResourceSpec, CapabilityProgressContract, CapabilityProgressDurability,
    CapabilityProgressMode, ClosedJsonSchema, CommandAudit, CommandOutcome, DataClassification,
    DataRegion, DeploymentClosure, Effect, EntityLifecycle, ExactDeploymentRef, ExactPolicyBinding,
    ExactSecretBindingRef, ExactVersionRef, ExternalLeafFailureMutationIds,
    ExternalLeafResumeMutationIds, FrozenSlotBinding, FrozenSlotTarget, GrpcCapabilityContract,
    HttpCapabilityContract, HttpCapabilityMethod, InstalledCapabilityCodecRef, InteractionKind,
    McpAuthPolicyDocument, McpAuthorizationPrincipalKind, McpClientCapabilities,
    McpDiscoverySnapshot, McpMetadataPolicy, McpMethodLimits, McpNegotiatedCapabilities,
    McpOAuthClientAuthenticationKind, McpOAuthEndpoint, McpProtocolPolicyDocument, McpServerLimits,
    McpServerResourceSpec, McpToolCapabilityContract, McpTransportBinding, McpTransportFeatures,
    NativeCapabilityContract, Permission, PermissionSet, PolicyDeploymentClosure, PolicyKind,
    PolicyResourceSpec, PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot,
    PublishedMcpMethod, PublishedVersionPayload, QuotaDimension, RegistryResourceKind,
    ResourceDocument, ResourceId, ResourceKind, RunBindingsSnapshot, SecretBindingPayload,
    SecretPurpose, SecretResolutionPolicy, Sha256Digest, TenantConfig, TenantPrincipalPayload,
    ValidationSummary, ValueRef, WorkClass, INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION,
    MCP_PROTOCOL_BASELINE, WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION,
};
use insight_platform_egress_rpc::{
    proto::egress_broker_service_server::EgressBrokerServiceServer, EgressBrokerGrpcService,
    EgressCallerWorkloadIdentity, EgressInternalRpcLimits, CAPABILITY_WORKER_WORKLOAD_IDENTITY,
    MCP_HOST_WORKLOAD_IDENTITY,
};
use insight_platform_invocations::{
    AdmitCapabilityInvocation, BackendInputRequest, CapabilityApprovalDecision,
    CapabilityApprovalDispatchMutationIds, CapabilityClaimSlot, CapabilityControlKind,
    CapabilityInputResponse, CapabilityInvocationRecord, CapabilityOutputValue, CapabilityProgress,
    CapabilitySignalAudit, CapabilityUncertainty, CapabilityWorkerAudit, ClaimCapabilityJobs,
    CommitCapabilityOutcome, ControlCapabilityInvocation, DispatchOutcome, EncryptedRemoteState,
    InvocationApprovalRequirement, InvocationOrigin, InvocationPolicyDecision,
    InvocationPolicyDecisionBundle, InvocationPolicyDisposition, InvocationTransaction,
    McpCapabilityRuntimeRequest, PrepareCapabilityDispatch, ReconciliationResolution,
    RecordCapabilityProgress, RemoteWait, ResolveCapabilityApproval, ResolveCapabilityInput,
    ResolveCapabilityReconciliation, WakeCapabilityInvocation,
};
use insight_platform_jobs::{JobFence, WakeSource};
use insight_platform_mcp_host::{
    McpAuthorizationBindingRecord, McpDiscoveryAdmission, McpDiscoveryOperationPayload,
    McpDiscoveryResultBinding, McpDiscoverySnapshotRecord, McpDiscoveryTransportEvidence,
    McpOperationOutcome, McpRemoteTaskCancelOutcome, McpStreamableHttpConnector,
    McpStreamableHttpRequest, McpTransportFailure, NewMcpAuthorizationBinding,
    NewMcpDiscoveryAdmission, NewMcpDiscoverySnapshotRecord,
};
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelProviderWireConnector, ModelProviderWireProtocol, ModelProviderWireRequest,
    ModelProviderWireStream,
};
use insight_platform_orchestrator::{
    derive_candidate_selection, DataPortKey, ExactDataPortRef, ExactRunValueRef,
    OrchestrationJobPayload, PlanLimits, PlanNodeKey, RunCurrentSnapshot, RuntimeDependencyKind,
    RuntimeDependencySlot, RuntimeNode, RuntimePlan, ScopeDataEnvironmentSnapshot,
    ScopeEnvironmentLimits,
};
use insight_platform_postgres::{
    capability_execution_repository::{
        ClaimedCapabilityExecution, ControlledCapabilityExecution, DriveExpiredCapabilityJobs,
        ExpiredCapabilityRecoverySlot, PreparedCapabilityExecution,
    },
    repository::JobRecord,
};
use insight_platform_postgres::{
    repository::{
        DeferOrchestrationCapabilityMutationIds, DeferOrchestrationToCapabilityInvocation,
        JobFence as RepositoryJobFence, NewPrincipal, NewQuotaAccount, NewSecretBinding, NewTenant,
        NewTenantPrincipal, OrchestrationYieldMutationIds, PgRepository, RepositoryError,
        ResolvedExpressionInput, SafetyScanShard, TypedPayload, MAX_ORCHESTRATION_QUOTA_LINES,
    },
    verify_schema,
};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
};
use std::{path::PathBuf, process::Stdio, time::Duration as StdDuration};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

#[derive(Clone)]
struct CompletingNativeAdapter {
    descriptor: InstalledNativeAdapter,
    response: CapabilityAdapterResponse,
}

struct EmptyModelWire;

#[async_trait::async_trait]
impl ModelProviderWireConnector for EmptyModelWire {
    async fn open(
        &self,
        _request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn cancel(
        &self,
        _protocol: ModelProviderWireProtocol,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Unsupported)
    }
}

struct CountingHttpTransport {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl HttpNetworkTransport for CountingHttpTransport {
    async fn round_trip(
        &self,
        request: HttpTransportRequest,
    ) -> Result<HttpTransportResponse, CapabilityAdapterFailure> {
        request.validate_at(Utc::now()).unwrap();
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(HttpTransportResponse {
            status: 200,
            headers: vec![],
            body: serde_json::to_vec(&json!({"accepted": true})).unwrap(),
            transport_evidence_digest: digest('9'),
        })
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Accepted)
    }
}

struct CountingGrpcTransport {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl GrpcNetworkTransport for CountingGrpcTransport {
    async fn unary(
        &self,
        request: GrpcTransportRequest,
    ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure> {
        request.validate_at(Utc::now()).unwrap();
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(GrpcTransportResponse {
            status_code: 0,
            trailing_metadata: vec![],
            message: serde_json::to_vec(&json!({"accepted": true})).unwrap(),
            transport_evidence_digest: digest('a'),
        })
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Accepted)
    }
}

struct CountingMcpTransport {
    calls: AtomicUsize,
    output_schema_digest: Sha256Digest,
}

#[async_trait::async_trait]
impl McpStreamableHttpConnector for CountingMcpTransport {
    async fn execute(
        &self,
        request: McpStreamableHttpRequest,
    ) -> Result<McpOperationOutcome, McpTransportFailure> {
        assert!(request.deadline > Utc::now());
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(McpOperationOutcome::Completed {
            result: insight_platform_contracts::ClosedJsonValue::build(
                self.output_schema_digest.clone(),
                json!({"accepted": true}),
            )
            .unwrap(),
            evidence_digest: digest('d'),
        })
    }

    async fn cancel_remote_task(
        &self,
        _request: McpStreamableHttpRequest,
    ) -> Result<McpRemoteTaskCancelOutcome, McpTransportFailure> {
        Ok(McpRemoteTaskCancelOutcome::Accepted)
    }
}

struct ProcessMtlsFixture {
    ca_pem: String,
    server_certificate_pem: String,
    server_key_pem: String,
    mcp_host_certificate_pem: String,
    mcp_host_key_pem: String,
    mcp_host_client_certificate_pem: String,
    mcp_host_client_key_pem: String,
    capability_certificate_pem: String,
    capability_key_pem: String,
}

fn process_mtls_fixture() -> ProcessMtlsFixture {
    let mut ca_parameters = CertificateParams::default();
    ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_parameters.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
    let issue = |subject_alt_names, extended_key_usage| {
        let mut parameters = CertificateParams::default();
        parameters.subject_alt_names = subject_alt_names;
        parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        parameters.extended_key_usages = vec![extended_key_usage];
        let key = KeyPair::generate().unwrap();
        let certificate = parameters.signed_by(&key, &ca).unwrap();
        (certificate.pem(), key.serialize_pem())
    };
    let (server_certificate_pem, server_key_pem) = issue(
        vec![SanType::DnsName("egress.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (mcp_host_certificate_pem, mcp_host_key_pem) = issue(
        vec![SanType::DnsName("mcp.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (mcp_host_client_certificate_pem, mcp_host_client_key_pem) = issue(
        vec![SanType::URI(MCP_HOST_WORKLOAD_IDENTITY.try_into().unwrap())],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let (capability_certificate_pem, capability_key_pem) = issue(
        vec![SanType::URI(
            CAPABILITY_WORKER_WORKLOAD_IDENTITY.try_into().unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    ProcessMtlsFixture {
        ca_pem: ca.pem(),
        server_certificate_pem,
        server_key_pem,
        mcp_host_certificate_pem,
        mcp_host_key_pem,
        mcp_host_client_certificate_pem,
        mcp_host_client_key_pem,
        capability_certificate_pem,
        capability_key_pem,
    }
}

#[async_trait::async_trait]
impl NativeCapabilityAdapter for CompletingNativeAdapter {
    fn descriptor(&self) -> InstalledNativeAdapter {
        self.descriptor.clone()
    }

    async fn invoke(
        &self,
        _request: &CapabilityAdapterRequest,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        Ok(self.response.clone())
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Accepted)
    }
}

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c8-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn reserve_loopback_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn mcp_oauth_endpoint(host: &str, path: &str) -> McpOAuthEndpoint {
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: host.to_owned(),
        port: 443,
        base_path: path.to_owned(),
    };
    McpOAuthEndpoint {
        endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
        endpoint,
    }
}

fn mcp_method_limits() -> McpMethodLimits {
    McpMethodLimits {
        maximum_request_bytes: 16_384,
        maximum_response_bytes: 65_536,
        maximum_metadata_entries: 32,
        maximum_progress_events: 32,
        maximum_pages: 16,
        minimum_poll_milliseconds: 10,
        maximum_poll_milliseconds: 5_000,
    }
}

fn mcp_protocol_document() -> McpProtocolPolicyDocument {
    McpProtocolPolicyDocument {
        schema_version: 1,
        offered_versions: vec![MCP_PROTOCOL_BASELINE.to_owned()],
        transport_features: McpTransportFeatures {
            streamable_http_get: true,
            streamable_http_sse: true,
            resumable_stream: true,
            session_affinity: true,
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
        method_limits: BTreeMap::from([(PublishedMcpMethod::ToolsCall, mcp_method_limits())]),
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

fn mcp_server_limits() -> McpServerLimits {
    McpServerLimits {
        maximum_message_bytes: 65_536,
        maximum_response_bytes: 1_048_576,
        maximum_headers: 32,
        maximum_sse_event_bytes: 8_192,
        maximum_in_flight: 8,
        maximum_connections: 4,
        maximum_sessions: 4,
        maximum_session_milliseconds: 3_600_000,
        idle_timeout_milliseconds: 30_000,
        initialize_timeout_milliseconds: 30_000,
        request_timeout_milliseconds: 60_000,
        total_timeout_milliseconds: 120_000,
    }
}

fn builtin_echo_module_digest() -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"insight.platform/v1/capability-worker/builtin\0");
    hasher.update(b"builtin-echo-module");
    let encoded = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{encoded}").parse().unwrap()
}

fn builtin_json_digest(domain: &str) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"insight.platform/v1/capability-worker/builtin\0");
    hasher.update(domain.as_bytes());
    let encoded = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{encoded}").parse().unwrap()
}

fn builtin_json_module_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-codec-module")
}

fn builtin_json_http_protocol_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-http-protocol-contract")
}

fn builtin_json_http_request_mapping_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-http-request-mapping")
}

fn builtin_json_http_response_mapping_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-http-response-mapping")
}

fn builtin_json_http_error_mapping_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-http-error-mapping")
}

fn builtin_json_grpc_protocol_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-grpc-protobuf-contract")
}

fn builtin_json_grpc_request_mapping_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-grpc-request-mapping")
}

fn builtin_json_grpc_response_mapping_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-grpc-response-mapping")
}

fn builtin_json_grpc_error_mapping_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-grpc-error-mapping")
}

fn builtin_json_mcp_output_mapping_digest() -> Sha256Digest {
    builtin_json_digest("builtin-json-mcp-output-mapping")
}

fn native_adapter_runtime_digest() -> Sha256Digest {
    canonical_digest(&json!([{
        "adapter_id": "builtin.echo",
        "adapter_version": "1.0.0",
        "entrypoint_id": "echo.inline",
        "module_digest": builtin_echo_module_digest(),
    }]))
    .unwrap()
    .parse()
    .unwrap()
}

fn native_worker_manifest_digest() -> Sha256Digest {
    canonical_digest(&json!({
        "manifest_version": WORKER_MANIFEST_VERSION,
        "worker_role": "capability.native",
        "work_class": "capability_native",
        "adapter_runtime_digest": native_adapter_runtime_digest(),
        "protocol_version": WORKER_PROTOCOL_VERSION,
        "max_concurrency": 2,
        "critical_control_reserved_slots": 1
    }))
    .unwrap()
    .parse()
    .unwrap()
}

fn resume_mutations(base: u16) -> ExternalLeafResumeMutationIds {
    ExternalLeafResumeMutationIds {
        continuation_node_execution_id: id(ResourceKind::NodeExecution, base),
        continuation_job_id: id(ResourceKind::Job, base + 1),
        run_event_id: id(ResourceKind::Event, base + 2),
        run_outbox_id: id(ResourceKind::OutboxEvent, base + 3),
        leaf_node_event_id: id(ResourceKind::Event, base + 4),
        leaf_node_outbox_id: id(ResourceKind::OutboxEvent, base + 5),
        continuation_node_event_id: id(ResourceKind::Event, base + 6),
        continuation_node_outbox_id: id(ResourceKind::OutboxEvent, base + 7),
        continuation_job_event_id: id(ResourceKind::Event, base + 8),
        continuation_job_outbox_id: id(ResourceKind::OutboxEvent, base + 9),
    }
}

fn failure_mutations(base: u16) -> ExternalLeafFailureMutationIds {
    ExternalLeafFailureMutationIds {
        convergence_job_id: id(ResourceKind::Job, base),
        run_event_id: id(ResourceKind::Event, base + 1),
        run_outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        leaf_node_event_id: id(ResourceKind::Event, base + 3),
        leaf_node_outbox_id: id(ResourceKind::OutboxEvent, base + 4),
        convergence_job_event_id: id(ResourceKind::Event, base + 5),
        convergence_job_outbox_id: id(ResourceKind::OutboxEvent, base + 6),
    }
}

fn capability_input_schema() -> ClosedJsonSchema {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "amount": {
                "description": "Bounded fixture amount.",
                "x-platform-classification": "internal",
                "type": "integer",
                "minimum": 0,
                "maximum": 1000
            }
        },
        "required": ["amount"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn capability_output_schema() -> ClosedJsonSchema {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "accepted": {
                "description": "Whether the fixture accepted the request.",
                "x-platform-classification": "internal",
                "type": "boolean"
            },
            "result": {
                "description": "Bounded fixture result.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "x-platform-max-bytes": 1024
            },
            "race": {
                "description": "Bounded race winner.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "x-platform-max-bytes": 1024
            }
        },
        "required": [],
        "additionalProperties": false
    }))
    .unwrap()
}

fn capability_error_schema() -> ClosedJsonSchema {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "error": {
                "description": "Bounded declared error.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "x-platform-max-bytes": 1024
            }
        },
        "required": ["error"],
        "additionalProperties": false
    }))
    .unwrap()
}

fn capability_artifacts() -> CapabilityArtifactContract {
    CapabilityArtifactContract {
        ports: vec![CapabilityArtifactPort {
            name: "input".to_owned(),
            direction: CapabilityArtifactDirection::Input,
            media_types: vec!["application/json".to_owned()],
            maximum_count: 1,
            maximum_single_bytes: 1_073_741_824,
            maximum_total_bytes: 1_073_741_824,
            maximum_classification: DataClassification::Restricted,
        }],
    }
}

fn capability_data_policy(policy: &ExactVersionRef) -> CapabilityDataFlowPolicy {
    CapabilityDataFlowPolicy {
        maximum_input_classification: DataClassification::Restricted,
        maximum_output_classification: DataClassification::Restricted,
        allowed_regions: vec!["global".parse::<DataRegion>().unwrap()],
        declassification_policy: Some(policy.clone()),
    }
}

fn audit(
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    principal_kind: PrincipalKind,
    base: u16,
    idempotency: char,
    request: char,
) -> CommandAudit {
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind,
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
}

fn allowed_policy(policy: &ExactVersionRef) -> InvocationPolicyDecisionBundle {
    InvocationPolicyDecisionBundle::build(
        vec![InvocationPolicyDecision {
            policy: policy.clone(),
            disposition: InvocationPolicyDisposition::Allowed,
            evidence_digest: digest('8'),
        }],
        None,
    )
    .unwrap()
}

fn approval_policy(policy: &ExactVersionRef) -> InvocationPolicyDecisionBundle {
    InvocationPolicyDecisionBundle::build(
        vec![InvocationPolicyDecision {
            policy: policy.clone(),
            disposition: InvocationPolicyDisposition::ApprovalRequired,
            evidence_digest: digest('9'),
        }],
        Some(InvocationApprovalRequirement {
            policy_revision: policy.clone(),
            eligible_principal_rule_digest: digest('a'),
            safe_prompt_key: "approve_capability_write".to_owned(),
        }),
    )
    .unwrap()
}

async fn execute_admit(
    repository: &PgRepository,
    command: AdmitCapabilityInvocation,
) -> Result<CommandOutcome<CapabilityInvocationRecord>, RepositoryError> {
    let mut transaction = repository.begin_invocation_transaction().await?;
    match transaction.admit_capability_invocation(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_approval(
    repository: &PgRepository,
    command: ResolveCapabilityApproval,
) -> Result<CommandOutcome<CapabilityInvocationRecord>, RepositoryError> {
    let mut transaction = repository.begin_invocation_transaction().await?;
    match transaction.resolve_capability_approval(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

fn worker_audit(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    base: u16,
    idempotency: char,
    request: char,
) -> CapabilityWorkerAudit {
    CapabilityWorkerAudit {
        tenant_id: tenant_id.clone(),
        worker_process_generation_id: worker_id.clone(),
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
}

async fn execute_prepare(
    repository: &PgRepository,
    command: PrepareCapabilityDispatch,
) -> Result<
    CommandOutcome<
        insight_platform_postgres::capability_execution_repository::PreparedCapabilityExecution,
    >,
    RepositoryError,
> {
    let mut transaction = repository.begin_invocation_transaction().await?;
    match transaction.prepare_capability_dispatch(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_outcome(
    repository: &PgRepository,
    command: CommitCapabilityOutcome,
) -> Result<
    CommandOutcome<
        insight_platform_postgres::capability_execution_repository::PreparedCapabilityExecution,
    >,
    RepositoryError,
> {
    let mut transaction = repository.begin_invocation_transaction().await?;
    match transaction.commit_capability_outcome(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_wake(
    repository: &PgRepository,
    command: WakeCapabilityInvocation,
) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
    let mut transaction = repository.begin_invocation_transaction().await?;
    match transaction.wake_capability_invocation(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_input(
    repository: &PgRepository,
    command: ResolveCapabilityInput,
) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
    let mut transaction = repository.begin_invocation_transaction().await?;
    match transaction.resolve_capability_input(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_progress(
    repository: &PgRepository,
    command: RecordCapabilityProgress,
) -> Result<CommandOutcome<JobRecord>, RepositoryError> {
    let mut transaction = repository.begin_invocation_transaction().await?;
    match transaction.record_capability_progress(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_control(
    repository: &PgRepository,
    command: ControlCapabilityInvocation,
) -> Result<CommandOutcome<ControlledCapabilityExecution>, RepositoryError> {
    let mut transaction = repository.begin_invocation_transaction().await?;
    match transaction.control_capability_invocation(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

async fn execute_reconciliation(
    repository: &PgRepository,
    command: ResolveCapabilityReconciliation,
) -> Result<CommandOutcome<PreparedCapabilityExecution>, RepositoryError> {
    let mut transaction = repository.begin_invocation_transaction().await?;
    match transaction.resolve_capability_reconciliation(command).await {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await?;
            Err(failure)
        }
    }
}

fn signal_audit(
    tenant_id: &ResourceId,
    base: u16,
    idempotency: char,
    request: char,
) -> CapabilitySignalAudit {
    CapabilitySignalAudit {
        tenant_id: tenant_id.clone(),
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
}

#[derive(Debug)]
struct ClaimEvidence {
    claimed: ClaimedCapabilityExecution,
    worker_id: ResourceId,
    lease_token: Sha256Digest,
}

fn native_cancel_worker(
    repository: &PgRepository,
    claim: &ClaimEvidence,
) -> CapabilityAdapterWorker<PgRepository> {
    let native_contract = match &claim
        .claimed
        .execution_contract
        .implementation
        .backend_contract
    {
        CapabilityBackendContract::Native(contract) => contract,
        _ => panic!("fixture cancellation did not resolve its Native contract"),
    };
    let mut registry = InstalledNativeRegistry::default();
    registry
        .install(Arc::new(CompletingNativeAdapter {
            descriptor: InstalledNativeAdapter {
                adapter_id: native_contract.adapter_id.clone(),
                adapter_version: native_contract.adapter_version.clone(),
                module_digest: native_contract.module_digest.clone(),
                entrypoint_id: native_contract.entrypoint_id.clone(),
            },
            // Cancellation never invokes this response path.
            response: CapabilityAdapterResponse {
                outcome: DispatchOutcome::Uncertain(CapabilityUncertainty {
                    observation_digest: digest('1'),
                    policy_path_digest: digest('2'),
                    external_identity_digest: digest('3'),
                    manual: true,
                }),
            },
        }))
        .unwrap();
    CapabilityAdapterWorker::new(
        Arc::new(CapabilityDispatcher::new(registry)),
        repository.clone(),
    )
}

impl ClaimEvidence {
    fn fence(&self) -> JobFence {
        JobFence {
            expected_version: u64::try_from(self.claimed.job.version).unwrap(),
            worker_process_generation_id: self.worker_id.clone(),
            lease_generation: u64::try_from(self.claimed.job.lease_epoch).unwrap(),
            token_digest: self.lease_token.clone(),
        }
    }
}

async fn claim_one(repository: &PgRepository, tenant_id: &ResourceId, base: u16) -> ClaimEvidence {
    let worker_id = id(ResourceKind::WorkerProcessGeneration, base);
    let lease_token = digest(char::from_digit(u32::from(base % 10), 10).unwrap_or('0'));
    let mut claims = repository
        .claim_capability_jobs(ClaimCapabilityJobs {
            work_class: WorkClass::CapabilityNative,
            worker_process_generation_id: worker_id.clone(),
            worker_manifest_digest: native_worker_manifest_digest(),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![CapabilityClaimSlot {
                lease_token_digest: lease_token.clone(),
                quota_reservation_id: id(ResourceKind::UsageReservation, base + 1),
                quota_entry_ids: vec![
                    id(ResourceKind::QuotaLedgerEntry, base + 2),
                    id(ResourceKind::QuotaLedgerEntry, base + 3),
                ],
                event_id: id(ResourceKind::Event, base + 4),
                outbox_id: id(ResourceKind::OutboxEvent, base + 5),
                resume_mutations: resume_mutations(base + 0x1000),
                failure_mutations: failure_mutations(base + 0x2000),
            }],
        })
        .await
        .unwrap();
    assert_eq!(
        claims.len(),
        1,
        "expected exactly one claim for {tenant_id}"
    );
    ClaimEvidence {
        claimed: claims.pop().unwrap(),
        worker_id,
        lease_token,
    }
}

async fn admit_prepare_and_claim(
    repository: &PgRepository,
    fixture: &Fixture,
    node_id: &ResourceId,
    base: u16,
) -> (CapabilityInvocationRecord, ResourceId, ClaimEvidence) {
    let admission = command_for_node(fixture, node_id, base);
    let admitted = match execute_admit(repository, admission).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh capability admission replayed"),
    };
    assert_eq!(admitted.payload.admission.attempt_limit, 1);
    let job_id = id(ResourceKind::Job, base + 0x10);
    let prepared = match execute_prepare(
        repository,
        PrepareCapabilityDispatch {
            audit: audit(
                &fixture.tenant_id,
                &fixture.principal_id,
                PrincipalKind::AgentRunner,
                base + 0x11,
                '1',
                '2',
            ),
            invocation_id: admitted.invocation_id.clone(),
            expected_invocation_version: admitted.version,
            job_id: job_id.clone(),
            scheduled_at: Utc::now() - Duration::seconds(1),
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh capability prepare replayed"),
    };
    let claim = claim_one(repository, &fixture.tenant_id, base + 0x20).await;
    assert_eq!(
        claim.claimed.invocation.invocation_id,
        admitted.invocation_id
    );
    assert_eq!(claim.claimed.job.job_id, job_id.to_string());
    assert_eq!(claim.claimed.job.attempt_no, 1);
    (prepared.invocation, job_id, claim)
}

struct Fixture {
    tenant_id: ResourceId,
    other_tenant_id: ResourceId,
    principal_id: ResourceId,
    denied_principal_id: ResourceId,
    other_principal_id: ResourceId,
    run_id: ResourceId,
    ready_node_id: ResourceId,
    approval_node_id: ResourceId,
    approval_cancel_node_id: ResourceId,
    deferred_node_id: ResourceId,
    uncertain_node_id: ResourceId,
    cancel_node_id: ResourceId,
    cancel_worker_node_id: ResourceId,
    process_recovery_node_id: ResourceId,
    inline_value_id: ResourceId,
    artifact_value_id: ResourceId,
    artifact_id: ResourceId,
    capability_deployment: ExactDeploymentRef,
    output_schema_digest: Sha256Digest,
    policy: ExactVersionRef,
    plan: RuntimePlan,
    selection_policy: ExactPolicyBinding,
    input_schema_digest: Sha256Digest,
}

#[test]
fn capability_admission_is_exact_atomic_replayable_and_approval_first_winner() {
    std::thread::Builder::new()
        .name("phase3-invocation-fixture".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(run_capability_phase3_fixture());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_capability_phase3_fixture() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let fixture = seed_fixture(&pool, &repository).await;

    let denied = AdmitCapabilityInvocation {
        audit: audit(
            &fixture.tenant_id,
            &fixture.denied_principal_id,
            PrincipalKind::AgentRunner,
            0x100,
            '1',
            '1',
        ),
        invocation_id: id(ResourceKind::CapabilityInvocation, 0x110),
        run_id: fixture.run_id.clone(),
        node_execution_id: fixture.ready_node_id.clone(),
        expected_run_version: 1,
        expected_node_version: 1,
        slot_id: "writer".to_owned(),
        input_value_id: fixture.inline_value_id.clone(),
        input_artifact_link_id: None,
        origin: InvocationOrigin::PlanNode {
            node_execution_id: fixture.ready_node_id.clone(),
        },
        selected_candidate_ordinal: 0,
        selector_input_digest: digest('2'),
        policy_decisions: allowed_policy(&fixture.policy),
        approval_task_id: None,
        requested_attempt_limit: 3,
        requested_retry_backoff_milliseconds: 100,
        mcp_runtime: None,
    };
    match execute_admit(&repository, denied).await {
        Err(RepositoryError::PermissionDenied) => {}
        other => panic!("denied admission returned {other:?}"),
    }

    let cross_tenant = AdmitCapabilityInvocation {
        audit: audit(
            &fixture.other_tenant_id,
            &fixture.other_principal_id,
            PrincipalKind::AgentRunner,
            0x120,
            '2',
            '2',
        ),
        invocation_id: id(ResourceKind::CapabilityInvocation, 0x130),
        run_id: fixture.run_id.clone(),
        node_execution_id: fixture.ready_node_id.clone(),
        expected_run_version: 1,
        expected_node_version: 1,
        slot_id: "writer".to_owned(),
        input_value_id: fixture.inline_value_id.clone(),
        input_artifact_link_id: None,
        origin: InvocationOrigin::PlanNode {
            node_execution_id: fixture.ready_node_id.clone(),
        },
        selected_candidate_ordinal: 0,
        selector_input_digest: digest('3'),
        policy_decisions: allowed_policy(&fixture.policy),
        approval_task_id: None,
        requested_attempt_limit: 3,
        requested_retry_backoff_milliseconds: 100,
        mcp_runtime: None,
    };
    assert!(matches!(
        execute_admit(&repository, cross_tenant).await,
        Err(RepositoryError::NotFound("run"))
    ));

    let mut invalid_candidate = command_for_inline(&fixture, 0x140);
    invalid_candidate.selected_candidate_ordinal = 1;
    assert!(matches!(
        execute_admit(&repository, invalid_candidate).await,
        Err(RepositoryError::InvalidInput(_))
    ));

    let inline = command_for_inline(&fixture, 0x150);
    let applied = match execute_admit(&repository, inline.clone()).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first inline admission replayed"),
    };
    assert_eq!(
        applied.state,
        insight_platform_contracts::InvocationState::Ready
    );
    assert_eq!(applied.payload.admission.attempt_limit, 1);
    assert_eq!(
        applied.payload.admission.deployment,
        fixture.capability_deployment
    );
    assert_eq!(
        applied.payload.admission.input.value_id,
        fixture.inline_value_id
    );
    assert!(matches!(
        execute_admit(&repository, inline.clone()).await.unwrap(),
        CommandOutcome::Replayed(record) if record == applied
    ));
    let mut conflict = inline;
    conflict.audit.request_digest = digest('f');
    assert!(matches!(
        execute_admit(&repository, conflict).await,
        Err(RepositoryError::IdempotencyConflict)
    ));

    let capability_job_id = id(ResourceKind::Job, 0x300);
    let quota_reservation_id = id(ResourceKind::UsageReservation, 0x305);
    let prepared = match execute_prepare(
        &repository,
        PrepareCapabilityDispatch {
            audit: audit(
                &fixture.tenant_id,
                &fixture.principal_id,
                PrincipalKind::AgentRunner,
                0x301,
                '1',
                '2',
            ),
            invocation_id: applied.invocation_id.clone(),
            expected_invocation_version: applied.version,
            job_id: capability_job_id.clone(),
            scheduled_at: Utc::now() - Duration::seconds(1),
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Capability dispatch prepare replayed"),
    };
    assert_eq!(prepared.invocation.version, applied.version + 1);
    assert_eq!(prepared.job.state, "ready");
    let execution_trace_ids: (String, String, String) = sqlx::query_as(
        r#"
        SELECT run.trace_id, invocation.trace_id, job.trace_id
        FROM insight_platform.runs AS run
        JOIN insight_platform.invocations AS invocation
          ON invocation.tenant_id = run.tenant_id AND invocation.run_id = run.run_id
        JOIN insight_platform.jobs AS job
          ON job.tenant_id = invocation.tenant_id
         AND job.invocation_id = invocation.invocation_id
        WHERE run.tenant_id = $1 AND invocation.invocation_id = $2 AND job.job_id = $3
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(applied.invocation_id.to_string())
    .bind(capability_job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(execution_trace_ids.0, execution_trace_ids.1);
    assert_eq!(execution_trace_ids.0, execution_trace_ids.2);
    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0x304);
    let lease_token = digest('3');
    let claim_slot = CapabilityClaimSlot {
        lease_token_digest: lease_token.clone(),
        quota_reservation_id: quota_reservation_id.clone(),
        quota_entry_ids: vec![
            id(ResourceKind::QuotaLedgerEntry, 0x306),
            id(ResourceKind::QuotaLedgerEntry, 0x307),
        ],
        event_id: id(ResourceKind::Event, 0x308),
        outbox_id: id(ResourceKind::OutboxEvent, 0x309),
        resume_mutations: resume_mutations(0x1300),
        failure_mutations: failure_mutations(0x2300),
    };
    let wrong_manifest_claims = repository
        .claim_capability_jobs(ClaimCapabilityJobs {
            work_class: WorkClass::CapabilityNative,
            worker_process_generation_id: worker_id.clone(),
            worker_manifest_digest: digest('f'),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![claim_slot.clone()],
        })
        .await
        .unwrap();
    assert!(wrong_manifest_claims.is_empty());
    let mut claims = repository
        .claim_capability_jobs(ClaimCapabilityJobs {
            work_class: WorkClass::CapabilityNative,
            worker_process_generation_id: worker_id.clone(),
            worker_manifest_digest: native_worker_manifest_digest(),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![claim_slot],
        })
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    let claimed = claims.pop().unwrap();
    assert_eq!(claimed.invocation.invocation_id, applied.invocation_id);
    assert_eq!(claimed.job.state, "running");
    assert_eq!(
        claimed.job.quota_reservation_id,
        Some(quota_reservation_id.to_string())
    );
    let output_json = json!({"accepted": true});
    let output_digest: Sha256Digest = canonical_digest(&output_json).unwrap().parse().unwrap();
    assert_eq!(claimed.fence.worker_process_generation_id, worker_id);
    assert_eq!(claimed.fence.token_digest, lease_token);
    let response = CapabilityAdapterResponse {
        outcome: DispatchOutcome::Completed(CapabilityOutputValue {
            value_id: id(ResourceKind::RunValue, 0x315),
            classification: DataClassification::Internal,
            schema_digest: fixture.output_schema_digest.clone(),
            content_digest: output_digest,
            value: ValueRef::Inline { value: output_json },
            artifact_link_id: None,
            validation_evidence_digest: digest('6'),
        }),
    };
    let completion_audit = worker_audit(&fixture.tenant_id, &worker_id, 0x310, '4', '5');
    assert!(matches!(
        claimed.adapter_job(
            native_worker_manifest_digest(),
            completion_audit.clone(),
            claimed.quota_reservation_entry_ids.clone(),
            None,
        ),
        Err(RepositoryError::InvalidInput(_))
    ));
    let worker_command = claimed
        .adapter_job(
            native_worker_manifest_digest(),
            completion_audit.clone(),
            vec![
                id(ResourceKind::QuotaLedgerEntry, 0x313),
                id(ResourceKind::QuotaLedgerEntry, 0x314),
            ],
            None,
        )
        .unwrap();
    assert_eq!(worker_command.execution.physical_attempt, 1);
    assert_eq!(worker_command.execution.attempt_limit, 1);
    assert_eq!(
        worker_command.execution.admission_digest,
        claimed.invocation.payload.admission.canonical_digest
    );
    let backend_contract = &claimed.execution_contract.implementation.backend_contract;
    let CapabilityBackendContract::Native(native_contract) = backend_contract else {
        panic!("fixture claim did not resolve its Native contract");
    };
    let mut registry = InstalledNativeRegistry::default();
    registry
        .install(Arc::new(CompletingNativeAdapter {
            descriptor: InstalledNativeAdapter {
                adapter_id: native_contract.adapter_id.clone(),
                adapter_version: native_contract.adapter_version.clone(),
                module_digest: native_contract.module_digest.clone(),
                entrypoint_id: native_contract.entrypoint_id.clone(),
            },
            response: response.clone(),
        }))
        .unwrap();
    let worker = CapabilityAdapterWorker::new(
        Arc::new(CapabilityDispatcher::new(registry)),
        repository.clone(),
    );
    let replay_outcome = CommitCapabilityOutcome {
        audit: completion_audit,
        invocation_id: worker_command.execution.invocation_id.clone(),
        job_id: worker_command.execution.job_id.clone(),
        expected_invocation_version: worker_command.expected_invocation_version,
        fence: worker_command.fence.clone(),
        quota_entry_ids: worker_command.quota_entry_ids.clone(),
        outcome: response.outcome,
        resume_mutations: None,
        failure_mutations: None,
    };
    let completed = match worker.execute(worker_command).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Capability outcome replayed"),
    };
    assert_eq!(
        completed.invocation.state,
        insight_platform_contracts::InvocationState::Succeeded
    );
    assert_eq!(completed.job.state, "succeeded");
    assert!(completed.job.quota_reservation_id.is_none());
    assert!(matches!(
        execute_outcome(&repository, replay_outcome).await.unwrap(),
        CommandOutcome::Replayed(record) if record == completed
    ));
    let quota_totals: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT count(*), COALESCE(sum(reserved_value), 0)::bigint,
               (SELECT count(*) FROM insight_platform.quota_ledger
                 WHERE tenant_id = $1)
        FROM insight_platform.quota_accounts WHERE tenant_id = $1
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(quota_totals, (2, 0, 4));

    let approval = command_for_artifact(&fixture, 0x180);
    let awaiting = match execute_admit(&repository, approval.clone()).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first approval admission replayed"),
    };
    assert_eq!(
        awaiting.state,
        insight_platform_contracts::InvocationState::AwaitingApproval
    );
    assert_eq!(
        awaiting.payload.admission.input_artifact_link_id,
        approval.input_artifact_link_id
    );
    assert!(matches!(
        execute_admit(&repository, approval.clone()).await.unwrap(),
        CommandOutcome::Replayed(record) if record == awaiting
    ));
    let task_trace_ids: (String, String, String) = sqlx::query_as(
        r#"
        SELECT run.trace_id, invocation.trace_id, task.trace_id
        FROM insight_platform.runs AS run
        JOIN insight_platform.invocations AS invocation
          ON invocation.tenant_id = run.tenant_id AND invocation.run_id = run.run_id
        JOIN insight_platform.tasks AS task
          ON task.tenant_id = invocation.tenant_id
         AND task.invocation_id = invocation.invocation_id
        WHERE run.tenant_id = $1 AND invocation.invocation_id = $2 AND task.task_id = $3
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(awaiting.invocation_id.to_string())
    .bind(approval.approval_task_id.as_ref().unwrap().to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(task_trace_ids.0, task_trace_ids.1);
    assert_eq!(task_trace_ids.0, task_trace_ids.2);

    let mut wrong_rule = approval_resolution(&fixture, &approval, 0x1a0, true);
    wrong_rule.eligible_principal_rule_digest = digest('b');
    assert!(matches!(
        execute_approval(&repository, wrong_rule).await,
        Err(RepositoryError::Conflict(
            "Capability approval Task binding"
        ))
    ));

    let approve = approval_resolution(&fixture, &approval, 0x1b0, true);
    let reject = approval_resolution(&fixture, &approval, 0x1c0, false);
    let (approve_result, reject_result) = tokio::join!(
        execute_approval(&repository, approve.clone()),
        execute_approval(&repository, reject.clone()),
    );
    let (winner, winner_command) = match (approve_result, reject_result) {
        (Ok(CommandOutcome::Applied(record)), Err(RepositoryError::Conflict(_))) => {
            (record, approve)
        }
        (Err(RepositoryError::Conflict(_)), Ok(CommandOutcome::Applied(record))) => {
            (record, reject)
        }
        other => panic!("approval race did not have one winner: {other:?}"),
    };
    assert!(matches!(
        execute_approval(&repository, winner_command).await.unwrap(),
        CommandOutcome::Replayed(record) if record == winner
    ));
    assert!(matches!(
        winner.state,
        insight_platform_contracts::InvocationState::Ready
            | insight_platform_contracts::InvocationState::Failed
    ));

    let counts: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.invocations
             WHERE tenant_id = $1 AND invocation_kind = 'capability'),
          (SELECT count(*) FROM insight_platform.tasks
             WHERE tenant_id = $1 AND task_kind = 'approval'),
          (SELECT count(*) FROM insight_platform.artifact_links
             WHERE tenant_id = $1 AND owner_kind = 'capability_invocation'
               AND target_artifact_id = $2 AND state = 'active'),
          (SELECT count(*) FROM insight_platform.receipts
             WHERE tenant_id = $1 AND scope_kind = 'capability_invocation'
               AND state = 'succeeded'),
          (SELECT count(*) FROM insight_platform.events
             WHERE tenant_id = $1 AND aggregate_kind = 'capability_invocation'),
          (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.artifact_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (2, 1, 1, 4, 6, 6));
    let orphan_negative_receipts: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM insight_platform.receipts
        WHERE receipt_id IN ($1, $2, $3, $4)
        "#,
    )
    .bind(id(ResourceKind::Receipt, 0x100).to_string())
    .bind(id(ResourceKind::Receipt, 0x120).to_string())
    .bind(id(ResourceKind::Receipt, 0x140).to_string())
    .bind(id(ResourceKind::Receipt, 0x1a0).to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(orphan_negative_receipts, 0);

    let mut cancel_approval = command_for_artifact(&fixture, 0x1d0);
    cancel_approval.node_execution_id = fixture.approval_cancel_node_id.clone();
    cancel_approval.origin = InvocationOrigin::PlanNode {
        node_execution_id: fixture.approval_cancel_node_id.clone(),
    };
    let cancel_awaiting = match execute_admit(&repository, cancel_approval.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first cancellable approval admission replayed"),
    };
    let mut cancel = approval_resolution(&fixture, &cancel_approval, 0x1e0, false);
    cancel.decision = CapabilityApprovalDecision::Cancel;
    let cancelled = match execute_approval(&repository, cancel.clone()).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first approval cancellation replayed"),
    };
    assert_eq!(
        cancel_awaiting.state,
        insight_platform_contracts::InvocationState::AwaitingApproval
    );
    assert_eq!(
        cancelled.state,
        insight_platform_contracts::InvocationState::Cancelled
    );
    assert!(cancelled.payload.failure.is_none());
    assert!(matches!(
        execute_approval(&repository, cancel).await.unwrap(),
        CommandOutcome::Replayed(record) if record == cancelled
    ));
    let cancelled_task_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.tasks WHERE tenant_id = $1 AND task_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(cancel_approval.approval_task_id.unwrap().to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cancelled_task_state, "cancelled");

    // A non-idempotent dispatch may defer and request input after its only physical attempt has
    // started. Callback/input continuation must rotate the lease fence without consuming attempt 2.
    let (_, deferred_job_id, first_claim) =
        admit_prepare_and_claim(&repository, &fixture, &fixture.deferred_node_id, 0x400).await;
    let original_expiry = first_claim.claimed.job.lease_expires_at;
    let original_heartbeat = first_claim.claimed.job.heartbeat_at;
    let progress_job = match execute_progress(
        &repository,
        RecordCapabilityProgress {
            audit: worker_audit(&fixture.tenant_id, &first_claim.worker_id, 0x450, '1', '2'),
            invocation_id: first_claim.claimed.invocation.invocation_id.clone(),
            job_id: deferred_job_id.clone(),
            expected_invocation_version: first_claim.claimed.invocation.version,
            fence: first_claim.fence(),
            progress: CapabilityProgress {
                sequence: 1,
                stage_key: "remote_started".to_owned(),
                schema_digest: digest('8'),
                payload_digest: digest('3'),
                payload_bytes: 32,
            },
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(job) => job,
        CommandOutcome::Replayed(_) => panic!("fresh progress replayed"),
    };
    assert_eq!(progress_job.lease_expires_at, original_expiry);
    assert_eq!(progress_job.heartbeat_at, original_heartbeat);
    let stale_progress_receipt = id(ResourceKind::Receipt, 0x460);
    let mut stale_progress_fence = first_claim.fence();
    stale_progress_fence.expected_version = u64::try_from(progress_job.version).unwrap();
    stale_progress_fence.token_digest = digest('f');
    assert!(execute_progress(
        &repository,
        RecordCapabilityProgress {
            audit: worker_audit(&fixture.tenant_id, &first_claim.worker_id, 0x460, '4', '5',),
            invocation_id: first_claim.claimed.invocation.invocation_id.clone(),
            job_id: deferred_job_id.clone(),
            expected_invocation_version: first_claim.claimed.invocation.version,
            fence: stale_progress_fence,
            progress: CapabilityProgress {
                sequence: 2,
                stage_key: "stale".to_owned(),
                schema_digest: digest('8'),
                payload_digest: digest('6'),
                payload_bytes: 16,
            },
        },
    )
    .await
    .is_err());
    let stale_progress_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(stale_progress_receipt.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stale_progress_receipts, 0);

    let remote_state = EncryptedRemoteState {
        scheme: "aes256_gcm_v1".to_owned(),
        key_id: "remote-state-key-1".to_owned(),
        key_reference_digest: digest('7'),
        ciphertext: "ZW5jcnlwdGVkLXJlbW90ZS1zdGF0ZQ==".to_owned(),
        plaintext_digest: digest('8'),
    };
    let callback_binding_digest = digest('9');
    let next_poll_at = Utc::now() + Duration::seconds(1);
    let deferred_fence = JobFence {
        expected_version: u64::try_from(progress_job.version).unwrap(),
        ..first_claim.fence()
    };
    let deferred = match execute_outcome(
        &repository,
        CommitCapabilityOutcome {
            audit: worker_audit(&fixture.tenant_id, &first_claim.worker_id, 0x470, '7', '8'),
            invocation_id: first_claim.claimed.invocation.invocation_id.clone(),
            job_id: deferred_job_id.clone(),
            expected_invocation_version: first_claim.claimed.invocation.version,
            fence: deferred_fence,
            quota_entry_ids: vec![
                id(ResourceKind::QuotaLedgerEntry, 0x473),
                id(ResourceKind::QuotaLedgerEntry, 0x474),
            ],
            outcome: DispatchOutcome::Deferred(RemoteWait {
                encrypted_state: remote_state.clone(),
                external_identity_digest: digest('a'),
                accepted_sources: vec![WakeSource::Callback, WakeSource::Poll],
                next_poll_at: Some(next_poll_at),
                callback_binding_digest: Some(callback_binding_digest.clone()),
            }),
            resume_mutations: None,
            failure_mutations: None,
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh deferred outcome replayed"),
    };
    assert_eq!(
        deferred.invocation.state,
        insight_platform_contracts::InvocationState::Deferred
    );
    assert_eq!(deferred.job.state, "waiting");
    assert!(deferred.job.quota_reservation_id.is_none());
    let deferred_generation = u64::try_from(deferred.job.lease_epoch).unwrap();

    let stale_callback_event = id(ResourceKind::Event, 0x481);
    let stale_callback_outbox = id(ResourceKind::OutboxEvent, 0x482);
    let stale_callback = match execute_wake(
        &repository,
        WakeCapabilityInvocation {
            audit: signal_audit(&fixture.tenant_id, 0x480, '9', 'a'),
            invocation_id: deferred.invocation.invocation_id.clone(),
            job_id: deferred_job_id.clone(),
            expected_generation: deferred_generation,
            source: WakeSource::Callback,
            callback_binding_digest: Some(digest('b')),
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh stale callback replayed"),
    };
    assert_eq!(stale_callback.invocation, deferred.invocation);
    let stale_callback_durable_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_id = $2)
             + (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1 AND outbox_id = $3)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(stale_callback_event.to_string())
    .bind(stale_callback_outbox.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stale_callback_durable_rows, 0);

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let callback_command = WakeCapabilityInvocation {
        audit: signal_audit(&fixture.tenant_id, 0x490, 'c', 'd'),
        invocation_id: deferred.invocation.invocation_id.clone(),
        job_id: deferred_job_id.clone(),
        expected_generation: deferred_generation,
        source: WakeSource::Callback,
        callback_binding_digest: Some(callback_binding_digest),
    };
    let poll_command = WakeCapabilityInvocation {
        audit: signal_audit(&fixture.tenant_id, 0x498, 'e', 'f'),
        invocation_id: deferred.invocation.invocation_id.clone(),
        job_id: deferred_job_id.clone(),
        expected_generation: deferred_generation,
        source: WakeSource::Poll,
        callback_binding_digest: None,
    };
    let callback_future = Box::pin(execute_wake(&repository, callback_command));
    let poll_future = Box::pin(execute_wake(&repository, poll_command));
    let (callback_result, poll_result) = tokio::join!(callback_future, poll_future);
    let callback_record = match callback_result.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh callback replayed"),
    };
    let poll_record = match poll_result.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh poll replayed"),
    };
    assert_eq!(callback_record.invocation, poll_record.invocation);
    assert_eq!(callback_record.job, poll_record.job);
    let wake_winner_rows: (i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.events
             WHERE tenant_id = $1 AND event_id IN ($2, $3)),
          (SELECT count(*) FROM insight_platform.outbox_events
             WHERE tenant_id = $1 AND outbox_id IN ($4, $5))
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::Event, 0x491).to_string())
    .bind(id(ResourceKind::Event, 0x499).to_string())
    .bind(id(ResourceKind::OutboxEvent, 0x492).to_string())
    .bind(id(ResourceKind::OutboxEvent, 0x49a).to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(wake_winner_rows, (1, 1));
    let woken = callback_record;
    assert_eq!(woken.job.state, "ready");
    let second_claim = claim_one(&repository, &fixture.tenant_id, 0x4a0).await;
    assert_eq!(second_claim.claimed.job.job_id, deferred_job_id.to_string());
    assert_eq!(second_claim.claimed.job.attempt_no, 1);
    assert!(second_claim.claimed.job.lease_epoch > deferred.job.lease_epoch);
    let resumed_execution = second_claim
        .claimed
        .adapter_execution(native_worker_manifest_digest())
        .unwrap();
    let resumed_continuation = resumed_execution
        .continuation
        .expect("the claimed Job must recover its encrypted remote continuation");
    assert_eq!(resumed_continuation.encrypted_remote_state, remote_state);
    assert_eq!(
        resumed_continuation.external_identity_digest,
        Some(digest('a'))
    );
    assert!(resumed_continuation.resume_input.is_none());
    assert!(resumed_continuation.poll_count <= 1);
    let persisted_remote_payload: String = sqlx::query_scalar(
        "SELECT payload::text FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(deferred_job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!persisted_remote_payload.contains("encrypted-remote-state"));
    let persisted_resume_flag: bool = sqlx::query_scalar(
        "SELECT (payload->>'resume_same_attempt')::boolean FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(deferred_job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!persisted_resume_flag);

    let input_task_id = id(ResourceKind::Interaction, 0x4c0);
    let response_schema =
        insight_platform_contracts::InteractionSchemaDocument::build(serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "approved": {"type": "boolean"}
            },
            "required": ["approved"],
            "additionalProperties": false
        }))
        .unwrap();
    let input_schema_digest = response_schema.canonical_digest.clone();
    let eligible_rule_digest = digest('d');
    let input_required = match execute_outcome(
        &repository,
        CommitCapabilityOutcome {
            audit: worker_audit(&fixture.tenant_id, &second_claim.worker_id, 0x4b0, 'e', 'f'),
            invocation_id: second_claim.claimed.invocation.invocation_id.clone(),
            job_id: deferred_job_id.clone(),
            expected_invocation_version: second_claim.claimed.invocation.version,
            fence: second_claim.fence(),
            quota_entry_ids: vec![
                id(ResourceKind::QuotaLedgerEntry, 0x4b3),
                id(ResourceKind::QuotaLedgerEntry, 0x4b4),
            ],
            outcome: DispatchOutcome::InputRequired(BackendInputRequest {
                input_task_id: input_task_id.clone(),
                interaction_kind: InteractionKind::Form,
                safe_prompt_key: "confirm_remote_write".to_owned(),
                response_schema,
                response_schema_digest: input_schema_digest.clone(),
                eligible_principal_rule_digest: eligible_rule_digest.clone(),
                exact_eligible_principal_id: None,
                opaque_state_digest: remote_state.plaintext_digest.clone(),
                encrypted_state: remote_state.clone(),
                external_identity_digest: digest('a'),
                deadline: Utc::now() + Duration::minutes(10),
            }),
            resume_mutations: None,
            failure_mutations: None,
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh input-required outcome replayed"),
    };
    assert_eq!(
        input_required.invocation.state,
        insight_platform_contracts::InvocationState::AwaitingInput
    );
    assert_eq!(input_required.job.state, "waiting");
    let input_generation = u64::try_from(input_required.job.lease_epoch).unwrap();
    let response_json = json!({"confirmed": true});
    let response_digest: Sha256Digest = canonical_digest(&response_json).unwrap().parse().unwrap();
    let mut wrong_input = ResolveCapabilityInput {
        audit: audit(
            &fixture.tenant_id,
            &fixture.principal_id,
            PrincipalKind::AgentRunner,
            0x4d0,
            '1',
            '2',
        ),
        invocation_id: input_required.invocation.invocation_id.clone(),
        job_id: deferred_job_id.clone(),
        input_task_id: input_task_id.clone(),
        expected_invocation_version: input_required.invocation.version,
        expected_job_version: u64::try_from(input_required.job.version).unwrap(),
        expected_wake_generation: input_generation,
        expected_task_generation: 1,
        expected_task_version: 1,
        eligible_principal_rule_digest: digest('f'),
        action: insight_platform_invocations::CapabilityInputAction::Accept,
        response: Some(CapabilityInputResponse {
            value_id: id(ResourceKind::RunValue, 0x4d5),
            classification: DataClassification::Internal,
            schema_digest: input_schema_digest.clone(),
            content_digest: response_digest.clone(),
            value: ValueRef::Inline {
                value: response_json.clone(),
            },
            artifact_link_id: None,
        }),
    };
    assert!(execute_input(&repository, wrong_input.clone())
        .await
        .is_err());
    let wrong_input_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(wrong_input.audit.receipt_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(wrong_input_receipts, 0);
    wrong_input.audit = audit(
        &fixture.tenant_id,
        &fixture.principal_id,
        PrincipalKind::AgentRunner,
        0x4e0,
        '3',
        '4',
    );
    wrong_input.eligible_principal_rule_digest = eligible_rule_digest;
    wrong_input.response.as_mut().unwrap().value_id = id(ResourceKind::RunValue, 0x4e5);
    let input_resolved = match execute_input(&repository, wrong_input).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh input response replayed"),
    };
    assert_eq!(input_resolved.job.state, "ready");
    let third_claim = claim_one(&repository, &fixture.tenant_id, 0x4f0).await;
    assert_eq!(third_claim.claimed.job.job_id, deferred_job_id.to_string());
    assert_eq!(third_claim.claimed.job.attempt_no, 1);
    assert!(third_claim.claimed.job.lease_epoch > second_claim.claimed.job.lease_epoch);
    let input_resume_execution = third_claim
        .claimed
        .adapter_execution(native_worker_manifest_digest())
        .unwrap();
    let input_resume = input_resume_execution
        .continuation
        .expect("the claimed Job must recover encrypted state and the winning input RunValue");
    assert_eq!(input_resume.encrypted_remote_state, remote_state);
    assert_eq!(input_resume.external_identity_digest, Some(digest('a')));
    assert_eq!(
        input_resume.resume_input_action,
        Some(insight_platform_invocations::CapabilityInputAction::Accept)
    );
    let resume_input = input_resume.resume_input.unwrap();
    assert_eq!(
        resume_input.exact.value_id,
        id(ResourceKind::RunValue, 0x4e5)
    );
    assert_eq!(
        resume_input.material,
        insight_platform_invocations::CapabilityExecutionInputMaterial::Inline {
            value: response_json.clone(),
        }
    );
    let final_json = json!({"result": "continued"});
    let final_digest: Sha256Digest = canonical_digest(&final_json).unwrap().parse().unwrap();
    let continued_completion = match execute_outcome(
        &repository,
        CommitCapabilityOutcome {
            audit: worker_audit(&fixture.tenant_id, &third_claim.worker_id, 0x500, '5', '6'),
            invocation_id: third_claim.claimed.invocation.invocation_id.clone(),
            job_id: deferred_job_id.clone(),
            expected_invocation_version: third_claim.claimed.invocation.version,
            fence: third_claim.fence(),
            quota_entry_ids: vec![
                id(ResourceKind::QuotaLedgerEntry, 0x503),
                id(ResourceKind::QuotaLedgerEntry, 0x504),
            ],
            outcome: DispatchOutcome::Completed(CapabilityOutputValue {
                value_id: id(ResourceKind::RunValue, 0x505),
                classification: DataClassification::Internal,
                schema_digest: fixture.output_schema_digest.clone(),
                content_digest: final_digest,
                value: ValueRef::Inline { value: final_json },
                artifact_link_id: None,
                validation_evidence_digest: digest('7'),
            }),
            resume_mutations: None,
            failure_mutations: None,
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh continued completion replayed"),
    };
    assert_eq!(
        continued_completion.invocation.state,
        insight_platform_contracts::InvocationState::Succeeded
    );
    assert_eq!(continued_completion.job.attempt_no, 1);

    // A killed worker's expired non-idempotent attempt releases execution quota and requires an
    // authorized manual disposition; the safety scanner never silently retries it.
    let (_, uncertain_job_id, _uncertain_claim) =
        admit_prepare_and_claim(&repository, &fixture, &fixture.uncertain_node_id, 0x600).await;
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(uncertain_job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let recovered = repository
        .drive_expired_capability_jobs(DriveExpiredCapabilityJobs {
            work_class: WorkClass::CapabilityNative,
            shard: SafetyScanShard::whole(),
            after: None,
            limit: 1,
            slots: vec![ExpiredCapabilityRecoverySlot {
                quota_entry_ids: vec![
                    id(ResourceKind::QuotaLedgerEntry, 0x653),
                    id(ResourceKind::QuotaLedgerEntry, 0x654),
                ],
                event_id: id(ResourceKind::Event, 0x655),
                outbox_id: id(ResourceKind::OutboxEvent, 0x656),
                failure_mutations: failure_mutations(0x2600),
            }],
        })
        .await
        .unwrap();
    assert_eq!(recovered.records.len(), 1);
    let uncertain = recovered.records.into_iter().next().unwrap();
    assert_eq!(
        uncertain.invocation.state,
        insight_platform_contracts::InvocationState::ReconciliationRequired
    );
    assert!(uncertain.job.quota_reservation_id.is_none());
    let reconciled = match execute_reconciliation(
        &repository,
        ResolveCapabilityReconciliation {
            audit: audit(
                &fixture.tenant_id,
                &fixture.principal_id,
                PrincipalKind::AgentRunner,
                0x660,
                'd',
                'e',
            ),
            invocation_id: uncertain.invocation.invocation_id.clone(),
            expected_invocation_version: uncertain.invocation.version,
            expected_job_version: u64::try_from(uncertain.job.version).unwrap(),
            resolution: ReconciliationResolution::Cancelled,
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh reconciliation replayed"),
    };
    assert_eq!(
        reconciled.invocation.state,
        insight_platform_contracts::InvocationState::Cancelled
    );

    // A durable control winner rotates only the optimistic Job version and the original physical
    // execution then submits the exact cancellation observation through the PostgreSQL authority.
    let (_, _, cancel_worker_claim) =
        admit_prepare_and_claim(&repository, &fixture, &fixture.cancel_worker_node_id, 0x800).await;
    let controlled = match execute_control(
        &repository,
        ControlCapabilityInvocation {
            audit: audit(
                &fixture.tenant_id,
                &fixture.principal_id,
                PrincipalKind::AgentRunner,
                0x850,
                '2',
                '3',
            ),
            invocation_id: cancel_worker_claim.claimed.invocation.invocation_id.clone(),
            expected_invocation_version: cancel_worker_claim.claimed.invocation.version,
            quota_entry_ids: vec![],
            kind: CapabilityControlKind::Cancel,
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh deterministic cancellation replayed"),
    };
    assert!(matches!(
        controlled.cancel_adapter_job(
            &cancel_worker_claim.claimed,
            native_worker_manifest_digest(),
            worker_audit(
                &fixture.tenant_id,
                &cancel_worker_claim.worker_id,
                0x860,
                '4',
                '5',
            ),
            cancel_worker_claim
                .claimed
                .quota_reservation_entry_ids
                .clone(),
            Utc::now() + Duration::seconds(1),
        ),
        Err(RepositoryError::InvalidInput(_))
    ));
    let cancel_worker_command = controlled
        .cancel_adapter_job(
            &cancel_worker_claim.claimed,
            native_worker_manifest_digest(),
            worker_audit(
                &fixture.tenant_id,
                &cancel_worker_claim.worker_id,
                0x870,
                '6',
                '7',
            ),
            vec![
                id(ResourceKind::QuotaLedgerEntry, 0x873),
                id(ResourceKind::QuotaLedgerEntry, 0x874),
            ],
            Utc::now() + Duration::seconds(1),
        )
        .unwrap();
    assert_eq!(
        cancel_worker_command.fence.expected_version,
        cancel_worker_claim.fence().expected_version + 1
    );
    assert_eq!(
        cancel_worker_command.fence.worker_process_generation_id,
        cancel_worker_claim.worker_id
    );
    assert_eq!(
        cancel_worker_command.fence.token_digest,
        cancel_worker_claim.lease_token
    );
    let cancelled = match native_cancel_worker(&repository, &cancel_worker_claim)
        .cancel(cancel_worker_command)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh deterministic cancellation outcome replayed"),
    };
    assert_eq!(
        cancelled.invocation.state,
        insight_platform_contracts::InvocationState::ReconciliationRequired
    );
    assert!(cancelled.job.quota_reservation_id.is_none());

    // Completion and runtime cancellation lock the same current facts. Exactly one transition
    // wins; a cancelling winner is then closed by the same fenced backend cancellation path.
    let (_, cancel_job_id, cancel_claim) =
        admit_prepare_and_claim(&repository, &fixture, &fixture.cancel_node_id, 0x700).await;
    let cancel_command = ControlCapabilityInvocation {
        audit: audit(
            &fixture.tenant_id,
            &fixture.principal_id,
            PrincipalKind::AgentRunner,
            0x750,
            'f',
            '1',
        ),
        invocation_id: cancel_claim.claimed.invocation.invocation_id.clone(),
        expected_invocation_version: cancel_claim.claimed.invocation.version,
        quota_entry_ids: vec![],
        kind: CapabilityControlKind::Cancel,
    };
    let race_json = json!({"race": "completed"});
    let race_digest: Sha256Digest = canonical_digest(&race_json).unwrap().parse().unwrap();
    let complete_command = CommitCapabilityOutcome {
        audit: worker_audit(&fixture.tenant_id, &cancel_claim.worker_id, 0x760, '2', '3'),
        invocation_id: cancel_claim.claimed.invocation.invocation_id.clone(),
        job_id: cancel_job_id.clone(),
        expected_invocation_version: cancel_claim.claimed.invocation.version,
        fence: cancel_claim.fence(),
        quota_entry_ids: vec![
            id(ResourceKind::QuotaLedgerEntry, 0x763),
            id(ResourceKind::QuotaLedgerEntry, 0x764),
        ],
        outcome: DispatchOutcome::Completed(CapabilityOutputValue {
            value_id: id(ResourceKind::RunValue, 0x765),
            classification: DataClassification::Internal,
            schema_digest: fixture.output_schema_digest.clone(),
            content_digest: race_digest,
            value: ValueRef::Inline { value: race_json },
            artifact_link_id: None,
            validation_evidence_digest: digest('4'),
        }),
        resume_mutations: None,
        failure_mutations: None,
    };
    let (cancel_result, complete_result) = tokio::join!(
        execute_control(&repository, cancel_command),
        execute_outcome(&repository, complete_command),
    );
    match (cancel_result, complete_result) {
        (Ok(CommandOutcome::Applied(controlled)), Err(_)) => {
            assert_eq!(
                controlled.invocation.state,
                insight_platform_contracts::InvocationState::Cancelling
            );
            let cancel_worker_command = controlled
                .cancel_adapter_job(
                    &cancel_claim.claimed,
                    native_worker_manifest_digest(),
                    worker_audit(&fixture.tenant_id, &cancel_claim.worker_id, 0x780, '5', '6'),
                    vec![
                        id(ResourceKind::QuotaLedgerEntry, 0x783),
                        id(ResourceKind::QuotaLedgerEntry, 0x784),
                    ],
                    Utc::now() + Duration::seconds(1),
                )
                .unwrap();
            assert!(
                cancel_worker_command.fence.expected_version
                    > cancel_claim.fence().expected_version
            );
            assert_eq!(
                cancel_worker_command.fence.worker_process_generation_id,
                cancel_claim.worker_id
            );
            let cancelled = match native_cancel_worker(&repository, &cancel_claim)
                .cancel(cancel_worker_command)
                .await
                .unwrap()
            {
                CommandOutcome::Applied(record) => record,
                CommandOutcome::Replayed(_) => panic!("fresh cancellation outcome replayed"),
            };
            assert_eq!(
                cancelled.invocation.state,
                insight_platform_contracts::InvocationState::ReconciliationRequired
            );
        }
        (Err(_), Ok(CommandOutcome::Applied(completed))) => {
            assert_eq!(
                completed.invocation.state,
                insight_platform_contracts::InvocationState::Succeeded
            );
        }
        other => panic!("cancel/completed race did not have one winner: {other:?}"),
    }

    let final_quota_totals: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT count(*), COALESCE(sum(reserved_value), 0)::bigint,
               (SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id = $1)
        FROM insight_platform.quota_accounts WHERE tenant_id = $1
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    // The deterministic cancellation path contributes one two-line reservation and one two-line
    // terminal settlement in addition to the original fixture's twenty-four ledger facts.
    assert_eq!(final_quota_totals, (2, 0, 28));
    run_native_worker_process_recovery(&pool, &repository, &fixture).await;
    run_plan_capability_owner(&pool, &repository, &fixture).await;
    run_remote_http_worker_process_recovery(&pool, &repository, &fixture).await;
}

async fn run_native_worker_process_recovery(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
) {
    let Ok(binary) = std::env::var("PLATFORM_CAPABILITY_NATIVE_WORKER_BIN") else {
        eprintln!(
            "PLATFORM_CAPABILITY_NATIVE_WORKER_BIN is unset; process recovery fixture skipped"
        );
        return;
    };
    let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL").unwrap();
    let admitted = match execute_admit(
        repository,
        command_for_node(fixture, &fixture.process_recovery_node_id, 0x900),
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("process recovery admission replayed"),
    };
    let job_id = id(ResourceKind::Job, 0x910);
    match execute_prepare(
        repository,
        PrepareCapabilityDispatch {
            audit: audit(
                &fixture.tenant_id,
                &fixture.principal_id,
                PrincipalKind::AgentRunner,
                0x911,
                '7',
                '8',
            ),
            invocation_id: admitted.invocation_id.clone(),
            expected_invocation_version: admitted.version,
            job_id: job_id.clone(),
            scheduled_at: Utc::now() - Duration::seconds(1),
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(_) => {}
        CommandOutcome::Replayed(_) => panic!("process recovery prepare replayed"),
    }

    let config = json!({
        "schema_version": 1,
        "observability_listen_address": reserve_loopback_address(),
        "worker_manifest": {
            "manifest_version": WORKER_MANIFEST_VERSION,
            "worker_role": "capability.native",
            "work_class": "capability_native",
            "adapter_runtime_digest": native_adapter_runtime_digest(),
            "protocol_version": WORKER_PROTOCOL_VERSION,
            "max_concurrency": 2,
            "critical_control_reserved_slots": 1
        },
        "installed_adapters": [{
            "adapter_id": "builtin.echo",
            "adapter_version": "1.0.0",
            "module_digest": builtin_echo_module_digest(),
            "entrypoint_id": "echo.inline"
        }],
        "database": {
            "business_max_connections": 2,
            "critical_control_max_connections": 2,
            "process_connection_budget": 4,
            "acquire_timeout_milliseconds": 1000
        },
        "timing": {
            "initial_scan_delay_milliseconds": 500,
            "receipt_ttl_milliseconds": 60000,
            "safety_scan_milliseconds": 20,
            "claim_failure_backoff_milliseconds": 5,
            "drain_grace_milliseconds": 1000
        }
    });
    let config_digest = canonical_digest(&config).unwrap();
    let config_path = PathBuf::from(format!(
        "/tmp/platform-capability-native-worker-{}.json",
        std::process::id()
    ));
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let spawn_worker = || {
        std::process::Command::new(&binary)
            .env("PLATFORM_CAPABILITY_NATIVE_WORKER_CONFIG", &config_path)
            .env(
                "PLATFORM_CAPABILITY_NATIVE_WORKER_CONFIG_DIGEST",
                &config_digest,
            )
            .env(
                "PLATFORM_CAPABILITY_NATIVE_WORKER_DATABASE_URL",
                &database_url,
            )
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut first = spawn_worker();
    let mut startup = String::new();
    std::io::BufRead::read_line(
        &mut std::io::BufReader::new(first.stderr.take().unwrap()),
        &mut startup,
    )
    .unwrap();
    assert!(
        startup.contains("platform-capability-native-worker started"),
        "{startup}"
    );
    sqlx::query(
        r#"
        CREATE FUNCTION insight_platform.test_pause_native_capability_commit()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.state = 'running' AND NEW.state = 'succeeded'
             AND NEW.work_class = 'capability_native' THEN
            PERFORM pg_sleep(30);
          END IF;
          RETURN NEW;
        END
        $$
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        CREATE TRIGGER test_pause_native_capability_commit
        BEFORE UPDATE ON insight_platform.jobs
        FOR EACH ROW EXECUTE FUNCTION insight_platform.test_pause_native_capability_commit()
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
    wait_for_database_state(pool, &job_id, "running", StdDuration::from_secs(10)).await;
    first.kill().unwrap();
    first.wait().unwrap();
    let reservation_id: String = sqlx::query_scalar(
        "SELECT quota_reservation_id FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query("DROP TRIGGER test_pause_native_capability_commit ON insight_platform.jobs")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION insight_platform.test_pause_native_capability_commit()")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let mut second = spawn_worker();
    wait_for_invocation_state(
        pool,
        &admitted.invocation_id,
        "reconciliation_required",
        StdDuration::from_secs(10),
    )
    .await;
    second.kill().unwrap();
    second.wait().unwrap();
    let reservation_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id = $1 AND correlation_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(reservation_id)
    .fetch_one(pool)
    .await
    .unwrap();
    // The process generates its reservation identity, so assert the current Job was fully
    // released rather than coupling the fixture to an opaque process-generated ID.
    let released: (String, Option<String>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT state, quota_reservation_id, lease_expires_at FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(released.0, "reconciliation_required");
    assert!(released.1.is_none());
    assert!(released.2.is_none());
    assert_eq!(reservation_count, 4);
    std::fs::remove_file(config_path).unwrap();
}

struct RemoteHttpProcessFixture {
    config: serde_json::Value,
    manifest_digest: Sha256Digest,
    node_id: ResourceId,
    grpc_deployment: ExactDeploymentRef,
    mcp_deployment: ExactDeploymentRef,
    mcp_authorization_binding_id: ResourceId,
}

struct RemoteMcpFacts {
    deployment: ExactDeploymentRef,
    authorization_binding_id: ResourceId,
    snapshot: McpDiscoverySnapshot,
    auth_policy: ExactVersionRef,
    protocol_policy: ExactVersionRef,
    tool_contract: McpToolCapabilityContract,
}

async fn seed_remote_mcp_facts(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
    package: &AuthoringPackage,
    validation: &ValidationSummary,
    remote_policies: &[ExactVersionRef],
) -> RemoteMcpFacts {
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let policy_resource = id(ResourceKind::Policy, 0x980);
    let server_resource = id(ResourceKind::McpServer, 0x983);
    insert_resource(
        pool,
        &fixture.tenant_id,
        &policy_resource,
        RegistryResourceKind::Policy,
        &fixture.principal_id,
    )
    .await;
    insert_resource(
        pool,
        &fixture.tenant_id,
        &server_resource,
        RegistryResourceKind::McpServer,
        &fixture.principal_id,
    )
    .await;

    let protocol = mcp_protocol_document();
    let protocol_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x981),
        protocol.canonical_digest().unwrap(),
    )
    .unwrap();
    let auth_document = McpAuthPolicyDocument {
        schema_version: 1,
        issuer: mcp_oauth_endpoint("auth.fixture.test", "/"),
        authorization_endpoint: mcp_oauth_endpoint("auth.fixture.test", "/oauth/authorize"),
        token_endpoint: mcp_oauth_endpoint("auth.fixture.test", "/oauth/token"),
        client_id: "insight-platform".to_owned(),
        client_authentication: McpOAuthClientAuthenticationKind::None,
        client_credential_purpose: None,
        pkce_secret_provider_id: id(ResourceKind::SecretProvider, 0x987),
        token_secret_provider_id: id(ResourceKind::SecretProvider, 0x987),
        redirect_uri: mcp_oauth_endpoint("platform.fixture.test", "/v1/mcp/oauth/callback"),
        resource_indicator: mcp_oauth_endpoint("mcp.fixture.test", "/mcp"),
        allowed_scopes: vec!["tools.call".to_owned(), "tools.read".to_owned()],
        maximum_token_response_bytes: 65_536,
        connect_timeout_milliseconds: 5_000,
        total_timeout_milliseconds: 30_000,
        maximum_clock_skew_seconds: 60,
    };
    let auth_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x982),
        auth_document.canonical_digest().unwrap(),
    )
    .unwrap();
    for (revision, revision_number, kind, rules_digest, mcp_protocol, mcp_auth) in [
        (
            protocol_policy.clone(),
            1,
            PolicyKind::Protocol,
            protocol_policy.semantic_digest.clone(),
            Some(protocol.clone()),
            None,
        ),
        (
            auth_policy.clone(),
            2,
            PolicyKind::McpAuth,
            auth_policy.semantic_digest.clone(),
            None,
            Some(auth_document),
        ),
    ] {
        insert_version(
            pool,
            &fixture.tenant_id,
            &policy_resource,
            &revision,
            revision_number,
            &fixture.principal_id,
            PublishedVersionPayload {
                document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                    authoring_package: package.clone(),
                    contract_digest: digest('4'),
                    dependency_versions: vec![],
                    policy_versions: vec![],
                    policy_kind: kind,
                    rules_digest,
                    selection: None,
                    scheduling: None,
                    retention: None,
                    model_safety: None,
                    model_budget: None,
                    model_public_projection: None,
                    mcp_protocol,
                    mcp_auth: mcp_auth.map(Box::new),
                    sandbox_isolation: None,
                    sandbox_resource: None,
                    sandbox_network: None,
                    sandbox_artifact_io: None,
                    sandbox_secret_resolution: None,
                })),
                validation: validation.clone(),
            },
        )
        .await;
    }

    let token_purpose = "mcp.oauth.token".parse::<SecretPurpose>().unwrap();
    let server_revision =
        ExactVersionRef::new(id(ResourceKind::McpServerRevision, 0x984), digest('6')).unwrap();
    insert_version(
        pool,
        &fixture.tenant_id,
        &server_resource,
        &server_revision,
        1,
        &fixture.principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::McpServer(McpServerResourceSpec {
                authoring_package: package.clone(),
                contract_digest: digest('7'),
                dependency_versions: vec![],
                policy_versions: vec![],
                transport: insight_platform_contracts::McpTransportKind::StreamableHttp,
                protocol_policy: protocol_policy.clone(),
                deployment_credential_requirements: vec![],
                authorization_credential_purpose: Some(token_purpose.clone()),
                limits: mcp_server_limits(),
            }),
            validation: validation.clone(),
        },
    )
    .await;

    let endpoint = auth_document_resource_endpoint();
    let mcp_closure = insight_platform_contracts::McpDeploymentClosure {
        server_revision: server_revision.clone(),
        server_identity_digest: endpoint.canonical_digest().unwrap(),
        transport: McpTransportBinding::StreamableHttp {
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint,
            network_policy: remote_policies[0].clone(),
            tls_policy: remote_policies[1].clone(),
        },
        protocol_policy: protocol_policy.clone(),
        trust_policy: remote_policies[2].clone(),
        auth_policy: Some(auth_policy.clone()),
        secret_bindings: vec![],
        conformance_evidence: package.artifact.clone(),
    };
    let mcp_payload = TypedPayload::new(1, &DeploymentClosure::McpServer(mcp_closure)).unwrap();
    let mcp_deployment_id = id(ResourceKind::McpDeployment, 0x985);
    insert_deployment(
        pool,
        &fixture.tenant_id,
        &mcp_deployment_id,
        &server_resource,
        &server_revision.revision_id,
        &fixture.principal_id,
        &mcp_payload,
    )
    .await;
    let deployment =
        ExactDeploymentRef::new(mcp_deployment_id, mcp_payload.digest.parse().unwrap()).unwrap();

    let token_binding = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 0x986),
        1,
        id(ResourceKind::SecretProvider, 0x987),
        token_purpose.clone(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: digest('8'),
        },
    )
    .unwrap();
    repository
        .create_secret_binding(NewSecretBinding {
            tenant_id: fixture.tenant_id.clone(),
            secret_binding_id: token_binding.secret_binding_id.clone(),
            purpose: token_purpose,
            provider_id: token_binding.provider_id.clone(),
            opaque_reference_ciphertext: vec![9, 8, 7],
            key_id: "fixture-key".to_owned(),
            reference_digest: digest('9'),
            payload: SecretBindingPayload {
                provider_id: token_binding.provider_id.clone(),
                resolution_policy: token_binding.resolution_policy.clone(),
            },
        })
        .await
        .unwrap();
    let authorization = McpAuthorizationBindingRecord::create(
        NewMcpAuthorizationBinding {
            tenant_id: fixture.tenant_id.clone(),
            authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 0x988),
            mcp_deployment: deployment.clone(),
            principal_kind: McpAuthorizationPrincipalKind::PerUser,
            principal_id: fixture.principal_id.clone(),
            principal_identity_kind: PrincipalKind::AgentRunner,
            principal_binding_generation: 1,
            audience_identity_digest: auth_document_resource_endpoint()
                .canonical_digest()
                .unwrap(),
            granted_scopes: vec!["tools.call".to_owned(), "tools.read".to_owned()],
            token_secret_binding: token_binding,
            expires_at: now + Duration::hours(4),
        },
        now,
    )
    .unwrap();
    let authorization_payload = TypedPayload::from_versioned(1, &authorization, 262_144).unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.resources (tenant_id, resource_id, resource_kind, lifecycle_state, gate_state, version, payload_schema_version, payload, payload_digest) VALUES ($1, $2, 'mcp_authorization_binding', 'active', 'enabled', 1, $3, $4, $5)",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(authorization.authorization_binding_id.to_string())
    .bind(authorization_payload.schema_version)
    .bind(&authorization_payload.value)
    .bind(&authorization_payload.digest)
    .execute(pool)
    .await
    .unwrap();

    let authorization_context = authorization.execution_context(now).unwrap();
    let source_operation_id = id(ResourceKind::McpOperation, 0x989);
    let snapshot_id = id(ResourceKind::McpDiscoverySnapshot, 0x98b);
    let artifact_link_id = id(ResourceKind::ArtifactLink, 0x98c);
    let snapshot = McpDiscoverySnapshot::build(
        snapshot_id.clone(),
        deployment.clone(),
        server_revision.clone(),
        protocol_policy.clone(),
        authorization_context.canonical_digest.clone(),
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
        package.artifact.clone(),
        now - Duration::seconds(2),
        now + Duration::hours(2),
    )
    .unwrap();
    let admission = McpDiscoveryAdmission::build(NewMcpDiscoveryAdmission {
        operation_id: source_operation_id.clone(),
        job_id: id(ResourceKind::Job, 0x98a),
        tenant_id: fixture.tenant_id.clone(),
        mcp_deployment: deployment.clone(),
        server_revision,
        protocol_profile: protocol_policy.clone(),
        authorization_binding_id: authorization.authorization_binding_id.clone(),
        authorization_generation: authorization.generation,
        authorization_context_digest: authorization_context.canonical_digest.clone(),
        principal_id: fixture.principal_id.clone(),
        artifact_preallocation:
            insight_platform_mcp_host::McpDiscoveryArtifactPreallocation::build(
                package.artifact.artifact_id().clone(),
                id(ResourceKind::InternalBlob, 0x991),
                id(ResourceKind::Job, 0x992),
                artifact_link_id.clone(),
                snapshot_id.clone(),
                id(ResourceKind::QuotaLedgerEntry, 0x995),
            )
            .unwrap(),
        artifact_policy: insight_platform_mcp_host::McpDiscoveryArtifactPolicyClosure::build(
            insight_platform_mcp_host::NewMcpDiscoveryArtifactPolicyClosure {
                quota_account_id: id(ResourceKind::QuotaAccount, 0x996),
                maximum_bytes: 1_048_576,
                retention_policy_revision: ExactVersionRef::new(
                    id(ResourceKind::PolicyRevision, 0x997),
                    digest('7'),
                )
                .unwrap(),
                artifact_io_policy_revision: ExactVersionRef::new(
                    id(ResourceKind::PolicyRevision, 0x998),
                    digest('8'),
                )
                .unwrap(),
                scanner_contract_digest: digest('9'),
                ruleset_digest: digest('a'),
                evidence_ttl_milliseconds: 60_000,
                retry_backoff_milliseconds: 1_000,
                write_storage_binding_digest: digest('b'),
                encryption_domain_id: id(ResourceKind::EncryptionDomain, 0x999),
                retain_until: now + Duration::hours(3),
            },
        )
        .unwrap(),
        requested_at: now - Duration::seconds(3),
        deadline: now + Duration::hours(2),
    })
    .unwrap();
    let preallocation = admission.artifact_preallocation.clone();
    let mut transport_evidence = McpDiscoveryTransportEvidence {
        schema_version: 1,
        negotiated_version: snapshot.negotiated_version.clone(),
        negotiated_capabilities: snapshot.negotiated_capabilities.clone(),
        descriptor_digest: package.artifact.content_digest().clone(),
        descriptor_size_bytes: package.artifact.byte_length(),
        descriptor_count: 1,
        verification_job_id: preallocation.verification_job_id,
        artifact_id: preallocation.artifact_id,
        blob_id: preallocation.blob_id,
        observed_at: now - Duration::seconds(2),
        expires_at: now + Duration::hours(1),
        canonical_digest: digest('0'),
    };
    let mut evidence_value = serde_json::to_value(&transport_evidence).unwrap();
    evidence_value
        .as_object_mut()
        .unwrap()
        .remove("canonical_digest");
    transport_evidence.canonical_digest =
        canonical_digest(&evidence_value).unwrap().parse().unwrap();
    let operation_payload = McpDiscoveryOperationPayload::pending(admission)
        .unwrap()
        .park_for_verification(transport_evidence)
        .unwrap()
        .complete(McpDiscoveryResultBinding {
            snapshot_id: snapshot_id.clone(),
            snapshot_digest: snapshot.canonical_digest.clone(),
            objects_artifact: package.artifact.clone(),
            artifact_link_id: artifact_link_id.clone(),
        })
        .unwrap();
    let operation_payload = TypedPayload::from_versioned(
        i32::try_from(operation_payload.schema_version).unwrap(),
        &operation_payload,
        1_048_576,
    )
    .unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.invocations (tenant_id, invocation_id, invocation_kind, owner_kind, owner_id, logical_key, deployment_id, state, payload_schema_version, payload, payload_digest, deadline, terminal_at, created_at, updated_at, trace_id) VALUES ($1, $2, 'mcp_discovery', 'mcp_operation', $2, 'remote-mcp-process-discovery', $3, 'succeeded', $4, $5, $6, $7, $8, $8, $8, $9)",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(source_operation_id.to_string())
    .bind(deployment.deployment_id.to_string())
    .bind(operation_payload.schema_version)
    .bind(operation_payload.value)
    .bind(operation_payload.digest)
    .bind(now + Duration::hours(2))
    .bind(now)
    .bind(insight_platform_contracts::TraceId::new().to_string())
    .execute(pool)
    .await
    .unwrap();
    let reference = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: package.artifact.artifact_id().clone(),
        owner_id: source_operation_id.clone(),
        reference_kind: ArtifactReferenceKind::Evidence,
        purpose: ArtifactPurpose::McpResource,
        created_by: fixture.principal_id.clone(),
    };
    let link_payload = TypedPayload::from_versioned(1, &reference, 262_144).unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.artifact_links (tenant_id, artifact_link_id, link_kind, owner_kind, owner_id, target_artifact_id, link_key_digest, state, payload_schema_version, payload, payload_digest) VALUES ($1, $2, 'reference', 'mcp_operation', $3, $4, $5, 'active', $6, $7, $8)",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(artifact_link_id.to_string())
    .bind(source_operation_id.to_string())
    .bind(package.artifact.artifact_id().to_string())
    .bind(reference.link_key_digest().unwrap().to_string())
    .bind(link_payload.schema_version)
    .bind(link_payload.value)
    .bind(link_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let snapshot_record = McpDiscoverySnapshotRecord::build(NewMcpDiscoverySnapshotRecord {
        tenant_id: fixture.tenant_id.clone(),
        source_operation_id,
        artifact_link_id,
        snapshot: snapshot.clone(),
        completed_at: now,
    })
    .unwrap();
    let snapshot_payload = TypedPayload::from_versioned(1, &snapshot_record, 1_048_576).unwrap();
    sqlx::query(
        "INSERT INTO insight_platform.resources (tenant_id, resource_id, resource_kind, lifecycle_state, gate_state, version, payload_schema_version, payload, payload_digest) VALUES ($1, $2, 'mcp_discovery_snapshot', 'active', 'enabled', 1, $3, $4, $5)",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(snapshot_id.to_string())
    .bind(snapshot_payload.schema_version)
    .bind(snapshot_payload.value)
    .bind(snapshot_payload.digest)
    .execute(pool)
    .await
    .unwrap();

    RemoteMcpFacts {
        deployment,
        authorization_binding_id: authorization.authorization_binding_id,
        snapshot: snapshot.clone(),
        auth_policy,
        protocol_policy: protocol_policy.clone(),
        tool_contract: McpToolCapabilityContract {
            remote_tool_name: "fixture_lookup".to_owned(),
            remote_input_schema_digest: fixture.input_schema_digest.clone(),
            output_mapping_digest: builtin_json_mcp_output_mapping_digest(),
            protocol_profile: protocol_policy,
            discovery_semantic_evidence_digest: snapshot.objects_digest,
            supports_task: false,
            supports_progress: true,
        },
    }
}

fn auth_document_resource_endpoint() -> CanonicalHttpEndpoint {
    CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: "mcp.fixture.test".to_owned(),
        port: 443,
        base_path: "/mcp".to_owned(),
    }
}

async fn prepare_remote_http_process_fixture(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
    egress_endpoint: &str,
) -> RemoteHttpProcessFixture {
    let package = AuthoringPackage {
        artifact: ArtifactRef::new(
            id(ResourceKind::Artifact, 0x20),
            digest('0'),
            2,
            "application/json",
            DataClassification::Internal,
            Some("fixture.json".to_owned()),
        )
        .unwrap(),
        manifest_digest: digest('1'),
    };
    let validation = ValidationSummary {
        validator_digest: digest('2'),
        validated_draft_digest: digest('3'),
        dependency_closure_digest: digest('4'),
        security_evidence_digest: digest('5'),
        warnings: vec![],
    };
    let mut remote_policies = Vec::new();
    for (offset, kind, semantic) in [
        (0x922, PolicyKind::Network, '1'),
        (0x924, PolicyKind::Tls, '2'),
        (0x926, PolicyKind::Trust, '3'),
    ] {
        let resource_id = id(ResourceKind::Policy, offset);
        let revision_id = id(ResourceKind::PolicyRevision, offset + 1);
        insert_resource(
            pool,
            &fixture.tenant_id,
            &resource_id,
            RegistryResourceKind::Policy,
            &fixture.principal_id,
        )
        .await;
        let exact = ExactVersionRef::new(revision_id, digest(semantic)).unwrap();
        insert_version(
            pool,
            &fixture.tenant_id,
            &resource_id,
            &exact,
            1,
            &fixture.principal_id,
            PublishedVersionPayload {
                document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                    authoring_package: package.clone(),
                    contract_digest: digest('4'),
                    dependency_versions: vec![],
                    policy_versions: vec![],
                    policy_kind: kind,
                    rules_digest: digest('5'),
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
                })),
                validation: validation.clone(),
            },
        )
        .await;
        remote_policies.push(exact);
    }

    let http_contract = HttpCapabilityContract {
        method: HttpCapabilityMethod::Post,
        protocol_contract_digest: builtin_json_http_protocol_digest(),
        request_mapping_digest: builtin_json_http_request_mapping_digest(),
        response_mapping_digest: builtin_json_http_response_mapping_digest(),
        error_mapping_digest: builtin_json_http_error_mapping_digest(),
        idempotency_header: None,
    };
    let backend_contract = CapabilityBackendContract::Http(http_contract.clone());
    let descriptor_digest = backend_contract.descriptor_digest().unwrap();
    let grpc_contract = GrpcCapabilityContract {
        protobuf_contract_digest: builtin_json_grpc_protocol_digest(),
        service_name: "insight.fixture.v1.Lookup".to_owned(),
        method_name: "Get".to_owned(),
        request_mapping_digest: builtin_json_grpc_request_mapping_digest(),
        response_mapping_digest: builtin_json_grpc_response_mapping_digest(),
        error_mapping_digest: builtin_json_grpc_error_mapping_digest(),
        idempotency_metadata_key: None,
    };
    let grpc_backend_contract = CapabilityBackendContract::Grpc(grpc_contract.clone());
    let grpc_descriptor_digest = grpc_backend_contract.descriptor_digest().unwrap();
    let mcp = seed_remote_mcp_facts(
        pool,
        repository,
        fixture,
        &package,
        &validation,
        &remote_policies,
    )
    .await;
    let mcp_backend_contract = CapabilityBackendContract::Mcp(mcp.tool_contract.clone());
    let mcp_descriptor_digest = mcp_backend_contract.descriptor_digest().unwrap();
    let installed_http = json!({
        "codec_id": "platform.json",
        "codec_version": "1.0.0",
        "module_digest": builtin_json_module_digest(),
        "worker_protocol_version": WORKER_PROTOCOL_VERSION,
        "descriptor_digest": descriptor_digest,
        "protocol_contract_digest": builtin_json_http_protocol_digest(),
        "request_mapping_digest": builtin_json_http_request_mapping_digest(),
        "response_mapping_digest": builtin_json_http_response_mapping_digest(),
        "error_mapping_digest": builtin_json_http_error_mapping_digest()
    });
    let installed_grpc = json!({
        "codec_id": "platform.json",
        "codec_version": "1.0.0",
        "module_digest": builtin_json_module_digest(),
        "worker_protocol_version": WORKER_PROTOCOL_VERSION,
        "descriptor_digest": grpc_descriptor_digest,
        "protobuf_contract_digest": builtin_json_grpc_protocol_digest(),
        "service_name": grpc_contract.service_name.clone(),
        "method_name": grpc_contract.method_name.clone(),
        "request_mapping_digest": builtin_json_grpc_request_mapping_digest(),
        "response_mapping_digest": builtin_json_grpc_response_mapping_digest(),
        "error_mapping_digest": builtin_json_grpc_error_mapping_digest()
    });
    let installed_mcp = json!({
        "codec_id": "platform.json",
        "codec_version": "1.0.0",
        "module_digest": builtin_json_module_digest(),
        "worker_protocol_version": WORKER_PROTOCOL_VERSION,
        "descriptor_digest": mcp_descriptor_digest,
        "remote_tool_name": mcp.tool_contract.remote_tool_name.clone(),
        "remote_input_schema_digest": mcp.tool_contract.remote_input_schema_digest.clone(),
        "output_mapping_digest": builtin_json_mcp_output_mapping_digest(),
        "protocol_profile_id": mcp.protocol_policy.revision_id.clone(),
        "protocol_profile_digest": mcp.protocol_policy.semantic_digest.clone(),
        "discovery_semantic_evidence_digest": mcp.tool_contract.discovery_semantic_evidence_digest.clone()
    });
    let installed_closure = json!({
        "schema_version": 1,
        "http": [installed_http.clone()],
        "grpc": [installed_grpc.clone()],
        "mcp": [installed_mcp.clone()]
    });
    let adapter_runtime_digest: Sha256Digest = canonical_digest(&installed_closure)
        .unwrap()
        .parse()
        .unwrap();
    let worker_manifest = json!({
        "manifest_version": WORKER_MANIFEST_VERSION,
        "worker_role": "capability.remote",
        "work_class": "capability_remote",
        "adapter_runtime_digest": adapter_runtime_digest,
        "protocol_version": WORKER_PROTOCOL_VERSION,
        "max_concurrency": 2,
        "critical_control_reserved_slots": 1
    });
    let manifest_digest: Sha256Digest =
        canonical_digest(&worker_manifest).unwrap().parse().unwrap();
    let codec = InstalledCapabilityCodecRef {
        schema_version: INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION,
        backend_kind: CapabilityBackendKind::Http,
        codec_id: "platform.json".to_owned(),
        codec_version: "1.0.0".to_owned(),
        module_digest: builtin_json_module_digest(),
        worker_protocol_version: WORKER_PROTOCOL_VERSION,
        descriptor_digest,
    };
    let implementation_exact = ExactVersionRef::new(
        id(ResourceKind::CapabilityImplementationRevision, 0x920),
        digest('9'),
    )
    .unwrap();
    insert_version(
        pool,
        &fixture.tenant_id,
        &id(ResourceKind::CapabilityImplementation, 0x18),
        &implementation_exact,
        2,
        &fixture.principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::CapabilityImplementation(
                CapabilityImplementationResourceSpec {
                    authoring_package: package.clone(),
                    contract_digest: digest('a'),
                    dependency_versions: vec![],
                    policy_versions: remote_policies.clone(),
                    interface_revision: ExactVersionRef::new(
                        id(ResourceKind::CapabilityInterfaceRevision, 0x17),
                        digest('e'),
                    )
                    .unwrap(),
                    backend_kind: CapabilityBackendKind::Http,
                    backend_contract: backend_contract.clone(),
                    backend_contract_digest: backend_contract.canonical_digest().unwrap(),
                    credential_requirements: vec![],
                    backend_limits: CapabilityBackendLimits {
                        maximum_request_bytes: 1_048_576,
                        maximum_response_bytes: 1_048_576,
                        maximum_diagnostic_bytes: 65_536,
                        connect_timeout_milliseconds: 100,
                        first_byte_timeout_milliseconds: 500,
                        idle_timeout_milliseconds: 1_000,
                        total_timeout_milliseconds: 5_000,
                    },
                    features: CapabilityBackendFeatures {
                        deferred: false,
                        input_required: false,
                        callback: false,
                        poll: false,
                        progress: true,
                        cancellation: true,
                        max_remote_state_bytes: 0,
                        max_poll_count: 0,
                    },
                },
            ),
            validation: validation.clone(),
        },
    )
    .await;
    let endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: "fixture.remote.test".to_owned(),
        port: 443,
        base_path: "/v1/execute".to_owned(),
    };
    let capability_closure = CapabilityDeploymentClosure {
        implementation: implementation_exact,
        interface: ExactVersionRef::new(
            id(ResourceKind::CapabilityInterfaceRevision, 0x17),
            digest('e'),
        )
        .unwrap(),
        backend: CapabilityBackendBinding::Http {
            codec,
            worker_manifest_digest: manifest_digest.clone(),
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint,
            network_policy: remote_policies[0].clone(),
            tls_policy: remote_policies[1].clone(),
            trust_policy: remote_policies[2].clone(),
        },
        secret_bindings: vec![],
        policies: vec![fixture.policy.clone()],
        conformance_evidence: package.artifact.clone(),
    };
    capability_closure.validate().unwrap();
    let capability_payload = TypedPayload::new(
        1,
        &DeploymentClosure::CapabilityInterface(capability_closure),
    )
    .unwrap();
    let deployment_id = id(ResourceKind::CapabilityDeployment, 0x921);
    insert_deployment(
        pool,
        &fixture.tenant_id,
        &deployment_id,
        &id(ResourceKind::CapabilityInterface, 0x16),
        &id(ResourceKind::CapabilityInterfaceRevision, 0x17),
        &fixture.principal_id,
        &capability_payload,
    )
    .await;
    let remote_deployment =
        ExactDeploymentRef::new(deployment_id, capability_payload.digest.parse().unwrap()).unwrap();

    let grpc_implementation_exact = ExactVersionRef::new(
        id(ResourceKind::CapabilityImplementationRevision, 0x933),
        digest('b'),
    )
    .unwrap();
    insert_version(
        pool,
        &fixture.tenant_id,
        &id(ResourceKind::CapabilityImplementation, 0x18),
        &grpc_implementation_exact,
        3,
        &fixture.principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::CapabilityImplementation(
                CapabilityImplementationResourceSpec {
                    authoring_package: AuthoringPackage {
                        artifact: ArtifactRef::new(
                            id(ResourceKind::Artifact, 0x20),
                            digest('0'),
                            2,
                            "application/json",
                            DataClassification::Internal,
                            Some("fixture.json".to_owned()),
                        )
                        .unwrap(),
                        manifest_digest: digest('1'),
                    },
                    contract_digest: digest('c'),
                    dependency_versions: vec![],
                    policy_versions: remote_policies.clone(),
                    interface_revision: ExactVersionRef::new(
                        id(ResourceKind::CapabilityInterfaceRevision, 0x17),
                        digest('e'),
                    )
                    .unwrap(),
                    backend_kind: CapabilityBackendKind::Grpc,
                    backend_contract: grpc_backend_contract.clone(),
                    backend_contract_digest: grpc_backend_contract.canonical_digest().unwrap(),
                    credential_requirements: vec![],
                    backend_limits: CapabilityBackendLimits {
                        maximum_request_bytes: 1_048_576,
                        maximum_response_bytes: 1_048_576,
                        maximum_diagnostic_bytes: 65_536,
                        connect_timeout_milliseconds: 100,
                        first_byte_timeout_milliseconds: 500,
                        idle_timeout_milliseconds: 1_000,
                        total_timeout_milliseconds: 5_000,
                    },
                    features: CapabilityBackendFeatures {
                        deferred: false,
                        input_required: false,
                        callback: false,
                        poll: false,
                        progress: true,
                        cancellation: true,
                        max_remote_state_bytes: 0,
                        max_poll_count: 0,
                    },
                },
            ),
            validation: ValidationSummary {
                validator_digest: digest('2'),
                validated_draft_digest: digest('3'),
                dependency_closure_digest: digest('4'),
                security_evidence_digest: digest('5'),
                warnings: vec![],
            },
        },
    )
    .await;
    let grpc_codec = InstalledCapabilityCodecRef {
        schema_version: INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION,
        backend_kind: CapabilityBackendKind::Grpc,
        codec_id: "platform.json".to_owned(),
        codec_version: "1.0.0".to_owned(),
        module_digest: builtin_json_module_digest(),
        worker_protocol_version: WORKER_PROTOCOL_VERSION,
        descriptor_digest: grpc_descriptor_digest,
    };
    let grpc_endpoint = CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: "fixture.grpc.remote.test".to_owned(),
        port: 443,
        base_path: "/insight.fixture.v1.Lookup/Get".to_owned(),
    };
    let grpc_closure = CapabilityDeploymentClosure {
        implementation: grpc_implementation_exact,
        interface: ExactVersionRef::new(
            id(ResourceKind::CapabilityInterfaceRevision, 0x17),
            digest('e'),
        )
        .unwrap(),
        backend: CapabilityBackendBinding::Grpc {
            codec: grpc_codec,
            worker_manifest_digest: manifest_digest.clone(),
            endpoint_identity_digest: grpc_endpoint.canonical_digest().unwrap(),
            endpoint: grpc_endpoint,
            network_policy: remote_policies[0].clone(),
            tls_policy: remote_policies[1].clone(),
            trust_policy: remote_policies[2].clone(),
        },
        secret_bindings: vec![],
        policies: vec![fixture.policy.clone()],
        conformance_evidence: ArtifactRef::new(
            id(ResourceKind::Artifact, 0x20),
            digest('0'),
            2,
            "application/json",
            DataClassification::Internal,
            Some("fixture.json".to_owned()),
        )
        .unwrap(),
    };
    grpc_closure.validate().unwrap();
    let grpc_payload =
        TypedPayload::new(1, &DeploymentClosure::CapabilityInterface(grpc_closure)).unwrap();
    let grpc_deployment_id = id(ResourceKind::CapabilityDeployment, 0x934);
    insert_deployment(
        pool,
        &fixture.tenant_id,
        &grpc_deployment_id,
        &id(ResourceKind::CapabilityInterface, 0x16),
        &id(ResourceKind::CapabilityInterfaceRevision, 0x17),
        &fixture.principal_id,
        &grpc_payload,
    )
    .await;
    let grpc_deployment =
        ExactDeploymentRef::new(grpc_deployment_id, grpc_payload.digest.parse().unwrap()).unwrap();

    let mcp_implementation_exact = ExactVersionRef::new(
        id(ResourceKind::CapabilityImplementationRevision, 0x990),
        digest('e'),
    )
    .unwrap();
    insert_version(
        pool,
        &fixture.tenant_id,
        &id(ResourceKind::CapabilityImplementation, 0x18),
        &mcp_implementation_exact,
        4,
        &fixture.principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::CapabilityImplementation(
                CapabilityImplementationResourceSpec {
                    authoring_package: package.clone(),
                    contract_digest: digest('f'),
                    dependency_versions: vec![],
                    policy_versions: vec![mcp.protocol_policy.clone(), mcp.auth_policy.clone()],
                    interface_revision: ExactVersionRef::new(
                        id(ResourceKind::CapabilityInterfaceRevision, 0x17),
                        digest('e'),
                    )
                    .unwrap(),
                    backend_kind: CapabilityBackendKind::Mcp,
                    backend_contract: mcp_backend_contract.clone(),
                    backend_contract_digest: mcp_backend_contract.canonical_digest().unwrap(),
                    credential_requirements: vec![],
                    backend_limits: CapabilityBackendLimits {
                        maximum_request_bytes: 1_048_576,
                        maximum_response_bytes: 1_048_576,
                        maximum_diagnostic_bytes: 65_536,
                        connect_timeout_milliseconds: 100,
                        first_byte_timeout_milliseconds: 500,
                        idle_timeout_milliseconds: 1_000,
                        total_timeout_milliseconds: 5_000,
                    },
                    features: CapabilityBackendFeatures {
                        deferred: false,
                        input_required: false,
                        callback: false,
                        poll: false,
                        progress: true,
                        cancellation: true,
                        max_remote_state_bytes: 0,
                        max_poll_count: 0,
                    },
                },
            ),
            validation: validation.clone(),
        },
    )
    .await;
    let mcp_codec = InstalledCapabilityCodecRef {
        schema_version: INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION,
        backend_kind: CapabilityBackendKind::Mcp,
        codec_id: "platform.json".to_owned(),
        codec_version: "1.0.0".to_owned(),
        module_digest: builtin_json_module_digest(),
        worker_protocol_version: WORKER_PROTOCOL_VERSION,
        descriptor_digest: mcp_descriptor_digest,
    };
    let mcp_capability_closure = CapabilityDeploymentClosure {
        implementation: mcp_implementation_exact,
        interface: ExactVersionRef::new(
            id(ResourceKind::CapabilityInterfaceRevision, 0x17),
            digest('e'),
        )
        .unwrap(),
        backend: CapabilityBackendBinding::Mcp {
            codec: mcp_codec,
            worker_manifest_digest: manifest_digest.clone(),
            mcp_deployment: mcp.deployment.clone(),
            discovery_snapshot_id: mcp.snapshot.snapshot_id.clone(),
            discovery_snapshot_digest: mcp.snapshot.canonical_digest.clone(),
            authorization_policy: mcp.auth_policy.clone(),
        },
        secret_bindings: vec![],
        policies: vec![fixture.policy.clone()],
        conformance_evidence: package.artifact.clone(),
    };
    mcp_capability_closure.validate().unwrap();
    let mcp_capability_payload = TypedPayload::new(
        1,
        &DeploymentClosure::CapabilityInterface(mcp_capability_closure),
    )
    .unwrap();
    let mcp_capability_deployment_id = id(ResourceKind::CapabilityDeployment, 0x991);
    insert_deployment(
        pool,
        &fixture.tenant_id,
        &mcp_capability_deployment_id,
        &id(ResourceKind::CapabilityInterface, 0x16),
        &id(ResourceKind::CapabilityInterfaceRevision, 0x17),
        &fixture.principal_id,
        &mcp_capability_payload,
    )
    .await;
    let mcp_deployment = ExactDeploymentRef::new(
        mcp_capability_deployment_id,
        mcp_capability_payload.digest.parse().unwrap(),
    )
    .unwrap();

    for (account_id, scope_kind, scope_id, metric) in [
        (
            id(ResourceKind::QuotaAccount, 0x930),
            "tenant",
            fixture.tenant_id.clone(),
            QuotaDimension::WorkClassConcurrentOperations,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x931),
            "capability_deployment",
            remote_deployment.deployment_id.clone(),
            QuotaDimension::CapabilityConcurrentInvocations,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x935),
            "capability_deployment",
            grpc_deployment.deployment_id.clone(),
            QuotaDimension::CapabilityConcurrentInvocations,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x992),
            "capability_deployment",
            mcp_deployment.deployment_id.clone(),
            QuotaDimension::CapabilityConcurrentInvocations,
        ),
    ] {
        repository
            .create_quota_account(NewQuotaAccount {
                tenant_id: fixture.tenant_id.to_string(),
                quota_account_id: account_id.to_string(),
                scope_kind: scope_kind.to_owned(),
                scope_id: scope_id.to_string(),
                work_class: WorkClass::CapabilityRemote.as_str().to_owned(),
                metric: metric.as_str().to_owned(),
                limit_value: 8,
                payload: TypedPayload::new(1, &json!({"fixture": "remote-capability"})).unwrap(),
            })
            .await
            .unwrap();
    }

    let agent_closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(
            id(ResourceKind::AgentInterfaceRevision, 0x13),
            digest('9'),
        )
        .unwrap(),
        plan: ExactVersionRef::new(id(ResourceKind::AgentPlanRevision, 0x14), digest('a')).unwrap(),
        entry_node_id: "start".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
        slots: vec![FrozenSlotBinding {
            slot_id: "writer".to_owned(),
            requirement_digest: digest('8'),
            target: FrozenSlotTarget::Capability {
                candidates: vec![remote_deployment],
                selection_policy: fixture.selection_policy.clone(),
                tool_alias: Some("write".to_owned()),
            },
            binding_digest: digest('9'),
        }],
        policies: vec![],
        execution_profile: fixture.selection_policy.clone(),
    };
    let agent_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(agent_closure.clone())).unwrap();
    sqlx::query(
        "UPDATE insight_platform.deployments SET bindings_digest = $3, payload_schema_version = $4, bindings = $5 WHERE tenant_id = $1 AND deployment_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::AgentDeployment, 0x15).to_string())
    .bind(&agent_payload.digest)
    .bind(agent_payload.schema_version)
    .bind(&agent_payload.value)
    .execute(pool)
    .await
    .unwrap();
    let principal = PrincipalSnapshot::build(
        fixture.tenant_id.clone(),
        fixture.principal_id.clone(),
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![
            Permission::ApprovalRespond,
            Permission::CapabilityInvoke,
            Permission::InteractionRespond,
            Permission::RuntimeControl,
        ])
        .unwrap(),
        1,
        1,
        1,
    )
    .unwrap();
    let run_bindings = RunBindingsSnapshot::build(
        ExactDeploymentRef::new(
            id(ResourceKind::AgentDeployment, 0x15),
            agent_payload.digest.parse().unwrap(),
        )
        .unwrap(),
        principal,
        &agent_closure,
    )
    .unwrap();
    let bindings_payload = TypedPayload::from_versioned(1, &run_bindings, 1_048_576).unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET bindings_schema_version = $3, bindings = $4, bindings_digest = $5 WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(bindings_payload.schema_version)
    .bind(&bindings_payload.value)
    .bind(run_bindings.canonical_digest.to_string())
    .execute(pool)
    .await
    .unwrap();

    let node_id = id(ResourceKind::NodeExecution, 0x932);
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let node_payload = TypedPayload::new(1, &json!({"fixture": "remote-capability"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, logical_key, node_kind, state,
            generation, version, payload_schema_version, payload, payload_digest,
            deadline, started_at, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, 'node_execution', $4,
            'capability-plan', 9, 'remote-capability-process-recovery', 'capability_call', 'running',
            1, 1, $5, $6, $7, $8, $9, $9, $9
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(id(ResourceKind::ScopeInstance, 0x31).to_string())
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .bind(now + Duration::hours(1))
    .bind(now)
    .execute(pool)
    .await
    .unwrap();

    RemoteHttpProcessFixture {
        config: json!({
            "schema_version": 1,
            "observability_listen_address": reserve_loopback_address(),
            "worker_manifest": worker_manifest,
            "installed_http_codecs": [installed_http],
            "installed_grpc_codecs": [installed_grpc],
            "installed_mcp_codecs": [installed_mcp],
            "database": {
                "business_max_connections": 2,
                "critical_control_max_connections": 2,
                "process_connection_budget": 4,
                "acquire_timeout_milliseconds": 1000
            },
            "egress": {
                "endpoint": egress_endpoint,
                "tls_server_name": "egress.test",
                "connect_timeout_milliseconds": 1000,
                "request_timeout_milliseconds": 30000,
                "maximum_rpc_metadata_bytes": 65536,
                "maximum_rpc_payload_bytes": 1048576
            },
            "mcp_host": {
                "endpoint": "https://127.0.0.1:9/",
                "tls_server_name": "mcp.test",
                "connect_timeout_milliseconds": 1000,
                "request_timeout_milliseconds": 30000,
                "maximum_rpc_message_bytes": 1048576
            },
            "timing": {
                "initial_scan_delay_milliseconds": 500,
                "receipt_ttl_milliseconds": 60000,
                "safety_scan_milliseconds": 20,
                "claim_failure_backoff_milliseconds": 5,
                "drain_grace_milliseconds": 1000
            }
        }),
        manifest_digest,
        node_id,
        grpc_deployment,
        mcp_deployment,
        mcp_authorization_binding_id: mcp.authorization_binding_id,
    }
}

async fn run_remote_http_worker_process_recovery(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
) {
    let Ok(binary) = std::env::var("PLATFORM_CAPABILITY_REMOTE_WORKER_BIN") else {
        eprintln!(
            "PLATFORM_CAPABILITY_REMOTE_WORKER_BIN is unset; remote process recovery fixture skipped"
        );
        return;
    };
    let mcp_host_binary = std::env::var("PLATFORM_MCP_HOST_BIN").ok();
    let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL").unwrap();
    let tls = process_mtls_fixture();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = incoming.local_addr().unwrap();
    let http = Arc::new(CountingHttpTransport {
        calls: AtomicUsize::new(0),
    });
    let grpc = Arc::new(CountingGrpcTransport {
        calls: AtomicUsize::new(0),
    });
    let mcp = Arc::new(CountingMcpTransport {
        calls: AtomicUsize::new(0),
        output_schema_digest: fixture.output_schema_digest.clone(),
    });
    let limits = EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap();
    let service = EgressBrokerServiceServer::new(
        EgressBrokerGrpcService::new(
            Arc::new(EmptyModelWire),
            Arc::clone(&http),
            Arc::clone(&grpc),
            limits,
        )
        .with_mcp_streamable_http(mcp.clone()),
    );
    let service =
        tonic::service::interceptor::InterceptedService::new(service, EgressCallerWorkloadIdentity);
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(
                        &tls.server_certificate_pem,
                        &tls.server_key_pem,
                    ))
                    .client_ca_root(Certificate::from_pem(tls.ca_pem.clone())),
            )
            .unwrap()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );
    let remote = prepare_remote_http_process_fixture(
        pool,
        repository,
        fixture,
        &format!("https://{address}/"),
    )
    .await;
    let run_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let mut admission = command_for_node(fixture, &remote.node_id, 0x940);
    admission.expected_run_version = u64::try_from(run_version).unwrap();
    let admitted = match execute_admit(repository, admission).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("remote process recovery admission replayed"),
    };
    let job_id = id(ResourceKind::Job, 0x950);
    match execute_prepare(
        repository,
        PrepareCapabilityDispatch {
            audit: audit(
                &fixture.tenant_id,
                &fixture.principal_id,
                PrincipalKind::AgentRunner,
                0x951,
                '1',
                '2',
            ),
            invocation_id: admitted.invocation_id.clone(),
            expected_invocation_version: admitted.version,
            job_id: job_id.clone(),
            scheduled_at: Utc::now() - Duration::seconds(1),
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(_) => {}
        CommandOutcome::Replayed(_) => panic!("remote process recovery prepare replayed"),
    }

    let temp_prefix = format!(
        "/tmp/platform-capability-remote-worker-{}",
        std::process::id()
    );
    let ca_path = PathBuf::from(format!("{temp_prefix}-ca.pem"));
    let cert_path = PathBuf::from(format!("{temp_prefix}-client.pem"));
    let key_path = PathBuf::from(format!("{temp_prefix}-client-key.pem"));
    std::fs::write(&ca_path, &tls.ca_pem).unwrap();
    std::fs::write(&cert_path, &tls.capability_certificate_pem).unwrap();
    std::fs::write(&key_path, &tls.capability_key_pem).unwrap();

    let write_config = |suffix: &str, config: &serde_json::Value| {
        let path = PathBuf::from(format!("{temp_prefix}-{suffix}.json"));
        std::fs::write(&path, serde_json::to_vec(config).unwrap()).unwrap();
        let digest = canonical_digest(config).unwrap();
        (path, digest)
    };
    let spawn_worker = |path: &PathBuf, config_digest: &str| {
        std::process::Command::new(&binary)
            .env("PLATFORM_CAPABILITY_REMOTE_WORKER_CONFIG", path)
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_CONFIG_DIGEST",
                config_digest,
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_DATABASE_URL",
                &database_url,
            )
            .env("PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CA_PATH", &ca_path)
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CERT_PATH",
                &cert_path,
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_KEY_PATH",
                &key_path,
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CA_PATH",
                &ca_path,
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CERT_PATH",
                &cert_path,
            )
            .env(
                "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_KEY_PATH",
                &key_path,
            )
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };

    let mut wrong_config = remote.config.clone();
    wrong_config["installed_http_codecs"][0]["descriptor_digest"] =
        serde_json::to_value(digest('f')).unwrap();
    let wrong_runtime: Sha256Digest = canonical_digest(&json!({
        "schema_version": 1,
        "http": wrong_config["installed_http_codecs"].clone(),
        "grpc": wrong_config["installed_grpc_codecs"].clone(),
        "mcp": wrong_config["installed_mcp_codecs"].clone()
    }))
    .unwrap()
    .parse()
    .unwrap();
    wrong_config["worker_manifest"]["adapter_runtime_digest"] =
        serde_json::to_value(wrong_runtime).unwrap();
    let (wrong_path, wrong_digest) = write_config("wrong", &wrong_config);
    let mut wrong = spawn_worker(&wrong_path, &wrong_digest);
    let wrong_startup = read_process_stderr_line(wrong.stderr.take().unwrap()).await;
    assert!(
        wrong_startup.contains("platform-capability-remote-worker started"),
        "{wrong_startup}"
    );
    tokio::time::sleep(StdDuration::from_millis(750)).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        "ready"
    );
    assert_eq!(http.calls.load(Ordering::SeqCst), 0);
    wrong.kill().unwrap();
    wrong.wait().unwrap();

    let (config_path, config_digest) = write_config("correct", &remote.config);
    assert_eq!(
        remote.manifest_digest,
        canonical_digest(&remote.config["worker_manifest"])
            .unwrap()
            .parse()
            .unwrap()
    );
    let mut first = spawn_worker(&config_path, &config_digest);
    let startup = read_process_stderr_line(first.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-capability-remote-worker started"),
        "{startup}"
    );
    sqlx::query(
        r#"
        CREATE FUNCTION insight_platform.test_pause_remote_capability_commit()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.state = 'running' AND NEW.state = 'succeeded'
             AND NEW.work_class = 'capability_remote' THEN
            PERFORM pg_sleep(30);
          END IF;
          RETURN NEW;
        END
        $$
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        CREATE TRIGGER test_pause_remote_capability_commit
        BEFORE UPDATE ON insight_platform.jobs
        FOR EACH ROW EXECUTE FUNCTION insight_platform.test_pause_remote_capability_commit()
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
    wait_for_atomic_count(&http.calls, 1, StdDuration::from_secs(10)).await;
    wait_for_database_state(pool, &job_id, "running", StdDuration::from_secs(10)).await;
    first.kill().unwrap();
    first.wait().unwrap();
    sqlx::query("DROP TRIGGER test_pause_remote_capability_commit ON insight_platform.jobs")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION insight_platform.test_pause_remote_capability_commit()")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let mut second = spawn_worker(&config_path, &config_digest);
    wait_for_invocation_state(
        pool,
        &admitted.invocation_id,
        "reconciliation_required",
        StdDuration::from_secs(10),
    )
    .await;
    assert_eq!(http.calls.load(Ordering::SeqCst), 1);
    second.kill().unwrap();
    second.wait().unwrap();

    let grpc_node_id = id(ResourceKind::NodeExecution, 0x936);
    rebind_run_capability_candidate(
        pool,
        fixture,
        remote.grpc_deployment.clone(),
        grpc_node_id.clone(),
        10,
        "remote-grpc-process-recovery",
    )
    .await;
    let run_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let mut grpc_admission = command_for_node(fixture, &grpc_node_id, 0x960);
    grpc_admission.expected_run_version = u64::try_from(run_version).unwrap();
    let grpc_admitted = match execute_admit(repository, grpc_admission).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("remote gRPC process recovery admission replayed"),
    };
    let grpc_job_id = id(ResourceKind::Job, 0x970);
    match execute_prepare(
        repository,
        PrepareCapabilityDispatch {
            audit: audit(
                &fixture.tenant_id,
                &fixture.principal_id,
                PrincipalKind::AgentRunner,
                0x971,
                '3',
                '4',
            ),
            invocation_id: grpc_admitted.invocation_id.clone(),
            expected_invocation_version: grpc_admitted.version,
            job_id: grpc_job_id.clone(),
            scheduled_at: Utc::now() - Duration::seconds(1),
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(_) => {}
        CommandOutcome::Replayed(_) => panic!("remote gRPC process recovery prepare replayed"),
    }

    let mut wrong_grpc_config = remote.config.clone();
    wrong_grpc_config["installed_grpc_codecs"][0]["descriptor_digest"] =
        serde_json::to_value(digest('f')).unwrap();
    let wrong_grpc_runtime: Sha256Digest = canonical_digest(&json!({
        "schema_version": 1,
        "http": wrong_grpc_config["installed_http_codecs"].clone(),
        "grpc": wrong_grpc_config["installed_grpc_codecs"].clone(),
        "mcp": wrong_grpc_config["installed_mcp_codecs"].clone()
    }))
    .unwrap()
    .parse()
    .unwrap();
    wrong_grpc_config["worker_manifest"]["adapter_runtime_digest"] =
        serde_json::to_value(wrong_grpc_runtime).unwrap();
    let (wrong_grpc_path, wrong_grpc_digest) = write_config("wrong-grpc", &wrong_grpc_config);
    let mut wrong_grpc = spawn_worker(&wrong_grpc_path, &wrong_grpc_digest);
    let wrong_grpc_startup = read_process_stderr_line(wrong_grpc.stderr.take().unwrap()).await;
    assert!(
        wrong_grpc_startup.contains("platform-capability-remote-worker started"),
        "{wrong_grpc_startup}"
    );
    tokio::time::sleep(StdDuration::from_millis(750)).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(grpc_job_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap(),
        "ready"
    );
    assert_eq!(grpc.calls.load(Ordering::SeqCst), 0);
    wrong_grpc.kill().unwrap();
    wrong_grpc.wait().unwrap();

    let mut grpc_first = spawn_worker(&config_path, &config_digest);
    let grpc_startup = read_process_stderr_line(grpc_first.stderr.take().unwrap()).await;
    assert!(
        grpc_startup.contains("platform-capability-remote-worker started"),
        "{grpc_startup}"
    );
    install_remote_commit_pause(pool).await;
    wait_for_atomic_count(&grpc.calls, 1, StdDuration::from_secs(10)).await;
    wait_for_database_state(pool, &grpc_job_id, "running", StdDuration::from_secs(10)).await;
    grpc_first.kill().unwrap();
    grpc_first.wait().unwrap();
    remove_remote_commit_pause(pool).await;
    expire_running_capability_job(pool, fixture, &grpc_job_id).await;
    let mut grpc_second = spawn_worker(&config_path, &config_digest);
    wait_for_invocation_state(
        pool,
        &grpc_admitted.invocation_id,
        "reconciliation_required",
        StdDuration::from_secs(10),
    )
    .await;
    assert_eq!(grpc.calls.load(Ordering::SeqCst), 1);
    grpc_second.kill().unwrap();
    grpc_second.wait().unwrap();

    if let Some(mcp_host_binary) = mcp_host_binary {
        let host_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let host_address = host_listener.local_addr().unwrap();
        drop(host_listener);
        let host_observability_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let host_observability_address = host_observability_listener.local_addr().unwrap();
        drop(host_observability_listener);
        let host_config_path = PathBuf::from(format!("{temp_prefix}-mcp-host.json"));
        let host_cert_path = PathBuf::from(format!("{temp_prefix}-mcp-host.pem"));
        let host_key_path = PathBuf::from(format!("{temp_prefix}-mcp-host-key.pem"));
        let host_egress_cert_path =
            PathBuf::from(format!("{temp_prefix}-mcp-host-egress-client.pem"));
        let host_egress_key_path =
            PathBuf::from(format!("{temp_prefix}-mcp-host-egress-client-key.pem"));
        std::fs::write(&host_cert_path, &tls.mcp_host_certificate_pem).unwrap();
        std::fs::write(&host_key_path, &tls.mcp_host_key_pem).unwrap();
        std::fs::write(&host_egress_cert_path, &tls.mcp_host_client_certificate_pem).unwrap();
        std::fs::write(&host_egress_key_path, &tls.mcp_host_client_key_pem).unwrap();
        let host_config = json!({
            "schema_version": 1,
            "listen_address": host_address.to_string(),
            "observability_listen_address": host_observability_address.to_string(),
            "tls_server_name": "mcp.test",
            "maximum_rpc_message_bytes": 1048576,
            "maximum_in_flight_requests": 8,
            "egress": {
                "endpoint": format!("https://{address}/"),
                "tls_server_name": "egress.test",
                "connect_timeout_milliseconds": 1000,
                "request_timeout_milliseconds": 30000,
                "maximum_rpc_metadata_bytes": 65536,
                "maximum_rpc_payload_bytes": 1048576
            },
            "drain_grace_milliseconds": 1000
        });
        std::fs::write(&host_config_path, serde_json::to_vec(&host_config).unwrap()).unwrap();
        let host_config_digest = canonical_digest(&host_config).unwrap();
        let mut host = std::process::Command::new(&mcp_host_binary)
            .env("PLATFORM_MCP_HOST_CONFIG", &host_config_path)
            .env("PLATFORM_MCP_HOST_CONFIG_DIGEST", &host_config_digest)
            .env("PLATFORM_MCP_HOST_SERVER_CLIENT_CA_PATH", &ca_path)
            .env("PLATFORM_MCP_HOST_SERVER_CERT_PATH", &host_cert_path)
            .env("PLATFORM_MCP_HOST_SERVER_KEY_PATH", &host_key_path)
            .env("PLATFORM_MCP_HOST_EGRESS_CA_PATH", &ca_path)
            .env("PLATFORM_MCP_HOST_EGRESS_CERT_PATH", &host_egress_cert_path)
            .env("PLATFORM_MCP_HOST_EGRESS_KEY_PATH", &host_egress_key_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let host_startup = read_process_stderr_line(host.stderr.take().unwrap()).await;
        assert!(
            host_startup.contains("platform-mcp-host started"),
            "{host_startup}"
        );

        let mcp_node_id = id(ResourceKind::NodeExecution, 0x993);
        rebind_run_capability_candidate(
            pool,
            fixture,
            remote.mcp_deployment.clone(),
            mcp_node_id.clone(),
            11,
            "remote-mcp-process-recovery",
        )
        .await;
        let run_version: i64 = sqlx::query_scalar(
            "SELECT version FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.run_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        let mut mcp_admission = command_for_node(fixture, &mcp_node_id, 0x994);
        mcp_admission.expected_run_version = u64::try_from(run_version).unwrap();
        mcp_admission.mcp_runtime = Some(McpCapabilityRuntimeRequest {
            mcp_operation_id: id(ResourceKind::McpOperation, 0x995),
            authorization_binding_id: remote.mcp_authorization_binding_id.clone(),
        });
        let mcp_admitted = match execute_admit(repository, mcp_admission).await.unwrap() {
            CommandOutcome::Applied(record) => record,
            CommandOutcome::Replayed(_) => panic!("remote MCP process recovery admission replayed"),
        };
        let mcp_job_id = id(ResourceKind::Job, 0x996);
        match execute_prepare(
            repository,
            PrepareCapabilityDispatch {
                audit: audit(
                    &fixture.tenant_id,
                    &fixture.principal_id,
                    PrincipalKind::AgentRunner,
                    0x997,
                    '5',
                    '6',
                ),
                invocation_id: mcp_admitted.invocation_id.clone(),
                expected_invocation_version: mcp_admitted.version,
                job_id: mcp_job_id.clone(),
                scheduled_at: Utc::now() - Duration::seconds(1),
            },
        )
        .await
        .unwrap()
        {
            CommandOutcome::Applied(_) => {}
            CommandOutcome::Replayed(_) => panic!("remote MCP process recovery prepare replayed"),
        }

        let mut mcp_config = remote.config.clone();
        mcp_config["mcp_host"]["endpoint"] =
            serde_json::Value::String(format!("https://{host_address}/"));
        let mut wrong_mcp_config = mcp_config.clone();
        wrong_mcp_config["installed_mcp_codecs"][0]["descriptor_digest"] =
            serde_json::to_value(digest('f')).unwrap();
        let wrong_mcp_runtime: Sha256Digest = canonical_digest(&json!({
            "schema_version": 1,
            "http": wrong_mcp_config["installed_http_codecs"].clone(),
            "grpc": wrong_mcp_config["installed_grpc_codecs"].clone(),
            "mcp": wrong_mcp_config["installed_mcp_codecs"].clone()
        }))
        .unwrap()
        .parse()
        .unwrap();
        wrong_mcp_config["worker_manifest"]["adapter_runtime_digest"] =
            serde_json::to_value(wrong_mcp_runtime).unwrap();
        let (wrong_mcp_path, wrong_mcp_digest) = write_config("wrong-mcp", &wrong_mcp_config);
        let mut wrong_mcp = spawn_worker(&wrong_mcp_path, &wrong_mcp_digest);
        let wrong_mcp_startup = read_process_stderr_line(wrong_mcp.stderr.take().unwrap()).await;
        assert!(
            wrong_mcp_startup.contains("platform-capability-remote-worker started"),
            "{wrong_mcp_startup}"
        );
        tokio::time::sleep(StdDuration::from_millis(750)).await;
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
            )
            .bind(fixture.tenant_id.to_string())
            .bind(mcp_job_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap(),
            "ready"
        );
        assert_eq!(mcp.calls.load(Ordering::SeqCst), 0);
        wrong_mcp.kill().unwrap();
        wrong_mcp.wait().unwrap();

        let (mcp_config_path, mcp_config_digest) = write_config("mcp", &mcp_config);
        let mut mcp_first = spawn_worker(&mcp_config_path, &mcp_config_digest);
        let mcp_startup = read_process_stderr_line(mcp_first.stderr.take().unwrap()).await;
        assert!(
            mcp_startup.contains("platform-capability-remote-worker started"),
            "{mcp_startup}"
        );
        install_remote_commit_pause(pool).await;
        wait_for_atomic_count(&mcp.calls, 1, StdDuration::from_secs(10)).await;
        wait_for_database_state(pool, &mcp_job_id, "running", StdDuration::from_secs(10)).await;
        mcp_first.kill().unwrap();
        mcp_first.wait().unwrap();
        remove_remote_commit_pause(pool).await;
        expire_running_capability_job(pool, fixture, &mcp_job_id).await;
        let mut mcp_second = spawn_worker(&mcp_config_path, &mcp_config_digest);
        wait_for_invocation_state(
            pool,
            &mcp_admitted.invocation_id,
            "reconciliation_required",
            StdDuration::from_secs(10),
        )
        .await;
        assert_eq!(mcp.calls.load(Ordering::SeqCst), 1);
        mcp_second.kill().unwrap();
        mcp_second.wait().unwrap();
        host.kill().unwrap();
        host.wait().unwrap();
        for path in [
            wrong_mcp_path,
            mcp_config_path,
            host_config_path,
            host_cert_path,
            host_key_path,
            host_egress_cert_path,
            host_egress_key_path,
        ] {
            std::fs::remove_file(path).unwrap();
        }
    } else {
        eprintln!("PLATFORM_MCP_HOST_BIN is unset; remote MCP process recovery fixture skipped");
    }

    let _ = shutdown_sender.send(());
    server.await.unwrap().unwrap();
    for path in [
        wrong_path,
        wrong_grpc_path,
        config_path,
        ca_path,
        cert_path,
        key_path,
    ] {
        std::fs::remove_file(path).unwrap();
    }
}

async fn wait_for_atomic_count(counter: &AtomicUsize, expected: usize, timeout: StdDuration) {
    let started = std::time::Instant::now();
    loop {
        if counter.load(Ordering::SeqCst) == expected {
            return;
        }
        assert!(
            started.elapsed() < timeout,
            "counter did not reach {expected}"
        );
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

async fn install_remote_commit_pause(pool: &PgPool) {
    sqlx::query(
        r#"
        CREATE FUNCTION insight_platform.test_pause_remote_capability_commit()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.state = 'running' AND NEW.state = 'succeeded'
             AND NEW.work_class = 'capability_remote' THEN
            PERFORM pg_sleep(30);
          END IF;
          RETURN NEW;
        END
        $$
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        CREATE TRIGGER test_pause_remote_capability_commit
        BEFORE UPDATE ON insight_platform.jobs
        FOR EACH ROW EXECUTE FUNCTION insight_platform.test_pause_remote_capability_commit()
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn remove_remote_commit_pause(pool: &PgPool) {
    sqlx::query("DROP TRIGGER test_pause_remote_capability_commit ON insight_platform.jobs")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION insight_platform.test_pause_remote_capability_commit()")
        .execute(pool)
        .await
        .unwrap();
}

async fn expire_running_capability_job(pool: &PgPool, fixture: &Fixture, job_id: &ResourceId) {
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn rebind_run_capability_candidate(
    pool: &PgPool,
    fixture: &Fixture,
    deployment: ExactDeploymentRef,
    node_id: ResourceId,
    ordinal: i32,
    logical_key: &str,
) {
    let agent_closure = AgentDeploymentClosure {
        interface: ExactVersionRef::new(
            id(ResourceKind::AgentInterfaceRevision, 0x13),
            digest('9'),
        )
        .unwrap(),
        plan: ExactVersionRef::new(id(ResourceKind::AgentPlanRevision, 0x14), digest('a')).unwrap(),
        entry_node_id: "start".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
        slots: vec![FrozenSlotBinding {
            slot_id: "writer".to_owned(),
            requirement_digest: digest('8'),
            target: FrozenSlotTarget::Capability {
                candidates: vec![deployment],
                selection_policy: fixture.selection_policy.clone(),
                tool_alias: Some("write".to_owned()),
            },
            binding_digest: digest('9'),
        }],
        policies: vec![],
        execution_profile: fixture.selection_policy.clone(),
    };
    let agent_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(agent_closure.clone())).unwrap();
    sqlx::query(
        "UPDATE insight_platform.deployments SET bindings_digest = $3, payload_schema_version = $4, bindings = $5 WHERE tenant_id = $1 AND deployment_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::AgentDeployment, 0x15).to_string())
    .bind(&agent_payload.digest)
    .bind(agent_payload.schema_version)
    .bind(&agent_payload.value)
    .execute(pool)
    .await
    .unwrap();
    let principal = PrincipalSnapshot::build(
        fixture.tenant_id.clone(),
        fixture.principal_id.clone(),
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![
            Permission::ApprovalRespond,
            Permission::CapabilityInvoke,
            Permission::InteractionRespond,
            Permission::RuntimeControl,
        ])
        .unwrap(),
        1,
        1,
        1,
    )
    .unwrap();
    let run_bindings = RunBindingsSnapshot::build(
        ExactDeploymentRef::new(
            id(ResourceKind::AgentDeployment, 0x15),
            agent_payload.digest.parse().unwrap(),
        )
        .unwrap(),
        principal,
        &agent_closure,
    )
    .unwrap();
    let bindings_payload = TypedPayload::from_versioned(1, &run_bindings, 1_048_576).unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET bindings_schema_version = $3, bindings = $4, bindings_digest = $5 WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(bindings_payload.schema_version)
    .bind(&bindings_payload.value)
    .bind(run_bindings.canonical_digest.to_string())
    .execute(pool)
    .await
    .unwrap();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let node_payload = TypedPayload::new(1, &json!({"fixture": logical_key})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, logical_key, node_kind, state,
            generation, version, payload_schema_version, payload, payload_digest,
            deadline, started_at, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, 'node_execution', $4,
            'capability-plan', $5, $6, 'capability_call', 'running',
            1, 1, $7, $8, $9, $10, $11, $11, $11
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(id(ResourceKind::ScopeInstance, 0x31).to_string())
    .bind(ordinal)
    .bind(logical_key)
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .bind(now + Duration::hours(1))
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
}

async fn read_process_stderr_line(stderr: std::process::ChildStderr) -> String {
    tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        std::io::BufRead::read_line(&mut std::io::BufReader::new(stderr), &mut line).unwrap();
        line
    })
    .await
    .unwrap()
}

async fn wait_for_database_state(
    pool: &PgPool,
    job_id: &ResourceId,
    expected: &str,
    timeout: StdDuration,
) {
    let started = std::time::Instant::now();
    loop {
        let state: Option<String> =
            sqlx::query_scalar("SELECT state FROM insight_platform.jobs WHERE job_id = $1")
                .bind(job_id.to_string())
                .fetch_optional(pool)
                .await
                .unwrap();
        if state.as_deref() == Some(expected) {
            return;
        }
        assert!(started.elapsed() < timeout, "Job did not reach {expected}");
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

async fn wait_for_invocation_state(
    pool: &PgPool,
    invocation_id: &ResourceId,
    expected: &str,
    timeout: StdDuration,
) {
    let started = std::time::Instant::now();
    loop {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM insight_platform.invocations WHERE invocation_id = $1",
        )
        .bind(invocation_id.to_string())
        .fetch_optional(pool)
        .await
        .unwrap();
        if state.as_deref() == Some(expected) {
            return;
        }
        assert!(
            started.elapsed() < timeout,
            "Invocation did not reach {expected}"
        );
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

fn capability_defer_mutations(base: u16) -> DeferOrchestrationCapabilityMutationIds {
    DeferOrchestrationCapabilityMutationIds {
        source: OrchestrationYieldMutationIds {
            receipt_id: id(ResourceKind::Receipt, base),
            quota_entry_ids: (0..MAX_ORCHESTRATION_QUOTA_LINES)
                .map(|offset| id(ResourceKind::QuotaLedgerEntry, base + 1 + offset as u16))
                .collect(),
            run_event_id: id(ResourceKind::Event, base + 5),
            run_outbox_id: id(ResourceKind::OutboxEvent, base + 6),
            node_event_id: id(ResourceKind::Event, base + 7),
            node_outbox_id: id(ResourceKind::OutboxEvent, base + 8),
            job_event_id: id(ResourceKind::Event, base + 9),
            job_outbox_id: id(ResourceKind::OutboxEvent, base + 10),
        },
        invocation_admit_receipt_id: id(ResourceKind::Receipt, base + 11),
        invocation_admit_event_id: id(ResourceKind::Event, base + 12),
        invocation_admit_outbox_id: id(ResourceKind::OutboxEvent, base + 13),
        invocation_prepare_receipt_id: id(ResourceKind::Receipt, base + 14),
        invocation_prepare_event_id: id(ResourceKind::Event, base + 15),
        invocation_prepare_outbox_id: id(ResourceKind::OutboxEvent, base + 16),
    }
}

async fn run_plan_capability_owner(pool: &PgPool, repository: &PgRepository, fixture: &Fixture) {
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET state = 'running', active_work_count = 1 WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let node_id = id(ResourceKind::NodeExecution, 0x8000);
    let node_payload = TypedPayload::new(1, &json!({"fixture": "capability-plan-owner"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, logical_key, node_kind, state,
            generation, version, payload_schema_version, payload, payload_digest,
            deadline, started_at, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, 'node_execution', $4,
            'capability-plan', 20, 'capability-plan-owner', 'capability_call', 'running',
            1, 1, $5, $6, $7, $8, $9, $9, $9
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(id(ResourceKind::ScopeInstance, 0x31).to_string())
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .bind(now + Duration::minutes(30))
    .bind(now)
    .execute(pool)
    .await
    .unwrap();

    let quota_account_id = id(ResourceKind::QuotaAccount, 0x8010);
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: fixture.tenant_id.to_string(),
            quota_account_id: quota_account_id.to_string(),
            scope_kind: "tenant".to_owned(),
            scope_id: fixture.tenant_id.to_string(),
            work_class: WorkClass::Orchestration.as_str().to_owned(),
            metric: "concurrent_jobs".to_owned(),
            limit_value: 4,
            payload: TypedPayload::new(1, &json!({"fixture": "capability-plan-owner"})).unwrap(),
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE insight_platform.quota_accounts SET reserved_value = 1 WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(quota_account_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let reservation_id = id(ResourceKind::UsageReservation, 0x8011);
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id, entry_kind,
            reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, 'reserve', 1, 0, 1, $5)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::QuotaLedgerEntry, 0x8012).to_string())
    .bind(quota_account_id.to_string())
    .bind(reservation_id.to_string())
    .bind(digest('1').to_string())
    .execute(pool)
    .await
    .unwrap();
    let source_job_id = id(ResourceKind::Job, 0x8020);
    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0x8021);
    let lease_token = digest('2');
    let bindings_digest: Sha256Digest = sqlx::query_scalar::<_, String>(
        "SELECT bindings_digest FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
    .parse()
    .unwrap();
    let source_payload = TypedPayload::new(
        1,
        &OrchestrationJobPayload {
            bindings_digest,
            node_execution_id: node_id.clone(),
            root_scope_id: id(ResourceKind::ScopeInstance, 0x31),
            retry_backoff_milliseconds: 100,
            wake_contract: None,
            convergence_failure: None,
            model_tool_continuation: None,
        },
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, run_id, node_id,
            state, version, attempt_no, attempt_limit, lease_epoch, worker_id,
            lease_token_digest, lease_expires_at, heartbeat_at, scheduled_at, deadline,
            request_digest, quota_reservation_id, payload_schema_version, payload,
            payload_digest, started_at, created_at, updated_at, trace_id
        ) VALUES (
            $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, $4, $3,
            'running', 2, 1, 3, 1, $5, $6, $7, $8, $8, $9,
            $10, $11, $12, $13, $14, $8, $8, $8,
            '0123456789abcdef0123456789abcdef'
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(source_job_id.to_string())
    .bind(node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(worker_id.to_string())
    .bind(lease_token.to_string())
    .bind(now + Duration::minutes(10))
    .bind(now)
    .bind(now + Duration::minutes(30))
    .bind(digest('3').to_string())
    .bind(reservation_id.to_string())
    .bind(source_payload.schema_version)
    .bind(&source_payload.value)
    .bind(&source_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let fence = RepositoryJobFence {
        tenant_id: fixture.tenant_id.to_string(),
        job_id: source_job_id.to_string(),
        worker_id,
        lease_epoch: 1,
        expected_job_version: 2,
        lease_token_digest: lease_token,
    };
    let input_json = json!({"amount": 7});
    let input_digest: Sha256Digest = canonical_digest(&input_json).unwrap().parse().unwrap();
    let input = ResolvedExpressionInput {
        run_value_id: fixture.inline_value_id.clone(),
        producing_node_id: None,
        value_kind: "capability_input".to_owned(),
        port: ExactDataPortRef::RunInput {
            schema_digest: fixture.input_schema_digest.clone(),
        },
        classification: DataClassification::Internal,
        schema_digest: fixture.input_schema_digest.clone(),
        content_digest: input_digest,
        value: ValueRef::Inline { value: input_json },
    };
    let selection_document = CandidateSelectionPolicyDocument {
        schema_version: 1,
        mode: CandidateSelectionMode::OnlyCandidate,
        route_schema_digest: None,
    };
    let selection_evidence = derive_candidate_selection(
        "writer",
        &fixture.selection_policy,
        &selection_document,
        std::slice::from_ref(&fixture.capability_deployment),
        None,
    )
    .unwrap();
    let invocation_id = id(ResourceKind::CapabilityInvocation, 0x8030);
    let capability_job_id = id(ResourceKind::Job, 0x8031);
    let command = DeferOrchestrationToCapabilityInvocation {
        fence: fence.clone(),
        plan: fixture.plan.clone(),
        invocation_id: invocation_id.clone(),
        capability_job_id: capability_job_id.clone(),
        input,
        route: None,
        selection_evidence,
        policy_decisions: approval_policy(&fixture.policy),
        approval_task_id: Some(id(ResourceKind::ApprovalTask, 0x8032)),
        input_artifact_link_id: None,
        mcp_runtime: None,
        idempotency_key_digest: digest('4'),
        request_digest: digest('5'),
        receipt_expires_at: now + Duration::minutes(30),
        mutations: capability_defer_mutations(0x8100),
    };
    let mut forged = command.clone();
    forged.selection_evidence.canonical_digest = digest('f');
    let mut transaction = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        transaction
            .defer_orchestration_to_capability_invocation(forged)
            .await,
        Err(RepositoryError::Conflict(
            "exact Capability candidate selection"
        ))
    ));
    transaction.rollback().await.unwrap();
    let leaked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(invocation_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(leaked, 0);

    let mut transaction = repository.begin_scheduler_transaction().await.unwrap();
    let deferred = match transaction
        .defer_orchestration_to_capability_invocation(command.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(deferred) => deferred,
        CommandOutcome::Replayed(_) => panic!("fresh Plan Capability dispatch replayed"),
    };
    transaction.commit().await.unwrap();
    assert_eq!(
        deferred.invocation.state,
        insight_platform_contracts::InvocationState::AwaitingApproval
    );
    assert!(deferred.capability_job.is_none());
    assert_eq!(deferred.run.state, "waiting");
    let mut transaction = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        transaction
            .defer_orchestration_to_capability_invocation(command)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    transaction.commit().await.unwrap();

    let approval_command = ResolveCapabilityApproval {
        audit: audit(
            &fixture.tenant_id,
            &fixture.principal_id,
            PrincipalKind::AgentRunner,
            0x8210,
            '6',
            '7',
        ),
        invocation_id: invocation_id.clone(),
        approval_task_id: id(ResourceKind::ApprovalTask, 0x8032),
        expected_invocation_version: 1,
        expected_task_generation: 1,
        expected_task_version: 1,
        eligible_principal_rule_digest: digest('a'),
        decision: CapabilityApprovalDecision::Approve,
        dispatch_mutations: Some(CapabilityApprovalDispatchMutationIds {
            receipt_id: id(ResourceKind::Receipt, 0x8220),
            event_id: id(ResourceKind::Event, 0x8221),
            outbox_id: id(ResourceKind::OutboxEvent, 0x8222),
        }),
        failure_mutations: None,
    };
    let approved = match execute_approval(repository, approval_command.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(approved) => approved,
        CommandOutcome::Replayed(_) => panic!("fresh Plan Capability approval replayed"),
    };
    assert_eq!(
        approved.payload.current_job_id,
        Some(capability_job_id.clone())
    );
    let prepared_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(capability_job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(prepared_state, "ready");
    assert!(matches!(
        execute_approval(repository, approval_command).await.unwrap(),
        CommandOutcome::Replayed(record) if record == approved
    ));

    let claim = claim_one(repository, &fixture.tenant_id, 0x9000).await;
    assert_eq!(claim.claimed.job.job_id, capability_job_id.to_string());
    let continuation_mutations = claim.claimed.resume_mutations.clone().unwrap();
    let output_json = json!({"result": "plan-owner"});
    let output_digest: Sha256Digest = canonical_digest(&output_json).unwrap().parse().unwrap();
    let completion_command = CommitCapabilityOutcome {
        audit: worker_audit(&fixture.tenant_id, &claim.worker_id, 0x9100, '6', '7'),
        invocation_id: invocation_id.clone(),
        job_id: capability_job_id.clone(),
        expected_invocation_version: claim.claimed.invocation.version,
        fence: claim.fence(),
        quota_entry_ids: vec![
            id(ResourceKind::QuotaLedgerEntry, 0x9103),
            id(ResourceKind::QuotaLedgerEntry, 0x9104),
        ],
        outcome: DispatchOutcome::Completed(CapabilityOutputValue {
            value_id: id(ResourceKind::RunValue, 0x9105),
            classification: DataClassification::Internal,
            schema_digest: fixture.output_schema_digest.clone(),
            content_digest: output_digest,
            value: ValueRef::Inline { value: output_json },
            artifact_link_id: None,
            validation_evidence_digest: digest('8'),
        }),
        resume_mutations: Some(continuation_mutations.clone()),
        failure_mutations: None,
    };
    let completed = match execute_outcome(repository, completion_command.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(completed) => completed,
        CommandOutcome::Replayed(_) => panic!("fresh Plan Capability outcome replayed"),
    };
    assert_eq!(
        completed.invocation.state,
        insight_platform_contracts::InvocationState::Succeeded
    );
    assert!(matches!(
        execute_outcome(repository, completion_command).await.unwrap(),
        CommandOutcome::Replayed(record) if record == completed
    ));
    let owner_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(node_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(owner_state, "succeeded");
    let continuation: (String, String, i64, i64) = sqlx::query_as(
        r#"
        SELECT node.state, job.state,
               (SELECT count(*) FROM insight_platform.run_nodes
                WHERE tenant_id = $1 AND node_id = $2),
               (SELECT count(*) FROM insight_platform.run_values
                WHERE tenant_id = $1 AND value_id = $4
                  AND node_id = $5 AND schema_digest = $6)
        FROM insight_platform.run_nodes AS node
        JOIN insight_platform.jobs AS job
          ON job.tenant_id = node.tenant_id AND job.node_id = node.node_id
        WHERE node.tenant_id = $1 AND node.node_id = $2 AND job.job_id = $3
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(
        continuation_mutations
            .continuation_node_execution_id
            .to_string(),
    )
    .bind(continuation_mutations.continuation_job_id.to_string())
    .bind(id(ResourceKind::RunValue, 0x9105).to_string())
    .bind(node_id.to_string())
    .bind(fixture.output_schema_digest.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(continuation, ("ready".to_owned(), "ready".to_owned(), 1, 1));
    let run_state: (String, i32) = sqlx::query_as(
        "SELECT state, active_work_count FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(run_state, ("running".to_owned(), 0));
}

fn command_for_inline(fixture: &Fixture, base: u16) -> AdmitCapabilityInvocation {
    AdmitCapabilityInvocation {
        audit: audit(
            &fixture.tenant_id,
            &fixture.principal_id,
            PrincipalKind::AgentRunner,
            base,
            'c',
            'd',
        ),
        invocation_id: id(ResourceKind::CapabilityInvocation, base + 3),
        run_id: fixture.run_id.clone(),
        node_execution_id: fixture.ready_node_id.clone(),
        expected_run_version: 1,
        expected_node_version: 1,
        slot_id: "writer".to_owned(),
        input_value_id: fixture.inline_value_id.clone(),
        input_artifact_link_id: None,
        origin: InvocationOrigin::PlanNode {
            node_execution_id: fixture.ready_node_id.clone(),
        },
        selected_candidate_ordinal: 0,
        selector_input_digest: digest('e'),
        policy_decisions: allowed_policy(&fixture.policy),
        approval_task_id: None,
        requested_attempt_limit: 3,
        requested_retry_backoff_milliseconds: 100,
        mcp_runtime: None,
    }
}

fn command_for_node(
    fixture: &Fixture,
    node_execution_id: &ResourceId,
    base: u16,
) -> AdmitCapabilityInvocation {
    let mut command = command_for_inline(fixture, base);
    command.node_execution_id = node_execution_id.clone();
    command.origin = InvocationOrigin::PlanNode {
        node_execution_id: node_execution_id.clone(),
    };
    command
}

fn command_for_artifact(fixture: &Fixture, base: u16) -> AdmitCapabilityInvocation {
    AdmitCapabilityInvocation {
        audit: audit(
            &fixture.tenant_id,
            &fixture.principal_id,
            PrincipalKind::AgentRunner,
            base,
            'd',
            'e',
        ),
        invocation_id: id(ResourceKind::CapabilityInvocation, base + 3),
        run_id: fixture.run_id.clone(),
        node_execution_id: fixture.approval_node_id.clone(),
        expected_run_version: 1,
        expected_node_version: 1,
        slot_id: "writer".to_owned(),
        input_value_id: fixture.artifact_value_id.clone(),
        input_artifact_link_id: Some(id(ResourceKind::ArtifactLink, base + 4)),
        origin: InvocationOrigin::PlanNode {
            node_execution_id: fixture.approval_node_id.clone(),
        },
        selected_candidate_ordinal: 0,
        selector_input_digest: digest('f'),
        policy_decisions: approval_policy(&fixture.policy),
        approval_task_id: Some(id(ResourceKind::ApprovalTask, base + 5)),
        requested_attempt_limit: 3,
        requested_retry_backoff_milliseconds: 100,
        mcp_runtime: None,
    }
}

fn approval_resolution(
    fixture: &Fixture,
    admission: &AdmitCapabilityInvocation,
    base: u16,
    approve: bool,
) -> ResolveCapabilityApproval {
    ResolveCapabilityApproval {
        audit: audit(
            &fixture.tenant_id,
            &fixture.principal_id,
            PrincipalKind::AgentRunner,
            base,
            if approve { '4' } else { '5' },
            if approve { '6' } else { '7' },
        ),
        invocation_id: admission.invocation_id.clone(),
        approval_task_id: admission.approval_task_id.clone().unwrap(),
        expected_invocation_version: 1,
        expected_task_generation: 1,
        expected_task_version: 1,
        eligible_principal_rule_digest: digest('a'),
        decision: if approve {
            CapabilityApprovalDecision::Approve
        } else {
            CapabilityApprovalDecision::Reject
        },
        dispatch_mutations: None,
        failure_mutations: None,
    }
}

async fn seed_fixture(pool: &PgPool, repository: &PgRepository) -> Fixture {
    let tenant_id = id(ResourceKind::Tenant, 1);
    let other_tenant_id = id(ResourceKind::Tenant, 2);
    let principal_id = id(ResourceKind::Principal, 3);
    let denied_principal_id = id(ResourceKind::Principal, 4);
    let other_principal_id = id(ResourceKind::Principal, 5);
    for tenant in [&tenant_id, &other_tenant_id] {
        repository
            .create_tenant(NewTenant {
                tenant_id: tenant.to_string(),
                state: "active".to_owned(),
                config: TenantConfig::default(),
            })
            .await
            .unwrap();
    }
    for (principal, character) in [
        (&principal_id, '1'),
        (&denied_principal_id, '2'),
        (&other_principal_id, '3'),
    ] {
        repository
            .create_principal(NewPrincipal {
                principal_id: principal.clone(),
                authentication_authority_digest: digest(character),
                subject_digest: digest(char::from_u32(character as u32 + 3).unwrap()),
                installation_bindings: PrincipalBindingsPayload {
                    installation_bindings: vec![],
                },
            })
            .await
            .unwrap();
    }
    let permissions = PermissionSet::new(vec![
        Permission::ApprovalRespond,
        Permission::CapabilityInvoke,
        Permission::InteractionRespond,
        Permission::RuntimeControl,
    ])
    .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant_id.clone(),
            principal_id: principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: permissions.clone(),
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant_id.clone(),
            principal_id: denied_principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::CapabilityRead]).unwrap(),
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: other_tenant_id.clone(),
            principal_id: other_principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload { permissions },
        })
        .await
        .unwrap();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT statement_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();

    let policy_resource = id(ResourceKind::Policy, 0x10);
    let policy_revision = id(ResourceKind::PolicyRevision, 0x11);
    let selection_policy_resource = id(ResourceKind::Policy, 0x72);
    let selection_policy_revision = id(ResourceKind::PolicyRevision, 0x73);
    let selection_policy_deployment = id(ResourceKind::PolicyDeployment, 0x71);
    let agent_resource = id(ResourceKind::Agent, 0x12);
    let agent_interface = id(ResourceKind::AgentInterfaceRevision, 0x13);
    let agent_plan = id(ResourceKind::AgentPlanRevision, 0x14);
    let agent_deployment = id(ResourceKind::AgentDeployment, 0x15);
    let capability_resource = id(ResourceKind::CapabilityInterface, 0x16);
    let capability_interface = id(ResourceKind::CapabilityInterfaceRevision, 0x17);
    let implementation_resource = id(ResourceKind::CapabilityImplementation, 0x18);
    let implementation_revision = id(ResourceKind::CapabilityImplementationRevision, 0x19);
    let capability_deployment_id = id(ResourceKind::CapabilityDeployment, 0x1a);
    for (resource_id, kind) in [
        (&policy_resource, RegistryResourceKind::Policy),
        (&selection_policy_resource, RegistryResourceKind::Policy),
        (&agent_resource, RegistryResourceKind::Agent),
        (
            &capability_resource,
            RegistryResourceKind::CapabilityInterface,
        ),
        (
            &implementation_resource,
            RegistryResourceKind::CapabilityImplementation,
        ),
    ] {
        insert_resource(pool, &tenant_id, resource_id, kind, &principal_id).await;
    }

    let package = AuthoringPackage {
        artifact: ArtifactRef::new(
            id(ResourceKind::Artifact, 0x20),
            digest('0'),
            2,
            "application/json",
            DataClassification::Internal,
            Some("fixture.json".to_owned()),
        )
        .unwrap(),
        manifest_digest: digest('1'),
    };
    let validation = ValidationSummary {
        validator_digest: digest('2'),
        validated_draft_digest: digest('3'),
        dependency_closure_digest: digest('4'),
        security_evidence_digest: digest('5'),
        warnings: vec![],
    };
    let policy_exact = ExactVersionRef::new(policy_revision.clone(), digest('6')).unwrap();
    insert_version(
        pool,
        &tenant_id,
        &policy_resource,
        &policy_exact,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                authoring_package: package.clone(),
                contract_digest: digest('7'),
                dependency_versions: vec![],
                policy_versions: vec![],
                policy_kind: PolicyKind::Retry,
                rules_digest: digest('8'),
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
            })),
            validation: validation.clone(),
        },
    )
    .await;
    seed_ready_artifact(
        pool,
        &tenant_id,
        &principal_id,
        &policy_revision,
        package.artifact.artifact_id(),
        &id(ResourceKind::InternalBlob, 0x21),
        &id(ResourceKind::EncryptionDomain, 0x22),
        now,
    )
    .await;
    let selection_document = CandidateSelectionPolicyDocument {
        schema_version: 1,
        mode: CandidateSelectionMode::OnlyCandidate,
        route_schema_digest: None,
    };
    let selection_exact =
        ExactVersionRef::new(selection_policy_revision.clone(), digest('f')).unwrap();
    insert_version(
        pool,
        &tenant_id,
        &selection_policy_resource,
        &selection_exact,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                authoring_package: package.clone(),
                contract_digest: digest('e'),
                dependency_versions: vec![],
                policy_versions: vec![],
                policy_kind: PolicyKind::Selection,
                rules_digest: selection_document.canonical_digest().unwrap(),
                selection: Some(selection_document),
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
            })),
            validation: validation.clone(),
        },
    )
    .await;
    let selection_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: selection_exact.clone(),
            applicability_digest: digest('d'),
            qualification_evidence: package.artifact.clone(),
        }),
    )
    .unwrap();
    insert_deployment(
        pool,
        &tenant_id,
        &selection_policy_deployment,
        &selection_policy_resource,
        &selection_policy_revision,
        &principal_id,
        &selection_payload,
    )
    .await;
    let selection_policy = ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(
            selection_policy_deployment,
            selection_payload.digest.parse().unwrap(),
        )
        .unwrap(),
        revision: selection_exact,
    };

    let agent_interface_exact = ExactVersionRef::new(agent_interface.clone(), digest('9')).unwrap();
    let agent_plan_exact = ExactVersionRef::new(agent_plan.clone(), digest('a')).unwrap();
    let plan_node_key = PlanNodeKey::new("capability-plan".to_owned()).unwrap();
    let finish_node_key = PlanNodeKey::new("finish".to_owned()).unwrap();
    let input_schema = capability_input_schema();
    let output_schema = capability_output_schema();
    let error_schema = capability_error_schema();
    let output_port = ExactDataPortRef::NodeOutput {
        producer_node_id: plan_node_key.clone(),
        port_id: DataPortKey::new("result".to_owned()).unwrap(),
        schema_digest: output_schema.canonical_digest.clone(),
    };
    let plan = RuntimePlan {
        plan_version: 4,
        interface_revision_id: agent_interface.clone(),
        entry_node_id: PlanNodeKey::new("start".to_owned()).unwrap(),
        dependency_slots: BTreeMap::from([(
            "writer".to_owned(),
            RuntimeDependencySlot {
                kind: RuntimeDependencyKind::Capability,
                requirement_digest: digest('8'),
            },
        )]),
        nodes: BTreeMap::from([
            (
                PlanNodeKey::new("start".to_owned()).unwrap(),
                RuntimeNode::Start {
                    next: plan_node_key.clone(),
                },
            ),
            (
                plan_node_key.clone(),
                RuntimeNode::CapabilityCall {
                    capability_slot_id: "writer".to_owned(),
                    input: ExactDataPortRef::RunInput {
                        schema_digest: input_schema.canonical_digest.clone(),
                    },
                    candidate_route: None,
                    output: output_port.clone(),
                    attempt_limit: 3,
                    retry_backoff_milliseconds: 100,
                    resume: finish_node_key.clone(),
                },
            ),
            (finish_node_key, RuntimeNode::Return { value: output_port }),
        ]),
    };
    let plan_limits = PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
    plan.validate(plan_limits).unwrap();
    let plan_digest = plan.canonical_digest(plan_limits).unwrap();
    let agent_document = ResourceDocument::Agent(AgentResourceSpec {
        authoring_package: package.clone(),
        contract_digest: digest('b'),
        dependency_versions: vec![],
        policy_versions: vec![policy_exact.clone()],
        input_schema: input_schema.clone(),
        output_schema: output_schema.clone(),
        error_schema,
        typed_plan_artifact_id: package.artifact.artifact_id().clone(),
        typed_plan_digest: plan_digest,
    });
    for (exact, revision) in [(&agent_interface_exact, 1), (&agent_plan_exact, 2)] {
        insert_version(
            pool,
            &tenant_id,
            &agent_resource,
            exact,
            revision,
            &principal_id,
            PublishedVersionPayload {
                document: agent_document.clone(),
                validation: validation.clone(),
            },
        )
        .await;
    }
    let interface_exact = ExactVersionRef::new(capability_interface.clone(), digest('e')).unwrap();
    let input_schema_digest = input_schema.canonical_digest.clone();
    let output_schema_digest = output_schema.canonical_digest.clone();
    insert_version(
        pool,
        &tenant_id,
        &capability_resource,
        &interface_exact,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::CapabilityInterface(CapabilityInterfaceResourceSpec {
                authoring_package: package.clone(),
                contract_digest: digest('1'),
                dependency_versions: vec![],
                policy_versions: vec![policy_exact.clone()],
                qualified_name: "fixture.writer".parse().unwrap(),
                input_schema,
                output_schema,
                error_schema: capability_error_schema(),
                artifacts: capability_artifacts(),
                data_policy: capability_data_policy(&policy_exact),
                execution_limits: CapabilityInterfaceLimits {
                    maximum_input_bytes: 1_048_576,
                    maximum_output_bytes: 1_048_576,
                    maximum_artifacts: 1,
                    maximum_execution_milliseconds: 60_000,
                },
                effect: Effect::NonIdempotentWrite,
                idempotency: CapabilityIdempotencyKind::None,
                cancellation: CapabilityCancellationKind::BestEffort,
                progress: CapabilityProgressContract {
                    mode: CapabilityProgressMode::Events,
                    schema_digest: Some(digest('8')),
                    max_events: 16,
                    max_bytes_per_event: 4_096,
                    minimum_interval_milliseconds: 1,
                    durability: CapabilityProgressDurability::CoarseDurable,
                },
            }),
            validation: validation.clone(),
        },
    )
    .await;
    let implementation_exact =
        ExactVersionRef::new(implementation_revision.clone(), digest('4')).unwrap();
    let native_contract = CapabilityBackendContract::Native(NativeCapabilityContract {
        adapter_id: "builtin.echo".to_owned(),
        adapter_version: "1.0.0".to_owned(),
        module_digest: builtin_echo_module_digest(),
        entrypoint_id: "echo.inline".to_owned(),
        worker_protocol_version: WORKER_PROTOCOL_VERSION,
    });
    let native_contract_digest = native_contract.canonical_digest().unwrap();
    insert_version(
        pool,
        &tenant_id,
        &implementation_resource,
        &implementation_exact,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::CapabilityImplementation(
                CapabilityImplementationResourceSpec {
                    authoring_package: package.clone(),
                    contract_digest: digest('5'),
                    dependency_versions: vec![],
                    policy_versions: vec![policy_exact.clone()],
                    interface_revision: interface_exact.clone(),
                    backend_kind: CapabilityBackendKind::Native,
                    backend_contract: native_contract,
                    backend_contract_digest: native_contract_digest,
                    credential_requirements: vec![],
                    backend_limits: CapabilityBackendLimits {
                        maximum_request_bytes: 1_048_576,
                        maximum_response_bytes: 1_048_576,
                        maximum_diagnostic_bytes: 65_536,
                        connect_timeout_milliseconds: 100,
                        first_byte_timeout_milliseconds: 500,
                        idle_timeout_milliseconds: 1_000,
                        total_timeout_milliseconds: 5_000,
                    },
                    features: CapabilityBackendFeatures {
                        deferred: true,
                        input_required: true,
                        callback: true,
                        poll: true,
                        progress: true,
                        cancellation: true,
                        max_remote_state_bytes: 16_384,
                        max_poll_count: 16,
                    },
                },
            ),
            validation,
        },
    )
    .await;
    let capability_closure = CapabilityDeploymentClosure {
        implementation: implementation_exact,
        interface: interface_exact,
        backend: CapabilityBackendBinding::Native {
            worker_manifest_digest: native_worker_manifest_digest(),
            adapter_module_digest: builtin_echo_module_digest(),
        },
        secret_bindings: vec![],
        policies: vec![policy_exact.clone()],
        conformance_evidence: package.artifact,
    };
    let capability_payload = TypedPayload::new(
        1,
        &DeploymentClosure::CapabilityInterface(capability_closure),
    )
    .unwrap();
    insert_deployment(
        pool,
        &tenant_id,
        &capability_deployment_id,
        &capability_resource,
        &capability_interface,
        &principal_id,
        &capability_payload,
    )
    .await;
    let capability_deployment = ExactDeploymentRef::new(
        capability_deployment_id,
        capability_payload.digest.parse().unwrap(),
    )
    .unwrap();
    for (account_id, scope_kind, scope_id, metric) in [
        (
            id(ResourceKind::QuotaAccount, 0x1b),
            "tenant",
            tenant_id.clone(),
            QuotaDimension::WorkClassConcurrentOperations,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x1c),
            "capability_deployment",
            capability_deployment.deployment_id.clone(),
            QuotaDimension::CapabilityConcurrentInvocations,
        ),
    ] {
        repository
            .create_quota_account(NewQuotaAccount {
                tenant_id: tenant_id.to_string(),
                quota_account_id: account_id.to_string(),
                scope_kind: scope_kind.to_owned(),
                scope_id: scope_id.to_string(),
                work_class: WorkClass::CapabilityNative.as_str().to_owned(),
                metric: metric.as_str().to_owned(),
                limit_value: 8,
                payload: TypedPayload::new(1, &json!({"fixture": "capability"})).unwrap(),
            })
            .await
            .unwrap();
    }
    let policy_binding = selection_policy.clone();
    let agent_closure = AgentDeploymentClosure {
        interface: agent_interface_exact,
        plan: agent_plan_exact,
        entry_node_id: "start".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
        slots: vec![FrozenSlotBinding {
            slot_id: "writer".to_owned(),
            requirement_digest: digest('8'),
            target: FrozenSlotTarget::Capability {
                candidates: vec![capability_deployment.clone()],
                selection_policy: policy_binding.clone(),
                tool_alias: Some("write".to_owned()),
            },
            binding_digest: digest('9'),
        }],
        policies: vec![],
        execution_profile: policy_binding,
    };
    let agent_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(agent_closure.clone())).unwrap();
    insert_deployment(
        pool,
        &tenant_id,
        &agent_deployment,
        &agent_resource,
        &agent_plan,
        &principal_id,
        &agent_payload,
    )
    .await;
    let principal_snapshot = PrincipalSnapshot::build(
        tenant_id.clone(),
        principal_id.clone(),
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![
            Permission::ApprovalRespond,
            Permission::CapabilityInvoke,
            Permission::InteractionRespond,
            Permission::RuntimeControl,
        ])
        .unwrap(),
        1,
        1,
        1,
    )
    .unwrap();
    let run_bindings = RunBindingsSnapshot::build(
        ExactDeploymentRef::new(
            agent_deployment.clone(),
            agent_payload.digest.parse().unwrap(),
        )
        .unwrap(),
        principal_snapshot,
        &agent_closure,
    )
    .unwrap();
    let run_id = id(ResourceKind::Run, 0x30);
    let scope_id = id(ResourceKind::ScopeInstance, 0x31);
    let ready_node_id = id(ResourceKind::NodeExecution, 0x32);
    let approval_node_id = id(ResourceKind::NodeExecution, 0x33);
    let approval_cancel_node_id = id(ResourceKind::NodeExecution, 0x3e);
    let deferred_node_id = id(ResourceKind::NodeExecution, 0x3a);
    let uncertain_node_id = id(ResourceKind::NodeExecution, 0x3b);
    let cancel_node_id = id(ResourceKind::NodeExecution, 0x3c);
    let cancel_worker_node_id = id(ResourceKind::NodeExecution, 0x3d);
    let process_recovery_node_id = id(ResourceKind::NodeExecution, 0x3f);
    let inline_value_id = id(ResourceKind::RunValue, 0x34);
    let artifact_value_id = id(ResourceKind::RunValue, 0x35);
    let artifact_id = id(ResourceKind::Artifact, 0x36);
    let current = RunCurrentSnapshot::initial(
        run_id.clone(),
        agent_deployment.clone(),
        inline_value_id.clone(),
    );
    let bindings_payload = TypedPayload::from_versioned(1, &run_bindings, 1_048_576).unwrap();
    let current_payload = TypedPayload::from_versioned(1, &current, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.runs (
            tenant_id, run_id, root_run_id, agent_deployment_id, principal_id,
            state, version, bindings_schema_version, bindings, bindings_digest,
            current_schema_version, current_payload, current_payload_digest,
            deadline, started_at, created_at, updated_at, trace_id
        ) VALUES (
            $1, $2, $2, $3, $4, 'running', 1, $5, $6, $7,
            $8, $9, $10, $11, $12, $12, $12,
            '0123456789abcdef0123456789abcdef'
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .bind(agent_deployment.to_string())
    .bind(principal_id.to_string())
    .bind(bindings_payload.schema_version)
    .bind(&bindings_payload.value)
    .bind(run_bindings.canonical_digest.to_string())
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(now + Duration::hours(1))
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    let inline_value = json!({"amount": 7});
    let inline_digest: Sha256Digest = canonical_digest(&inline_value).unwrap().parse().unwrap();
    let root_environment = ScopeDataEnvironmentSnapshot::build(
        BTreeMap::from([(
            ExactDataPortRef::RunInput {
                schema_digest: input_schema_digest.clone(),
            },
            ExactRunValueRef {
                value_id: inline_value_id.clone(),
                schema_digest: input_schema_digest.clone(),
                content_digest: inline_digest.clone(),
            },
        )]),
        ScopeEnvironmentLimits::from_profile(&checked_in_hard_limit_profile()).unwrap(),
    )
    .unwrap();
    let root_payload = TypedPayload::new(
        1,
        &json!({
            "root_run_id": run_id,
            "environment": root_environment,
        }),
    )
    .unwrap();
    let node_payload = TypedPayload::new(1, &json!({"fixture": "capability"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, record_kind, scope_id, logical_key,
            node_kind, state, generation, version, payload_schema_version,
            payload, payload_digest, deadline, started_at, created_at, updated_at
        ) VALUES (
            $1, $2, $3, 'scope_instance', $2, 'root', 'root', 'open', 1, 1,
            $4, $5, $6, $7, $8, $8, $8
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(scope_id.to_string())
    .bind(run_id.to_string())
    .bind(root_payload.schema_version)
    .bind(&root_payload.value)
    .bind(&root_payload.digest)
    .bind(now + Duration::hours(1))
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    for (node_id, logical_key, ordinal) in [
        (&ready_node_id, "capability-ready", 1_i32),
        (&approval_node_id, "capability-approval", 2_i32),
        (&deferred_node_id, "capability-deferred", 3_i32),
        (&uncertain_node_id, "capability-uncertain", 4_i32),
        (&cancel_node_id, "capability-cancel", 5_i32),
        (&cancel_worker_node_id, "capability-cancel-worker", 6_i32),
        (
            &approval_cancel_node_id,
            "capability-approval-cancel",
            7_i32,
        ),
        (
            &process_recovery_node_id,
            "capability-process-recovery",
            8_i32,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_nodes (
                tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
                plan_node_key, activation_ordinal, logical_key, node_kind, state,
                generation, version, payload_schema_version, payload, payload_digest,
                deadline, started_at, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, 'node_execution', $4,
                $5, $6, $5, 'capability_call', 'running',
                1, 1, $7, $8, $9, $10, $11, $11, $11
            )
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(node_id.to_string())
        .bind(run_id.to_string())
        .bind(scope_id.to_string())
        .bind(logical_key)
        .bind(ordinal)
        .bind(node_payload.schema_version)
        .bind(&node_payload.value)
        .bind(&node_payload.digest)
        .bind(now + Duration::hours(1))
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, value_kind, classification,
            schema_digest, content_digest, inline_value
        ) VALUES ($1, $2, $3, 'capability_input', 'internal', $4, $5, $6)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(inline_value_id.to_string())
    .bind(run_id.to_string())
    .bind(input_schema_digest.to_string())
    .bind(inline_digest.to_string())
    .bind(inline_value)
    .execute(pool)
    .await
    .unwrap();
    seed_ready_artifact(
        pool,
        &tenant_id,
        &principal_id,
        &policy_revision,
        &artifact_id,
        &id(ResourceKind::InternalBlob, 0x37),
        &id(ResourceKind::EncryptionDomain, 0x38),
        now,
    )
    .await;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, value_kind, classification,
            schema_digest, content_digest, artifact_id
        ) VALUES ($1, $2, $3, 'capability_input', 'internal', $4, $5, $6)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_value_id.to_string())
    .bind(run_id.to_string())
    .bind(input_schema_digest.to_string())
    .bind(digest('0').to_string())
    .bind(artifact_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET input_value_id = $3 WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .bind(inline_value_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    Fixture {
        tenant_id,
        other_tenant_id,
        principal_id,
        denied_principal_id,
        other_principal_id,
        run_id,
        ready_node_id,
        approval_node_id,
        approval_cancel_node_id,
        deferred_node_id,
        uncertain_node_id,
        cancel_node_id,
        cancel_worker_node_id,
        process_recovery_node_id,
        inline_value_id,
        artifact_value_id,
        artifact_id,
        capability_deployment,
        output_schema_digest,
        policy: policy_exact,
        plan,
        selection_policy,
        input_schema_digest,
    }
}

async fn insert_resource(
    pool: &PgPool,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
    kind: RegistryResourceKind,
    principal_id: &ResourceId,
) {
    let payload = TypedPayload::new(1, &json!({"created_by": principal_id})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            draft_generation, version, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, $3, $4, $5, 1, 1, $6, $7, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(kind.as_str())
    .bind(EntityLifecycle::Active.as_str())
    .bind(AdministrativeGate::Enabled.as_str())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_version(
    pool: &PgPool,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
    exact: &ExactVersionRef,
    revision_no: i64,
    principal_id: &ResourceId,
    published: PublishedVersionPayload,
) {
    published
        .validate_for(
            match resource_id.kind() {
                ResourceKind::Policy => RegistryResourceKind::Policy,
                ResourceKind::Agent => RegistryResourceKind::Agent,
                ResourceKind::CapabilityInterface => RegistryResourceKind::CapabilityInterface,
                ResourceKind::CapabilityImplementation => {
                    RegistryResourceKind::CapabilityImplementation
                }
                ResourceKind::McpServer => RegistryResourceKind::McpServer,
                _ => panic!("unexpected fixture Resource kind"),
            },
            &exact.revision_id,
        )
        .unwrap();
    let payload = TypedPayload::new(1, &published).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resource_versions (
            tenant_id, resource_version_id, resource_id, resource_version_kind,
            revision_no, content_digest, payload_schema_version, payload,
            payload_digest, created_by
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(exact.revision_id.to_string())
    .bind(resource_id.to_string())
    .bind(exact.resource_kind.descriptor().name)
    .bind(revision_no)
    .bind(exact.semantic_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_deployment(
    pool: &PgPool,
    tenant_id: &ResourceId,
    deployment_id: &ResourceId,
    resource_id: &ResourceId,
    version_id: &ResourceId,
    principal_id: &ResourceId,
    payload: &TypedPayload,
) {
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id,
            environment, bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(deployment_id.to_string())
    .bind(resource_id.to_string())
    .bind(version_id.to_string())
    .bind(&payload.digest)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn seed_ready_artifact(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    retention_policy_revision_id: &ResourceId,
    artifact_id: &ResourceId,
    blob_id: &ResourceId,
    encryption_domain_id: &ResourceId,
    now: DateTime<Utc>,
) {
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest, security_domain_digest,
            object_reference_ciphertext, object_generation, key_id, encryption_domain_id,
            content_digest, size_bytes, state, version, verified_at, created_at, updated_at
        ) VALUES (
            $1, $2, 's3', $3, $4, $5, 'version-1', 'key-1', $6,
            $7, 2, 'verified', 1, $8, $8, $8
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .bind(digest('b').to_string())
    .bind(digest('c').to_string())
    .bind(vec![1_u8, 2, 3])
    .bind(encryption_domain_id.to_string())
    .bind(digest('0').to_string())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    let metadata = TypedPayload::new(1, &json!({"display_name": "capability-input.json"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, version, metadata_schema_version,
            metadata, metadata_digest, retention_policy_revision_id, retain_until,
            created_by, created_at, updated_at
        ) VALUES (
            $1, $2, $3, 'capability_input', 'internal',
            2, $4, 'application/json', 'application/json', 'ready', 1,
            $5, $6, $7, $8, $9, $10, $11, $11
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .bind(blob_id.to_string())
    .bind(digest('0').to_string())
    .bind(metadata.schema_version)
    .bind(&metadata.value)
    .bind(&metadata.digest)
    .bind(retention_policy_revision_id.to_string())
    .bind(now + Duration::days(2))
    .bind(principal_id.to_string())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
}
