use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_artifact_rpc::{
    proto::artifact_data_worker_service_server::ArtifactDataWorkerServiceServer,
    ArtifactDataWorkerGrpcService, ArtifactInternalRpcLimits, ArtifactWorkloadStageAuthority,
    ArtifactWorkloadStageError, McpDiscoveryWorkerWorkloadIdentity,
    MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY,
};
use insight_platform_artifacts::{
    ArtifactBackendFailure, ArtifactBlobBackend, ArtifactBlobDeletionEvidence,
    ArtifactReferenceSnapshot, ArtifactScanDisposition, ArtifactScanEvidence,
    ArtifactScanEvidenceDraft, ArtifactScanRequest, ArtifactScanner, ArtifactWorkerService,
    DeleteArtifactBlobGeneration, StageWorkloadArtifact, StageWorkloadArtifactRequest,
    WorkloadArtifactStagePreflight,
};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityTransportCancelOutcome, CapabilityTransportCancelRequest,
    GrpcNetworkTransport, GrpcTransportRequest, GrpcTransportResponse, HttpNetworkTransport,
    HttpTransportRequest, HttpTransportResponse,
};
use insight_platform_context::{
    AdmitContextSubscriptionRefresh, ContextSubscriptionAdmissionAudit,
    ContextSubscriptionRefreshCause, ContextSubscriptionRefreshEvidence,
    ContextSubscriptionRefreshFailureClass, ContextSubscriptionRefreshRequest,
    ContextSubscriptionRefreshResponse, CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
    CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
};
use insight_platform_contracts::{
    canonical_digest, AllowedMcpServerCapabilities, ArtifactPurpose, ArtifactRef,
    ArtifactReferenceKind, ArtifactRetentionPolicy, ArtifactWorkloadAudience, AuthoringPackage,
    CapabilityEndpointScheme, CodeTrustClass, CommandAudit, CommandOutcome, ContextBackendBinding,
    ContextBackendContract, ContextBackendKind, ContextBackendLimits, ContextCitationContract,
    ContextCitationStrength, ContextConsistencyMode, ContextDataPolicyContract,
    ContextDeploymentClosure, ContextImplementationContract, ContextImplementationResourceSpec,
    ContextInterfaceLimits, ContextInterfaceResourceSpec, ContextLocatorKind,
    ContextPaginationContract, ContextRankingContract, DataClassification, DataRegion,
    DeploymentClosure, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef,
    McpAuthPolicyDocument, McpAuthorizationPrincipalKind, McpClientCapabilities,
    McpDiscoverySnapshot, McpMetadataPolicy, McpMethodLimits, McpNegotiatedCapabilities,
    McpOAuthClientAuthenticationKind, McpOAuthEndpoint, McpProtocolPolicyDocument, McpServerLimits,
    McpServerResourceSpec, McpSessionState, McpTransportBinding, McpTransportFeatures, Permission,
    PermissionSet, PolicyDeploymentClosure, PolicyKind, PolicyResourceSpec,
    PrincipalBindingsPayload, PrincipalKind, PublishedMcpMethod, PublishedVersionPayload,
    QuotaDimension, RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind,
    SandboxArtifactIoPolicyDocument, SandboxIsolationClass, SandboxIsolationPolicyDocument,
    SandboxNetworkPolicyDocument, SandboxResourcePolicyDocument, SandboxRuntimeFamily,
    SandboxSecretDeliveryMode, SandboxSecretResolutionPolicyDocument, SecretBindingPayload,
    SecretPurpose, SecretResolutionPolicy, Sha256Digest, TenantConfig, TenantPrincipalPayload,
    ValidationSummary, WorkClass, WorkerManifest, MCP_PROTOCOL_BASELINE, WORKER_MANIFEST_VERSION,
    WORKER_PROTOCOL_VERSION,
};
use insight_platform_egress::{
    AeadMcpRemoteTaskStateCodec, AeadMcpSubscriptionStateCodec, DnsResolutionError,
    EgressDnsResolver, InstalledMcpStreamableHttpEndpoint,
    InstalledMcpStreamableHttpEndpointCatalog, McpRemoteTaskStateKey,
    McpStreamableHttpEgressLimits, McpSubscriptionStateKey, ReqwestMcpStreamableHttpConnector,
    ReqwestMcpStreamableHttpSubscriptionConnector, ResolvedSecretMaterial,
    SecretMaterialResolutionError, SecretMaterialResolver, SensitiveMcpRemoteTaskStateKey,
    SensitiveMcpSubscriptionStateKey,
};
use insight_platform_egress_rpc::{
    proto::egress_broker_service_server::EgressBrokerServiceServer, EgressBrokerGrpcService,
    EgressCallerWorkloadIdentity, EgressInternalRpcLimits, EgressMcpSubscriptionBridge,
    EgressMcpSubscriptionBridgeLimits, MCP_HOST_WORKLOAD_IDENTITY,
    MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY,
};
use insight_platform_jobs::{JobFence as DomainJobFence, LeasePolicy};
use insight_platform_mcp_host::{
    CompleteMcpSubscriptionReconcile, CompleteMcpSubscriptionRefresh,
    ContextSubscriptionRefreshResolver, CreateMcpDiscoveryOperation, CreateMcpResourceSubscription,
    EncryptedMcpState, FinalizeMcpDiscovery, McpAuthorizationBindingRecord, McpDiscoveryAdmission,
    McpDiscoveryCandidate, McpDiscoveryContractQuery, McpDiscoveryExecutionContractResolver,
    McpDiscoveryOperationPayload, McpDiscoveryOperationRecord, McpDiscoveryResultBinding,
    McpDiscoverySnapshotRecord, McpDiscoveryTransportEvidence, McpExecutionContractQuery,
    McpNotificationApplyDisposition, McpNotificationAudit, McpNotificationClass,
    McpNotificationCommit, McpResourceRefreshConnector, McpResourceRefreshTransportEvidence,
    McpResourceRefreshTransportRequest, McpSubscriptionContractQuery,
    McpSubscriptionExecutionResolver, McpSubscriptionReconcileScan, McpSubscriptionRecord,
    McpSubscriptionRecoveryCause, McpSubscriptionRecoveryScan, McpSubscriptionWorkerAudit,
    McpTransportFailure, McpWorkerAudit, NewMcpAuthorizationBinding, NewMcpDiscoveryAdmission,
    NewMcpDiscoverySnapshotRecord, ParkMcpDiscoveryVerification, RecoverDueMcpSubscription,
    RecoverExpiredMcpDiscoveryJob, ReportMcpSubscriptionTransportTermination,
    SaveMcpSubscriptionSession, WakeMcpSubscriptionReconcile,
};
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelProviderWireConnector, ModelProviderWireProtocol, ModelProviderWireRequest,
    ModelProviderWireStream,
};
use insight_platform_postgres::{
    artifact_repository::{ArtifactExecutionSlot, StartedArtifactExecution},
    mcp_repository::{
        ClaimContextSubscriptionRefreshJobs, CommitContextSubscriptionRefresh,
        ContextSubscriptionRefreshClaimSlot, ContextSubscriptionRefreshRecoverySlot,
        DriveExpiredContextSubscriptionRefreshJobs,
    },
    repository::{
        ArtifactWorkerRole, ClaimArtifactJobs, ClaimJobs, HeartbeatJob,
        JobFence as RepositoryJobFence, NewPrincipal, NewQuotaAccount, NewSecretBinding, NewTenant,
        NewTenantPrincipal, PgRepository, RepositoryError, TypedPayload,
    },
    verify_schema,
};
use insight_platform_sandbox::SandboxExecutionPolicyClosure;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    ServerConfig,
};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    collections::BTreeMap,
    io::Write as _,
    net::SocketAddr,
    path::PathBuf,
    process::{Child, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration as StdDuration,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::Notify;
use tokio_rustls::TlsAcceptor;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1cf-32e4-75e1-a9e8-d95ca0f6{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn discovery_preallocation(
    suffix: u16,
) -> insight_platform_mcp_host::McpDiscoveryArtifactPreallocation {
    insight_platform_mcp_host::McpDiscoveryArtifactPreallocation::build(
        id(ResourceKind::Artifact, suffix),
        id(ResourceKind::InternalBlob, suffix + 1),
        id(ResourceKind::Job, suffix + 2),
        id(ResourceKind::ArtifactLink, suffix + 3),
        id(ResourceKind::McpDiscoverySnapshot, suffix + 4),
        id(ResourceKind::QuotaLedgerEntry, suffix + 5),
    )
    .unwrap()
}

fn discovery_artifact_policy(
    suffix: u16,
    now: DateTime<Utc>,
) -> insight_platform_mcp_host::McpDiscoveryArtifactPolicyClosure {
    insight_platform_mcp_host::McpDiscoveryArtifactPolicyClosure::build(
        insight_platform_mcp_host::NewMcpDiscoveryArtifactPolicyClosure {
            quota_account_id: id(ResourceKind::QuotaAccount, suffix),
            maximum_bytes: 1_048_576,
            retention_policy_revision: exact(ResourceKind::PolicyRevision, suffix + 1, "retention"),
            artifact_io_policy_revision: exact(
                ResourceKind::PolicyRevision,
                suffix + 2,
                "artifact-io",
            ),
            scanner_contract_digest: named_digest("artifact-scanner-contract"),
            ruleset_digest: named_digest("artifact-rules"),
            evidence_ttl_milliseconds: 60_000,
            retry_backoff_milliseconds: 1_000,
            write_storage_binding_digest: named_digest("artifact-write-storage-binding"),
            encryption_domain_id: id(ResourceKind::EncryptionDomain, suffix + 3),
            retain_until: now + Duration::hours(3),
        },
    )
    .unwrap()
}

fn named_digest(name: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({"fixture": name}))
        .unwrap()
        .parse()
        .unwrap()
}

fn digest_without_field<T: Serialize>(value: &T, field: &str) -> Sha256Digest {
    let mut value = serde_json::to_value(value).unwrap();
    value.as_object_mut().unwrap().remove(field);
    canonical_digest(&value).unwrap().parse().unwrap()
}

fn exact(kind: ResourceKind, suffix: u16, name: &str) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), named_digest(name)).unwrap()
}

fn artifact(suffix: u16, name: &str) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        named_digest(name),
        128,
        "application/json",
        DataClassification::Internal,
        Some(format!("{name}.json")),
    )
    .unwrap()
}

fn authoring(suffix: u16, name: &str) -> AuthoringPackage {
    AuthoringPackage {
        artifact: artifact(suffix, name),
        manifest_digest: named_digest(&format!("{name}-manifest")),
    }
}

fn validation(name: &str) -> ValidationSummary {
    ValidationSummary {
        validator_digest: named_digest(&format!("{name}-validator")),
        validated_draft_digest: named_digest(&format!("{name}-draft")),
        dependency_closure_digest: named_digest(&format!("{name}-dependencies")),
        security_evidence_digest: named_digest(&format!("{name}-security")),
        warnings: vec![],
    }
}

fn endpoint(host: &str, path: &str) -> insight_platform_contracts::CanonicalHttpEndpoint {
    insight_platform_contracts::CanonicalHttpEndpoint {
        scheme: CapabilityEndpointScheme::Https,
        host: host.to_owned(),
        port: 443,
        base_path: path.to_owned(),
    }
}

fn oauth_endpoint(host: &str, path: &str) -> McpOAuthEndpoint {
    let endpoint = endpoint(host, path);
    McpOAuthEndpoint {
        endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
        endpoint,
    }
}

fn method_limits() -> McpMethodLimits {
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

fn protocol_document() -> McpProtocolPolicyDocument {
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
            tools: false,
            resources: true,
            prompts: false,
            logging: false,
            tasks: false,
            subscriptions: true,
        },
        experimental_features: vec![],
        method_limits: BTreeMap::from([
            (PublishedMcpMethod::ResourcesList, method_limits()),
            (PublishedMcpMethod::ResourcesRead, method_limits()),
        ]),
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

fn context_worker_manifest() -> WorkerManifest {
    WorkerManifest {
        manifest_version: WORKER_MANIFEST_VERSION,
        worker_role: "context-worker".to_owned(),
        work_class: WorkClass::Context,
        adapter_runtime_digest: named_digest("context-subscription-runtime"),
        protocol_version: WORKER_PROTOCOL_VERSION,
        max_concurrency: 4,
        critical_control_reserved_slots: 1,
    }
}

fn context_worker_manifest_digest() -> Sha256Digest {
    context_worker_manifest().canonical_digest().unwrap()
}

struct EmptyModel;

#[async_trait]
impl ModelProviderWireConnector for EmptyModel {
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

struct EmptyHttp;

#[async_trait]
impl HttpNetworkTransport for EmptyHttp {
    async fn round_trip(
        &self,
        _request: HttpTransportRequest,
    ) -> Result<HttpTransportResponse, CapabilityAdapterFailure> {
        unreachable!()
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Accepted)
    }
}

struct EmptyGrpc;

#[async_trait]
impl GrpcNetworkTransport for EmptyGrpc {
    async fn unary(
        &self,
        _request: GrpcTransportRequest,
    ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure> {
        unreachable!()
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Accepted)
    }
}

struct ProcessRefreshConnector {
    calls: AtomicUsize,
    first_started: Notify,
    second_started: Notify,
    second_release: Notify,
}

#[async_trait]
impl McpResourceRefreshConnector for ProcessRefreshConnector {
    async fn refresh_resources(
        &self,
        request: McpResourceRefreshTransportRequest,
    ) -> Result<McpResourceRefreshTransportEvidence, McpTransportFailure> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            self.first_started.notify_one();
            tokio::time::sleep(StdDuration::from_secs(30)).await;
        } else if call == 1 {
            self.second_started.notify_one();
            self.second_release.notified().await;
        }
        Ok(McpResourceRefreshTransportEvidence {
            schema_version: request.schema_version,
            execution_identity_digest: request.execution_identity_digest,
            request_digest: request.request_digest,
            response_digest: named_digest(&format!("process-refresh-response-{call}")),
            resource_set_digest: named_digest("process-refresh-resource-set"),
            resource_count: 1,
            item_count: 2,
            byte_count: 512,
            remote_revision: Some(format!("process-revision-{call}")),
            cursor: None,
            observed_at: Utc::now(),
        })
    }
}

const PROTOCOL_EGRESS_CONFIG_ENV: &str = "PLATFORM_SUBSCRIPTION_EGRESS_FIXTURE_CONFIG";
const DISCOVERY_ARTIFACT_CONFIG_ENV: &str = "PLATFORM_DISCOVERY_ARTIFACT_FIXTURE_CONFIG";
const DISCOVERY_ARTIFACT_DATABASE_URL_ENV: &str =
    "PLATFORM_DISCOVERY_ARTIFACT_FIXTURE_DATABASE_URL";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtocolEgressFixtureConfig {
    egress_address: SocketAddr,
    mcp_address: SocketAddr,
    ca_pem: String,
    egress_server_cert: String,
    egress_server_key: String,
    mcp_server_cert: String,
    mcp_server_key: String,
    endpoint: InstalledMcpStreamableHttpEndpoint,
    control_prefix: String,
    resource_uri: String,
    pause_second_initialize: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryArtifactFixtureConfig {
    listen_address: SocketAddr,
    ca_pem: String,
    server_cert: String,
    server_key: String,
    control_prefix: String,
}

struct DiscoveryArtifactStageAuthority {
    repository: PgRepository,
    pool: PgPool,
    control_prefix: String,
}

#[async_trait]
impl ArtifactWorkloadStageAuthority for DiscoveryArtifactStageAuthority {
    async fn stage_workload_artifact(
        &self,
        request: StageWorkloadArtifactRequest,
    ) -> Result<insight_platform_artifacts::StagedWorkloadArtifact, ArtifactWorkloadStageError>
    {
        let preflight = self
            .repository
            .authorize_workload_artifact_stage(&request)
            .await
            .map_err(map_discovery_artifact_fixture_error)?;
        let WorkloadArtifactStagePreflight::Authorized(authority) = preflight else {
            let WorkloadArtifactStagePreflight::Replayed(staged) = preflight else {
                unreachable!("closed preflight has two variants");
            };
            return Ok(staged);
        };
        std::fs::write(
            protocol_control_path(&self.control_prefix, "artifact-stage.started"),
            b"started",
        )
        .unwrap();
        let staged_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
            .await
            .map_err(|_| ArtifactWorkloadStageError::Unavailable)?;
        match self
            .repository
            .stage_workload_artifact(StageWorkloadArtifact {
                schema_version: 1,
                tenant_id: request.tenant_id,
                caller: ArtifactWorkloadAudience::McpHost,
                producer_job_id: request.producer_job_id,
                producer_fence: request.producer_fence,
                verification_job_id: request.verification_job_id,
                artifact_id: request.artifact_id,
                blob_id: request.blob_id,
                content_digest: request.descriptor_digest,
                size_bytes: u64::try_from(request.descriptor_bytes.len())
                    .map_err(|_| ArtifactWorkloadStageError::Integrity)?,
                media_type: request.media_type,
                storage_backend: "s3".to_owned(),
                storage_binding_digest: authority.write_storage_binding_digest,
                object_reference_ciphertext: vec![0x6b; 48],
                object_generation: "discovery-process-generation-1".to_owned(),
                key_id: "discovery-process-key".to_owned(),
                encryption_domain_id: authority.encryption_domain_id,
                backend_evidence_digest: named_digest("discovery-process-backend-evidence"),
                staged_at,
            })
            .await
            .map_err(map_discovery_artifact_fixture_error)?
        {
            CommandOutcome::Applied(staged) | CommandOutcome::Replayed(staged) => Ok(staged),
        }
    }
}

fn map_discovery_artifact_fixture_error(error: RepositoryError) -> ArtifactWorkloadStageError {
    match error {
        RepositoryError::PermissionDenied | RepositoryError::QuotaExceeded => {
            ArtifactWorkloadStageError::Denied
        }
        RepositoryError::StaleFence | RepositoryError::LeaseExpired => {
            ArtifactWorkloadStageError::StaleFence
        }
        RepositoryError::Conflict(_) | RepositoryError::IdempotencyConflict => {
            ArtifactWorkloadStageError::Conflict
        }
        RepositoryError::Database(_) => ArtifactWorkloadStageError::Unavailable,
        RepositoryError::InvalidInput(_)
        | RepositoryError::NotFound(_)
        | RepositoryError::CorruptRow(_) => ArtifactWorkloadStageError::Integrity,
    }
}

struct ProtocolFixtureDns(SocketAddr);

#[async_trait]
impl EgressDnsResolver for ProtocolFixtureDns {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DnsResolutionError> {
        if host != "mcp.example.test" || port != self.0.port() {
            return Err(DnsResolutionError::NoAddresses);
        }
        Ok(vec![self.0])
    }
}

struct ProtocolFixtureSecrets;

#[async_trait]
impl SecretMaterialResolver for ProtocolFixtureSecrets {
    async fn resolve(
        &self,
        _tenant_id: &ResourceId,
        binding: &ExactSecretBindingRef,
    ) -> Result<ResolvedSecretMaterial, SecretMaterialResolutionError> {
        ResolvedSecretMaterial::new(
            binding.secret_binding_id.clone(),
            binding.provider_id.clone(),
            binding.purpose.clone(),
            binding.binding_generation,
            named_digest("token-version"),
            b"subscription-protocol-token".to_vec(),
        )
        .map_err(|_| SecretMaterialResolutionError::InvalidEvidence)
    }
}

struct DiscoveryArtifactScanner {
    content_digest: Sha256Digest,
    size_bytes: u64,
    media_type: String,
    observed_at: DateTime<Utc>,
}

impl ArtifactScanner for DiscoveryArtifactScanner {
    async fn scan(
        &self,
        request: ArtifactScanRequest,
    ) -> Result<ArtifactScanEvidence, ArtifactBackendFailure> {
        let observed_at = self.observed_at;
        ArtifactScanEvidenceDraft {
            schema_version: 1,
            scan_kind: request.job.scan_kind,
            scan_job_id: request.job_id,
            scan_policy_revision: request.job.scan_policy_revision,
            scanner_contract_digest: request.job.scanner_contract_digest,
            ruleset_digest: request.job.ruleset_digest,
            object_generation: request.job.object_generation,
            content_digest: self.content_digest.clone(),
            size_bytes: self.size_bytes,
            verified_media_type: self.media_type.clone(),
            disposition: ArtifactScanDisposition::Verified,
            reason_class: None,
            observed_at,
            expires_at: observed_at
                + Duration::milliseconds(
                    i64::try_from(request.job.evidence_ttl_milliseconds).unwrap(),
                ),
        }
        .seal()
        .map_err(|_| ArtifactBackendFailure {
            retryable: false,
            reason_class: "fixture_scan_invalid".to_owned(),
        })
    }
}

struct UnusedDiscoveryBlobBackend;

impl ArtifactBlobBackend for UnusedDiscoveryBlobBackend {
    async fn delete_generation(
        &self,
        _request: DeleteArtifactBlobGeneration,
    ) -> Result<ArtifactBlobDeletionEvidence, ArtifactBackendFailure> {
        Err(ArtifactBackendFailure {
            retryable: false,
            reason_class: "fixture_blob_delete_unused".to_owned(),
        })
    }
}

fn protocol_control_path(prefix: &str, name: &str) -> PathBuf {
    PathBuf::from(format!("{prefix}-{name}"))
}

async fn read_http_request(
    stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
) -> Option<(String, String, Option<serde_json::Value>)> {
    let mut request = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 1_024];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        request.extend_from_slice(&chunk[..read]);
        if request.len() > 131_072 {
            return None;
        }
        if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = String::from_utf8(request[..header_end].to_vec()).ok()?;
    let lower = headers.to_ascii_lowercase();
    let request_method = headers.split_whitespace().next()?.to_owned();
    if !matches!(request_method.as_str(), "GET" | "POST")
        || !headers.starts_with(&format!("{request_method} /mcp HTTP/1.1\r\n"))
        || !lower.contains("host: mcp.example.test:")
        || !lower.contains("authorization: bearer subscription-protocol-token")
    {
        return None;
    }
    if request_method == "GET" {
        return Some((request_method, headers, None));
    }
    let content_length = lower
        .lines()
        .find_map(|line| line.strip_prefix("content-length: "))?
        .parse::<usize>()
        .ok()?;
    while request.len() - header_end < content_length {
        let mut chunk = [0_u8; 1_024];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        request.extend_from_slice(&chunk[..read]);
    }
    let body = serde_json::from_slice(&request[header_end..header_end + content_length]).ok()?;
    Some((request_method, headers, Some(body)))
}

async fn serve_protocol_request(
    stream: tokio::net::TcpStream,
    tls: Arc<ServerConfig>,
    config: ProtocolEgressFixtureConfig,
    initialize_attempts: Arc<AtomicUsize>,
    log_lock: Arc<tokio::sync::Mutex<()>>,
) {
    let Ok(mut stream) = TlsAcceptor::from(tls).accept(stream).await else {
        return;
    };
    let Some((request_method, headers, body)) = read_http_request(&mut stream).await else {
        return;
    };
    if request_method == "GET" {
        let lower = headers.to_ascii_lowercase();
        if !lower.contains("accept: text/event-stream")
            || !lower.contains("mcp-session-id: protocol-session-")
        {
            return;
        }
        {
            let _guard = log_lock.lock().await;
            let mut log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(protocol_control_path(&config.control_prefix, "methods.log"))
                .unwrap();
            writeln!(log, "sse/get").unwrap();
        }
        std::fs::write(
            protocol_control_path(&config.control_prefix, "sse.started"),
            b"started",
        )
        .unwrap();
        let response = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: keep-alive\r\n\r\n";
        if stream.write_all(response.as_bytes()).await.is_ok() {
            tokio::time::sleep(StdDuration::from_secs(30)).await;
        }
        return;
    }
    let Some(body) = body else {
        return;
    };
    let Some(method) = body.get("method").and_then(serde_json::Value::as_str) else {
        return;
    };
    {
        let _guard = log_lock.lock().await;
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(protocol_control_path(&config.control_prefix, "methods.log"))
            .unwrap();
        writeln!(log, "{method}").unwrap();
    }
    let attempt = if method == "initialize" {
        let attempt = initialize_attempts.fetch_add(1, Ordering::SeqCst) + 1;
        std::fs::write(
            protocol_control_path(
                &config.control_prefix,
                &format!("attempt-{attempt}.started"),
            ),
            b"started",
        )
        .unwrap();
        if attempt == 1 {
            tokio::time::sleep(StdDuration::from_secs(30)).await;
        } else if attempt == 2 && config.pause_second_initialize {
            let release = protocol_control_path(&config.control_prefix, "attempt-2.release");
            while !release.exists() {
                tokio::time::sleep(StdDuration::from_millis(10)).await;
            }
        }
        Some(attempt)
    } else {
        None
    };
    let id = body.get("id").cloned();
    let (status, session, response_body) = match method {
        "initialize" => (
            "200 OK",
            Some(format!("protocol-session-{}", attempt.unwrap())),
            serde_json::to_vec(&serde_json::json!({
                "id": id,
                "jsonrpc": "2.0",
                "result": {
                    "capabilities": {"resources": {"subscribe": true}},
                    "protocolVersion": MCP_PROTOCOL_BASELINE,
                    "serverInfo": {"name": "process-fixture", "version": "1"}
                }
            }))
            .unwrap(),
        ),
        "notifications/initialized" => ("202 Accepted", None, Vec::new()),
        "resources/subscribe" => (
            "200 OK",
            None,
            serde_json::to_vec(&serde_json::json!({
                "id": id,
                "jsonrpc": "2.0",
                "result": {}
            }))
            .unwrap(),
        ),
        "resources/list" => (
            "200 OK",
            None,
            serde_json::to_vec(&serde_json::json!({
                "id": id,
                "jsonrpc": "2.0",
                "result": {"resources": [
                    {"name": "other", "uri": "mcp://untrusted.example/other"},
                    {"name": "root", "uri": config.resource_uri}
                ]}
            }))
            .unwrap(),
        ),
        "resources/read" => (
            "200 OK",
            None,
            serde_json::to_vec(&serde_json::json!({
                "id": id,
                "jsonrpc": "2.0",
                "result": {"contents": [{
                    "mimeType": "text/plain",
                    "text": "independent process body canary",
                    "uri": config.resource_uri
                }]}
            }))
            .unwrap(),
        ),
        _ => return,
    };
    let session_header = session
        .map(|value| format!("mcp-session-id: {value}\r\n"))
        .unwrap_or_default();
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{session_header}content-length: {}\r\nconnection: close\r\n\r\n",
        response_body.len()
    );
    if stream.write_all(response.as_bytes()).await.is_ok() {
        let _ = stream.write_all(&response_body).await;
        let _ = stream.shutdown().await;
    }
}

async fn run_protocol_egress_fixture(config: ProtocolEgressFixtureConfig) {
    let server_certificate =
        CertificateDer::from_pem_slice(config.mcp_server_cert.as_bytes()).unwrap();
    let server_key = PrivateKeyDer::from_pem_slice(config.mcp_server_key.as_bytes()).unwrap();
    let tls = Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![server_certificate], server_key)
            .unwrap(),
    );
    let mcp_listener = tokio::net::TcpListener::bind(config.mcp_address)
        .await
        .unwrap();
    let fake_config = config.clone();
    let fake_server = tokio::spawn(async move {
        let existing_attempts = std::fs::read_to_string(protocol_control_path(
            &fake_config.control_prefix,
            "methods.log",
        ))
        .map(|log| log.lines().filter(|method| *method == "initialize").count())
        .unwrap_or(0);
        let attempts = Arc::new(AtomicUsize::new(existing_attempts));
        let log_lock = Arc::new(tokio::sync::Mutex::new(()));
        loop {
            let (stream, _) = mcp_listener.accept().await.unwrap();
            tokio::spawn(serve_protocol_request(
                stream,
                Arc::clone(&tls),
                fake_config.clone(),
                Arc::clone(&attempts),
                Arc::clone(&log_lock),
            ));
        }
    });
    let remote_tasks = Arc::new(
        AeadMcpRemoteTaskStateCodec::new(
            "protocol-fixture-key".to_owned(),
            vec![McpRemoteTaskStateKey {
                key_id: "protocol-fixture-key".to_owned(),
                key_reference_digest: named_digest("protocol-fixture-key"),
                key_material: SensitiveMcpRemoteTaskStateKey::new(vec![7; 32]).unwrap(),
            }],
        )
        .unwrap(),
    );
    let connector = Arc::new(
        ReqwestMcpStreamableHttpConnector::new(
            InstalledMcpStreamableHttpEndpointCatalog::new(vec![config.endpoint.clone()]).unwrap(),
            Arc::new(ProtocolFixtureSecrets),
            Arc::new(ProtocolFixtureDns(config.mcp_address)),
            remote_tasks,
            McpStreamableHttpEgressLimits::default(),
        )
        .unwrap()
        .allow_loopback_for_protocol_fixture(),
    );
    let limits = EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap();
    let bridge = Arc::new(
        EgressMcpSubscriptionBridge::new(
            limits,
            EgressMcpSubscriptionBridgeLimits {
                maximum_pending: 4,
                maximum_active: 4,
                event_buffer_capacity: 8,
            },
        )
        .unwrap(),
    );
    let subscription_sessions = Arc::new(
        AeadMcpSubscriptionStateCodec::new(
            "protocol-subscription-key".to_owned(),
            vec![McpSubscriptionStateKey {
                key_id: "protocol-subscription-key".to_owned(),
                key_reference_digest: named_digest("protocol-subscription-key"),
                key_material: SensitiveMcpSubscriptionStateKey::new(vec![11; 32]).unwrap(),
            }],
        )
        .unwrap(),
    );
    let subscription_connector = Arc::new(
        ReqwestMcpStreamableHttpSubscriptionConnector::new(
            InstalledMcpStreamableHttpEndpointCatalog::new(vec![config.endpoint.clone()]).unwrap(),
            Arc::new(ProtocolFixtureSecrets),
            Arc::new(ProtocolFixtureDns(config.mcp_address)),
            subscription_sessions,
            bridge.sink(),
            McpStreamableHttpEgressLimits::default(),
        )
        .unwrap()
        .allow_loopback_for_protocol_fixture(),
    );
    let service = EgressBrokerServiceServer::new(
        EgressBrokerGrpcService::new(
            Arc::new(EmptyModel),
            Arc::new(EmptyHttp),
            Arc::new(EmptyGrpc),
            limits,
        )
        .with_mcp_discovery(connector.clone())
        .with_mcp_resource_refresh(connector)
        .with_mcp_streamable_http_subscription(subscription_connector, bridge),
    );
    let service =
        tonic::service::interceptor::InterceptedService::new(service, EgressCallerWorkloadIdentity);
    eprintln!("subscription protocol Egress fixture started");
    Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(
                    config.egress_server_cert,
                    config.egress_server_key,
                ))
                .client_ca_root(Certificate::from_pem(config.ca_pem)),
        )
        .unwrap()
        .add_service(service)
        .serve(config.egress_address)
        .await
        .unwrap();
    fake_server.abort();
}

#[test]
fn subscription_egress_protocol_fixture_process() {
    let Ok(path) = std::env::var(PROTOCOL_EGRESS_CONFIG_ENV) else {
        return;
    };
    let config: ProtocolEgressFixtureConfig =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_protocol_egress_fixture(config));
}

async fn run_discovery_artifact_fixture(config: DiscoveryArtifactFixtureConfig) {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&std::env::var(DISCOVERY_ARTIFACT_DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let service = ArtifactDataWorkerServiceServer::new(ArtifactDataWorkerGrpcService::new(
        Arc::new(DiscoveryArtifactStageAuthority {
            repository: PgRepository::new(pool.clone()),
            pool,
            control_prefix: config.control_prefix,
        }),
        ArtifactInternalRpcLimits::with_write_limit(1_048_576, 262_144, 2_097_152).unwrap(),
    ));
    let service = tonic::service::interceptor::InterceptedService::new(
        service,
        McpDiscoveryWorkerWorkloadIdentity,
    );
    eprintln!("discovery Artifact fixture started");
    Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(config.server_cert, config.server_key))
                .client_ca_root(Certificate::from_pem(config.ca_pem)),
        )
        .unwrap()
        .add_service(service)
        .serve(config.listen_address)
        .await
        .unwrap();
}

#[test]
fn discovery_artifact_fixture_process() {
    let Ok(path) = std::env::var(DISCOVERY_ARTIFACT_CONFIG_ENV) else {
        return;
    };
    let config: DiscoveryArtifactFixtureConfig =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_discovery_artifact_fixture(config));
}

struct ProcessTlsFixture {
    ca: String,
    egress_server_cert: String,
    egress_server_key: String,
    artifact_server_cert: String,
    artifact_server_key: String,
    resource_host_server_cert: String,
    resource_host_server_key: String,
    resource_host_client_cert: String,
    resource_host_client_key: String,
    context_worker_client_cert: String,
    context_worker_client_key: String,
    subscription_worker_client_cert: String,
    subscription_worker_client_key: String,
    discovery_worker_client_cert: String,
    discovery_worker_client_key: String,
    mcp_server_cert: String,
    mcp_server_key: String,
}

fn process_tls_fixture() -> ProcessTlsFixture {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    let issue = |sans, usage| {
        let mut params = CertificateParams::default();
        params.subject_alt_names = sans;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![usage];
        let key = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &ca).unwrap();
        (certificate.pem(), key.serialize_pem())
    };
    let (egress_server_cert, egress_server_key) = issue(
        vec![SanType::DnsName("egress.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (artifact_server_cert, artifact_server_key) = issue(
        vec![SanType::DnsName(
            "artifact-data-worker.test".try_into().unwrap(),
        )],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (resource_host_server_cert, resource_host_server_key) = issue(
        vec![SanType::DnsName("resource-host.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (resource_host_client_cert, resource_host_client_key) = issue(
        vec![SanType::URI(MCP_HOST_WORKLOAD_IDENTITY.try_into().unwrap())],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let (context_worker_client_cert, context_worker_client_key) = issue(
        vec![SanType::URI(
            insight_platform_egress_rpc::CONTEXT_WORKER_WORKLOAD_IDENTITY
                .try_into()
                .unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let (subscription_worker_client_cert, subscription_worker_client_key) = issue(
        vec![SanType::URI(
            MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY
                .try_into()
                .unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let (discovery_worker_client_cert, discovery_worker_client_key) = issue(
        vec![SanType::URI(
            MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY.try_into().unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let (mcp_server_cert, mcp_server_key) = issue(
        vec![SanType::DnsName("mcp.example.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    ProcessTlsFixture {
        ca: ca.pem(),
        egress_server_cert,
        egress_server_key,
        artifact_server_cert,
        artifact_server_key,
        resource_host_server_cert,
        resource_host_server_key,
        resource_host_client_cert,
        resource_host_client_key,
        context_worker_client_cert,
        context_worker_client_key,
        subscription_worker_client_cert,
        subscription_worker_client_key,
        discovery_worker_client_cert,
        discovery_worker_client_key,
        mcp_server_cert,
        mcp_server_key,
    }
}

fn sandbox_policy_closure(
    token_purpose: SecretPurpose,
    write_storage_binding_digest: Sha256Digest,
) -> SandboxExecutionPolicyClosure {
    SandboxExecutionPolicyClosure {
        isolation: SandboxIsolationPolicyDocument {
            schema_version: 1,
            minimum_isolation: SandboxIsolationClass::SandboxedContainer,
            allowed_runtime_families: vec![SandboxRuntimeFamily::Python],
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
            maximum_result_bytes: 1_048_576,
            maximum_artifact_output_bytes: 0,
            maximum_network_connections: 0,
            maximum_network_request_bytes: 0,
            maximum_network_response_bytes: 0,
            maximum_startup_milliseconds: 30_000,
            maximum_idle_milliseconds: 60_000,
            maximum_wall_milliseconds: 300_000,
            maximum_cleanup_milliseconds: 10_000,
            maximum_wasm_fuel: None,
            maximum_wasm_memory_pages: None,
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
            allowed_input_media_types: vec!["application/octet-stream".to_owned()],
            allowed_output_media_types: vec![],
            maximum_input_artifacts: 8,
            maximum_output_artifacts: 0,
            scanner_contract_digest: named_digest("artifact-scanner-contract"),
            verification_evidence_ttl_milliseconds: 60_000,
            verification_retry_backoff_milliseconds: 1_000,
            write_storage_binding_digest,
            encryption_domain_id: id(ResourceKind::EncryptionDomain, 0x0f05),
            deny_symlink: true,
            deny_hardlink: true,
            deny_device: true,
            deny_fifo: true,
            deny_socket: true,
            deny_sparse_file: true,
            archive_expansion_disabled: true,
        },
        secret_resolution: Some(SandboxSecretResolutionPolicyDocument {
            schema_version: 1,
            allowed_purposes: vec![token_purpose],
            maximum_reads_per_binding: 1,
            delivery_mode: SandboxSecretDeliveryMode::BrokeredMemory,
            deny_environment: true,
            deny_argv: true,
            deny_snapshot: true,
            scrub_after_read: true,
        }),
    }
}

fn server_limits() -> McpServerLimits {
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

fn policy_document(
    suffix: u16,
    kind: PolicyKind,
    rules_digest: Sha256Digest,
    protocol: Option<McpProtocolPolicyDocument>,
    auth: Option<McpAuthPolicyDocument>,
) -> ResourceDocument {
    let mut document = PolicyResourceSpec {
        authoring_package: authoring(suffix, &format!("policy-{suffix}")),
        contract_digest: named_digest(&format!("policy-contract-{suffix}")),
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
        mcp_protocol: protocol,
        mcp_auth: auth.map(Box::new),
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        sandbox_secret_resolution: None,
    };
    let fixture_sandbox = sandbox_policy_closure(
        "fixture.policy.secret".parse::<SecretPurpose>().unwrap(),
        named_digest("artifact-write-storage-binding"),
    );
    match kind {
        PolicyKind::Network if document.mcp_protocol.is_none() => {
            document.rules_digest = fixture_sandbox.network.canonical_digest().unwrap();
            document.sandbox_network = Some(fixture_sandbox.network);
        }
        PolicyKind::Resource => {
            document.rules_digest = fixture_sandbox.resource.canonical_digest().unwrap();
            document.sandbox_resource = Some(fixture_sandbox.resource);
        }
        _ => {}
    }
    ResourceDocument::Policy(Box::new(document))
}

async fn insert_resource(
    pool: &PgPool,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
    kind: &str,
) {
    let payload = TypedPayload::new(1, &serde_json::json!({"fixture": true})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, $3, 'active', 'enabled', $4, $5, $6)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(kind)
    .bind(payload.schema_version)
    .bind(payload.value)
    .bind(payload.digest)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_version(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    resource_id: &ResourceId,
    exact: &ExactVersionRef,
    revision_no: i64,
    document: ResourceDocument,
) {
    document.validate().unwrap_or_else(|failure| {
        panic!(
            "invalid fixture ResourceVersion {} revision {}: {failure}",
            exact.revision_id, revision_no
        )
    });
    let payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document,
            validation: validation(&format!("version-{revision_no}")),
        },
    )
    .unwrap();
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
    .bind(payload.value)
    .bind(payload.digest)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_deployment(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    deployment_id: &ResourceId,
    resource_id: &ResourceId,
    resource_version_id: &ResourceId,
    closure: &DeploymentClosure,
) -> ExactDeploymentRef {
    let payload = TypedPayload::new(1, closure).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.deployments (
            tenant_id, deployment_id, resource_id, resource_version_id,
            environment, bindings_digest, payload_schema_version, bindings, created_by
        ) VALUES ($1, $2, $3, $4, 'fixture', $5, $6, $7, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(deployment_id.to_string())
    .bind(resource_id.to_string())
    .bind(resource_version_id.to_string())
    .bind(&payload.digest)
    .bind(payload.schema_version)
    .bind(payload.value)
    .bind(principal_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    ExactDeploymentRef::new(deployment_id.clone(), payload.digest.parse().unwrap()).unwrap()
}

async fn activate_deployment(
    pool: &PgPool,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
    deployment_id: &ResourceId,
) {
    sqlx::query(
        r#"
        UPDATE insight_platform.resources
        SET active_version_id = NULL, active_deployment_id = $3,
            version = version + 1, updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND resource_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(deployment_id.to_string())
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_ready_artifact(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    reference: &ArtifactRef,
    purpose: ArtifactPurpose,
    retention_revision: &ExactVersionRef,
) {
    let blob_id =
        ResourceId::from_uuid_v7(ResourceKind::InternalBlob, reference.artifact_id().uuid())
            .unwrap();
    let now = Utc::now();
    let metadata = TypedPayload::new(
        1,
        &serde_json::json!({"display_name": reference.display_name()}),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest, security_domain_digest,
            object_reference_ciphertext, object_generation, key_id, encryption_domain_id,
            content_digest, size_bytes, state, version, verified_at, created_at, updated_at
        ) VALUES ($1, $2, 'fixture', $3, $4, $5, 'generation-1', 'fixture-key', $6,
                  $7, $8, 'verified', 1, $9, $9, $9)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .bind(named_digest("storage").to_string())
    .bind(named_digest("security-domain").to_string())
    .bind(vec![1_u8, 2, 3])
    .bind(id(ResourceKind::EncryptionDomain, 0x171).to_string())
    .bind(reference.content_digest().to_string())
    .bind(i64::try_from(reference.byte_length()).unwrap())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification, expected_size_bytes,
            expected_digest, declared_media_type, verified_media_type, state, version,
            metadata_schema_version, metadata, metadata_digest, retention_policy_revision_id,
            retain_until, created_by, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, 'ready', 1,
                  $9, $10, $11, $12, $13, $14, $15, $15)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(reference.artifact_id().to_string())
    .bind(blob_id.to_string())
    .bind(purpose.as_str())
    .bind(reference.classification().as_str())
    .bind(i64::try_from(reference.byte_length()).unwrap())
    .bind(reference.content_digest().to_string())
    .bind(reference.media_type())
    .bind(metadata.schema_version)
    .bind(metadata.value)
    .bind(metadata.digest)
    .bind(retention_revision.revision_id.to_string())
    .bind(now + Duration::days(1))
    .bind(principal_id.to_string())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
}

struct Fixture {
    tenant_id: ResourceId,
    other_tenant_id: ResourceId,
    principal_id: ResourceId,
    mcp_deployment: ExactDeploymentRef,
    context_deployment: ExactDeploymentRef,
    authorization: McpAuthorizationBindingRecord,
    snapshot: McpDiscoverySnapshot,
    subscription_id: ResourceId,
    job_id: ResourceId,
    deadline: DateTime<Utc>,
    mcp_endpoint: insight_platform_contracts::CanonicalHttpEndpoint,
    protocol_policy: ExactVersionRef,
    network_policy: ExactVersionRef,
    tls_policy: ExactVersionRef,
    trust_policy: ExactVersionRef,
    auth_policy: ExactVersionRef,
    token_purpose: SecretPurpose,
}

async fn seed(
    pool: &PgPool,
    repository: &PgRepository,
    now: DateTime<Utc>,
    mcp_port: u16,
    write_storage_binding_digest: Sha256Digest,
) -> Fixture {
    let tenant_id = id(ResourceKind::Tenant, 1);
    let other_tenant_id = id(ResourceKind::Tenant, 2);
    let principal_id = id(ResourceKind::Principal, 3);
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
    repository
        .create_principal(NewPrincipal {
            principal_id: principal_id.clone(),
            authentication_authority_digest: named_digest("auth-authority"),
            subject_digest: named_digest("subject"),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    for tenant in [&tenant_id, &other_tenant_id] {
        repository
            .bind_tenant_principal(NewTenantPrincipal {
                tenant_id: tenant.clone(),
                principal_id: principal_id.clone(),
                principal_kind: PrincipalKind::AgentRunner,
                payload: TenantPrincipalPayload {
                    permissions: PermissionSet::new(vec![
                        Permission::ContextRead,
                        Permission::McpRead,
                        Permission::McpWrite,
                    ])
                    .unwrap(),
                },
            })
            .await
            .unwrap();
    }

    let policy_resource = id(ResourceKind::Policy, 0x10);
    let retention_policy_resource = id(ResourceKind::Policy, 0x14);
    let artifact_io_policy_resource = id(ResourceKind::Policy, 0x15);
    let server_resource = id(ResourceKind::McpServer, 0x11);
    let context_interface_resource = id(ResourceKind::ContextSourceInterface, 0x12);
    let context_implementation_resource = id(ResourceKind::ContextSourceImplementation, 0x13);
    for (resource, kind) in [
        (&policy_resource, RegistryResourceKind::Policy),
        (&retention_policy_resource, RegistryResourceKind::Policy),
        (&artifact_io_policy_resource, RegistryResourceKind::Policy),
        (&server_resource, RegistryResourceKind::McpServer),
        (
            &context_interface_resource,
            RegistryResourceKind::ContextSourceInterface,
        ),
        (
            &context_implementation_resource,
            RegistryResourceKind::ContextSourceImplementation,
        ),
    ] {
        insert_resource(pool, &tenant_id, resource, kind.as_str()).await;
    }

    let protocol = protocol_document();
    let protocol_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x20),
        protocol.canonical_digest().unwrap(),
    )
    .unwrap();
    let mut resource_indicator = oauth_endpoint("mcp.example.test", "/mcp");
    resource_indicator.endpoint.port = mcp_port;
    resource_indicator.endpoint_identity_digest =
        resource_indicator.endpoint.canonical_digest().unwrap();
    let auth_profile = McpAuthPolicyDocument {
        schema_version: 1,
        issuer: oauth_endpoint("auth.example.test", "/"),
        authorization_endpoint: oauth_endpoint("auth.example.test", "/oauth/authorize"),
        token_endpoint: oauth_endpoint("auth.example.test", "/oauth/token"),
        client_id: "insight-platform".to_owned(),
        client_authentication: McpOAuthClientAuthenticationKind::None,
        client_credential_purpose: None,
        pkce_secret_provider_id: id(ResourceKind::SecretProvider, 0x2d),
        token_secret_provider_id: id(ResourceKind::SecretProvider, 0x2e),
        redirect_uri: oauth_endpoint("platform.example.test", "/v1/mcp/oauth/callback"),
        resource_indicator,
        allowed_scopes: vec![
            "resources.read".to_owned(),
            "resources.subscribe".to_owned(),
        ],
        maximum_token_response_bytes: 65_536,
        connect_timeout_milliseconds: 5_000,
        total_timeout_milliseconds: 30_000,
        maximum_clock_skew_seconds: 60,
    };
    let auth_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x21),
        auth_profile.canonical_digest().unwrap(),
    )
    .unwrap();
    let network_policy = exact(ResourceKind::PolicyRevision, 0x22, "network");
    let tls_policy = exact(ResourceKind::PolicyRevision, 0x23, "tls");
    let trust_policy = exact(ResourceKind::PolicyRevision, 0x24, "trust");
    let uri_policy = exact(ResourceKind::PolicyRevision, 0x25, "uri");
    let parser_policy = exact(ResourceKind::PolicyRevision, 0x26, "parser");
    let ranking_policy = exact(ResourceKind::PolicyRevision, 0x27, "ranking");
    let data_policy = exact(ResourceKind::PolicyRevision, 0x28, "data");
    let cache_policy = exact(ResourceKind::PolicyRevision, 0x29, "cache");
    let chunker_policy = exact(ResourceKind::PolicyRevision, 0x2a, "chunker");
    let retention = ArtifactRetentionPolicy {
        version: 1,
        minimum_retention_seconds: 3_600,
        gc_grace_seconds: 3_600,
        tombstone_retention_seconds: 86_400,
        retain_provenance_sources: true,
        delete_requires_approval: false,
    };
    let retention_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x2b),
        retention.canonical_digest().unwrap(),
    )
    .unwrap();
    let artifact_io = sandbox_policy_closure(
        "fixture.policy.secret".parse::<SecretPurpose>().unwrap(),
        write_storage_binding_digest,
    )
    .artifact_io;
    let artifact_io_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x2c),
        artifact_io.canonical_digest().unwrap(),
    )
    .unwrap();
    let policy_documents = vec![
        (
            protocol_policy.clone(),
            policy_document(
                0x80,
                PolicyKind::Protocol,
                protocol_policy.semantic_digest.clone(),
                Some(protocol),
                None,
            ),
        ),
        (
            auth_policy.clone(),
            policy_document(
                0x81,
                PolicyKind::McpAuth,
                auth_policy.semantic_digest.clone(),
                None,
                Some(auth_profile.clone()),
            ),
        ),
        (
            network_policy.clone(),
            policy_document(
                0x82,
                PolicyKind::Network,
                named_digest("network"),
                None,
                None,
            ),
        ),
        (
            tls_policy.clone(),
            policy_document(0x83, PolicyKind::Tls, named_digest("tls"), None, None),
        ),
        (
            trust_policy.clone(),
            policy_document(0x84, PolicyKind::Trust, named_digest("trust"), None, None),
        ),
        (
            uri_policy.clone(),
            policy_document(0x85, PolicyKind::Resource, named_digest("uri"), None, None),
        ),
        (
            parser_policy.clone(),
            policy_document(0x86, PolicyKind::Parser, named_digest("parser"), None, None),
        ),
        (
            ranking_policy.clone(),
            policy_document(
                0x87,
                PolicyKind::Ranking,
                named_digest("ranking"),
                None,
                None,
            ),
        ),
        (
            data_policy.clone(),
            policy_document(0x88, PolicyKind::DataFlow, named_digest("data"), None, None),
        ),
        (
            cache_policy.clone(),
            policy_document(
                0x89,
                PolicyKind::DataFlow,
                named_digest("cache"),
                None,
                None,
            ),
        ),
        (
            chunker_policy.clone(),
            policy_document(
                0x8a,
                PolicyKind::Chunker,
                named_digest("chunker"),
                None,
                None,
            ),
        ),
    ];
    for (index, (reference, document)) in policy_documents.into_iter().enumerate() {
        insert_version(
            pool,
            &tenant_id,
            &principal_id,
            &policy_resource,
            &reference,
            i64::try_from(index + 1).unwrap(),
            document,
        )
        .await;
    }
    insert_version(
        pool,
        &tenant_id,
        &principal_id,
        &retention_policy_resource,
        &retention_policy,
        1,
        ResourceDocument::Policy(Box::new(PolicyResourceSpec {
            authoring_package: authoring(0x8b, "retention-policy"),
            contract_digest: named_digest("retention-policy-contract"),
            dependency_versions: vec![],
            policy_versions: vec![],
            policy_kind: PolicyKind::Retention,
            rules_digest: retention.canonical_digest().unwrap(),
            selection: None,
            scheduling: None,
            retention: Some(retention),
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
    )
    .await;
    insert_version(
        pool,
        &tenant_id,
        &principal_id,
        &artifact_io_policy_resource,
        &artifact_io_policy,
        1,
        ResourceDocument::Policy(Box::new(PolicyResourceSpec {
            authoring_package: authoring(0x8c, "artifact-io-policy"),
            contract_digest: named_digest("artifact-io-policy-contract"),
            dependency_versions: vec![],
            policy_versions: vec![],
            policy_kind: PolicyKind::ArtifactIo,
            rules_digest: artifact_io.canonical_digest().unwrap(),
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
            sandbox_artifact_io: Some(artifact_io),
            sandbox_secret_resolution: None,
        })),
    )
    .await;

    let retention_deployment_id = id(ResourceKind::PolicyDeployment, 0x4b);
    let retention_deployment = insert_deployment(
        pool,
        &tenant_id,
        &principal_id,
        &retention_deployment_id,
        &retention_policy_resource,
        &retention_policy.revision_id,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: retention_policy.clone(),
            applicability_digest: named_digest("retention-applicability"),
            qualification_evidence: artifact(0x10b, "retention-qualification"),
        }),
    )
    .await;
    activate_deployment(
        pool,
        &tenant_id,
        &retention_policy_resource,
        &retention_deployment_id,
    )
    .await;
    let artifact_io_deployment_id = id(ResourceKind::PolicyDeployment, 0x4c);
    let artifact_io_deployment = insert_deployment(
        pool,
        &tenant_id,
        &principal_id,
        &artifact_io_deployment_id,
        &artifact_io_policy_resource,
        &artifact_io_policy.revision_id,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: artifact_io_policy.clone(),
            applicability_digest: named_digest("artifact-io-applicability"),
            qualification_evidence: artifact(0x10c, "artifact-io-qualification"),
        }),
    )
    .await;
    activate_deployment(
        pool,
        &tenant_id,
        &artifact_io_policy_resource,
        &artifact_io_deployment_id,
    )
    .await;
    let tenant_config = TypedPayload::new(
        1,
        &TenantConfig {
            scheduling_policy: None,
            artifact_retention_policy: Some(retention_deployment),
            artifact_io_policy: Some(artifact_io_deployment),
        },
    )
    .unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.tenants
        SET config_schema_version = $2, config = $3, config_digest = $4,
            version = version + 1, updated_at = clock_timestamp()
        WHERE tenant_id = $1
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(tenant_config.schema_version)
    .bind(tenant_config.value)
    .bind(tenant_config.digest)
    .execute(pool)
    .await
    .unwrap();
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: tenant_id.to_string(),
            quota_account_id: id(ResourceKind::QuotaAccount, 0x4d).to_string(),
            scope_kind: "tenant".to_owned(),
            scope_id: tenant_id.to_string(),
            work_class: WorkClass::Artifact.as_str().to_owned(),
            metric: "artifact.staging_bytes".to_owned(),
            limit_value: 4_194_304,
            payload: TypedPayload::new(1, &serde_json::json!({"fixture": "mcp-discovery"}))
                .unwrap(),
        })
        .await
        .unwrap();

    let server_revision = exact(ResourceKind::McpServerRevision, 0x30, "server");
    let token_purpose = "mcp.oauth.token".parse::<SecretPurpose>().unwrap();
    insert_version(
        pool,
        &tenant_id,
        &principal_id,
        &server_resource,
        &server_revision,
        1,
        ResourceDocument::McpServer(McpServerResourceSpec {
            authoring_package: authoring(0x90, "server"),
            contract_digest: named_digest("server-contract"),
            dependency_versions: vec![],
            policy_versions: vec![],
            transport: insight_platform_contracts::McpTransportKind::StreamableHttp,
            protocol_policy: protocol_policy.clone(),
            deployment_credential_requirements: vec![],
            authorization_credential_purpose: Some(token_purpose.clone()),
            limits: server_limits(),
        }),
    )
    .await;

    let interface_revision = exact(
        ResourceKind::ContextSourceInterfaceRevision,
        0x31,
        "context-interface",
    );
    insert_version(
        pool,
        &tenant_id,
        &principal_id,
        &context_interface_resource,
        &interface_revision,
        1,
        ResourceDocument::ContextSourceInterface(ContextInterfaceResourceSpec {
            authoring_package: authoring(0x91, "context-interface"),
            contract_digest: named_digest("context-interface-contract"),
            dependency_versions: vec![],
            policy_versions: vec![],
            query_schema_digest: named_digest("query-schema"),
            filter_schema_digest: named_digest("filter-schema"),
            item_schema_digest: named_digest("item-schema"),
            observation_schema_digest: named_digest("observation-schema"),
            allowed_consistency: vec![ContextConsistencyMode::ExternalObservation],
            citation: ContextCitationContract {
                allowed_strengths: vec![ContextCitationStrength::ObservationOnly],
                locator_kinds: vec![ContextLocatorKind::RemoteOpaque],
                require_content_digest: true,
                maximum_display_label_bytes: 256,
            },
            pagination: ContextPaginationContract {
                maximum_page_size: 100,
                maximum_cursor_bytes: 1_024,
                cursor_ttl_milliseconds: 60_000,
            },
            ranking: ContextRankingContract {
                score_domain_digest: named_digest("score-domain"),
                reranker_contract_digest: None,
                maximum_candidates: 1_000,
            },
            data_policy: ContextDataPolicyContract {
                maximum_classification: DataClassification::Confidential,
                allowed_regions: vec!["cn-east-1".parse::<DataRegion>().unwrap()],
                entitlement_policy: data_policy.clone(),
                cache_policy,
                maximum_retention_milliseconds: 86_400_000,
            },
            limits: ContextInterfaceLimits {
                maximum_query_bytes: 4_096,
                maximum_filter_bytes: 4_096,
                maximum_item_bytes: 65_536,
                maximum_total_bytes: 1_048_576,
                maximum_items: 100,
                maximum_fan_out: 4,
            },
        }),
    )
    .await;
    let implementation_revision = exact(
        ResourceKind::ContextSourceImplementationRevision,
        0x32,
        "context-implementation",
    );
    insert_version(
        pool,
        &tenant_id,
        &principal_id,
        &context_implementation_resource,
        &implementation_revision,
        1,
        ResourceDocument::ContextSourceImplementation(ContextImplementationResourceSpec {
            authoring_package: authoring(0x92, "context-implementation"),
            contract_digest: named_digest("context-implementation-contract"),
            dependency_versions: vec![interface_revision.clone()],
            policy_versions: vec![],
            interface_revision: interface_revision.clone(),
            backend_kind: ContextBackendKind::McpResources,
            contract: ContextImplementationContract {
                backend: ContextBackendContract::McpResources {
                    resource_contract_digest: named_digest("mcp-resource-contract"),
                    uri_policy,
                },
                credential_requirements: vec![],
                limits: ContextBackendLimits {
                    maximum_request_bytes: 65_536,
                    maximum_response_bytes: 1_048_576,
                    maximum_candidates: 1_000,
                    maximum_remote_state_bytes: 65_536,
                    maximum_poll_count: 16,
                    total_timeout_milliseconds: 120_000,
                },
            },
        }),
    )
    .await;

    let objects = artifact(0x100, "mcp-discovery-objects");
    insert_ready_artifact(
        pool,
        &tenant_id,
        &principal_id,
        &objects,
        ArtifactPurpose::McpResource,
        &trust_policy,
    )
    .await;
    let mut mcp_endpoint = endpoint("mcp.example.test", "/mcp");
    mcp_endpoint.port = mcp_port;
    let mcp_closure = insight_platform_contracts::McpDeploymentClosure {
        server_revision: server_revision.clone(),
        server_identity_digest: mcp_endpoint.canonical_digest().unwrap(),
        transport: McpTransportBinding::StreamableHttp {
            endpoint_identity_digest: mcp_endpoint.canonical_digest().unwrap(),
            endpoint: mcp_endpoint.clone(),
            network_policy: network_policy.clone(),
            tls_policy: tls_policy.clone(),
        },
        protocol_policy: protocol_policy.clone(),
        trust_policy: trust_policy.clone(),
        auth_policy: Some(auth_policy.clone()),
        secret_bindings: vec![],
        conformance_evidence: objects.clone(),
    };
    let mcp_deployment = insert_deployment(
        pool,
        &tenant_id,
        &principal_id,
        &id(ResourceKind::McpDeployment, 0x40),
        &server_resource,
        &server_revision.revision_id,
        &DeploymentClosure::McpServer(mcp_closure.clone()),
    )
    .await;

    let token_binding = ExactSecretBindingRef::build(
        id(ResourceKind::SecretBinding, 0x41),
        1,
        id(ResourceKind::SecretProvider, 0x2e),
        token_purpose.clone(),
        SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: named_digest("token-version"),
        },
    )
    .unwrap();
    repository
        .create_secret_binding(NewSecretBinding {
            tenant_id: tenant_id.clone(),
            secret_binding_id: token_binding.secret_binding_id.clone(),
            purpose: token_purpose.clone(),
            provider_id: token_binding.provider_id.clone(),
            opaque_reference_ciphertext: vec![9, 8, 7],
            key_id: "fixture-key".to_owned(),
            reference_digest: named_digest("token-reference"),
            payload: SecretBindingPayload {
                provider_id: token_binding.provider_id.clone(),
                resolution_policy: token_binding.resolution_policy.clone(),
            },
        })
        .await
        .unwrap();
    let authorization = McpAuthorizationBindingRecord::create(
        NewMcpAuthorizationBinding {
            tenant_id: tenant_id.clone(),
            authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 0x43),
            mcp_deployment: mcp_deployment.clone(),
            principal_kind: McpAuthorizationPrincipalKind::PerUser,
            principal_id: principal_id.clone(),
            principal_identity_kind: PrincipalKind::AgentRunner,
            principal_binding_generation: 1,
            audience_identity_digest: mcp_closure.server_identity_digest.clone(),
            granted_scopes: vec![
                "resources.read".to_owned(),
                "resources.subscribe".to_owned(),
            ],
            token_secret_binding: token_binding,
            expires_at: now + Duration::hours(4),
        },
        now,
    )
    .unwrap();
    let authorization_payload = TypedPayload::from_versioned(1, &authorization, 262_144).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            version, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'mcp_authorization_binding', 'active', 'enabled',
                  1, $3, $4, $5)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(authorization.authorization_binding_id.to_string())
    .bind(authorization_payload.schema_version)
    .bind(authorization_payload.value)
    .bind(authorization_payload.digest)
    .execute(pool)
    .await
    .unwrap();

    let authorization_context = authorization.execution_context(now).unwrap();
    let source_operation_id = id(ResourceKind::McpOperation, 0x44);
    let source_job_id = id(ResourceKind::Job, 0x45);
    let snapshot_id = id(ResourceKind::McpDiscoverySnapshot, 0x46);
    let artifact_link_id = id(ResourceKind::ArtifactLink, 0x47);
    let snapshot = McpDiscoverySnapshot::build(
        snapshot_id.clone(),
        mcp_deployment.clone(),
        server_revision.clone(),
        protocol_policy.clone(),
        authorization_context.canonical_digest.clone(),
        MCP_PROTOCOL_BASELINE.to_owned(),
        McpNegotiatedCapabilities {
            tools: false,
            resources: true,
            prompts: false,
            logging: false,
            tasks: false,
            tasks_list: false,
            tasks_cancel: false,
            tasks_tools_call: false,
            elicitation: false,
            sampling: false,
            roots: false,
            subscriptions: true,
        },
        objects.clone(),
        now - Duration::seconds(2),
        now + Duration::hours(2),
    )
    .unwrap();
    let admission = McpDiscoveryAdmission::build(NewMcpDiscoveryAdmission {
        operation_id: source_operation_id.clone(),
        job_id: source_job_id,
        tenant_id: tenant_id.clone(),
        mcp_deployment: mcp_deployment.clone(),
        server_revision: server_revision.clone(),
        protocol_profile: protocol_policy.clone(),
        authorization_binding_id: authorization.authorization_binding_id.clone(),
        authorization_generation: authorization.generation,
        authorization_context_digest: authorization_context.canonical_digest.clone(),
        principal_id: principal_id.clone(),
        artifact_preallocation:
            insight_platform_mcp_host::McpDiscoveryArtifactPreallocation::build(
                objects.artifact_id().clone(),
                id(ResourceKind::InternalBlob, 0x701),
                id(ResourceKind::Job, 0x702),
                artifact_link_id.clone(),
                snapshot_id.clone(),
                id(ResourceKind::QuotaLedgerEntry, 0x705),
            )
            .unwrap(),
        artifact_policy: discovery_artifact_policy(0x710, now),
        requested_at: now - Duration::seconds(3),
        deadline: now + Duration::hours(2),
    })
    .unwrap();
    let preallocation = admission.artifact_preallocation.clone();
    let mut transport_evidence = McpDiscoveryTransportEvidence {
        schema_version: 1,
        negotiated_version: snapshot.negotiated_version.clone(),
        negotiated_capabilities: snapshot.negotiated_capabilities.clone(),
        descriptor_digest: objects.content_digest().clone(),
        descriptor_size_bytes: objects.byte_length(),
        descriptor_count: 1,
        verification_job_id: preallocation.verification_job_id,
        artifact_id: preallocation.artifact_id,
        blob_id: preallocation.blob_id,
        observed_at: now - Duration::seconds(2),
        expires_at: now + Duration::hours(1),
        canonical_digest: named_digest("pending-discovery-transport-evidence"),
    };
    transport_evidence.canonical_digest =
        digest_without_field(&transport_evidence, "canonical_digest");
    let operation_payload = McpDiscoveryOperationPayload::pending(admission)
        .unwrap()
        .park_for_verification(transport_evidence)
        .unwrap()
        .complete(McpDiscoveryResultBinding {
            snapshot_id: snapshot_id.clone(),
            snapshot_digest: snapshot.canonical_digest.clone(),
            objects_artifact: objects.clone(),
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
        r#"
        INSERT INTO insight_platform.invocations (
            tenant_id, invocation_id, invocation_kind, owner_kind, owner_id,
            logical_key, deployment_id, state, payload_schema_version, payload,
            payload_digest, deadline, terminal_at, created_at, updated_at, trace_id
        ) VALUES ($1, $2, 'mcp_discovery', 'mcp_operation', $2,
                  'fixture-discovery', $3, 'succeeded', $4, $5, $6, $7, $8, $8, $8, $9)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(source_operation_id.to_string())
    .bind(mcp_deployment.deployment_id.to_string())
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
        artifact_id: objects.artifact_id().clone(),
        owner_id: source_operation_id.clone(),
        reference_kind: ArtifactReferenceKind::Evidence,
        purpose: ArtifactPurpose::McpResource,
        created_by: principal_id.clone(),
    };
    let link_payload = TypedPayload::from_versioned(1, &reference, 262_144).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest
        ) VALUES ($1, $2, 'reference', 'mcp_operation', $3,
                  $4, $5, 'active', $6, $7, $8)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_link_id.to_string())
    .bind(source_operation_id.to_string())
    .bind(objects.artifact_id().to_string())
    .bind(reference.link_key_digest().unwrap().to_string())
    .bind(link_payload.schema_version)
    .bind(link_payload.value)
    .bind(link_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let snapshot_record = McpDiscoverySnapshotRecord::build(NewMcpDiscoverySnapshotRecord {
        tenant_id: tenant_id.clone(),
        source_operation_id,
        artifact_link_id,
        snapshot: snapshot.clone(),
        completed_at: now,
    })
    .unwrap();
    let snapshot_payload = TypedPayload::from_versioned(1, &snapshot_record, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            version, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'mcp_discovery_snapshot', 'active', 'enabled',
                  1, $3, $4, $5)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(snapshot_id.to_string())
    .bind(snapshot_payload.schema_version)
    .bind(snapshot_payload.value)
    .bind(snapshot_payload.digest)
    .execute(pool)
    .await
    .unwrap();

    let context_closure = ContextDeploymentClosure {
        implementation: implementation_revision,
        interface: interface_revision.clone(),
        required_worker_manifest_digest: context_worker_manifest_digest(),
        backend: ContextBackendBinding::McpResources {
            mcp_deployment: mcp_deployment.clone(),
            discovery_snapshot_id: snapshot.snapshot_id.clone(),
            discovery_snapshot_digest: snapshot.canonical_digest.clone(),
        },
        secret_bindings: vec![],
        network_policy: None,
        tls_policy: None,
        trust_policy: None,
        parser_policy,
        chunker_policy,
        embedding_model_deployment: None,
        ranking_policy,
        data_policy,
        conformance_evidence: objects,
    };
    let context_deployment = insert_deployment(
        pool,
        &tenant_id,
        &principal_id,
        &id(ResourceKind::ContextDeployment, 0x48),
        &context_interface_resource,
        &interface_revision.revision_id,
        &DeploymentClosure::ContextSourceInterface(context_closure),
    )
    .await;

    let deadline =
        DateTime::from_timestamp((now + Duration::hours(1)).timestamp(), 987_654_321).unwrap();
    assert_ne!(deadline.timestamp_subsec_nanos() % 1_000, 0);
    Fixture {
        tenant_id,
        other_tenant_id,
        principal_id,
        mcp_deployment,
        context_deployment,
        authorization,
        snapshot,
        subscription_id: id(ResourceKind::McpOperation, 0x49),
        job_id: id(ResourceKind::Job, 0x4a),
        deadline,
        mcp_endpoint,
        protocol_policy,
        network_policy,
        tls_policy,
        trust_policy,
        auth_policy,
        token_purpose,
    }
}

fn command_audit(
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    base: u16,
    now: DateTime<Utc>,
) -> CommandAudit {
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: named_digest(&format!("command-key-{base}")),
        request_digest: named_digest("placeholder"),
        receipt_expires_at: now + Duration::hours(2),
    }
}

fn create_command(fixture: &Fixture, now: DateTime<Utc>) -> CreateMcpResourceSubscription {
    let context = fixture.authorization.execution_context(now).unwrap();
    let mut command = CreateMcpResourceSubscription {
        audit: command_audit(&fixture.tenant_id, &fixture.principal_id, 0x200, now),
        subscription_id: fixture.subscription_id.clone(),
        job_id: fixture.job_id.clone(),
        logical_key: "fixture-resource-subscription".to_owned(),
        execution: McpExecutionContractQuery {
            schema_version: 1,
            tenant_id: fixture.tenant_id.clone(),
            mcp_deployment: fixture.mcp_deployment.clone(),
            discovery_snapshot_id: fixture.snapshot.snapshot_id.clone(),
            discovery_snapshot_digest: fixture.snapshot.canonical_digest.clone(),
            authorization_binding_id: fixture.authorization.authorization_binding_id.clone(),
            authorization_generation: fixture.authorization.generation,
            authorization_context_digest: context.canonical_digest,
            principal_id: fixture.principal_id.clone(),
        },
        context_deployment: fixture.context_deployment.clone(),
        resource_uri: "mcp://catalog.example.test/items/42".to_owned(),
        // The fixture proves four in-process attempts before exercising a production Worker
        // crash/recovery pair, which requires two further physical attempts.
        attempt_limit: 6,
        deadline: fixture.deadline,
    };
    command.audit.request_digest = command.request_digest().unwrap();
    command
}

async fn create_subscription(
    repository: &PgRepository,
    command: CreateMcpResourceSubscription,
) -> Result<CommandOutcome<McpSubscriptionRecord>, RepositoryError> {
    let mut transaction = repository.begin_registry_transaction().await.unwrap();
    match transaction.create_mcp_resource_subscription(command).await {
        Ok(outcome) => {
            transaction.commit().await.unwrap();
            Ok(outcome)
        }
        Err(failure) => {
            transaction.rollback().await.unwrap();
            Err(failure)
        }
    }
}

async fn claim_and_start(
    repository: &PgRepository,
    fixture: &Fixture,
    worker_id: &ResourceId,
    token_domain: &str,
) -> DomainJobFence {
    let tokens = (0..16)
        .map(|index| named_digest(&format!("{token_domain}-{index}")))
        .collect::<Vec<_>>();
    let claimed = repository
        .claim_jobs(ClaimJobs {
            work_class: "mcp".to_owned(),
            worker_id: worker_id.clone(),
            limit: u16::try_from(tokens.len()).unwrap(),
            lease_milliseconds: 60_000,
            lease_token_digests: tokens,
        })
        .await
        .unwrap()
        .into_iter()
        .find(|job| {
            job.tenant_id == fixture.tenant_id.to_string()
                && job.job_id == fixture.job_id.to_string()
        })
        .expect("subscription Job must be claimed");
    let token = claimed
        .lease_token_digest
        .as_ref()
        .unwrap()
        .parse::<Sha256Digest>()
        .unwrap();
    let started = repository
        .start_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: fixture.job_id.to_string(),
            worker_id: worker_id.clone(),
            lease_epoch: claimed.lease_epoch,
            expected_job_version: claimed.version,
            lease_token_digest: token.clone(),
        })
        .await
        .unwrap();
    DomainJobFence {
        expected_version: u64::try_from(started.version).unwrap(),
        worker_process_generation_id: worker_id.clone(),
        lease_generation: u64::try_from(started.lease_epoch).unwrap(),
        token_digest: token,
    }
}

fn worker_audit(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    base: u16,
    now: DateTime<Utc>,
) -> McpSubscriptionWorkerAudit {
    McpSubscriptionWorkerAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: tenant_id.clone(),
        worker_process_generation_id: worker_id.clone(),
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: named_digest(&format!("worker-key-{base}")),
        request_digest: named_digest("placeholder"),
        receipt_expires_at: now + Duration::hours(2),
    }
}

struct SessionCommandInput {
    target: McpSessionState,
    opaque: Option<EncryptedMcpState>,
    expires_at: Option<DateTime<Utc>>,
    base: u16,
    now: DateTime<Utc>,
}

fn session_command(
    fixture: &Fixture,
    worker_id: &ResourceId,
    fence: DomainJobFence,
    record: &McpSubscriptionRecord,
    input: SessionCommandInput,
) -> SaveMcpSubscriptionSession {
    let mut command = SaveMcpSubscriptionSession {
        audit: worker_audit(&fixture.tenant_id, worker_id, input.base, input.now),
        subscription_id: fixture.subscription_id.clone(),
        job_id: fixture.job_id.clone(),
        fence,
        expected_subscription_version: record.version,
        expected_session_version: record.payload.session.version,
        target: input.target,
        encrypted_opaque_session: input.opaque,
        expires_at: input.expires_at,
        phase_evidence_digest: matches!(
            input.target,
            McpSessionState::Ready
                | McpSessionState::Degraded
                | McpSessionState::ReauthRequired
                | McpSessionState::Failed
        )
        .then(|| named_digest("session-phase-evidence")),
    };
    command.audit.request_digest = command.request_digest().unwrap();
    command
}

fn notification(
    fixture: &Fixture,
    session_generation: u64,
    event_generation: u64,
    base: u16,
    now: DateTime<Utc>,
) -> McpNotificationCommit {
    McpNotificationCommit {
        audit: McpNotificationAudit {
            tenant_id: fixture.tenant_id.clone(),
            receipt_id: id(ResourceKind::Receipt, base),
            event_id: id(ResourceKind::Event, base + 1),
            outbox_id: id(ResourceKind::OutboxEvent, base + 2),
            receipt_expires_at: now + Duration::hours(2),
        },
        tenant_id: fixture.tenant_id.clone(),
        subscription_id: fixture.subscription_id.clone(),
        authorization_generation: fixture.authorization.generation,
        session_generation,
        event_key_digest: named_digest(&format!("notification-key-{event_generation}")),
        event_generation,
        class: McpNotificationClass::ResourceUpdated,
        resource_uri_digest: Some(named_digest("resource-uri-placeholder")),
        body_digest: named_digest(&format!("notification-body-{event_generation}")),
        wire_bytes: 512,
        received_at: now,
    }
}

fn context_refresh_admission(
    record: &McpSubscriptionRecord,
    base: u16,
) -> AdmitContextSubscriptionRefresh {
    let pending = record.payload.pending_invalidation.as_ref().unwrap();
    let binding = &record.payload.binding;
    let cause = match pending.class {
        McpNotificationClass::ResourceUpdated => ContextSubscriptionRefreshCause::ResourceUpdated,
        McpNotificationClass::ResourceListChanged => {
            ContextSubscriptionRefreshCause::ResourceListChanged
        }
        McpNotificationClass::ToolListChanged => ContextSubscriptionRefreshCause::ToolListChanged,
        McpNotificationClass::PromptListChanged => {
            ContextSubscriptionRefreshCause::PromptListChanged
        }
    };
    let mut request = ContextSubscriptionRefreshRequest {
        schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
        tenant_id: record.tenant_id.clone(),
        subscription_id: record.subscription_id.clone(),
        context_deployment: binding.context_deployment.clone(),
        mcp_deployment: binding.mcp_deployment.clone(),
        discovery_snapshot_id: binding.discovery_snapshot_id.clone(),
        discovery_snapshot_digest: binding.discovery_snapshot_digest.clone(),
        resource_uri: binding.resource_uri.clone(),
        resource_uri_digest: binding.resource_uri_digest.clone(),
        authorization_generation: binding.authorization_generation,
        session_generation: pending.session_generation,
        event_generation: pending.event_generation,
        event_key_digest: pending.event_key_digest.clone(),
        body_digest: pending.body_digest.clone(),
        cause,
        deadline: record.deadline,
        request_digest: named_digest("context-refresh-placeholder"),
    };
    request.request_digest = request.canonical_request_digest().unwrap();
    AdmitContextSubscriptionRefresh {
        request,
        audit: ContextSubscriptionAdmissionAudit {
            schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
            request_id: id(ResourceKind::ServerRequest, base),
            correlation_digest: named_digest(&format!("context-refresh-correlation-{base}")),
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
        },
    }
}

fn context_reconcile_admission(
    record: &McpSubscriptionRecord,
    base: u16,
) -> AdmitContextSubscriptionRefresh {
    let binding = &record.payload.binding;
    let mut request = ContextSubscriptionRefreshRequest {
        schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
        tenant_id: record.tenant_id.clone(),
        subscription_id: record.subscription_id.clone(),
        context_deployment: binding.context_deployment.clone(),
        mcp_deployment: binding.mcp_deployment.clone(),
        discovery_snapshot_id: binding.discovery_snapshot_id.clone(),
        discovery_snapshot_digest: binding.discovery_snapshot_digest.clone(),
        resource_uri: binding.resource_uri.clone(),
        resource_uri_digest: binding.resource_uri_digest.clone(),
        authorization_generation: binding.authorization_generation,
        session_generation: record.payload.session.generation,
        event_generation: record.version,
        event_key_digest: binding.resource_uri_digest.clone(),
        body_digest: binding.discovery_snapshot_digest.clone(),
        cause: ContextSubscriptionRefreshCause::FullReconcile {
            observed_subscription_version: record.version,
        },
        deadline: record.deadline,
        request_digest: named_digest("context-reconcile-placeholder"),
    };
    request.request_digest = request.canonical_request_digest().unwrap();
    AdmitContextSubscriptionRefresh {
        request,
        audit: ContextSubscriptionAdmissionAudit {
            schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
            request_id: id(ResourceKind::ServerRequest, base),
            correlation_digest: named_digest(&format!("context-reconcile-correlation-{base}")),
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
        },
    }
}

async fn monitor_process_stderr(
    stderr: std::process::ChildStderr,
) -> (String, tokio::task::JoinHandle<()>) {
    let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
    let monitor = tokio::task::spawn_blocking(move || {
        let mut reader = std::io::BufReader::new(stderr);
        let mut line = String::new();
        std::io::BufRead::read_line(&mut reader, &mut line).unwrap();
        startup_tx.send(line).unwrap();
        loop {
            let mut line = String::new();
            if std::io::BufRead::read_line(&mut reader, &mut line).unwrap() == 0 {
                break;
            }
            eprint!("{line}");
        }
    });
    (startup_rx.await.unwrap(), monitor)
}

fn available_address() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

#[derive(Clone)]
struct ProductionArtifactProcessFixture {
    binary: String,
    endpoint: String,
    bucket: String,
    key_id: String,
    kms_binding_digest: Sha256Digest,
    storage_binding_digest: Sha256Digest,
}

impl ProductionArtifactProcessFixture {
    fn from_environment() -> Option<Self> {
        let Ok(binary) = std::env::var("PLATFORM_ARTIFACT_DATA_WORKER_BIN") else {
            return None;
        };
        let endpoint = std::env::var("PLATFORM_TEST_AWS_ENDPOINT")
            .expect("production Artifact process fixture requires PLATFORM_TEST_AWS_ENDPOINT");
        let bucket = std::env::var("PLATFORM_TEST_S3_BUCKET")
            .expect("production Artifact process fixture requires PLATFORM_TEST_S3_BUCKET");
        let key_id = std::env::var("PLATFORM_TEST_KMS_KEY_ID")
            .expect("production Artifact process fixture requires PLATFORM_TEST_KMS_KEY_ID");
        let kms_binding_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "connect_timeout_milliseconds": 1000,
            "endpoint": endpoint,
            "key_id": key_id,
            "operation_timeout_milliseconds": 5000,
            "provider": "aws_kms",
            "region": "us-east-1",
            "schema_version": 1,
        }))
        .unwrap()
        .parse()
        .unwrap();
        let storage_binding_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "backend": "s3",
            "bucket": bucket,
            "connect_timeout_milliseconds": 1000,
            "endpoint": endpoint,
            "force_path_style": true,
            "kms_binding_digest": kms_binding_digest,
            "maximum_object_bytes": 67108864,
            "operation_timeout_milliseconds": 5000,
            "region": "us-east-1",
            "schema_version": 1,
        }))
        .unwrap()
        .parse()
        .unwrap();
        Some(Self {
            binary,
            endpoint,
            bucket,
            key_id,
            kms_binding_digest,
            storage_binding_digest,
        })
    }

    fn config(
        &self,
        controller_address: SocketAddr,
        guest_address: SocketAddr,
        observability_address: SocketAddr,
    ) -> serde_json::Value {
        let ruleset_digest = sandbox_policy_closure(
            "fixture.policy.secret".parse::<SecretPurpose>().unwrap(),
            self.storage_binding_digest.clone(),
        )
        .artifact_io
        .canonical_digest()
        .unwrap();
        serde_json::json!({
            "schema_version": 1,
            "audience": "data_worker",
            "controller_listen_address": controller_address.to_string(),
            "guest_listen_address": guest_address.to_string(),
            "observability_listen_address": observability_address.to_string(),
            "guest_identity": {
                "issuer": "https://kubernetes.default.svc.cluster.local",
                "audience": "insight-platform-gvisor-guest",
                "namespace": "insight-platform-sandbox-guests",
                "service_account_name": "insight-platform-gvisor-guest",
                "jwks": {"keys": [{
                    "kty": "RSA",
                    "kid": "guest-key-1",
                    "use": "sig",
                    "alg": "RS256",
                    "n": "sXch4-7u-lQpR0lJHJj3-JpGcC7dCqHj8P5mW52w8GQ",
                    "e": "AQAB"
                }]},
                "jwks_digest": "sha256:ed90fd7173d7d068236917e3cf1f9f58a55977a88a6355375591ee694347ad49"
            },
            "read_database_max_connections": 4,
            "work_database_max_connections": 4,
            "database_acquire_timeout_milliseconds": 5000,
            "artifact_provider_catalog": {
                "schema_version": 1,
                "write_storage_binding_digest": self.storage_binding_digest,
                "s3_storage_bindings": [{
                    "schema_version": 1,
                    "storage_binding_digest": self.storage_binding_digest,
                    "endpoint": self.endpoint,
                    "region": "us-east-1",
                    "bucket": self.bucket,
                    "force_path_style": true,
                    "kms_binding_digest": self.kms_binding_digest,
                    "connect_timeout_milliseconds": 1000,
                    "operation_timeout_milliseconds": 5000,
                    "maximum_object_bytes": 67108864
                }],
                "kms_key_bindings": [{
                    "schema_version": 1,
                    "kms_binding_digest": self.kms_binding_digest,
                    "endpoint": self.endpoint,
                    "region": "us-east-1",
                    "key_id": self.key_id,
                    "connect_timeout_milliseconds": 1000,
                    "operation_timeout_milliseconds": 5000
                }]
            },
            "broker": {
                "maximum_in_flight": 8,
                "maximum_read_bytes": 67108864,
                "operation_timeout_milliseconds": 5000
            },
            "rpc": {
                "maximum_request_bytes": 1048576,
                "maximum_write_request_bytes": 2097152,
                "maximum_chunk_bytes": 262144
            },
            "scan_worker": {
                "scanner_contract_digest": named_digest("artifact-scanner-contract"),
                "ruleset_digest": ruleset_digest,
                "claim_batch": 1,
                "lease_milliseconds": 30000,
                "receipt_ttl_milliseconds": 60000,
                "poll_milliseconds": 20
            },
            "tls_handshake_timeout_milliseconds": 5000,
            "shutdown_grace_milliseconds": 1000
        })
    }
}

struct ProcessFixtureFiles {
    prefix: String,
    ca: PathBuf,
    artifact_server_cert: PathBuf,
    artifact_server_key: PathBuf,
    resource_host_server_cert: PathBuf,
    resource_host_server_key: PathBuf,
    resource_host_client_cert: PathBuf,
    resource_host_client_key: PathBuf,
    context_worker_client_cert: PathBuf,
    context_worker_client_key: PathBuf,
    subscription_worker_client_cert: PathBuf,
    subscription_worker_client_key: PathBuf,
    discovery_worker_client_cert: PathBuf,
    discovery_worker_client_key: PathBuf,
    mcp_server_cert: PathBuf,
    mcp_server_key: PathBuf,
}

impl ProcessFixtureFiles {
    fn new(tls: &ProcessTlsFixture) -> Self {
        let prefix = format!(
            "/private/tmp/platform-subscription-process-l3-{}",
            std::process::id()
        );
        let path = |suffix: &str| PathBuf::from(format!("{prefix}-{suffix}"));
        let files = Self {
            prefix: prefix.clone(),
            ca: path("ca.pem"),
            artifact_server_cert: path("artifact-server.pem"),
            artifact_server_key: path("artifact-server-key.pem"),
            resource_host_server_cert: path("resource-host.pem"),
            resource_host_server_key: path("resource-host-key.pem"),
            resource_host_client_cert: path("resource-host-client.pem"),
            resource_host_client_key: path("resource-host-client-key.pem"),
            context_worker_client_cert: path("context-worker-client.pem"),
            context_worker_client_key: path("context-worker-client-key.pem"),
            subscription_worker_client_cert: path("subscription-worker-client.pem"),
            subscription_worker_client_key: path("subscription-worker-client-key.pem"),
            discovery_worker_client_cert: path("discovery-worker-client.pem"),
            discovery_worker_client_key: path("discovery-worker-client-key.pem"),
            mcp_server_cert: path("mcp-server.pem"),
            mcp_server_key: path("mcp-server-key.pem"),
        };
        for (path, contents) in [
            (&files.ca, &tls.ca),
            (&files.artifact_server_cert, &tls.artifact_server_cert),
            (&files.artifact_server_key, &tls.artifact_server_key),
            (
                &files.resource_host_server_cert,
                &tls.resource_host_server_cert,
            ),
            (
                &files.resource_host_server_key,
                &tls.resource_host_server_key,
            ),
            (
                &files.resource_host_client_cert,
                &tls.resource_host_client_cert,
            ),
            (
                &files.resource_host_client_key,
                &tls.resource_host_client_key,
            ),
            (
                &files.context_worker_client_cert,
                &tls.context_worker_client_cert,
            ),
            (
                &files.context_worker_client_key,
                &tls.context_worker_client_key,
            ),
            (
                &files.subscription_worker_client_cert,
                &tls.subscription_worker_client_cert,
            ),
            (
                &files.subscription_worker_client_key,
                &tls.subscription_worker_client_key,
            ),
            (
                &files.discovery_worker_client_cert,
                &tls.discovery_worker_client_cert,
            ),
            (
                &files.discovery_worker_client_key,
                &tls.discovery_worker_client_key,
            ),
            (&files.mcp_server_cert, &tls.mcp_server_cert),
            (&files.mcp_server_key, &tls.mcp_server_key),
        ] {
            std::fs::write(path, contents).unwrap();
        }
        files
    }

    fn write_config(&self, name: &str, value: &serde_json::Value) -> (PathBuf, String) {
        let path = PathBuf::from(format!("{}-{name}.json", self.prefix));
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        (path, canonical_digest(value).unwrap())
    }
}

struct ResourceHostSpawn<'a> {
    binary: &'a str,
    database_url: &'a str,
    files: &'a ProcessFixtureFiles,
    config_path: &'a PathBuf,
    config_digest: &'a str,
}

fn spawn_resource_host(input: ResourceHostSpawn<'_>) -> Child {
    std::process::Command::new(input.binary)
        .env("PLATFORM_MCP_RESOURCE_HOST_CONFIG", input.config_path)
        .env(
            "PLATFORM_MCP_RESOURCE_HOST_CONFIG_DIGEST",
            input.config_digest,
        )
        .env(
            "PLATFORM_MCP_RESOURCE_HOST_DATABASE_URL",
            input.database_url,
        )
        .env(
            "PLATFORM_MCP_RESOURCE_HOST_SERVER_CLIENT_CA_PATH",
            &input.files.ca,
        )
        .env(
            "PLATFORM_MCP_RESOURCE_HOST_SERVER_CERT_PATH",
            &input.files.resource_host_server_cert,
        )
        .env(
            "PLATFORM_MCP_RESOURCE_HOST_SERVER_KEY_PATH",
            &input.files.resource_host_server_key,
        )
        .env("PLATFORM_MCP_RESOURCE_HOST_EGRESS_CA_PATH", &input.files.ca)
        .env(
            "PLATFORM_MCP_RESOURCE_HOST_EGRESS_CERT_PATH",
            &input.files.resource_host_client_cert,
        )
        .env(
            "PLATFORM_MCP_RESOURCE_HOST_EGRESS_KEY_PATH",
            &input.files.resource_host_client_key,
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

struct ContextWorkerSpawn<'a> {
    binary: &'a str,
    database_url: &'a str,
    files: &'a ProcessFixtureFiles,
    config_path: &'a PathBuf,
    config_digest: &'a str,
}

fn spawn_subscription_context_worker(input: ContextWorkerSpawn<'_>) -> Child {
    std::process::Command::new(input.binary)
        .env(
            "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_CONFIG",
            input.config_path,
        )
        .env(
            "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_CONFIG_DIGEST",
            input.config_digest,
        )
        .env(
            "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_DATABASE_URL",
            input.database_url,
        )
        .env(
            "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_CA_PATH",
            &input.files.ca,
        )
        .env(
            "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_CERT_PATH",
            &input.files.context_worker_client_cert,
        )
        .env(
            "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_KEY_PATH",
            &input.files.context_worker_client_key,
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

struct SubscriptionWorkerSpawn<'a> {
    binary: &'a str,
    database_url: &'a str,
    files: &'a ProcessFixtureFiles,
    config_path: &'a PathBuf,
    config_digest: &'a str,
}

fn spawn_mcp_subscription_worker(input: SubscriptionWorkerSpawn<'_>) -> Child {
    std::process::Command::new(input.binary)
        .env("PLATFORM_MCP_SUBSCRIPTION_WORKER_CONFIG", input.config_path)
        .env(
            "PLATFORM_MCP_SUBSCRIPTION_WORKER_CONFIG_DIGEST",
            input.config_digest,
        )
        .env(
            "PLATFORM_MCP_SUBSCRIPTION_WORKER_DATABASE_URL",
            input.database_url,
        )
        .env(
            "PLATFORM_MCP_SUBSCRIPTION_WORKER_CLIENT_CERT_PATH",
            &input.files.subscription_worker_client_cert,
        )
        .env(
            "PLATFORM_MCP_SUBSCRIPTION_WORKER_CLIENT_KEY_PATH",
            &input.files.subscription_worker_client_key,
        )
        .env(
            "PLATFORM_MCP_SUBSCRIPTION_WORKER_EGRESS_CA_PATH",
            &input.files.ca,
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

struct DiscoveryWorkerSpawn<'a> {
    binary: &'a str,
    database_url: &'a str,
    files: &'a ProcessFixtureFiles,
    config_path: &'a PathBuf,
    config_digest: &'a str,
}

fn spawn_mcp_discovery_worker(input: DiscoveryWorkerSpawn<'_>) -> Child {
    std::process::Command::new(input.binary)
        .env("PLATFORM_MCP_DISCOVERY_WORKER_CONFIG", input.config_path)
        .env(
            "PLATFORM_MCP_DISCOVERY_WORKER_CONFIG_DIGEST",
            input.config_digest,
        )
        .env(
            "PLATFORM_MCP_DISCOVERY_WORKER_DATABASE_URL",
            input.database_url,
        )
        .env(
            "PLATFORM_MCP_DISCOVERY_WORKER_CLIENT_CERT_PATH",
            &input.files.discovery_worker_client_cert,
        )
        .env(
            "PLATFORM_MCP_DISCOVERY_WORKER_CLIENT_KEY_PATH",
            &input.files.discovery_worker_client_key,
        )
        .env(
            "PLATFORM_MCP_DISCOVERY_WORKER_EGRESS_CA_PATH",
            &input.files.ca,
        )
        .env(
            "PLATFORM_MCP_DISCOVERY_WORKER_ARTIFACT_CA_PATH",
            &input.files.ca,
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

struct ArtifactDataWorkerSpawn<'a> {
    fixture: &'a ProductionArtifactProcessFixture,
    database_url: &'a str,
    files: &'a ProcessFixtureFiles,
    config_path: &'a PathBuf,
    config_digest: &'a str,
}

fn spawn_artifact_data_worker(input: ArtifactDataWorkerSpawn<'_>) -> Child {
    std::process::Command::new(&input.fixture.binary)
        .env("PLATFORM_ARTIFACT_DATA_WORKER_CONFIG", input.config_path)
        .env(
            "PLATFORM_ARTIFACT_DATA_WORKER_CONFIG_DIGEST",
            input.config_digest,
        )
        .env("PLATFORM_ARTIFACT_DATA_WORKER_AUDIENCE", "data_worker")
        .env(
            "PLATFORM_ARTIFACT_DATA_WORKER_READ_DATABASE_URL",
            input.database_url,
        )
        .env(
            "PLATFORM_ARTIFACT_DATA_WORKER_WORK_DATABASE_URL",
            input.database_url,
        )
        .env(
            "PLATFORM_ARTIFACT_DATA_WORKER_CLIENT_CA_PATH",
            &input.files.ca,
        )
        .env(
            "PLATFORM_ARTIFACT_DATA_WORKER_CERT_PATH",
            &input.files.artifact_server_cert,
        )
        .env(
            "PLATFORM_ARTIFACT_DATA_WORKER_KEY_PATH",
            &input.files.artifact_server_key,
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

async fn wait_for_context_job_state(
    pool: &PgPool,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    expected: &str,
    timeout: StdDuration,
) {
    let started = std::time::Instant::now();
    loop {
        let state: String = sqlx::query_scalar(
            "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        if state == expected {
            return;
        }
        assert!(
            started.elapsed() < timeout,
            "Job did not reach {expected}; current={state}"
        );
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
}

async fn install_context_commit_pause(pool: &PgPool) {
    sqlx::query(
        r#"
        CREATE FUNCTION insight_platform.test_pause_subscription_context_commit()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.state = 'running' AND NEW.state = 'succeeded'
             AND NEW.work_class = 'context'
             AND NEW.owner_kind = 'mcp_operation' THEN
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
        "CREATE TRIGGER test_pause_subscription_context_commit BEFORE UPDATE ON insight_platform.jobs FOR EACH ROW EXECUTE FUNCTION insight_platform.test_pause_subscription_context_commit()",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn remove_context_commit_pause(pool: &PgPool) {
    sqlx::query("DROP TRIGGER test_pause_subscription_context_commit ON insight_platform.jobs")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION insight_platform.test_pause_subscription_context_commit()")
        .execute(pool)
        .await
        .unwrap();
}

async fn expire_running_context_job(pool: &PgPool, fixture: &Fixture, job_id: &ResourceId) {
    let updated = sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);
}

async fn run_subscription_process_l3(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
    active_subscription: &McpSubscriptionRecord,
    database_url: &str,
    resource_host_binary: &str,
    context_worker_binary: &str,
) {
    let admission = context_reconcile_admission(active_subscription, 0x360);
    let accepted = repository
        .admit_context_subscription_refresh(admission)
        .await
        .unwrap();
    wait_for_context_job_state(
        pool,
        &fixture.tenant_id,
        &accepted.job_id,
        "ready",
        StdDuration::from_secs(5),
    )
    .await;

    let tls = process_tls_fixture();
    let files = ProcessFixtureFiles::new(&tls);
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let egress_address = incoming.local_addr().unwrap();
    let connector = Arc::new(ProcessRefreshConnector {
        calls: AtomicUsize::new(0),
        first_started: Notify::new(),
        second_started: Notify::new(),
        second_release: Notify::new(),
    });
    let egress_limits = EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap();
    let egress_service = EgressBrokerServiceServer::new(
        EgressBrokerGrpcService::new(
            Arc::new(EmptyModel),
            Arc::new(EmptyHttp),
            Arc::new(EmptyGrpc),
            egress_limits,
        )
        .with_mcp_resource_refresh(connector.clone()),
    );
    let egress_service = tonic::service::interceptor::InterceptedService::new(
        egress_service,
        EgressCallerWorkloadIdentity,
    );
    let (egress_shutdown, egress_shutdown_rx) = tokio::sync::oneshot::channel();
    let egress_server = tokio::spawn(
        Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(
                        &tls.egress_server_cert,
                        &tls.egress_server_key,
                    ))
                    .client_ca_root(Certificate::from_pem(tls.ca.clone())),
            )
            .unwrap()
            .add_service(egress_service)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = egress_shutdown_rx.await;
            }),
    );

    let resource_host_address = available_address();
    let resource_host_observability = available_address();
    let resource_host_config = serde_json::json!({
        "schema_version": 1,
        "listen_address": resource_host_address.to_string(),
        "observability_listen_address": resource_host_observability.to_string(),
        "maximum_rpc_message_bytes": 1048576,
        "maximum_in_flight_requests": 8,
        "database_max_connections": 4,
        "database_acquire_timeout_milliseconds": 5000,
        "egress": {
            "endpoint": format!("https://{egress_address}/"),
            "tls_server_name": "egress.test",
            "connect_timeout_milliseconds": 1000,
            "request_timeout_milliseconds": 30000,
            "maximum_rpc_metadata_bytes": 65536,
            "maximum_rpc_payload_bytes": 1048576
        },
        "drain_grace_milliseconds": 1000
    });
    let (resource_host_config_path, resource_host_config_digest) =
        files.write_config("resource-host", &resource_host_config);
    let context_worker_config = serde_json::json!({
        "schema_version": 1,
        "observability_listen_address": available_address().to_string(),
        "worker_manifest": context_worker_manifest(),
        "database_max_connections": 4,
        "database_acquire_timeout_milliseconds": 5000,
        "receipt_ttl_seconds": 3600,
        "scan_interval_milliseconds": 20,
        "failure_backoff_milliseconds": 5,
        "drain_grace_milliseconds": 1000,
        "host": {
            "endpoint": format!("https://{resource_host_address}/"),
            "tls_server_name": "resource-host.test",
            "connect_timeout_milliseconds": 1000,
            "request_timeout_milliseconds": 30000,
            "maximum_rpc_message_bytes": 1048576
        }
    });
    let (context_worker_config_path, context_worker_config_digest) =
        files.write_config("context-worker", &context_worker_config);

    let spawn_host = || {
        spawn_resource_host(ResourceHostSpawn {
            binary: resource_host_binary,
            database_url,
            files: &files,
            config_path: &resource_host_config_path,
            config_digest: &resource_host_config_digest,
        })
    };
    let spawn_worker = || {
        spawn_subscription_context_worker(ContextWorkerSpawn {
            binary: context_worker_binary,
            database_url,
            files: &files,
            config_path: &context_worker_config_path,
            config_digest: &context_worker_config_digest,
        })
    };

    let mut host = spawn_host();
    let (startup, first_host_stderr) = monitor_process_stderr(host.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-mcp-resource-host started"),
        "{startup}"
    );
    let mut first_worker = spawn_worker();
    let (startup, first_worker_stderr) =
        monitor_process_stderr(first_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-subscription-context-worker started"),
        "{startup}"
    );
    tokio::time::timeout(
        StdDuration::from_secs(10),
        connector.first_started.notified(),
    )
    .await
    .unwrap();
    host.kill().unwrap();
    host.wait().unwrap();
    first_host_stderr.await.unwrap();
    first_worker.kill().unwrap();
    first_worker.wait().unwrap();
    first_worker_stderr.await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(accepted.job_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    let mut host = spawn_host();
    let (startup, second_host_stderr) = monitor_process_stderr(host.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-mcp-resource-host started"),
        "{startup}"
    );
    let mut second_worker = spawn_worker();
    let (startup, second_worker_stderr) =
        monitor_process_stderr(second_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-subscription-context-worker started"),
        "{startup}"
    );
    tokio::time::timeout(
        StdDuration::from_secs(10),
        connector.second_started.notified(),
    )
    .await
    .unwrap();
    install_context_commit_pause(pool).await;
    connector.second_release.notify_one();
    wait_for_context_job_state(
        pool,
        &fixture.tenant_id,
        &accepted.job_id,
        "running",
        StdDuration::from_secs(10),
    )
    .await;
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    second_worker.kill().unwrap();
    second_worker.wait().unwrap();
    second_worker_stderr.await.unwrap();
    remove_context_commit_pause(pool).await;
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(accepted.job_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let mut third_worker = spawn_worker();
    let (startup, third_worker_stderr) =
        monitor_process_stderr(third_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-subscription-context-worker started"),
        "{startup}"
    );
    wait_for_context_job_state(
        pool,
        &fixture.tenant_id,
        &accepted.job_id,
        "succeeded",
        StdDuration::from_secs(15),
    )
    .await;
    assert_eq!(connector.calls.load(Ordering::SeqCst), 3);
    let completed_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND aggregate_id = $2 AND event_type = 'context.subscription_refresh.completed'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(accepted.job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(completed_events, 1);
    third_worker.kill().unwrap();
    third_worker.wait().unwrap();
    third_worker_stderr.await.unwrap();
    host.kill().unwrap();
    host.wait().unwrap();
    second_host_stderr.await.unwrap();
    let _ = egress_shutdown.send(());
    egress_server.await.unwrap().unwrap();
}

async fn wait_for_subscription_session_state(
    pool: &PgPool,
    fixture: &Fixture,
    expected_subscription_state: &str,
    expected_session_state: &str,
    timeout: StdDuration,
) {
    let started = std::time::Instant::now();
    loop {
        let row = sqlx::query(
            r#"
            SELECT state, payload -> 'session' ->> 'state' AS session_state
            FROM insight_platform.invocations
            WHERE tenant_id = $1 AND invocation_id = $2
              AND invocation_kind = 'mcp_subscription'
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.subscription_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        let subscription_state = row.try_get::<String, _>("state").unwrap();
        let session_state = row.try_get::<String, _>("session_state").unwrap();
        if subscription_state == expected_subscription_state
            && session_state == expected_session_state
        {
            return;
        }
        assert!(
            started.elapsed() < timeout,
            "subscription did not reach {expected_subscription_state}/{expected_session_state}; current={subscription_state}/{session_state}"
        );
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
}

async fn run_logical_subscription_worker_process_l3(
    pool: &PgPool,
    fixture: &Fixture,
    active_subscription: &McpSubscriptionRecord,
    database_url: &str,
    subscription_worker_binary: &str,
) {
    let tls = process_tls_fixture();
    let files = ProcessFixtureFiles::new(&tls);
    let egress_address = available_address();
    let mcp_address = SocketAddr::from(([127, 0, 0, 1], fixture.mcp_endpoint.port));
    let control_prefix = format!("{}-logical-protocol", files.prefix);
    for name in [
        "methods.log",
        "attempt-1.started",
        "attempt-2.started",
        "sse.started",
    ] {
        let _ = std::fs::remove_file(protocol_control_path(&control_prefix, name));
    }
    let protocol_config = ProtocolEgressFixtureConfig {
        egress_address,
        mcp_address,
        ca_pem: tls.ca.clone(),
        egress_server_cert: tls.egress_server_cert.clone(),
        egress_server_key: tls.egress_server_key.clone(),
        mcp_server_cert: tls.mcp_server_cert.clone(),
        mcp_server_key: tls.mcp_server_key.clone(),
        endpoint: InstalledMcpStreamableHttpEndpoint {
            schema_version: 1,
            deployment: fixture.mcp_deployment.clone(),
            endpoint: fixture.mcp_endpoint.clone(),
            endpoint_identity_digest: fixture.mcp_endpoint.canonical_digest().unwrap(),
            server_identity_digest: fixture.mcp_endpoint.canonical_digest().unwrap(),
            protocol_policy: fixture.protocol_policy.clone(),
            network_policy: fixture.network_policy.clone(),
            tls_policy: fixture.tls_policy.clone(),
            trust_policy: fixture.trust_policy.clone(),
            auth_policy: fixture.auth_policy.clone(),
            token_credential_purpose: fixture.token_purpose.clone(),
            trusted_root_pem: tls.ca.clone(),
        },
        control_prefix: control_prefix.clone(),
        resource_uri: active_subscription.payload.binding.resource_uri.clone(),
        pause_second_initialize: false,
    };
    let protocol_config_path =
        PathBuf::from(format!("{}-logical-protocol-egress.json", files.prefix));
    std::fs::write(
        &protocol_config_path,
        serde_json::to_vec(&protocol_config).unwrap(),
    )
    .unwrap();
    let config = serde_json::json!({
        "schema_version": 1,
        "observability_listen_address": available_address().to_string(),
        "database_max_connections": 8,
        "database_acquire_timeout_milliseconds": 5000,
        "claim_batch_size": 1,
        "recovery_batch_size": 4,
        "reconcile_batch_size": 4,
        "reconcile_minimum_idle_milliseconds": 60000,
        "maximum_concurrency": 1,
        "lease_milliseconds": 30000,
        "scan_interval_milliseconds": 500,
        "failure_backoff_milliseconds": 20,
        "heartbeat_interval_milliseconds": 5000,
        "receipt_ttl_milliseconds": 60000,
        "drain_grace_milliseconds": 1000,
        "notification": {
            "maximum_in_flight": 4,
            "maximum_wire_bytes": 65536,
            "maximum_tracked_bindings": 64,
            "maximum_events_per_window": 64,
            "window_milliseconds": 60000
        },
        "egress": {
            "endpoint": format!("https://{egress_address}/"),
            "tls_server_name": "egress.test",
            "connect_timeout_milliseconds": 1000,
            "request_timeout_milliseconds": 30000,
            "maximum_rpc_metadata_bytes": 65536,
            "maximum_rpc_payload_bytes": 1048576
        }
    });
    let (config_path, config_digest) = files.write_config("subscription-worker", &config);
    let spawn_worker = || {
        spawn_mcp_subscription_worker(SubscriptionWorkerSpawn {
            binary: subscription_worker_binary,
            database_url,
            files: &files,
            config_path: &config_path,
            config_digest: &config_digest,
        })
    };

    let mut egress = spawn_protocol_egress_process(&protocol_config_path);
    let (startup, first_egress_stderr) =
        monitor_process_stderr(egress.stderr.take().unwrap()).await;
    assert!(
        startup.contains("subscription protocol Egress fixture started"),
        "{startup}"
    );
    let mut first_worker = spawn_worker();
    let (startup, first_stderr) = monitor_process_stderr(first_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-mcp-subscription-worker started"),
        "{startup}"
    );
    wait_for_control_file(
        &protocol_control_path(&control_prefix, "attempt-1.started"),
        StdDuration::from_secs(10),
    )
    .await;
    egress.kill().unwrap();
    egress.wait().unwrap();
    first_egress_stderr.await.unwrap();
    first_worker.kill().unwrap();
    first_worker.wait().unwrap();
    first_stderr.await.unwrap();
    expire_running_context_job(pool, fixture, &fixture.job_id).await;

    let mut egress = spawn_protocol_egress_process(&protocol_config_path);
    let (startup, second_egress_stderr) =
        monitor_process_stderr(egress.stderr.take().unwrap()).await;
    assert!(
        startup.contains("subscription protocol Egress fixture started"),
        "{startup}"
    );
    let mut second_worker = spawn_worker();
    let (startup, second_stderr) =
        monitor_process_stderr(second_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-mcp-subscription-worker started"),
        "{startup}"
    );
    wait_for_subscription_session_state(
        pool,
        fixture,
        "active",
        "ready",
        StdDuration::from_secs(15),
    )
    .await;
    wait_for_control_file(
        &protocol_control_path(&control_prefix, "sse.started"),
        StdDuration::from_secs(10),
    )
    .await;
    let methods =
        std::fs::read_to_string(protocol_control_path(&control_prefix, "methods.log")).unwrap();
    assert_eq!(
        methods.lines().collect::<Vec<_>>(),
        vec![
            "initialize",
            "initialize",
            "notifications/initialized",
            "resources/subscribe",
            "sse/get"
        ]
    );
    let ready_events: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM insight_platform.events
        WHERE tenant_id = $1 AND aggregate_id = $2
          AND event_type = 'mcp.subscription_session_changed'
          AND payload ->> 'session_state' = 'ready'
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.subscription_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(ready_events, 2);
    second_worker.kill().unwrap();
    second_worker.wait().unwrap();
    second_stderr.await.unwrap();
    egress.kill().unwrap();
    egress.wait().unwrap();
    second_egress_stderr.await.unwrap();
}

async fn wait_for_control_file(path: &std::path::Path, timeout: StdDuration) {
    let started = std::time::Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < timeout,
            "control file was not created: {}",
            path.display()
        );
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
}

async fn read_process_http(address: SocketAddr, path: &str) -> String {
    let started = std::time::Instant::now();
    let mut stream = loop {
        match tokio::net::TcpStream::connect(address).await {
            Ok(stream) => break stream,
            Err(error) => {
                assert!(
                    started.elapsed() < StdDuration::from_secs(10),
                    "process HTTP endpoint {address} did not become ready: {error}"
                );
                tokio::time::sleep(StdDuration::from_millis(10)).await;
            }
        }
    };
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: process.test\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

fn spawn_protocol_egress_process(config_path: &std::path::Path) -> Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("subscription_egress_protocol_fixture_process")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PROTOCOL_EGRESS_CONFIG_ENV, config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn spawn_discovery_artifact_process(config_path: &std::path::Path, database_url: &str) -> Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("discovery_artifact_fixture_process")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(DISCOVERY_ARTIFACT_CONFIG_ENV, config_path)
        .env(DISCOVERY_ARTIFACT_DATABASE_URL_ENV, database_url)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

async fn run_discovery_worker_process_l3(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
    operation: &McpDiscoveryOperationRecord,
    database_url: &str,
    discovery_worker_binary: &str,
    production_artifact: Option<&ProductionArtifactProcessFixture>,
) {
    let tls = process_tls_fixture();
    let files = ProcessFixtureFiles::new(&tls);
    let egress_address = available_address();
    let artifact_address = available_address();
    let mcp_address = SocketAddr::from(([127, 0, 0, 1], fixture.mcp_endpoint.port));
    let control_prefix = format!("{}-discovery", files.prefix);
    for name in [
        "methods.log",
        "attempt-1.started",
        "attempt-2.started",
        "artifact-stage.started",
    ] {
        let _ = std::fs::remove_file(protocol_control_path(&control_prefix, name));
    }
    let protocol_config = ProtocolEgressFixtureConfig {
        egress_address,
        mcp_address,
        ca_pem: tls.ca.clone(),
        egress_server_cert: tls.egress_server_cert.clone(),
        egress_server_key: tls.egress_server_key.clone(),
        mcp_server_cert: tls.mcp_server_cert.clone(),
        mcp_server_key: tls.mcp_server_key.clone(),
        endpoint: InstalledMcpStreamableHttpEndpoint {
            schema_version: 1,
            deployment: fixture.mcp_deployment.clone(),
            endpoint: fixture.mcp_endpoint.clone(),
            endpoint_identity_digest: fixture.mcp_endpoint.canonical_digest().unwrap(),
            server_identity_digest: fixture.mcp_endpoint.canonical_digest().unwrap(),
            protocol_policy: fixture.protocol_policy.clone(),
            network_policy: fixture.network_policy.clone(),
            tls_policy: fixture.tls_policy.clone(),
            trust_policy: fixture.trust_policy.clone(),
            auth_policy: fixture.auth_policy.clone(),
            token_credential_purpose: fixture.token_purpose.clone(),
            trusted_root_pem: tls.ca.clone(),
        },
        control_prefix: control_prefix.clone(),
        resource_uri: "mcp://catalog.example.test/discovery".to_owned(),
        pause_second_initialize: false,
    };
    let protocol_config_path = PathBuf::from(format!("{}-discovery-egress.json", files.prefix));
    std::fs::write(
        &protocol_config_path,
        serde_json::to_vec(&protocol_config).unwrap(),
    )
    .unwrap();
    let artifact_config = DiscoveryArtifactFixtureConfig {
        listen_address: artifact_address,
        ca_pem: tls.ca.clone(),
        server_cert: tls.artifact_server_cert.clone(),
        server_key: tls.artifact_server_key.clone(),
        control_prefix: control_prefix.clone(),
    };
    let artifact_config_path = PathBuf::from(format!("{}-discovery-artifact.json", files.prefix));
    std::fs::write(
        &artifact_config_path,
        serde_json::to_vec(&artifact_config).unwrap(),
    )
    .unwrap();
    let artifact_guest_address = available_address();
    let artifact_observability_address = available_address();
    let production_artifact_config = production_artifact.map(|fixture| {
        files.write_config(
            "artifact-data-worker",
            &fixture.config(
                artifact_address,
                artifact_guest_address,
                artifact_observability_address,
            ),
        )
    });
    let observability_address = available_address();
    let worker_config = serde_json::json!({
        "schema_version": 1,
        "observability_listen_address": observability_address.to_string(),
        "database_max_connections": 8,
        "database_acquire_timeout_milliseconds": 5000,
        "claim_batch_size": 1,
        "recovery_batch_size": 4,
        "maximum_concurrency": 1,
        "lease_milliseconds": 30000,
        "scan_interval_milliseconds": 20,
        "failure_backoff_milliseconds": 20,
        "heartbeat_interval_milliseconds": 5000,
        "retry_backoff_milliseconds": 20,
        "receipt_ttl_milliseconds": 60000,
        "drain_grace_milliseconds": 1000,
        "egress": {
            "endpoint": format!("https://{egress_address}/"),
            "tls_server_name": "egress.test",
            "connect_timeout_milliseconds": 1000,
            "request_timeout_milliseconds": 30000,
            "maximum_rpc_metadata_bytes": 65536,
            "maximum_rpc_payload_bytes": 1048576
        },
        "artifact_data_worker": {
            "endpoint": format!("https://{artifact_address}/"),
            "tls_server_name": "artifact-data-worker.test",
            "connect_timeout_milliseconds": 1000,
            "request_timeout_milliseconds": 30000,
            "maximum_read_request_bytes": 1048576,
            "maximum_chunk_bytes": 262144,
            "maximum_write_request_bytes": 2097152
        }
    });
    let (worker_config_path, worker_config_digest) =
        files.write_config("discovery-worker", &worker_config);
    let spawn_worker = || {
        spawn_mcp_discovery_worker(DiscoveryWorkerSpawn {
            binary: discovery_worker_binary,
            database_url,
            files: &files,
            config_path: &worker_config_path,
            config_digest: &worker_config_digest,
        })
    };

    let mut artifact = if let (Some(fixture), Some((config_path, config_digest))) =
        (production_artifact, production_artifact_config.as_ref())
    {
        spawn_artifact_data_worker(ArtifactDataWorkerSpawn {
            fixture,
            database_url,
            files: &files,
            config_path,
            config_digest,
        })
    } else {
        spawn_discovery_artifact_process(&artifact_config_path, database_url)
    };
    let (startup, artifact_stderr) = monitor_process_stderr(artifact.stderr.take().unwrap()).await;
    if production_artifact.is_some() {
        assert!(
            startup.contains("platform-artifact-data-worker started for data_worker audience"),
            "{startup}"
        );
        let ready = read_process_http(artifact_observability_address, "/readyz").await;
        assert!(ready.starts_with("HTTP/1.1 200 OK"), "{ready}");
    } else {
        assert!(
            startup.contains("discovery Artifact fixture started"),
            "{startup}"
        );
    }
    let mut egress = spawn_protocol_egress_process(&protocol_config_path);
    let (startup, first_egress_stderr) =
        monitor_process_stderr(egress.stderr.take().unwrap()).await;
    assert!(
        startup.contains("subscription protocol Egress fixture started"),
        "{startup}"
    );
    let mut first_worker = spawn_worker();
    let (startup, first_worker_stderr) =
        monitor_process_stderr(first_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-mcp-discovery-worker started"),
        "{startup}"
    );
    wait_for_control_file(
        &protocol_control_path(&control_prefix, "attempt-1.started"),
        StdDuration::from_secs(10),
    )
    .await;
    let ready = read_process_http(observability_address, "/readyz").await;
    assert!(ready.starts_with("HTTP/1.1 200 OK"), "{ready}");
    let metrics = read_process_http(observability_address, "/metrics").await;
    assert!(metrics.starts_with("HTTP/1.1 200 OK"), "{metrics}");
    assert!(metrics.contains(
        "insight_platform_capacity_units{component_role=\"mcp-discovery-worker\",resource=\"discovery_jobs\",state=\"available\"} 0"
    ));
    assert!(metrics.contains(
        "insight_platform_capacity_units{component_role=\"mcp-discovery-worker\",resource=\"discovery_jobs\",state=\"used\"} 1"
    ));
    egress.kill().unwrap();
    egress.wait().unwrap();
    first_egress_stderr.await.unwrap();
    first_worker.kill().unwrap();
    first_worker.wait().unwrap();
    first_worker_stderr.await.unwrap();
    let expired = sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(operation.job_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(expired.rows_affected(), 1);

    let mut egress = spawn_protocol_egress_process(&protocol_config_path);
    let (startup, second_egress_stderr) =
        monitor_process_stderr(egress.stderr.take().unwrap()).await;
    assert!(
        startup.contains("subscription protocol Egress fixture started"),
        "{startup}"
    );
    let mut second_worker = spawn_worker();
    let (startup, second_worker_stderr) =
        monitor_process_stderr(second_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-mcp-discovery-worker started"),
        "{startup}"
    );
    if production_artifact.is_none() {
        wait_for_control_file(
            &protocol_control_path(&control_prefix, "artifact-stage.started"),
            StdDuration::from_secs(15),
        )
        .await;
        wait_for_context_job_state(
            pool,
            &fixture.tenant_id,
            &operation.job_id,
            "waiting",
            StdDuration::from_secs(15),
        )
        .await;

        let artifact_row: (String, i64, String) = sqlx::query_as(
            "SELECT expected_digest, expected_size_bytes, declared_media_type FROM insight_platform.artifacts WHERE tenant_id = $1 AND artifact_id = $2 AND state = 'verifying'",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(operation.payload.admission.artifact_preallocation.artifact_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        let scan_worker = id(ResourceKind::WorkerProcessGeneration, 0x748);
        let mut claims = repository
            .claim_artifact_jobs(ClaimArtifactJobs {
                role: ArtifactWorkerRole::DataWorker,
                worker_id: scan_worker.clone(),
                limit: 1,
                lease_milliseconds: 30_000,
                lease_token_digests: vec![named_digest("discovery-process-scan-claim")],
            })
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        let claim = claims.pop().unwrap();
        assert_eq!(
            claim.job_id,
            operation
                .payload
                .admission
                .artifact_preallocation
                .verification_job_id
                .to_string()
        );
        let token: Sha256Digest = claim.lease_token_digest.as_ref().unwrap().parse().unwrap();
        let started = repository
            .start_job(RepositoryJobFence {
                tenant_id: fixture.tenant_id.to_string(),
                job_id: claim.job_id.clone(),
                worker_id: scan_worker.clone(),
                lease_epoch: claim.lease_epoch,
                expected_job_version: claim.version,
                lease_token_digest: token.clone(),
            })
            .await
            .unwrap();
        let execution = repository
            .load_started_artifact_execution(
                ArtifactWorkerRole::DataWorker,
                fixture.tenant_id.clone(),
                operation
                    .payload
                    .admission
                    .artifact_preallocation
                    .verification_job_id
                    .clone(),
                DomainJobFence {
                    expected_version: u64::try_from(started.version).unwrap(),
                    worker_process_generation_id: scan_worker,
                    lease_generation: u64::try_from(started.lease_epoch).unwrap(),
                    token_digest: token,
                },
                ArtifactExecutionSlot {
                    receipt_id: id(ResourceKind::Receipt, 0x749),
                    event_id: id(ResourceKind::Event, 0x74a),
                    outbox_id: id(ResourceKind::OutboxEvent, 0x74b),
                    duplicate_blob_cleanup_job_id: id(ResourceKind::Job, 0x74c),
                    receipt_expires_at: Utc::now() + Duration::minutes(10),
                },
            )
            .await
            .unwrap();
        let StartedArtifactExecution::Scan(execution) = execution else {
            panic!("discovery process verification Job must be a scan");
        };
        let scan_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(pool)
            .await
            .unwrap();
        ArtifactWorkerService::new(
            DiscoveryArtifactScanner {
                content_digest: artifact_row.0.parse().unwrap(),
                size_bytes: u64::try_from(artifact_row.1).unwrap(),
                media_type: artifact_row.2,
                observed_at: scan_now,
            },
            UnusedDiscoveryBlobBackend,
            repository.clone(),
        )
        .execute_scan(execution, scan_now)
        .await
        .unwrap();
    }
    wait_for_context_job_state(
        pool,
        &fixture.tenant_id,
        &operation.job_id,
        "succeeded",
        StdDuration::from_secs(15),
    )
    .await;
    let terminal: (String, String, i32, String, i64, i64) = sqlx::query_as(
        r#"
        SELECT invocation.state, artifact.state, owner.attempt_no, verification.state,
          (SELECT count(*) FROM insight_platform.resources
           WHERE tenant_id = $1 AND resource_id = $5 AND resource_kind = 'mcp_discovery_snapshot'),
          (SELECT count(*) FROM insight_platform.artifact_links
           WHERE tenant_id = $1 AND artifact_link_id = $6 AND state = 'active')
        FROM insight_platform.invocations AS invocation
        JOIN insight_platform.jobs AS owner
          ON owner.tenant_id = invocation.tenant_id
         AND owner.owner_kind = 'mcp_operation'
         AND owner.owner_id = invocation.invocation_id
         AND owner.job_id = $3
        JOIN insight_platform.artifacts AS artifact
          ON artifact.tenant_id = invocation.tenant_id AND artifact.artifact_id = $4
        JOIN insight_platform.jobs AS verification
          ON verification.tenant_id = invocation.tenant_id AND verification.job_id = $7
        WHERE invocation.tenant_id = $1 AND invocation.invocation_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(operation.operation_id.to_string())
    .bind(operation.job_id.to_string())
    .bind(
        operation
            .payload
            .admission
            .artifact_preallocation
            .artifact_id
            .to_string(),
    )
    .bind(
        operation
            .payload
            .admission
            .artifact_preallocation
            .snapshot_id
            .to_string(),
    )
    .bind(
        operation
            .payload
            .admission
            .artifact_preallocation
            .artifact_link_id
            .to_string(),
    )
    .bind(
        operation
            .payload
            .admission
            .artifact_preallocation
            .verification_job_id
            .to_string(),
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        terminal,
        (
            "succeeded".to_owned(),
            "ready".to_owned(),
            3,
            "succeeded".to_owned(),
            1,
            1
        )
    );
    if let Some(provider) = production_artifact {
        let blob: (String, String, i32, String, String, String) = sqlx::query_as(
            r#"
            SELECT backend, storage_binding_digest, octet_length(object_reference_ciphertext),
              object_generation, key_id, state
            FROM insight_platform.artifact_blobs
            WHERE tenant_id = $1 AND blob_id = $2
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(
            operation
                .payload
                .admission
                .artifact_preallocation
                .blob_id
                .to_string(),
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(blob.0, "s3");
        assert_eq!(blob.1, provider.storage_binding_digest.to_string());
        assert!(blob.2 > 0);
        assert!(!blob.3.is_empty());
        assert_eq!(blob.4, provider.key_id);
        assert_eq!(blob.5, "verified");
    }
    let methods =
        std::fs::read_to_string(protocol_control_path(&control_prefix, "methods.log")).unwrap();
    assert_eq!(
        methods.lines().collect::<Vec<_>>(),
        vec![
            "initialize",
            "initialize",
            "notifications/initialized",
            "resources/list"
        ]
    );

    second_worker.kill().unwrap();
    second_worker.wait().unwrap();
    second_worker_stderr.await.unwrap();
    egress.kill().unwrap();
    egress.wait().unwrap();
    second_egress_stderr.await.unwrap();
    artifact.kill().unwrap();
    artifact.wait().unwrap();
    artifact_stderr.await.unwrap();
}

async fn run_subscription_protocol_process_l3(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
    active_subscription: &McpSubscriptionRecord,
    database_url: &str,
    resource_host_binary: &str,
    context_worker_binary: &str,
) {
    let admission = context_reconcile_admission(active_subscription, 0x380);
    let accepted = repository
        .admit_context_subscription_refresh(admission)
        .await
        .unwrap();
    let tls = process_tls_fixture();
    let files = ProcessFixtureFiles::new(&tls);
    let egress_address = available_address();
    let mcp_address = SocketAddr::from(([127, 0, 0, 1], fixture.mcp_endpoint.port));
    let control_prefix = format!("{}-protocol", files.prefix);
    for name in [
        "methods.log",
        "attempt-1.started",
        "attempt-2.started",
        "attempt-2.release",
        "attempt-3.started",
    ] {
        let _ = std::fs::remove_file(protocol_control_path(&control_prefix, name));
    }
    let endpoint = InstalledMcpStreamableHttpEndpoint {
        schema_version: 1,
        deployment: fixture.mcp_deployment.clone(),
        endpoint: fixture.mcp_endpoint.clone(),
        endpoint_identity_digest: fixture.mcp_endpoint.canonical_digest().unwrap(),
        server_identity_digest: fixture.mcp_endpoint.canonical_digest().unwrap(),
        protocol_policy: fixture.protocol_policy.clone(),
        network_policy: fixture.network_policy.clone(),
        tls_policy: fixture.tls_policy.clone(),
        trust_policy: fixture.trust_policy.clone(),
        auth_policy: fixture.auth_policy.clone(),
        token_credential_purpose: fixture.token_purpose.clone(),
        trusted_root_pem: tls.ca.clone(),
    };
    let protocol_config = ProtocolEgressFixtureConfig {
        egress_address,
        mcp_address,
        ca_pem: tls.ca.clone(),
        egress_server_cert: tls.egress_server_cert.clone(),
        egress_server_key: tls.egress_server_key.clone(),
        mcp_server_cert: tls.mcp_server_cert.clone(),
        mcp_server_key: tls.mcp_server_key.clone(),
        endpoint,
        control_prefix: control_prefix.clone(),
        resource_uri: active_subscription.payload.binding.resource_uri.clone(),
        pause_second_initialize: true,
    };
    let protocol_config_path = PathBuf::from(format!("{}-protocol-egress.json", files.prefix));
    std::fs::write(
        &protocol_config_path,
        serde_json::to_vec(&protocol_config).unwrap(),
    )
    .unwrap();

    let resource_host_address = available_address();
    let resource_host_config = serde_json::json!({
        "schema_version": 1,
        "listen_address": resource_host_address.to_string(),
        "observability_listen_address": available_address().to_string(),
        "maximum_rpc_message_bytes": 1048576,
        "maximum_in_flight_requests": 8,
        "database_max_connections": 4,
        "database_acquire_timeout_milliseconds": 5000,
        "egress": {
            "endpoint": format!("https://{egress_address}/"),
            "tls_server_name": "egress.test",
            "connect_timeout_milliseconds": 1000,
            "request_timeout_milliseconds": 30000,
            "maximum_rpc_metadata_bytes": 65536,
            "maximum_rpc_payload_bytes": 1048576
        },
        "drain_grace_milliseconds": 1000
    });
    let (resource_host_config_path, resource_host_config_digest) =
        files.write_config("protocol-resource-host", &resource_host_config);
    let context_worker_config = serde_json::json!({
        "schema_version": 1,
        "observability_listen_address": available_address().to_string(),
        "worker_manifest": context_worker_manifest(),
        "database_max_connections": 4,
        "database_acquire_timeout_milliseconds": 5000,
        "receipt_ttl_seconds": 3600,
        "scan_interval_milliseconds": 20,
        "failure_backoff_milliseconds": 5,
        "drain_grace_milliseconds": 1000,
        "host": {
            "endpoint": format!("https://{resource_host_address}/"),
            "tls_server_name": "resource-host.test",
            "connect_timeout_milliseconds": 1000,
            "request_timeout_milliseconds": 30000,
            "maximum_rpc_message_bytes": 1048576
        }
    });
    let (context_worker_config_path, context_worker_config_digest) =
        files.write_config("protocol-context-worker", &context_worker_config);
    let spawn_host = || {
        spawn_resource_host(ResourceHostSpawn {
            binary: resource_host_binary,
            database_url,
            files: &files,
            config_path: &resource_host_config_path,
            config_digest: &resource_host_config_digest,
        })
    };
    let spawn_worker = || {
        spawn_subscription_context_worker(ContextWorkerSpawn {
            binary: context_worker_binary,
            database_url,
            files: &files,
            config_path: &context_worker_config_path,
            config_digest: &context_worker_config_digest,
        })
    };

    let mut egress = spawn_protocol_egress_process(&protocol_config_path);
    let (startup, first_egress_stderr) =
        monitor_process_stderr(egress.stderr.take().unwrap()).await;
    assert!(
        startup.contains("subscription protocol Egress fixture started"),
        "{startup}"
    );
    let mut host = spawn_host();
    let (startup, first_host_stderr) = monitor_process_stderr(host.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-mcp-resource-host started"),
        "{startup}"
    );
    let mut first_worker = spawn_worker();
    let (startup, first_worker_stderr) =
        monitor_process_stderr(first_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-subscription-context-worker started"),
        "{startup}"
    );
    wait_for_control_file(
        &protocol_control_path(&control_prefix, "attempt-1.started"),
        StdDuration::from_secs(10),
    )
    .await;
    egress.kill().unwrap();
    egress.wait().unwrap();
    first_egress_stderr.await.unwrap();
    host.kill().unwrap();
    host.wait().unwrap();
    first_host_stderr.await.unwrap();
    first_worker.kill().unwrap();
    first_worker.wait().unwrap();
    first_worker_stderr.await.unwrap();
    expire_running_context_job(pool, fixture, &accepted.job_id).await;

    let mut egress = spawn_protocol_egress_process(&protocol_config_path);
    let (startup, second_egress_stderr) =
        monitor_process_stderr(egress.stderr.take().unwrap()).await;
    assert!(
        startup.contains("subscription protocol Egress fixture started"),
        "{startup}"
    );
    let mut host = spawn_host();
    let (startup, second_host_stderr) = monitor_process_stderr(host.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-mcp-resource-host started"),
        "{startup}"
    );
    let mut second_worker = spawn_worker();
    let (startup, second_worker_stderr) =
        monitor_process_stderr(second_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-subscription-context-worker started"),
        "{startup}"
    );
    wait_for_control_file(
        &protocol_control_path(&control_prefix, "attempt-2.started"),
        StdDuration::from_secs(10),
    )
    .await;
    install_context_commit_pause(pool).await;
    std::fs::write(
        protocol_control_path(&control_prefix, "attempt-2.release"),
        b"release",
    )
    .unwrap();
    wait_for_context_job_state(
        pool,
        &fixture.tenant_id,
        &accepted.job_id,
        "running",
        StdDuration::from_secs(10),
    )
    .await;
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    second_worker.kill().unwrap();
    second_worker.wait().unwrap();
    second_worker_stderr.await.unwrap();
    remove_context_commit_pause(pool).await;
    expire_running_context_job(pool, fixture, &accepted.job_id).await;

    let mut third_worker = spawn_worker();
    let (startup, third_worker_stderr) =
        monitor_process_stderr(third_worker.stderr.take().unwrap()).await;
    assert!(
        startup.contains("platform-subscription-context-worker started"),
        "{startup}"
    );
    wait_for_context_job_state(
        pool,
        &fixture.tenant_id,
        &accepted.job_id,
        "succeeded",
        StdDuration::from_secs(15),
    )
    .await;
    let completed_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND aggregate_id = $2 AND event_type = 'context.subscription_refresh.completed'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(accepted.job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(completed_events, 1);
    let methods =
        std::fs::read_to_string(protocol_control_path(&control_prefix, "methods.log")).unwrap();
    assert_eq!(
        methods
            .lines()
            .filter(|method| *method == "initialize")
            .count(),
        3
    );
    assert_eq!(
        methods
            .lines()
            .filter(|method| *method == "notifications/initialized")
            .count(),
        2
    );
    assert_eq!(
        methods
            .lines()
            .filter(|method| *method == "resources/list")
            .count(),
        2
    );
    assert_eq!(
        methods
            .lines()
            .filter(|method| *method == "resources/read")
            .count(),
        2
    );

    third_worker.kill().unwrap();
    third_worker.wait().unwrap();
    third_worker_stderr.await.unwrap();
    host.kill().unwrap();
    host.wait().unwrap();
    second_host_stderr.await.unwrap();
    egress.kill().unwrap();
    egress.wait().unwrap();
    second_egress_stderr.await.unwrap();
}

#[test]
fn mcp_subscription_is_durable_fenced_coalesced_and_tenant_scoped() {
    std::thread::Builder::new()
        .name("phase4-mcp-subscription-fixture".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(mcp_subscription_fixture());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn mcp_subscription_fixture() {
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
    let now = Utc::now();
    let production_artifact = ProductionArtifactProcessFixture::from_environment();
    let write_storage_binding_digest = production_artifact
        .as_ref()
        .map(|fixture| fixture.storage_binding_digest.clone())
        .unwrap_or_else(|| named_digest("artifact-write-storage-binding"));
    let fixture = seed(
        &pool,
        &repository,
        now,
        available_address().port(),
        write_storage_binding_digest,
    )
    .await;
    let context_quota_account_id = id(ResourceKind::QuotaAccount, 0x5f0);
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: fixture.tenant_id.to_string(),
            quota_account_id: context_quota_account_id.to_string(),
            scope_kind: "tenant".to_owned(),
            scope_id: fixture.tenant_id.to_string(),
            work_class: WorkClass::Context.as_str().to_owned(),
            metric: QuotaDimension::WorkClassConcurrentOperations
                .as_str()
                .to_owned(),
            limit_value: 4,
            payload: TypedPayload::new(
                1,
                &serde_json::json!({"fixture": "context-subscription-refresh"}),
            )
            .unwrap(),
        })
        .await
        .unwrap();
    let mut discovery_audit = command_audit(&fixture.tenant_id, &fixture.principal_id, 0x600, now);
    discovery_audit.idempotency_key_digest = named_digest("public-discovery-key");
    discovery_audit.request_digest = named_digest("public-discovery-request");
    let discovery_deadline =
        DateTime::from_timestamp((now + Duration::minutes(30)).timestamp(), 123_456_789).unwrap();
    assert_ne!(discovery_deadline.timestamp_subsec_nanos() % 1_000, 0);
    let first_discovery = CreateMcpDiscoveryOperation {
        audit: discovery_audit,
        operation_id: id(ResourceKind::McpOperation, 0x610),
        job_id: id(ResourceKind::Job, 0x611),
        logical_key: "public-discovery-request".to_owned(),
        mcp_deployment: fixture.mcp_deployment.clone(),
        authorization_binding_id: fixture.authorization.authorization_binding_id.clone(),
        artifact_preallocation: discovery_preallocation(0x630),
        attempt_limit: 3,
        deadline: discovery_deadline,
    };
    let original = match repository
        .create_mcp_discovery_operation(first_discovery.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first discovery command must apply"),
    };
    let mut replay = first_discovery;
    replay.audit.receipt_id = id(ResourceKind::Receipt, 0x620);
    replay.audit.event_id = id(ResourceKind::Event, 0x621);
    replay.audit.outbox_id = id(ResourceKind::OutboxEvent, 0x622);
    replay.operation_id = id(ResourceKind::McpOperation, 0x623);
    replay.job_id = id(ResourceKind::Job, 0x624);
    let replayed = match repository
        .create_mcp_discovery_operation(replay)
        .await
        .unwrap()
    {
        CommandOutcome::Replayed(record) => record,
        CommandOutcome::Applied(_) => panic!("discovery replay must reuse the first Job"),
    };
    assert_eq!(replayed.operation_id, original.operation_id);
    assert_eq!(replayed.job_id, original.job_id);

    let discovery_worker_a = id(ResourceKind::WorkerProcessGeneration, 0x650);
    let discovery_worker_b = id(ResourceKind::WorkerProcessGeneration, 0x651);
    let left_claim = repository.claim_mcp_discovery_jobs(ClaimJobs {
        work_class: WorkClass::Mcp.as_str().to_owned(),
        worker_id: discovery_worker_a,
        limit: 1,
        lease_milliseconds: 60_000,
        lease_token_digests: vec![named_digest("discovery-claim-left")],
    });
    let right_claim = repository.claim_mcp_discovery_jobs(ClaimJobs {
        work_class: WorkClass::Mcp.as_str().to_owned(),
        worker_id: discovery_worker_b,
        limit: 1,
        lease_milliseconds: 60_000,
        lease_token_digests: vec![named_digest("discovery-claim-right")],
    });
    let (left_claim, right_claim) = tokio::join!(left_claim, right_claim);
    let mut discovery_claims = left_claim
        .unwrap()
        .into_iter()
        .chain(right_claim.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(discovery_claims.len(), 1);
    let discovery_claim = discovery_claims.pop().unwrap();
    assert_eq!(discovery_claim.job_id, original.job_id.to_string());
    let discovery_worker: ResourceId = discovery_claim.worker_id.as_ref().unwrap().parse().unwrap();
    let discovery_token: Sha256Digest = discovery_claim
        .lease_token_digest
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    let discovery_started = repository
        .start_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: original.job_id.to_string(),
            worker_id: discovery_worker,
            lease_epoch: discovery_claim.lease_epoch,
            expected_job_version: discovery_claim.version,
            lease_token_digest: discovery_token,
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(original.job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let observations = repository.list_expired_mcp_discovery_jobs(2).await.unwrap();
    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(observation.job_id, original.job_id);
    assert_eq!(
        observation.job_version,
        u64::try_from(discovery_started.version).unwrap()
    );
    assert_eq!(observation.physical_attempt, 1);
    let recovery = RecoverExpiredMcpDiscoveryJob {
        tenant_id: observation.tenant_id.clone(),
        operation_id: observation.operation_id.clone(),
        job_id: observation.job_id.clone(),
        observed_operation_version: observation.operation_version,
        observed_job_version: observation.job_version,
        observed_lease_generation: observation.lease_generation,
        retry_at: Some(observation.observed_at + Duration::seconds(1)),
        event_id: id(ResourceKind::Event, 0x652),
        outbox_id: id(ResourceKind::OutboxEvent, 0x653),
    };
    let recovered_discovery = repository
        .recover_expired_mcp_discovery_job(recovery.clone())
        .await
        .unwrap();
    assert!(matches!(
        recovered_discovery.state,
        insight_platform_mcp_host::McpDiscoveryOperationState::Pending
    ));
    let recovered_job_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(original.job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(recovered_job_state, "retry_scheduled");
    assert!(repository
        .recover_expired_mcp_discovery_job(recovery)
        .await
        .is_err());

    tokio::time::sleep(StdDuration::from_millis(1_100)).await;
    let resumed_worker = id(ResourceKind::WorkerProcessGeneration, 0x654);
    let mut resumed_claims = repository
        .claim_mcp_discovery_jobs(ClaimJobs {
            work_class: WorkClass::Mcp.as_str().to_owned(),
            worker_id: resumed_worker.clone(),
            limit: 1,
            lease_milliseconds: 60_000,
            lease_token_digests: vec![named_digest("discovery-resumed-claim")],
        })
        .await
        .unwrap();
    assert_eq!(resumed_claims.len(), 1);
    let resumed_claim = resumed_claims.pop().unwrap();
    let resumed_token: Sha256Digest = resumed_claim
        .lease_token_digest
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    let resumed_started = repository
        .start_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: original.job_id.to_string(),
            worker_id: resumed_worker.clone(),
            lease_epoch: resumed_claim.lease_epoch,
            expected_job_version: resumed_claim.version,
            lease_token_digest: resumed_token.clone(),
        })
        .await
        .unwrap();
    let resumed_fence = DomainJobFence {
        expected_version: u64::try_from(resumed_started.version).unwrap(),
        worker_process_generation_id: resumed_worker.clone(),
        lease_generation: u64::try_from(resumed_started.lease_epoch).unwrap(),
        token_digest: resumed_token,
    };
    let resolved_discovery = repository
        .resolve_mcp_discovery_execution(&McpDiscoveryContractQuery {
            schema_version: 1,
            tenant_id: fixture.tenant_id.clone(),
            operation_id: original.operation_id.clone(),
            job_id: original.job_id.clone(),
            fence: resumed_fence.clone(),
        })
        .await
        .unwrap();
    let descriptor_bytes =
        br#"{"prompts":[],"resources":[],"schema_version":1,"tools":[]}"#.to_vec();
    let observed_at = Utc::now();
    let candidate = McpDiscoveryCandidate::build(
        MCP_PROTOCOL_BASELINE.to_owned(),
        fixture.snapshot.negotiated_capabilities.clone(),
        descriptor_bytes.clone(),
        0,
        observed_at,
        observed_at + Duration::minutes(5),
        &resolved_discovery.contract,
    )
    .unwrap();
    let stage_request = StageWorkloadArtifactRequest {
        schema_version: 1,
        tenant_id: fixture.tenant_id.clone(),
        producer_job_id: original.job_id.clone(),
        producer_fence: resumed_fence.clone(),
        verification_job_id: original
            .payload
            .admission
            .artifact_preallocation
            .verification_job_id
            .clone(),
        artifact_id: original
            .payload
            .admission
            .artifact_preallocation
            .artifact_id
            .clone(),
        blob_id: original
            .payload
            .admission
            .artifact_preallocation
            .blob_id
            .clone(),
        descriptor_bytes,
        descriptor_digest: candidate.descriptor_digest.clone(),
        media_type: original
            .payload
            .admission
            .artifact_policy
            .declared_media_type
            .clone(),
    };
    let WorkloadArtifactStagePreflight::Authorized(stage_authority) = repository
        .authorize_workload_artifact_stage(&stage_request)
        .await
        .unwrap()
    else {
        panic!("first discovery Artifact stage must require physical storage evidence");
    };
    let staged_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let staged = match repository
        .stage_workload_artifact(StageWorkloadArtifact {
            schema_version: 1,
            tenant_id: fixture.tenant_id.clone(),
            caller: ArtifactWorkloadAudience::McpHost,
            producer_job_id: original.job_id.clone(),
            producer_fence: resumed_fence.clone(),
            verification_job_id: stage_request.verification_job_id.clone(),
            artifact_id: stage_request.artifact_id.clone(),
            blob_id: stage_request.blob_id.clone(),
            content_digest: stage_request.descriptor_digest.clone(),
            size_bytes: u64::try_from(stage_request.descriptor_bytes.len()).unwrap(),
            media_type: stage_request.media_type.clone(),
            storage_backend: "s3".to_owned(),
            storage_binding_digest: stage_authority.write_storage_binding_digest,
            object_reference_ciphertext: vec![0x5a; 48],
            object_generation: "discovery-stage-generation-1".to_owned(),
            key_id: "discovery-stage-key".to_owned(),
            encryption_domain_id: stage_authority.encryption_domain_id,
            backend_evidence_digest: named_digest("discovery-stage-backend-evidence"),
            staged_at,
        })
        .await
        .unwrap()
    {
        CommandOutcome::Applied(staged) => staged,
        CommandOutcome::Replayed(_) => panic!("first discovery Artifact stage must apply"),
    };
    let transport_evidence = McpDiscoveryTransportEvidence::build(
        &candidate,
        &staged,
        &original.payload.admission.artifact_preallocation,
    )
    .unwrap();
    assert!(matches!(
        repository
            .authorize_workload_artifact_stage(&stage_request)
            .await
            .unwrap(),
        WorkloadArtifactStagePreflight::Replayed(ref replayed) if replayed == &staged
    ));
    let park = ParkMcpDiscoveryVerification {
        audit: McpWorkerAudit {
            tenant_id: fixture.tenant_id.clone(),
            worker_process_generation_id: resumed_worker,
            receipt_id: id(ResourceKind::Receipt, 0x655),
            event_id: id(ResourceKind::Event, 0x656),
            outbox_id: id(ResourceKind::OutboxEvent, 0x657),
            idempotency_key_digest: named_digest("discovery-park-key"),
            request_digest: named_digest("discovery-park-request"),
            receipt_expires_at: Utc::now() + Duration::minutes(10),
        },
        operation_id: original.operation_id.clone(),
        job_id: original.job_id.clone(),
        fence: resumed_fence,
        expected_operation_version: resolved_discovery.operation_version,
        staged,
        evidence: transport_evidence,
    };
    let parked = repository
        .park_mcp_discovery_for_verification(park.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(parked) = parked else {
        panic!("first discovery verification park must apply");
    };
    assert!(matches!(
        parked.state,
        insight_platform_mcp_host::McpDiscoveryOperationState::Running
    ));
    assert!(parked.payload.pending_verification.is_some());
    assert!(matches!(
        repository
            .park_mcp_discovery_for_verification(park)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let parked_states: (String, String) = sqlx::query_as(
        r#"
        SELECT owner.state, verification.state
        FROM insight_platform.jobs AS owner
        JOIN insight_platform.jobs AS verification
          ON verification.tenant_id = owner.tenant_id AND verification.job_id = $3
        WHERE owner.tenant_id = $1 AND owner.job_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(original.job_id.to_string())
    .bind(stage_request.verification_job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(parked_states, ("waiting".to_owned(), "ready".to_owned()));

    let scan_worker = id(ResourceKind::WorkerProcessGeneration, 0x658);
    let mut scan_claims = repository
        .claim_artifact_jobs(ClaimArtifactJobs {
            role: ArtifactWorkerRole::DataWorker,
            worker_id: scan_worker.clone(),
            limit: 1,
            lease_milliseconds: 60_000,
            lease_token_digests: vec![named_digest("discovery-scan-claim")],
        })
        .await
        .unwrap();
    assert_eq!(scan_claims.len(), 1);
    let scan_claim = scan_claims.pop().unwrap();
    assert_eq!(
        scan_claim.job_id,
        stage_request.verification_job_id.to_string()
    );
    let scan_token: Sha256Digest = scan_claim
        .lease_token_digest
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    let scan_started = repository
        .start_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: stage_request.verification_job_id.to_string(),
            worker_id: scan_worker.clone(),
            lease_epoch: scan_claim.lease_epoch,
            expected_job_version: scan_claim.version,
            lease_token_digest: scan_token.clone(),
        })
        .await
        .unwrap();
    let scan_execution = repository
        .load_started_artifact_execution(
            ArtifactWorkerRole::DataWorker,
            fixture.tenant_id.clone(),
            stage_request.verification_job_id.clone(),
            DomainJobFence {
                expected_version: u64::try_from(scan_started.version).unwrap(),
                worker_process_generation_id: scan_worker,
                lease_generation: u64::try_from(scan_started.lease_epoch).unwrap(),
                token_digest: scan_token,
            },
            ArtifactExecutionSlot {
                receipt_id: id(ResourceKind::Receipt, 0x659),
                event_id: id(ResourceKind::Event, 0x65a),
                outbox_id: id(ResourceKind::OutboxEvent, 0x65b),
                duplicate_blob_cleanup_job_id: id(ResourceKind::Job, 0x65c),
                receipt_expires_at: Utc::now() + Duration::minutes(10),
            },
        )
        .await
        .unwrap();
    let StartedArtifactExecution::Scan(scan_execution) = scan_execution else {
        panic!("discovery verification Job must start as an Artifact scan");
    };
    let scan_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let scan_service = ArtifactWorkerService::new(
        DiscoveryArtifactScanner {
            content_digest: stage_request.descriptor_digest.clone(),
            size_bytes: u64::try_from(stage_request.descriptor_bytes.len()).unwrap(),
            media_type: stage_request.media_type.clone(),
            observed_at: scan_now,
        },
        UnusedDiscoveryBlobBackend,
        repository.clone(),
    );
    let verified = scan_service
        .execute_scan(scan_execution, scan_now)
        .await
        .unwrap();
    assert!(matches!(verified, CommandOutcome::Applied(_)));
    let after_scan_states: (String, String, String) = sqlx::query_as(
        r#"
        SELECT owner.state, verification.state, artifact.state
        FROM insight_platform.jobs AS owner
        JOIN insight_platform.jobs AS verification
          ON verification.tenant_id = owner.tenant_id AND verification.job_id = $3
        JOIN insight_platform.artifacts AS artifact
          ON artifact.tenant_id = owner.tenant_id AND artifact.artifact_id = $4
        WHERE owner.tenant_id = $1 AND owner.job_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(original.job_id.to_string())
    .bind(stage_request.verification_job_id.to_string())
    .bind(stage_request.artifact_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        after_scan_states,
        (
            "ready".to_owned(),
            "waiting".to_owned(),
            "verified".to_owned()
        )
    );

    let finalize_worker = id(ResourceKind::WorkerProcessGeneration, 0x65d);
    let mut finalize_claims = repository
        .claim_mcp_discovery_jobs(ClaimJobs {
            work_class: WorkClass::Mcp.as_str().to_owned(),
            worker_id: finalize_worker.clone(),
            limit: 1,
            lease_milliseconds: 60_000,
            lease_token_digests: vec![named_digest("discovery-finalize-claim")],
        })
        .await
        .unwrap();
    assert_eq!(finalize_claims.len(), 1);
    let finalize_claim = finalize_claims.pop().unwrap();
    let finalize_token: Sha256Digest = finalize_claim
        .lease_token_digest
        .as_ref()
        .unwrap()
        .parse()
        .unwrap();
    let finalize_started = repository
        .start_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: original.job_id.to_string(),
            worker_id: finalize_worker.clone(),
            lease_epoch: finalize_claim.lease_epoch,
            expected_job_version: finalize_claim.version,
            lease_token_digest: finalize_token.clone(),
        })
        .await
        .unwrap();
    let finalize_fence = DomainJobFence {
        expected_version: u64::try_from(finalize_started.version).unwrap(),
        worker_process_generation_id: finalize_worker.clone(),
        lease_generation: u64::try_from(finalize_started.lease_epoch).unwrap(),
        token_digest: finalize_token,
    };
    let finalize_resolution = repository
        .resolve_mcp_discovery_execution(&McpDiscoveryContractQuery {
            schema_version: 1,
            tenant_id: fixture.tenant_id.clone(),
            operation_id: original.operation_id.clone(),
            job_id: original.job_id.clone(),
            fence: finalize_fence.clone(),
        })
        .await
        .unwrap();
    assert!(finalize_resolution.pending_verification.is_some());
    let finalize = FinalizeMcpDiscovery {
        audit: McpWorkerAudit {
            tenant_id: fixture.tenant_id.clone(),
            worker_process_generation_id: finalize_worker,
            receipt_id: id(ResourceKind::Receipt, 0x65e),
            event_id: id(ResourceKind::Event, 0x65f),
            outbox_id: id(ResourceKind::OutboxEvent, 0x660),
            idempotency_key_digest: named_digest("discovery-finalize-key"),
            request_digest: named_digest("discovery-finalize-request"),
            receipt_expires_at: Utc::now() + Duration::minutes(10),
        },
        operation_id: original.operation_id.clone(),
        job_id: original.job_id.clone(),
        fence: finalize_fence,
        expected_operation_version: finalize_resolution.operation_version,
    };
    let finalized = repository
        .finalize_mcp_discovery_after_verification(finalize.clone())
        .await
        .unwrap();
    let CommandOutcome::Applied(finalized) = finalized else {
        panic!("first discovery finalize must apply");
    };
    assert!(matches!(
        finalized.state,
        insight_platform_mcp_host::McpDiscoveryOperationState::Succeeded
    ));
    assert!(finalized.payload.result.is_some());
    assert!(matches!(
        repository
            .finalize_mcp_discovery_after_verification(finalize)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let terminal_states: (String, String, String, i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT owner.state, verification.state, artifact.state,
          (SELECT count(*) FROM insight_platform.resources
           WHERE tenant_id = $1 AND resource_id = $5 AND resource_kind = 'mcp_discovery_snapshot'),
          (SELECT count(*) FROM insight_platform.artifact_links
           WHERE tenant_id = $1 AND artifact_link_id = $6 AND state = 'active'),
          quota.reserved_value,
          (SELECT count(*) FROM insight_platform.quota_ledger
           WHERE tenant_id = $1 AND quota_account_id = quota.quota_account_id
             AND correlation_id = $4 AND entry_kind = 'settle'
             AND reserved_amount = $7 AND used_amount = 0)
        FROM insight_platform.jobs AS owner
        JOIN insight_platform.jobs AS verification
          ON verification.tenant_id = owner.tenant_id AND verification.job_id = $3
        JOIN insight_platform.artifacts AS artifact
          ON artifact.tenant_id = owner.tenant_id AND artifact.artifact_id = $4
        JOIN insight_platform.quota_accounts AS quota
          ON quota.tenant_id = owner.tenant_id AND quota.quota_account_id = $8
        WHERE owner.tenant_id = $1 AND owner.job_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(original.job_id.to_string())
    .bind(stage_request.verification_job_id.to_string())
    .bind(stage_request.artifact_id.to_string())
    .bind(
        original
            .payload
            .admission
            .artifact_preallocation
            .snapshot_id
            .to_string(),
    )
    .bind(
        original
            .payload
            .admission
            .artifact_preallocation
            .artifact_link_id
            .to_string(),
    )
    .bind(i64::try_from(original.payload.admission.artifact_policy.maximum_bytes).unwrap())
    .bind(
        original
            .payload
            .admission
            .artifact_policy
            .quota_account_id
            .to_string(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        terminal_states,
        (
            "succeeded".to_owned(),
            "succeeded".to_owned(),
            "ready".to_owned(),
            1,
            1,
            0,
            1
        )
    );
    if let Ok(discovery_worker_binary) = std::env::var("PLATFORM_MCP_DISCOVERY_WORKER_BIN") {
        let mut process_audit =
            command_audit(&fixture.tenant_id, &fixture.principal_id, 0x700, now);
        process_audit.idempotency_key_digest = named_digest("process-discovery-key");
        process_audit.request_digest = named_digest("process-discovery-request");
        let process_discovery = match repository
            .create_mcp_discovery_operation(CreateMcpDiscoveryOperation {
                audit: process_audit,
                operation_id: id(ResourceKind::McpOperation, 0x710),
                job_id: id(ResourceKind::Job, 0x711),
                logical_key: "process-discovery-request".to_owned(),
                mcp_deployment: fixture.mcp_deployment.clone(),
                authorization_binding_id: fixture.authorization.authorization_binding_id.clone(),
                artifact_preallocation: discovery_preallocation(0x720),
                attempt_limit: 3,
                deadline: now + Duration::minutes(30),
            })
            .await
            .unwrap()
        {
            CommandOutcome::Applied(record) => record,
            CommandOutcome::Replayed(_) => panic!("process discovery admission must apply"),
        };
        run_discovery_worker_process_l3(
            &pool,
            &repository,
            &fixture,
            &process_discovery,
            &database_url,
            &discovery_worker_binary,
            production_artifact.as_ref(),
        )
        .await;
    } else {
        eprintln!("discovery Worker binary is unset; discovery process L3 skipped");
    }
    let command = create_command(&fixture, now);

    let applied = create_subscription(&repository, command.clone())
        .await
        .unwrap();
    let mut record = match applied {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first create must apply"),
    };
    assert_eq!(record.state.to_string(), "pending");
    assert!(matches!(
        create_subscription(&repository, command.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let mut conflicting = command.clone();
    conflicting.resource_uri = "mcp://catalog.example.test/items/43".to_owned();
    assert!(create_subscription(&repository, conflicting).await.is_err());

    let mut cross_tenant = command.clone();
    cross_tenant.audit = command_audit(&fixture.other_tenant_id, &fixture.principal_id, 0x210, now);
    cross_tenant.subscription_id = id(ResourceKind::McpOperation, 0x213);
    cross_tenant.job_id = id(ResourceKind::Job, 0x214);
    cross_tenant.execution.tenant_id = fixture.other_tenant_id.clone();
    cross_tenant.audit.request_digest = cross_tenant.request_digest().unwrap();
    assert!(create_subscription(&repository, cross_tenant)
        .await
        .is_err());

    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0x220);
    let mut fence = claim_and_start(&repository, &fixture, &worker_id, "session-lease").await;
    let resolved = repository
        .resolve_mcp_subscription_execution(&McpSubscriptionContractQuery {
            schema_version: 1,
            tenant_id: fixture.tenant_id.clone(),
            subscription_id: fixture.subscription_id.clone(),
            job_id: fixture.job_id.clone(),
            fence: fence.clone(),
        })
        .await
        .unwrap();
    assert_eq!(resolved.record.version, record.version);
    assert!(resolved
        .record
        .payload
        .session
        .binding_key
        .validate_canonical_for(&resolved.contract)
        .is_ok());
    let connecting = session_command(
        &fixture,
        &worker_id,
        fence.clone(),
        &record,
        SessionCommandInput {
            target: McpSessionState::Connecting,
            opaque: None,
            expires_at: None,
            base: 0x230,
            now,
        },
    );
    record = match repository
        .save_mcp_subscription_session(connecting.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("connecting must apply"),
    };
    assert!(matches!(
        repository
            .save_mcp_subscription_session(connecting)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    fence.expected_version += 1;
    let initializing = session_command(
        &fixture,
        &worker_id,
        fence.clone(),
        &record,
        SessionCommandInput {
            target: McpSessionState::Initializing,
            opaque: None,
            expires_at: None,
            base: 0x240,
            now,
        },
    );
    record = match repository
        .save_mcp_subscription_session(initializing)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("initializing must apply"),
    };
    fence.expected_version += 1;
    let ready = session_command(
        &fixture,
        &worker_id,
        fence,
        &record,
        SessionCommandInput {
            target: McpSessionState::Ready,
            opaque: Some(EncryptedMcpState {
                scheme: "aes256_gcm_v1".to_owned(),
                ciphertext: b"opaque-session-ciphertext-canary".to_vec(),
                key_id: "opaque-session-key-canary".to_owned(),
                key_reference_digest: named_digest("opaque-session-key-reference"),
                plaintext_digest: named_digest("opaque-session"),
            }),
            expires_at: Some(now + Duration::minutes(30)),
            base: 0x250,
            now,
        },
    );
    record = match repository
        .save_mcp_subscription_session(ready)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("ready must apply"),
    };
    assert_eq!(record.state.to_string(), "active");
    assert_eq!(record.payload.session.generation, 1);

    let resource_uri_digest = record.payload.binding.resource_uri_digest.clone();
    let mut first = notification(&fixture, 1, 1, 0x260, now);
    first.resource_uri_digest = Some(named_digest("resource-subresource-uri"));
    let mut second = notification(&fixture, 1, 2, 0x270, now + Duration::milliseconds(1));
    second.resource_uri_digest = Some(resource_uri_digest);
    let (first_outcome, second_outcome) = tokio::join!(
        repository.commit_mcp_notification(first.clone()),
        repository.commit_mcp_notification(second.clone())
    );
    let outcomes = [first_outcome.unwrap(), second_outcome.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.disposition == McpNotificationApplyDisposition::Wake)
            .count(),
        1
    );
    assert!(outcomes.iter().all(|outcome| !outcome.replayed));
    let mut redelivery = second.clone();
    redelivery.audit.receipt_id = id(ResourceKind::Receipt, 0x275);
    redelivery.audit.event_id = id(ResourceKind::Event, 0x276);
    redelivery.audit.outbox_id = id(ResourceKind::OutboxEvent, 0x277);
    redelivery.received_at += Duration::seconds(1);
    let replay = repository
        .commit_mcp_notification(redelivery)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(
        replay
            .record
            .payload
            .pending_invalidation
            .as_ref()
            .unwrap()
            .event_generation,
        2
    );

    let admission = context_refresh_admission(&replay.record, 0x278);
    let accepted = repository
        .admit_context_subscription_refresh(admission.clone())
        .await
        .unwrap();
    let replayed_acceptance = repository
        .admit_context_subscription_refresh(admission.clone())
        .await
        .unwrap();
    assert_eq!(replayed_acceptance, accepted);
    let admitted_job = sqlx::query(
        r#"
        SELECT work_class, owner_kind, owner_id, invocation_id, state,
               request_digest, payload_digest
        FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(accepted.job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        admitted_job.try_get::<String, _>("work_class").unwrap(),
        "context"
    );
    assert_eq!(
        admitted_job.try_get::<String, _>("owner_kind").unwrap(),
        "mcp_operation"
    );
    assert_eq!(
        admitted_job.try_get::<String, _>("owner_id").unwrap(),
        fixture.subscription_id.to_string()
    );
    assert_eq!(
        admitted_job
            .try_get::<Option<String>, _>("invocation_id")
            .unwrap(),
        Some(fixture.subscription_id.to_string())
    );
    assert_eq!(admitted_job.try_get::<String, _>("state").unwrap(), "ready");
    assert_eq!(
        admitted_job.try_get::<String, _>("request_digest").unwrap(),
        admission.request.request_digest.to_string()
    );
    assert_eq!(
        admitted_job.try_get::<String, _>("payload_digest").unwrap(),
        accepted.durable_work_digest.to_string()
    );
    let mut stale_admission = admission;
    stale_admission.request.session_generation += 1;
    stale_admission.request.request_digest =
        stale_admission.request.canonical_request_digest().unwrap();
    assert!(repository
        .admit_context_subscription_refresh(stale_admission)
        .await
        .is_err());
    let admitted_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.jobs WHERE tenant_id = $1 AND work_class = 'context' AND owner_kind = 'mcp_operation' AND owner_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.subscription_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(admitted_count, 1);
    let admission_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_type = 'context.subscription_refresh_admitted'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(admission_event_count, 1);

    // The MCP worker settles its own accepted invalidation and clears the pending marker before
    // the independent Context worker claims the durable refresh Job.
    let refresh_worker = id(ResourceKind::WorkerProcessGeneration, 0x280);
    let refresh_fence =
        claim_and_start(&repository, &fixture, &refresh_worker, "refresh-lease").await;
    let mut refresh = CompleteMcpSubscriptionRefresh {
        audit: worker_audit(&fixture.tenant_id, &refresh_worker, 0x290, now),
        subscription_id: fixture.subscription_id.clone(),
        job_id: fixture.job_id.clone(),
        fence: refresh_fence,
        expected_subscription_version: replay.record.version,
        expected_session_generation: 1,
        expected_event_generation: 2,
        refresh_evidence_digest: accepted.durable_work_digest.clone(),
    };
    refresh.audit.request_digest = refresh.request_digest().unwrap();
    let refreshed = match repository
        .complete_mcp_subscription_refresh(refresh.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("refresh must apply"),
    };
    assert!(refreshed.payload.pending_invalidation.is_none());
    assert!(matches!(
        repository
            .complete_mcp_subscription_refresh(refresh)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));

    let context_worker = id(ResourceKind::WorkerProcessGeneration, 0x279);
    let context_worker_manifest = context_worker_manifest_digest();
    let candidates = repository
        .scan_claimable_context_subscription_refresh_jobs(&context_worker_manifest, 8)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].job_id, accepted.job_id);
    let claims = repository
        .claim_context_subscription_refresh_jobs(ClaimContextSubscriptionRefreshJobs {
            worker_process_generation_id: context_worker.clone(),
            worker_manifest_digest: context_worker_manifest,
            slots: vec![ContextSubscriptionRefreshClaimSlot {
                tenant_id: fixture.tenant_id.clone(),
                job_id: accepted.job_id.clone(),
                lease_token_digest: named_digest("context-refresh-lease"),
                quota_reservation_id: id(ResourceKind::UsageReservation, 0x27a),
                quota_reserve_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x27b),
                event_id: id(ResourceKind::Event, 0x27c),
                outbox_id: id(ResourceKind::OutboxEvent, 0x27d),
            }],
            lease_policy: LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 30_000,
            },
        })
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].quota_account_id, context_quota_account_id);
    let dispatch_attempt = claims[0].attempt.clone();
    let evidence = ContextSubscriptionRefreshEvidence {
        schema_version: CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
        execution_identity_digest: dispatch_attempt.execution_identity_digest().unwrap(),
        request_digest: dispatch_attempt.request.request_digest.clone(),
        response_digest: named_digest("context-refresh-response"),
        resource_set_digest: named_digest("context-refresh-resource-set"),
        resource_count: 1,
        item_count: 2,
        byte_count: 512,
        remote_revision: Some("revision-1".to_owned()),
        cursor: None,
        observed_at: now,
    };
    let heartbeat = repository
        .heartbeat_job(HeartbeatJob {
            fence: RepositoryJobFence {
                tenant_id: fixture.tenant_id.to_string(),
                job_id: accepted.job_id.to_string(),
                worker_id: context_worker,
                lease_epoch: claims[0].job.lease_epoch,
                expected_job_version: claims[0].job.version,
                lease_token_digest: named_digest("context-refresh-lease"),
            },
            lease_milliseconds: 30_000,
        })
        .await
        .unwrap();
    let stale_commit = CommitContextSubscriptionRefresh {
        attempt: dispatch_attempt.clone(),
        response: ContextSubscriptionRefreshResponse::Completed {
            evidence: evidence.clone(),
        },
        quota_settlement_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x27e),
        event_id: id(ResourceKind::Event, 0x27f),
        outbox_id: id(ResourceKind::OutboxEvent, 0x281),
    };
    assert!(matches!(
        repository
            .commit_context_subscription_refresh(stale_commit)
            .await,
        Err(RepositoryError::StaleFence)
    ));
    let mut terminal_attempt = dispatch_attempt;
    terminal_attempt.job_fence.expected_version = u64::try_from(heartbeat.version).unwrap();
    let resolved = repository
        .resolve_context_subscription_refresh(&terminal_attempt)
        .await
        .unwrap();
    assert_eq!(
        resolved.subscription.subscription_id,
        terminal_attempt.request.subscription_id
    );
    assert_eq!(
        resolved.contract.deployment,
        terminal_attempt.request.mcp_deployment
    );
    let commit = CommitContextSubscriptionRefresh {
        attempt: terminal_attempt,
        response: ContextSubscriptionRefreshResponse::Completed { evidence },
        quota_settlement_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x27e),
        event_id: id(ResourceKind::Event, 0x27f),
        outbox_id: id(ResourceKind::OutboxEvent, 0x281),
    };
    let completed_context_job = repository
        .commit_context_subscription_refresh(commit.clone())
        .await
        .unwrap();
    assert_eq!(completed_context_job.state, "succeeded");
    assert_eq!(
        repository
            .commit_context_subscription_refresh(commit)
            .await
            .unwrap(),
        completed_context_job
    );
    let context_quota_reserved: i64 = sqlx::query_scalar(
        "SELECT reserved_value FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(context_quota_account_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(context_quota_reserved, 0);
    let context_observations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_kind = 'context'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(context_observations, 0);

    let mut stale = notification(&fixture, 1, 1, 0x2a0, now + Duration::seconds(1));
    stale.event_key_digest = named_digest("stale-distinct-key");
    stale.resource_uri_digest = Some(refreshed.payload.binding.resource_uri_digest.clone());
    let stale = repository.commit_mcp_notification(stale).await.unwrap();
    assert_eq!(stale.disposition, McpNotificationApplyDisposition::Stale);
    assert!(!stale.replayed);

    let job = sqlx::query(
        "SELECT state, wake_kind, wake_state, worker_id FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(job.try_get::<String, _>("state").unwrap(), "waiting");
    assert_eq!(
        job.try_get::<Option<String>, _>("wake_kind")
            .unwrap()
            .as_deref(),
        Some("remote_invocation")
    );
    assert_eq!(
        job.try_get::<Option<String>, _>("wake_state")
            .unwrap()
            .as_deref(),
        Some("pending")
    );
    assert!(job
        .try_get::<Option<String>, _>("worker_id")
        .unwrap()
        .is_none());

    sqlx::query(
        r#"
        UPDATE insight_platform.invocations
        SET created_at = created_at - interval '5 minutes',
            updated_at = updated_at - interval '5 minutes'
        WHERE tenant_id = $1 AND invocation_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.subscription_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let due = repository
        .list_due_mcp_subscription_reconciliations(McpSubscriptionReconcileScan {
            tenant_id: fixture.tenant_id.clone(),
            limit: 8,
            minimum_idle_milliseconds: 60_000,
        })
        .await
        .unwrap();
    assert_eq!(due.len(), 1);
    let scan_worker = id(ResourceKind::WorkerProcessGeneration, 0x2b0);
    let mut wake = WakeMcpSubscriptionReconcile {
        audit: worker_audit(&fixture.tenant_id, &scan_worker, 0x2c0, now),
        candidate: due[0].clone(),
    };
    wake.audit.request_digest = wake.request_digest().unwrap();
    let woken = match repository
        .wake_mcp_subscription_reconcile(wake.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first periodic reconcile wake must apply"),
    };
    assert!(matches!(
        repository
            .wake_mcp_subscription_reconcile(wake)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let reconcile_admission = context_reconcile_admission(&woken, 0x2c8);
    let reconcile_acceptance = repository
        .admit_context_subscription_refresh(reconcile_admission.clone())
        .await
        .unwrap();
    assert_eq!(
        repository
            .admit_context_subscription_refresh(reconcile_admission)
            .await
            .unwrap(),
        reconcile_acceptance
    );
    assert!(repository
        .scan_claimable_context_subscription_refresh_jobs(
            &named_digest("wrong-context-worker-manifest"),
            8,
        )
        .await
        .unwrap()
        .is_empty());
    let reconcile_context_candidates = repository
        .scan_claimable_context_subscription_refresh_jobs(&context_worker_manifest_digest(), 8)
        .await
        .unwrap();
    assert_eq!(reconcile_context_candidates.len(), 1);
    assert_eq!(
        reconcile_context_candidates[0].job_id,
        reconcile_acceptance.job_id
    );
    let recovery_worker = id(ResourceKind::WorkerProcessGeneration, 0x2ca);
    let first_reconcile_claim = repository
        .claim_context_subscription_refresh_jobs(ClaimContextSubscriptionRefreshJobs {
            worker_process_generation_id: recovery_worker,
            worker_manifest_digest: context_worker_manifest_digest(),
            slots: vec![ContextSubscriptionRefreshClaimSlot {
                tenant_id: fixture.tenant_id.clone(),
                job_id: reconcile_acceptance.job_id.clone(),
                lease_token_digest: named_digest("reconcile-context-lease-1"),
                quota_reservation_id: id(ResourceKind::UsageReservation, 0x2cb),
                quota_reserve_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x2cc),
                event_id: id(ResourceKind::Event, 0x2cd),
                outbox_id: id(ResourceKind::OutboxEvent, 0x2ce),
            }],
            lease_policy: LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 30_000,
            },
        })
        .await
        .unwrap();
    assert_eq!(first_reconcile_claim[0].attempt.attempt_number, 1);
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(reconcile_acceptance.job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let recovered = repository
        .drive_expired_context_subscription_refresh_jobs(
            DriveExpiredContextSubscriptionRefreshJobs {
                shard_index: 0,
                shard_count: 1,
                limit: 1,
                retry_backoff_milliseconds: 1,
                slots: vec![ContextSubscriptionRefreshRecoverySlot {
                    quota_settlement_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x2cf),
                    event_id: id(ResourceKind::Event, 0x2d1),
                    outbox_id: id(ResourceKind::OutboxEvent, 0x2d2),
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].state, "retry_scheduled");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let second_reconcile_claim = repository
        .claim_context_subscription_refresh_jobs(ClaimContextSubscriptionRefreshJobs {
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 0x2d3),
            worker_manifest_digest: context_worker_manifest_digest(),
            slots: vec![ContextSubscriptionRefreshClaimSlot {
                tenant_id: fixture.tenant_id.clone(),
                job_id: reconcile_acceptance.job_id.clone(),
                lease_token_digest: named_digest("reconcile-context-lease-2"),
                quota_reservation_id: id(ResourceKind::UsageReservation, 0x2d4),
                quota_reserve_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x2d5),
                event_id: id(ResourceKind::Event, 0x2d6),
                outbox_id: id(ResourceKind::OutboxEvent, 0x2d7),
            }],
            lease_policy: LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 30_000,
            },
        })
        .await
        .unwrap();
    assert_eq!(second_reconcile_claim[0].attempt.attempt_number, 2);
    let retry_commit = CommitContextSubscriptionRefresh {
        attempt: second_reconcile_claim[0].attempt.clone(),
        response: ContextSubscriptionRefreshResponse::RetryableFailure {
            class: ContextSubscriptionRefreshFailureClass::Dependency,
            evidence_digest: named_digest("retryable-context-refresh"),
            retry_after_milliseconds: 1,
        },
        quota_settlement_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x2d8),
        event_id: id(ResourceKind::Event, 0x2d9),
        outbox_id: id(ResourceKind::OutboxEvent, 0x2da),
    };
    assert_eq!(
        repository
            .commit_context_subscription_refresh(retry_commit)
            .await
            .unwrap()
            .state,
        "retry_scheduled"
    );
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let terminal_reconcile_claim = repository
        .claim_context_subscription_refresh_jobs(ClaimContextSubscriptionRefreshJobs {
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 0x2db),
            worker_manifest_digest: context_worker_manifest_digest(),
            slots: vec![ContextSubscriptionRefreshClaimSlot {
                tenant_id: fixture.tenant_id.clone(),
                job_id: reconcile_acceptance.job_id.clone(),
                lease_token_digest: named_digest("reconcile-context-lease-3"),
                quota_reservation_id: id(ResourceKind::UsageReservation, 0x2dc),
                quota_reserve_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x2dd),
                event_id: id(ResourceKind::Event, 0x2de),
                outbox_id: id(ResourceKind::OutboxEvent, 0x2df),
            }],
            lease_policy: LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 30_000,
            },
        })
        .await
        .unwrap();
    assert_eq!(terminal_reconcile_claim[0].attempt.attempt_number, 3);
    let terminal_attempt = terminal_reconcile_claim[0].attempt.clone();
    let terminal_evidence = ContextSubscriptionRefreshEvidence {
        schema_version: CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
        execution_identity_digest: terminal_attempt.execution_identity_digest().unwrap(),
        request_digest: terminal_attempt.request.request_digest.clone(),
        response_digest: named_digest("reconcile-context-response"),
        resource_set_digest: named_digest("reconcile-context-resource-set"),
        resource_count: 2,
        item_count: 4,
        byte_count: 1_024,
        remote_revision: Some("revision-2".to_owned()),
        cursor: Some("cursor-2".to_owned()),
        observed_at: Utc::now(),
    };
    assert_eq!(
        repository
            .commit_context_subscription_refresh(CommitContextSubscriptionRefresh {
                attempt: terminal_attempt,
                response: ContextSubscriptionRefreshResponse::Completed {
                    evidence: terminal_evidence,
                },
                quota_settlement_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x2e1),
                event_id: id(ResourceKind::Event, 0x2e2),
                outbox_id: id(ResourceKind::OutboxEvent, 0x2e3),
            })
            .await
            .unwrap()
            .state,
        "succeeded"
    );
    let total_context_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.jobs WHERE tenant_id = $1 AND work_class = 'context' AND owner_kind = 'mcp_operation' AND owner_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.subscription_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(total_context_jobs, 2);
    let reconcile_worker = id(ResourceKind::WorkerProcessGeneration, 0x2d0);
    let reconcile_fence =
        claim_and_start(&repository, &fixture, &reconcile_worker, "reconcile-lease").await;
    let mut reconcile = CompleteMcpSubscriptionReconcile {
        audit: worker_audit(&fixture.tenant_id, &reconcile_worker, 0x2e0, now),
        subscription_id: fixture.subscription_id.clone(),
        job_id: fixture.job_id.clone(),
        fence: reconcile_fence,
        expected_subscription_version: refreshed.version,
        expected_session_generation: refreshed.payload.session.generation,
        reconcile_evidence_digest: reconcile_acceptance.durable_work_digest,
    };
    reconcile.audit.request_digest = reconcile.request_digest().unwrap();
    let reconciled = repository
        .complete_mcp_subscription_reconcile(reconcile)
        .await
        .unwrap();
    let reconciled = match reconciled {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("periodic reconcile must apply"),
    };
    if let (Ok(resource_host_binary), Ok(context_worker_binary)) = (
        std::env::var("PLATFORM_MCP_RESOURCE_HOST_BIN"),
        std::env::var("PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_BIN"),
    ) {
        if std::env::var("PLATFORM_RUN_IN_PROCESS_SUBSCRIPTION_FIXTURE").as_deref() == Ok("1") {
            run_subscription_process_l3(
                &pool,
                &repository,
                &fixture,
                &reconciled,
                &database_url,
                &resource_host_binary,
                &context_worker_binary,
            )
            .await;
        }
        run_subscription_protocol_process_l3(
            &pool,
            &repository,
            &fixture,
            &reconciled,
            &database_url,
            &resource_host_binary,
            &context_worker_binary,
        )
        .await;
    } else {
        eprintln!("subscription process binaries are unset; Resource Refresh process L3 skipped");
    }
    let reconcile_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_type IN ('mcp.subscription_reconcile_due', 'mcp.subscription_reconciled')",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(reconcile_events, 2);

    let loss_worker = id(ResourceKind::WorkerProcessGeneration, 0x2f0);
    let mut stale_termination = ReportMcpSubscriptionTransportTermination {
        audit: worker_audit(&fixture.tenant_id, &loss_worker, 0x2f4, now),
        subscription_id: fixture.subscription_id.clone(),
        expected_authorization_generation: fixture.authorization.generation,
        expected_session_generation: reconciled.payload.session.generation + 1,
        reported_at: now,
        session_loss_evidence_digest: named_digest("stale-transport-loss-evidence"),
    };
    stale_termination.audit.request_digest = stale_termination.request_digest().unwrap();
    assert!(matches!(
        repository
            .report_mcp_subscription_transport_termination(stale_termination)
            .await,
        Err(RepositoryError::StaleFence)
    ));

    let mut session_loss = ReportMcpSubscriptionTransportTermination {
        audit: worker_audit(&fixture.tenant_id, &loss_worker, 0x300, now),
        subscription_id: fixture.subscription_id.clone(),
        expected_authorization_generation: fixture.authorization.generation,
        expected_session_generation: reconciled.payload.session.generation,
        reported_at: now,
        session_loss_evidence_digest: named_digest("transport-loss-evidence"),
    };
    session_loss.audit.request_digest = session_loss.request_digest().unwrap();
    let lost = match repository
        .report_mcp_subscription_transport_termination(session_loss.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("session loss must apply"),
    };
    assert!(matches!(
        repository
            .report_mcp_subscription_transport_termination(session_loss)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    assert_eq!(lost.state.to_string(), "pending");
    assert_eq!(lost.payload.session.state, McpSessionState::Disconnected);
    assert!(lost.payload.session.encrypted_opaque_session.is_none());
    assert!(lost.payload.full_reconcile_required);
    let requeued_job = sqlx::query(
        "SELECT state, wake_kind, wake_state, worker_id FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(requeued_job.try_get::<String, _>("state").unwrap(), "ready");
    assert!(requeued_job
        .try_get::<Option<String>, _>("wake_kind")
        .unwrap()
        .is_none());
    assert!(requeued_job
        .try_get::<Option<String>, _>("wake_state")
        .unwrap()
        .is_none());
    assert!(requeued_job
        .try_get::<Option<String>, _>("worker_id")
        .unwrap()
        .is_none());

    let rebuild_worker = id(ResourceKind::WorkerProcessGeneration, 0x310);
    let rebuild_fence =
        claim_and_start(&repository, &fixture, &rebuild_worker, "rebuild-lease").await;
    let connecting = session_command(
        &fixture,
        &rebuild_worker,
        rebuild_fence,
        &lost,
        SessionCommandInput {
            target: McpSessionState::Connecting,
            opaque: None,
            expires_at: None,
            base: 0x320,
            now,
        },
    );
    let connecting = match repository
        .save_mcp_subscription_session(connecting)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("rebuild connecting must apply"),
    };
    sqlx::query(
        "UPDATE insight_platform.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let recoveries = repository
        .list_due_mcp_subscription_recoveries(McpSubscriptionRecoveryScan {
            tenant_id: fixture.tenant_id.clone(),
            limit: 8,
        })
        .await
        .unwrap();
    assert_eq!(recoveries.len(), 1);
    assert_eq!(
        recoveries[0].cause,
        McpSubscriptionRecoveryCause::ExpiredLease
    );
    assert_eq!(recoveries[0].subscription_version, connecting.version);
    let recovery_worker = id(ResourceKind::WorkerProcessGeneration, 0x330);
    let mut recovery = RecoverDueMcpSubscription {
        audit: worker_audit(&fixture.tenant_id, &recovery_worker, 0x340, now),
        candidate: recoveries[0].clone(),
    };
    recovery.audit.request_digest = recovery.request_digest().unwrap();
    let recovered = match repository
        .recover_due_mcp_subscription(recovery.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("expired lease recovery must apply"),
    };
    assert!(matches!(
        repository
            .recover_due_mcp_subscription(recovery)
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    assert_eq!(
        recovered.payload.session.state,
        McpSessionState::Disconnected
    );
    assert!(recovered.payload.full_reconcile_required);

    let invalidation_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_type = 'mcp.subscription_invalidated'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(invalidation_events, 1);
    let notification_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND operation = 'mcp.notification'",
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(notification_receipts, 3);
    let durable_projection: String = sqlx::query_scalar(
        r#"
        SELECT concat_ws('|',
            COALESCE((SELECT string_agg(to_jsonb(receipt)::text, '|') FROM insight_platform.receipts AS receipt WHERE tenant_id = $1), ''),
            COALESCE((SELECT string_agg(to_jsonb(event)::text, '|') FROM insight_platform.events AS event WHERE tenant_id = $1), ''),
            COALESCE((SELECT string_agg(to_jsonb(outbox)::text, '|') FROM insight_platform.outbox_events AS outbox WHERE tenant_id = $1), '')
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!durable_projection.contains("opaque-session-key-canary"));
    assert!(!durable_projection.contains("opaque-session-ciphertext-canary"));
    assert!(!durable_projection.contains("mcp://catalog.example.test/items/42"));
    if let Ok(subscription_worker_binary) = std::env::var("PLATFORM_MCP_SUBSCRIPTION_WORKER_BIN") {
        run_logical_subscription_worker_process_l3(
            &pool,
            &fixture,
            &recovered,
            &database_url,
            &subscription_worker_binary,
        )
        .await;
    } else {
        eprintln!("subscription Worker binary is unset; logical subscription process L3 skipped");
    }
}
