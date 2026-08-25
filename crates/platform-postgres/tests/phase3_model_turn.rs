use chrono::{DateTime, Duration, Utc};
use insight_platform_artifact_rpc::{
    proto::artifact_scheduler_service_server::ArtifactSchedulerServiceServer,
    ArtifactInternalRpcLimits, ArtifactSchedulerGrpcService, LeasedArtifactBytes,
    SchedulerRunValueResponseBroker, SchedulerSkillPackageResponseBroker,
    SchedulerTypedPlanResponseBroker, SchedulerWorkloadIdentity, SCHEDULER_WORKLOAD_IDENTITY,
};
use insight_platform_artifacts::{
    SchedulerRunValueReadError, SchedulerRunValueReadRequest, SchedulerSkillPackageReadError,
    SchedulerSkillPackageReadRequest, SchedulerTypedPlanReadError, SchedulerTypedPlanReadRequest,
};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityTransportCancelOutcome, CapabilityTransportCancelRequest,
    GrpcNetworkTransport, GrpcTransportRequest, GrpcTransportResponse, HttpNetworkTransport,
    HttpTransportRequest, HttpTransportResponse,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, checked_in_hard_limit_profile, AdministrativeGate,
    AgentDeploymentClosure, AgentResourceSpec, ArtifactRef, AuthoringPackage,
    CandidateSelectionMode, CandidateSelectionPolicyDocument, CapabilityArtifactContract,
    CapabilityBackendBinding, CapabilityBackendContract, CapabilityBackendFeatures,
    CapabilityBackendKind, CapabilityBackendLimits, CapabilityCancellationKind,
    CapabilityDataFlowPolicy, CapabilityDeploymentClosure, CapabilityIdempotencyKind,
    CapabilityImplementationResourceSpec, CapabilityInterfaceLimits,
    CapabilityInterfaceResourceSpec, CapabilityProgressContract, CapabilityProgressDurability,
    CapabilityProgressMode, ClosedJsonSchema, ClosedJsonValue, CommandAudit, CommandOutcome,
    ContextWindowContract, DataClassification, DataRegion, DeploymentClosure, Effect,
    EntityLifecycle, ExactDeploymentRef, ExactPolicyBinding, ExactSecretBindingRef,
    ExactVersionRef, FrozenSlotBinding, FrozenSlotTarget, InstalledModelAdapter, JobState,
    ModelBudgetPolicyDocument, ModelCatalogEvidence, ModelDeploymentClosure,
    ModelIdentityStability, ModelLimits, ModelModalities, ModelProfileResourceSpec,
    ModelProviderDeploymentClosure, ModelProviderResourceSpec, ModelPublicProjectionPolicyDocument,
    ModelSafetyPolicyDocument, ModelToolContract, ModelTurnState, ModelUsageContract,
    NativeCapabilityContract, Permission, PermissionSet, PolicyDeploymentClosure, PolicyKind,
    PolicyResourceSpec, PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot,
    ProviderDataHandlingContract, ProviderModelIdentity, ProviderRequestLimits,
    ProviderTrainingPolicy, PublishedVersionPayload, QuotaDimension, RegistryResourceKind,
    ResourceDocument, ResourceId, ResourceKind, RunBindingsSnapshot, SchedulingPolicyDocument,
    SecretBindingPayload, SecretPurpose, SecretResolutionPolicy, Sha256Digest,
    StructuredOutputContract, TenantConfig, TenantPrincipalPayload, ValidationSummary, ValueRef,
    WorkClass, WorkerManifest, WORKER_MANIFEST_VERSION, WORKER_PROTOCOL_VERSION,
};
use insight_platform_egress_rpc::{
    proto::egress_broker_service_server::EgressBrokerServiceServer, EgressBrokerGrpcService,
    EgressCallerWorkloadIdentity, EgressInternalRpcLimits, MODEL_WORKER_WORKLOAD_IDENTITY,
};
use insight_platform_invocations::{
    CapabilityClaimSlot, CapabilityOutputValue, CapabilityWorkerAudit, ClaimCapabilityJobs,
    CommitCapabilityOutcome, DispatchOutcome, InvocationPolicyDecision,
    InvocationPolicyDecisionBundle, InvocationPolicyDisposition, InvocationTransaction,
    SafeBackendFailure,
};
use insight_platform_jobs::JobFence;
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelProviderWireConnector, ModelProviderWireEvent, ModelProviderWireProtocol,
    ModelProviderWireRequest, ModelProviderWireStream, ANTHROPIC_MESSAGES_ADAPTER_NAME,
    OPENAI_RESPONSES_ADAPTER_NAME,
};
use insight_platform_models::{
    model_failure, AccountingQuality, CanonicalFinishReason, CanonicalMessage,
    CanonicalMessagePart, CanonicalMessageRole, CanonicalModelRequest, CanonicalModelResponse,
    ClaimModelJobs, CommitModelCancellationOutcome, CommitModelOutcome, ControlModelTurn,
    CreateModelTurn, ModelAttemptMeasurement, ModelClaimSlot, ModelContentSource, ModelControlKind,
    ModelDispatchOutcome, ModelExecutionInputMaterial, ModelObservation, ModelOutputValue,
    ModelRequestValue, ModelResponseContract, ModelToolIntent, ModelToolProjection,
    ModelTurnLimits, ModelTurnTransaction, ModelUsage, ModelWorkerAudit, PrepareModelDispatch,
    SafeTraceContext,
};
use insight_platform_orchestrator::{
    derive_candidate_selection, DataPortKey, ExactDataPortRef, ExactRunValueRef,
    OrchestrationJobPayload, PlanLimits, PlanNodeKey, RunCurrentSnapshot, RuntimeDependencyKind,
    RuntimeDependencySlot, RuntimeNode, RuntimePlan, ScopeDataEnvironmentSnapshot,
    ScopeEnvironmentLimits,
};
use insight_platform_postgres::{
    model_turn_repository::{
        ClaimedModelExecution, ControlledModelExecution, PreparedModelExecution,
    },
    repository::{
        ClaimOrchestrationJobs, ContinueModelToolResultsToModelTurn,
        DeferOrchestrationModelMutationIds, DeferOrchestrationToModelTurn,
        DispatchModelToolCapabilities, JobFence as RepositoryJobFence,
        ModelToolCapabilityAdmission, ModelToolCapabilityMutationIds, NewPrincipal,
        NewQuotaAccount, NewSecretBinding, NewTenant, NewTenantPrincipal, OrchestrationClaimSlot,
        OrchestrationYieldMutationIds, PgRepository, RepositoryError, ResolvedExpressionInput,
        SafetyScanShard, StartOrchestrationJob, TypedPayload,
    },
    verify_schema,
};
use insight_platform_registry::CreateDeployment;
use insight_platform_security::BindTenantSchedulingPolicy;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{
    collections::BTreeMap,
    io::BufRead as _,
    path::{Path, PathBuf},
    process::{Child, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration as StdDuration,
};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn raw_digest(bytes: &[u8]) -> Sha256Digest {
    let mut encoded = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded.parse().unwrap()
}

fn builtin_echo_module_digest() -> Sha256Digest {
    raw_digest(b"insight.platform/v1/capability-worker/builtin\0builtin-echo-module")
}

fn named_digest(label: &str) -> Sha256Digest {
    canonical_digest(&json!({"phase3_model_turn": label}))
        .unwrap()
        .parse()
        .unwrap()
}

fn production_model_worker_manifest() -> WorkerManifest {
    WorkerManifest {
        manifest_version: WORKER_MANIFEST_VERSION,
        worker_role: "model-worker".to_owned(),
        work_class: WorkClass::Model,
        adapter_runtime_digest: named_digest("production-model-adapter-runtime"),
        protocol_version: WORKER_PROTOCOL_VERSION,
        max_concurrency: 2,
        critical_control_reserved_slots: 1,
    }
}

fn production_model_worker_manifest_digest() -> Sha256Digest {
    production_model_worker_manifest()
        .canonical_digest()
        .unwrap()
}

fn production_capability_worker_manifest() -> WorkerManifest {
    let installed = json!([{
        "adapter_id": "builtin.echo",
        "adapter_version": "1.0.0",
        "module_digest": builtin_echo_module_digest(),
        "entrypoint_id": "echo.inline",
    }]);
    WorkerManifest {
        manifest_version: WORKER_MANIFEST_VERSION,
        worker_role: "capability.native".to_owned(),
        work_class: WorkClass::CapabilityNative,
        adapter_runtime_digest: canonical_digest(&installed).unwrap().parse().unwrap(),
        protocol_version: WORKER_PROTOCOL_VERSION,
        max_concurrency: 4,
        critical_control_reserved_slots: 1,
    }
}

fn production_capability_worker_manifest_digest() -> Sha256Digest {
    production_capability_worker_manifest()
        .canonical_digest()
        .unwrap()
}

struct ProcessModelWire {
    calls: AtomicUsize,
}

struct ToolChainModelWire {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl ModelProviderWireConnector for ToolChainModelWire {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
        assert_eq!(request.protocol, ModelProviderWireProtocol::OpenAiResponses);
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
        let response = if ordinal == 0 {
            json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "model": "fixture-model-2026-08",
                    "system_fingerprint": "fixture-tool-chain",
                    "output": [{
                        "id": "fc_tool_chain",
                        "type": "function_call",
                        "status": "completed",
                        "call_id": "call_tool_chain",
                        "name": "search",
                        "arguments": "{\"query\":\"durable tool chain\"}"
                    }],
                    "usage": {"input_tokens": 50, "output_tokens": 10, "total_tokens": 60}
                }
            })
        } else {
            json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "model": "fixture-model-2026-08",
                    "system_fingerprint": "fixture-tool-chain",
                    "output": [{
                        "id": "msg_tool_chain",
                        "type": "message",
                        "status": "completed",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "{\"answer\":\"complete\"}"}]
                    }],
                    "usage": {"input_tokens": 70, "output_tokens": 10, "total_tokens": 80}
                }
            })
        };
        Ok(Box::pin(futures::stream::iter([Ok(
            ModelProviderWireEvent {
                event_name: "response.completed".to_owned(),
                data: response,
            },
        )])))
    }

    async fn cancel(
        &self,
        _protocol: ModelProviderWireProtocol,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Accepted)
    }
}

#[async_trait::async_trait]
impl ModelProviderWireConnector for ProcessModelWire {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
        assert_eq!(request.protocol, ModelProviderWireProtocol::OpenAiResponses);
        self.calls.fetch_add(1, Ordering::SeqCst);
        let events = vec![
            ModelProviderWireEvent {
                event_name: "response.created".to_owned(),
                data: json!({"type": "response.created", "response": {}}),
            },
            ModelProviderWireEvent {
                event_name: "response.completed".to_owned(),
                data: json!({
                    "type": "response.completed",
                    "response": {
                        "status": "completed",
                        "model": "fixture-model-2026-08",
                        "system_fingerprint": "fixture-fingerprint",
                        "output": [{
                            "id": "msg_process",
                            "type": "message",
                            "status": "completed",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "{\"answer\":\"recovered\"}"}]
                        }],
                        "usage": {"input_tokens": 50, "output_tokens": 10, "total_tokens": 60}
                    }
                }),
            },
        ];
        Ok(Box::pin(futures::stream::iter(events.into_iter().map(Ok))))
    }

    async fn cancel(
        &self,
        _protocol: ModelProviderWireProtocol,
        _request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        Ok(ModelAdapterCancelOutcome::Accepted)
    }
}

struct EmptyProcessHttp;

#[async_trait::async_trait]
impl HttpNetworkTransport for EmptyProcessHttp {
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

struct EmptyProcessGrpc;

#[async_trait::async_trait]
impl GrpcNetworkTransport for EmptyProcessGrpc {
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

struct ModelProcessTls {
    ca: String,
    server_cert: String,
    server_key: String,
    client_cert: String,
    client_key: String,
    artifact_server_cert: String,
    artifact_server_key: String,
    scheduler_cert: String,
    scheduler_key: String,
}

fn model_process_tls() -> ModelProcessTls {
    let mut parameters = CertificateParams::default();
    parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    parameters.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = CertifiedIssuer::self_signed(parameters, KeyPair::generate().unwrap()).unwrap();
    let issue = |sans, usage| {
        let mut parameters = CertificateParams::default();
        parameters.subject_alt_names = sans;
        parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        parameters.extended_key_usages = vec![usage];
        let key = KeyPair::generate().unwrap();
        let certificate = parameters.signed_by(&key, &ca).unwrap();
        (certificate.pem(), key.serialize_pem())
    };
    let (server_cert, server_key) = issue(
        vec![SanType::DnsName("egress.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (client_cert, client_key) = issue(
        vec![SanType::URI(
            MODEL_WORKER_WORKLOAD_IDENTITY.try_into().unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let (artifact_server_cert, artifact_server_key) = issue(
        vec![SanType::DnsName("artifact.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (scheduler_cert, scheduler_key) = issue(
        vec![SanType::URI(
            SCHEDULER_WORKLOAD_IDENTITY.try_into().unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    ModelProcessTls {
        ca: ca.pem(),
        server_cert,
        server_key,
        client_cert,
        client_key,
        artifact_server_cert,
        artifact_server_key,
        scheduler_cert,
        scheduler_key,
    }
}

struct ModelTypedPlanBroker {
    bytes: Vec<u8>,
    reads: AtomicUsize,
}

#[async_trait::async_trait]
impl SchedulerTypedPlanResponseBroker for ModelTypedPlanBroker {
    async fn read_typed_plan_for_response(
        &self,
        request: SchedulerTypedPlanReadRequest,
    ) -> Result<LeasedArtifactBytes, SchedulerTypedPlanReadError> {
        request
            .validate_at(Utc::now())
            .map_err(|_| SchedulerTypedPlanReadError::Denied)?;
        let value: serde_json::Value = serde_json::from_slice(&self.bytes)
            .map_err(|_| SchedulerTypedPlanReadError::Integrity)?;
        let digest: Sha256Digest = canonical_digest(&value)
            .map_err(|_| SchedulerTypedPlanReadError::Integrity)?
            .parse()
            .map_err(|_| SchedulerTypedPlanReadError::Integrity)?;
        if digest != *request.artifact.content_digest()
            || u64::try_from(self.bytes.len()).ok() != Some(request.artifact.byte_length())
        {
            return Err(SchedulerTypedPlanReadError::Integrity);
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(LeasedArtifactBytes::new(self.bytes.clone(), ()))
    }
}

#[async_trait::async_trait]
impl SchedulerRunValueResponseBroker for ModelTypedPlanBroker {
    async fn read_run_value_for_response(
        &self,
        _request: SchedulerRunValueReadRequest,
    ) -> Result<LeasedArtifactBytes, SchedulerRunValueReadError> {
        Err(SchedulerRunValueReadError::Denied)
    }
}

#[async_trait::async_trait]
impl SchedulerSkillPackageResponseBroker for ModelTypedPlanBroker {
    async fn read_skill_package_for_response(
        &self,
        _request: SchedulerSkillPackageReadRequest,
    ) -> Result<LeasedArtifactBytes, SchedulerSkillPackageReadError> {
        Err(SchedulerSkillPackageReadError::Denied)
    }
}

fn model_orchestration_process_config(artifact_endpoint: String) -> serde_json::Value {
    json!({
        "schema_version": 1,
        "worker_manifest": WorkerManifest {
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: "orchestration-worker".to_owned(),
            work_class: WorkClass::Orchestration,
            adapter_runtime_digest: named_digest("production-orchestration-runtime"),
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency: 4,
            critical_control_reserved_slots: 1,
        },
        "database": {
            "business_max_connections": 4,
            "critical_control_reserved_connections": 2,
            "process_connection_budget": 6,
            "acquire_timeout_milliseconds": 1000,
            "statement_timeout_milliseconds": 30000,
            "idle_timeout_milliseconds": 60000,
            "max_lifetime_milliseconds": 600000
        },
        "artifact": {
            "endpoint": artifact_endpoint,
            "tls_server_name": "artifact.test",
            "connect_timeout_milliseconds": 1000,
            "request_timeout_milliseconds": 5000,
            "maximum_request_bytes": 262144,
            "maximum_chunk_bytes": 262144
        },
        "timing": {
            "coordinator_coalesce_milliseconds": 5,
            "coordinator_scan_milliseconds": 20,
            "coordinator_scan_jitter_milliseconds": 1,
            "claim_failure_backoff_milliseconds": 5,
            "drain_grace_milliseconds": 1000,
            "heartbeat_jitter_milliseconds": 1,
            "store_retry_backoff_milliseconds": 5,
            "safety_scan_milliseconds": 20,
            "safety_scan_jitter_milliseconds": 1,
            "safety_failure_backoff_milliseconds": 5,
            "handoff_retry_milliseconds": 5
        },
        "plan_maximum_bytes": 1048576,
        "safety_shard": SafetyScanShard::whole()
    })
}

fn model_tool_chain_worker_config(egress_endpoint: String, nats_url: String) -> serde_json::Value {
    let manifest = production_model_worker_manifest();
    let manifest_digest = manifest.canonical_digest().unwrap();
    json!({
        "schema_version": 1,
        "worker_manifest": manifest,
        "installed_adapters": [
            {
                "qualified_name": OPENAI_RESPONSES_ADAPTER_NAME,
                "worker_manifest_digest": manifest_digest,
                "adapter_contract_digest": digest('3')
            },
            {
                "qualified_name": ANTHROPIC_MESSAGES_ADAPTER_NAME,
                "worker_manifest_digest": manifest_digest,
                "adapter_contract_digest": named_digest("anthropic-production-adapter")
            }
        ],
        "database_max_connections": 4,
        "database_acquire_timeout_milliseconds": 1000,
        "egress_endpoint": egress_endpoint,
        "egress_tls_server_name": "egress.test",
        "egress_connect_timeout_milliseconds": 1000,
        "egress_request_timeout_milliseconds": 30000,
        "maximum_rpc_metadata_bytes": 65536,
        "maximum_rpc_payload_bytes": 1048576,
        "live_delta": {
            "servers": [nats_url],
            "namespace": "model_tool_chain",
            "connect_timeout_milliseconds": 1000,
            "publish_timeout_milliseconds": 1000,
            "reconnect_backoff_milliseconds": 100,
            "drain_timeout_milliseconds": 1000,
            "maximum_pending_messages": 16,
            "maximum_pending_bytes": 1048576
        },
        "receipt_ttl_seconds": 60,
        "claim_scan_milliseconds": 20,
        "claim_failure_backoff_milliseconds": 5,
        "drain_grace_milliseconds": 1000
    })
}

fn model_tool_chain_capability_worker_config() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "worker_manifest": production_capability_worker_manifest(),
        "installed_adapters": [{
            "adapter_id": "builtin.echo",
            "adapter_version": "1.0.0",
            "module_digest": builtin_echo_module_digest(),
            "entrypoint_id": "echo.inline"
        }],
        "database": {
            "business_max_connections": 4,
            "critical_control_max_connections": 2,
            "process_connection_budget": 6,
            "acquire_timeout_milliseconds": 1000
        },
        "timing": {
            "initial_scan_delay_milliseconds": 0,
            "receipt_ttl_milliseconds": 60000,
            "safety_scan_milliseconds": 20,
            "claim_failure_backoff_milliseconds": 5,
            "drain_grace_milliseconds": 1000
        }
    })
}

fn write_model_process_config(
    prefix: &Path,
    suffix: &str,
    value: &serde_json::Value,
) -> (PathBuf, String) {
    let path = PathBuf::from(format!("{}-{suffix}.json", prefix.display()));
    std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    (path, canonical_digest(value).unwrap())
}

fn spawn_model_orchestration_worker(
    binary: &str,
    config_path: &Path,
    config_digest: &str,
    database_url: &str,
    ca_path: &Path,
    cert_path: &Path,
    key_path: &Path,
) -> Child {
    std::process::Command::new(binary)
        .env("PLATFORM_ORCHESTRATION_WORKER_CONFIG", config_path)
        .env("PLATFORM_ORCHESTRATION_WORKER_CONFIG_DIGEST", config_digest)
        .env("PLATFORM_ORCHESTRATION_WORKER_DATABASE_URL", database_url)
        .env("PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_CA_PATH", ca_path)
        .env(
            "PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_CERT_PATH",
            cert_path,
        )
        .env("PLATFORM_ORCHESTRATION_WORKER_ARTIFACT_KEY_PATH", key_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn spawn_model_tool_chain_worker(
    binary: &str,
    config_path: &Path,
    config_digest: &str,
    database_url: &str,
    egress_ca_path: &Path,
    egress_cert_path: &Path,
    egress_key_path: &Path,
    nats_ca_path: &Path,
    nats_cert_path: &Path,
    nats_key_path: &Path,
) -> Child {
    std::process::Command::new(binary)
        .env("PLATFORM_MODEL_WORKER_CONFIG", config_path)
        .env("PLATFORM_MODEL_WORKER_CONFIG_DIGEST", config_digest)
        .env("PLATFORM_MODEL_WORKER_DATABASE_URL", database_url)
        .env("PLATFORM_MODEL_WORKER_EGRESS_CA_PATH", egress_ca_path)
        .env("PLATFORM_MODEL_WORKER_EGRESS_CERT_PATH", egress_cert_path)
        .env("PLATFORM_MODEL_WORKER_EGRESS_KEY_PATH", egress_key_path)
        .env("PLATFORM_MODEL_WORKER_NATS_CA_PATH", nats_ca_path)
        .env("PLATFORM_MODEL_WORKER_NATS_CERT_PATH", nats_cert_path)
        .env("PLATFORM_MODEL_WORKER_NATS_KEY_PATH", nats_key_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn spawn_model_tool_chain_capability_worker(
    binary: &str,
    config_path: &Path,
    config_digest: &str,
    database_url: &str,
) -> Child {
    std::process::Command::new(binary)
        .env("PLATFORM_CAPABILITY_NATIVE_WORKER_CONFIG", config_path)
        .env(
            "PLATFORM_CAPABILITY_NATIVE_WORKER_CONFIG_DIGEST",
            config_digest,
        )
        .env(
            "PLATFORM_CAPABILITY_NATIVE_WORKER_DATABASE_URL",
            database_url,
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn observe_process_start(child: &mut Child) -> std::thread::JoinHandle<()> {
    let stderr = child.stderr.take().unwrap();
    let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
    let logger = std::thread::spawn(move || {
        for (ordinal, line) in std::io::BufReader::new(stderr).lines().enumerate() {
            let line = line.unwrap();
            eprintln!("{line}");
            if ordinal == 0 {
                let _ = started_sender.send(line);
            }
        }
    });
    let first = started_receiver
        .recv_timeout(StdDuration::from_secs(10))
        .expect("Worker did not report startup");
    assert!(first.contains("started generation="), "{first}");
    logger
}

fn version(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), digest(character)).unwrap()
}

fn artifact(suffix: u16, character: char, purpose: &str) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        digest(character),
        16,
        "application/json",
        DataClassification::Internal,
        Some(format!("{purpose}.json")),
    )
    .unwrap()
}

fn authoring(suffix: u16, character: char) -> AuthoringPackage {
    AuthoringPackage {
        artifact: artifact(suffix, character, "authoring"),
        manifest_digest: digest(character),
    }
}

fn policy(suffix: u16, character: char) -> ExactVersionRef {
    version(ResourceKind::PolicyRevision, suffix, character)
}

fn audit(
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    base: u16,
    idempotency: char,
    request: char,
) -> CommandAudit {
    CommandAudit {
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
}

fn worker_audit(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    base: u16,
    idempotency: char,
    request: char,
) -> ModelWorkerAudit {
    ModelWorkerAudit {
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

fn capability_worker_audit(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    base: u16,
) -> CapabilityWorkerAudit {
    CapabilityWorkerAudit {
        tenant_id: tenant_id.clone(),
        worker_process_generation_id: worker_id.clone(),
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: named_digest("model-tool-worker-idempotency"),
        request_digest: named_digest("model-tool-worker-request"),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
}

fn closed_object_schema(property: &str) -> insight_platform_models::ClosedSchemaDocument {
    insight_platform_models::ClosedSchemaDocument::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            property: {
                "description": "Bounded fixture field.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "x-platform-max-bytes": 1024
            }
        },
        "required": [property]
    }))
    .unwrap()
}

fn agent_schema() -> ClosedJsonSchema {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": false
    }))
    .unwrap()
}

fn run_input_schema() -> ClosedJsonSchema {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "prompt": {
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "x-platform-max-bytes": 1024,
                "x-platform-classification": "internal"
            }
        },
        "required": ["prompt"],
        "additionalProperties": false
    }))
    .unwrap()
}

async fn execute_create(
    repository: &PgRepository,
    command: CreateModelTurn,
) -> Result<CommandOutcome<insight_platform_models::ModelTurnRecord>, RepositoryError> {
    let mut transaction = repository.begin_model_turn_transaction().await?;
    match transaction.create_model_turn(command).await {
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

async fn execute_prepare(
    repository: &PgRepository,
    command: PrepareModelDispatch,
) -> Result<CommandOutcome<PreparedModelExecution>, RepositoryError> {
    let mut transaction = repository.begin_model_turn_transaction().await?;
    match transaction.prepare_model_dispatch(command).await {
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
    command: CommitModelOutcome,
) -> Result<CommandOutcome<PreparedModelExecution>, RepositoryError> {
    let mut transaction = repository.begin_model_turn_transaction().await?;
    match transaction.commit_model_outcome(command).await {
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

async fn execute_cancellation_outcome(
    repository: &PgRepository,
    command: CommitModelCancellationOutcome,
) -> Result<CommandOutcome<PreparedModelExecution>, RepositoryError> {
    let mut transaction = repository.begin_model_turn_transaction().await?;
    match transaction.commit_model_cancellation_outcome(command).await {
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
    command: ControlModelTurn,
) -> Result<CommandOutcome<ControlledModelExecution>, RepositoryError> {
    let mut transaction = repository.begin_model_turn_transaction().await?;
    match transaction.control_model_turn(command).await {
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

#[derive(Debug)]
struct ClaimEvidence {
    claimed: ClaimedModelExecution,
    worker_id: ResourceId,
    lease_token: Sha256Digest,
    usage_reservation_id: ResourceId,
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

async fn claim_one(repository: &PgRepository, base: u16) -> ClaimEvidence {
    let worker_id = id(ResourceKind::WorkerProcessGeneration, base);
    let lease_token = digest(char::from_digit(u32::from(base % 10), 10).unwrap_or('0'));
    let usage_reservation_id = id(ResourceKind::UsageReservation, base + 1);
    let mut claims = repository
        .claim_model_jobs(ClaimModelJobs {
            worker_process_generation_id: worker_id.clone(),
            worker_manifest_digest: production_model_worker_manifest_digest(),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![ModelClaimSlot {
                lease_token_digest: lease_token.clone(),
                usage_reservation_id: usage_reservation_id.clone(),
                quota_entry_ids: (0..4)
                    .map(|offset| id(ResourceKind::QuotaLedgerEntry, base + 2 + offset))
                    .collect(),
                event_id: id(ResourceKind::Event, base + 6),
                outbox_id: id(ResourceKind::OutboxEvent, base + 7),
                resume_mutations: None,
                failure_mutations: None,
                tool_continuation_mutations: None,
            }],
        })
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    ClaimEvidence {
        claimed: claims.pop().unwrap(),
        worker_id,
        lease_token,
        usage_reservation_id,
    }
}

struct Fixture {
    tenant_id: ResourceId,
    principal_id: ResourceId,
    provider_resource_id: ResourceId,
    provider_revision: ExactVersionRef,
    provider_closure: ModelProviderDeploymentClosure,
    profile_resource_id: ResourceId,
    profile_revision: ExactVersionRef,
    model_closure: ModelDeploymentClosure,
    run_id: ResourceId,
    scope_id: ResourceId,
    primary_node_id: ResourceId,
    cancel_node_id: ResourceId,
    capability_deployment: ExactDeploymentRef,
    alternate_capability_deployment: ExactDeploymentRef,
    model_deployment: ExactDeploymentRef,
    selection_policy_binding: ExactPolicyBinding,
    invocation_policy: ExactVersionRef,
    tool_slot_binding: FrozenSlotBinding,
    capability_interface_revision: ExactVersionRef,
    argument_schema: insight_platform_models::ClosedSchemaDocument,
    capability_output_schema: insight_platform_models::ClosedSchemaDocument,
    output_schema: insight_platform_models::ClosedSchemaDocument,
    parameter_schema_digest: Sha256Digest,
    truncation_policy: ExactVersionRef,
    deadline: DateTime<Utc>,
    runtime_plan: RuntimePlan,
}

fn model_runtime_plan(
    interface_revision_id: ResourceId,
    output_schema: &ClosedJsonSchema,
) -> RuntimePlan {
    let start = PlanNodeKey::new("start".to_owned()).unwrap();
    let model = PlanNodeKey::new("model".to_owned()).unwrap();
    let finish = PlanNodeKey::new("finish".to_owned()).unwrap();
    let output = ExactDataPortRef::NodeOutput {
        producer_node_id: model.clone(),
        port_id: DataPortKey::new("response".to_owned()).unwrap(),
        schema_digest: output_schema.canonical_digest.clone(),
    };
    let plan = RuntimePlan {
        plan_version: 4,
        interface_revision_id,
        entry_node_id: start.clone(),
        dependency_slots: BTreeMap::from([
            (
                "primary_model".to_owned(),
                RuntimeDependencySlot {
                    kind: RuntimeDependencyKind::Model,
                    requirement_digest: digest('e'),
                },
            ),
            (
                "search".to_owned(),
                RuntimeDependencySlot {
                    kind: RuntimeDependencyKind::Capability,
                    requirement_digest: digest('1'),
                },
            ),
        ]),
        nodes: BTreeMap::from([
            (
                start,
                RuntimeNode::Start {
                    next: model.clone(),
                },
            ),
            (
                model.clone(),
                RuntimeNode::ModelLoop {
                    model_slot_id: "primary_model".to_owned(),
                    skill_slot_ids: vec![],
                    capability_slot_ids: vec!["search".to_owned()],
                    input: ExactDataPortRef::RunInput {
                        schema_digest: run_input_schema().canonical_digest,
                    },
                    model_route: None,
                    output: output.clone(),
                    maximum_rounds: 4,
                    maximum_capability_calls: 8,
                    maximum_parallel_calls_per_round: 2,
                    token_budget: 4_096,
                    resume: finish.clone(),
                },
            ),
            (finish, RuntimeNode::Return { value: output }),
        ]),
    };
    plan.validate(PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap())
        .unwrap();
    plan
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

#[allow(clippy::too_many_arguments)]
async fn insert_version(
    pool: &PgPool,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
    resource_kind: RegistryResourceKind,
    exact: &ExactVersionRef,
    revision_no: i64,
    principal_id: &ResourceId,
    published: PublishedVersionPayload,
) {
    published
        .validate_for(resource_kind, &exact.revision_id)
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

async fn insert_ready_artifact(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    retention_policy_revision_id: &ResourceId,
    artifact: &ArtifactRef,
    suffix: u16,
) {
    let blob_id = id(ResourceKind::InternalBlob, suffix);
    let database_now = Utc::now();
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
    .bind(digest('1').to_string())
    .bind(digest('2').to_string())
    .bind(vec![1_u8, 2, 3])
    .bind(id(ResourceKind::Policy, suffix).to_string())
    .bind(artifact.content_digest().to_string())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(database_now)
    .execute(pool)
    .await
    .unwrap();
    let metadata = TypedPayload::new(1, &json!({"fixture": "model-conformance"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification, expected_size_bytes,
            expected_digest, declared_media_type, verified_media_type, state, version,
            metadata_schema_version, metadata, metadata_digest, retention_policy_revision_id,
            retain_until, created_by, created_at, updated_at
        ) VALUES ($1, $2, $3, 'conformance', $4, $5, $6, $7, $7, 'ready', 1,
                  $8, $9, $10, $11, $12, $13, $14, $14)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(blob_id.to_string())
    .bind(artifact.classification().as_str())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(artifact.content_digest().to_string())
    .bind(artifact.media_type())
    .bind(metadata.schema_version)
    .bind(&metadata.value)
    .bind(&metadata.digest)
    .bind(retention_policy_revision_id.to_string())
    .bind(database_now + Duration::days(1))
    .bind(principal_id.to_string())
    .bind(database_now)
    .execute(pool)
    .await
    .unwrap();
}

fn validation() -> ValidationSummary {
    ValidationSummary {
        validator_digest: digest('8'),
        validated_draft_digest: digest('9'),
        dependency_closure_digest: digest('a'),
        security_evidence_digest: digest('b'),
        warnings: vec![],
    }
}

async fn seed_policy_versions(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    policy_resource: &ResourceId,
    policies: &[ExactVersionRef],
) {
    for (index, exact) in policies.iter().enumerate() {
        let selection = (index == 8).then_some(CandidateSelectionPolicyDocument {
            schema_version: 1,
            mode: CandidateSelectionMode::OrderedFirst,
            route_schema_digest: None,
        });
        let scheduling = (index == 12).then_some(SchedulingPolicyDocument {
            version: 1,
            weight: 1,
            burst: 4,
            aging_rounds: 4,
        });
        let model_budget = (index == 6).then_some(ModelBudgetPolicyDocument {
            schema_version: 1,
            maximum_attempts_per_turn: 3,
            maximum_input_tokens_per_turn: 3_000,
            maximum_output_tokens_per_turn: 512,
            maximum_total_tokens_per_turn: 3_512,
            cost_ceiling_microunits_per_turn: 1_000_000,
        });
        let model_public_projection = (index == 7).then_some(ModelPublicProjectionPolicyDocument {
            schema_version: 1,
            reject_prompt_overflow: true,
            retain_source_map: true,
            retain_sensitive_prompt_body: false,
        });
        let safety_instruction = "Follow exact platform safety and authorization contracts.";
        let model_safety = (index == 9).then(|| ModelSafetyPolicyDocument {
            schema_version: 1,
            contract_id: "platform.model_safety.v1".to_owned(),
            platform_instruction: safety_instruction.to_owned(),
            instruction_content_digest: raw_digest(safety_instruction.as_bytes()),
            instruction_byte_budget: 1_024,
            instruction_token_budget: 256,
            pre_dispatch_rules_digest: digest('1'),
            post_response_rules_digest: digest('2'),
        });
        let policy_kind = match index {
            0 => PolicyKind::Protocol,
            1 => PolicyKind::Network,
            2 => PolicyKind::Tls,
            3 => PolicyKind::Trust,
            4 | 5 => PolicyKind::DataHandling,
            6 => PolicyKind::Budget,
            7 => PolicyKind::PublicProjection,
            8 => PolicyKind::Selection,
            9 => PolicyKind::ModelSafety,
            10 => PolicyKind::Authorization,
            11 => PolicyKind::Execution,
            12 => PolicyKind::Scheduling,
            _ => unreachable!(),
        };
        let rules_digest = if let Some(document) = &selection {
            document.canonical_digest().unwrap()
        } else if let Some(document) = &scheduling {
            document.canonical_digest().unwrap()
        } else if let Some(document) = &model_budget {
            document.canonical_digest().unwrap()
        } else if let Some(document) = &model_public_projection {
            document.canonical_digest().unwrap()
        } else if let Some(document) = &model_safety {
            document.canonical_digest().unwrap()
        } else {
            digest('e')
        };
        insert_version(
            pool,
            tenant_id,
            policy_resource,
            RegistryResourceKind::Policy,
            exact,
            i64::try_from(index + 1).unwrap(),
            principal_id,
            PublishedVersionPayload {
                document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                    authoring_package: authoring(0x90 + u16::try_from(index).unwrap(), 'c'),
                    contract_digest: digest('d'),
                    dependency_versions: vec![],
                    policy_versions: vec![],
                    policy_kind,
                    rules_digest,
                    selection,
                    scheduling,
                    retention: None,
                    model_safety,
                    model_budget,
                    model_public_projection,
                    mcp_protocol: None,
                    mcp_auth: None,
                    sandbox_isolation: None,
                    sandbox_resource: None,
                    sandbox_network: None,
                    sandbox_artifact_io: None,
                    sandbox_secret_resolution: None,
                })),
                validation: validation(),
            },
        )
        .await;
    }
}

async fn seed_fixture(pool: &PgPool, repository: &PgRepository) -> Fixture {
    let tenant_id = id(ResourceKind::Tenant, 1);
    let principal_id = id(ResourceKind::Principal, 2);
    repository
        .create_tenant(NewTenant {
            tenant_id: tenant_id.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    repository
        .create_principal(NewPrincipal {
            principal_id: principal_id.clone(),
            authentication_authority_digest: named_digest("authentication_authority"),
            subject_digest: named_digest("subject"),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant_id.clone(),
            principal_id: principal_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::CapabilityInvoke,
                    Permission::ModelInvoke,
                    Permission::ModelDeploy,
                    Permission::RuntimeControl,
                    Permission::TenantManage,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();

    let policy_resource = id(ResourceKind::Policy, 0x10);
    let agent_resource = id(ResourceKind::Agent, 0x11);
    let provider_resource = id(ResourceKind::ModelProvider, 0x12);
    let profile_resource = id(ResourceKind::ModelProfile, 0x13);
    let capability_interface_resource = id(ResourceKind::CapabilityInterface, 0x14);
    let capability_implementation_resource = id(ResourceKind::CapabilityImplementation, 0x15);
    for (resource_id, kind) in [
        (&policy_resource, RegistryResourceKind::Policy),
        (&agent_resource, RegistryResourceKind::Agent),
        (&provider_resource, RegistryResourceKind::ModelProvider),
        (&profile_resource, RegistryResourceKind::ModelProfile),
        (
            &capability_interface_resource,
            RegistryResourceKind::CapabilityInterface,
        ),
        (
            &capability_implementation_resource,
            RegistryResourceKind::CapabilityImplementation,
        ),
    ] {
        insert_resource(pool, &tenant_id, resource_id, kind, &principal_id).await;
    }

    let protocol_policy = policy(0x20, '1');
    let network_policy = policy(0x21, '2');
    let tls_policy = policy(0x22, '3');
    let trust_policy = policy(0x23, '4');
    let provider_data_policy = policy(0x24, '5');
    let model_data_policy = policy(0x25, '6');
    let budget_policy = policy(0x26, '7');
    let projection_policy = policy(0x27, '8');
    let selection_policy = policy(0x28, '9');
    let truncation_policy = policy(0x29, 'a');
    let agent_policy = policy(0x2a, 'b');
    let execution_profile = policy(0x2b, 'c');
    let scheduling_policy = policy(0x2c, 'd');
    let policies = vec![
        protocol_policy.clone(),
        network_policy.clone(),
        tls_policy.clone(),
        trust_policy.clone(),
        provider_data_policy.clone(),
        model_data_policy.clone(),
        budget_policy.clone(),
        projection_policy.clone(),
        selection_policy.clone(),
        truncation_policy.clone(),
        agent_policy.clone(),
        execution_profile.clone(),
        scheduling_policy.clone(),
    ];
    seed_policy_versions(pool, &tenant_id, &principal_id, &policy_resource, &policies).await;

    let provider_revision = version(ResourceKind::ModelProviderRevision, 0x30, 'd');
    let provider_secret_binding_id = id(ResourceKind::SecretBinding, 0x31);
    let secret_provider_id = id(ResourceKind::SecretProvider, 0x31);
    let provider_secret_purpose = "provider.api_key".parse::<SecretPurpose>().unwrap();
    let secret_resolution_policy = SecretResolutionPolicy::Pinned {
        opaque_version_identity_digest: digest('0'),
    };
    repository
        .create_secret_binding(NewSecretBinding {
            tenant_id: tenant_id.clone(),
            secret_binding_id: provider_secret_binding_id.clone(),
            purpose: provider_secret_purpose.clone(),
            provider_id: secret_provider_id.clone(),
            opaque_reference_ciphertext: vec![1, 2, 3],
            key_id: "fixture-key".to_owned(),
            reference_digest: digest('f'),
            payload: SecretBindingPayload {
                provider_id: secret_provider_id.clone(),
                resolution_policy: secret_resolution_policy.clone(),
            },
        })
        .await
        .unwrap();
    let provider_secret_binding = ExactSecretBindingRef::build(
        provider_secret_binding_id,
        1,
        secret_provider_id,
        provider_secret_purpose,
        secret_resolution_policy,
    )
    .unwrap();
    let parameter_schema_digest = digest('e');
    let region: DataRegion = "cn-east-1".parse().unwrap();
    let provider = ModelProviderResourceSpec {
        authoring_package: authoring(0xa0, 'f'),
        contract_digest: digest('1'),
        dependency_versions: vec![protocol_policy.clone()],
        policy_versions: vec![protocol_policy.clone()],
        installed_adapter: InstalledModelAdapter {
            qualified_name: OPENAI_RESPONSES_ADAPTER_NAME.to_owned(),
            worker_manifest_digest: production_model_worker_manifest_digest(),
            adapter_contract_digest: digest('3'),
        },
        protocol_policy: protocol_policy.clone(),
        credential_requirements: vec!["provider.api_key".parse::<SecretPurpose>().unwrap()],
        request_limits: ProviderRequestLimits {
            maximum_request_bytes: 1_048_576,
            // Leave bounded room for the canonical ModelOutputValue envelope inside
            // the shared 1 MiB Inline RunValue contract.
            maximum_response_bytes: 524_288,
            maximum_messages: 32,
            maximum_parts: 64,
            maximum_tools: 8,
            maximum_parallel_tool_calls: 8,
            maximum_stream_delta_bytes: 262_144,
            connect_timeout_milliseconds: 1_000,
            first_byte_timeout_milliseconds: 2_000,
            idle_timeout_milliseconds: 3_000,
            total_timeout_milliseconds: 30_000,
        },
    };
    let implementation_authoring = authoring(0xa5, '5');
    insert_ready_artifact(
        pool,
        &tenant_id,
        &principal_id,
        &agent_policy.revision_id,
        &implementation_authoring.artifact,
        0xa5,
    )
    .await;
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
        &provider_resource,
        RegistryResourceKind::ModelProvider,
        &provider_revision,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::ModelProvider(provider),
            validation: validation(),
        },
    )
    .await;

    let provider_conformance = artifact(0xa1, '5', "conformance");
    insert_ready_artifact(
        pool,
        &tenant_id,
        &principal_id,
        &provider_data_policy.revision_id,
        &provider_conformance,
        0xa1,
    )
    .await;
    let provider_closure = ModelProviderDeploymentClosure {
        provider_revision: provider_revision.clone(),
        endpoint_identity_digest: digest('4'),
        secret_bindings: vec![provider_secret_binding],
        protocol_policy: protocol_policy.clone(),
        network_policy,
        tls_policy,
        trust_policy,
        data_policy: provider_data_policy,
        region: region.clone(),
        conformance_evidence: provider_conformance,
    };
    let provider_payload = TypedPayload::new(
        1,
        &DeploymentClosure::ModelProvider(provider_closure.clone()),
    )
    .unwrap();
    let provider_deployment_id = id(ResourceKind::ModelProviderDeployment, 0x32);
    insert_deployment(
        pool,
        &tenant_id,
        &provider_deployment_id,
        &provider_resource,
        &provider_revision.revision_id,
        &principal_id,
        &provider_payload,
    )
    .await;
    let provider_deployment = ExactDeploymentRef::new(
        provider_deployment_id,
        provider_payload.digest.parse().unwrap(),
    )
    .unwrap();

    let profile_revision = version(ResourceKind::ModelProfileRevision, 0x33, '6');
    let profile = ModelProfileResourceSpec {
        authoring_package: authoring(0xa2, '7'),
        contract_digest: digest('8'),
        dependency_versions: vec![provider_revision.clone()],
        policy_versions: vec![model_data_policy.clone()],
        provider_revision: provider_revision.clone(),
        model_identity: ProviderModelIdentity {
            value: "fixture-model-2026-08".to_owned(),
            stability: ModelIdentityStability::Pinned,
        },
        modalities: ModelModalities {
            input: vec![insight_platform_contracts::ModelModality::Text],
            output: vec![insight_platform_contracts::ModelModality::Text],
        },
        context: ContextWindowContract {
            maximum_context_tokens: 4_096,
            maximum_output_tokens: 512,
            tokenizer_contract_digest: digest('9'),
            estimator_contract_digest: digest('a'),
        },
        tools: ModelToolContract {
            supported: true,
            parallel: true,
            maximum_tools: 8,
            maximum_calls_per_turn: 8,
            maximum_argument_bytes: 16_384,
        },
        structured_output: StructuredOutputContract {
            native: true,
            textual_json_fallback: true,
            may_combine_with_tool_intent: false,
            maximum_schema_bytes: 65_536,
            maximum_output_bytes: 1_048_576,
        },
        parameter_schema_digest: parameter_schema_digest.clone(),
        usage: ModelUsageContract {
            provider_reports_usage: true,
            reports_cached_input_tokens: false,
            reports_reasoning_tokens: false,
            // OpenAI Responses reports token usage but not an authoritative monetary cost.
            reports_cost: false,
            cost_currency: None,
            estimator_contract_digest: digest('a'),
        },
        data_handling: ProviderDataHandlingContract {
            maximum_classification: DataClassification::Confidential,
            allowed_regions: vec![region],
            maximum_retention_milliseconds: 86_400_000,
            training: ProviderTrainingPolicy::Prohibited,
            subprocessor_set_digest: digest('b'),
        },
        limits: ModelLimits {
            maximum_messages: 16,
            maximum_parts: 32,
            maximum_text_bytes: 32_768,
            maximum_tools: 8,
            maximum_parallel_tool_calls: 8,
            maximum_rounds: 8,
            maximum_input_tokens: 3_000,
            maximum_output_tokens: 512,
        },
        catalog_evidence: ModelCatalogEvidence {
            artifact: artifact(0xa3, 'c', "catalog"),
            source_digest: digest('d'),
            adapter_contract_digest: digest('3'),
            observed_at: Utc::now() - Duration::minutes(1),
            expires_at: Utc::now() + Duration::days(1),
        },
    };
    insert_version(
        pool,
        &tenant_id,
        &profile_resource,
        RegistryResourceKind::ModelProfile,
        &profile_revision,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::ModelProfile(Box::new(profile)),
            validation: validation(),
        },
    )
    .await;

    let model_closure = ModelDeploymentClosure {
        profile_revision: profile_revision.clone(),
        provider_deployment: provider_deployment.clone(),
        data_policy: model_data_policy,
        safety_policy: truncation_policy.clone(),
        budget_policy,
        public_projection_policy: projection_policy,
        generation_defaults: ClosedJsonValue::build(
            parameter_schema_digest.clone(),
            json!({"temperature": 0}),
        )
        .unwrap(),
    };
    let model_payload =
        TypedPayload::new(1, &DeploymentClosure::ModelProfile(model_closure.clone())).unwrap();
    let model_deployment_id = id(ResourceKind::ModelDeployment, 0x34);
    insert_deployment(
        pool,
        &tenant_id,
        &model_deployment_id,
        &profile_resource,
        &profile_revision.revision_id,
        &principal_id,
        &model_payload,
    )
    .await;
    let model_deployment =
        ExactDeploymentRef::new(model_deployment_id, model_payload.digest.parse().unwrap())
            .unwrap();

    let argument_schema = closed_object_schema("query");
    let capability_output_schema = argument_schema.clone();
    let output_schema = closed_object_schema("answer");
    let interface_revision = version(ResourceKind::CapabilityInterfaceRevision, 0x35, 'e');
    let implementation_revision =
        version(ResourceKind::CapabilityImplementationRevision, 0x36, 'f');
    insert_version(
        pool,
        &tenant_id,
        &capability_interface_resource,
        RegistryResourceKind::CapabilityInterface,
        &interface_revision,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::CapabilityInterface(CapabilityInterfaceResourceSpec {
                authoring_package: authoring(0xa4, '1'),
                contract_digest: digest('2'),
                dependency_versions: vec![],
                policy_versions: vec![agent_policy.clone()],
                qualified_name: "fixture.model_tool".parse().unwrap(),
                input_schema: argument_schema.clone(),
                output_schema: capability_output_schema.clone(),
                error_schema: closed_object_schema("error"),
                artifacts: CapabilityArtifactContract { ports: vec![] },
                data_policy: CapabilityDataFlowPolicy {
                    maximum_input_classification: DataClassification::Restricted,
                    maximum_output_classification: DataClassification::Restricted,
                    allowed_regions: vec!["global".parse::<DataRegion>().unwrap()],
                    declassification_policy: None,
                },
                execution_limits: CapabilityInterfaceLimits {
                    maximum_input_bytes: 1_048_576,
                    maximum_output_bytes: 1_048_576,
                    maximum_artifacts: 0,
                    maximum_execution_milliseconds: 60_000,
                },
                effect: Effect::ReadOnly,
                idempotency: CapabilityIdempotencyKind::CallerKey,
                cancellation: CapabilityCancellationKind::BestEffort,
                progress: CapabilityProgressContract {
                    mode: CapabilityProgressMode::None,
                    schema_digest: None,
                    max_events: 0,
                    max_bytes_per_event: 0,
                    minimum_interval_milliseconds: 0,
                    durability: CapabilityProgressDurability::None,
                },
            }),
            validation: validation(),
        },
    )
    .await;
    insert_version(
        pool,
        &tenant_id,
        &capability_implementation_resource,
        RegistryResourceKind::CapabilityImplementation,
        &implementation_revision,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::CapabilityImplementation(
                CapabilityImplementationResourceSpec {
                    authoring_package: implementation_authoring.clone(),
                    contract_digest: digest('6'),
                    dependency_versions: vec![interface_revision.clone()],
                    policy_versions: vec![agent_policy.clone()],
                    interface_revision: interface_revision.clone(),
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
                        deferred: false,
                        input_required: false,
                        callback: false,
                        poll: false,
                        progress: false,
                        cancellation: true,
                        max_remote_state_bytes: 0,
                        max_poll_count: 0,
                    },
                },
            ),
            validation: validation(),
        },
    )
    .await;
    let capability_closure = CapabilityDeploymentClosure {
        implementation: implementation_revision,
        interface: interface_revision.clone(),
        backend: CapabilityBackendBinding::Native {
            worker_manifest_digest: production_capability_worker_manifest_digest(),
            adapter_module_digest: builtin_echo_module_digest(),
        },
        secret_bindings: vec![],
        policies: vec![agent_policy.clone()],
        conformance_evidence: implementation_authoring.artifact,
    };
    let capability_payload = TypedPayload::new(
        1,
        &DeploymentClosure::CapabilityInterface(capability_closure.clone()),
    )
    .unwrap();
    let capability_deployment_id = id(ResourceKind::CapabilityDeployment, 0x37);
    insert_deployment(
        pool,
        &tenant_id,
        &capability_deployment_id,
        &capability_interface_resource,
        &interface_revision.revision_id,
        &principal_id,
        &capability_payload,
    )
    .await;
    let capability_deployment = ExactDeploymentRef::new(
        capability_deployment_id,
        capability_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let alternate_capability_deployment_id = id(ResourceKind::CapabilityDeployment, 0x38);
    let mut alternate_capability_closure = capability_closure;
    alternate_capability_closure
        .policies
        .push(selection_policy.clone());
    let alternate_capability_payload = TypedPayload::new(
        1,
        &DeploymentClosure::CapabilityInterface(alternate_capability_closure),
    )
    .unwrap();
    insert_deployment(
        pool,
        &tenant_id,
        &alternate_capability_deployment_id,
        &capability_interface_resource,
        &interface_revision.revision_id,
        &principal_id,
        &alternate_capability_payload,
    )
    .await;
    let alternate_capability_deployment = ExactDeploymentRef::new(
        alternate_capability_deployment_id,
        alternate_capability_payload.digest.parse().unwrap(),
    )
    .unwrap();
    for (account_id, scope_kind, scope_id, metric) in [
        (
            id(ResourceKind::QuotaAccount, 0x44),
            "tenant",
            tenant_id.clone(),
            QuotaDimension::WorkClassConcurrentOperations,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x45),
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
                payload: TypedPayload::new(1, &json!({"fixture": "model-tool"})).unwrap(),
            })
            .await
            .unwrap();
    }

    let agent_interface = version(ResourceKind::AgentInterfaceRevision, 0x38, '8');
    let runtime_plan = model_runtime_plan(agent_interface.revision_id.clone(), &output_schema);
    let runtime_plan_digest = runtime_plan
        .canonical_digest(PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap())
        .unwrap();
    let agent_plan = ExactVersionRef::new(
        id(ResourceKind::AgentPlanRevision, 0x39),
        runtime_plan_digest.clone(),
    )
    .unwrap();
    let runtime_plan_bytes = canonical_json(&serde_json::to_value(&runtime_plan).unwrap()).unwrap();
    let agent_document = ResourceDocument::Agent(AgentResourceSpec {
        authoring_package: authoring(0xa6, 'a'),
        contract_digest: digest('b'),
        dependency_versions: vec![],
        policy_versions: vec![agent_policy.clone()],
        input_schema: run_input_schema(),
        output_schema: output_schema.clone(),
        error_schema: agent_schema(),
        typed_plan_artifact_id: id(ResourceKind::Artifact, 0xa6),
        typed_plan_digest: runtime_plan_digest.clone(),
    });
    for (exact, revision_no) in [(&agent_interface, 1), (&agent_plan, 2)] {
        insert_version(
            pool,
            &tenant_id,
            &agent_resource,
            RegistryResourceKind::Agent,
            exact,
            revision_no,
            &principal_id,
            PublishedVersionPayload {
                document: agent_document.clone(),
                validation: validation(),
            },
        )
        .await;
    }
    let typed_plan_artifact = ArtifactRef::new(
        id(ResourceKind::Artifact, 0xa6),
        runtime_plan_digest,
        u64::try_from(runtime_plan_bytes.len()).unwrap(),
        "application/json",
        DataClassification::Internal,
        Some("model-runtime-plan.json".to_owned()),
    )
    .unwrap();
    insert_ready_artifact(
        pool,
        &tenant_id,
        &principal_id,
        &agent_policy.revision_id,
        &typed_plan_artifact,
        0xb6,
    )
    .await;
    sqlx::query(
        "UPDATE insight_platform.artifacts SET purpose = 'typed_plan' WHERE tenant_id = $1 AND artifact_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(typed_plan_artifact.artifact_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resource_versions SET artifact_id = $3 WHERE tenant_id = $1 AND resource_version_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(agent_plan.revision_id.to_string())
    .bind(typed_plan_artifact.artifact_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    let selection_policy_deployment_id = id(ResourceKind::PolicyDeployment, 0x3b);
    let selection_policy_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: selection_policy.clone(),
            applicability_digest: digest('c'),
            qualification_evidence: authoring(0xb8, '8').artifact,
        }),
    )
    .unwrap();
    insert_deployment(
        pool,
        &tenant_id,
        &selection_policy_deployment_id,
        &policy_resource,
        &selection_policy.revision_id,
        &principal_id,
        &selection_policy_payload,
    )
    .await;
    let selection_policy_binding = ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(
            selection_policy_deployment_id,
            selection_policy_payload.digest.parse().unwrap(),
        )
        .unwrap(),
        revision: selection_policy.clone(),
    };
    let scheduling_policy_deployment_id = id(ResourceKind::PolicyDeployment, 0x3e);
    let scheduling_policy_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: scheduling_policy,
            applicability_digest: digest('f'),
            qualification_evidence: authoring(0xb9, '9').artifact,
        }),
    )
    .unwrap();
    insert_deployment(
        pool,
        &tenant_id,
        &scheduling_policy_deployment_id,
        &policy_resource,
        &id(ResourceKind::PolicyRevision, 0x2c),
        &principal_id,
        &scheduling_policy_payload,
    )
    .await;
    sqlx::query(
        "UPDATE insight_platform.resources SET active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(policy_resource.to_string())
    .bind(scheduling_policy_deployment_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let scheduling_policy_deployment = ExactDeploymentRef::new(
        scheduling_policy_deployment_id,
        scheduling_policy_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let mut security = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        security
            .bind_tenant_scheduling_policy(BindTenantSchedulingPolicy {
                audit: audit(&tenant_id, &principal_id, 0xc0, 'c', 'd'),
                expected_tenant_version: 1,
                policy: scheduling_policy_deployment,
            })
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    security.commit().await.unwrap();
    let agent_policy_binding = ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(id(ResourceKind::PolicyDeployment, 0x3c), digest('d'))
            .unwrap(),
        revision: agent_policy.clone(),
    };
    let execution_profile_binding = ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(id(ResourceKind::PolicyDeployment, 0x3d), digest('e'))
            .unwrap(),
        revision: execution_profile,
    };
    let model_slot_binding = FrozenSlotBinding {
        slot_id: "primary_model".to_owned(),
        requirement_digest: digest('e'),
        target: FrozenSlotTarget::Model {
            candidates: vec![model_deployment.clone()],
            selection_policy: selection_policy_binding.clone(),
        },
        binding_digest: digest('f'),
    };
    let tool_slot_binding = FrozenSlotBinding {
        slot_id: "search".to_owned(),
        requirement_digest: digest('1'),
        target: FrozenSlotTarget::Capability {
            candidates: vec![
                capability_deployment.clone(),
                alternate_capability_deployment.clone(),
            ],
            selection_policy: selection_policy_binding.clone(),
            tool_alias: Some("search".to_owned()),
        },
        binding_digest: digest('2'),
    };
    let agent_closure = AgentDeploymentClosure {
        interface: agent_interface,
        plan: agent_plan.clone(),
        entry_node_id: "start".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
        slots: vec![model_slot_binding, tool_slot_binding.clone()],
        policies: vec![agent_policy_binding],
        execution_profile: execution_profile_binding,
    };
    let agent_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(agent_closure.clone())).unwrap();
    let agent_deployment_id = id(ResourceKind::AgentDeployment, 0x3a);
    insert_deployment(
        pool,
        &tenant_id,
        &agent_deployment_id,
        &agent_resource,
        &agent_plan.revision_id,
        &principal_id,
        &agent_payload,
    )
    .await;

    for (account_id, scope_kind, scope_id, metric, limit_value) in [
        (
            id(ResourceKind::QuotaAccount, 0x40),
            "tenant",
            tenant_id.clone(),
            QuotaDimension::WorkClassConcurrentOperations,
            16,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x41),
            "model_deployment",
            model_deployment.deployment_id.clone(),
            QuotaDimension::ModelRequests,
            16,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x42),
            "model_deployment",
            model_deployment.deployment_id.clone(),
            QuotaDimension::ModelTokens,
            16_384,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x43),
            "model_deployment",
            model_deployment.deployment_id.clone(),
            QuotaDimension::ModelCostMicrounits,
            2_000_000,
        ),
    ] {
        repository
            .create_quota_account(NewQuotaAccount {
                tenant_id: tenant_id.to_string(),
                quota_account_id: account_id.to_string(),
                scope_kind: scope_kind.to_owned(),
                scope_id: scope_id.to_string(),
                work_class: WorkClass::Model.as_str().to_owned(),
                metric: metric.as_str().to_owned(),
                limit_value,
                payload: TypedPayload::new(1, &json!({"fixture": "model_turn"})).unwrap(),
            })
            .await
            .unwrap();
    }

    let run_id = id(ResourceKind::Run, 0x50);
    let scope_id = id(ResourceKind::ScopeInstance, 0x51);
    let primary_node_id = id(ResourceKind::NodeExecution, 0x52);
    let cancel_node_id = id(ResourceKind::NodeExecution, 0x53);
    let seed_input_value_id = id(ResourceKind::RunValue, 0x54);
    let principal_snapshot = PrincipalSnapshot::build(
        tenant_id.clone(),
        principal_id.clone(),
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![
            Permission::CapabilityInvoke,
            Permission::ModelInvoke,
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
            agent_deployment_id.clone(),
            agent_payload.digest.parse().unwrap(),
        )
        .unwrap(),
        principal_snapshot,
        &agent_closure,
    )
    .unwrap();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT statement_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let deadline = now + Duration::hours(1);
    let current = RunCurrentSnapshot::initial(
        run_id.clone(),
        agent_deployment_id.clone(),
        seed_input_value_id,
    );
    let bindings_payload = TypedPayload::from_versioned(1, &run_bindings, 1_048_576).unwrap();
    let current_payload = TypedPayload::from_versioned(1, &current, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.runs (
            tenant_id, run_id, root_run_id, agent_deployment_id, principal_id,
            state, version, bindings_schema_version, bindings, bindings_digest,
            current_schema_version, current_payload, current_payload_digest,
            deadline, started_at, created_at, updated_at
        ) VALUES (
            $1, $2, $2, $3, $4, 'running', 1, $5, $6, $7,
            $8, $9, $10, $11, $12, $12, $12
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .bind(agent_deployment_id.to_string())
    .bind(principal_id.to_string())
    .bind(bindings_payload.schema_version)
    .bind(&bindings_payload.value)
    .bind(run_bindings.canonical_digest.to_string())
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(deadline)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    let seed_input = json!({"prompt": "fixture"});
    let seed_input_digest: Sha256Digest = canonical_digest(&seed_input).unwrap().parse().unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, value_kind, classification,
            schema_digest, content_digest, inline_value
        ) VALUES ($1, $2, $3, 'run_input', 'internal', $4, $5, $6)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(id(ResourceKind::RunValue, 0x54).to_string())
    .bind(run_id.to_string())
    .bind(run_input_schema().canonical_digest.to_string())
    .bind(seed_input_digest.to_string())
    .bind(seed_input)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET input_value_id = $3 WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .bind(id(ResourceKind::RunValue, 0x54).to_string())
    .execute(pool)
    .await
    .unwrap();

    let node_payload = TypedPayload::new(1, &json!({"fixture": "model_loop"})).unwrap();
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
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .bind(deadline)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    for (node_id, logical_key, ordinal) in [
        (&primary_node_id, "model-primary", 1_i32),
        (&cancel_node_id, "model-cancel", 2_i32),
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
                $5, $6, $5, 'model_loop', 'running',
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
        .bind(deadline)
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
    }

    Fixture {
        tenant_id,
        principal_id,
        provider_resource_id: provider_resource,
        provider_revision,
        provider_closure,
        profile_resource_id: profile_resource,
        profile_revision,
        model_closure,
        run_id,
        scope_id,
        primary_node_id,
        cancel_node_id,
        capability_deployment,
        alternate_capability_deployment,
        model_deployment,
        selection_policy_binding,
        invocation_policy: agent_policy,
        tool_slot_binding,
        capability_interface_revision: interface_revision,
        argument_schema,
        capability_output_schema,
        output_schema,
        parameter_schema_digest,
        truncation_policy,
        deadline,
        runtime_plan,
    }
}

fn command_for_node(fixture: &Fixture, node_id: &ResourceId, base: u16) -> CreateModelTurn {
    let model_turn_id = id(ResourceKind::ModelTurn, base);
    let request = CanonicalModelRequest {
        schema_version: 1,
        model_turn_id: model_turn_id.clone(),
        messages: vec![CanonicalMessage {
            role: CanonicalMessageRole::Platform,
            parts: vec![CanonicalMessagePart::Text("Answer safely.".to_owned())],
            classification: DataClassification::Internal,
            source: ModelContentSource {
                source_kind: "agent_contract".to_owned(),
                source_id: "agent-fixture".to_owned(),
                source_digest: digest('1'),
                content_digest: digest('1'),
                assembly_phase: insight_platform_models::PromptAssemblyPhase::AgentContract,
                ordinal: 0,
                byte_budget: 1_024,
                token_budget: 256,
                trusted_instruction: true,
            },
        }],
        tools: vec![ModelToolProjection {
            projected_name: "search".to_owned(),
            capability_deployment: fixture.capability_deployment.clone(),
            interface_revision: fixture.capability_interface_revision.clone(),
            input_schema: fixture.argument_schema.clone(),
            output_schema_digest: fixture.capability_output_schema.canonical_digest.clone(),
            effect: Effect::ReadOnly,
        }],
        response_contract: ModelResponseContract {
            output_schema_digest: fixture.output_schema.canonical_digest.clone(),
            structured_schema: Some(fixture.output_schema.clone()),
            allow_tool_intents: true,
            allow_message_with_tool_intents: false,
        },
        generation_parameters: ClosedJsonValue::build(
            fixture.parameter_schema_digest.clone(),
            json!({"temperature": 0}),
        )
        .unwrap(),
        max_output_tokens: 100,
        input_token_estimate: 100,
        estimator_contract_digest: digest('a'),
        source_map_digest: digest('2'),
        truncation_policy: fixture.truncation_policy.clone(),
        classification: DataClassification::Internal,
        deadline: fixture.deadline,
        trace_context: SafeTraceContext {
            trace_id_digest: digest('3'),
            parent_span_id_digest: digest('4'),
        },
    };
    let request_json = serde_json::to_value(&request).unwrap();
    let request_digest: Sha256Digest = canonical_digest(&request_json).unwrap().parse().unwrap();
    CreateModelTurn {
        audit: audit(
            &fixture.tenant_id,
            &fixture.principal_id,
            base + 1,
            '5',
            '6',
        ),
        model_turn_id,
        run_id: fixture.run_id.clone(),
        node_execution_id: node_id.clone(),
        scope_instance_id: fixture.scope_id.clone(),
        expected_run_version: 1,
        expected_node_version: 1,
        round_ordinal: 1,
        slot_id: "primary_model".to_owned(),
        selected_candidate_ordinal: 0,
        selector_input_digest: digest('7'),
        request: ModelRequestValue {
            value_id: id(ResourceKind::RunValue, base + 4),
            classification: DataClassification::Internal,
            schema_digest: digest('8'),
            content_digest: request_digest,
            value: ValueRef::Inline {
                value: request_json,
            },
            request,
        },
        tool_slots: vec![fixture.tool_slot_binding.clone()],
        requested_attempt_limit: 3,
        cost_ceiling_microunits: 10_000,
    }
}

async fn seed_running_model_orchestration(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
    base: u16,
) -> (RepositoryJobFence, DeferOrchestrationToModelTurn) {
    let input_value = json!({"prompt": "fixture"});
    let input_digest: Sha256Digest = canonical_digest(&input_value).unwrap().parse().unwrap();
    let input_port = ExactDataPortRef::RunInput {
        schema_digest: run_input_schema().canonical_digest,
    };
    let environment = ScopeDataEnvironmentSnapshot::build(
        BTreeMap::from([(
            input_port.clone(),
            ExactRunValueRef {
                value_id: id(ResourceKind::RunValue, 0x54),
                schema_digest: run_input_schema().canonical_digest,
                content_digest: input_digest.clone(),
            },
        )]),
        ScopeEnvironmentLimits::from_profile(&checked_in_hard_limit_profile()).unwrap(),
    )
    .unwrap();
    let scope_payload = TypedPayload::new(
        1,
        &json!({"root_run_id": fixture.run_id, "environment": environment}),
    )
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.run_nodes SET payload_schema_version = $3, payload = $4, payload_digest = $5 WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.scope_id.to_string())
    .bind(scope_payload.schema_version)
    .bind(&scope_payload.value)
    .bind(&scope_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET active_work_count = 1 WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let node_id = id(ResourceKind::NodeExecution, base);
    let node_payload = TypedPayload::new(1, &json!({"fixture": "model-owner"})).unwrap();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, logical_key, node_kind, state,
            generation, version, payload_schema_version, payload, payload_digest,
            deadline, started_at, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, 'node_execution', $4,
            'model', 3, ('model-owner-' || $2), 'model_loop', 'running',
            1, 1, $5, $6, $7, $8, $9, $9, $9
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(fixture.scope_id.to_string())
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .bind(fixture.deadline)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();

    let quota_account_id = id(ResourceKind::QuotaAccount, 0x801);
    if base == 0x800 {
        repository
            .create_quota_account(NewQuotaAccount {
                tenant_id: fixture.tenant_id.to_string(),
                quota_account_id: quota_account_id.to_string(),
                scope_kind: "tenant".to_owned(),
                scope_id: fixture.tenant_id.to_string(),
                work_class: WorkClass::Orchestration.as_str().to_owned(),
                metric: "concurrent_jobs".to_owned(),
                limit_value: 4,
                payload: TypedPayload::new(1, &json!({"fixture": "model-owner"})).unwrap(),
            })
            .await
            .unwrap();
    }
    sqlx::query(
        "UPDATE insight_platform.quota_accounts SET reserved_value = 1 WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(quota_account_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let reservation_id = id(ResourceKind::UsageReservation, base + 2);
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id, entry_kind,
            reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, 'reserve', 1, 0, 1, $5)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::QuotaLedgerEntry, base + 3).to_string())
    .bind(quota_account_id.to_string())
    .bind(reservation_id.to_string())
    .bind(named_digest("model-owner-reserve").to_string())
    .execute(pool)
    .await
    .unwrap();
    let source_job_id = id(ResourceKind::Job, base + 4);
    let worker_id = id(ResourceKind::WorkerProcessGeneration, base + 5);
    let lease_token_digest = named_digest("model-owner-lease");
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
    let job_payload = TypedPayload::new(
        1,
        &OrchestrationJobPayload {
            bindings_digest,
            node_execution_id: node_id.clone(),
            root_scope_id: fixture.scope_id.clone(),
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
            tenant_id, job_id, work_class, owner_kind, owner_id, run_id, node_id,
            state, version, attempt_no, attempt_limit, lease_epoch, worker_id,
            lease_token_digest, lease_expires_at, heartbeat_at, scheduled_at, deadline,
            request_digest, quota_reservation_id, payload_schema_version, payload,
            payload_digest, started_at, created_at, updated_at
        ) VALUES (
            $1, $2, 'orchestration', 'node_execution', $3, $4, $3,
            'running', 2, 1, 3, 1, $5, $6, $7, $8, $8, $9,
            $10, $11, $12, $13, $14, $8, $8, $8
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(source_job_id.to_string())
    .bind(node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(worker_id.to_string())
    .bind(lease_token_digest.to_string())
    .bind(now + Duration::minutes(10))
    .bind(now)
    .bind(fixture.deadline)
    .bind(named_digest("model-owner-source").to_string())
    .bind(reservation_id.to_string())
    .bind(job_payload.schema_version)
    .bind(&job_payload.value)
    .bind(&job_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let fence = RepositoryJobFence {
        tenant_id: fixture.tenant_id.to_string(),
        job_id: source_job_id.to_string(),
        worker_id,
        lease_epoch: 1,
        expected_job_version: 2,
        lease_token_digest,
    };
    let create = command_for_node(fixture, &node_id, base + 0x30);
    let selection_document = CandidateSelectionPolicyDocument {
        schema_version: 1,
        mode: CandidateSelectionMode::OrderedFirst,
        route_schema_digest: None,
    };
    let selection_evidence = derive_candidate_selection(
        "primary_model",
        &fixture.selection_policy_binding,
        &selection_document,
        std::slice::from_ref(&fixture.model_deployment),
        None,
    )
    .unwrap();
    let mutations = DeferOrchestrationModelMutationIds {
        source: OrchestrationYieldMutationIds {
            receipt_id: id(ResourceKind::Receipt, base + 0x40),
            quota_entry_ids: (base + 0x41..=base + 0x44)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
            run_event_id: id(ResourceKind::Event, base + 0x45),
            run_outbox_id: id(ResourceKind::OutboxEvent, base + 0x46),
            node_event_id: id(ResourceKind::Event, base + 0x47),
            node_outbox_id: id(ResourceKind::OutboxEvent, base + 0x48),
            job_event_id: id(ResourceKind::Event, base + 0x49),
            job_outbox_id: id(ResourceKind::OutboxEvent, base + 0x4a),
        },
        model_create_receipt_id: id(ResourceKind::Receipt, base + 0x4b),
        model_create_event_id: id(ResourceKind::Event, base + 0x4c),
        model_create_outbox_id: id(ResourceKind::OutboxEvent, base + 0x4d),
        model_prepare_receipt_id: id(ResourceKind::Receipt, base + 0x4e),
        model_prepare_event_id: id(ResourceKind::Event, base + 0x4f),
        model_prepare_outbox_id: id(ResourceKind::OutboxEvent, base + 0x50),
    };
    let command = DeferOrchestrationToModelTurn {
        fence: fence.clone(),
        plan: fixture.runtime_plan.clone(),
        model_turn_id: create.model_turn_id,
        model_job_id: id(ResourceKind::Job, base + 0x51),
        input: ResolvedExpressionInput {
            run_value_id: id(ResourceKind::RunValue, 0x54),
            producing_node_id: None,
            value_kind: "run_input".to_owned(),
            port: input_port,
            classification: DataClassification::Internal,
            schema_digest: run_input_schema().canonical_digest,
            content_digest: input_digest,
            value: ValueRef::Inline { value: input_value },
        },
        request: create.request,
        route: None,
        selection_evidence,
        tool_slots: vec![fixture.tool_slot_binding.clone()],
        requested_attempt_limit: 3,
        cost_ceiling_microunits: 10_000,
        idempotency_key_digest: named_digest("model-owner-idempotency"),
        request_digest: named_digest("model-owner-request"),
        receipt_expires_at: fixture.deadline,
        mutations,
    };
    (fence, command)
}

async fn seed_ready_model_orchestration(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
) -> (ResourceId, ResourceId) {
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'cancelled', version = version + 1,
            terminal_at = clock_timestamp(), updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'node_execution'
          AND terminal_at IS NULL
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let base = 0x800;
    let _ = seed_running_model_orchestration(pool, repository, fixture, base).await;
    let node_id = id(ResourceKind::NodeExecution, base);
    let job_id = id(ResourceKind::Job, base + 4);
    let reservation_id = id(ResourceKind::UsageReservation, base + 2);
    sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'ready', version = 1, attempt_no = 0, lease_epoch = 0,
            worker_id = NULL, lease_token_digest = NULL, lease_expires_at = NULL,
            heartbeat_at = NULL, quota_reservation_id = NULL, started_at = NULL,
            scheduled_at = clock_timestamp() - interval '1 second', updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND job_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.run_nodes SET state = 'ready', enqueue_round = 0, started_at = NULL, updated_at = clock_timestamp() WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(node_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET state = 'running', active_work_count = 0, updated_at = clock_timestamp() WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "DELETE FROM insight_platform.quota_ledger WHERE tenant_id = $1 AND correlation_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(reservation_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.quota_accounts SET reserved_value = 0 WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::QuotaAccount, 0x801).to_string())
    .execute(pool)
    .await
    .unwrap();
    (node_id, job_id)
}

async fn admit_prepare_and_claim(
    repository: &PgRepository,
    command: &CreateModelTurn,
    base: u16,
) -> ClaimEvidence {
    let admitted = match execute_create(repository, command.clone()).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh ModelTurn admission replayed"),
    };
    assert_eq!(admitted.state, ModelTurnState::Ready);
    let job_id = id(ResourceKind::Job, base);
    let prepared = match execute_prepare(
        repository,
        PrepareModelDispatch {
            audit: audit(
                &command.audit.tenant_id,
                &command.audit.principal_id,
                base + 1,
                '9',
                'a',
            ),
            model_turn_id: command.model_turn_id.clone(),
            expected_turn_version: admitted.version,
            job_id: job_id.clone(),
            scheduled_at: Utc::now() - Duration::seconds(1),
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh Model dispatch prepare replayed"),
    };
    assert_eq!(prepared.job.job_id, job_id.to_string());
    claim_one(repository, base + 0x10).await
}

fn tool_response(fixture: &Fixture, query: serde_json::Value) -> CanonicalModelResponse {
    CanonicalModelResponse {
        schema_version: 1,
        message: None,
        structured_output: None,
        tool_intents: vec![ModelToolIntent {
            call_id: "call_1".to_owned(),
            projected_tool_name: "search".to_owned(),
            arguments: ClosedJsonValue::build(
                fixture.argument_schema.canonical_digest.clone(),
                query,
            )
            .unwrap(),
        }],
        finish_reason: CanonicalFinishReason::ToolUse,
        usage: ModelUsage {
            input_tokens: Some(50),
            output_tokens: Some(20),
            cached_input_tokens: None,
            reasoning_tokens: None,
            provider_reported_cost: None,
            accounting_quality: AccountingQuality::ProviderReported,
        },
        observation: ModelObservation {
            request_sent: true,
            provider_response_digest: Some(digest('b')),
            actual_model_identity: Some("fixture-model-2026-08".to_owned()),
            model_fingerprint: Some("fixture-fingerprint".to_owned()),
            possible_duplicate_charge: false,
            stream_delta_count: 0,
            stream_bytes: 0,
        },
    }
}

fn two_tool_response(fixture: &Fixture) -> CanonicalModelResponse {
    let mut response = tool_response(fixture, json!({"query": "fanout-1"}));
    response.tool_intents.push(ModelToolIntent {
        call_id: "call_2".to_owned(),
        projected_tool_name: "search".to_owned(),
        arguments: ClosedJsonValue::build(
            fixture.argument_schema.canonical_digest.clone(),
            json!({"query": "fanout-2"}),
        )
        .unwrap(),
    });
    response
}

fn final_response(fixture: &Fixture) -> CanonicalModelResponse {
    let mut response = tool_response(fixture, json!({"query": "unused"}));
    response.structured_output = Some(
        ClosedJsonValue::build(
            fixture.output_schema.canonical_digest.clone(),
            json!({"answer": "done"}),
        )
        .unwrap(),
    );
    response.tool_intents.clear();
    response.finish_reason = CanonicalFinishReason::Completed;
    response
}

fn resume_mutations(base: u16) -> insight_platform_contracts::ExternalLeafResumeMutationIds {
    insight_platform_contracts::ExternalLeafResumeMutationIds {
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

fn failure_mutations(base: u16) -> insight_platform_contracts::ExternalLeafFailureMutationIds {
    insight_platform_contracts::ExternalLeafFailureMutationIds {
        convergence_job_id: id(ResourceKind::Job, base),
        run_event_id: id(ResourceKind::Event, base + 1),
        run_outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        leaf_node_event_id: id(ResourceKind::Event, base + 3),
        leaf_node_outbox_id: id(ResourceKind::OutboxEvent, base + 4),
        convergence_job_event_id: id(ResourceKind::Event, base + 5),
        convergence_job_outbox_id: id(ResourceKind::OutboxEvent, base + 6),
    }
}

fn allowed_tool_policy(fixture: &Fixture) -> InvocationPolicyDecisionBundle {
    InvocationPolicyDecisionBundle::build(
        vec![InvocationPolicyDecision {
            policy: fixture.invocation_policy.clone(),
            disposition: InvocationPolicyDisposition::Allowed,
            evidence_digest: named_digest("model-tool-policy-allowed"),
        }],
        None,
    )
    .unwrap()
}

fn output(
    fixture: &Fixture,
    response: CanonicalModelResponse,
    value_id: ResourceId,
) -> ModelOutputValue {
    let value = serde_json::to_value(&response).unwrap();
    let content_digest: Sha256Digest = canonical_digest(&value).unwrap().parse().unwrap();
    let structured_output_value_id = response
        .structured_output
        .as_ref()
        .map(|_| ResourceId::from_uuid_v7(ResourceKind::RunValue, uuid::Uuid::now_v7()).unwrap());
    ModelOutputValue {
        value_id,
        structured_output_value_id,
        classification: DataClassification::Internal,
        schema_digest: fixture.output_schema.canonical_digest.clone(),
        content_digest,
        value: ValueRef::Inline { value },
        response,
        validation_evidence_digest: digest('c'),
    }
}

fn measurement(
    input_tokens: u64,
    output_tokens: u64,
    _cost_microunits: i64,
) -> ModelAttemptMeasurement {
    ModelAttemptMeasurement {
        usage: Some(ModelUsage {
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
            cached_input_tokens: None,
            reasoning_tokens: None,
            provider_reported_cost: None,
            accounting_quality: AccountingQuality::ProviderReported,
        }),
        observation: ModelObservation {
            request_sent: true,
            provider_response_digest: Some(digest('d')),
            actual_model_identity: Some("fixture-model-2026-08".to_owned()),
            model_fingerprint: None,
            possible_duplicate_charge: true,
            stream_delta_count: 0,
            stream_bytes: 0,
        },
    }
}

async fn assert_model_deployment_closures_fail_closed(
    repository: &PgRepository,
    fixture: &Fixture,
) {
    let mut wrong_protocol = fixture.provider_closure.clone();
    std::mem::swap(
        &mut wrong_protocol.protocol_policy,
        &mut wrong_protocol.network_policy,
    );
    let mut transaction = repository.begin_registry_transaction().await.unwrap();
    let failure = transaction
        .create_deployment(CreateDeployment {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x700, '1', '2'),
            deployment_id: id(ResourceKind::ModelProviderDeployment, 0x703),
            resource_id: fixture.provider_resource_id.clone(),
            resource_version_id: fixture.provider_revision.revision_id.clone(),
            environment: "fixture-negative".to_owned(),
            closure: DeploymentClosure::ModelProvider(wrong_protocol),
            expected_resource_version: 1,
        })
        .await
        .unwrap_err();
    transaction.rollback().await.unwrap();
    assert!(matches!(
        failure,
        RepositoryError::Conflict("Model Provider Deployment protocol closure")
    ));

    let mut wrong_secret_generation = fixture.provider_closure.clone();
    wrong_secret_generation.secret_bindings[0].binding_generation = 2;
    let mut transaction = repository.begin_registry_transaction().await.unwrap();
    let failure = transaction
        .create_deployment(CreateDeployment {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x720, '5', '6'),
            deployment_id: id(ResourceKind::ModelProviderDeployment, 0x723),
            resource_id: fixture.provider_resource_id.clone(),
            resource_version_id: fixture.provider_revision.revision_id.clone(),
            environment: "fixture-negative".to_owned(),
            closure: DeploymentClosure::ModelProvider(wrong_secret_generation),
            expected_resource_version: 1,
        })
        .await
        .unwrap_err();
    transaction.rollback().await.unwrap();
    assert!(matches!(
        failure,
        RepositoryError::Conflict("exact SecretBinding reference")
    ));

    let mut wrong_parameter_schema = fixture.model_closure.clone();
    wrong_parameter_schema.generation_defaults =
        ClosedJsonValue::build(digest('0'), json!({"temperature": 0})).unwrap();
    let mut transaction = repository.begin_registry_transaction().await.unwrap();
    let failure = transaction
        .create_deployment(CreateDeployment {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x710, '3', '4'),
            deployment_id: id(ResourceKind::ModelDeployment, 0x713),
            resource_id: fixture.profile_resource_id.clone(),
            resource_version_id: fixture.profile_revision.revision_id.clone(),
            environment: "fixture-negative".to_owned(),
            closure: DeploymentClosure::ModelProfile(wrong_parameter_schema),
            expected_resource_version: 1,
        })
        .await
        .unwrap_err();
    transaction.rollback().await.unwrap();
    assert!(matches!(
        failure,
        RepositoryError::Conflict("Model Profile Deployment binding closure")
    ));
}

async fn run_model_worker_process_recovery(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
) {
    let (Ok(binary), Ok(nats_url), Ok(nats_ca), Ok(nats_cert), Ok(nats_key)) = (
        std::env::var("PLATFORM_MODEL_WORKER_BIN"),
        std::env::var("PLATFORM_TEST_NATS_URL"),
        std::env::var("PLATFORM_TEST_NATS_CA_PATH"),
        std::env::var("PLATFORM_TEST_NATS_CERT_PATH"),
        std::env::var("PLATFORM_TEST_NATS_KEY_PATH"),
    ) else {
        eprintln!("Model Worker binary or TLS NATS fixture is unset; process recovery skipped");
        return;
    };
    let tls = model_process_tls();
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = incoming.local_addr().unwrap();
    let wire = Arc::new(ProcessModelWire {
        calls: AtomicUsize::new(0),
    });
    let service = EgressBrokerServiceServer::new(EgressBrokerGrpcService::new(
        wire.clone(),
        Arc::new(EmptyProcessHttp),
        Arc::new(EmptyProcessGrpc),
        EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap(),
    ));
    let service =
        tonic::service::interceptor::InterceptedService::new(service, EgressCallerWorkloadIdentity);
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(&tls.server_cert, &tls.server_key))
                    .client_ca_root(Certificate::from_pem(tls.ca.clone())),
            )
            .unwrap()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );

    let node_id = id(ResourceKind::NodeExecution, 0xc00);
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let node_payload = TypedPayload::new(1, &json!({"fixture": "model-process-recovery"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, logical_key, node_kind, state,
            generation, version, payload_schema_version, payload, payload_digest,
            deadline, started_at, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, 'node_execution', $4,
                  'model-process-recovery', 12, 'model-process-recovery', 'model_loop', 'running',
                  1, 1, $5, $6, $7, $8, $9, $9, $9)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(fixture.scope_id.to_string())
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .bind(fixture.deadline)
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    let command = command_for_node(fixture, &node_id, 0xc10);
    let admitted = match execute_create(repository, command.clone()).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh Model process admission replayed"),
    };
    let job_id = id(ResourceKind::Job, 0xc20);
    match execute_prepare(
        repository,
        PrepareModelDispatch {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, 0xc21, '1', '2'),
            model_turn_id: command.model_turn_id.clone(),
            expected_turn_version: admitted.version,
            job_id: job_id.clone(),
            // Keep the work ineligible until the production process has passed its
            // strict startup schema verification. The commit-window trigger below
            // is test-only schema state and must be installed after that check.
            scheduled_at: Utc::now() + Duration::seconds(5),
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(_) => {}
        CommandOutcome::Replayed(_) => panic!("fresh Model process prepare replayed"),
    }

    let prefix = format!("/tmp/platform-model-worker-process-{}", std::process::id());
    let ca_path = PathBuf::from(format!("{prefix}-egress-ca.pem"));
    let cert_path = PathBuf::from(format!("{prefix}-egress-client.pem"));
    let key_path = PathBuf::from(format!("{prefix}-egress-client-key.pem"));
    std::fs::write(&ca_path, &tls.ca).unwrap();
    std::fs::write(&cert_path, &tls.client_cert).unwrap();
    std::fs::write(&key_path, &tls.client_key).unwrap();
    let manifest = production_model_worker_manifest();
    let manifest_digest = manifest.canonical_digest().unwrap();
    let config = json!({
        "schema_version": 1,
        "worker_manifest": manifest,
        "installed_adapters": [
            {
                "qualified_name": OPENAI_RESPONSES_ADAPTER_NAME,
                "worker_manifest_digest": manifest_digest,
                "adapter_contract_digest": digest('3')
            },
            {
                "qualified_name": ANTHROPIC_MESSAGES_ADAPTER_NAME,
                "worker_manifest_digest": manifest_digest,
                "adapter_contract_digest": named_digest("anthropic-production-adapter")
            }
        ],
        "database_max_connections": 4,
        "database_acquire_timeout_milliseconds": 1000,
        "egress_endpoint": format!("https://{address}/"),
        "egress_tls_server_name": "egress.test",
        "egress_connect_timeout_milliseconds": 1000,
        "egress_request_timeout_milliseconds": 30000,
        "maximum_rpc_metadata_bytes": 65536,
        "maximum_rpc_payload_bytes": 1048576,
        "live_delta": {
            "servers": [nats_url],
            "namespace": "model_process_recovery",
            "connect_timeout_milliseconds": 1000,
            "publish_timeout_milliseconds": 1000,
            "reconnect_backoff_milliseconds": 100,
            "drain_timeout_milliseconds": 1000,
            "maximum_pending_messages": 16,
            "maximum_pending_bytes": 1048576
        },
        "receipt_ttl_seconds": 60,
        "claim_scan_milliseconds": 20,
        "claim_failure_backoff_milliseconds": 5,
        "drain_grace_milliseconds": 1000
    });
    let write_config = |suffix: &str, value: &serde_json::Value| {
        let path = PathBuf::from(format!("{prefix}-{suffix}.json"));
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        (path, canonical_digest(value).unwrap())
    };
    let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL").unwrap();
    let spawn_worker = |path: &PathBuf, digest: &str| {
        std::process::Command::new(&binary)
            .env("PLATFORM_MODEL_WORKER_CONFIG", path)
            .env("PLATFORM_MODEL_WORKER_CONFIG_DIGEST", digest)
            .env("PLATFORM_MODEL_WORKER_DATABASE_URL", &database_url)
            .env("PLATFORM_MODEL_WORKER_EGRESS_CA_PATH", &ca_path)
            .env("PLATFORM_MODEL_WORKER_EGRESS_CERT_PATH", &cert_path)
            .env("PLATFORM_MODEL_WORKER_EGRESS_KEY_PATH", &key_path)
            .env("PLATFORM_MODEL_WORKER_NATS_CA_PATH", &nats_ca)
            .env("PLATFORM_MODEL_WORKER_NATS_CERT_PATH", &nats_cert)
            .env("PLATFORM_MODEL_WORKER_NATS_KEY_PATH", &nats_key)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut wrong_config = config.clone();
    let wrong_manifest = WorkerManifest {
        adapter_runtime_digest: digest('f'),
        ..production_model_worker_manifest()
    };
    let wrong_manifest_digest = wrong_manifest.canonical_digest().unwrap();
    wrong_config["worker_manifest"] = serde_json::to_value(wrong_manifest).unwrap();
    for adapter in wrong_config["installed_adapters"].as_array_mut().unwrap() {
        adapter["worker_manifest_digest"] =
            serde_json::to_value(wrong_manifest_digest.clone()).unwrap();
    }
    let (wrong_path, wrong_digest) = write_config("wrong", &wrong_config);
    let mut wrong = spawn_worker(&wrong_path, &wrong_digest);
    let wrong_startup = read_model_process_line(wrong.stderr.take().unwrap()).await;
    assert!(
        wrong_startup.contains("started generation="),
        "{wrong_startup}"
    );
    tokio::time::sleep(StdDuration::from_millis(500)).await;
    assert_eq!(wire.calls.load(Ordering::SeqCst), 0);
    assert_eq!(model_job_state(pool, &job_id).await, "ready");
    wrong.kill().unwrap();
    wrong.wait().unwrap();

    let (config_path, config_digest) = write_config("correct", &config);
    let mut first = spawn_worker(&config_path, &config_digest);
    let startup = read_model_process_line(first.stderr.take().unwrap()).await;
    assert!(startup.contains("started generation="), "{startup}");

    sqlx::query(
        r#"
        CREATE FUNCTION insight_platform.test_pause_model_commit()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.state = 'running' AND NEW.state = 'succeeded' AND NEW.work_class = 'model' THEN
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
    sqlx::query("CREATE TRIGGER test_pause_model_commit BEFORE UPDATE ON insight_platform.jobs FOR EACH ROW EXECUTE FUNCTION insight_platform.test_pause_model_commit()")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE insight_platform.jobs SET scheduled_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'ready'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    wait_model_calls(&wire.calls, 1).await;
    wait_model_job_state(pool, &job_id, "running").await;
    first.kill().unwrap();
    first.wait().unwrap();
    sqlx::query("DROP TRIGGER test_pause_model_commit ON insight_platform.jobs")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION insight_platform.test_pause_model_commit()")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'")
        .bind(fixture.tenant_id.to_string())
        .bind(job_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    let mut second = spawn_worker(&config_path, &config_digest);
    wait_model_turn_state(pool, &command.model_turn_id, "succeeded").await;
    assert_eq!(wire.calls.load(Ordering::SeqCst), 2);
    let recovery_facts: (i64, i64, bool, bool) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.events
             WHERE tenant_id = $1 AND aggregate_id = $2
               AND event_type = 'model.lease_recovered'),
          jsonb_array_length(invocation.payload->'attempts')::bigint,
          job.worker_id IS NULL AND job.lease_token_digest IS NULL
            AND job.quota_reservation_id IS NULL,
          invocation.payload->'attempts'->0->>'outcome' = 'retry_scheduled'
            AND (invocation.payload->'attempts'->0->>'request_sent')::boolean
        FROM insight_platform.invocations AS invocation
        JOIN insight_platform.jobs AS job
          ON job.tenant_id = invocation.tenant_id AND job.owner_id = invocation.invocation_id
        WHERE invocation.tenant_id = $1 AND invocation.invocation_id = $2
          AND job.job_id = $3
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(command.model_turn_id.to_string())
    .bind(job_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(recovery_facts, (1, 2, true, true));
    second.kill().unwrap();
    second.wait().unwrap();
    let _ = shutdown_sender.send(());
    server.await.unwrap().unwrap();
    for path in [wrong_path, config_path, ca_path, cert_path, key_path] {
        std::fs::remove_file(path).unwrap();
    }
}

async fn read_model_process_line(stderr: std::process::ChildStderr) -> String {
    tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        std::io::BufRead::read_line(&mut std::io::BufReader::new(stderr), &mut line).unwrap();
        line
    })
    .await
    .unwrap()
}

async fn model_job_state(pool: &PgPool, job_id: &ResourceId) -> String {
    sqlx::query_scalar("SELECT state FROM insight_platform.jobs WHERE job_id = $1")
        .bind(job_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap()
}

#[test]
fn production_workers_complete_model_tool_result_return_chain() {
    let (
        Ok(database_url),
        Ok(orchestration_binary),
        Ok(model_binary),
        Ok(capability_binary),
        Ok(nats_url),
        Ok(nats_ca_path),
        Ok(nats_cert_path),
        Ok(nats_key_path),
    ) = (
        std::env::var("PLATFORM_MODEL_TOOL_CHAIN_TEST_DATABASE_URL"),
        std::env::var("PLATFORM_ORCHESTRATION_WORKER_BIN"),
        std::env::var("PLATFORM_MODEL_WORKER_BIN"),
        std::env::var("PLATFORM_CAPABILITY_NATIVE_WORKER_BIN"),
        std::env::var("PLATFORM_TEST_NATS_URL"),
        std::env::var("PLATFORM_TEST_NATS_CA_PATH"),
        std::env::var("PLATFORM_TEST_NATS_CERT_PATH"),
        std::env::var("PLATFORM_TEST_NATS_KEY_PATH"),
    )
    else {
        eprintln!("Model tool-chain database, Worker binaries, or TLS NATS fixture is unset; process chain skipped");
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(32 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(&database_url)
            .await
            .unwrap();
        verify_schema(&pool).await.unwrap();
        let repository = PgRepository::new(pool.clone());
        let fixture = seed_fixture(&pool, &repository).await;
        let (node_id, source_job_id) =
            seed_ready_model_orchestration(&pool, &repository, &fixture).await;

        let tls = model_process_tls();
        let wire = Arc::new(ToolChainModelWire {
            calls: AtomicUsize::new(0),
        });
        let egress_incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let egress_address = egress_incoming.local_addr().unwrap();
        let egress_service = EgressBrokerServiceServer::new(EgressBrokerGrpcService::new(
            wire.clone(),
            Arc::new(EmptyProcessHttp),
            Arc::new(EmptyProcessGrpc),
            EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap(),
        ));
        let egress_service = tonic::service::interceptor::InterceptedService::new(
            egress_service,
            EgressCallerWorkloadIdentity,
        );
        let (egress_shutdown_sender, egress_shutdown_receiver) = tokio::sync::oneshot::channel();
        let egress_server = tokio::spawn(
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(&tls.server_cert, &tls.server_key))
                        .client_ca_root(Certificate::from_pem(tls.ca.clone())),
                )
                .unwrap()
                .add_service(egress_service)
                .serve_with_incoming_shutdown(egress_incoming, async move {
                    let _ = egress_shutdown_receiver.await;
                }),
        );
        let broker = Arc::new(ModelTypedPlanBroker {
            bytes: canonical_json(&serde_json::to_value(&fixture.runtime_plan).unwrap()).unwrap(),
            reads: AtomicUsize::new(0),
        });
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let service = ArtifactSchedulerServiceServer::new(ArtifactSchedulerGrpcService::new(
            broker.clone(),
            ArtifactInternalRpcLimits::new(262_144, 262_144).unwrap(),
        ));
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            SchedulerWorkloadIdentity,
        );
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(
                            &tls.artifact_server_cert,
                            &tls.artifact_server_key,
                        ))
                        .client_ca_root(Certificate::from_pem(tls.ca.clone())),
                )
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_receiver.await;
                }),
        );
        let prefix = PathBuf::from(format!(
            "/tmp/platform-model-tool-orchestration-{}",
            std::process::id()
        ));
        let ca_path = PathBuf::from(format!("{}-ca.pem", prefix.display()));
        let cert_path = PathBuf::from(format!("{}-scheduler.pem", prefix.display()));
        let key_path = PathBuf::from(format!("{}-scheduler-key.pem", prefix.display()));
        let model_cert_path = PathBuf::from(format!("{}-model.pem", prefix.display()));
        let model_key_path = PathBuf::from(format!("{}-model-key.pem", prefix.display()));
        std::fs::write(&ca_path, &tls.ca).unwrap();
        std::fs::write(&cert_path, &tls.scheduler_cert).unwrap();
        std::fs::write(&key_path, &tls.scheduler_key).unwrap();
        std::fs::write(&model_cert_path, &tls.client_cert).unwrap();
        std::fs::write(&model_key_path, &tls.client_key).unwrap();
        let config = model_orchestration_process_config(format!("https://{address}/"));
        let (config_path, config_digest) =
            write_model_process_config(&prefix, "orchestration", &config);
        let mut worker = spawn_model_orchestration_worker(
            &orchestration_binary,
            &config_path,
            &config_digest,
            &database_url,
            &ca_path,
            &cert_path,
            &key_path,
        );
        let worker_log = observe_process_start(&mut worker);
        let started = std::time::Instant::now();
        loop {
            let state: Option<(String, String, String, i32, i64)> = sqlx::query_as(
                r#"
                SELECT source.state, node.state, run.state, run.active_work_count,
                       (SELECT count(*) FROM insight_platform.jobs
                        WHERE tenant_id = $1 AND run_id = $2
                          AND work_class = 'model' AND state = 'ready')
                FROM insight_platform.jobs AS source
                JOIN insight_platform.run_nodes AS node
                  ON node.tenant_id = source.tenant_id AND node.node_id = source.node_id
                JOIN insight_platform.runs AS run
                  ON run.tenant_id = source.tenant_id AND run.run_id = source.run_id
                WHERE source.tenant_id = $1 AND source.job_id = $3
                "#,
            )
            .bind(fixture.tenant_id.to_string())
            .bind(fixture.run_id.to_string())
            .bind(source_job_id.to_string())
            .fetch_optional(&pool)
            .await
            .unwrap();
            if state
                == Some((
                    "succeeded".to_owned(),
                    "waiting".to_owned(),
                    "waiting".to_owned(),
                    0,
                    1,
                ))
            {
                break;
            }
            assert!(
                started.elapsed() < StdDuration::from_secs(10),
                "orchestration dispatch stalled with state={state:?}, artifact_reads={}",
                broker.reads.load(Ordering::SeqCst)
            );
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        assert!(broker.reads.load(Ordering::SeqCst) >= 1);
        let model_owner: String = sqlx::query_scalar(
            "SELECT node_id FROM insight_platform.jobs WHERE tenant_id = $1 AND run_id = $2 AND work_class = 'model'",
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.run_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(model_owner, node_id.to_string());

        let model_config = model_tool_chain_worker_config(
            format!("https://{egress_address}/"),
            nats_url,
        );
        let (model_config_path, model_config_digest) =
            write_model_process_config(&prefix, "model", &model_config);
        let mut model_worker = spawn_model_tool_chain_worker(
            &model_binary,
            &model_config_path,
            &model_config_digest,
            &database_url,
            &ca_path,
            &model_cert_path,
            &model_key_path,
            Path::new(&nats_ca_path),
            Path::new(&nats_cert_path),
            Path::new(&nats_key_path),
        );
        let model_log = observe_process_start(&mut model_worker);
        let started = std::time::Instant::now();
        loop {
            let state: (i64, i64) = sqlx::query_as(
                r#"
                SELECT
                  (SELECT count(*) FROM insight_platform.jobs
                   WHERE tenant_id = $1 AND run_id = $2
                     AND work_class = 'model' AND state = 'succeeded'),
                  (SELECT count(*) FROM insight_platform.jobs
                   WHERE tenant_id = $1 AND run_id = $2
                     AND work_class LIKE 'capability_%' AND state = 'ready')
                "#,
            )
            .bind(fixture.tenant_id.to_string())
            .bind(fixture.run_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
            if state == (1, 1) {
                break;
            }
            if let Some(status) = model_worker.try_wait().unwrap() {
                panic!("Model Worker exited before tool fan-out: {status}");
            }
            assert!(
                started.elapsed() < StdDuration::from_secs(10),
                "Model tool fan-out stalled with state={state:?}, provider_calls={}",
                wire.calls.load(Ordering::SeqCst)
            );
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        assert_eq!(wire.calls.load(Ordering::SeqCst), 1);

        let capability_config = model_tool_chain_capability_worker_config();
        let (capability_config_path, capability_config_digest) =
            write_model_process_config(&prefix, "capability", &capability_config);
        let mut capability_worker = spawn_model_tool_chain_capability_worker(
            &capability_binary,
            &capability_config_path,
            &capability_config_digest,
            &database_url,
        );
        let capability_log = observe_process_start(&mut capability_worker);
        let started = std::time::Instant::now();
        loop {
            let state: (String, i32) = sqlx::query_as(
                "SELECT state, active_work_count FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
            )
            .bind(fixture.tenant_id.to_string())
            .bind(fixture.run_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
            if state == ("succeeded".to_owned(), 0) {
                break;
            }
            assert!(
                started.elapsed() < StdDuration::from_secs(10),
                "Model tool-result continuation stalled with run={state:?}, provider_calls={}",
                wire.calls.load(Ordering::SeqCst)
            );
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        assert_eq!(wire.calls.load(Ordering::SeqCst), 2);
        let chain_facts: (i64, i64, i64, i64, i64, i64, i64, i64, bool) = sqlx::query_as(
            r#"
            SELECT
              (SELECT count(*) FROM insight_platform.jobs
               WHERE tenant_id = $1 AND run_id = $2
                 AND work_class = 'model' AND state = 'succeeded'),
              (SELECT count(*) FROM insight_platform.jobs
               WHERE tenant_id = $1 AND run_id = $2
                 AND work_class = 'capability_native' AND state = 'succeeded'),
              (SELECT count(*) FROM insight_platform.invocations
               WHERE tenant_id = $1 AND run_id = $2
                 AND invocation_kind = 'model' AND state = 'succeeded'),
              (SELECT count(*) FROM insight_platform.invocations
               WHERE tenant_id = $1 AND run_id = $2
                 AND invocation_kind = 'capability' AND state = 'succeeded'),
              (SELECT count(*) FROM insight_platform.jobs
               WHERE tenant_id = $1 AND run_id = $2
                 AND state NOT IN ('succeeded', 'failed', 'cancelled')),
              (SELECT count(*) FROM insight_platform.run_nodes
               WHERE tenant_id = $1 AND run_id = $2
                 AND record_kind = 'node_execution'
                 AND node_kind = 'return' AND state = 'succeeded'),
              (SELECT count(*) FROM insight_platform.run_values
               WHERE tenant_id = $1 AND run_id = $2
                 AND value_kind = 'model_response'),
              (SELECT count(*) FROM insight_platform.run_values
               WHERE tenant_id = $1 AND run_id = $2
                 AND value_kind = 'model_structured_output'),
              EXISTS (
                SELECT 1
                FROM insight_platform.runs AS run
                JOIN insight_platform.run_values AS value
                  ON value.tenant_id = run.tenant_id
                 AND value.run_id = run.run_id
                 AND value.value_id = run.output_value_id
                WHERE run.tenant_id = $1 AND run.run_id = $2
                  AND value.value_kind = 'model_structured_output'
                  AND value.schema_digest = $3
                  AND value.inline_value = '{"answer":"complete"}'::jsonb
              )
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.run_id.to_string())
        .bind(fixture.output_schema.canonical_digest.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(chain_facts, (2, 1, 2, 1, 0, 1, 2, 1, true));

        capability_worker.kill().unwrap();
        capability_worker.wait().unwrap();
        capability_log.join().unwrap();
        model_worker.kill().unwrap();
        model_worker.wait().unwrap();
        model_log.join().unwrap();

        worker.kill().unwrap();
        worker.wait().unwrap();
        worker_log.join().unwrap();
        let _ = shutdown_sender.send(());
        server.await.unwrap().unwrap();
        let _ = egress_shutdown_sender.send(());
        egress_server.await.unwrap().unwrap();
        for path in [
            config_path,
            model_config_path,
            capability_config_path,
            ca_path,
            cert_path,
            key_path,
            model_cert_path,
            model_key_path,
        ] {
            std::fs::remove_file(path).unwrap();
        }
    });
}

async fn wait_model_job_state(pool: &PgPool, job_id: &ResourceId, expected: &str) {
    let started = std::time::Instant::now();
    while model_job_state(pool, job_id).await != expected {
        assert!(started.elapsed() < StdDuration::from_secs(10));
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

async fn wait_model_turn_state(pool: &PgPool, turn_id: &ResourceId, expected: &str) {
    let started = std::time::Instant::now();
    loop {
        let state: String = sqlx::query_scalar(
            "SELECT state FROM insight_platform.invocations WHERE invocation_id = $1",
        )
        .bind(turn_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        if state == expected {
            return;
        }
        assert!(started.elapsed() < StdDuration::from_secs(10));
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

async fn wait_model_calls(calls: &AtomicUsize, expected: usize) {
    let started = std::time::Instant::now();
    while calls.load(Ordering::SeqCst) != expected {
        assert!(started.elapsed() < StdDuration::from_secs(10));
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

#[test]
fn model_turn_is_exact_atomic_quota_accounted_and_first_winner() {
    std::thread::Builder::new()
        .name("phase3-model-turn-fixture".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(model_turn_fixture());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn model_turn_fixture() {
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
    let policy_facts = repository
        .load_exact_model_policy_facts(&fixture.tenant_id, &fixture.model_deployment)
        .await
        .unwrap();
    assert_eq!(policy_facts.deployment, fixture.model_closure);
    assert_eq!(policy_facts.budget.maximum_attempts_per_turn, 3);
    assert!(policy_facts.public_projection.reject_prompt_overflow);
    assert_eq!(policy_facts.safety.contract_id, "platform.model_safety.v1");
    let assembly_facts = repository
        .load_exact_controller_model_assembly_facts(
            &fixture.tenant_id,
            &fixture.run_id,
            "primary_model",
            &fixture.model_deployment,
            std::slice::from_ref(&fixture.tool_slot_binding),
        )
        .await
        .unwrap();
    assert!(assembly_facts.skills.is_empty());
    assert_eq!(assembly_facts.tools.len(), 1);
    assert_eq!(
        assembly_facts.tools[0].capability_deployment,
        fixture.capability_deployment
    );
    assert_eq!(
        assembly_facts.agent.output_schema.canonical_digest,
        fixture.output_schema.canonical_digest
    );
    let mut wrong_deployment = fixture.model_deployment.clone();
    wrong_deployment.deployment_digest = digest('0');
    assert!(matches!(
        repository
            .load_exact_model_policy_facts(&fixture.tenant_id, &wrong_deployment)
            .await,
        Err(RepositoryError::Conflict("exact Model Deployment"))
    ));
    assert_model_deployment_closures_fail_closed(&repository, &fixture).await;
    let mut nonselected_tool = command_for_node(&fixture, &fixture.primary_node_id, 0x0d0);
    nonselected_tool.request.request.tools[0].capability_deployment =
        fixture.alternate_capability_deployment.clone();
    let nonselected_request = serde_json::to_value(&nonselected_tool.request.request).unwrap();
    nonselected_tool.request.content_digest = canonical_digest(&nonselected_request)
        .unwrap()
        .parse()
        .unwrap();
    nonselected_tool.request.value = ValueRef::Inline {
        value: nonselected_request,
    };
    assert!(matches!(
        execute_create(&repository, nonselected_tool).await,
        Err(RepositoryError::Conflict(
            "exact Model tool candidate selection"
        ))
    ));

    let primary = command_for_node(&fixture, &fixture.primary_node_id, 0x100);
    let first_claim = admit_prepare_and_claim(&repository, &primary, 0x120).await;
    assert_eq!(
        first_claim.claimed.turn.model_turn_id,
        primary.model_turn_id
    );
    assert_eq!(first_claim.claimed.turn.state, ModelTurnState::InFlight);
    assert_eq!(first_claim.claimed.job.attempt_no, 1);
    assert_eq!(first_claim.claimed.quota_account_ids.len(), 4);
    assert!(matches!(
        &first_claim.claimed.request_input.material,
        ModelExecutionInputMaterial::Inline { value }
            if value == &serde_json::to_value(&primary.request.request).unwrap()
    ));
    let adapter_job = first_claim
        .claimed
        .adapter_job(
            primary.request.request.clone(),
            first_claim
                .claimed
                .turn
                .payload
                .admission
                .provider
                .installed_adapter
                .worker_manifest_digest
                .clone(),
            worker_audit(&fixture.tenant_id, &first_claim.worker_id, 0x140, '9', '8'),
            (0..4)
                .map(|offset| id(ResourceKind::QuotaLedgerEntry, 0x143 + offset))
                .collect(),
            ModelTurnLimits::from_profile(
                &insight_platform_contracts::checked_in_hard_limit_profile(),
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        adapter_job.execution.request_digest,
        first_claim.claimed.turn.payload.admission.request_digest
    );
    assert!(matches!(
        execute_create(&repository, primary.clone()).await.unwrap(),
        CommandOutcome::Replayed(record) if record == first_claim.claimed.turn
    ));

    let reserved_after_claim: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT metric, reserved_value, used_value
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND work_class = 'model'
        ORDER BY metric
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(reserved_after_claim.len(), 4);
    assert!(reserved_after_claim
        .iter()
        .all(|(_, reserved, used)| *reserved > 0 && *used == 0));

    let invalid_receipt = id(ResourceKind::Receipt, 0x150);
    let invalid_event = id(ResourceKind::Event, 0x151);
    let invalid_outbox = id(ResourceKind::OutboxEvent, 0x152);
    let invalid_response = tool_response(&fixture, json!({"query": 42}));
    let invalid = CommitModelOutcome {
        audit: worker_audit(&fixture.tenant_id, &first_claim.worker_id, 0x150, '1', '2'),
        model_turn_id: primary.model_turn_id.clone(),
        job_id: id(ResourceKind::Job, 0x120),
        expected_turn_version: first_claim.claimed.turn.version,
        fence: first_claim.fence(),
        usage_reservation_id: first_claim.usage_reservation_id.clone(),
        quota_entry_ids: (0..4)
            .map(|offset| id(ResourceKind::QuotaLedgerEntry, 0x153 + offset))
            .collect(),
        request: primary.request.request.clone(),
        outcome: ModelDispatchOutcome::Succeeded(Box::new(output(
            &fixture,
            invalid_response,
            id(ResourceKind::RunValue, 0x157),
        ))),
        resume_mutations: None,
        failure_mutations: None,
        tool_continuation_mutations: None,
    };
    assert!(matches!(
        execute_outcome(&repository, invalid).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let invalid_durable_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2)
        + (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_id = $3)
        + (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1 AND outbox_id = $4)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(invalid_receipt.to_string())
    .bind(invalid_event.to_string())
    .bind(invalid_outbox.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(invalid_durable_rows, 0);
    let quota_after_invalid: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT metric, reserved_value, used_value
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND work_class = 'model'
        ORDER BY metric
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(quota_after_invalid, reserved_after_claim);

    let retry_at = Utc::now() + Duration::milliseconds(100);
    let retry = CommitModelOutcome {
        audit: worker_audit(&fixture.tenant_id, &first_claim.worker_id, 0x160, '3', '4'),
        model_turn_id: primary.model_turn_id.clone(),
        job_id: id(ResourceKind::Job, 0x120),
        expected_turn_version: first_claim.claimed.turn.version,
        fence: first_claim.fence(),
        usage_reservation_id: first_claim.usage_reservation_id.clone(),
        quota_entry_ids: (0..4)
            .map(|offset| id(ResourceKind::QuotaLedgerEntry, 0x163 + offset))
            .collect(),
        request: primary.request.request.clone(),
        outcome: ModelDispatchOutcome::RetryableFailure {
            failure: model_failure(
                insight_platform_contracts::FailureClass::External,
                insight_platform_contracts::Retryability::SafeWithinPolicy,
            ),
            retry_at,
            measurement: measurement(10, 0, 10),
        },
        resume_mutations: None,
        failure_mutations: None,
        tool_continuation_mutations: None,
    };
    let retried = match execute_outcome(&repository, retry).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh retry replayed"),
    };
    assert_eq!(retried.turn.state, ModelTurnState::RetryScheduled);
    assert_eq!(retried.job.state, "retry_scheduled");
    assert!(retried.job.quota_reservation_id.is_none());
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let second_claim = claim_one(&repository, 0x170).await;
    assert_eq!(
        second_claim.claimed.turn.model_turn_id,
        primary.model_turn_id
    );
    assert_eq!(second_claim.claimed.job.attempt_no, 2);
    assert_ne!(
        second_claim.usage_reservation_id,
        first_claim.usage_reservation_id
    );
    let valid_response = tool_response(&fixture, json!({"query": "rust"}));
    let completed_command = CommitModelOutcome {
        audit: worker_audit(&fixture.tenant_id, &second_claim.worker_id, 0x180, '5', '6'),
        model_turn_id: primary.model_turn_id.clone(),
        job_id: id(ResourceKind::Job, 0x120),
        expected_turn_version: second_claim.claimed.turn.version,
        fence: second_claim.fence(),
        usage_reservation_id: second_claim.usage_reservation_id.clone(),
        quota_entry_ids: (0..4)
            .map(|offset| id(ResourceKind::QuotaLedgerEntry, 0x183 + offset))
            .collect(),
        request: primary.request.request.clone(),
        outcome: ModelDispatchOutcome::Succeeded(Box::new(output(
            &fixture,
            valid_response,
            id(ResourceKind::RunValue, 0x187),
        ))),
        resume_mutations: None,
        failure_mutations: None,
        tool_continuation_mutations: None,
    };
    let completed = match execute_outcome(&repository, completed_command.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh completion replayed"),
    };
    assert_eq!(completed.turn.state, ModelTurnState::Succeeded);
    assert_eq!(completed.job.state, "succeeded");
    assert_eq!(completed.turn.payload.attempts.len(), 2);
    assert_eq!(
        completed
            .turn
            .payload
            .result
            .as_ref()
            .unwrap()
            .tool_intent_count,
        1
    );
    assert!(matches!(
        execute_outcome(&repository, completed_command).await.unwrap(),
        CommandOutcome::Replayed(record) if record == completed
    ));

    let quota_totals: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT metric, reserved_value, used_value
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND work_class = 'model'
        ORDER BY metric
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(quota_totals.iter().all(|(_, reserved, _)| *reserved == 0));
    assert!(quota_totals.iter().any(|(metric, _, used)| {
        metric == QuotaDimension::ModelRequests.as_str() && *used == 2
    }));
    assert!(quota_totals.iter().any(|(metric, _, used)| {
        metric == QuotaDimension::ModelTokens.as_str() && *used == 80
    }));
    assert!(quota_totals.iter().any(|(metric, _, used)| {
        metric == QuotaDimension::ModelCostMicrounits.as_str() && *used == 0
    }));
    assert!(quota_totals.iter().any(|(metric, _, used)| {
        metric == QuotaDimension::WorkClassConcurrentOperations.as_str() && *used == 0
    }));
    let output_rows: (i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT count(*) FROM insight_platform.run_values
             WHERE tenant_id = $1 AND run_id = $2 AND value_kind = 'model_response'),
          (SELECT count(*) FROM insight_platform.events
             WHERE tenant_id = $1 AND aggregate_kind = 'model_turn'
               AND event_type IN ('model.started', 'model.retry_scheduled', 'model.tool_intent')
               AND run_id = $2)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(output_rows, (1, 4));

    // This production-process matrix conservatively charges an unknown first Provider
    // attempt, so run it after the focused quota assertions above and before the
    // orchestration continuation deliberately moves the Run to waiting.
    run_model_worker_process_recovery(&pool, &repository, &fixture).await;

    let stale_receipt = id(ResourceKind::Receipt, 0x190);
    let mut stale_fence = second_claim.fence();
    stale_fence.expected_version = u64::try_from(completed.job.version).unwrap();
    stale_fence.token_digest = digest('f');
    let stale = CommitModelOutcome {
        audit: worker_audit(&fixture.tenant_id, &second_claim.worker_id, 0x190, '7', '8'),
        model_turn_id: primary.model_turn_id.clone(),
        job_id: id(ResourceKind::Job, 0x120),
        expected_turn_version: completed.turn.version,
        fence: stale_fence,
        usage_reservation_id: second_claim.usage_reservation_id,
        quota_entry_ids: (0..4)
            .map(|offset| id(ResourceKind::QuotaLedgerEntry, 0x193 + offset))
            .collect(),
        request: primary.request.request,
        outcome: ModelDispatchOutcome::PermanentFailure {
            failure: model_failure(
                insight_platform_contracts::FailureClass::External,
                insight_platform_contracts::Retryability::Never,
            ),
            measurement: measurement(1, 0, 1),
        },
        resume_mutations: None,
        failure_mutations: None,
        tool_continuation_mutations: None,
    };
    assert!(execute_outcome(&repository, stale).await.is_err());
    let stale_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(stale_receipt.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stale_rows, 0);

    let cancellation = command_for_node(&fixture, &fixture.cancel_node_id, 0x300);
    let cancellation_claim = admit_prepare_and_claim(&repository, &cancellation, 0x320).await;
    let completion_racer = CommitModelOutcome {
        audit: worker_audit(
            &fixture.tenant_id,
            &cancellation_claim.worker_id,
            0x350,
            '9',
            'a',
        ),
        model_turn_id: cancellation.model_turn_id.clone(),
        job_id: id(ResourceKind::Job, 0x320),
        expected_turn_version: cancellation_claim.claimed.turn.version,
        fence: cancellation_claim.fence(),
        usage_reservation_id: cancellation_claim.usage_reservation_id.clone(),
        quota_entry_ids: (0..4)
            .map(|offset| id(ResourceKind::QuotaLedgerEntry, 0x353 + offset))
            .collect(),
        request: cancellation.request.request.clone(),
        outcome: ModelDispatchOutcome::Succeeded(Box::new(output(
            &fixture,
            tool_response(&fixture, json!({"query": "race"})),
            id(ResourceKind::RunValue, 0x357),
        ))),
        resume_mutations: None,
        failure_mutations: None,
        tool_continuation_mutations: None,
    };
    let control_racer = ControlModelTurn {
        audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x360, 'b', 'c'),
        model_turn_id: cancellation.model_turn_id.clone(),
        expected_turn_version: cancellation_claim.claimed.turn.version,
        quota_entry_ids: vec![],
        kind: ModelControlKind::Cancel,
    };
    let (completion_result, control_result) = tokio::join!(
        execute_outcome(&repository, completion_racer),
        execute_control(&repository, control_racer),
    );
    match (completion_result, control_result) {
        (Ok(CommandOutcome::Applied(record)), Err(_)) => {
            assert_eq!(record.turn.state, ModelTurnState::Succeeded);
        }
        (Err(_), Ok(CommandOutcome::Applied(controlled))) => {
            assert_eq!(controlled.turn.state, ModelTurnState::Cancelling);
            let discovered = repository
                .scan_cancelling_model_executions(&cancellation_claim.worker_id, 16)
                .await
                .unwrap();
            assert_eq!(discovered.len(), 1);
            assert_eq!(discovered[0].turn.model_turn_id, cancellation.model_turn_id);
            assert_eq!(
                discovered[0].job_projection().unwrap().unwrap().state,
                JobState::Cancelling
            );
            let cancelling_job = controlled.job.as_ref().unwrap();
            let cancellation_fence = JobFence {
                expected_version: u64::try_from(cancelling_job.version).unwrap(),
                worker_process_generation_id: cancellation_claim.worker_id.clone(),
                lease_generation: u64::try_from(cancelling_job.lease_epoch).unwrap(),
                token_digest: cancellation_claim.lease_token.clone(),
            };
            let cancelled = execute_cancellation_outcome(
                &repository,
                CommitModelCancellationOutcome {
                    audit: worker_audit(
                        &fixture.tenant_id,
                        &cancellation_claim.worker_id,
                        0x370,
                        'd',
                        'e',
                    ),
                    model_turn_id: cancellation.model_turn_id,
                    job_id: id(ResourceKind::Job, 0x320),
                    expected_turn_version: controlled.turn.version,
                    fence: Some(cancellation_fence),
                    usage_reservation_id: cancellation_claim.usage_reservation_id,
                    quota_entry_ids: (0..4)
                        .map(|offset| id(ResourceKind::QuotaLedgerEntry, 0x373 + offset))
                        .collect(),
                    measurement: measurement(5, 0, 5),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                cancelled,
                CommandOutcome::Applied(record) if record.turn.state == ModelTurnState::Cancelled
            ));
        }
        other => panic!("Model cancellation/completion race had no single winner: {other:?}"),
    }
    let race_receipts: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_id IN ($2, $3)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::Receipt, 0x350).to_string())
    .bind(id(ResourceKind::Receipt, 0x360).to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(race_receipts, 1);

    let (_source_fence, owner_command) =
        seed_running_model_orchestration(&pool, &repository, &fixture, 0x800).await;
    let owner_request = owner_command.request.request.clone();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let deferred = match scheduler
        .defer_orchestration_to_model_turn(owner_command.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(deferred) => deferred,
        CommandOutcome::Replayed(_) => panic!("fresh Model owner dispatch replayed"),
    };
    scheduler.commit().await.unwrap();
    assert_eq!(deferred.turn.state, ModelTurnState::Ready);
    assert_eq!(deferred.model_job.state, JobState::Ready.as_str());
    assert_eq!(deferred.source_job.state, JobState::Succeeded.as_str());
    assert_eq!(deferred.run.state, "waiting");
    assert_eq!(deferred.run.active_work_count, 0);
    let node_state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(deferred.node_id.clone())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(node_state, "waiting");

    let mut replay = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        replay
            .defer_orchestration_to_model_turn(owner_command)
            .await
            .unwrap(),
        CommandOutcome::Replayed(record)
            if record.turn.model_turn_id == deferred.turn.model_turn_id
                && record.model_job.job_id == deferred.model_job.job_id
    ));
    replay.commit().await.unwrap();

    let owner_worker_id = id(ResourceKind::WorkerProcessGeneration, 0x860);
    let owner_lease_token = named_digest("model-owner-worker-lease");
    let owner_reservation_id = id(ResourceKind::UsageReservation, 0x861);
    let owner_resume_mutations = resume_mutations(0x870);
    let owner_failure_mutations = failure_mutations(0x880);
    let mut owner_claims = repository
        .claim_model_jobs(ClaimModelJobs {
            worker_process_generation_id: owner_worker_id.clone(),
            worker_manifest_digest: production_model_worker_manifest_digest(),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![ModelClaimSlot {
                lease_token_digest: owner_lease_token.clone(),
                usage_reservation_id: owner_reservation_id.clone(),
                quota_entry_ids: (0x862..=0x865)
                    .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                    .collect(),
                event_id: id(ResourceKind::Event, 0x866),
                outbox_id: id(ResourceKind::OutboxEvent, 0x867),
                resume_mutations: Some(owner_resume_mutations.clone()),
                failure_mutations: Some(owner_failure_mutations),
                tool_continuation_mutations: Some(
                    insight_platform_models::ModelToolContinuationMutationIds {
                        continuation_job_id: id(ResourceKind::Job, 0x890),
                        run_event_id: id(ResourceKind::Event, 0x891),
                        run_outbox_id: id(ResourceKind::OutboxEvent, 0x892),
                        node_event_id: id(ResourceKind::Event, 0x893),
                        node_outbox_id: id(ResourceKind::OutboxEvent, 0x894),
                        continuation_job_event_id: id(ResourceKind::Event, 0x895),
                        continuation_job_outbox_id: id(ResourceKind::OutboxEvent, 0x896),
                    },
                ),
            }],
        })
        .await
        .unwrap();
    assert_eq!(owner_claims.len(), 1);
    let owner_claim = owner_claims.pop().unwrap();
    assert_eq!(
        owner_claim.resume_mutations.as_ref(),
        Some(&owner_resume_mutations)
    );
    assert!(owner_claim.failure_mutations.is_some());
    assert!(owner_claim.tool_continuation_mutations.is_some());
    let owner_fence = JobFence {
        expected_version: u64::try_from(owner_claim.job.version).unwrap(),
        worker_process_generation_id: owner_worker_id.clone(),
        lease_generation: u64::try_from(owner_claim.job.lease_epoch).unwrap(),
        token_digest: owner_lease_token,
    };
    let mut tool_transaction = repository.begin_model_turn_transaction().await.unwrap();
    let tool_handoff = tool_transaction
        .commit_model_outcome(CommitModelOutcome {
            audit: worker_audit(&fixture.tenant_id, &owner_worker_id, 0x8a0, '3', '4'),
            model_turn_id: owner_claim.turn.model_turn_id.clone(),
            job_id: owner_claim.job.job_id.parse().unwrap(),
            expected_turn_version: owner_claim.turn.version,
            fence: owner_fence.clone(),
            usage_reservation_id: owner_reservation_id.clone(),
            quota_entry_ids: (0x8a3..=0x8a6)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
            request: owner_request.clone(),
            outcome: ModelDispatchOutcome::Succeeded(Box::new(output(
                &fixture,
                tool_response(&fixture, json!({"query": "durable handoff"})),
                id(ResourceKind::RunValue, 0x8a7),
            ))),
            resume_mutations: None,
            failure_mutations: None,
            tool_continuation_mutations: owner_claim.tool_continuation_mutations.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        tool_handoff,
        CommandOutcome::Applied(record) if record.turn.state == ModelTurnState::Succeeded
    ));
    tool_transaction.rollback().await.unwrap();
    let owner_completed = execute_outcome(
        &repository,
        CommitModelOutcome {
            audit: worker_audit(&fixture.tenant_id, &owner_worker_id, 0x890, '1', '2'),
            model_turn_id: owner_claim.turn.model_turn_id.clone(),
            job_id: owner_claim.job.job_id.parse().unwrap(),
            expected_turn_version: owner_claim.turn.version,
            fence: owner_fence,
            usage_reservation_id: owner_reservation_id,
            quota_entry_ids: (0x893..=0x896)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
            request: owner_request,
            outcome: ModelDispatchOutcome::Succeeded(Box::new(output(
                &fixture,
                final_response(&fixture),
                id(ResourceKind::RunValue, 0x897),
            ))),
            resume_mutations: owner_claim.resume_mutations,
            failure_mutations: None,
            tool_continuation_mutations: None,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        owner_completed,
        CommandOutcome::Applied(record) if record.turn.state == ModelTurnState::Succeeded
    ));
    let owner_states: (String, String, i32) = sqlx::query_as(
        r#"
        SELECT leaf.state, continuation.state, run.active_work_count
        FROM insight_platform.run_nodes AS leaf
        JOIN insight_platform.run_nodes AS continuation
          ON continuation.tenant_id = leaf.tenant_id AND continuation.node_id = $3
        JOIN insight_platform.runs AS run
          ON run.tenant_id = leaf.tenant_id AND run.run_id = leaf.run_id
        WHERE leaf.tenant_id = $1 AND leaf.node_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(deferred.node_id)
    .bind(
        owner_resume_mutations
            .continuation_node_execution_id
            .to_string(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        owner_states,
        ("succeeded".to_owned(), "ready".to_owned(), 0)
    );

    sqlx::query(
        "UPDATE insight_platform.jobs SET state = 'cancelled', terminal_at = clock_timestamp(), updated_at = clock_timestamp() WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(owner_resume_mutations.continuation_job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.run_nodes SET state = 'cancelled', terminal_at = clock_timestamp(), updated_at = clock_timestamp() WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(owner_resume_mutations.continuation_node_execution_id.to_string())
    .execute(&pool)
    .await
    .unwrap();

    let (_, tool_owner_command) =
        seed_running_model_orchestration(&pool, &repository, &fixture, 0x900).await;
    let tool_owner_request = tool_owner_command.request.request.clone();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let tool_owner = match scheduler
        .defer_orchestration_to_model_turn(tool_owner_command)
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh tool owner dispatch replayed"),
    };
    scheduler.commit().await.unwrap();
    let tool_worker_id = id(ResourceKind::WorkerProcessGeneration, 0x960);
    let tool_lease_token = named_digest("model-tool-worker-lease");
    let tool_reservation_id = id(ResourceKind::UsageReservation, 0x961);
    let tool_continuation_job_id = id(ResourceKind::Job, 0x990);
    let tool_continuation_mutations = insight_platform_models::ModelToolContinuationMutationIds {
        continuation_job_id: tool_continuation_job_id.clone(),
        run_event_id: id(ResourceKind::Event, 0x991),
        run_outbox_id: id(ResourceKind::OutboxEvent, 0x992),
        node_event_id: id(ResourceKind::Event, 0x993),
        node_outbox_id: id(ResourceKind::OutboxEvent, 0x994),
        continuation_job_event_id: id(ResourceKind::Event, 0x995),
        continuation_job_outbox_id: id(ResourceKind::OutboxEvent, 0x996),
    };
    let mut claims = repository
        .claim_model_jobs(ClaimModelJobs {
            worker_process_generation_id: tool_worker_id.clone(),
            worker_manifest_digest: production_model_worker_manifest_digest(),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![ModelClaimSlot {
                lease_token_digest: tool_lease_token.clone(),
                usage_reservation_id: tool_reservation_id.clone(),
                quota_entry_ids: (0x962..=0x965)
                    .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                    .collect(),
                event_id: id(ResourceKind::Event, 0x966),
                outbox_id: id(ResourceKind::OutboxEvent, 0x967),
                resume_mutations: Some(resume_mutations(0x970)),
                failure_mutations: Some(failure_mutations(0x980)),
                tool_continuation_mutations: Some(tool_continuation_mutations.clone()),
            }],
        })
        .await
        .unwrap();
    let tool_claim = claims.pop().unwrap();
    let tool_output_id = id(ResourceKind::RunValue, 0x9a7);
    let fanout_response = two_tool_response(&fixture);
    execute_outcome(
        &repository,
        CommitModelOutcome {
            audit: worker_audit(&fixture.tenant_id, &tool_worker_id, 0x9a0, '5', '6'),
            model_turn_id: tool_claim.turn.model_turn_id.clone(),
            job_id: tool_claim.job.job_id.parse().unwrap(),
            expected_turn_version: tool_claim.turn.version,
            fence: JobFence {
                expected_version: u64::try_from(tool_claim.job.version).unwrap(),
                worker_process_generation_id: tool_worker_id,
                lease_generation: u64::try_from(tool_claim.job.lease_epoch).unwrap(),
                token_digest: tool_lease_token,
            },
            usage_reservation_id: tool_reservation_id,
            quota_entry_ids: (0x9a3..=0x9a6)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
            request: tool_owner_request,
            outcome: ModelDispatchOutcome::Succeeded(Box::new(output(
                &fixture,
                fanout_response.clone(),
                tool_output_id,
            ))),
            resume_mutations: None,
            failure_mutations: None,
            tool_continuation_mutations: Some(tool_continuation_mutations),
        },
    )
    .await
    .unwrap();

    let orchestration_worker = id(ResourceKind::WorkerProcessGeneration, 0x9b0);
    let orchestration_token = named_digest("model-tool-orchestration-lease");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let mut claimed = scheduler
        .claim_orchestration_jobs(ClaimOrchestrationJobs {
            worker_id: orchestration_worker.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![OrchestrationClaimSlot {
                lease_token_digest: orchestration_token.clone(),
                quota_reservation_id: id(ResourceKind::UsageReservation, 0x9b1),
                quota_entry_ids: (0x9b2..=0x9b5)
                    .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                    .collect(),
                run_event_id: id(ResourceKind::Event, 0x9b6),
                run_outbox_id: id(ResourceKind::OutboxEvent, 0x9b7),
                node_event_id: id(ResourceKind::Event, 0x9b8),
                node_outbox_id: id(ResourceKind::OutboxEvent, 0x9b9),
                job_event_id: id(ResourceKind::Event, 0x9ba),
                job_outbox_id: id(ResourceKind::OutboxEvent, 0x9bb),
            }],
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    let claimed = claimed.pop().unwrap();
    assert_eq!(claimed.job.job_id, tool_continuation_job_id.to_string());
    let start = StartOrchestrationJob {
        fence: RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: claimed.job.job_id.clone(),
            worker_id: orchestration_worker.clone(),
            lease_epoch: claimed.job.lease_epoch,
            expected_job_version: claimed.job.version,
            lease_token_digest: orchestration_token.clone(),
        },
        receipt_id: id(ResourceKind::Receipt, 0x9bc),
        idempotency_key_digest: named_digest("model-tool-orchestration-start"),
        request_digest: named_digest("model-tool-orchestration-start-request"),
        receipt_expires_at: fixture.deadline,
        job_event_id: id(ResourceKind::Event, 0x9bd),
        job_outbox_id: id(ResourceKind::OutboxEvent, 0x9be),
        node_event_id: id(ResourceKind::Event, 0x9bf),
        node_outbox_id: id(ResourceKind::OutboxEvent, 0x9c0),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let started = match scheduler
        .start_orchestration_job(start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh tool continuation start replayed"),
    };
    scheduler.commit().await.unwrap();
    let fence = RepositoryJobFence {
        expected_job_version: started.version,
        ..start.fence
    };
    let continuation = insight_platform_orchestrator::ModelToolContinuation {
        model_turn_id: tool_owner.turn.model_turn_id,
        response_value_id: id(ResourceKind::RunValue, 0x9a7),
        response_digest: canonical_digest(&serde_json::to_value(&fanout_response).unwrap())
            .unwrap()
            .parse()
            .unwrap(),
        round_ordinal: 1,
        tool_intent_count: 2,
        results: Vec::new(),
    };
    let facts = repository
        .load_model_tool_continuation_facts(&fence, &fixture.runtime_plan, &continuation)
        .await
        .unwrap();
    assert_eq!(facts.calls.len(), 2);
    let calls = facts
        .calls
        .iter()
        .enumerate()
        .map(|(ordinal, fact)| {
            let base = if ordinal == 0 { 0x9d0 } else { 0x9f0 };
            let call_id_digest: Sha256Digest = canonical_digest(&json!({"call_id": fact.call_id}))
                .unwrap()
                .parse()
                .unwrap();
            ModelToolCapabilityAdmission {
                call_id: fact.call_id.clone(),
                call_id_digest,
                projected_tool_name: fact.projected_tool_name.clone(),
                slot_id: fact.slot_id.clone(),
                selected_candidate_ordinal: fact.selected_candidate_ordinal,
                selected_deployment: fact.selected_deployment.clone(),
                input_value_id: id(ResourceKind::RunValue, base),
                arguments: fact.arguments.clone(),
                invocation_id: id(ResourceKind::CapabilityInvocation, base + 1),
                capability_job_id: id(ResourceKind::Job, base + 2),
                policy_decisions: allowed_tool_policy(&fixture),
                approval_task_id: None,
                mcp_runtime: None,
                requested_attempt_limit: 3,
                requested_retry_backoff_milliseconds: 100,
                idempotency_key_digest: named_digest(&format!(
                    "model-tool-capability-idempotency-{ordinal}"
                )),
                mutations: ModelToolCapabilityMutationIds {
                    admit_receipt_id: id(ResourceKind::Receipt, base + 3),
                    admit_event_id: id(ResourceKind::Event, base + 4),
                    admit_outbox_id: id(ResourceKind::OutboxEvent, base + 5),
                    prepare_receipt_id: id(ResourceKind::Receipt, base + 6),
                    prepare_event_id: id(ResourceKind::Event, base + 7),
                    prepare_outbox_id: id(ResourceKind::OutboxEvent, base + 8),
                    sibling_cancel_event_id: id(ResourceKind::Event, base + 9),
                    sibling_cancel_outbox_id: id(ResourceKind::OutboxEvent, base + 10),
                },
            }
        })
        .collect();
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let dispatched = scheduler
        .dispatch_model_tool_capabilities(DispatchModelToolCapabilities {
            fence,
            plan: fixture.runtime_plan.clone(),
            continuation,
            calls,
            request_digest: named_digest("model-tool-fanout"),
            receipt_expires_at: fixture.deadline,
            source_mutations: OrchestrationYieldMutationIds {
                receipt_id: id(ResourceKind::Receipt, 0x9e0),
                quota_entry_ids: (0x9e1..=0x9e4)
                    .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                    .collect(),
                run_event_id: id(ResourceKind::Event, 0x9e5),
                run_outbox_id: id(ResourceKind::OutboxEvent, 0x9e6),
                node_event_id: id(ResourceKind::Event, 0x9e7),
                node_outbox_id: id(ResourceKind::OutboxEvent, 0x9e8),
                job_event_id: id(ResourceKind::Event, 0x9e9),
                job_outbox_id: id(ResourceKind::OutboxEvent, 0x9ea),
            },
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    let CommandOutcome::Applied(dispatched) = dispatched else {
        panic!("fresh Model tool fanout replayed")
    };
    assert_eq!(dispatched.invocations.len(), 2);
    assert_eq!(dispatched.capability_jobs.len(), 2);
    assert_eq!(dispatched.source_job.state, JobState::Succeeded.as_str());
    assert_eq!(dispatched.run.state, "waiting");
    assert_eq!(dispatched.run.active_work_count, 0);
    let origin = &dispatched.invocations[0].payload.admission.origin_key;
    assert!(matches!(
        origin,
        insight_platform_invocations::InvocationOrigin::ModelToolCall { model_turn_id, .. }
            if model_turn_id == &dispatched.invocations[0].owner_id
    ));

    let capability_worker_id = id(ResourceKind::WorkerProcessGeneration, 0xa00);
    let capability_lease = named_digest("model-tool-capability-lease");
    let capability_resume = resume_mutations(0xa20);
    let capability_failure = failure_mutations(0xa40);
    let mut claims = repository
        .claim_capability_jobs(ClaimCapabilityJobs {
            work_class: WorkClass::CapabilityNative,
            worker_process_generation_id: capability_worker_id.clone(),
            worker_manifest_digest: production_capability_worker_manifest_digest(),
            limit: 2,
            lease_milliseconds: 30_000,
            slots: vec![
                CapabilityClaimSlot {
                    lease_token_digest: capability_lease.clone(),
                    quota_reservation_id: id(ResourceKind::UsageReservation, 0xa01),
                    quota_entry_ids: vec![
                        id(ResourceKind::QuotaLedgerEntry, 0xa02),
                        id(ResourceKind::QuotaLedgerEntry, 0xa03),
                    ],
                    event_id: id(ResourceKind::Event, 0xa04),
                    outbox_id: id(ResourceKind::OutboxEvent, 0xa05),
                    resume_mutations: capability_resume.clone(),
                    failure_mutations: capability_failure,
                },
                CapabilityClaimSlot {
                    lease_token_digest: named_digest("unclaimed-model-tool-capability-lease"),
                    quota_reservation_id: id(ResourceKind::UsageReservation, 0xa51),
                    quota_entry_ids: vec![
                        id(ResourceKind::QuotaLedgerEntry, 0xa52),
                        id(ResourceKind::QuotaLedgerEntry, 0xa53),
                    ],
                    event_id: id(ResourceKind::Event, 0xa54),
                    outbox_id: id(ResourceKind::OutboxEvent, 0xa55),
                    resume_mutations: resume_mutations(0xa56),
                    failure_mutations: failure_mutations(0xa70),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    let capability_claim = claims.pop().unwrap();
    assert_eq!(
        capability_claim.resume_mutations,
        Some(capability_resume.clone())
    );
    let mut invocation_transaction = repository.begin_invocation_transaction().await.unwrap();
    let failed = invocation_transaction
        .commit_capability_outcome(CommitCapabilityOutcome {
            audit: capability_worker_audit(&fixture.tenant_id, &capability_worker_id, 0xa06),
            invocation_id: capability_claim.invocation.invocation_id.clone(),
            job_id: capability_claim.job.job_id.parse().unwrap(),
            expected_invocation_version: capability_claim.invocation.version,
            fence: capability_claim.fence.clone(),
            quota_entry_ids: vec![
                id(ResourceKind::QuotaLedgerEntry, 0xa09),
                id(ResourceKind::QuotaLedgerEntry, 0xa0a),
            ],
            outcome: DispatchOutcome::PermanentFailure(SafeBackendFailure {
                failure: insight_platform_contracts::Failure {
                    code: insight_platform_contracts::FailureCode::Platform {
                        code: insight_platform_contracts::PlatformFailureCode::CapabilityFailed,
                    },
                    class: insight_platform_contracts::FailureClass::External,
                    retryability: insight_platform_contracts::Retryability::Never,
                    safe_message: Some("fixture capability failure".to_owned()),
                    details_ref: None,
                    source: insight_platform_contracts::FailureSource::Capability,
                },
                evidence_digest: named_digest("model-tool-permanent-failure"),
            }),
            resume_mutations: None,
            failure_mutations: capability_claim.failure_mutations.clone(),
        })
        .await
        .unwrap();
    assert!(matches!(
        failed,
        CommandOutcome::Applied(record)
            if record.invocation.state == insight_platform_contracts::InvocationState::Failed
    ));
    invocation_transaction.rollback().await.unwrap();

    let tool_result_value = json!({"query": "tool-result"});
    let tool_result_digest: Sha256Digest = canonical_digest(&tool_result_value)
        .unwrap()
        .parse()
        .unwrap();
    let tool_result_value_id = id(ResourceKind::RunValue, 0xa10);
    let mut invocation_transaction = repository.begin_invocation_transaction().await.unwrap();
    let completed = invocation_transaction
        .commit_capability_outcome(CommitCapabilityOutcome {
            audit: capability_worker_audit(&fixture.tenant_id, &capability_worker_id, 0xa06),
            invocation_id: capability_claim.invocation.invocation_id.clone(),
            job_id: capability_claim.job.job_id.parse().unwrap(),
            expected_invocation_version: capability_claim.invocation.version,
            fence: capability_claim.fence,
            quota_entry_ids: vec![
                id(ResourceKind::QuotaLedgerEntry, 0xa09),
                id(ResourceKind::QuotaLedgerEntry, 0xa0a),
            ],
            outcome: DispatchOutcome::Completed(CapabilityOutputValue {
                value_id: tool_result_value_id.clone(),
                classification: DataClassification::Internal,
                schema_digest: fixture.capability_output_schema.canonical_digest.clone(),
                content_digest: tool_result_digest,
                value: ValueRef::Inline {
                    value: tool_result_value,
                },
                artifact_link_id: None,
                validation_evidence_digest: named_digest("model-tool-validation"),
            }),
            resume_mutations: capability_claim.resume_mutations,
            failure_mutations: None,
        })
        .await
        .unwrap();
    invocation_transaction.commit().await.unwrap();
    assert!(matches!(
        completed,
        CommandOutcome::Applied(record)
            if record.invocation.state == insight_platform_contracts::InvocationState::Succeeded
    ));
    let partial_result: (String, i32, i64) = sqlx::query_as(
        r#"
        SELECT node.state, run.active_work_count,
               (SELECT count(*) FROM insight_platform.jobs AS continuation
                WHERE continuation.tenant_id = node.tenant_id
                  AND continuation.job_id = $3)
        FROM insight_platform.run_nodes AS node
        JOIN insight_platform.runs AS run
          ON run.tenant_id = node.tenant_id AND run.run_id = node.run_id
        WHERE node.tenant_id = $1 AND node.node_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(tool_owner.node_id.clone())
    .bind(capability_resume.continuation_job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(partial_result, ("waiting".to_owned(), 0, 0));

    let second_worker_id = id(ResourceKind::WorkerProcessGeneration, 0xa60);
    let second_lease = named_digest("model-tool-capability-second-lease");
    let second_resume = resume_mutations(0xa70);
    let mut second_claims = repository
        .claim_capability_jobs(ClaimCapabilityJobs {
            work_class: WorkClass::CapabilityNative,
            worker_process_generation_id: second_worker_id.clone(),
            worker_manifest_digest: production_capability_worker_manifest_digest(),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![CapabilityClaimSlot {
                lease_token_digest: second_lease,
                quota_reservation_id: id(ResourceKind::UsageReservation, 0xa61),
                quota_entry_ids: vec![
                    id(ResourceKind::QuotaLedgerEntry, 0xa62),
                    id(ResourceKind::QuotaLedgerEntry, 0xa63),
                ],
                event_id: id(ResourceKind::Event, 0xa64),
                outbox_id: id(ResourceKind::OutboxEvent, 0xa65),
                resume_mutations: second_resume.clone(),
                failure_mutations: failure_mutations(0xa90),
            }],
        })
        .await
        .unwrap();
    assert_eq!(second_claims.len(), 1);
    let second_claim = second_claims.pop().unwrap();
    let second_result_value = json!({"query": "tool-result-2"});
    let second_result_digest: Sha256Digest = canonical_digest(&second_result_value)
        .unwrap()
        .parse()
        .unwrap();
    let second_result_value_id = id(ResourceKind::RunValue, 0xaa0);
    let mut invocation_transaction = repository.begin_invocation_transaction().await.unwrap();
    let second_completed = invocation_transaction
        .commit_capability_outcome(CommitCapabilityOutcome {
            audit: capability_worker_audit(&fixture.tenant_id, &second_worker_id, 0xa66),
            invocation_id: second_claim.invocation.invocation_id.clone(),
            job_id: second_claim.job.job_id.parse().unwrap(),
            expected_invocation_version: second_claim.invocation.version,
            fence: second_claim.fence,
            quota_entry_ids: vec![
                id(ResourceKind::QuotaLedgerEntry, 0xa69),
                id(ResourceKind::QuotaLedgerEntry, 0xa6a),
            ],
            outcome: DispatchOutcome::Completed(CapabilityOutputValue {
                value_id: second_result_value_id.clone(),
                classification: DataClassification::Internal,
                schema_digest: fixture.capability_output_schema.canonical_digest.clone(),
                content_digest: second_result_digest,
                value: ValueRef::Inline {
                    value: second_result_value,
                },
                artifact_link_id: None,
                validation_evidence_digest: named_digest("model-tool-validation-second"),
            }),
            resume_mutations: second_claim.resume_mutations,
            failure_mutations: None,
        })
        .await
        .unwrap();
    invocation_transaction.commit().await.unwrap();
    assert!(matches!(
        second_completed,
        CommandOutcome::Applied(record)
            if record.invocation.state == insight_platform_contracts::InvocationState::Succeeded
    ));
    let result_job: (String, String, i32) = sqlx::query_as(
        r#"
        SELECT job.state, node.state, run.active_work_count
        FROM insight_platform.jobs AS job
        JOIN insight_platform.run_nodes AS node
          ON node.tenant_id = job.tenant_id AND node.node_id = job.node_id
        JOIN insight_platform.runs AS run
          ON run.tenant_id = job.tenant_id AND run.run_id = job.run_id
        WHERE job.tenant_id = $1 AND job.job_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(second_resume.continuation_job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(result_job, ("ready".to_owned(), "ready".to_owned(), 0));

    let result_orchestration_worker = id(ResourceKind::WorkerProcessGeneration, 0xb00);
    let result_orchestration_lease = named_digest("model-tool-result-orchestration-lease");
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let mut claims = scheduler
        .claim_orchestration_jobs(ClaimOrchestrationJobs {
            worker_id: result_orchestration_worker.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
            slots: vec![OrchestrationClaimSlot {
                lease_token_digest: result_orchestration_lease.clone(),
                quota_reservation_id: id(ResourceKind::UsageReservation, 0xb01),
                quota_entry_ids: (0xb02..=0xb05)
                    .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                    .collect(),
                run_event_id: id(ResourceKind::Event, 0xb06),
                run_outbox_id: id(ResourceKind::OutboxEvent, 0xb07),
                node_event_id: id(ResourceKind::Event, 0xb08),
                node_outbox_id: id(ResourceKind::OutboxEvent, 0xb09),
                job_event_id: id(ResourceKind::Event, 0xb0a),
                job_outbox_id: id(ResourceKind::OutboxEvent, 0xb0b),
            }],
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    let result_claim = claims.pop().unwrap();
    assert_eq!(
        result_claim.job.job_id,
        second_resume.continuation_job_id.to_string()
    );
    let start = StartOrchestrationJob {
        fence: RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: result_claim.job.job_id.clone(),
            worker_id: result_orchestration_worker,
            lease_epoch: result_claim.job.lease_epoch,
            expected_job_version: result_claim.job.version,
            lease_token_digest: result_orchestration_lease,
        },
        receipt_id: id(ResourceKind::Receipt, 0xb0c),
        idempotency_key_digest: named_digest("model-tool-result-start"),
        request_digest: named_digest("model-tool-result-start-request"),
        receipt_expires_at: fixture.deadline,
        job_event_id: id(ResourceKind::Event, 0xb0d),
        job_outbox_id: id(ResourceKind::OutboxEvent, 0xb0e),
        node_event_id: id(ResourceKind::Event, 0xb0f),
        node_outbox_id: id(ResourceKind::OutboxEvent, 0xb10),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let started = match scheduler
        .start_orchestration_job(start.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh Model tool result start replayed"),
    };
    scheduler.commit().await.unwrap();
    let result_fence = RepositoryJobFence {
        expected_job_version: started.version,
        ..start.fence
    };
    let mut job_payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(second_resume.continuation_job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    job_payload
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    let job_payload: OrchestrationJobPayload = serde_json::from_value(job_payload).unwrap();
    let result_continuation = job_payload.model_tool_continuation.unwrap();
    assert_eq!(result_continuation.results.len(), 2);
    assert_eq!(
        result_continuation.results[0].output_value_id,
        tool_result_value_id
    );
    assert_eq!(
        result_continuation.results[1].output_value_id,
        second_result_value_id
    );
    let result_facts = repository
        .load_model_tool_result_continuation_facts(
            &result_fence,
            &fixture.runtime_plan,
            &result_continuation,
        )
        .await
        .unwrap();
    let next_model_turn_id = id(ResourceKind::ModelTurn, 0xb20);
    let next_request_value_id = id(ResourceKind::RunValue, 0xb21);
    let mut next_request = result_facts.previous_request.request.clone();
    next_request.model_turn_id = next_model_turn_id.clone();
    let result_source_value = serde_json::to_value(&result_facts.results).unwrap();
    let result_source_digest: Sha256Digest = canonical_digest(&result_source_value)
        .unwrap()
        .parse()
        .unwrap();
    next_request.messages.push(CanonicalMessage {
        role: CanonicalMessageRole::Tool,
        parts: result_facts
            .results
            .iter()
            .cloned()
            .map(CanonicalMessagePart::ToolResult)
            .collect(),
        classification: DataClassification::Internal,
        source: ModelContentSource {
            source_kind: "capability_tool_result".to_owned(),
            source_id: "tool-result-fixture".to_owned(),
            source_digest: result_source_digest.clone(),
            content_digest: result_source_digest.clone(),
            assembly_phase: insight_platform_models::PromptAssemblyPhase::CapabilityToolResult,
            ordinal: 0,
            byte_budget: 1_024,
            token_budget: 256,
            trusted_instruction: false,
        },
    });
    next_request.input_token_estimate += 1;
    next_request.source_map_digest = canonical_digest(&json!({
        "previous_source_map_digest": next_request.source_map_digest,
        "tool_result_source_digest": result_source_digest,
    }))
    .unwrap()
    .parse()
    .unwrap();
    let next_request_json = serde_json::to_value(&next_request).unwrap();
    let next_request_digest: Sha256Digest = canonical_digest(&next_request_json)
        .unwrap()
        .parse()
        .unwrap();
    let next_request = ModelRequestValue {
        value_id: next_request_value_id,
        classification: DataClassification::Internal,
        schema_digest: result_facts.previous_request.schema_digest,
        content_digest: next_request_digest,
        value: ValueRef::Inline {
            value: next_request_json,
        },
        request: next_request,
    };
    let next_mutations = DeferOrchestrationModelMutationIds {
        source: OrchestrationYieldMutationIds {
            receipt_id: id(ResourceKind::Receipt, 0xb30),
            quota_entry_ids: (0xb31..=0xb34)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
            run_event_id: id(ResourceKind::Event, 0xb35),
            run_outbox_id: id(ResourceKind::OutboxEvent, 0xb36),
            node_event_id: id(ResourceKind::Event, 0xb37),
            node_outbox_id: id(ResourceKind::OutboxEvent, 0xb38),
            job_event_id: id(ResourceKind::Event, 0xb39),
            job_outbox_id: id(ResourceKind::OutboxEvent, 0xb3a),
        },
        model_create_receipt_id: id(ResourceKind::Receipt, 0xb3b),
        model_create_event_id: id(ResourceKind::Event, 0xb3c),
        model_create_outbox_id: id(ResourceKind::OutboxEvent, 0xb3d),
        model_prepare_receipt_id: id(ResourceKind::Receipt, 0xb3e),
        model_prepare_event_id: id(ResourceKind::Event, 0xb3f),
        model_prepare_outbox_id: id(ResourceKind::OutboxEvent, 0xb40),
    };
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let continued = scheduler
        .continue_model_tool_results_to_model_turn(ContinueModelToolResultsToModelTurn {
            fence: result_fence,
            plan: fixture.runtime_plan.clone(),
            continuation: result_continuation,
            model_turn_id: next_model_turn_id,
            model_job_id: id(ResourceKind::Job, 0xb22),
            request: next_request,
            requested_attempt_limit: result_facts.requested_attempt_limit,
            cost_ceiling_microunits: result_facts.cost_ceiling_microunits,
            idempotency_key_digest: named_digest("model-tool-next-turn-idempotency"),
            request_digest: named_digest("model-tool-next-turn-request"),
            receipt_expires_at: fixture.deadline,
            mutations: next_mutations,
        })
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    let CommandOutcome::Applied(continued) = continued else {
        panic!("fresh Model tool continuation replayed")
    };
    assert_eq!(continued.turn.round_ordinal, 2);
    assert_eq!(continued.turn.state, ModelTurnState::Ready);
    assert_eq!(continued.model_job.state, JobState::Ready.as_str());
    assert_eq!(continued.run.state, "waiting");
    assert_eq!(continued.run.active_work_count, 0);
}
