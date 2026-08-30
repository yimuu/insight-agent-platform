use chrono::{DateTime, Duration, Utc};
use insight_platform_artifact_rpc::{
    proto::artifact_scheduler_service_server::ArtifactSchedulerServiceServer,
    ArtifactInternalRpcLimits, ArtifactSchedulerGrpcService, LeasedArtifactBytes,
    SchedulerRunValueResponseBroker, SchedulerSkillPackageResponseBroker,
    SchedulerTypedPlanResponseBroker, SchedulerWorkloadIdentity, SCHEDULER_WORKLOAD_IDENTITY,
};
use insight_platform_artifacts::{
    ArtifactBackendFailure, ArtifactBlobBackend, ArtifactBlobDeletionEvidence,
    ArtifactScanDisposition, ArtifactScanEvidence, ArtifactScanEvidenceDraft, ArtifactScanRequest,
    ArtifactScanner, ArtifactWorkerService, DeleteArtifactBlobGeneration,
    SchedulerRunValueReadError, SchedulerRunValueReadRequest, SchedulerSkillPackageReadError,
    SchedulerSkillPackageReadRequest, SchedulerTypedPlanLease, SchedulerTypedPlanReadError,
    SchedulerTypedPlanReadRequest, SchedulerTypedPlanRequestResolver, StageWorkloadArtifact,
    StageWorkloadArtifactRequest, WorkloadArtifactStagePreflight,
};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityTransportCancelOutcome, CapabilityTransportCancelRequest,
    GrpcNetworkTransport, GrpcTransportRequest, GrpcTransportResponse, HttpNetworkTransport,
    HttpTransportRequest, HttpTransportResponse,
};
use insight_platform_context::{
    CitationLocator, ClaimContextJobs, CommitContextDatasetBuild, CommitContextOutcome,
    ContextBackendOutcome, ContextCitation, ContextClaimSlot, ContextDatasetArtifactPreallocation,
    ContextDatasetArtifactPreallocations, ContextDatasetBuildJobPayload, ContextItem,
    ContextObservation, ContextObservationOutput, ContextQueryRequest, ContextQueryTransaction,
    ContextRetrievalEvidence, ContextSignalAudit, ContextWorkerAudit, CreateContextQuery,
    FailContextDatasetBuildVerification, NormalizedContextScore,
    ParkContextDatasetBuildVerification, PrepareContextDispatch, ReadOnlySqlExecutionBinding,
    ReadOnlySqlPlan, RemoteContextFailure, RemoteContextItem, RemoteContextSearchConnector,
    RemoteContextSearchRequest, RemoteContextSearchResponse, RequestContextDatasetBuild,
    SqlColumnRef, SqlComparisonOperator, SqlObjectName, SqlPredicate, SqlProjection,
    SqlProjectionExpression, SqlSource, WakeContextDispatch, READONLY_DATABASE_CAPABILITY,
    TEXT2SQL_PLAN_VALUE_KIND,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, checked_in_hard_limit_profile, AdministrativeGate,
    AgentDeploymentClosure, AgentResourceSpec, ArtifactRef, ArtifactRetentionPolicy,
    ArtifactWorkloadAudience, AuthoringPackage, CandidateSelectionMode,
    CandidateSelectionPolicyDocument, CanonicalHttpEndpoint, CapabilityArtifactContract,
    CapabilityBackendBinding, CapabilityBackendContract, CapabilityBackendFeatures,
    CapabilityBackendKind, CapabilityBackendLimits, CapabilityCancellationKind,
    CapabilityDataFlowPolicy, CapabilityDeploymentClosure, CapabilityEndpointScheme,
    CapabilityIdempotencyKind, CapabilityImplementationResourceSpec, CapabilityInterfaceLimits,
    CapabilityInterfaceResourceSpec, CapabilityProgressContract, CapabilityProgressDurability,
    CapabilityProgressMode, ClosedJsonSchema, ClosedJsonValue, CommandAudit, CommandOutcome,
    ContextBackendBinding, ContextBackendContract, ContextBackendKind, ContextBackendLimits,
    ContextBindingSnapshot, ContextCitationContract, ContextCitationStrength,
    ContextConsistencyMode, ContextConsistencyPolicy, ContextDataPolicyContract,
    ContextDatasetGenerationSpec, ContextDeploymentClosure, ContextImplementationContract,
    ContextImplementationResourceSpec, ContextInterfaceLimits, ContextInterfaceResourceSpec,
    ContextLocatorKind, ContextPaginationContract, ContextQueryState, ContextRankingContract,
    DataClassification, DataRegion, DeploymentClosure, Effect, EntityLifecycle, ExactDeploymentRef,
    ExactPolicyBinding, ExactVersionRef, ExternalLeafFailureMutationIds,
    ExternalLeafResumeMutationIds, Failure, FailureClass, FailureCode, FailureSource,
    FrozenSlotBinding, FrozenSlotTarget, JobState, NativeCapabilityContract, Permission,
    PermissionSet, PlatformFailureCode, PolicyDeploymentClosure, PolicyKind, PolicyResourceSpec,
    PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot, PublishedVersionPayload,
    QuotaDimension, RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind, Retryability,
    RunBindingsSnapshot, SandboxArtifactIoPolicyDocument, SchedulerPriority,
    SchedulingPolicyDocument, Sha256Digest, TenantConfig, TenantPrincipalPayload,
    ValidationSummary, ValueRef, WorkClass, WorkerManifest, WORKER_MANIFEST_VERSION,
    WORKER_PROTOCOL_VERSION,
};
use insight_platform_egress_rpc::{
    proto::egress_broker_service_server::EgressBrokerServiceServer, EgressBrokerGrpcService,
    EgressCallerWorkloadIdentity, EgressInternalRpcLimits, CONTEXT_WORKER_WORKLOAD_IDENTITY,
};
use insight_platform_invocations::{
    AdmitCapabilityInvocation, ExactInvocationValueRef, InvocationOrigin, InvocationPolicyDecision,
    InvocationPolicyDecisionBundle, InvocationPolicyDisposition, InvocationTransaction,
    InvocationValueStorage,
};
use insight_platform_jobs::{JobFence, LeasePolicy, WakeContract, WakeKind, WakeSource};
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelProviderWireConnector, ModelProviderWireProtocol, ModelProviderWireRequest,
    ModelProviderWireStream,
};
use insight_platform_orchestrator::{
    DataPortKey, ExactDataPortRef, ExactRunValueRef, OrchestrationJobPayload, PlanLimits,
    PlanNodeKey, RunCurrentSnapshot, RuntimeDependencyKind, RuntimeDependencySlot, RuntimeNode,
    RuntimePlan, ScopeDataEnvironmentSnapshot, ScopeEnvironmentLimits,
};
use insight_platform_postgres::{
    artifact_repository::{ArtifactExecutionSlot, StartedArtifactExecution},
    context_query_repository::{
        ClaimedContextExecution, DriveExpiredContextJobs, ExpiredContextRecoverySlot,
        PreparedContextExecution,
    },
    repository::{
        ArtifactWorkerRole, ClaimArtifactJobs, ClaimJobs, DeferOrchestrationContextMutationIds,
        DeferOrchestrationToContextQuery, JobFence as RepositoryJobFence, NewPrincipal,
        NewQuotaAccount, NewTenant, NewTenantPrincipal, OrchestrationYieldMutationIds,
        PgRepository, RepositoryError, ResolvedExpressionInput, SafetyScanShard, TypedPayload,
    },
    verify_schema,
};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{
    collections::BTreeMap,
    io::BufRead as _,
    path::{Path, PathBuf},
    process::{Child, Stdio},
    sync::{
        atomic::{AtomicU16, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration as StdDuration,
};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

static CONTEXT_FIXTURE_NAMESPACE: AtomicU16 = AtomicU16::new(0xa0f6);
static CONTEXT_FIXTURE_NAMESPACE_LOCK: Mutex<()> = Mutex::new(());

fn select_fixture_namespace(namespace: u16) -> MutexGuard<'static, ()> {
    let guard = CONTEXT_FIXTURE_NAMESPACE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    CONTEXT_FIXTURE_NAMESPACE.store(namespace, Ordering::SeqCst);
    guard
}

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    let namespace = CONTEXT_FIXTURE_NAMESPACE.load(Ordering::SeqCst);
    format!(
        "{}_0198f1c9-32e4-75e1-a9e8-d95c{namespace:04x}{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn dataset_artifact_preallocations(base: u16) -> ContextDatasetArtifactPreallocations {
    let allocation = |offset: u16| ContextDatasetArtifactPreallocation {
        schema_version: 1,
        artifact_id: id(ResourceKind::Artifact, base + offset),
        blob_id: id(ResourceKind::InternalBlob, base + offset + 1),
        verification_job_id: id(ResourceKind::Job, base + offset + 2),
        quota_entry_id: id(ResourceKind::QuotaLedgerEntry, base + offset + 3),
    };
    ContextDatasetArtifactPreallocations {
        schema_version: 1,
        generation_id: id(ResourceKind::DatasetGeneration, base + 8),
        index_manifest: allocation(0),
        validation_evidence: allocation(4),
    }
}

fn named_digest(label: &str) -> Sha256Digest {
    let namespace = CONTEXT_FIXTURE_NAMESPACE.load(Ordering::SeqCst);
    canonical_digest(&json!({
        "phase3_context": label,
        "fixture_namespace": namespace,
    }))
    .unwrap()
    .parse()
    .unwrap()
}

fn raw_digest(bytes: &[u8]) -> Sha256Digest {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").unwrap();
    }
    encoded.parse().unwrap()
}

struct ContextDatasetArtifactScanner {
    content_digest: Sha256Digest,
    size_bytes: u64,
    media_type: String,
    observed_at: DateTime<Utc>,
    disposition: ArtifactScanDisposition,
}

impl ArtifactScanner for ContextDatasetArtifactScanner {
    async fn scan(
        &self,
        request: ArtifactScanRequest,
    ) -> Result<ArtifactScanEvidence, ArtifactBackendFailure> {
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
            disposition: self.disposition,
            reason_class: (self.disposition != ArtifactScanDisposition::Verified)
                .then(|| "context_dataset_fixture_rejection".to_owned()),
            observed_at: self.observed_at,
            expires_at: self.observed_at
                + Duration::milliseconds(
                    i64::try_from(request.job.evidence_ttl_milliseconds).unwrap(),
                ),
        }
        .seal()
        .map_err(|_| ArtifactBackendFailure {
            retryable: false,
            reason_class: "context_dataset_fixture_scan_invalid".to_owned(),
        })
    }
}

struct UnusedContextDatasetBlobBackend;

impl ArtifactBlobBackend for UnusedContextDatasetBlobBackend {
    async fn delete_generation(
        &self,
        _request: DeleteArtifactBlobGeneration,
    ) -> Result<ArtifactBlobDeletionEvidence, ArtifactBackendFailure> {
        Err(ArtifactBackendFailure {
            retryable: false,
            reason_class: "context_dataset_blob_delete_unused".to_owned(),
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn stage_context_dataset_artifact(
    pool: &PgPool,
    repository: &PgRepository,
    tenant_id: &ResourceId,
    producer_job_id: &ResourceId,
    producer_fence: &JobFence,
    allocation: &ContextDatasetArtifactPreallocation,
    media_type: &str,
    descriptor_bytes: Vec<u8>,
    suffix: u16,
) -> StageWorkloadArtifactRequest {
    let request = StageWorkloadArtifactRequest {
        schema_version: 1,
        tenant_id: tenant_id.clone(),
        producer_job_id: producer_job_id.clone(),
        producer_fence: producer_fence.clone(),
        verification_job_id: allocation.verification_job_id.clone(),
        artifact_id: allocation.artifact_id.clone(),
        blob_id: allocation.blob_id.clone(),
        descriptor_digest: raw_digest(&descriptor_bytes),
        descriptor_bytes,
        media_type: media_type.to_owned(),
    };
    let WorkloadArtifactStagePreflight::Authorized(authority) = repository
        .authorize_workload_artifact_stage(&request)
        .await
        .unwrap()
    else {
        panic!("first Context Dataset Artifact stage must require storage evidence");
    };
    assert_eq!(authority.caller, ArtifactWorkloadAudience::ContextWorker);
    let staged_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    assert!(matches!(
        repository
            .stage_workload_artifact(StageWorkloadArtifact {
                schema_version: 1,
                tenant_id: tenant_id.clone(),
                caller: ArtifactWorkloadAudience::ContextWorker,
                producer_job_id: producer_job_id.clone(),
                producer_fence: producer_fence.clone(),
                verification_job_id: request.verification_job_id.clone(),
                artifact_id: request.artifact_id.clone(),
                blob_id: request.blob_id.clone(),
                content_digest: request.descriptor_digest.clone(),
                size_bytes: u64::try_from(request.descriptor_bytes.len()).unwrap(),
                media_type: request.media_type.clone(),
                storage_backend: "s3".to_owned(),
                storage_binding_digest: authority.write_storage_binding_digest,
                object_reference_ciphertext: vec![0x6b; 48],
                object_generation: format!("context-dataset-generation-{suffix}"),
                key_id: "context-dataset-key".to_owned(),
                encryption_domain_id: authority.encryption_domain_id,
                backend_evidence_digest: named_digest(&format!("context-dataset-stage-{suffix}")),
                staged_at,
            })
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    request
}

async fn scan_context_dataset_artifacts(
    pool: &PgPool,
    repository: &PgRepository,
    tenant_id: &ResourceId,
    requests: &[StageWorkloadArtifactRequest],
    suffix: u16,
) {
    scan_context_dataset_artifacts_with_dispositions(
        pool,
        repository,
        tenant_id,
        requests,
        suffix,
        &vec![ArtifactScanDisposition::Verified; requests.len()],
    )
    .await;
}

async fn scan_context_dataset_artifacts_with_dispositions(
    pool: &PgPool,
    repository: &PgRepository,
    tenant_id: &ResourceId,
    requests: &[StageWorkloadArtifactRequest],
    suffix: u16,
    dispositions: &[ArtifactScanDisposition],
) {
    assert_eq!(requests.len(), dispositions.len());
    let worker_id = id(ResourceKind::WorkerProcessGeneration, suffix);
    let mut claims = repository
        .claim_artifact_jobs(ClaimArtifactJobs {
            role: ArtifactWorkerRole::DataWorker,
            worker_id: worker_id.clone(),
            limit: u16::try_from(requests.len()).unwrap(),
            lease_milliseconds: 60_000,
            lease_token_digests: (0..requests.len())
                .map(|index| named_digest(&format!("context-dataset-scan-{suffix}-{index}")))
                .collect(),
        })
        .await
        .unwrap();
    assert_eq!(claims.len(), requests.len());
    claims.sort_by(|left, right| left.job_id.cmp(&right.job_id));
    for (index, claim) in claims.into_iter().enumerate() {
        let request = requests
            .iter()
            .find(|request| request.verification_job_id.to_string() == claim.job_id)
            .unwrap();
        let token: Sha256Digest = claim.lease_token_digest.as_ref().unwrap().parse().unwrap();
        let started = repository
            .start_job(RepositoryJobFence {
                tenant_id: tenant_id.to_string(),
                job_id: claim.job_id,
                worker_id: worker_id.clone(),
                lease_epoch: claim.lease_epoch,
                expected_job_version: claim.version,
                lease_token_digest: token.clone(),
            })
            .await
            .unwrap();
        let execution = repository
            .load_started_artifact_execution(
                ArtifactWorkerRole::DataWorker,
                tenant_id.clone(),
                request.verification_job_id.clone(),
                JobFence {
                    expected_version: u64::try_from(started.version).unwrap(),
                    worker_process_generation_id: worker_id.clone(),
                    lease_generation: u64::try_from(started.lease_epoch).unwrap(),
                    token_digest: token,
                },
                ArtifactExecutionSlot {
                    receipt_id: id(
                        ResourceKind::Receipt,
                        suffix + 1 + u16::try_from(index * 4).unwrap(),
                    ),
                    event_id: id(
                        ResourceKind::Event,
                        suffix + 2 + u16::try_from(index * 4).unwrap(),
                    ),
                    outbox_id: id(
                        ResourceKind::OutboxEvent,
                        suffix + 3 + u16::try_from(index * 4).unwrap(),
                    ),
                    duplicate_blob_cleanup_job_id: id(
                        ResourceKind::Job,
                        suffix + 4 + u16::try_from(index * 4).unwrap(),
                    ),
                    receipt_expires_at: Utc::now() + Duration::minutes(10),
                },
            )
            .await
            .unwrap();
        let StartedArtifactExecution::Scan(execution) = execution else {
            panic!("Context Dataset verification must start as an Artifact scan");
        };
        let observed_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(pool)
            .await
            .unwrap();
        let service = ArtifactWorkerService::new(
            ContextDatasetArtifactScanner {
                content_digest: request.descriptor_digest.clone(),
                size_bytes: u64::try_from(request.descriptor_bytes.len()).unwrap(),
                media_type: request.media_type.clone(),
                observed_at,
                disposition: dispositions[index],
            },
            UnusedContextDatasetBlobBackend,
            repository.clone(),
        );
        assert!(matches!(
            service.execute_scan(execution, observed_at).await.unwrap(),
            CommandOutcome::Applied(_)
        ));
    }
}

fn closed_object_schema(property: &str) -> ClosedJsonSchema {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            property: {
                "description": "Bounded fixture field.",
                "x-platform-classification": "internal",
                "type": "string",
                "minLength": 1,
                "maxLength": 4096,
                "x-platform-max-bytes": 16384
            }
        },
        "required": [property],
        "additionalProperties": false
    }))
    .unwrap()
}

fn agent_schema() -> ClosedJsonSchema {
    closed_object_schema("question")
}

fn query_schema_digest() -> Sha256Digest {
    agent_schema().canonical_digest
}

fn readonly_sql_plan_schema() -> ClosedJsonSchema {
    let properties = [
        "schema_version",
        "catalog_context_query_id",
        "catalog_observation_id",
        "catalog_observation_digest",
        "catalog_projection_digest",
        "execution",
        "from",
        "joins",
        "projections",
        "predicates",
        "group_by",
        "order_by",
        "parameters",
        "limit",
        "offset",
        "generated_sql_digest",
        "validation_evidence_digest",
        "canonical_digest",
    ];
    let property_schemas = properties
        .iter()
        .map(|property| {
            (
                (*property).to_owned(),
                json!({
                    "description": "Field in the closed, typed read-only SQL plan.",
                    "x-platform-classification": "internal"
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": property_schemas,
        "required": properties,
        "additionalProperties": false
    }))
    .unwrap()
}

fn version(kind: ResourceKind, suffix: u16) -> ExactVersionRef {
    ExactVersionRef::new(id(kind, suffix), named_digest(&format!("version-{suffix}"))).unwrap()
}

fn policy_binding(revision: ExactVersionRef, suffix: u16) -> ExactPolicyBinding {
    ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(
            id(ResourceKind::PolicyDeployment, suffix),
            named_digest(&format!("policy-deployment-{suffix}")),
        )
        .unwrap(),
        revision,
    }
}

fn artifact(suffix: u16) -> ArtifactRef {
    ArtifactRef::new(
        id(ResourceKind::Artifact, suffix),
        named_digest(&format!("artifact-{suffix}")),
        16,
        "application/json",
        DataClassification::Internal,
        Some(format!("context-{suffix}.json")),
    )
    .unwrap()
}

fn authoring(suffix: u16) -> AuthoringPackage {
    AuthoringPackage {
        artifact: artifact(suffix),
        manifest_digest: named_digest(&format!("manifest-{suffix}")),
    }
}

fn audit(
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    base: u16,
    operation: &str,
) -> CommandAudit {
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: tenant_id.clone(),
        principal_id: principal_id.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: named_digest(&format!("idempotency-{operation}-{base}")),
        request_digest: named_digest(&format!("request-{operation}-{base}")),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
}

fn worker_audit(
    tenant_id: &ResourceId,
    worker_id: &ResourceId,
    base: u16,
    operation: &str,
) -> ContextWorkerAudit {
    ContextWorkerAudit {
        tenant_id: tenant_id.clone(),
        worker_process_generation_id: worker_id.clone(),
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: named_digest(&format!("idempotency-{operation}-{base}")),
        request_digest: named_digest(&format!("request-{operation}-{base}")),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
}

fn signal_audit(tenant_id: &ResourceId, base: u16, operation: &str) -> ContextSignalAudit {
    ContextSignalAudit {
        tenant_id: tenant_id.clone(),
        receipt_id: id(ResourceKind::Receipt, base),
        event_id: id(ResourceKind::Event, base + 1),
        outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        idempotency_key_digest: named_digest(&format!("idempotency-{operation}-{base}")),
        request_digest: named_digest(&format!("request-{operation}-{base}")),
        receipt_expires_at: Utc::now() + Duration::hours(2),
    }
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

fn validation() -> ValidationSummary {
    ValidationSummary {
        validator_digest: named_digest("validator"),
        validated_draft_digest: named_digest("validated-draft"),
        dependency_closure_digest: named_digest("dependency-closure"),
        security_evidence_digest: named_digest("security-evidence"),
        warnings: vec![],
    }
}

async fn execute_create(
    repository: &PgRepository,
    command: CreateContextQuery,
) -> Result<CommandOutcome<insight_platform_context::ContextQueryRecord>, RepositoryError> {
    let mut transaction = repository.begin_context_query_transaction().await?;
    match transaction.create_context_query(command).await {
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
    command: PrepareContextDispatch,
) -> Result<CommandOutcome<PreparedContextExecution>, RepositoryError> {
    let mut transaction = repository.begin_context_query_transaction().await?;
    match transaction.prepare_context_dispatch(command).await {
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
    command: CommitContextOutcome,
) -> Result<CommandOutcome<PreparedContextExecution>, RepositoryError> {
    let mut transaction = repository.begin_context_query_transaction().await?;
    match transaction.commit_context_outcome(command).await {
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
    command: WakeContextDispatch,
) -> Result<CommandOutcome<PreparedContextExecution>, RepositoryError> {
    let mut transaction = repository.begin_context_query_transaction().await?;
    match transaction.wake_context_dispatch(command).await {
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

async fn execute_admit(
    repository: &PgRepository,
    command: AdmitCapabilityInvocation,
) -> Result<CommandOutcome<insight_platform_invocations::CapabilityInvocationRecord>, RepositoryError>
{
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
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT statement_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let blob_id = id(ResourceKind::InternalBlob, suffix);
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest, security_domain_digest,
            object_reference_ciphertext, object_generation, key_id, encryption_domain_id,
            content_digest, size_bytes, state, version, verified_at, created_at, updated_at
        ) VALUES ($1, $2, 'fixture-context', $3, $4, $5, 'generation-1', 'fixture-key', $6,
                  $7, $8, 'verified', 1, $9, $9, $9)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(blob_id.to_string())
    .bind(named_digest("artifact-storage").to_string())
    .bind(named_digest("artifact-security-domain").to_string())
    .bind(vec![1_u8, 2, 3])
    .bind(id(ResourceKind::EncryptionDomain, suffix).to_string())
    .bind(artifact.content_digest().to_string())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(now)
    .execute(pool)
    .await
    .unwrap();
    let metadata = TypedPayload::new(1, &json!({"fixture": "context-conformance"})).unwrap();
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
    run_id: ResourceId,
    node_id: ResourceId,
    text2sql_node_id: ResourceId,
    context_deployment: ExactDeploymentRef,
    context_worker_manifest_digest: Sha256Digest,
    interface_revision: ExactVersionRef,
    readonly_capability_deployment: ExactDeploymentRef,
    readonly_capability_interface: ExactVersionRef,
    readonly_input_schema_digest: Sha256Digest,
    catalog_projection_digest: Sha256Digest,
    database_identity_digest: Sha256Digest,
    invocation_policies: Vec<ExactVersionRef>,
    score_domain_digest: Sha256Digest,
    deadline: DateTime<Utc>,
    runtime_plan: RuntimePlan,
}

fn context_runtime_plan(interface_contract_digest: Sha256Digest) -> RuntimePlan {
    let start = PlanNodeKey::new("start".to_owned()).unwrap();
    let catalog = PlanNodeKey::new("catalog".to_owned()).unwrap();
    let finish = PlanNodeKey::new("finish".to_owned()).unwrap();
    let request = ExactDataPortRef::RunInput {
        schema_digest: query_schema_digest(),
    };
    let result = ExactDataPortRef::NodeOutput {
        producer_node_id: catalog.clone(),
        port_id: DataPortKey::new("items".to_owned()).unwrap(),
        schema_digest: named_digest("observation-schema"),
    };
    let plan = RuntimePlan {
        plan_version: 5,
        interface_contract_digest,
        entry_node_id: start.clone(),
        dependency_slots: BTreeMap::from([(
            "catalog".to_owned(),
            RuntimeDependencySlot {
                kind: RuntimeDependencyKind::Context,
                requirement_digest: named_digest("slot-requirement"),
            },
        )]),
        nodes: BTreeMap::from([
            (
                start,
                RuntimeNode::Start {
                    next: catalog.clone(),
                },
            ),
            (
                catalog,
                RuntimeNode::ContextQuery {
                    context_slot_id: "catalog".to_owned(),
                    request,
                    result,
                    maximum_items: 20,
                    resume: finish.clone(),
                },
            ),
            (
                finish,
                RuntimeNode::Return {
                    value: ExactDataPortRef::RunInput {
                        schema_digest: agent_schema().canonical_digest,
                    },
                },
            ),
        ]),
    };
    plan.validate(PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap())
        .unwrap();
    plan
}

async fn seed_policy_versions(
    pool: &PgPool,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    policy_resource: &ResourceId,
    policies: &[(ExactVersionRef, PolicyKind)],
) {
    for (index, (exact, policy_kind)) in policies.iter().enumerate() {
        let selection =
            (*policy_kind == PolicyKind::Selection).then_some(CandidateSelectionPolicyDocument {
                schema_version: 1,
                mode: CandidateSelectionMode::OnlyCandidate,
                route_schema_digest: None,
            });
        let rules_digest = selection.as_ref().map_or_else(
            || named_digest(&format!("policy-rules-{index}")),
            |document| document.canonical_digest().unwrap(),
        );
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
                    authoring_package: authoring(0x90 + u16::try_from(index).unwrap()),
                    contract_digest: named_digest(&format!("policy-contract-{index}")),
                    dependency_versions: vec![],
                    policy_versions: vec![],
                    policy_kind: *policy_kind,
                    rules_digest,
                    selection,
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
                validation: validation(),
            },
        )
        .await;
    }
}

async fn seed_fixture(pool: &PgPool, repository: &PgRepository) -> Fixture {
    seed_fixture_with_backend(pool, repository, ContextFixtureBackend::SqlCatalog).await
}

async fn seed_fixture_with_native_adapter(
    pool: &PgPool,
    repository: &PgRepository,
    native_adapter: Option<(Sha256Digest, Sha256Digest)>,
) -> Fixture {
    let backend = native_adapter.map_or(
        ContextFixtureBackend::SqlCatalog,
        |(installed_adapter_digest, adapter_contract_digest)| {
            ContextFixtureBackend::NativeCatalog {
                installed_adapter_digest,
                adapter_contract_digest,
            }
        },
    );
    seed_fixture_with_backend(pool, repository, backend).await
}

#[derive(Clone)]
enum ContextFixtureBackend {
    SqlCatalog,
    NativeCatalog {
        installed_adapter_digest: Sha256Digest,
        adapter_contract_digest: Sha256Digest,
    },
    RemoteSearch {
        endpoint: CanonicalHttpEndpoint,
        protocol_contract_digest: Sha256Digest,
        result_mapping_digest: Sha256Digest,
        installed_adapter_digest: Sha256Digest,
    },
}

async fn seed_fixture_with_backend(
    pool: &PgPool,
    repository: &PgRepository,
    backend_fixture: ContextFixtureBackend,
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
            authentication_authority_digest: named_digest("authentication-authority"),
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
                    Permission::ContextQuery,
                    Permission::ContextWrite,
                    Permission::RuntimeControl,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();

    let policy_resource = id(ResourceKind::Policy, 0x10);
    let agent_resource = id(ResourceKind::Agent, 0x11);
    let interface_resource = id(ResourceKind::ContextSourceInterface, 0x12);
    let implementation_resource = id(ResourceKind::ContextSourceImplementation, 0x13);
    let capability_resource = id(ResourceKind::CapabilityInterface, 0x14);
    let capability_implementation_resource = id(ResourceKind::CapabilityImplementation, 0x15);
    let retention_policy_resource = id(ResourceKind::Policy, 0x16);
    let artifact_io_policy_resource = id(ResourceKind::Policy, 0x17);
    for (resource, kind) in [
        (&policy_resource, RegistryResourceKind::Policy),
        (&agent_resource, RegistryResourceKind::Agent),
        (
            &interface_resource,
            RegistryResourceKind::ContextSourceInterface,
        ),
        (
            &implementation_resource,
            RegistryResourceKind::ContextSourceImplementation,
        ),
        (
            &capability_resource,
            RegistryResourceKind::CapabilityInterface,
        ),
        (
            &capability_implementation_resource,
            RegistryResourceKind::CapabilityImplementation,
        ),
        (&retention_policy_resource, RegistryResourceKind::Policy),
        (&artifact_io_policy_resource, RegistryResourceKind::Policy),
    ] {
        insert_resource(pool, &tenant_id, resource, kind, &principal_id).await;
    }

    let authorization_policy = version(ResourceKind::PolicyRevision, 0x20);
    let ranking_policy = version(ResourceKind::PolicyRevision, 0x21);
    let parser_policy = version(ResourceKind::PolicyRevision, 0x22);
    let data_policy = version(ResourceKind::PolicyRevision, 0x23);
    let entitlement_policy = version(ResourceKind::PolicyRevision, 0x24);
    let cache_policy = version(ResourceKind::PolicyRevision, 0x25);
    let execution_profile = version(ResourceKind::PolicyRevision, 0x26);
    let invocation_policy = version(ResourceKind::PolicyRevision, 0x27);
    let chunker_policy = version(ResourceKind::PolicyRevision, 0x28);
    let network_policy = version(ResourceKind::PolicyRevision, 0x29);
    let tls_policy = version(ResourceKind::PolicyRevision, 0x2a);
    let trust_policy = version(ResourceKind::PolicyRevision, 0x2b);
    let scheduling_policy = version(ResourceKind::PolicyRevision, 0x2c);
    let mut policies = vec![
        (authorization_policy.clone(), PolicyKind::Authorization),
        (ranking_policy.clone(), PolicyKind::Ranking),
        (parser_policy.clone(), PolicyKind::Parser),
        (data_policy.clone(), PolicyKind::DataFlow),
        (entitlement_policy.clone(), PolicyKind::Authorization),
        (cache_policy.clone(), PolicyKind::Authorization),
        (execution_profile.clone(), PolicyKind::Execution),
        (invocation_policy.clone(), PolicyKind::Selection),
        (chunker_policy.clone(), PolicyKind::Chunker),
    ];
    if matches!(&backend_fixture, ContextFixtureBackend::RemoteSearch { .. }) {
        policies.extend([
            (network_policy.clone(), PolicyKind::Network),
            (tls_policy.clone(), PolicyKind::Tls),
            (trust_policy.clone(), PolicyKind::Trust),
        ]);
    }
    seed_policy_versions(pool, &tenant_id, &principal_id, &policy_resource, &policies).await;
    insert_ready_artifact(
        pool,
        &tenant_id,
        &principal_id,
        &authorization_policy.revision_id,
        &artifact(0xa2),
        0xb0,
    )
    .await;
    let scheduling = SchedulingPolicyDocument {
        version: 1,
        weight: 1,
        burst: 4,
        aging_rounds: 2,
    };
    insert_version(
        pool,
        &tenant_id,
        &policy_resource,
        RegistryResourceKind::Policy,
        &scheduling_policy,
        20,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                authoring_package: authoring(0xaf),
                contract_digest: named_digest("scheduling-policy-contract"),
                dependency_versions: vec![],
                policy_versions: vec![],
                policy_kind: PolicyKind::Scheduling,
                rules_digest: scheduling.canonical_digest().unwrap(),
                selection: None,
                scheduling: Some(scheduling),
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
            validation: validation(),
        },
    )
    .await;
    let scheduling_deployment_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: scheduling_policy.clone(),
            applicability_digest: named_digest("scheduling-applicability"),
            qualification_evidence: artifact(0xa2),
        }),
    )
    .unwrap();
    let scheduling_deployment_id = id(ResourceKind::PolicyDeployment, 0x79);
    insert_deployment(
        pool,
        &tenant_id,
        &scheduling_deployment_id,
        &policy_resource,
        &scheduling_policy.revision_id,
        &principal_id,
        &scheduling_deployment_payload,
    )
    .await;
    sqlx::query(
        "UPDATE insight_platform.resources SET active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(policy_resource.to_string())
    .bind(scheduling_deployment_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let scheduling_deployment = ExactDeploymentRef::new(
        scheduling_deployment_id,
        scheduling_deployment_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let retention = ArtifactRetentionPolicy {
        version: 1,
        minimum_retention_seconds: 3_600,
        gc_grace_seconds: 3_600,
        tombstone_retention_seconds: 86_400,
        retain_provenance_sources: true,
        delete_requires_approval: false,
    };
    let retention_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x2d),
        retention.canonical_digest().unwrap(),
    )
    .unwrap();
    insert_version(
        pool,
        &tenant_id,
        &retention_policy_resource,
        RegistryResourceKind::Policy,
        &retention_policy,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                authoring_package: authoring(0xa6),
                contract_digest: named_digest("retention-contract"),
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
            validation: validation(),
        },
    )
    .await;
    let artifact_io = SandboxArtifactIoPolicyDocument {
        schema_version: 3,
        allowed_input_media_types: vec![],
        allowed_output_media_types: vec![
            insight_platform_context::CONTEXT_DATASET_INDEX_MANIFEST_MEDIA_TYPE.to_owned(),
            insight_platform_context::CONTEXT_DATASET_VALIDATION_EVIDENCE_MEDIA_TYPE.to_owned(),
        ],
        maximum_input_artifacts: 0,
        maximum_output_artifacts: 2,
        scanner_contract_digest: named_digest("context-dataset-scanner"),
        verification_evidence_ttl_milliseconds: 60_000,
        verification_retry_backoff_milliseconds: 1_000,
        write_storage_binding_digest: named_digest("context-dataset-storage"),
        encryption_domain_id: id(ResourceKind::EncryptionDomain, 0x2e),
        deny_symlink: true,
        deny_hardlink: true,
        deny_device: true,
        deny_fifo: true,
        deny_socket: true,
        deny_sparse_file: true,
        archive_expansion_disabled: true,
    };
    let artifact_io_policy = ExactVersionRef::new(
        id(ResourceKind::PolicyRevision, 0x2f),
        artifact_io.canonical_digest().unwrap(),
    )
    .unwrap();
    insert_version(
        pool,
        &tenant_id,
        &artifact_io_policy_resource,
        RegistryResourceKind::Policy,
        &artifact_io_policy,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::Policy(Box::new(PolicyResourceSpec {
                authoring_package: authoring(0xa7),
                contract_digest: named_digest("artifact-io-contract"),
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
            validation: validation(),
        },
    )
    .await;
    let retention_deployment_id = id(ResourceKind::PolicyDeployment, 0x7a);
    let retention_deployment_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: retention_policy.clone(),
            applicability_digest: named_digest("retention-applicability"),
            qualification_evidence: artifact(0xa2),
        }),
    )
    .unwrap();
    insert_deployment(
        pool,
        &tenant_id,
        &retention_deployment_id,
        &retention_policy_resource,
        &retention_policy.revision_id,
        &principal_id,
        &retention_deployment_payload,
    )
    .await;
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2")
        .bind(tenant_id.to_string())
        .bind(retention_policy_resource.to_string())
        .bind(retention_deployment_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    let retention_deployment = ExactDeploymentRef::new(
        retention_deployment_id,
        retention_deployment_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let artifact_io_deployment_id = id(ResourceKind::PolicyDeployment, 0x7b);
    let artifact_io_deployment_payload = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: artifact_io_policy.clone(),
            applicability_digest: named_digest("artifact-io-applicability"),
            qualification_evidence: artifact(0xa2),
        }),
    )
    .unwrap();
    insert_deployment(
        pool,
        &tenant_id,
        &artifact_io_deployment_id,
        &artifact_io_policy_resource,
        &artifact_io_policy.revision_id,
        &principal_id,
        &artifact_io_deployment_payload,
    )
    .await;
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id = $3 WHERE tenant_id = $1 AND resource_id = $2")
        .bind(tenant_id.to_string())
        .bind(artifact_io_policy_resource.to_string())
        .bind(artifact_io_deployment_id.to_string())
        .execute(pool)
        .await
        .unwrap();
    let artifact_io_deployment = ExactDeploymentRef::new(
        artifact_io_deployment_id,
        artifact_io_deployment_payload.digest.parse().unwrap(),
    )
    .unwrap();
    let tenant_config = TenantConfig {
        scheduling_policy: Some(scheduling_deployment),
        artifact_retention_policy: Some(retention_deployment),
        artifact_io_policy: Some(artifact_io_deployment),
    };
    let tenant_config_payload = TypedPayload::new(1, &tenant_config).unwrap();
    sqlx::query(
        "UPDATE insight_platform.tenants SET version = version + 1, config_schema_version = $2, config = $3, config_digest = $4, updated_at = clock_timestamp() WHERE tenant_id = $1",
    )
    .bind(tenant_id.to_string())
    .bind(tenant_config_payload.schema_version)
    .bind(&tenant_config_payload.value)
    .bind(&tenant_config_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: tenant_id.to_string(),
            quota_account_id: id(ResourceKind::QuotaAccount, 0x7c).to_string(),
            scope_kind: "tenant".to_owned(),
            scope_id: tenant_id.to_string(),
            work_class: WorkClass::Artifact.as_str().to_owned(),
            metric: "artifact.staging_bytes".to_owned(),
            limit_value: 8_388_608,
            payload: TypedPayload::new(1, &json!({"fixture": "context-dataset-artifact"})).unwrap(),
        })
        .await
        .unwrap();

    let interface_revision = version(ResourceKind::ContextSourceInterfaceRevision, 0x30);
    let implementation_revision = version(ResourceKind::ContextSourceImplementationRevision, 0x31);
    let query_schema_digest = query_schema_digest();
    let item_schema_digest = named_digest("item-schema");
    let score_domain_digest = named_digest("score-domain");
    let interface = ContextInterfaceResourceSpec {
        authoring_package: authoring(0xa0),
        contract_digest: named_digest("interface-contract"),
        dependency_versions: vec![],
        policy_versions: vec![entitlement_policy.clone(), cache_policy.clone()],
        query_schema_digest: query_schema_digest.clone(),
        filter_schema_digest: named_digest("filter-schema"),
        item_schema_digest,
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
            score_domain_digest: score_domain_digest.clone(),
            reranker_contract_digest: None,
            maximum_candidates: 1_000,
        },
        data_policy: ContextDataPolicyContract {
            maximum_classification: DataClassification::Confidential,
            allowed_regions: vec!["cn-east-1".parse::<DataRegion>().unwrap()],
            entitlement_policy,
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
    };
    insert_version(
        pool,
        &tenant_id,
        &interface_resource,
        RegistryResourceKind::ContextSourceInterface,
        &interface_revision,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::ContextSourceInterface(interface),
            validation: validation(),
        },
    )
    .await;

    let catalog_projection_digest = named_digest("catalog-projection");
    let database_identity_digest = named_digest("database");
    let backend_kind = match &backend_fixture {
        ContextFixtureBackend::SqlCatalog => ContextBackendKind::SqlCatalog,
        ContextFixtureBackend::NativeCatalog { .. } => ContextBackendKind::NativeCatalog,
        ContextFixtureBackend::RemoteSearch { .. } => ContextBackendKind::RemoteSearch,
    };
    let backend_contract = match &backend_fixture {
        ContextFixtureBackend::SqlCatalog => ContextBackendContract::SqlCatalog {
            dialect: "postgres".to_owned(),
            catalog_projection_digest: catalog_projection_digest.clone(),
        },
        ContextFixtureBackend::NativeCatalog {
            adapter_contract_digest,
            ..
        } => ContextBackendContract::NativeCatalog {
            adapter_contract_digest: adapter_contract_digest.clone(),
        },
        ContextFixtureBackend::RemoteSearch {
            protocol_contract_digest,
            result_mapping_digest,
            ..
        } => ContextBackendContract::RemoteSearch {
            protocol_contract_digest: protocol_contract_digest.clone(),
            result_mapping_digest: result_mapping_digest.clone(),
        },
    };
    let implementation = ContextImplementationResourceSpec {
        authoring_package: authoring(0xa1),
        contract_digest: named_digest("implementation-contract"),
        dependency_versions: vec![interface_revision.clone()],
        policy_versions: vec![],
        interface_revision: interface_revision.clone(),
        backend_kind,
        contract: ContextImplementationContract {
            backend: backend_contract,
            credential_requirements: vec![],
            limits: ContextBackendLimits {
                maximum_request_bytes: 65_536,
                maximum_response_bytes: 1_048_576,
                maximum_candidates: 1_000,
                maximum_remote_state_bytes: 1_024,
                maximum_poll_count: 4,
                total_timeout_milliseconds: 30_000,
            },
        },
    };
    insert_version(
        pool,
        &tenant_id,
        &implementation_resource,
        RegistryResourceKind::ContextSourceImplementation,
        &implementation_revision,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::ContextSourceImplementation(implementation),
            validation: validation(),
        },
    )
    .await;

    let context_worker_manifest_digest = match &backend_fixture {
        ContextFixtureBackend::SqlCatalog => named_digest("context-worker-manifest"),
        ContextFixtureBackend::NativeCatalog {
            installed_adapter_digest,
            ..
        }
        | ContextFixtureBackend::RemoteSearch {
            installed_adapter_digest,
            ..
        } => native_context_worker_manifest(installed_adapter_digest.clone())
            .canonical_digest()
            .unwrap(),
    };
    let backend_binding = match backend_fixture {
        ContextFixtureBackend::SqlCatalog => ContextBackendBinding::SqlCatalog {
            database_identity_digest: database_identity_digest.clone(),
            dialect: "postgres".to_owned(),
            catalog_scope_digest: named_digest("catalog-scope"),
        },
        ContextFixtureBackend::NativeCatalog {
            installed_adapter_digest,
            ..
        } => ContextBackendBinding::NativeCatalog {
            installed_adapter_digest,
        },
        ContextFixtureBackend::RemoteSearch { endpoint, .. } => {
            ContextBackendBinding::RemoteSearch {
                endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
                endpoint,
                region: "cn-east-1".parse().unwrap(),
            }
        }
    };
    let remote_search = matches!(&backend_binding, ContextBackendBinding::RemoteSearch { .. });
    let context_closure = ContextDeploymentClosure {
        implementation: implementation_revision,
        interface: interface_revision.clone(),
        required_worker_manifest_digest: context_worker_manifest_digest.clone(),
        backend: backend_binding,
        secret_bindings: vec![],
        network_policy: remote_search.then_some(network_policy),
        tls_policy: remote_search.then_some(tls_policy),
        trust_policy: remote_search.then_some(trust_policy),
        parser_policy: parser_policy.clone(),
        chunker_policy,
        embedding_model_deployment: None,
        ranking_policy: ranking_policy.clone(),
        data_policy: data_policy.clone(),
        conformance_evidence: artifact(0xa2),
    };
    let context_payload = TypedPayload::new(
        1,
        &DeploymentClosure::ContextSourceInterface(context_closure),
    )
    .unwrap();
    let context_deployment_id = id(ResourceKind::ContextDeployment, 0x32);
    insert_deployment(
        pool,
        &tenant_id,
        &context_deployment_id,
        &interface_resource,
        &interface_revision.revision_id,
        &principal_id,
        &context_payload,
    )
    .await;
    let context_deployment = ExactDeploymentRef::new(
        context_deployment_id,
        context_payload.digest.parse().unwrap(),
    )
    .unwrap();

    let readonly_capability_interface = version(ResourceKind::CapabilityInterfaceRevision, 0x33);
    let readonly_capability_implementation =
        version(ResourceKind::CapabilityImplementationRevision, 0x34);
    let readonly_input_schema = readonly_sql_plan_schema();
    let readonly_input_schema_digest = readonly_input_schema.canonical_digest.clone();
    insert_version(
        pool,
        &tenant_id,
        &capability_resource,
        RegistryResourceKind::CapabilityInterface,
        &readonly_capability_interface,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::CapabilityInterface(CapabilityInterfaceResourceSpec {
                authoring_package: authoring(0xa3),
                contract_digest: named_digest("readonly-interface-contract"),
                dependency_versions: vec![],
                policy_versions: vec![invocation_policy.clone()],
                qualified_name: READONLY_DATABASE_CAPABILITY.parse().unwrap(),
                input_schema: readonly_input_schema,
                output_schema: closed_object_schema("rows"),
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
                    maximum_output_bytes: 16 * 1_048_576,
                    maximum_artifacts: 0,
                    maximum_execution_milliseconds: 60_000,
                },
                effect: Effect::ReadOnly,
                idempotency: CapabilityIdempotencyKind::CallerKey,
                cancellation: CapabilityCancellationKind::Confirmed,
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
    let native_contract = CapabilityBackendContract::Native(NativeCapabilityContract {
        adapter_id: "builtin.database_readonly".to_owned(),
        adapter_version: "1.0.0".to_owned(),
        module_digest: named_digest("readonly-module"),
        entrypoint_id: "database.query.readonly".to_owned(),
        worker_protocol_version: WORKER_PROTOCOL_VERSION,
    });
    let native_contract_digest = native_contract.canonical_digest().unwrap();
    insert_version(
        pool,
        &tenant_id,
        &capability_implementation_resource,
        RegistryResourceKind::CapabilityImplementation,
        &readonly_capability_implementation,
        1,
        &principal_id,
        PublishedVersionPayload {
            document: ResourceDocument::CapabilityImplementation(
                CapabilityImplementationResourceSpec {
                    authoring_package: authoring(0xa4),
                    contract_digest: named_digest("readonly-implementation-contract"),
                    dependency_versions: vec![readonly_capability_interface.clone()],
                    policy_versions: vec![invocation_policy.clone()],
                    interface_revision: readonly_capability_interface.clone(),
                    backend_kind: CapabilityBackendKind::Native,
                    backend_contract: native_contract,
                    backend_contract_digest: native_contract_digest,
                    credential_requirements: vec![],
                    backend_limits: CapabilityBackendLimits {
                        maximum_request_bytes: 65_536,
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
    let readonly_closure = CapabilityDeploymentClosure {
        implementation: readonly_capability_implementation,
        interface: readonly_capability_interface.clone(),
        backend: CapabilityBackendBinding::Native {
            worker_manifest_digest: named_digest("readonly-worker"),
            adapter_module_digest: named_digest("readonly-module"),
        },
        secret_bindings: vec![],
        policies: vec![invocation_policy.clone()],
        conformance_evidence: artifact(0xa2),
    };
    let readonly_payload =
        TypedPayload::new(1, &DeploymentClosure::CapabilityInterface(readonly_closure)).unwrap();
    let readonly_capability_deployment_id = id(ResourceKind::CapabilityDeployment, 0x35);
    insert_deployment(
        pool,
        &tenant_id,
        &readonly_capability_deployment_id,
        &capability_resource,
        &readonly_capability_interface.revision_id,
        &principal_id,
        &readonly_payload,
    )
    .await;
    let readonly_capability_deployment = ExactDeploymentRef::new(
        readonly_capability_deployment_id,
        readonly_payload.digest.parse().unwrap(),
    )
    .unwrap();

    let agent_interface = version(ResourceKind::AgentInterfaceRevision, 0x40);
    let runtime_plan = context_runtime_plan(named_digest("agent-contract"));
    let runtime_plan_digest = runtime_plan
        .canonical_digest(PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap())
        .unwrap();
    let agent_plan = ExactVersionRef::new(
        id(ResourceKind::AgentPlanRevision, 0x41),
        runtime_plan_digest.clone(),
    )
    .unwrap();
    let runtime_plan_bytes = canonical_json(&serde_json::to_value(&runtime_plan).unwrap()).unwrap();
    let agent_document = ResourceDocument::Agent(AgentResourceSpec {
        authoring_package: authoring(0xa5),
        contract_digest: named_digest("agent-contract"),
        dependency_versions: vec![],
        policy_versions: vec![authorization_policy.clone()],
        input_schema: agent_schema(),
        output_schema: agent_schema(),
        error_schema: agent_schema(),
        typed_plan_artifact_id: id(ResourceKind::Artifact, 0xa5),
        typed_plan_digest: runtime_plan_digest.clone(),
    });
    for (exact, revision_no) in [(&agent_interface, 1), (&agent_plan, 1)] {
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
        id(ResourceKind::Artifact, 0xa5),
        runtime_plan_digest,
        u64::try_from(runtime_plan_bytes.len()).unwrap(),
        "application/json",
        DataClassification::Internal,
        Some("context-runtime-plan.json".to_owned()),
    )
    .unwrap();
    insert_ready_artifact(
        pool,
        &tenant_id,
        &principal_id,
        &authorization_policy.revision_id,
        &typed_plan_artifact,
        0xb5,
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
    let agent_deployment_id = id(ResourceKind::AgentDeployment, 0x42);
    let binding = ContextBindingSnapshot::build(
        id(ResourceKind::ContextBinding, 0x43),
        agent_deployment_id.clone(),
        context_deployment.clone(),
        ContextConsistencyPolicy::ExternalObservation,
        vec!["customer_id".to_owned()],
        authorization_policy.clone(),
        ranking_policy.clone(),
    )
    .unwrap();
    let agent_closure = AgentDeploymentClosure {
        interface: agent_interface,
        plan: agent_plan.clone(),
        entry_node_id: "start".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
        slots: vec![
            FrozenSlotBinding {
                slot_id: "catalog".to_owned(),
                requirement_digest: named_digest("slot-requirement"),
                target: FrozenSlotTarget::Context {
                    binding: Box::new(binding),
                },
                binding_digest: named_digest("slot-binding"),
            },
            FrozenSlotBinding {
                slot_id: "readonly_sql".to_owned(),
                requirement_digest: named_digest("readonly-slot-requirement"),
                target: FrozenSlotTarget::Capability {
                    candidates: vec![readonly_capability_deployment.clone()],
                    selection_policy: policy_binding(invocation_policy.clone(), 0x72),
                    tool_alias: Some("database_query".to_owned()),
                },
                binding_digest: named_digest("readonly-slot-binding"),
            },
        ],
        policies: vec![
            policy_binding(authorization_policy, 0x73),
            policy_binding(ranking_policy, 0x74),
            policy_binding(parser_policy, 0x75),
            policy_binding(data_policy, 0x76),
            policy_binding(invocation_policy.clone(), 0x77),
        ],
        execution_profile: policy_binding(execution_profile, 0x78),
    };
    let agent_payload =
        TypedPayload::new(1, &DeploymentClosure::Agent(agent_closure.clone())).unwrap();
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
            id(ResourceKind::QuotaAccount, 0x50),
            "tenant",
            tenant_id.clone(),
            QuotaDimension::WorkClassConcurrentOperations,
            8,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x51),
            "context_deployment",
            context_deployment.deployment_id.clone(),
            QuotaDimension::ContextQueries,
            8,
        ),
        (
            id(ResourceKind::QuotaAccount, 0x52),
            "context_deployment",
            context_deployment.deployment_id.clone(),
            QuotaDimension::ContextResultBytes,
            8_388_608,
        ),
    ] {
        repository
            .create_quota_account(NewQuotaAccount {
                tenant_id: tenant_id.to_string(),
                quota_account_id: account_id.to_string(),
                scope_kind: scope_kind.to_owned(),
                scope_id: scope_id.to_string(),
                work_class: WorkClass::Context.as_str().to_owned(),
                metric: metric.as_str().to_owned(),
                limit_value,
                payload: TypedPayload::new(1, &json!({"fixture": "context"})).unwrap(),
            })
            .await
            .unwrap();
    }
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: tenant_id.to_string(),
            quota_account_id: id(ResourceKind::QuotaAccount, 0x53).to_string(),
            scope_kind: "tenant".to_owned(),
            scope_id: tenant_id.to_string(),
            work_class: WorkClass::Orchestration.as_str().to_owned(),
            metric: "concurrent_jobs".to_owned(),
            limit_value: 8,
            payload: TypedPayload::new(1, &json!({"fixture": "context-orchestration"})).unwrap(),
        })
        .await
        .unwrap();

    let run_id = id(ResourceKind::Run, 0x60);
    let scope_id = id(ResourceKind::ScopeInstance, 0x61);
    let node_id = id(ResourceKind::NodeExecution, 0x62);
    let text2sql_node_id = id(ResourceKind::NodeExecution, 0x64);
    let input_value_id = id(ResourceKind::RunValue, 0x63);
    let principal_snapshot = PrincipalSnapshot::build(
        tenant_id.clone(),
        principal_id.clone(),
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![
            Permission::CapabilityInvoke,
            Permission::ContextQuery,
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
        input_value_id.clone(),
    );
    let bindings_payload = TypedPayload::from_versioned(1, &run_bindings, 1_048_576).unwrap();
    let current_payload = TypedPayload::from_versioned(1, &current, 1_048_576).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.runs (
            tenant_id, run_id, root_run_id, agent_deployment_id, principal_id,
            state, version, active_work_count, bindings_schema_version, bindings, bindings_digest,
            current_schema_version, current_payload, current_payload_digest,
            deadline, started_at, created_at, updated_at, trace_id
        ) VALUES (
            $1, $2, $2, $3, $4, 'running', 1, 1, $5, $6, $7,
            $8, $9, $10, $11, $12, $12, $12,
            '0123456789abcdef0123456789abcdef'
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
    let node_payload = TypedPayload::new(1, &json!({"fixture": "context_query"})).unwrap();
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
    for (child_node_id, plan_key, ordinal, node_kind) in [
        (&node_id, "catalog", 1_i32, "context_query"),
        (&text2sql_node_id, "readonly_sql", 2_i32, "capability_call"),
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
                $5, $6, $5, $7, 'running',
                1, 1, $8, $9, $10, $11, $12, $12, $12
            )
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(child_node_id.to_string())
        .bind(run_id.to_string())
        .bind(scope_id.to_string())
        .bind(plan_key)
        .bind(ordinal)
        .bind(node_kind)
        .bind(node_payload.schema_version)
        .bind(&node_payload.value)
        .bind(&node_payload.digest)
        .bind(deadline)
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
    }
    let input = json!({"question": "top customers"});
    let input_digest: Sha256Digest = canonical_digest(&input).unwrap().parse().unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value
        ) VALUES ($1, $2, $3, $4, 'context_query', 'internal', $5, $6, $7)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(input_value_id.to_string())
    .bind(run_id.to_string())
    .bind(node_id.to_string())
    .bind(query_schema_digest.to_string())
    .bind(input_digest.to_string())
    .bind(input)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.runs SET input_value_id = $3 WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .bind(input_value_id.to_string())
    .execute(pool)
    .await
    .unwrap();

    Fixture {
        tenant_id,
        other_tenant_id,
        principal_id,
        run_id,
        node_id,
        text2sql_node_id,
        context_deployment,
        context_worker_manifest_digest,
        interface_revision,
        readonly_capability_deployment,
        readonly_capability_interface,
        readonly_input_schema_digest,
        catalog_projection_digest,
        database_identity_digest,
        invocation_policies: run_bindings
            .policies
            .iter()
            .map(|binding| binding.revision.clone())
            .collect(),
        score_domain_digest,
        deadline,
        runtime_plan,
    }
}

fn create_command(fixture: &Fixture, base: u16) -> CreateContextQuery {
    let input_value = json!({"question": "top customers"});
    let input_digest: Sha256Digest = canonical_digest(&input_value).unwrap().parse().unwrap();
    CreateContextQuery {
        audit: audit(&fixture.tenant_id, &fixture.principal_id, base, "create"),
        context_query_id: id(ResourceKind::ContextQuery, base + 3),
        run_id: fixture.run_id.clone(),
        node_execution_id: fixture.node_id.clone(),
        expected_run_version: 1,
        expected_node_version: 1,
        slot_id: "catalog".to_owned(),
        request: ContextQueryRequest {
            schema_version: 1,
            input: ExactInvocationValueRef {
                schema_version: 1,
                value_id: id(ResourceKind::RunValue, 0x63),
                run_id: fixture.run_id.clone(),
                producing_node_id: Some(fixture.node_id.clone()),
                value_kind: "context_query".to_owned(),
                classification: DataClassification::Internal,
                schema_digest: query_schema_digest(),
                content_digest: input_digest,
                storage: InvocationValueStorage::Inline,
            },
            input_artifact_link_id: None,
            normalized_query_digest: named_digest("normalized-query"),
            normalized_filter_digest: named_digest("normalized-filter"),
            requested_projection: vec!["customer_id".to_owned()],
            query_bytes: 64,
            filter_bytes: 2,
            page_size: 20,
            page_ordinal: 0,
            cursor_digest: None,
        },
        requested_attempt_limit: 3,
        result_byte_ceiling: 1_048_576,
    }
}

fn digest_without_field<T: Serialize>(value: &T, field: &str) -> Sha256Digest {
    let mut value = serde_json::to_value(value).unwrap();
    value.as_object_mut().unwrap().remove(field).unwrap();
    canonical_digest(&value).unwrap().parse().unwrap()
}

struct ClaimEvidence {
    claimed: ClaimedContextExecution,
    worker_id: ResourceId,
    lease_token: Sha256Digest,
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

async fn seed_running_context_orchestration(
    pool: &PgPool,
    _repository: &PgRepository,
    fixture: &Fixture,
) -> (RepositoryJobFence, DeferOrchestrationToContextQuery) {
    let input_value = json!({"question": "top customers"});
    let input_digest: Sha256Digest = canonical_digest(&input_value).unwrap().parse().unwrap();
    let input_port = ExactDataPortRef::RunInput {
        schema_digest: query_schema_digest(),
    };
    let environment = ScopeDataEnvironmentSnapshot::build(
        BTreeMap::from([(
            input_port.clone(),
            ExactRunValueRef {
                value_id: id(ResourceKind::RunValue, 0x63),
                schema_digest: query_schema_digest(),
                content_digest: input_digest.clone(),
            },
        )]),
        ScopeEnvironmentLimits::from_profile(&checked_in_hard_limit_profile()).unwrap(),
    )
    .unwrap();
    let scope_payload = TypedPayload::new(
        1,
        &json!({
            "root_run_id": fixture.run_id,
            "environment": environment,
        }),
    )
    .unwrap();
    let scope_id = id(ResourceKind::ScopeInstance, 0x61);
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET payload_schema_version = $3, payload = $4, payload_digest = $5
        WHERE tenant_id = $1 AND node_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(scope_id.to_string())
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
    let owner_node_id = id(ResourceKind::NodeExecution, 0x804);
    let owner_node_payload = TypedPayload::new(1, &json!({"fixture": "context-owner"})).unwrap();
    let owner_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
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
            'catalog', 3, 'catalog-owner', 'context_query', 'running',
            1, 1, $5, $6, $7, $8, $9, $9, $9
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(owner_node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(scope_id.to_string())
    .bind(owner_node_payload.schema_version)
    .bind(&owner_node_payload.value)
    .bind(&owner_node_payload.digest)
    .bind(fixture.deadline)
    .bind(owner_now)
    .execute(pool)
    .await
    .unwrap();

    let quota_account_id = id(ResourceKind::QuotaAccount, 0x53);
    sqlx::query(
        "UPDATE insight_platform.quota_accounts SET reserved_value = 1 WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(quota_account_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    let reservation_id = "ures_0198f1c9-32e4-75e1-a9e8-d95ca0f60801";
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id, entry_kind,
            reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, 'reserve', 1, 0, 1, $5)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::QuotaLedgerEntry, 0x801).to_string())
    .bind(quota_account_id.to_string())
    .bind(reservation_id)
    .bind(named_digest("context-owner-reserve").to_string())
    .execute(pool)
    .await
    .unwrap();

    let source_job_id = id(ResourceKind::Job, 0x802);
    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0x803);
    let lease_token_digest = named_digest("context-owner-lease");
    let job_payload = TypedPayload::new(
        1,
        &OrchestrationJobPayload {
            bindings_digest: sqlx::query_scalar::<_, String>(
                "SELECT bindings_digest FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
            )
            .bind(fixture.tenant_id.to_string())
            .bind(fixture.run_id.to_string())
            .fetch_one(pool)
            .await
            .unwrap()
            .parse()
            .unwrap(),
            node_execution_id: owner_node_id.clone(),
            root_scope_id: scope_id,
            retry_backoff_milliseconds: 100,
            wake_contract: None,
            convergence_failure: None,
            model_tool_continuation: None,
        },
    )
    .unwrap();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
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
    .bind(owner_node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(worker_id.to_string())
    .bind(lease_token_digest.to_string())
    .bind(now + Duration::minutes(10))
    .bind(now)
    .bind(fixture.deadline)
    .bind(named_digest("context-owner-source-job").to_string())
    .bind(reservation_id)
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
    let mutations = DeferOrchestrationContextMutationIds {
        source: OrchestrationYieldMutationIds {
            receipt_id: id(ResourceKind::Receipt, 0x810),
            quota_entry_ids: (0x811..=0x814)
                .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
                .collect(),
            run_event_id: id(ResourceKind::Event, 0x815),
            run_outbox_id: id(ResourceKind::OutboxEvent, 0x816),
            node_event_id: id(ResourceKind::Event, 0x817),
            node_outbox_id: id(ResourceKind::OutboxEvent, 0x818),
            job_event_id: id(ResourceKind::Event, 0x819),
            job_outbox_id: id(ResourceKind::OutboxEvent, 0x81a),
        },
        context_create_receipt_id: id(ResourceKind::Receipt, 0x81b),
        context_create_event_id: id(ResourceKind::Event, 0x81c),
        context_create_outbox_id: id(ResourceKind::OutboxEvent, 0x81d),
        context_prepare_receipt_id: id(ResourceKind::Receipt, 0x81e),
        context_prepare_event_id: id(ResourceKind::Event, 0x81f),
        context_prepare_outbox_id: id(ResourceKind::OutboxEvent, 0x820),
    };
    let command = DeferOrchestrationToContextQuery {
        fence: fence.clone(),
        plan: fixture.runtime_plan.clone(),
        context_query_id: id(ResourceKind::ContextQuery, 0x821),
        context_job_id: id(ResourceKind::Job, 0x822),
        input: ResolvedExpressionInput {
            run_value_id: id(ResourceKind::RunValue, 0x63),
            producing_node_id: Some(fixture.node_id.clone()),
            value_kind: "context_query".to_owned(),
            port: input_port,
            classification: DataClassification::Internal,
            schema_digest: query_schema_digest(),
            content_digest: input_digest.clone(),
            value: ValueRef::Inline {
                value: input_value.clone(),
            },
        },
        materialized_input: ClosedJsonValue::build(query_schema_digest(), input_value).unwrap(),
        input_artifact_link_id: None,
        idempotency_key_digest: named_digest("context-owner-idempotency"),
        request_digest: named_digest("context-owner-request"),
        receipt_expires_at: fixture.deadline,
        mutations,
    };
    (fence, command)
}

async fn park_direct_context_leaf(
    pool: &PgPool,
    fixture: &Fixture,
    context_query_id: &ResourceId,
    context_job_id: &ResourceId,
    active_work_count: i32,
) {
    let input_value = json!({"question": "top customers"});
    let input_digest: Sha256Digest = canonical_digest(&input_value).unwrap().parse().unwrap();
    let request_port = ExactDataPortRef::RunInput {
        schema_digest: query_schema_digest(),
    };
    let environment = ScopeDataEnvironmentSnapshot::build(
        BTreeMap::from([(
            request_port,
            ExactRunValueRef {
                value_id: id(ResourceKind::RunValue, 0x63),
                schema_digest: query_schema_digest(),
                content_digest: input_digest,
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
        r#"
        UPDATE insight_platform.run_nodes
        SET payload_schema_version = $3, payload = $4, payload_digest = $5
        WHERE tenant_id = $1 AND node_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::ScopeInstance, 0x61).to_string())
    .bind(scope_payload.schema_version)
    .bind(&scope_payload.value)
    .bind(&scope_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let plan_digest = fixture
        .runtime_plan
        .canonical_digest(PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap())
        .unwrap();
    let wait_payload = TypedPayload::new(
        1,
        &json!({
            "plan_node_key": "catalog",
            "plan_digest": plan_digest,
            "source_orchestration_job_id": id(ResourceKind::Job, 0x940),
            "context_query_id": context_query_id,
            "context_job_id": context_job_id,
            "result_port": {
                "source": "node_output",
                "producer_node_id": "catalog",
                "port_id": "items",
                "schema_digest": named_digest("observation-schema"),
            },
            "resume_plan_node_key": "finish",
            "resume_node_kind": "return",
            "root_scope_id": id(ResourceKind::ScopeInstance, 0x61),
            "continuation_attempt_limit": 3,
            "retry_backoff_milliseconds": 100,
            "priority": SchedulerPriority::Normal,
            "deadline": fixture.deadline,
        }),
    )
    .unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'waiting', payload_schema_version = $3,
            payload = $4, payload_digest = $5
        WHERE tenant_id = $1 AND node_id = $2 AND state = 'running'
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.node_id.to_string())
    .bind(wait_payload.schema_version)
    .bind(&wait_payload.value)
    .bind(&wait_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    let current_value: serde_json::Value = sqlx::query_scalar(
        "SELECT current_payload FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let mut current: RunCurrentSnapshot = serde_json::from_value(current_value).unwrap();
    current.waiting_reason = Some("context_query".to_owned());
    let current_payload = TypedPayload::from_versioned(1, &current, 1_048_576).unwrap();
    sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = 'waiting', active_work_count = $3,
            current_schema_version = $4, current_payload = $5,
            current_payload_digest = $6
        WHERE tenant_id = $1 AND run_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(active_work_count)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .execute(pool)
    .await
    .unwrap();
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
            node_execution_id: fixture.node_id.clone(),
            root_scope_id: id(ResourceKind::ScopeInstance, 0x61),
            retry_backoff_milliseconds: 100,
            wake_contract: None,
            convergence_failure: None,
            model_tool_continuation: None,
        },
    )
    .unwrap();
    let terminal_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, run_id, node_id,
            state, version, attempt_no, attempt_limit, lease_epoch, scheduled_at,
            deadline, request_digest, result_digest, payload_schema_version,
            payload, payload_digest, started_at, terminal_at, created_at, updated_at,
            trace_id
        ) VALUES (
            $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, $4, $3,
            'succeeded', 3, 1, 3, 1, $5, $6, $7, $8, $9, $10, $11,
            $5, $5, $5, $5, '0123456789abcdef0123456789abcdef'
        )
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::Job, 0x940).to_string())
    .bind(fixture.node_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(terminal_at)
    .bind(fixture.deadline)
    .bind(named_digest("direct-context-source-request").to_string())
    .bind(wait_payload.digest.clone())
    .bind(source_payload.schema_version)
    .bind(&source_payload.value)
    .bind(&source_payload.digest)
    .execute(pool)
    .await
    .unwrap();
}

async fn claim(
    repository: &PgRepository,
    fixture: &Fixture,
    job_id: ResourceId,
    base: u16,
) -> ClaimEvidence {
    claim_with_manifest(
        repository,
        fixture,
        job_id,
        base,
        fixture.context_worker_manifest_digest.clone(),
    )
    .await
    .unwrap()
}

async fn claim_with_manifest(
    repository: &PgRepository,
    fixture: &Fixture,
    job_id: ResourceId,
    base: u16,
    worker_manifest_digest: Sha256Digest,
) -> Result<ClaimEvidence, RepositoryError> {
    let worker_id = id(ResourceKind::WorkerProcessGeneration, base);
    let lease_token = named_digest(&format!("lease-{base}"));
    let mut claimed = repository
        .claim_context_jobs(ClaimContextJobs {
            worker_process_generation_id: worker_id.clone(),
            worker_manifest_digest,
            slots: vec![ContextClaimSlot {
                tenant_id: fixture.tenant_id.clone(),
                job_id,
                lease_token_digest: lease_token.clone(),
                quota_reservation_id: id(ResourceKind::UsageReservation, base + 1),
                quota_entry_ids: [
                    id(ResourceKind::QuotaLedgerEntry, base + 2),
                    id(ResourceKind::QuotaLedgerEntry, base + 3),
                    id(ResourceKind::QuotaLedgerEntry, base + 4),
                ],
                event_id: id(ResourceKind::Event, base + 5),
                outbox_id: id(ResourceKind::OutboxEvent, base + 6),
                resume_mutations: resume_mutations(base + 0x1000),
                failure_mutations: failure_mutations(base + 0x2000),
            }],
            lease_policy: LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
        })
        .await?;
    assert_eq!(claimed.len(), 1);
    Ok(ClaimEvidence {
        claimed: claimed.pop().unwrap(),
        worker_id,
        lease_token,
    })
}

fn output(
    fixture: &Fixture,
    query: &insight_platform_context::ContextQueryRecord,
    observed_at: DateTime<Utc>,
    value_id: ResourceId,
) -> ContextObservationOutput {
    let content = ValueRef::Inline {
        value: json!("authorized row"),
    };
    let content_digest: Sha256Digest = canonical_digest(&json!("authorized row"))
        .unwrap()
        .parse()
        .unwrap();
    let item = ContextItem {
        item_id: id(ResourceKind::ContextItem, 0x180),
        source_item_identity_digest: named_digest("source-item"),
        content: content.clone(),
        structured_fields: ClosedJsonValue::build(
            named_digest("structured-schema"),
            json!({"customer_id": 42}),
        )
        .unwrap(),
        score: Some(NormalizedContextScore {
            millionths: 900_000,
            score_domain_digest: fixture.score_domain_digest.clone(),
        }),
        classification: DataClassification::Internal,
        citation: ContextCitation {
            context_deployment: fixture.context_deployment.clone(),
            interface_revision: fixture.interface_revision.clone(),
            dataset_view: query.payload.admission.dataset_view.clone(),
            locator: CitationLocator::RemoteOpaque {
                locator_digest: named_digest("locator"),
            },
            strength: ContextCitationStrength::ObservationOnly,
            content_digest,
            observed_at,
            display_label: "authorized row".to_owned(),
        },
        authorization_evidence_digest: named_digest("authorization-evidence"),
    };
    let total_bytes = match &content {
        ValueRef::Inline { value } => {
            u64::try_from(serde_json::to_vec(value).unwrap().len()).unwrap()
        }
        ValueRef::Artifact { artifact } => artifact.byte_length(),
    };
    let mut observation = ContextObservation {
        schema_version: 1,
        observation_id: id(ResourceKind::ContextObservation, 0x181),
        context_query_id: query.context_query_id.clone(),
        dataset_view: query.payload.admission.dataset_view.clone(),
        normalized_query_digest: query
            .payload
            .admission
            .request
            .normalized_query_digest
            .clone(),
        items: vec![item],
        next_cursor_digest: None,
        evidence: ContextRetrievalEvidence {
            backend_request_digest: named_digest("backend-request"),
            backend_response_digest: named_digest("backend-response"),
            authorization_evidence_digest: named_digest("authorization-evidence"),
            ranking_evidence_digest: named_digest("ranking-evidence"),
            candidate_count: 1,
            rejected_count: 0,
            truncated: false,
        },
        observed_at,
        total_bytes,
        canonical_digest: named_digest("placeholder"),
    };
    observation.canonical_digest = digest_without_field(&observation, "canonical_digest");
    let mut unsigned = serde_json::to_value(&observation).unwrap();
    unsigned.as_object_mut().unwrap().remove("canonical_digest");
    ContextObservationOutput {
        value_id,
        value_kind: "context_observation".to_owned(),
        classification: DataClassification::Internal,
        value: ValueRef::Inline { value: unsigned },
        artifact_link_id: None,
        observation,
        validation_evidence_digest: named_digest("validation-evidence"),
    }
}

fn text2sql_policy(fixture: &Fixture) -> InvocationPolicyDecisionBundle {
    InvocationPolicyDecisionBundle::build(
        fixture
            .invocation_policies
            .iter()
            .cloned()
            .map(|policy| InvocationPolicyDecision {
                policy,
                disposition: InvocationPolicyDisposition::Allowed,
                evidence_digest: named_digest("text2sql-policy-evidence"),
            })
            .collect(),
        None,
    )
    .unwrap()
}

fn text2sql_plan(
    fixture: &Fixture,
    completed: &insight_platform_context::ContextQueryRecord,
) -> ReadOnlySqlPlan {
    let result = completed.payload.result.as_ref().unwrap();
    let mut plan = ReadOnlySqlPlan {
        schema_version: 1,
        catalog_context_query_id: completed.context_query_id.clone(),
        catalog_observation_id: result.observation.observation_id.clone(),
        catalog_observation_digest: result.observation.canonical_digest.clone(),
        catalog_projection_digest: fixture.catalog_projection_digest.clone(),
        execution: ReadOnlySqlExecutionBinding {
            capability_name: READONLY_DATABASE_CAPABILITY.parse().unwrap(),
            capability_deployment: fixture.readonly_capability_deployment.clone(),
            interface_revision: fixture.readonly_capability_interface.clone(),
            effect: Effect::ReadOnly,
            database_identity_digest: fixture.database_identity_digest.clone(),
            dialect: "postgres".to_owned(),
            allowed_schemas: vec!["analytics".to_owned()],
            statement_timeout_milliseconds: 5_000,
            row_limit: 1_000,
            byte_limit: 1_048_576,
            cost_gate_digest: named_digest("text2sql-cost-gate"),
        },
        from: SqlSource {
            object: SqlObjectName {
                schema: "analytics".to_owned(),
                object: "orders".to_owned(),
            },
            alias: "orders".to_owned(),
        },
        joins: vec![],
        projections: vec![SqlProjection {
            expression: SqlProjectionExpression::Column {
                column: SqlColumnRef {
                    source_alias: "orders".to_owned(),
                    column: "total".to_owned(),
                },
            },
            output_name: "total".to_owned(),
        }],
        predicates: vec![SqlPredicate {
            column: SqlColumnRef {
                source_alias: "orders".to_owned(),
                column: "customer_id".to_owned(),
            },
            operator: SqlComparisonOperator::Equal,
            parameter_ordinals: vec![0],
        }],
        group_by: vec![],
        order_by: vec![],
        parameters: vec![ClosedJsonValue::build(
            named_digest("text2sql-parameter-schema"),
            json!(42),
        )
        .unwrap()],
        limit: 100,
        offset: 0,
        generated_sql_digest: named_digest("generated-select"),
        validation_evidence_digest: named_digest("sql-validation"),
        canonical_digest: named_digest("placeholder"),
    };
    plan.canonical_digest = digest_without_field(&plan, "canonical_digest");
    plan
}

async fn insert_text2sql_plan_value(
    pool: &PgPool,
    fixture: &Fixture,
    value_id: &ResourceId,
    plan: &ReadOnlySqlPlan,
) {
    let value = serde_json::to_value(plan).unwrap();
    let content_digest: Sha256Digest = canonical_digest(&value).unwrap().parse().unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value
        ) VALUES ($1, $2, $3, $4, $5, 'internal', $6, $7, $8)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(value_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(fixture.text2sql_node_id.to_string())
    .bind(TEXT2SQL_PLAN_VALUE_KIND)
    .bind(fixture.readonly_input_schema_digest.to_string())
    .bind(content_digest.to_string())
    .bind(value)
    .execute(pool)
    .await
    .unwrap();
}

fn text2sql_invocation_command(
    fixture: &Fixture,
    input_value_id: ResourceId,
    base: u16,
) -> AdmitCapabilityInvocation {
    AdmitCapabilityInvocation {
        audit: audit(
            &fixture.tenant_id,
            &fixture.principal_id,
            base,
            "text2sql-admit",
        ),
        invocation_id: id(ResourceKind::CapabilityInvocation, base + 3),
        run_id: fixture.run_id.clone(),
        node_execution_id: fixture.text2sql_node_id.clone(),
        expected_run_version: 2,
        expected_node_version: 1,
        slot_id: "readonly_sql".to_owned(),
        input_value_id,
        input_artifact_link_id: None,
        origin: InvocationOrigin::PlanNode {
            node_execution_id: fixture.text2sql_node_id.clone(),
        },
        selected_candidate_ordinal: 0,
        selector_input_digest: named_digest("text2sql-selector"),
        policy_decisions: text2sql_policy(fixture),
        approval_task_id: None,
        requested_attempt_limit: 2,
        requested_retry_backoff_milliseconds: 100,
        mcp_runtime: None,
    }
}

#[test]
fn context_query_is_atomic_quota_accounted_deferred_and_tenant_scoped() {
    // These tests share one real database in the workspace gate. Serialize the fixture builder
    // and assign each scenario a disjoint nominal-ID namespace while preserving its internal
    // concurrency and cross-process visibility.
    let _fixture_namespace = select_fixture_namespace(0xa0f6);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(runtime.spawn(async {
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

    let dataset_id = id(ResourceKind::ContextDataset, 0x700);
    let mut build_audit = audit(
        &fixture.tenant_id,
        &fixture.principal_id,
        0x701,
        "dataset-build",
    );
    build_audit.idempotency_key_digest = named_digest("dataset-build-key");
    build_audit.request_digest = named_digest("dataset-build-request");
    let first_build = RequestContextDatasetBuild {
        audit: build_audit,
        context_resource_id: id(ResourceKind::ContextSourceInterface, 0x12),
        context_deployment: fixture.context_deployment.clone(),
        dataset_id: dataset_id.clone(),
        job_id: id(ResourceKind::Job, 0x704),
        artifact_preallocations: dataset_artifact_preallocations(0xb00),
        attempt_limit: 3,
        deadline: Utc::now() + Duration::hours(1),
    };
    let original_job = match repository
        .request_context_dataset_build(first_build.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(job) => job,
        CommandOutcome::Replayed(_) => panic!("first Dataset build must apply"),
    };
    let empty_dataset_roots: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.resources WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(dataset_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(empty_dataset_roots, 0);
    let mut replay = first_build.clone();
    replay.audit.receipt_id = id(ResourceKind::Receipt, 0x710);
    replay.audit.event_id = id(ResourceKind::Event, 0x711);
    replay.audit.outbox_id = id(ResourceKind::OutboxEvent, 0x712);
    replay.dataset_id = id(ResourceKind::ContextDataset, 0x713);
    replay.job_id = id(ResourceKind::Job, 0x714);
    let replayed_job = match repository
        .request_context_dataset_build(replay)
        .await
        .unwrap()
    {
        CommandOutcome::Replayed(job) => job,
        CommandOutcome::Applied(_) => panic!("Dataset build replay must reuse the first Job"),
    };
    assert_eq!(replayed_job.job_id, original_job.job_id);
    assert_eq!(replayed_job.owner_id, dataset_id.to_string());
    let mut concurrent = first_build;
    concurrent.audit = audit(
        &fixture.tenant_id,
        &fixture.principal_id,
        0x720,
        "dataset-build-concurrent",
    );
    concurrent.job_id = id(ResourceKind::Job, 0x723);
    assert!(matches!(
        repository.request_context_dataset_build(concurrent).await,
        Err(RepositoryError::Conflict("Context Dataset build"))
    ));
    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0x730);
    let lease_token_digest = named_digest("dataset-build-lease");
    let wrong_source_claim = repository
        .claim_context_dataset_build_jobs_for_sources(
            ClaimJobs {
                work_class: WorkClass::Context.to_string(),
                worker_id: worker_id.clone(),
                limit: 1,
                lease_milliseconds: 30_000,
                lease_token_digests: vec![named_digest("dataset-wrong-source-lease")],
            },
            &[named_digest("dataset-uninstalled-context-deployment")],
        )
        .await
        .unwrap();
    assert!(wrong_source_claim.is_empty());
    let claimed = repository
        .claim_context_dataset_build_jobs_for_sources(
            ClaimJobs {
                work_class: WorkClass::Context.to_string(),
                worker_id: worker_id.clone(),
                limit: 1,
                lease_milliseconds: 30_000,
                lease_token_digests: vec![lease_token_digest.clone()],
            },
            std::slice::from_ref(&fixture.context_deployment.deployment_digest),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.job_id, original_job.job_id);
    let abandoned_started = repository
        .start_context_dataset_build_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: claimed.job_id.clone(),
            worker_id: worker_id.clone(),
            lease_epoch: claimed.lease_epoch,
            expected_job_version: claimed.version,
            lease_token_digest: lease_token_digest.clone(),
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(&claimed.job_id)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        repository
            .recover_expired_context_dataset_build_jobs_for_sources(
                1,
                1,
                std::slice::from_ref(&fixture.context_deployment.deployment_digest),
            )
            .await
            .unwrap(),
        1
    );
    sqlx::query(
        "UPDATE insight_platform.jobs SET retry_at = clock_timestamp() - interval '1 millisecond' WHERE tenant_id = $1 AND job_id = $2 AND state = 'retry_scheduled'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(&claimed.job_id)
    .execute(&pool)
    .await
    .unwrap();
    let replacement_worker = id(ResourceKind::WorkerProcessGeneration, 0x731);
    let replacement_token = named_digest("dataset-build-replacement-lease");
    let replacement_claim = repository
        .claim_context_dataset_build_jobs_for_sources(
            ClaimJobs {
                work_class: WorkClass::Context.to_string(),
                worker_id: replacement_worker.clone(),
                limit: 1,
                lease_milliseconds: 30_000,
                lease_token_digests: vec![replacement_token.clone()],
            },
            std::slice::from_ref(&fixture.context_deployment.deployment_digest),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let started = repository
        .start_context_dataset_build_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: replacement_claim.job_id,
            worker_id: replacement_worker.clone(),
            lease_epoch: replacement_claim.lease_epoch,
            expected_job_version: replacement_claim.version,
            lease_token_digest: replacement_token.clone(),
        })
        .await
        .unwrap();
    assert_eq!(abandoned_started.attempt_no, 1);
    assert_eq!(started.attempt_no, 2);
    let build_payload: ContextDatasetBuildJobPayload =
        serde_json::from_value(started.payload.value.clone()).unwrap();
    let producer_fence = JobFence {
        expected_version: u64::try_from(started.version).unwrap(),
        worker_process_generation_id: replacement_worker,
        lease_generation: u64::try_from(started.lease_epoch).unwrap(),
        token_digest: replacement_token,
    };
    let index_request = stage_context_dataset_artifact(
        &pool,
        &repository,
        &fixture.tenant_id,
        &build_payload.job_id,
        &producer_fence,
        &build_payload.artifact_preallocations.index_manifest,
        &build_payload.artifact_stages.index_manifest.declared_media_type,
        br#"{"items":[{"content":"local deterministic context item","ordinal":0}],"schema_version":1}"#.to_vec(),
        0xb30,
    )
    .await;
    let validation_request = stage_context_dataset_artifact(
        &pool,
        &repository,
        &fixture.tenant_id,
        &build_payload.job_id,
        &producer_fence,
        &build_payload.artifact_preallocations.validation_evidence,
        &build_payload
            .artifact_stages
            .validation_evidence
            .declared_media_type,
        br#"{"item_count":1,"schema_version":1,"valid":true}"#.to_vec(),
        0xb31,
    )
    .await;
    let parked = repository
        .park_context_dataset_build_for_verification(ParkContextDatasetBuildVerification {
            tenant_id: fixture.tenant_id.clone(),
            job_id: build_payload.job_id.clone(),
            dataset_id: dataset_id.clone(),
            fence: producer_fence,
            event_id: id(ResourceKind::Event, 0xb32),
            outbox_id: id(ResourceKind::OutboxEvent, 0xb33),
        })
        .await
        .unwrap();
    assert_eq!(parked.state, JobState::Waiting.to_string());
    scan_context_dataset_artifacts(
        &pool,
        &repository,
        &fixture.tenant_id,
        &[index_request, validation_request],
        0xb40,
    )
    .await;
    let resumed_worker = id(ResourceKind::WorkerProcessGeneration, 0xb50);
    let resumed_token = named_digest("dataset-build-resumed-lease");
    let resumed_claim = repository
        .claim_context_dataset_build_jobs(ClaimJobs {
            work_class: WorkClass::Context.to_string(),
            worker_id: resumed_worker.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![resumed_token.clone()],
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let resumed_started = repository
        .start_context_dataset_build_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: resumed_claim.job_id,
            worker_id: resumed_worker.clone(),
            lease_epoch: resumed_claim.lease_epoch,
            expected_job_version: resumed_claim.version,
            lease_token_digest: resumed_token.clone(),
        })
        .await
        .unwrap();
    assert_eq!(resumed_started.attempt_no, started.attempt_no);
    let resumed_fence = JobFence {
        expected_version: u64::try_from(resumed_started.version).unwrap(),
        worker_process_generation_id: resumed_worker,
        lease_generation: u64::try_from(resumed_started.lease_epoch).unwrap(),
        token_digest: resumed_token.clone(),
    };
    let resolved = repository
        .resolve_context_dataset_build(
            fixture.tenant_id.clone(),
            build_payload.job_id.clone(),
            resumed_fence.clone(),
        )
        .await
        .unwrap();
    let completion = CommitContextDatasetBuild {
        tenant_id: fixture.tenant_id.clone(),
        job_id: build_payload.job_id.clone(),
        dataset_id: dataset_id.clone(),
        generation_id: build_payload.artifact_preallocations.generation_id.clone(),
        fence: resumed_fence,
        lease_token_digest: resumed_token,
        generation: ContextDatasetGenerationSpec {
            context_deployment: resolved.payload.context_deployment,
            source_manifest_digest: resolved.index_manifest.content_digest().clone(),
            parser_profile: resolved.payload.parser_profile,
            chunker_profile: resolved.payload.chunker_profile,
            embedding_model_deployment: resolved.payload.embedding_model_deployment,
            ranking_profile: resolved.payload.ranking_profile,
            index_manifest: resolved.index_manifest,
            validation_evidence: resolved.validation_evidence,
            created_by_operation_id: build_payload.job_id,
        },
        event_id: id(ResourceKind::Event, 0x732),
        outbox_id: id(ResourceKind::OutboxEvent, 0x733),
    };
    let completed = repository
        .commit_context_dataset_build(completion.clone())
        .await
        .unwrap();
    assert_eq!(completed.state, JobState::Succeeded.to_string());
    let first_generation_id = completion.generation_id.clone();
    let active_generation: String = sqlx::query_scalar(
        "SELECT active_version_id FROM insight_platform.resources WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(dataset_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(active_generation, completion.generation_id.to_string());
    assert!(matches!(
        repository.commit_context_dataset_build(completion).await,
        Err(RepositoryError::StaleFence | RepositoryError::Conflict(_))
    ));

    let mut rebuild_audit = audit(
        &fixture.tenant_id,
        &fixture.principal_id,
        0x740,
        "dataset-rebuild",
    );
    rebuild_audit.idempotency_key_digest = named_digest("dataset-rebuild-key");
    rebuild_audit.request_digest = named_digest("dataset-rebuild-request");
    let rebuild_job = match repository
        .request_context_dataset_build(RequestContextDatasetBuild {
            audit: rebuild_audit,
            context_resource_id: id(ResourceKind::ContextSourceInterface, 0x12),
            context_deployment: fixture.context_deployment.clone(),
            dataset_id: dataset_id.clone(),
            job_id: id(ResourceKind::Job, 0x743),
            artifact_preallocations: dataset_artifact_preallocations(0xb10),
            attempt_limit: 3,
            deadline: Utc::now() + Duration::hours(1),
        })
        .await
        .unwrap()
    {
        CommandOutcome::Applied(job) => job,
        CommandOutcome::Replayed(_) => panic!("Dataset rebuild must apply"),
    };
    let rebuild_worker = id(ResourceKind::WorkerProcessGeneration, 0x744);
    let rebuild_token = named_digest("dataset-rebuild-lease");
    let rebuild_claim = repository
        .claim_context_dataset_build_jobs(ClaimJobs {
            work_class: WorkClass::Context.to_string(),
            worker_id: rebuild_worker.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![rebuild_token.clone()],
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(rebuild_claim.job_id, rebuild_job.job_id);
    let rebuild_started = repository
        .start_context_dataset_build_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: rebuild_claim.job_id,
            worker_id: rebuild_worker.clone(),
            lease_epoch: rebuild_claim.lease_epoch,
            expected_job_version: rebuild_claim.version,
            lease_token_digest: rebuild_token.clone(),
        })
        .await
        .unwrap();
    let rebuild_payload: ContextDatasetBuildJobPayload =
        serde_json::from_value(rebuild_started.payload.value.clone()).unwrap();
    assert_eq!(
        rebuild_payload.expected_active_generation_id,
        Some(first_generation_id)
    );
    assert_eq!(rebuild_payload.expected_dataset_version, Some(1));
    let rebuild_fence = JobFence {
        expected_version: u64::try_from(rebuild_started.version).unwrap(),
        worker_process_generation_id: rebuild_worker,
        lease_generation: u64::try_from(rebuild_started.lease_epoch).unwrap(),
        token_digest: rebuild_token,
    };
    let rebuild_index = stage_context_dataset_artifact(
        &pool,
        &repository,
        &fixture.tenant_id,
        &rebuild_payload.job_id,
        &rebuild_fence,
        &rebuild_payload.artifact_preallocations.index_manifest,
        &rebuild_payload.artifact_stages.index_manifest.declared_media_type,
        br#"{"items":[{"content":"updated deterministic context item","ordinal":0}],"schema_version":1}"#.to_vec(),
        0xb60,
    )
    .await;
    let rebuild_validation = stage_context_dataset_artifact(
        &pool,
        &repository,
        &fixture.tenant_id,
        &rebuild_payload.job_id,
        &rebuild_fence,
        &rebuild_payload
            .artifact_preallocations
            .validation_evidence,
        &rebuild_payload
            .artifact_stages
            .validation_evidence
            .declared_media_type,
        br#"{"item_count":1,"schema_version":1,"valid":true}"#.to_vec(),
        0xb61,
    )
    .await;
    repository
        .park_context_dataset_build_for_verification(ParkContextDatasetBuildVerification {
            tenant_id: fixture.tenant_id.clone(),
            job_id: rebuild_payload.job_id.clone(),
            dataset_id: dataset_id.clone(),
            fence: rebuild_fence,
            event_id: id(ResourceKind::Event, 0xb62),
            outbox_id: id(ResourceKind::OutboxEvent, 0xb63),
        })
        .await
        .unwrap();
    scan_context_dataset_artifacts(
        &pool,
        &repository,
        &fixture.tenant_id,
        &[rebuild_index, rebuild_validation],
        0xb70,
    )
    .await;
    let rebuild_resumed_worker = id(ResourceKind::WorkerProcessGeneration, 0xb80);
    let rebuild_resumed_token = named_digest("dataset-rebuild-resumed-lease");
    let rebuild_resumed_claim = repository
        .claim_context_dataset_build_jobs(ClaimJobs {
            work_class: WorkClass::Context.to_string(),
            worker_id: rebuild_resumed_worker.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![rebuild_resumed_token.clone()],
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let rebuild_resumed_started = repository
        .start_context_dataset_build_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: rebuild_resumed_claim.job_id,
            worker_id: rebuild_resumed_worker.clone(),
            lease_epoch: rebuild_resumed_claim.lease_epoch,
            expected_job_version: rebuild_resumed_claim.version,
            lease_token_digest: rebuild_resumed_token.clone(),
        })
        .await
        .unwrap();
    let rebuild_resumed_fence = JobFence {
        expected_version: u64::try_from(rebuild_resumed_started.version).unwrap(),
        worker_process_generation_id: rebuild_resumed_worker,
        lease_generation: u64::try_from(rebuild_resumed_started.lease_epoch).unwrap(),
        token_digest: rebuild_resumed_token.clone(),
    };
    let rebuild_resolved = repository
        .resolve_context_dataset_build(
            fixture.tenant_id.clone(),
            rebuild_payload.job_id.clone(),
            rebuild_resumed_fence.clone(),
        )
        .await
        .unwrap();
    let second_generation_id = rebuild_payload
        .artifact_preallocations
        .generation_id
        .clone();
    repository
        .commit_context_dataset_build(CommitContextDatasetBuild {
            tenant_id: fixture.tenant_id.clone(),
            job_id: rebuild_payload.job_id.clone(),
            dataset_id: dataset_id.clone(),
            generation_id: second_generation_id.clone(),
            fence: rebuild_resumed_fence,
            lease_token_digest: rebuild_resumed_token,
            generation: ContextDatasetGenerationSpec {
                context_deployment: rebuild_resolved.payload.context_deployment,
                source_manifest_digest: rebuild_resolved.index_manifest.content_digest().clone(),
                parser_profile: rebuild_resolved.payload.parser_profile,
                chunker_profile: rebuild_resolved.payload.chunker_profile,
                embedding_model_deployment: rebuild_resolved.payload.embedding_model_deployment,
                ranking_profile: rebuild_resolved.payload.ranking_profile,
                index_manifest: rebuild_resolved.index_manifest,
                validation_evidence: rebuild_resolved.validation_evidence,
                created_by_operation_id: rebuild_payload.job_id,
            },
            event_id: id(ResourceKind::Event, 0x746),
            outbox_id: id(ResourceKind::OutboxEvent, 0x747),
        })
        .await
        .unwrap();
    let rebuilt: (i64, String, i64) = sqlx::query_as(
        r#"
        SELECT resource.version, resource.active_version_id, count(version.resource_version_id)
        FROM insight_platform.resources AS resource
        JOIN insight_platform.resource_versions AS version
          ON version.tenant_id = resource.tenant_id
         AND version.resource_id = resource.resource_id
        WHERE resource.tenant_id = $1 AND resource.resource_id = $2
        GROUP BY resource.version, resource.active_version_id
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(dataset_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rebuilt, (2, second_generation_id.to_string(), 2));

    let mut rejected_audit = audit(
        &fixture.tenant_id,
        &fixture.principal_id,
        0xc00,
        "dataset-rejected-rebuild",
    );
    rejected_audit.idempotency_key_digest = named_digest("dataset-rejected-rebuild-key");
    rejected_audit.request_digest = named_digest("dataset-rejected-rebuild-request");
    let rejected_job = match repository
        .request_context_dataset_build(RequestContextDatasetBuild {
            audit: rejected_audit,
            context_resource_id: id(ResourceKind::ContextSourceInterface, 0x12),
            context_deployment: fixture.context_deployment.clone(),
            dataset_id: dataset_id.clone(),
            job_id: id(ResourceKind::Job, 0xc04),
            artifact_preallocations: dataset_artifact_preallocations(0xc10),
            attempt_limit: 3,
            deadline: Utc::now() + Duration::hours(1),
        })
        .await
        .unwrap()
    {
        CommandOutcome::Applied(job) => job,
        CommandOutcome::Replayed(_) => panic!("rejected Dataset rebuild must apply"),
    };
    let rejected_worker = id(ResourceKind::WorkerProcessGeneration, 0xc30);
    let rejected_token = named_digest("dataset-rejected-build-lease");
    let rejected_claim = repository
        .claim_context_dataset_build_jobs(ClaimJobs {
            work_class: WorkClass::Context.to_string(),
            worker_id: rejected_worker.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![rejected_token.clone()],
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(rejected_claim.job_id, rejected_job.job_id);
    let rejected_started = repository
        .start_context_dataset_build_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: rejected_claim.job_id,
            worker_id: rejected_worker.clone(),
            lease_epoch: rejected_claim.lease_epoch,
            expected_job_version: rejected_claim.version,
            lease_token_digest: rejected_token.clone(),
        })
        .await
        .unwrap();
    let rejected_payload: ContextDatasetBuildJobPayload =
        serde_json::from_value(rejected_started.payload.value.clone()).unwrap();
    let rejected_fence = JobFence {
        expected_version: u64::try_from(rejected_started.version).unwrap(),
        worker_process_generation_id: rejected_worker,
        lease_generation: u64::try_from(rejected_started.lease_epoch).unwrap(),
        token_digest: rejected_token,
    };
    let rejected_index = stage_context_dataset_artifact(
        &pool,
        &repository,
        &fixture.tenant_id,
        &rejected_payload.job_id,
        &rejected_fence,
        &rejected_payload.artifact_preallocations.index_manifest,
        &rejected_payload.artifact_stages.index_manifest.declared_media_type,
        br#"{"items":[{"content":"rejected context item","ordinal":0}],"schema_version":1}"#
            .to_vec(),
        0xc40,
    )
    .await;
    let rejected_validation = stage_context_dataset_artifact(
        &pool,
        &repository,
        &fixture.tenant_id,
        &rejected_payload.job_id,
        &rejected_fence,
        &rejected_payload
            .artifact_preallocations
            .validation_evidence,
        &rejected_payload
            .artifact_stages
            .validation_evidence
            .declared_media_type,
        br#"{"item_count":1,"schema_version":1,"valid":false}"#.to_vec(),
        0xc41,
    )
    .await;
    repository
        .park_context_dataset_build_for_verification(ParkContextDatasetBuildVerification {
            tenant_id: fixture.tenant_id.clone(),
            job_id: rejected_payload.job_id.clone(),
            dataset_id: dataset_id.clone(),
            fence: rejected_fence,
            event_id: id(ResourceKind::Event, 0xc42),
            outbox_id: id(ResourceKind::OutboxEvent, 0xc43),
        })
        .await
        .unwrap();
    scan_context_dataset_artifacts_with_dispositions(
        &pool,
        &repository,
        &fixture.tenant_id,
        &[rejected_index, rejected_validation],
        0xc50,
        &[
            ArtifactScanDisposition::Verified,
            ArtifactScanDisposition::Rejected,
        ],
    )
    .await;
    let rejected_resumed_worker = id(ResourceKind::WorkerProcessGeneration, 0xc70);
    let rejected_resumed_token = named_digest("dataset-rejected-resumed-lease");
    let rejected_resumed_claim = repository
        .claim_context_dataset_build_jobs(ClaimJobs {
            work_class: WorkClass::Context.to_string(),
            worker_id: rejected_resumed_worker.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![rejected_resumed_token.clone()],
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    let rejected_resumed_started = repository
        .start_context_dataset_build_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: rejected_resumed_claim.job_id,
            worker_id: rejected_resumed_worker.clone(),
            lease_epoch: rejected_resumed_claim.lease_epoch,
            expected_job_version: rejected_resumed_claim.version,
            lease_token_digest: rejected_resumed_token.clone(),
        })
        .await
        .unwrap();
    assert_eq!(rejected_resumed_started.attempt_no, 1);
    let failed = repository
        .fail_context_dataset_build_verification(FailContextDatasetBuildVerification {
            tenant_id: fixture.tenant_id.clone(),
            job_id: rejected_payload.job_id.clone(),
            dataset_id: dataset_id.clone(),
            fence: JobFence {
                expected_version: u64::try_from(rejected_resumed_started.version).unwrap(),
                worker_process_generation_id: rejected_resumed_worker,
                lease_generation: u64::try_from(rejected_resumed_started.lease_epoch).unwrap(),
                token_digest: rejected_resumed_token,
            },
            event_id: id(ResourceKind::Event, 0xc72),
            outbox_id: id(ResourceKind::OutboxEvent, 0xc73),
        })
        .await
        .unwrap();
    assert_eq!(failed.state, JobState::Failed.to_string());
    let after_rejection: (i64, String, i64, i64) = sqlx::query_as(
        r#"
        SELECT resource.version, resource.active_version_id,
               count(version.resource_version_id), quota.reserved_value
        FROM insight_platform.resources AS resource
        JOIN insight_platform.resource_versions AS version
          ON version.tenant_id = resource.tenant_id
         AND version.resource_id = resource.resource_id
        JOIN insight_platform.quota_accounts AS quota
          ON quota.tenant_id = resource.tenant_id
         AND quota.metric = 'artifact.staging_bytes'
        WHERE resource.tenant_id = $1 AND resource.resource_id = $2
        GROUP BY resource.version, resource.active_version_id, quota.reserved_value
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(dataset_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        after_rejection,
        (2, second_generation_id.to_string(), 2, 0)
    );

    let command = create_command(&fixture, 0x100);
    let mut cross_tenant = command.clone();
    cross_tenant.audit = audit(
        &fixture.other_tenant_id,
        &fixture.principal_id,
        0x110,
        "cross-tenant",
    );
    cross_tenant.context_query_id = id(ResourceKind::ContextQuery, 0x113);
    assert!(matches!(
        execute_create(&repository, cross_tenant).await,
        Err(RepositoryError::NotFound(_))
    ));
    let leaked_cross_tenant_receipt: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
    )
    .bind(fixture.other_tenant_id.to_string())
    .bind(id(ResourceKind::Receipt, 0x110).to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(leaked_cross_tenant_receipt, 0);

    let created = match execute_create(&repository, command.clone()).await.unwrap() {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Context create replayed"),
    };
    assert_eq!(created.state, ContextQueryState::Ready);
    assert!(matches!(
        execute_create(&repository, command.clone()).await.unwrap(),
        CommandOutcome::Replayed(record) if record == created
    ));

    let job_id = id(ResourceKind::Job, 0x120);
    let prepared = match execute_prepare(
        &repository,
        PrepareContextDispatch {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x121, "prepare"),
            context_query_id: created.context_query_id.clone(),
            expected_query_version: created.version,
            job_id: job_id.clone(),
            scheduled_at: created.created_at,
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Context prepare replayed"),
    };
    assert_eq!(prepared.job.state, JobState::Ready.as_str());
    assert!(repository
        .scan_claimable_native_context_jobs(
            &fixture.context_worker_manifest_digest,
            &named_digest("uninstalled-native-adapter"),
            &named_digest("wrong-backend-contract"),
            1,
        )
        .await
        .unwrap()
        .is_empty());
    assert!(matches!(
        claim_with_manifest(
            &repository,
            &fixture,
            job_id.clone(),
            0x130,
            named_digest("wrong-context-worker-manifest"),
        )
        .await,
        Err(RepositoryError::Conflict("Context Worker manifest"))
    ));
    let untouched: (String, i32, bool, bool) = sqlx::query_as(
        "SELECT state, attempt_no, worker_id IS NULL, quota_reservation_id IS NULL FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(untouched, ("ready".to_owned(), 0, true, true));
    let first_claim = claim(&repository, &fixture, job_id.clone(), 0x130).await;
    assert_eq!(first_claim.claimed.query.state, ContextQueryState::InFlight);
    assert_eq!(first_claim.claimed.job.attempt_no, 1);
    assert_eq!(first_claim.claimed.quota_account_ids.len(), 3);
    assert_eq!(
        first_claim.claimed.query_input,
        ValueRef::Inline {
            value: json!({"question": "top customers"}),
        }
    );

    let reserved: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT metric, reserved_value, used_value
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND work_class = 'context'
        ORDER BY metric
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(reserved.len(), 3);
    assert!(reserved
        .iter()
        .all(|(_, reserved_value, used)| *reserved_value > 0 && *used == 0));

    let mut forged = output(
        &fixture,
        &first_claim.claimed.query,
        Utc::now(),
        id(ResourceKind::RunValue, 0x140),
    );
    forged.observation.items[0].citation.content_digest = named_digest("forged-content");
    let invalid_receipt = id(ResourceKind::Receipt, 0x141);
    assert!(matches!(
        execute_outcome(
            &repository,
            CommitContextOutcome {
                audit: worker_audit(&fixture.tenant_id, &first_claim.worker_id, 0x141, "invalid",),
                context_query_id: created.context_query_id.clone(),
                expected_query_version: first_claim.claimed.query.version,
                job_id: job_id.clone(),
                fence: first_claim.fence(),
                outcome: ContextBackendOutcome::Completed(Box::new(forged)),
                quota_entry_ids: [
                    id(ResourceKind::QuotaLedgerEntry, 0x144),
                    id(ResourceKind::QuotaLedgerEntry, 0x145),
                    id(ResourceKind::QuotaLedgerEntry, 0x146),
                ],
                resume_mutations: Some(resume_mutations(0x900)),
                failure_mutations: None,
            },
        )
        .await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let invalid_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(invalid_receipt.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(invalid_receipts, 0);
    let reserved_after_invalid: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT metric, reserved_value, used_value
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND work_class = 'context'
        ORDER BY metric
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(reserved_after_invalid, reserved);

    let mut foreign_citation = output(
        &fixture,
        &first_claim.claimed.query,
        Utc::now(),
        id(ResourceKind::RunValue, 0x148),
    );
    foreign_citation.observation.items[0]
        .citation
        .context_deployment = ExactDeploymentRef::new(
        id(ResourceKind::ContextDeployment, 0x149),
        named_digest("foreign-tenant-context-deployment"),
    )
    .unwrap();
    foreign_citation.observation.canonical_digest =
        digest_without_field(&foreign_citation.observation, "canonical_digest");
    let foreign_citation_receipt = id(ResourceKind::Receipt, 0x14a);
    assert!(matches!(
        execute_outcome(
            &repository,
            CommitContextOutcome {
                audit: worker_audit(
                    &fixture.tenant_id,
                    &first_claim.worker_id,
                    0x14a,
                    "foreign-citation",
                ),
                context_query_id: created.context_query_id.clone(),
                expected_query_version: first_claim.claimed.query.version,
                job_id: job_id.clone(),
                fence: first_claim.fence(),
                outcome: ContextBackendOutcome::Completed(Box::new(foreign_citation)),
                quota_entry_ids: [
                    id(ResourceKind::QuotaLedgerEntry, 0x14d),
                    id(ResourceKind::QuotaLedgerEntry, 0x14e),
                    id(ResourceKind::QuotaLedgerEntry, 0x14f),
                ],
                resume_mutations: Some(resume_mutations(0x910)),
                failure_mutations: None,
            },
        )
        .await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let foreign_citation_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(foreign_citation_receipt.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(foreign_citation_rows, 0);

    let wake = WakeContract {
        kind: WakeKind::RemoteInvocation,
        generation: u64::try_from(first_claim.claimed.job.lease_epoch).unwrap(),
        accepted_sources: vec![WakeSource::Callback, WakeSource::Poll],
        expected_response_schema_digest: None,
        opaque_state_digest: Some(named_digest("remote-state")),
        next_poll_at: None,
        poll_count: 0,
        poll_limit: 4,
        callback_binding_digest: Some(named_digest("callback-binding")),
        deadline: fixture.deadline,
    };
    let deferred = match execute_outcome(
        &repository,
        CommitContextOutcome {
            audit: worker_audit(
                &fixture.tenant_id,
                &first_claim.worker_id,
                0x150,
                "deferred",
            ),
            context_query_id: created.context_query_id.clone(),
            expected_query_version: first_claim.claimed.query.version,
            job_id: job_id.clone(),
            fence: first_claim.fence(),
            outcome: ContextBackendOutcome::Deferred {
                wake,
                remote_state_digest: named_digest("remote-state"),
            },
            quota_entry_ids: [
                id(ResourceKind::QuotaLedgerEntry, 0x153),
                id(ResourceKind::QuotaLedgerEntry, 0x154),
                id(ResourceKind::QuotaLedgerEntry, 0x155),
            ],
            resume_mutations: None,
            failure_mutations: None,
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Context outcome replayed"),
    };
    assert_eq!(deferred.query.state, ContextQueryState::Deferred);
    assert_eq!(deferred.job.state, JobState::Waiting.as_str());

    let woken = match execute_wake(
        &repository,
        WakeContextDispatch {
            audit: signal_audit(&fixture.tenant_id, 0x160, "wake"),
            context_query_id: created.context_query_id.clone(),
            expected_query_version: deferred.query.version,
            job_id: job_id.clone(),
            expected_wake_generation: u64::try_from(deferred.job.wake_generation).unwrap(),
            source: WakeSource::Poll,
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Context wake replayed"),
    };
    assert_eq!(woken.job.state, JobState::Ready.as_str());
    let resumed = claim(&repository, &fixture, job_id.clone(), 0x170).await;
    assert_eq!(resumed.claimed.job.attempt_no, 1);
    sqlx::query(
        "UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(job_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    let recovered = repository
        .drive_expired_context_jobs(DriveExpiredContextJobs {
            shard: SafetyScanShard::whole(),
            after: None,
            limit: 1,
            retry_backoff_milliseconds: 5,
            slots: vec![ExpiredContextRecoverySlot {
                quota_entry_ids: [
                    id(ResourceKind::QuotaLedgerEntry, 0x183),
                    id(ResourceKind::QuotaLedgerEntry, 0x184),
                    id(ResourceKind::QuotaLedgerEntry, 0x185),
                ],
                event_id: id(ResourceKind::Event, 0x186),
                outbox_id: id(ResourceKind::OutboxEvent, 0x187),
            }],
        })
        .await
        .unwrap();
    assert_eq!(recovered.records.len(), 1);
    assert_eq!(
        recovered.records[0].query.state,
        ContextQueryState::RetryScheduled
    );
    assert_eq!(recovered.records[0].job.state, JobState::RetryScheduled.as_str());
    assert!(recovered.records[0].job.worker_id.is_none());
    assert!(recovered.records[0].job.quota_reservation_id.is_none());
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let replayed = claim(&repository, &fixture, job_id.clone(), 0x1a0).await;
    assert_eq!(replayed.claimed.job.attempt_no, 2);
    park_direct_context_leaf(
        &pool,
        &fixture,
        &created.context_query_id,
        &job_id,
        1,
    )
    .await;

    let completed_output = output(
        &fixture,
        &replayed.claimed.query,
        Utc::now(),
        id(ResourceKind::RunValue, 0x182),
    );
    let expected_result_bytes = completed_output.observation.total_bytes;
    let completion_command = CommitContextOutcome {
            audit: worker_audit(&fixture.tenant_id, &replayed.worker_id, 0x190, "complete"),
            context_query_id: created.context_query_id.clone(),
            expected_query_version: replayed.claimed.query.version,
            job_id: job_id.clone(),
            fence: replayed.fence(),
            outcome: ContextBackendOutcome::Completed(Box::new(completed_output)),
            quota_entry_ids: [
                id(ResourceKind::QuotaLedgerEntry, 0x193),
                id(ResourceKind::QuotaLedgerEntry, 0x194),
                id(ResourceKind::QuotaLedgerEntry, 0x195),
            ],
            resume_mutations: Some(resume_mutations(0x920)),
            failure_mutations: None,
        };
    let completed = match execute_outcome(&repository, completion_command.clone())
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Context completion replayed"),
    };
    assert_eq!(completed.query.state, ContextQueryState::Succeeded);
    assert_eq!(completed.job.state, JobState::Succeeded.as_str());
    assert_eq!(completed.job.attempt_no, 2);
    assert!(matches!(
        execute_outcome(&repository, completion_command).await.unwrap(),
        CommandOutcome::Replayed(record) if record == completed
    ));
    let terminal_atomic: (String, String, String, String, String, i64) = sqlx::query_as(
        r#"
        SELECT run.state, leaf.state, continuation.state, continuation.plan_node_key,
               job.state,
               (SELECT count(*) FROM insight_platform.run_values
                WHERE tenant_id = $1 AND value_id = $7
                  AND schema_digest = $8 AND node_id = $3)
        FROM insight_platform.runs AS run
        JOIN insight_platform.run_nodes AS leaf
          ON leaf.tenant_id = run.tenant_id AND leaf.node_id = $3
        JOIN insight_platform.run_nodes AS continuation
          ON continuation.tenant_id = run.tenant_id AND continuation.node_id = $4
        JOIN insight_platform.jobs AS job
          ON job.tenant_id = run.tenant_id AND job.job_id = $5 AND job.node_id = $4
        WHERE run.tenant_id = $1 AND run.run_id = $2
          AND continuation.parent_node_id = $3 AND continuation.scope_id = $6
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(fixture.node_id.to_string())
    .bind(id(ResourceKind::NodeExecution, 0x920).to_string())
    .bind(id(ResourceKind::Job, 0x921).to_string())
    .bind(id(ResourceKind::ScopeInstance, 0x61).to_string())
    .bind(id(ResourceKind::RunValue, 0x182).to_string())
    .bind(named_digest("observation-schema").to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        terminal_atomic,
        (
            "running".to_owned(),
            "succeeded".to_owned(),
            "ready".to_owned(),
            "finish".to_owned(),
            "ready".to_owned(),
            1,
        )
    );

    let valid_plan = text2sql_plan(&fixture, &completed.query);
    let valid_plan_value_id = id(ResourceKind::RunValue, 0x1b0);
    insert_text2sql_plan_value(&pool, &fixture, &valid_plan_value_id, &valid_plan).await;
    let valid_admission = text2sql_invocation_command(&fixture, valid_plan_value_id, 0x1b1);
    let admitted = match execute_admit(&repository, valid_admission.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("fresh Text2SQL invocation replayed"),
    };
    assert_eq!(
        admitted.payload.admission.capability_name.as_str(),
        READONLY_DATABASE_CAPABILITY
    );
    assert_eq!(admitted.payload.admission.effect, Effect::ReadOnly);
    assert_eq!(
        admitted.payload.admission.deployment,
        fixture.readonly_capability_deployment
    );
    assert!(matches!(
        execute_admit(&repository, valid_admission).await.unwrap(),
        CommandOutcome::Replayed(record) if record == admitted
    ));

    let mut drifted_plan = valid_plan;
    drifted_plan.catalog_observation_digest = named_digest("drifted-catalog-observation");
    drifted_plan.canonical_digest = digest_without_field(&drifted_plan, "canonical_digest");
    let drifted_value_id = id(ResourceKind::RunValue, 0x1c0);
    insert_text2sql_plan_value(&pool, &fixture, &drifted_value_id, &drifted_plan).await;
    let drifted_admission = text2sql_invocation_command(&fixture, drifted_value_id, 0x1c1);
    let drifted_invocation_id = drifted_admission.invocation_id.clone();
    let drifted_receipt_id = drifted_admission.audit.receipt_id.clone();
    assert!(matches!(
        execute_admit(&repository, drifted_admission).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let drifted_durable_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT (SELECT count(*) FROM insight_platform.invocations
                 WHERE tenant_id = $1 AND invocation_id = $2)
             + (SELECT count(*) FROM insight_platform.receipts
                 WHERE tenant_id = $1 AND receipt_id = $3)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(drifted_invocation_id.to_string())
    .bind(drifted_receipt_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(drifted_durable_rows, 0);

    let stale_wake = match execute_wake(
        &repository,
        WakeContextDispatch {
            audit: signal_audit(&fixture.tenant_id, 0x198, "stale-wake"),
            context_query_id: created.context_query_id.clone(),
            expected_query_version: deferred.query.version,
            job_id: job_id.clone(),
            expected_wake_generation: u64::try_from(deferred.job.wake_generation).unwrap(),
            source: WakeSource::Callback,
        },
    )
    .await
    .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first stale Context wake replayed"),
    };
    assert_eq!(stale_wake.query, completed.query);
    assert_eq!(stale_wake.job, completed.job);
    let stale_wake_receipt: (String, Option<String>) = sqlx::query_as(
        r#"
        SELECT state, disposition
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_id = $2 AND receipt_kind = 'callback'
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(id(ResourceKind::Receipt, 0x198).to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stale_wake_receipt.0, "succeeded");
    assert_eq!(stale_wake_receipt.1.as_deref(), Some("rejected_stale"));

    let stale_receipt = id(ResourceKind::Receipt, 0x1a0);
    assert!(matches!(
        execute_outcome(
            &repository,
            CommitContextOutcome {
                audit: worker_audit(&fixture.tenant_id, &first_claim.worker_id, 0x1a0, "stale",),
                context_query_id: created.context_query_id.clone(),
                expected_query_version: first_claim.claimed.query.version,
                job_id: job_id.clone(),
                fence: first_claim.fence(),
                outcome: ContextBackendOutcome::Completed(Box::new(output(
                    &fixture,
                    &first_claim.claimed.query,
                    Utc::now(),
                    id(ResourceKind::RunValue, 0x1a3),
                ))),
                quota_entry_ids: [
                    id(ResourceKind::QuotaLedgerEntry, 0x1a4),
                    id(ResourceKind::QuotaLedgerEntry, 0x1a5),
                    id(ResourceKind::QuotaLedgerEntry, 0x1a6),
                ],
                resume_mutations: Some(resume_mutations(0x930)),
                failure_mutations: None,
            },
        )
        .await,
        Err(RepositoryError::Conflict("Context outcome fence"))
    ));
    let stale_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(stale_receipt.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stale_receipts, 0);

    let settled: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT metric, reserved_value, used_value
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND work_class = 'context'
        ORDER BY metric
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(settled
        .iter()
        .all(|(_, reserved_value, _)| *reserved_value == 0));
    assert_eq!(
        settled
            .iter()
            .find(|(metric, _, _)| metric == QuotaDimension::ContextQueries.as_str())
            .unwrap()
            .2,
        2
    );
    assert_eq!(
        u64::try_from(
            settled
                .iter()
                .find(|(metric, _, _)| metric == QuotaDimension::ContextResultBytes.as_str())
                .unwrap()
                .2,
        )
        .unwrap(),
        expected_result_bytes
    );
    assert_eq!(
        settled
            .iter()
            .find(|(metric, _, _)| {
                metric == QuotaDimension::WorkClassConcurrentOperations.as_str()
            })
            .unwrap()
            .2,
        0
    );

    let durable_pairs: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM insight_platform.events AS event
        JOIN insight_platform.outbox_events AS outbox
          ON outbox.tenant_id = event.tenant_id AND outbox.event_id = event.event_id
        WHERE event.tenant_id = $1 AND event.event_type LIKE 'context.%'
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(durable_pairs >= 6);

    let (_fence, owner_command) =
        seed_running_context_orchestration(&pool, &repository, &fixture).await;
    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let rolled_back = scheduler
        .defer_orchestration_to_context_query(owner_command.clone())
        .await
        .unwrap();
    assert!(matches!(rolled_back, CommandOutcome::Applied(_)));
    scheduler.rollback().await.unwrap();
    let rolled_back_queries: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(owner_command.context_query_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rolled_back_queries, 0);

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let applied = scheduler
        .defer_orchestration_to_context_query(owner_command.clone())
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    let CommandOutcome::Applied(applied) = applied else {
        panic!("first orchestration-owned Context dispatch replayed");
    };
    assert_eq!(applied.run.state, "waiting");
    assert_eq!(applied.run.active_work_count, 0);
    assert_eq!(applied.source_job.state, "succeeded");
    assert_eq!(applied.context_job.state, "ready");
    assert_eq!(applied.query.state, ContextQueryState::Ready);
    assert_eq!(applied.settled_quota_account_ids.len(), 1);

    let mut scheduler = repository.begin_scheduler_transaction().await.unwrap();
    let replayed = scheduler
        .defer_orchestration_to_context_query(owner_command)
        .await
        .unwrap();
    scheduler.commit().await.unwrap();
    assert!(matches!(
        replayed,
        CommandOutcome::Replayed(record)
            if record.query.context_query_id == applied.query.context_query_id
                && record.context_job.job_id == applied.context_job.job_id
    ));
    let owner_atomic: (String, String, String, i64, i64) = sqlx::query_as(
        r#"
        SELECT run.state, node.state, source.state,
               (SELECT count(*) FROM insight_platform.invocations
                WHERE tenant_id = $1 AND invocation_id = $5),
               (SELECT count(*) FROM insight_platform.receipts
                WHERE tenant_id = $1 AND receipt_id IN ($6, $7, $8) AND state = 'succeeded')
        FROM insight_platform.runs AS run
        JOIN insight_platform.run_nodes AS node
          ON node.tenant_id = run.tenant_id AND node.node_id = $3
        JOIN insight_platform.jobs AS source
          ON source.tenant_id = run.tenant_id AND source.job_id = $4
        WHERE run.tenant_id = $1 AND run.run_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(id(ResourceKind::NodeExecution, 0x804).to_string())
    .bind(id(ResourceKind::Job, 0x802).to_string())
    .bind(id(ResourceKind::ContextQuery, 0x821).to_string())
    .bind(id(ResourceKind::Receipt, 0x810).to_string())
    .bind(id(ResourceKind::Receipt, 0x81b).to_string())
    .bind(id(ResourceKind::Receipt, 0x81e).to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        owner_atomic,
        (
            "waiting".to_owned(),
            "waiting".to_owned(),
            "succeeded".to_owned(),
            1,
            3
        )
    );
    let owner_claim = claim(
        &repository,
        &fixture,
        applied.context_job.job_id.parse().unwrap(),
        0x830,
    )
    .await;
    let terminal_failure = Failure {
        code: FailureCode::Platform {
            code: PlatformFailureCode::ContextQueryFailed,
        },
        class: FailureClass::External,
        retryability: Retryability::Never,
        safe_message: Some("context source rejected the query".to_owned()),
        details_ref: None,
        source: FailureSource::Context,
    };
    let failure_command = CommitContextOutcome {
        audit: worker_audit(
            &fixture.tenant_id,
            &owner_claim.worker_id,
            0x840,
            "permanent-failure",
        ),
        context_query_id: applied.query.context_query_id.clone(),
        expected_query_version: owner_claim.claimed.query.version,
        job_id: applied.context_job.job_id.parse().unwrap(),
        fence: owner_claim.fence(),
        outcome: ContextBackendOutcome::PermanentFailure {
            failure: terminal_failure.clone(),
        },
        quota_entry_ids: [
            id(ResourceKind::QuotaLedgerEntry, 0x843),
            id(ResourceKind::QuotaLedgerEntry, 0x844),
            id(ResourceKind::QuotaLedgerEntry, 0x845),
        ],
        resume_mutations: None,
        failure_mutations: Some(owner_claim.claimed.failure_mutations.clone()),
    };
    let failed = match execute_outcome(&repository, failure_command.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first Context permanent failure replayed"),
    };
    assert_eq!(failed.query.state, ContextQueryState::Failed);
    assert_eq!(failed.job.state, JobState::Failed.as_str());
    assert!(matches!(
        execute_outcome(&repository, failure_command).await.unwrap(),
        CommandOutcome::Replayed(record) if record == failed
    ));
    let failure_handoff: (String, String, i32, serde_json::Value) = sqlx::query_as(
        r#"
        SELECT node.state, job.state, run.active_work_count, job.payload
        FROM insight_platform.runs AS run
        JOIN insight_platform.run_nodes AS node
          ON node.tenant_id = run.tenant_id AND node.node_id = $3
        JOIN insight_platform.jobs AS job
          ON job.tenant_id = run.tenant_id AND job.job_id = $4 AND job.node_id = node.node_id
        WHERE run.tenant_id = $1 AND run.run_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .bind(id(ResourceKind::NodeExecution, 0x804).to_string())
    .bind(
        owner_claim
            .claimed
            .failure_mutations
            .convergence_job_id
            .to_string(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(failure_handoff.0, "ready");
    assert_eq!(failure_handoff.1, "ready");
    assert_eq!(failure_handoff.2, 0);
    assert_eq!(
        failure_handoff.3.get("convergence_failure"),
        Some(&serde_json::to_value(terminal_failure).unwrap())
    );
        }))
        .unwrap();
}

struct EmptyModelWire;

struct TypedPlanArtifactBroker {
    bytes: Vec<u8>,
    reads: AtomicUsize,
}

#[async_trait::async_trait]
impl SchedulerTypedPlanResponseBroker for TypedPlanArtifactBroker {
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
impl SchedulerRunValueResponseBroker for TypedPlanArtifactBroker {
    async fn read_run_value_for_response(
        &self,
        _request: SchedulerRunValueReadRequest,
    ) -> Result<LeasedArtifactBytes, SchedulerRunValueReadError> {
        Err(SchedulerRunValueReadError::Denied)
    }
}

#[async_trait::async_trait]
impl SchedulerSkillPackageResponseBroker for TypedPlanArtifactBroker {
    async fn read_skill_package_for_response(
        &self,
        _request: SchedulerSkillPackageReadRequest,
    ) -> Result<LeasedArtifactBytes, SchedulerSkillPackageReadError> {
        Err(SchedulerSkillPackageReadError::Denied)
    }
}

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

struct EmptyHttpTransport;

#[async_trait::async_trait]
impl HttpNetworkTransport for EmptyHttpTransport {
    async fn round_trip(
        &self,
        _request: HttpTransportRequest,
    ) -> Result<HttpTransportResponse, CapabilityAdapterFailure> {
        unreachable!("Remote Context qualification must not use Capability HTTP")
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Unsupported)
    }
}

struct EmptyGrpcTransport;

#[async_trait::async_trait]
impl GrpcNetworkTransport for EmptyGrpcTransport {
    async fn unary(
        &self,
        _request: GrpcTransportRequest,
    ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure> {
        unreachable!("Remote Context qualification must not use Capability gRPC")
    }

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Unsupported)
    }
}

struct CountingRemoteContext {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl RemoteContextSearchConnector for CountingRemoteContext {
    async fn query(
        &self,
        request: RemoteContextSearchRequest,
    ) -> Result<RemoteContextSearchResponse, RemoteContextFailure> {
        request.validate_at(Utc::now()).unwrap();
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(RemoteContextSearchResponse {
            schema_version: 1,
            items: vec![RemoteContextItem {
                source_item_identity_digest: named_digest("remote-source-item"),
                content: json!({"item": "authorized remote search entry"}),
                structured_fields: json!({}),
                score_millionths: Some(850_000),
                locator_digest: named_digest("remote-locator"),
                authorization_evidence_digest: named_digest("remote-authorization"),
                display_label: "authorized remote search entry".to_owned(),
                classification: DataClassification::Internal,
            }],
            next_cursor_digest: None,
            backend_request_digest: request.normalized_query_digest,
            backend_response_digest: named_digest("remote-backend-response"),
            ranking_evidence_digest: named_digest("remote-ranking"),
            remote_revision_digest: Some(named_digest("remote-revision")),
            observed_at: Utc::now(),
        })
    }
}

struct ContextProcessMtlsFixture {
    ca_pem: String,
    server_certificate_pem: String,
    server_key_pem: String,
    client_certificate_pem: String,
    client_key_pem: String,
    artifact_server_certificate_pem: String,
    artifact_server_key_pem: String,
    scheduler_certificate_pem: String,
    scheduler_key_pem: String,
}

fn context_process_mtls_fixture() -> ContextProcessMtlsFixture {
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
    let (client_certificate_pem, client_key_pem) = issue(
        vec![SanType::URI(
            CONTEXT_WORKER_WORKLOAD_IDENTITY.try_into().unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let (artifact_server_certificate_pem, artifact_server_key_pem) = issue(
        vec![SanType::DnsName("artifact.test".try_into().unwrap())],
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (scheduler_certificate_pem, scheduler_key_pem) = issue(
        vec![SanType::URI(
            SCHEDULER_WORKLOAD_IDENTITY.try_into().unwrap(),
        )],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    ContextProcessMtlsFixture {
        ca_pem: ca.pem(),
        server_certificate_pem,
        server_key_pem,
        client_certificate_pem,
        client_key_pem,
        artifact_server_certificate_pem,
        artifact_server_key_pem,
        scheduler_certificate_pem,
        scheduler_key_pem,
    }
}

#[test]
fn production_native_context_worker_recovers_commit_window_process_loss() {
    let (Ok(database_url), Ok(binary)) = (
        std::env::var("PLATFORM_CONTEXT_WORKER_TEST_DATABASE_URL"),
        std::env::var("PLATFORM_CONTEXT_WORKER_BIN"),
    ) else {
        eprintln!("Context Worker binary or dedicated PostgreSQL fixture is unset; process recovery skipped");
        return;
    };
    let _fixture_namespace = select_fixture_namespace(0xa0f7);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
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
        let installed_adapter_digest = named_digest("production-native-context-adapter");
        let adapter_contract_digest = named_digest("production-native-context-contract");
        let fixture = seed_fixture_with_native_adapter(
            &pool,
            &repository,
            Some((
                installed_adapter_digest.clone(),
                adapter_contract_digest.clone(),
            )),
        )
        .await;
        let command = create_command(&fixture, 0x900);
        let created = match execute_create(&repository, command).await.unwrap() {
            CommandOutcome::Applied(record) => record,
            CommandOutcome::Replayed(_) => panic!("fresh native Context create replayed"),
        };
        let job_id = id(ResourceKind::Job, 0x920);
        match execute_prepare(
            &repository,
            PrepareContextDispatch {
                audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x921, "native-prepare"),
                context_query_id: created.context_query_id.clone(),
                expected_query_version: created.version,
                job_id: job_id.clone(),
                scheduled_at: Utc::now() + Duration::seconds(5),
            },
        )
        .await
        .unwrap()
        {
            CommandOutcome::Applied(_) => {}
            CommandOutcome::Replayed(_) => panic!("fresh native Context prepare replayed"),
        }
        park_direct_context_leaf(
            &pool,
            &fixture,
            &created.context_query_id,
            &job_id,
            0,
        )
        .await;

        let config = native_context_process_config(
            installed_adapter_digest.clone(),
            adapter_contract_digest,
        );
        let prefix = PathBuf::from(format!(
            "/tmp/platform-context-worker-process-{}",
            std::process::id()
        ));
        let (config_path, config_digest) = write_context_process_config(&prefix, "correct", &config);
        let mut wrong_config = config.clone();
        let wrong_digest = named_digest("wrong-installed-native-context-adapter");
        wrong_config["worker_manifest"]["adapter_runtime_digest"] =
            serde_json::to_value(wrong_digest.clone()).unwrap();
        wrong_config["native_catalog"]["installed_adapter_digest"] =
            serde_json::to_value(wrong_digest).unwrap();
        let (wrong_path, wrong_config_digest) =
            write_context_process_config(&prefix, "wrong", &wrong_config);

        let mut wrong = spawn_context_worker(
            &binary,
            &wrong_path,
            &wrong_config_digest,
            &database_url,
        );
        let wrong_log = observe_context_worker_start(&mut wrong);
        assert!(wrong.try_wait().unwrap().is_none());
        tokio::time::sleep(StdDuration::from_millis(300)).await;
        let unclaimed: (String, i32, bool, bool) = sqlx::query_as(
            r#"
            SELECT state, attempt_no, worker_id IS NULL, quota_reservation_id IS NULL
            FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(unclaimed, ("ready".to_owned(), 0, true, true));
        wrong.kill().unwrap();
        wrong.wait().unwrap();
        wrong_log.join().unwrap();

        let mut first =
            spawn_context_worker(&binary, &config_path, &config_digest, &database_url);
        let first_log = observe_context_worker_start(&mut first);
        assert!(first.try_wait().unwrap().is_none());
        sqlx::query(
            r#"
            CREATE FUNCTION insight_platform.test_pause_context_commit()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF OLD.state = 'running' AND NEW.state = 'succeeded' AND NEW.work_class = 'context' THEN
                PERFORM pg_sleep(30);
              END IF;
              RETURN NEW;
            END
            $$
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("CREATE TRIGGER test_pause_context_commit BEFORE UPDATE ON insight_platform.jobs FOR EACH ROW EXECUTE FUNCTION insight_platform.test_pause_context_commit()")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE insight_platform.jobs SET scheduled_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'ready'")
            .bind(fixture.tenant_id.to_string())
            .bind(job_id.to_string())
            .execute(&pool)
            .await
            .unwrap();
        wait_context_job_state(&pool, &job_id, "running").await;
        tokio::time::sleep(StdDuration::from_millis(100)).await;
        first.kill().unwrap();
        first.wait().unwrap();
        first_log.join().unwrap();
        sqlx::query("DROP TRIGGER test_pause_context_commit ON insight_platform.jobs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP FUNCTION insight_platform.test_pause_context_commit()")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'")
            .bind(fixture.tenant_id.to_string())
            .bind(job_id.to_string())
            .execute(&pool)
            .await
            .unwrap();

        let mut second =
            spawn_context_worker(&binary, &config_path, &config_digest, &database_url);
        let second_log = observe_context_worker_start(&mut second);
        assert!(second.try_wait().unwrap().is_none());
        wait_context_query_state(&pool, &created.context_query_id, "succeeded").await;
        let evidence: (i64, i32, bool, i64, String, i32, String, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT count(*) FROM insight_platform.events
                 WHERE tenant_id = $1 AND aggregate_id = $2
                   AND event_type = 'context.lease_recovered'),
              job.attempt_no,
              job.worker_id IS NULL AND job.lease_token_digest IS NULL
                AND job.quota_reservation_id IS NULL,
              (SELECT count(*) FROM insight_platform.run_values
                 WHERE tenant_id = $1 AND run_id = $3
                   AND value_kind = 'context_observation'),
              (SELECT state FROM insight_platform.runs
                 WHERE tenant_id = $1 AND run_id = $3),
              (SELECT active_work_count FROM insight_platform.runs
                 WHERE tenant_id = $1 AND run_id = $3),
              (SELECT state FROM insight_platform.run_nodes
                 WHERE tenant_id = $1 AND node_id = $5),
              (SELECT count(*)
                 FROM insight_platform.run_nodes AS resume
                 JOIN insight_platform.jobs AS resume_job
                   ON resume_job.tenant_id = resume.tenant_id
                  AND resume_job.node_id = resume.node_id
                WHERE resume.tenant_id = $1 AND resume.run_id = $3
                  AND resume.plan_node_key = 'finish'
                  AND resume.node_kind = 'return'
                  AND resume.state = 'ready' AND resume_job.state = 'ready')
            FROM insight_platform.jobs AS job
            WHERE job.tenant_id = $1 AND job.job_id = $4
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(created.context_query_id.to_string())
        .bind(fixture.run_id.to_string())
        .bind(job_id.to_string())
        .bind(fixture.node_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            evidence,
            (
                1,
                2,
                true,
                1,
                "running".to_owned(),
                0,
                "succeeded".to_owned(),
                1,
            )
        );
        second.kill().unwrap();
        second.wait().unwrap();
        second_log.join().unwrap();
        for path in [wrong_path, config_path] {
            std::fs::remove_file(path).unwrap();
        }
    });
}

#[test]
fn production_remote_context_worker_recovers_mtls_response_commit_window() {
    let (Ok(database_url), Ok(binary), Ok(orchestration_binary)) = (
        std::env::var("PLATFORM_REMOTE_CONTEXT_WORKER_TEST_DATABASE_URL"),
        std::env::var("PLATFORM_REMOTE_CONTEXT_WORKER_BIN"),
        std::env::var("PLATFORM_ORCHESTRATION_WORKER_BIN"),
    ) else {
        eprintln!(
            "Remote Context/Orchestration Worker binary or dedicated PostgreSQL fixture is unset; process recovery skipped"
        );
        return;
    };
    let _fixture_namespace = select_fixture_namespace(0xa0f8);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
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
        let tls = context_process_mtls_fixture();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let remote = Arc::new(CountingRemoteContext {
            calls: AtomicUsize::new(0),
        });
        let service = EgressBrokerServiceServer::new(
            EgressBrokerGrpcService::new(
                Arc::new(EmptyModelWire),
                Arc::new(EmptyHttpTransport),
                Arc::new(EmptyGrpcTransport),
                EgressInternalRpcLimits::new(65_536, 1_048_576).unwrap(),
            )
            .with_remote_context(remote.clone()),
        );
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            EgressCallerWorkloadIdentity,
        );
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

        let installed_adapter_digest = named_digest("production-remote-context-adapter");
        let fixture = seed_fixture_with_backend(
            &pool,
            &repository,
            ContextFixtureBackend::RemoteSearch {
                endpoint: CanonicalHttpEndpoint {
                    scheme: CapabilityEndpointScheme::Https,
                    host: "search.example.test".to_owned(),
                    port: 443,
                    base_path: "/v1/query".to_owned(),
                },
                protocol_contract_digest: named_digest("remote-protocol-contract"),
                result_mapping_digest: named_digest("remote-result-mapping"),
                installed_adapter_digest: installed_adapter_digest.clone(),
            },
        )
        .await;
        let command = create_command(&fixture, 0x980);
        let created = match execute_create(&repository, command).await.unwrap() {
            CommandOutcome::Applied(record) => record,
            CommandOutcome::Replayed(_) => panic!("fresh remote Context create replayed"),
        };
        let job_id = id(ResourceKind::Job, 0x990);
        match execute_prepare(
            &repository,
            PrepareContextDispatch {
                audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x991, "remote-prepare"),
                context_query_id: created.context_query_id.clone(),
                expected_query_version: created.version,
                job_id: job_id.clone(),
                scheduled_at: Utc::now() + Duration::seconds(5),
            },
        )
        .await
        .unwrap()
        {
            CommandOutcome::Applied(_) => {}
            CommandOutcome::Replayed(_) => panic!("fresh remote Context prepare replayed"),
        }
        park_direct_context_leaf(&pool, &fixture, &created.context_query_id, &job_id, 0).await;

        let prefix = PathBuf::from(format!(
            "/tmp/platform-remote-context-worker-process-{}",
            std::process::id()
        ));
        let ca_path = PathBuf::from(format!("{}-ca.pem", prefix.display()));
        let cert_path = PathBuf::from(format!("{}-client.pem", prefix.display()));
        let key_path = PathBuf::from(format!("{}-client-key.pem", prefix.display()));
        std::fs::write(&ca_path, &tls.ca_pem).unwrap();
        std::fs::write(&cert_path, &tls.client_certificate_pem).unwrap();
        std::fs::write(&key_path, &tls.client_key_pem).unwrap();
        let config = remote_context_process_config(
            installed_adapter_digest.clone(),
            format!("https://{address}/"),
        );
        let (config_path, config_digest) =
            write_context_process_config(&prefix, "remote-correct", &config);
        let mut wrong_config = config.clone();
        let wrong_adapter_digest = named_digest("wrong-production-remote-context-adapter");
        wrong_config["installed_adapter_digest"] =
            serde_json::to_value(wrong_adapter_digest.clone()).unwrap();
        wrong_config["worker_manifest"]["adapter_runtime_digest"] =
            serde_json::to_value(wrong_adapter_digest).unwrap();
        let (wrong_path, wrong_digest) =
            write_context_process_config(&prefix, "remote-wrong", &wrong_config);

        let mut wrong = spawn_remote_context_worker(
            &binary,
            &wrong_path,
            &wrong_digest,
            &database_url,
            &ca_path,
            &cert_path,
            &key_path,
        );
        let wrong_log = observe_context_worker_start(&mut wrong);
        tokio::time::sleep(StdDuration::from_millis(300)).await;
        let unclaimed: (String, i32, bool, bool) = sqlx::query_as(
            r#"
            SELECT state, attempt_no, worker_id IS NULL, quota_reservation_id IS NULL
            FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(unclaimed, ("ready".to_owned(), 0, true, true));
        assert_eq!(remote.calls.load(Ordering::SeqCst), 0);
        wrong.kill().unwrap();
        wrong.wait().unwrap();
        wrong_log.join().unwrap();

        let mut first = spawn_remote_context_worker(
            &binary,
            &config_path,
            &config_digest,
            &database_url,
            &ca_path,
            &cert_path,
            &key_path,
        );
        let first_log = observe_context_worker_start(&mut first);
        sqlx::query(
            r#"
            CREATE FUNCTION insight_platform.test_pause_remote_context_commit()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF OLD.state = 'running' AND NEW.state = 'succeeded' AND NEW.work_class = 'context' THEN
                PERFORM pg_sleep(30);
              END IF;
              RETURN NEW;
            END
            $$
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("CREATE TRIGGER test_pause_remote_context_commit BEFORE UPDATE ON insight_platform.jobs FOR EACH ROW EXECUTE FUNCTION insight_platform.test_pause_remote_context_commit()")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE insight_platform.jobs SET scheduled_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'ready'")
            .bind(fixture.tenant_id.to_string())
            .bind(job_id.to_string())
            .execute(&pool)
            .await
            .unwrap();
        wait_remote_context_calls(&remote.calls, 1).await;
        wait_context_job_state(&pool, &job_id, "running").await;
        tokio::time::sleep(StdDuration::from_millis(100)).await;
        first.kill().unwrap();
        first.wait().unwrap();
        first_log.join().unwrap();
        sqlx::query("DROP TRIGGER test_pause_remote_context_commit ON insight_platform.jobs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP FUNCTION insight_platform.test_pause_remote_context_commit()")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE insight_platform.jobs SET heartbeat_at = clock_timestamp() - interval '2 seconds', lease_expires_at = clock_timestamp() - interval '1 second' WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'")
            .bind(fixture.tenant_id.to_string())
            .bind(job_id.to_string())
            .execute(&pool)
            .await
            .unwrap();

        let mut second = spawn_remote_context_worker(
            &binary,
            &config_path,
            &config_digest,
            &database_url,
            &ca_path,
            &cert_path,
            &key_path,
        );
        let second_log = observe_context_worker_start(&mut second);
        wait_context_query_state(&pool, &created.context_query_id, "succeeded").await;
        wait_remote_context_calls(&remote.calls, 2).await;
        let evidence: (i64, i32, bool, i64, String, i32, String, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT count(*) FROM insight_platform.events
                 WHERE tenant_id = $1 AND aggregate_id = $2
                   AND event_type = 'context.lease_recovered'),
              job.attempt_no,
              job.worker_id IS NULL AND job.lease_token_digest IS NULL
                AND job.quota_reservation_id IS NULL,
              (SELECT count(*) FROM insight_platform.run_values
                 WHERE tenant_id = $1 AND run_id = $3
                   AND value_kind = 'context_observation'),
              (SELECT state FROM insight_platform.runs
                 WHERE tenant_id = $1 AND run_id = $3),
              (SELECT active_work_count FROM insight_platform.runs
                 WHERE tenant_id = $1 AND run_id = $3),
              (SELECT state FROM insight_platform.run_nodes
                 WHERE tenant_id = $1 AND node_id = $5),
              (SELECT count(*)
                 FROM insight_platform.run_nodes AS resume
                 JOIN insight_platform.jobs AS resume_job
                   ON resume_job.tenant_id = resume.tenant_id
                  AND resume_job.node_id = resume.node_id
                WHERE resume.tenant_id = $1 AND resume.run_id = $3
                  AND resume.plan_node_key = 'finish'
                  AND resume.node_kind = 'return'
                  AND resume.state = 'ready' AND resume_job.state = 'ready')
            FROM insight_platform.jobs AS job
            WHERE job.tenant_id = $1 AND job.job_id = $4
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(created.context_query_id.to_string())
        .bind(fixture.run_id.to_string())
        .bind(job_id.to_string())
        .bind(fixture.node_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            evidence,
            (
                1,
                2,
                true,
                1,
                "running".to_owned(),
                0,
                "succeeded".to_owned(),
                1,
            )
        );
        assert_eq!(remote.calls.load(Ordering::SeqCst), 2);
        second.kill().unwrap();
        second.wait().unwrap();
        second_log.join().unwrap();

        sqlx::query(
            r#"
            UPDATE insight_platform.run_nodes
            SET state = 'cancelled', version = version + 1,
                terminal_at = clock_timestamp(), updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND node_id = $2 AND state = 'running'
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.text2sql_node_id.to_string())
        .execute(&pool)
        .await
        .unwrap();

        let plan_bytes = canonical_json(&serde_json::to_value(&fixture.runtime_plan).unwrap())
            .unwrap();
        let plan_broker = Arc::new(TypedPlanArtifactBroker {
            bytes: plan_bytes,
            reads: AtomicUsize::new(0),
        });
        let artifact_incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let artifact_address = artifact_incoming.local_addr().unwrap();
        let artifact_service = ArtifactSchedulerServiceServer::new(
            ArtifactSchedulerGrpcService::new(
                plan_broker.clone(),
                ArtifactInternalRpcLimits::new(262_144, 262_144).unwrap(),
            ),
        );
        let artifact_service = tonic::service::interceptor::InterceptedService::new(
            artifact_service,
            SchedulerWorkloadIdentity,
        );
        let (artifact_shutdown_sender, artifact_shutdown_receiver) = tokio::sync::oneshot::channel();
        let artifact_server = tokio::spawn(
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(
                            &tls.artifact_server_certificate_pem,
                            &tls.artifact_server_key_pem,
                        ))
                        .client_ca_root(Certificate::from_pem(tls.ca_pem.clone())),
                )
                .unwrap()
                .add_service(artifact_service)
                .serve_with_incoming_shutdown(artifact_incoming, async move {
                    let _ = artifact_shutdown_receiver.await;
                }),
        );
        let scheduler_cert_path =
            PathBuf::from(format!("{}-scheduler-client.pem", prefix.display()));
        let scheduler_key_path =
            PathBuf::from(format!("{}-scheduler-client-key.pem", prefix.display()));
        std::fs::write(&scheduler_cert_path, &tls.scheduler_certificate_pem).unwrap();
        std::fs::write(&scheduler_key_path, &tls.scheduler_key_pem).unwrap();
        let orchestration_config = orchestration_process_config(format!(
            "https://{artifact_address}/"
        ));
        let (orchestration_config_path, orchestration_config_digest) =
            write_context_process_config(&prefix, "orchestration", &orchestration_config);
        let mut orchestration = spawn_orchestration_worker(
            &orchestration_binary,
            &orchestration_config_path,
            &orchestration_config_digest,
            &database_url,
            &ca_path,
            &scheduler_cert_path,
            &scheduler_key_path,
        );
        let orchestration_log = observe_context_worker_start(&mut orchestration);
        let running_resume = loop {
            let row = sqlx::query_as::<_, (String, String, i64, String)>(
                r#"
                SELECT resume_job.job_id, resume_job.worker_id,
                       resume_job.lease_epoch, resume_job.lease_token_digest
                FROM insight_platform.run_nodes AS resume
                JOIN insight_platform.jobs AS resume_job
                  ON resume_job.tenant_id = resume.tenant_id
                 AND resume_job.node_id = resume.node_id
                WHERE resume.tenant_id = $1 AND resume.run_id = $2
                  AND resume.plan_node_key = 'finish'
                  AND resume_job.state = 'running'
                "#,
            )
            .bind(fixture.tenant_id.to_string())
            .bind(fixture.run_id.to_string())
            .fetch_optional(&pool)
            .await
            .unwrap();
            if let Some(row) = row {
                break row;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        };
        let exact_plan_read = repository
            .resolve_typed_plan_read(SchedulerTypedPlanLease {
                tenant_id: fixture.tenant_id.clone(),
                run_id: fixture.run_id.clone(),
                orchestration_job_id: running_resume.0.parse().unwrap(),
                worker_process_generation_id: running_resume.1.parse().unwrap(),
                lease_generation: u64::try_from(running_resume.2).unwrap(),
                lease_token_digest: running_resume.3.parse().unwrap(),
                request_digest: named_digest("orchestration-plan-read-diagnostic"),
                maximum_bytes: 1_048_576,
                deadline: Utc::now() + Duration::seconds(1),
            })
            .await
            .unwrap();
        assert_eq!(
            exact_plan_read.artifact.content_digest(),
            &fixture
                .runtime_plan
                .canonical_digest(
                    PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap(),
                )
                .unwrap()
        );
        wait_remote_context_calls(&plan_broker.reads, 1).await;
        wait_run_state(&pool, &fixture.run_id, "succeeded").await;
        let terminal: (String, String, String, bool, i64) = sqlx::query_as(
            r#"
            SELECT run.state, resume.state, resume_job.state,
                   run.output_value_id IS NOT NULL,
                   (SELECT count(*) FROM insight_platform.run_nodes
                      WHERE tenant_id = run.tenant_id AND run_id = run.run_id
                        AND plan_node_key = 'finish' AND node_kind = 'return')
            FROM insight_platform.runs AS run
            JOIN insight_platform.run_nodes AS resume
              ON resume.tenant_id = run.tenant_id AND resume.run_id = run.run_id
             AND resume.plan_node_key = 'finish' AND resume.node_kind = 'return'
            JOIN insight_platform.jobs AS resume_job
              ON resume_job.tenant_id = resume.tenant_id AND resume_job.node_id = resume.node_id
            WHERE run.tenant_id = $1 AND run.run_id = $2
            "#,
        )
        .bind(fixture.tenant_id.to_string())
        .bind(fixture.run_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            terminal,
            (
                "succeeded".to_owned(),
                "succeeded".to_owned(),
                "succeeded".to_owned(),
                true,
                1,
            )
        );
        assert!(plan_broker.reads.load(Ordering::SeqCst) >= 1);
        orchestration.kill().unwrap();
        orchestration.wait().unwrap();
        orchestration_log.join().unwrap();
        let _ = artifact_shutdown_sender.send(());
        artifact_server.await.unwrap().unwrap();
        let _ = shutdown_sender.send(());
        server.await.unwrap().unwrap();
        for path in [
            wrong_path,
            config_path,
            ca_path,
            cert_path,
            key_path,
            scheduler_cert_path,
            scheduler_key_path,
            orchestration_config_path,
        ] {
            std::fs::remove_file(path).unwrap();
        }
    });
}

fn native_context_process_config(
    installed_adapter_digest: Sha256Digest,
    adapter_contract_digest: Sha256Digest,
) -> serde_json::Value {
    let manifest = native_context_worker_manifest(installed_adapter_digest.clone());
    json!({
        "schema_version": 1,
        "observability_listen_address": reserve_loopback_address(),
        "worker_manifest": manifest,
        "native_catalog": {
            "schema_version": 1,
            "installed_adapter_digest": installed_adapter_digest,
            "adapter_contract_digest": adapter_contract_digest,
            "source_item_identity_digest": named_digest("native-source-item"),
            "content": "authorized native catalog entry",
            "structured_fields_schema_digest": named_digest("native-structured-fields"),
            "score_millionths": 900000,
            "locator_digest": named_digest("native-locator"),
            "authorization_evidence_digest": named_digest("native-authorization"),
            "ranking_evidence_digest": named_digest("native-ranking"),
            "display_label": "authorized native catalog entry",
            "classification": "internal"
        },
        "database_max_connections": 4,
        "database_acquire_timeout_milliseconds": 1000,
        "receipt_ttl_seconds": 60,
        "scan_interval_milliseconds": 20,
        "failure_backoff_milliseconds": 5,
        "drain_grace_milliseconds": 1000
    })
}

fn remote_context_process_config(
    installed_adapter_digest: Sha256Digest,
    egress_endpoint: String,
) -> serde_json::Value {
    let manifest = native_context_worker_manifest(installed_adapter_digest.clone());
    json!({
        "schema_version": 1,
        "observability_listen_address": reserve_loopback_address(),
        "worker_manifest": manifest,
        "installed_adapter_digest": installed_adapter_digest,
        "egress_endpoint": egress_endpoint,
        "egress_tls_server_name": "egress.test",
        "maximum_rpc_metadata_bytes": 65536,
        "maximum_rpc_payload_bytes": 1048576,
        "connect_timeout_milliseconds": 1000,
        "request_timeout_milliseconds": 30000,
        "database_max_connections": 4,
        "database_acquire_timeout_milliseconds": 1000,
        "receipt_ttl_seconds": 60,
        "scan_interval_milliseconds": 20,
        "failure_backoff_milliseconds": 5,
        "drain_grace_milliseconds": 1000
    })
}

fn reserve_loopback_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn orchestration_process_config(artifact_endpoint: String) -> serde_json::Value {
    json!({
        "schema_version": 1,
        "observability_listen_address": reserve_loopback_address(),
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

fn native_context_worker_manifest(installed_adapter_digest: Sha256Digest) -> WorkerManifest {
    WorkerManifest {
        manifest_version: WORKER_MANIFEST_VERSION,
        worker_role: "context-worker".to_owned(),
        work_class: WorkClass::Context,
        adapter_runtime_digest: installed_adapter_digest,
        protocol_version: WORKER_PROTOCOL_VERSION,
        max_concurrency: 4,
        critical_control_reserved_slots: 1,
    }
}

fn write_context_process_config(
    prefix: &Path,
    suffix: &str,
    value: &serde_json::Value,
) -> (PathBuf, String) {
    let path = PathBuf::from(format!("{}-{suffix}.json", prefix.display()));
    std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    (path, canonical_digest(value).unwrap())
}

fn spawn_context_worker(
    binary: &str,
    config_path: &Path,
    config_digest: &str,
    database_url: &str,
) -> Child {
    std::process::Command::new(binary)
        .env("PLATFORM_CONTEXT_WORKER_CONFIG", config_path)
        .env("PLATFORM_CONTEXT_WORKER_CONFIG_DIGEST", config_digest)
        .env("PLATFORM_CONTEXT_WORKER_DATABASE_URL", database_url)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn spawn_remote_context_worker(
    binary: &str,
    config_path: &Path,
    config_digest: &str,
    database_url: &str,
    ca_path: &Path,
    cert_path: &Path,
    key_path: &Path,
) -> Child {
    std::process::Command::new(binary)
        .env("PLATFORM_REMOTE_CONTEXT_WORKER_CONFIG", config_path)
        .env(
            "PLATFORM_REMOTE_CONTEXT_WORKER_CONFIG_DIGEST",
            config_digest,
        )
        .env("PLATFORM_REMOTE_CONTEXT_WORKER_DATABASE_URL", database_url)
        .env("PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CA_PATH", ca_path)
        .env("PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CERT_PATH", cert_path)
        .env("PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_KEY_PATH", key_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn spawn_orchestration_worker(
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

fn observe_context_worker_start(child: &mut Child) -> std::thread::JoinHandle<()> {
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
        .expect("Context Worker did not report startup");
    assert!(
        matches!(
            first.as_str(),
            "platform-context-worker started"
                | "platform-remote-context-worker started"
                | "platform-orchestration-worker started"
        ),
        "{first}"
    );
    logger
}

async fn wait_remote_context_calls(calls: &AtomicUsize, expected: usize) {
    for _ in 0..400 {
        if calls.load(Ordering::SeqCst) >= expected {
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(25)).await;
    }
    panic!(
        "Remote Context call count did not reach {expected}; observed {}",
        calls.load(Ordering::SeqCst)
    );
}

async fn wait_run_state(pool: &PgPool, run_id: &ResourceId, expected: &str) {
    for _ in 0..400 {
        let state: String =
            sqlx::query_scalar("SELECT state FROM insight_platform.runs WHERE run_id = $1")
                .bind(run_id.to_string())
                .fetch_one(pool)
                .await
                .unwrap();
        if state == expected {
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(25)).await;
    }
    panic!("Run {run_id} did not reach {expected}");
}

async fn context_job_state(pool: &PgPool, job_id: &ResourceId) -> String {
    sqlx::query_scalar("SELECT state FROM insight_platform.jobs WHERE job_id = $1")
        .bind(job_id.to_string())
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn wait_context_job_state(pool: &PgPool, job_id: &ResourceId, expected: &str) {
    let started = std::time::Instant::now();
    while context_job_state(pool, job_id).await != expected {
        assert!(started.elapsed() < StdDuration::from_secs(10));
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

async fn wait_context_query_state(pool: &PgPool, query_id: &ResourceId, expected: &str) {
    let started = std::time::Instant::now();
    loop {
        let state: String = sqlx::query_scalar(
            "SELECT state FROM insight_platform.invocations WHERE invocation_id = $1",
        )
        .bind(query_id.to_string())
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
