use chrono::{DateTime, Duration, Utc};
use insight_platform_context::{
    CitationLocator, ClaimContextJobs, CommitContextDatasetBuild, CommitContextOutcome,
    ContextBackendOutcome, ContextCitation, ContextClaimSlot, ContextDatasetBuildJobPayload,
    ContextItem, ContextObservation, ContextObservationOutput, ContextQueryRequest,
    ContextQueryTransaction, ContextRetrievalEvidence, ContextSignalAudit, ContextWorkerAudit,
    CreateContextQuery, NormalizedContextScore, PrepareContextDispatch,
    ReadOnlySqlExecutionBinding, ReadOnlySqlPlan, RequestContextDatasetBuild, SqlColumnRef,
    SqlComparisonOperator, SqlObjectName, SqlPredicate, SqlProjection, SqlProjectionExpression,
    SqlSource, WakeContextDispatch, READONLY_DATABASE_CAPABILITY, TEXT2SQL_PLAN_VALUE_KIND,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, AdministrativeGate, AgentDeploymentClosure,
    AgentResourceSpec, ArtifactRef, AuthoringPackage, CapabilityArtifactContract,
    CapabilityBackendBinding, CapabilityBackendContract, CapabilityBackendFeatures,
    CapabilityBackendKind, CapabilityBackendLimits, CapabilityCancellationKind,
    CapabilityDataFlowPolicy, CapabilityDeploymentClosure, CapabilityIdempotencyKind,
    CapabilityImplementationResourceSpec, CapabilityInterfaceLimits,
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
    PermissionSet, PlatformFailureCode, PolicyKind, PolicyResourceSpec, PrincipalBindingsPayload,
    PrincipalKind, PrincipalSnapshot, PublishedVersionPayload, QuotaDimension,
    RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind, Retryability,
    RunBindingsSnapshot, SchedulerPriority, Sha256Digest, TenantConfig, TenantPrincipalPayload,
    ValidationSummary, ValueRef, WorkClass, WORKER_PROTOCOL_VERSION,
};
use insight_platform_invocations::{
    AdmitCapabilityInvocation, ExactInvocationValueRef, InvocationOrigin, InvocationPolicyDecision,
    InvocationPolicyDecisionBundle, InvocationPolicyDisposition, InvocationTransaction,
    InvocationValueStorage,
};
use insight_platform_jobs::{JobFence, LeasePolicy, WakeContract, WakeKind, WakeSource};
use insight_platform_orchestrator::{
    DataPortKey, ExactDataPortRef, ExactRunValueRef, OrchestrationJobPayload, PlanLimits,
    PlanNodeKey, RunCurrentSnapshot, RuntimeDependencyKind, RuntimeDependencySlot, RuntimeNode,
    RuntimePlan, ScopeDataEnvironmentSnapshot, ScopeEnvironmentLimits,
};
use insight_platform_postgres::{
    context_query_repository::{ClaimedContextExecution, PreparedContextExecution},
    repository::{
        ClaimJobs, DeferOrchestrationContextMutationIds, DeferOrchestrationToContextQuery,
        JobFence as RepositoryJobFence, NewPrincipal, NewQuotaAccount, NewTenant,
        NewTenantPrincipal, OrchestrationYieldMutationIds, PgRepository, RepositoryError,
        ResolvedExpressionInput, TypedPayload,
    },
    verify_schema,
};
use serde::Serialize;
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::collections::BTreeMap;

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f6{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn named_digest(label: &str) -> Sha256Digest {
    canonical_digest(&json!({"phase3_context": label}))
        .unwrap()
        .parse()
        .unwrap()
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
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": false
    }))
    .unwrap()
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

fn context_runtime_plan(interface_revision_id: ResourceId) -> RuntimePlan {
    let start = PlanNodeKey::new("start".to_owned()).unwrap();
    let catalog = PlanNodeKey::new("catalog".to_owned()).unwrap();
    let finish = PlanNodeKey::new("finish".to_owned()).unwrap();
    let request = ExactDataPortRef::RunInput {
        schema_digest: named_digest("query-schema"),
    };
    let result = ExactDataPortRef::NodeOutput {
        producer_node_id: catalog.clone(),
        port_id: DataPortKey::new("items".to_owned()).unwrap(),
        schema_digest: named_digest("observation-schema"),
    };
    let plan = RuntimePlan {
        plan_version: 4,
        interface_revision_id,
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
    policies: &[ExactVersionRef],
) {
    for (index, exact) in policies.iter().enumerate() {
        insert_version(
            pool,
            tenant_id,
            policy_resource,
            RegistryResourceKind::Policy,
            exact,
            i64::try_from(index + 1).unwrap(),
            principal_id,
            PublishedVersionPayload {
                document: ResourceDocument::Policy(PolicyResourceSpec {
                    authoring_package: authoring(0x90 + u16::try_from(index).unwrap()),
                    contract_digest: named_digest(&format!("policy-contract-{index}")),
                    dependency_versions: vec![],
                    policy_versions: vec![],
                    policy_kind: PolicyKind::Authorization,
                    rules_digest: named_digest(&format!("policy-rules-{index}")),
                    selection: None,
                    scheduling: None,
                    retention: None,
                    mcp_protocol: None,
                    mcp_auth: None,
                    sandbox_isolation: None,
                    sandbox_resource: None,
                    sandbox_network: None,
                    sandbox_artifact_io: None,
                    sandbox_secret_resolution: None,
                }),
                validation: validation(),
            },
        )
        .await;
    }
}

async fn seed_fixture(pool: &PgPool, repository: &PgRepository) -> Fixture {
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
    let policies = vec![
        authorization_policy.clone(),
        ranking_policy.clone(),
        parser_policy.clone(),
        data_policy.clone(),
        entitlement_policy.clone(),
        cache_policy.clone(),
        execution_profile.clone(),
        invocation_policy.clone(),
        chunker_policy.clone(),
    ];
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

    let interface_revision = version(ResourceKind::ContextSourceInterfaceRevision, 0x30);
    let implementation_revision = version(ResourceKind::ContextSourceImplementationRevision, 0x31);
    let query_schema_digest = named_digest("query-schema");
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
    let implementation = ContextImplementationResourceSpec {
        authoring_package: authoring(0xa1),
        contract_digest: named_digest("implementation-contract"),
        dependency_versions: vec![interface_revision.clone()],
        policy_versions: vec![],
        interface_revision: interface_revision.clone(),
        backend_kind: ContextBackendKind::SqlCatalog,
        contract: ContextImplementationContract {
            backend: ContextBackendContract::SqlCatalog {
                dialect: "postgres".to_owned(),
                catalog_projection_digest: catalog_projection_digest.clone(),
            },
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

    let context_closure = ContextDeploymentClosure {
        implementation: implementation_revision,
        interface: interface_revision.clone(),
        backend: ContextBackendBinding::SqlCatalog {
            database_identity_digest: database_identity_digest.clone(),
            dialect: "postgres".to_owned(),
            catalog_scope_digest: named_digest("catalog-scope"),
        },
        secret_bindings: vec![],
        network_policy: None,
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
    let agent_plan = version(ResourceKind::AgentPlanRevision, 0x41);
    let runtime_plan = context_runtime_plan(agent_interface.revision_id.clone());
    let runtime_plan_digest = runtime_plan
        .canonical_digest(PlanLimits::from_profile(&checked_in_hard_limit_profile()).unwrap())
        .unwrap();
    let agent_document = ResourceDocument::Agent(AgentResourceSpec {
        authoring_package: authoring(0xa5),
        contract_digest: named_digest("agent-contract"),
        dependency_versions: vec![],
        policy_versions: vec![authorization_policy.clone()],
        input_schema: agent_schema(),
        output_schema: agent_schema(),
        error_schema: agent_schema(),
        typed_plan_artifact_id: id(ResourceKind::Artifact, 0xa5),
        typed_plan_digest: runtime_plan_digest,
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
                schema_digest: named_digest("query-schema"),
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
    repository: &PgRepository,
    fixture: &Fixture,
) -> (RepositoryJobFence, DeferOrchestrationToContextQuery) {
    let input_value = json!({"question": "top customers"});
    let input_digest: Sha256Digest = canonical_digest(&input_value).unwrap().parse().unwrap();
    let input_port = ExactDataPortRef::RunInput {
        schema_digest: named_digest("query-schema"),
    };
    let environment = ScopeDataEnvironmentSnapshot::build(
        BTreeMap::from([(
            input_port.clone(),
            ExactRunValueRef {
                value_id: id(ResourceKind::RunValue, 0x63),
                schema_digest: named_digest("query-schema"),
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

    let quota_account_id = id(ResourceKind::QuotaAccount, 0x800);
    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: fixture.tenant_id.to_string(),
            quota_account_id: quota_account_id.to_string(),
            scope_kind: "tenant".to_owned(),
            scope_id: fixture.tenant_id.to_string(),
            work_class: WorkClass::Orchestration.as_str().to_owned(),
            metric: "concurrent_jobs".to_owned(),
            limit_value: 4,
            payload: TypedPayload::new(1, &json!({"fixture": "context-owner"})).unwrap(),
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
            schema_digest: named_digest("query-schema"),
            content_digest: input_digest.clone(),
            value: ValueRef::Inline {
                value: input_value.clone(),
            },
        },
        materialized_input: ClosedJsonValue::build(named_digest("query-schema"), input_value)
            .unwrap(),
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
) {
    let input_value = json!({"question": "top customers"});
    let input_digest: Sha256Digest = canonical_digest(&input_value).unwrap().parse().unwrap();
    let request_port = ExactDataPortRef::RunInput {
        schema_digest: named_digest("query-schema"),
    };
    let environment = ScopeDataEnvironmentSnapshot::build(
        BTreeMap::from([(
            request_port,
            ExactRunValueRef {
                value_id: id(ResourceKind::RunValue, 0x63),
                schema_digest: named_digest("query-schema"),
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
        SET state = 'waiting', active_work_count = 0,
            current_schema_version = $3, current_payload = $4,
            current_payload_digest = $5
        WHERE tenant_id = $1 AND run_id = $2
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
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
            tenant_id, job_id, work_class, owner_kind, owner_id, run_id, node_id,
            state, version, attempt_no, attempt_limit, lease_epoch, scheduled_at,
            deadline, request_digest, result_digest, payload_schema_version,
            payload, payload_digest, started_at, terminal_at, created_at, updated_at
        ) VALUES (
            $1, $2, 'orchestration', 'node_execution', $3, $4, $3,
            'succeeded', 3, 1, 3, 1, $5, $6, $7, $8, $9, $10, $11,
            $5, $5, $5, $5
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
    let worker_id = id(ResourceKind::WorkerProcessGeneration, base);
    let lease_token = named_digest(&format!("lease-{base}"));
    let mut claimed = repository
        .claim_context_jobs(ClaimContextJobs {
            worker_process_generation_id: worker_id.clone(),
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
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    ClaimEvidence {
        claimed: claimed.pop().unwrap(),
        worker_id,
        lease_token,
    }
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
    let claimed = repository
        .claim_jobs(ClaimJobs {
            work_class: WorkClass::Context.to_string(),
            worker_id: worker_id.clone(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![lease_token_digest.clone()],
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.job_id, original_job.job_id);
    let started = repository
        .start_job(RepositoryJobFence {
            tenant_id: fixture.tenant_id.to_string(),
            job_id: claimed.job_id.clone(),
            worker_id: worker_id.clone(),
            lease_epoch: claimed.lease_epoch,
            expected_job_version: claimed.version,
            lease_token_digest: lease_token_digest.clone(),
        })
        .await
        .unwrap();
    let build_payload: ContextDatasetBuildJobPayload =
        serde_json::from_value(started.payload.value.clone()).unwrap();
    let completion = CommitContextDatasetBuild {
        tenant_id: fixture.tenant_id.clone(),
        job_id: started.job_id.parse().unwrap(),
        dataset_id: dataset_id.clone(),
        generation_id: id(ResourceKind::DatasetGeneration, 0x731),
        fence: insight_platform_jobs::JobFence {
            expected_version: u64::try_from(started.version).unwrap(),
            worker_process_generation_id: worker_id,
            lease_generation: u64::try_from(started.lease_epoch).unwrap(),
            token_digest: lease_token_digest.clone(),
        },
        lease_token_digest,
        generation: ContextDatasetGenerationSpec {
            context_deployment: build_payload.context_deployment,
            source_manifest_digest: named_digest("dataset-source-manifest"),
            parser_profile: build_payload.parser_profile,
            chunker_profile: build_payload.chunker_profile,
            embedding_model_deployment: build_payload.embedding_model_deployment,
            ranking_profile: build_payload.ranking_profile,
            index_manifest: artifact(0xa2),
            validation_evidence: artifact(0xa2),
            created_by_operation_id: started.job_id.parse().unwrap(),
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
        .claim_jobs(ClaimJobs {
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
        .start_job(RepositoryJobFence {
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
    let second_generation_id = id(ResourceKind::DatasetGeneration, 0x745);
    repository
        .commit_context_dataset_build(CommitContextDatasetBuild {
            tenant_id: fixture.tenant_id.clone(),
            job_id: rebuild_started.job_id.parse().unwrap(),
            dataset_id: dataset_id.clone(),
            generation_id: second_generation_id.clone(),
            fence: insight_platform_jobs::JobFence {
                expected_version: u64::try_from(rebuild_started.version).unwrap(),
                worker_process_generation_id: rebuild_worker,
                lease_generation: u64::try_from(rebuild_started.lease_epoch).unwrap(),
                token_digest: rebuild_token.clone(),
            },
            lease_token_digest: rebuild_token,
            generation: ContextDatasetGenerationSpec {
                context_deployment: rebuild_payload.context_deployment,
                source_manifest_digest: named_digest("dataset-rebuild-source-manifest"),
                parser_profile: rebuild_payload.parser_profile,
                chunker_profile: rebuild_payload.chunker_profile,
                embedding_model_deployment: rebuild_payload.embedding_model_deployment,
                ranking_profile: rebuild_payload.ranking_profile,
                index_manifest: artifact(0xa2),
                validation_evidence: artifact(0xa2),
                created_by_operation_id: rebuild_started.job_id.parse().unwrap(),
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
    let first_claim = claim(&repository, &fixture, job_id.clone(), 0x130).await;
    assert_eq!(first_claim.claimed.query.state, ContextQueryState::InFlight);
    assert_eq!(first_claim.claimed.job.attempt_no, 1);
    assert_eq!(first_claim.claimed.quota_account_ids.len(), 3);

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
    park_direct_context_leaf(
        &pool,
        &fixture,
        &created.context_query_id,
        &job_id,
    )
    .await;

    let completed_output = output(
        &fixture,
        &resumed.claimed.query,
        Utc::now(),
        id(ResourceKind::RunValue, 0x182),
    );
    let expected_result_bytes = completed_output.observation.total_bytes;
    let completion_command = CommitContextOutcome {
            audit: worker_audit(&fixture.tenant_id, &resumed.worker_id, 0x190, "complete"),
            context_query_id: created.context_query_id.clone(),
            expected_query_version: resumed.claimed.query.version,
            job_id: job_id.clone(),
            fence: resumed.fence(),
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
    assert_eq!(completed.job.attempt_no, 1);
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
        1
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
