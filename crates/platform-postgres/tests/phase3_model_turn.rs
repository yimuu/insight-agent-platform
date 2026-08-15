use chrono::{DateTime, Duration, Utc};
use insight_platform_artifacts::{ArtifactObjectReadAuthority, ArtifactObjectReadAuthorityError};
use insight_platform_contracts::{
    canonical_digest, AdministrativeGate, AgentDeploymentClosure, AgentResourceSpec, ArtifactRef,
    AuthoringPackage, CapabilityArtifactContract, CapabilityBackendBinding,
    CapabilityBackendContract, CapabilityBackendFeatures, CapabilityBackendKind,
    CapabilityBackendLimits, CapabilityCancellationKind, CapabilityDataFlowPolicy,
    CapabilityDeploymentClosure, CapabilityIdempotencyKind, CapabilityImplementationResourceSpec,
    CapabilityInterfaceLimits, CapabilityInterfaceResourceSpec, CapabilityProgressContract,
    CapabilityProgressDurability, CapabilityProgressMode, ClosedJsonValue, CommandAudit,
    CommandOutcome, ContextWindowContract, DataClassification, DataRegion, DecimalMoney,
    DeploymentClosure, Effect, EntityLifecycle, ExactDeploymentRef, ExactSecretBindingRef,
    ExactVersionRef, FrozenSlotBinding, FrozenSlotTarget, InstalledModelAdapter, JobState,
    ModelArtifactDeliveryContract, ModelCatalogEvidence, ModelDeploymentClosure,
    ModelIdentityStability, ModelLimits, ModelModalities, ModelProfileResourceSpec,
    ModelProviderDeploymentClosure, ModelProviderResourceSpec, ModelToolContract, ModelTurnState,
    ModelUsageContract, NativeCapabilityContract, Permission, PermissionSet, PolicyKind,
    PolicyResourceSpec, PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot,
    ProviderDataHandlingContract, ProviderModelIdentity, ProviderRequestLimits,
    ProviderTrainingPolicy, PublishedVersionPayload, QuotaDimension, RegistryResourceKind,
    ResourceDocument, ResourceId, ResourceKind, RunBindingsSnapshot, SecretBindingPayload,
    SecretPurpose, SecretResolutionPolicy, Sha256Digest, StructuredOutputContract, TenantConfig,
    TenantPrincipalPayload, ValidationSummary, ValueRef, WorkClass, WORKER_PROTOCOL_VERSION,
};
use insight_platform_jobs::JobFence;
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
use insight_platform_orchestrator::RunCurrentSnapshot;
use insight_platform_postgres::{
    model_turn_repository::{
        ClaimedModelExecution, ControlledModelExecution, PreparedModelExecution,
    },
    repository::{
        NewPrincipal, NewQuotaAccount, NewSecretBinding, NewTenant, NewTenantPrincipal,
        PgRepository, RepositoryError, TypedPayload,
    },
    verify_schema,
};
use insight_platform_registry::CreateDeployment;
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};

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

fn named_digest(label: &str) -> Sha256Digest {
    canonical_digest(&json!({"phase3_model_turn": label}))
        .unwrap()
        .parse()
        .unwrap()
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
    artifact_node_id: ResourceId,
    capability_deployment: ExactDeploymentRef,
    capability_interface_revision: ExactVersionRef,
    argument_schema: insight_platform_models::ClosedSchemaDocument,
    output_schema: insight_platform_models::ClosedSchemaDocument,
    parameter_schema_digest: Sha256Digest,
    truncation_policy: ExactVersionRef,
    deadline: DateTime<Utc>,
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
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification, expected_size_bytes,
            expected_digest, declared_media_type, verified_media_type, state, version,
            metadata_schema_version, metadata, metadata_digest, retention_policy_revision_id,
            retain_until, created_by, created_at, updated_at
        ) VALUES ($1, $2, $3, 'conformance', $4, $5, $6, $7, $7, 'ready', 1,
                  1, '{}'::jsonb, $8, $9, $10, $11, $12, $12)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(blob_id.to_string())
    .bind(artifact.classification().as_str())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(artifact.content_digest().to_string())
    .bind(artifact.media_type())
    .bind(digest('3').to_string())
    .bind(retention_policy_revision_id.to_string())
    .bind(database_now + Duration::days(1))
    .bind(principal_id.to_string())
    .bind(database_now)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_ready_model_request_artifact(
    pool: &PgPool,
    fixture: &Fixture,
    artifact: &ArtifactRef,
    suffix: u16,
) {
    let blob_id = id(ResourceKind::InternalBlob, suffix);
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let metadata = TypedPayload::new(
        1,
        &json!({
            "display_name": artifact.display_name(),
        }),
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest, security_domain_digest,
            object_reference_ciphertext, object_generation, key_id, encryption_domain_id,
            content_digest, size_bytes, state, version, verified_at, created_at, updated_at
        ) VALUES ($1, $2, 's3', $3, $4, $5, 'model-request-generation-1',
                  'model-request-kms-key', $6, $7, $8, 'verified', 1, $9, $9, $9)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(blob_id.to_string())
    .bind(named_digest("model_request_storage_binding").to_string())
    .bind(named_digest("model_request_security_domain").to_string())
    .bind(b"encrypted-model-request-locator".to_vec())
    .bind(id(ResourceKind::EncryptionDomain, suffix).to_string())
    .bind(artifact.content_digest().to_string())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(database_now)
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
        ) VALUES ($1, $2, $3, 'run_input', $4, $5, $6, $7, $7, 'ready', 1,
                  $8, $9, $10, $11, $12, $13, $14, $14)
        "#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(blob_id.to_string())
    .bind(artifact.classification().as_str())
    .bind(i64::try_from(artifact.byte_length()).unwrap())
    .bind(artifact.content_digest().to_string())
    .bind(artifact.media_type())
    .bind(metadata.schema_version)
    .bind(&metadata.value)
    .bind(&metadata.digest)
    .bind(fixture.provider_closure.data_policy.revision_id.to_string())
    .bind(database_now + Duration::days(1))
    .bind(fixture.principal_id.to_string())
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
                    authoring_package: authoring(0x90 + u16::try_from(index).unwrap(), 'c'),
                    contract_digest: digest('d'),
                    dependency_versions: vec![],
                    policy_versions: vec![],
                    policy_kind: PolicyKind::Retry,
                    rules_digest: digest('e'),
                    scheduling: None,
                    retention: None,
                    mcp_protocol: None,
                    mcp_auth: None,
                    sandbox_isolation: None,
                    sandbox_resource: None,
                    sandbox_network: None,
                    sandbox_artifact_io: None,
                    model_output_artifact_io: None,
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
    let principal_id = id(ResourceKind::Principal, 2);
    repository
        .create_tenant(NewTenant {
            tenant_id: tenant_id.to_string(),
            state: "active".to_owned(),
            config: TenantConfig {
                scheduling_policy: None,
            },
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
                    Permission::ModelInvoke,
                    Permission::ModelDeploy,
                    Permission::RuntimeControl,
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
            qualified_name: "fixture.responses/v1".to_owned(),
            worker_manifest_digest: digest('2'),
            adapter_contract_digest: digest('3'),
        },
        protocol_policy: protocol_policy.clone(),
        credential_requirements: vec!["provider.api_key".parse::<SecretPurpose>().unwrap()],
        request_limits: ProviderRequestLimits {
            maximum_request_bytes: 1_048_576,
            maximum_response_bytes: 1_048_576,
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
    let native_contract = CapabilityBackendContract::Native(NativeCapabilityContract {
        adapter_id: "builtin.model_tool_fixture".to_owned(),
        adapter_version: "1.0.0".to_owned(),
        module_digest: digest('7'),
        entrypoint_id: "model.tool.fixture".to_owned(),
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
        artifact_delivery: ModelArtifactDeliveryContract {
            supported_modalities: vec![],
            provider_file_upload: false,
            maximum_artifacts: 0,
            maximum_single_artifact_bytes: 0,
            maximum_total_artifact_bytes: 0,
            remote_retention_milliseconds: 0,
        },
        usage: ModelUsageContract {
            provider_reports_usage: true,
            reports_cached_input_tokens: false,
            reports_reasoning_tokens: false,
            reports_cost: true,
            cost_currency: Some("USD".to_owned()),
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
            maximum_artifacts: 0,
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
                output_schema: output_schema.clone(),
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
            worker_manifest_digest: digest('6'),
            adapter_module_digest: digest('7'),
        },
        secret_bindings: vec![],
        policies: vec![agent_policy.clone()],
        conformance_evidence: implementation_authoring.artifact,
    };
    let capability_payload = TypedPayload::new(
        1,
        &DeploymentClosure::CapabilityInterface(capability_closure),
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

    let agent_interface = version(ResourceKind::AgentInterfaceRevision, 0x38, '8');
    let agent_plan = version(ResourceKind::AgentPlanRevision, 0x39, '9');
    let agent_document = ResourceDocument::Agent(AgentResourceSpec {
        authoring_package: authoring(0xa6, 'a'),
        contract_digest: digest('b'),
        dependency_versions: vec![],
        policy_versions: vec![agent_policy.clone()],
        interface_schema_digest: digest('c'),
        typed_plan_digest: digest('d'),
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
    let agent_closure = AgentDeploymentClosure {
        interface: agent_interface,
        plan: agent_plan.clone(),
        slots: vec![
            FrozenSlotBinding {
                slot_id: "primary_model".to_owned(),
                requirement_digest: digest('e'),
                target: FrozenSlotTarget::Model {
                    candidates: vec![model_deployment.clone()],
                    selection_policy: selection_policy.clone(),
                },
                binding_digest: digest('f'),
            },
            FrozenSlotBinding {
                slot_id: "search".to_owned(),
                requirement_digest: digest('1'),
                target: FrozenSlotTarget::Capability {
                    candidates: vec![capability_deployment.clone()],
                    selection_policy: agent_policy.clone(),
                    tool_alias: Some("search".to_owned()),
                },
                binding_digest: digest('2'),
            },
        ],
        policies: vec![agent_policy],
        execution_profile,
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
            100_000,
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
    let artifact_node_id = id(ResourceKind::NodeExecution, 0x55);
    let seed_input_value_id = id(ResourceKind::RunValue, 0x54);
    let principal_snapshot = PrincipalSnapshot::build(
        tenant_id.clone(),
        principal_id.clone(),
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![Permission::ModelInvoke, Permission::RuntimeControl]).unwrap(),
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
    .bind(digest('e').to_string())
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
        (&artifact_node_id, "model-artifact", 3_i32),
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
        artifact_node_id,
        capability_deployment,
        capability_interface_revision: interface_revision,
        argument_schema,
        output_schema,
        parameter_schema_digest,
        truncation_policy,
        deadline,
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
                source_digest: digest('1'),
                trusted_instruction: true,
            },
        }],
        tools: vec![ModelToolProjection {
            projected_name: "search".to_owned(),
            capability_deployment: fixture.capability_deployment.clone(),
            interface_revision: fixture.capability_interface_revision.clone(),
            input_schema: fixture.argument_schema.clone(),
            output_schema_digest: fixture.output_schema.canonical_digest.clone(),
            effect: Effect::ReadOnly,
        }],
        response_contract: ModelResponseContract {
            output_schema_digest: fixture.output_schema.canonical_digest.clone(),
            structured_schema: Some(fixture.output_schema.clone()),
            allow_tool_intents: true,
            allow_message_with_tool_intents: false,
        },
        artifact_inputs: vec![],
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
            artifact_link_id: None,
            request,
        },
        requested_attempt_limit: 3,
        cost_ceiling_microunits: 10_000,
    }
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
            provider_reported_cost: Some(DecimalMoney::new("USD", 123, 6).unwrap()),
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

fn output(
    fixture: &Fixture,
    response: CanonicalModelResponse,
    value_id: ResourceId,
) -> ModelOutputValue {
    let value = serde_json::to_value(&response).unwrap();
    let content_digest: Sha256Digest = canonical_digest(&value).unwrap().parse().unwrap();
    ModelOutputValue {
        value_id,
        classification: DataClassification::Internal,
        schema_digest: fixture.output_schema.canonical_digest.clone(),
        content_digest,
        value: ValueRef::Inline { value },
        artifact_link_id: None,
        artifact_outputs: vec![],
        response,
        validation_evidence_digest: digest('c'),
    }
}

fn measurement(
    input_tokens: u64,
    output_tokens: u64,
    cost_microunits: i64,
) -> ModelAttemptMeasurement {
    ModelAttemptMeasurement {
        usage: Some(ModelUsage {
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
            cached_input_tokens: None,
            reasoning_tokens: None,
            provider_reported_cost: Some(DecimalMoney::new("USD", cost_microunits, 6).unwrap()),
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
    assert_model_deployment_closures_fail_closed(&repository, &fixture).await;

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
        metric == QuotaDimension::ModelCostMicrounits.as_str() && *used == 133
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

    let mut artifact_command = command_for_node(&fixture, &fixture.artifact_node_id, 0x500);
    let request_value = serde_json::to_value(&artifact_command.request.request).unwrap();
    let request_bytes = serde_jcs::to_vec(&request_value).unwrap();
    let request_content_digest: Sha256Digest =
        canonical_digest(&request_value).unwrap().parse().unwrap();
    let request_artifact = ArtifactRef::new(
        id(ResourceKind::Artifact, 0x510),
        request_content_digest.clone(),
        u64::try_from(request_bytes.len()).unwrap(),
        "application/json",
        artifact_command.request.classification,
        Some("model-request.json".to_owned()),
    )
    .unwrap();
    insert_ready_model_request_artifact(&pool, &fixture, &request_artifact, 0x511).await;
    artifact_command.request.content_digest = request_content_digest;
    artifact_command.request.value = ValueRef::Artifact {
        artifact: request_artifact.clone(),
    };
    artifact_command.request.artifact_link_id = Some(id(ResourceKind::ArtifactLink, 0x512));
    let artifact_claim = admit_prepare_and_claim(&repository, &artifact_command, 0x520).await;
    assert!(matches!(
        &artifact_claim.claimed.request_input.material,
        ModelExecutionInputMaterial::LinkedArtifact { artifact_link_id }
            if Some(artifact_link_id) == artifact_command.request.artifact_link_id.as_ref()
    ));
    let limits =
        ModelTurnLimits::from_profile(&insight_platform_contracts::checked_in_hard_limit_profile())
            .unwrap();
    let read_request = artifact_claim
        .claimed
        .artifact_read_request(limits)
        .unwrap()
        .unwrap();
    assert_eq!(read_request.artifact(), Some(&request_artifact));
    let authorized = <PgRepository as ArtifactObjectReadAuthority<_>>::authorize_object_read(
        &repository,
        &read_request,
    )
    .await
    .unwrap();
    assert_eq!(authorized.tenant_id, fixture.tenant_id);
    assert_eq!(authorized.artifact, request_artifact);
    let replay = <PgRepository as ArtifactObjectReadAuthority<_>>::authorize_object_read(
        &repository,
        &read_request,
    )
    .await
    .unwrap();
    assert_eq!(replay.authorization_digest, authorized.authorization_digest);

    if let Ok(configured_role) = std::env::var("PLATFORM_ARTIFACT_BROKER_TEST_ROLE") {
        assert_eq!(configured_role, "platform_artifact_broker_qualification");
        let restricted_pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|connection, _metadata| {
                Box::pin(async move {
                    sqlx::query("SET ROLE platform_artifact_broker_qualification")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .unwrap();
        verify_schema(&restricted_pool).await.unwrap();
        let restricted_repository = PgRepository::new(restricted_pool.clone());
        let restricted = <PgRepository as ArtifactObjectReadAuthority<_>>::authorize_object_read(
            &restricted_repository,
            &read_request,
        )
        .await
        .unwrap();
        assert_eq!(
            restricted.authorization_digest,
            authorized.authorization_digest
        );
        let forbidden_update = sqlx::query(
            "UPDATE insight_platform.jobs SET version = version WHERE tenant_id = $1 AND job_id = $2",
        )
        .bind(read_request.tenant_id.to_string())
        .bind(read_request.job_id.to_string())
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
        let forbidden_read =
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM insight_platform.secret_bindings")
                .fetch_one(&restricted_pool)
                .await
                .unwrap_err();
        assert_eq!(
            forbidden_read
                .as_database_error()
                .and_then(|failure| failure.code().map(|code| code.into_owned()))
                .as_deref(),
            Some("42501")
        );
        restricted_pool.close().await;
    }

    let mut stale_read = read_request;
    stale_read.fence.expected_version += 1;
    assert!(matches!(
        <PgRepository as ArtifactObjectReadAuthority<_>>::authorize_object_read(
            &repository,
            &stale_read,
        )
        .await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
}
