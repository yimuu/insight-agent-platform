use chrono::{Duration, Utc};
use insight_platform_contracts::{
    ActiveTarget, AdministrativeGate, AgentDeploymentClosure, AgentProductState, AgentResourceSpec,
    ArtifactRef, ArtifactRetentionPolicy, AuthoringPackage, ClosedJsonSchema, CodeTrustClass,
    DataClassification, DeploymentClosure, EntityLifecycle, ExactDeploymentRef, ExactPolicyBinding,
    ExactVersionRef, Permission, PermissionSet, PolicyDeploymentClosure, PolicyKind,
    PolicyResourceSpec, PrincipalBindingsPayload, PrincipalKind, PrincipalSnapshot,
    PublishedVersionPayload, RegistryResourceKind, ResourceDocument, ResourceDraftPayload,
    ResourceId, ResourceKind, RunBindingsSnapshot, SandboxCleanupPolicy, SandboxEntrypointKind,
    SandboxIsolationClass, SandboxPackageResourceSpec, SandboxProfileDeploymentClosure,
    SandboxProfileResourceSpec, SandboxRuntimeFamily, Sha256Digest, SkillArtifactSliceRef,
    SkillDeploymentClosure, SkillInstructionAudience, SkillInstructionPhase,
    SkillInstructionSection, SkillInterface, SkillPackageEntry, SkillPackageEntryKind,
    SkillPackageManifest, SkillResourceSpec, TenantConfig, TenantPrincipalPayload,
    ValidationSummary,
};
use insight_platform_postgres::{
    product_repository::AgentProductListQuery,
    repository::{
        ClaimJobs, CommitRegistryValidation, JobFence, NewPrincipal, NewTenant, NewTenantPrincipal,
        PgRepository, RegistryValidationCommitOutcome, RepositoryError, TypedPayload,
    },
    verify_schema,
};
use insight_platform_registry::{
    ActivateResource, CommandAudit, CommandOutcome, CreateDeployment, CreateResourceDraft,
    NewPublishedVersion, PublishResourceVersions, RecordResourceValidation,
    RequestResourceValidation, SetResourceGate, SuspendResourceDeployment,
    TransitionResourceLifecycle,
};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};

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

const TENANT_ID: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367c00";
const TENANT_B_ID: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367c01";
const PRINCIPAL_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367c02";
const DENIED_PRINCIPAL_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367c03";
const VALIDATOR_PRINCIPAL_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367c0b";
const ARTIFACT_ID: &str = "art_0198f1c3-8f49-7c3e-b1f3-773c28367c04";
const TYPED_PLAN_ARTIFACT_ID: &str = "art_0198f1c3-8f49-7c3e-b1f3-773c28367c09";
const TYPED_PLAN_BLOB_ID: &str = "iblb_0198f1c3-8f49-7c3e-b1f3-773c28367c0a";
const ARTIFACT_BLOB_ID: &str = "iblb_0198f1c3-8f49-7c3e-b1f3-773c28367ca1";
const RETENTION_POLICY_ID: &str = "pol_0198f1c3-8f49-7c3e-b1f3-773c28367ca2";
const RETENTION_REVISION_ID: &str = "prev_0198f1c3-8f49-7c3e-b1f3-773c28367ca3";
const ENCRYPTION_DOMAIN_ID: &str = "enc_0198f1c3-8f49-7c3e-b1f3-773c28367ca4";
const RESOURCE_ID: &str = "pol_0198f1c3-8f49-7c3e-b1f3-773c28367c05";
const VERSION_ID: &str = "prev_0198f1c3-8f49-7c3e-b1f3-773c28367c06";
const POLICY_DEPLOYMENT_ID: &str = "pdep_0198f1c3-8f49-7c3e-b1f3-773c28367c07";
const JOB_ID: &str = "job_0198f1c3-8f49-7c3e-b1f3-773c28367c08";
const ROLLBACK_RESOURCE_ID: &str = "pol_0198f1c3-8f49-7c3e-b1f3-773c28367cb0";
const AGENT_ID: &str = "agt_0198f1c3-8f49-7c3e-b1f3-773c28367cd0";
const AGENT_INTERFACE_ID: &str = "aif_0198f1c3-8f49-7c3e-b1f3-773c28367cd1";
const AGENT_PLAN_ID: &str = "arev_0198f1c3-8f49-7c3e-b1f3-773c28367cd2";
const AGENT_DEPLOYMENT_ID: &str = "adep_0198f1c3-8f49-7c3e-b1f3-773c28367cd3";
const AGENT_DEPLOYMENT_REPLAY_CANDIDATE_ID: &str = "adep_0198f1c3-8f49-7c3e-b1f3-773c28367cd4";
const AGENT_DEPLOYMENT_BAD_BATCH_ID: &str = "adep_0198f1c3-8f49-7c3e-b1f3-773c28367cd5";
const AGENT_DEPLOYMENT_BAD_OWNER_ID: &str = "adep_0198f1c3-8f49-7c3e-b1f3-773c28367cd6";
const LATE_AGENT_ID: &str = "agt_0198f1c3-8f49-7c3e-b1f3-773c28367cd7";
const POLICY_VERSION_2_ID: &str = "prev_0198f1c3-8f49-7c3e-b1f3-773c28367ce1";
const POLICY_VERSION_3_ID: &str = "prev_0198f1c3-8f49-7c3e-b1f3-773c28367ce2";
const POLICY_VERSION_4_ID: &str = "prev_0198f1c3-8f49-7c3e-b1f3-773c28367ce3";
const POLICY_DEPLOYMENT_2_ID: &str = "pdep_0198f1c3-8f49-7c3e-b1f3-773c28367ce4";
const POLICY_DEPLOYMENT_3_ID: &str = "pdep_0198f1c3-8f49-7c3e-b1f3-773c28367ce5";
const POLICY_DEPLOYMENT_4_ID: &str = "pdep_0198f1c3-8f49-7c3e-b1f3-773c28367ce6";
const SKILL_ID: &str = "skl_0198f1c3-8f49-7c3e-b1f3-773c28367ce7";
const SKILL_VERSION_ID: &str = "srev_0198f1c3-8f49-7c3e-b1f3-773c28367ce8";
const SKILL_DEPLOYMENT_ID: &str = "skdep_0198f1c3-8f49-7c3e-b1f3-773c28367ce9";
const SKILL_PACKAGE_ARTIFACT_ID: &str = "art_0198f1c3-8f49-7c3e-b1f3-773c28367ced";
const SKILL_PACKAGE_BLOB_ID: &str = "iblb_0198f1c3-8f49-7c3e-b1f3-773c28367cee";
const SANDBOX_PROFILE_ID: &str = "sxp_0198f1c3-8f49-7c3e-b1f3-773c28367cea";
const SANDBOX_PROFILE_VERSION_ID: &str = "sxrev_0198f1c3-8f49-7c3e-b1f3-773c28367ceb";
const SANDBOX_PROFILE_DEPLOYMENT_ID: &str = "sxdep_0198f1c3-8f49-7c3e-b1f3-773c28367cec";

fn id(value: &str) -> ResourceId {
    value.parse().unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn audit(
    tenant_id: &str,
    principal_id: &str,
    suffix: &str,
    idempotency: char,
    request: char,
) -> CommandAudit {
    CommandAudit {
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
        tenant_id: id(tenant_id),
        principal_id: id(principal_id),
        principal_kind: PrincipalKind::TenantAdmin,
        receipt_id: id(&format!("rcp_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")),
        event_id: id(&format!("evt_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")),
        outbox_id: id(&format!("obx_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(1),
    }
}

#[track_caller]
fn applied<T>(outcome: CommandOutcome<T>) -> T {
    match outcome {
        CommandOutcome::Applied(value) => value,
        CommandOutcome::Replayed(_) => panic!("expected an applied command"),
    }
}

macro_rules! registry_command {
    ($repository:expr, $method:ident, $command:expr) => {{
        let mut transaction = $repository.begin_registry_transaction().await.unwrap();
        let result = transaction.$method($command).await;
        match result {
            Ok(value) => {
                transaction.commit().await.unwrap();
                Ok(value)
            }
            Err(failure) => {
                transaction.rollback().await.unwrap();
                Err(failure)
            }
        }
    }};
}

fn sandbox_id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1d1-8f49-7c3e-b1f3-773c2836{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

async fn prove_sandbox_package_runtime_bundle_publication(
    pool: &PgPool,
    repository: &PgRepository,
    authoring_artifact: &ArtifactRef,
) {
    let runtime_resource_id = sandbox_id(ResourceKind::SandboxRuntime, 1);
    let runtime_revision = ExactVersionRef::new(
        sandbox_id(ResourceKind::SandboxRuntimeRevision, 2),
        digest('1'),
    )
    .unwrap();
    let package_resource_id = sandbox_id(ResourceKind::SandboxPackage, 3);
    let package_revision_id = sandbox_id(ResourceKind::SandboxPackageRevision, 4);
    let bundle_blob_id = sandbox_id(ResourceKind::InternalBlob, 5);
    let bundle_artifact = ArtifactRef::new(
        sandbox_id(ResourceKind::Artifact, 6),
        digest('2'),
        32,
        "application/wasm",
        DataClassification::Internal,
        Some("published-module.wasm".to_owned()),
    )
    .unwrap();
    let runtime_payload = TypedPayload::new(1, &json!({"fixture": "sandbox-runtime"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'sandbox_runtime', 'active', 'enabled', $3, $4, $5)
        "#,
    )
    .bind(TENANT_ID)
    .bind(runtime_resource_id.to_string())
    .bind(runtime_payload.schema_version)
    .bind(&runtime_payload.value)
    .bind(&runtime_payload.digest)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resource_versions (
            tenant_id, resource_version_id, resource_id, resource_version_kind,
            revision_no, content_digest, payload_schema_version, payload,
            payload_digest, created_by
        ) VALUES ($1, $2, $3, 'sandbox_runtime_revision', 1, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(TENANT_ID)
    .bind(runtime_revision.revision_id.to_string())
    .bind(runtime_resource_id.to_string())
    .bind(runtime_revision.semantic_digest.to_string())
    .bind(runtime_payload.schema_version)
    .bind(&runtime_payload.value)
    .bind(&runtime_payload.digest)
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();

    let package_document = ResourceDocument::SandboxPackage(SandboxPackageResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: authoring_artifact.clone(),
            manifest_digest: digest('3'),
        },
        contract_digest: digest('4'),
        dependency_versions: vec![],
        policy_versions: vec![],
        source_artifact: authoring_artifact.clone(),
        source_digest: authoring_artifact.content_digest().clone(),
        runtime_revision,
        entrypoint_kind: SandboxEntrypointKind::WasmExport,
        entrypoint: "run".to_owned(),
        dependency_lock_digest: digest('5'),
        runtime_bundle_artifact: bundle_artifact.clone(),
        build_evidence: authoring_artifact.clone(),
        trust_class: CodeTrustClass::BuiltIn,
        package_digest: digest('6'),
    });
    let draft = ResourceDraftPayload {
        display_name: "Sandbox package publication fixture".to_owned(),
        document: package_document.clone(),
        validation: None,
    };
    assert!(matches!(
        registry_command!(
            repository,
            create_resource_draft,
            CreateResourceDraft {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "8a10", '1', '2'),
                resource_id: package_resource_id.clone(),
                draft: draft.clone(),
            }
        )
        .unwrap(),
        CommandOutcome::Applied(_)
    ));
    let draft_digest = draft.document_digest().unwrap();
    let now = Utc::now();
    let validator_digest = digest('7');
    let validation = ValidationSummary {
        validator_digest,
        validated_draft_digest: draft_digest.clone(),
        dependency_closure_digest: digest('9'),
        security_evidence_digest: digest('a'),
        warnings: vec![],
    };
    registry_command!(
        repository,
        record_resource_validation,
        RecordResourceValidation {
            audit: audit(TENANT_ID, PRINCIPAL_ID, "8a30", '5', '6'),
            resource_id: package_resource_id.clone(),
            expected_resource_version: 1,
            expected_draft_digest: draft_digest.clone(),
            validation: validation.clone(),
        }
    )
    .unwrap();

    macro_rules! publish {
        ($suffix:literal, $idempotency:literal, $request:literal, $document:expr) => {{
            registry_command!(
                repository,
                publish_resource_versions,
                PublishResourceVersions {
                    audit: audit(TENANT_ID, PRINCIPAL_ID, $suffix, $idempotency, $request,),
                    resource_id: package_resource_id.clone(),
                    expected_resource_version: 2,
                    expected_draft_digest: draft_digest.clone(),
                    versions: vec![NewPublishedVersion {
                        resource_version_id: package_revision_id.clone(),
                        revision_no: 1,
                        content_digest: digest('b'),
                        artifact_id: None,
                        payload: PublishedVersionPayload {
                            document: $document,
                            validation: validation.clone(),
                        },
                    }],
                }
            )
        }};
    }

    assert!(matches!(
        publish!("8a40", '7', '8', package_document.clone()),
        Err(RepositoryError::NotFound(
            "ready Sandbox runtime bundle artifact"
        ))
    ));

    let artifact_metadata = TypedPayload::new(1, &json!({"fixture": "runtime-bundle"})).unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest, security_domain_digest,
            object_reference_ciphertext, key_id, encryption_domain_id, state
        ) VALUES ($1, $2, 'fixture', $3, $4, $5, 'fixture-key', $6, 'pending')
        "#,
    )
    .bind(TENANT_ID)
    .bind(bundle_blob_id.to_string())
    .bind(digest('c').to_string())
    .bind(digest('d').to_string())
    .bind(vec![4_u8, 5, 6])
    .bind(sandbox_id(ResourceKind::EncryptionDomain, 9).to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'package', 'internal', $4, $5,
                  'application/wasm', 'application/wasm', 'ready', $6, $7, $8,
                  $9, $10, $11)
        "#,
    )
    .bind(TENANT_ID)
    .bind(bundle_artifact.artifact_id().to_string())
    .bind(bundle_blob_id.to_string())
    .bind(i64::try_from(bundle_artifact.byte_length()).unwrap())
    .bind(bundle_artifact.content_digest().to_string())
    .bind(artifact_metadata.schema_version)
    .bind(&artifact_metadata.value)
    .bind(&artifact_metadata.digest)
    .bind(RETENTION_REVISION_ID)
    .bind(now + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        publish!("8a50", '9', 'a', package_document.clone()),
        Err(RepositoryError::NotFound(
            "ready Sandbox runtime bundle artifact"
        ))
    ));

    sqlx::query(
        r#"
        UPDATE insight_platform.artifact_blobs
        SET object_generation = 'fixture-generation', content_digest = $3,
            size_bytes = $4, state = 'verified', verified_at = clock_timestamp(),
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND blob_id = $2
        "#,
    )
    .bind(TENANT_ID)
    .bind(bundle_blob_id.to_string())
    .bind(bundle_artifact.content_digest().to_string())
    .bind(i64::try_from(bundle_artifact.byte_length()).unwrap())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.artifacts SET state = 'pending', updated_at = clock_timestamp() WHERE tenant_id = $1 AND artifact_id = $2",
    )
    .bind(TENANT_ID)
    .bind(bundle_artifact.artifact_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        publish!("8a60", 'b', 'c', package_document.clone()),
        Err(RepositoryError::NotFound(
            "ready Sandbox runtime bundle artifact"
        ))
    ));

    sqlx::query(
        r#"
        UPDATE insight_platform.artifacts
        SET state = 'ready', updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND artifact_id = $2
        "#,
    )
    .bind(TENANT_ID)
    .bind(bundle_artifact.artifact_id().to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.artifact_blobs SET content_digest = $3, updated_at = clock_timestamp() WHERE tenant_id = $1 AND blob_id = $2",
    )
    .bind(TENANT_ID)
    .bind(bundle_blob_id.to_string())
    .bind(digest('e').to_string())
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        publish!("8a70", 'd', 'e', package_document.clone()),
        Err(RepositoryError::NotFound(
            "ready Sandbox runtime bundle artifact"
        ))
    ));

    sqlx::query(
        "UPDATE insight_platform.artifact_blobs SET content_digest = $3, size_bytes = $4, updated_at = clock_timestamp() WHERE tenant_id = $1 AND blob_id = $2",
    )
    .bind(TENANT_ID)
    .bind(bundle_blob_id.to_string())
    .bind(bundle_artifact.content_digest().to_string())
    .bind(i64::try_from(bundle_artifact.byte_length() + 1).unwrap())
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        publish!("8a80", 'f', '0', package_document.clone()),
        Err(RepositoryError::NotFound(
            "ready Sandbox runtime bundle artifact"
        ))
    ));

    sqlx::query(
        "UPDATE insight_platform.artifact_blobs SET size_bytes = $3, updated_at = clock_timestamp() WHERE tenant_id = $1 AND blob_id = $2",
    )
    .bind(TENANT_ID)
    .bind(bundle_blob_id.to_string())
    .bind(i64::try_from(bundle_artifact.byte_length()).unwrap())
    .execute(pool)
    .await
    .unwrap();
    let alternate_bundle_artifact = ArtifactRef::new(
        sandbox_id(ResourceKind::Artifact, 10),
        bundle_artifact.content_digest().clone(),
        bundle_artifact.byte_length(),
        bundle_artifact.media_type(),
        bundle_artifact.classification(),
        None,
    )
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'package', 'internal', $4, $5,
                  'application/wasm', 'application/wasm', 'ready', $6, $7, $8,
                  $9, $10, $11)
        "#,
    )
    .bind(TENANT_ID)
    .bind(alternate_bundle_artifact.artifact_id().to_string())
    .bind(bundle_blob_id.to_string())
    .bind(i64::try_from(alternate_bundle_artifact.byte_length()).unwrap())
    .bind(alternate_bundle_artifact.content_digest().to_string())
    .bind(artifact_metadata.schema_version)
    .bind(&artifact_metadata.value)
    .bind(&artifact_metadata.digest)
    .bind(RETENTION_REVISION_ID)
    .bind(now + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    let mut drifted_published_document = package_document.clone();
    let ResourceDocument::SandboxPackage(spec) = &mut drifted_published_document else {
        unreachable!();
    };
    spec.runtime_bundle_artifact = alternate_bundle_artifact;
    assert!(matches!(
        publish!("8a90", '0', '1', drifted_published_document),
        Err(RepositoryError::InvalidInput(message))
            if message == "published document does not match the validated draft"
    ));

    let published = publish!("8aa0", '2', '3', package_document).unwrap();
    assert!(matches!(published, CommandOutcome::Applied(_)));
}

#[tokio::test]
async fn resource_lifecycle_is_typed_atomic_and_not_auto_activated() {
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

    for tenant_id in [TENANT_ID, TENANT_B_ID] {
        repository
            .create_tenant(NewTenant {
                tenant_id: tenant_id.to_owned(),
                state: "active".to_owned(),
                config: TenantConfig::default(),
            })
            .await
            .unwrap();
    }
    for (principal_id, subject) in [
        (PRINCIPAL_ID, '1'),
        (DENIED_PRINCIPAL_ID, '2'),
        (VALIDATOR_PRINCIPAL_ID, '3'),
    ] {
        repository
            .create_principal(NewPrincipal {
                principal_id: id(principal_id),
                authentication_authority_digest: digest(subject),
                subject_digest: digest(if subject == '1' { '3' } else { '4' }),
                installation_bindings: PrincipalBindingsPayload {
                    installation_bindings: vec![],
                },
            })
            .await
            .unwrap();
    }
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(TENANT_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::TenantAdmin,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::PolicyWrite,
                    Permission::PolicyPublish,
                    Permission::PolicyActivate,
                    Permission::AgentRead,
                    Permission::AgentWrite,
                    Permission::AgentPublish,
                    Permission::AgentDeploy,
                    Permission::AgentActivate,
                    Permission::SkillRead,
                    Permission::SkillWrite,
                    Permission::SkillPublish,
                    Permission::SkillBind,
                    Permission::SkillActivate,
                    Permission::SandboxWrite,
                    Permission::SandboxPublish,
                    Permission::SandboxActivate,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(TENANT_ID),
            principal_id: id(VALIDATOR_PRINCIPAL_ID),
            principal_kind: PrincipalKind::ServiceIdentity,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::PolicyWrite]).unwrap(),
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(TENANT_ID),
            principal_id: id(DENIED_PRINCIPAL_ID),
            principal_kind: PrincipalKind::TenantAdmin,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::PolicyRead]).unwrap(),
            },
        })
        .await
        .unwrap();

    let authoring_artifact = ArtifactRef::new(
        id(ARTIFACT_ID),
        digest('5'),
        16,
        "application/json",
        DataClassification::Internal,
        Some("policy.json".to_owned()),
    )
    .unwrap();
    let retention = ArtifactRetentionPolicy {
        version: 1,
        minimum_retention_seconds: 3_600,
        gc_grace_seconds: 86_400,
        tombstone_retention_seconds: 2_592_000,
        retain_provenance_sources: true,
        delete_requires_approval: true,
    };
    let retention_document = ResourceDocument::Policy(Box::new(PolicyResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: authoring_artifact.clone(),
            manifest_digest: digest('a'),
        },
        contract_digest: digest('b'),
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
    }));
    let retention_resource_payload = TypedPayload::new(
        1,
        &ResourceDraftPayload {
            display_name: "Built-in Artifact retention".to_owned(),
            document: retention_document.clone(),
            validation: None,
        },
    )
    .unwrap();
    let retention_version_payload = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: retention_document,
            validation: ValidationSummary {
                validator_digest: digest('c'),
                validated_draft_digest: digest('d'),
                dependency_closure_digest: digest('e'),
                security_evidence_digest: digest('f'),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    let artifact_metadata = TypedPayload::new(1, &json!({"fixture": "authoring"})).unwrap();
    let mut bootstrap = pool.begin().await.unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'policy', 'active', 'enabled', $3, $4, $5)
        "#,
    )
    .bind(TENANT_ID)
    .bind(RETENTION_POLICY_ID)
    .bind(retention_resource_payload.schema_version)
    .bind(&retention_resource_payload.value)
    .bind(&retention_resource_payload.digest)
    .execute(&mut *bootstrap)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at,
            created_at, updated_at
        ) SELECT $1, $2, 'fixture', $3, $4, $5, 'generation-plan', 'fixture-key',
                 $6, $7, 16, 'verified', observed_at, observed_at, observed_at
          FROM (SELECT clock_timestamp() AS observed_at) AS clock
        "#,
    )
    .bind(TENANT_ID)
    .bind(TYPED_PLAN_BLOB_ID)
    .bind(digest('8').to_string())
    .bind(digest('9').to_string())
    .bind(vec![4_u8, 5, 6])
    .bind(ENCRYPTION_DOMAIN_ID)
    .bind(digest('b').to_string())
    .execute(&mut *bootstrap)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at,
            created_at, updated_at
        ) SELECT $1, $2, 'fixture', $3, $4, $5, 'generation-1', 'fixture-key',
                 $6, $7, 16, 'verified', observed_at, observed_at, observed_at
          FROM (SELECT clock_timestamp() AS observed_at) AS clock
        "#,
    )
    .bind(TENANT_ID)
    .bind(ARTIFACT_BLOB_ID)
    .bind(digest('1').to_string())
    .bind(digest('2').to_string())
    .bind(vec![1_u8, 2, 3])
    .bind(ENCRYPTION_DOMAIN_ID)
    .bind(digest('5').to_string())
    .execute(&mut *bootstrap)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'typed_plan', 'internal', 16, $4,
                  'application/json', 'application/json', 'ready', $5, $6, $7,
                  $8, $9, $10)
        "#,
    )
    .bind(TENANT_ID)
    .bind(TYPED_PLAN_ARTIFACT_ID)
    .bind(TYPED_PLAN_BLOB_ID)
    .bind(digest('b').to_string())
    .bind(artifact_metadata.schema_version)
    .bind(&artifact_metadata.value)
    .bind(&artifact_metadata.digest)
    .bind(RETENTION_REVISION_ID)
    .bind(Utc::now() + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(&mut *bootstrap)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'authoring_document', 'internal', 16, $4,
                  'application/json', 'application/json', 'ready', $5, $6, $7,
                  $8, $9, $10)
        "#,
    )
    .bind(TENANT_ID)
    .bind(ARTIFACT_ID)
    .bind(ARTIFACT_BLOB_ID)
    .bind(digest('5').to_string())
    .bind(artifact_metadata.schema_version)
    .bind(&artifact_metadata.value)
    .bind(&artifact_metadata.digest)
    .bind(RETENTION_REVISION_ID)
    .bind(Utc::now() + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(&mut *bootstrap)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at,
            created_at, updated_at
        ) SELECT $1, $2, 'fixture', $3, $4, $5, 'generation-skill', 'fixture-key',
                 $6, $7, 96, 'verified', observed_at, observed_at, observed_at
          FROM (SELECT clock_timestamp() AS observed_at) AS clock
        "#,
    )
    .bind(TENANT_ID)
    .bind(SKILL_PACKAGE_BLOB_ID)
    .bind(digest('d').to_string())
    .bind(digest('e').to_string())
    .bind(vec![7_u8, 8, 9])
    .bind(ENCRYPTION_DOMAIN_ID)
    .bind(digest('c').to_string())
    .execute(&mut *bootstrap)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'authoring_document', 'internal', 96, $4,
                  $5, $5, 'ready', $6, $7, $8, $9, $10, $11)
        "#,
    )
    .bind(TENANT_ID)
    .bind(SKILL_PACKAGE_ARTIFACT_ID)
    .bind(SKILL_PACKAGE_BLOB_ID)
    .bind(digest('c').to_string())
    .bind(insight_platform_contracts::SKILL_PACKAGE_MEDIA_TYPE)
    .bind(artifact_metadata.schema_version)
    .bind(&artifact_metadata.value)
    .bind(&artifact_metadata.digest)
    .bind(RETENTION_REVISION_ID)
    .bind(Utc::now() + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(&mut *bootstrap)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.resource_versions (
            tenant_id, resource_version_id, resource_id, resource_version_kind,
            revision_no, content_digest, artifact_id, payload_schema_version,
            payload, payload_digest, created_by
        ) VALUES ($1, $2, $3, 'policy_revision', 1, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(TENANT_ID)
    .bind(RETENTION_REVISION_ID)
    .bind(RETENTION_POLICY_ID)
    .bind(&retention_version_payload.digest)
    .bind(ARTIFACT_ID)
    .bind(retention_version_payload.schema_version)
    .bind(&retention_version_payload.value)
    .bind(&retention_version_payload.digest)
    .bind(PRINCIPAL_ID)
    .execute(&mut *bootstrap)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.resources SET active_version_id = $3 WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(RETENTION_POLICY_ID)
    .bind(RETENTION_REVISION_ID)
    .execute(&mut *bootstrap)
    .await
    .unwrap();
    bootstrap.commit().await.unwrap();

    prove_sandbox_package_runtime_bundle_publication(&pool, &repository, &authoring_artifact).await;

    let document = ResourceDocument::Policy(Box::new(PolicyResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: authoring_artifact,
            manifest_digest: digest('6'),
        },
        contract_digest: digest('7'),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: PolicyKind::Authorization,
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
    }));
    let draft = ResourceDraftPayload {
        display_name: "Tenant authorization policy".to_owned(),
        document: document.clone(),
        validation: None,
    };
    let mut rollback_transaction = repository.begin_registry_transaction().await.unwrap();
    assert!(matches!(
        rollback_transaction
            .create_resource_draft(CreateResourceDraft {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7cb1", '8', '9'),
                resource_id: id(ROLLBACK_RESOURCE_ID),
                draft: draft.clone(),
            })
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    rollback_transaction.rollback().await.unwrap();
    let rolled_back_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.resources WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(ROLLBACK_RESOURCE_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rolled_back_rows, 0);

    let create_audit = audit(TENANT_ID, PRINCIPAL_ID, "7c10", '9', 'a');
    let create_command = CreateResourceDraft {
        audit: create_audit.clone(),
        resource_id: id(RESOURCE_ID),
        draft: draft.clone(),
    };
    let created = applied(
        registry_command!(repository, create_resource_draft, create_command.clone()).unwrap(),
    );
    assert_eq!(created.lifecycle_state, "active");
    assert_eq!(created.gate_state, "enabled");
    assert!(created.active_version_id.is_none());
    assert!(matches!(
        registry_command!(repository, create_resource_draft, create_command).unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let readable = repository
        .read_resource_for_principal(
            &id(TENANT_ID),
            &id(DENIED_PRINCIPAL_ID),
            PrincipalKind::TenantAdmin,
            insight_platform_contracts::RegistryResourceKind::Policy,
            &id(RESOURCE_ID),
        )
        .await
        .unwrap();
    assert_eq!(readable.resource_id, RESOURCE_ID);
    assert_eq!(readable.payload.value["display_name"], draft.display_name);
    assert!(matches!(
        repository
            .read_resource_for_principal(
                &id(TENANT_ID),
                &id(PRINCIPAL_ID),
                PrincipalKind::TenantAdmin,
                insight_platform_contracts::RegistryResourceKind::Policy,
                &id(RESOURCE_ID),
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_resource_for_principal(
                &id(TENANT_B_ID),
                &id(DENIED_PRINCIPAL_ID),
                PrincipalKind::TenantAdmin,
                insight_platform_contracts::RegistryResourceKind::Policy,
                &id(RESOURCE_ID),
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_resource_for_principal(
                &id(TENANT_ID),
                &id(DENIED_PRINCIPAL_ID),
                PrincipalKind::TenantAdmin,
                insight_platform_contracts::RegistryResourceKind::Agent,
                &id(RESOURCE_ID),
            )
            .await,
        Err(RepositoryError::NotFound("resource"))
    ));
    let server_retry = registry_command!(
        repository,
        create_resource_draft,
        CreateResourceDraft {
            audit: create_audit.clone(),
            resource_id: id(ROLLBACK_RESOURCE_ID),
            draft: draft.clone(),
        }
    )
    .unwrap();
    assert!(matches!(
        server_retry,
        CommandOutcome::Replayed(record) if record.resource_id == RESOURCE_ID
    ));
    let mut conflict_audit = audit(TENANT_ID, PRINCIPAL_ID, "7c13", '9', 'b');
    conflict_audit.idempotency_key_digest = create_audit.idempotency_key_digest.clone();
    assert!(matches!(
        registry_command!(
            repository,
            create_resource_draft,
            CreateResourceDraft {
                audit: conflict_audit,
                resource_id: id(RESOURCE_ID),
                draft: draft.clone(),
            }
        ),
        Err(RepositoryError::IdempotencyConflict)
    ));

    let draft_digest = draft.document_digest().unwrap();
    let now = Utc::now();
    let validation_request = RequestResourceValidation {
        audit: audit(TENANT_ID, PRINCIPAL_ID, "7c20", 'c', 'd'),
        resource_id: id(RESOURCE_ID),
        expected_resource_version: 1,
        job_id: id(JOB_ID),
        validator_digest: digest('e'),
        validation_profile_digest: digest('f'),
        attempt_limit: 3,
        scheduled_at: now,
        deadline: now + Duration::minutes(5),
    };
    let validation_job = applied(
        registry_command!(
            repository,
            request_resource_validation,
            validation_request.clone()
        )
        .unwrap(),
    );
    assert_eq!(validation_job.state, "ready");
    assert_eq!(validation_job.work_class, "registry_validation");
    assert_eq!(validation_job.owner_kind, "job");
    sqlx::query(
        "UPDATE insight_platform.jobs SET version = 2, updated_at = clock_timestamp() WHERE tenant_id = $1 AND job_id = $2",
    )
    .bind(TENANT_ID)
    .bind(JOB_ID)
    .execute(&pool)
    .await
    .unwrap();
    let mut replay_request = validation_request;
    replay_request.job_id = sandbox_id(ResourceKind::Job, 0x99);
    let replayed_validation =
        registry_command!(repository, request_resource_validation, replay_request).unwrap();
    let CommandOutcome::Replayed(replayed_validation) = replayed_validation else {
        panic!("expected exact historical validation acceptance replay");
    };
    assert_eq!(replayed_validation.job_id, JOB_ID);
    assert_eq!(replayed_validation.version, 1);
    assert_eq!(replayed_validation.state, "ready");

    let worker_id = sandbox_id(ResourceKind::WorkerProcessGeneration, 0x71);
    assert!(matches!(
        repository
            .claim_jobs(ClaimJobs {
                work_class: "registry_validation".to_owned(),
                worker_id: sandbox_id(ResourceKind::WorkerProcessGeneration, 0x70),
                limit: 1,
                lease_milliseconds: 30_000,
                lease_token_digests: vec![digest('9')],
            })
            .await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let ineligible_claims = repository
        .claim_registry_validation_jobs(
            ClaimJobs {
                work_class: "registry_validation".to_owned(),
                worker_id: sandbox_id(ResourceKind::WorkerProcessGeneration, 0x70),
                limit: 1,
                lease_milliseconds: 30_000,
                lease_token_digests: vec![digest('9')],
            },
            &id(DENIED_PRINCIPAL_ID),
        )
        .await
        .unwrap();
    assert!(ineligible_claims.is_empty());

    let claimed = repository
        .claim_registry_validation_jobs(
            ClaimJobs {
                work_class: "registry_validation".to_owned(),
                worker_id: worker_id.clone(),
                limit: 1,
                lease_milliseconds: 30_000,
                lease_token_digests: vec![digest('0')],
            },
            &id(VALIDATOR_PRINCIPAL_ID),
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let claimed = claimed.into_iter().next().unwrap();
    let claimed_fence = JobFence {
        tenant_id: claimed.tenant_id.clone(),
        job_id: claimed.job_id.clone(),
        worker_id,
        lease_epoch: claimed.lease_epoch,
        expected_job_version: claimed.version,
        lease_token_digest: claimed.lease_token_digest.unwrap().parse().unwrap(),
    };
    let running = repository.start_job(claimed_fence.clone()).await.unwrap();
    let committed = repository
        .commit_registry_validation(CommitRegistryValidation {
            fence: JobFence {
                expected_job_version: running.version,
                ..claimed_fence
            },
            validator_principal_id: id(VALIDATOR_PRINCIPAL_ID),
            validator_digest: digest('e'),
            validation_profile_digest: digest('f'),
            receipt_id: sandbox_id(ResourceKind::Receipt, 0x72),
            resource_event_id: sandbox_id(ResourceKind::Event, 0x73),
            resource_outbox_id: sandbox_id(ResourceKind::OutboxEvent, 0x74),
            job_event_id: sandbox_id(ResourceKind::Event, 0x75),
            job_outbox_id: sandbox_id(ResourceKind::OutboxEvent, 0x76),
            idempotency_key_digest: digest('1'),
            request_digest: digest('2'),
            receipt_expires_at: Utc::now() + Duration::hours(1),
        })
        .await
        .unwrap();
    let RegistryValidationCommitOutcome::Committed { job, resource } = committed else {
        panic!("expected Registry Validation commit")
    };
    assert_eq!(job.state, "succeeded");
    assert_eq!(job.payload, validation_job.payload);
    assert_eq!(resource.version, 2);
    let mut validated_payload = resource.payload.value;
    validated_payload
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    let validation = serde_json::from_value::<ResourceDraftPayload>(validated_payload)
        .unwrap()
        .validation
        .unwrap();
    assert_eq!(validation.validated_draft_digest, draft_digest);

    assert!(matches!(
        registry_command!(
            repository,
            publish_resource_versions,
            PublishResourceVersions {
                audit: audit(TENANT_ID, DENIED_PRINCIPAL_ID, "7c40", '4', '5'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 2,
                expected_draft_digest: draft_digest.clone(),
                versions: vec![NewPublishedVersion {
                    resource_version_id: id(VERSION_ID),
                    revision_no: 1,
                    content_digest: digest('6'),
                    artifact_id: None,
                    payload: PublishedVersionPayload {
                        document: document.clone(),
                        validation: validation.clone(),
                    },
                }],
            }
        ),
        Err(RepositoryError::PermissionDenied)
    ));

    let publish_audit = audit(TENANT_ID, PRINCIPAL_ID, "7c50", '7', '8');
    let published_payload = PublishedVersionPayload {
        document,
        validation,
    };
    let publish_command = PublishResourceVersions {
        audit: publish_audit.clone(),
        resource_id: id(RESOURCE_ID),
        expected_resource_version: 2,
        expected_draft_digest: draft_digest,
        versions: vec![NewPublishedVersion {
            resource_version_id: id(VERSION_ID),
            revision_no: 1,
            content_digest: digest('6'),
            artifact_id: None,
            payload: published_payload.clone(),
        }],
    };
    let published =
        applied(registry_command!(repository, publish_resource_versions, publish_command).unwrap());
    assert_eq!(published.resource.version, 3);
    assert!(published.resource.active_version_id.is_none());
    assert!(published.resource.active_deployment_id.is_none());
    let readable_version = repository
        .read_resource_version_for_principal(
            &id(TENANT_ID),
            &id(DENIED_PRINCIPAL_ID),
            PrincipalKind::TenantAdmin,
            insight_platform_contracts::RegistryResourceKind::Policy,
            &id(RESOURCE_ID),
            &id(VERSION_ID),
        )
        .await
        .unwrap();
    assert_eq!(readable_version.resource_version_id, VERSION_ID);
    assert_eq!(readable_version.content_digest, digest('6').to_string());
    assert!(matches!(
        repository
            .read_resource_version_for_principal(
                &id(TENANT_ID),
                &id(PRINCIPAL_ID),
                PrincipalKind::TenantAdmin,
                insight_platform_contracts::RegistryResourceKind::Policy,
                &id(RESOURCE_ID),
                &id(VERSION_ID),
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_resource_version_for_principal(
                &id(TENANT_ID),
                &id(DENIED_PRINCIPAL_ID),
                PrincipalKind::TenantAdmin,
                insight_platform_contracts::RegistryResourceKind::Policy,
                &id(RETENTION_POLICY_ID),
                &id(VERSION_ID),
            )
            .await,
        Err(RepositoryError::NotFound("resource version"))
    ));

    let policy_closure = PolicyDeploymentClosure {
        policy_revision: ExactVersionRef::new(id(VERSION_ID), digest('6')).unwrap(),
        applicability_digest: digest('5'),
        qualification_evidence: draft.document.authoring_package().artifact.clone(),
    };
    let policy_deployment = applied(
        registry_command!(
            repository,
            create_deployment,
            CreateDeployment {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7c55", '8', '9'),
                deployment_id: id(POLICY_DEPLOYMENT_ID),
                resource_id: id(RESOURCE_ID),
                resource_version_id: id(VERSION_ID),
                environment: "test".to_owned(),
                closure: DeploymentClosure::Policy(policy_closure),
                expected_resource_version: 3,
            }
        )
        .unwrap(),
    );
    let policy_deployment_digest: Sha256Digest = policy_deployment.bindings.digest.parse().unwrap();
    let target = ActiveTarget::Deployment {
        deployment: ExactDeploymentRef::new(
            id(POLICY_DEPLOYMENT_ID),
            policy_deployment_digest.clone(),
        )
        .unwrap(),
    };
    let activated = applied(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7c60", '9', 'a'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 4,
                target: target.clone(),
            }
        )
        .unwrap(),
    );
    assert!(activated.active_version_id.is_none());
    assert_eq!(
        activated.active_deployment_id.as_deref(),
        Some(POLICY_DEPLOYMENT_ID)
    );
    assert_eq!(activated.version, 5);

    let suspended = applied(
        registry_command!(
            repository,
            set_resource_gate,
            SetResourceGate {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7c70", 'b', 'c'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 5,
                target: AdministrativeGate::Suspended,
            }
        )
        .unwrap(),
    );
    assert_eq!(suspended.version, 6);
    let failed_audit = audit(TENANT_ID, PRINCIPAL_ID, "7c80", 'd', 'e');
    assert!(matches!(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: failed_audit.clone(),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 5,
                target,
            }
        ),
        Err(RepositoryError::Conflict("resource"))
    ));
    let failed_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
    )
    .bind(TENANT_ID)
    .bind(failed_audit.receipt_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(failed_receipts, 0);

    let archived = applied(
        registry_command!(
            repository,
            transition_resource_lifecycle,
            TransitionResourceLifecycle {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7c90", 'f', '0'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 6,
                target: EntityLifecycle::Archived,
            }
        )
        .unwrap(),
    );
    assert_eq!(archived.lifecycle_state, "archived");
    assert!(matches!(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: audit(TENANT_B_ID, PRINCIPAL_ID, "7ca0", '1', '2'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 7,
                target: ActiveTarget::Deployment {
                    deployment: ExactDeploymentRef::new(
                        id(POLICY_DEPLOYMENT_ID),
                        policy_deployment_digest.clone(),
                    )
                    .unwrap(),
                },
            }
        ),
        Err(RepositoryError::NotFound("resource"))
    ));

    let restored = applied(
        registry_command!(
            repository,
            transition_resource_lifecycle,
            TransitionResourceLifecycle {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7ca1", '3', '4'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 7,
                target: EntityLifecycle::Active,
            }
        )
        .unwrap(),
    );
    assert_eq!(restored.version, 8);
    let enabled = applied(
        registry_command!(
            repository,
            set_resource_gate,
            SetResourceGate {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7ca2", '5', '6'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 8,
                target: AdministrativeGate::Enabled,
            }
        )
        .unwrap(),
    );
    assert_eq!(enabled.version, 9);

    let policy_ref = ExactVersionRef::new(id(VERSION_ID), digest('6')).unwrap();
    let policy_binding = ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(id(POLICY_DEPLOYMENT_ID), policy_deployment_digest)
            .unwrap(),
        revision: policy_ref.clone(),
    };
    let agent_document = ResourceDocument::Agent(AgentResourceSpec {
        authoring_name: "deployment-agent".to_owned(),
        required_features: vec![],
        authoring_package: draft.document.authoring_package().clone(),
        contract_digest: digest('a'),
        dependency_versions: vec![policy_ref.clone()],
        policy_versions: vec![policy_ref.clone()],
        author_instructions: None,
        input_schema: agent_schema(),
        output_schema: agent_schema(),
        error_schema: agent_schema(),
        typed_plan_artifact_id: id(TYPED_PLAN_ARTIFACT_ID),
        typed_plan_digest: digest('b'),
    });
    let agent_draft = ResourceDraftPayload {
        display_name: "Deployment closure agent".to_owned(),
        document: agent_document.clone(),
        validation: None,
    };
    applied(
        registry_command!(
            repository,
            create_resource_draft,
            CreateResourceDraft {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7cd4", '3', '4'),
                resource_id: id(AGENT_ID),
                draft: agent_draft.clone(),
            }
        )
        .unwrap(),
    );
    let agent_draft_digest = agent_draft.document_digest().unwrap();
    let agent_validation = ValidationSummary {
        validator_digest: digest('5'),
        validated_draft_digest: agent_draft_digest.clone(),
        dependency_closure_digest: digest('6'),
        security_evidence_digest: digest('7'),
        warnings: vec![],
    };
    applied(
        registry_command!(
            repository,
            record_resource_validation,
            RecordResourceValidation {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7cd5", '8', '9'),
                resource_id: id(AGENT_ID),
                expected_resource_version: 1,
                expected_draft_digest: agent_draft_digest.clone(),
                validation: agent_validation.clone(),
            }
        )
        .unwrap(),
    );
    let interface_ref = ExactVersionRef::new(id(AGENT_INTERFACE_ID), digest('a')).unwrap();
    let plan_ref = ExactVersionRef::new(id(AGENT_PLAN_ID), digest('b')).unwrap();
    let agent_versions = vec![
        NewPublishedVersion {
            resource_version_id: id(AGENT_INTERFACE_ID),
            revision_no: 1,
            content_digest: digest('a'),
            artifact_id: None,
            payload: PublishedVersionPayload {
                document: agent_document.clone(),
                validation: agent_validation.clone(),
            },
        },
        NewPublishedVersion {
            resource_version_id: id(AGENT_PLAN_ID),
            revision_no: 1,
            content_digest: digest('b'),
            artifact_id: Some(id(TYPED_PLAN_ARTIFACT_ID)),
            payload: PublishedVersionPayload {
                document: agent_document,
                validation: agent_validation,
            },
        },
    ];
    let mut unbound_plan_versions = agent_versions.clone();
    unbound_plan_versions[1].artifact_id = None;
    assert!(matches!(
        registry_command!(
            repository,
            publish_resource_versions,
            PublishResourceVersions {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7cdf", '0', '1'),
                resource_id: id(AGENT_ID),
                expected_resource_version: 2,
                expected_draft_digest: agent_draft_digest.clone(),
                versions: unbound_plan_versions,
            }
        ),
        Err(RepositoryError::InvalidInput(message))
            if message == "Agent Plan revision must bind the exact typed Plan artifact and digest"
    ));
    let agent_published = applied(
        registry_command!(
            repository,
            publish_resource_versions,
            PublishResourceVersions {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7cd6", 'a', 'b'),
                resource_id: id(AGENT_ID),
                expected_resource_version: 2,
                expected_draft_digest: agent_draft_digest,
                versions: agent_versions,
            }
        )
        .unwrap(),
    );
    assert_eq!(agent_published.versions.len(), 2);
    assert!(agent_published.resource.active_deployment_id.is_none());

    let closure = AgentDeploymentClosure {
        interface: interface_ref.clone(),
        plan: plan_ref.clone(),
        entry_node_id: "start".to_owned(),
        entry_node_kind: insight_platform_contracts::PlanNodeKind::Start,
        slots: vec![],
        policies: vec![policy_binding.clone()],
        execution_profile: policy_binding,
    };
    sqlx::query(
        "UPDATE insight_platform.resource_versions SET revision_no = 2 WHERE tenant_id = $1 AND resource_version_id = $2",
    )
    .bind(TENANT_ID)
    .bind(AGENT_INTERFACE_ID)
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        registry_command!(
            repository,
            create_deployment,
            CreateDeployment {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7ce0", '1', '2'),
                deployment_id: id(AGENT_DEPLOYMENT_BAD_BATCH_ID),
                resource_id: id(AGENT_ID),
                resource_version_id: id(AGENT_PLAN_ID),
                environment: "test".to_owned(),
                closure: DeploymentClosure::Agent(closure.clone()),
                expected_resource_version: 3,
            }
        ),
        Err(RepositoryError::Conflict(
            "Agent Deployment Interface/Plan publish batch"
        ))
    ));
    sqlx::query(
        "UPDATE insight_platform.resource_versions SET revision_no = 1, resource_id = $1 WHERE tenant_id = $2 AND resource_version_id = $3",
    )
    .bind(RESOURCE_ID)
    .bind(TENANT_ID)
    .bind(AGENT_INTERFACE_ID)
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        registry_command!(
            repository,
            create_deployment,
            CreateDeployment {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7ce1", '3', '4'),
                deployment_id: id(AGENT_DEPLOYMENT_BAD_OWNER_ID),
                resource_id: id(AGENT_ID),
                resource_version_id: id(AGENT_PLAN_ID),
                environment: "test".to_owned(),
                closure: DeploymentClosure::Agent(closure.clone()),
                expected_resource_version: 3,
            }
        ),
        Err(RepositoryError::Conflict(
            "Agent Deployment Interface/Plan publish batch"
        ))
    ));
    sqlx::query(
        "UPDATE insight_platform.resource_versions SET resource_id = $1 WHERE tenant_id = $2 AND resource_version_id = $3",
    )
    .bind(AGENT_ID)
    .bind(TENANT_ID)
    .bind(AGENT_INTERFACE_ID)
    .execute(&pool)
    .await
    .unwrap();
    let deployment_audit = audit(TENANT_ID, PRINCIPAL_ID, "7cd7", 'c', 'd');
    let deployment = applied(
        registry_command!(
            repository,
            create_deployment,
            CreateDeployment {
                audit: deployment_audit.clone(),
                deployment_id: id(AGENT_DEPLOYMENT_ID),
                resource_id: id(AGENT_ID),
                resource_version_id: id(AGENT_PLAN_ID),
                environment: "test".to_owned(),
                closure: DeploymentClosure::Agent(closure.clone()),
                expected_resource_version: 3,
            }
        )
        .unwrap(),
    );
    let active_after_deploy: Option<String> = sqlx::query_scalar(
        "SELECT active_deployment_id FROM insight_platform.resources WHERE tenant_id = $1 AND resource_id = $2",
    )
    .bind(TENANT_ID)
    .bind(AGENT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(active_after_deploy.is_none());

    let deployment_replay = registry_command!(
        repository,
        create_deployment,
        CreateDeployment {
            audit: deployment_audit,
            deployment_id: id(AGENT_DEPLOYMENT_REPLAY_CANDIDATE_ID),
            resource_id: id(AGENT_ID),
            resource_version_id: id(AGENT_PLAN_ID),
            environment: "test".to_owned(),
            closure: DeploymentClosure::Agent(closure.clone()),
            expected_resource_version: 3,
        }
    )
    .unwrap();
    assert!(matches!(
        deployment_replay,
        CommandOutcome::Replayed(ref replayed) if replayed == &deployment
    ));
    let replay_candidate_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM insight_platform.deployments WHERE tenant_id = $1 AND deployment_id = $2)",
    )
    .bind(TENANT_ID)
    .bind(AGENT_DEPLOYMENT_REPLAY_CANDIDATE_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!replay_candidate_exists);

    let read_deployment = repository
        .read_deployment_for_principal(
            &id(TENANT_ID),
            &id(PRINCIPAL_ID),
            PrincipalKind::TenantAdmin,
            RegistryResourceKind::Agent,
            &id(AGENT_ID),
            &id(AGENT_DEPLOYMENT_ID),
        )
        .await
        .unwrap();
    assert_eq!(read_deployment, deployment);
    assert!(matches!(
        repository
            .read_deployment_for_principal(
                &id(TENANT_ID),
                &id(DENIED_PRINCIPAL_ID),
                PrincipalKind::TenantAdmin,
                RegistryResourceKind::Agent,
                &id(AGENT_ID),
                &id(AGENT_DEPLOYMENT_ID),
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_deployment_for_principal(
                &id(TENANT_B_ID),
                &id(PRINCIPAL_ID),
                PrincipalKind::TenantAdmin,
                RegistryResourceKind::Agent,
                &id(AGENT_ID),
                &id(AGENT_DEPLOYMENT_ID),
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_deployment_for_principal(
                &id(TENANT_ID),
                &id(PRINCIPAL_ID),
                PrincipalKind::TenantAdmin,
                RegistryResourceKind::Agent,
                &id(RESOURCE_ID),
                &id(AGENT_DEPLOYMENT_ID),
            )
            .await,
        Err(RepositoryError::NotFound("deployment"))
    ));

    let deployment_digest: Sha256Digest = deployment.bindings.digest.parse().unwrap();
    let run_bindings = RunBindingsSnapshot::build(
        ExactDeploymentRef::new(id(AGENT_DEPLOYMENT_ID), deployment_digest.clone()).unwrap(),
        PrincipalSnapshot::build(
            id(TENANT_ID),
            id(PRINCIPAL_ID),
            PrincipalKind::TenantAdmin,
            PermissionSet::new(vec![
                Permission::PolicyWrite,
                Permission::PolicyPublish,
                Permission::PolicyActivate,
                Permission::AgentWrite,
                Permission::AgentPublish,
                Permission::AgentDeploy,
                Permission::AgentActivate,
            ])
            .unwrap(),
            1,
            1,
            1,
        )
        .unwrap(),
        &closure,
    )
    .unwrap();
    assert_eq!(run_bindings.agent_interface, interface_ref);
    assert_eq!(run_bindings.plan, plan_ref);

    let activation_audit = audit(TENANT_ID, PRINCIPAL_ID, "7cd9", 'e', 'f');
    let activated_agent = applied(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: activation_audit.clone(),
                resource_id: id(AGENT_ID),
                expected_resource_version: 4,
                target: ActiveTarget::Deployment {
                    deployment: ExactDeploymentRef::new(
                        id(AGENT_DEPLOYMENT_ID),
                        deployment_digest.clone(),
                    )
                    .unwrap(),
                },
            }
        )
        .unwrap(),
    );
    assert_eq!(
        activated_agent.active_deployment_id.as_deref(),
        Some(AGENT_DEPLOYMENT_ID)
    );
    assert_eq!(activated_agent.gate_state, "enabled");
    assert_eq!(activated_agent.version, 5);
    let activation_replay = registry_command!(
        repository,
        activate_resource,
        ActivateResource {
            audit: activation_audit.clone(),
            resource_id: id(AGENT_ID),
            expected_resource_version: 4,
            target: ActiveTarget::Deployment {
                deployment: ExactDeploymentRef::new(
                    id(AGENT_DEPLOYMENT_ID),
                    deployment_digest.clone(),
                )
                .unwrap(),
            },
        }
    )
    .unwrap();
    assert!(matches!(
        activation_replay,
        CommandOutcome::Replayed(ref replayed) if replayed == &activated_agent
    ));

    let suspension_audit = audit(TENANT_ID, PRINCIPAL_ID, "7cda", '1', '2');
    let suspended_agent = applied(
        registry_command!(
            repository,
            suspend_resource_deployment,
            SuspendResourceDeployment {
                audit: suspension_audit.clone(),
                resource_id: id(AGENT_ID),
                deployment_id: id(AGENT_DEPLOYMENT_ID),
                expected_resource_version: 5,
            }
        )
        .unwrap(),
    );
    assert_eq!(suspended_agent.gate_state, "suspended");
    assert_eq!(suspended_agent.version, 6);
    let suspension_replay = registry_command!(
        repository,
        suspend_resource_deployment,
        SuspendResourceDeployment {
            audit: suspension_audit.clone(),
            resource_id: id(AGENT_ID),
            deployment_id: id(AGENT_DEPLOYMENT_ID),
            expected_resource_version: 5,
        }
    )
    .unwrap();
    assert!(matches!(
        suspension_replay,
        CommandOutcome::Replayed(ref replayed) if replayed == &suspended_agent
    ));

    let restored_agent = applied(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7cdb", '3', '4'),
                resource_id: id(AGENT_ID),
                expected_resource_version: 6,
                target: ActiveTarget::Deployment {
                    deployment:
                        ExactDeploymentRef::new(id(AGENT_DEPLOYMENT_ID), deployment_digest,)
                            .unwrap(),
                },
            }
        )
        .unwrap(),
    );
    assert_eq!(restored_agent.gate_state, "enabled");
    assert_eq!(restored_agent.version, 7);
    let late_activation_replay = registry_command!(
        repository,
        activate_resource,
        ActivateResource {
            audit: activation_audit,
            resource_id: id(AGENT_ID),
            expected_resource_version: 4,
            target: ActiveTarget::Deployment {
                deployment: ExactDeploymentRef::new(
                    id(AGENT_DEPLOYMENT_ID),
                    deployment.bindings.digest.parse().unwrap(),
                )
                .unwrap(),
            },
        }
    )
    .unwrap();
    assert!(matches!(
        late_activation_replay,
        CommandOutcome::Replayed(ref replayed) if replayed == &activated_agent
    ));
    let late_suspension_replay = registry_command!(
        repository,
        suspend_resource_deployment,
        SuspendResourceDeployment {
            audit: suspension_audit,
            resource_id: id(AGENT_ID),
            deployment_id: id(AGENT_DEPLOYMENT_ID),
            expected_resource_version: 5,
        }
    )
    .unwrap();
    assert!(matches!(
        late_suspension_replay,
        CommandOutcome::Replayed(ref replayed) if replayed == &suspended_agent
    ));

    let first_agent_page = repository
        .list_agent_products(AgentProductListQuery {
            tenant_id: id(TENANT_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::TenantAdmin,
            state: None,
            environment: None,
            snapshot_at: None,
            boundary: None,
            fetch_limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(first_agent_page.records.len(), 1);
    assert_eq!(
        first_agent_page.records[0].product_state,
        AgentProductState::Ready
    );
    assert_eq!(
        first_agent_page.records[0]
            .active_deployment
            .as_ref()
            .map(|deployment| deployment.environment.as_str()),
        Some("test")
    );
    let first_boundary = (
        first_agent_page.records[0].resource.updated_at,
        id(AGENT_ID),
    );

    sqlx::query(
        r#"
        INSERT INTO insight_platform.resources (
            tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
            draft_generation, active_version_id, active_deployment_id, version,
            payload_schema_version, payload, payload_digest, created_at, updated_at
        )
        SELECT tenant_id, $3, resource_kind, lifecycle_state, gate_state,
               draft_generation, NULL, NULL, version,
               payload_schema_version, payload, payload_digest,
               clock_timestamp(), clock_timestamp()
        FROM insight_platform.resources
        WHERE tenant_id = $1 AND resource_id = $2
        "#,
    )
    .bind(TENANT_ID)
    .bind(AGENT_ID)
    .bind(LATE_AGENT_ID)
    .execute(&pool)
    .await
    .unwrap();

    let frozen_next_page = repository
        .list_agent_products(AgentProductListQuery {
            tenant_id: id(TENANT_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::TenantAdmin,
            state: None,
            environment: None,
            snapshot_at: Some(first_agent_page.snapshot_at),
            boundary: Some(first_boundary),
            fetch_limit: 51,
        })
        .await
        .unwrap();
    assert!(frozen_next_page.records.is_empty());

    let current_agents = repository
        .list_agent_products(AgentProductListQuery {
            tenant_id: id(TENANT_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::TenantAdmin,
            state: None,
            environment: None,
            snapshot_at: None,
            boundary: None,
            fetch_limit: 51,
        })
        .await
        .unwrap();
    assert_eq!(current_agents.records.len(), 2);

    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(TENANT_B_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::TenantAdmin,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::AgentRead]).unwrap(),
            },
        })
        .await
        .unwrap();
    let other_tenant = repository
        .list_agent_products(AgentProductListQuery {
            tenant_id: id(TENANT_B_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::TenantAdmin,
            state: None,
            environment: None,
            snapshot_at: None,
            boundary: None,
            fetch_limit: 51,
        })
        .await
        .unwrap();
    assert!(other_tenant.records.is_empty());

    let ready_only = repository
        .list_agent_products(AgentProductListQuery {
            tenant_id: id(TENANT_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::TenantAdmin,
            state: Some(AgentProductState::Ready),
            environment: Some("test".to_owned()),
            snapshot_at: None,
            boundary: None,
            fetch_limit: 51,
        })
        .await
        .unwrap();
    assert_eq!(ready_only.records.len(), 1);
    assert_eq!(ready_only.records[0].resource.resource_id, AGENT_ID);

    let first_update_audit = audit(TENANT_ID, PRINCIPAL_ID, "7ce0", '1', '2');
    let mut first_updated_draft = draft.clone();
    first_updated_draft.display_name = "First replay-stable draft".to_owned();
    let first_update = applied(
        registry_command!(
            repository,
            update_resource_draft,
            insight_platform_registry::UpdateResourceDraft {
                audit: first_update_audit.clone(),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 9,
                draft: first_updated_draft.clone(),
            }
        )
        .unwrap(),
    );
    assert_eq!(first_update.version, 10);
    assert_eq!(first_update.draft_generation, 2);

    let mut second_updated_draft = draft;
    second_updated_draft.display_name = "Later current draft".to_owned();
    let second_update = applied(
        registry_command!(
            repository,
            update_resource_draft,
            insight_platform_registry::UpdateResourceDraft {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7ce1", '3', '4'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 10,
                draft: second_updated_draft,
            }
        )
        .unwrap(),
    );
    assert_eq!(second_update.version, 11);

    let replay = registry_command!(
        repository,
        update_resource_draft,
        insight_platform_registry::UpdateResourceDraft {
            audit: first_update_audit,
            resource_id: id(RESOURCE_ID),
            expected_resource_version: 9,
            draft: first_updated_draft,
        }
    )
    .unwrap();
    let CommandOutcome::Replayed(replayed) = replay else {
        panic!("expected exact historical draft update replay");
    };
    assert_eq!(replayed.version, 10);
    assert_eq!(replayed.draft_generation, 2);
    assert_eq!(
        replayed.payload.value["display_name"],
        "First replay-stable draft"
    );

    let prepared_replay = repository
        .prepare_resource_publish(
            &publish_audit,
            insight_platform_contracts::RegistryResourceKind::Policy,
            &id(RESOURCE_ID),
        )
        .await
        .unwrap();
    let insight_platform_postgres::repository::ResourcePublishPreparation::Replayed(
        prepared_replay,
    ) = prepared_replay
    else {
        panic!("expected publication preparation to resolve the stored Receipt");
    };
    assert_eq!(prepared_replay.resource.version, 3);
    assert_eq!(prepared_replay.versions[0].resource_version_id, VERSION_ID);

    let publish_replay = registry_command!(
        repository,
        publish_resource_versions,
        PublishResourceVersions {
            audit: publish_audit,
            resource_id: id(RESOURCE_ID),
            expected_resource_version: 2,
            expected_draft_digest: published_payload.validation.validated_draft_digest.clone(),
            versions: vec![NewPublishedVersion {
                resource_version_id: sandbox_id(ResourceKind::PolicyRevision, 0x9a),
                revision_no: 1,
                content_digest: digest('6'),
                artifact_id: None,
                payload: published_payload,
            }],
        }
    )
    .unwrap();
    let CommandOutcome::Replayed(publish_replay) = publish_replay else {
        panic!("expected exact historical publication replay");
    };
    assert_eq!(publish_replay.resource.version, 3);
    assert_eq!(publish_replay.resource.draft_generation, 1);
    assert!(publish_replay.resource.active_version_id.is_none());
    assert_eq!(publish_replay.versions[0].resource_version_id, VERSION_ID);

    let qualification_artifact = ArtifactRef::new(
        id(ARTIFACT_ID),
        digest('5'),
        16,
        "application/json",
        DataClassification::Internal,
        Some("policy.json".to_owned()),
    )
    .unwrap();
    let mut policy_bindings = vec![ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(
            id(POLICY_DEPLOYMENT_ID),
            policy_deployment.bindings.digest.parse().unwrap(),
        )
        .unwrap(),
        revision: ExactVersionRef::new(id(VERSION_ID), digest('6')).unwrap(),
    }];
    let dependency_policy_document = ResourceDocument::Policy(Box::new(PolicyResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: qualification_artifact.clone(),
            manifest_digest: digest('6'),
        },
        contract_digest: digest('7'),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: PolicyKind::Authorization,
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
    }));
    for (ordinal, (version_id, deployment_id, content_digest)) in [
        (POLICY_VERSION_2_ID, POLICY_DEPLOYMENT_2_ID, digest('7')),
        (POLICY_VERSION_3_ID, POLICY_DEPLOYMENT_3_ID, digest('8')),
        (POLICY_VERSION_4_ID, POLICY_DEPLOYMENT_4_ID, digest('9')),
    ]
    .into_iter()
    .enumerate()
    {
        let revision = ExactVersionRef::new(id(version_id), content_digest).unwrap();
        let published = TypedPayload::new(
            1,
            &PublishedVersionPayload {
                document: dependency_policy_document.clone(),
                validation: ValidationSummary {
                    validator_digest: digest('1'),
                    validated_draft_digest: digest('2'),
                    dependency_closure_digest: digest('3'),
                    security_evidence_digest: digest('4'),
                    warnings: vec![],
                },
            },
        )
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resource_versions (
                tenant_id, resource_version_id, resource_id, resource_version_kind,
                revision_no, content_digest, payload_schema_version, payload,
                payload_digest, created_by
            ) VALUES ($1, $2, $3, 'policy_revision', $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(TENANT_ID)
        .bind(version_id)
        .bind(RESOURCE_ID)
        .bind(2 + ordinal as i64)
        .bind(revision.semantic_digest.to_string())
        .bind(published.schema_version)
        .bind(&published.value)
        .bind(&published.digest)
        .bind(PRINCIPAL_ID)
        .execute(&pool)
        .await
        .unwrap();
        let deployment_closure = DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: revision.clone(),
            applicability_digest: digest('5'),
            qualification_evidence: qualification_artifact.clone(),
        });
        let deployment = TypedPayload::new(1, &deployment_closure).unwrap();
        sqlx::query(
            r#"
            INSERT INTO insight_platform.deployments (
                tenant_id, deployment_id, resource_id, resource_version_id,
                environment, bindings_digest, payload_schema_version, bindings, created_by
            ) VALUES ($1, $2, $3, $4, 'test', $5, $6, $7, $8)
            "#,
        )
        .bind(TENANT_ID)
        .bind(deployment_id)
        .bind(RESOURCE_ID)
        .bind(version_id)
        .bind(&deployment.digest)
        .bind(deployment.schema_version)
        .bind(&deployment.value)
        .bind(PRINCIPAL_ID)
        .execute(&pool)
        .await
        .unwrap();
        policy_bindings.push(ExactPolicyBinding {
            deployment: ExactDeploymentRef::new(
                id(deployment_id),
                deployment.digest.parse().unwrap(),
            )
            .unwrap(),
            revision,
        });
    }

    let skill_entries = vec![
        SkillPackageEntry {
            path: "instructions/method.md".to_owned(),
            kind: SkillPackageEntryKind::Instruction,
            media_type: "text/markdown".to_owned(),
            byte_length: 8,
            content_digest: digest('8'),
            data_classification: DataClassification::Internal,
            executable: false,
        },
        SkillPackageEntry {
            path: "skill.json".to_owned(),
            kind: SkillPackageEntryKind::Manifest,
            media_type: "application/json".to_owned(),
            byte_length: 8,
            content_digest: digest('9'),
            data_classification: DataClassification::Internal,
            executable: false,
        },
    ];
    let skill_manifest_digest: Sha256Digest =
        insight_platform_contracts::canonical_digest(&json!({
            "entries": skill_entries,
            "schema_version": 1,
            "total_byte_length": 16,
        }))
        .unwrap()
        .parse()
        .unwrap();
    let instruction_sections = vec![SkillInstructionSection {
        section_id: "method".to_owned(),
        phase: SkillInstructionPhase::Planning,
        audience: SkillInstructionAudience::Planner,
        body: SkillArtifactSliceRef {
            path: "instructions/method.md".to_owned(),
            content_digest: digest('8'),
            byte_offset: 0,
            byte_length: 8,
        },
        max_tokens: 8,
        data_classification: DataClassification::Internal,
    }];
    let instruction_set_digest: Sha256Digest = insight_platform_contracts::canonical_digest(
        &serde_json::to_value(&instruction_sections).unwrap(),
    )
    .unwrap()
    .parse()
    .unwrap();
    let requirement_set_digest: Sha256Digest =
        insight_platform_contracts::canonical_digest(&json!({
            "capability": [],
            "context": [],
            "model": [],
            "skill_dependencies": [],
        }))
        .unwrap()
        .parse()
        .unwrap();
    let skill_document = ResourceDocument::Skill(SkillResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: ArtifactRef::new(
                id(SKILL_PACKAGE_ARTIFACT_ID),
                digest('c'),
                96,
                insight_platform_contracts::SKILL_PACKAGE_MEDIA_TYPE,
                DataClassification::Internal,
                None,
            )
            .unwrap(),
            manifest_digest: skill_manifest_digest.clone(),
        },
        contract_digest: digest('2'),
        dependency_versions: vec![],
        policy_versions: vec![policy_bindings[0].revision.clone()],
        interface: SkillInterface {
            qualified_name: "review.method".to_owned(),
            purpose: "Provide a bounded review method".to_owned(),
            task_input_schema: agent_schema(),
            produced_guidance_schema: agent_schema(),
            compatible_agent_interfaces: vec![id(AGENT_INTERFACE_ID)],
        },
        manifest: SkillPackageManifest {
            schema_version: 1,
            entries: skill_entries,
            total_byte_length: 16,
            canonical_digest: skill_manifest_digest,
        },
        instruction_sections,
        skill_dependencies: vec![],
        capability_requirements: vec![],
        context_requirements: vec![],
        model_requirements: vec![],
        instruction_set_digest,
        requirement_set_digest,
    });
    let skill_draft = ResourceDraftPayload {
        display_name: "Qualified deployment skill".to_owned(),
        document: skill_document.clone(),
        validation: None,
    };
    applied(
        registry_command!(
            repository,
            create_resource_draft,
            CreateResourceDraft {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9b10", '4', '2'),
                resource_id: id(SKILL_ID),
                draft: skill_draft.clone(),
            }
        )
        .unwrap(),
    );
    let skill_draft_digest = skill_draft.document_digest().unwrap();
    let skill_validation = ValidationSummary {
        validator_digest: digest('5'),
        validated_draft_digest: skill_draft_digest.clone(),
        dependency_closure_digest: digest('6'),
        security_evidence_digest: digest('7'),
        warnings: vec![],
    };
    applied(
        registry_command!(
            repository,
            record_resource_validation,
            RecordResourceValidation {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9b11", '6', '4'),
                resource_id: id(SKILL_ID),
                expected_resource_version: 1,
                expected_draft_digest: skill_draft_digest.clone(),
                validation: skill_validation.clone(),
            }
        )
        .unwrap(),
    );
    applied(
        registry_command!(
            repository,
            publish_resource_versions,
            PublishResourceVersions {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9b12", '4', '6'),
                resource_id: id(SKILL_ID),
                expected_resource_version: 2,
                expected_draft_digest: skill_draft_digest,
                versions: vec![NewPublishedVersion {
                    resource_version_id: id(SKILL_VERSION_ID),
                    revision_no: 1,
                    content_digest: digest('8'),
                    artifact_id: None,
                    payload: PublishedVersionPayload {
                        document: skill_document,
                        validation: skill_validation,
                    },
                }],
            }
        )
        .unwrap(),
    );
    let skill_closure = SkillDeploymentClosure {
        skill_revision: ExactVersionRef::new(id(SKILL_VERSION_ID), digest('8')).unwrap(),
        requirements: vec![],
        selection_policy: policy_bindings[0].clone(),
        qualification_evidence: qualification_artifact.clone(),
    };
    let skill_deployment = applied(
        registry_command!(
            repository,
            create_deployment,
            CreateDeployment {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9b13", '4', '8'),
                deployment_id: id(SKILL_DEPLOYMENT_ID),
                resource_id: id(SKILL_ID),
                resource_version_id: id(SKILL_VERSION_ID),
                environment: "test".to_owned(),
                closure: DeploymentClosure::Skill(skill_closure.clone()),
                expected_resource_version: 3,
            }
        )
        .unwrap(),
    );
    let skill_deployment_ref = ExactDeploymentRef::new(
        id(SKILL_DEPLOYMENT_ID),
        skill_deployment.bindings.digest.parse().unwrap(),
    )
    .unwrap();
    let skill_activation_audit = audit(TENANT_ID, PRINCIPAL_ID, "9b14", '4', 'a');
    let activated_skill = applied(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: skill_activation_audit.clone(),
                resource_id: id(SKILL_ID),
                expected_resource_version: 4,
                target: ActiveTarget::Deployment {
                    deployment: skill_deployment_ref.clone(),
                },
            }
        )
        .unwrap(),
    );
    assert_eq!(
        activated_skill.active_deployment_id.as_deref(),
        Some(SKILL_DEPLOYMENT_ID)
    );
    assert!(matches!(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: skill_activation_audit,
                resource_id: id(SKILL_ID),
                expected_resource_version: 4,
                target: ActiveTarget::Deployment {
                    deployment: skill_deployment_ref.clone(),
                },
            }
        )
        .unwrap(),
        CommandOutcome::Replayed(ref replayed) if replayed == &activated_skill
    ));
    let skill_suspension_audit = audit(TENANT_ID, PRINCIPAL_ID, "9b15", '2', 'c');
    let suspended_skill = applied(
        registry_command!(
            repository,
            suspend_resource_deployment,
            SuspendResourceDeployment {
                audit: skill_suspension_audit.clone(),
                resource_id: id(SKILL_ID),
                deployment_id: id(SKILL_DEPLOYMENT_ID),
                expected_resource_version: 5,
            }
        )
        .unwrap(),
    );
    assert_eq!(suspended_skill.gate_state, "suspended");

    let policy_revisions = policy_bindings
        .iter()
        .map(|binding| binding.revision.clone())
        .collect::<Vec<_>>();
    let sandbox_profile_document = ResourceDocument::SandboxProfile(SandboxProfileResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: qualification_artifact.clone(),
            manifest_digest: digest('2'),
        },
        contract_digest: digest('3'),
        dependency_versions: vec![],
        policy_versions: policy_revisions.clone(),
        allowed_trust_classes: vec![CodeTrustClass::BuiltIn],
        allowed_runtime_families: vec![SandboxRuntimeFamily::WasmWasi],
        minimum_isolation: SandboxIsolationClass::Wasm,
        isolation_policy: policy_revisions[0].clone(),
        resource_policy: policy_revisions[1].clone(),
        network_policy: policy_revisions[2].clone(),
        artifact_io_policy: policy_revisions[3].clone(),
        secret_policy: None,
        cleanup: SandboxCleanupPolicy::SingleUseDestroy,
        max_job_duration_milliseconds: 60_000,
        semantic_digest: digest('4'),
    });
    let sandbox_profile_draft = ResourceDraftPayload {
        display_name: "WASI deployment profile".to_owned(),
        document: sandbox_profile_document.clone(),
        validation: None,
    };
    applied(
        registry_command!(
            repository,
            create_resource_draft,
            CreateResourceDraft {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9b20", '5', '2'),
                resource_id: id(SANDBOX_PROFILE_ID),
                draft: sandbox_profile_draft.clone(),
            }
        )
        .unwrap(),
    );
    let sandbox_profile_draft_digest = sandbox_profile_draft.document_digest().unwrap();
    let sandbox_profile_validation = ValidationSummary {
        validator_digest: digest('5'),
        validated_draft_digest: sandbox_profile_draft_digest.clone(),
        dependency_closure_digest: digest('6'),
        security_evidence_digest: digest('7'),
        warnings: vec![],
    };
    applied(
        registry_command!(
            repository,
            record_resource_validation,
            RecordResourceValidation {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9b21", '7', '4'),
                resource_id: id(SANDBOX_PROFILE_ID),
                expected_resource_version: 1,
                expected_draft_digest: sandbox_profile_draft_digest.clone(),
                validation: sandbox_profile_validation.clone(),
            }
        )
        .unwrap(),
    );
    applied(
        registry_command!(
            repository,
            publish_resource_versions,
            PublishResourceVersions {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9b22", '5', '6'),
                resource_id: id(SANDBOX_PROFILE_ID),
                expected_resource_version: 2,
                expected_draft_digest: sandbox_profile_draft_digest,
                versions: vec![NewPublishedVersion {
                    resource_version_id: id(SANDBOX_PROFILE_VERSION_ID),
                    revision_no: 1,
                    content_digest: digest('8'),
                    artifact_id: None,
                    payload: PublishedVersionPayload {
                        document: sandbox_profile_document,
                        validation: sandbox_profile_validation,
                    },
                }],
            }
        )
        .unwrap(),
    );
    let sandbox_profile_closure = SandboxProfileDeploymentClosure {
        profile_revision: ExactVersionRef::new(id(SANDBOX_PROFILE_VERSION_ID), digest('8'))
            .unwrap(),
        runtime_revision: ExactVersionRef::new(
            sandbox_id(ResourceKind::SandboxRuntimeRevision, 2),
            digest('1'),
        )
        .unwrap(),
        policy_bindings,
        qualification_evidence: qualification_artifact,
    };
    let wrong_owner_audit = audit(TENANT_ID, PRINCIPAL_ID, "9b23", '5', '8');
    assert!(matches!(
        registry_command!(
            repository,
            create_deployment,
            CreateDeployment {
                audit: wrong_owner_audit.clone(),
                deployment_id: sandbox_id(ResourceKind::SkillDeployment, 0x8a23),
                resource_id: id(SANDBOX_PROFILE_ID),
                resource_version_id: id(SANDBOX_PROFILE_VERSION_ID),
                environment: "test".to_owned(),
                closure: DeploymentClosure::Skill(skill_closure),
                expected_resource_version: 3,
            }
        ),
        Err(RepositoryError::InvalidInput(_))
    ));
    let wrong_owner_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2",
    )
    .bind(TENANT_ID)
    .bind(wrong_owner_audit.receipt_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(wrong_owner_receipts, 0);
    let sandbox_profile_deployment = applied(
        registry_command!(
            repository,
            create_deployment,
            CreateDeployment {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "9b24", '5', 'a'),
                deployment_id: id(SANDBOX_PROFILE_DEPLOYMENT_ID),
                resource_id: id(SANDBOX_PROFILE_ID),
                resource_version_id: id(SANDBOX_PROFILE_VERSION_ID),
                environment: "test".to_owned(),
                closure: DeploymentClosure::SandboxProfile(sandbox_profile_closure),
                expected_resource_version: 3,
            }
        )
        .unwrap(),
    );
    let sandbox_profile_deployment_ref = ExactDeploymentRef::new(
        id(SANDBOX_PROFILE_DEPLOYMENT_ID),
        sandbox_profile_deployment.bindings.digest.parse().unwrap(),
    )
    .unwrap();
    let sandbox_activation_audit = audit(TENANT_ID, PRINCIPAL_ID, "9b25", '5', 'c');
    let activated_sandbox_profile = applied(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: sandbox_activation_audit.clone(),
                resource_id: id(SANDBOX_PROFILE_ID),
                expected_resource_version: 4,
                target: ActiveTarget::Deployment {
                    deployment: sandbox_profile_deployment_ref.clone(),
                },
            }
        )
        .unwrap(),
    );
    assert_eq!(
        activated_sandbox_profile.active_deployment_id.as_deref(),
        Some(SANDBOX_PROFILE_DEPLOYMENT_ID)
    );
    assert!(matches!(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: sandbox_activation_audit.clone(),
                resource_id: id(SANDBOX_PROFILE_ID),
                expected_resource_version: 4,
                target: ActiveTarget::Deployment {
                    deployment: sandbox_profile_deployment_ref,
                },
            }
        )
        .unwrap(),
        CommandOutcome::Replayed(ref replayed) if replayed == &activated_sandbox_profile
    ));
    let sandbox_suspension_audit = audit(TENANT_ID, PRINCIPAL_ID, "9b26", '3', 'e');
    let suspended_sandbox_profile = applied(
        registry_command!(
            repository,
            suspend_resource_deployment,
            SuspendResourceDeployment {
                audit: sandbox_suspension_audit.clone(),
                resource_id: id(SANDBOX_PROFILE_ID),
                deployment_id: id(SANDBOX_PROFILE_DEPLOYMENT_ID),
                expected_resource_version: 5,
            }
        )
        .unwrap(),
    );
    assert_eq!(suspended_sandbox_profile.gate_state, "suspended");
    assert!(matches!(
        registry_command!(
            repository,
            suspend_resource_deployment,
            SuspendResourceDeployment {
                audit: sandbox_suspension_audit.clone(),
                resource_id: id(SANDBOX_PROFILE_ID),
                deployment_id: id(SANDBOX_PROFILE_DEPLOYMENT_ID),
                expected_resource_version: 5,
            }
        )
        .unwrap(),
        CommandOutcome::Replayed(ref replayed) if replayed == &suspended_sandbox_profile
    ));
    for command_audit in [
        skill_suspension_audit,
        sandbox_activation_audit,
        sandbox_suspension_audit,
    ] {
        let atomic_evidence: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND receipt_id = $2),
              (SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_id = $3),
              (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1 AND outbox_id = $4)
            "#,
        )
        .bind(TENANT_ID)
        .bind(command_audit.receipt_id.to_string())
        .bind(command_audit.event_id.to_string())
        .bind(command_audit.outbox_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(atomic_evidence, (1, 1, 1));
    }
}
