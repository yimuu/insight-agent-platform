use chrono::{Duration, Utc};
use insight_platform_contracts::{
    ActiveTarget, AdministrativeGate, AgentDeploymentClosure, AgentResourceSpec, ArtifactRef,
    ArtifactRetentionPolicy, AuthoringPackage, CodeTrustClass, DataClassification,
    DeploymentClosure, EntityLifecycle, ExactDeploymentRef, ExactVersionRef, Permission,
    PermissionSet, PolicyKind, PolicyResourceSpec, PrincipalBindingsPayload, PrincipalKind,
    PrincipalSnapshot, PublishedVersionPayload, ResourceDocument, ResourceDraftPayload, ResourceId,
    ResourceKind, RunBindingsSnapshot, SandboxEntrypointKind, SandboxPackageResourceSpec,
    Sha256Digest, TenantConfig, TenantPrincipalPayload, ValidationSummary,
};
use insight_platform_postgres::{
    repository::{
        NewPrincipal, NewTenant, NewTenantPrincipal, PgRepository, RepositoryError, TypedPayload,
    },
    verify_schema,
};
use insight_platform_registry::{
    ActivateResource, CommandAudit, CommandOutcome, CreateDeployment, CreateResourceDraft,
    NewPublishedVersion, PublishResourceVersions, RecordResourceValidation,
    RequestResourceValidation, SetResourceGate, TransitionResourceLifecycle,
};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};

const TENANT_ID: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367c00";
const TENANT_B_ID: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367c01";
const PRINCIPAL_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367c02";
const DENIED_PRINCIPAL_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367c03";
const ARTIFACT_ID: &str = "art_0198f1c3-8f49-7c3e-b1f3-773c28367c04";
const ARTIFACT_BLOB_ID: &str = "iblb_0198f1c3-8f49-7c3e-b1f3-773c28367ca1";
const RETENTION_POLICY_ID: &str = "pol_0198f1c3-8f49-7c3e-b1f3-773c28367ca2";
const RETENTION_REVISION_ID: &str = "prev_0198f1c3-8f49-7c3e-b1f3-773c28367ca3";
const ENCRYPTION_DOMAIN_ID: &str = "enc_0198f1c3-8f49-7c3e-b1f3-773c28367ca4";
const RESOURCE_ID: &str = "pol_0198f1c3-8f49-7c3e-b1f3-773c28367c05";
const VERSION_ID: &str = "prev_0198f1c3-8f49-7c3e-b1f3-773c28367c06";
const JOB_ID: &str = "job_0198f1c3-8f49-7c3e-b1f3-773c28367c08";
const ROLLBACK_RESOURCE_ID: &str = "pol_0198f1c3-8f49-7c3e-b1f3-773c28367cb0";
const AGENT_ID: &str = "agt_0198f1c3-8f49-7c3e-b1f3-773c28367cd0";
const AGENT_INTERFACE_ID: &str = "aif_0198f1c3-8f49-7c3e-b1f3-773c28367cd1";
const AGENT_PLAN_ID: &str = "arev_0198f1c3-8f49-7c3e-b1f3-773c28367cd2";
const AGENT_DEPLOYMENT_ID: &str = "adep_0198f1c3-8f49-7c3e-b1f3-773c28367cd3";

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
    let validator_digest = digest('7');
    let now = Utc::now();
    registry_command!(
        repository,
        request_resource_validation,
        RequestResourceValidation {
            audit: audit(TENANT_ID, PRINCIPAL_ID, "8a20", '3', '4'),
            resource_id: package_resource_id.clone(),
            expected_resource_version: 1,
            job_id: sandbox_id(ResourceKind::Job, 8),
            validator_digest: validator_digest.clone(),
            validation_profile_digest: digest('8'),
            attempt_limit: 1,
            scheduled_at: now,
            deadline: now + Duration::minutes(5),
        }
    )
    .unwrap();
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
                config: TenantConfig {
                    scheduling_policy: None,
                },
            })
            .await
            .unwrap();
    }
    for (principal_id, subject) in [(PRINCIPAL_ID, '1'), (DENIED_PRINCIPAL_ID, '2')] {
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
                    Permission::AgentWrite,
                    Permission::AgentPublish,
                    Permission::AgentDeploy,
                    Permission::AgentActivate,
                    Permission::SandboxWrite,
                    Permission::SandboxPublish,
                ])
                .unwrap(),
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
    let retention_document = ResourceDocument::Policy(PolicyResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: authoring_artifact.clone(),
            manifest_digest: digest('a'),
        },
        contract_digest: digest('b'),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: PolicyKind::Retention,
        rules_digest: retention.canonical_digest().unwrap(),
        scheduling: None,
        retention: Some(retention),
        mcp_protocol: None,
        mcp_auth: None,
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        sandbox_secret_resolution: None,
    });
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
            encryption_domain_id, content_digest, size_bytes, state, verified_at
        ) VALUES ($1, $2, 'fixture', $3, $4, $5, 'generation-1', 'fixture-key',
                  $6, $7, 16, 'verified', clock_timestamp())
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

    let document = ResourceDocument::Policy(PolicyResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: authoring_artifact,
            manifest_digest: digest('6'),
        },
        contract_digest: digest('7'),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: PolicyKind::Authorization,
        rules_digest: digest('8'),
        scheduling: None,
        retention: None,
        mcp_protocol: None,
        mcp_auth: None,
        sandbox_isolation: None,
        sandbox_resource: None,
        sandbox_network: None,
        sandbox_artifact_io: None,
        sandbox_secret_resolution: None,
    });
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

    let validation = ValidationSummary {
        validator_digest: digest('e'),
        validated_draft_digest: draft_digest.clone(),
        dependency_closure_digest: digest('0'),
        security_evidence_digest: digest('1'),
        warnings: vec![],
    };
    let validated = applied(
        registry_command!(
            repository,
            record_resource_validation,
            RecordResourceValidation {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7c30", '2', '3'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 1,
                expected_draft_digest: draft_digest.clone(),
                validation: validation.clone(),
            }
        )
        .unwrap(),
    );
    assert_eq!(validated.version, 2);

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

    let published = applied(
        registry_command!(
            repository,
            publish_resource_versions,
            PublishResourceVersions {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7c50", '7', '8'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 2,
                expected_draft_digest: draft_digest,
                versions: vec![NewPublishedVersion {
                    resource_version_id: id(VERSION_ID),
                    revision_no: 1,
                    content_digest: digest('6'),
                    artifact_id: None,
                    payload: PublishedVersionPayload {
                        document,
                        validation,
                    },
                }],
            }
        )
        .unwrap(),
    );
    assert_eq!(published.resource.version, 3);
    assert!(published.resource.active_version_id.is_none());
    assert!(published.resource.active_deployment_id.is_none());

    let target = ActiveTarget::Version {
        version: ExactVersionRef::new(id(VERSION_ID), digest('6')).unwrap(),
    };
    let activated = applied(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7c60", '9', 'a'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 3,
                target: target.clone(),
            }
        )
        .unwrap(),
    );
    assert_eq!(activated.active_version_id.as_deref(), Some(VERSION_ID));
    assert_eq!(activated.version, 4);

    let suspended = applied(
        registry_command!(
            repository,
            set_resource_gate,
            SetResourceGate {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7c70", 'b', 'c'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 4,
                target: AdministrativeGate::Suspended,
            }
        )
        .unwrap(),
    );
    assert_eq!(suspended.version, 5);
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
                expected_resource_version: 5,
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
                expected_resource_version: 6,
                target: ActiveTarget::Version {
                    version: ExactVersionRef::new(id(VERSION_ID), digest('6')).unwrap(),
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
                expected_resource_version: 6,
                target: EntityLifecycle::Active,
            }
        )
        .unwrap(),
    );
    assert_eq!(restored.version, 7);
    let enabled = applied(
        registry_command!(
            repository,
            set_resource_gate,
            SetResourceGate {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7ca2", '5', '6'),
                resource_id: id(RESOURCE_ID),
                expected_resource_version: 7,
                target: AdministrativeGate::Enabled,
            }
        )
        .unwrap(),
    );
    assert_eq!(enabled.version, 8);

    let policy_ref = ExactVersionRef::new(id(VERSION_ID), digest('6')).unwrap();
    let agent_document = ResourceDocument::Agent(AgentResourceSpec {
        authoring_package: draft.document.authoring_package().clone(),
        contract_digest: digest('a'),
        dependency_versions: vec![policy_ref.clone()],
        policy_versions: vec![policy_ref.clone()],
        interface_schema_digest: digest('b'),
        typed_plan_digest: digest('c'),
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
    let agent_published = applied(
        registry_command!(
            repository,
            publish_resource_versions,
            PublishResourceVersions {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7cd6", 'a', 'b'),
                resource_id: id(AGENT_ID),
                expected_resource_version: 2,
                expected_draft_digest: agent_draft_digest,
                versions: vec![
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
                        artifact_id: None,
                        payload: PublishedVersionPayload {
                            document: agent_document,
                            validation: agent_validation,
                        },
                    },
                ],
            }
        )
        .unwrap(),
    );
    assert_eq!(agent_published.versions.len(), 2);
    assert!(agent_published.resource.active_deployment_id.is_none());

    let closure = AgentDeploymentClosure {
        interface: interface_ref.clone(),
        plan: plan_ref.clone(),
        slots: vec![],
        policies: vec![policy_ref.clone()],
        execution_profile: policy_ref.clone(),
    };
    let deployment = applied(
        registry_command!(
            repository,
            create_deployment,
            CreateDeployment {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7cd7", 'c', 'd'),
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

    let activated_agent = applied(
        registry_command!(
            repository,
            activate_resource,
            ActivateResource {
                audit: audit(TENANT_ID, PRINCIPAL_ID, "7cd9", 'e', 'f'),
                resource_id: id(AGENT_ID),
                expected_resource_version: 4,
                target: ActiveTarget::Deployment {
                    deployment:
                        ExactDeploymentRef::new(id(AGENT_DEPLOYMENT_ID), deployment_digest,)
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
                expected_resource_version: 8,
                draft: first_updated_draft.clone(),
            }
        )
        .unwrap(),
    );
    assert_eq!(first_update.version, 9);
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
                expected_resource_version: 9,
                draft: second_updated_draft,
            }
        )
        .unwrap(),
    );
    assert_eq!(second_update.version, 10);

    let replay = registry_command!(
        repository,
        update_resource_draft,
        insight_platform_registry::UpdateResourceDraft {
            audit: first_update_audit,
            resource_id: id(RESOURCE_ID),
            expected_resource_version: 8,
            draft: first_updated_draft,
        }
    )
    .unwrap();
    let CommandOutcome::Replayed(replayed) = replay else {
        panic!("expected exact historical draft update replay");
    };
    assert_eq!(replayed.version, 9);
    assert_eq!(replayed.draft_generation, 2);
    assert_eq!(
        replayed.payload.value["display_name"],
        "First replay-stable draft"
    );
}
