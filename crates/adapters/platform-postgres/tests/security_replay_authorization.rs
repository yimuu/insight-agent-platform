use chrono::{Duration, Utc};
use insight_platform_contracts::*;
use insight_platform_postgres::{
    repository::{NewPrincipal, NewTenant, NewTenantPrincipal, PgRepository, RepositoryError},
    verify_schema,
};
use insight_platform_security::{
    BindTenantArtifactPolicies, BindTenantPrincipal, BindTenantSchedulingPolicy,
    CreateSecretBinding, EncryptedOpaqueReference, RevokeSecretBinding, RevokeTenantPrincipal,
    RotateSecretBinding, UpdateTenantPrincipalPermissions,
};
use sqlx::{postgres::PgPoolOptions, PgPool};

fn id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}
fn digest(value: impl serde::Serialize) -> Sha256Digest {
    canonical_digest(&serde_json::to_value(value).unwrap())
        .unwrap()
        .parse()
        .unwrap()
}
fn audit(tenant: &ResourceId, actor: &ResourceId, operation: &str) -> CommandAudit {
    let receipt = id(ResourceKind::Receipt);
    CommandAudit {
        trace: TraceIdentityV1::generate(),
        tenant_id: tenant.clone(),
        principal_id: actor.clone(),
        principal_kind: PrincipalKind::TenantAdmin,
        receipt_id: receipt.clone(),
        event_id: id(ResourceKind::Event),
        outbox_id: id(ResourceKind::OutboxEvent),
        idempotency_key_digest: digest(&receipt),
        request_digest: digest(operation),
        receipt_expires_at: Utc::now() + Duration::hours(1),
    }
}
async fn seed_principal(repository: &PgRepository, principal: &ResourceId) {
    repository
        .create_principal(NewPrincipal {
            principal_id: principal.clone(),
            authentication_authority_digest: digest(principal),
            subject_digest: digest("subject"),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
}
async fn seed_admin(repository: &PgRepository, tenant: &ResourceId, actor: &ResourceId) {
    seed_principal(repository, actor).await;
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant.clone(),
            principal_id: actor.clone(),
            principal_kind: PrincipalKind::TenantAdmin,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::TenantManage,
                    Permission::SecretBind,
                    Permission::SecretRotate,
                    Permission::SecretRevoke,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();
}
async fn seed_policy(
    pool: &PgPool,
    tenant: &ResourceId,
    actor: &ResourceId,
    kind: PolicyKind,
) -> ExactDeploymentRef {
    let resource = id(ResourceKind::Policy);
    let revision = id(ResourceKind::PolicyRevision);
    let deployment = id(ResourceKind::PolicyDeployment);
    let evidence = ArtifactRef::new(
        id(ResourceKind::Artifact),
        digest("fixture policy evidence"),
        1,
        "application/json",
        DataClassification::Internal,
        None,
    )
    .unwrap();
    let (field, rules) = match kind {
        PolicyKind::Scheduling => (
            "scheduling",
            serde_json::json!({"version":1,"weight":1,"burst":16,"aging_rounds":4}),
        ),
        PolicyKind::Retention => (
            "retention",
            serde_json::json!({"version":1,"minimum_retention_seconds":3600,"gc_grace_seconds":86400,"tombstone_retention_seconds":2592000,"retain_provenance_sources":true,"delete_requires_approval":true}),
        ),
        PolicyKind::ArtifactIo => (
            "sandbox_artifact_io",
            serde_json::json!({"schema_version":3,"allowed_input_media_types":["application/json"],"allowed_output_media_types":[],"maximum_input_artifacts":1,"maximum_output_artifacts":0,
            "scanner_contract_digest":digest("fixture scanner"),"verification_evidence_ttl_milliseconds":60000,"verification_retry_backoff_milliseconds":1000,
            "write_storage_binding_digest":digest("fixture storage"),"encryption_domain_id":id(ResourceKind::EncryptionDomain),
            "deny_symlink":true,"deny_hardlink":true,"deny_device":true,"deny_fifo":true,"deny_socket":true,"deny_sparse_file":true,"archive_expansion_disabled":true}),
        ),
        _ => unreachable!("closed fixture policy kinds"),
    };
    let rules_digest = digest(&rules);
    let mut spec = serde_json::json!({"authoring_package":{"artifact":evidence,"manifest_digest":digest("authoring")},"contract_digest":digest("policy contract"),
        "dependency_versions":[],"policy_versions":[],"policy_kind":kind,"rules_digest":rules_digest});
    spec[field] = rules;
    let document = ResourceDocument::Policy(Box::new(serde_json::from_value(spec).unwrap()));
    document.validate().unwrap();
    let version = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document,
            validation: ValidationSummary {
                program_requirement: None,
                validator_digest: digest("validator"),
                validated_draft_digest: digest("draft"),
                dependency_closure_digest: digest("dependencies"),
                security_evidence_digest: digest("security"),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    sqlx::query("INSERT INTO insight_platform.resources(tenant_id,resource_id,resource_kind,lifecycle_state,gate_state,payload_digest) VALUES($1,$2,'policy','active','enabled',$3)")
        .bind(tenant.to_string()).bind(resource.to_string()).bind(&version.digest).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.resource_versions(tenant_id,resource_version_id,resource_id,resource_version_kind,revision_no,content_digest,payload_schema_version,payload,payload_digest,created_by) VALUES($1,$2,$3,'policy_revision',1,$4,1,$5,$6,$7)")
        .bind(tenant.to_string()).bind(revision.to_string()).bind(resource.to_string()).bind(rules_digest.to_string()).bind(&version.value).bind(&version.digest).bind(actor.to_string()).execute(pool).await.unwrap();
    let closure = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: ExactVersionRef::new(revision.clone(), rules_digest).unwrap(),
            applicability_digest: digest("applicability"),
            qualification_evidence: evidence,
        }),
    )
    .unwrap();
    sqlx::query("INSERT INTO insight_platform.deployments(tenant_id,deployment_id,resource_id,resource_version_id,environment,bindings_digest,payload_schema_version,bindings,created_by) VALUES($1,$2,$3,$4,'fixture',$5,1,$6,$7)")
        .bind(tenant.to_string()).bind(deployment.to_string()).bind(resource.to_string()).bind(revision.to_string()).bind(&closure.digest).bind(&closure.value).bind(actor.to_string()).execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2")
        .bind(tenant.to_string()).bind(resource.to_string()).bind(deployment.to_string()).execute(pool).await.unwrap();
    ExactDeploymentRef::new(deployment, closure.digest.parse().unwrap()).unwrap()
}
async fn journal_counts(pool: &PgPool, tenant: &ResourceId) -> (i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1),(SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id=$1)")
        .bind(tenant.to_string()).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn revoked_administrator_cannot_replay_any_security_projection() {
    let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("PLATFORM_TEST_DATABASE_URL is required for the real security replay fixture");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let tenant = id(ResourceKind::Tenant);
    let admin = id(ResourceKind::Principal);
    let revoker = id(ResourceKind::Principal);
    let member = id(ResourceKind::Principal);
    repository
        .create_tenant(NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    seed_admin(&repository, &tenant, &admin).await;
    seed_admin(&repository, &tenant, &revoker).await;
    seed_principal(&repository, &member).await;

    macro_rules! apply {
        ($method:ident, $command:expr) => {{
            let mut transaction = repository.begin_security_transaction().await.unwrap();
            assert!(matches!(
                transaction.$method($command.clone()).await.unwrap(),
                CommandOutcome::Applied(_)
            ));
            transaction.commit().await.unwrap();
        }};
    }
    let bind = BindTenantPrincipal {
        audit: audit(&tenant, &admin, "bind"),
        principal_id: member.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        permissions: PermissionSet::new(vec![Permission::AgentRead]).unwrap(),
    };
    apply!(bind_tenant_principal, bind);
    let update = UpdateTenantPrincipalPermissions {
        audit: audit(&tenant, &admin, "update"),
        principal_id: member.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        expected_generation: 1,
        expected_version: 1,
        permissions: PermissionSet::new(vec![Permission::AgentRead, Permission::AgentRun]).unwrap(),
    };
    apply!(update_tenant_principal_permissions, update);
    let revoke_member = RevokeTenantPrincipal {
        audit: audit(&tenant, &admin, "revoke member"),
        principal_id: member,
        principal_kind: PrincipalKind::AgentRunner,
        expected_generation: 2,
        expected_version: 2,
    };
    apply!(revoke_tenant_principal, revoke_member);
    let secret = id(ResourceKind::SecretBinding);
    let secret_payload = SecretBindingPayload {
        provider_id: id(ResourceKind::SecretProvider),
        resolution_policy: SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: digest("secret version"),
        },
    };
    let create = CreateSecretBinding {
        audit: audit(&tenant, &admin, "create secret"),
        secret_binding_id: secret.clone(),
        purpose: "model.provider".parse().unwrap(),
        encrypted_reference: EncryptedOpaqueReference::new(vec![1, 2, 3]).unwrap(),
        key_id: "fixture key".to_owned(),
        reference_digest: digest("secret reference"),
        payload: secret_payload.clone(),
    };
    apply!(create_secret_binding, create);
    let rotate = RotateSecretBinding {
        audit: audit(&tenant, &admin, "rotate secret"),
        secret_binding_id: secret.clone(),
        expected_generation: 1,
        expected_version: 1,
        encrypted_reference: EncryptedOpaqueReference::new(vec![4, 5, 6]).unwrap(),
        key_id: "rotated fixture key".to_owned(),
        reference_digest: digest("rotated reference"),
        payload: secret_payload,
        provider_evidence_digest: digest("rotation evidence"),
    };
    apply!(rotate_secret_binding, rotate);
    let revoke_secret = RevokeSecretBinding {
        audit: audit(&tenant, &admin, "revoke secret"),
        secret_binding_id: secret,
        expected_generation: 2,
        expected_version: 2,
    };
    apply!(revoke_secret_binding, revoke_secret);
    let scheduling = BindTenantSchedulingPolicy {
        audit: audit(&tenant, &admin, "bind scheduling"),
        expected_tenant_version: 1,
        policy: seed_policy(&pool, &tenant, &admin, PolicyKind::Scheduling).await,
    };
    apply!(bind_tenant_scheduling_policy, scheduling);
    let artifact = BindTenantArtifactPolicies {
        audit: audit(&tenant, &admin, "bind artifact"),
        expected_tenant_version: 2,
        retention_policy: seed_policy(&pool, &tenant, &admin, PolicyKind::Retention).await,
        artifact_io_policy: seed_policy(&pool, &tenant, &admin, PolicyKind::ArtifactIo).await,
    };
    apply!(bind_tenant_artifact_policies, artifact);

    macro_rules! replay {
        ($method:ident, $command:expr, $denied:expr) => {{
            let mut transaction = repository.begin_security_transaction().await.unwrap();
            let result = transaction.$method($command.clone()).await;
            if $denied {
                assert!(
                    matches!(result, Err(RepositoryError::PermissionDenied)),
                    "{} returned {result:?}",
                    stringify!($method)
                );
            } else {
                assert!(
                    matches!(result, Ok(CommandOutcome::Replayed(_))),
                    "{} returned {result:?}",
                    stringify!($method)
                );
            }
            transaction.rollback().await.unwrap();
        }};
    }
    for denied in [false, true] {
        if denied {
            let revoke_admin = RevokeTenantPrincipal {
                audit: audit(&tenant, &revoker, "revoke administrator"),
                principal_id: admin.clone(),
                principal_kind: PrincipalKind::TenantAdmin,
                expected_generation: 1,
                expected_version: 1,
            };
            apply!(revoke_tenant_principal, revoke_admin);
        }
        let before = journal_counts(&pool, &tenant).await;
        replay!(bind_tenant_principal, bind, denied);
        replay!(update_tenant_principal_permissions, update, denied);
        replay!(revoke_tenant_principal, revoke_member, denied);
        replay!(create_secret_binding, create, denied);
        replay!(rotate_secret_binding, rotate, denied);
        replay!(revoke_secret_binding, revoke_secret, denied);
        replay!(bind_tenant_scheduling_policy, scheduling, denied);
        replay!(bind_tenant_artifact_policies, artifact, denied);
        assert_eq!(journal_counts(&pool, &tenant).await, before);
    }
}
