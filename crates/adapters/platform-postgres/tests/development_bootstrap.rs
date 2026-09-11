#[path = "support/artifact_prepare_role.rs"]
mod artifact_prepare_role;
#[path = "support/model_configuration_reads.rs"]
mod model_configuration_reads;
#[path = "support/model_policy_bootstrap.rs"]
mod model_policy_bootstrap;
mod support;

use chrono::{Duration, Utc};
use insight_platform_artifacts::{ArtifactJobPayload, ArtifactScanJobSnapshot, ArtifactScanKind};
use insight_platform_contracts::*;
use insight_platform_postgres::{repository::*, verify_schema};
use sqlx::{postgres::PgPoolOptions, Row};

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}

fn principal(label: &str) -> NewPrincipal {
    NewPrincipal {
        principal_id: fresh(ResourceKind::Principal),
        authentication_authority_digest: support::digest("bootstrap-authority"),
        subject_digest: support::digest(label),
        installation_bindings: PrincipalBindingsPayload {
            installation_bindings: vec![],
        },
    }
}

fn command() -> BootstrapDevelopmentProfile {
    let tenant = fresh(ResourceKind::Tenant);
    let developer = principal("developer");
    let scanner = insight_platform_artifacts::execution::integrity_scanner_contract_digest();
    BootstrapDevelopmentProfile {
        installation: BootstrapInstallationOperator {
            principal_id: fresh(ResourceKind::Principal),
            request_id: fresh(ResourceKind::ServerRequest),
            authentication_authority_digest: support::digest("bootstrap-authority"),
            subject_digest: support::digest("operator"),
            evidence_digest: support::digest("bootstrap-evidence"),
        },
        tenant: NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".into(),
            config: TenantConfig::default(),
        },
        tenant_principal_bindings: vec![NewTenantPrincipal {
            tenant_id: tenant,
            principal_id: developer.principal_id.clone(),
            principal_kind: PrincipalKind::AgentAuthor,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![
                    Permission::AgentRead,
                    Permission::ArtifactWrite,
                ])
                .unwrap(),
            },
        }],
        developer,
        service_principals: vec![principal("service")],
        artifact_authority: Some(DevelopmentArtifactAuthoritySeed {
            authoring_artifact_id: fresh(ResourceKind::Artifact),
            authoring_blob_id: fresh(ResourceKind::InternalBlob),
            retention_policy_id: fresh(ResourceKind::Policy),
            retention_policy_revision_id: fresh(ResourceKind::PolicyRevision),
            retention_policy_deployment_id: fresh(ResourceKind::PolicyDeployment),
            artifact_io_policy_id: fresh(ResourceKind::Policy),
            artifact_io_policy_revision_id: fresh(ResourceKind::PolicyRevision),
            artifact_io_policy_deployment_id: fresh(ResourceKind::PolicyDeployment),
            scheduling_policy_id: fresh(ResourceKind::Policy),
            scheduling_policy_revision_id: fresh(ResourceKind::PolicyRevision),
            scheduling_policy_deployment_id: fresh(ResourceKind::PolicyDeployment),
            staging_quota_account_id: fresh(ResourceKind::QuotaAccount),
            orchestration_quota_account_id: fresh(ResourceKind::QuotaAccount),
            retention_policy: ArtifactRetentionPolicy {
                version: 1,
                minimum_retention_seconds: 60,
                gc_grace_seconds: 60,
                tombstone_retention_seconds: 60,
                retain_provenance_sources: true,
                delete_requires_approval: false,
            },
            artifact_io_policy: SandboxArtifactIoPolicyDocument {
                schema_version: 3,
                allowed_input_media_types: vec!["application/json".into()],
                allowed_output_media_types: vec!["application/json".into()],
                maximum_input_artifacts: 16,
                maximum_output_artifacts: 16,
                scanner_contract_digest: scanner,
                verification_evidence_ttl_milliseconds: 60_000,
                verification_retry_backoff_milliseconds: 100,
                write_storage_binding_digest: support::digest("storage"),
                encryption_domain_id: fresh(ResourceKind::EncryptionDomain),
                deny_symlink: true,
                deny_hardlink: true,
                deny_device: true,
                deny_fifo: true,
                deny_socket: true,
                deny_sparse_file: true,
                archive_expansion_disabled: true,
            },
            scheduling_policy: SchedulingPolicyDocument {
                version: 1,
                weight: 2,
                burst: 16,
                aging_rounds: 4,
            },
            staging_quota_bytes: 1_048_576,
            orchestration_concurrent_jobs: 16,
        }),
    }
}

async fn fairness(pool: &sqlx::PgPool, tenant: &ResourceId) -> serde_json::Value {
    sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(s) ORDER BY work_class) FROM insight_platform.scheduler_tenant_state s WHERE tenant_id=$1")
        .bind(tenant.to_string()).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn bootstrap_atomically_binds_scheduling_and_replay_preserves_current_authority() {
    let url = std::env::var("PLATFORM_TEST_DEV_BOOTSTRAP_DATABASE_URL")
        .expect("PLATFORM_TEST_DEV_BOOTSTRAP_DATABASE_URL requires an isolated fresh current-schema database");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let command = command();
    let tenant: ResourceId = command.tenant.tenant_id.parse().unwrap();
    let seed = command.artifact_authority.as_ref().unwrap();
    assert!(matches!(
        repository
            .bootstrap_development_profile(command.clone())
            .await
            .unwrap(),
        BootstrapOutcome::Created
    ));
    Box::pin(artifact_prepare_role::verify(&pool, &url, &command)).await;

    let rows = sqlx::query("SELECT policy_version_id, rules_digest FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1")
        .bind(tenant.to_string()).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), WorkClass::ALL.len());
    for row in rows {
        assert_eq!(
            row.get::<String, _>("policy_version_id"),
            seed.scheduling_policy_revision_id.to_string()
        );
        assert_eq!(
            row.get::<String, _>("rules_digest"),
            seed.scheduling_policy
                .canonical_digest()
                .unwrap()
                .to_string()
        );
    }

    // A real Artifact claim must progress with the bootstrap binding, without any
    // fixture-side fairness repair or extra TenantManage privilege for the developer.
    let artifact = fresh(ResourceKind::Artifact);
    let job = fresh(ResourceKind::Job);
    let scanner = seed.artifact_io_policy.scanner_contract_digest.clone();
    let payload = ArtifactJobPayload::Scan {
        scan: ArtifactScanJobSnapshot {
            schema_version: 2,
            scan_kind: ArtifactScanKind::Initial,
            operation_id: fresh(ResourceKind::Job),
            producer_job_id: None,
            artifact_id: artifact.clone(),
            blob_id: fresh(ResourceKind::InternalBlob),
            expected_artifact_version: 1,
            expected_blob_version: 1,
            expected_operation_version: 1,
            object_generation: "fixture-generation".into(),
            scan_policy_revision: ExactVersionRef::new(
                seed.artifact_io_policy_revision_id.clone(),
                seed.artifact_io_policy.canonical_digest().unwrap(),
            )
            .unwrap(),
            scanner_contract_digest: scanner.clone(),
            ruleset_digest: seed.artifact_io_policy.canonical_digest().unwrap(),
            evidence_ttl_milliseconds: 60_000,
            retry_backoff_milliseconds: 100,
        },
    };
    repository
        .create_job(NewJob {
            tenant_id: tenant.to_string(),
            job_id: job.to_string(),
            job_kind: JobKind::ArtifactScan.as_str().into(),
            work_class: WorkClass::Artifact.as_str().into(),
            owner_kind: "artifact".into(),
            owner_id: artifact.to_string(),
            trace_id: TraceIdentityV1::generate().trace_id,
            invocation_id: None,
            run_id: None,
            node_id: None,
            attempt_limit: 3,
            scheduled_at: Utc::now(),
            deadline: Utc::now() + Duration::minutes(10),
            priority: SchedulerPriority::Normal,
            request_digest: support::digest("scan-request").to_string(),
            effect_key_digest: None,
            execution_requirement: payload.execution_requirement().unwrap(),
            payload: TypedPayload::new(2, &payload).unwrap(),
        })
        .await
        .unwrap();
    let mut manifest = support::manifest(
        "artifact-data-worker",
        WorkClass::Artifact,
        insight_platform_artifacts::execution::data_worker_execution_capabilities(&scanner),
    );
    manifest.worker_build_digest =
        insight_platform_worker::execution::executable_digest(&std::env::current_exe().unwrap())
            .unwrap();
    let claim = ClaimArtifactJobs {
        role: ArtifactWorkerRole::DataWorker,
        worker_manifest: manifest,
        worker_id: fresh(ResourceKind::WorkerProcessGeneration),
        limit: 1,
        lease_milliseconds: 60_000,
        lease_token_digests: vec![support::digest("scan-lease")],
    };
    let mut claimed = vec![];
    for _ in 0..520 {
        claimed = repository.claim_artifact_jobs(claim.clone()).await.unwrap();
        if !claimed.is_empty() {
            break;
        }
    }
    assert_eq!(
        claimed.len(),
        1,
        "a bootstrapped Artifact job must be claimable"
    );
    assert_eq!(claimed[0].job_id, job.to_string());
    assert_eq!(claimed[0].state, "leased");
    assert_eq!(claimed[0].lease_epoch, 1);
    assert_eq!(claimed[0].attempt_no, 0, "claim does not impersonate start");
    let after_claim = fairness(&pool, &tenant).await;
    assert!(matches!(
        repository
            .bootstrap_development_profile(command.clone())
            .await
            .unwrap(),
        BootstrapOutcome::Replayed
    ));
    assert_eq!(
        fairness(&pool, &tenant).await,
        after_claim,
        "replay must not reset credit, cohorts, versions or timestamps"
    );

    // Legitimate later binding is owned by the public security command. Restart
    // verifies that current exact policy, rather than forcing the original seed.
    support::bind_fixture_scheduling_policy(&pool, &repository, &tenant).await;
    let rebound = fairness(&pool, &tenant).await;
    assert_ne!(after_claim, rebound);
    assert!(matches!(
        repository
            .bootstrap_development_profile(command.clone())
            .await
            .unwrap(),
        BootstrapOutcome::Replayed
    ));
    assert_eq!(fairness(&pool, &tenant).await, rebound);

    // The default is a mutable management setting, including a retained exact pointer whose
    // Model may later become unavailable. Bootstrap must preserve the fact; resolution owns
    // its current deployment/gate checks (covered by the Model default command fixture).
    let mut config: serde_json::Value =
        sqlx::query_scalar("SELECT config FROM insight_platform.tenants WHERE tenant_id=$1")
            .bind(tenant.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        config.as_object_mut().unwrap().remove("schema_version"),
        Some(serde_json::json!(1))
    );
    let mut config: insight_platform_contracts::TenantConfig =
        serde_json::from_value(config).unwrap();
    config.default_model = Some(
        insight_platform_contracts::ExactDeploymentRef::new(
            fresh(ResourceKind::ModelDeployment),
            support::digest("retained-model-default"),
        )
        .unwrap(),
    );
    let payload = TypedPayload::with_limit(1, &config, 65_536).unwrap();
    sqlx::query("UPDATE insight_platform.tenants SET config=$2,config_digest=$3,version=version+1 WHERE tenant_id=$1")
        .bind(tenant.to_string()).bind(&payload.value).bind(&payload.digest).execute(&pool).await.unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM insight_platform.tenants t WHERE tenant_id=$1")
            .bind(tenant.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(matches!(
        repository
            .bootstrap_development_profile(command.clone())
            .await
            .unwrap(),
        BootstrapOutcome::Replayed
    ));
    let after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(t) FROM insight_platform.tenants t WHERE tenant_id=$1")
            .bind(tenant.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        before, after,
        "bootstrap cannot rotate or erase the user's default pointer"
    );

    model_policy_bootstrap::verify(&pool, &repository, &command).await;

    // Administratively injected corruption is diagnosed, never repaired by replay.
    sqlx::query("UPDATE insight_platform.scheduler_tenant_state SET policy_version_id=NULL,policy_version_digest=NULL,rules_digest=NULL WHERE tenant_id=$1 AND work_class='artifact'")
        .bind(tenant.to_string()).execute(&pool).await.unwrap();
    let unbound = fairness(&pool, &tenant).await;
    assert!(matches!(
        repository
            .bootstrap_development_profile(command.clone())
            .await,
        Err(RepositoryError::Conflict(_))
    ));
    assert_eq!(fairness(&pool, &tenant).await, unbound);
    sqlx::query("UPDATE insight_platform.scheduler_tenant_state s SET policy_version_id=other.policy_version_id,policy_version_digest=other.policy_version_digest,rules_digest=$2 FROM insight_platform.scheduler_tenant_state other WHERE s.tenant_id=$1 AND s.work_class='artifact' AND other.tenant_id=s.tenant_id AND other.work_class='orchestration'")
        .bind(tenant.to_string()).bind(support::digest("wrong-rules").to_string()).execute(&pool).await.unwrap();
    let drift = fairness(&pool, &tenant).await;
    assert!(matches!(
        repository.bootstrap_development_profile(command).await,
        Err(RepositoryError::Conflict(_))
    ));
    assert_eq!(fairness(&pool, &tenant).await, drift);
}
