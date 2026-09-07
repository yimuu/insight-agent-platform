use insight_platform_contracts::Sha256Digest;
fn digest(label: &str) -> Sha256Digest {
    insight_platform_contracts::canonical_digest(
        &serde_json::json!({"qualification_fixture": label}),
    )
    .unwrap()
    .parse()
    .unwrap()
}

/// Installs a validated scheduling Policy and binds it through the same security
/// transaction used by control-plane commands. No unbound-worker bypass exists.
pub async fn bind_fixture_scheduling_policy(
    pool: &sqlx::PgPool,
    repository: &insight_platform_postgres::repository::PgRepository,
    tenant: &insight_platform_contracts::ResourceId,
) {
    use insight_platform_contracts::*;
    use insight_platform_postgres::repository::{NewPrincipal, NewTenantPrincipal};
    let fresh = |kind| ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap();
    let principal = fresh(ResourceKind::Principal);
    let resource = fresh(ResourceKind::Policy);
    let revision = fresh(ResourceKind::PolicyRevision);
    let deployment = fresh(ResourceKind::PolicyDeployment);
    let identity = canonical_digest(&serde_json::json!({"principal":principal}))
        .unwrap()
        .parse()
        .unwrap();
    repository
        .create_principal(NewPrincipal {
            principal_id: principal.clone(),
            authentication_authority_digest: identity,
            subject_digest: digest("scheduler-fixture-subject"),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant.clone(),
            principal_id: principal.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::TenantManage]).unwrap(),
            },
        })
        .await
        .unwrap();
    let scheduling = SchedulingPolicyDocument {
        version: 1,
        weight: 1,
        burst: 16,
        aging_rounds: 4,
    };
    let rules = scheduling.canonical_digest().unwrap();
    let evidence = ArtifactRef::new(
        fresh(ResourceKind::Artifact),
        digest("scheduler-fixture-evidence"),
        1,
        "application/json",
        DataClassification::Internal,
        None,
    )
    .unwrap();
    let document = ResourceDocument::Policy(Box::new(PolicyResourceSpec {
        authoring_package: AuthoringPackage {
            artifact: evidence.clone(),
            manifest_digest: digest("scheduler-fixture-authoring"),
        },
        contract_digest: digest("scheduler-fixture-contract"),
        dependency_versions: vec![],
        policy_versions: vec![],
        policy_kind: PolicyKind::Scheduling,
        rules_digest: rules.clone(),
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
    }));
    document.validate().unwrap();
    let published = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document,
            validation: ValidationSummary {
                program_requirement: None,
                validator_digest: digest("fixture-validator"),
                validated_draft_digest: digest("fixture-draft"),
                dependency_closure_digest: digest("fixture-dependencies"),
                security_evidence_digest: digest("fixture-security"),
                warnings: vec![],
            },
        },
    )
    .unwrap();
    sqlx::query("INSERT INTO insight_platform.resources(tenant_id,resource_id,resource_kind,lifecycle_state,gate_state,payload_digest) VALUES($1,$2,'policy','active','enabled',$3)").bind(tenant.to_string()).bind(resource.to_string()).bind(&published.digest).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.resource_versions(tenant_id,resource_version_id,resource_id,resource_version_kind,revision_no,content_digest,payload_schema_version,payload,payload_digest,created_by) VALUES($1,$2,$3,'policy_revision',1,$4,1,$5,$6,$7)").bind(tenant.to_string()).bind(revision.to_string()).bind(resource.to_string()).bind(rules.to_string()).bind(&published.value).bind(&published.digest).bind(principal.to_string()).execute(pool).await.unwrap();
    let bindings = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(PolicyDeploymentClosure {
            policy_revision: ExactVersionRef::new(revision, rules).unwrap(),
            applicability_digest: digest("scheduler-fixture-applicability"),
            qualification_evidence: evidence,
        }),
    )
    .unwrap();
    sqlx::query("INSERT INTO insight_platform.deployments(tenant_id,deployment_id,resource_id,resource_version_id,environment,bindings_digest,payload_schema_version,bindings,created_by) SELECT $1,$2,$3,resource_version_id,'fixture',$4,1,$5,$6 FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_id=$3").bind(tenant.to_string()).bind(deployment.to_string()).bind(resource.to_string()).bind(&bindings.digest).bind(&bindings.value).bind(principal.to_string()).execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.resources SET active_deployment_id=$3 WHERE tenant_id=$1 AND resource_id=$2").bind(tenant.to_string()).bind(resource.to_string()).bind(deployment.to_string()).execute(pool).await.unwrap();
    let version =
        sqlx::query_scalar("SELECT version FROM insight_platform.tenants WHERE tenant_id=$1")
            .bind(tenant.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
    let now = chrono::Utc::now();
    let mut transaction = repository.begin_security_transaction().await.unwrap();
    transaction
        .bind_tenant_scheduling_policy(insight_platform_security::BindTenantSchedulingPolicy {
            audit: CommandAudit {
                trace: TraceIdentityV1::generate(),
                tenant_id: tenant.clone(),
                principal_id: principal,
                principal_kind: PrincipalKind::AgentRunner,
                receipt_id: fresh(ResourceKind::Receipt),
                event_id: fresh(ResourceKind::Event),
                outbox_id: fresh(ResourceKind::OutboxEvent),
                idempotency_key_digest: canonical_digest(
                    &serde_json::json!({"deployment":deployment}),
                )
                .unwrap()
                .parse()
                .unwrap(),
                request_digest: bindings.digest.parse().unwrap(),
                receipt_expires_at: now + chrono::Duration::hours(1),
            },
            expected_tenant_version: version,
            policy: ExactDeploymentRef::new(deployment, bindings.digest.parse().unwrap()).unwrap(),
        })
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}
