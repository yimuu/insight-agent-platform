//! Real PostgreSQL transaction/authorization proof. The locator is synthetic; no provider IO is claimed.
use super::*;
use insight_platform_artifacts::{
    ArtifactObjectReadAuthority, ArtifactObjectReadAuthorityError, ArtifactOrigin,
    GatewayArtifactReadAuthority, GatewayArtifactReadRequest,
};
use insight_platform_registry::model_policy_bootstrap::build_model_policy_bootstrap;

async fn snapshot(pool: &sqlx::PgPool, tenant: &ResourceId) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    for query in [
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.tenants t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.resources t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.resource_versions t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.deployments t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.artifacts t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.artifact_blobs t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.artifact_links t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.events t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.receipts t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.quota_accounts t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.scheduler_tenant_state t WHERE tenant_id=$1",
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM insight_platform.tenant_principals t WHERE tenant_id=$1"
    ] {
        rows.push(
            sqlx::query_scalar(query)
                .bind(tenant.to_string())
                .fetch_one(pool)
                .await
                .unwrap(),
        );
    }
    rows
}
fn material(
    seed: &ModelPolicyBootstrapSeedV1,
    storage: Sha256Digest,
) -> ModelPolicyArtifactMaterialV1 {
    let built = build_model_policy_bootstrap(seed).unwrap();
    let generation = "synthetic-pg-transaction-proof";
    let evidence=canonical_digest(&serde_json::json!({"schema_version":1,"kind":"s3_workload_stage","tenant_id":seed.tenant_id,
        "artifact_id":seed.authoring_artifact_id,"blob_id":seed.authoring_blob_id,"object_generation":generation,
        "size_bytes":built.declaration_bytes.len(),"storage_binding_digest":storage})).unwrap().parse().unwrap();
    ModelPolicyArtifactMaterialV1 {
        schema_version: 1,
        seed_digest: seed.canonical_digest().unwrap(),
        content_digest: built.content_digest,
        size_bytes: built.declaration_bytes.len() as u64,
        storage_backend: "s3".into(),
        storage_binding_digest: storage,
        object_reference_ciphertext: b"synthetic-encrypted-locator-not-a-provider-proof".to_vec(),
        object_generation: generation.into(),
        key_id: "synthetic-pg-key".into(),
        backend_evidence_digest: evidence,
    }
}
fn new_seed(
    base: &BootstrapDevelopmentProfile,
    retention: ExactVersionRef,
    encryption: ResourceId,
) -> ModelPolicyBootstrapSeedV1 {
    ModelPolicyBootstrapSeedV1 {
        schema_version: 1,
        tenant_id: base.tenant.tenant_id.parse().unwrap(),
        installation_principal_id: base.installation.principal_id.clone(),
        created_by: base.developer.principal_id.clone(),
        request_id: fresh(ResourceKind::ServerRequest),
        environment: "development".into(),
        authoring_artifact_id: fresh(ResourceKind::Artifact),
        authoring_blob_id: fresh(ResourceKind::InternalBlob),
        model_quota_account_id: fresh(ResourceKind::QuotaAccount),
        encryption_domain_id: encryption,
        retention_policy: retention,
        retain_until: UtcTimestamp::from_datetime(Utc::now() + Duration::days(30)),
        policies: ModelBootstrapPolicyRole::ALL.map(|role| ModelBootstrapPolicyIdentityV1 {
            role,
            resource_id: fresh(ResourceKind::Policy),
            revision_id: fresh(ResourceKind::PolicyRevision),
            deployment_id: fresh(ResourceKind::PolicyDeployment),
            artifact_reference_id: fresh(ResourceKind::ArtifactLink),
        }),
    }
}

pub(super) async fn verify(
    pool: &sqlx::PgPool,
    repository: &PgRepository,
    base: &BootstrapDevelopmentProfile,
) {
    let tenant: ResourceId = base.tenant.tenant_id.parse().unwrap();
    let (retention, encryption, storage) = repository
        .read_model_policy_bootstrap_artifact_inputs(base)
        .await
        .unwrap();
    let seed = new_seed(base, retention, encryption);
    let physical = material(&seed, storage.clone());
    let before = snapshot(pool, &tenant).await;
    let mut bad = physical.clone();
    bad.object_generation = "not-the-sealed-generation".into();
    assert!(repository
        .bootstrap_model_policy_authority(base, &seed, &bad)
        .await
        .is_err());
    assert_eq!(snapshot(pool, &tenant).await, before);

    // Fail late, after nine resources/versions/references/deployments were inserted in the transaction.
    // The conflicting historical revision is outside the target resource set and must not be rewritten.
    let mut late = seed.clone();
    late.policies[9].revision_id = base
        .artifact_authority
        .as_ref()
        .unwrap()
        .scheduling_policy_revision_id
        .clone();
    let late_physical = material(&late, storage.clone());
    assert!(repository
        .bootstrap_model_policy_authority(base, &late, &late_physical)
        .await
        .is_err());
    assert_eq!(
        snapshot(pool, &tenant).await,
        before,
        "late unique conflict must roll back every seeded fact"
    );

    let (left, right) = tokio::join!(
        repository.bootstrap_model_policy_authority(base, &seed, &physical),
        repository.bootstrap_model_policy_authority(base, &seed, &physical)
    );
    assert!(matches!(
        (left.unwrap(), right.unwrap()),
        (BootstrapOutcome::Created, BootstrapOutcome::Replayed)
            | (BootstrapOutcome::Replayed, BootstrapOutcome::Created)
    ));
    repository
        .verify_model_policy_authority(base, &seed, &physical)
        .await
        .unwrap();
    let quota: (i64,i64,i64)=sqlx::query_as("SELECT limit_value,reserved_value,used_value FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$2").bind(tenant.to_string()).bind(seed.model_quota_account_id.to_string()).fetch_one(pool).await.unwrap();
    assert_eq!(
        quota,
        (
            insight_platform_contracts::INITIAL_MODEL_CONCURRENCY_LIMIT as i64,
            0,
            0
        )
    );
    // Verification preserves later valid allocation and reservations instead of repairing roots.
    sqlx::query("UPDATE insight_platform.quota_accounts SET limit_value=9,reserved_value=1,version=version+1 WHERE tenant_id=$1 AND quota_account_id=$2").bind(tenant.to_string()).bind(seed.model_quota_account_id.to_string()).execute(pool).await.unwrap();
    let changed = snapshot(pool, &tenant).await;
    repository
        .verify_model_policy_authority(base, &seed, &physical)
        .await
        .unwrap();
    repository
        .bootstrap_model_policy_authority(base, &seed, &physical)
        .await
        .unwrap();
    assert_eq!(snapshot(pool, &tenant).await, changed);
    let removed:serde_json::Value=sqlx::query_scalar("DELETE FROM insight_platform.quota_accounts q WHERE tenant_id=$1 AND quota_account_id=$2 RETURNING to_jsonb(q)").bind(tenant.to_string()).bind(seed.model_quota_account_id.to_string()).fetch_one(pool).await.unwrap();
    let missing = snapshot(pool, &tenant).await;
    assert!(repository
        .verify_model_policy_authority(base, &seed, &physical)
        .await
        .is_err());
    assert!(repository
        .bootstrap_model_policy_authority(base, &seed, &physical)
        .await
        .is_err());
    assert_eq!(snapshot(pool, &tenant).await, missing);
    sqlx::query("INSERT INTO insight_platform.quota_accounts SELECT * FROM jsonb_populate_record(NULL::insight_platform.quota_accounts,$1)").bind(removed).execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.quota_accounts SET scope_id=$2 WHERE tenant_id=$1 AND quota_account_id=$3").bind(tenant.to_string()).bind(seed.authoring_artifact_id.to_string()).bind(seed.model_quota_account_id.to_string()).execute(pool).await.unwrap();
    let broken = snapshot(pool, &tenant).await;
    assert!(repository
        .verify_model_policy_authority(base, &seed, &physical)
        .await
        .is_err());
    assert!(repository
        .bootstrap_model_policy_authority(base, &seed, &physical)
        .await
        .is_err());
    assert_eq!(snapshot(pool, &tenant).await, broken);
    sqlx::query("UPDATE insight_platform.quota_accounts SET scope_id=$1 WHERE tenant_id=$1 AND quota_account_id=$2").bind(tenant.to_string()).bind(seed.model_quota_account_id.to_string()).execute(pool).await.unwrap();
    let complete = snapshot(pool, &tenant).await;
    assert!(matches!(
        repository
            .bootstrap_model_policy_authority(base, &seed, &physical)
            .await
            .unwrap(),
        BootstrapOutcome::Replayed
    ));
    assert_eq!(snapshot(pool, &tenant).await, complete);
    let built = build_model_policy_bootstrap(&seed).unwrap();
    let links:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.artifact_links l JOIN insight_platform.resource_versions v ON v.tenant_id=l.tenant_id AND v.resource_version_id=l.owner_id WHERE l.tenant_id=$1 AND l.target_artifact_id=$2 AND l.link_kind='reference' AND l.owner_kind='resource_version' AND l.state='active' AND v.artifact_id=$2 AND v.resource_version_kind='policy_revision'")
        .bind(tenant.to_string()).bind(seed.authoring_artifact_id.to_string()).fetch_one(pool).await.unwrap();
    assert_eq!(links, 10);

    let read = GatewayArtifactReadRequest {
        authority: GatewayArtifactReadAuthority::PublicContent,
        tenant_id: tenant.clone(),
        principal_id: base.developer.principal_id.clone(),
        principal_kind: PrincipalKind::AgentAuthor,
        artifact: built.authoring_artifact.clone(),
        request_digest: support::digest("model-policy-read"),
        maximum_bytes: 65_536,
        deadline: Utc::now() + Duration::seconds(30),
    };
    assert!(matches!(
        repository.authorize_object_read(&read).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let binding=sqlx::query("SELECT permissions_schema_version,permissions,permissions_digest FROM insight_platform.tenant_principals WHERE tenant_id=$1 AND principal_id=$2")
        .bind(tenant.to_string()).bind(base.developer.principal_id.to_string()).fetch_one(pool).await.unwrap();
    let allowed = TypedPayload::new(
        1,
        &TenantPrincipalPayload {
            permissions: PermissionSet::new(vec![
                Permission::AgentRead,
                Permission::ArtifactRead,
                Permission::AgentWrite,
                Permission::PolicyRead,
                Permission::ModelRead,
            ])
            .unwrap(),
        },
    )
    .unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2")
        .bind(tenant.to_string()).bind(base.developer.principal_id.to_string()).bind(&allowed.value).bind(&allowed.digest).execute(pool).await.unwrap();
    let observed = repository
        .load_gateway_artifact(
            tenant.clone(),
            base.developer.principal_id.clone(),
            PrincipalKind::AgentAuthor,
            seed.authoring_artifact_id.clone(),
        )
        .await
        .unwrap();
    assert_eq!(observed.content, Some(built.authoring_artifact.clone()));
    assert_eq!(
        observed.artifact.metadata.origin,
        ArtifactOrigin::Installation {
            request_id: seed.request_id.clone()
        }
    );
    assert!(observed.artifact.metadata.upload_operation_id().is_err());
    let authorized = repository.authorize_object_read(&read).await.unwrap();
    assert_eq!(authorized.artifact, built.authoring_artifact);
    assert_eq!(authorized.blob_id, seed.authoring_blob_id);
    assert_eq!(authorized.object_generation, physical.object_generation);
    let mut foreign = read.clone();
    foreign.tenant_id = fresh(ResourceKind::Tenant);
    assert!(repository.authorize_object_read(&foreign).await.is_err());
    let protocol = ModelProviderWireProtocol::OpenAiResponses;
    let catalog = ModelInstallationCatalogV2 {
        schema_version: 2,
        environment: seed.environment.clone(),
        secret_provider_id: fresh(ResourceKind::SecretProvider),
        policies: built.configuration_policies(),
        adapters: vec![InstalledModelAdapter {
            qualified_name: protocol.qualified_name().into(),
            worker_manifest_digest: support::digest("synthetic-installed-model-worker"),
            adapter_contract_digest: protocol.adapter_contract_digest(),
        }],
    };
    assert!(catalog.validate());
    super::model_configuration_reads::verify(
        pool,
        repository,
        base,
        &catalog,
        &built.authoring_artifact,
    )
    .await;
    // The parent fixture deliberately retained an unavailable default pointer; it must fail clearly.
    assert!(repository
        .read_agent_authoring_model_for_principal(
            &tenant,
            &base.developer.principal_id,
            PrincipalKind::AgentAuthor,
            &catalog
        )
        .await
        .is_err());
    let original_config =
        sqlx::query("SELECT config,config_digest FROM insight_platform.tenants WHERE tenant_id=$1")
            .bind(tenant.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
    let mut config = original_config.get::<serde_json::Value, _>("config");
    config.as_object_mut().unwrap().remove("schema_version");
    let mut config: TenantConfig = serde_json::from_value(config).unwrap();
    config.default_model = None;
    let config = TypedPayload::new(1, &config).unwrap();
    sqlx::query(
        "UPDATE insight_platform.tenants SET config=$2,config_digest=$3 WHERE tenant_id=$1",
    )
    .bind(tenant.to_string())
    .bind(&config.value)
    .bind(&config.digest)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(
        repository
            .read_agent_authoring_model_for_principal(
                &tenant,
                &base.developer.principal_id,
                PrincipalKind::AgentAuthor,
                &catalog
            )
            .await
            .unwrap(),
        None
    );
    let mut wrong = catalog.clone();
    wrong.environment = "wrong-environment".into();
    assert!(repository
        .read_agent_authoring_model_for_principal(
            &tenant,
            &base.developer.principal_id,
            PrincipalKind::AgentAuthor,
            &wrong
        )
        .await
        .is_err());
    let mut wrong = catalog.clone();
    wrong.policies.execution = built.policy(ModelBootstrapPolicyRole::Tls).exact.clone();
    assert!(repository
        .read_agent_authoring_model_for_principal(
            &tenant,
            &base.developer.principal_id,
            PrincipalKind::AgentAuthor,
            &wrong
        )
        .await
        .is_err());
    let execution = built
        .policy(ModelBootstrapPolicyRole::Execution)
        .identity
        .resource_id
        .to_string();
    sqlx::query("UPDATE insight_platform.resources SET gate_state='disabled' WHERE tenant_id=$1 AND resource_id=$2")
        .bind(tenant.to_string()).bind(&execution).execute(pool).await.unwrap();
    assert!(repository
        .read_agent_authoring_model_for_principal(
            &tenant,
            &base.developer.principal_id,
            PrincipalKind::AgentAuthor,
            &catalog
        )
        .await
        .is_err());
    sqlx::query("UPDATE insight_platform.resources SET gate_state='enabled' WHERE tenant_id=$1 AND resource_id=$2")
        .bind(tenant.to_string()).bind(&execution).execute(pool).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.tenants SET config=$2,config_digest=$3 WHERE tenant_id=$1",
    )
    .bind(tenant.to_string())
    .bind(original_config.get::<serde_json::Value, _>("config"))
    .bind(original_config.get::<String, _>("config_digest"))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions_schema_version=$3,permissions=$4,permissions_digest=$5 WHERE tenant_id=$1 AND principal_id=$2")
        .bind(tenant.to_string()).bind(base.developer.principal_id.to_string()).bind(binding.get::<i32,_>("permissions_schema_version"))
        .bind(binding.get::<serde_json::Value,_>("permissions")).bind(binding.get::<String,_>("permissions_digest")).execute(pool).await.unwrap();
    assert_eq!(snapshot(pool, &tenant).await, complete);
    assert!(matches!(
        repository
            .read_agent_authoring_model_for_principal(
                &tenant,
                &base.developer.principal_id,
                PrincipalKind::AgentAuthor,
                &catalog
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));

    // Current settings/credits already changed in the parent fixture. Legitimate current policy gates,
    // released references and withdrawn installation permission also survive verification/recovery.
    let principal=sqlx::query("SELECT payload_schema_version,payload,payload_digest FROM insight_platform.principals WHERE principal_id=$1")
        .bind(base.installation.principal_id.to_string()).fetch_one(pool).await.unwrap();
    let revoked = TypedPayload::new(
        1,
        &PrincipalBindingsPayload {
            installation_bindings: vec![],
        },
    )
    .unwrap();
    sqlx::query(
        "UPDATE insight_platform.principals SET payload=$2,payload_digest=$3 WHERE principal_id=$1",
    )
    .bind(base.installation.principal_id.to_string())
    .bind(&revoked.value)
    .bind(&revoked.digest)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("UPDATE insight_platform.resources SET gate_state='disabled',active_deployment_id=NULL,version=version+1 WHERE tenant_id=$1 AND resource_id=$2")
        .bind(tenant.to_string()).bind(seed.policies[0].resource_id.to_string()).execute(pool).await.unwrap();
    sqlx::query("UPDATE insight_platform.artifact_links SET state='released',version=version+1,released_at=clock_timestamp(),updated_at=clock_timestamp() WHERE tenant_id=$1 AND artifact_link_id=$2")
        .bind(tenant.to_string()).bind(seed.policies[0].artifact_reference_id.to_string()).execute(pool).await.unwrap();
    let changed = snapshot(pool, &tenant).await;
    repository.verify_installation_profile(base).await.unwrap();
    repository
        .verify_model_policy_authority(base, &seed, &physical)
        .await
        .unwrap();
    assert!(matches!(
        repository
            .bootstrap_model_policy_authority(base, &seed, &physical)
            .await
            .unwrap(),
        BootstrapOutcome::Replayed
    ));
    assert_eq!(snapshot(pool, &tenant).await, changed);
    let fresh_seed = new_seed(
        base,
        seed.retention_policy.clone(),
        seed.encryption_domain_id.clone(),
    );
    let fresh_material = material(&fresh_seed, storage);
    assert!(matches!(
        repository
            .bootstrap_model_policy_authority(base, &fresh_seed, &fresh_material)
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert_eq!(snapshot(pool, &tenant).await, changed);
    // A different frozen reference identity cannot silently adopt the previous declaration.
    let mut drift = seed.clone();
    drift.policies[0].artifact_reference_id = fresh(ResourceKind::ArtifactLink);
    let drift_material = material(&drift, physical.storage_binding_digest.clone());
    assert!(repository
        .verify_model_policy_authority(base, &drift, &drift_material)
        .await
        .is_err());
    assert!(repository
        .bootstrap_model_policy_authority(base, &drift, &drift_material)
        .await
        .is_err());
    assert_eq!(snapshot(pool, &tenant).await, changed);
    sqlx::query("UPDATE insight_platform.principals SET payload_schema_version=$2,payload=$3,payload_digest=$4 WHERE principal_id=$1")
        .bind(base.installation.principal_id.to_string()).bind(principal.get::<i32,_>("payload_schema_version"))
        .bind(principal.get::<serde_json::Value,_>("payload")).bind(principal.get::<String,_>("payload_digest")).execute(pool).await.unwrap();
}
