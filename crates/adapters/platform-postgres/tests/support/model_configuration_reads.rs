//! Live PostgreSQL current-authority reads; direct draft fixtures make no provider IO claims.
use super::*;
use insight_platform_registry::model_configuration::*;

pub(super) async fn verify(
    pool: &sqlx::PgPool,
    repository: &PgRepository,
    base: &BootstrapDevelopmentProfile,
    catalog: &ModelInstallationCatalogV1,
    artifact: &ArtifactRef,
) {
    let tenant: ResourceId = base.tenant.tenant_id.parse().unwrap();
    let principal = &base.developer.principal_id;
    let kind = PrincipalKind::AgentAuthor;
    let original=sqlx::query("SELECT permissions,permissions_digest FROM insight_platform.tenant_principals WHERE tenant_id=$1 AND principal_id=$2")
        .bind(tenant.to_string()).bind(principal.to_string()).fetch_one(pool).await.unwrap();
    assert!(repository
        .model_configuration_facts(&tenant, principal, kind, catalog, None, Some(artifact))
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .model_configuration_resources(
            &tenant,
            principal,
            kind,
            RegistryResourceKind::ModelProvider,
            None
        )
        .await
        .unwrap()
        .is_empty());
    assert!(repository
        .model_configuration_resources(&tenant, principal, kind, RegistryResourceKind::Policy, None)
        .await
        .is_err());
    assert!(repository
        .model_configuration_resources(
            &tenant,
            principal,
            kind,
            RegistryResourceKind::ModelProvider,
            Some(&fresh(ResourceKind::ModelProfile))
        )
        .await
        .is_err());
    let mut wrong = catalog.clone();
    wrong.policies.protocol = wrong.policies.safety.clone();
    assert!(repository
        .model_configuration_facts(&tenant, principal, kind, &wrong, None, None)
        .await
        .is_err());
    let mut wrong = catalog.clone();
    wrong.environment = "other".into();
    assert!(repository
        .model_configuration_facts(&tenant, principal, kind, &wrong, None, None)
        .await
        .is_err());
    let changed = ArtifactRef::new(
        artifact.artifact_id().clone(),
        support::digest("wrong-declaration-content"),
        artifact.byte_length(),
        artifact.media_type(),
        artifact.classification(),
        artifact.display_name().map(str::to_owned),
    )
    .unwrap();
    assert!(repository
        .model_configuration_facts(&tenant, principal, kind, catalog, None, Some(&changed))
        .await
        .is_err());
    let source = ModelSourceConfigurationV1 {
        schema_version: 1,
        alias: "fixture.source".parse().unwrap(),
        display_name: "Fixture source".into(),
        destination_digest: catalog.destinations[0].canonical_digest().unwrap(),
        credential: ExactSecretBindingRef::build(
            fresh(ResourceKind::SecretBinding),
            1,
            catalog.secret_provider_id.clone(),
            MODEL_API_KEY_PURPOSE.parse().unwrap(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: support::digest("missing-credential"),
            },
        )
        .unwrap(),
    };
    assert!(matches!(
        repository
            .model_configuration_facts(
                &tenant,
                principal,
                kind,
                catalog,
                Some(&ModelConfigurationInputV1::Source(source.clone())),
                None
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    let allowed = TypedPayload::new(
        1,
        &TenantPrincipalPayload {
            permissions: PermissionSet::new(vec![
                Permission::ModelRead,
                Permission::ModelWrite,
                Permission::SecretBind,
            ])
            .unwrap(),
        },
    )
    .unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2")
        .bind(tenant.to_string()).bind(principal.to_string()).bind(&allowed.value).bind(&allowed.digest).execute(pool).await.unwrap();
    // A well-shaped binding never establishes an existing, current Secret authority.
    assert!(repository
        .model_configuration_facts(
            &tenant,
            principal,
            kind,
            catalog,
            Some(&ModelConfigurationInputV1::Source(source.clone())),
            None
        )
        .await
        .is_err());
    let declaration = declare_model_source(&source, catalog).unwrap();
    let declaration_artifact = ArtifactRef::new(
        fresh(ResourceKind::Artifact),
        declaration.content_digest.clone(),
        u64::from(declaration.size_bytes),
        "application/json",
        DataClassification::Internal,
        None,
    )
    .unwrap();
    let compiled = compile_model_source(&source, catalog, &declaration_artifact).unwrap();
    let mut identities = Vec::new();
    for index in 0..28 {
        let id = fresh(ResourceKind::ModelProvider);
        let mut draft = compiled.draft.clone();
        draft.alias = Some(format!("page.source.{index}").parse().unwrap());
        let payload = TypedPayload::new(1, &draft).unwrap();
        sqlx::query("INSERT INTO insight_platform.resources(tenant_id,resource_id,resource_kind,lifecycle_state,gate_state,payload,payload_digest) VALUES($1,$2,'model_provider','active','enabled',$3,$4)")
            .bind(tenant.to_string()).bind(id.to_string()).bind(payload.value).bind(payload.digest).execute(pool).await.unwrap();
        identities.push(id);
    }
    identities.sort();
    let first = repository
        .model_configuration_resources(
            &tenant,
            principal,
            kind,
            RegistryResourceKind::ModelProvider,
            None,
        )
        .await
        .unwrap();
    assert_eq!(first.len(), 26);
    assert!(first.iter().all(|(_, deployment)| deployment.is_none()));
    assert_eq!(
        first
            .iter()
            .map(|(r, _)| r.resource_id.clone())
            .collect::<Vec<_>>(),
        identities[..26]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    // The API emits 25 items and uses the 26th only as evidence of another page.
    let second = repository
        .model_configuration_resources(
            &tenant,
            principal,
            kind,
            RegistryResourceKind::ModelProvider,
            Some(&identities[24]),
        )
        .await
        .unwrap();
    assert_eq!(
        second
            .iter()
            .map(|(r, _)| r.resource_id.clone())
            .collect::<Vec<_>>(),
        identities[25..]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    assert!(repository
        .model_configuration_resources(
            &fresh(ResourceKind::Tenant),
            principal,
            kind,
            RegistryResourceKind::ModelProvider,
            None
        )
        .await
        .is_err());
    sqlx::query("UPDATE insight_platform.tenants SET state='suspended' WHERE tenant_id=$1")
        .bind(tenant.to_string())
        .execute(pool)
        .await
        .unwrap();
    assert!(repository
        .model_configuration_resources(
            &tenant,
            principal,
            kind,
            RegistryResourceKind::ModelProvider,
            Some(&identities[24])
        )
        .await
        .is_err());
    assert!(repository
        .model_configuration_facts(&tenant, principal, kind, catalog, None, None)
        .await
        .is_err());
    sqlx::query("UPDATE insight_platform.tenants SET state='active' WHERE tenant_id=$1")
        .bind(tenant.to_string())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE insight_platform.resources SET gate_state='disabled' WHERE tenant_id=$1 AND resource_id=(SELECT resource_id FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_version_id=$2)")
        .bind(tenant.to_string()).bind(catalog.policies.protocol.revision_id.to_string()).execute(pool).await.unwrap();
    assert!(repository
        .model_configuration_facts(&tenant, principal, kind, catalog, None, None)
        .await
        .is_err());
    sqlx::query("UPDATE insight_platform.resources SET gate_state='enabled' WHERE tenant_id=$1 AND resource_id=(SELECT resource_id FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_version_id=$2)")
        .bind(tenant.to_string()).bind(catalog.policies.protocol.revision_id.to_string()).execute(pool).await.unwrap();
    let denied = TypedPayload::new(
        1,
        &TenantPrincipalPayload {
            permissions: PermissionSet::new(vec![Permission::AgentRead]).unwrap(),
        },
    )
    .unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2")
        .bind(tenant.to_string()).bind(principal.to_string()).bind(denied.value).bind(denied.digest).execute(pool).await.unwrap();
    assert!(matches!(
        repository
            .model_configuration_resources(
                &tenant,
                principal,
                kind,
                RegistryResourceKind::ModelProvider,
                Some(&identities[24])
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .model_configuration_facts(&tenant, principal, kind, catalog, None, None)
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2")
        .bind(tenant.to_string()).bind(principal.to_string()).bind(original.get::<serde_json::Value,_>("permissions")).bind(original.get::<String,_>("permissions_digest")).execute(pool).await.unwrap();
    sqlx::query(
        "DELETE FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=ANY($2)",
    )
    .bind(tenant.to_string())
    .bind(
        identities
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    )
    .execute(pool)
    .await
    .unwrap();
}
