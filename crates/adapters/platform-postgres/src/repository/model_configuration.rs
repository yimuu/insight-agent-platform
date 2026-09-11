//! Read-only current authority for the pure model configuration compiler.
use super::*;
use insight_platform_contracts::ModelInstallationCatalogV1;
use insight_platform_registry::model_configuration::{
    ModelConfigurationInputV1, ModelConfigurationSourceFacts,
};

impl PgRepository {
    pub async fn model_configuration_resources(
        &self,
        tenant: &ResourceId,
        principal: &ResourceId,
        kind: PrincipalKind,
        resource_kind: RegistryResourceKind,
        after: Option<&ResourceId>,
    ) -> Result<Vec<(ResourceRecord, Option<ExactDeploymentRef>)>, RepositoryError> {
        if !matches!(
            resource_kind,
            RegistryResourceKind::ModelProvider | RegistryResourceKind::ModelProfile
        ) || after.is_some_and(|id| id.kind() != resource_kind.id_kind())
        {
            return Err(RepositoryError::InvalidInput(
                "model resource page".to_owned(),
            ));
        }
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let current = load_current_principal_snapshot(&mut tx, tenant, principal, kind).await?;
        if !current.permissions.contains(Permission::ModelRead)
            || load_tenant(&mut tx, tenant).await?.state != "active"
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let rows=sqlx::query("SELECT * FROM insight_platform.resources WHERE tenant_id=$1 AND resource_kind=$2 AND ($3::text IS NULL OR resource_id>$3) ORDER BY resource_id LIMIT 26")
            .bind(tenant.to_string()).bind(resource_kind.as_str()).bind(after.map(ToString::to_string)).fetch_all(&mut *tx).await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let resource = resource_from_row(row)?;
            let exact = if let Some(id) = &resource.active_deployment_id {
                let id: ResourceId = id.parse().map_err(|_| {
                    RepositoryError::CorruptRow("model active deployment identity".to_owned())
                })?;
                if Some(id.kind()) != resource_kind.deployment_kind() {
                    return Err(RepositoryError::CorruptRow(
                        "model active deployment kind".to_owned(),
                    ));
                }
                let deployment = load_deployment(&mut tx, tenant, &id).await?;
                let closure = decode_deployment_closure(&deployment.bindings)?;
                if deployment.resource_id != resource.resource_id
                    || closure.resource_kind() != resource_kind
                {
                    return Err(RepositoryError::CorruptRow(
                        "model active deployment owner".to_owned(),
                    ));
                }
                Some(
                    ExactDeploymentRef::new(
                        id,
                        deployment.bindings.digest.parse().map_err(|_| {
                            RepositoryError::CorruptRow("model deployment digest".to_owned())
                        })?,
                    )
                    .map_err(|_| {
                        RepositoryError::CorruptRow("model exact deployment".to_owned())
                    })?,
                )
            } else {
                None
            };
            items.push((resource, exact));
        }
        tx.commit().await?;
        Ok(items)
    }
    /// All compiler inputs are checked in one snapshot. The later ordinary publication commands
    /// reauthorize and validate exact dependencies before committing; a preview grants no permit.
    pub async fn model_configuration_facts(
        &self,
        tenant: &ResourceId,
        principal: &ResourceId,
        kind: PrincipalKind,
        catalog: &ModelInstallationCatalogV1,
        input: Option<&ModelConfigurationInputV1>,
        artifact: Option<&ArtifactRef>,
    ) -> Result<Option<ModelConfigurationSourceFacts>, RepositoryError> {
        if !catalog.validate() {
            return Err(RepositoryError::InvalidInput(
                "model installation catalog".to_owned(),
            ));
        }
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let current = load_current_principal_snapshot(&mut tx, tenant, principal, kind).await?;
        let permission = if input.is_some() {
            Permission::ModelWrite
        } else {
            Permission::ModelRead
        };
        if !current.permissions.contains(permission)
            || load_tenant(&mut tx, tenant).await?.state != "active"
        {
            return Err(RepositoryError::PermissionDenied);
        }
        // The installed catalog contains exact references, not authorization or a substitute for
        // the current Policy resource gates. Enforce each owning policy kind as well as its digest.
        for (exact, expected) in [
            (&catalog.policies.protocol, PolicyKind::Protocol),
            (&catalog.policies.safety, PolicyKind::ModelSafety),
            (&catalog.policies.budget, PolicyKind::Budget),
            (
                &catalog.policies.public_projection,
                PolicyKind::PublicProjection,
            ),
            (&catalog.policies.selection.revision, PolicyKind::Selection),
            (&catalog.policies.execution.revision, PolicyKind::Execution),
        ] {
            configuration_policy(&mut tx, tenant, exact, expected).await?;
        }
        for binding in [&catalog.policies.selection, &catalog.policies.execution] {
            let deployment =
                load_deployment(&mut tx, tenant, &binding.deployment.deployment_id).await?;
            if deployment.bindings.digest != binding.deployment.deployment_digest.to_string()
                || deployment.environment != catalog.environment
            {
                return Err(RepositoryError::Conflict(
                    "model installation policy deployment",
                ));
            }
            let DeploymentClosure::Policy(closure) =
                decode_deployment_closure(&deployment.bindings)?
            else {
                return Err(RepositoryError::Conflict("model installation policy kind"));
            };
            if closure.policy_revision != binding.revision {
                return Err(RepositoryError::Conflict(
                    "model installation policy revision",
                ));
            }
        }
        for destination in &catalog.destinations {
            for (exact, expected) in [
                (&destination.grant.network_policy, PolicyKind::Network),
                (&destination.grant.tls_policy, PolicyKind::Tls),
                (&destination.grant.trust_policy, PolicyKind::Trust),
                (&destination.grant.data_policy, PolicyKind::DataHandling),
            ] {
                configuration_policy(&mut tx, tenant, exact, expected).await?;
            }
        }
        let facts = match input {
            Some(ModelConfigurationInputV1::Source(source)) => {
                if !current.permissions.contains(Permission::SecretBind) {
                    return Err(RepositoryError::PermissionDenied);
                }
                validate_exact_secret_bindings_at_creation(
                    &mut tx,
                    tenant,
                    std::slice::from_ref(&source.credential),
                )
                .await?;
                None
            }
            Some(ModelConfigurationInputV1::Model(model)) => {
                let row=sqlx::query("SELECT d.* FROM insight_platform.deployments d JOIN insight_platform.resources r ON r.tenant_id=d.tenant_id AND r.resource_id=d.resource_id WHERE d.tenant_id=$1 AND d.deployment_id=$2 AND d.bindings_digest=$3 AND d.environment=$4 AND r.resource_kind='model_provider' AND r.lifecycle_state='active' AND r.gate_state='enabled' AND r.active_deployment_id=d.deployment_id")
                    .bind(tenant.to_string()).bind(model.source.deployment_id.to_string()).bind(model.source.deployment_digest.to_string()).bind(&catalog.environment)
                    .fetch_optional(&mut *tx).await?.ok_or(RepositoryError::NotFound("active model source"))?;
                let deployment = deployment_from_row(row)?;
                let DeploymentClosure::ModelProvider(closure) =
                    decode_deployment_closure(&deployment.bindings)?
                else {
                    return Err(RepositoryError::Conflict("model source kind"));
                };
                if deployment.resource_version_id
                    != closure.provider_revision.revision_id.to_string()
                {
                    return Err(RepositoryError::Conflict("model source revision"));
                }
                validate_deployment_closure_exists(
                    &mut tx,
                    tenant,
                    &DeploymentClosure::ModelProvider(closure.clone()),
                )
                .await?;
                validate_exact_secret_bindings_at_creation(
                    &mut tx,
                    tenant,
                    &closure.secret_bindings,
                )
                .await?;
                let published = crate::invocation_repository::load_enabled_exact_published_version(
                    &mut tx,
                    tenant,
                    &closure.provider_revision,
                    RegistryResourceKind::ModelProvider,
                )
                .await?;
                let ResourceDocument::ModelProvider(provider) = published.document else {
                    return Err(RepositoryError::Conflict("model provider document"));
                };
                Some(ModelConfigurationSourceFacts {
                    deployment: model.source.clone(),
                    closure,
                    provider,
                })
            }
            None => None,
        };
        if let Some(artifact) = artifact {
            artifact.validate().map_err(|_| {
                RepositoryError::InvalidInput("model declaration Artifact".to_owned())
            })?;
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.artifacts a JOIN insight_platform.artifact_blobs b ON b.tenant_id=a.tenant_id AND b.blob_id=a.blob_id WHERE a.tenant_id=$1 AND a.artifact_id=$2 AND a.state='ready' AND a.terminal_at IS NULL AND b.state='verified' AND b.deleted_at IS NULL AND b.content_digest=$3 AND b.size_bytes=$4 AND a.verified_media_type=$5 AND a.classification=$6)")
                .bind(tenant.to_string()).bind(artifact.artifact_id().to_string()).bind(artifact.content_digest().to_string())
                .bind(i64::try_from(artifact.byte_length()).map_err(|_|RepositoryError::InvalidInput("model declaration size".to_owned()))?)
                .bind(artifact.media_type()).bind(artifact.classification().as_str()).fetch_one(&mut *tx).await?;
            if !exists {
                return Err(RepositoryError::NotFound(
                    "ready model declaration Artifact",
                ));
            }
        }
        tx.commit().await?;
        Ok(facts)
    }
}
pub(super) async fn configuration_policy(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    exact: &ExactVersionRef,
    kind: PolicyKind,
) -> Result<(), RepositoryError> {
    let published = crate::invocation_repository::load_enabled_exact_published_version(
        tx,
        tenant,
        exact,
        RegistryResourceKind::Policy,
    )
    .await?;
    match published.document {
        ResourceDocument::Policy(policy) if policy.policy_kind == kind => Ok(()),
        _ => Err(RepositoryError::Conflict("model configuration policy kind")),
    }
}
