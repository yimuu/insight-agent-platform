use super::*;
use insight_platform_registry::authoring::BindTenantModelDefault;

impl PgRepository {
    pub async fn read_model_default_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
    ) -> Result<TenantRecord, RepositoryError> {
        let mut transaction = begin_read_only_repeatable(self.pool()).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::ModelRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        let tenant = load_tenant(&mut transaction, tenant_id).await?;
        if tenant.state != "active" {
            return Err(RepositoryError::PermissionDenied);
        }
        transaction.commit().await?;
        Ok(tenant)
    }
}

impl PgRegistryTransaction {
    pub async fn bind_tenant_model_default(
        &mut self,
        command: BindTenantModelDefault,
    ) -> Result<CommandOutcome<TenantRecord>, RepositoryError> {
        command.validate_at(Utc::now()).map_err(|_| {
            RepositoryError::InvalidInput("model default command is invalid".to_owned())
        })?;
        let mut transaction = self.transaction.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::ModelWrite).await?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "tenant",
            &command.audit.tenant_id.to_string(),
            "model.default.set",
        )
        .await?
        {
            let record = load_tenant(&mut transaction, &command.audit.tenant_id).await?;
            if record.state != "active" {
                return Err(RepositoryError::PermissionDenied);
            }
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_tenant_for_update(&mut transaction, &command.audit.tenant_id).await?;
        if current.state != "active" || current.version != command.expected_tenant_version {
            return Err(RepositoryError::Conflict("tenant"));
        }
        if let Some(model) = &command.model {
            validate_default_model_closure(&mut transaction, &command.audit.tenant_id, model, true)
                .await?;
        }
        let mut config = current.config;
        config.default_model = command.model.clone();
        config
            .validate()
            .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
        let config = TypedPayload::with_limit(1, &config, 65_536)?;
        let row = sqlx::query(
            "UPDATE insight_platform.tenants SET config_schema_version=$3, config=$4, config_digest=$5, version=version+1, updated_at=clock_timestamp() WHERE tenant_id=$1 AND version=$2 AND state='active' RETURNING tenant_id,state,version,config_schema_version,config,config_digest,created_at,updated_at",
        ).bind(command.audit.tenant_id.to_string())
            .bind(command.expected_tenant_version).bind(config.schema_version)
            .bind(&config.value).bind(&config.digest)
            .fetch_optional(&mut *transaction).await?
            .ok_or(RepositoryError::Conflict("tenant"))?;
        let record = tenant_from_row(row)?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "tenant",
            &record.tenant_id,
            record.version,
            "model.default_changed",
            &TypedPayload::new(1, &serde_json::json!({"default_model": command.model}))?,
        )
        .await?;
        terminalize_command_receipt(&mut transaction, &command.audit, &record.tenant_id, "bound")
            .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(record))
    }
}

pub(crate) async fn validate_default_model_closure(
    transaction: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    model: &ExactDeploymentRef,
    lock_resources: bool,
) -> Result<(), RepositoryError> {
    if load_tenant(transaction, tenant).await?.state != "active" {
        return Err(RepositoryError::PermissionDenied);
    }
    let model_record = default_binding(transaction, tenant, model, true, lock_resources).await?;
    let closure = decode_deployment_closure(&model_record.bindings)?;
    let DeploymentClosure::ModelProfile(profile) = &closure else {
        return Err(RepositoryError::Conflict("default Model Deployment kind"));
    };
    if model_record.resource_version_id != profile.profile_revision.revision_id.to_string() {
        return Err(RepositoryError::Conflict(
            "default Model Deployment revision",
        ));
    }
    let provider_record = default_binding(
        transaction,
        tenant,
        &profile.provider_deployment,
        false,
        lock_resources,
    )
    .await?;
    let provider_closure = decode_deployment_closure(&provider_record.bindings)?;
    let DeploymentClosure::ModelProvider(provider) = &provider_closure else {
        return Err(RepositoryError::Conflict(
            "default Model Provider Deployment kind",
        ));
    };
    if provider_record.resource_version_id != provider.provider_revision.revision_id.to_string() {
        return Err(RepositoryError::Conflict(
            "default Model Provider Deployment revision",
        ));
    }
    for (exact, kind) in [
        (&provider.protocol_policy, PolicyKind::Protocol),
        (&provider.network_policy, PolicyKind::Network),
        (&provider.tls_policy, PolicyKind::Tls),
        (&provider.trust_policy, PolicyKind::Trust),
        (&provider.data_policy, PolicyKind::DataHandling),
        (&profile.data_policy, PolicyKind::DataHandling),
        (&profile.safety_policy, PolicyKind::ModelSafety),
        (&profile.budget_policy, PolicyKind::Budget),
        (
            &profile.public_projection_policy,
            PolicyKind::PublicProjection,
        ),
    ] {
        model_configuration::configuration_policy(transaction, tenant, exact, kind).await?;
    }
    for exact in &provider.secret_bindings {
        let current =
            load_secret_binding_metadata(transaction, tenant, &exact.secret_binding_id).await?;
        let payload: SecretBindingPayload =
            decode_typed_payload(&current.payload, "SecretBinding")?;
        let binding_id = current
            .secret_binding_id
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("SecretBinding identity".to_owned()))?;
        let purpose = current
            .purpose
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("SecretBinding purpose".to_owned()))?;
        let generation = u64::try_from(current.generation)
            .map_err(|_| RepositoryError::CorruptRow("SecretBinding generation".to_owned()))?;
        if current.state != "active"
            || current.provider_id != exact.provider_id.to_string()
            || payload.provider_id != exact.provider_id
            || payload.resolution_policy != exact.resolution_policy
            || !exact.permits_resolved_generation(&binding_id, &purpose, generation)
        {
            return Err(RepositoryError::Conflict(
                "default Model Provider SecretBinding",
            ));
        }
    }
    validate_deployment_closure_exists(transaction, tenant, &provider_closure).await?;
    validate_deployment_closure_exists(transaction, tenant, &closure).await
}

async fn default_binding(
    transaction: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    exact: &ExactDeploymentRef,
    require_active_head: bool,
    lock_resources: bool,
) -> Result<DeploymentRecord, RepositoryError> {
    let kind = deployment_owner_kind(exact.resource_kind)
        .ok_or(RepositoryError::Conflict("default model deployment kind"))?;
    let query = if lock_resources {
        "SELECT d.* FROM insight_platform.deployments d JOIN insight_platform.resources r ON r.tenant_id=d.tenant_id AND r.resource_id=d.resource_id WHERE d.tenant_id=$1 AND d.deployment_id=$2 AND d.bindings_digest=$3 AND r.resource_kind=$4 AND r.lifecycle_state='active' AND r.gate_state='enabled' AND (NOT $5::boolean OR r.active_deployment_id=d.deployment_id) FOR SHARE OF r"
    } else {
        "SELECT d.* FROM insight_platform.deployments d JOIN insight_platform.resources r ON r.tenant_id=d.tenant_id AND r.resource_id=d.resource_id WHERE d.tenant_id=$1 AND d.deployment_id=$2 AND d.bindings_digest=$3 AND r.resource_kind=$4 AND r.lifecycle_state='active' AND r.gate_state='enabled' AND (NOT $5::boolean OR r.active_deployment_id=d.deployment_id)"
    };
    let row = sqlx::query(query)
        .bind(tenant.to_string())
        .bind(exact.deployment_id.to_string())
        .bind(exact.deployment_digest.to_string())
        .bind(kind.as_str())
        .bind(require_active_head)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("default model deployment"))?;
    deployment_from_row(row)
}
