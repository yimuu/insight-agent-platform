//! PostgreSQL registry queries. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgRepository {
    pub async fn read_resource_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        expected_kind: RegistryResourceKind,
        resource_id: &ResourceId,
    ) -> Result<ResourceRecord, RepositoryError> {
        if resource_id.kind() != expected_kind.id_kind() {
            return Err(RepositoryError::NotFound("resource"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal
            .permissions
            .contains(read_permission(expected_kind))
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let resource = load_resource(&mut transaction, tenant_id, resource_id).await?;
        if resource.resource_kind != expected_kind.as_str() {
            return Err(RepositoryError::NotFound("resource"));
        }
        transaction.commit().await?;
        Ok(resource)
    }

    /// Installed execution/selection policies and the tenant's optional default are resolved in
    /// one current-authority snapshot. This query creates no binding and mutates no default.
    pub async fn read_agent_authoring_model_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        catalog: &insight_platform_contracts::ModelInstallationCatalogV2,
    ) -> Result<Option<ExactDeploymentRef>, RepositoryError> {
        if !catalog.validate() {
            return Err(RepositoryError::CorruptRow(
                "authoring installation catalog".to_owned(),
            ));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if [
            Permission::AgentWrite,
            Permission::PolicyRead,
            Permission::ModelRead,
        ]
        .iter()
        .any(|permission| !principal.permissions.contains(*permission))
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let tenant = load_tenant(&mut transaction, tenant_id).await?;
        if tenant.state != "active" {
            return Err(RepositoryError::PermissionDenied);
        }
        for (binding, kind) in [
            (&catalog.policies.execution, PolicyKind::Execution),
            (&catalog.policies.selection, PolicyKind::Selection),
        ] {
            let (revision, _) = load_exact_active_policy_deployment(
                &mut transaction,
                tenant_id,
                &binding.deployment,
                kind,
            )
            .await?;
            let deployment = load_deployment(
                &mut transaction,
                tenant_id,
                &binding.deployment.deployment_id,
            )
            .await?;
            if revision != binding.revision || deployment.environment != catalog.environment {
                return Err(RepositoryError::Conflict("authoring installation policy"));
            }
        }
        if let Some(model) = &tenant.config.default_model {
            model_default_commands::validate_default_model_closure(
                &mut transaction,
                tenant_id,
                model,
                false,
            )
            .await?;
            let deployment =
                load_deployment(&mut transaction, tenant_id, &model.deployment_id).await?;
            if deployment.environment != catalog.environment {
                return Err(RepositoryError::Conflict(
                    "authoring default model environment",
                ));
            }
            let DeploymentClosure::ModelProfile(profile) =
                decode_deployment_closure(&deployment.bindings)?
            else {
                return Err(RepositoryError::Conflict("authoring default model kind"));
            };
            let provider = load_deployment(
                &mut transaction,
                tenant_id,
                &profile.provider_deployment.deployment_id,
            )
            .await?;
            let DeploymentClosure::ModelProvider(provider_closure) =
                decode_deployment_closure(&provider.bindings)?
            else {
                return Err(RepositoryError::Conflict("authoring default provider kind"));
            };
            if provider.environment != catalog.environment
                || provider_closure
                    .secret_bindings
                    .iter()
                    .any(|secret| secret.provider_id != catalog.secret_provider_id)
            {
                return Err(RepositoryError::Conflict(
                    "authoring default provider installation",
                ));
            }
        }
        transaction.commit().await?;
        Ok(tenant.config.default_model)
    }

    pub async fn read_resource_version_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        expected_kind: RegistryResourceKind,
        resource_id: &ResourceId,
        resource_version_id: &ResourceId,
    ) -> Result<ResourceVersionRecord, RepositoryError> {
        if resource_id.kind() != expected_kind.id_kind()
            || !expected_kind.allows_version_kind(resource_version_id.kind())
        {
            return Err(RepositoryError::NotFound("resource version"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal
            .permissions
            .contains(read_permission(expected_kind))
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let row = sqlx::query(
            r#"
            SELECT version.tenant_id, version.resource_version_id, version.resource_id,
                   version.resource_version_kind, version.revision_no, version.content_digest,
                   version.artifact_id, version.payload_schema_version, version.payload,
                   version.payload_digest, version.created_by, version.created_at,
                   resource.resource_kind AS parent_resource_kind
            FROM insight_platform.resource_versions AS version
            JOIN insight_platform.resources AS resource
              ON resource.tenant_id = version.tenant_id
             AND resource.resource_id = version.resource_id
            WHERE version.tenant_id = $1 AND version.resource_id = $2
              AND version.resource_version_id = $3
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(resource_version_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("resource version"))?;
        if row.try_get::<String, _>("parent_resource_kind")? != expected_kind.as_str() {
            return Err(RepositoryError::NotFound("resource version"));
        }
        let record = resource_version_from_row(row)?;
        if record.resource_version_kind != resource_version_id.kind().descriptor().name {
            return Err(RepositoryError::CorruptRow(
                "resource version kind differs from its nominal ID".to_owned(),
            ));
        }
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn read_deployment_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        expected_kind: RegistryResourceKind,
        resource_id: &ResourceId,
        deployment_id: &ResourceId,
    ) -> Result<DeploymentRecord, RepositoryError> {
        if resource_id.kind() != expected_kind.id_kind()
            || expected_kind.deployment_kind() != Some(deployment_id.kind())
        {
            return Err(RepositoryError::NotFound("deployment"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal
            .permissions
            .contains(read_permission(expected_kind))
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let row = sqlx::query(
            r#"
            SELECT deployment.tenant_id, deployment.deployment_id, deployment.resource_id,
                   deployment.resource_version_id, deployment.environment,
                   deployment.bindings_digest, deployment.payload_schema_version,
                   deployment.bindings, deployment.created_by, deployment.created_at,
                   resource.resource_kind AS parent_resource_kind
            FROM insight_platform.deployments AS deployment
            JOIN insight_platform.resources AS resource
              ON resource.tenant_id = deployment.tenant_id
             AND resource.resource_id = deployment.resource_id
            WHERE deployment.tenant_id = $1 AND deployment.resource_id = $2
              AND deployment.deployment_id = $3
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(deployment_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("deployment"))?;
        if row.try_get::<String, _>("parent_resource_kind")? != expected_kind.as_str() {
            return Err(RepositoryError::NotFound("deployment"));
        }
        let record = deployment_from_row(row)?;
        if record.deployment_id != deployment_id.to_string()
            || record.resource_id != resource_id.to_string()
        {
            return Err(RepositoryError::CorruptRow(
                "deployment identity differs from its nominal path".to_owned(),
            ));
        }
        let closure = decode_deployment_closure(&record.bindings)?;
        if closure.resource_kind() != expected_kind {
            return Err(RepositoryError::CorruptRow(
                "deployment closure kind differs from its parent resource".to_owned(),
            ));
        }
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn read_mcp_deployment_for_discovery(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        resource_id: &ResourceId,
        deployment_id: &ResourceId,
    ) -> Result<DeploymentRecord, RepositoryError> {
        let expected_kind = RegistryResourceKind::McpServer;
        if resource_id.kind() != expected_kind.id_kind()
            || expected_kind.deployment_kind() != Some(deployment_id.kind())
        {
            return Err(RepositoryError::NotFound("MCP deployment"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::McpWrite) {
            return Err(RepositoryError::PermissionDenied);
        }
        let row = sqlx::query(
            r#"
            SELECT deployment.tenant_id, deployment.deployment_id, deployment.resource_id,
                   deployment.resource_version_id, deployment.environment,
                   deployment.bindings_digest, deployment.payload_schema_version,
                   deployment.bindings, deployment.created_by, deployment.created_at,
                   resource.resource_kind AS parent_resource_kind
            FROM insight_platform.deployments AS deployment
            JOIN insight_platform.resources AS resource
              ON resource.tenant_id = deployment.tenant_id
             AND resource.resource_id = deployment.resource_id
            WHERE deployment.tenant_id = $1 AND deployment.resource_id = $2
              AND deployment.deployment_id = $3
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(deployment_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("MCP deployment"))?;
        if row.try_get::<String, _>("parent_resource_kind")? != expected_kind.as_str() {
            return Err(RepositoryError::NotFound("MCP deployment"));
        }
        let record = deployment_from_row(row)?;
        if record.deployment_id != deployment_id.to_string()
            || record.resource_id != resource_id.to_string()
        {
            return Err(RepositoryError::CorruptRow(
                "MCP deployment identity differs from its nominal path".to_owned(),
            ));
        }
        let closure = decode_deployment_closure(&record.bindings)?;
        if closure.resource_kind() != expected_kind {
            return Err(RepositoryError::CorruptRow(
                "MCP deployment closure kind differs from its parent resource".to_owned(),
            ));
        }
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn read_context_deployment_for_build(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        resource_id: &ResourceId,
        deployment_id: &ResourceId,
    ) -> Result<DeploymentRecord, RepositoryError> {
        let expected_kind = RegistryResourceKind::ContextSourceInterface;
        if resource_id.kind() != expected_kind.id_kind()
            || expected_kind.deployment_kind() != Some(deployment_id.kind())
        {
            return Err(RepositoryError::NotFound("Context Deployment"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::ContextWrite) {
            return Err(RepositoryError::PermissionDenied);
        }
        let row = sqlx::query(
            r#"
            SELECT deployment.tenant_id, deployment.deployment_id, deployment.resource_id,
                   deployment.resource_version_id, deployment.environment,
                   deployment.bindings_digest, deployment.payload_schema_version,
                   deployment.bindings, deployment.created_by, deployment.created_at,
                   resource.resource_kind AS parent_resource_kind
            FROM insight_platform.deployments AS deployment
            JOIN insight_platform.resources AS resource
              ON resource.tenant_id = deployment.tenant_id
             AND resource.resource_id = deployment.resource_id
            WHERE deployment.tenant_id = $1 AND deployment.resource_id = $2
              AND deployment.deployment_id = $3
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(deployment_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("Context Deployment"))?;
        if row.try_get::<String, _>("parent_resource_kind")? != expected_kind.as_str() {
            return Err(RepositoryError::NotFound("Context Deployment"));
        }
        let record = deployment_from_row(row)?;
        if record.deployment_id != deployment_id.to_string()
            || record.resource_id != resource_id.to_string()
        {
            return Err(RepositoryError::CorruptRow(
                "Context Deployment identity differs from its nominal path".to_owned(),
            ));
        }
        let closure = decode_deployment_closure(&record.bindings)?;
        if closure.resource_kind() != expected_kind {
            return Err(RepositoryError::CorruptRow(
                "Context Deployment closure kind differs from its parent resource".to_owned(),
            ));
        }
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn read_deployment_for_activator(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        expected_kind: RegistryResourceKind,
        resource_id: &ResourceId,
        deployment_id: &ResourceId,
    ) -> Result<DeploymentRecord, RepositoryError> {
        if resource_id.kind() != expected_kind.id_kind()
            || expected_kind.deployment_kind() != Some(deployment_id.kind())
        {
            return Err(RepositoryError::NotFound("deployment"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal
            .permissions
            .contains(activate_permission(expected_kind))
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let deployment = load_deployment(&mut transaction, tenant_id, deployment_id).await?;
        if deployment.resource_id != resource_id.to_string() {
            return Err(RepositoryError::NotFound("deployment"));
        }
        let resource = load_resource(&mut transaction, tenant_id, resource_id).await?;
        if resource.resource_kind != expected_kind.as_str() {
            return Err(RepositoryError::NotFound("deployment"));
        }
        let closure = decode_deployment_closure(&deployment.bindings)?;
        if closure.resource_kind() != expected_kind {
            return Err(RepositoryError::CorruptRow(
                "deployment closure kind differs from its parent resource".to_owned(),
            ));
        }
        transaction.commit().await?;
        Ok(deployment)
    }

    pub async fn read_resource_for_writer(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        expected_kind: RegistryResourceKind,
        resource_id: &ResourceId,
    ) -> Result<ResourceRecord, RepositoryError> {
        if resource_id.kind() != expected_kind.id_kind() {
            return Err(RepositoryError::NotFound("resource"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal
            .permissions
            .contains(write_permission(expected_kind))
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let resource = load_resource(&mut transaction, tenant_id, resource_id).await?;
        if resource.resource_kind != expected_kind.as_str() {
            return Err(RepositoryError::NotFound("resource"));
        }
        transaction.commit().await?;
        Ok(resource)
    }

    pub async fn prepare_resource_publish(
        &self,
        audit: &CommandAudit,
        expected_kind: RegistryResourceKind,
        resource_id: &ResourceId,
    ) -> Result<ResourcePublishPreparation, RepositoryError> {
        if resource_id.kind() != expected_kind.id_kind() {
            return Err(RepositoryError::NotFound("resource"));
        }
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            &audit.tenant_id,
            &audit.principal_id,
            audit.principal_kind,
        )
        .await?;
        if !principal
            .permissions
            .contains(publish_permission(expected_kind))
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let receipt = sqlx::query(
            r#"
            SELECT request_digest, state
            FROM insight_platform.receipts
            WHERE tenant_id = $1 AND receipt_kind = 'command'
              AND scope_kind = 'resource' AND scope_id = $2 AND dedupe_owner_id = $3
              AND operation = 'resource.publish' AND idempotency_key_digest = $4
            "#,
        )
        .bind(audit.tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(audit.principal_id.to_string())
        .bind(audit.idempotency_key_digest.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(receipt) = receipt {
            if receipt.try_get::<String, _>("request_digest")? != audit.request_digest.to_string() {
                return Err(RepositoryError::IdempotencyConflict);
            }
            if receipt.try_get::<String, _>("state")? != "succeeded" {
                return Err(RepositoryError::Conflict("command receipt"));
            }
            let published =
                load_resource_publish_receipt(&mut transaction, audit, resource_id).await?;
            transaction.commit().await?;
            return Ok(ResourcePublishPreparation::Replayed(published));
        }
        let resource = load_resource(&mut transaction, &audit.tenant_id, resource_id).await?;
        if resource.resource_kind != expected_kind.as_str() {
            return Err(RepositoryError::NotFound("resource"));
        }
        transaction.commit().await?;
        Ok(ResourcePublishPreparation::Current(resource))
    }
}
