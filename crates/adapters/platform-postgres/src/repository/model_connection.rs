use super::*;
use insight_platform_contracts::{
    ModelConnectionError, ModelConnectionProbeAuthorizationV1, ModelConnectionProbePermitV1,
    ModelConnectionTargetV1, ModelProviderWireProtocol, MODEL_API_KEY_PURPOSE,
    MODEL_PROBE_MAXIMUM_OUTPUT_TOKENS,
};
use insight_platform_security::ModelConnectionProbeAuthority;

#[async_trait::async_trait]
impl ModelConnectionProbeAuthority for PgRepository {
    async fn authorize_model_connection_probe(
        &self,
        request: &ModelConnectionProbeAuthorizationV1,
    ) -> Result<ModelConnectionProbePermitV1, ModelConnectionError> {
        self.authorize_model_connection_probe_inner(request)
            .await
            .map_err(|e| match e {
                RepositoryError::Database(_) => ModelConnectionError::Unavailable,
                _ => ModelConnectionError::Rejected,
            })
    }
}
impl PgRepository {
    async fn authorize_model_connection_probe_inner(
        &self,
        request: &ModelConnectionProbeAuthorizationV1,
    ) -> Result<ModelConnectionProbePermitV1, RepositoryError> {
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        if !request.validate_at(now) {
            return Err(RepositoryError::PermissionDenied);
        }
        let principal = load_current_principal_snapshot(
            &mut tx,
            &request.tenant_id,
            &request.principal_id,
            request.principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::ModelRead)
            || !principal.permissions.contains(Permission::SecretBind)
        {
            return Err(RepositoryError::PermissionDenied);
        }
        model_default_commands::validate_default_model_closure(
            &mut tx,
            &request.tenant_id,
            &request.model_deployment,
            false,
        )
        .await?;
        let model = load_deployment(
            &mut tx,
            &request.tenant_id,
            &request.model_deployment.deployment_id,
        )
        .await?;
        let DeploymentClosure::ModelProfile(closure) = decode_deployment_closure(&model.bindings)?
        else {
            return Err(RepositoryError::PermissionDenied);
        };
        let provider_record = load_deployment(
            &mut tx,
            &request.tenant_id,
            &closure.provider_deployment.deployment_id,
        )
        .await?;
        if model.environment != request.environment
            || provider_record.environment != request.environment
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let DeploymentClosure::ModelProvider(provider) =
            decode_deployment_closure(&provider_record.bindings)?
        else {
            return Err(RepositoryError::PermissionDenied);
        };
        let profile = crate::invocation_repository::load_enabled_exact_published_version(
            &mut tx,
            &request.tenant_id,
            &closure.profile_revision,
            RegistryResourceKind::ModelProfile,
        )
        .await?;
        let ResourceDocument::ModelProfile(profile) = profile.document else {
            return Err(RepositoryError::PermissionDenied);
        };
        let document = crate::invocation_repository::load_enabled_exact_published_version(
            &mut tx,
            &request.tenant_id,
            &provider.provider_revision,
            RegistryResourceKind::ModelProvider,
        )
        .await?;
        let ResourceDocument::ModelProvider(document) = document.document else {
            return Err(RepositoryError::PermissionDenied);
        };
        let protocol = [
            ModelProviderWireProtocol::OpenAiResponses,
            ModelProviderWireProtocol::AnthropicMessages,
        ]
        .into_iter()
        .find(|p| p.qualified_name() == document.installed_adapter.qualified_name)
        .ok_or(RepositoryError::PermissionDenied)?;
        if provider.secret_bindings.len() != 1
            || provider.secret_bindings[0].purpose.as_str() != MODEL_API_KEY_PURPOSE
            || !profile
                .modalities
                .input
                .contains(&insight_platform_contracts::ModelModality::Text)
            || !profile
                .modalities
                .output
                .contains(&insight_platform_contracts::ModelModality::Text)
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let secret = load_secret_binding_metadata(
            &mut tx,
            &request.tenant_id,
            &provider.secret_bindings[0].secret_binding_id,
        )
        .await?;
        let target = ModelConnectionTargetV1 {
            schema_version: 1,
            model_deployment: request.model_deployment.clone(),
            profile_revision: closure.profile_revision,
            provider_deployment: closure.provider_deployment,
            provider,
            model_identity: profile.model_identity,
            protocol,
            installed_adapter: document.installed_adapter,
            request_limits: document.request_limits,
            maximum_output_tokens: profile
                .limits
                .maximum_output_tokens
                .min(MODEL_PROBE_MAXIMUM_OUTPUT_TOKENS),
            maximum_input_text_bytes: profile.limits.maximum_text_bytes,
            credential_generation: u64::try_from(secret.generation)
                .map_err(|_| RepositoryError::PermissionDenied)?,
        };
        let permit = ModelConnectionProbePermitV1 {
            schema_version: 1,
            request_digest: request
                .canonical_digest()
                .map_err(|_| RepositoryError::PermissionDenied)?,
            target_digest: target
                .canonical_digest()
                .map_err(|_| RepositoryError::PermissionDenied)?,
            target,
            valid_until: request.deadline.clone(),
        };
        tx.commit().await?;
        Ok(permit)
    }

    pub async fn read_model_credential_for_principal(
        &self,
        tenant: &ResourceId,
        principal_id: &ResourceId,
        kind: PrincipalKind,
        binding: &ResourceId,
    ) -> Result<SecretBindingMetadataRecord, RepositoryError> {
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        if binding.kind() != ResourceKind::SecretBinding
            || load_tenant(&mut tx, tenant).await?.state != "active"
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let principal =
            load_current_principal_snapshot(&mut tx, tenant, principal_id, kind).await?;
        if !principal.permissions.contains(Permission::SecretBind)
            && !principal.permissions.contains(Permission::SecretRevoke)
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let binding = load_secret_binding_metadata(&mut tx, tenant, binding).await?;
        if binding.purpose != MODEL_API_KEY_PURPOSE {
            return Err(RepositoryError::NotFound("model credential"));
        }
        tx.commit().await?;
        Ok(binding)
    }
}

impl PgSecurityTransaction {
    pub async fn revoke_model_credential(
        &mut self,
        command: insight_platform_security::RevokeSecretBinding,
    ) -> Result<CommandOutcome<SecretBindingMetadataRecord>, RepositoryError> {
        if load_tenant(&mut self.transaction, &command.audit.tenant_id)
            .await?
            .state
            != "active"
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let binding = load_secret_binding_metadata(
            &mut self.transaction,
            &command.audit.tenant_id,
            &command.secret_binding_id,
        )
        .await?;
        if binding.purpose != MODEL_API_KEY_PURPOSE {
            return Err(RepositoryError::NotFound("model credential"));
        }
        self.revoke_secret_binding(command).await
    }
}
