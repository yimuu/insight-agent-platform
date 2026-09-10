//! Current, read-only authorization for the physical Model Provider boundary.
use super::*;
use crate::repository::{
    begin_read_only_repeatable, load_current_principal_snapshot, load_run,
    load_secret_binding_resolution_in_transaction,
};
use insight_platform_contracts::{
    ModelDispatchAuthorizationError, ModelDispatchAuthorizationV1, ModelDispatchPermitV1,
    SecretBindingState,
};
use insight_platform_security::ModelDispatchAuthority;

#[async_trait::async_trait]
impl ModelDispatchAuthority for PgRepository {
    async fn authorize_model_dispatch(
        &self,
        request: &ModelDispatchAuthorizationV1,
    ) -> Result<ModelDispatchPermitV1, ModelDispatchAuthorizationError> {
        self.authorize_current_model_dispatch(request)
            .await
            .map_err(|error| match error {
                RepositoryError::Database(_) | RepositoryError::CapacityUnavailable => {
                    ModelDispatchAuthorizationError::Unavailable
                }
                _ => ModelDispatchAuthorizationError::Rejected,
            })
    }
}

impl PgRepository {
    async fn authorize_current_model_dispatch(
        &self,
        request: &ModelDispatchAuthorizationV1,
    ) -> Result<ModelDispatchPermitV1, RepositoryError> {
        if !request.validate_at(Utc::now()) {
            return Err(RepositoryError::PermissionDenied);
        }
        let mut transaction = begin_read_only_repeatable(self.pool()).await?;
        let now = database_now(&mut transaction).await?;
        if !request.validate_at(now) {
            return Err(RepositoryError::PermissionDenied);
        }
        let turn = load_model_turn(
            &mut transaction,
            &request.tenant_id,
            &request.model_turn_id,
            false,
            self.model_turn_limits(),
        )
        .await?;
        let job =
            load_model_job(&mut transaction, &request.tenant_id, &request.job_id, false).await?;
        let projection = job_projection(&job)?;
        let payload: ModelJobPayload = decode_versioned_payload(&job.payload, "Model Job")?;
        payload
            .validate_for(&turn, &projection, self.model_turn_limits())
            .map_err(|_| RepositoryError::PermissionDenied)?;
        let lease = projection
            .lease
            .as_ref()
            .ok_or(RepositoryError::PermissionDenied)?;
        let admission = &turn.payload.admission;
        let closure = &admission.provider_closure;
        let limits = &admission.provider.request_limits;
        if turn.state != ModelTurnState::InFlight
            || projection.state != JobState::Running
            || turn.payload.current_job_id.as_ref() != Some(&request.job_id)
            || projection.attempt_count != request.attempt_no
            || lease.worker_process_generation_id != request.worker_process_generation_id
            || lease.lease_generation != request.lease_generation
            || lease.expires_at <= now
            || turn.deadline <= now
            || request.deadline > admission.deadline
            || admission.canonical_digest != request.admission_digest
            || admission.request_digest != request.model_request_digest
            || admission.provider_deployment != request.provider_deployment
            || admission.provider_revision != request.provider_revision
            || closure.endpoint_identity_digest != request.endpoint_identity_digest
            || closure.secret_bindings != request.secret_bindings
            || closure.network_policy != request.network_policy
            || closure.tls_policy != request.tls_policy
            || closure.trust_policy != request.trust_policy
            || closure.data_policy != request.data_policy
            || closure.region != request.region
            || admission.provider.installed_adapter.qualified_name != request.adapter_qualified_name
            || request.maximum_request_bytes > limits.maximum_request_bytes
            || request.maximum_response_bytes > limits.maximum_response_bytes
            || request.connect_timeout_milliseconds > limits.connect_timeout_milliseconds
            || request.total_timeout_milliseconds > limits.total_timeout_milliseconds
            || payload.active_usage_reservation_id.is_none()
            || payload
                .active_usage_reservation_id
                .as_ref()
                .map(ToString::to_string)
                != job.quota_reservation_id
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let current = load_current_principal_snapshot(
            &mut transaction,
            &request.tenant_id,
            &admission.principal.principal_id,
            admission.principal.principal_kind,
        )
        .await?;
        if !current.permissions.contains(Permission::ModelInvoke) {
            return Err(RepositoryError::PermissionDenied);
        }
        let tenant_active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.tenants WHERE tenant_id=$1 AND state='active')")
            .bind(request.tenant_id.to_string()).fetch_one(&mut *transaction).await?;
        if !tenant_active {
            return Err(RepositoryError::PermissionDenied);
        }
        let run = load_run(&mut transaction, &request.tenant_id, &turn.run_id).await?;
        let node_active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.run_nodes WHERE tenant_id=$1 AND node_id=$2 AND run_id=$3 AND record_kind='node_execution' AND scope_id=$5 AND state IN ('running','waiting') AND node_kind='model_loop' AND deadline>$4)")
            .bind(request.tenant_id.to_string()).bind(turn.node_execution_id.to_string())
            .bind(turn.run_id.to_string()).bind(now).bind(turn.scope_instance_id.to_string()).fetch_one(&mut *transaction).await?;
        if !matches!(
            run.state.parse::<RunState>(),
            Ok(RunState::Running | RunState::Waiting)
        ) || !node_active
            || run.deadline <= now
            || run.current.control.pause_requested
            || run.current.control.cancel_requested_at.is_some()
            || run.current.control.timeout_requested_at.is_some()
            || run.bindings.canonical_digest != admission.run_bindings_digest
        {
            return Err(RepositoryError::PermissionDenied);
        }
        for exact in [&admission.model_deployment, &admission.provider_deployment] {
            let deployment =
                load_deployment(&mut transaction, &request.tenant_id, &exact.deployment_id).await?;
            let resource_id = deployment
                .resource_id
                .parse()
                .map_err(|_| RepositoryError::PermissionDenied)?;
            let resource =
                load_resource(&mut transaction, &request.tenant_id, &resource_id).await?;
            if deployment.bindings.digest != exact.deployment_digest.to_string()
                || resource.lifecycle_state != EntityLifecycle::Active.as_str()
                || resource.gate_state != "enabled"
            {
                return Err(RepositoryError::PermissionDenied);
            }
        }
        for exact in &admission.policies {
            load_enabled_exact_published_version(
                &mut transaction,
                &request.tenant_id,
                exact,
                RegistryResourceKind::Policy,
            )
            .await?;
        }
        for exact in &closure.secret_bindings {
            let current = load_secret_binding_resolution_in_transaction(
                &mut transaction,
                &request.tenant_id,
                &exact.secret_binding_id,
            )
            .await?;
            if current.state != SecretBindingState::Active
                || current.provider_id != exact.provider_id
                || current.payload.provider_id != exact.provider_id
                || current.payload.resolution_policy != exact.resolution_policy
                || !exact.permits_resolved_generation(
                    &current.secret_binding_id,
                    &current.purpose,
                    current.generation,
                )
            {
                return Err(RepositoryError::PermissionDenied);
            }
        }
        let request_digest = canonical_digest(
            &serde_json::to_value(request).map_err(|_| RepositoryError::PermissionDenied)?,
        )
        .map_err(|_| RepositoryError::PermissionDenied)?
        .parse()
        .map_err(|_| RepositoryError::PermissionDenied)?;
        let permit = ModelDispatchPermitV1 {
            schema_version: 1,
            request_digest,
            valid_until: lease
                .expires_at
                .min(request.deadline)
                .min(turn.deadline)
                .min(run.deadline),
        };
        transaction.commit().await?;
        Ok(permit)
    }
}
