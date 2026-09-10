//! Read-only current authorization for a Remote Context physical dispatch.
use super::*;
use crate::repository::{
    begin_read_only_repeatable, load_current_principal_snapshot, load_run,
    load_secret_binding_resolution_in_transaction,
};
use insight_platform_context::RemoteContextSearchRequest;
use insight_platform_contracts::{
    ContextDispatchAuthorizationError, ContextDispatchAuthorizationV1, ContextDispatchPermitV1,
    SecretBindingState,
};
use insight_platform_security::ContextDispatchAuthority;

#[async_trait::async_trait]
impl ContextDispatchAuthority for PgRepository {
    async fn authorize_context_dispatch(
        &self,
        request: &ContextDispatchAuthorizationV1,
    ) -> Result<ContextDispatchPermitV1, ContextDispatchAuthorizationError> {
        self.authorize_current_context_dispatch(request)
            .await
            .map_err(|error| match error {
                RepositoryError::Database(_) | RepositoryError::CapacityUnavailable => {
                    ContextDispatchAuthorizationError::Unavailable
                }
                _ => ContextDispatchAuthorizationError::Rejected,
            })
    }
}
impl PgRepository {
    async fn authorize_current_context_dispatch(
        &self,
        request: &ContextDispatchAuthorizationV1,
    ) -> Result<ContextDispatchPermitV1, RepositoryError> {
        if !request.validate_at(Utc::now()) {
            return Err(RepositoryError::PermissionDenied);
        }
        let mut transaction = begin_read_only_repeatable(self.pool()).await?;
        let now = database_now(&mut transaction).await?;
        if !request.validate_at(now) {
            return Err(RepositoryError::PermissionDenied);
        }
        let query = load_context_query(
            &mut transaction,
            &request.tenant_id,
            &request.context_query_id,
            false,
            self.context_query_limits(),
        )
        .await?;
        let job =
            load_context_job(&mut transaction, &request.tenant_id, &request.job_id, false).await?;
        let projection = job_projection(&job)?;
        let payload: ContextJobPayload = decode_versioned_payload(&job.payload, "Context Job")?;
        payload
            .validate_for(&query, &projection, self.context_query_limits())
            .map_err(|_| RepositoryError::PermissionDenied)?;
        let lease = projection
            .lease
            .as_ref()
            .ok_or(RepositoryError::PermissionDenied)?;
        let admission = &query.payload.admission;
        if query.state != ContextQueryState::InFlight
            || projection.state != JobState::Running
            || query.payload.current_job_id.as_ref() != Some(&request.job_id)
            || projection.attempt_count != request.physical_attempt
            || lease.worker_process_generation_id != request.worker_process_generation_id
            || lease.lease_generation != request.lease_generation
            || lease.token_digest != request.lease_token_digest
            || lease.expires_at <= now
            || query.deadline <= now
            || request.deadline != admission.deadline
            || admission.canonical_digest != request.admission_digest
            || admission.request.input.content_digest != request.input_content_digest
            || job.quota_reservation_id.is_none()
            || payload.physical_outcome.is_some()
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let expected = RemoteContextSearchRequest::from_admission(
            request.tenant_id.clone(),
            request.job_id.clone(),
            projection.attempt_count,
            &insight_platform_jobs::JobFence {
                expected_version: projection.version,
                worker_process_generation_id: lease.worker_process_generation_id.clone(),
                lease_generation: lease.lease_generation,
                token_digest: lease.token_digest.clone(),
            },
            admission,
            ValueRef::Inline {
                value: serde_json::Value::Null,
            },
        )
        .map_err(|_| RepositoryError::PermissionDenied)?;
        if expected
            .metadata_digest()
            .map_err(|_| RepositoryError::PermissionDenied)?
            != request.request_metadata_digest
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
        if !current.permissions.contains(Permission::ContextQuery)
            || current.canonical_digest != admission.principal.canonical_digest
            || admission.grant.principal_digest != current.canonical_digest
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let tenant_active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.tenants WHERE tenant_id=$1 AND state='active')")
            .bind(request.tenant_id.to_string()).fetch_one(&mut *transaction).await?;
        let run = load_run(&mut transaction, &request.tenant_id, &query.run_id).await?;
        let node_active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM insight_platform.run_nodes WHERE tenant_id=$1 AND node_id=$2 AND run_id=$3 AND record_kind='node_execution' AND state IN ('running','waiting') AND node_kind='context_query' AND deadline>$4)")
            .bind(request.tenant_id.to_string()).bind(query.node_execution_id.to_string()).bind(query.run_id.to_string()).bind(now).fetch_one(&mut *transaction).await?;
        if !tenant_active
            || !node_active
            || !matches!(
                run.state.parse::<RunState>(),
                Ok(RunState::Running | RunState::Waiting)
            )
            || run.deadline <= now
            || run.current.control.pause_requested
            || run.current.control.cancel_requested_at.is_some()
            || run.current.control.timeout_requested_at.is_some()
            || run.bindings.canonical_digest != admission.run_bindings_digest
            || admission.grant.policy_generation != run.bindings.principal.binding_generation
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let deployment = load_exact_context_deployment(
            &mut transaction,
            &request.tenant_id,
            &admission.binding.context_deployment,
        )
        .await?;
        let resource = load_resource(
            &mut transaction,
            &request.tenant_id,
            &parse_id(&deployment.resource_id, "Context resource")?,
        )
        .await?;
        if resource.lifecycle_state != EntityLifecycle::Active.as_str()
            || resource.gate_state != "enabled"
        {
            return Err(RepositoryError::PermissionDenied);
        }
        for (exact, kind) in [
            (
                &admission.interface_revision,
                RegistryResourceKind::ContextSourceInterface,
            ),
            (
                &admission.implementation_revision,
                RegistryResourceKind::ContextSourceImplementation,
            ),
        ] {
            load_enabled_exact_published_version(&mut transaction, &request.tenant_id, exact, kind)
                .await?;
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
        for exact in &admission.context_closure.secret_bindings {
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
        let permit = ContextDispatchPermitV1 {
            schema_version: 1,
            request_digest: canonical_digest(
                &serde_json::to_value(request).map_err(|_| RepositoryError::PermissionDenied)?,
            )
            .map_err(|_| RepositoryError::PermissionDenied)?
            .parse()
            .map_err(|_| RepositoryError::PermissionDenied)?,
            valid_until: lease
                .expires_at
                .min(request.deadline)
                .min(query.deadline)
                .min(run.deadline),
        };
        transaction.commit().await?;
        Ok(permit)
    }
}
