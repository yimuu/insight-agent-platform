//! Current authority checks for ephemeral model text; never grants execution authority.
use super::*;
use insight_platform_models::ModelLiveTextDelta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveRunStatus {
    Open,
    Terminal,
    Cancelled,
}
impl PgRepository {
    pub async fn authorize_live_run(
        &self,
        tenant: &ResourceId,
        principal_id: &ResourceId,
        kind: PrincipalKind,
        binding_generation: u64,
        run_id: &ResourceId,
        delta: Option<&ModelLiveTextDelta>,
    ) -> Result<(LiveRunStatus, bool), RepositoryError> {
        let mut tx = begin_read_only_repeatable(&self.pool).await?;
        let principal =
            load_current_principal_snapshot(&mut tx, tenant, principal_id, kind).await?;
        if principal.binding_generation != binding_generation
            || !principal.permissions.contains(Permission::RuntimeRead)
            || !insight_platform_contracts::permits_content_disclosure(
                &principal,
                insight_platform_contracts::ExecutionAuthorizationPurpose::ContentDisclosure,
            )
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let run = load_run(&mut tx, tenant, run_id).await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        let status = if run.cancel_generation > 0 || run.state == "cancelled" {
            LiveRunStatus::Cancelled
        } else if run.terminal_at.is_some() || run.deadline <= now {
            LiveRunStatus::Terminal
        } else {
            LiveRunStatus::Open
        };
        let mut allowed = false;
        if let Some(delta) = delta.filter(|_| status == LiveRunStatus::Open) {
            delta
                .validate(self.model_turn_limits())
                .map_err(|_| RepositoryError::InvalidInput("invalid live text".into()))?;
            if &delta.tenant_id != tenant || &delta.run_id != run_id {
                return Err(RepositoryError::PermissionDenied);
            }
            let turn = crate::model_turn_repository::load_model_turn(
                &mut tx,
                tenant,
                &delta.model_turn_id,
                false,
                self.model_turn_limits(),
            )
            .await?;
            if turn.run_id == *run_id
                && turn.state == insight_platform_contracts::ModelTurnState::InFlight
                && turn.deadline > now
                && turn.payload.current_job_id.as_ref() == Some(&delta.job_id)
                && turn.payload.admission.request_digest == delta.request_digest
                && turn.payload.admission.request.classification == delta.classification
            {
                let job = crate::model_turn_repository::load_model_job(
                    &mut tx,
                    tenant,
                    &delta.job_id,
                    false,
                )
                .await?;
                let projection = job_projection(&job)?;
                let observed_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                    .fetch_one(&mut *tx)
                    .await?;
                allowed = job.run_id.as_deref() == Some(run_id.to_string().as_str())
                    && job.owner_id == delta.model_turn_id.to_string()
                    && projection.state == insight_platform_contracts::JobState::Running
                    && projection.attempt_count == delta.attempt_no
                    && run.deadline > observed_at
                    && turn.deadline > observed_at
                    && projection.deadline > observed_at
                    && projection.lease_generation == delta.lease_generation
                    && projection.lease.as_ref().is_some_and(|lease| {
                        lease.worker_process_generation_id == delta.worker_process_generation_id
                            && lease.expires_at > observed_at
                    });
            }
        }
        tx.commit().await?;
        Ok((status, allowed))
    }
}
