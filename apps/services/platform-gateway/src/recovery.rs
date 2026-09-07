//! Management composition of existing Run hold and Task cleanup commands.
use super::{
    map_run_repository_error, CommandAudit, PgRepository, ResourceId, ResourceKind,
    RunApplicationError,
};
use async_trait::async_trait;
use insight_platform_api::recovery::{
    RecoveryApplication, RecoveryIntent, RecoveryRequest, RecoveryResultV1,
};
use insight_platform_contracts::CommandOutcome;
use insight_platform_mcp_host::RecoverMcpOAuthPkceCleanup;
use insight_platform_orchestrator::history::{PlaceRunHistoryHold, ReleaseRunHistoryHold};
use std::sync::Arc;

pub(crate) struct PgRecovery(pub Arc<PgRepository>);
fn new_id(kind: ResourceKind) -> Result<ResourceId, RunApplicationError> {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).map_err(|_| RunApplicationError::Internal)
}
fn audit(intent: &RecoveryIntent) -> Result<CommandAudit, RunApplicationError> {
    Ok(CommandAudit {
        trace: intent.principal.trace,
        tenant_id: intent.principal.tenant_id.clone(),
        principal_id: intent.principal.principal_id.clone(),
        principal_kind: intent.principal.principal_kind,
        receipt_id: new_id(ResourceKind::Receipt)?,
        event_id: new_id(ResourceKind::Event)?,
        outbox_id: new_id(ResourceKind::OutboxEvent)?,
        idempotency_key_digest: intent.idempotency_key_digest.clone(),
        request_digest: intent.idempotency_key_digest.clone(),
        receipt_expires_at: chrono::Utc::now() + chrono::Duration::hours(24),
    })
}
fn current<T>(outcome: CommandOutcome<T>) -> T {
    match outcome {
        CommandOutcome::Applied(value) | CommandOutcome::Replayed(value) => value,
    }
}
#[async_trait]
impl RecoveryApplication for PgRecovery {
    async fn execute(
        &self,
        intent: RecoveryIntent,
    ) -> Result<RecoveryResultV1, RunApplicationError> {
        intent
            .principal
            .validate()
            .map_err(|_| RunApplicationError::Unauthenticated)?;
        intent.request.validate()?;
        if intent.deadline <= chrono::Utc::now() {
            return Err(RunApplicationError::Unavailable);
        }
        let audit = audit(&intent)?;
        match intent.request {
            RecoveryRequest::Place(request) => {
                let mut command = PlaceRunHistoryHold {
                    audit,
                    run_id: request.run_id,
                    expected_run_version: request.expected_run_version,
                    reason_evidence_digest: request.reason_evidence_digest,
                };
                command.audit.request_digest = command
                    .request_digest()
                    .map_err(|_| RunApplicationError::Invalid)?;
                let hold_key = command.audit.request_digest.clone();
                let outcome = self
                    .0
                    .place_run_history_hold(command)
                    .await
                    .map_err(map_run_repository_error)?;
                Ok(RecoveryResultV1::HistoryHold {
                    schema_version: 1,
                    hold_key,
                    current: current(outcome).into(),
                })
            }
            RecoveryRequest::Release(request) => {
                let mut command = ReleaseRunHistoryHold {
                    audit,
                    run_id: request.run_id,
                    expected_run_version: request.expected_run_version,
                    hold_key: request.hold_key.clone(),
                    release_evidence_digest: request.release_evidence_digest,
                };
                command.audit.request_digest = command
                    .request_digest()
                    .map_err(|_| RunApplicationError::Invalid)?;
                let outcome = self
                    .0
                    .release_run_history_hold(command)
                    .await
                    .map_err(map_run_repository_error)?;
                Ok(RecoveryResultV1::HistoryHold {
                    schema_version: 1,
                    hold_key: request.hold_key,
                    current: current(outcome).into(),
                })
            }
            RecoveryRequest::Recover(request) => {
                let task_id = request.task_id.clone();
                let mut command = RecoverMcpOAuthPkceCleanup {
                    audit,
                    task_id: request.task_id,
                    expected_task_generation: request.expected_task_generation,
                    expected_task_version: request.expected_task_version,
                    previous_job_id: request.previous_job_id,
                    new_job_id: new_id(ResourceKind::Job)?,
                    attempt_limit: request.attempt_limit,
                    recovery_evidence_digest: request.recovery_evidence_digest,
                };
                command.audit.request_digest = command
                    .request_digest()
                    .map_err(|_| RunApplicationError::Invalid)?;
                let outcome = self
                    .0
                    .recover_mcp_oauth_pkce_cleanup(command)
                    .await
                    .map_err(map_run_repository_error)?;
                let cleanup_job_id = current(outcome)
                    .job_id
                    .parse()
                    .map_err(|_| RunApplicationError::Internal)?;
                Ok(RecoveryResultV1::PkceCleanup {
                    schema_version: 1,
                    task_id,
                    cleanup_job_id,
                })
            }
        }
    }
}
