//! Completed model effects may settle after Run control has closed new business.
use super::*;

pub(super) fn model_run_convergence_goal(
    run: &RunRecord,
    database_now: DateTime<Utc>,
) -> Result<Option<insight_platform_orchestrator::RunConvergenceGoal>, RepositoryError> {
    insight_platform_orchestrator::decide_run_convergence(
        run.state.parse().map_err(
            |error: insight_platform_contracts::state::StateParseError| {
                RepositoryError::CorruptRow(error.to_string())
            },
        )?,
        u64::try_from(run.version)
            .map_err(|_| RepositoryError::CorruptRow("negative Run version".into()))?,
        run.deadline,
        &run.current,
        false,
        database_now,
    )
    .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn settle_suppressed_model_continuation(
    transaction: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    goal: &insight_platform_orchestrator::RunConvergenceGoal,
    release_active_permit: bool,
    event_id: &ResourceId,
    outbox_id: &ResourceId,
    owner_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let settled = super::convergence_commands::update_run_step(
        transaction,
        run,
        goal,
        i32::from(release_active_permit),
        false,
        database_now,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &settled.tenant_id,
        event_id,
        outbox_id,
        "run",
        &settled.run_id,
        settled.version,
        Some(&settled.run_id),
        "run.model_continuation_suppressed",
        &TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "owner_id": owner_id,
                "reason": goal.reason.as_str(),
                "active_permit_released": release_active_permit,
            }),
            65_536,
        )?,
    )
    .await
}
