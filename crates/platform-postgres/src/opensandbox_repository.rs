use crate::{
    capability_execution_repository::{
        insert_capability_value_and_reference, update_capability_invocation,
    },
    invocation_repository::{
        load_capability_execution_input, load_capability_invocation,
        load_exact_capability_interface_spec, validate_capability_value_against_schema,
    },
    repository::{
        append_scheduler_event, job_from_row, job_projection, JobRecord, PgRepository,
        RepositoryError, TypedPayload, MAX_JOB_LEASE_MILLISECONDS,
    },
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, Failure, FailureClass, FailureCode, FailureSource, InvocationState, JobState,
    PlatformFailureCode, QuotaDimension, ResourceId, ResourceKind, Retryability, Sha256Digest,
    ValueRef,
};
use insight_platform_invocations::{
    decide_detached_job_control, decide_detached_job_outcome, CapabilityControlKind,
    CapabilityExecutionInputMaterial, CapabilityInvocationRecord, CapabilityOutputValue,
    CapabilityUncertainty, DetachedCapabilityJobOutcome, DetachedSandboxSourceKind,
    SafeBackendFailure,
};
use insight_platform_jobs::{
    decide_claim, decide_claim_continuation, decide_expired_continuation, decide_expired_lease,
    decide_heartbeat, decide_observation_update, decide_owner_cancelling, decide_owner_terminal,
    decide_reconciliation, decide_resume, decide_start, decide_terminal,
    decide_terminal_physical_update, JobFence, JobProjection, LeasePolicy,
};
use insight_platform_sandbox::opensandbox::{
    AuthorizeCandidateCreateV1, AuthorizeSandboxActivationV1, CandidateCreateAuthorizationV1,
    ClaimedSandboxCleanupV1, CommitSandboxTerminalV1, HeartbeatSandboxJobV1, LeasedSandboxJobV1,
    PhysicalDecision, ReconcileSandboxControlsV1, RecordProvisioningIntentV1,
    RecordSandboxCleanupObservationV1, RecordSandboxObservationV1, SandboxCandidatePurposeV1,
    SandboxCandidateV1, SandboxClaimV1, SandboxCleanupClaimV1, SandboxCleanupObservationV1,
    SandboxContractError as OpenSandboxContractError, SandboxControlKindV1,
    SandboxDispatcherJobPayloadV1, SandboxExecutionRequestV1, SandboxFailureClassV1,
    SandboxFencedIdentityV1, SandboxJobRepository, SandboxOrphanDecisionV1,
    SandboxOrphanDispositionV1, SandboxPhysicalPhaseV1, SandboxRepositoryDecisionV1,
    SandboxRunnerOutcomeV1, SandboxTerminalOutcomeV1, SelectSandboxCandidateV1,
    MAX_SANDBOX_JOB_PAYLOAD_BYTES,
};
use serde_json::Value;
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};
use std::collections::BTreeSet;
use uuid::Uuid;

const SANDBOX_JOB_KIND: &str = "sandbox_capability_execution";
const SANDBOX_QUOTA_LINE_COUNT: usize = 4;

impl From<OpenSandboxContractError> for RepositoryError {
    fn from(failure: OpenSandboxContractError) -> Self {
        match failure {
            OpenSandboxContractError::CandidateSelectionConflict
            | OpenSandboxContractError::CandidateCreateConflict
            | OpenSandboxContractError::TerminalConflict
            | OpenSandboxContractError::RunnerBootChanged => {
                Self::Conflict("OpenSandbox first-winner")
            }
            _ => Self::InvalidInput(failure.to_string()),
        }
    }
}

#[derive(Debug)]
struct LockedOpenSandboxJob {
    record: JobRecord,
    job: JobProjection,
    payload: SandboxDispatcherJobPayloadV1,
}

#[derive(Debug)]
struct OpenSandboxQuotaLine {
    tenant_id: String,
    quota_account_id: String,
    metric: String,
    reserved_amount: i64,
    account_version: i64,
}

#[async_trait]
impl SandboxJobRepository for PgRepository {
    type Error = RepositoryError;

    async fn claim(&self, request: SandboxClaimV1) -> Result<Vec<LeasedSandboxJobV1>, Self::Error> {
        request.validate(
            self.recovery_batch_limit(),
            u64::try_from(MAX_JOB_LEASE_MILLISECONDS).expect("positive lease maximum"),
        )?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM insight_platform.jobs
            WHERE job_kind = $1 AND work_class = 'sandbox' AND owner_kind = 'job'
              AND payload ? 'plan' AND terminal_at IS NULL AND deadline > $2
              AND (
                (state = 'ready' AND scheduled_at <= $2)
                OR (state = 'leased' AND lease_expires_at <= $2)
                OR (state = 'running' AND lease_expires_at <= $2 AND payload ? 'physical')
              )
            ORDER BY priority DESC, scheduled_at, created_at, job_id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(SANDBOX_JOB_KIND)
        .bind(database_now)
        .bind(i64::from(request.limit))
        .fetch_all(&mut *transaction)
        .await?;

        let mut claimed = Vec::with_capacity(rows.len());
        for (row, lease_token_digest) in rows
            .into_iter()
            .zip(request.lease_token_digests.iter().cloned())
        {
            let current = locked_from_row(row)?;
            let claimable = prepare_claimable_job(&current, database_now)?;
            let continuing = current.payload.physical.is_some();
            let next = if continuing {
                let leased = decide_claim_continuation(
                    &claimable,
                    database_now,
                    request.worker_process_generation_id.clone(),
                    lease_token_digest.clone(),
                    lease_policy(request.lease_milliseconds),
                )?;
                let fence = fence_for(&leased)?;
                decide_resume(&leased, &fence, database_now)?
            } else {
                decide_claim(
                    &claimable,
                    database_now,
                    request.worker_process_generation_id.clone(),
                    lease_token_digest.clone(),
                    lease_policy(request.lease_milliseconds),
                )?
            };
            let input = load_request_input(&mut transaction, &current.payload, false).await?;
            let physical_attempt = current
                .payload
                .physical
                .as_deref()
                .map(|physical| physical.provisioning_token.physical_attempt)
                .unwrap_or_else(|| next.attempt_count.saturating_add(1));
            let hydrated = current.payload.plan.bind_request(
                next.lease_generation,
                physical_attempt,
                request.worker_process_generation_id.clone(),
                next.trace,
                input,
            )?;
            let updated = update_job(
                &mut transaction,
                &current.record,
                &next,
                &current.payload,
                database_now,
                current.record.result_digest.as_deref(),
                current.record.quota_reservation_id.as_deref(),
            )
            .await?;
            let next_job = job_projection(&updated)?;
            let fence = fence_for(&next_job)?;
            let usage_reservation_id = parse_expected_id(
                updated.quota_reservation_id.as_deref().ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "OpenSandbox Job has no usage reservation".to_owned(),
                    )
                })?,
                ResourceKind::UsageReservation,
                "OpenSandbox usage reservation",
            )?;
            let leased = LeasedSandboxJobV1 {
                job: next_job,
                payload: current.payload,
                request: hydrated,
                fence,
                usage_reservation_id,
            };
            leased.validate()?;
            append_job_event(
                &mut transaction,
                &updated,
                "job.claimed",
                serde_json::json!({
                    "job_id": updated.job_id,
                    "lease_generation": updated.lease_epoch,
                    "worker_process_generation_id": updated.worker_id,
                }),
            )
            .await?;
            claimed.push(leased);
        }
        transaction.commit().await?;
        Ok(claimed)
    }

    async fn reconcile_controls(
        &self,
        request: ReconcileSandboxControlsV1,
    ) -> Result<Vec<SandboxRepositoryDecisionV1>, Self::Error> {
        request.validate(self.recovery_batch_limit())?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let candidates = sqlx::query(
            r#"
            SELECT tenant_id, job_id
            FROM insight_platform.jobs
            WHERE job_kind = $1 AND work_class = 'sandbox' AND owner_kind = 'job'
              AND payload ? 'plan' AND terminal_at IS NULL
              AND (
                (state = 'cancelling' AND payload ? 'control')
                OR (
                  deadline <= $2
                  AND state IN ('ready', 'leased', 'running', 'waiting', 'retry_scheduled')
                  AND NOT (payload ? 'control')
                )
              )
            ORDER BY deadline, updated_at, job_id
            LIMIT $3
            "#,
        )
        .bind(SANDBOX_JOB_KIND)
        .bind(database_now)
        .bind(i64::from(request.limit))
        .fetch_all(&mut *transaction)
        .await?;

        let mut reconciled = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let tenant_id = parse_expected_id(
                &candidate.try_get::<String, _>("tenant_id")?,
                ResourceKind::Tenant,
                "OpenSandbox control tenant",
            )?;
            let job_id = parse_expected_id(
                &candidate.try_get::<String, _>("job_id")?,
                ResourceKind::Job,
                "OpenSandbox control Job",
            )?;
            let snapshot = load_job(&mut transaction, &tenant_id, &job_id, false).await?;
            let reservation_id = snapshot
                .record
                .quota_reservation_id
                .as_deref()
                .ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "OpenSandbox controlled Job has no quota reservation".to_owned(),
                    )
                })?
                .to_owned();

            // A control terminal mutates every authority in the same order as normal terminal
            // commit. The candidate read above holds no row lock.
            let quota = lock_quota_bundle(
                &mut transaction,
                &snapshot.record.tenant_id,
                &reservation_id,
                &snapshot.payload,
            )
            .await?;
            let invocation = load_capability_invocation(
                &mut transaction,
                &snapshot.payload.plan.tenant_id,
                &snapshot.payload.plan.invocation_id,
                true,
            )
            .await?;
            let current = load_job(&mut transaction, &tenant_id, &job_id, true).await?;
            if current.record.version != snapshot.record.version
                || current.record.payload.digest != snapshot.record.payload.digest
                || current.record.quota_reservation_id.as_deref() != Some(reservation_id.as_str())
                || current.payload.plan.invocation_id != invocation.invocation_id
            {
                return Err(RepositoryError::Conflict(
                    "OpenSandbox control authority snapshot",
                ));
            }

            let (controlled_invocation, cancelling_job, controlled_payload) =
                if let Some(control) = current.payload.control.as_deref() {
                    if current.job.state != JobState::Cancelling
                        || invocation.state != InvocationState::Cancelling
                        || control.target_invocation_version != invocation.version
                    {
                        return Err(RepositoryError::Conflict(
                            "OpenSandbox explicit control binding",
                        ));
                    }
                    (
                        invocation.clone(),
                        current.job.clone(),
                        current.payload.clone(),
                    )
                } else {
                    if database_now < current.job.deadline {
                        return Err(RepositoryError::Conflict("OpenSandbox timeout is not due"));
                    }
                    let controlled_invocation = decide_detached_job_control(
                        &invocation,
                        DetachedSandboxSourceKind::SandboxCapability,
                        &current.job,
                        CapabilityControlKind::Timeout,
                        database_now,
                    )?;
                    let cancelling_job = decide_owner_cancelling(&current.job)?;
                    let controlled_payload = current.payload.request_control(
                        SandboxControlKindV1::Timeout,
                        database_now,
                        controlled_invocation.version,
                    )?;
                    (controlled_invocation, cancelling_job, controlled_payload)
                };

            controlled_payload.validate_for(&cancelling_job)?;
            let control = controlled_payload.control.as_deref().ok_or_else(|| {
                RepositoryError::CorruptRow(
                    "OpenSandbox cancelling Job has no control intent".to_owned(),
                )
            })?;
            let terminal_state = control.kind.job_state();
            let terminal_job = decide_owner_terminal(&cancelling_job, terminal_state)?;
            let terminal_payload = controlled_payload.terminal_control()?;
            terminal_payload.validate_for(&terminal_job)?;
            let detached_outcome = match control.kind {
                SandboxControlKindV1::Cancel => DetachedCapabilityJobOutcome::Cancelled,
                SandboxControlKindV1::Timeout => DetachedCapabilityJobOutcome::TimedOut,
            };
            let invocation_decision = decide_detached_job_outcome(
                &controlled_invocation,
                DetachedSandboxSourceKind::SandboxCapability,
                &terminal_job,
                terminal_job.attempt_count,
                &detached_outcome,
                database_now,
                self.invocation_limits(),
            )?;
            let terminal_outcome = match control.kind {
                SandboxControlKindV1::Cancel => SandboxTerminalOutcomeV1::Cancelled {
                    evidence_digest: control.intent_digest.clone(),
                },
                SandboxControlKindV1::Timeout => SandboxTerminalOutcomeV1::TimedOut {
                    evidence_digest: control.intent_digest.clone(),
                },
            };
            settle_quota_bundle(
                &mut transaction,
                &reservation_id,
                &quota,
                &terminal_outcome,
                &current.payload.plan.request_digest,
            )
            .await?;
            let result_digest = terminal_payload
                .terminal
                .as_deref()
                .map(|terminal| terminal.terminal_digest.to_string());
            let updated = update_job(
                &mut transaction,
                &current.record,
                &terminal_job,
                &terminal_payload,
                database_now,
                result_digest.as_deref(),
                None,
            )
            .await?;
            update_capability_invocation(
                &mut transaction,
                &invocation,
                &invocation_decision.invocation,
            )
            .await?;
            append_job_event(
                &mut transaction,
                &updated,
                "sandbox.job.controlled",
                serde_json::json!({
                    "control_intent_digest": control.intent_digest,
                    "control_kind": control.kind,
                    "job_id": updated.job_id,
                    "physical_attempt": terminal_job.attempt_count,
                    "terminal_digest": result_digest,
                    "terminal_state": terminal_job.state,
                }),
            )
            .await?;
            reconciled.push(decision_from(updated, terminal_payload)?);
        }
        transaction.commit().await?;
        Ok(reconciled)
    }

    async fn heartbeat(
        &self,
        command: HeartbeatSandboxJobV1,
    ) -> Result<SandboxRepositoryDecisionV1, Self::Error> {
        command.identity.validate()?;
        if command.lease_milliseconds == 0
            || command.lease_milliseconds
                > u64::try_from(MAX_JOB_LEASE_MILLISECONDS).expect("positive lease maximum")
        {
            return Err(RepositoryError::InvalidInput(
                "OpenSandbox heartbeat lease is outside the closed bound".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let current = load_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.job_id,
            true,
        )
        .await?;
        require_fenced_identity(&current, &command.identity)?;
        let next = decide_heartbeat(
            &current.job,
            &command.identity.fence,
            database_now,
            lease_policy(command.lease_milliseconds),
        )?;
        let updated = update_job(
            &mut transaction,
            &current.record,
            &next,
            &current.payload,
            database_now,
            current.record.result_digest.as_deref(),
            current.record.quota_reservation_id.as_deref(),
        )
        .await?;
        transaction.commit().await?;
        decision_from(updated, current.payload)
    }

    async fn record_provisioning_intent(
        &self,
        command: RecordProvisioningIntentV1,
    ) -> Result<SandboxRepositoryDecisionV1, Self::Error> {
        command.identity.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let current = load_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.job_id,
            true,
        )
        .await?;
        require_same_lease_identity(&current, &command.identity, database_now)?;
        let activation_digest = command.activation_token.digest()?;
        if current.payload.physical.as_deref().is_some_and(|physical| {
            physical.activation_token == command.activation_token
                && physical.activation_token_digest == activation_digest
        }) {
            transaction.commit().await?;
            return decision_from(current.record, current.payload);
        }
        require_fenced_identity(&current, &command.identity)?;
        let request = hydrate_current_request(&mut transaction, &current, false).await?;
        let next_job = decide_start(&current.job, &command.identity.fence, database_now)?;
        let next_payload =
            current
                .payload
                .begin(&request, command.activation_token, database_now)?;
        next_payload.validate_for(&next_job)?;
        let updated = update_job(
            &mut transaction,
            &current.record,
            &next_job,
            &next_payload,
            database_now,
            None,
            current.record.quota_reservation_id.as_deref(),
        )
        .await?;
        append_job_event(
            &mut transaction,
            &updated,
            "sandbox.job.phase_changed",
            physical_event_payload(&next_payload),
        )
        .await?;
        transaction.commit().await?;
        decision_from(updated, next_payload)
    }

    async fn authorize_candidate_create(
        &self,
        command: AuthorizeCandidateCreateV1,
    ) -> Result<PhysicalDecision<CandidateCreateAuthorizationV1>, Self::Error> {
        command.identity.validate()?;
        command.limits.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let current = load_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.job_id,
            true,
        )
        .await?;
        require_same_lease_identity(&current, &command.identity, database_now)?;
        let request = hydrate_current_request(&mut transaction, &current, false).await?;
        let decision = match current.payload.authorize_candidate_create(
            &request,
            &command.limits,
            database_now,
            command.create_ordinal,
        )? {
            PhysicalDecision::Replayed(payload) => {
                transaction.commit().await?;
                PhysicalDecision::Replayed(CandidateCreateAuthorizationV1 {
                    decision: decision_from(current.record, payload)?,
                    create_ordinal: command.create_ordinal,
                })
            }
            PhysicalDecision::Applied(payload) => {
                require_fenced_identity(&current, &command.identity)?;
                let next_job =
                    decide_observation_update(&current.job, &command.identity.fence, database_now)?;
                let updated = update_job(
                    &mut transaction,
                    &current.record,
                    &next_job,
                    &payload,
                    database_now,
                    current.record.result_digest.as_deref(),
                    current.record.quota_reservation_id.as_deref(),
                )
                .await?;
                append_job_event(
                    &mut transaction,
                    &updated,
                    "sandbox.candidate_create.authorized",
                    serde_json::json!({
                        "job_id": updated.job_id,
                        "physical_attempt": request.physical_attempt,
                        "create_ordinal": command.create_ordinal,
                    }),
                )
                .await?;
                let authorization = CandidateCreateAuthorizationV1 {
                    decision: decision_from(updated, payload)?,
                    create_ordinal: command.create_ordinal,
                };
                authorization.validate()?;
                transaction.commit().await?;
                PhysicalDecision::Applied(authorization)
            }
        };
        match &decision {
            PhysicalDecision::Applied(authorization)
            | PhysicalDecision::Replayed(authorization) => authorization.validate()?,
        }
        Ok(decision)
    }

    async fn select_candidate(
        &self,
        command: SelectSandboxCandidateV1,
    ) -> Result<PhysicalDecision<SandboxRepositoryDecisionV1>, Self::Error> {
        command.identity.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let current = load_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.job_id,
            true,
        )
        .await?;
        require_same_lease_identity(&current, &command.identity, database_now)?;
        let request = hydrate_current_request(&mut transaction, &current, false).await?;
        match current
            .payload
            .select_candidate(&request, &command.candidate)?
        {
            PhysicalDecision::Replayed(payload) => {
                transaction.commit().await?;
                Ok(PhysicalDecision::Replayed(decision_from(
                    current.record,
                    payload,
                )?))
            }
            PhysicalDecision::Applied(payload) => {
                require_fenced_identity(&current, &command.identity)?;
                let next_job =
                    decide_observation_update(&current.job, &command.identity.fence, database_now)?;
                let updated = update_job(
                    &mut transaction,
                    &current.record,
                    &next_job,
                    &payload,
                    database_now,
                    current.record.result_digest.as_deref(),
                    current.record.quota_reservation_id.as_deref(),
                )
                .await?;
                append_job_event(
                    &mut transaction,
                    &updated,
                    "sandbox.job.phase_changed",
                    physical_event_payload(&payload),
                )
                .await?;
                transaction.commit().await?;
                Ok(PhysicalDecision::Applied(decision_from(updated, payload)?))
            }
        }
    }

    async fn authorize_activation(
        &self,
        command: AuthorizeSandboxActivationV1,
    ) -> Result<PhysicalDecision<SandboxRepositoryDecisionV1>, Self::Error> {
        command.identity.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let current = load_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.job_id,
            true,
        )
        .await?;
        require_same_lease_identity(&current, &command.identity, database_now)?;
        match current
            .payload
            .authorize_activation(&command.sandbox_id, command.boot_id)?
        {
            PhysicalDecision::Replayed(payload) => {
                transaction.commit().await?;
                Ok(PhysicalDecision::Replayed(decision_from(
                    current.record,
                    payload,
                )?))
            }
            PhysicalDecision::Applied(payload) => {
                require_fenced_identity(&current, &command.identity)?;
                let next_job =
                    decide_observation_update(&current.job, &command.identity.fence, database_now)?;
                let updated = update_job(
                    &mut transaction,
                    &current.record,
                    &next_job,
                    &payload,
                    database_now,
                    current.record.result_digest.as_deref(),
                    current.record.quota_reservation_id.as_deref(),
                )
                .await?;
                append_job_event(
                    &mut transaction,
                    &updated,
                    "sandbox.job.phase_changed",
                    physical_event_payload(&payload),
                )
                .await?;
                transaction.commit().await?;
                Ok(PhysicalDecision::Applied(decision_from(updated, payload)?))
            }
        }
    }

    async fn record_physical_observation(
        &self,
        command: RecordSandboxObservationV1,
    ) -> Result<SandboxRepositoryDecisionV1, Self::Error> {
        command.identity.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let current = load_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.job_id,
            true,
        )
        .await?;
        require_same_lease_identity(&current, &command.identity, database_now)?;
        let request = hydrate_current_request(&mut transaction, &current, false).await?;
        let payload = current
            .payload
            .record_observation(&request, command.observation)?;
        if payload == current.payload {
            transaction.commit().await?;
            return decision_from(current.record, payload);
        }
        require_fenced_identity(&current, &command.identity)?;
        let next_job =
            decide_observation_update(&current.job, &command.identity.fence, database_now)?;
        let updated = update_job(
            &mut transaction,
            &current.record,
            &next_job,
            &payload,
            database_now,
            current.record.result_digest.as_deref(),
            current.record.quota_reservation_id.as_deref(),
        )
        .await?;
        append_job_event(
            &mut transaction,
            &updated,
            "sandbox.job.phase_changed",
            physical_event_payload(&payload),
        )
        .await?;
        transaction.commit().await?;
        decision_from(updated, payload)
    }

    async fn commit_terminal(
        &self,
        command: CommitSandboxTerminalV1,
    ) -> Result<PhysicalDecision<SandboxRepositoryDecisionV1>, Self::Error> {
        command.identity.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;

        // Read only enough immutable identity to acquire the global mutable-authority lock order:
        // quota -> Invocation -> Job. Every field is revalidated after the Job row is locked.
        let snapshot = load_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.job_id,
            false,
        )
        .await?;
        if snapshot.payload.terminal.is_some() {
            let invocation = load_capability_invocation(
                &mut transaction,
                &snapshot.payload.plan.tenant_id,
                &snapshot.payload.plan.invocation_id,
                false,
            )
            .await?;
            let input = load_request_input_for_invocation(
                &mut transaction,
                &snapshot.payload,
                &invocation,
                true,
            )
            .await?;
            let physical_attempt = snapshot
                .payload
                .physical
                .as_deref()
                .ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "terminal OpenSandbox Job has no physical evidence".to_owned(),
                    )
                })?
                .provisioning_token
                .physical_attempt;
            let replay_request = snapshot.payload.plan.bind_request(
                command.identity.fence.lease_generation,
                physical_attempt,
                command.identity.fence.worker_process_generation_id.clone(),
                snapshot.job.trace,
                input,
            )?;
            if snapshot
                .payload
                .terminal_replays(&replay_request, &command.outcome)?
            {
                transaction.commit().await?;
                return Ok(PhysicalDecision::Replayed(decision_from(
                    snapshot.record,
                    snapshot.payload,
                )?));
            }
            return Err(RepositoryError::Conflict(
                "OpenSandbox terminal first-winner",
            ));
        }
        let reservation_id = snapshot
            .record
            .quota_reservation_id
            .as_deref()
            .ok_or_else(|| {
                RepositoryError::CorruptRow(
                    "OpenSandbox terminal Job has no quota reservation".to_owned(),
                )
            })?
            .to_owned();
        let quota = lock_quota_bundle(
            &mut transaction,
            &snapshot.record.tenant_id,
            &reservation_id,
            &snapshot.payload,
        )
        .await?;
        let invocation = load_capability_invocation(
            &mut transaction,
            &snapshot.payload.plan.tenant_id,
            &snapshot.payload.plan.invocation_id,
            true,
        )
        .await?;
        let current = load_job(
            &mut transaction,
            &command.identity.tenant_id,
            &command.identity.job_id,
            true,
        )
        .await?;
        require_fenced_identity(&current, &command.identity)?;
        if current.record.quota_reservation_id.as_deref() != Some(reservation_id.as_str())
            || current.payload.plan.invocation_id != invocation.invocation_id
        {
            return Err(RepositoryError::Conflict(
                "OpenSandbox terminal authority snapshot",
            ));
        }
        let request =
            hydrate_request_for_invocation(&mut transaction, &current, &invocation).await?;
        let payload = current
            .payload
            .terminal(&request, command.outcome.clone())?;
        let next_job = if command.outcome.job_state() == JobState::ReconciliationRequired {
            decide_reconciliation(&current.job, &command.identity.fence, database_now)?
        } else {
            decide_terminal(
                &current.job,
                &command.identity.fence,
                database_now,
                command.outcome.job_state(),
            )?
        };
        payload.validate_for(&next_job)?;

        let detached = normalize_terminal_outcome(
            &mut transaction,
            &current,
            &invocation,
            &request,
            &command.outcome,
        )
        .await?;
        let invocation_decision = decide_detached_job_outcome(
            &invocation,
            DetachedSandboxSourceKind::SandboxCapability,
            &next_job,
            next_job.attempt_count,
            &detached,
            database_now,
            self.invocation_limits(),
        )?;
        if let Some(output) = invocation_decision.output.as_ref() {
            insert_capability_value_and_reference(
                &mut transaction,
                &invocation_decision.invocation,
                output,
                database_now,
            )
            .await?;
        }
        settle_quota_bundle(
            &mut transaction,
            &reservation_id,
            &quota,
            &command.outcome,
            &current.payload.plan.request_digest,
        )
        .await?;
        let result_digest = payload
            .terminal
            .as_deref()
            .map(|terminal| terminal.terminal_digest.to_string());
        let updated = update_job(
            &mut transaction,
            &current.record,
            &next_job,
            &payload,
            database_now,
            result_digest.as_deref(),
            None,
        )
        .await?;
        update_capability_invocation(
            &mut transaction,
            &invocation,
            &invocation_decision.invocation,
        )
        .await?;
        let event_type = if next_job.state == JobState::Succeeded {
            "sandbox.job.completed"
        } else {
            "sandbox.job.failed"
        };
        append_job_event(
            &mut transaction,
            &updated,
            event_type,
            serde_json::json!({
                "job_id": updated.job_id,
                "physical_attempt": next_job.attempt_count,
                "terminal_digest": result_digest,
                "terminal_state": next_job.state,
            }),
        )
        .await?;
        transaction.commit().await?;
        Ok(PhysicalDecision::Applied(decision_from(updated, payload)?))
    }

    async fn claim_cleanup(
        &self,
        request: SandboxCleanupClaimV1,
    ) -> Result<Vec<ClaimedSandboxCleanupV1>, Self::Error> {
        request.validate(
            self.recovery_batch_limit(),
            u64::try_from(MAX_JOB_LEASE_MILLISECONDS).expect("positive lease maximum"),
        )?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM insight_platform.jobs
            WHERE job_kind = $1 AND work_class = 'sandbox' AND owner_kind = 'job'
              AND payload ? 'plan' AND payload ? 'terminal'
              AND (payload #>> '{cleanup,required}')::boolean = true
              AND (
                payload #>> '{cleanup,owner_process_generation_id}' = $4
                OR payload #>> '{cleanup,owner_process_generation_id}' IS NULL
                OR (payload #>> '{cleanup,expires_at}')::timestamptz <= $2
              )
            ORDER BY
              CASE
                WHEN payload #>> '{cleanup,owner_process_generation_id}' = $4
                  AND (payload #>> '{cleanup,expires_at}')::timestamptz > $2
                THEN 0
                ELSE 1
              END,
              updated_at,
              job_id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(SANDBOX_JOB_KIND)
        .bind(database_now)
        .bind(i64::from(request.limit))
        .bind(request.process_generation_id.to_string())
        .fetch_all(&mut *transaction)
        .await?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let current = locked_from_row(row)?;
            if current.payload.cleanup.owner_process_generation_id.as_ref()
                == Some(&request.process_generation_id)
                && current
                    .payload
                    .cleanup
                    .expires_at
                    .is_some_and(|expires_at| expires_at > database_now)
            {
                let physical_attempt = current
                    .payload
                    .physical
                    .as_deref()
                    .ok_or_else(|| {
                        RepositoryError::CorruptRow(
                            "claimed cleanup has no physical evidence".to_owned(),
                        )
                    })?
                    .provisioning_token
                    .physical_attempt;
                let claim = ClaimedSandboxCleanupV1 {
                    fence: insight_platform_sandbox::opensandbox::SandboxCleanupFenceV1 {
                        schema_version: 1,
                        tenant_id: current.job.tenant_id.clone(),
                        job_id: current.job.job_id.clone(),
                        expected_job_version: current.job.version,
                        physical_attempt,
                        cleanup_generation: current.payload.cleanup.generation,
                        process_generation_id: request.process_generation_id.clone(),
                        expires_at: current.payload.cleanup.expires_at.ok_or_else(|| {
                            RepositoryError::CorruptRow("claimed cleanup has no expiry".to_owned())
                        })?,
                    },
                    job: current.job,
                    payload: current.payload,
                };
                claim.validate()?;
                claims.push(claim);
                continue;
            }
            let next_job = decide_terminal_physical_update(&current.job, current.job.version)?;
            let (payload, fence) = current.payload.claim_cleanup(
                &current.job,
                request.process_generation_id.clone(),
                database_now,
                request.lease_milliseconds,
                next_job.version,
            )?;
            let updated = update_job(
                &mut transaction,
                &current.record,
                &next_job,
                &payload,
                database_now,
                current.record.result_digest.as_deref(),
                None,
            )
            .await?;
            let claim = ClaimedSandboxCleanupV1 {
                job: job_projection(&updated)?,
                payload,
                fence,
            };
            claim.validate()?;
            claims.push(claim);
        }
        transaction.commit().await?;
        Ok(claims)
    }

    async fn record_cleanup_observation(
        &self,
        command: RecordSandboxCleanupObservationV1,
    ) -> Result<ClaimedSandboxCleanupV1, Self::Error> {
        command.fence.validate()?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let current = load_job(
            &mut transaction,
            &command.fence.tenant_id,
            &command.fence.job_id,
            true,
        )
        .await?;
        if !current.payload.cleanup.required
            && cleanup_observation_replays(&current.payload, &command.observation)
            && current.job.version == command.fence.expected_job_version.saturating_add(1)
        {
            let result = ClaimedSandboxCleanupV1 {
                fence: insight_platform_sandbox::opensandbox::SandboxCleanupFenceV1 {
                    expected_job_version: current.job.version,
                    ..command.fence
                },
                job: current.job,
                payload: current.payload,
            };
            result.validate()?;
            transaction.commit().await?;
            return Ok(result);
        }
        let next_job =
            decide_terminal_physical_update(&current.job, command.fence.expected_job_version)?;
        let (payload, next_fence) = current.payload.record_cleanup_observation(
            &current.job,
            &command.fence,
            database_now,
            command.observation,
            next_job.version,
        )?;
        let updated = update_job(
            &mut transaction,
            &current.record,
            &next_job,
            &payload,
            database_now,
            current.record.result_digest.as_deref(),
            None,
        )
        .await?;
        let fence = next_fence.unwrap_or({
            insight_platform_sandbox::opensandbox::SandboxCleanupFenceV1 {
                expected_job_version: next_job.version,
                ..command.fence
            }
        });
        let result = ClaimedSandboxCleanupV1 {
            job: job_projection(&updated)?,
            payload,
            fence,
        };
        result.validate()?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn decide_orphan(
        &self,
        candidate: SandboxCandidateV1,
    ) -> Result<SandboxOrphanDecisionV1, Self::Error> {
        candidate.validate_shape()?;
        if candidate.metadata.purpose != SandboxCandidatePurposeV1::Job {
            return Err(RepositoryError::InvalidInput(
                "readiness candidates are not business orphans".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let row =
            sqlx::query("SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2")
                .bind(candidate.metadata.tenant_id.to_string())
                .bind(candidate.metadata.job_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(SandboxOrphanDecisionV1::new(
                candidate,
                SandboxOrphanDispositionV1::DeleteMissingOwner,
            )?);
        };
        let current = locked_from_row(row)?;
        let Some(physical) = current.payload.physical.as_deref() else {
            transaction.commit().await?;
            return Ok(SandboxOrphanDecisionV1::new(
                candidate,
                SandboxOrphanDispositionV1::DeleteStaleAttempt,
            )?);
        };
        if candidate.metadata.physical_attempt != physical.provisioning_token.physical_attempt {
            transaction.commit().await?;
            return Ok(SandboxOrphanDecisionV1::new(
                candidate,
                SandboxOrphanDispositionV1::DeleteStaleAttempt,
            )?);
        }
        candidate
            .metadata
            .validate_for_plan(&current.payload.plan, &physical.provisioning_token)?;
        if candidate.metadata.create_ordinal > physical.create_authorization_count {
            return Err(RepositoryError::InvalidInput(
                "OpenSandbox candidate create ordinal was not authorized".to_owned(),
            ));
        }
        let disposition = if physical.phase == SandboxPhysicalPhaseV1::Provisioning {
            SandboxOrphanDispositionV1::RetainProvisioning
        } else if physical.selected_sandbox_id.as_ref() == Some(&candidate.sandbox_id) {
            SandboxOrphanDispositionV1::RetainSelected
        } else if physical.candidate_ids.contains(&candidate.sandbox_id) {
            SandboxOrphanDispositionV1::DeleteUnselected
        } else {
            SandboxOrphanDispositionV1::DeleteLateCandidate
        };
        transaction.commit().await?;
        Ok(SandboxOrphanDecisionV1::new(candidate, disposition)?)
    }

    async fn recover(
        &self,
        tenant_id: &ResourceId,
        job_id: &ResourceId,
    ) -> Result<SandboxRepositoryDecisionV1, Self::Error> {
        let mut transaction = self.pool().begin().await?;
        let current = load_job(&mut transaction, tenant_id, job_id, false).await?;
        let decision = SandboxRepositoryDecisionV1 {
            fence: current
                .job
                .lease
                .as_ref()
                .map(|_| fence_for(&current.job))
                .transpose()?,
            job: current.job,
            payload: current.payload,
        };
        decision.validate()?;
        transaction.commit().await?;
        Ok(decision)
    }
}

fn prepare_claimable_job(
    current: &LockedOpenSandboxJob,
    database_now: DateTime<Utc>,
) -> Result<JobProjection, RepositoryError> {
    match current.job.state {
        JobState::Ready => Ok(current.job.clone()),
        JobState::Leased if current.payload.physical.is_none() => Ok(decide_expired_lease(
            &current.job,
            current.job.version,
            current.job.lease_generation,
            database_now,
            JobState::Ready,
            None,
        )?),
        JobState::Running if current.payload.physical.is_some() => Ok(decide_expired_continuation(
            &current.job,
            current.job.version,
            current.job.lease_generation,
            database_now,
        )?),
        _ => Err(RepositoryError::Conflict("OpenSandbox continuation claim")),
    }
}

fn lease_policy(requested_milliseconds: u64) -> LeasePolicy {
    LeasePolicy {
        requested_milliseconds,
        hard_maximum_milliseconds: u64::try_from(MAX_JOB_LEASE_MILLISECONDS)
            .expect("positive lease maximum"),
    }
}

fn locked_from_row(row: PgRow) -> Result<LockedOpenSandboxJob, RepositoryError> {
    let record = job_from_row(row)?;
    let job = job_projection(&record)?;
    let payload: SandboxDispatcherJobPayloadV1 =
        serde_json::from_value(record.payload.value.clone())
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    payload
        .validate_for(&job)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if record.job_kind != SANDBOX_JOB_KIND
        || record.work_class != "sandbox"
        || record.owner_kind != "job"
        || record.owner_id != record.job_id
        || record.request_digest != payload.submission_digest.to_string()
        || record.invocation_id.as_deref() != Some(payload.plan.invocation_id.to_string().as_str())
        || record.run_id.is_none()
        || record.node_id.is_none()
    {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox Job relational and payload bindings differ".to_owned(),
        ));
    }
    Ok(LockedOpenSandboxJob {
        record,
        job,
        payload,
    })
}

async fn load_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<LockedOpenSandboxJob, RepositoryError> {
    let query = if for_update {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("OpenSandbox Job"))?;
    locked_from_row(row)
}

fn require_fenced_identity(
    current: &LockedOpenSandboxJob,
    identity: &SandboxFencedIdentityV1,
) -> Result<(), RepositoryError> {
    let fence = fence_for(&current.job)?;
    if current.job.tenant_id != identity.tenant_id
        || current.job.job_id != identity.job_id
        || fence != identity.fence
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

fn require_same_lease_identity(
    current: &LockedOpenSandboxJob,
    identity: &SandboxFencedIdentityV1,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if current.payload.control.is_some()
        || current.payload.terminal.is_some()
        || current.job.state == JobState::Cancelling
    {
        return Err(RepositoryError::StaleFence);
    }
    let lease = current
        .job
        .lease
        .as_ref()
        .ok_or(RepositoryError::StaleFence)?;
    if current.job.tenant_id != identity.tenant_id
        || current.job.job_id != identity.job_id
        || lease.worker_process_generation_id != identity.fence.worker_process_generation_id
        || lease.lease_generation != identity.fence.lease_generation
        || lease.token_digest != identity.fence.token_digest
        || database_now >= lease.expires_at
    {
        return Err(RepositoryError::StaleFence);
    }
    Ok(())
}

fn fence_for(job: &JobProjection) -> Result<JobFence, RepositoryError> {
    let lease = job.lease.as_ref().ok_or(RepositoryError::StaleFence)?;
    Ok(JobFence {
        expected_version: job.version,
        worker_process_generation_id: lease.worker_process_generation_id.clone(),
        lease_generation: lease.lease_generation,
        token_digest: lease.token_digest.clone(),
    })
}

async fn load_request_input(
    transaction: &mut Transaction<'_, Postgres>,
    payload: &SandboxDispatcherJobPayloadV1,
    lock_invocation: bool,
) -> Result<Value, RepositoryError> {
    let invocation = load_capability_invocation(
        transaction,
        &payload.plan.tenant_id,
        &payload.plan.invocation_id,
        lock_invocation,
    )
    .await?;
    load_request_input_for_invocation(transaction, payload, &invocation, false).await
}

async fn load_request_input_for_invocation(
    transaction: &mut Transaction<'_, Postgres>,
    payload: &SandboxDispatcherJobPayloadV1,
    invocation: &CapabilityInvocationRecord,
    allow_terminal: bool,
) -> Result<Value, RepositoryError> {
    let allowed_state = matches!(
        invocation.state,
        InvocationState::Deferred | InvocationState::Cancelling
    ) || (allow_terminal
        && matches!(
            invocation.state,
            InvocationState::Succeeded
                | InvocationState::Failed
                | InvocationState::Cancelled
                | InvocationState::TimedOut
                | InvocationState::ReconciliationRequired
        ));
    if invocation.tenant_id != payload.plan.tenant_id
        || invocation.invocation_id != payload.plan.invocation_id
        || invocation.payload.current_job_id.as_ref() != Some(&payload.plan.job_id)
        || !allowed_state
        || invocation.deadline != payload.plan.deadline_at
        || invocation.payload.admission.output_schema_digest != payload.plan.output_schema_digest
    {
        return Err(RepositoryError::Conflict(
            "OpenSandbox CapabilityInvocation binding",
        ));
    }
    let input = load_capability_execution_input(transaction, invocation).await?;
    if input.exact.value_id != payload.plan.input_value_id
        || input.exact.schema_digest != payload.plan.input_schema_digest
        || input.exact.content_digest != payload.plan.input_digest
        || input.exact.classification != payload.plan.classification
    {
        return Err(RepositoryError::Conflict("OpenSandbox input RunValue"));
    }
    match input.material {
        CapabilityExecutionInputMaterial::Inline { value } => Ok(value),
        CapabilityExecutionInputMaterial::LinkedArtifact { .. } => {
            Err(RepositoryError::InvalidInput(
                "OpenSandbox Artifact input port is inactive in CR-216".to_owned(),
            ))
        }
    }
}

async fn hydrate_current_request(
    transaction: &mut Transaction<'_, Postgres>,
    current: &LockedOpenSandboxJob,
    lock_invocation: bool,
) -> Result<SandboxExecutionRequestV1, RepositoryError> {
    let input = load_request_input(transaction, &current.payload, lock_invocation).await?;
    let lease = current
        .job
        .lease
        .as_ref()
        .ok_or(RepositoryError::StaleFence)?;
    let physical_attempt = current
        .payload
        .physical
        .as_deref()
        .map(|physical| physical.provisioning_token.physical_attempt)
        .unwrap_or_else(|| current.job.attempt_count.saturating_add(1));
    Ok(current.payload.plan.bind_request(
        lease.lease_generation,
        physical_attempt,
        lease.worker_process_generation_id.clone(),
        current.job.trace,
        input,
    )?)
}

async fn hydrate_request_for_invocation(
    transaction: &mut Transaction<'_, Postgres>,
    current: &LockedOpenSandboxJob,
    invocation: &CapabilityInvocationRecord,
) -> Result<SandboxExecutionRequestV1, RepositoryError> {
    let input =
        load_request_input_for_invocation(transaction, &current.payload, invocation, false).await?;
    let lease = current
        .job
        .lease
        .as_ref()
        .ok_or(RepositoryError::StaleFence)?;
    let physical_attempt = current
        .payload
        .physical
        .as_deref()
        .ok_or_else(|| {
            RepositoryError::CorruptRow(
                "OpenSandbox terminal Job has no physical evidence".to_owned(),
            )
        })?
        .provisioning_token
        .physical_attempt;
    Ok(current.payload.plan.bind_request(
        lease.lease_generation,
        physical_attempt,
        lease.worker_process_generation_id.clone(),
        current.job.trace,
        input,
    )?)
}

fn decision_from(
    record: JobRecord,
    payload: SandboxDispatcherJobPayloadV1,
) -> Result<SandboxRepositoryDecisionV1, RepositoryError> {
    let job = job_projection(&record)?;
    let fence = job.lease.as_ref().map(|_| fence_for(&job)).transpose()?;
    let decision = SandboxRepositoryDecisionV1 {
        job,
        payload,
        fence,
    };
    decision.validate()?;
    Ok(decision)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    next: &JobProjection,
    payload: &SandboxDispatcherJobPayloadV1,
    database_now: DateTime<Utc>,
    result_digest: Option<&str>,
    quota_reservation_id: Option<&str>,
) -> Result<JobRecord, RepositoryError> {
    payload.validate_for(next)?;
    let stored = TypedPayload::from_versioned(1, payload, MAX_SANDBOX_JOB_PAYLOAD_BYTES)?;
    let (worker_id, token_digest, expires_at, heartbeat_at) = next
        .lease
        .as_ref()
        .map(|lease| {
            (
                Some(lease.worker_process_generation_id.to_string()),
                Some(lease.token_digest.to_string()),
                Some(lease.expires_at),
                Some(lease.heartbeat_at),
            )
        })
        .unwrap_or((None, None, None, None));
    let started_at = if current.started_at.is_none() && next.state == JobState::Running {
        Some(database_now)
    } else {
        current.started_at
    };
    let terminal_at = if matches!(
        next.state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    ) {
        current.terminal_at.or(Some(database_now))
    } else {
        current.terminal_at
    };
    let version = to_i64(next.version, "OpenSandbox Job version")?;
    let attempt = i32::try_from(next.attempt_count).map_err(|_| {
        RepositoryError::InvalidInput("OpenSandbox attempt count exceeds integer".to_owned())
    })?;
    let lease_generation = to_i64(next.lease_generation, "OpenSandbox lease generation")?;
    let row = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, attempt_no = $6, lease_epoch = $7,
            worker_id = $8, lease_token_digest = $9, lease_expires_at = $10,
            heartbeat_at = $11, scheduled_at = $12, retry_at = $13,
            result_digest = $14, quota_reservation_id = $15,
            payload_schema_version = $16, payload = $17, payload_digest = $18,
            started_at = $19, terminal_at = $20, updated_at = $21
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
        RETURNING *
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.job_id)
    .bind(current.version)
    .bind(next.state.as_str())
    .bind(version)
    .bind(attempt)
    .bind(lease_generation)
    .bind(worker_id)
    .bind(token_digest)
    .bind(expires_at)
    .bind(heartbeat_at)
    .bind(next.scheduled_at)
    .bind(next.retry_at)
    .bind(result_digest)
    .bind(quota_reservation_id)
    .bind(stored.schema_version)
    .bind(&stored.value)
    .bind(&stored.digest)
    .bind(started_at)
    .bind(terminal_at)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("OpenSandbox Job"))?;
    job_from_row(row)
}

async fn normalize_terminal_outcome(
    transaction: &mut Transaction<'_, Postgres>,
    current: &LockedOpenSandboxJob,
    invocation: &CapabilityInvocationRecord,
    request: &SandboxExecutionRequestV1,
    outcome: &SandboxTerminalOutcomeV1,
) -> Result<DetachedCapabilityJobOutcome, RepositoryError> {
    match outcome {
        SandboxTerminalOutcomeV1::Succeeded { result } => {
            let SandboxRunnerOutcomeV1::Succeeded {
                output,
                output_schema_digest,
                output_digest,
                ..
            } = &result.result
            else {
                return Err(RepositoryError::CorruptRow(
                    "OpenSandbox success has a non-success runner frame".to_owned(),
                ));
            };
            let interface = load_exact_capability_interface_spec(
                transaction,
                &invocation.tenant_id,
                &invocation.payload.admission,
            )
            .await?;
            if !interface.data_policy.permits_output(
                invocation.payload.admission.input.classification,
                request.classification,
            ) {
                return Err(RepositoryError::InvalidInput(
                    "OpenSandbox output violates the frozen data-flow policy".to_owned(),
                ));
            }
            let value = ValueRef::Inline {
                value: output.clone(),
            };
            validate_capability_value_against_schema(
                &interface.output_schema,
                &value,
                interface.execution_limits.maximum_output_bytes,
            )?;
            let validation_evidence_digest = parse_digest(
                canonical_digest(&serde_json::json!({
                    "classification": request.classification,
                    "interface_revision": invocation.payload.admission.interface,
                    "invocation_id": invocation.invocation_id,
                    "job_id": current.job.job_id,
                    "output_content_digest": output_digest,
                    "output_schema_digest": output_schema_digest,
                    "sandbox_request_digest": request.request_digest,
                    "schema_version": 1,
                    "tenant_id": invocation.tenant_id,
                }))
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
                "OpenSandbox validation evidence",
            )?;
            Ok(DetachedCapabilityJobOutcome::Completed(
                CapabilityOutputValue {
                    value_id: request.output_value_id.clone(),
                    classification: request.classification,
                    schema_digest: output_schema_digest.clone(),
                    content_digest: output_digest.clone(),
                    value,
                    artifact_link_id: None,
                    validation_evidence_digest,
                },
            ))
        }
        SandboxTerminalOutcomeV1::Failed {
            failure_class,
            evidence_digest,
            ..
        } => Ok(DetachedCapabilityJobOutcome::PermanentFailure(
            SafeBackendFailure {
                failure: Failure {
                    code: FailureCode::Platform {
                        code: PlatformFailureCode::CapabilityFailed,
                    },
                    class: match failure_class {
                        SandboxFailureClassV1::PackageTimedOut => FailureClass::Deadline,
                        SandboxFailureClassV1::OutputInvalid => FailureClass::Validation,
                        SandboxFailureClassV1::OutputTooLarge => FailureClass::Resource,
                        SandboxFailureClassV1::PackageFailed => FailureClass::External,
                        SandboxFailureClassV1::RunnerFailure => FailureClass::Platform,
                    },
                    retryability: Retryability::Never,
                    safe_message: Some("Sandbox execution failed".to_owned()),
                    details_ref: None,
                    source: FailureSource::Capability,
                },
                evidence_digest: evidence_digest.clone(),
            },
        )),
        SandboxTerminalOutcomeV1::Cancelled { .. } => Ok(DetachedCapabilityJobOutcome::Cancelled),
        SandboxTerminalOutcomeV1::TimedOut { .. } => Ok(DetachedCapabilityJobOutcome::TimedOut),
        SandboxTerminalOutcomeV1::UnknownOutcome { evidence_digest } => Ok(
            DetachedCapabilityJobOutcome::Uncertain(CapabilityUncertainty {
                observation_digest: evidence_digest.clone(),
                policy_path_digest: invocation
                    .payload
                    .admission
                    .policies
                    .canonical_digest
                    .clone(),
                external_identity_digest: current
                    .payload
                    .physical
                    .as_deref()
                    .map(|physical| physical.provisioning_token_digest.clone())
                    .unwrap_or_else(|| evidence_digest.clone()),
                manual: true,
            }),
        ),
    }
}

async fn lock_quota_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    reservation_id: &str,
    payload: &SandboxDispatcherJobPayloadV1,
) -> Result<Vec<OpenSandboxQuotaLine>, RepositoryError> {
    let rows = sqlx::query(
        r#"
        SELECT account.tenant_id, account.quota_account_id, account.scope_kind,
               account.scope_id, account.work_class, account.metric,
               account.reserved_value, account.version,
               reserve.reserved_amount, reserve.used_amount AS reservation_used_amount
        FROM insight_platform.quota_ledger AS reserve
        JOIN insight_platform.quota_accounts AS account
          ON account.tenant_id = reserve.tenant_id
         AND account.quota_account_id = reserve.quota_account_id
        WHERE reserve.tenant_id = $1 AND reserve.correlation_id = $2
          AND reserve.entry_kind = 'reserve'
        ORDER BY account.tenant_id, account.quota_account_id
        FOR UPDATE OF account
        "#,
    )
    .bind(tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != SANDBOX_QUOTA_LINE_COUNT {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox quota reservation must contain four lines".to_owned(),
        ));
    }
    let settled: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM insight_platform.quota_ledger WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle')",
    )
    .bind(tenant_id)
    .bind(reservation_id)
    .fetch_one(&mut **transaction)
    .await?;
    if settled {
        return Err(RepositoryError::Conflict("OpenSandbox quota settlement"));
    }
    let mut metrics = BTreeSet::new();
    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        let metric: String = row.try_get("metric")?;
        let reserved_amount: i64 = row.try_get("reserved_amount")?;
        if row.try_get::<String, _>("tenant_id")? != tenant_id
            || row.try_get::<String, _>("scope_kind")? != "tenant"
            || row.try_get::<String, _>("scope_id")? != tenant_id
            || row.try_get::<String, _>("work_class")? != "sandbox"
            || !metrics.insert(metric.clone())
            || reserved_amount != quota_reservation_amount(&metric, payload)?
            || row.try_get::<i64, _>("reservation_used_amount")? != 0
            || row.try_get::<i64, _>("reserved_value")? < reserved_amount
        {
            return Err(RepositoryError::CorruptRow(
                "OpenSandbox quota reservation differs from the frozen plan".to_owned(),
            ));
        }
        lines.push(OpenSandboxQuotaLine {
            tenant_id: tenant_id.to_owned(),
            quota_account_id: row.try_get("quota_account_id")?,
            metric,
            reserved_amount,
            account_version: row.try_get("version")?,
        });
    }
    if metrics != sandbox_quota_metrics() {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox quota reservation is incomplete".to_owned(),
        ));
    }
    Ok(lines)
}

async fn settle_quota_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    reservation_id: &str,
    lines: &[OpenSandboxQuotaLine],
    outcome: &SandboxTerminalOutcomeV1,
    request_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    if lines.len() != SANDBOX_QUOTA_LINE_COUNT {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox quota settlement is incomplete".to_owned(),
        ));
    }
    for line in lines {
        let used_amount = quota_used_amount(line, outcome)?;
        let version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value - $4,
                used_value = used_value + $5,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value >= $4
              AND reserved_value + used_value - $4 + $5 <= limit_value
            RETURNING version
            "#,
        )
        .bind(&line.tenant_id)
        .bind(&line.quota_account_id)
        .bind(line.account_version)
        .bind(line.reserved_amount)
        .bind(used_amount)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("OpenSandbox quota account"))?;
        let entry_id = new_id(ResourceKind::QuotaLedgerEntry)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version, request_digest
            ) VALUES ($1, $2, $3, $4, 'settle', $5, $6, $7, $8)
            "#,
        )
        .bind(&line.tenant_id)
        .bind(entry_id.to_string())
        .bind(&line.quota_account_id)
        .bind(reservation_id)
        .bind(line.reserved_amount)
        .bind(used_amount)
        .bind(version)
        .bind(request_digest.to_string())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

fn quota_reservation_amount(
    metric: &str,
    payload: &SandboxDispatcherJobPayloadV1,
) -> Result<i64, RepositoryError> {
    let limits = &payload.plan.limits;
    let amount = if metric == QuotaDimension::SandboxConcurrentExecutions.as_str() {
        1
    } else if metric == QuotaDimension::SandboxCpuSeconds.as_str() {
        ceil_div(
            u64::from(limits.cpu_millicores)
                .checked_mul(limits.wall_milliseconds)
                .ok_or_else(|| {
                    RepositoryError::InvalidInput("OpenSandbox CPU ceiling overflow".to_owned())
                })?,
            1_000_000,
        )
    } else if metric == QuotaDimension::SandboxMemoryMebibytes.as_str() {
        u64::from(limits.memory_mebibytes)
    } else if metric == QuotaDimension::SandboxOutputBytes.as_str() {
        limits.maximum_output_bytes
    } else {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox quota metric is not registered".to_owned(),
        ));
    };
    i64::try_from(amount)
        .map_err(|_| RepositoryError::InvalidInput("OpenSandbox quota exceeds bigint".to_owned()))
}

fn quota_used_amount(
    line: &OpenSandboxQuotaLine,
    outcome: &SandboxTerminalOutcomeV1,
) -> Result<i64, RepositoryError> {
    let amount = if line.metric == QuotaDimension::SandboxConcurrentExecutions.as_str()
        || line.metric == QuotaDimension::SandboxMemoryMebibytes.as_str()
    {
        0
    } else if line.metric == QuotaDimension::SandboxCpuSeconds.as_str() {
        line.reserved_amount
    } else if line.metric == QuotaDimension::SandboxOutputBytes.as_str() {
        match outcome {
            SandboxTerminalOutcomeV1::Succeeded { result } => match &result.result {
                SandboxRunnerOutcomeV1::Succeeded {
                    declared_output_bytes,
                    ..
                } => i64::try_from(*declared_output_bytes).map_err(|_| {
                    RepositoryError::InvalidInput(
                        "OpenSandbox output usage exceeds bigint".to_owned(),
                    )
                })?,
                SandboxRunnerOutcomeV1::Failed { .. } => line.reserved_amount,
            },
            SandboxTerminalOutcomeV1::Failed { result, .. } => result
                .as_deref()
                .and_then(|frame| match frame.result {
                    SandboxRunnerOutcomeV1::Failed {
                        diagnostic_bytes, ..
                    } => Some(i64::from(diagnostic_bytes)),
                    SandboxRunnerOutcomeV1::Succeeded { .. } => None,
                })
                .unwrap_or(line.reserved_amount),
            SandboxTerminalOutcomeV1::Cancelled { .. }
            | SandboxTerminalOutcomeV1::TimedOut { .. }
            | SandboxTerminalOutcomeV1::UnknownOutcome { .. } => line.reserved_amount,
        }
    } else {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox quota metric is not registered".to_owned(),
        ));
    };
    if amount < 0 || amount > line.reserved_amount {
        return Err(RepositoryError::QuotaExceeded);
    }
    Ok(amount)
}

fn sandbox_quota_metrics() -> BTreeSet<String> {
    BTreeSet::from([
        QuotaDimension::SandboxConcurrentExecutions
            .as_str()
            .to_owned(),
        QuotaDimension::SandboxCpuSeconds.as_str().to_owned(),
        QuotaDimension::SandboxMemoryMebibytes.as_str().to_owned(),
        QuotaDimension::SandboxOutputBytes.as_str().to_owned(),
    ])
}

fn cleanup_observation_replays(
    payload: &SandboxDispatcherJobPayloadV1,
    observation: &SandboxCleanupObservationV1,
) -> bool {
    let Some(physical) = payload.physical.as_deref() else {
        return false;
    };
    match observation {
        SandboxCleanupObservationV1::Absent {
            sandbox_id,
            evidence_digest,
        } => {
            physical.absent_candidate_ids.contains(sandbox_id)
                && physical.absence_evidence_digest.as_ref() == Some(evidence_digest)
        }
    }
}

fn ceil_div(numerator: u64, denominator: u64) -> u64 {
    numerator.div_ceil(denominator)
}

async fn append_job_event(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    event_type: &str,
    value: Value,
) -> Result<(), RepositoryError> {
    let event_id = new_id(ResourceKind::Event)?;
    let outbox_id = new_id(ResourceKind::OutboxEvent)?;
    append_scheduler_event(
        transaction,
        &job.tenant_id,
        &event_id,
        &outbox_id,
        "job",
        &job.job_id,
        job.version,
        job.run_id.as_deref(),
        event_type,
        &TypedPayload::new(1, &value)?,
    )
    .await
}

fn physical_event_payload(payload: &SandboxDispatcherJobPayloadV1) -> Value {
    serde_json::json!({
        "job_id": payload.plan.job_id,
        "physical_attempt": payload.physical.as_deref().map(|physical| physical.provisioning_token.physical_attempt),
        "physical_phase": payload.physical.as_deref().map(|physical| physical.phase),
        "request_digest": payload.plan.request_digest,
    })
}

async fn database_now(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<DateTime<Utc>, RepositoryError> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await?)
}

fn parse_expected_id(
    value: &str,
    kind: ResourceKind,
    label: &'static str,
) -> Result<ResourceId, RepositoryError> {
    ResourceId::parse_expected(value, kind)
        .map_err(|failure| RepositoryError::CorruptRow(format!("{label}: {failure}")))
}

fn parse_digest(value: String, label: &'static str) -> Result<Sha256Digest, RepositoryError> {
    value
        .parse()
        .map_err(|failure| RepositoryError::InvalidInput(format!("{label}: {failure}")))
}

fn to_i64(value: u64, label: &'static str) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::InvalidInput(format!("{label} exceeds bigint")))
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, RepositoryError> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7())
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))
}
