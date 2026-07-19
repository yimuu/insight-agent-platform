//! Production orchestration around the pure planner and durable scheduler
//! repository. Every crash hook is on the actual side-effect path.

use std::{
    cell::Cell, collections::BTreeMap, future::Future, panic::AssertUnwindSafe, pin::Pin,
    sync::Once,
};

use chrono::Utc;
use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use crate::engine::{
    plan::LinkedPlan, EffectEvidence, SchedulerAction, SchedulerDecision, SchedulerPlanner,
    SchedulerQuiescence, TransitionOutcome, ValueRef, WorkerArtifactStore, WorkerExecutionContext,
    WorkerExecutorRegistry, WorkerFailure, WorkerFailureClass,
};

use super::{
    FencedSchedulerRunCommand, RepositoryError, SchedulerCommitReceipt, SchedulerCrashInjector,
    SchedulerCrashPoint, SchedulerDurableRepository, SchedulerFailureDisposition,
    SchedulerTaskClaim, SchedulerTaskClaimMode, SchedulerTaskCommitOutcome,
    SchedulerTaskCompletionReceipt, SchedulerTaskFailure, SchedulerTaskHeartbeatOutcome,
    SchedulerTaskOutcome, SchedulerTaskSuccess, StageArtifactCommand, VerifyArtifactCommand,
    REPOSITORY_DATA_INVALID,
};

const WORKER_EXECUTOR_PANICKED: &str = "WORKER_EXECUTOR_PANICKED";
const WORKER_LEASE_LOST: &str = "WORKER_LEASE_LOST";
const ARTIFACT_STAGING_GRACE_SECONDS: i64 = 3_600;

thread_local! {
    static EXECUTOR_PANIC_REDACTION: Cell<bool> = const { Cell::new(false) };
}

static INSTALL_EXECUTOR_PANIC_HOOK: Once = Once::new();

struct ExecutorPanicRedactionGuard(bool);

impl ExecutorPanicRedactionGuard {
    fn enter() -> Self {
        let previous = EXECUTOR_PANIC_REDACTION.with(|active| active.replace(true));
        Self(previous)
    }
}

impl Drop for ExecutorPanicRedactionGuard {
    fn drop(&mut self) {
        EXECUTOR_PANIC_REDACTION.with(|active| active.set(self.0));
    }
}

fn redact_executor_panic_payload<F: Future>(future: F) -> impl Future<Output = F::Output> {
    INSTALL_EXECUTOR_PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |information| {
            if EXECUTOR_PANIC_REDACTION.with(Cell::get) {
                // Panic payloads can contain provider responses, prompts or
                // secrets. Keep the signal while dropping both payload and
                // location at this untrusted extension boundary.
                eprintln!("worker executor panicked; payload redacted");
            } else {
                previous(information);
            }
        }));
    });
    let mut future = Box::pin(future);
    std::future::poll_fn(move |context| {
        let _guard = ExecutorPanicRedactionGuard::enter();
        future.as_mut().poll(context)
    })
}

pub trait SchedulerWorkerFailurePolicy: Send + Sync {
    /// Freeze retry/backoff/timeout here. The returned disposition is included
    /// in the canonical result intent and is never recomputed during replay.
    fn freeze(
        &self,
        claim: &SchedulerTaskClaim,
        failure: &WorkerFailure,
    ) -> Result<SchedulerTaskFailure, RepositoryError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TerminalSchedulerWorkerFailurePolicy;

impl SchedulerWorkerFailurePolicy for TerminalSchedulerWorkerFailurePolicy {
    fn freeze(
        &self,
        claim: &SchedulerTaskClaim,
        failure: &WorkerFailure,
    ) -> Result<SchedulerTaskFailure, RepositoryError> {
        let evidence = if failure.class() == WorkerFailureClass::EffectOutcomeUnknown {
            EffectEvidence::Unknown
        } else {
            // The attempt is marked started immediately before entering the
            // exact implementation. Conservatively retain that evidence.
            EffectEvidence::Started
        };
        let disposition = SchedulerFailureDisposition::Terminal;
        SchedulerTaskFailure::from_worker_failure(claim, failure, evidence, disposition)
    }
}

/// Production retry authority. Every decision is derived only from the
/// immutable policy carried by the claimed durable task and is frozen into the
/// committed outcome together with an absolute retry timestamp.
#[derive(Debug, Default, Clone, Copy)]
pub struct FrozenSchedulerWorkerFailurePolicy;

impl SchedulerWorkerFailurePolicy for FrozenSchedulerWorkerFailurePolicy {
    fn freeze(
        &self,
        claim: &SchedulerTaskClaim,
        failure: &WorkerFailure,
    ) -> Result<SchedulerTaskFailure, RepositoryError> {
        let policy = claim.envelope().request().effect_policy();
        let evidence = claim.lease_loss_evidence().unwrap_or_else(|| {
            if failure.class() == WorkerFailureClass::EffectOutcomeUnknown {
                EffectEvidence::Unknown
            } else {
                EffectEvidence::Started
            }
        });
        let attempt_no = claim.envelope().attempt_no().get();
        let remaining_attempts = policy.max_attempts().saturating_sub(attempt_no);
        let disposition = if failure.retryable()
            && remaining_attempts > 0
            && evidence.permits_automatic_retry(policy.effect_idempotency())
        {
            let retry_at = Utc::now()
                + chrono::Duration::milliseconds(
                    i64::try_from(retry_backoff_ms(policy, attempt_no))
                        .map_err(|_| RepositoryError::invalid_configuration())?,
                );
            SchedulerFailureDisposition::Retry {
                retry_at,
                remaining_attempts,
            }
        } else {
            SchedulerFailureDisposition::Terminal
        };
        SchedulerTaskFailure::from_worker_failure(claim, failure, evidence, disposition)
    }
}

fn retry_backoff_ms(policy: &crate::engine::WorkerEffectPolicy, attempt_no: u32) -> u64 {
    let exponent = attempt_no.saturating_sub(1).min(63);
    policy
        .initial_backoff_ms()
        .saturating_mul(1_u64 << exponent)
        .min(policy.max_backoff_ms())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerDriveOutcome {
    Applied(SchedulerCommitReceipt),
    Quiescent(SchedulerQuiescence),
    Fenced,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerRecoveryOutcome {
    Quiescent(SchedulerQuiescence),
    Fenced,
    ActionBudgetExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerWorkerPumpOutcome {
    NoTask,
    AcknowledgedRecoveredResult,
    Committed {
        receipt: SchedulerTaskCompletionReceipt,
        acknowledged: bool,
    },
    LeaseLost {
        effect_evidence: EffectEvidence,
    },
}

pub async fn drive_scheduler_once<R, I>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    crash: &I,
) -> Result<SchedulerDriveOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
{
    let planner = SchedulerPlanner::new(linked);
    let loaded = repository.load_scheduler_facts(fence.run_id()).await;
    let (decision, has_checkpoints) = match loaded {
        Ok(facts) => {
            let has_checkpoints = !facts.checkpoints().is_empty();
            let decision = match planner.plan(&facts) {
                Ok(decision) => decision,
                Err(error) => SchedulerDecision::Action(Box::new(
                    planner
                        .fail_closed_action(&facts, &error)
                        .map_err(|_| RepositoryError::invalid_data())?,
                )),
            };
            (decision, has_checkpoints)
        }
        Err(error) if error.code() == REPOSITORY_DATA_INVALID => {
            let projection = repository
                .load_run(fence.run_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            if projection.lifecycle().is_terminal() {
                let quiescence = match projection.lifecycle() {
                    crate::engine::RunLifecycle::Succeeded => SchedulerQuiescence::RunSucceeded,
                    crate::engine::RunLifecycle::Cancelled => SchedulerQuiescence::RunCancelled,
                    _ => SchedulerQuiescence::RunFailed,
                };
                return Ok(SchedulerDriveOutcome::Quiescent(quiescence));
            }
            (
                SchedulerDecision::Action(Box::new(
                    planner
                        .fail_closed_action_at(
                            fence.run_id(),
                            projection.projection_version(),
                            crate::engine::SchedulerPlanningFailure::FactInconsistent,
                        )
                        .map_err(|_| RepositoryError::invalid_data())?,
                )),
                false,
            )
        }
        Err(error) => return Err(error),
    };
    let SchedulerDecision::Action(action) = decision else {
        let SchedulerDecision::Quiescent(quiescence) = decision else {
            unreachable!("SchedulerDecision is closed")
        };
        return Ok(SchedulerDriveOutcome::Quiescent(quiescence));
    };

    let branch_commit = matches!(
        action.intent().action(),
        SchedulerAction::SelectBranchAndAdmit { .. }
    );
    let join_commit = matches!(
        action.intent().action(),
        SchedulerAction::CompleteFork { .. }
    );
    let downstream_admission = branch_commit
        || (matches!(
            action.intent().action(),
            SchedulerAction::AdmitActivation { .. }
        ) && has_checkpoints);
    if branch_commit {
        crash.hit(SchedulerCrashPoint::BeforeBranchCommit)?;
    }
    if join_commit {
        crash.hit(SchedulerCrashPoint::BeforeJoinCommit)?;
    }
    if downstream_admission {
        crash.hit(SchedulerCrashPoint::BeforeDownstreamAdmission)?;
    }
    let outcome = repository.commit_scheduler_action(fence, &action).await?;
    let receipt = match outcome {
        TransitionOutcome::Committed { result } => result,
        TransitionOutcome::ExactReplay { authoritative } => authoritative,
        TransitionOutcome::StaleLease | TransitionOutcome::StateConflict => {
            return Ok(SchedulerDriveOutcome::Fenced)
        }
    };
    if branch_commit {
        crash.hit(SchedulerCrashPoint::AfterBranchCommit)?;
    }
    if join_commit {
        crash.hit(SchedulerCrashPoint::AfterJoinCommit)?;
    }
    if downstream_admission {
        crash.hit(SchedulerCrashPoint::AfterDownstreamAdmission)?;
    }
    Ok(SchedulerDriveOutcome::Applied(receipt))
}

pub async fn drive_scheduler_until_quiescent<R, I>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    crash: &I,
    max_actions: u32,
) -> Result<SchedulerRecoveryOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
{
    if max_actions == 0 {
        return Err(RepositoryError::invalid_configuration());
    }
    for _ in 0..max_actions {
        match drive_scheduler_once(repository, linked, fence, crash).await? {
            SchedulerDriveOutcome::Applied(_) => {}
            SchedulerDriveOutcome::Quiescent(quiescence) => {
                return Ok(SchedulerRecoveryOutcome::Quiescent(quiescence))
            }
            SchedulerDriveOutcome::Fenced => return Ok(SchedulerRecoveryOutcome::Fenced),
        }
    }
    Ok(SchedulerRecoveryOutcome::ActionBudgetExhausted)
}

// This is the stable non-artifact worker-pump boundary. Keeping each runtime
// authority explicit at the call site is clearer than hiding lease, fairness,
// cancellation, and crash-injection policy in a loosely-related options bag.
#[allow(clippy::too_many_arguments)]
pub async fn consume_scheduler_task_once<R, I, P>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    claimant: &str,
    claim_seconds: u32,
    max_claimed_per_run: u32,
    cancellation: CancellationToken,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerWorkerFailurePolicy,
{
    consume_scheduler_task_once_inner(
        repository,
        registry,
        failure_policy,
        claimant,
        claim_seconds,
        max_claimed_per_run,
        cancellation,
        None,
        crash,
    )
    .await
}

/// Production worker path with content-addressed materialization for values
/// larger than the store's inline threshold.
#[allow(clippy::too_many_arguments)]
pub async fn consume_scheduler_task_once_with_artifact_store<R, I, P>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    claimant: &str,
    claim_seconds: u32,
    max_claimed_per_run: u32,
    cancellation: CancellationToken,
    artifact_store: &dyn WorkerArtifactStore,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerWorkerFailurePolicy,
{
    consume_scheduler_task_once_inner(
        repository,
        registry,
        failure_policy,
        claimant,
        claim_seconds,
        max_claimed_per_run,
        cancellation,
        Some(artifact_store),
        crash,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn consume_scheduler_task_once_inner<R, I, P>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    claimant: &str,
    claim_seconds: u32,
    max_claimed_per_run: u32,
    cancellation: CancellationToken,
    artifact_store: Option<&dyn WorkerArtifactStore>,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerWorkerFailurePolicy,
{
    let mut claims = repository
        .claim_scheduler_tasks_with_run_limit(claimant, claim_seconds, 1, max_claimed_per_run)
        .await?;
    let Some(mut claim) = claims.pop() else {
        return Ok(SchedulerWorkerPumpOutcome::NoTask);
    };
    if claim.mode() == SchedulerTaskClaimMode::Acknowledge {
        return if repository.acknowledge_scheduler_task(&claim).await? {
            Ok(SchedulerWorkerPumpOutcome::AcknowledgedRecoveredResult)
        } else {
            Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::Committed,
            })
        };
    }
    if claim.mode() == SchedulerTaskClaimMode::FinalizeLeaseLoss {
        let class = if claim.lease_loss_evidence() == Some(EffectEvidence::NotStarted) {
            WorkerFailureClass::InfrastructureFailure
        } else {
            WorkerFailureClass::EffectOutcomeUnknown
        };
        let failure = WorkerFailure::new(class, WORKER_LEASE_LOST, true)
            .expect("the closed lease-loss failure is valid");
        let outcome = SchedulerTaskOutcome::Failed(failure_policy.freeze(&claim, &failure)?);
        return commit_worker_outcome(repository, &claim, outcome, None, crash).await;
    }

    // Start the complete operation budget before mark-start. The durable
    // transaction establishes the authoritative DB-clock deadline, while
    // this earlier monotonic timer is a conservative local guard: repository
    // latency must never buy extra time in which the external executor may be
    // entered. The wall-clock deadline passed to the executor is frozen from
    // the same pre-mark boundary.
    let timeout_ms = claim.envelope().request().effect_policy().timeout_ms();
    let timeout_duration = std::time::Duration::from_millis(timeout_ms);
    let budget_started_at = tokio::time::Instant::now();
    let wall_started_at = Utc::now();
    let timeout_delta = chrono::Duration::milliseconds(
        i64::try_from(timeout_ms).map_err(|_| RepositoryError::invalid_configuration())?,
    );
    let operation_deadline = wall_started_at
        .checked_add_signed(timeout_delta)
        .ok_or_else(RepositoryError::invalid_configuration)?;
    let local_deadline = budget_started_at
        .checked_add(timeout_duration)
        .ok_or_else(RepositoryError::invalid_configuration)?;
    let mut deadline = Box::pin(tokio::time::sleep_until(local_deadline));

    match repository.mark_scheduler_task_started(&claim).await? {
        TransitionOutcome::Committed { .. } => {}
        TransitionOutcome::ExactReplay { .. } => {
            return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::Started,
            })
        }
        TransitionOutcome::StaleLease | TransitionOutcome::StateConflict => {
            return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::NotStarted,
            })
        }
    }
    let heartbeat_period_ms = (u64::from(claim_seconds).saturating_mul(1_000) / 3).max(1);
    let mut heartbeat =
        tokio::time::interval(std::time::Duration::from_millis(heartbeat_period_ms));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Tokio intervals tick immediately once. Consume that tick, then renew
    // explicitly before the external call: mark-start may have used most of
    // the originally claimed lease while waiting on a busy repository.
    heartbeat.tick().await;
    match repository
        .heartbeat_scheduler_task(&claim, claim_seconds)
        .await?
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => claim = renewed,
        SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed) => {
            claim = renewed;
            return commit_runtime_deadline(repository, &claim, crash).await;
        }
        SchedulerTaskHeartbeatOutcome::LeaseLost => {
            return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::Unknown,
            })
        }
    }
    if deadline.is_elapsed() {
        return await_and_commit_runtime_deadline(repository, &mut claim, claim_seconds, crash)
            .await;
    }
    let context = WorkerExecutionContext::new(
        claim.envelope().attempt_no(),
        claim.envelope().lease_epoch(),
        claim.envelope().fencing_token(),
        operation_deadline,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    crash.hit(SchedulerCrashPoint::BeforeExternalCall)?;
    let task_cancellation = cancellation.child_token();
    let request = claim.envelope().request().clone();
    // An executor is an extension boundary. A panic must not unwind the
    // worker pump or leave a started effect without a durable outcome. We do
    // not expose the panic payload because it may contain provider data or a
    // secret. Since the effect was marked started immediately above, its
    // outcome is conservatively unknown and is never retried unless a future
    // policy explicitly proves that safe.
    let mut worker = Box::pin(
        AssertUnwindSafe(redact_executor_panic_payload(registry.execute(
            &context,
            &request,
            task_cancellation.clone(),
        )))
        .catch_unwind(),
    );
    let mut cancellation_observed = false;
    let mut deadline_authority_observed = false;
    let worker_result = loop {
        tokio::select! {
            biased;
            _ = deadline.as_mut() => {
                task_cancellation.cancel();
                break None;
            }
            result = &mut worker => break Some(match result {
                Ok(result) => result,
                Err(_) => Err(WorkerFailure::new(
                    WorkerFailureClass::EffectOutcomeUnknown,
                    WORKER_EXECUTOR_PANICKED,
                    false,
                ).expect("the closed executor-panic failure is valid")),
            }),
            _ = cancellation.cancelled(), if !cancellation_observed => {
                cancellation_observed = true;
                task_cancellation.cancel();
            }
            _ = heartbeat.tick() => {
                match repository.heartbeat_scheduler_task(&claim, claim_seconds).await? {
                    SchedulerTaskHeartbeatOutcome::Renewed(renewed) => claim = renewed,
                    SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed) => {
                        claim = renewed;
                        deadline_authority_observed = true;
                        task_cancellation.cancel();
                        break None;
                    }
                    SchedulerTaskHeartbeatOutcome::LeaseLost => {
                        task_cancellation.cancel();
                        return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                            effect_evidence: EffectEvidence::Unknown,
                        });
                    }
                }
            }
        }
    };
    crash.hit(SchedulerCrashPoint::AfterExternalCall)?;
    let pending_outcome = match worker_result {
        Some(Ok(result)) => {
            let local_evidence = result.effect_evidence();
            let success = match artifact_store {
                Some(store) => match materialize_worker_success(
                    repository,
                    &mut claim,
                    claim_seconds,
                    &mut heartbeat,
                    deadline.as_mut(),
                    result,
                    store,
                )
                .await?
                {
                    GuardedWorkerStep::Completed(success) => success,
                    GuardedWorkerStep::OperationDeadlineElapsed => {
                        return await_and_commit_runtime_deadline(
                            repository,
                            &mut claim,
                            claim_seconds,
                            crash,
                        )
                        .await
                    }
                    GuardedWorkerStep::LeaseLost => {
                        return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                            effect_evidence: local_evidence,
                        })
                    }
                },
                None => {
                    let success = SchedulerTaskSuccess::inline(result)?;
                    if deadline.is_elapsed() {
                        return await_and_commit_runtime_deadline(
                            repository,
                            &mut claim,
                            claim_seconds,
                            crash,
                        )
                        .await;
                    }
                    success
                }
            };
            PendingWorkerOutcome::Succeeded(success)
        }
        Some(Err(failure)) => PendingWorkerOutcome::Failed(failure),
        None => {
            return if deadline_authority_observed {
                commit_runtime_deadline(repository, &claim, crash).await
            } else {
                await_and_commit_runtime_deadline(repository, &mut claim, claim_seconds, crash)
                    .await
            }
        }
    };

    // The final renewal is the hand-off from long-running runtime work to the
    // single durable first-winner commit. We deliberately do not race or
    // cancel a started commit future: doing so could make a committed success
    // look like a local timeout and submit a conflicting intent.
    if deadline.is_elapsed() {
        return await_and_commit_runtime_deadline(repository, &mut claim, claim_seconds, crash)
            .await;
    }
    match repository
        .heartbeat_scheduler_task(&claim, claim_seconds)
        .await?
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => claim = renewed,
        SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed) => {
            claim = renewed;
            return commit_runtime_deadline(repository, &claim, crash).await;
        }
        SchedulerTaskHeartbeatOutcome::LeaseLost => {
            return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                effect_evidence: pending_outcome.effect_evidence(),
            })
        }
    }
    if deadline.is_elapsed() {
        return await_and_commit_runtime_deadline(repository, &mut claim, claim_seconds, crash)
            .await;
    }
    let outcome = match pending_outcome {
        PendingWorkerOutcome::Succeeded(success) => SchedulerTaskOutcome::Succeeded(success),
        PendingWorkerOutcome::Failed(failure) => {
            SchedulerTaskOutcome::Failed(failure_policy.freeze(&claim, &failure)?)
        }
    };
    commit_worker_outcome(repository, &claim, outcome, Some(claim_seconds), crash).await
}

enum PendingWorkerOutcome {
    Succeeded(SchedulerTaskSuccess),
    Failed(WorkerFailure),
}

impl PendingWorkerOutcome {
    fn effect_evidence(&self) -> EffectEvidence {
        match self {
            Self::Succeeded(success) => success.result().effect_evidence(),
            Self::Failed(failure)
                if failure.class() == WorkerFailureClass::EffectOutcomeUnknown =>
            {
                EffectEvidence::Unknown
            }
            Self::Failed(_) => EffectEvidence::Started,
        }
    }
}

enum GuardedWorkerStep<T> {
    Completed(T),
    OperationDeadlineElapsed,
    LeaseLost,
}

async fn await_worker_step<R, F, T>(
    repository: &R,
    claim: &mut SchedulerTaskClaim,
    claim_seconds: u32,
    heartbeat: &mut tokio::time::Interval,
    mut deadline: Pin<&mut tokio::time::Sleep>,
    future: F,
) -> Result<GuardedWorkerStep<T>, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    F: Future<Output = Result<T, RepositoryError>>,
{
    tokio::pin!(future);
    loop {
        let heartbeat_claim = claim.clone();
        let renewal = async {
            heartbeat.tick().await;
            repository
                .heartbeat_scheduler_task(&heartbeat_claim, claim_seconds)
                .await
        };
        tokio::pin!(renewal);
        tokio::select! {
            biased;
            _ = deadline.as_mut() => {
                return Ok(GuardedWorkerStep::OperationDeadlineElapsed)
            }
            result = &mut future => return Ok(GuardedWorkerStep::Completed(result?)),
            renewed = &mut renewal => {
                match renewed? {
                    SchedulerTaskHeartbeatOutcome::Renewed(renewed) => *claim = renewed,
                    SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed) => {
                        *claim = renewed;
                        return Ok(GuardedWorkerStep::OperationDeadlineElapsed)
                    }
                    SchedulerTaskHeartbeatOutcome::LeaseLost => {
                        return Ok(GuardedWorkerStep::LeaseLost)
                    }
                }
            }
        }
    }
}

async fn materialize_worker_success<R>(
    repository: &R,
    claim: &mut SchedulerTaskClaim,
    claim_seconds: u32,
    heartbeat: &mut tokio::time::Interval,
    mut deadline: Pin<&mut tokio::time::Sleep>,
    result: crate::engine::worker::TaskExecutionResult,
    artifact_store: &dyn WorkerArtifactStore,
) -> Result<GuardedWorkerStep<SchedulerTaskSuccess>, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
{
    let mut value_refs = BTreeMap::new();
    // A single worker result may bind the same content-addressed value to
    // multiple output ports. Keep the full ArtifactRef as the cache key so a
    // media-type change cannot accidentally alias a distinct artifact
    // identity, while identical values share one stage/put/verify sequence.
    let mut materialized_artifacts = Vec::<(crate::engine::ArtifactRef, ValueRef)>::new();
    for (port_id, output) in result.outputs() {
        let bytes =
            serde_jcs::to_vec(output.value()).map_err(|_| RepositoryError::canonicalization())?;
        let value_ref = if bytes.len() <= artifact_store.inline_threshold_bytes() {
            ValueRef::inline(output.value().clone()).map_err(|_| RepositoryError::invalid_data())?
        } else {
            let artifact =
                artifact_store.artifact_for_bytes(&bytes, Some("application/json".to_owned()))?;
            if let Some((_, cached)) = materialized_artifacts
                .iter()
                .find(|(cached_artifact, _)| cached_artifact == &artifact)
            {
                cached.clone()
            } else {
                let locator = artifact_store.storage_locator(&artifact)?;
                let retain_until = claim
                    .claim_expires_at()
                    .checked_add_signed(chrono::Duration::seconds(ARTIFACT_STAGING_GRACE_SECONDS))
                    .ok_or_else(RepositoryError::invalid_configuration)?;
                let stage = repository.stage_artifact(StageArtifactCommand::new(
                    claim.run_id().clone(),
                    artifact.clone(),
                    locator,
                    Some(retain_until),
                ));
                match await_worker_step(
                    repository,
                    claim,
                    claim_seconds,
                    heartbeat,
                    deadline.as_mut(),
                    stage,
                )
                .await?
                {
                    GuardedWorkerStep::Completed(
                        TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. },
                    ) => {}
                    GuardedWorkerStep::Completed(
                        TransitionOutcome::StaleLease | TransitionOutcome::StateConflict,
                    ) => {
                        return Err(RepositoryError::invalid_data());
                    }
                    GuardedWorkerStep::OperationDeadlineElapsed => {
                        return Ok(GuardedWorkerStep::OperationDeadlineElapsed)
                    }
                    GuardedWorkerStep::LeaseLost => return Ok(GuardedWorkerStep::LeaseLost),
                }
                let (actual_hash, actual_size) = match await_worker_step(
                    repository,
                    claim,
                    claim_seconds,
                    heartbeat,
                    deadline.as_mut(),
                    artifact_store.put_and_verify(&artifact, &bytes),
                )
                .await?
                {
                    GuardedWorkerStep::Completed(verified) => verified,
                    GuardedWorkerStep::OperationDeadlineElapsed => {
                        return Ok(GuardedWorkerStep::OperationDeadlineElapsed)
                    }
                    GuardedWorkerStep::LeaseLost => return Ok(GuardedWorkerStep::LeaseLost),
                };
                let verify = repository.verify_artifact(VerifyArtifactCommand::new(
                    claim.run_id().clone(),
                    artifact.artifact_id().clone(),
                    actual_hash,
                    actual_size,
                ));
                match await_worker_step(
                    repository,
                    claim,
                    claim_seconds,
                    heartbeat,
                    deadline.as_mut(),
                    verify,
                )
                .await?
                {
                    GuardedWorkerStep::Completed(
                        TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. },
                    ) => {}
                    GuardedWorkerStep::Completed(
                        TransitionOutcome::StaleLease | TransitionOutcome::StateConflict,
                    ) => {
                        return Err(RepositoryError::invalid_data());
                    }
                    GuardedWorkerStep::OperationDeadlineElapsed => {
                        return Ok(GuardedWorkerStep::OperationDeadlineElapsed)
                    }
                    GuardedWorkerStep::LeaseLost => return Ok(GuardedWorkerStep::LeaseLost),
                }
                let value_ref = ValueRef::Artifact(artifact.clone());
                materialized_artifacts.push((artifact, value_ref.clone()));
                value_ref
            }
        };
        value_refs.insert(port_id.clone(), value_ref);
    }
    SchedulerTaskSuccess::with_value_refs(result, value_refs).map(GuardedWorkerStep::Completed)
}

async fn await_and_commit_runtime_deadline<R, I>(
    repository: &R,
    claim: &mut SchedulerTaskClaim,
    claim_seconds: u32,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
{
    if await_runtime_deadline_authority(repository, claim, claim_seconds).await? {
        commit_runtime_deadline(repository, claim, crash).await
    } else {
        Ok(SchedulerWorkerPumpOutcome::LeaseLost {
            effect_evidence: EffectEvidence::Unknown,
        })
    }
}

async fn await_runtime_deadline_authority<R>(
    repository: &R,
    claim: &mut SchedulerTaskClaim,
    claim_seconds: u32,
) -> Result<bool, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
{
    // A conservative local timer may expire before the DB-clock deadline
    // (for example when mark-start spent time waiting before its transaction).
    // Stop external work immediately, but keep the exact claim fresh and do
    // not submit the private timeout intent until the repository grants that
    // authority. The first check is immediate; subsequent checks use a short
    // bounded exponential backoff that remains safely below the lease-renewal
    // cadence, avoiding both a long claim_seconds / 3 delay and a tight DB
    // polling loop.
    let mut first_check = true;
    let max_backoff_ms = (u64::from(claim_seconds).saturating_mul(1_000) / 4).clamp(1, 1_000);
    let mut backoff_ms = 10_u64.min(max_backoff_ms);
    loop {
        if first_check {
            first_check = false;
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        }
        match repository
            .heartbeat_scheduler_task(claim, claim_seconds)
            .await?
        {
            SchedulerTaskHeartbeatOutcome::Renewed(renewed) => {
                *claim = renewed;
                backoff_ms = backoff_ms.saturating_mul(2).min(max_backoff_ms);
            }
            SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed) => {
                *claim = renewed;
                return Ok(true);
            }
            SchedulerTaskHeartbeatOutcome::LeaseLost => return Ok(false),
        }
    }
}

async fn commit_runtime_deadline<R, I>(
    repository: &R,
    claim: &SchedulerTaskClaim,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
{
    let outcome = SchedulerTaskOutcome::Failed(SchedulerTaskFailure::from_runtime_deadline(claim)?);
    commit_worker_outcome(repository, claim, outcome, None, crash).await
}

async fn commit_worker_outcome<R, I>(
    repository: &R,
    claim: &SchedulerTaskClaim,
    mut outcome: SchedulerTaskOutcome,
    deadline_claim_seconds: Option<u32>,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
{
    let mut claim = claim.clone();
    crash.hit(SchedulerCrashPoint::BeforeResultCommit)?;
    let receipt = loop {
        let local_evidence = match &outcome {
            SchedulerTaskOutcome::Succeeded(success) => success.result().effect_evidence(),
            SchedulerTaskOutcome::Failed(failure) => failure.effect_evidence(),
        };
        match repository
            .commit_scheduler_task_outcome(&claim, &outcome)
            .await?
        {
            SchedulerTaskCommitOutcome::Committed { result } => break result,
            SchedulerTaskCommitOutcome::ExactReplay { authoritative } => break authoritative,
            SchedulerTaskCommitOutcome::StaleLease | SchedulerTaskCommitOutcome::StateConflict => {
                return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                    effect_evidence: local_evidence,
                })
            }
            SchedulerTaskCommitOutcome::OperationDeadlineElapsed => {
                if matches!(
                    &outcome,
                    SchedulerTaskOutcome::Failed(failure) if failure.is_runtime_deadline()
                ) {
                    return Err(RepositoryError::invalid_data());
                }
                let claim_seconds =
                    deadline_claim_seconds.ok_or_else(RepositoryError::invalid_configuration)?;
                if !await_runtime_deadline_authority(repository, &mut claim, claim_seconds).await? {
                    return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                        effect_evidence: local_evidence,
                    });
                }
                // The first transaction was fully rolled back before this
                // authority result. Submit the private deadline intent only
                // after obtaining a renewed deadline-authority claim; never
                // infer it from a generic repository failure.
                outcome = SchedulerTaskOutcome::Failed(
                    SchedulerTaskFailure::from_runtime_deadline(&claim)?,
                );
            }
        }
    };
    crash.hit(SchedulerCrashPoint::AfterResultCommit)?;
    let acknowledged = if matches!(outcome, SchedulerTaskOutcome::Succeeded(_)) {
        repository.acknowledge_scheduler_task(&claim).await?
    } else {
        false
    };
    Ok(SchedulerWorkerPumpOutcome::Committed {
        receipt,
        acknowledged,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Duration;

    use super::*;
    use crate::engine::{
        plan::VersionTag, ActivationId, AttemptNo, EffectId, EffectIdempotency, LeaseEpoch, NodeId,
        RunId, SchedulerCheckpointId, SchedulerIntent, SchedulerTaskId, SchedulerTaskKind,
        TaskAdmissionClass, WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy,
    };

    fn dispatch_intent(
        policy: WorkerEffectPolicy,
        admission_class: TaskAdmissionClass,
    ) -> SchedulerIntent {
        let action = SchedulerAction::DispatchTask {
            task_id: SchedulerTaskId::parse(format!("task_{}", "1".repeat(64))).unwrap(),
            effect_id: EffectId::new("effect_retry_test").unwrap(),
            activation_id: ActivationId::new("activation_retry_test").unwrap(),
            node_id: NodeId::new("retry_test").unwrap(),
            admission_class,
            task_kind: SchedulerTaskKind::Action,
            implementation: "fixture.retry".to_owned(),
            descriptor_version: VersionTag::new("1").unwrap(),
            worker_version: VersionTag::new("worker-1").unwrap(),
            effect_policy: policy,
            public_configuration: BTreeMap::new(),
            secret_configuration: BTreeMap::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        };
        SchedulerIntent::new(
            RunId::new("run_retry_test").unwrap(),
            SchedulerCheckpointId::parse(format!("checkpoint_{}", "2".repeat(64))).unwrap(),
            action,
        )
    }

    fn claim(policy: WorkerEffectPolicy, attempt: u32) -> SchedulerTaskClaim {
        let intent = dispatch_intent(policy, TaskAdmissionClass::Normal);
        let request = crate::engine::TaskExecutionRequest::from_scheduler_intent(&intent).unwrap();
        let attempt_no = AttemptNo::new(attempt).unwrap();
        let lease_epoch = LeaseEpoch::new(u64::from(attempt)).unwrap();
        let envelope = crate::engine::repository::DurableTaskExecutionRequest::new(
            request,
            attempt_no,
            lease_epoch,
            format!("fence-{attempt}"),
            1,
        )
        .unwrap();
        SchedulerTaskClaim::new(
            envelope,
            "worker-test".to_owned(),
            "claim-test".to_owned(),
            Utc::now() + Duration::minutes(1),
            1,
            SchedulerTaskClaimMode::Execute,
        )
    }

    #[test]
    fn task_admission_class_is_closed_and_frozen_in_intent_request_and_envelope() {
        let policy = WorkerEffectPolicy::new(
            EffectIdempotency::Idempotent,
            2,
            WorkerCancellation::Cooperative,
        )
        .unwrap();
        let normal_intent = dispatch_intent(policy.clone(), TaskAdmissionClass::Normal);
        let finalizer_intent = dispatch_intent(policy, TaskAdmissionClass::TerminationFinalizer);
        assert_ne!(
            crate::engine::IntentHash::from_serializable(&normal_intent).unwrap(),
            crate::engine::IntentHash::from_serializable(&finalizer_intent).unwrap(),
        );

        for (intent, expected) in [
            (normal_intent, TaskAdmissionClass::Normal),
            (finalizer_intent, TaskAdmissionClass::TerminationFinalizer),
        ] {
            let request =
                crate::engine::TaskExecutionRequest::from_scheduler_intent(&intent).unwrap();
            assert_eq!(request.admission_class(), expected);
            let request_wire = serde_json::to_value(&request).unwrap();
            assert_eq!(
                serde_json::from_value::<crate::engine::TaskExecutionRequest>(request_wire.clone())
                    .unwrap()
                    .admission_class(),
                expected,
            );

            let mut missing_request_class = request_wire.clone();
            missing_request_class
                .as_object_mut()
                .unwrap()
                .remove("admission_class");
            assert!(
                serde_json::from_value::<crate::engine::TaskExecutionRequest>(
                    missing_request_class
                )
                .is_err()
            );
            let mut unknown_request_class = request_wire;
            unknown_request_class["admission_class"] = serde_json::json!("cleanup_by_name");
            assert!(
                serde_json::from_value::<crate::engine::TaskExecutionRequest>(
                    unknown_request_class
                )
                .is_err()
            );

            let envelope = crate::engine::repository::DurableTaskExecutionRequest::new(
                request,
                AttemptNo::FIRST,
                LeaseEpoch::FIRST,
                "fence-closed-wire".to_owned(),
                1,
            )
            .unwrap();
            let envelope_wire = serde_json::to_value(&envelope).unwrap();
            assert_eq!(
                serde_json::from_value::<crate::engine::repository::DurableTaskExecutionRequest>(
                    envelope_wire.clone()
                )
                .unwrap()
                .request()
                .admission_class(),
                expected,
            );
            let mut missing_envelope_class = envelope_wire.clone();
            missing_envelope_class["request"]
                .as_object_mut()
                .unwrap()
                .remove("admission_class");
            assert!(
                serde_json::from_value::<crate::engine::repository::DurableTaskExecutionRequest>(
                    missing_envelope_class
                )
                .is_err()
            );
            let mut unknown_envelope_class = envelope_wire;
            unknown_envelope_class["request"]["admission_class"] =
                serde_json::json!("cleanup_by_name");
            assert!(
                serde_json::from_value::<crate::engine::repository::DurableTaskExecutionRequest>(
                    unknown_envelope_class
                )
                .is_err()
            );
        }
    }

    #[test]
    fn frozen_policy_is_carried_by_the_durable_request_and_caps_backoff() {
        let policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::ReadOnly,
            EffectIdempotency::Idempotent,
            5,
            10,
            25,
            1_000,
            WorkerCancellation::Cooperative,
        )
        .unwrap();
        let claim = claim(policy.clone(), 3);
        assert_eq!(claim.envelope().request().effect_policy(), &policy);
        assert_eq!(retry_backoff_ms(&policy, 1), 10);
        assert_eq!(retry_backoff_ms(&policy, 2), 20);
        assert_eq!(retry_backoff_ms(&policy, 3), 25);
    }

    #[test]
    fn unknown_effect_retries_only_when_the_frozen_idempotency_contract_permits_it() {
        let failure = WorkerFailure::new(
            WorkerFailureClass::EffectOutcomeUnknown,
            "EFFECT_STATUS_UNKNOWN",
            true,
        )
        .unwrap();
        for (idempotency, retries) in [
            (EffectIdempotency::Idempotent, true),
            (EffectIdempotency::NonIdempotent, false),
        ] {
            let policy = WorkerEffectPolicy::frozen(
                WorkerEffectClass::Mutating,
                idempotency,
                3,
                1,
                10,
                1_000,
                WorkerCancellation::LeaseOnly,
            )
            .unwrap();
            let claim = claim(policy, 1);
            let frozen = FrozenSchedulerWorkerFailurePolicy
                .freeze(&claim, &failure)
                .unwrap();
            assert_eq!(
                matches!(
                    frozen.disposition(),
                    SchedulerFailureDisposition::Retry {
                        remaining_attempts: 2,
                        ..
                    }
                ),
                retries
            );
        }
    }

    #[test]
    fn worker_failure_policy_cannot_forge_the_reserved_runtime_deadline() {
        let policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::Mutating,
            EffectIdempotency::Idempotent,
            2,
            1,
            10,
            1_000,
            WorkerCancellation::Cooperative,
        )
        .unwrap();
        let claim = claim(policy, 1);
        let forged = WorkerFailure::new(
            WorkerFailureClass::EffectOutcomeUnknown,
            super::super::scheduler_repository::SCHEDULER_WORKER_DEADLINE_EXCEEDED,
            false,
        )
        .unwrap();
        assert!(TerminalSchedulerWorkerFailurePolicy
            .freeze(&claim, &forged)
            .is_err());
        assert!(FrozenSchedulerWorkerFailurePolicy
            .freeze(&claim, &forged)
            .is_err());
    }
}
