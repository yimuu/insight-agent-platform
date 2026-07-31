//! Production orchestration around the pure planner and durable scheduler
//! repository. Every crash hook is on the actual side-effect path.

use std::{
    cell::Cell,
    collections::BTreeMap,
    future::{pending, Future},
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Once,
    time::{Duration as StdDuration, Instant},
};

use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt;
#[cfg(test)]
use insight_durable::model_tool_parent_resume::adapter::{
    model_tool_parent_resume_activate_checkpointed, model_tool_parent_resume_ready_cancelled,
    model_tool_parent_resume_ready_continue, model_tool_parent_resume_ready_failed,
};
#[cfg(test)]
use insight_durable::model_tool_queue::adapter::{
    model_tool_batch_activation_new, model_tool_task_claim_new, model_tool_task_commit_receipt_new,
};
use insight_durable::scheduler_repository::adapter::SchedulerTaskFailureAdapter as _;
#[cfg(test)]
use insight_durable::scheduler_repository::adapter::{
    DurableTaskExecutionRequestAdapter as _, SchedulerCommitReceiptAdapter as _,
    SchedulerTaskClaimAdapter as _, SchedulerTaskCompletionReceiptAdapter as _,
};
use insight_durable::{
    FencedSchedulerRunCommand, ModelToolBatchActivation, ModelToolBatchActivationOutcome,
    ModelToolCallCheckpoint, ModelToolFailureClass, ModelToolParentResume, ModelToolTaskClaim,
    ModelToolTaskCommitReceipt, ModelToolTaskDisposition, ModelToolTaskHeartbeatOutcome,
    ModelToolTaskOutcome, ModelToolTaskTransitionOutcome, SchedulerCommitReceipt,
    SchedulerCrashInjector, SchedulerCrashPoint, SchedulerDurableRepository,
    SchedulerFailureDisposition, SchedulerTaskClaim, SchedulerTaskClaimMode,
    SchedulerTaskCommitOutcome, SchedulerTaskCompletionReceipt, SchedulerTaskFailure,
    SchedulerTaskHeartbeatOutcome, SchedulerTaskOutcome, SchedulerTaskSuccess,
    StageArtifactCommand, VerifyArtifactCommand,
};
use insight_engine::{
    artifact_store::WorkerArtifactStore,
    plan::{DescriptorValue, LinkedPlan},
    repository::{adapter as repository_adapter, RepositoryError, REPOSITORY_DATA_INVALID},
    run_stream::{
        LiveRunObservationIdentity, LiveRunStreamBroker, LiveRunStreamPayload,
        LiveRunStreamPublication, LiveRunStreamPublishOutcome, RunPublicError,
        RunToolPublicProjection,
    },
    worker::{
        TaskExecutionRequest, WorkerExecutionContext, WorkerExecutorRegistry, WorkerFailure,
        WorkerFailureClass, WorkerRuntimeServices, WORKER_MODEL_TOOL_CLAIM_INVALID,
    },
    EffectEvidence, PlannedSchedulerAction, SchedulerAction, SchedulerDecision, SchedulerPlanner,
    SchedulerQuiescence, SchedulerTaskKind, TransitionOutcome, ValueRef,
};
use tokio_util::sync::CancellationToken;

use insight_engine::worker::adapter::{
    self as worker_adapter, ModelCallPublicItemReservationError,
    ModelCallPublicItemReservationKind, ModelCallPublicItemReservationRequest,
};

const WORKER_EXECUTOR_PANICKED: &str = "WORKER_EXECUTOR_PANICKED";
const WORKER_LEASE_LOST: &str = "WORKER_LEASE_LOST";
const LLM_TOOL_EXECUTION_FAILED: &str = "LLM_TOOL_EXECUTION_FAILED";
const LLM_TOOL_EXECUTION_CANCELLED: &str = "LLM_TOOL_EXECUTION_CANCELLED";
const LLM_TOOL_EFFECT_OUTCOME_UNKNOWN: &str = "LLM_TOOL_EFFECT_OUTCOME_UNKNOWN";
const LLM_TOOL_ROUND_LIMIT: &str = "LLM_TOOL_ROUND_LIMIT";
const LLM_TOOL_CALL_LIMIT: &str = "LLM_TOOL_CALL_LIMIT";
const MODEL_TOOL_RESULT_INVALID: &str = "MODEL_TOOL_RESULT_INVALID";
const MODEL_TOOL_RESULT_SCHEMA_INVALID: &str = "MODEL_TOOL_RESULT_SCHEMA_INVALID";
const MODEL_TOOL_PUBLIC_RESULT_INVALID: &str = "MODEL_TOOL_PUBLIC_RESULT_INVALID";
const MODEL_TOOL_DEADLINE_EXCEEDED: &str = "WORKER_DEADLINE_EXCEEDED";
const ARTIFACT_STAGING_GRACE_SECONDS: i64 = 3_600;
const MODEL_TOOL_PROGRESS_QUEUE_CAPACITY: usize = 16;
const MODEL_TOOL_PROGRESS_BURST: u32 = 8;
const MODEL_TOOL_PROGRESS_WINDOW: StdDuration = StdDuration::from_secs(1);
const MODEL_TOOL_PROGRESS_TOTAL_LIMIT: u32 = 1_024;

fn repository_canonicalization() -> RepositoryError {
    repository_adapter::canonicalization()
}

fn repository_invalid_configuration() -> RepositoryError {
    repository_adapter::invalid_configuration()
}

fn repository_invalid_data() -> RepositoryError {
    repository_adapter::invalid_data()
}

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

/// Prepares non-authoritative side data needed by one scheduler action before
/// the repository attempts its atomic commit. Implementations may write only
/// replay-safe staging state; the repository transaction remains the sole
/// authority for consuming that state.
#[async_trait]
pub trait SchedulerActionPreparer: Send + Sync {
    async fn prepare_scheduler_action(
        &self,
        action: &PlannedSchedulerAction,
    ) -> Result<(), RepositoryError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSchedulerActionPreparer;

#[async_trait]
impl SchedulerActionPreparer for NoopSchedulerActionPreparer {
    async fn prepare_scheduler_action(
        &self,
        _action: &PlannedSchedulerAction,
    ) -> Result<(), RepositoryError> {
        Ok(())
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
                        .map_err(|_| repository_invalid_configuration())?,
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

fn retry_backoff_ms(policy: &insight_engine::WorkerEffectPolicy, attempt_no: u32) -> u64 {
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

#[derive(Debug, Clone, PartialEq)]
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
    SuspendedForTools {
        activation: ModelToolBatchActivation,
        exact_replay: bool,
    },
}

/// Minimal repository surface owned by the durable model-tool worker pump.
///
/// The blanket implementation keeps production on the single scheduler
/// repository authority. The narrow trait also makes the pump's fencing
/// contract testable without a mock for every unrelated scheduler method.
#[async_trait]
pub trait ModelToolWorkerRepository: Send + Sync {
    async fn claim_model_tool_tasks(
        &self,
        claimed_by: &str,
        claim_seconds: u32,
        limit: u32,
        max_claimed_per_run: u32,
    ) -> Result<Vec<ModelToolTaskClaim>, RepositoryError>;

    async fn mark_model_tool_task_started(
        &self,
        claim: &ModelToolTaskClaim,
    ) -> Result<ModelToolTaskTransitionOutcome<()>, RepositoryError>;

    async fn heartbeat_model_tool_task(
        &self,
        claim: &ModelToolTaskClaim,
        claim_seconds: u32,
    ) -> Result<ModelToolTaskHeartbeatOutcome, RepositoryError>;

    async fn commit_model_tool_task_outcome(
        &self,
        claim: &ModelToolTaskClaim,
        outcome: &ModelToolTaskOutcome,
    ) -> Result<ModelToolTaskTransitionOutcome<ModelToolTaskCommitReceipt>, RepositoryError>;
}

#[async_trait]
impl<T> ModelToolWorkerRepository for T
where
    T: SchedulerDurableRepository + ?Sized,
{
    async fn claim_model_tool_tasks(
        &self,
        claimed_by: &str,
        claim_seconds: u32,
        limit: u32,
        max_claimed_per_run: u32,
    ) -> Result<Vec<ModelToolTaskClaim>, RepositoryError> {
        SchedulerDurableRepository::claim_model_tool_calls(
            self,
            claimed_by,
            claim_seconds,
            limit,
            max_claimed_per_run,
        )
        .await
    }

    async fn mark_model_tool_task_started(
        &self,
        claim: &ModelToolTaskClaim,
    ) -> Result<ModelToolTaskTransitionOutcome<()>, RepositoryError> {
        SchedulerDurableRepository::mark_model_tool_call_started(self, claim).await
    }

    async fn heartbeat_model_tool_task(
        &self,
        claim: &ModelToolTaskClaim,
        claim_seconds: u32,
    ) -> Result<ModelToolTaskHeartbeatOutcome, RepositoryError> {
        SchedulerDurableRepository::heartbeat_model_tool_call(self, claim, claim_seconds).await
    }

    async fn commit_model_tool_task_outcome(
        &self,
        claim: &ModelToolTaskClaim,
        outcome: &ModelToolTaskOutcome,
    ) -> Result<ModelToolTaskTransitionOutcome<ModelToolTaskCommitReceipt>, RepositoryError> {
        SchedulerDurableRepository::commit_model_tool_call_outcome(self, claim, outcome).await
    }
}

/// Best-effort sink for already-authorized workflow tool observations.
/// Implementations must not treat publication loss as execution failure.
pub trait ModelToolLiveObserver: Send + Sync {
    fn publish_model_tool_observation(
        &self,
        publication: LiveRunStreamPublication,
    ) -> LiveRunStreamPublishOutcome;
}

/// Observer used by the compatibility pump API. It deliberately produces no
/// live side effects while preserving the exact durable execution path.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopModelToolLiveObserver;

impl ModelToolLiveObserver for NoopModelToolLiveObserver {
    fn publish_model_tool_observation(
        &self,
        _publication: LiveRunStreamPublication,
    ) -> LiveRunStreamPublishOutcome {
        LiveRunStreamPublishOutcome::NoSubscriber
    }
}

/// Best-effort sink for an already-authorized, durably committed first-class
/// Retrieval observation. Publication loss is never an execution failure.
pub trait SchedulerRetrievalLiveObserver: Send + Sync {
    fn publish_retrieval_observation(
        &self,
        publication: LiveRunStreamPublication,
    ) -> LiveRunStreamPublishOutcome;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSchedulerRetrievalLiveObserver;

impl SchedulerRetrievalLiveObserver for NoopSchedulerRetrievalLiveObserver {
    fn publish_retrieval_observation(
        &self,
        _publication: LiveRunStreamPublication,
    ) -> LiveRunStreamPublishOutcome {
        LiveRunStreamPublishOutcome::NoSubscriber
    }
}

impl<T> SchedulerRetrievalLiveObserver for T
where
    T: LiveRunStreamBroker + ?Sized,
{
    fn publish_retrieval_observation(
        &self,
        publication: LiveRunStreamPublication,
    ) -> LiveRunStreamPublishOutcome {
        self.publish(publication)
    }
}

impl<T> ModelToolLiveObserver for T
where
    T: LiveRunStreamBroker + ?Sized,
{
    fn publish_model_tool_observation(
        &self,
        publication: LiveRunStreamPublication,
    ) -> LiveRunStreamPublishOutcome {
        self.publish(publication)
    }
}

/// One body-free durable model-tool worker observation. Tool arguments,
/// results, safe-error bodies, and provider payloads are intentionally absent.
#[derive(Clone, PartialEq)]
pub enum ModelToolWorkerPumpOutcome {
    NoTask,
    Succeeded {
        receipt: ModelToolTaskCommitReceipt,
        exact_replay: bool,
    },
    RetryScheduled {
        receipt: ModelToolTaskCommitReceipt,
        exact_replay: bool,
    },
    Failed {
        receipt: ModelToolTaskCommitReceipt,
        exact_replay: bool,
    },
    Cancelled {
        receipt: ModelToolTaskCommitReceipt,
        exact_replay: bool,
    },
    LeaseLost {
        effect_evidence: EffectEvidence,
    },
    StateConflict {
        effect_evidence: EffectEvidence,
    },
    RunTerminal {
        effect_evidence: EffectEvidence,
    },
}

impl std::fmt::Debug for ModelToolWorkerPumpOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoTask => formatter.write_str("NoTask"),
            Self::Succeeded {
                receipt,
                exact_replay,
            } => debug_model_tool_receipt(formatter, "Succeeded", receipt, *exact_replay),
            Self::RetryScheduled {
                receipt,
                exact_replay,
            } => debug_model_tool_receipt(formatter, "RetryScheduled", receipt, *exact_replay),
            Self::Failed {
                receipt,
                exact_replay,
            } => debug_model_tool_receipt(formatter, "Failed", receipt, *exact_replay),
            Self::Cancelled {
                receipt,
                exact_replay,
            } => debug_model_tool_receipt(formatter, "Cancelled", receipt, *exact_replay),
            Self::LeaseLost { effect_evidence } => formatter
                .debug_struct("LeaseLost")
                .field("effect_evidence", effect_evidence)
                .finish(),
            Self::StateConflict { effect_evidence } => formatter
                .debug_struct("StateConflict")
                .field("effect_evidence", effect_evidence)
                .finish(),
            Self::RunTerminal { effect_evidence } => formatter
                .debug_struct("RunTerminal")
                .field("effect_evidence", effect_evidence)
                .finish(),
        }
    }
}

fn debug_model_tool_receipt(
    formatter: &mut std::fmt::Formatter<'_>,
    name: &str,
    receipt: &ModelToolTaskCommitReceipt,
    exact_replay: bool,
) -> std::fmt::Result {
    formatter
        .debug_struct(name)
        .field("tool_task_id", receipt.tool_task_id())
        .field("committed_attempt_no", &receipt.committed_attempt_no())
        .field("committed_lease_epoch", &receipt.committed_lease_epoch())
        .field("continuation_status", &receipt.continuation_status())
        .field("exact_replay", &exact_replay)
        .finish()
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
    drive_scheduler_once_with_preparer(
        repository,
        linked,
        fence,
        crash,
        &NoopSchedulerActionPreparer,
    )
    .await
}

async fn drive_scheduler_once_with_preparer<R, I, P>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    crash: &I,
    preparer: &P,
) -> Result<SchedulerDriveOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerActionPreparer + ?Sized,
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
                        .map_err(|_| repository_invalid_data())?,
                )),
            };
            (decision, has_checkpoints)
        }
        Err(error) if error.code() == REPOSITORY_DATA_INVALID => {
            let projection = repository
                .load_run(fence.run_id())
                .await?
                .ok_or_else(repository_invalid_data)?;
            if projection.lifecycle().is_terminal() {
                let quiescence = match projection.lifecycle() {
                    insight_engine::RunLifecycle::Succeeded => SchedulerQuiescence::RunSucceeded,
                    insight_engine::RunLifecycle::Cancelled => SchedulerQuiescence::RunCancelled,
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
                            insight_engine::SchedulerPlanningFailure::FactInconsistent,
                        )
                        .map_err(|_| repository_invalid_data())?,
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
    preparer.prepare_scheduler_action(&action).await?;
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
    drive_scheduler_until_quiescent_with_preparer(
        repository,
        linked,
        fence,
        crash,
        max_actions,
        &NoopSchedulerActionPreparer,
    )
    .await
}

pub async fn drive_scheduler_until_quiescent_with_preparer<R, I, P>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    crash: &I,
    max_actions: u32,
    preparer: &P,
) -> Result<SchedulerRecoveryOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerActionPreparer + ?Sized,
{
    if max_actions == 0 {
        return Err(repository_invalid_configuration());
    }
    for _ in 0..max_actions {
        match drive_scheduler_once_with_preparer(repository, linked, fence, crash, preparer).await?
        {
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
        &NoopSchedulerRetrievalLiveObserver,
        crash,
    )
    .await
}

/// Executes one generic scheduler task and publishes only a first-winner,
/// durably committed Retrieval public projection.
#[allow(clippy::too_many_arguments)]
pub async fn consume_scheduler_task_once_with_retrieval_observer<R, I, P, O>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    claimant: &str,
    claim_seconds: u32,
    max_claimed_per_run: u32,
    cancellation: CancellationToken,
    observer: &O,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerWorkerFailurePolicy,
    O: SchedulerRetrievalLiveObserver + ?Sized,
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
        observer,
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
        &NoopSchedulerRetrievalLiveObserver,
        crash,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn consume_scheduler_task_once_with_artifact_store_and_retrieval_observer<R, I, P, O>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    claimant: &str,
    claim_seconds: u32,
    max_claimed_per_run: u32,
    cancellation: CancellationToken,
    artifact_store: &dyn WorkerArtifactStore,
    observer: &O,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerWorkerFailurePolicy,
    O: SchedulerRetrievalLiveObserver + ?Sized,
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
        observer,
        crash,
    )
    .await
}

async fn receive_model_call_public_item_request(
    requests: &mut Option<tokio::sync::mpsc::Receiver<ModelCallPublicItemReservationRequest>>,
) -> Option<ModelCallPublicItemReservationRequest> {
    match requests.as_mut() {
        Some(requests) => requests.recv().await,
        None => std::future::pending().await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn consume_scheduler_task_once_inner<R, I, P, O>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    claimant: &str,
    claim_seconds: u32,
    max_claimed_per_run: u32,
    cancellation: CancellationToken,
    artifact_store: Option<&dyn WorkerArtifactStore>,
    retrieval_observer: &O,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerWorkerFailurePolicy,
    O: SchedulerRetrievalLiveObserver + ?Sized,
{
    let mut claims = repository
        .claim_scheduler_tasks_with_run_limit(claimant, claim_seconds, 1, max_claimed_per_run)
        .await?;
    let Some(claim) = claims.pop() else {
        return Ok(SchedulerWorkerPumpOutcome::NoTask);
    };
    consume_claimed_scheduler_task_with_artifact_store_and_retrieval_observer(
        repository,
        registry,
        failure_policy,
        claim_seconds,
        cancellation,
        artifact_store,
        retrieval_observer,
        crash,
        claim,
    )
    .await
}

/// Executes one scheduler task whose durable lease was acquired by a caller.
///
/// Separating discovery from execution lets the host coordinate one bounded
/// worker pool without making every idle execution slot poll the database.
/// The lease remains the sole execution authority, so the execution path is
/// otherwise identical to the compatibility pump APIs above.
#[allow(clippy::too_many_arguments)]
pub async fn consume_claimed_scheduler_task_with_artifact_store_and_retrieval_observer<R, I, P, O>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    claim_seconds: u32,
    cancellation: CancellationToken,
    artifact_store: Option<&dyn WorkerArtifactStore>,
    retrieval_observer: &O,
    crash: &I,
    claim: SchedulerTaskClaim,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerWorkerFailurePolicy,
    O: SchedulerRetrievalLiveObserver + ?Sized,
{
    consume_claimed_scheduler_task_with_artifact_store_retrieval_observer_and_runtime_services(
        repository,
        registry,
        failure_policy,
        claim_seconds,
        cancellation,
        artifact_store,
        retrieval_observer,
        crash,
        WorkerRuntimeServices::default(),
        claim,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn consume_claimed_scheduler_task_with_artifact_store_retrieval_observer_and_runtime_services<
    R,
    I,
    P,
    O,
>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    failure_policy: &P,
    claim_seconds: u32,
    cancellation: CancellationToken,
    artifact_store: Option<&dyn WorkerArtifactStore>,
    retrieval_observer: &O,
    crash: &I,
    base_runtime_services: WorkerRuntimeServices,
    mut claim: SchedulerTaskClaim,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    P: SchedulerWorkerFailurePolicy,
    O: SchedulerRetrievalLiveObserver + ?Sized,
{
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

    let request = claim.envelope().request().clone();
    let parent_resume = if model_tool_continuation_enabled(&request) {
        match repository.load_model_tool_parent_resume(&claim).await? {
            SchedulerTaskCommitOutcome::Committed { result }
            | SchedulerTaskCommitOutcome::ExactReplay {
                authoritative: result,
            } => result,
            SchedulerTaskCommitOutcome::OperationDeadlineElapsed => {
                return commit_runtime_deadline(repository, &claim, crash).await
            }
            SchedulerTaskCommitOutcome::StaleLease => {
                return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                    effect_evidence: EffectEvidence::Unknown,
                })
            }
            SchedulerTaskCommitOutcome::StateConflict => return Err(repository_invalid_data()),
        }
    } else {
        None
    };

    // Initial execution starts one conservative local budget before
    // mark-start. A continuation instead restores the original durable
    // Attempt deadline; reclaiming a parent task must never buy another full
    // timeout after tools have been running or waiting.
    let budget_started_at = tokio::time::Instant::now();
    let operation_deadline = if let Some(resume) = &parent_resume {
        resume.operation_deadline()
    } else {
        let timeout_delta = chrono::Duration::milliseconds(
            i64::try_from(request.effect_policy().timeout_ms())
                .map_err(|_| repository_invalid_configuration())?,
        );
        Utc::now()
            .checked_add_signed(timeout_delta)
            .ok_or_else(repository_invalid_configuration)?
    };
    let remaining = operation_deadline
        .signed_duration_since(Utc::now())
        .to_std()
        .unwrap_or(std::time::Duration::ZERO);
    let local_deadline = budget_started_at
        .checked_add(remaining)
        .ok_or_else(repository_invalid_configuration)?;
    let mut deadline = Box::pin(tokio::time::sleep_until(local_deadline));

    if parent_resume.is_none() {
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
    if let Some(resume) = &parent_resume {
        if let Some(model_call_no) = resume.checkpointed_model_call_no() {
            return match activate_model_tool_handoff(repository, &claim, model_call_no, crash)
                .await?
            {
                ModelToolActivationResolution::Suspended(outcome) => Ok(outcome),
                ModelToolActivationResolution::LeaseLost => {
                    Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                        effect_evidence: EffectEvidence::Started,
                    })
                }
                ModelToolActivationResolution::Failed(failure) => {
                    let outcome =
                        SchedulerTaskOutcome::Failed(failure_policy.freeze(&claim, &failure)?);
                    commit_worker_outcome(repository, &claim, outcome, Some(claim_seconds), crash)
                        .await
                }
            };
        }
        if let Some(failure) = parent_resume_failure(resume) {
            let outcome = SchedulerTaskOutcome::Failed(failure_policy.freeze(&claim, &failure)?);
            return commit_worker_outcome(repository, &claim, outcome, Some(claim_seconds), crash)
                .await;
        }
    }
    let model_call = if request.task_kind() == SchedulerTaskKind::Llm
        && request.implementation() == "core.llm"
    {
        let publish = match request.public_configuration().get("publish") {
            Some(DescriptorValue::Boolean(value)) => *value,
            _ => return Err(repository_invalid_data()),
        };
        let model_call_no = parent_resume
            .as_ref()
            .and_then(ModelToolParentResume::next_model_call_no)
            .unwrap_or(1);
        match repository
            .reserve_model_call(&claim, model_call_no, publish)
            .await?
        {
            SchedulerTaskCommitOutcome::Committed { result }
            | SchedulerTaskCommitOutcome::ExactReplay {
                authoritative: result,
            } => Some(result),
            SchedulerTaskCommitOutcome::OperationDeadlineElapsed => {
                return commit_runtime_deadline(repository, &claim, crash).await
            }
            SchedulerTaskCommitOutcome::StaleLease => {
                return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                    effect_evidence: EffectEvidence::Started,
                })
            }
            SchedulerTaskCommitOutcome::StateConflict => return Err(repository_invalid_data()),
        }
    } else {
        None
    };
    let (runtime_services, mut public_item_requests) = match model_call.as_ref() {
        Some(authority) if authority.publication_enabled() && authority.public_item().is_none() => {
            let (allocator, requests) =
                worker_adapter::model_call_public_item_reservation_channel();
            (
                worker_adapter::services_with_model_call_public_item_allocator(
                    base_runtime_services,
                    allocator,
                ),
                Some(requests),
            )
        }
        _ => (base_runtime_services, None),
    };
    let public_item_model_call_no = model_call
        .as_ref()
        .map(insight_engine::worker::ModelCallAuthority::model_call_no);
    let mut context = WorkerExecutionContext::new(
        claim.envelope().attempt_no(),
        claim.envelope().lease_epoch(),
        claim.envelope().fencing_token(),
        operation_deadline,
    )
    .map_err(|_| repository_invalid_data())?;
    if let Some(model_call) = model_call {
        context = context.with_model_call(model_call);
    }
    if let Some(resume) = &parent_resume {
        context = context
            .with_model_continuation(resume.turns().to_vec())
            .map_err(|_| repository_invalid_data())?;
    }
    crash.hit(SchedulerCrashPoint::BeforeExternalCall)?;
    let task_cancellation = cancellation.child_token();
    // An executor is an extension boundary. A panic must not unwind the
    // worker pump or leave a started effect without a durable outcome. We do
    // not expose the panic payload because it may contain provider data or a
    // secret. Since the effect was marked started immediately above, its
    // outcome is conservatively unknown and is never retried unless a future
    // policy explicitly proves that safe.
    let mut worker = Box::pin(
        AssertUnwindSafe(redact_executor_panic_payload(
            worker_adapter::execute_with_runtime_services(
                registry,
                &context,
                &request,
                &runtime_services,
                task_cancellation.clone(),
            ),
        ))
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
            reservation = receive_model_call_public_item_request(&mut public_item_requests),
                if public_item_requests.is_some() => {
                let Some(reservation) = reservation else {
                    public_item_requests = None;
                    continue;
                };
                let model_call_no = public_item_model_call_no
                    .ok_or_else(repository_invalid_data)?;
                let reservation_kind = worker_adapter::reservation_kind(&reservation).clone();
                let reservation_outcome = match reservation_kind {
                    ModelCallPublicItemReservationKind::Message => repository
                        .reserve_model_call_public_item(&claim, model_call_no)
                        .await?,
                    ModelCallPublicItemReservationKind::FunctionCall {
                        call_index,
                        call_id,
                        tool_name,
                    } => repository
                        .reserve_model_call_public_function_item(
                            &claim,
                            model_call_no,
                            call_index,
                            &call_id,
                            &tool_name,
                        )
                        .await?,
                };
                match reservation_outcome {
                    SchedulerTaskCommitOutcome::Committed { result }
                    | SchedulerTaskCommitOutcome::ExactReplay {
                        authoritative: result,
                    } => {
                        worker_adapter::respond_reservation(reservation, Ok(result));
                    }
                    SchedulerTaskCommitOutcome::OperationDeadlineElapsed => {
                        worker_adapter::respond_reservation(
                            reservation,
                            Err(ModelCallPublicItemReservationError::OperationDeadlineElapsed),
                        );
                        task_cancellation.cancel();
                        deadline_authority_observed = true;
                        break None;
                    }
                    SchedulerTaskCommitOutcome::StaleLease => {
                        worker_adapter::respond_reservation(
                            reservation,
                            Err(ModelCallPublicItemReservationError::StaleLease),
                        );
                        task_cancellation.cancel();
                        return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                            effect_evidence: EffectEvidence::Started,
                        });
                    }
                    SchedulerTaskCommitOutcome::StateConflict => {
                        worker_adapter::respond_reservation(
                            reservation,
                            Err(ModelCallPublicItemReservationError::StateConflict),
                        );
                        task_cancellation.cancel();
                        return Err(repository_invalid_data());
                    }
                }
            }
        }
    };
    crash.hit(SchedulerCrashPoint::AfterExternalCall)?;
    let model_checkpoint = match worker_result.as_ref() {
        Some(Ok(result)) => match (result.model_call(), result.model_tool_call_batch()) {
            (Some(completion), Some(batch)) => {
                let checkpoint = ModelToolCallCheckpoint::new(completion.clone(), batch.clone())
                    .map_err(|_| repository_invalid_data())?;
                Some(
                    repository
                        .checkpoint_model_tool_call_batch(&claim, &checkpoint)
                        .await?,
                )
            }
            (Some(completion), None) => Some(
                repository
                    .checkpoint_model_call_completion(&claim, completion)
                    .await?,
            ),
            (None, Some(_)) => return Err(repository_invalid_data()),
            (None, None) => None,
        },
        Some(Err(failure)) => match failure.model_call() {
            Some(completion) => Some(
                repository
                    .checkpoint_model_call_completion(&claim, completion)
                    .await?,
            ),
            None => None,
        },
        None => None,
    };
    if let Some(model_checkpoint) = model_checkpoint {
        match model_checkpoint {
            SchedulerTaskCommitOutcome::Committed { .. }
            | SchedulerTaskCommitOutcome::ExactReplay { .. } => {}
            SchedulerTaskCommitOutcome::OperationDeadlineElapsed => {
                return commit_runtime_deadline(repository, &claim, crash).await
            }
            SchedulerTaskCommitOutcome::StaleLease => {
                return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                    effect_evidence: worker_result_effect_evidence(&worker_result),
                })
            }
            SchedulerTaskCommitOutcome::StateConflict => return Err(repository_invalid_data()),
        }
    }
    let pending_outcome = match worker_result {
        // The complete Provider batch is durable at this point. Renew the
        // exact parent claim, then atomically activate all frozen Action tasks
        // and leave the already-running parent Attempt suspended behind the
        // durable all-settled barrier.
        Some(Ok(result)) if result.model_tool_call_batch().is_some() => {
            let model_call_no = result
                .model_tool_call_batch()
                .map(insight_engine::worker::ModelToolCallBatch::model_call_no)
                .ok_or_else(repository_invalid_data)?;
            if deadline.is_elapsed() {
                return await_and_commit_runtime_deadline(
                    repository,
                    &mut claim,
                    claim_seconds,
                    crash,
                )
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
                        effect_evidence: EffectEvidence::Committed,
                    })
                }
            }
            match activate_model_tool_handoff(repository, &claim, model_call_no, crash).await? {
                ModelToolActivationResolution::Suspended(outcome) => return Ok(outcome),
                ModelToolActivationResolution::LeaseLost => {
                    return Ok(SchedulerWorkerPumpOutcome::LeaseLost {
                        effect_evidence: EffectEvidence::Committed,
                    })
                }
                ModelToolActivationResolution::Failed(failure) => {
                    PendingWorkerOutcome::Failed(failure)
                }
            }
        }
        Some(Ok(result)) => {
            let result = result.without_model_call();
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
            PendingWorkerOutcome::Succeeded(Box::new(success))
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
        PendingWorkerOutcome::Succeeded(success) => SchedulerTaskOutcome::Succeeded(*success),
        PendingWorkerOutcome::Failed(failure) => {
            SchedulerTaskOutcome::Failed(failure_policy.freeze(&claim, &failure)?)
        }
    };
    commit_worker_outcome_with_retrieval_observer(
        repository,
        &claim,
        outcome,
        Some(claim_seconds),
        Some(retrieval_observer),
        crash,
    )
    .await
}

fn model_tool_continuation_enabled(request: &TaskExecutionRequest) -> bool {
    request.task_kind() == SchedulerTaskKind::Llm
        && request.implementation() == "core.llm"
        && request
            .deployment_binding()
            .get("tools")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|tools| !tools.is_empty())
}

fn parent_resume_failure(resume: &ModelToolParentResume) -> Option<WorkerFailure> {
    match resume {
        ModelToolParentResume::ReadyFailed {
            effect_outcome_unknown,
            ..
        } => Some(
            WorkerFailure::new(
                if *effect_outcome_unknown {
                    WorkerFailureClass::EffectOutcomeUnknown
                } else {
                    WorkerFailureClass::InfrastructureFailure
                },
                if *effect_outcome_unknown {
                    LLM_TOOL_EFFECT_OUTCOME_UNKNOWN
                } else {
                    LLM_TOOL_EXECUTION_FAILED
                },
                false,
            )
            .expect("the closed parent tool failure is valid"),
        ),
        ModelToolParentResume::ReadyCancelled { .. } => Some(
            WorkerFailure::new(
                WorkerFailureClass::ControlTermination,
                LLM_TOOL_EXECUTION_CANCELLED,
                false,
            )
            .expect("the closed parent tool cancellation is valid"),
        ),
        ModelToolParentResume::ActivateCheckpointed { .. }
        | ModelToolParentResume::ReadyContinue { .. } => None,
    }
}

enum ModelToolActivationResolution {
    Suspended(SchedulerWorkerPumpOutcome),
    Failed(WorkerFailure),
    LeaseLost,
}

async fn activate_model_tool_handoff<R, I>(
    repository: &R,
    claim: &SchedulerTaskClaim,
    model_call_no: u32,
    crash: &I,
) -> Result<ModelToolActivationResolution, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
{
    crash.hit(SchedulerCrashPoint::BeforeDownstreamAdmission)?;
    let activation = repository
        .activate_model_tool_call_batch(claim, model_call_no)
        .await?;
    crash.hit(SchedulerCrashPoint::AfterDownstreamAdmission)?;
    Ok(match activation {
        ModelToolBatchActivationOutcome::Activated(activation) => {
            ModelToolActivationResolution::Suspended(
                SchedulerWorkerPumpOutcome::SuspendedForTools {
                    activation,
                    exact_replay: false,
                },
            )
        }
        ModelToolBatchActivationOutcome::ExactReplay(activation) => {
            ModelToolActivationResolution::Suspended(
                SchedulerWorkerPumpOutcome::SuspendedForTools {
                    activation,
                    exact_replay: true,
                },
            )
        }
        ModelToolBatchActivationOutcome::StaleParentLease
        | ModelToolBatchActivationOutcome::RunTerminal => ModelToolActivationResolution::LeaseLost,
        ModelToolBatchActivationOutcome::StateConflict => return Err(repository_invalid_data()),
        ModelToolBatchActivationOutcome::RoundLimitExceeded => {
            ModelToolActivationResolution::Failed(
                WorkerFailure::new(
                    WorkerFailureClass::InfrastructureFailure,
                    LLM_TOOL_ROUND_LIMIT,
                    false,
                )
                .expect("the closed model-tool round-limit failure is valid"),
            )
        }
        ModelToolBatchActivationOutcome::CallLimitExceeded => {
            ModelToolActivationResolution::Failed(
                WorkerFailure::new(
                    WorkerFailureClass::InfrastructureFailure,
                    LLM_TOOL_CALL_LIMIT,
                    false,
                )
                .expect("the closed model-tool call-limit failure is valid"),
            )
        }
    })
}

/// Claims and executes at most one durable model-tool Action. This pump owns
/// only the child tool task: it never runs or schedules the parent LLM
/// continuation. The queue barrier remains the sole continuation authority.
#[allow(clippy::too_many_arguments)]
pub async fn consume_model_tool_task_once<R, I>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    claimant: &str,
    claim_seconds: u32,
    max_claimed_per_run: u32,
    cancellation: CancellationToken,
    crash: &I,
) -> Result<ModelToolWorkerPumpOutcome, RepositoryError>
where
    R: ModelToolWorkerRepository + ?Sized,
    I: SchedulerCrashInjector,
{
    consume_model_tool_task_once_with_observer(
        repository,
        registry,
        claimant,
        claim_seconds,
        max_claimed_per_run,
        cancellation,
        &NoopModelToolLiveObserver,
        crash,
    )
    .await
}

/// Executes one durable model-tool task while publishing only projections
/// authorized by the claim's frozen effective public policy. Observation is
/// live-only and cannot change any durable execution result.
#[allow(clippy::too_many_arguments)]
pub async fn consume_model_tool_task_once_with_observer<R, I, O>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    claimant: &str,
    claim_seconds: u32,
    max_claimed_per_run: u32,
    cancellation: CancellationToken,
    observer: &O,
    crash: &I,
) -> Result<ModelToolWorkerPumpOutcome, RepositoryError>
where
    R: ModelToolWorkerRepository + ?Sized,
    I: SchedulerCrashInjector,
    O: ModelToolLiveObserver + ?Sized,
{
    let mut claims = repository
        .claim_model_tool_tasks(claimant, claim_seconds, 1, max_claimed_per_run)
        .await?;
    let Some(claim) = claims.pop() else {
        return Ok(ModelToolWorkerPumpOutcome::NoTask);
    };
    consume_claimed_model_tool_task_with_observer(
        repository,
        registry,
        claim_seconds,
        cancellation,
        observer,
        crash,
        claim,
    )
    .await
}

/// Executes one model-tool task whose durable lease was acquired by a caller.
///
/// Hosts may use this entry point to share one coordinated concurrency budget
/// between ordinary scheduler work and model-tool work.
#[allow(clippy::too_many_arguments)]
pub async fn consume_claimed_model_tool_task_with_observer<R, I, O>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    claim_seconds: u32,
    cancellation: CancellationToken,
    observer: &O,
    crash: &I,
    claim: ModelToolTaskClaim,
) -> Result<ModelToolWorkerPumpOutcome, RepositoryError>
where
    R: ModelToolWorkerRepository + ?Sized,
    I: SchedulerCrashInjector,
    O: ModelToolLiveObserver + ?Sized,
{
    consume_claimed_model_tool_task_with_observer_and_runtime_services(
        repository,
        registry,
        claim_seconds,
        cancellation,
        observer,
        crash,
        WorkerRuntimeServices::default(),
        claim,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn consume_claimed_model_tool_task_with_observer_and_runtime_services<R, I, O>(
    repository: &R,
    registry: &WorkerExecutorRegistry,
    claim_seconds: u32,
    cancellation: CancellationToken,
    observer: &O,
    crash: &I,
    base_runtime_services: WorkerRuntimeServices,
    mut claim: ModelToolTaskClaim,
) -> Result<ModelToolWorkerPumpOutcome, RepositoryError>
where
    R: ModelToolWorkerRepository + ?Sized,
    I: SchedulerCrashInjector,
    O: ModelToolLiveObserver + ?Sized,
{
    // Freeze the complete timeout budget before mark-start. Repository and
    // heartbeat latency cannot buy extra time in which the Action may run.
    let timeout_ms = claim.identity().action().effect_policy().timeout_ms();
    let timeout_duration = std::time::Duration::from_millis(timeout_ms);
    let budget_started_at = tokio::time::Instant::now();
    let operation_deadline = Utc::now()
        .checked_add_signed(chrono::Duration::milliseconds(
            i64::try_from(timeout_ms).map_err(|_| repository_invalid_configuration())?,
        ))
        .ok_or_else(repository_invalid_configuration)?;
    let local_deadline = budget_started_at
        .checked_add(timeout_duration)
        .ok_or_else(repository_invalid_configuration)?;
    let mut deadline = Box::pin(tokio::time::sleep_until(local_deadline));

    match repository.mark_model_tool_task_started(&claim).await? {
        ModelToolTaskTransitionOutcome::Committed(())
        | ModelToolTaskTransitionOutcome::ExactReplay(()) => {}
        ModelToolTaskTransitionOutcome::StaleLease => {
            return Ok(ModelToolWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::NotStarted,
            })
        }
        ModelToolTaskTransitionOutcome::StateConflict => {
            return Ok(ModelToolWorkerPumpOutcome::StateConflict {
                effect_evidence: EffectEvidence::NotStarted,
            })
        }
        ModelToolTaskTransitionOutcome::RunTerminal => {
            return Ok(ModelToolWorkerPumpOutcome::RunTerminal {
                effect_evidence: EffectEvidence::NotStarted,
            })
        }
    }

    let heartbeat_period_ms = (u64::from(claim_seconds).saturating_mul(1_000) / 3).max(1);
    let mut heartbeat =
        tokio::time::interval(std::time::Duration::from_millis(heartbeat_period_ms));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    match repository
        .heartbeat_model_tool_task(&claim, claim_seconds)
        .await?
    {
        ModelToolTaskHeartbeatOutcome::Renewed(renewed) => claim = *renewed,
        ModelToolTaskHeartbeatOutcome::StaleLease => {
            return Ok(ModelToolWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::Unknown,
            })
        }
        ModelToolTaskHeartbeatOutcome::StateConflict => {
            return Ok(ModelToolWorkerPumpOutcome::StateConflict {
                effect_evidence: EffectEvidence::Started,
            })
        }
        ModelToolTaskHeartbeatOutcome::RunTerminal => {
            return Ok(ModelToolWorkerPumpOutcome::RunTerminal {
                effect_evidence: EffectEvidence::Started,
            })
        }
    }

    // Process shutdown is not a workflow cancellation intent. Stop renewing
    // the started claim and let durable lease-loss recovery decide its fate.
    if cancellation.is_cancelled() {
        return Ok(ModelToolWorkerPumpOutcome::LeaseLost {
            effect_evidence: EffectEvidence::Unknown,
        });
    }

    let request = match TaskExecutionRequest::from_model_tool_claim(&claim) {
        Ok(request) => request,
        Err(_) => {
            let outcome = model_tool_invariant_failure(
                WORKER_MODEL_TOOL_CLAIM_INVALID,
                EffectEvidence::Started,
            )?;
            return commit_model_tool_outcome(
                repository,
                &mut claim,
                claim_seconds,
                outcome,
                local_deadline,
                false,
                crash,
            )
            .await
            .map(ModelToolCommitCompletion::into_pump_outcome);
        }
    };
    let context = match WorkerExecutionContext::new(
        claim.tool_attempt_no(),
        claim.lease_epoch(),
        claim.fencing_token(),
        operation_deadline,
    ) {
        Ok(context) => context,
        Err(_) => {
            let outcome = model_tool_invariant_failure(
                WORKER_MODEL_TOOL_CLAIM_INVALID,
                EffectEvidence::Started,
            )?;
            return commit_model_tool_outcome(
                repository,
                &mut claim,
                claim_seconds,
                outcome,
                local_deadline,
                false,
                crash,
            )
            .await
            .map(ModelToolCommitCompletion::into_pump_outcome);
        }
    };

    if deadline.is_elapsed() {
        let outcome =
            ModelToolTaskOutcome::cancelled(MODEL_TOOL_DEADLINE_EXCEEDED, EffectEvidence::Started)
                .map_err(|_| repository_invalid_data())?;
        return commit_model_tool_outcome(
            repository,
            &mut claim,
            claim_seconds,
            outcome,
            local_deadline,
            false,
            crash,
        )
        .await
        .map(ModelToolCommitCompletion::into_pump_outcome);
    }

    crash.hit(SchedulerCrashPoint::BeforeExternalCall)?;
    let mut live_publication = ModelToolLivePublication::from_validated_claim(observer, &claim);
    // This is deliberately the last step before the registry Action boundary.
    // It cannot be delayed until the first heartbeat or terminal result.
    live_publication.started();
    let (runtime_services, mut progress_requests) =
        live_publication.runtime_services(base_runtime_services);
    // Keep shutdown authority separate from the executor token. A child token
    // would auto-cancel before this select can classify the cause, allowing a
    // cooperative executor's ControlTermination to be mistaken for a durable
    // business cancellation. Only the winning runtime branch cancels it.
    let task_cancellation = CancellationToken::new();
    let mut worker = Box::pin(
        AssertUnwindSafe(redact_executor_panic_payload(
            worker_adapter::execute_with_runtime_services(
                registry,
                &context,
                &request,
                &runtime_services,
                task_cancellation.clone(),
            ),
        ))
        .catch_unwind(),
    );
    let execution = loop {
        tokio::select! {
            biased;
            _ = deadline.as_mut() => {
                task_cancellation.cancel();
                break ModelToolExecutionExit::Deadline;
            }
            result = &mut worker => {
                let result = match result {
                    Ok(result) => result,
                    Err(_) => Err(WorkerFailure::new(
                        WorkerFailureClass::EffectOutcomeUnknown,
                        WORKER_EXECUTOR_PANICKED,
                        false,
                    ).expect("the closed executor-panic failure is valid")),
                };
                break ModelToolExecutionExit::Worker(Box::new(result));
            }
            _ = cancellation.cancelled() => {
                task_cancellation.cancel();
                break ModelToolExecutionExit::Shutdown;
            }
            progress = receive_model_tool_progress(&mut progress_requests) => {
                if let Some(progress) = progress {
                    let result = live_publication.progress(progress.value());
                    progress.respond(result);
                }
            }
            _ = heartbeat.tick() => {
                match repository
                    .heartbeat_model_tool_task(&claim, claim_seconds)
                    .await?
                {
                    ModelToolTaskHeartbeatOutcome::Renewed(renewed) => claim = *renewed,
                    ModelToolTaskHeartbeatOutcome::StaleLease => {
                        task_cancellation.cancel();
                        break ModelToolExecutionExit::StaleLease;
                    }
                    ModelToolTaskHeartbeatOutcome::StateConflict => {
                        task_cancellation.cancel();
                        break ModelToolExecutionExit::StateConflict;
                    }
                    ModelToolTaskHeartbeatOutcome::RunTerminal => {
                        task_cancellation.cancel();
                        break ModelToolExecutionExit::RunTerminal;
                    }
                }
            }
        }
    };
    crash.hit(SchedulerCrashPoint::AfterExternalCall)?;

    let outcome = match execution {
        ModelToolExecutionExit::Shutdown | ModelToolExecutionExit::StaleLease => {
            return Ok(ModelToolWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::Unknown,
            })
        }
        ModelToolExecutionExit::StateConflict => {
            return Ok(ModelToolWorkerPumpOutcome::StateConflict {
                effect_evidence: EffectEvidence::Unknown,
            })
        }
        ModelToolExecutionExit::RunTerminal => {
            return Ok(ModelToolWorkerPumpOutcome::RunTerminal {
                effect_evidence: EffectEvidence::Unknown,
            })
        }
        ModelToolExecutionExit::Deadline => {
            ModelToolTaskOutcome::cancelled(MODEL_TOOL_DEADLINE_EXCEEDED, EffectEvidence::Unknown)
                .map_err(|_| repository_invalid_data())?
        }
        ModelToolExecutionExit::Worker(result) => match *result {
            Err(failure) => model_tool_outcome_from_worker_failure(&failure)?,
            Ok(result) => model_tool_outcome_from_worker_success(&claim, &result)?,
        },
    };

    let completion = commit_model_tool_outcome(
        repository,
        &mut claim,
        claim_seconds,
        outcome,
        local_deadline,
        true,
        crash,
    )
    .await?;
    live_publication.finish(&completion);
    Ok(completion.into_pump_outcome())
}

enum ModelToolExecutionExit {
    Worker(Box<Result<insight_engine::worker::TaskExecutionResult, WorkerFailure>>),
    Deadline,
    Shutdown,
    StaleLease,
    StateConflict,
    RunTerminal,
}

struct ModelToolLivePublication<'a, O: ?Sized> {
    observer: &'a O,
    state: Option<ModelToolLivePublicationState>,
}

struct ModelToolLivePublicationState {
    projection: RunToolPublicProjection,
    identity: LiveRunObservationIdentity,
    call_id: String,
    tool_name: String,
    started_arguments: Option<serde_json::Value>,
    next_local_sequence: u64,
    progress_window_started_at: Instant,
    progress_in_window: u32,
    progress_total: u32,
}

impl<'a, O> ModelToolLivePublication<'a, O>
where
    O: ModelToolLiveObserver + ?Sized,
{
    /// Builds only from the frozen effective policy and the complete argument
    /// object which already passed the synthetic Action request validator.
    /// Any projection corruption disables observation without affecting work.
    fn from_validated_claim(observer: &'a O, claim: &ModelToolTaskClaim) -> Self {
        let state = (|| {
            let projection = RunToolPublicProjection::from_frozen_effective_policy(
                claim.identity().action().effective_public_policy(),
            )
            .ok()?;
            if !projection.call_authorized() {
                return None;
            }
            let completed_arguments = projection
                .project_validated_completed_arguments(claim.arguments())
                .ok()?;
            let started_arguments = completed_arguments.workflow_started_arguments().cloned();
            let identity = LiveRunObservationIdentity::new(
                claim.run_id().clone(),
                claim.parent_activation_id().clone(),
                claim.tool_attempt_no(),
                claim.identity().tool_task_id().as_str(),
            )
            .ok()?;
            Some(ModelToolLivePublicationState {
                projection,
                identity,
                call_id: claim.identity().call_id().to_owned(),
                tool_name: claim.identity().action().name().to_owned(),
                started_arguments,
                next_local_sequence: 0,
                progress_window_started_at: Instant::now(),
                progress_in_window: 0,
                progress_total: 0,
            })
        })();
        Self { observer, state }
    }

    fn started(&mut self) {
        let Some(state) = &self.state else {
            return;
        };
        let payload = LiveRunStreamPayload::ToolStarted {
            call_id: state.call_id.clone(),
            tool_name: state.tool_name.clone(),
            arguments: state.started_arguments.clone(),
        };
        self.emit(payload);
    }

    fn runtime_services(
        &self,
        services: WorkerRuntimeServices,
    ) -> (
        WorkerRuntimeServices,
        Option<worker_adapter::ModelToolProgressRequests>,
    ) {
        let Some(state) = &self.state else {
            return (services, None);
        };
        if !state.projection.progress_authorized() {
            return (services, None);
        }
        let (publisher, requests) =
            worker_adapter::model_tool_progress_channel(MODEL_TOOL_PROGRESS_QUEUE_CAPACITY);
        (
            worker_adapter::services_with_model_tool_progress_publisher(services, publisher),
            Some(requests),
        )
    }

    fn progress(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<worker_adapter::ModelToolProgressDisposition, worker_adapter::ModelToolProgressError>
    {
        let Some(state) = &mut self.state else {
            return Ok(worker_adapter::ModelToolProgressDisposition::Dropped);
        };
        let content = state
            .projection
            .project_validated_progress(value)
            .map_err(|_| worker_adapter::invalid_model_tool_progress())?
            .ok_or_else(worker_adapter::invalid_model_tool_progress)?;
        let now = Instant::now();
        if now.duration_since(state.progress_window_started_at) >= MODEL_TOOL_PROGRESS_WINDOW {
            state.progress_window_started_at = now;
            state.progress_in_window = 0;
        }
        if state.progress_total >= MODEL_TOOL_PROGRESS_TOTAL_LIMIT
            || state.progress_in_window >= MODEL_TOOL_PROGRESS_BURST
        {
            state.next_local_sequence = state.next_local_sequence.saturating_add(1);
            return Ok(worker_adapter::ModelToolProgressDisposition::Dropped);
        }
        state.progress_total = state.progress_total.saturating_add(1);
        state.progress_in_window = state.progress_in_window.saturating_add(1);
        let payload = LiveRunStreamPayload::ToolProgress {
            call_id: state.call_id.clone(),
            tool_name: state.tool_name.clone(),
            content,
        };
        let Ok(publication) = LiveRunStreamPublication::new_run_observation(
            state.identity.clone(),
            state.next_local_sequence,
            payload,
        ) else {
            return Ok(worker_adapter::ModelToolProgressDisposition::Dropped);
        };
        state.next_local_sequence = state.next_local_sequence.saturating_add(1);
        let outcome = self.observer.publish_model_tool_observation(publication);
        Ok(
            if matches!(
                outcome,
                LiveRunStreamPublishOutcome::Enqueued
                    | LiveRunStreamPublishOutcome::EnqueuedAfterGap
                    | LiveRunStreamPublishOutcome::EnqueuedAfterBestEffortLoss
            ) {
                worker_adapter::ModelToolProgressDisposition::Published
            } else {
                worker_adapter::ModelToolProgressDisposition::Dropped
            },
        )
    }

    fn finish(&mut self, completion: &ModelToolCommitCompletion) {
        let Some(intent) = completion.committed_intent.as_ref() else {
            return;
        };
        let Some(duration_ms) = completion.duration_ms else {
            return;
        };
        match (&completion.pump_outcome, intent) {
            (
                ModelToolWorkerPumpOutcome::Succeeded { .. },
                ModelToolTaskOutcome::Succeeded { result },
            ) => self.completed(result, duration_ms),
            (ModelToolWorkerPumpOutcome::Failed { .. }, ModelToolTaskOutcome::Failed { .. })
            | (
                ModelToolWorkerPumpOutcome::Cancelled { .. },
                ModelToolTaskOutcome::Cancelled { .. },
            ) => self.failed(intent, duration_ms),
            // A retry is not the terminal failure of this durable tool call.
            // The next Attempt receives its own observation identity and seq 0.
            _ => {}
        }
    }

    fn completed(&mut self, result: &serde_json::Value, duration_ms: u64) {
        let Some(state) = &self.state else {
            return;
        };
        let content = match state.projection.project_validated_completed_result(
            &state.call_id,
            &state.tool_name,
            result,
        ) {
            Ok(Some(projected)) => projected.content().to_vec(),
            Ok(None) => Vec::new(),
            Err(_) => return,
        };
        let payload = LiveRunStreamPayload::ToolCompleted {
            call_id: state.call_id.clone(),
            tool_name: state.tool_name.clone(),
            content,
            duration_ms,
        };
        self.emit(payload);
    }

    fn failed(&mut self, intent: &ModelToolTaskOutcome, duration_ms: u64) {
        let Some(state) = &self.state else {
            return;
        };
        let Some(error) = model_tool_public_error(intent) else {
            return;
        };
        let payload = LiveRunStreamPayload::ToolFailed {
            call_id: state.call_id.clone(),
            tool_name: state.tool_name.clone(),
            error,
            duration_ms,
        };
        self.emit(payload);
    }

    fn emit(&mut self, payload: LiveRunStreamPayload) {
        let Some(state) = &mut self.state else {
            return;
        };
        let Ok(publication) = LiveRunStreamPublication::new_run_observation(
            state.identity.clone(),
            state.next_local_sequence,
            payload,
        ) else {
            return;
        };
        state.next_local_sequence = state.next_local_sequence.saturating_add(1);
        // Every broker outcome, including loss, closure, or ordering rejection,
        // is observational and therefore intentionally ignored.
        let _ = self.observer.publish_model_tool_observation(publication);
    }
}

async fn receive_model_tool_progress(
    requests: &mut Option<worker_adapter::ModelToolProgressRequests>,
) -> Option<worker_adapter::ModelToolProgressRequest> {
    match requests {
        Some(requests) => requests.recv().await,
        None => pending().await,
    }
}

fn model_tool_public_error(intent: &ModelToolTaskOutcome) -> Option<RunPublicError> {
    let (code, message) = match intent {
        ModelToolTaskOutcome::Failed {
            class: ModelToolFailureClass::Safe,
            code,
            ..
        } => (code, "The tool request was rejected."),
        ModelToolTaskOutcome::Failed {
            class: ModelToolFailureClass::Infrastructure,
            code,
            ..
        } => (code, "The tool could not be completed."),
        ModelToolTaskOutcome::Failed {
            class: ModelToolFailureClass::EffectOutcomeUnknown,
            code,
            ..
        } => (code, "The tool outcome could not be confirmed."),
        ModelToolTaskOutcome::Cancelled { code, .. } if code == MODEL_TOOL_DEADLINE_EXCEEDED => {
            (code, "The tool execution timed out.")
        }
        ModelToolTaskOutcome::Cancelled { code, .. } => (code, "The tool execution was cancelled."),
        ModelToolTaskOutcome::Succeeded { .. } => return None,
    };
    Some(RunPublicError {
        code: code.clone(),
        message: message.to_owned(),
    })
}

fn model_tool_outcome_from_worker_success(
    claim: &ModelToolTaskClaim,
    result: &insight_engine::worker::TaskExecutionResult,
) -> Result<ModelToolTaskOutcome, RepositoryError> {
    let Some((port_id, output)) = result.outputs().iter().next() else {
        return model_tool_invariant_failure(MODEL_TOOL_RESULT_INVALID, EffectEvidence::Started);
    };
    if result.outputs().len() != 1
        || port_id.as_str() != "model_tool_result"
        || result.effect_evidence() != EffectEvidence::Committed
        || result.model_call().is_some()
        || result.model_tool_call_batch().is_some()
    {
        return model_tool_invariant_failure(MODEL_TOOL_RESULT_INVALID, EffectEvidence::Started);
    }
    let validator =
        insight_engine::schema::compile_schema_2020(claim.identity().action().output_schema())
            .map_err(|_| repository_invalid_data())?;
    if !validator.is_valid(output.value()) {
        return model_tool_invariant_failure(
            MODEL_TOOL_RESULT_SCHEMA_INVALID,
            EffectEvidence::Started,
        );
    }
    let public_projection = match RunToolPublicProjection::from_frozen_effective_policy(
        claim.identity().action().effective_public_policy(),
    ) {
        Ok(projection) => projection,
        Err(_) => {
            return model_tool_invariant_failure(
                MODEL_TOOL_PUBLIC_RESULT_INVALID,
                EffectEvidence::Started,
            )
        }
    };
    if public_projection
        .project_validated_completed_result(
            claim.identity().call_id(),
            claim.identity().action().name(),
            output.value(),
        )
        .is_err()
    {
        return model_tool_invariant_failure(
            MODEL_TOOL_PUBLIC_RESULT_INVALID,
            EffectEvidence::Started,
        );
    }
    match ModelToolTaskOutcome::succeeded(output.value().clone()) {
        Ok(outcome) => Ok(outcome),
        // A syntactically valid worker result can still exceed the queue's
        // hard body bound. Keep that rejection stable and non-retryable.
        Err(_) => model_tool_invariant_failure(MODEL_TOOL_RESULT_INVALID, EffectEvidence::Started),
    }
}

fn model_tool_outcome_from_worker_failure(
    failure: &WorkerFailure,
) -> Result<ModelToolTaskOutcome, RepositoryError> {
    match failure.class() {
        WorkerFailureClass::SafeBusinessFailure => ModelToolTaskOutcome::failed(
            ModelToolFailureClass::Safe,
            failure.code(),
            failure.retryable(),
            EffectEvidence::Started,
        ),
        WorkerFailureClass::InfrastructureFailure => ModelToolTaskOutcome::failed(
            ModelToolFailureClass::Infrastructure,
            failure.code(),
            failure.retryable(),
            EffectEvidence::Started,
        ),
        WorkerFailureClass::EffectOutcomeUnknown => ModelToolTaskOutcome::failed(
            ModelToolFailureClass::EffectOutcomeUnknown,
            failure.code(),
            failure.retryable(),
            EffectEvidence::Unknown,
        ),
        WorkerFailureClass::ControlTermination => {
            ModelToolTaskOutcome::cancelled(failure.code(), EffectEvidence::Unknown)
        }
        WorkerFailureClass::InvariantCorruption => ModelToolTaskOutcome::failed(
            ModelToolFailureClass::Infrastructure,
            failure.code(),
            false,
            EffectEvidence::Started,
        ),
    }
    .map_err(|_| repository_invalid_data())
}

fn model_tool_invariant_failure(
    code: &str,
    effect_evidence: EffectEvidence,
) -> Result<ModelToolTaskOutcome, RepositoryError> {
    ModelToolTaskOutcome::failed(
        ModelToolFailureClass::Infrastructure,
        code,
        false,
        effect_evidence,
    )
    .map_err(|_| repository_invalid_data())
}

fn model_tool_outcome_evidence(outcome: &ModelToolTaskOutcome) -> EffectEvidence {
    match outcome {
        ModelToolTaskOutcome::Succeeded { .. } => EffectEvidence::Committed,
        ModelToolTaskOutcome::Failed {
            effect_evidence, ..
        }
        | ModelToolTaskOutcome::Cancelled {
            effect_evidence, ..
        } => *effect_evidence,
    }
}

fn model_tool_deadline_outcome(
    entered_implementation: bool,
) -> Result<ModelToolTaskOutcome, RepositoryError> {
    ModelToolTaskOutcome::cancelled(
        MODEL_TOOL_DEADLINE_EXCEEDED,
        if entered_implementation {
            EffectEvidence::Unknown
        } else {
            EffectEvidence::Started
        },
    )
    .map_err(|_| repository_invalid_data())
}

struct ModelToolCommitCompletion {
    pump_outcome: ModelToolWorkerPumpOutcome,
    committed_intent: Option<ModelToolTaskOutcome>,
    duration_ms: Option<u64>,
}

impl ModelToolCommitCompletion {
    fn into_pump_outcome(self) -> ModelToolWorkerPumpOutcome {
        self.pump_outcome
    }
}

#[allow(clippy::too_many_arguments)]
async fn commit_model_tool_outcome<R, I>(
    repository: &R,
    claim: &mut ModelToolTaskClaim,
    claim_seconds: u32,
    mut outcome: ModelToolTaskOutcome,
    local_deadline: tokio::time::Instant,
    entered_implementation: bool,
    crash: &I,
) -> Result<ModelToolCommitCompletion, RepositoryError>
where
    R: ModelToolWorkerRepository + ?Sized,
    I: SchedulerCrashInjector,
{
    if tokio::time::Instant::now() >= local_deadline {
        outcome = model_tool_deadline_outcome(entered_implementation)?;
    }
    let authority_evidence = model_tool_outcome_evidence(&outcome);
    match repository
        .heartbeat_model_tool_task(claim, claim_seconds)
        .await?
    {
        ModelToolTaskHeartbeatOutcome::Renewed(renewed) => *claim = *renewed,
        ModelToolTaskHeartbeatOutcome::StaleLease => {
            return Ok(ModelToolCommitCompletion {
                pump_outcome: ModelToolWorkerPumpOutcome::LeaseLost {
                    effect_evidence: authority_evidence,
                },
                committed_intent: None,
                duration_ms: None,
            })
        }
        ModelToolTaskHeartbeatOutcome::StateConflict => {
            return Ok(ModelToolCommitCompletion {
                pump_outcome: ModelToolWorkerPumpOutcome::StateConflict {
                    effect_evidence: authority_evidence,
                },
                committed_intent: None,
                duration_ms: None,
            })
        }
        ModelToolTaskHeartbeatOutcome::RunTerminal => {
            return Ok(ModelToolCommitCompletion {
                pump_outcome: ModelToolWorkerPumpOutcome::RunTerminal {
                    effect_evidence: authority_evidence,
                },
                committed_intent: None,
                duration_ms: None,
            })
        }
    }
    if tokio::time::Instant::now() >= local_deadline {
        outcome = model_tool_deadline_outcome(entered_implementation)?;
    }
    let local_evidence = model_tool_outcome_evidence(&outcome);
    crash.hit(SchedulerCrashPoint::BeforeResultCommit)?;
    let (receipt, exact_replay) = match repository
        .commit_model_tool_task_outcome(claim, &outcome)
        .await?
    {
        ModelToolTaskTransitionOutcome::Committed(receipt) => (receipt, false),
        ModelToolTaskTransitionOutcome::ExactReplay(receipt) => (receipt, true),
        ModelToolTaskTransitionOutcome::StaleLease => {
            return Ok(ModelToolCommitCompletion {
                pump_outcome: ModelToolWorkerPumpOutcome::LeaseLost {
                    effect_evidence: local_evidence,
                },
                committed_intent: None,
                duration_ms: None,
            })
        }
        ModelToolTaskTransitionOutcome::StateConflict => {
            return Ok(ModelToolCommitCompletion {
                pump_outcome: ModelToolWorkerPumpOutcome::StateConflict {
                    effect_evidence: local_evidence,
                },
                committed_intent: None,
                duration_ms: None,
            })
        }
        ModelToolTaskTransitionOutcome::RunTerminal => {
            return Ok(ModelToolCommitCompletion {
                pump_outcome: ModelToolWorkerPumpOutcome::RunTerminal {
                    effect_evidence: local_evidence,
                },
                committed_intent: None,
                duration_ms: None,
            })
        }
    };
    crash.hit(SchedulerCrashPoint::AfterResultCommit)?;
    let duration_ms = receipt.duration_ms();
    let pump_outcome = match receipt.disposition() {
        ModelToolTaskDisposition::Succeeded => ModelToolWorkerPumpOutcome::Succeeded {
            receipt,
            exact_replay,
        },
        ModelToolTaskDisposition::RetryScheduled => ModelToolWorkerPumpOutcome::RetryScheduled {
            receipt,
            exact_replay,
        },
        ModelToolTaskDisposition::Failed => ModelToolWorkerPumpOutcome::Failed {
            receipt,
            exact_replay,
        },
        ModelToolTaskDisposition::Cancelled => ModelToolWorkerPumpOutcome::Cancelled {
            receipt,
            exact_replay,
        },
    };
    Ok(ModelToolCommitCompletion {
        pump_outcome,
        committed_intent: Some(outcome),
        duration_ms,
    })
}

fn worker_result_effect_evidence(
    result: &Option<Result<insight_engine::worker::TaskExecutionResult, WorkerFailure>>,
) -> EffectEvidence {
    match result {
        Some(Ok(result)) => result.effect_evidence(),
        Some(Err(failure)) if failure.class() == WorkerFailureClass::EffectOutcomeUnknown => {
            EffectEvidence::Unknown
        }
        Some(Err(_)) => EffectEvidence::Started,
        None => EffectEvidence::Unknown,
    }
}

enum PendingWorkerOutcome {
    Succeeded(Box<SchedulerTaskSuccess>),
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
    mut result: insight_engine::worker::TaskExecutionResult,
    artifact_store: &dyn WorkerArtifactStore,
) -> Result<GuardedWorkerStep<SchedulerTaskSuccess>, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
{
    let artifact_payloads = result.take_artifact_payloads();
    let mut referenced_artifacts = Vec::with_capacity(artifact_payloads.len());
    for payload in artifact_payloads {
        let expected = artifact_store.artifact_for_bytes(
            payload.bytes(),
            payload.artifact().media_type().map(str::to_owned),
        )?;
        if &expected != payload.artifact() {
            return Err(repository_invalid_data());
        }
        let artifact = payload.artifact().clone();
        let locator = artifact_store.storage_locator(&artifact)?;
        let retain_until = claim
            .claim_expires_at()
            .checked_add_signed(chrono::Duration::seconds(ARTIFACT_STAGING_GRACE_SECONDS))
            .ok_or_else(repository_invalid_configuration)?;
        match await_worker_step(
            repository,
            claim,
            claim_seconds,
            heartbeat,
            deadline.as_mut(),
            repository.stage_artifact(StageArtifactCommand::new(
                claim.run_id().clone(),
                artifact.clone(),
                locator,
                Some(retain_until),
            )),
        )
        .await?
        {
            GuardedWorkerStep::Completed(
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. },
            ) => {}
            GuardedWorkerStep::Completed(
                TransitionOutcome::StaleLease | TransitionOutcome::StateConflict,
            ) => return Err(repository_invalid_data()),
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
            artifact_store.put_and_verify(&artifact, payload.bytes()),
        )
        .await?
        {
            GuardedWorkerStep::Completed(verified) => verified,
            GuardedWorkerStep::OperationDeadlineElapsed => {
                return Ok(GuardedWorkerStep::OperationDeadlineElapsed)
            }
            GuardedWorkerStep::LeaseLost => return Ok(GuardedWorkerStep::LeaseLost),
        };
        match await_worker_step(
            repository,
            claim,
            claim_seconds,
            heartbeat,
            deadline.as_mut(),
            repository.verify_artifact(VerifyArtifactCommand::new(
                claim.run_id().clone(),
                artifact.artifact_id().clone(),
                actual_hash,
                actual_size,
            )),
        )
        .await?
        {
            GuardedWorkerStep::Completed(
                TransitionOutcome::Committed { .. } | TransitionOutcome::ExactReplay { .. },
            ) => {}
            GuardedWorkerStep::Completed(
                TransitionOutcome::StaleLease | TransitionOutcome::StateConflict,
            ) => return Err(repository_invalid_data()),
            GuardedWorkerStep::OperationDeadlineElapsed => {
                return Ok(GuardedWorkerStep::OperationDeadlineElapsed)
            }
            GuardedWorkerStep::LeaseLost => return Ok(GuardedWorkerStep::LeaseLost),
        }
        referenced_artifacts.push(artifact);
    }
    let mut value_refs = BTreeMap::new();
    // A single worker result may bind the same content-addressed value to
    // multiple output ports. Keep the full ArtifactRef as the cache key so a
    // media-type change cannot accidentally alias a distinct artifact
    // identity, while identical values share one stage/put/verify sequence.
    let mut materialized_artifacts = Vec::<(insight_engine::ArtifactRef, ValueRef)>::new();
    for (port_id, output) in result.outputs() {
        let bytes = serde_jcs::to_vec(output.value()).map_err(|_| repository_canonicalization())?;
        let value_ref = if bytes.len() <= artifact_store.inline_threshold_bytes() {
            ValueRef::inline(output.value().clone()).map_err(|_| repository_invalid_data())?
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
                    .ok_or_else(repository_invalid_configuration)?;
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
                        return Err(repository_invalid_data());
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
                        return Err(repository_invalid_data());
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
    SchedulerTaskSuccess::with_value_refs_and_artifacts(result, value_refs, referenced_artifacts)
        .map(GuardedWorkerStep::Completed)
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
    outcome: SchedulerTaskOutcome,
    deadline_claim_seconds: Option<u32>,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
{
    commit_worker_outcome_with_retrieval_observer(
        repository,
        claim,
        outcome,
        deadline_claim_seconds,
        None::<&NoopSchedulerRetrievalLiveObserver>,
        crash,
    )
    .await
}

async fn commit_worker_outcome_with_retrieval_observer<R, I, O>(
    repository: &R,
    claim: &SchedulerTaskClaim,
    mut outcome: SchedulerTaskOutcome,
    deadline_claim_seconds: Option<u32>,
    retrieval_observer: Option<&O>,
    crash: &I,
) -> Result<SchedulerWorkerPumpOutcome, RepositoryError>
where
    R: SchedulerDurableRepository + ?Sized,
    I: SchedulerCrashInjector,
    O: SchedulerRetrievalLiveObserver + ?Sized,
{
    let mut claim = claim.clone();
    crash.hit(SchedulerCrashPoint::BeforeResultCommit)?;
    let (receipt, first_winner) = loop {
        let local_evidence = match &outcome {
            SchedulerTaskOutcome::Succeeded(success) => success.result().effect_evidence(),
            SchedulerTaskOutcome::Failed(failure) => failure.effect_evidence(),
        };
        match repository
            .commit_scheduler_task_outcome(&claim, &outcome)
            .await?
        {
            SchedulerTaskCommitOutcome::Committed { result } => break (result, true),
            SchedulerTaskCommitOutcome::ExactReplay { authoritative } => {
                break (authoritative, false)
            }
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
                    return Err(repository_invalid_data());
                }
                let claim_seconds =
                    deadline_claim_seconds.ok_or_else(repository_invalid_configuration)?;
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
    if first_winner {
        if let Some(observer) = retrieval_observer {
            publish_committed_retrieval_observation(observer, &claim, &outcome);
        }
    }
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

fn publish_committed_retrieval_observation(
    observer: &(impl SchedulerRetrievalLiveObserver + ?Sized),
    claim: &SchedulerTaskClaim,
    outcome: &SchedulerTaskOutcome,
) {
    let SchedulerTaskOutcome::Succeeded(success) = outcome else {
        return;
    };
    let Some(retrieval) = success
        .result()
        .retrieval_completion()
        .and_then(insight_engine::retrieval::RetrievalCompletion::public)
    else {
        return;
    };
    let Ok(identity) = LiveRunObservationIdentity::new(
        claim.run_id().clone(),
        claim.activation_id().clone(),
        claim.envelope().attempt_no(),
        retrieval.retrieval_id().to_owned(),
    ) else {
        return;
    };
    let payload = LiveRunStreamPayload::RetrievalCompleted {
        retrieval_id: retrieval.retrieval_id().to_owned(),
        query: retrieval.query().map(ToOwned::to_owned),
        results: retrieval.results().to_vec(),
    };
    let Ok(publication) = LiveRunStreamPublication::new_run_observation(identity, 0, payload)
    else {
        return;
    };
    // Observation delivery is best-effort and cannot roll back or alter the
    // already committed task-success transaction.
    let _ = observer.publish_retrieval_observation(publication);
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex as StdMutex,
        },
    };

    use chrono::Duration;
    use serde_json::{json, Value};
    use tokio::sync::{Mutex, Notify};

    use super::*;
    use insight_durable::{
        AcknowledgeArtifactDeletionCommand, ArtifactDurableRepository, ArtifactReceipt,
        ArtifactReferenceAuthority, ArtifactRetentionRelease, ArtifactStoreAuthority,
        BindArtifactStoreAuthorityCommand, CommitReceipt, CreateRunCommand, DurableRepository,
        OrphanSweepBatch, OrphanSweepCommand, PayloadId, PayloadReceipt, PlanInstallOutcome,
        PlanPublicationOutcome, ProjectionAudit, ProjectionDurableRepository,
        ProjectionRebuildSnapshot, ProjectionRepairReceipt, ProjectionSubject,
        PublishVersionedPlanCommand, PutInlinePayloadCommand, ReferenceArtifactCommand,
        ReleaseRunArtifactRetentionCommand, RetainedArtifact, RunProjection, RunTransitionCommand,
        SchedulerStoredValue, StageArtifactCommand, StoredInlinePayload, VerifyArtifactCommand,
        VersionedPlan, VersionedPlanCatalog,
    };
    use insight_engine::run_stream::RunStreamEvent;
    use insight_engine::{
        plan::{DataPortId, VersionTag},
        worker::{
            LeafTaskExecutor, ModelCallAuthority, ModelCallCompletion, ModelContinuationTurn,
            ModelFinishReason, ModelTokenUsage, ModelToolCall, ModelToolCallBatch, ModelToolResult,
            TaskExecutionResult,
        },
        ActivationId, AttemptNo, EffectId, EffectIdempotency, LeaseEpoch, NodeId,
        PlannedSchedulerAction, RunId, RuntimeValue, SchedulerCheckpointId, SchedulerFacts,
        SchedulerIntent, SchedulerTaskId, SchedulerTaskKind, TaskAdmissionClass, TransitionKey,
        WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy,
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
            deployment_binding: serde_json::json!({}),
            public_configuration: BTreeMap::new(),
            secret_configuration: BTreeMap::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        };
        insight_engine::internal::scheduler_intent(
            RunId::new("run_retry_test").unwrap(),
            SchedulerCheckpointId::parse(format!("checkpoint_{}", "2".repeat(64))).unwrap(),
            action,
        )
    }

    fn claim(policy: WorkerEffectPolicy, attempt: u32) -> SchedulerTaskClaim {
        let intent = dispatch_intent(policy, TaskAdmissionClass::Normal);
        let request =
            insight_engine::worker::TaskExecutionRequest::from_scheduler_intent(&intent).unwrap();
        let attempt_no = AttemptNo::new(attempt).unwrap();
        let lease_epoch = LeaseEpoch::new(u64::from(attempt)).unwrap();
        let envelope = insight_durable::DurableTaskExecutionRequest::new(
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
            insight_engine::IntentHash::from_serializable(&normal_intent).unwrap(),
            insight_engine::IntentHash::from_serializable(&finalizer_intent).unwrap(),
        );

        for (intent, expected) in [
            (normal_intent, TaskAdmissionClass::Normal),
            (finalizer_intent, TaskAdmissionClass::TerminationFinalizer),
        ] {
            let request =
                insight_engine::worker::TaskExecutionRequest::from_scheduler_intent(&intent)
                    .unwrap();
            assert_eq!(request.admission_class(), expected);
            let request_wire = serde_json::to_value(&request).unwrap();
            assert_eq!(
                serde_json::from_value::<insight_engine::worker::TaskExecutionRequest>(
                    request_wire.clone()
                )
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
                serde_json::from_value::<insight_engine::worker::TaskExecutionRequest>(
                    missing_request_class
                )
                .is_err()
            );
            let mut unknown_request_class = request_wire;
            unknown_request_class["admission_class"] = serde_json::json!("cleanup_by_name");
            assert!(
                serde_json::from_value::<insight_engine::worker::TaskExecutionRequest>(
                    unknown_request_class
                )
                .is_err()
            );

            let envelope = insight_durable::DurableTaskExecutionRequest::new(
                request,
                AttemptNo::FIRST,
                LeaseEpoch::FIRST,
                "fence-closed-wire".to_owned(),
                1,
            )
            .unwrap();
            let envelope_wire = serde_json::to_value(&envelope).unwrap();
            assert_eq!(
                serde_json::from_value::<insight_durable::DurableTaskExecutionRequest>(
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
                serde_json::from_value::<insight_durable::DurableTaskExecutionRequest>(
                    missing_envelope_class
                )
                .is_err()
            );
            let mut unknown_envelope_class = envelope_wire;
            unknown_envelope_class["request"]["admission_class"] =
                serde_json::json!("cleanup_by_name");
            assert!(
                serde_json::from_value::<insight_durable::DurableTaskExecutionRequest>(
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
            insight_durable::scheduler_repository::adapter::WORKER_DEADLINE_EXCEEDED,
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

    #[derive(Clone, Copy)]
    enum MockModelToolHeartbeat {
        Renewed,
        StaleLease,
        StateConflict,
        RunTerminal,
    }

    #[derive(Clone, Copy)]
    enum MockModelToolCommit {
        Committed,
        ExactReplay,
    }

    struct MockModelToolRepository {
        claim: Mutex<Option<ModelToolTaskClaim>>,
        heartbeat_steps: Mutex<VecDeque<MockModelToolHeartbeat>>,
        heartbeat_count: AtomicUsize,
        commit_mode: MockModelToolCommit,
        retry_failure: bool,
        committed_outcomes: Mutex<Vec<ModelToolTaskOutcome>>,
        committed_projection_versions: Mutex<Vec<u64>>,
    }

    impl MockModelToolRepository {
        fn new(
            claim: ModelToolTaskClaim,
            heartbeat_steps: impl IntoIterator<Item = MockModelToolHeartbeat>,
        ) -> Self {
            Self {
                claim: Mutex::new(Some(claim)),
                heartbeat_steps: Mutex::new(heartbeat_steps.into_iter().collect()),
                heartbeat_count: AtomicUsize::new(0),
                commit_mode: MockModelToolCommit::Committed,
                retry_failure: false,
                committed_outcomes: Mutex::new(Vec::new()),
                committed_projection_versions: Mutex::new(Vec::new()),
            }
        }

        fn scheduling_retry(mut self) -> Self {
            self.retry_failure = true;
            self
        }

        fn exact_replay(mut self) -> Self {
            self.commit_mode = MockModelToolCommit::ExactReplay;
            self
        }
    }

    fn renewed_model_tool_claim(claim: &ModelToolTaskClaim) -> ModelToolTaskClaim {
        model_tool_task_claim_new(
            claim.run_id().clone(),
            claim.parent_activation_id().clone(),
            claim.parent_attempt_no(),
            claim.model_call_no(),
            claim.identity().clone(),
            claim.arguments().clone(),
            claim.tool_attempt_no(),
            claim.lease_epoch(),
            claim.fencing_token().to_owned(),
            claim.claimed_by().to_owned(),
            claim.claim_token().to_owned(),
            Utc::now() + Duration::minutes(1),
            claim.projection_version() + 1,
        )
        .unwrap()
    }

    #[async_trait]
    impl ModelToolWorkerRepository for MockModelToolRepository {
        async fn claim_model_tool_tasks(
            &self,
            _claimed_by: &str,
            _claim_seconds: u32,
            limit: u32,
            _max_claimed_per_run: u32,
        ) -> Result<Vec<ModelToolTaskClaim>, RepositoryError> {
            assert_eq!(limit, 1);
            Ok(self.claim.lock().await.take().into_iter().collect())
        }

        async fn mark_model_tool_task_started(
            &self,
            _claim: &ModelToolTaskClaim,
        ) -> Result<ModelToolTaskTransitionOutcome<()>, RepositoryError> {
            Ok(ModelToolTaskTransitionOutcome::Committed(()))
        }

        async fn heartbeat_model_tool_task(
            &self,
            claim: &ModelToolTaskClaim,
            _claim_seconds: u32,
        ) -> Result<ModelToolTaskHeartbeatOutcome, RepositoryError> {
            self.heartbeat_count.fetch_add(1, Ordering::SeqCst);
            let step = self
                .heartbeat_steps
                .lock()
                .await
                .pop_front()
                .unwrap_or(MockModelToolHeartbeat::Renewed);
            Ok(match step {
                MockModelToolHeartbeat::Renewed => ModelToolTaskHeartbeatOutcome::Renewed(
                    Box::new(renewed_model_tool_claim(claim)),
                ),
                MockModelToolHeartbeat::StaleLease => ModelToolTaskHeartbeatOutcome::StaleLease,
                MockModelToolHeartbeat::StateConflict => {
                    ModelToolTaskHeartbeatOutcome::StateConflict
                }
                MockModelToolHeartbeat::RunTerminal => ModelToolTaskHeartbeatOutcome::RunTerminal,
            })
        }

        async fn commit_model_tool_task_outcome(
            &self,
            claim: &ModelToolTaskClaim,
            outcome: &ModelToolTaskOutcome,
        ) -> Result<ModelToolTaskTransitionOutcome<ModelToolTaskCommitReceipt>, RepositoryError>
        {
            self.committed_outcomes.lock().await.push(outcome.clone());
            self.committed_projection_versions
                .lock()
                .await
                .push(claim.projection_version());
            let (disposition, next_available_at, continuation_status) = match outcome {
                ModelToolTaskOutcome::Succeeded { .. } => (
                    ModelToolTaskDisposition::Succeeded,
                    None,
                    insight_durable::ModelToolContinuationStatus::ReadyContinue,
                ),
                ModelToolTaskOutcome::Failed { retryable, .. }
                    if *retryable && self.retry_failure =>
                {
                    (
                        ModelToolTaskDisposition::RetryScheduled,
                        Some(Utc::now() + Duration::milliseconds(10)),
                        insight_durable::ModelToolContinuationStatus::WaitingTools,
                    )
                }
                ModelToolTaskOutcome::Failed { .. } => (
                    ModelToolTaskDisposition::Failed,
                    None,
                    insight_durable::ModelToolContinuationStatus::ReadyFailed,
                ),
                ModelToolTaskOutcome::Cancelled { .. } => (
                    ModelToolTaskDisposition::Cancelled,
                    None,
                    insight_durable::ModelToolContinuationStatus::ReadyCancelled,
                ),
            };
            let receipt = model_tool_task_commit_receipt_new(
                claim.identity().tool_task_id().clone(),
                disposition,
                claim.tool_attempt_no(),
                claim.lease_epoch(),
                next_available_at,
                continuation_status,
                if disposition == ModelToolTaskDisposition::RetryScheduled {
                    None
                } else {
                    Some(1)
                },
            )?;
            Ok(match self.commit_mode {
                MockModelToolCommit::Committed => {
                    ModelToolTaskTransitionOutcome::Committed(receipt)
                }
                MockModelToolCommit::ExactReplay => {
                    ModelToolTaskTransitionOutcome::ExactReplay(receipt)
                }
            })
        }
    }

    struct RecordingModelToolObserver {
        publications: StdMutex<Vec<LiveRunStreamPublication>>,
        outcome: LiveRunStreamPublishOutcome,
    }

    impl RecordingModelToolObserver {
        fn returning(outcome: LiveRunStreamPublishOutcome) -> Self {
            Self {
                publications: StdMutex::new(Vec::new()),
                outcome,
            }
        }

        fn publications(&self) -> Vec<LiveRunStreamPublication> {
            self.publications.lock().unwrap().clone()
        }
    }

    impl ModelToolLiveObserver for RecordingModelToolObserver {
        fn publish_model_tool_observation(
            &self,
            publication: LiveRunStreamPublication,
        ) -> LiveRunStreamPublishOutcome {
            self.publications.lock().unwrap().push(publication);
            self.outcome
        }
    }

    enum ModelToolExecutorBehavior {
        Fixed(Result<TaskExecutionResult, WorkerFailure>),
        AssertStartedVisible {
            observer: Arc<RecordingModelToolObserver>,
            result: Result<TaskExecutionResult, WorkerFailure>,
        },
        Pending,
        CancellationAware {
            entered: Arc<Notify>,
        },
        PublishProgress {
            values: Vec<Value>,
            outcomes: Arc<StdMutex<Vec<String>>>,
            result: Result<TaskExecutionResult, WorkerFailure>,
        },
    }

    struct MockModelToolExecutor {
        behavior: ModelToolExecutorBehavior,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl LeafTaskExecutor for MockModelToolExecutor {
        async fn execute(
            &self,
            _context: &WorkerExecutionContext,
            _request: &TaskExecutionRequest,
            cancellation: CancellationToken,
        ) -> Result<TaskExecutionResult, WorkerFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match &self.behavior {
                ModelToolExecutorBehavior::Fixed(result) => result.clone(),
                ModelToolExecutorBehavior::AssertStartedVisible { observer, result } => {
                    assert_eq!(observer.publications().len(), 1);
                    result.clone()
                }
                ModelToolExecutorBehavior::Pending => std::future::pending().await,
                ModelToolExecutorBehavior::CancellationAware { entered } => {
                    entered.notify_one();
                    cancellation.cancelled().await;
                    Err(WorkerFailure::new(
                        WorkerFailureClass::ControlTermination,
                        "TOOL_CANCELLED",
                        false,
                    )
                    .unwrap())
                }
                ModelToolExecutorBehavior::PublishProgress { result, .. } => result.clone(),
            }
        }

        async fn execute_with_runtime_services(
            &self,
            context: &WorkerExecutionContext,
            request: &TaskExecutionRequest,
            services: &WorkerRuntimeServices,
            cancellation: CancellationToken,
        ) -> Result<TaskExecutionResult, WorkerFailure> {
            let ModelToolExecutorBehavior::PublishProgress {
                values,
                outcomes,
                result,
            } = &self.behavior
            else {
                return self.execute(context, request, cancellation).await;
            };
            self.calls.fetch_add(1, Ordering::SeqCst);
            let publisher = worker_adapter::services_model_tool_progress_publisher(services)
                .expect("public progress policy installs one scoped publisher");
            for value in values {
                let outcome = publisher.publish(value.clone()).await;
                outcomes.lock().unwrap().push(match outcome {
                    Ok(worker_adapter::ModelToolProgressDisposition::Published) => {
                        "published".to_owned()
                    }
                    Ok(worker_adapter::ModelToolProgressDisposition::Dropped) => {
                        "dropped".to_owned()
                    }
                    Err(error) => error.code().to_owned(),
                });
            }
            result.clone()
        }
    }

    fn model_tool_worker_claim(timeout_ms: u64, output_schema: Value) -> ModelToolTaskClaim {
        let private_policy = json!({
            "call": false,
            "arguments": "private",
            "result": null
        });
        model_tool_worker_claim_with_policies(
            timeout_ms,
            output_schema,
            private_policy.clone(),
            private_policy,
        )
    }

    fn model_tool_worker_claim_with_public_policy(
        timeout_ms: u64,
        output_schema: Value,
        public_policy: Value,
    ) -> ModelToolTaskClaim {
        model_tool_worker_claim_with_policies(
            timeout_ms,
            output_schema,
            public_policy.clone(),
            public_policy,
        )
    }

    fn model_tool_worker_claim_with_policies(
        timeout_ms: u64,
        output_schema: Value,
        descriptor_public_policy: Value,
        effective_public_policy: Value,
    ) -> ModelToolTaskClaim {
        let effect_policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::ReadOnly,
            EffectIdempotency::Idempotent,
            3,
            10,
            100,
            timeout_ms,
            WorkerCancellation::Cooperative,
        )
        .unwrap();
        let descriptor_hash = "d".repeat(64);
        let deployment_binding = json!({
            "adapter": "native_action",
            "action_id": "lookup",
            "action_version": "1.2.3",
            "descriptor_hash": descriptor_hash,
            "effect": "read_only",
            "idempotency": "idempotent",
            "cancellation": "cooperative",
            "required_capabilities": [],
            "public": descriptor_public_policy,
        });
        let action = insight_durable::model_tool_queue::adapter::parse_action_from_stored_evidence(
            "lookup".to_owned(),
            "lookup".to_owned(),
            "1.2.3".to_owned(),
            descriptor_hash,
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$defs": {},
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "detail": {"type": "string"}
                },
                "required": ["query", "detail"],
                "additionalProperties": false
            }),
            output_schema,
            effect_policy,
            deployment_binding,
            effective_public_policy,
        )
        .unwrap();
        let run_id = RunId::new("run_model_tool_pump").unwrap();
        let parent_activation_id = ActivationId::new("activation_model_tool_parent").unwrap();
        let identity = insight_durable::model_tool_queue::adapter::deterministic_tool_identity(
            &run_id,
            &parent_activation_id,
            AttemptNo::FIRST,
            1,
            0,
            "call_lookup",
            action,
            None,
            None,
            None,
        )
        .unwrap();
        model_tool_task_claim_new(
            run_id,
            parent_activation_id,
            AttemptNo::FIRST,
            1,
            identity,
            json!({
                "query": "white blood cell",
                "detail": "model-visible detail"
            }),
            AttemptNo::FIRST,
            LeaseEpoch::FIRST,
            "model-tool-fence".to_owned(),
            "worker-model-tool".to_owned(),
            "model-tool-claim".to_owned(),
            Utc::now() + Duration::minutes(1),
            1,
        )
        .unwrap()
    }

    fn model_tool_result(value: Value) -> TaskExecutionResult {
        TaskExecutionResult::new(
            BTreeMap::from([(
                DataPortId::new("model_tool_result").unwrap(),
                RuntimeValue::new(value).unwrap(),
            )]),
            EffectEvidence::Committed,
        )
    }

    fn model_tool_public_schema(properties: Value, required: &[&str]) -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {},
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false
        })
    }

    fn model_tool_registry(
        behavior: ModelToolExecutorBehavior,
        calls: Arc<AtomicUsize>,
    ) -> WorkerExecutorRegistry {
        let mut registry = WorkerExecutorRegistry::new();
        registry
            .register(
                SchedulerTaskKind::Action,
                "lookup",
                VersionTag::new("1").unwrap(),
                VersionTag::new("1.2.3").unwrap(),
                Arc::new(MockModelToolExecutor { behavior, calls }),
            )
            .unwrap();
        registry
    }

    #[tokio::test]
    async fn model_tool_live_observer_publishes_validated_fields_or_all_before_action_and_typed_success(
    ) {
        for (arguments_policy, expected_arguments) in [
            (json!(["query"]), json!({"query": "white blood cell"})),
            (
                json!("all"),
                json!({
                    "query": "white blood cell",
                    "detail": "model-visible detail"
                }),
            ),
        ] {
            let result_schema = model_tool_public_schema(
                json!({
                    "type": {"const": "output_text"},
                    "text": {"type": "string"}
                }),
                &["type", "text"],
            );
            let claim = model_tool_worker_claim_with_public_policy(
                30_000,
                result_schema.clone(),
                json!({
                    "call": true,
                    "arguments": arguments_policy,
                    "result": result_schema
                }),
            );
            let expected_run_id = claim.run_id().clone();
            let expected_activation_id = claim.parent_activation_id().clone();
            let expected_source_id = claim.identity().tool_task_id().as_str().to_owned();
            let repository = MockModelToolRepository::new(
                claim,
                [
                    MockModelToolHeartbeat::Renewed,
                    MockModelToolHeartbeat::Renewed,
                ],
            );
            let observer = Arc::new(RecordingModelToolObserver::returning(
                // A closed broker is observational only. Keeping this outcome
                // in the happy-path test proves it cannot change execution.
                LiveRunStreamPublishOutcome::RunClosed,
            ));
            let calls = Arc::new(AtomicUsize::new(0));
            let registry = model_tool_registry(
                ModelToolExecutorBehavior::AssertStartedVisible {
                    observer: Arc::clone(&observer),
                    result: Ok(model_tool_result(json!({
                        "type": "output_text",
                        "text": "safe interpretation"
                    }))),
                },
                Arc::clone(&calls),
            );

            let outcome = consume_model_tool_task_once_with_observer(
                &repository,
                &registry,
                "worker-model-tool",
                30,
                4,
                CancellationToken::new(),
                observer.as_ref(),
                &insight_durable::NoSchedulerCrash,
            )
            .await
            .unwrap();

            assert!(matches!(
                outcome,
                ModelToolWorkerPumpOutcome::Succeeded {
                    exact_replay: false,
                    ..
                }
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let publications = observer.publications();
            assert_eq!(publications.len(), 2);
            for (expected_sequence, publication) in publications.iter().enumerate() {
                let identity = publication.run_observation_identity().unwrap();
                assert_eq!(identity.run_id(), &expected_run_id);
                assert_eq!(identity.activation_id(), &expected_activation_id);
                assert_eq!(identity.attempt_no(), AttemptNo::FIRST);
                assert_eq!(identity.source_id(), expected_source_id);
                assert_eq!(publication.local_sequence(), expected_sequence as u64);
            }
            assert!(matches!(
                publications[0].clone().into_public_event(11),
                RunStreamEvent::RunToolStarted {
                    sequence_number: 11,
                    call_id,
                    tool_name,
                    arguments: Some(arguments),
                } if call_id == "call_lookup"
                    && tool_name == "lookup"
                    && arguments == expected_arguments
            ));
            match publications[1].clone().into_public_event(12) {
                RunStreamEvent::RunToolCompleted {
                    sequence_number: 12,
                    call_id,
                    tool_name,
                    duration_ms,
                    content,
                } => {
                    assert_eq!(call_id, "call_lookup");
                    assert_eq!(tool_name, "lookup");
                    assert_eq!(duration_ms, 1);
                    assert_eq!(content.len(), 1);
                    assert_eq!(content[0].text(), Some("safe interpretation"));
                }
                event => panic!("expected typed tool completion, got {event:?}"),
            }
        }
    }

    #[tokio::test]
    async fn model_tool_progress_is_validated_best_effort_ordered_and_does_not_change_success() {
        let progress_schema = model_tool_public_schema(
            json!({
                "completed": {"type": "integer", "minimum": 0},
                "total": {"type": "integer", "minimum": 1}
            }),
            &["completed", "total"],
        );
        let claim = model_tool_worker_claim_with_public_policy(
            30_000,
            model_tool_public_schema(json!({"ok": {"type": "boolean"}}), &["ok"]),
            json!({
                "call": true,
                "arguments": "private",
                "progress": progress_schema,
                "result": null
            }),
        );
        let repository = MockModelToolRepository::new(
            claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        );
        let observer = RecordingModelToolObserver::returning(LiveRunStreamPublishOutcome::Enqueued);
        let outcomes = Arc::new(StdMutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = model_tool_registry(
            ModelToolExecutorBehavior::PublishProgress {
                values: vec![
                    json!({"completed": 1, "total": 10}),
                    json!({"completed": "private invalid body", "total": 10}),
                    json!({"completed": 2, "total": 10}),
                    json!({"completed": 3, "total": 10}),
                    json!({"completed": 4, "total": 10}),
                    json!({"completed": 5, "total": 10}),
                    json!({"completed": 6, "total": 10}),
                    json!({"completed": 7, "total": 10}),
                    json!({"completed": 8, "total": 10}),
                    json!({"completed": 9, "total": 10}),
                    json!({"completed": 10, "total": 10}),
                ],
                outcomes: Arc::clone(&outcomes),
                result: Ok(model_tool_result(json!({"ok": true}))),
            },
            Arc::clone(&calls),
        );

        let outcome = consume_model_tool_task_once_with_observer(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &observer,
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            ModelToolWorkerPumpOutcome::Succeeded { .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec![
                "published",
                "MODEL_TOOL_PUBLIC_PROGRESS_INVALID",
                "published",
                "published",
                "published",
                "published",
                "published",
                "published",
                "published",
                "dropped",
                "dropped",
            ]
        );
        let events = observer
            .publications()
            .into_iter()
            .enumerate()
            .map(|(sequence, publication)| publication.into_public_event(sequence as u64))
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 10);
        assert!(matches!(events[0], RunStreamEvent::RunToolStarted { .. }));
        assert!(matches!(
            events[1],
            RunStreamEvent::RunToolProgress { ref content, .. }
                if content[0].json() == Some(&json!({"completed": 1, "total": 10}))
        ));
        assert!(matches!(
            events[2],
            RunStreamEvent::RunToolProgress { ref content, .. }
                if content[0].json() == Some(&json!({"completed": 2, "total": 10}))
        ));
        assert!(matches!(
            events[8],
            RunStreamEvent::RunToolProgress { ref content, .. }
                if content[0].json() == Some(&json!({"completed": 8, "total": 10}))
        ));
        assert!(matches!(
            events[9],
            RunStreamEvent::RunToolCompleted {
                duration_ms: 1,
                ref content,
                ..
            } if content.is_empty()
        ));
    }

    #[tokio::test]
    async fn model_tool_live_observer_honors_effective_double_authorization_and_private_bodies() {
        let descriptor_public_policy = json!({
            "call": true,
            "arguments": "all",
            "result": null
        });
        let effective_private_policy = json!({
            "call": false,
            "arguments": "private",
            "result": null
        });
        let claim = model_tool_worker_claim_with_policies(
            30_000,
            json!({"type": "object"}),
            descriptor_public_policy,
            effective_private_policy,
        );
        let repository = MockModelToolRepository::new(
            claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        );
        let observer = RecordingModelToolObserver::returning(LiveRunStreamPublishOutcome::Enqueued);
        let registry = model_tool_registry(
            ModelToolExecutorBehavior::Fixed(Ok(model_tool_result(json!({
                "raw_secret": "must never publish"
            })))),
            Arc::new(AtomicUsize::new(0)),
        );

        let outcome = consume_model_tool_task_once_with_observer(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &observer,
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            ModelToolWorkerPumpOutcome::Succeeded { .. }
        ));
        assert!(observer.publications().is_empty());

        let metadata_policy = json!({
            "call": true,
            "arguments": "private",
            "result": null
        });
        let claim = model_tool_worker_claim_with_public_policy(
            30_000,
            json!({"type": "object"}),
            metadata_policy,
        );
        let repository = MockModelToolRepository::new(
            claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        );
        let observer = RecordingModelToolObserver::returning(LiveRunStreamPublishOutcome::Enqueued);
        let registry = model_tool_registry(
            ModelToolExecutorBehavior::Fixed(Ok(model_tool_result(json!({
                "raw_secret": "must never publish"
            })))),
            Arc::new(AtomicUsize::new(0)),
        );

        let outcome = consume_model_tool_task_once_with_observer(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &observer,
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            ModelToolWorkerPumpOutcome::Succeeded { .. }
        ));
        let publications = observer.publications();
        assert_eq!(publications.len(), 2);
        assert!(matches!(
            publications[0].clone().into_public_event(0),
            RunStreamEvent::RunToolStarted {
                arguments: None,
                ..
            }
        ));
        assert!(matches!(
            publications[1].clone().into_public_event(1),
            RunStreamEvent::RunToolCompleted { ref content, .. }
                if content.is_empty()
        ));
        let wire = serde_json::to_string(
            &publications
                .into_iter()
                .enumerate()
                .map(|(sequence, publication)| publication.into_public_event(sequence as u64))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(!wire.contains("raw_secret"));
        assert!(!wire.contains("must never publish"));
    }

    #[tokio::test]
    async fn model_tool_live_projection_rejection_commits_a_stable_failure_before_publication() {
        let public_result_schema = model_tool_public_schema(
            json!({
                "type": {"const": "output_text"},
                "text": {"type": "string"}
            }),
            &["type", "text"],
        );
        let claim = model_tool_worker_claim_with_public_policy(
            30_000,
            json!({"type": "object"}),
            json!({
                "call": true,
                "arguments": "private",
                "result": public_result_schema
            }),
        );
        let repository = MockModelToolRepository::new(
            claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        );
        let observer = RecordingModelToolObserver::returning(LiveRunStreamPublishOutcome::Enqueued);
        let registry = model_tool_registry(
            ModelToolExecutorBehavior::Fixed(Ok(model_tool_result(json!({
                "raw_secret": "fails only the public projection"
            })))),
            Arc::new(AtomicUsize::new(0)),
        );

        let outcome = consume_model_tool_task_once_with_observer(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &observer,
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            ModelToolWorkerPumpOutcome::Failed {
                exact_replay: false,
                ..
            }
        ));
        assert!(matches!(
            &repository.committed_outcomes.lock().await[0],
            ModelToolTaskOutcome::Failed {
                class: ModelToolFailureClass::Infrastructure,
                code,
                retryable: false,
                effect_evidence: EffectEvidence::Started,
            } if code == MODEL_TOOL_PUBLIC_RESULT_INVALID
        ));
        let publications = observer.publications();
        assert_eq!(publications.len(), 2);
        assert!(matches!(
            publications[0].clone().into_public_event(0),
            RunStreamEvent::RunToolStarted { .. }
        ));
        assert!(matches!(
            publications[1].clone().into_public_event(1),
            RunStreamEvent::RunToolFailed { ref error, .. }
                if error.code == MODEL_TOOL_PUBLIC_RESULT_INVALID
                    && error.message == "The tool could not be completed."
        ));
    }

    #[tokio::test]
    async fn model_tool_live_observer_suppresses_retry_failure_and_sanitizes_final_failure() {
        let metadata_policy = json!({
            "call": true,
            "arguments": "private",
            "result": null
        });
        let retry_claim = model_tool_worker_claim_with_public_policy(
            30_000,
            json!({"type": "object"}),
            metadata_policy.clone(),
        );
        let retry_repository = MockModelToolRepository::new(
            retry_claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        )
        .scheduling_retry();
        let retry_observer =
            RecordingModelToolObserver::returning(LiveRunStreamPublishOutcome::Enqueued);
        let retry_registry = model_tool_registry(
            ModelToolExecutorBehavior::Fixed(Err(WorkerFailure::new(
                WorkerFailureClass::InfrastructureFailure,
                "TOOL_TEMPORARY_FAILURE",
                true,
            )
            .unwrap())),
            Arc::new(AtomicUsize::new(0)),
        );

        let retry_outcome = consume_model_tool_task_once_with_observer(
            &retry_repository,
            &retry_registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &retry_observer,
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();
        assert!(matches!(
            retry_outcome,
            ModelToolWorkerPumpOutcome::RetryScheduled { .. }
        ));
        let retry_publications = retry_observer.publications();
        assert_eq!(retry_publications.len(), 1);
        assert_eq!(retry_publications[0].local_sequence(), 0);
        assert!(matches!(
            retry_publications[0].clone().into_public_event(0),
            RunStreamEvent::RunToolStarted { .. }
        ));

        let final_claim = model_tool_worker_claim_with_public_policy(
            30_000,
            json!({"type": "object"}),
            metadata_policy,
        );
        let final_repository = MockModelToolRepository::new(
            final_claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        );
        let final_observer =
            RecordingModelToolObserver::returning(LiveRunStreamPublishOutcome::Enqueued);
        let safe_failure = WorkerFailure::safe_business(
            "TOOL_REJECTED",
            false,
            RuntimeValue::new(json!({
                "kind": "safe_error",
                "code": "TOOL_REJECTED",
                "message": "sensitive executor body"
            }))
            .unwrap(),
        )
        .unwrap();
        let final_registry = model_tool_registry(
            ModelToolExecutorBehavior::Fixed(Err(safe_failure)),
            Arc::new(AtomicUsize::new(0)),
        );

        let final_outcome = consume_model_tool_task_once_with_observer(
            &final_repository,
            &final_registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &final_observer,
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();
        assert!(matches!(
            final_outcome,
            ModelToolWorkerPumpOutcome::Failed { .. }
        ));
        let final_publications = final_observer.publications();
        assert_eq!(final_publications.len(), 2);
        assert_eq!(final_publications[1].local_sequence(), 1);
        match final_publications[1].clone().into_public_event(1) {
            RunStreamEvent::RunToolFailed { error, .. } => {
                assert_eq!(error.code, "TOOL_REJECTED");
                assert_eq!(error.message, "The tool request was rejected.");
            }
            event => panic!("expected terminal tool failure, got {event:?}"),
        }
        let wire =
            serde_json::to_string(&final_publications[1].clone().into_public_event(1)).unwrap();
        assert!(!wire.contains("sensitive executor body"));
    }

    #[tokio::test]
    async fn model_tool_live_observer_never_fakes_terminal_after_authority_loss() {
        let metadata_policy = json!({
            "call": true,
            "arguments": "private",
            "result": null
        });
        for heartbeat in [
            MockModelToolHeartbeat::StaleLease,
            MockModelToolHeartbeat::StateConflict,
            MockModelToolHeartbeat::RunTerminal,
        ] {
            let claim = model_tool_worker_claim_with_public_policy(
                30_000,
                json!({"type": "object"}),
                metadata_policy.clone(),
            );
            let repository =
                MockModelToolRepository::new(claim, [MockModelToolHeartbeat::Renewed, heartbeat]);
            let observer =
                RecordingModelToolObserver::returning(LiveRunStreamPublishOutcome::Enqueued);
            let registry = model_tool_registry(
                ModelToolExecutorBehavior::Fixed(Ok(model_tool_result(json!({
                    "private": "result"
                })))),
                Arc::new(AtomicUsize::new(0)),
            );

            let outcome = consume_model_tool_task_once_with_observer(
                &repository,
                &registry,
                "worker-model-tool",
                30,
                4,
                CancellationToken::new(),
                &observer,
                &insight_durable::NoSchedulerCrash,
            )
            .await
            .unwrap();
            assert!(matches!(
                (heartbeat, outcome),
                (
                    MockModelToolHeartbeat::StaleLease,
                    ModelToolWorkerPumpOutcome::LeaseLost { .. }
                ) | (
                    MockModelToolHeartbeat::StateConflict,
                    ModelToolWorkerPumpOutcome::StateConflict { .. }
                ) | (
                    MockModelToolHeartbeat::RunTerminal,
                    ModelToolWorkerPumpOutcome::RunTerminal { .. }
                )
            ));
            let publications = observer.publications();
            assert_eq!(publications.len(), 1);
            assert!(matches!(
                publications[0].clone().into_public_event(0),
                RunStreamEvent::RunToolStarted { .. }
            ));
        }
    }

    #[test]
    fn model_tool_worker_failure_mapping_freezes_class_retry_and_effect_evidence() {
        let safe = WorkerFailure::safe_business(
            "TOOL_REJECTED",
            false,
            RuntimeValue::new(json!({
                "kind": "safe_error",
                "code": "TOOL_REJECTED",
                "message": "The request is not available."
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            model_tool_outcome_from_worker_failure(&safe).unwrap(),
            ModelToolTaskOutcome::Failed {
                class: ModelToolFailureClass::Safe,
                retryable: false,
                effect_evidence: EffectEvidence::Started,
                ..
            }
        ));

        let infrastructure = WorkerFailure::new(
            WorkerFailureClass::InfrastructureFailure,
            "TOOL_UNAVAILABLE",
            true,
        )
        .unwrap();
        assert!(matches!(
            model_tool_outcome_from_worker_failure(&infrastructure).unwrap(),
            ModelToolTaskOutcome::Failed {
                class: ModelToolFailureClass::Infrastructure,
                retryable: true,
                effect_evidence: EffectEvidence::Started,
                ..
            }
        ));

        let unknown = WorkerFailure::new(
            WorkerFailureClass::EffectOutcomeUnknown,
            "TOOL_EFFECT_UNKNOWN",
            true,
        )
        .unwrap();
        assert!(matches!(
            model_tool_outcome_from_worker_failure(&unknown).unwrap(),
            ModelToolTaskOutcome::Failed {
                class: ModelToolFailureClass::EffectOutcomeUnknown,
                effect_evidence: EffectEvidence::Unknown,
                ..
            }
        ));

        let control = WorkerFailure::new(
            WorkerFailureClass::ControlTermination,
            "TOOL_CANCELLED",
            false,
        )
        .unwrap();
        assert!(matches!(
            model_tool_outcome_from_worker_failure(&control).unwrap(),
            ModelToolTaskOutcome::Cancelled {
                effect_evidence: EffectEvidence::Unknown,
                ..
            }
        ));

        let invariant = WorkerFailure::new(
            WorkerFailureClass::InvariantCorruption,
            "TOOL_RESULT_CORRUPT",
            false,
        )
        .unwrap();
        assert!(matches!(
            model_tool_outcome_from_worker_failure(&invariant).unwrap(),
            ModelToolTaskOutcome::Failed {
                class: ModelToolFailureClass::Infrastructure,
                retryable: false,
                effect_evidence: EffectEvidence::Started,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn model_tool_pump_stale_initial_heartbeat_never_enters_action_or_commits() {
        let claim = model_tool_worker_claim(30_000, json!({"type": "object"}));
        let repository = MockModelToolRepository::new(claim, [MockModelToolHeartbeat::StaleLease]);
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = model_tool_registry(
            ModelToolExecutorBehavior::Fixed(Ok(model_tool_result(json!({"answer": "ok"})))),
            Arc::clone(&calls),
        );

        let outcome = consume_model_tool_task_once(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            ModelToolWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::Unknown
            }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(repository.committed_outcomes.lock().await.is_empty());
    }

    #[tokio::test]
    async fn model_tool_pump_preserves_initial_state_conflict_and_run_terminal_fences() {
        for heartbeat in [
            MockModelToolHeartbeat::StateConflict,
            MockModelToolHeartbeat::RunTerminal,
        ] {
            let claim = model_tool_worker_claim(30_000, json!({"type": "object"}));
            let repository = MockModelToolRepository::new(claim, [heartbeat]);
            let calls = Arc::new(AtomicUsize::new(0));
            let registry = model_tool_registry(
                ModelToolExecutorBehavior::Fixed(Ok(model_tool_result(json!({
                    "answer": "not executed"
                })))),
                Arc::clone(&calls),
            );

            let outcome = consume_model_tool_task_once(
                &repository,
                &registry,
                "worker-model-tool",
                30,
                4,
                CancellationToken::new(),
                &insight_durable::NoSchedulerCrash,
            )
            .await
            .unwrap();

            assert!(matches!(
                (heartbeat, outcome),
                (
                    MockModelToolHeartbeat::StateConflict,
                    ModelToolWorkerPumpOutcome::StateConflict {
                        effect_evidence: EffectEvidence::Started
                    }
                ) | (
                    MockModelToolHeartbeat::RunTerminal,
                    ModelToolWorkerPumpOutcome::RunTerminal {
                        effect_evidence: EffectEvidence::Started
                    }
                )
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(repository.committed_outcomes.lock().await.is_empty());
        }
    }

    #[tokio::test]
    async fn model_tool_pump_shutdown_abandons_started_claim_without_business_commit() {
        let claim = model_tool_worker_claim_with_public_policy(
            30_000,
            json!({"type": "object"}),
            json!({
                "call": true,
                "arguments": "private",
                "result": null
            }),
        );
        let repository = MockModelToolRepository::new(claim, [MockModelToolHeartbeat::Renewed]);
        let observer = RecordingModelToolObserver::returning(LiveRunStreamPublishOutcome::Enqueued);
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let registry = model_tool_registry(
            ModelToolExecutorBehavior::CancellationAware {
                entered: Arc::clone(&entered),
            },
            Arc::clone(&calls),
        );
        let cancellation = CancellationToken::new();
        let cancel_after_entry = {
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                entered.notified().await;
                cancellation.cancel();
            })
        };

        let outcome = consume_model_tool_task_once_with_observer(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            cancellation,
            &observer,
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();
        cancel_after_entry.await.unwrap();

        assert!(matches!(
            outcome,
            ModelToolWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::Unknown
            }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(repository.committed_outcomes.lock().await.is_empty());
        let publications = observer.publications();
        assert_eq!(publications.len(), 1);
        assert!(matches!(
            publications[0].clone().into_public_event(0),
            RunStreamEvent::RunToolStarted { .. }
        ));
    }

    #[tokio::test]
    async fn model_tool_pump_preserves_retry_scheduled_receipt() {
        let claim = model_tool_worker_claim(30_000, json!({"type": "object"}));
        let repository = MockModelToolRepository::new(
            claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        )
        .scheduling_retry();
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = model_tool_registry(
            ModelToolExecutorBehavior::Fixed(Err(WorkerFailure::new(
                WorkerFailureClass::InfrastructureFailure,
                "TOOL_TEMPORARY_FAILURE",
                true,
            )
            .unwrap())),
            Arc::clone(&calls),
        );

        let outcome = consume_model_tool_task_once(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        let ModelToolWorkerPumpOutcome::RetryScheduled {
            receipt,
            exact_replay: false,
        } = outcome
        else {
            panic!("retry receipt must remain explicit")
        };
        assert!(receipt.next_available_at().is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let committed = repository.committed_outcomes.lock().await;
        assert!(matches!(
            &committed[0],
            ModelToolTaskOutcome::Failed {
                class: ModelToolFailureClass::Infrastructure,
                retryable: true,
                effect_evidence: EffectEvidence::Started,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn model_tool_pump_deadline_heartbeats_before_committing_unknown_cancellation() {
        let claim = model_tool_worker_claim(15, json!({"type": "object"}));
        let repository = MockModelToolRepository::new(
            claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = model_tool_registry(ModelToolExecutorBehavior::Pending, Arc::clone(&calls));

        let outcome = consume_model_tool_task_once(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            ModelToolWorkerPumpOutcome::Cancelled {
                exact_replay: false,
                ..
            }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(repository.heartbeat_count.load(Ordering::SeqCst), 2);
        let committed = repository.committed_outcomes.lock().await;
        assert!(matches!(
            &committed[0],
            ModelToolTaskOutcome::Cancelled {
                code,
                effect_evidence: EffectEvidence::Unknown,
            } if code == MODEL_TOOL_DEADLINE_EXCEEDED
        ));
    }

    #[tokio::test]
    async fn model_tool_pump_rejects_real_result_outside_frozen_output_schema() {
        let claim = model_tool_worker_claim(
            30_000,
            json!({
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"],
                "additionalProperties": false
            }),
        );
        let repository = MockModelToolRepository::new(
            claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = model_tool_registry(
            ModelToolExecutorBehavior::Fixed(Ok(model_tool_result(json!({"answer": 7})))),
            Arc::clone(&calls),
        );

        let outcome = consume_model_tool_task_once(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            ModelToolWorkerPumpOutcome::Failed {
                exact_replay: false,
                ..
            }
        ));
        let committed = repository.committed_outcomes.lock().await;
        assert!(matches!(
            &committed[0],
            ModelToolTaskOutcome::Failed {
                class: ModelToolFailureClass::Infrastructure,
                code,
                retryable: false,
                effect_evidence: EffectEvidence::Started,
            } if code == MODEL_TOOL_RESULT_SCHEMA_INVALID
        ));
    }

    #[tokio::test]
    async fn model_tool_pump_surfaces_exact_replay_without_reclassifying_receipt() {
        let claim = model_tool_worker_claim(30_000, json!({"type": "object"}));
        let repository = MockModelToolRepository::new(
            claim,
            [
                MockModelToolHeartbeat::Renewed,
                MockModelToolHeartbeat::Renewed,
            ],
        )
        .exact_replay();
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = model_tool_registry(
            ModelToolExecutorBehavior::Fixed(Ok(model_tool_result(json!({"answer": "ok"})))),
            calls,
        );

        let outcome = consume_model_tool_task_once(
            &repository,
            &registry,
            "worker-model-tool",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            ModelToolWorkerPumpOutcome::Succeeded {
                exact_replay: true,
                ..
            }
        ));
    }

    #[derive(Debug, Clone, Copy)]
    enum ParentHeartbeatStep {
        Renewed,
        DeadlineElapsed,
        LeaseLost,
    }

    #[derive(Debug, Clone, Copy)]
    enum ParentCheckpointStep {
        Committed,
        ExactReplay,
        StaleLease,
        StateConflict,
        DeadlineElapsed,
    }

    struct MockParentContinuationRepository {
        claim: Mutex<Option<SchedulerTaskClaim>>,
        resume: SchedulerTaskCommitOutcome<Option<ModelToolParentResume>>,
        heartbeat_steps: Mutex<VecDeque<ParentHeartbeatStep>>,
        checkpoint_step: ParentCheckpointStep,
        activation_steps: Mutex<VecDeque<ModelToolBatchActivationOutcome>>,
        mark_started_count: AtomicUsize,
        provider_reservations: Mutex<Vec<(u32, bool)>>,
        checkpointed_completions: Mutex<Vec<ModelCallCompletion>>,
        committed_outcomes: Mutex<Vec<SchedulerTaskOutcome>>,
        events: Mutex<Vec<&'static str>>,
    }

    impl MockParentContinuationRepository {
        fn new(
            claim: SchedulerTaskClaim,
            resume: Option<ModelToolParentResume>,
            activation_steps: impl IntoIterator<Item = ModelToolBatchActivationOutcome>,
        ) -> Self {
            Self {
                claim: Mutex::new(Some(claim)),
                resume: SchedulerTaskCommitOutcome::Committed { result: resume },
                heartbeat_steps: Mutex::new(VecDeque::new()),
                checkpoint_step: ParentCheckpointStep::Committed,
                activation_steps: Mutex::new(activation_steps.into_iter().collect()),
                mark_started_count: AtomicUsize::new(0),
                provider_reservations: Mutex::new(Vec::new()),
                checkpointed_completions: Mutex::new(Vec::new()),
                committed_outcomes: Mutex::new(Vec::new()),
                events: Mutex::new(Vec::new()),
            }
        }

        fn with_heartbeats(mut self, steps: impl IntoIterator<Item = ParentHeartbeatStep>) -> Self {
            self.heartbeat_steps = Mutex::new(steps.into_iter().collect());
            self
        }

        fn with_checkpoint_step(mut self, step: ParentCheckpointStep) -> Self {
            self.checkpoint_step = step;
            self
        }

        async fn push_event(&self, event: &'static str) {
            self.events.lock().await.push(event);
        }
    }

    struct RecordingParentLlmExecutor {
        calls: Arc<AtomicUsize>,
        contexts: Arc<StdMutex<Vec<WorkerExecutionContext>>>,
        result: Result<TaskExecutionResult, WorkerFailure>,
    }

    #[async_trait]
    impl LeafTaskExecutor for RecordingParentLlmExecutor {
        async fn execute(
            &self,
            context: &WorkerExecutionContext,
            _request: &TaskExecutionRequest,
            _cancellation: CancellationToken,
        ) -> Result<TaskExecutionResult, WorkerFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.contexts.lock().unwrap().push(context.clone());
            self.result.clone()
        }
    }

    fn parent_llm_claim(timeout_ms: u64) -> SchedulerTaskClaim {
        let policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::ReadOnly,
            EffectIdempotency::Idempotent,
            1,
            1,
            1,
            timeout_ms,
            WorkerCancellation::Cooperative,
        )
        .unwrap();
        let intent = insight_engine::internal::scheduler_intent(
            RunId::new("run_parent_continuation_runtime").unwrap(),
            SchedulerCheckpointId::parse(format!("checkpoint_{}", "8".repeat(64))).unwrap(),
            SchedulerAction::DispatchTask {
                task_id: SchedulerTaskId::parse(format!("task_{}", "7".repeat(64))).unwrap(),
                effect_id: EffectId::new("effect_parent_continuation_runtime").unwrap(),
                activation_id: ActivationId::new("activation_parent_continuation_runtime").unwrap(),
                node_id: NodeId::new("parent_llm").unwrap(),
                admission_class: TaskAdmissionClass::Normal,
                task_kind: SchedulerTaskKind::Llm,
                implementation: "core.llm".to_owned(),
                descriptor_version: VersionTag::new("1").unwrap(),
                worker_version: VersionTag::new("mock-llm-1").unwrap(),
                effect_policy: policy,
                deployment_binding: json!({
                    "provider": "mock",
                    "model": "mock-model",
                    "tools": [{"name": "lookup"}],
                }),
                public_configuration: BTreeMap::from([(
                    "publish".to_owned(),
                    DescriptorValue::Boolean(false),
                )]),
                secret_configuration: BTreeMap::new(),
                inputs: Vec::new(),
                outputs: Vec::new(),
            },
        );
        let request = TaskExecutionRequest::from_scheduler_intent(&intent).unwrap();
        let envelope = insight_durable::DurableTaskExecutionRequest::new(
            request,
            AttemptNo::FIRST,
            LeaseEpoch::FIRST,
            "parent-continuation-fence".to_owned(),
            1,
        )
        .unwrap();
        SchedulerTaskClaim::new(
            envelope,
            "parent-continuation-worker".to_owned(),
            "parent-continuation-claim".to_owned(),
            Utc::now() + Duration::minutes(1),
            1,
            SchedulerTaskClaimMode::Execute,
        )
    }

    fn parent_continuation_turn() -> ModelContinuationTurn {
        let call =
            ModelToolCall::new(0, "call_parent_lookup", "lookup", json!({"query": "WBC"})).unwrap();
        let result =
            ModelToolResult::new("call_parent_lookup", json!({"value": "normal range"})).unwrap();
        ModelContinuationTurn::new(
            1,
            Some("I will check the reference range.".to_owned()),
            vec![call],
            vec![result],
        )
        .unwrap()
    }

    fn parent_stop_result(model_call_no: u32) -> TaskExecutionResult {
        TaskExecutionResult::new(BTreeMap::new(), EffectEvidence::Committed).with_model_call(
            ModelCallCompletion::new(model_call_no, ModelFinishReason::Stop, None, None, None)
                .unwrap(),
        )
    }

    fn parent_tool_batch_result(model_call_no: u32) -> TaskExecutionResult {
        let call = ModelToolCall::new(
            0,
            format!("call_round_{model_call_no}"),
            "lookup",
            json!({"query": "platelet"}),
        )
        .unwrap();
        let batch = ModelToolCallBatch::new(model_call_no, None, vec![call]).unwrap();
        TaskExecutionResult::new(BTreeMap::new(), EffectEvidence::Committed)
            .with_model_call(
                ModelCallCompletion::new(
                    model_call_no,
                    ModelFinishReason::ToolCalls,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            )
            .with_model_tool_call_batch(batch)
    }

    fn parent_llm_registry(
        result: Result<TaskExecutionResult, WorkerFailure>,
    ) -> (
        WorkerExecutorRegistry,
        Arc<AtomicUsize>,
        Arc<StdMutex<Vec<WorkerExecutionContext>>>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let contexts = Arc::new(StdMutex::new(Vec::new()));
        let mut registry = WorkerExecutorRegistry::new();
        registry
            .register(
                SchedulerTaskKind::Llm,
                "core.llm",
                VersionTag::new("1").unwrap(),
                VersionTag::new("mock-llm-1").unwrap(),
                Arc::new(RecordingParentLlmExecutor {
                    calls: Arc::clone(&calls),
                    contexts: Arc::clone(&contexts),
                    result,
                }),
            )
            .unwrap();
        (registry, calls, contexts)
    }

    fn parent_activation(
        claim: &SchedulerTaskClaim,
        model_call_no: u32,
    ) -> ModelToolBatchActivation {
        let identity = model_tool_worker_claim(30_000, json!({"type": "object"}))
            .identity()
            .clone();
        model_tool_batch_activation_new(
            claim.run_id().clone(),
            claim.activation_id().clone(),
            claim.envelope().attempt_no(),
            model_call_no,
            vec![identity],
        )
        .unwrap()
    }

    fn unexpected_parent_repository_call<T>() -> Result<T, RepositoryError> {
        panic!("unexpected parent-continuation repository call")
    }

    #[async_trait]
    impl DurableRepository for MockParentContinuationRepository {
        async fn install_versioned_plan(
            &self,
            _plan: &VersionedPlan,
        ) -> Result<PlanInstallOutcome, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn publish_versioned_plan(
            &self,
            _command: PublishVersionedPlanCommand,
        ) -> Result<PlanPublicationOutcome, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn publish_builtin_versioned_plan(
            &self,
            _plan: &VersionedPlan,
        ) -> Result<insight_durable::PublicationHead, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn load_versioned_plan_catalog(
            &self,
        ) -> Result<VersionedPlanCatalog, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn create_run(
            &self,
            _transition_key: TransitionKey,
            _command: CreateRunCommand,
        ) -> Result<TransitionOutcome<CommitReceipt>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn commit_run_transition(
            &self,
            _transition_key: TransitionKey,
            _command: RunTransitionCommand,
        ) -> Result<TransitionOutcome<CommitReceipt>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn load_run(
            &self,
            _run_id: &RunId,
        ) -> Result<Option<RunProjection>, RepositoryError> {
            unexpected_parent_repository_call()
        }
    }

    #[async_trait]
    impl ProjectionDurableRepository for MockParentContinuationRepository {
        async fn audit_projection(
            &self,
            _run_id: &RunId,
            _subject: &ProjectionSubject,
        ) -> Result<ProjectionAudit, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn load_authoritative_rebuild_snapshot(
            &self,
            _run_id: &RunId,
            _subject: &ProjectionSubject,
        ) -> Result<Option<ProjectionRebuildSnapshot>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn repair_projection(
            &self,
            _run_id: &RunId,
            _subject: &ProjectionSubject,
        ) -> Result<ProjectionRepairReceipt, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn repair_all_projections(
            &self,
            _run_id: &RunId,
        ) -> Result<Vec<ProjectionRepairReceipt>, RepositoryError> {
            unexpected_parent_repository_call()
        }
    }

    #[async_trait]
    impl ArtifactDurableRepository for MockParentContinuationRepository {
        async fn bind_artifact_store_authority(
            &self,
            _command: BindArtifactStoreAuthorityCommand,
        ) -> Result<TransitionOutcome<ArtifactStoreAuthority>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn put_inline_payload(
            &self,
            _command: PutInlinePayloadCommand,
        ) -> Result<TransitionOutcome<PayloadReceipt>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn get_inline_payload(
            &self,
            _run_id: &RunId,
            _payload_id: &PayloadId,
        ) -> Result<Option<StoredInlinePayload>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn get_retained_artifact(
            &self,
            _run_id: &RunId,
            _artifact_id: &insight_engine::ArtifactId,
        ) -> Result<Option<RetainedArtifact>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn stage_artifact(
            &self,
            _command: StageArtifactCommand,
        ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn verify_artifact(
            &self,
            _command: VerifyArtifactCommand,
        ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn reference_artifact(
            &self,
            _command: ReferenceArtifactCommand,
        ) -> Result<TransitionOutcome<ArtifactReferenceAuthority>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn list_unreleased_terminal_artifact_runs(
            &self,
            _limit: u32,
        ) -> Result<Vec<RunId>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn release_run_artifact_retention(
            &self,
            _transition_key: TransitionKey,
            _command: ReleaseRunArtifactRetentionCommand,
        ) -> Result<TransitionOutcome<ArtifactRetentionRelease>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn sweep_orphan_artifacts(
            &self,
            _transition_key: TransitionKey,
            _command: OrphanSweepCommand,
        ) -> Result<TransitionOutcome<OrphanSweepBatch>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn acknowledge_artifact_deleted(
            &self,
            _command: AcknowledgeArtifactDeletionCommand,
        ) -> Result<TransitionOutcome<ArtifactReceipt>, RepositoryError> {
            unexpected_parent_repository_call()
        }
    }

    #[async_trait]
    impl SchedulerDurableRepository for MockParentContinuationRepository {
        async fn load_scheduler_facts(
            &self,
            _run_id: &RunId,
        ) -> Result<SchedulerFacts, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn commit_scheduler_action(
            &self,
            _fence: &FencedSchedulerRunCommand,
            _action: &PlannedSchedulerAction,
        ) -> Result<TransitionOutcome<SchedulerCommitReceipt>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn claim_scheduler_tasks(
            &self,
            _claimed_by: &str,
            _claim_seconds: u32,
            limit: u32,
        ) -> Result<Vec<SchedulerTaskClaim>, RepositoryError> {
            assert_eq!(limit, 1);
            self.push_event("claim").await;
            Ok(self.claim.lock().await.take().into_iter().collect())
        }

        async fn mark_scheduler_task_started(
            &self,
            _claim: &SchedulerTaskClaim,
        ) -> Result<TransitionOutcome<SchedulerCommitReceipt>, RepositoryError> {
            self.mark_started_count.fetch_add(1, Ordering::SeqCst);
            self.push_event("mark_started").await;
            Ok(TransitionOutcome::Committed {
                result: parent_scheduler_commit_receipt(),
            })
        }

        async fn reserve_model_call(
            &self,
            _claim: &SchedulerTaskClaim,
            model_call_no: u32,
            publish: bool,
        ) -> Result<SchedulerTaskCommitOutcome<ModelCallAuthority>, RepositoryError> {
            self.push_event("reserve_model_call").await;
            self.provider_reservations
                .lock()
                .await
                .push((model_call_no, publish));
            Ok(SchedulerTaskCommitOutcome::Committed {
                result: ModelCallAuthority::new("resp_parent_continuation", model_call_no, None)
                    .unwrap(),
            })
        }

        async fn checkpoint_model_call_completion(
            &self,
            _claim: &SchedulerTaskClaim,
            completion: &ModelCallCompletion,
        ) -> Result<SchedulerTaskCommitOutcome<()>, RepositoryError> {
            self.push_event("checkpoint_completion").await;
            self.checkpointed_completions
                .lock()
                .await
                .push(completion.clone());
            Ok(parent_checkpoint_outcome(self.checkpoint_step))
        }

        async fn checkpoint_model_tool_call_batch(
            &self,
            _claim: &SchedulerTaskClaim,
            _checkpoint: &ModelToolCallCheckpoint,
        ) -> Result<SchedulerTaskCommitOutcome<()>, RepositoryError> {
            self.push_event("checkpoint_tool_batch").await;
            Ok(parent_checkpoint_outcome(self.checkpoint_step))
        }

        async fn load_model_tool_parent_resume(
            &self,
            _claim: &SchedulerTaskClaim,
        ) -> Result<SchedulerTaskCommitOutcome<Option<ModelToolParentResume>>, RepositoryError>
        {
            self.push_event("load_resume").await;
            Ok(self.resume.clone())
        }

        async fn activate_model_tool_call_batch(
            &self,
            _parent_claim: &SchedulerTaskClaim,
            _model_call_no: u32,
        ) -> Result<ModelToolBatchActivationOutcome, RepositoryError> {
            self.push_event("activate_tool_batch").await;
            self.activation_steps
                .lock()
                .await
                .pop_front()
                .ok_or_else(repository_invalid_configuration)
        }

        async fn heartbeat_scheduler_task(
            &self,
            claim: &SchedulerTaskClaim,
            _claim_seconds: u32,
        ) -> Result<SchedulerTaskHeartbeatOutcome, RepositoryError> {
            self.push_event("heartbeat").await;
            let step = self
                .heartbeat_steps
                .lock()
                .await
                .pop_front()
                .unwrap_or(ParentHeartbeatStep::Renewed);
            Ok(match step {
                ParentHeartbeatStep::Renewed => {
                    SchedulerTaskHeartbeatOutcome::Renewed(claim.clone())
                }
                ParentHeartbeatStep::DeadlineElapsed => {
                    SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(claim.clone())
                }
                ParentHeartbeatStep::LeaseLost => SchedulerTaskHeartbeatOutcome::LeaseLost,
            })
        }

        async fn commit_scheduler_task_outcome(
            &self,
            claim: &SchedulerTaskClaim,
            outcome: &SchedulerTaskOutcome,
        ) -> Result<SchedulerTaskCommitOutcome<SchedulerTaskCompletionReceipt>, RepositoryError>
        {
            self.push_event("commit_parent").await;
            self.committed_outcomes.lock().await.push(outcome.clone());
            Ok(SchedulerTaskCommitOutcome::Committed {
                result: parent_task_completion_receipt(claim),
            })
        }

        async fn acknowledge_scheduler_task(
            &self,
            _claim: &SchedulerTaskClaim,
        ) -> Result<bool, RepositoryError> {
            self.push_event("acknowledge").await;
            Ok(true)
        }

        async fn load_scheduler_value(
            &self,
            _run_id: &RunId,
            _port_id: &DataPortId,
        ) -> Result<Option<SchedulerStoredValue>, RepositoryError> {
            unexpected_parent_repository_call()
        }

        async fn list_recoverable_scheduler_runs(
            &self,
            _limit: u32,
        ) -> Result<Vec<RunId>, RepositoryError> {
            unexpected_parent_repository_call()
        }
    }

    fn parent_checkpoint_outcome(step: ParentCheckpointStep) -> SchedulerTaskCommitOutcome<()> {
        match step {
            ParentCheckpointStep::Committed => SchedulerTaskCommitOutcome::Committed { result: () },
            ParentCheckpointStep::ExactReplay => {
                SchedulerTaskCommitOutcome::ExactReplay { authoritative: () }
            }
            ParentCheckpointStep::StaleLease => SchedulerTaskCommitOutcome::StaleLease,
            ParentCheckpointStep::StateConflict => SchedulerTaskCommitOutcome::StateConflict,
            ParentCheckpointStep::DeadlineElapsed => {
                SchedulerTaskCommitOutcome::OperationDeadlineElapsed
            }
        }
    }

    fn parent_scheduler_commit_receipt() -> SchedulerCommitReceipt {
        SchedulerCommitReceipt::new(
            1,
            "event_parent_continuation".to_owned(),
            SchedulerCheckpointId::parse(format!("checkpoint_{}", "9".repeat(64))).unwrap(),
            1,
        )
    }

    fn parent_task_completion_receipt(
        claim: &SchedulerTaskClaim,
    ) -> SchedulerTaskCompletionReceipt {
        SchedulerTaskCompletionReceipt::new(
            parent_scheduler_commit_receipt(),
            claim.task_id().clone(),
            claim.envelope().attempt_no(),
            claim.envelope().lease_epoch(),
            BTreeMap::new(),
        )
    }

    #[tokio::test]
    async fn parent_ready_continue_restores_deadline_and_transcript_reserves_next_call_without_mark_start(
    ) {
        let claim = parent_llm_claim(120_000);
        let original_deadline = Utc::now() + Duration::seconds(45);
        let turn = parent_continuation_turn();
        let resume =
            model_tool_parent_resume_ready_continue(vec![turn.clone()], original_deadline).unwrap();
        let repository = MockParentContinuationRepository::new(claim, Some(resume), []);
        let (registry, provider_calls, contexts) = parent_llm_registry(Ok(parent_stop_result(2)));

        let outcome = consume_scheduler_task_once(
            &repository,
            &registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "parent-continuation-worker",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            SchedulerWorkerPumpOutcome::Committed {
                acknowledged: true,
                ..
            }
        ));
        assert_eq!(repository.mark_started_count.load(Ordering::SeqCst), 0);
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *repository.provider_reservations.lock().await,
            vec![(2, false)]
        );
        {
            let contexts = contexts.lock().unwrap();
            assert_eq!(contexts.len(), 1);
            assert_eq!(contexts[0].deadline(), original_deadline);
            assert_eq!(contexts[0].model_call().unwrap().model_call_no(), 2);
            assert_eq!(contexts[0].continuation_turns(), &[turn]);
        }
        assert_eq!(
            *repository.events.lock().await,
            vec![
                "claim",
                "load_resume",
                "heartbeat",
                "reserve_model_call",
                "checkpoint_completion",
                "heartbeat",
                "commit_parent",
                "acknowledge",
            ]
        );
    }

    #[tokio::test]
    async fn parent_ready_failure_unknown_and_cancellation_never_call_provider_and_commit_closed_terminal(
    ) {
        let deadline = Utc::now() + Duration::seconds(45);
        let cases = [
            (
                model_tool_parent_resume_ready_failed(1, deadline, false).unwrap(),
                WorkerFailureClass::InfrastructureFailure,
                LLM_TOOL_EXECUTION_FAILED,
                EffectEvidence::Started,
            ),
            (
                model_tool_parent_resume_ready_failed(1, deadline, true).unwrap(),
                WorkerFailureClass::EffectOutcomeUnknown,
                LLM_TOOL_EFFECT_OUTCOME_UNKNOWN,
                EffectEvidence::Unknown,
            ),
            (
                model_tool_parent_resume_ready_cancelled(1, deadline).unwrap(),
                WorkerFailureClass::ControlTermination,
                LLM_TOOL_EXECUTION_CANCELLED,
                EffectEvidence::Started,
            ),
        ];

        for (resume, expected_class, expected_code, expected_evidence) in cases {
            let repository =
                MockParentContinuationRepository::new(parent_llm_claim(120_000), Some(resume), []);
            let (registry, provider_calls, contexts) =
                parent_llm_registry(Ok(parent_stop_result(2)));

            let outcome = consume_scheduler_task_once(
                &repository,
                &registry,
                &TerminalSchedulerWorkerFailurePolicy,
                "parent-terminal-worker",
                30,
                4,
                CancellationToken::new(),
                &insight_durable::NoSchedulerCrash,
            )
            .await
            .unwrap();

            assert!(matches!(
                outcome,
                SchedulerWorkerPumpOutcome::Committed {
                    acknowledged: false,
                    ..
                }
            ));
            assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
            assert!(contexts.lock().unwrap().is_empty());
            assert_eq!(repository.mark_started_count.load(Ordering::SeqCst), 0);
            assert!(repository.provider_reservations.lock().await.is_empty());
            let committed = repository.committed_outcomes.lock().await;
            assert_eq!(committed.len(), 1);
            let SchedulerTaskOutcome::Failed(failure) = &committed[0] else {
                panic!("ready terminal state must commit a failure")
            };
            assert_eq!(failure.class(), expected_class);
            assert_eq!(failure.code(), expected_code);
            assert_eq!(failure.effect_evidence(), expected_evidence);
            assert!(!failure.retryable());
            assert_eq!(
                failure.disposition(),
                &SchedulerFailureDisposition::Terminal
            );
        }
    }

    #[tokio::test]
    async fn checkpointed_parent_handoff_activated_and_exact_replay_suspend_without_provider() {
        for exact_replay in [false, true] {
            let claim = parent_llm_claim(120_000);
            let expected = parent_activation(&claim, 1);
            let activation = if exact_replay {
                ModelToolBatchActivationOutcome::ExactReplay(expected.clone())
            } else {
                ModelToolBatchActivationOutcome::Activated(expected.clone())
            };
            let resume = model_tool_parent_resume_activate_checkpointed(
                1,
                Utc::now() + Duration::seconds(45),
            )
            .unwrap();
            let repository =
                MockParentContinuationRepository::new(claim, Some(resume), [activation]);
            let (registry, provider_calls, contexts) =
                parent_llm_registry(Ok(parent_stop_result(2)));

            let outcome = consume_scheduler_task_once(
                &repository,
                &registry,
                &TerminalSchedulerWorkerFailurePolicy,
                "checkpoint-handoff-worker",
                30,
                4,
                CancellationToken::new(),
                &insight_durable::NoSchedulerCrash,
            )
            .await
            .unwrap();

            assert_eq!(
                outcome,
                SchedulerWorkerPumpOutcome::SuspendedForTools {
                    activation: expected,
                    exact_replay,
                }
            );
            assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
            assert!(contexts.lock().unwrap().is_empty());
            assert_eq!(repository.mark_started_count.load(Ordering::SeqCst), 0);
            assert!(repository.committed_outcomes.lock().await.is_empty());
            assert_eq!(
                *repository.events.lock().await,
                vec!["claim", "load_resume", "heartbeat", "activate_tool_batch"]
            );
        }
    }

    #[tokio::test]
    async fn checkpointed_parent_handoff_limits_commit_stable_failures_without_provider() {
        for (activation, expected_code) in [
            (
                ModelToolBatchActivationOutcome::RoundLimitExceeded,
                LLM_TOOL_ROUND_LIMIT,
            ),
            (
                ModelToolBatchActivationOutcome::CallLimitExceeded,
                LLM_TOOL_CALL_LIMIT,
            ),
        ] {
            let resume = model_tool_parent_resume_activate_checkpointed(
                1,
                Utc::now() + Duration::seconds(45),
            )
            .unwrap();
            let repository = MockParentContinuationRepository::new(
                parent_llm_claim(120_000),
                Some(resume),
                [activation],
            );
            let (registry, provider_calls, contexts) =
                parent_llm_registry(Ok(parent_stop_result(2)));

            let outcome = consume_scheduler_task_once(
                &repository,
                &registry,
                &TerminalSchedulerWorkerFailurePolicy,
                "checkpoint-limit-worker",
                30,
                4,
                CancellationToken::new(),
                &insight_durable::NoSchedulerCrash,
            )
            .await
            .unwrap();

            assert!(matches!(
                outcome,
                SchedulerWorkerPumpOutcome::Committed {
                    acknowledged: false,
                    ..
                }
            ));
            assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
            assert!(contexts.lock().unwrap().is_empty());
            assert_eq!(repository.mark_started_count.load(Ordering::SeqCst), 0);
            let committed = repository.committed_outcomes.lock().await;
            let SchedulerTaskOutcome::Failed(failure) = &committed[0] else {
                panic!("limit handoff must commit a stable parent failure")
            };
            assert_eq!(failure.class(), WorkerFailureClass::InfrastructureFailure);
            assert_eq!(failure.code(), expected_code);
            assert_eq!(failure.effect_evidence(), EffectEvidence::Started);
            assert!(!failure.retryable());
        }
    }

    #[tokio::test]
    async fn checkpointed_parent_handoff_stale_terminal_and_conflict_are_fenced_without_provider() {
        for activation in [
            ModelToolBatchActivationOutcome::StaleParentLease,
            ModelToolBatchActivationOutcome::RunTerminal,
        ] {
            let resume = model_tool_parent_resume_activate_checkpointed(
                1,
                Utc::now() + Duration::seconds(45),
            )
            .unwrap();
            let repository = MockParentContinuationRepository::new(
                parent_llm_claim(120_000),
                Some(resume),
                [activation],
            );
            let (registry, provider_calls, _) = parent_llm_registry(Ok(parent_stop_result(2)));
            let outcome = consume_scheduler_task_once(
                &repository,
                &registry,
                &TerminalSchedulerWorkerFailurePolicy,
                "checkpoint-stale-worker",
                30,
                4,
                CancellationToken::new(),
                &insight_durable::NoSchedulerCrash,
            )
            .await
            .unwrap();
            assert_eq!(
                outcome,
                SchedulerWorkerPumpOutcome::LeaseLost {
                    effect_evidence: EffectEvidence::Started,
                }
            );
            assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
            assert!(repository.committed_outcomes.lock().await.is_empty());
        }

        let resume =
            model_tool_parent_resume_activate_checkpointed(1, Utc::now() + Duration::seconds(45))
                .unwrap();
        let repository = MockParentContinuationRepository::new(
            parent_llm_claim(120_000),
            Some(resume),
            [ModelToolBatchActivationOutcome::StateConflict],
        );
        let (registry, provider_calls, _) = parent_llm_registry(Ok(parent_stop_result(2)));
        let error = consume_scheduler_task_once(
            &repository,
            &registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "checkpoint-conflict-worker",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), REPOSITORY_DATA_INVALID);
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn provider_tool_checkpoint_is_durable_before_activation_and_then_suspends_parent() {
        for checkpoint_exact_replay in [false, true] {
            let claim = parent_llm_claim(120_000);
            let expected = parent_activation(&claim, 1);
            let repository = MockParentContinuationRepository::new(
                claim,
                None,
                [ModelToolBatchActivationOutcome::Activated(expected.clone())],
            )
            .with_checkpoint_step(if checkpoint_exact_replay {
                ParentCheckpointStep::ExactReplay
            } else {
                ParentCheckpointStep::Committed
            });
            let (registry, provider_calls, contexts) =
                parent_llm_registry(Ok(parent_tool_batch_result(1)));

            let outcome = consume_scheduler_task_once(
                &repository,
                &registry,
                &TerminalSchedulerWorkerFailurePolicy,
                "initial-tool-call-worker",
                30,
                4,
                CancellationToken::new(),
                &insight_durable::NoSchedulerCrash,
            )
            .await
            .unwrap();

            assert_eq!(
                outcome,
                SchedulerWorkerPumpOutcome::SuspendedForTools {
                    activation: expected,
                    exact_replay: false,
                }
            );
            assert_eq!(repository.mark_started_count.load(Ordering::SeqCst), 1);
            assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
            assert_eq!(contexts.lock().unwrap().len(), 1);
            assert_eq!(
                *repository.provider_reservations.lock().await,
                vec![(1, false)]
            );
            assert!(repository.committed_outcomes.lock().await.is_empty());
            let events = repository.events.lock().await.clone();
            let checkpoint = events
                .iter()
                .position(|event| *event == "checkpoint_tool_batch")
                .unwrap();
            let activation = events
                .iter()
                .position(|event| *event == "activate_tool_batch")
                .unwrap();
            assert!(checkpoint < activation);
            assert_eq!(
                events,
                vec![
                    "claim",
                    "load_resume",
                    "mark_started",
                    "heartbeat",
                    "reserve_model_call",
                    "checkpoint_tool_batch",
                    "heartbeat",
                    "activate_tool_batch",
                ]
            );
        }
    }

    #[tokio::test]
    async fn provider_tool_checkpoint_crash_recovers_without_replaying_provider() {
        let initial_claim = parent_llm_claim(120_000);
        let initial_repository = MockParentContinuationRepository::new(
            initial_claim,
            None,
            [ModelToolBatchActivationOutcome::StateConflict],
        );
        let (initial_registry, initial_provider_calls, _) =
            parent_llm_registry(Ok(parent_tool_batch_result(1)));
        let crash = insight_durable::FailOnceSchedulerCrash::new(
            SchedulerCrashPoint::BeforeDownstreamAdmission,
        );

        let error = consume_scheduler_task_once(
            &initial_repository,
            &initial_registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "checkpoint-crash-worker",
            30,
            4,
            CancellationToken::new(),
            &crash,
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED");
        assert!(crash.fired());
        assert_eq!(initial_provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *initial_repository.events.lock().await,
            vec![
                "claim",
                "load_resume",
                "mark_started",
                "heartbeat",
                "reserve_model_call",
                "checkpoint_tool_batch",
                "heartbeat",
            ],
            "the crash cut is after the immutable batch checkpoint but before any child authority exists",
        );

        // The SQLite/PostgreSQL repository tests prove that an expired parent
        // is reconstructed in this exact state from the durable checkpoint.
        // This runtime half proves recovery enters activation directly and
        // cannot make the preceding Provider request a second time.
        let recovered_claim = parent_llm_claim(120_000);
        let activation = parent_activation(&recovered_claim, 1);
        let resume =
            model_tool_parent_resume_activate_checkpointed(1, Utc::now() + Duration::seconds(45))
                .unwrap();
        let recovered_repository = MockParentContinuationRepository::new(
            recovered_claim,
            Some(resume),
            [ModelToolBatchActivationOutcome::Activated(
                activation.clone(),
            )],
        );
        let (recovered_registry, recovered_provider_calls, recovered_contexts) =
            parent_llm_registry(Ok(parent_stop_result(2)));

        let recovered = consume_scheduler_task_once(
            &recovered_repository,
            &recovered_registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "checkpoint-recovery-worker",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert_eq!(
            recovered,
            SchedulerWorkerPumpOutcome::SuspendedForTools {
                activation,
                exact_replay: false,
            }
        );
        assert_eq!(recovered_provider_calls.load(Ordering::SeqCst), 0);
        assert!(recovered_contexts.lock().unwrap().is_empty());
        assert_eq!(
            *recovered_repository.events.lock().await,
            vec!["claim", "load_resume", "heartbeat", "activate_tool_batch"]
        );
    }

    #[tokio::test]
    async fn provider_tool_materialization_commit_crash_keeps_parent_suspended_without_provider_replay(
    ) {
        let claim = parent_llm_claim(120_000);
        let activation = parent_activation(&claim, 1);
        let repository = MockParentContinuationRepository::new(
            claim,
            None,
            [ModelToolBatchActivationOutcome::Activated(activation)],
        );
        let (initial_registry, initial_provider_calls, initial_contexts) =
            parent_llm_registry(Ok(parent_tool_batch_result(1)));
        let crash = insight_durable::FailOnceSchedulerCrash::new(
            SchedulerCrashPoint::AfterDownstreamAdmission,
        );

        let error = consume_scheduler_task_once(
            &repository,
            &initial_registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "materialization-crash-worker",
            30,
            4,
            CancellationToken::new(),
            &crash,
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED");
        assert!(crash.fired());
        assert_eq!(initial_provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(initial_contexts.lock().unwrap().len(), 1);
        assert!(repository.activation_steps.lock().await.is_empty());
        assert_eq!(
            *repository.events.lock().await,
            vec![
                "claim",
                "load_resume",
                "mark_started",
                "heartbeat",
                "reserve_model_call",
                "checkpoint_tool_batch",
                "heartbeat",
                "activate_tool_batch",
            ],
            "the post-materialization crash hook is reached only after the checkpoint and the successful activation transaction",
        );

        // A committed active/waiting_tools batch owns continuation authority.
        // The suspended parent therefore cannot be reclaimed on restart; only
        // the durable child queue may make progress until its barrier wakes the
        // parent. In particular, recovery must not replay model call 1.
        let (recovery_registry, recovery_provider_calls, recovery_contexts) =
            parent_llm_registry(Ok(parent_stop_result(2)));
        let recovery = consume_scheduler_task_once(
            &repository,
            &recovery_registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "materialization-recovery-worker",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert_eq!(recovery, SchedulerWorkerPumpOutcome::NoTask);
        assert_eq!(recovery_provider_calls.load(Ordering::SeqCst), 0);
        assert!(recovery_contexts.lock().unwrap().is_empty());
        assert_eq!(
            *repository.events.lock().await,
            vec![
                "claim",
                "load_resume",
                "mark_started",
                "heartbeat",
                "reserve_model_call",
                "checkpoint_tool_batch",
                "heartbeat",
                "activate_tool_batch",
                "claim",
            ],
        );
    }

    #[tokio::test]
    async fn invalid_provider_tool_payload_checkpoints_usage_then_fails_without_waiting_for_run_deadline(
    ) {
        let repository = MockParentContinuationRepository::new(parent_llm_claim(120_000), None, []);
        let usage = ModelTokenUsage {
            input_tokens: Some(10),
            cached_tokens: Some(1),
            output_tokens: Some(2),
            reasoning_tokens: Some(0),
            total_tokens: Some(12),
        };
        let failure = WorkerFailure::new(
            WorkerFailureClass::InfrastructureFailure,
            "LLM_TOOL_CALL_INVALID",
            false,
        )
        .unwrap()
        .with_model_call(
            ModelCallCompletion::new(
                1,
                ModelFinishReason::Invalid,
                Some(usage.clone()),
                None,
                None,
            )
            .unwrap(),
        );
        let (registry, provider_calls, _) = parent_llm_registry(Err(failure));

        let outcome = consume_scheduler_task_once(
            &repository,
            &registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "invalid-tool-payload-worker",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            SchedulerWorkerPumpOutcome::Committed {
                acknowledged: false,
                ..
            }
        ));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        let checkpointed = repository.checkpointed_completions.lock().await;
        assert_eq!(checkpointed.len(), 1);
        assert_eq!(checkpointed[0].finish_reason(), ModelFinishReason::Invalid);
        assert_eq!(checkpointed[0].usage(), Some(&usage));
        drop(checkpointed);
        let committed = repository.committed_outcomes.lock().await;
        assert_eq!(committed.len(), 1);
        let SchedulerTaskOutcome::Failed(failure) = &committed[0] else {
            panic!("invalid tool payload must commit a terminal failure")
        };
        assert_eq!(failure.code(), "LLM_TOOL_CALL_INVALID");
        assert_eq!(failure.effect_evidence(), EffectEvidence::Started);
        assert_eq!(
            *repository.events.lock().await,
            vec![
                "claim",
                "load_resume",
                "mark_started",
                "heartbeat",
                "reserve_model_call",
                "checkpoint_completion",
                "heartbeat",
                "commit_parent",
            ]
        );
    }

    #[tokio::test]
    async fn parent_resume_deadline_authority_precedes_tool_terminal_state() {
        let expired_deadline = Utc::now() - Duration::seconds(1);
        let resume = model_tool_parent_resume_ready_failed(1, expired_deadline, false).unwrap();
        let repository =
            MockParentContinuationRepository::new(parent_llm_claim(120_000), Some(resume), [])
                .with_heartbeats([ParentHeartbeatStep::DeadlineElapsed]);
        let (registry, provider_calls, contexts) = parent_llm_registry(Ok(parent_stop_result(2)));

        let outcome = consume_scheduler_task_once(
            &repository,
            &registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "deadline-priority-worker",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            SchedulerWorkerPumpOutcome::Committed {
                acknowledged: false,
                ..
            }
        ));
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(contexts.lock().unwrap().is_empty());
        assert_eq!(repository.mark_started_count.load(Ordering::SeqCst), 0);
        let committed = repository.committed_outcomes.lock().await;
        let SchedulerTaskOutcome::Failed(failure) = &committed[0] else {
            panic!("deadline authority must commit a failure")
        };
        assert_eq!(failure.code(), "WORKER_DEADLINE_EXCEEDED");
        assert_eq!(failure.class(), WorkerFailureClass::EffectOutcomeUnknown);
        assert_eq!(failure.effect_evidence(), EffectEvidence::Unknown);
        assert_eq!(
            failure.disposition(),
            &SchedulerFailureDisposition::TimedOut
        );
    }

    #[tokio::test]
    async fn parent_resume_initial_heartbeat_lease_loss_never_reserves_or_calls_provider() {
        let resume = model_tool_parent_resume_ready_continue(
            vec![parent_continuation_turn()],
            Utc::now() + Duration::seconds(45),
        )
        .unwrap();
        let repository =
            MockParentContinuationRepository::new(parent_llm_claim(120_000), Some(resume), [])
                .with_heartbeats([ParentHeartbeatStep::LeaseLost]);
        let (registry, provider_calls, contexts) = parent_llm_registry(Ok(parent_stop_result(2)));

        let outcome = consume_scheduler_task_once(
            &repository,
            &registry,
            &TerminalSchedulerWorkerFailurePolicy,
            "continuation-stale-worker",
            30,
            4,
            CancellationToken::new(),
            &insight_durable::NoSchedulerCrash,
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            SchedulerWorkerPumpOutcome::LeaseLost {
                effect_evidence: EffectEvidence::Unknown,
            }
        );
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(contexts.lock().unwrap().is_empty());
        assert!(repository.provider_reservations.lock().await.is_empty());
        assert!(repository.committed_outcomes.lock().await.is_empty());
    }

    #[tokio::test]
    async fn provider_checkpoint_fence_and_deadline_outcomes_prevent_tool_activation() {
        for checkpoint_step in [
            ParentCheckpointStep::StaleLease,
            ParentCheckpointStep::StateConflict,
            ParentCheckpointStep::DeadlineElapsed,
        ] {
            let repository = MockParentContinuationRepository::new(
                parent_llm_claim(120_000),
                None,
                [ModelToolBatchActivationOutcome::StateConflict],
            )
            .with_checkpoint_step(checkpoint_step);
            let (registry, provider_calls, _) =
                parent_llm_registry(Ok(parent_tool_batch_result(1)));

            let result = consume_scheduler_task_once(
                &repository,
                &registry,
                &TerminalSchedulerWorkerFailurePolicy,
                "checkpoint-fence-worker",
                30,
                4,
                CancellationToken::new(),
                &insight_durable::NoSchedulerCrash,
            )
            .await;

            assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
            assert!(!repository
                .events
                .lock()
                .await
                .contains(&"activate_tool_batch"));
            match checkpoint_step {
                ParentCheckpointStep::StaleLease => assert_eq!(
                    result.unwrap(),
                    SchedulerWorkerPumpOutcome::LeaseLost {
                        effect_evidence: EffectEvidence::Committed,
                    }
                ),
                ParentCheckpointStep::StateConflict => {
                    assert_eq!(result.unwrap_err().code(), REPOSITORY_DATA_INVALID)
                }
                ParentCheckpointStep::DeadlineElapsed => {
                    assert!(matches!(
                        result.unwrap(),
                        SchedulerWorkerPumpOutcome::Committed {
                            acknowledged: false,
                            ..
                        }
                    ));
                    let committed = repository.committed_outcomes.lock().await;
                    let SchedulerTaskOutcome::Failed(failure) = &committed[0] else {
                        panic!("checkpoint deadline must commit a timeout")
                    };
                    assert_eq!(failure.code(), "WORKER_DEADLINE_EXCEEDED");
                    assert_eq!(
                        failure.disposition(),
                        &SchedulerFailureDisposition::TimedOut
                    );
                }
                ParentCheckpointStep::Committed | ParentCheckpointStep::ExactReplay => {
                    unreachable!()
                }
            }
        }
    }
}
