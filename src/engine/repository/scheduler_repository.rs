//! Durable boundary between the pure scheduler planner and repository-owned
//! projections, task delivery, and recovery.

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

use crate::engine::{
    plan::{DataPortId, NodeKind, Plan, PlanIndex, PlanType, VersionTag},
    worker::{
        ModelCallAuthority, ModelCallCompletion, ModelFinishReason, ModelToolCallBatch,
        ResponseItemAuthority, TaskExecutionRequest, TaskExecutionResult, WorkerFailure,
        WorkerFailureClass,
    },
    ActivationId, AttemptNo, ContentHash, DefinitionRevisionId, DeploymentRevisionId,
    EffectEvidence, ExecutionRevisionPin, LeaseEpoch, LeaseFence, NodeId, PlannedSchedulerAction,
    RunId, RuntimeValue, SafeError, SchedulerAction, SchedulerCheckpointId, SchedulerFacts,
    SchedulerTaskId, TransitionOutcome, ValueRef,
};

#[cfg(test)]
use crate::engine::EffectIdempotency;

use super::{
    ArtifactDurableRepository, DurableRepository, FencedSchedulerRunCommand,
    ModelToolBatchActivationOutcome, ModelToolParentResume, ModelToolTaskClaim,
    ModelToolTaskCommitReceipt, ModelToolTaskHeartbeatOutcome, ModelToolTaskOutcome,
    ModelToolTaskTransitionOutcome, ProjectionDurableRepository, RepositoryError,
};

pub const SCHEDULER_CHECKPOINT_SCHEMA_VERSION: u32 = 2;
pub const SCHEDULER_TASK_ENVELOPE_SCHEMA_VERSION: u32 = 3;
pub(crate) const SCHEDULER_WORKER_DEADLINE_EXCEEDED: &str = "WORKER_DEADLINE_EXCEEDED";

/// Closed durable record for a model call which stopped to request tools.
///
/// This deliberately records only the model boundary: it neither changes an
/// activation/task state nor starts a tool.  The later scheduler handoff owns
/// those transitions.  Keeping the completion and batch inseparable here is
/// what prevents a crash from leaving an apparently completed `tool_calls`
/// model call without the exact calls needed to continue it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelToolCallCheckpoint {
    completion: ModelCallCompletion,
    batch: ModelToolCallBatch,
}

impl ModelToolCallCheckpoint {
    pub fn new(
        completion: ModelCallCompletion,
        batch: ModelToolCallBatch,
    ) -> Result<Self, &'static str> {
        if completion.finish_reason() != ModelFinishReason::ToolCalls
            || completion.model_call_no() != batch.model_call_no()
            || batch.calls().is_empty()
        {
            return Err("MODEL_TOOL_CALL_CHECKPOINT_INVALID");
        }
        Ok(Self { completion, batch })
    }

    pub fn completion(&self) -> &ModelCallCompletion {
        &self.completion
    }

    pub fn batch(&self) -> &ModelToolCallBatch {
        &self.batch
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerCommitReceipt {
    event_seq: u64,
    event_id: String,
    checkpoint_id: SchedulerCheckpointId,
    scheduler_projection_version: u64,
}

impl SchedulerCommitReceipt {
    pub(crate) fn new(
        event_seq: u64,
        event_id: String,
        checkpoint_id: SchedulerCheckpointId,
        scheduler_projection_version: u64,
    ) -> Self {
        Self {
            event_seq,
            event_id,
            checkpoint_id,
            scheduler_projection_version,
        }
    }

    pub fn event_seq(&self) -> u64 {
        self.event_seq
    }

    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    pub fn checkpoint_id(&self) -> &SchedulerCheckpointId {
        &self.checkpoint_id
    }

    pub fn scheduler_projection_version(&self) -> u64 {
        self.scheduler_projection_version
    }
}

/// Complete immutable worker request plus the repository-minted attempt fence.
/// This value, not the legacy compact task envelope, is serialized into the
/// durable task outbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableTaskExecutionRequest {
    schema_version: u32,
    request: TaskExecutionRequest,
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    dispatch_scheduler_projection_version: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableTaskExecutionRequestWire {
    schema_version: u32,
    request: TaskExecutionRequest,
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    dispatch_scheduler_projection_version: u64,
}

impl<'de> Deserialize<'de> for DurableTaskExecutionRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = DurableTaskExecutionRequestWire::deserialize(deserializer)?;
        Self::build(
            wire.schema_version,
            wire.request,
            wire.attempt_no,
            wire.lease_epoch,
            wire.fencing_token,
            wire.dispatch_scheduler_projection_version,
        )
        .map_err(D::Error::custom)
    }
}

impl DurableTaskExecutionRequest {
    pub(crate) fn new(
        request: TaskExecutionRequest,
        attempt_no: AttemptNo,
        lease_epoch: LeaseEpoch,
        fencing_token: String,
        dispatch_scheduler_projection_version: u64,
    ) -> Result<Self, RepositoryError> {
        Self::build(
            SCHEDULER_TASK_ENVELOPE_SCHEMA_VERSION,
            request,
            attempt_no,
            lease_epoch,
            fencing_token,
            dispatch_scheduler_projection_version,
        )
    }

    fn build(
        schema_version: u32,
        request: TaskExecutionRequest,
        attempt_no: AttemptNo,
        lease_epoch: LeaseEpoch,
        fencing_token: String,
        dispatch_scheduler_projection_version: u64,
    ) -> Result<Self, RepositoryError> {
        if schema_version != SCHEDULER_TASK_ENVELOPE_SCHEMA_VERSION {
            return Err(RepositoryError::invalid_data());
        }
        if fencing_token.is_empty() || fencing_token.chars().any(char::is_control) {
            return Err(RepositoryError::invalid_configuration());
        }
        LeaseFence::new(attempt_no, lease_epoch).map_err(|_| RepositoryError::invalid_data())?;
        Ok(Self {
            schema_version,
            request,
            attempt_no,
            lease_epoch,
            fencing_token,
            dispatch_scheduler_projection_version,
        })
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn request(&self) -> &TaskExecutionRequest {
        &self.request
    }

    pub fn attempt_no(&self) -> AttemptNo {
        self.attempt_no
    }

    pub fn lease_epoch(&self) -> LeaseEpoch {
        self.lease_epoch
    }

    pub fn fencing_token(&self) -> &str {
        &self.fencing_token
    }

    pub fn dispatch_scheduler_projection_version(&self) -> u64 {
        self.dispatch_scheduler_projection_version
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerTaskClaimMode {
    Execute,
    /// A previous worker lease expired after the effect may have started.
    /// This claim may only durably finalize that lost Attempt; it must never
    /// invoke the external implementation again.
    FinalizeLeaseLoss,
    Acknowledge,
}

impl SchedulerTaskClaimMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Execute => "execute",
            Self::FinalizeLeaseLoss => "finalize_lease_loss",
            Self::Acknowledge => "acknowledge",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "execute" => Ok(Self::Execute),
            "finalize_lease_loss" => Ok(Self::FinalizeLeaseLoss),
            "acknowledge" => Ok(Self::Acknowledge),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
// The renewed claim is deliberately returned by value: it replaces the local
// fencing snapshot atomically and avoids a heap allocation on every heartbeat.
#[allow(clippy::large_enum_variant)]
pub enum SchedulerTaskHeartbeatOutcome {
    Renewed(SchedulerTaskClaim),
    /// The task/Attempt claim is still exact and fresh, but the authoritative
    /// database clock has reached the operation deadline. The contained claim
    /// is renewed atomically with this observation and is the only authority
    /// the runtime may use to submit the private deadline transition.
    OperationDeadlineElapsed(SchedulerTaskClaim),
    LeaseLost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerTaskCommitOutcome<T> {
    Committed { result: T },
    ExactReplay { authoritative: T },
    StaleLease,
    StateConflict,
    OperationDeadlineElapsed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerTaskClaim {
    envelope: DurableTaskExecutionRequest,
    claimed_by: String,
    claim_token: String,
    claim_expires_at: DateTime<Utc>,
    task_projection_version: u64,
    mode: SchedulerTaskClaimMode,
    lease_loss_evidence: Option<EffectEvidence>,
}

impl SchedulerTaskClaim {
    pub(crate) fn new(
        envelope: DurableTaskExecutionRequest,
        claimed_by: String,
        claim_token: String,
        claim_expires_at: DateTime<Utc>,
        task_projection_version: u64,
        mode: SchedulerTaskClaimMode,
    ) -> Self {
        Self {
            envelope,
            claimed_by,
            claim_token,
            claim_expires_at,
            task_projection_version,
            mode,
            lease_loss_evidence: None,
        }
    }

    pub(crate) fn with_lease_loss_evidence(mut self, evidence: EffectEvidence) -> Self {
        self.lease_loss_evidence = Some(evidence);
        self
    }

    pub fn task_id(&self) -> &SchedulerTaskId {
        self.envelope.request().task_id()
    }

    pub fn run_id(&self) -> &RunId {
        self.envelope.request().run_id()
    }

    pub fn activation_id(&self) -> &ActivationId {
        self.envelope.request().activation_id()
    }

    pub fn envelope(&self) -> &DurableTaskExecutionRequest {
        &self.envelope
    }

    pub fn claimed_by(&self) -> &str {
        &self.claimed_by
    }

    pub fn claim_token(&self) -> &str {
        &self.claim_token
    }

    pub fn claim_expires_at(&self) -> DateTime<Utc> {
        self.claim_expires_at
    }

    pub fn task_projection_version(&self) -> u64 {
        self.task_projection_version
    }

    pub fn mode(&self) -> SchedulerTaskClaimMode {
        self.mode
    }

    pub fn lease_loss_evidence(&self) -> Option<EffectEvidence> {
        self.lease_loss_evidence
    }
}

pub(crate) fn validate_runtime_value_ref(
    runtime_value: &RuntimeValue,
    value_ref: &ValueRef,
) -> Result<(), RepositoryError> {
    let canonical = serde_jcs::to_vec(runtime_value.value())
        .map_err(|_| RepositoryError::canonicalization())?;
    if ContentHash::from_bytes(&canonical) != *value_ref.content_hash() {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStoredValue {
    run_id: RunId,
    port_id: DataPortId,
    owner_activation_id: ActivationId,
    runtime_value: RuntimeValue,
    value_ref: ValueRef,
    declared_type: PlanType,
    projection_version: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerStoredValueWire {
    run_id: RunId,
    port_id: DataPortId,
    owner_activation_id: ActivationId,
    runtime_value: RuntimeValue,
    value_ref: ValueRef,
    declared_type: PlanType,
    projection_version: u64,
}

impl<'de> Deserialize<'de> for SchedulerStoredValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SchedulerStoredValueWire::deserialize(deserializer)?;
        Self::new(
            wire.run_id,
            wire.port_id,
            wire.owner_activation_id,
            wire.runtime_value,
            wire.value_ref,
            wire.declared_type,
            wire.projection_version,
        )
        .map_err(D::Error::custom)
    }
}

impl SchedulerStoredValue {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        run_id: RunId,
        port_id: DataPortId,
        owner_activation_id: ActivationId,
        runtime_value: RuntimeValue,
        value_ref: ValueRef,
        declared_type: PlanType,
        projection_version: u64,
    ) -> Result<Self, RepositoryError> {
        if !runtime_value.matches(&declared_type) {
            return Err(RepositoryError::invalid_data());
        }
        validate_runtime_value_ref(&runtime_value, &value_ref)?;
        Ok(Self {
            run_id,
            port_id,
            owner_activation_id,
            runtime_value,
            value_ref,
            declared_type,
            projection_version,
        })
    }

    pub fn port_id(&self) -> &DataPortId {
        &self.port_id
    }

    pub fn runtime_value(&self) -> &RuntimeValue {
        &self.runtime_value
    }

    pub fn value_ref(&self) -> &ValueRef {
        &self.value_ref
    }

    pub fn declared_type(&self) -> &PlanType {
        &self.declared_type
    }

    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerTaskSuccess {
    result: TaskExecutionResult,
    value_refs: BTreeMap<DataPortId, ValueRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerTaskSuccessWire {
    result: TaskExecutionResult,
    value_refs: BTreeMap<DataPortId, ValueRef>,
}

impl<'de> Deserialize<'de> for SchedulerTaskSuccess {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SchedulerTaskSuccessWire::deserialize(deserializer)?;
        Self::with_value_refs(wire.result, wire.value_refs).map_err(D::Error::custom)
    }
}

impl SchedulerTaskSuccess {
    /// Default adapter path: persist every validated worker output as a
    /// content-addressed inline payload in the same result transaction.
    pub fn inline(result: TaskExecutionResult) -> Result<Self, RepositoryError> {
        let value_refs = result
            .outputs()
            .iter()
            .map(|(port_id, value)| {
                ValueRef::inline(value.value().clone())
                    .map(|value_ref| (port_id.clone(), value_ref))
                    .map_err(|_| RepositoryError::invalid_data())
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Self::with_value_refs(result, value_refs)
    }

    /// Artifact references must already be staged and verified. This only
    /// supplies them to the fenced result commit; it performs no side write.
    pub fn with_value_refs(
        result: TaskExecutionResult,
        value_refs: BTreeMap<DataPortId, ValueRef>,
    ) -> Result<Self, RepositoryError> {
        if result.outputs().keys().ne(value_refs.keys()) {
            return Err(RepositoryError::invalid_configuration());
        }
        let success = Self { result, value_refs };
        success.validate_value_ref_integrity()?;
        Ok(success)
    }

    /// Revalidates the binding between each runtime value and its durable
    /// inline/artifact reference. Repository commits call this again so a
    /// value/reference pair can never rely only on an adapter-side check.
    pub(crate) fn validate_value_ref_integrity(&self) -> Result<(), RepositoryError> {
        if self.result.outputs().keys().ne(self.value_refs.keys()) {
            return Err(RepositoryError::invalid_data());
        }
        for (port_id, runtime_value) in self.result.outputs() {
            let value_ref = self
                .value_refs
                .get(port_id)
                .ok_or_else(RepositoryError::invalid_data)?;
            validate_runtime_value_ref(runtime_value, value_ref)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn unchecked_for_test(
        result: TaskExecutionResult,
        value_refs: BTreeMap<DataPortId, ValueRef>,
    ) -> Self {
        Self { result, value_refs }
    }

    pub fn result(&self) -> &TaskExecutionResult {
        &self.result
    }

    pub fn value_refs(&self) -> &BTreeMap<DataPortId, ValueRef> {
        &self.value_refs
    }
}

#[cfg(test)]
pub(crate) fn scheduler_validation_fixture(
    effect_idempotency: EffectIdempotency,
    max_attempts: u32,
) -> (SchedulerTaskClaim, DataPortId, TaskExecutionResult) {
    use chrono::Duration;

    use crate::engine::{
        plan::{PortName, VersionTag},
        ActivationId, EffectId, NodeId, SchedulerAction, SchedulerIntent, SchedulerTaskKind,
        TaskAdmissionClass, TaskOutputContract, WorkerCancellation, WorkerEffectPolicy,
    };

    let port_id = DataPortId::new("integrity_output").unwrap();
    let action = SchedulerAction::DispatchTask {
        task_id: SchedulerTaskId::parse(format!("task_{}", "a".repeat(64))).unwrap(),
        effect_id: EffectId::new("effect_integrity_test").unwrap(),
        activation_id: ActivationId::new("activation_integrity_test").unwrap(),
        node_id: NodeId::new("node_integrity_test").unwrap(),
        admission_class: TaskAdmissionClass::Normal,
        task_kind: SchedulerTaskKind::Action,
        implementation: "fixture.integrity".to_owned(),
        descriptor_version: VersionTag::new("1").unwrap(),
        worker_version: VersionTag::new("worker-1").unwrap(),
        effect_policy: WorkerEffectPolicy::new(
            effect_idempotency,
            max_attempts,
            WorkerCancellation::Cooperative,
        )
        .unwrap(),
        deployment_binding: serde_json::json!({}),
        public_configuration: BTreeMap::new(),
        secret_configuration: BTreeMap::new(),
        inputs: Vec::new(),
        outputs: vec![TaskOutputContract::new(
            port_id.clone(),
            PortName::new("output").unwrap(),
            PlanType::Any,
            true,
        )],
    };
    let request = TaskExecutionRequest::from_scheduler_intent(&SchedulerIntent::new(
        RunId::new("run_integrity_test").unwrap(),
        SchedulerCheckpointId::parse(format!("checkpoint_{}", "b".repeat(64))).unwrap(),
        action,
    ))
    .unwrap();
    let claim = SchedulerTaskClaim::new(
        DurableTaskExecutionRequest::new(
            request,
            AttemptNo::FIRST,
            LeaseEpoch::FIRST,
            "fence".to_owned(),
            1,
        )
        .unwrap(),
        "integrity-worker".to_owned(),
        "integrity-claim".to_owned(),
        Utc::now() + Duration::minutes(1),
        1,
        SchedulerTaskClaimMode::Execute,
    );
    let result = TaskExecutionResult::new(
        BTreeMap::from([(
            port_id.clone(),
            RuntimeValue::new(serde_json::json!({"answer": "value-a"})).unwrap(),
        )]),
        EffectEvidence::Committed,
    );
    (claim, port_id, result)
}

#[cfg(test)]
pub(crate) fn scheduler_success_validation_fixture(
) -> (SchedulerTaskClaim, DataPortId, TaskExecutionResult) {
    scheduler_validation_fixture(EffectIdempotency::Idempotent, 1)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum SchedulerFailureDisposition {
    Retry {
        retry_at: DateTime<Utc>,
        remaining_attempts: u32,
    },
    Terminal,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SchedulerTaskFailureSource {
    Worker,
    RuntimeDeadline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct SchedulerFailureClaimBinding {
    run_id: RunId,
    task_id: SchedulerTaskId,
    activation_id: ActivationId,
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    claimed_by: String,
    claim_token: String,
    claim_expires_at: DateTime<Utc>,
    task_projection_version: u64,
    mode: SchedulerTaskClaimMode,
    lease_loss_evidence: Option<EffectEvidence>,
    policy_hash: ContentHash,
}

impl SchedulerFailureClaimBinding {
    fn for_claim(claim: &SchedulerTaskClaim) -> Result<Self, RepositoryError> {
        let policy = serde_jcs::to_vec(claim.envelope().request().effect_policy())
            .map_err(|_| RepositoryError::canonicalization())?;
        Ok(Self {
            run_id: claim.run_id().clone(),
            task_id: claim.task_id().clone(),
            activation_id: claim.activation_id().clone(),
            attempt_no: claim.envelope().attempt_no(),
            lease_epoch: claim.envelope().lease_epoch(),
            fencing_token: claim.envelope().fencing_token().to_owned(),
            claimed_by: claim.claimed_by().to_owned(),
            claim_token: claim.claim_token().to_owned(),
            claim_expires_at: claim.claim_expires_at(),
            task_projection_version: claim.task_projection_version(),
            mode: claim.mode(),
            lease_loss_evidence: claim.lease_loss_evidence(),
            policy_hash: ContentHash::from_bytes(&policy),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerTaskFailure {
    source: SchedulerTaskFailureSource,
    claim_binding: SchedulerFailureClaimBinding,
    class: WorkerFailureClass,
    code: String,
    retryable: bool,
    safe_error: Option<SafeError>,
    effect_evidence: EffectEvidence,
    disposition: SchedulerFailureDisposition,
}

impl SchedulerTaskFailure {
    /// Freeze retry/backoff/timeout before entering the repository. Replay
    /// never recomputes policy from a clock or mutable configuration.
    pub(crate) fn from_worker_failure(
        claim: &SchedulerTaskClaim,
        failure: &WorkerFailure,
        effect_evidence: EffectEvidence,
        disposition: SchedulerFailureDisposition,
    ) -> Result<Self, RepositoryError> {
        if failure.code() == SCHEDULER_WORKER_DEADLINE_EXCEEDED {
            return Err(RepositoryError::invalid_configuration());
        }
        let frozen = Self {
            source: SchedulerTaskFailureSource::Worker,
            claim_binding: SchedulerFailureClaimBinding::for_claim(claim)?,
            class: failure.class(),
            code: failure.code().to_owned(),
            retryable: failure.retryable(),
            safe_error: failure.typed_safe_error().cloned(),
            effect_evidence,
            disposition,
        };
        frozen.validate_intrinsic()?;
        if matches!(
            &frozen.disposition,
            SchedulerFailureDisposition::Retry { .. }
        ) && !frozen.effect_evidence.permits_automatic_retry(
            claim
                .envelope()
                .request()
                .effect_policy()
                .effect_idempotency(),
        ) {
            return Err(RepositoryError::invalid_configuration());
        }
        Ok(frozen)
    }

    pub(crate) fn from_runtime_deadline(
        claim: &SchedulerTaskClaim,
    ) -> Result<Self, RepositoryError> {
        let frozen = Self {
            source: SchedulerTaskFailureSource::RuntimeDeadline,
            claim_binding: SchedulerFailureClaimBinding::for_claim(claim)?,
            class: WorkerFailureClass::EffectOutcomeUnknown,
            code: SCHEDULER_WORKER_DEADLINE_EXCEEDED.to_owned(),
            retryable: false,
            safe_error: None,
            effect_evidence: EffectEvidence::Unknown,
            disposition: SchedulerFailureDisposition::TimedOut,
        };
        frozen.validate_intrinsic()?;
        Ok(frozen)
    }

    fn validate_intrinsic(&self) -> Result<(), RepositoryError> {
        let valid_worker_failure = match (&self.class, &self.safe_error) {
            (WorkerFailureClass::SafeBusinessFailure, Some(safe_error)) => {
                WorkerFailure::safe_business(
                    self.code.clone(),
                    self.retryable,
                    safe_error.runtime_value().clone(),
                )
                .is_ok()
            }
            (WorkerFailureClass::SafeBusinessFailure, None) => false,
            (_, Some(_)) => false,
            (class, None) => WorkerFailure::new(*class, self.code.clone(), self.retryable).is_ok(),
        };
        let evidence_is_valid = match self.class {
            WorkerFailureClass::EffectOutcomeUnknown => {
                self.effect_evidence == EffectEvidence::Unknown
            }
            _ => self.effect_evidence != EffectEvidence::Unknown,
        };
        let timed_out_contract = self.source == SchedulerTaskFailureSource::RuntimeDeadline
            && self.class == WorkerFailureClass::EffectOutcomeUnknown
            && self.code == SCHEDULER_WORKER_DEADLINE_EXCEEDED
            && !self.retryable
            && self.effect_evidence == EffectEvidence::Unknown;
        let disposition_is_valid = match &self.disposition {
            SchedulerFailureDisposition::Retry {
                remaining_attempts, ..
            } => {
                self.retryable
                    && *remaining_attempts > 0
                    && self.code != SCHEDULER_WORKER_DEADLINE_EXCEEDED
            }
            SchedulerFailureDisposition::Terminal => {
                self.source == SchedulerTaskFailureSource::Worker
                    && self.code != SCHEDULER_WORKER_DEADLINE_EXCEEDED
            }
            SchedulerFailureDisposition::TimedOut => timed_out_contract,
        };
        let source_is_valid = match self.source {
            SchedulerTaskFailureSource::Worker => {
                self.code != SCHEDULER_WORKER_DEADLINE_EXCEEDED
                    && !matches!(self.disposition, SchedulerFailureDisposition::TimedOut)
            }
            SchedulerTaskFailureSource::RuntimeDeadline => timed_out_contract,
        };
        if !valid_worker_failure || !evidence_is_valid || !disposition_is_valid || !source_is_valid
        {
            return Err(RepositoryError::invalid_configuration());
        }
        Ok(())
    }

    pub(crate) fn validate_for_authority(
        &self,
        claim: &SchedulerTaskClaim,
        current_effect_evidence: EffectEvidence,
    ) -> Result<(), RepositoryError> {
        self.validate_intrinsic()?;
        if self.claim_binding != SchedulerFailureClaimBinding::for_claim(claim)? {
            return Err(RepositoryError::invalid_data());
        }
        if !current_effect_evidence.can_transition_to(self.effect_evidence) {
            return Err(RepositoryError::invalid_data());
        }
        if self.source == SchedulerTaskFailureSource::RuntimeDeadline
            && (claim.mode() != SchedulerTaskClaimMode::Execute
                || claim.lease_loss_evidence().is_some()
                || current_effect_evidence != EffectEvidence::Started)
        {
            return Err(RepositoryError::invalid_data());
        }
        match claim.mode() {
            SchedulerTaskClaimMode::Execute if claim.lease_loss_evidence().is_none() => {}
            SchedulerTaskClaimMode::FinalizeLeaseLoss
                if self.source == SchedulerTaskFailureSource::Worker
                    && claim.lease_loss_evidence()
                        == Some(current_effect_evidence.after_lease_loss())
                    && claim.lease_loss_evidence() == Some(self.effect_evidence) => {}
            SchedulerTaskClaimMode::Execute
            | SchedulerTaskClaimMode::FinalizeLeaseLoss
            | SchedulerTaskClaimMode::Acknowledge => {
                return Err(RepositoryError::invalid_data());
            }
        }
        let SchedulerFailureDisposition::Retry {
            remaining_attempts, ..
        } = &self.disposition
        else {
            return Ok(());
        };
        let policy = claim.envelope().request().effect_policy();
        let expected_remaining = policy
            .max_attempts()
            .saturating_sub(claim.envelope().attempt_no().get());
        if *remaining_attempts != expected_remaining
            || *remaining_attempts == 0
            || !self
                .effect_evidence
                .permits_automatic_retry(policy.effect_idempotency())
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn unchecked_for_test(
        claim: &SchedulerTaskClaim,
        class: WorkerFailureClass,
        code: impl Into<String>,
        retryable: bool,
        effect_evidence: EffectEvidence,
        disposition: SchedulerFailureDisposition,
    ) -> Self {
        Self {
            source: SchedulerTaskFailureSource::Worker,
            claim_binding: SchedulerFailureClaimBinding::for_claim(claim).unwrap(),
            class,
            code: code.into(),
            retryable,
            safe_error: None,
            effect_evidence,
            disposition,
        }
    }

    pub fn class(&self) -> WorkerFailureClass {
        self.class
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn retryable(&self) -> bool {
        self.retryable
    }

    pub fn effect_evidence(&self) -> EffectEvidence {
        self.effect_evidence
    }

    pub fn safe_error(&self) -> Option<&RuntimeValue> {
        self.safe_error.as_ref().map(SafeError::runtime_value)
    }

    pub fn disposition(&self) -> &SchedulerFailureDisposition {
        &self.disposition
    }

    pub(crate) fn is_runtime_deadline(&self) -> bool {
        self.source == SchedulerTaskFailureSource::RuntimeDeadline
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum SchedulerTaskOutcome {
    Succeeded(SchedulerTaskSuccess),
    Failed(SchedulerTaskFailure),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerTaskCompletionReceipt {
    commit: SchedulerCommitReceipt,
    task_id: SchedulerTaskId,
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    output_projection_versions: BTreeMap<DataPortId, u64>,
}

impl SchedulerTaskCompletionReceipt {
    pub(crate) fn new(
        commit: SchedulerCommitReceipt,
        task_id: SchedulerTaskId,
        attempt_no: AttemptNo,
        lease_epoch: LeaseEpoch,
        output_projection_versions: BTreeMap<DataPortId, u64>,
    ) -> Self {
        Self {
            commit,
            task_id,
            attempt_no,
            lease_epoch,
            output_projection_versions,
        }
    }

    pub fn commit(&self) -> &SchedulerCommitReceipt {
        &self.commit
    }

    pub fn task_id(&self) -> &SchedulerTaskId {
        &self.task_id
    }

    pub fn output_projection_versions(&self) -> &BTreeMap<DataPortId, u64> {
        &self.output_projection_versions
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerCrashPoint {
    BeforeBranchCommit,
    AfterBranchCommit,
    BeforeJoinCommit,
    AfterJoinCommit,
    BeforeExternalCall,
    AfterExternalCall,
    BeforeResultCommit,
    AfterResultCommit,
    BeforeDownstreamAdmission,
    AfterDownstreamAdmission,
}

pub trait SchedulerCrashInjector: Send + Sync {
    fn hit(&self, point: SchedulerCrashPoint) -> Result<(), RepositoryError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoSchedulerCrash;

impl SchedulerCrashInjector for NoSchedulerCrash {
    fn hit(&self, _point: SchedulerCrashPoint) -> Result<(), RepositoryError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct FailOnceSchedulerCrash {
    point: SchedulerCrashPoint,
    armed: AtomicBool,
}

impl FailOnceSchedulerCrash {
    pub fn new(point: SchedulerCrashPoint) -> Self {
        Self {
            point,
            armed: AtomicBool::new(true),
        }
    }

    pub fn fired(&self) -> bool {
        !self.armed.load(Ordering::SeqCst)
    }
}

impl SchedulerCrashInjector for FailOnceSchedulerCrash {
    fn hit(&self, point: SchedulerCrashPoint) -> Result<(), RepositoryError> {
        if point == self.point && self.armed.swap(false, Ordering::SeqCst) {
            return Err(RepositoryError::scheduler_crash_injected());
        }
        Ok(())
    }
}

#[async_trait]
pub trait SchedulerDurableRepository:
    DurableRepository + ProjectionDurableRepository + ArtifactDurableRepository
{
    async fn load_scheduler_facts(&self, run_id: &RunId)
        -> Result<SchedulerFacts, RepositoryError>;

    async fn commit_scheduler_action(
        &self,
        fence: &FencedSchedulerRunCommand,
        action: &PlannedSchedulerAction,
    ) -> Result<TransitionOutcome<SchedulerCommitReceipt>, RepositoryError>;

    async fn claim_scheduler_tasks(
        &self,
        claimed_by: &str,
        claim_seconds: u32,
        limit: u32,
    ) -> Result<Vec<SchedulerTaskClaim>, RepositoryError>;

    async fn claim_scheduler_tasks_with_run_limit(
        &self,
        claimed_by: &str,
        claim_seconds: u32,
        limit: u32,
        max_claimed_per_run: u32,
    ) -> Result<Vec<SchedulerTaskClaim>, RepositoryError> {
        if max_claimed_per_run == 0 {
            return Err(RepositoryError::invalid_configuration());
        }
        self.claim_scheduler_tasks(claimed_by, claim_seconds, limit)
            .await
    }

    async fn mark_scheduler_task_started(
        &self,
        claim: &SchedulerTaskClaim,
    ) -> Result<TransitionOutcome<SchedulerCommitReceipt>, RepositoryError>;

    /// Reserves the durable identity for one Provider request before that
    /// request is sent. When `publish` is true this atomically freezes the
    /// response item identity and append-only output index as well.
    ///
    /// The exact running Attempt claim is the authority. Replaying the same
    /// identity returns the original reservation; a stale/expired/terminal
    /// claim can never mint or recover write authority.
    async fn reserve_model_call(
        &self,
        claim: &SchedulerTaskClaim,
        model_call_no: u32,
        publish: bool,
    ) -> Result<SchedulerTaskCommitOutcome<ModelCallAuthority>, RepositoryError> {
        let _ = (claim, model_call_no, publish);
        Err(RepositoryError::invalid_configuration())
    }

    /// Lazily allocates the append-only public message identity for a model
    /// call after the Provider has produced the first non-empty text. The
    /// current renewable claim is the only authority; reservation is a
    /// fenced CAS and exact replay returns the original item/index.
    async fn reserve_model_call_public_item(
        &self,
        claim: &SchedulerTaskClaim,
        model_call_no: u32,
    ) -> Result<SchedulerTaskCommitOutcome<ResponseItemAuthority>, RepositoryError> {
        let _ = (claim, model_call_no);
        Err(RepositoryError::invalid_configuration())
    }

    /// Lazily allocates one standard function-call output item after the
    /// Provider has supplied stable call identity, but before any argument
    /// fragment is published. The repository rechecks the frozen
    /// `call:true, arguments:all` policy under the current renewable claim.
    async fn reserve_model_call_public_function_item(
        &self,
        claim: &SchedulerTaskClaim,
        model_call_no: u32,
        call_index: u32,
        call_id: &str,
        tool_name: &str,
    ) -> Result<SchedulerTaskCommitOutcome<ResponseItemAuthority>, RepositoryError> {
        let _ = (claim, model_call_no, call_index, call_id, tool_name);
        Err(RepositoryError::invalid_configuration())
    }

    /// Fenced, idempotent checkpoint for normalized finish reason, Provider
    /// usage and the completed (or explicitly incomplete) public item.
    /// Transient deltas are deliberately outside this durable boundary.
    async fn checkpoint_model_call_completion(
        &self,
        claim: &SchedulerTaskClaim,
        completion: &ModelCallCompletion,
    ) -> Result<SchedulerTaskCommitOutcome<()>, RepositoryError> {
        let _ = (claim, completion);
        Err(RepositoryError::invalid_configuration())
    }

    /// Atomically checkpoints a `tool_calls` model completion and its exact
    /// assistant/tool-call batch as a non-executable durable handoff intent.
    /// No child has execution authority in this state. Recovery must classify
    /// the parent as `ActivateCheckpointed`, never call the Provider again,
    /// and complete materialization through `activate_model_tool_call_batch`.
    async fn checkpoint_model_tool_call_batch(
        &self,
        claim: &SchedulerTaskClaim,
        checkpoint: &ModelToolCallCheckpoint,
    ) -> Result<SchedulerTaskCommitOutcome<()>, RepositoryError> {
        let _ = (claim, checkpoint);
        Err(RepositoryError::invalid_configuration())
    }

    /// Loads the only durable continuation state authorized by this exact,
    /// fresh parent task claim. `Committed { result: None }` denotes the
    /// initial model call; every recovery state is returned as `Some`.
    ///
    /// The repository must derive this from the latest model-call row for the
    /// current Attempt. Older ready batches are never continuation authority.
    async fn load_model_tool_parent_resume(
        &self,
        claim: &SchedulerTaskClaim,
    ) -> Result<SchedulerTaskCommitOutcome<Option<ModelToolParentResume>>, RepositoryError> {
        let _ = claim;
        Err(RepositoryError::invalid_configuration())
    }

    /// Atomically upgrades an immutable model tool-call checkpoint into all
    /// executable tool-task authorities and the parent's `waiting_tools`
    /// barrier using only the parent LLM's frozen deployment binding. There
    /// must be no durable state in which a proper subset is executable or the
    /// children are executable without the parent barrier. This method never
    /// executes an Action.
    async fn activate_model_tool_call_batch(
        &self,
        parent_claim: &SchedulerTaskClaim,
        model_call_no: u32,
    ) -> Result<ModelToolBatchActivationOutcome, RepositoryError> {
        let _ = (parent_claim, model_call_no);
        Err(RepositoryError::invalid_configuration())
    }

    /// Claims ready tool calls with a hard per-Run concurrency bound.
    async fn claim_model_tool_calls(
        &self,
        claimed_by: &str,
        claim_seconds: u32,
        limit: u32,
        max_claimed_per_run: u32,
    ) -> Result<Vec<ModelToolTaskClaim>, RepositoryError> {
        let _ = (claimed_by, claim_seconds, limit, max_claimed_per_run);
        Err(RepositoryError::invalid_configuration())
    }

    async fn mark_model_tool_call_started(
        &self,
        claim: &ModelToolTaskClaim,
    ) -> Result<ModelToolTaskTransitionOutcome<()>, RepositoryError> {
        let _ = claim;
        Err(RepositoryError::invalid_configuration())
    }

    async fn heartbeat_model_tool_call(
        &self,
        claim: &ModelToolTaskClaim,
        claim_seconds: u32,
    ) -> Result<ModelToolTaskHeartbeatOutcome, RepositoryError> {
        let _ = (claim, claim_seconds);
        Err(RepositoryError::invalid_configuration())
    }

    async fn commit_model_tool_call_outcome(
        &self,
        claim: &ModelToolTaskClaim,
        outcome: &ModelToolTaskOutcome,
    ) -> Result<ModelToolTaskTransitionOutcome<ModelToolTaskCommitReceipt>, RepositoryError> {
        let _ = (claim, outcome);
        Err(RepositoryError::invalid_configuration())
    }

    /// Renews both the task-outbox claim and the exact Attempt lease with one
    /// CAS. The returned claim carries the new task projection version and is
    /// the only claim which may subsequently commit an outcome.
    async fn heartbeat_scheduler_task(
        &self,
        claim: &SchedulerTaskClaim,
        claim_seconds: u32,
    ) -> Result<SchedulerTaskHeartbeatOutcome, RepositoryError>;

    /// Exact replay is checked before current fences. A non-replayed stale or
    /// expired claim returns StateConflict without changing effect evidence,
    /// projections, or acknowledgement state.
    async fn commit_scheduler_task_outcome(
        &self,
        claim: &SchedulerTaskClaim,
        outcome: &SchedulerTaskOutcome,
    ) -> Result<SchedulerTaskCommitOutcome<SchedulerTaskCompletionReceipt>, RepositoryError>;

    async fn acknowledge_scheduler_task(
        &self,
        claim: &SchedulerTaskClaim,
    ) -> Result<bool, RepositoryError>;

    async fn load_scheduler_value(
        &self,
        run_id: &RunId,
        port_id: &DataPortId,
    ) -> Result<Option<SchedulerStoredValue>, RepositoryError>;

    async fn list_recoverable_scheduler_runs(
        &self,
        limit: u32,
    ) -> Result<Vec<RunId>, RepositoryError>;
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSubflowBindingDocument {
    node_id: NodeId,
    binding: StoredSubflowBinding,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSubflowBinding {
    adapter: String,
    definition_revision_id: DefinitionRevisionId,
    deployment_revision_id: DeploymentRevisionId,
    plan_hash: ContentHash,
    binding_hash: ContentHash,
    interface_version: VersionTag,
}

impl StoredSubflowBinding {
    fn execution_revision(&self) -> ExecutionRevisionPin {
        ExecutionRevisionPin::new(
            self.definition_revision_id.clone(),
            self.deployment_revision_id.clone(),
            self.plan_hash.clone(),
            self.binding_hash.clone(),
        )
    }
}

fn stored_subflow_binding(
    parent: &super::VersionedPlan,
    node_id: &NodeId,
) -> Result<StoredSubflowBinding, RepositoryError> {
    let entries = parent
        .resolved_bindings()
        .as_array()
        .ok_or_else(RepositoryError::invalid_data)?;
    let mut matches = entries.iter().filter(|entry| {
        entry.get("node_id").and_then(serde_json::Value::as_str) == Some(node_id.as_str())
    });
    let entry = matches.next().ok_or_else(RepositoryError::invalid_data)?;
    if matches.next().is_some() {
        return Err(RepositoryError::invalid_data());
    }
    let document = serde_json::from_value::<StoredSubflowBindingDocument>(entry.clone())
        .map_err(|_| RepositoryError::invalid_data())?;
    if document.node_id != *node_id || document.binding.adapter != "durable_subflow" {
        return Err(RepositoryError::invalid_data());
    }
    Ok(document.binding)
}

/// Re-derives a StartSubflow action from durable facts and the exact parent
/// and child deployment revisions before either backend opens the committing
/// transaction. The action precondition is checked again under the backend's
/// normal CAS, so a concurrent fact change can only turn this validation into
/// a later StateConflict; it cannot authorize stale derived values.
pub(crate) async fn validate_subflow_admission<R: SchedulerDurableRepository + ?Sized>(
    repository: &R,
    fence: &FencedSchedulerRunCommand,
    action: &PlannedSchedulerAction,
) -> Result<(), RepositoryError> {
    let SchedulerAction::StartSubflow {
        invocation,
        execution_revision,
        interface_version,
        timeout_ms,
        run_input,
        outputs,
    } = action.intent().action()
    else {
        return Ok(());
    };
    if action.intent().run_id() != fence.run_id() {
        return Err(RepositoryError::invalid_data());
    }

    let facts = repository.load_scheduler_facts(fence.run_id()).await?;
    if facts.projection_version() != action.precondition().expected_projection_version() {
        // Exact replay or a concurrent winner is decided by the transactional
        // path. Do not reinterpret its already-stale semantic snapshot here.
        return Ok(());
    }
    let run = repository
        .load_run(fence.run_id())
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
    if run.projection_version() != facts.projection_version() {
        return Ok(());
    }
    let catalog = repository.load_versioned_plan_catalog().await?;
    let mut parent_matches = catalog.plans().iter().filter(|plan| {
        plan.definition_id() == run.definition_id()
            && plan.definition_revision_id() == run.definition_revision_id()
            && plan.deployment_revision_id() == run.deployment_revision_id()
            && plan.plan_hash() == run.plan_hash()
            && plan.binding_hash() == run.binding_hash()
    });
    let parent = parent_matches
        .next()
        .ok_or_else(RepositoryError::invalid_data)?;
    if parent_matches.next().is_some() {
        return Err(RepositoryError::invalid_data());
    }
    let parent_plan = serde_json::from_value::<Plan>(parent.canonical_plan().clone())
        .map_err(|_| RepositoryError::invalid_data())?;
    let index = PlanIndex::new(&parent_plan).map_err(|_| RepositoryError::invalid_data())?;
    let node = index
        .node(invocation.node_id())
        .ok_or_else(RepositoryError::invalid_data)?;
    let NodeKind::SubflowCall(descriptor) = node.kind() else {
        return Err(RepositoryError::invalid_data());
    };
    let derived_invocation = crate::engine::scheduler::derive_subflow_invocation(
        &index,
        &facts,
        node,
        descriptor,
        invocation.occurrence(),
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if derived_invocation != *invocation {
        return Err(RepositoryError::invalid_data());
    }
    if descriptor.interface_version != *interface_version
        || descriptor.timeout_ms != *timeout_ms
        || descriptor.invocation_scope_id != *invocation.static_scope_id()
        || descriptor.definition_revision_id != *execution_revision.definition_revision_id()
    {
        return Err(RepositoryError::invalid_data());
    }

    let binding = stored_subflow_binding(parent, invocation.node_id())?;
    if binding.interface_version != *interface_version
        || binding.execution_revision() != *execution_revision
    {
        return Err(RepositoryError::invalid_data());
    }
    let mut child_matches = catalog.plans().iter().filter(|plan| {
        plan.definition_revision_id() == execution_revision.definition_revision_id()
            && plan.deployment_revision_id() == execution_revision.deployment_revision_id()
            && plan.plan_hash() == execution_revision.plan_hash()
            && plan.binding_hash() == execution_revision.binding_hash()
    });
    let child = child_matches
        .next()
        .ok_or_else(RepositoryError::invalid_data)?;
    if child_matches.next().is_some() {
        return Err(RepositoryError::invalid_data());
    }
    let child_plan = serde_json::from_value::<Plan>(child.canonical_plan().clone())
        .map_err(|_| RepositoryError::invalid_data())?;
    let derived = crate::engine::scheduler::derive_subflow_admission(
        &index,
        &facts,
        node,
        descriptor,
        invocation.occurrence(),
        child_plan.metadata().input_contract(),
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if derived.run_input != *run_input || derived.outputs != *outputs {
        return Err(RepositoryError::invalid_data());
    }
    let [output] = derived.outputs.as_slice() else {
        return Err(RepositoryError::invalid_data());
    };
    if output.name().as_str() != "result"
        || output.value_type() != child_plan.metadata().output_type()
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        RuntimeValue, SchedulerFailureDisposition, SchedulerStoredValue, SchedulerTaskFailure,
        SchedulerTaskSuccess, SCHEDULER_TASK_ENVELOPE_SCHEMA_VERSION,
        SCHEDULER_WORKER_DEADLINE_EXCEEDED,
    };
    use crate::engine::{
        plan::{DataPortId, PlanType},
        ActivationId, ArtifactId, ArtifactRef, ContentHash, EffectEvidence, EffectIdempotency,
        RunId, TaskExecutionResult, ValueRef, WorkerFailure, WorkerFailureClass,
    };

    #[test]
    fn stored_value_deserialize_rejects_runtime_value_reference_mismatch() {
        let runtime_value = RuntimeValue::new(json!({"answer": "value-a"})).unwrap();
        let value_ref = ValueRef::inline(runtime_value.value().clone()).unwrap();
        let stored = SchedulerStoredValue::new(
            RunId::new("run_stored_value_serde").unwrap(),
            DataPortId::new("stored_output").unwrap(),
            ActivationId::new("stored_activation").unwrap(),
            runtime_value,
            value_ref,
            PlanType::Any,
            1,
        )
        .unwrap();
        let mut wire = serde_json::to_value(&stored).unwrap();
        assert_eq!(
            serde_json::from_value::<SchedulerStoredValue>(wire.clone()).unwrap(),
            stored
        );
        wire["runtime_value"] =
            serde_json::to_value(RuntimeValue::new(json!({"answer": "value-b"})).unwrap()).unwrap();
        assert!(serde_json::from_value::<SchedulerStoredValue>(wire).is_err());
    }

    #[test]
    fn task_success_binds_every_runtime_value_to_its_jcs_content_hash() {
        let port = DataPortId::new("answer_output").unwrap();
        let value = RuntimeValue::new(json!({"b": [2, 3], "a": 1})).unwrap();
        let result = TaskExecutionResult::new(
            BTreeMap::from([(port.clone(), value.clone())]),
            EffectEvidence::Committed,
        );

        let inline = SchedulerTaskSuccess::inline(result.clone()).unwrap();
        assert_eq!(
            serde_json::from_value::<SchedulerTaskSuccess>(serde_json::to_value(&inline).unwrap())
                .unwrap(),
            inline
        );

        let canonical = serde_jcs::to_vec(value.value()).unwrap();
        let artifact = ArtifactRef::new(
            ArtifactId::new("artifact_valid_value").unwrap(),
            ContentHash::from_bytes(&canonical),
            u64::try_from(canonical.len()).unwrap(),
            Some("application/json".to_owned()),
        )
        .unwrap();
        assert!(SchedulerTaskSuccess::with_value_refs(
            result.clone(),
            BTreeMap::from([(port.clone(), ValueRef::Artifact(artifact))]),
        )
        .is_ok());

        let wrong_artifact = ArtifactRef::new(
            ArtifactId::new("artifact_wrong_value").unwrap(),
            ContentHash::from_bytes(b"different JSON value"),
            20,
            Some("application/json".to_owned()),
        )
        .unwrap();
        assert!(SchedulerTaskSuccess::with_value_refs(
            result.clone(),
            BTreeMap::from([(port.clone(), ValueRef::Artifact(wrong_artifact.clone()))]),
        )
        .is_err());
        let forged_wire = json!({
            "result": result,
            "value_refs": {port.as_str(): ValueRef::Artifact(wrong_artifact)},
        });
        assert!(serde_json::from_value::<SchedulerTaskSuccess>(forged_wire).is_err());
    }

    #[test]
    fn durable_task_failure_revalidates_safe_error_class_and_code_authorities() {
        let (claim, _, _) =
            super::scheduler_validation_fixture(EffectIdempotency::NonIdempotent, 2);
        for invalid in [
            SchedulerTaskFailure::unchecked_for_test(
                &claim,
                WorkerFailureClass::ControlTermination,
                "CONTROL_RETRY",
                true,
                EffectEvidence::NotStarted,
                SchedulerFailureDisposition::Retry {
                    retry_at: chrono::Utc::now(),
                    remaining_attempts: 1,
                },
            ),
            SchedulerTaskFailure::unchecked_for_test(
                &claim,
                WorkerFailureClass::EffectOutcomeUnknown,
                "UNKNOWN_WITH_STARTED",
                true,
                EffectEvidence::Started,
                SchedulerFailureDisposition::Terminal,
            ),
            SchedulerTaskFailure::unchecked_for_test(
                &claim,
                WorkerFailureClass::InfrastructureFailure,
                "ZERO_REMAINING",
                true,
                EffectEvidence::NotStarted,
                SchedulerFailureDisposition::Retry {
                    retry_at: chrono::Utc::now(),
                    remaining_attempts: 0,
                },
            ),
            SchedulerTaskFailure::unchecked_for_test(
                &claim,
                WorkerFailureClass::InfrastructureFailure,
                SCHEDULER_WORKER_DEADLINE_EXCEEDED,
                false,
                EffectEvidence::Started,
                SchedulerFailureDisposition::TimedOut,
            ),
            SchedulerTaskFailure::unchecked_for_test(
                &claim,
                WorkerFailureClass::EffectOutcomeUnknown,
                "NOT_A_DEADLINE",
                false,
                EffectEvidence::Unknown,
                SchedulerFailureDisposition::TimedOut,
            ),
        ] {
            assert!(invalid.validate_intrinsic().is_err());
        }

        let retryable = WorkerFailure::new(
            WorkerFailureClass::InfrastructureFailure,
            "RETRY_POLICY_CHECK",
            true,
        )
        .unwrap();
        let retry = SchedulerFailureDisposition::Retry {
            retry_at: chrono::Utc::now(),
            remaining_attempts: 1,
        };
        assert!(SchedulerTaskFailure::from_worker_failure(
            &claim,
            &retryable,
            EffectEvidence::Started,
            retry.clone(),
        )
        .is_err());
        let (idempotent_claim, _, _) =
            super::scheduler_validation_fixture(EffectIdempotency::Idempotent, 2);
        assert!(SchedulerTaskFailure::from_worker_failure(
            &idempotent_claim,
            &retryable,
            EffectEvidence::Started,
            retry,
        )
        .is_ok());
    }

    #[test]
    fn durable_task_failure_is_bound_to_the_exact_claim_and_deadline_source() {
        let (claim, _, _) = super::scheduler_validation_fixture(EffectIdempotency::Idempotent, 2);
        let worker_failure = WorkerFailure::new(
            WorkerFailureClass::InfrastructureFailure,
            "BOUND_WORKER_FAILURE",
            false,
        )
        .unwrap();
        let failure = SchedulerTaskFailure::from_worker_failure(
            &claim,
            &worker_failure,
            EffectEvidence::Started,
            SchedulerFailureDisposition::Terminal,
        )
        .unwrap();
        assert!(failure
            .validate_for_authority(&claim, EffectEvidence::Started)
            .is_ok());

        let transplanted_claim = super::SchedulerTaskClaim::new(
            claim.envelope().clone(),
            claim.claimed_by().to_owned(),
            "different-claim-token".to_owned(),
            claim.claim_expires_at(),
            claim.task_projection_version(),
            claim.mode(),
        );
        assert!(failure
            .validate_for_authority(&transplanted_claim, EffectEvidence::Started)
            .is_err());

        let forged_deadline = WorkerFailure::new(
            WorkerFailureClass::EffectOutcomeUnknown,
            SCHEDULER_WORKER_DEADLINE_EXCEEDED,
            false,
        )
        .unwrap();
        assert!(SchedulerTaskFailure::from_worker_failure(
            &claim,
            &forged_deadline,
            EffectEvidence::Unknown,
            SchedulerFailureDisposition::Terminal,
        )
        .is_err());

        let deadline = SchedulerTaskFailure::from_runtime_deadline(&claim).unwrap();
        assert!(deadline
            .validate_for_authority(&claim, EffectEvidence::Started)
            .is_ok());
        assert!(deadline
            .validate_for_authority(&claim, EffectEvidence::NotStarted)
            .is_err());
    }

    #[test]
    fn durable_task_envelope_deserialize_validates_schema_and_lease_fence() {
        let (claim, _, _) = super::scheduler_success_validation_fixture();
        let mut wire = serde_json::to_value(claim.envelope()).unwrap();
        assert_eq!(
            serde_json::from_value::<super::DurableTaskExecutionRequest>(wire.clone()).unwrap(),
            *claim.envelope()
        );
        wire["schema_version"] = json!(SCHEDULER_TASK_ENVELOPE_SCHEMA_VERSION + 1);
        assert!(serde_json::from_value::<super::DurableTaskExecutionRequest>(wire).is_err());

        let mut wire = serde_json::to_value(claim.envelope()).unwrap();
        wire["attempt_no"] = json!(2);
        wire["lease_epoch"] = json!(1);
        assert!(serde_json::from_value::<super::DurableTaskExecutionRequest>(wire).is_err());
    }
}
