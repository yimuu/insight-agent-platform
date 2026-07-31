use super::RepositoryErrorExt as _;

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use insight_engine::run_stream::RunToolPublicProjection;
use insight_engine::schema::compile_schema_2020;
use insight_engine::worker::{ModelToolCallBatch, ResponseItemAuthority};
use insight_engine::{
    ActivationId, AttemptNo, ContentHash, EffectEvidence, EffectId, EffectIdempotency, LeaseEpoch,
    RunId, SchedulerTaskId, WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy,
};

use super::{
    common::{function_call_response_item_id, PreparedResponseFunctionPublication},
    RepositoryError,
};

pub const MAX_MODEL_TOOL_RESULT_BYTES: usize = 1_048_576;
pub const FUNCTION_CALL_COMPLETE_SEAL_INDEX: u64 = 3;
pub(crate) const MAX_MODEL_TOOL_ARGUMENT_BYTES: usize = 262_144;

pub(crate) fn prepare_model_function_call_publications(
    run_id: &RunId,
    activation_id: &ActivationId,
    attempt_no: AttemptNo,
    batch: &ModelToolCallBatch,
    deployment_binding: &Value,
    publish: bool,
) -> Result<Vec<PreparedResponseFunctionPublication>, RepositoryError> {
    let contract = parse_frozen_model_tool_contract(deployment_binding)?;
    let mut supplied = batch
        .public_function_calls()
        .iter()
        .map(|publication| (publication.call_index(), publication))
        .collect::<BTreeMap<_, _>>();
    if supplied.len() != batch.public_function_calls().len() {
        return Err(RepositoryError::invalid_data());
    }
    let mut prepared = Vec::new();
    for call in batch.calls() {
        let action = contract
            .tools
            .get(call.name())
            .ok_or_else(RepositoryError::invalid_data)?;
        validate_tool_arguments(action, call.arguments())?;
        let projection =
            RunToolPublicProjection::from_frozen_effective_policy(action.effective_public_policy())
                .map_err(|_| RepositoryError::invalid_data())?;
        let authorized = publish && projection.raw_argument_deltas_authorized();
        let publication = supplied.remove(&call.index());
        if !authorized {
            if publication.is_some() {
                return Err(RepositoryError::invalid_data());
            }
            continue;
        }
        let publication = publication.ok_or_else(RepositoryError::invalid_data)?;
        let expected_item_id = function_call_response_item_id(
            run_id,
            activation_id,
            attempt_no,
            batch.model_call_no(),
            call.index(),
            call.call_id(),
            call.name(),
        );
        if publication.public_item().item_id() != expected_item_id {
            return Err(RepositoryError::invalid_data());
        }
        let projected = projection
            .project_validated_completed_arguments(call.arguments())
            .map_err(|_| RepositoryError::invalid_data())?;
        let arguments_jcs = projected
            .standard_function_call_arguments()
            .ok_or_else(RepositoryError::invalid_data)?
            .to_owned();
        let seal_index = publication
            .completed_seal_index()
            .ok_or_else(RepositoryError::invalid_data)?;
        prepared.push(PreparedResponseFunctionPublication {
            call_index: call.index(),
            item: publication.public_item().clone(),
            seal_index,
            reserved_safe_item: json!({
                "id": expected_item_id,
                "type": "function_call",
                "status": "incomplete",
                "call_id": call.call_id(),
                "name": call.name(),
                "arguments": "",
            }),
            terminal_item_status: "completed",
            terminal_safe_item: json!({
                "id": publication.public_item().item_id(),
                "type": "function_call",
                "status": "completed",
                "call_id": call.call_id(),
                "name": call.name(),
                "arguments": arguments_jcs,
            }),
        });
    }
    if !supplied.is_empty() {
        return Err(RepositoryError::invalid_data());
    }
    Ok(prepared)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolTaskStatus {
    Pending,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl ModelToolTaskStatus {
    pub(crate) fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "pending" => Ok(Self::Pending),
            "claimed" => Ok(Self::Claimed),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

/// Action execution authority frozen from the parent LLM deployment binding.
/// It is deliberately self-contained so a later executor never consults the
/// mutable action catalog to reconstruct identity, policy, or schemas.
#[derive(Clone, PartialEq)]
pub struct FrozenModelToolAction {
    name: String,
    action_id: String,
    action_version: String,
    descriptor_hash: String,
    input_schema: Value,
    output_schema: Value,
    effect_policy: WorkerEffectPolicy,
    deployment_binding: Value,
    effective_public_policy: Value,
}

impl FrozenModelToolAction {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn action_id(&self) -> &str {
        &self.action_id
    }
    pub fn action_version(&self) -> &str {
        &self.action_version
    }
    pub fn descriptor_hash(&self) -> &str {
        &self.descriptor_hash
    }
    pub fn input_schema(&self) -> &Value {
        &self.input_schema
    }
    pub fn output_schema(&self) -> &Value {
        &self.output_schema
    }
    pub fn effect_policy(&self) -> &WorkerEffectPolicy {
        &self.effect_policy
    }
    pub fn deployment_binding(&self) -> &Value {
        &self.deployment_binding
    }
    pub fn effective_public_policy(&self) -> &Value {
        &self.effective_public_policy
    }
}

impl std::fmt::Debug for FrozenModelToolAction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrozenModelToolAction")
            .field("action_id", &self.action_id)
            .field("action_version", &self.action_version)
            .field("descriptor_hash", &self.descriptor_hash)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq)]
pub struct ModelToolTaskIdentity {
    tool_task_id: SchedulerTaskId,
    effect_id: EffectId,
    call_index: u32,
    call_id: String,
    action: FrozenModelToolAction,
    public_item: Option<ResponseItemAuthority>,
    public_arguments_jcs: Option<String>,
    public_seal_index: Option<u64>,
}

impl ModelToolTaskIdentity {
    pub fn tool_task_id(&self) -> &SchedulerTaskId {
        &self.tool_task_id
    }
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    pub fn call_index(&self) -> u32 {
        self.call_index
    }
    pub fn call_id(&self) -> &str {
        &self.call_id
    }
    pub fn action(&self) -> &FrozenModelToolAction {
        &self.action
    }
    pub fn public_item(&self) -> Option<&ResponseItemAuthority> {
        self.public_item.as_ref()
    }
    pub fn public_arguments_jcs(&self) -> Option<&str> {
        self.public_arguments_jcs.as_deref()
    }
    pub fn public_seal_index(&self) -> Option<u64> {
        self.public_seal_index
    }
}

impl std::fmt::Debug for ModelToolTaskIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelToolTaskIdentity")
            .field("tool_task_id", &self.tool_task_id)
            .field("effect_id", &self.effect_id)
            .field("call_index", &self.call_index)
            .field("action_id", &self.action.action_id)
            .field("public", &self.public_item.is_some())
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct ModelToolBatchActivation {
    run_id: RunId,
    activation_id: ActivationId,
    parent_attempt_no: AttemptNo,
    model_call_no: u32,
    tasks: Vec<ModelToolTaskIdentity>,
}

impl ModelToolBatchActivation {
    pub(crate) fn new(
        run_id: RunId,
        activation_id: ActivationId,
        parent_attempt_no: AttemptNo,
        model_call_no: u32,
        tasks: Vec<ModelToolTaskIdentity>,
    ) -> Result<Self, RepositoryError> {
        if model_call_no == 0
            || tasks.is_empty()
            || tasks
                .iter()
                .enumerate()
                .any(|(index, task)| task.call_index != index as u32)
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            run_id,
            activation_id,
            parent_attempt_no,
            model_call_no,
            tasks,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }
    pub fn parent_attempt_no(&self) -> AttemptNo {
        self.parent_attempt_no
    }
    pub fn model_call_no(&self) -> u32 {
        self.model_call_no
    }
    pub fn tasks(&self) -> &[ModelToolTaskIdentity] {
        &self.tasks
    }
}

impl std::fmt::Debug for ModelToolBatchActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelToolBatchActivation")
            .field("run_id", &self.run_id)
            .field("activation_id", &self.activation_id)
            .field("parent_attempt_no", &self.parent_attempt_no)
            .field("model_call_no", &self.model_call_no)
            .field("call_count", &self.tasks.len())
            .field(
                "public_call_count",
                &self
                    .tasks
                    .iter()
                    .filter(|task| task.public_item.is_some())
                    .count(),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub enum ModelToolBatchActivationOutcome {
    Activated(ModelToolBatchActivation),
    ExactReplay(ModelToolBatchActivation),
    StaleParentLease,
    StateConflict,
    RunTerminal,
    RoundLimitExceeded,
    CallLimitExceeded,
}

#[derive(Clone, PartialEq)]
pub struct ModelToolTaskClaim {
    run_id: RunId,
    parent_activation_id: ActivationId,
    parent_attempt_no: AttemptNo,
    model_call_no: u32,
    identity: ModelToolTaskIdentity,
    arguments: Value,
    tool_attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    claimed_by: String,
    claim_token: String,
    claim_expires_at: DateTime<Utc>,
    projection_version: u64,
}

impl ModelToolTaskClaim {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        run_id: RunId,
        parent_activation_id: ActivationId,
        parent_attempt_no: AttemptNo,
        model_call_no: u32,
        identity: ModelToolTaskIdentity,
        arguments: Value,
        tool_attempt_no: AttemptNo,
        lease_epoch: LeaseEpoch,
        fencing_token: String,
        claimed_by: String,
        claim_token: String,
        claim_expires_at: DateTime<Utc>,
        projection_version: u64,
    ) -> Result<Self, RepositoryError> {
        if model_call_no == 0
            || !arguments.is_object()
            || serde_jcs::to_vec(&arguments)
                .map_err(|_| RepositoryError::canonicalization())?
                .len()
                > MAX_MODEL_TOOL_ARGUMENT_BYTES
            || fencing_token.is_empty()
            || claimed_by.is_empty()
            || claim_token.is_empty()
            || projection_version == 0
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            run_id,
            parent_activation_id,
            parent_attempt_no,
            model_call_no,
            identity,
            arguments,
            tool_attempt_no,
            lease_epoch,
            fencing_token,
            claimed_by,
            claim_token,
            claim_expires_at,
            projection_version,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn parent_activation_id(&self) -> &ActivationId {
        &self.parent_activation_id
    }
    pub fn parent_attempt_no(&self) -> AttemptNo {
        self.parent_attempt_no
    }
    pub fn model_call_no(&self) -> u32 {
        self.model_call_no
    }
    pub fn identity(&self) -> &ModelToolTaskIdentity {
        &self.identity
    }
    pub fn arguments(&self) -> &Value {
        &self.arguments
    }
    pub fn tool_attempt_no(&self) -> AttemptNo {
        self.tool_attempt_no
    }
    pub fn lease_epoch(&self) -> LeaseEpoch {
        self.lease_epoch
    }
    pub fn fencing_token(&self) -> &str {
        &self.fencing_token
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
    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }
}

impl insight_engine::worker::ModelToolTaskClaimView for ModelToolTaskClaim {
    fn model_tool_execution_spec(
        &self,
    ) -> Result<insight_engine::worker::ModelToolExecutionSpec, &'static str> {
        let identity = self.identity();
        let action = identity.action();
        let action = insight_engine::worker::ModelToolActionExecutionSpec::new(
            action.action_id(),
            action.action_version(),
            action.descriptor_hash(),
            action.input_schema().clone(),
            action.effect_policy().clone(),
            action.deployment_binding().clone(),
        );
        insight_engine::worker::ModelToolExecutionSpec::new(
            self.run_id().clone(),
            self.parent_activation_id().clone(),
            self.model_call_no(),
            identity.tool_task_id().clone(),
            identity.effect_id().clone(),
            identity.call_index(),
            action,
            self.arguments().clone(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelToolFailureClass {
    Safe,
    Infrastructure,
    EffectOutcomeUnknown,
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ModelToolTaskOutcome {
    Succeeded {
        result: Value,
    },
    Failed {
        class: ModelToolFailureClass,
        code: String,
        retryable: bool,
        effect_evidence: EffectEvidence,
    },
    Cancelled {
        code: String,
        effect_evidence: EffectEvidence,
    },
}

impl ModelToolTaskOutcome {
    pub fn succeeded(result: Value) -> Result<Self, &'static str> {
        let bytes = serde_jcs::to_vec(&result).map_err(|_| "MODEL_TOOL_RESULT_INVALID")?;
        if bytes.len() > MAX_MODEL_TOOL_RESULT_BYTES {
            return Err("MODEL_TOOL_RESULT_TOO_LARGE");
        }
        Ok(Self::Succeeded { result })
    }

    pub fn failed(
        class: ModelToolFailureClass,
        code: impl Into<String>,
        retryable: bool,
        effect_evidence: EffectEvidence,
    ) -> Result<Self, &'static str> {
        let code = code.into();
        if !valid_body_free_code(&code)
            || !matches!(
                effect_evidence,
                EffectEvidence::NotStarted | EffectEvidence::Started | EffectEvidence::Unknown
            )
            || (class == ModelToolFailureClass::EffectOutcomeUnknown
                && effect_evidence != EffectEvidence::Unknown)
        {
            return Err("MODEL_TOOL_FAILURE_INVALID");
        }
        Ok(Self::Failed {
            class,
            code,
            retryable,
            effect_evidence,
        })
    }

    pub fn cancelled(
        code: impl Into<String>,
        effect_evidence: EffectEvidence,
    ) -> Result<Self, &'static str> {
        let code = code.into();
        if !valid_body_free_code(&code)
            || !matches!(
                effect_evidence,
                EffectEvidence::NotStarted | EffectEvidence::Started | EffectEvidence::Unknown
            )
        {
            return Err("MODEL_TOOL_CANCELLATION_INVALID");
        }
        Ok(Self::Cancelled {
            code,
            effect_evidence,
        })
    }

    pub(crate) fn canonical_hash(&self) -> Result<ContentHash, RepositoryError> {
        let bytes = serde_jcs::to_vec(self).map_err(|_| RepositoryError::canonicalization())?;
        Ok(ContentHash::from_bytes(&bytes))
    }
}

fn valid_body_free_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolTaskDisposition {
    Succeeded,
    RetryScheduled,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelToolContinuationStatus {
    WaitingTools,
    ReadyContinue,
    ReadyFailed,
    ReadyCancelled,
}

impl ModelToolContinuationStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::WaitingTools => "waiting_tools",
            Self::ReadyContinue => "ready_continue",
            Self::ReadyFailed => "ready_failed",
            Self::ReadyCancelled => "ready_cancelled",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "waiting_tools" => Ok(Self::WaitingTools),
            "ready_continue" => Ok(Self::ReadyContinue),
            "ready_failed" => Ok(Self::ReadyFailed),
            "ready_cancelled" => Ok(Self::ReadyCancelled),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

impl ModelToolTaskDisposition {
    pub(crate) fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "succeeded" => Ok(Self::Succeeded),
            "retry_scheduled" => Ok(Self::RetryScheduled),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct ModelToolTaskCommitReceipt {
    tool_task_id: SchedulerTaskId,
    disposition: ModelToolTaskDisposition,
    committed_attempt_no: AttemptNo,
    committed_lease_epoch: LeaseEpoch,
    next_available_at: Option<DateTime<Utc>>,
    continuation_status: ModelToolContinuationStatus,
    duration_ms: Option<u64>,
}

impl ModelToolTaskCommitReceipt {
    pub(crate) fn new(
        tool_task_id: SchedulerTaskId,
        disposition: ModelToolTaskDisposition,
        committed_attempt_no: AttemptNo,
        committed_lease_epoch: LeaseEpoch,
        next_available_at: Option<DateTime<Utc>>,
        continuation_status: ModelToolContinuationStatus,
        duration_ms: Option<u64>,
    ) -> Result<Self, RepositoryError> {
        if (disposition == ModelToolTaskDisposition::RetryScheduled) != next_available_at.is_some()
            || (disposition == ModelToolTaskDisposition::RetryScheduled && duration_ms.is_some())
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            tool_task_id,
            disposition,
            committed_attempt_no,
            committed_lease_epoch,
            next_available_at,
            continuation_status,
            duration_ms,
        })
    }

    pub fn tool_task_id(&self) -> &SchedulerTaskId {
        &self.tool_task_id
    }
    pub fn disposition(&self) -> ModelToolTaskDisposition {
        self.disposition
    }
    pub fn committed_attempt_no(&self) -> AttemptNo {
        self.committed_attempt_no
    }
    pub fn committed_lease_epoch(&self) -> LeaseEpoch {
        self.committed_lease_epoch
    }
    pub fn next_available_at(&self) -> Option<DateTime<Utc>> {
        self.next_available_at
    }
    pub fn continuation_status(&self) -> ModelToolContinuationStatus {
        self.continuation_status
    }
    /// Elapsed wall-clock time from the logical call's first durable start to
    /// its terminal commit. Retries intentionally do not reset this clock.
    pub fn duration_ms(&self) -> Option<u64> {
        self.duration_ms
    }
}

#[derive(Clone, PartialEq)]
pub enum ModelToolTaskTransitionOutcome<T> {
    Committed(T),
    ExactReplay(T),
    StaleLease,
    StateConflict,
    RunTerminal,
}

#[derive(Clone, PartialEq)]
pub enum ModelToolTaskHeartbeatOutcome {
    Renewed(Box<ModelToolTaskClaim>),
    StaleLease,
    StateConflict,
    RunTerminal,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolLimitsWire {
    max_rounds: u32,
    max_calls: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolBindingWire {
    name: String,
    title: Option<String>,
    description: Option<String>,
    action_id: String,
    action_version: String,
    descriptor_hash: String,
    input_schema: Value,
    output_schema: Value,
    effect: String,
    idempotency: String,
    cancellation: String,
    required_capabilities: Vec<String>,
    effect_policy: WorkerEffectPolicy,
    public_policy: Value,
    effective_public_policy: Value,
}

pub(crate) struct FrozenModelToolContract {
    pub max_rounds: u32,
    pub max_calls: u32,
    pub tools: BTreeMap<String, FrozenModelToolAction>,
}

pub(crate) fn parse_frozen_model_tool_contract(
    deployment_binding: &Value,
) -> Result<FrozenModelToolContract, RepositoryError> {
    serde_jcs::to_vec(deployment_binding).map_err(|_| RepositoryError::canonicalization())?;
    let object = deployment_binding
        .as_object()
        .ok_or_else(RepositoryError::invalid_data)?;
    if object.get("adapter").and_then(Value::as_str) != Some("core.llm") {
        return Err(RepositoryError::invalid_data());
    }
    let limits: ToolLimitsWire = serde_json::from_value(
        object
            .get("tool_limits")
            .cloned()
            .ok_or_else(RepositoryError::invalid_data)?,
    )
    .map_err(|_| RepositoryError::invalid_data())?;
    if limits.max_rounds == 0
        || limits.max_calls == 0
        || limits.max_rounds > limits.max_calls
        || limits.max_calls > 1_024
    {
        return Err(RepositoryError::invalid_data());
    }
    let tools = object
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(RepositoryError::invalid_data)?;
    let mut frozen = BTreeMap::new();
    for value in tools {
        let wire: ToolBindingWire =
            serde_json::from_value(value.clone()).map_err(|_| RepositoryError::invalid_data())?;
        if !valid_model_tool_name(&wire.name)
            || !valid_qualified_name(&wire.action_id)
            || wire.title.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > 512 || value.chars().any(char::is_control)
            })
            || wire.description.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > 16 * 1024 || value.chars().any(char::is_control)
            })
            || wire.action_version.is_empty()
            || wire.action_version.len() > 64
            || !is_lower_sha256(&wire.descriptor_hash)
            || wire
                .required_capabilities
                .iter()
                .any(|value| !valid_qualified_name(value))
            || wire
                .required_capabilities
                .windows(2)
                .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
            || !wire.public_policy.is_object()
            || !wire.effective_public_policy.is_object()
        {
            return Err(RepositoryError::invalid_data());
        }
        compile_schema_2020(&wire.input_schema).map_err(|_| RepositoryError::invalid_data())?;
        compile_schema_2020(&wire.output_schema).map_err(|_| RepositoryError::invalid_data())?;
        let expected_effect = match wire.effect.as_str() {
            "pure" => WorkerEffectClass::Pure,
            "read_only" => WorkerEffectClass::ReadOnly,
            "mutating" => WorkerEffectClass::Mutating,
            _ => return Err(RepositoryError::invalid_data()),
        };
        let expected_idempotency = match wire.idempotency.as_str() {
            "idempotent" => EffectIdempotency::Idempotent,
            "non_idempotent" => EffectIdempotency::NonIdempotent,
            _ => return Err(RepositoryError::invalid_data()),
        };
        let expected_cancellation = match wire.cancellation.as_str() {
            "cooperative" => WorkerCancellation::Cooperative,
            "not_supported" => WorkerCancellation::LeaseOnly,
            _ => return Err(RepositoryError::invalid_data()),
        };
        if wire.effect_policy.effect_class() != expected_effect
            || wire.effect_policy.effect_idempotency() != expected_idempotency
            || wire.effect_policy.cancellation() != expected_cancellation
        {
            return Err(RepositoryError::invalid_data());
        }
        let deployment_binding = json!({
            "adapter": "native_action",
            "action_id": wire.action_id,
            "action_version": wire.action_version,
            "descriptor_hash": wire.descriptor_hash,
            "effect": wire.effect,
            "idempotency": wire.idempotency,
            "cancellation": wire.cancellation,
            "required_capabilities": wire.required_capabilities,
            "public": wire.public_policy,
        });
        let action = FrozenModelToolAction {
            name: wire.name.clone(),
            action_id: deployment_binding["action_id"]
                .as_str()
                .expect("constructed action id")
                .to_owned(),
            action_version: deployment_binding["action_version"]
                .as_str()
                .expect("constructed action version")
                .to_owned(),
            descriptor_hash: deployment_binding["descriptor_hash"]
                .as_str()
                .expect("constructed descriptor hash")
                .to_owned(),
            input_schema: wire.input_schema,
            output_schema: wire.output_schema,
            effect_policy: wire.effect_policy,
            deployment_binding,
            effective_public_policy: wire.effective_public_policy,
        };
        if frozen.insert(wire.name, action).is_some() {
            return Err(RepositoryError::invalid_data());
        }
    }
    Ok(FrozenModelToolContract {
        max_rounds: limits.max_rounds,
        max_calls: limits.max_calls,
        tools: frozen,
    })
}

fn valid_model_tool_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub(crate) fn validate_tool_arguments(
    action: &FrozenModelToolAction,
    arguments: &Value,
) -> Result<(), RepositoryError> {
    let bytes = serde_jcs::to_vec(arguments).map_err(|_| RepositoryError::canonicalization())?;
    if !arguments.is_object()
        || bytes.len() > MAX_MODEL_TOOL_ARGUMENT_BYTES
        || !compile_schema_2020(action.input_schema())
            .map_err(|_| RepositoryError::invalid_data())?
            .is_valid(arguments)
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

pub(crate) fn validate_tool_result(
    action: &FrozenModelToolAction,
    result: &Value,
) -> Result<(), RepositoryError> {
    let bytes = serde_jcs::to_vec(result).map_err(|_| RepositoryError::canonicalization())?;
    if bytes.len() > MAX_MODEL_TOOL_RESULT_BYTES
        || !compile_schema_2020(action.output_schema())
            .map_err(|_| RepositoryError::invalid_data())?
            .is_valid(result)
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn deterministic_tool_identity(
    run_id: &RunId,
    activation_id: &ActivationId,
    parent_attempt_no: AttemptNo,
    model_call_no: u32,
    call_index: u32,
    call_id: &str,
    action: FrozenModelToolAction,
    public_item: Option<ResponseItemAuthority>,
    public_arguments_jcs: Option<String>,
    public_seal_index: Option<u64>,
) -> Result<ModelToolTaskIdentity, RepositoryError> {
    if public_item.is_some() != public_arguments_jcs.is_some()
        || public_item.is_some() != public_seal_index.is_some()
        || public_seal_index.is_some_and(|seal| seal < 3)
    {
        return Err(RepositoryError::invalid_data());
    }
    if let Some(arguments) = &public_arguments_jcs {
        let value: Value =
            serde_json::from_str(arguments).map_err(|_| RepositoryError::invalid_data())?;
        if !value.is_object()
            || serde_jcs::to_string(&value).map_err(|_| RepositoryError::canonicalization())?
                != *arguments
        {
            return Err(RepositoryError::invalid_data());
        }
    }
    let evidence = json!({
        "run_id": run_id,
        "activation_id": activation_id,
        "parent_attempt_no": parent_attempt_no,
        "model_call_no": model_call_no,
        "call_index": call_index,
        "call_id": call_id,
        "action_id": action.action_id,
        "action_version": action.action_version,
        "descriptor_hash": action.descriptor_hash,
    });
    let canonical =
        serde_jcs::to_vec(&evidence).map_err(|_| RepositoryError::canonicalization())?;
    let task_hash = ContentHash::from_bytes(
        &[b"model_tool_task.v1\0".as_slice(), canonical.as_slice()].concat(),
    );
    let effect_hash = ContentHash::from_bytes(
        &[b"model_tool_effect.v1\0".as_slice(), canonical.as_slice()].concat(),
    );
    let tool_task_id = SchedulerTaskId::parse(format!(
        "task_{}",
        task_hash
            .as_str()
            .strip_prefix("sha256:")
            .ok_or_else(RepositoryError::invalid_data)?
    ))
    .map_err(|_| RepositoryError::invalid_data())?;
    let effect_id = EffectId::new(format!(
        "effect_{}",
        effect_hash
            .as_str()
            .strip_prefix("sha256:")
            .ok_or_else(RepositoryError::invalid_data)?
    ))
    .map_err(|_| RepositoryError::invalid_data())?;
    Ok(ModelToolTaskIdentity {
        tool_task_id,
        effect_id,
        call_index,
        call_id: call_id.to_owned(),
        action,
        public_item,
        public_arguments_jcs,
        public_seal_index,
    })
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_qualified_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.split('.').all(|segment| {
            let mut bytes = segment.bytes();
            bytes
                .next()
                .is_some_and(|first| first == b'_' || first.is_ascii_alphabetic())
                && bytes.all(|byte| byte == b'_' || byte == b'-' || byte.is_ascii_alphanumeric())
        })
}

pub(crate) fn parse_action_from_stored_evidence(
    evidence: StoredModelToolActionEvidence,
) -> Result<FrozenModelToolAction, RepositoryError> {
    let StoredModelToolActionEvidence {
        name,
        action_id,
        action_version,
        descriptor_hash,
        input_schema,
        output_schema,
        effect_policy,
        deployment_binding,
        effective_public_policy,
    } = evidence;
    let synthetic = json!({
        "adapter": "core.llm",
        "tool_limits": {"max_rounds": 1, "max_calls": 1},
        "tools": [{
            "name": name,
            "action_id": action_id,
            "action_version": action_version,
            "descriptor_hash": descriptor_hash,
            "input_schema": input_schema,
            "output_schema": output_schema,
            "effect": deployment_binding.get("effect").cloned().unwrap_or(Value::Null),
            "idempotency": deployment_binding.get("idempotency").cloned().unwrap_or(Value::Null),
            "cancellation": deployment_binding.get("cancellation").cloned().unwrap_or(Value::Null),
            "required_capabilities": deployment_binding.get("required_capabilities").cloned().unwrap_or(Value::Null),
            "effect_policy": effect_policy,
            "public_policy": deployment_binding.get("public").cloned().unwrap_or(Value::Null),
            "effective_public_policy": effective_public_policy,
        }]
    });
    parse_frozen_model_tool_contract(&synthetic)?
        .tools
        .into_values()
        .next()
        .ok_or_else(RepositoryError::invalid_data)
}

pub(crate) struct StoredModelToolActionEvidence {
    pub name: String,
    pub action_id: String,
    pub action_version: String,
    pub descriptor_hash: String,
    pub input_schema: Value,
    pub output_schema: Value,
    pub effect_policy: WorkerEffectPolicy,
    pub deployment_binding: Value,
    pub effective_public_policy: Value,
}

#[doc(hidden)]
pub mod adapter {
    use super::*;

    pub fn prepare_model_function_call_publications(
        run_id: &RunId,
        activation_id: &ActivationId,
        attempt_no: AttemptNo,
        batch: &ModelToolCallBatch,
        deployment_binding: &Value,
        publish: bool,
    ) -> Result<Vec<PreparedResponseFunctionPublication>, RepositoryError> {
        super::prepare_model_function_call_publications(
            run_id,
            activation_id,
            attempt_no,
            batch,
            deployment_binding,
            publish,
        )
    }

    pub fn model_tool_task_status_parse(
        value: &str,
    ) -> Result<ModelToolTaskStatus, RepositoryError> {
        ModelToolTaskStatus::parse(value)
    }

    pub fn model_tool_batch_activation_new(
        run_id: RunId,
        activation_id: ActivationId,
        parent_attempt_no: AttemptNo,
        model_call_no: u32,
        tasks: Vec<ModelToolTaskIdentity>,
    ) -> Result<ModelToolBatchActivation, RepositoryError> {
        ModelToolBatchActivation::new(
            run_id,
            activation_id,
            parent_attempt_no,
            model_call_no,
            tasks,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn model_tool_task_claim_new(
        run_id: RunId,
        parent_activation_id: ActivationId,
        parent_attempt_no: AttemptNo,
        model_call_no: u32,
        identity: ModelToolTaskIdentity,
        arguments: Value,
        tool_attempt_no: AttemptNo,
        lease_epoch: LeaseEpoch,
        fencing_token: String,
        claimed_by: String,
        claim_token: String,
        claim_expires_at: DateTime<Utc>,
        projection_version: u64,
    ) -> Result<ModelToolTaskClaim, RepositoryError> {
        ModelToolTaskClaim::new(
            run_id,
            parent_activation_id,
            parent_attempt_no,
            model_call_no,
            identity,
            arguments,
            tool_attempt_no,
            lease_epoch,
            fencing_token,
            claimed_by,
            claim_token,
            claim_expires_at,
            projection_version,
        )
    }

    pub fn model_tool_task_outcome_canonical_hash(
        outcome: &ModelToolTaskOutcome,
    ) -> Result<ContentHash, RepositoryError> {
        ModelToolTaskOutcome::canonical_hash(outcome)
    }

    pub fn model_tool_continuation_status_as_str(
        status: ModelToolContinuationStatus,
    ) -> &'static str {
        ModelToolContinuationStatus::as_str(status)
    }

    pub fn model_tool_continuation_status_parse(
        value: &str,
    ) -> Result<ModelToolContinuationStatus, RepositoryError> {
        ModelToolContinuationStatus::parse(value)
    }

    pub fn model_tool_task_disposition_parse(
        value: &str,
    ) -> Result<ModelToolTaskDisposition, RepositoryError> {
        ModelToolTaskDisposition::parse(value)
    }

    pub fn model_tool_task_commit_receipt_new(
        tool_task_id: SchedulerTaskId,
        disposition: ModelToolTaskDisposition,
        committed_attempt_no: AttemptNo,
        committed_lease_epoch: LeaseEpoch,
        next_available_at: Option<DateTime<Utc>>,
        continuation_status: ModelToolContinuationStatus,
        duration_ms: Option<u64>,
    ) -> Result<ModelToolTaskCommitReceipt, RepositoryError> {
        ModelToolTaskCommitReceipt::new(
            tool_task_id,
            disposition,
            committed_attempt_no,
            committed_lease_epoch,
            next_available_at,
            continuation_status,
            duration_ms,
        )
    }

    pub fn parse_frozen_model_tool_contract(
        deployment_binding: &Value,
    ) -> Result<(u32, u32, BTreeMap<String, FrozenModelToolAction>), RepositoryError> {
        let contract = super::parse_frozen_model_tool_contract(deployment_binding)?;
        Ok((contract.max_rounds, contract.max_calls, contract.tools))
    }

    pub fn validate_tool_arguments(
        action: &FrozenModelToolAction,
        arguments: &Value,
    ) -> Result<(), RepositoryError> {
        super::validate_tool_arguments(action, arguments)
    }

    pub fn validate_tool_result(
        action: &FrozenModelToolAction,
        result: &Value,
    ) -> Result<(), RepositoryError> {
        super::validate_tool_result(action, result)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn deterministic_tool_identity(
        run_id: &RunId,
        activation_id: &ActivationId,
        parent_attempt_no: AttemptNo,
        model_call_no: u32,
        call_index: u32,
        call_id: &str,
        action: FrozenModelToolAction,
        public_item: Option<ResponseItemAuthority>,
        public_arguments_jcs: Option<String>,
        public_seal_index: Option<u64>,
    ) -> Result<ModelToolTaskIdentity, RepositoryError> {
        super::deterministic_tool_identity(
            run_id,
            activation_id,
            parent_attempt_no,
            model_call_no,
            call_index,
            call_id,
            action,
            public_item,
            public_arguments_jcs,
            public_seal_index,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn parse_action_from_stored_evidence(
        name: String,
        action_id: String,
        action_version: String,
        descriptor_hash: String,
        input_schema: Value,
        output_schema: Value,
        effect_policy: WorkerEffectPolicy,
        deployment_binding: Value,
        effective_public_policy: Value,
    ) -> Result<FrozenModelToolAction, RepositoryError> {
        super::parse_action_from_stored_evidence(StoredModelToolActionEvidence {
            name,
            action_id,
            action_version,
            descriptor_hash,
            input_schema,
            output_schema,
            effect_policy,
            deployment_binding,
            effective_public_policy,
        })
    }
}
