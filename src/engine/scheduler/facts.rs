use std::collections::{BTreeMap, BTreeSet};

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::engine::{
    plan::{BranchCaseId, ControlPortId, DataPortId, LoopFlavor, PlanJoinMode, PlanType, ScopeId},
    ActivationId, ControlTokenId, ForkGroupId, LegId, MapItemIdentity, NodeId, PublicErrorCode,
    RunId, ScopeInstanceId, SignalId, TerminationReason, TimerId, WorkerFailureClass,
};

use super::{
    LogicalOccurrence, SchedulerCheckpointId, SchedulerError, SchedulerTaskId, SchedulerWaitId,
    SCHEDULER_FACT_INCONSISTENT, SCHEDULER_VALUE_TYPE_MISMATCH,
};

const MAX_FAILURE_CODE_BYTES: usize = 128;

/// A canonical JSON value paired with its exact literal type.
///
/// Runtime facts cannot claim that an arbitrary value has a broader or
/// unrelated type: the type is always derived from the value itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeValue {
    value: Value,
    value_type: PlanType,
}

impl<'de> Deserialize<'de> for RuntimeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            value: Value,
            value_type: PlanType,
        }

        let wire = Wire::deserialize(deserializer)?;
        let authoritative = RuntimeValue::new(wire.value).map_err(D::Error::custom)?;
        if authoritative.value_type != wire.value_type {
            return Err(D::Error::custom(
                "runtime value type must equal the canonical literal type derived from value",
            ));
        }
        Ok(authoritative)
    }
}

impl RuntimeValue {
    pub fn new(value: Value) -> Result<Self, SchedulerError> {
        let value_type = PlanType::literal(value).map_err(|error| {
            SchedulerError::new(
                SCHEDULER_VALUE_TYPE_MISMATCH,
                format!("runtime value is outside the canonical Plan value domain: {error}"),
            )
        })?;
        let PlanType::Literal { value } = &value_type else {
            unreachable!("PlanType::literal always returns a normalized Literal")
        };
        Ok(Self {
            value: value.clone(),
            value_type,
        })
    }

    pub fn value(&self) -> &Value {
        &self.value
    }

    pub fn value_type(&self) -> &PlanType {
        &self.value_type
    }

    pub fn matches(&self, expected: &PlanType) -> bool {
        self.value_type.is_assignable_to(expected)
    }
}

/// The only runtime value that may cross the safe-business-failure boundary.
///
/// The inner `RuntimeValue` remains useful for ordinary Plan ports, but it can
/// enter Catch, `all_settled`, or a public workflow failure only after this
/// constructor validates the closed public contract. Deserialization repeats
/// the validation so corrupted durable facts fail closed on restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct SafeError {
    runtime_value: RuntimeValue,
}

impl<'de> Deserialize<'de> for SafeError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let runtime_value = RuntimeValue::deserialize(deserializer)?;
        Self::try_from(runtime_value).map_err(D::Error::custom)
    }
}

impl TryFrom<RuntimeValue> for SafeError {
    type Error = SchedulerError;

    fn try_from(runtime_value: RuntimeValue) -> Result<Self, Self::Error> {
        let object = runtime_value.value().as_object().ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_VALUE_TYPE_MISMATCH,
                "safe business error must be a closed object",
            )
        })?;
        if object.len() != 3 || object.get("kind") != Some(&Value::String("safe_error".to_owned()))
        {
            return Err(SchedulerError::new(
                SCHEDULER_VALUE_TYPE_MISMATCH,
                "safe business error must contain exactly kind, code, and message",
            ));
        }
        let code = object.get("code").and_then(Value::as_str).ok_or_else(|| {
            SchedulerError::new(
                SCHEDULER_VALUE_TYPE_MISMATCH,
                "safe business error code must be a public symbolic string",
            )
        })?;
        PublicErrorCode::new(code).map_err(|_| {
            SchedulerError::new(
                SCHEDULER_VALUE_TYPE_MISMATCH,
                "safe business error code must be a bounded uppercase identifier",
            )
        })?;
        let message = object
            .get("message")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                SchedulerError::new(
                    SCHEDULER_VALUE_TYPE_MISMATCH,
                    "safe business error message must be a string",
                )
            })?;
        if message.trim().is_empty() || message.chars().count() > 512 {
            return Err(SchedulerError::new(
                SCHEDULER_VALUE_TYPE_MISMATCH,
                "safe business error message must be non-empty and at most 512 characters",
            ));
        }
        Ok(Self { runtime_value })
    }
}

impl SafeError {
    pub fn new(code: PublicErrorCode, message: impl Into<String>) -> Result<Self, SchedulerError> {
        Self::try_from(RuntimeValue::new(serde_json::json!({
            "kind": "safe_error",
            "code": code.as_str(),
            "message": message.into(),
        }))?)
    }

    pub fn runtime_value(&self) -> &RuntimeValue {
        &self.runtime_value
    }

    pub fn value(&self) -> &Value {
        self.runtime_value.value()
    }

    pub fn into_runtime_value(self) -> RuntimeValue {
        self.runtime_value
    }

    pub fn code(&self) -> &str {
        self.runtime_value.value()["code"]
            .as_str()
            .expect("SafeError constructor validates code")
    }

    pub fn message(&self) -> &str {
        self.runtime_value.value()["message"]
            .as_str()
            .expect("SafeError constructor validates message")
    }
}

/// Body-free failure evidence emitted by a worker or child run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskFailureFact {
    class: WorkerFailureClass,
    code: String,
    safe_error: Option<SafeError>,
}

impl<'de> Deserialize<'de> for TaskFailureFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            class: WorkerFailureClass,
            code: String,
            safe_error: Option<RuntimeValue>,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.class, wire.code, wire.safe_error).map_err(D::Error::custom)
    }
}

impl TaskFailureFact {
    pub fn new(
        class: WorkerFailureClass,
        code: impl Into<String>,
        safe_error: Option<RuntimeValue>,
    ) -> Result<Self, SchedulerError> {
        let code = code.into();
        if code.is_empty()
            || code.len() > MAX_FAILURE_CODE_BYTES
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            || code.as_bytes()[0].is_ascii_digit()
        {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "task failure code must be a bounded body-free symbolic code",
            ));
        }
        if matches!(class, WorkerFailureClass::SafeBusinessFailure) != safe_error.is_some() {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "only safe business failures may carry one typed public error",
            ));
        }
        let safe_error = safe_error.map(SafeError::try_from).transpose()?;
        if safe_error
            .as_ref()
            .is_some_and(|safe_error| safe_error.code() != code)
        {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "safe business failure code must equal its SafeError code",
            ));
        }
        Ok(Self {
            class,
            code,
            safe_error,
        })
    }

    pub fn class(&self) -> WorkerFailureClass {
        self.class
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn safe_error(&self) -> Option<&RuntimeValue> {
        self.safe_error.as_ref().map(SafeError::runtime_value)
    }

    pub(crate) fn typed_safe_error(&self) -> Option<&SafeError> {
        self.safe_error.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskOutcomeFact {
    Succeeded {
        outputs: BTreeMap<DataPortId, RuntimeValue>,
    },
    Failed {
        failure: TaskFailureFact,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "outcome",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RunTerminalFact {
    Succeeded(RuntimeValue),
    Failed(SafeError),
    FailedInternal(TaskFailureFact),
    FailedPlanning(super::SchedulerPlanningFailure),
    Cancelled,
    TimedOut,
    Interrupted,
}

/// Pending recovery hint projected from `run_reuse_candidates`. The scheduler
/// matches all three target identities before choosing the closed admission
/// action; repository code remains the final compatibility authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReuseCandidateFact {
    candidate_id: String,
    target_scope_instance_id: ScopeInstanceId,
    target_node_id: NodeId,
    occurrence: LogicalOccurrence,
    projection_version: u64,
}

impl ReuseCandidateFact {
    pub fn new(
        candidate_id: impl Into<String>,
        target_scope_instance_id: ScopeInstanceId,
        target_node_id: NodeId,
        occurrence: LogicalOccurrence,
        projection_version: u64,
    ) -> Result<Self, SchedulerError> {
        let candidate_id = candidate_id.into();
        if candidate_id.is_empty()
            || candidate_id.len() > 256
            || candidate_id
                .chars()
                .any(|value| value.is_control() || value.is_whitespace())
        {
            return Err(inconsistent("reuse candidate id is not a durable label"));
        }
        Ok(Self {
            candidate_id,
            target_scope_instance_id,
            target_node_id,
            occurrence,
            projection_version,
        })
    }

    pub fn candidate_id(&self) -> &str {
        &self.candidate_id
    }
    pub fn target_scope_instance_id(&self) -> &ScopeInstanceId {
        &self.target_scope_instance_id
    }
    pub fn target_node_id(&self) -> &NodeId {
        &self.target_node_id
    }
    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }
    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }
}

/// A Redrive-only effect lineage mapping for a source Activation whose result
/// was not materialized as reuse. It is keyed by immutable node/occurrence
/// identity so a restarted scheduler emits the inherited provider key in the
/// normal DispatchTask intent. Fork never projects these facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedriveEffectFact {
    source_activation_id: ActivationId,
    target_node_id: NodeId,
    occurrence: LogicalOccurrence,
    effect_id: crate::engine::EffectId,
}

impl RedriveEffectFact {
    pub fn new(
        source_activation_id: ActivationId,
        target_node_id: NodeId,
        occurrence: LogicalOccurrence,
        effect_id: crate::engine::EffectId,
    ) -> Self {
        Self {
            source_activation_id,
            target_node_id,
            occurrence,
            effect_id,
        }
    }

    pub fn source_activation_id(&self) -> &ActivationId {
        &self.source_activation_id
    }

    pub fn target_node_id(&self) -> &NodeId {
        &self.target_node_id
    }

    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }

    pub fn effect_id(&self) -> &crate::engine::EffectId {
        &self.effect_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OccurrenceValueKey {
    occurrence: LogicalOccurrence,
    port_id: DataPortId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OccurrenceNodeKey {
    occurrence: LogicalOccurrence,
    node_id: NodeId,
}

impl OccurrenceNodeKey {
    pub fn new(occurrence: LogicalOccurrence, node_id: NodeId) -> Self {
        Self {
            occurrence,
            node_id,
        }
    }
}

impl OccurrenceValueKey {
    pub fn new(occurrence: LogicalOccurrence, port_id: DataPortId) -> Self {
        Self {
            occurrence,
            port_id,
        }
    }

    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }

    pub fn port_id(&self) -> &DataPortId {
        &self.port_id
    }
}

/// The successor that a committed Branch decision admits and consumes into.
///
/// Keeping the complete admission in the same closed fact prevents a durable
/// Branch selection from existing without its correlated successor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessorAdmissionFact {
    pub(crate) activation_id: ActivationId,
    pub(crate) node_id: NodeId,
    pub(crate) scope_instance_id: ScopeInstanceId,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) input_port: ControlPortId,
}

impl SuccessorAdmissionFact {
    pub fn new(
        activation_id: ActivationId,
        node_id: NodeId,
        scope_instance_id: ScopeInstanceId,
        occurrence: LogicalOccurrence,
        input_port: ControlPortId,
    ) -> Self {
        Self {
            activation_id,
            node_id,
            scope_instance_id,
            occurrence,
            input_port,
        }
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }

    pub fn input_port(&self) -> &ControlPortId {
        &self.input_port
    }
}

/// One atomic exclusive-branch decision, correlated token emission, successor
/// admission, and immediate token consumption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BranchSelectionAdmissionFact {
    pub(crate) branch_activation_id: ActivationId,
    pub(crate) branch_node_id: NodeId,
    pub(crate) branch_scope_instance_id: ScopeInstanceId,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) case_id: BranchCaseId,
    pub(crate) output_port: ControlPortId,
    pub(crate) token_id: ControlTokenId,
    pub(crate) successor: SuccessorAdmissionFact,
}

impl<'de> Deserialize<'de> for BranchSelectionAdmissionFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            branch_activation_id: ActivationId,
            branch_node_id: NodeId,
            branch_scope_instance_id: ScopeInstanceId,
            occurrence: LogicalOccurrence,
            case_id: BranchCaseId,
            output_port: ControlPortId,
            token_id: ControlTokenId,
            successor: SuccessorAdmissionFact,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.branch_activation_id,
            wire.branch_node_id,
            wire.branch_scope_instance_id,
            wire.occurrence,
            wire.case_id,
            wire.output_port,
            wire.token_id,
            wire.successor,
        )
        .map_err(D::Error::custom)
    }
}

impl BranchSelectionAdmissionFact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        branch_activation_id: ActivationId,
        branch_node_id: NodeId,
        branch_scope_instance_id: ScopeInstanceId,
        occurrence: LogicalOccurrence,
        case_id: BranchCaseId,
        output_port: ControlPortId,
        token_id: ControlTokenId,
        successor: SuccessorAdmissionFact,
    ) -> Result<Self, SchedulerError> {
        if branch_activation_id == *successor.activation_id()
            || successor.occurrence().parent().as_ref() != Some(&occurrence)
        {
            return Err(inconsistent(
                "Branch successor must be a distinct activation in the selected edge occurrence",
            ));
        }
        Ok(Self {
            branch_activation_id,
            branch_node_id,
            branch_scope_instance_id,
            occurrence,
            case_id,
            output_port,
            token_id,
            successor,
        })
    }

    pub fn branch_activation_id(&self) -> &ActivationId {
        &self.branch_activation_id
    }

    pub fn branch_node_id(&self) -> &NodeId {
        &self.branch_node_id
    }

    pub fn branch_scope_instance_id(&self) -> &ScopeInstanceId {
        &self.branch_scope_instance_id
    }

    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }

    pub fn case_id(&self) -> &BranchCaseId {
        &self.case_id
    }

    pub fn output_port(&self) -> &ControlPortId {
        &self.output_port
    }

    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }

    pub fn successor(&self) -> &SuccessorAdmissionFact {
        &self.successor
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ForkGroupFact {
    pub(crate) group_id: ForkGroupId,
    pub(crate) fork_node_id: NodeId,
    pub(crate) fork_activation_id: ActivationId,
    pub(crate) parent_scope_instance_id: ScopeInstanceId,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) mode: PlanJoinMode,
    pub(crate) members: Vec<LegId>,
}

impl<'de> Deserialize<'de> for ForkGroupFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            group_id: ForkGroupId,
            fork_node_id: NodeId,
            fork_activation_id: ActivationId,
            parent_scope_instance_id: ScopeInstanceId,
            occurrence: LogicalOccurrence,
            mode: PlanJoinMode,
            members: Vec<LegId>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.group_id,
            wire.fork_node_id,
            wire.fork_activation_id,
            wire.parent_scope_instance_id,
            wire.occurrence,
            wire.mode,
            wire.members,
        )
        .map_err(D::Error::custom)
    }
}

impl ForkGroupFact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group_id: ForkGroupId,
        fork_node_id: NodeId,
        fork_activation_id: ActivationId,
        parent_scope_instance_id: ScopeInstanceId,
        occurrence: LogicalOccurrence,
        mode: PlanJoinMode,
        members: Vec<LegId>,
    ) -> Result<Self, SchedulerError> {
        if members.is_empty() || members.iter().collect::<BTreeSet<_>>().len() != members.len() {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "fork group members must be non-empty and unique",
            ));
        }
        Ok(Self {
            group_id,
            fork_node_id,
            fork_activation_id,
            parent_scope_instance_id,
            occurrence,
            mode,
            members,
        })
    }

    pub fn group_id(&self) -> &ForkGroupId {
        &self.group_id
    }
    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }
    pub fn members(&self) -> &[LegId] {
        &self.members
    }
    pub fn mode(&self) -> PlanJoinMode {
        self.mode
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkLegKey {
    group_id: ForkGroupId,
    leg_id: LegId,
}

impl ForkLegKey {
    pub fn new(group_id: ForkGroupId, leg_id: LegId) -> Self {
        Self { group_id, leg_id }
    }
    pub fn group_id(&self) -> &ForkGroupId {
        &self.group_id
    }
    pub fn leg_id(&self) -> &LegId {
        &self.leg_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkLegFact {
    pub(crate) key: ForkLegKey,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) scope_instance_id: ScopeInstanceId,
    pub(crate) static_scope_id: ScopeId,
    pub(crate) child_node_id: NodeId,
    pub(crate) child_activation_id: ActivationId,
    pub(crate) token_id: ControlTokenId,
}

impl ForkLegFact {
    pub fn new(
        key: ForkLegKey,
        occurrence: LogicalOccurrence,
        scope_instance_id: ScopeInstanceId,
        static_scope_id: ScopeId,
        child_node_id: NodeId,
        child_activation_id: ActivationId,
        token_id: ControlTokenId,
    ) -> Self {
        Self {
            key,
            occurrence,
            scope_instance_id,
            static_scope_id,
            child_node_id,
            child_activation_id,
            token_id,
        }
    }
    pub fn key(&self) -> &ForkLegKey {
        &self.key
    }
    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }
    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }
    pub fn child_node_id(&self) -> &NodeId {
        &self.child_node_id
    }
    pub fn static_scope_id(&self) -> &ScopeId {
        &self.static_scope_id
    }
    pub fn child_activation_id(&self) -> &ActivationId {
        &self.child_activation_id
    }
    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkLegAdmissionFact {
    pub(crate) leg: ForkLegFact,
    pub(crate) output_port: ControlPortId,
}

impl ForkLegAdmissionFact {
    pub fn new(leg: ForkLegFact, output_port: ControlPortId) -> Self {
        Self { leg, output_port }
    }

    pub fn leg(&self) -> &ForkLegFact {
        &self.leg
    }

    pub fn output_port(&self) -> &ControlPortId {
        &self.output_port
    }
}

/// One complete ordered Fork admission aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForkAdmissionFact {
    pub(crate) group: ForkGroupFact,
    pub(crate) legs: Vec<ForkLegAdmissionFact>,
}

impl<'de> Deserialize<'de> for ForkAdmissionFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            group: ForkGroupFact,
            legs: Vec<ForkLegAdmissionFact>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.group, wire.legs).map_err(D::Error::custom)
    }
}

impl ForkAdmissionFact {
    pub fn new(
        group: ForkGroupFact,
        legs: Vec<ForkLegAdmissionFact>,
    ) -> Result<Self, SchedulerError> {
        let exact_order = group
            .members()
            .iter()
            .zip(&legs)
            .all(|(member, admission)| {
                admission.leg().key().group_id() == group.group_id()
                    && admission.leg().key().leg_id() == member
            });
        let unique_tokens = legs
            .iter()
            .map(|value| value.leg().token_id())
            .collect::<BTreeSet<_>>()
            .len()
            == legs.len();
        let unique_activations = legs
            .iter()
            .map(|value| value.leg().child_activation_id())
            .collect::<BTreeSet<_>>()
            .len()
            == legs.len();
        let unique_scopes = legs
            .iter()
            .map(|value| value.leg().scope_instance_id())
            .collect::<BTreeSet<_>>()
            .len()
            == legs.len();
        let unique_outputs = legs
            .iter()
            .map(ForkLegAdmissionFact::output_port)
            .collect::<BTreeSet<_>>()
            .len()
            == legs.len();
        if legs.len() != group.members().len()
            || !exact_order
            || !unique_tokens
            || !unique_activations
            || !unique_scopes
            || !unique_outputs
        {
            return Err(inconsistent(
                "Fork admission must contain every declared member exactly once in declaration order",
            ));
        }
        Ok(Self { group, legs })
    }

    pub fn group(&self) -> &ForkGroupFact {
        &self.group
    }

    pub fn legs(&self) -> &[ForkLegAdmissionFact] {
        &self.legs
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum StructuralOutcomeFact {
    Succeeded { value: RuntimeValue },
    Failed { failure: TaskFailureFact },
}

impl StructuralOutcomeFact {
    pub fn failure(&self) -> Option<&TaskFailureFact> {
        match self {
            Self::Succeeded { .. } => None,
            Self::Failed { failure } => Some(failure),
        }
    }

    pub fn value(&self) -> Option<&RuntimeValue> {
        match self {
            Self::Succeeded { value } => Some(value),
            Self::Failed { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapItemSeed {
    pub(crate) ordinal: u32,
    pub(crate) identity: MapItemIdentity,
    pub(crate) value: RuntimeValue,
}

impl MapItemSeed {
    pub fn new(ordinal: u32, identity: MapItemIdentity, value: RuntimeValue) -> Self {
        Self {
            ordinal,
            identity,
            value,
        }
    }
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }
    pub fn identity(&self) -> &MapItemIdentity {
        &self.identity
    }
    pub fn stable_dynamic_key(&self) -> String {
        self.identity.stable_dynamic_key()
    }
    pub fn value(&self) -> &RuntimeValue {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MapInstanceFact {
    pub(crate) map_activation_id: ActivationId,
    pub(crate) map_node_id: NodeId,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) items: Vec<MapItemSeed>,
    pub(crate) max_concurrency: Option<u32>,
}

impl<'de> Deserialize<'de> for MapInstanceFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            map_activation_id: ActivationId,
            map_node_id: NodeId,
            occurrence: LogicalOccurrence,
            items: Vec<MapItemSeed>,
            max_concurrency: Option<u32>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.map_activation_id,
            wire.map_node_id,
            wire.occurrence,
            wire.items,
            wire.max_concurrency,
        )
        .map_err(D::Error::custom)
    }
}

impl MapInstanceFact {
    pub fn new(
        map_activation_id: ActivationId,
        map_node_id: NodeId,
        occurrence: LogicalOccurrence,
        items: Vec<MapItemSeed>,
        max_concurrency: Option<u32>,
    ) -> Result<Self, SchedulerError> {
        let identities = items
            .iter()
            .map(MapItemSeed::identity)
            .collect::<BTreeSet<_>>();
        let ordinals = items
            .iter()
            .map(MapItemSeed::ordinal)
            .collect::<BTreeSet<_>>();
        if identities.len() != items.len()
            || ordinals.len() != items.len()
            || items
                .iter()
                .enumerate()
                .any(|(index, item)| item.ordinal() != u32::try_from(index).unwrap_or(u32::MAX))
            || items.iter().any(|item| {
                matches!(
                    item.identity(),
                    MapItemIdentity::Ordinal(identity_ordinal)
                        if *identity_ordinal != item.ordinal()
                )
            })
            || max_concurrency == Some(0)
        {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "map snapshot requires unique keys, canonical input order and positive concurrency",
            ));
        }
        Ok(Self {
            map_activation_id,
            map_node_id,
            occurrence,
            items,
            max_concurrency,
        })
    }

    pub fn map_activation_id(&self) -> &ActivationId {
        &self.map_activation_id
    }
    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }
    pub fn items(&self) -> &[MapItemSeed] {
        &self.items
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapItemKey {
    map_activation_id: ActivationId,
    item_identity: MapItemIdentity,
}

impl MapItemKey {
    pub fn new(map_activation_id: ActivationId, item_identity: MapItemIdentity) -> Self {
        Self {
            map_activation_id,
            item_identity,
        }
    }
    pub fn map_activation_id(&self) -> &ActivationId {
        &self.map_activation_id
    }
    pub fn item_identity(&self) -> &MapItemIdentity {
        &self.item_identity
    }
    pub fn stable_dynamic_key(&self) -> String {
        self.item_identity.stable_dynamic_key()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapItemFact {
    pub(crate) key: MapItemKey,
    pub(crate) ordinal: u32,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) scope_instance_id: ScopeInstanceId,
    pub(crate) static_scope_id: ScopeId,
    pub(crate) child_node_id: NodeId,
    pub(crate) child_activation_id: ActivationId,
    pub(crate) token_id: ControlTokenId,
}

impl MapItemFact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: MapItemKey,
        ordinal: u32,
        occurrence: LogicalOccurrence,
        scope_instance_id: ScopeInstanceId,
        static_scope_id: ScopeId,
        child_node_id: NodeId,
        child_activation_id: ActivationId,
        token_id: ControlTokenId,
    ) -> Self {
        Self {
            key,
            ordinal,
            occurrence,
            scope_instance_id,
            static_scope_id,
            child_node_id,
            child_activation_id,
            token_id,
        }
    }
    pub fn key(&self) -> &MapItemKey {
        &self.key
    }
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }
    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }
    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }
    pub fn child_node_id(&self) -> &NodeId {
        &self.child_node_id
    }
    pub fn static_scope_id(&self) -> &ScopeId {
        &self.static_scope_id
    }
    pub fn child_activation_id(&self) -> &ActivationId {
        &self.child_activation_id
    }
    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LoopInstanceFact {
    pub(crate) loop_activation_id: ActivationId,
    pub(crate) loop_node_id: NodeId,
    pub(crate) flavor: LoopFlavor,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) state: RuntimeValue,
    pub(crate) next_iteration: u32,
    pub(crate) started_at_ms: u64,
    pub(crate) deadline_at_ms: Option<u64>,
    pub(crate) completed: bool,
}

impl<'de> Deserialize<'de> for LoopInstanceFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            loop_activation_id: ActivationId,
            loop_node_id: NodeId,
            flavor: LoopFlavor,
            occurrence: LogicalOccurrence,
            state: RuntimeValue,
            next_iteration: u32,
            started_at_ms: u64,
            deadline_at_ms: Option<u64>,
            completed: bool,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.loop_activation_id,
            wire.loop_node_id,
            wire.flavor,
            wire.occurrence,
            wire.state,
            wire.next_iteration,
            wire.started_at_ms,
            wire.deadline_at_ms,
            wire.completed,
        )
        .map_err(D::Error::custom)
    }
}

impl LoopInstanceFact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        loop_activation_id: ActivationId,
        loop_node_id: NodeId,
        flavor: LoopFlavor,
        occurrence: LogicalOccurrence,
        state: RuntimeValue,
        next_iteration: u32,
        started_at_ms: u64,
        deadline_at_ms: Option<u64>,
        completed: bool,
    ) -> Result<Self, SchedulerError> {
        if deadline_at_ms.is_some_and(|deadline| deadline < started_at_ms) {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "loop deadline cannot precede its committed start time",
            ));
        }
        Ok(Self {
            loop_activation_id,
            loop_node_id,
            flavor,
            occurrence,
            state,
            next_iteration,
            started_at_ms,
            deadline_at_ms,
            completed,
        })
    }
    pub fn loop_activation_id(&self) -> &ActivationId {
        &self.loop_activation_id
    }
    pub fn flavor(&self) -> LoopFlavor {
        self.flavor
    }
    pub fn state(&self) -> &RuntimeValue {
        &self.state
    }
    pub fn next_iteration(&self) -> u32 {
        self.next_iteration
    }
    pub fn completed(&self) -> bool {
        self.completed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopIterationKey {
    loop_activation_id: ActivationId,
    iteration: u32,
}

impl LoopIterationKey {
    pub fn new(loop_activation_id: ActivationId, iteration: u32) -> Self {
        Self {
            loop_activation_id,
            iteration,
        }
    }
    pub fn loop_activation_id(&self) -> &ActivationId {
        &self.loop_activation_id
    }
    pub fn iteration(&self) -> u32 {
        self.iteration
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoopIterationFact {
    pub(crate) key: LoopIterationKey,
    pub(crate) flavor: LoopFlavor,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) scope_instance_id: ScopeInstanceId,
    pub(crate) static_scope_id: ScopeId,
    pub(crate) child_node_id: NodeId,
    pub(crate) child_activation_id: ActivationId,
    pub(crate) token_id: ControlTokenId,
    pub(crate) state: RuntimeValue,
}

impl<'de> Deserialize<'de> for LoopIterationFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            key: LoopIterationKey,
            flavor: LoopFlavor,
            occurrence: LogicalOccurrence,
            scope_instance_id: ScopeInstanceId,
            static_scope_id: ScopeId,
            child_node_id: NodeId,
            child_activation_id: ActivationId,
            token_id: ControlTokenId,
            state: RuntimeValue,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.key,
            wire.flavor,
            wire.occurrence,
            wire.scope_instance_id,
            wire.static_scope_id,
            wire.child_node_id,
            wire.child_activation_id,
            wire.token_id,
            wire.state,
        )
        .map_err(D::Error::custom)
    }
}

impl LoopIterationFact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: LoopIterationKey,
        flavor: LoopFlavor,
        occurrence: LogicalOccurrence,
        scope_instance_id: ScopeInstanceId,
        static_scope_id: ScopeId,
        child_node_id: NodeId,
        child_activation_id: ActivationId,
        token_id: ControlTokenId,
        state: RuntimeValue,
    ) -> Result<Self, SchedulerError> {
        let marker = match flavor {
            LoopFlavor::Workflow => "loop_iteration",
            LoopFlavor::Agent => "agent_loop_turn",
        };
        let expected = format!("{marker}:{}", key.iteration());
        if !occurrence
            .segments()
            .iter()
            .any(|segment| segment == &expected)
        {
            return Err(inconsistent(
                "Loop occurrence is inconsistent with its flavor and iteration identity",
            ));
        }
        Ok(Self {
            key,
            flavor,
            occurrence,
            scope_instance_id,
            static_scope_id,
            child_node_id,
            child_activation_id,
            token_id,
            state,
        })
    }
    pub fn key(&self) -> &LoopIterationKey {
        &self.key
    }
    pub fn flavor(&self) -> LoopFlavor {
        self.flavor
    }
    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }
    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }
    pub fn child_node_id(&self) -> &NodeId {
        &self.child_node_id
    }
    pub fn static_scope_id(&self) -> &ScopeId {
        &self.static_scope_id
    }
    pub fn child_activation_id(&self) -> &ActivationId {
        &self.child_activation_id
    }
    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "subject", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitSubjectFact {
    Signal { signal_id: SignalId },
    Timer { timer_id: TimerId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskWaitFact {
    pub(crate) assignees: Vec<String>,
    pub(crate) candidate_groups: Vec<String>,
    pub(crate) claim_lease_ms: u64,
    pub(crate) request: RuntimeValue,
}

impl HumanTaskWaitFact {
    pub fn new(
        assignees: Vec<String>,
        candidate_groups: Vec<String>,
        claim_lease_ms: u64,
        request: RuntimeValue,
    ) -> Result<Self, SchedulerError> {
        if claim_lease_ms == 0 || claim_lease_ms > 30 * 24 * 60 * 60 * 1_000 {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "human task claim lease is outside the durable bound",
            ));
        }
        let request_size = serde_jcs::to_vec(request.value())
            .map_err(|_| {
                SchedulerError::new(
                    SCHEDULER_FACT_INCONSISTENT,
                    "human task request is not canonically serializable",
                )
            })?
            .len();
        if request_size > 1024 * 1024 {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "human task request exceeds the one-megabyte inline boundary",
            ));
        }
        Ok(Self {
            assignees,
            candidate_groups,
            claim_lease_ms,
            request,
        })
    }

    pub fn assignees(&self) -> &[String] {
        &self.assignees
    }
    pub fn candidate_groups(&self) -> &[String] {
        &self.candidate_groups
    }
    pub fn claim_lease_ms(&self) -> u64 {
        self.claim_lease_ms
    }
    pub fn request(&self) -> &RuntimeValue {
        &self.request
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WaitRegistrationFact {
    pub(crate) wait_id: SchedulerWaitId,
    pub(crate) activation_id: ActivationId,
    pub(crate) node_id: NodeId,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) signal_name: Option<String>,
    pub(crate) signal_id: Option<SignalId>,
    pub(crate) timer_id: Option<TimerId>,
    pub(crate) due_at_ms: Option<u64>,
    pub(crate) payload_type: Option<PlanType>,
    pub(crate) human_task: Option<HumanTaskWaitFact>,
}

impl<'de> Deserialize<'de> for WaitRegistrationFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            wait_id: SchedulerWaitId,
            activation_id: ActivationId,
            node_id: NodeId,
            occurrence: LogicalOccurrence,
            signal_name: Option<String>,
            signal_id: Option<SignalId>,
            timer_id: Option<TimerId>,
            due_at_ms: Option<u64>,
            payload_type: Option<PlanType>,
            #[serde(default)]
            human_task: Option<HumanTaskWaitFact>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.wait_id,
            wire.activation_id,
            wire.node_id,
            wire.occurrence,
            wire.signal_name,
            wire.signal_id,
            wire.timer_id,
            wire.due_at_ms,
            wire.payload_type,
        )
        .and_then(|registration| match wire.human_task {
            Some(human_task) => registration.with_human_task(human_task),
            None => Ok(registration),
        })
        .map_err(D::Error::custom)
    }
}

impl WaitRegistrationFact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        wait_id: SchedulerWaitId,
        activation_id: ActivationId,
        node_id: NodeId,
        occurrence: LogicalOccurrence,
        signal_name: Option<String>,
        signal_id: Option<SignalId>,
        timer_id: Option<TimerId>,
        due_at_ms: Option<u64>,
        payload_type: Option<PlanType>,
    ) -> Result<Self, SchedulerError> {
        let signal_contract =
            signal_name.is_some() && signal_id.is_some() && payload_type.is_some();
        let timer_contract = timer_id.is_some() && due_at_ms.is_some();
        if !signal_contract && !timer_contract {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "durable wait must declare a complete signal and/or timer subject",
            ));
        }
        if signal_name.as_ref().is_some_and(|name| {
            name.is_empty() || name.len() > 128 || name.chars().any(char::is_control)
        }) {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "signal name must be non-empty, bounded and body-free",
            ));
        }
        Ok(Self {
            wait_id,
            activation_id,
            node_id,
            occurrence,
            signal_name,
            signal_id,
            timer_id,
            due_at_ms,
            payload_type,
            human_task: None,
        })
    }

    pub fn with_human_task(
        mut self,
        human_task: HumanTaskWaitFact,
    ) -> Result<Self, SchedulerError> {
        if self.signal_id.is_none() || self.signal_name.is_none() || self.payload_type.is_none() {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "human work item requires a typed completion signal",
            ));
        }
        self.human_task = Some(human_task);
        Ok(self)
    }
    pub fn wait_id(&self) -> &SchedulerWaitId {
        &self.wait_id
    }
    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }
    pub fn signal_id(&self) -> Option<&SignalId> {
        self.signal_id.as_ref()
    }
    pub fn timer_id(&self) -> Option<&TimerId> {
        self.timer_id.as_ref()
    }
    pub fn due_at_ms(&self) -> Option<u64> {
        self.due_at_ms
    }
    pub fn human_task(&self) -> Option<&HumanTaskWaitFact> {
        self.human_task.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WaitResolutionFact {
    pub(crate) subject: WaitSubjectFact,
    pub(crate) payload: Option<RuntimeValue>,
}

impl<'de> Deserialize<'de> for WaitResolutionFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            subject: WaitSubjectFact,
            payload: Option<RuntimeValue>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.subject, wire.payload).map_err(D::Error::custom)
    }
}

impl WaitResolutionFact {
    pub fn new(
        subject: WaitSubjectFact,
        payload: Option<RuntimeValue>,
    ) -> Result<Self, SchedulerError> {
        if matches!(subject, WaitSubjectFact::Signal { .. }) != payload.is_some() {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "signal resolution requires one payload and timer resolution requires none",
            ));
        }
        Ok(Self { subject, payload })
    }
    pub fn subject(&self) -> &WaitSubjectFact {
        &self.subject
    }
    pub fn payload(&self) -> Option<&RuntimeValue> {
        self.payload.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubflowInvocationFact {
    pub(crate) child_run_id: RunId,
    pub(crate) parent_activation_id: ActivationId,
    pub(crate) node_id: NodeId,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) invocation_scope_instance_id: ScopeInstanceId,
    pub(crate) parent_scope_instance_id: ScopeInstanceId,
    pub(crate) static_scope_id: ScopeId,
}

impl<'de> Deserialize<'de> for SubflowInvocationFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            child_run_id: RunId,
            parent_activation_id: ActivationId,
            node_id: NodeId,
            occurrence: LogicalOccurrence,
            invocation_scope_instance_id: ScopeInstanceId,
            parent_scope_instance_id: ScopeInstanceId,
            static_scope_id: ScopeId,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.child_run_id,
            wire.parent_activation_id,
            wire.node_id,
            wire.occurrence,
            wire.invocation_scope_instance_id,
            wire.parent_scope_instance_id,
            wire.static_scope_id,
        )
        .map_err(D::Error::custom)
    }
}

impl SubflowInvocationFact {
    pub fn new(
        child_run_id: RunId,
        parent_activation_id: ActivationId,
        node_id: NodeId,
        occurrence: LogicalOccurrence,
        invocation_scope_instance_id: ScopeInstanceId,
        parent_scope_instance_id: ScopeInstanceId,
        static_scope_id: ScopeId,
    ) -> Result<Self, SchedulerError> {
        if invocation_scope_instance_id == parent_scope_instance_id {
            return Err(inconsistent(
                "Subflow invocation scope must be distinct from its parent scope",
            ));
        }
        Ok(Self {
            child_run_id,
            parent_activation_id,
            node_id,
            occurrence,
            invocation_scope_instance_id,
            parent_scope_instance_id,
            static_scope_id,
        })
    }
    pub fn child_run_id(&self) -> &RunId {
        &self.child_run_id
    }
    pub fn parent_activation_id(&self) -> &ActivationId {
        &self.parent_activation_id
    }
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    pub fn occurrence(&self) -> &LogicalOccurrence {
        &self.occurrence
    }
    pub fn invocation_scope_instance_id(&self) -> &ScopeInstanceId {
        &self.invocation_scope_instance_id
    }
    pub fn parent_scope_instance_id(&self) -> &ScopeInstanceId {
        &self.parent_scope_instance_id
    }
    pub fn static_scope_id(&self) -> &ScopeId {
        &self.static_scope_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubflowOutcomeFact {
    Succeeded {
        outputs: BTreeMap<DataPortId, RuntimeValue>,
    },
    Failed {
        failure: TaskFailureFact,
    },
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorBoundaryPhase {
    Protected,
    Handler,
    /// The protected/handler outcome has been frozen and the durable
    /// finalizer control path is now the only admissible authored work.
    Finalizer,
    Completed,
}

/// Frozen continuation carried by an ErrorBoundary while its finalizer runs.
/// Keeping this in the checkpoint fact makes restart/replay independent from
/// an in-memory exception stack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ErrorBoundaryExit {
    Continue,
    /// Authored Return frozen while one or more enclosing finalizers run.
    /// The value is the already-resolved workflow output, so replay never
    /// depends on re-evaluating mutable ambient state.
    Return {
        activation_id: ActivationId,
        output: RuntimeValue,
    },
    Rethrow {
        failure: TaskFailureFact,
    },
    Terminate {
        reason: TerminationReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorBoundaryFact {
    pub(crate) boundary_activation_id: ActivationId,
    pub(crate) node_id: NodeId,
    pub(crate) occurrence: LogicalOccurrence,
    pub(crate) phase: ErrorBoundaryPhase,
    pub(crate) safe_error: Option<SafeError>,
    pub(crate) exit: ErrorBoundaryExit,
}

impl<'de> Deserialize<'de> for ErrorBoundaryFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            boundary_activation_id: ActivationId,
            node_id: NodeId,
            occurrence: LogicalOccurrence,
            phase: ErrorBoundaryPhase,
            safe_error: Option<RuntimeValue>,
            exit: ErrorBoundaryExit,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::with_exit(
            wire.boundary_activation_id,
            wire.node_id,
            wire.occurrence,
            wire.phase,
            wire.safe_error,
            wire.exit,
        )
        .map_err(D::Error::custom)
    }
}

impl ErrorBoundaryFact {
    pub fn new(
        boundary_activation_id: ActivationId,
        node_id: NodeId,
        occurrence: LogicalOccurrence,
        phase: ErrorBoundaryPhase,
        safe_error: Option<RuntimeValue>,
    ) -> Result<Self, SchedulerError> {
        Self::with_exit(
            boundary_activation_id,
            node_id,
            occurrence,
            phase,
            safe_error,
            ErrorBoundaryExit::Continue,
        )
    }

    pub fn with_exit(
        boundary_activation_id: ActivationId,
        node_id: NodeId,
        occurrence: LogicalOccurrence,
        phase: ErrorBoundaryPhase,
        safe_error: Option<RuntimeValue>,
        exit: ErrorBoundaryExit,
    ) -> Result<Self, SchedulerError> {
        if matches!(phase, ErrorBoundaryPhase::Handler) != safe_error.is_some()
            && !matches!(phase, ErrorBoundaryPhase::Completed)
        {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "only a handling error boundary carries a safe business error",
            ));
        }
        if !matches!(
            phase,
            ErrorBoundaryPhase::Finalizer | ErrorBoundaryPhase::Completed
        ) && !matches!(exit, ErrorBoundaryExit::Continue)
        {
            return Err(SchedulerError::new(
                SCHEDULER_FACT_INCONSISTENT,
                "only a finalizing or completed error boundary may carry an unwind exit",
            ));
        }
        let safe_error = safe_error.map(SafeError::try_from).transpose()?;
        Ok(Self {
            boundary_activation_id,
            node_id,
            occurrence,
            phase,
            safe_error,
            exit,
        })
    }
    pub fn boundary_activation_id(&self) -> &ActivationId {
        &self.boundary_activation_id
    }
    pub fn phase(&self) -> ErrorBoundaryPhase {
        self.phase
    }
    pub fn safe_error(&self) -> Option<&RuntimeValue> {
        self.safe_error.as_ref().map(SafeError::runtime_value)
    }
    pub fn exit(&self) -> &ErrorBoundaryExit {
        &self.exit
    }
}

/// Immutable committed projection supplied to one pure planning call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchedulerFacts {
    run_id: RunId,
    projection_version: u64,
    observed_time_ms: u64,
    run_input: RuntimeValue,
    checkpoints: BTreeSet<SchedulerCheckpointId>,
    admitted_activations: BTreeSet<ActivationId>,
    emitted_tokens: BTreeSet<ControlTokenId>,
    consumed_tokens: BTreeSet<ControlTokenId>,
    dispatched_tasks: BTreeSet<SchedulerTaskId>,
    completed_tasks: BTreeSet<SchedulerTaskId>,
    task_outcomes: BTreeMap<SchedulerTaskId, TaskOutcomeFact>,
    values: BTreeMap<DataPortId, RuntimeValue>,
    value_owners: BTreeMap<DataPortId, ActivationId>,
    occurrence_values: BTreeMap<OccurrenceValueKey, RuntimeValue>,
    occurrence_value_owners: BTreeMap<OccurrenceValueKey, ActivationId>,
    reuse_candidates: Vec<ReuseCandidateFact>,
    redrive_effects: Vec<RedriveEffectFact>,
    reused_activations: BTreeSet<ActivationId>,
    branch_selections: BTreeMap<NodeId, BranchCaseId>,
    occurrence_branch_selections: BTreeMap<OccurrenceNodeKey, BranchCaseId>,
    fork_groups: BTreeMap<ForkGroupId, ForkGroupFact>,
    fork_legs: BTreeMap<ForkLegKey, ForkLegFact>,
    fork_settlements: BTreeMap<ForkLegKey, StructuralOutcomeFact>,
    completed_forks: BTreeSet<ForkGroupId>,
    map_instances: BTreeMap<ActivationId, MapInstanceFact>,
    map_items: BTreeMap<MapItemKey, MapItemFact>,
    map_settlements: BTreeMap<MapItemKey, StructuralOutcomeFact>,
    completed_maps: BTreeSet<ActivationId>,
    loop_instances: BTreeMap<ActivationId, LoopInstanceFact>,
    loop_iterations: BTreeMap<LoopIterationKey, LoopIterationFact>,
    loop_settlements: BTreeMap<LoopIterationKey, StructuralOutcomeFact>,
    waits: BTreeMap<SchedulerWaitId, WaitRegistrationFact>,
    wait_resolutions: BTreeMap<SchedulerWaitId, WaitResolutionFact>,
    subflows: BTreeMap<RunId, SubflowInvocationFact>,
    child_subflow_outcomes: BTreeMap<RunId, SubflowOutcomeFact>,
    settled_subflows: BTreeMap<RunId, SubflowOutcomeFact>,
    child_cancellation_requests: BTreeSet<RunId>,
    boundary_states: BTreeMap<ActivationId, ErrorBoundaryFact>,
    scope_cancellation_requests: BTreeSet<ScopeInstanceId>,
    run_termination_reason: Option<TerminationReason>,
    terminal: Option<RunTerminalFact>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerFactsWire {
    run_id: RunId,
    projection_version: u64,
    observed_time_ms: u64,
    run_input: RuntimeValue,
    checkpoints: BTreeSet<SchedulerCheckpointId>,
    admitted_activations: BTreeSet<ActivationId>,
    emitted_tokens: BTreeSet<ControlTokenId>,
    consumed_tokens: BTreeSet<ControlTokenId>,
    dispatched_tasks: BTreeSet<SchedulerTaskId>,
    completed_tasks: BTreeSet<SchedulerTaskId>,
    task_outcomes: BTreeMap<SchedulerTaskId, TaskOutcomeFact>,
    values: BTreeMap<DataPortId, RuntimeValue>,
    #[serde(default)]
    value_owners: BTreeMap<DataPortId, ActivationId>,
    occurrence_values: BTreeMap<OccurrenceValueKey, RuntimeValue>,
    #[serde(default)]
    occurrence_value_owners: BTreeMap<OccurrenceValueKey, ActivationId>,
    reuse_candidates: Vec<ReuseCandidateFact>,
    #[serde(default)]
    redrive_effects: Vec<RedriveEffectFact>,
    reused_activations: BTreeSet<ActivationId>,
    branch_selections: BTreeMap<NodeId, BranchCaseId>,
    occurrence_branch_selections: BTreeMap<OccurrenceNodeKey, BranchCaseId>,
    fork_groups: BTreeMap<ForkGroupId, ForkGroupFact>,
    fork_legs: BTreeMap<ForkLegKey, ForkLegFact>,
    fork_settlements: BTreeMap<ForkLegKey, StructuralOutcomeFact>,
    completed_forks: BTreeSet<ForkGroupId>,
    map_instances: BTreeMap<ActivationId, MapInstanceFact>,
    map_items: BTreeMap<MapItemKey, MapItemFact>,
    map_settlements: BTreeMap<MapItemKey, StructuralOutcomeFact>,
    completed_maps: BTreeSet<ActivationId>,
    loop_instances: BTreeMap<ActivationId, LoopInstanceFact>,
    loop_iterations: BTreeMap<LoopIterationKey, LoopIterationFact>,
    loop_settlements: BTreeMap<LoopIterationKey, StructuralOutcomeFact>,
    waits: BTreeMap<SchedulerWaitId, WaitRegistrationFact>,
    wait_resolutions: BTreeMap<SchedulerWaitId, WaitResolutionFact>,
    subflows: BTreeMap<RunId, SubflowInvocationFact>,
    child_subflow_outcomes: BTreeMap<RunId, SubflowOutcomeFact>,
    settled_subflows: BTreeMap<RunId, SubflowOutcomeFact>,
    child_cancellation_requests: BTreeSet<RunId>,
    boundary_states: BTreeMap<ActivationId, ErrorBoundaryFact>,
    scope_cancellation_requests: BTreeSet<ScopeInstanceId>,
    run_termination_reason: Option<TerminationReason>,
    terminal: Option<RunTerminalFact>,
}

impl<'de> Deserialize<'de> for SchedulerFacts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SchedulerFactsWire::deserialize(deserializer)?;
        let facts = Self {
            run_id: wire.run_id,
            projection_version: wire.projection_version,
            observed_time_ms: wire.observed_time_ms,
            run_input: wire.run_input,
            checkpoints: wire.checkpoints,
            admitted_activations: wire.admitted_activations,
            emitted_tokens: wire.emitted_tokens,
            consumed_tokens: wire.consumed_tokens,
            dispatched_tasks: wire.dispatched_tasks,
            completed_tasks: wire.completed_tasks,
            task_outcomes: wire.task_outcomes,
            values: wire.values,
            value_owners: wire.value_owners,
            occurrence_values: wire.occurrence_values,
            occurrence_value_owners: wire.occurrence_value_owners,
            reuse_candidates: wire.reuse_candidates,
            redrive_effects: wire.redrive_effects,
            reused_activations: wire.reused_activations,
            branch_selections: wire.branch_selections,
            occurrence_branch_selections: wire.occurrence_branch_selections,
            fork_groups: wire.fork_groups,
            fork_legs: wire.fork_legs,
            fork_settlements: wire.fork_settlements,
            completed_forks: wire.completed_forks,
            map_instances: wire.map_instances,
            map_items: wire.map_items,
            map_settlements: wire.map_settlements,
            completed_maps: wire.completed_maps,
            loop_instances: wire.loop_instances,
            loop_iterations: wire.loop_iterations,
            loop_settlements: wire.loop_settlements,
            waits: wire.waits,
            wait_resolutions: wire.wait_resolutions,
            subflows: wire.subflows,
            child_subflow_outcomes: wire.child_subflow_outcomes,
            settled_subflows: wire.settled_subflows,
            child_cancellation_requests: wire.child_cancellation_requests,
            boundary_states: wire.boundary_states,
            scope_cancellation_requests: wire.scope_cancellation_requests,
            run_termination_reason: wire.run_termination_reason,
            terminal: wire.terminal,
        };
        facts.validate().map_err(D::Error::custom)?;
        Ok(facts)
    }
}

impl SchedulerFacts {
    pub fn new(run_id: RunId, projection_version: u64, run_input: RuntimeValue) -> Self {
        Self {
            run_id,
            projection_version,
            observed_time_ms: 0,
            run_input,
            checkpoints: BTreeSet::new(),
            admitted_activations: BTreeSet::new(),
            emitted_tokens: BTreeSet::new(),
            consumed_tokens: BTreeSet::new(),
            dispatched_tasks: BTreeSet::new(),
            completed_tasks: BTreeSet::new(),
            task_outcomes: BTreeMap::new(),
            values: BTreeMap::new(),
            value_owners: BTreeMap::new(),
            occurrence_values: BTreeMap::new(),
            occurrence_value_owners: BTreeMap::new(),
            reuse_candidates: Vec::new(),
            redrive_effects: Vec::new(),
            reused_activations: BTreeSet::new(),
            branch_selections: BTreeMap::new(),
            occurrence_branch_selections: BTreeMap::new(),
            fork_groups: BTreeMap::new(),
            fork_legs: BTreeMap::new(),
            fork_settlements: BTreeMap::new(),
            completed_forks: BTreeSet::new(),
            map_instances: BTreeMap::new(),
            map_items: BTreeMap::new(),
            map_settlements: BTreeMap::new(),
            completed_maps: BTreeSet::new(),
            loop_instances: BTreeMap::new(),
            loop_iterations: BTreeMap::new(),
            loop_settlements: BTreeMap::new(),
            waits: BTreeMap::new(),
            wait_resolutions: BTreeMap::new(),
            subflows: BTreeMap::new(),
            child_subflow_outcomes: BTreeMap::new(),
            settled_subflows: BTreeMap::new(),
            child_cancellation_requests: BTreeSet::new(),
            boundary_states: BTreeMap::new(),
            scope_cancellation_requests: BTreeSet::new(),
            run_termination_reason: None,
            terminal: None,
        }
    }

    fn validate(&self) -> Result<(), SchedulerError> {
        if !self.completed_tasks.is_subset(&self.dispatched_tasks)
            || self
                .task_outcomes
                .keys()
                .any(|task| !self.dispatched_tasks.contains(task))
        {
            return Err(inconsistent("task completion has no committed dispatch"));
        }
        if self
            .value_owners
            .keys()
            .any(|port| !self.values.contains_key(port))
            || self
                .occurrence_value_owners
                .keys()
                .any(|key| !self.occurrence_values.contains_key(key))
        {
            return Err(inconsistent(
                "scheduler value ownership references an absent committed value",
            ));
        }
        let mut reuse_keys = BTreeSet::new();
        let mut candidate_ids = BTreeSet::new();
        for candidate in &self.reuse_candidates {
            ReuseCandidateFact::new(
                candidate.candidate_id.clone(),
                candidate.target_scope_instance_id.clone(),
                candidate.target_node_id.clone(),
                candidate.occurrence.clone(),
                candidate.projection_version,
            )?;
            if !candidate_ids.insert(candidate.candidate_id.as_str())
                || !reuse_keys.insert((
                    &candidate.target_scope_instance_id,
                    &candidate.target_node_id,
                    &candidate.occurrence,
                ))
            {
                return Err(inconsistent(
                    "pending reuse candidates contain an ambiguous admission identity",
                ));
            }
        }
        let mut redrive_keys = BTreeSet::new();
        let mut inherited_effects = BTreeSet::new();
        for effect in &self.redrive_effects {
            if !redrive_keys.insert((&effect.target_node_id, &effect.occurrence))
                || !inherited_effects.insert(&effect.effect_id)
            {
                return Err(inconsistent(
                    "redrive effect lineage contains an ambiguous admission identity",
                ));
            }
        }
        for (id, group) in &self.fork_groups {
            if id != group.group_id() {
                return Err(inconsistent("fork group map key does not match its fact"));
            }
            ForkGroupFact::new(
                group.group_id.clone(),
                group.fork_node_id.clone(),
                group.fork_activation_id.clone(),
                group.parent_scope_instance_id.clone(),
                group.occurrence.clone(),
                group.mode,
                group.members.clone(),
            )?;
        }
        for (key, leg) in &self.fork_legs {
            let Some(group) = self.fork_groups.get(key.group_id()) else {
                return Err(inconsistent("fork leg references an unknown group"));
            };
            if key != leg.key() || !group.members().contains(key.leg_id()) {
                return Err(inconsistent("fork leg is not a declared group member"));
            }
        }
        if self
            .fork_settlements
            .keys()
            .any(|key| !self.fork_legs.contains_key(key))
            || self
                .completed_forks
                .iter()
                .any(|group| !self.fork_groups.contains_key(group))
        {
            return Err(inconsistent(
                "fork settlement references an unspawned child",
            ));
        }
        for (id, map) in &self.map_instances {
            if id != map.map_activation_id() {
                return Err(inconsistent("map instance map key does not match its fact"));
            }
            MapInstanceFact::new(
                map.map_activation_id.clone(),
                map.map_node_id.clone(),
                map.occurrence.clone(),
                map.items.clone(),
                map.max_concurrency,
            )?;
        }
        for (key, item) in &self.map_items {
            let Some(map) = self.map_instances.get(key.map_activation_id()) else {
                return Err(inconsistent("map item references an unknown map instance"));
            };
            let seed = map
                .items()
                .iter()
                .find(|seed| seed.identity() == key.item_identity());
            if key != item.key() || seed.is_none_or(|seed| seed.ordinal() != item.ordinal()) {
                return Err(inconsistent(
                    "map item identity or ordinal is absent from its persisted input snapshot",
                ));
            }
        }
        if self
            .map_settlements
            .keys()
            .any(|key| !self.map_items.contains_key(key))
            || self
                .completed_maps
                .iter()
                .any(|map| !self.map_instances.contains_key(map))
        {
            return Err(inconsistent("map settlement references an unspawned child"));
        }
        for (id, loop_instance) in &self.loop_instances {
            if id != loop_instance.loop_activation_id() {
                return Err(inconsistent(
                    "loop instance map key does not match its fact",
                ));
            }
        }
        if self.loop_iterations.iter().any(|(key, iteration)| {
            let Some(instance) = self.loop_instances.get(key.loop_activation_id()) else {
                return true;
            };
            iteration.key() != key
                || iteration.flavor() != instance.flavor()
                || LoopIterationFact::new(
                    iteration.key.clone(),
                    iteration.flavor,
                    iteration.occurrence.clone(),
                    iteration.scope_instance_id.clone(),
                    iteration.static_scope_id.clone(),
                    iteration.child_node_id.clone(),
                    iteration.child_activation_id.clone(),
                    iteration.token_id.clone(),
                    iteration.state.clone(),
                )
                .is_err()
        }) || self
            .loop_settlements
            .keys()
            .any(|key| !self.loop_iterations.contains_key(key))
        {
            return Err(inconsistent(
                "loop iteration references an unknown loop instance",
            ));
        }
        for (id, wait) in &self.waits {
            if id != wait.wait_id() {
                return Err(inconsistent("wait map key does not match its registration"));
            }
        }
        for (id, resolution) in &self.wait_resolutions {
            let Some(wait) = self.waits.get(id) else {
                return Err(inconsistent(
                    "wait resolution references an unknown registration",
                ));
            };
            validate_wait_resolution(wait, resolution)?;
        }
        if self.subflows.iter().any(|(id, invocation)| {
            id != invocation.child_run_id()
                || SubflowInvocationFact::new(
                    invocation.child_run_id.clone(),
                    invocation.parent_activation_id.clone(),
                    invocation.node_id.clone(),
                    invocation.occurrence.clone(),
                    invocation.invocation_scope_instance_id.clone(),
                    invocation.parent_scope_instance_id.clone(),
                    invocation.static_scope_id.clone(),
                )
                .is_err()
        }) || self
            .child_subflow_outcomes
            .keys()
            .any(|id| !self.subflows.contains_key(id))
            || self
                .settled_subflows
                .iter()
                .any(|(id, outcome)| self.child_subflow_outcomes.get(id) != Some(outcome))
            || self
                .child_cancellation_requests
                .iter()
                .any(|id| !self.subflows.contains_key(id))
        {
            return Err(inconsistent(
                "child run fact references an unknown invocation",
            ));
        }
        if self.boundary_states.iter().any(|(id, boundary)| {
            id != boundary.boundary_activation_id()
                || ErrorBoundaryFact::with_exit(
                    boundary.boundary_activation_id.clone(),
                    boundary.node_id.clone(),
                    boundary.occurrence.clone(),
                    boundary.phase,
                    boundary
                        .safe_error
                        .as_ref()
                        .map(|error| error.runtime_value().clone()),
                    boundary.exit.clone(),
                )
                .is_err()
        }) {
            return Err(inconsistent(
                "error boundary fact is not internally consistent",
            ));
        }
        Ok(())
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }
    pub fn observed_time_ms(&self) -> u64 {
        self.observed_time_ms
    }
    pub fn run_input(&self) -> &RuntimeValue {
        &self.run_input
    }
    pub fn checkpoints(&self) -> &BTreeSet<SchedulerCheckpointId> {
        &self.checkpoints
    }
    pub fn admitted_activations(&self) -> &BTreeSet<ActivationId> {
        &self.admitted_activations
    }
    pub fn emitted_tokens(&self) -> &BTreeSet<ControlTokenId> {
        &self.emitted_tokens
    }
    pub fn consumed_tokens(&self) -> &BTreeSet<ControlTokenId> {
        &self.consumed_tokens
    }
    pub fn dispatched_tasks(&self) -> &BTreeSet<SchedulerTaskId> {
        &self.dispatched_tasks
    }
    pub fn completed_tasks(&self) -> &BTreeSet<SchedulerTaskId> {
        &self.completed_tasks
    }
    pub fn task_outcomes(&self) -> &BTreeMap<SchedulerTaskId, TaskOutcomeFact> {
        &self.task_outcomes
    }
    pub fn values(&self) -> &BTreeMap<DataPortId, RuntimeValue> {
        &self.values
    }
    pub fn occurrence_values(&self) -> &BTreeMap<OccurrenceValueKey, RuntimeValue> {
        &self.occurrence_values
    }
    pub fn pending_reuse_candidate(
        &self,
        scope_instance_id: &ScopeInstanceId,
        node_id: &NodeId,
        occurrence: &LogicalOccurrence,
    ) -> Option<&ReuseCandidateFact> {
        self.reuse_candidates.iter().find(|candidate| {
            candidate.target_scope_instance_id() == scope_instance_id
                && candidate.target_node_id() == node_id
                && candidate.occurrence() == occurrence
        })
    }
    pub fn redrive_effect(
        &self,
        node_id: &NodeId,
        occurrence: &LogicalOccurrence,
    ) -> Option<&RedriveEffectFact> {
        self.redrive_effects
            .iter()
            .find(|effect| effect.target_node_id() == node_id && effect.occurrence() == occurrence)
    }
    pub fn reused_activations(&self) -> &BTreeSet<ActivationId> {
        &self.reused_activations
    }
    pub fn branch_selections(&self) -> &BTreeMap<NodeId, BranchCaseId> {
        &self.branch_selections
    }
    pub fn branch_selection_at(
        &self,
        node: &NodeId,
        occurrence: &LogicalOccurrence,
    ) -> Option<&BranchCaseId> {
        let mut cursor = Some(occurrence.clone());
        while let Some(candidate) = cursor {
            if let Some(value) = self
                .occurrence_branch_selections
                .get(&OccurrenceNodeKey::new(candidate.clone(), node.clone()))
            {
                return Some(value);
            }
            cursor = candidate.parent();
        }
        self.branch_selections.get(node)
    }
    pub fn fork_groups(&self) -> &BTreeMap<ForkGroupId, ForkGroupFact> {
        &self.fork_groups
    }
    pub fn fork_legs(&self) -> &BTreeMap<ForkLegKey, ForkLegFact> {
        &self.fork_legs
    }
    pub fn fork_settlements(&self) -> &BTreeMap<ForkLegKey, StructuralOutcomeFact> {
        &self.fork_settlements
    }
    pub fn completed_forks(&self) -> &BTreeSet<ForkGroupId> {
        &self.completed_forks
    }
    pub fn map_instances(&self) -> &BTreeMap<ActivationId, MapInstanceFact> {
        &self.map_instances
    }
    pub fn map_items(&self) -> &BTreeMap<MapItemKey, MapItemFact> {
        &self.map_items
    }
    pub fn map_settlements(&self) -> &BTreeMap<MapItemKey, StructuralOutcomeFact> {
        &self.map_settlements
    }
    pub fn completed_maps(&self) -> &BTreeSet<ActivationId> {
        &self.completed_maps
    }
    pub fn loop_instances(&self) -> &BTreeMap<ActivationId, LoopInstanceFact> {
        &self.loop_instances
    }
    pub fn loop_iterations(&self) -> &BTreeMap<LoopIterationKey, LoopIterationFact> {
        &self.loop_iterations
    }
    pub fn loop_settlements(&self) -> &BTreeMap<LoopIterationKey, StructuralOutcomeFact> {
        &self.loop_settlements
    }
    pub fn waits(&self) -> &BTreeMap<SchedulerWaitId, WaitRegistrationFact> {
        &self.waits
    }
    pub fn wait_resolutions(&self) -> &BTreeMap<SchedulerWaitId, WaitResolutionFact> {
        &self.wait_resolutions
    }
    pub fn subflows(&self) -> &BTreeMap<RunId, SubflowInvocationFact> {
        &self.subflows
    }
    pub fn child_subflow_outcomes(&self) -> &BTreeMap<RunId, SubflowOutcomeFact> {
        &self.child_subflow_outcomes
    }
    pub fn settled_subflows(&self) -> &BTreeMap<RunId, SubflowOutcomeFact> {
        &self.settled_subflows
    }
    pub fn child_cancellation_requests(&self) -> &BTreeSet<RunId> {
        &self.child_cancellation_requests
    }
    pub fn boundary_states(&self) -> &BTreeMap<ActivationId, ErrorBoundaryFact> {
        &self.boundary_states
    }
    pub fn scope_cancellation_requests(&self) -> &BTreeSet<ScopeInstanceId> {
        &self.scope_cancellation_requests
    }
    pub fn run_termination_reason(&self) -> Option<TerminationReason> {
        self.run_termination_reason
    }
    pub fn run_cancellation_requested(&self) -> bool {
        self.run_termination_reason == Some(TerminationReason::Cancelled)
    }
    pub fn terminal(&self) -> Option<&RunTerminalFact> {
        self.terminal.as_ref()
    }

    pub fn value_at(
        &self,
        port_id: &DataPortId,
        occurrence: &LogicalOccurrence,
    ) -> Option<&RuntimeValue> {
        let mut cursor = Some(occurrence.clone());
        while let Some(candidate) = cursor {
            if let Some(value) = self
                .occurrence_values
                .get(&OccurrenceValueKey::new(candidate.clone(), port_id.clone()))
            {
                return Some(value);
            }
            cursor = candidate.parent();
        }
        self.values.get(port_id)
    }

    pub(crate) fn exact_occurrence_value_at(
        &self,
        port_id: &DataPortId,
        occurrence: &LogicalOccurrence,
    ) -> Option<&RuntimeValue> {
        self.occurrence_values.get(&OccurrenceValueKey::new(
            occurrence.clone(),
            port_id.clone(),
        ))
    }

    pub(crate) fn exact_occurrence_value_owner_at(
        &self,
        port_id: &DataPortId,
        occurrence: &LogicalOccurrence,
    ) -> Option<&ActivationId> {
        self.occurrence_value_owners.get(&OccurrenceValueKey::new(
            occurrence.clone(),
            port_id.clone(),
        ))
    }

    pub(crate) fn value_owner_at(
        &self,
        port_id: &DataPortId,
        occurrence: &LogicalOccurrence,
    ) -> Option<&ActivationId> {
        let mut cursor = Some(occurrence.clone());
        while let Some(candidate) = cursor {
            if let Some(owner) = self
                .occurrence_value_owners
                .get(&OccurrenceValueKey::new(candidate.clone(), port_id.clone()))
            {
                return Some(owner);
            }
            cursor = candidate.parent();
        }
        self.value_owners.get(port_id)
    }

    pub fn set_projection_version(&mut self, value: u64) {
        self.projection_version = value;
    }
    pub fn set_observed_time_ms(&mut self, value: u64) {
        self.observed_time_ms = value;
    }
    pub fn commit_checkpoint(&mut self, value: SchedulerCheckpointId) {
        self.checkpoints.insert(value);
    }
    pub fn record_activation(&mut self, value: ActivationId) {
        self.admitted_activations.insert(value);
    }
    pub fn record_emitted_token(&mut self, value: ControlTokenId) {
        self.emitted_tokens.insert(value);
    }
    pub fn record_consumed_token(&mut self, value: ControlTokenId) {
        self.consumed_tokens.insert(value);
    }
    pub fn record_dispatched_task(&mut self, value: SchedulerTaskId) {
        self.dispatched_tasks.insert(value);
    }
    pub fn record_completed_task(&mut self, value: SchedulerTaskId) {
        self.completed_tasks.insert(value);
    }
    pub fn record_task_outcome(&mut self, task_id: SchedulerTaskId, outcome: TaskOutcomeFact) {
        self.dispatched_tasks.insert(task_id.clone());
        self.completed_tasks.insert(task_id.clone());
        self.task_outcomes.insert(task_id, outcome);
    }
    pub fn record_value(&mut self, port: DataPortId, value: RuntimeValue) {
        self.values.insert(port, value);
    }
    pub fn record_value_from(
        &mut self,
        port: DataPortId,
        owner: ActivationId,
        value: RuntimeValue,
    ) {
        self.value_owners.insert(port.clone(), owner);
        self.values.insert(port, value);
    }
    pub fn record_occurrence_value(
        &mut self,
        occurrence: LogicalOccurrence,
        port: DataPortId,
        value: RuntimeValue,
    ) {
        self.occurrence_values
            .insert(OccurrenceValueKey::new(occurrence, port), value);
    }
    pub fn record_occurrence_value_from(
        &mut self,
        occurrence: LogicalOccurrence,
        port: DataPortId,
        owner: ActivationId,
        value: RuntimeValue,
    ) {
        let key = OccurrenceValueKey::new(occurrence, port);
        self.occurrence_value_owners.insert(key.clone(), owner);
        self.occurrence_values.insert(key, value);
    }
    pub fn record_reuse_candidate(&mut self, candidate: ReuseCandidateFact) {
        self.reuse_candidates.push(candidate);
        self.reuse_candidates
            .sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
    }
    pub fn record_redrive_effect(&mut self, effect: RedriveEffectFact) {
        self.redrive_effects.push(effect);
        self.redrive_effects.sort_by(|left, right| {
            (&left.target_node_id, &left.occurrence)
                .cmp(&(&right.target_node_id, &right.occurrence))
        });
    }
    pub fn record_reused_activation(&mut self, activation_id: ActivationId) {
        self.admitted_activations.insert(activation_id.clone());
        self.reused_activations.insert(activation_id);
    }
    pub fn record_branch_selection(&mut self, node: NodeId, case: BranchCaseId) {
        self.branch_selections.insert(node, case);
    }
    pub fn record_occurrence_branch_selection(
        &mut self,
        occurrence: LogicalOccurrence,
        node: NodeId,
        case: BranchCaseId,
    ) {
        self.occurrence_branch_selections
            .insert(OccurrenceNodeKey::new(occurrence, node), case);
    }
    pub fn record_fork_group(&mut self, group: ForkGroupFact) {
        self.fork_groups.insert(group.group_id.clone(), group);
    }
    pub fn record_fork_leg(&mut self, leg: ForkLegFact) {
        self.admitted_activations
            .insert(leg.child_activation_id.clone());
        self.emitted_tokens.insert(leg.token_id.clone());
        self.fork_legs.insert(leg.key.clone(), leg);
    }
    pub fn settle_fork_leg(&mut self, key: ForkLegKey, outcome: StructuralOutcomeFact) {
        self.fork_settlements.insert(key, outcome);
    }
    pub fn complete_fork(&mut self, group: ForkGroupId) {
        self.completed_forks.insert(group);
    }
    pub fn record_map_instance(&mut self, map: MapInstanceFact) {
        self.map_instances
            .insert(map.map_activation_id.clone(), map);
    }
    pub fn record_map_item(
        &mut self,
        item: MapItemFact,
        item_port: DataPortId,
        value: RuntimeValue,
    ) {
        self.admitted_activations
            .insert(item.child_activation_id.clone());
        self.emitted_tokens.insert(item.token_id.clone());
        self.record_occurrence_value(item.occurrence.clone(), item_port, value);
        self.map_items.insert(item.key.clone(), item);
    }
    pub fn settle_map_item(&mut self, key: MapItemKey, outcome: StructuralOutcomeFact) {
        self.map_settlements.insert(key, outcome);
    }
    pub fn complete_map(&mut self, id: ActivationId) {
        self.completed_maps.insert(id);
    }
    pub fn record_loop_instance(&mut self, value: LoopInstanceFact) {
        self.loop_instances
            .insert(value.loop_activation_id.clone(), value);
    }
    pub fn record_loop_iteration(&mut self, value: LoopIterationFact, state_port: DataPortId) {
        self.admitted_activations
            .insert(value.child_activation_id.clone());
        self.emitted_tokens.insert(value.token_id.clone());
        self.record_occurrence_value(value.occurrence.clone(), state_port, value.state.clone());
        self.loop_iterations.insert(value.key.clone(), value);
    }
    pub fn settle_loop_iteration(&mut self, key: LoopIterationKey, outcome: StructuralOutcomeFact) {
        self.loop_settlements.insert(key, outcome);
    }
    pub fn advance_loop(
        &mut self,
        id: &ActivationId,
        state: RuntimeValue,
    ) -> Result<(), SchedulerError> {
        let value = self
            .loop_instances
            .get_mut(id)
            .ok_or_else(|| inconsistent("advanced loop is unknown"))?;
        value.state = state;
        value.next_iteration = value
            .next_iteration
            .checked_add(1)
            .ok_or_else(|| inconsistent("loop iteration counter overflowed"))?;
        Ok(())
    }
    pub fn complete_loop(
        &mut self,
        id: &ActivationId,
        state: RuntimeValue,
    ) -> Result<(), SchedulerError> {
        let value = self
            .loop_instances
            .get_mut(id)
            .ok_or_else(|| inconsistent("completed loop is unknown"))?;
        value.state = state;
        value.completed = true;
        Ok(())
    }
    pub fn register_wait(&mut self, wait: WaitRegistrationFact) {
        self.waits.insert(wait.wait_id.clone(), wait);
    }
    pub fn resolve_wait_first_winner(
        &mut self,
        wait_id: SchedulerWaitId,
        resolution: WaitResolutionFact,
    ) -> Result<bool, SchedulerError> {
        let registration = self
            .waits
            .get(&wait_id)
            .ok_or_else(|| inconsistent("resolved wait is not registered"))?;
        validate_wait_resolution(registration, &resolution)?;
        if self.wait_resolutions.contains_key(&wait_id) {
            return Ok(false);
        }
        self.wait_resolutions.insert(wait_id, resolution);
        Ok(true)
    }
    pub fn record_subflow(&mut self, value: SubflowInvocationFact) {
        self.subflows.insert(value.child_run_id.clone(), value);
    }
    pub fn observe_subflow_outcome(&mut self, id: RunId, value: SubflowOutcomeFact) {
        self.child_subflow_outcomes.insert(id, value);
    }
    pub fn settle_subflow(&mut self, id: RunId, value: SubflowOutcomeFact) {
        self.settled_subflows.insert(id, value);
    }
    pub fn request_child_cancellation(&mut self, id: RunId) {
        self.child_cancellation_requests.insert(id);
    }
    pub fn record_boundary(&mut self, value: ErrorBoundaryFact) {
        self.boundary_states
            .insert(value.boundary_activation_id.clone(), value);
    }
    pub fn request_scope_cancellation(&mut self, id: ScopeInstanceId) {
        self.scope_cancellation_requests.insert(id);
    }
    pub fn record_scope_cancelled_and_drained(&mut self, id: &ScopeInstanceId) {
        let failure = TaskFailureFact::new(
            WorkerFailureClass::ControlTermination,
            "SIBLING_CANCELLED",
            None,
        )
        .expect("the internal sibling-cancellation fact is valid");
        if let Some(key) = self
            .fork_legs
            .values()
            .find(|leg| leg.scope_instance_id() == id)
            .map(|leg| leg.key().clone())
        {
            self.fork_settlements.insert(
                key,
                StructuralOutcomeFact::Failed {
                    failure: failure.clone(),
                },
            );
        }
        if let Some(key) = self
            .map_items
            .values()
            .find(|item| item.scope_instance_id() == id)
            .map(|item| item.key().clone())
        {
            self.map_settlements
                .insert(key, StructuralOutcomeFact::Failed { failure });
        }
    }
    pub fn request_run_cancellation(&mut self) {
        self.request_run_termination(TerminationReason::Cancelled);
    }
    pub fn request_run_termination(&mut self, reason: TerminationReason) {
        if self.run_termination_reason.is_none() {
            self.run_termination_reason = Some(reason);
        }
    }
    pub fn record_terminal(&mut self, terminal: RunTerminalFact) {
        self.terminal = Some(terminal);
    }
}

fn validate_wait_resolution(
    wait: &WaitRegistrationFact,
    resolution: &WaitResolutionFact,
) -> Result<(), SchedulerError> {
    match resolution.subject() {
        WaitSubjectFact::Signal { signal_id } => {
            if wait.signal_id.as_ref() != Some(signal_id) {
                return Err(inconsistent(
                    "signal resolution does not match its registration",
                ));
            }
            let payload = resolution
                .payload()
                .ok_or_else(|| inconsistent("signal resolution has no payload"))?;
            if !payload.matches(
                wait.payload_type
                    .as_ref()
                    .ok_or_else(|| inconsistent("signal wait has no payload contract"))?,
            ) {
                return Err(SchedulerError::new(
                    SCHEDULER_VALUE_TYPE_MISMATCH,
                    "signal payload does not satisfy its frozen type",
                ));
            }
        }
        WaitSubjectFact::Timer { timer_id } => {
            if wait.timer_id.as_ref() != Some(timer_id) || resolution.payload().is_some() {
                return Err(inconsistent(
                    "timer resolution does not match its registration",
                ));
            }
        }
    }
    Ok(())
}

fn inconsistent(message: &'static str) -> SchedulerError {
    SchedulerError::new(SCHEDULER_FACT_INCONSISTENT, message)
}

#[cfg(test)]
mod tests {
    use super::{RuntimeValue, SafeError, TaskFailureFact};
    use crate::engine::{plan::PlanType, WorkerFailureClass};
    use serde_json::json;

    #[test]
    fn runtime_value_wire_rederives_its_literal_type_and_rejects_a_forged_claim() {
        let value = RuntimeValue::new(json!({"answer": ["a", "b"]})).unwrap();
        let encoded = serde_json::to_value(&value).unwrap();
        assert_eq!(
            serde_json::from_value::<RuntimeValue>(encoded.clone()).unwrap(),
            value
        );

        let mut forged = encoded;
        forged["value_type"] = serde_json::to_value(PlanType::Any).unwrap();
        let error = serde_json::from_value::<RuntimeValue>(forged).unwrap_err();
        assert!(error
            .to_string()
            .contains("must equal the canonical literal type"));
    }

    #[test]
    fn safe_error_accepts_only_the_closed_public_contract() {
        let valid = RuntimeValue::new(json!({
            "kind": "safe_error",
            "code": "RISK_REJECTED",
            "message": "risk policy rejected the request"
        }))
        .unwrap();
        let safe = SafeError::try_from(valid.clone()).unwrap();
        assert_eq!(safe.code(), "RISK_REJECTED");
        assert_eq!(
            serde_json::from_value::<SafeError>(serde_json::to_value(&safe).unwrap()).unwrap(),
            safe
        );
        assert!(TaskFailureFact::new(
            WorkerFailureClass::SafeBusinessFailure,
            "RISK_REJECTED",
            Some(valid.clone()),
        )
        .is_ok());
        assert!(TaskFailureFact::new(
            WorkerFailureClass::SafeBusinessFailure,
            "DIFFERENT_CODE",
            Some(valid),
        )
        .is_err());

        for invalid in [
            json!({"kind": "safe_error", "code": "lowercase", "message": "rejected"}),
            json!({"kind": "safe_error", "code": "RISK", "message": "  "}),
            json!({"kind": "error", "code": "RISK", "message": "rejected"}),
            json!({
                "kind": "safe_error",
                "code": "RISK",
                "message": "rejected",
                "private_details": "must not cross the boundary"
            }),
            json!("RISK"),
        ] {
            let invalid = RuntimeValue::new(invalid).unwrap();
            assert!(SafeError::try_from(invalid.clone()).is_err());
            assert!(TaskFailureFact::new(
                WorkerFailureClass::SafeBusinessFailure,
                "RISK",
                Some(invalid),
            )
            .is_err());
        }
    }

    #[test]
    fn persisted_safe_error_revalidates_instead_of_trusting_runtime_value_shape() {
        let invalid = RuntimeValue::new(json!({
            "kind": "safe_error",
            "code": "not_public",
            "message": "rejected"
        }))
        .unwrap();
        let wire = serde_json::json!({
            "class": "safe_business_failure",
            "code": "RISK_REJECTED",
            "safe_error": serde_json::to_value(invalid).unwrap(),
        });
        assert!(serde_json::from_value::<TaskFailureFact>(wire).is_err());

        let mismatched_authorities = serde_json::json!({
            "class": "safe_business_failure",
            "code": "OUTER_CODE",
            "safe_error": serde_json::to_value(
                RuntimeValue::new(json!({
                    "kind": "safe_error",
                    "code": "PAYLOAD_CODE",
                    "message": "rejected"
                }))
                .unwrap()
            )
            .unwrap(),
        });
        assert!(serde_json::from_value::<TaskFailureFact>(mismatched_authorities).is_err());
    }
}
