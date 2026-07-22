use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    plan::{ControlPortId, DataPortId, DescriptorValue, PortName, SecretRef, VersionTag},
    ActivationId, ControlTokenId, EffectId, ExecutionRevisionPin, ForkGroupId, IntentHash, NodeId,
    RunId, ScopeInstanceId, TerminationReason, TransitionKey, WorkerEffectPolicy,
};

use super::{
    BranchSelectionAdmissionFact, ErrorBoundaryFact, ForkAdmissionFact, ForkLegFact,
    LogicalOccurrence, LoopInstanceFact, LoopIterationFact, MapInstanceFact, MapItemFact,
    RuntimeValue, SafeError, SchedulerCheckpointId, SchedulerTaskId, StructuralOutcomeFact,
    SubflowInvocationFact, TaskFailureFact, WaitRegistrationFact,
};

pub const SCHEDULER_INTENT_SCHEMA_VERSION: u32 = 11;

/// The immutable contract the scheduler derives from the actually admitted
/// leaf occurrence. Recovery metadata is only a hint; repository admission
/// must compare every hash here before it may materialize a reused result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReuseAdmissionContract {
    node_config_hash: crate::ContentHash,
    descriptor_hash: crate::ContentHash,
    input_value_hash: crate::ContentHash,
    output_schema_hash: crate::ContentHash,
    effect_policy_hash: crate::ContentHash,
    data_dependencies_hash: crate::ContentHash,
}

impl ReuseAdmissionContract {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_config_hash: crate::ContentHash,
        descriptor_hash: crate::ContentHash,
        input_value_hash: crate::ContentHash,
        output_schema_hash: crate::ContentHash,
        effect_policy_hash: crate::ContentHash,
        data_dependencies_hash: crate::ContentHash,
    ) -> Self {
        Self {
            node_config_hash,
            descriptor_hash,
            input_value_hash,
            output_schema_hash,
            effect_policy_hash,
            data_dependencies_hash,
        }
    }

    pub fn node_config_hash(&self) -> &crate::ContentHash {
        &self.node_config_hash
    }
    pub fn descriptor_hash(&self) -> &crate::ContentHash {
        &self.descriptor_hash
    }
    pub fn input_value_hash(&self) -> &crate::ContentHash {
        &self.input_value_hash
    }
    pub fn output_schema_hash(&self) -> &crate::ContentHash {
        &self.output_schema_hash
    }
    pub fn effect_policy_hash(&self) -> &crate::ContentHash {
        &self.effect_policy_hash
    }
    pub fn data_dependencies_hash(&self) -> &crate::ContentHash {
        &self.data_dependencies_hash
    }
}

/// One pending candidate observed in the committed recovery projection.
/// `contract=None` is deliberate for non-leaf nodes and forces a durable
/// rejection followed by ordinary admission in the same transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReuseAdmissionCandidate {
    candidate_id: String,
    expected_projection_version: u64,
    contract: Option<ReuseAdmissionContract>,
}

impl<'de> Deserialize<'de> for ReuseAdmissionCandidate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            candidate_id: String,
            expected_projection_version: u64,
            contract: Option<ReuseAdmissionContract>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.candidate_id,
            wire.expected_projection_version,
            wire.contract,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl ReuseAdmissionCandidate {
    pub fn new(
        candidate_id: impl Into<String>,
        expected_projection_version: u64,
        contract: Option<ReuseAdmissionContract>,
    ) -> Result<Self, super::SchedulerError> {
        let candidate_id = candidate_id.into();
        if candidate_id.is_empty()
            || candidate_id.len() > 256
            || candidate_id
                .chars()
                .any(|value| value.is_control() || value.is_whitespace())
        {
            return Err(super::SchedulerError::new(
                super::SCHEDULER_FACT_INCONSISTENT,
                "reuse candidate id is not a bounded durable label",
            ));
        }
        Ok(Self {
            candidate_id,
            expected_projection_version,
            contract,
        })
    }

    pub fn candidate_id(&self) -> &str {
        &self.candidate_id
    }
    pub fn expected_projection_version(&self) -> u64 {
        self.expected_projection_version
    }
    pub fn contract(&self) -> Option<&ReuseAdmissionContract> {
        self.contract.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerTaskKind {
    Llm,
    Action,
    Retrieval,
    Http,
    Tool,
}

/// Closed admission authority for one durable worker task.
///
/// `TerminationFinalizer` is structural scheduler authority: it is minted
/// only while traversing an authored ErrorFinalizer scope. It must never be
/// inferred from a node id, implementation name, or worker error string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAdmissionClass {
    Normal,
    TerminationFinalizer,
}

impl TaskAdmissionClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::TerminationFinalizer => "termination_finalizer",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundTaskInput {
    port_id: DataPortId,
    name: PortName,
    value: RuntimeValue,
    /// Exact source Activations that supplied this immutable input. This is
    /// audit/recovery evidence, not part of the provider payload and not a
    /// value-compatibility shortcut.
    #[serde(default)]
    source_activations: BTreeSet<ActivationId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskOutputContract {
    port_id: DataPortId,
    name: PortName,
    value_type: crate::plan::PlanType,
    required: bool,
}

impl TaskOutputContract {
    pub(crate) fn new(
        port_id: DataPortId,
        name: PortName,
        value_type: crate::plan::PlanType,
        required: bool,
    ) -> Self {
        Self {
            port_id,
            name,
            value_type,
            required,
        }
    }

    pub fn port_id(&self) -> &DataPortId {
        &self.port_id
    }

    pub fn name(&self) -> &PortName {
        &self.name
    }

    pub fn value_type(&self) -> &crate::plan::PlanType {
        &self.value_type
    }

    pub fn required(&self) -> bool {
        self.required
    }
}

impl BoundTaskInput {
    pub(crate) fn with_source_activations(
        port_id: DataPortId,
        name: PortName,
        value: RuntimeValue,
        source_activations: BTreeSet<ActivationId>,
    ) -> Self {
        Self {
            port_id,
            name,
            value,
            source_activations,
        }
    }

    pub fn port_id(&self) -> &DataPortId {
        &self.port_id
    }

    pub fn name(&self) -> &PortName {
        &self.name
    }

    pub fn value(&self) -> &RuntimeValue {
        &self.value
    }

    pub fn source_activations(&self) -> &BTreeSet<ActivationId> {
        &self.source_activations
    }
}

fn contract_hash<T: Serialize + ?Sized>(
    value: &T,
) -> Result<crate::ContentHash, super::SchedulerError> {
    serde_jcs::to_vec(value)
        .map(|encoded| crate::ContentHash::from_bytes(&encoded))
        .map_err(|_| {
            super::SchedulerError::new(
                super::SCHEDULER_FACT_INCONSISTENT,
                "reuse admission contract is not canonically serializable",
            )
        })
}

impl ReuseAdmissionContract {
    /// Derive the six compatibility hashes from a frozen worker request. When
    /// `port_mapping` is present every source data port must be explicitly
    /// mapped to its target identity; this is the migrate boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn from_task_parts(
        task_kind: SchedulerTaskKind,
        implementation: &str,
        descriptor_version: &VersionTag,
        worker_version: &VersionTag,
        effect_policy: &WorkerEffectPolicy,
        deployment_binding: &serde_json::Value,
        public_configuration: &BTreeMap<String, DescriptorValue>,
        secret_configuration: &BTreeMap<String, SecretRef>,
        inputs: &[BoundTaskInput],
        outputs: &[TaskOutputContract],
        port_mapping: Option<&BTreeMap<DataPortId, DataPortId>>,
    ) -> Result<Self, super::SchedulerError> {
        let map_port = |port: &DataPortId| -> Result<DataPortId, super::SchedulerError> {
            match port_mapping {
                Some(mapping) => mapping.get(port).cloned().ok_or_else(|| {
                    super::SchedulerError::new(
                        super::SCHEDULER_FACT_INCONSISTENT,
                        "migrate reuse contract omitted an admitted data-port mapping",
                    )
                }),
                None => Ok(port.clone()),
            }
        };

        let mut input_projection = Vec::with_capacity(inputs.len());
        let mut dependency_projection = Vec::with_capacity(inputs.len());
        for input in inputs {
            let port_id = map_port(input.port_id())?;
            input_projection.push(serde_json::json!({
                "port_id": port_id,
                "value": input.value(),
            }));
            dependency_projection.push(serde_json::json!({
                "port_id": port_id,
                "value_hash": contract_hash(input.value())?,
            }));
        }
        input_projection
            .sort_by(|left, right| left["port_id"].as_str().cmp(&right["port_id"].as_str()));
        dependency_projection
            .sort_by(|left, right| left["port_id"].as_str().cmp(&right["port_id"].as_str()));

        let mut output_projection = Vec::with_capacity(outputs.len());
        for output in outputs {
            output_projection.push(serde_json::json!({
                "port_id": map_port(output.port_id())?,
                "value_type": output.value_type(),
                "required": output.required(),
            }));
        }
        output_projection
            .sort_by(|left, right| left["port_id"].as_str().cmp(&right["port_id"].as_str()));

        // `runtime_bindings` is compiler-owned descriptor metadata whose
        // values are concrete DataPort identities. A cross-revision migrate
        // must compare the source descriptor in the target port namespace;
        // otherwise a harmless node rename changes generated port IDs and
        // makes an explicit, fully checked mapping impossible to reuse.
        let mut mapped_public_configuration = public_configuration.clone();
        if let Some(port_mapping) = port_mapping {
            if let Some(runtime_bindings) = mapped_public_configuration.get_mut("runtime_bindings")
            {
                let DescriptorValue::Object(runtime_bindings) = runtime_bindings else {
                    return Err(super::SchedulerError::new(
                        super::SCHEDULER_FACT_INCONSISTENT,
                        "runtime binding configuration is not an object",
                    ));
                };
                for binding in runtime_bindings.values_mut() {
                    let DescriptorValue::String(source_port) = binding else {
                        return Err(super::SchedulerError::new(
                            super::SCHEDULER_FACT_INCONSISTENT,
                            "runtime binding does not name a data port",
                        ));
                    };
                    let target_port = port_mapping
                        .iter()
                        .find_map(|(source, target)| {
                            (source.as_str() == source_port).then_some(target)
                        })
                        .ok_or_else(|| {
                            super::SchedulerError::new(
                                super::SCHEDULER_FACT_INCONSISTENT,
                                "migrate reuse contract omitted a runtime-binding port mapping",
                            )
                        })?;
                    *source_port = target_port.as_str().to_owned();
                }
            }
        }

        Ok(Self::new(
            contract_hash(&serde_json::json!({
                "public_configuration": mapped_public_configuration,
                "secret_configuration": secret_configuration,
            }))?,
            contract_hash(&serde_json::json!({
                "task_kind": task_kind,
                "implementation": implementation,
                "descriptor_version": descriptor_version,
                "worker_version": worker_version,
                "deployment_binding": deployment_binding,
            }))?,
            contract_hash(&input_projection)?,
            contract_hash(&output_projection)?,
            contract_hash(effect_policy)?,
            contract_hash(&dependency_projection)?,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeOutput {
    Values {
        values: BTreeMap<DataPortId, RuntimeValue>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerCancellationReason {
    SiblingFailed,
    ParentRunCancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchedulerAction {
    /// Durable fail-closed authority for a deterministic planner error. It has
    /// no Activation identity because planning can fail before entry admission.
    FailRunPlanning {
        failure: super::SchedulerPlanningFailure,
    },
    AdmitActivation {
        activation_id: ActivationId,
        node_id: NodeId,
        scope_instance_id: ScopeInstanceId,
        occurrence: LogicalOccurrence,
        reuse_candidate: Option<ReuseAdmissionCandidate>,
    },
    ConsumeToken {
        token_id: ControlTokenId,
        target_activation_id: ActivationId,
        input_port: ControlPortId,
    },
    EmitToken {
        token_id: ControlTokenId,
        source_activation_id: ActivationId,
        output_port: ControlPortId,
        scope_instance_id: ScopeInstanceId,
    },
    DispatchTask {
        task_id: SchedulerTaskId,
        effect_id: EffectId,
        activation_id: ActivationId,
        node_id: NodeId,
        admission_class: TaskAdmissionClass,
        task_kind: SchedulerTaskKind,
        implementation: String,
        descriptor_version: VersionTag,
        worker_version: VersionTag,
        effect_policy: WorkerEffectPolicy,
        deployment_binding: serde_json::Value,
        public_configuration: BTreeMap<String, DescriptorValue>,
        secret_configuration: BTreeMap<String, SecretRef>,
        inputs: Vec<BoundTaskInput>,
        outputs: Vec<TaskOutputContract>,
    },
    CommitNativeOutput {
        activation_id: ActivationId,
        node_id: NodeId,
        occurrence: LogicalOccurrence,
        output: NativeOutput,
    },
    SelectBranchAndAdmit {
        selection: BranchSelectionAdmissionFact,
    },
    OpenFork {
        admission: ForkAdmissionFact,
    },
    SettleForkLeg {
        leg: ForkLegFact,
        outcome: StructuralOutcomeFact,
    },
    CompleteFork {
        group_id: ForkGroupId,
        join_activation_id: ActivationId,
    },
    RequestScopeCancellation {
        scope_instance_id: ScopeInstanceId,
        reason: SchedulerCancellationReason,
    },
    OpenMap {
        map: MapInstanceFact,
    },
    SpawnMapItem {
        item: MapItemFact,
        item_port: DataPortId,
        item_value: RuntimeValue,
        output_port: ControlPortId,
    },
    SettleMapItem {
        item: MapItemFact,
        outcome: StructuralOutcomeFact,
    },
    CompleteMap {
        map_activation_id: ActivationId,
    },
    OpenLoop {
        loop_instance: LoopInstanceFact,
    },
    StartLoopIteration {
        iteration: LoopIterationFact,
        state_port: DataPortId,
        output_port: ControlPortId,
    },
    AdvanceLoop {
        iteration: LoopIterationFact,
        state: RuntimeValue,
    },
    SettleLoopIteration {
        iteration: LoopIterationFact,
        outcome: StructuralOutcomeFact,
    },
    CompleteLoop {
        loop_activation_id: ActivationId,
        iteration: Option<LoopIterationFact>,
        state: RuntimeValue,
    },
    RegisterWait {
        registration: WaitRegistrationFact,
    },
    CommitOccurrenceValues {
        activation_id: ActivationId,
        node_id: NodeId,
        occurrence: LogicalOccurrence,
        values: BTreeMap<DataPortId, RuntimeValue>,
    },
    StartSubflow {
        invocation: SubflowInvocationFact,
        execution_revision: ExecutionRevisionPin,
        interface_version: VersionTag,
        timeout_ms: u64,
        run_input: RuntimeValue,
        outputs: Vec<TaskOutputContract>,
    },
    RequestChildRunCancellation {
        child_run_id: RunId,
    },
    /// A terminal child Run is only an observation. This closed action is the
    /// sole authority that acknowledges the exact outcome into the parent,
    /// terminals the call activation, and settles its invocation scope.
    SettleSubflow {
        invocation: SubflowInvocationFact,
        outcome: super::SubflowOutcomeFact,
    },
    OpenErrorBoundary {
        boundary: ErrorBoundaryFact,
    },
    TransitionErrorBoundary {
        boundary: ErrorBoundaryFact,
    },
    CompleteRun {
        activation_id: ActivationId,
        output: RuntimeValue,
    },
    FailRun {
        activation_id: ActivationId,
        error: SafeError,
    },
    FailRunInternal {
        activation_id: ActivationId,
        failure: TaskFailureFact,
    },
    CancelRun {
        activation_id: ActivationId,
        /// The first-winner durable Run intent. The historical variant name is
        /// retained only as an internal action label; repositories must commit
        /// the terminal lifecycle corresponding to this exact reason.
        reason: TerminationReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerIntent {
    schema_version: u32,
    run_id: RunId,
    checkpoint_id: SchedulerCheckpointId,
    action: SchedulerAction,
}

impl<'de> Deserialize<'de> for SchedulerIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema_version: u32,
            run_id: RunId,
            checkpoint_id: SchedulerCheckpointId,
            action: SchedulerAction,
        }
        let wire = Wire::deserialize(deserializer)?;
        if wire.schema_version != SCHEDULER_INTENT_SCHEMA_VERSION {
            return Err(serde::de::Error::custom(
                "unsupported scheduler intent schema version",
            ));
        }
        Ok(Self {
            schema_version: wire.schema_version,
            run_id: wire.run_id,
            checkpoint_id: wire.checkpoint_id,
            action: wire.action,
        })
    }
}

impl SchedulerIntent {
    pub(crate) fn new(
        run_id: RunId,
        checkpoint_id: SchedulerCheckpointId,
        action: SchedulerAction,
    ) -> Self {
        Self {
            schema_version: SCHEDULER_INTENT_SCHEMA_VERSION,
            run_id,
            checkpoint_id,
            action,
        }
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn checkpoint_id(&self) -> &SchedulerCheckpointId {
        &self.checkpoint_id
    }

    pub fn action(&self) -> &SchedulerAction {
        &self.action
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerPrecondition {
    expected_projection_version: u64,
}

impl SchedulerPrecondition {
    pub(crate) fn new(expected_projection_version: u64) -> Self {
        Self {
            expected_projection_version,
        }
    }

    pub fn expected_projection_version(self) -> u64 {
        self.expected_projection_version
    }
}

/// One inert repository command. Planning does not execute it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedSchedulerAction {
    precondition: SchedulerPrecondition,
    transition_key: TransitionKey,
    intent_hash: IntentHash,
    intent: SchedulerIntent,
}

impl<'de> Deserialize<'de> for PlannedSchedulerAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            precondition: SchedulerPrecondition,
            transition_key: TransitionKey,
            intent_hash: IntentHash,
            intent: SchedulerIntent,
        }

        let wire = Wire::deserialize(deserializer)?;
        let derived =
            IntentHash::from_serializable(&wire.intent).map_err(serde::de::Error::custom)?;
        if wire.intent_hash != derived {
            return Err(serde::de::Error::custom(
                "scheduler action intent hash does not match its canonical intent",
            ));
        }
        Ok(Self {
            precondition: wire.precondition,
            transition_key: wire.transition_key,
            intent_hash: wire.intent_hash,
            intent: wire.intent,
        })
    }
}

impl PlannedSchedulerAction {
    pub(crate) fn new(
        precondition: SchedulerPrecondition,
        transition_key: TransitionKey,
        intent_hash: IntentHash,
        intent: SchedulerIntent,
    ) -> Self {
        Self {
            precondition,
            transition_key,
            intent_hash,
            intent,
        }
    }

    pub fn precondition(&self) -> SchedulerPrecondition {
        self.precondition
    }

    pub fn transition_key(&self) -> &TransitionKey {
        &self.transition_key
    }

    pub fn intent_hash(&self) -> &IntentHash {
        &self.intent_hash
    }

    pub fn intent(&self) -> &SchedulerIntent {
        &self.intent
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchedulerQuiescence {
    WaitingForTask {
        task_id: SchedulerTaskId,
        activation_id: ActivationId,
    },
    WaitingForChildren {
        scope_instance_ids: Vec<ScopeInstanceId>,
    },
    WaitingForDrain {
        scope_instance_ids: Vec<ScopeInstanceId>,
    },
    WaitingForWait {
        wait_id: super::SchedulerWaitId,
        activation_id: ActivationId,
    },
    WaitingForChildRun {
        child_run_id: RunId,
        activation_id: ActivationId,
    },
    RunSucceeded,
    RunFailed,
    RunCancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "decision",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SchedulerDecision {
    Action(Box<PlannedSchedulerAction>),
    Quiescent(SchedulerQuiescence),
}

impl SchedulerDecision {
    pub fn action(&self) -> Option<&PlannedSchedulerAction> {
        match self {
            Self::Action(action) => Some(action.as_ref()),
            Self::Quiescent(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{RuntimeValue, SchedulerAction};

    #[test]
    fn persisted_fail_run_action_rejects_an_unvalidated_runtime_value() {
        let invalid = RuntimeValue::new(json!({
            "kind": "safe_error",
            "code": "invalid_code",
            "message": "must fail closed"
        }))
        .unwrap();
        let wire = json!({
            "kind": "fail_run",
            "activation_id": "activation_safe_error_restore",
            "error": serde_json::to_value(invalid).unwrap(),
        });
        assert!(serde_json::from_value::<SchedulerAction>(wire).is_err());
    }
}
