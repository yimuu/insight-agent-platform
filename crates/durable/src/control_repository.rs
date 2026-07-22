use super::RepositoryErrorExt as _;

use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

use insight_engine::{
    ActivationId, ContentHash, ControlTokenId, ControlTokenProvenance, DefinitionRevisionId,
    DeploymentRevisionId, EffectId, ExecutionControlFrame, ExecutionKind,
    ExecutionLegSettlementClass, ExecutionScopeKind, ForkGroupId, ForkLeg, JoinMode, LegId, NodeId,
    RunId, ScopeInstance, ScopeInstanceId, ScopeKind, TransitionKey, TransitionOutcome,
};

use super::RepositoryError;

const MAX_LABEL_BYTES: usize = 512;

pub(crate) struct ScopeStorage<'a> {
    pub static_scope_id: &'a str,
    pub stable_dynamic_key: String,
    pub scope_kind: &'static str,
    pub event_kind: ExecutionScopeKind,
}

pub(crate) fn scope_storage(scope: &ScopeInstance) -> Result<ScopeStorage<'_>, RepositoryError> {
    let (owner, stable_dynamic_key, scope_kind, event_kind) = match scope.kind() {
        ScopeKind::Root => return Err(invalid_command()),
        ScopeKind::MapItem {
            owner, identity, ..
        } => (
            owner,
            identity.stable_dynamic_key(),
            "map_item",
            ExecutionScopeKind::MapItem,
        ),
        ScopeKind::LoopIteration { owner, iteration } => (
            owner,
            format!("iteration:{iteration}"),
            "loop_iteration",
            ExecutionScopeKind::LoopIteration,
        ),
        ScopeKind::SubflowInvocation {
            owner,
            invocation_key,
        } => (
            owner,
            format!("invocation:{}", invocation_key.as_str()),
            "subflow_invocation",
            ExecutionScopeKind::SubflowInvocation,
        ),
        ScopeKind::AgentLoopTurn { owner, turn } => (
            owner,
            format!("turn:{turn}"),
            "agent_loop_turn",
            ExecutionScopeKind::AgentLoopTurn,
        ),
        ScopeKind::ParallelLeg { owner, leg_id } => (
            owner,
            format!("leg:{}", leg_id.as_str()),
            "parallel_leg",
            ExecutionScopeKind::ParallelLeg,
        ),
    };
    Ok(ScopeStorage {
        static_scope_id: owner.as_str(),
        stable_dynamic_key,
        scope_kind,
        event_kind,
    })
}

pub(crate) fn event_control_frames(
    provenance: &ControlTokenProvenance,
) -> Vec<ExecutionControlFrame> {
    provenance
        .frames()
        .iter()
        .map(|frame| match frame {
            insight_engine::control::ControlFrame::Branch(frame) => ExecutionControlFrame::Branch {
                branch_activation_id: frame.branch_activation_id().clone(),
                selected_port: frame.selected_port().clone(),
                scope_instance_id: frame.scope_instance_id().clone(),
            },
            insight_engine::control::ControlFrame::ForkLeg(frame) => {
                ExecutionControlFrame::ForkLeg {
                    fork_activation_id: frame.fork_activation_id().clone(),
                    fork_group_id: frame.fork_group_id().clone(),
                    leg_id: frame.leg_id().clone(),
                    scope_instance_id: frame.scope_instance_id().clone(),
                }
            }
        })
        .collect()
}

pub(crate) const fn join_mode_str(mode: JoinMode) -> &'static str {
    match mode {
        JoinMode::AllSuccess => "all_success",
        JoinMode::AllSettled => "all_settled",
    }
}

pub(crate) const fn settlement_str(value: ExecutionLegSettlementClass) -> &'static str {
    match value {
        ExecutionLegSettlementClass::Succeeded => "succeeded",
        ExecutionLegSettlementClass::SafeFailure => "safe_failure",
        ExecutionLegSettlementClass::InfrastructureFailure => "infrastructure_failure",
        ExecutionLegSettlementClass::Panic => "panic",
        ExecutionLegSettlementClass::Cancelled => "cancelled",
        ExecutionLegSettlementClass::TimedOut => "timed_out",
    }
}

pub(crate) fn parse_settlement(
    value: &str,
) -> Result<ExecutionLegSettlementClass, RepositoryError> {
    match value {
        "succeeded" => Ok(ExecutionLegSettlementClass::Succeeded),
        "safe_failure" => Ok(ExecutionLegSettlementClass::SafeFailure),
        "infrastructure_failure" => Ok(ExecutionLegSettlementClass::InfrastructureFailure),
        "panic" => Ok(ExecutionLegSettlementClass::Panic),
        "cancelled" => Ok(ExecutionLegSettlementClass::Cancelled),
        "timed_out" => Ok(ExecutionLegSettlementClass::TimedOut),
        _ => Err(RepositoryError::invalid_data()),
    }
}

fn invalid_command() -> RepositoryError {
    RepositoryError::invalid_configuration()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct DurableLabel(String);

impl DurableLabel {
    fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_LABEL_BYTES
            || value
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(invalid_command());
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for DurableLabel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DurableLabel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct StableActivationKey(String);

impl StableActivationKey {
    fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        if value.is_empty() || value.len() > 131_072 || value.chars().any(char::is_control) {
            return Err(invalid_command());
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for StableActivationKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for StableActivationKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlCommitReceipt {
    event_seq: u64,
    event_id: String,
    projection_version: u64,
}

impl ControlCommitReceipt {
    pub(crate) fn new(event_seq: u64, event_id: String, projection_version: u64) -> Self {
        Self {
            event_seq,
            event_id,
            projection_version,
        }
    }

    pub fn event_seq(&self) -> u64 {
        self.event_seq
    }

    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    pub fn projection_version(&self) -> u64 {
        self.projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateChildScopeCommand {
    run_id: RunId,
    scope: ScopeInstance,
    expected_parent_projection_version: u64,
}

impl CreateChildScopeCommand {
    pub fn new(
        run_id: RunId,
        scope: ScopeInstance,
        expected_parent_projection_version: u64,
    ) -> Result<Self, RepositoryError> {
        scope.validate().map_err(|_| invalid_command())?;
        if scope.parent().is_none() {
            return Err(invalid_command());
        }
        Ok(Self {
            run_id,
            scope,
            expected_parent_projection_version,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn scope(&self) -> &ScopeInstance {
        &self.scope
    }

    pub fn expected_parent_projection_version(&self) -> u64 {
        self.expected_parent_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseScopeAdmissionCommand {
    run_id: RunId,
    scope_instance_id: ScopeInstanceId,
    expected_projection_version: u64,
}

impl CloseScopeAdmissionCommand {
    pub fn new(
        run_id: RunId,
        scope_instance_id: ScopeInstanceId,
        expected_projection_version: u64,
    ) -> Self {
        Self {
            run_id,
            scope_instance_id,
            expected_projection_version,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }
    pub fn expected_projection_version(&self) -> u64 {
        self.expected_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettleScopeCommand {
    run_id: RunId,
    scope_instance_id: ScopeInstanceId,
    expected_projection_version: u64,
}

impl SettleScopeCommand {
    pub fn new(
        run_id: RunId,
        scope_instance_id: ScopeInstanceId,
        expected_projection_version: u64,
    ) -> Self {
        Self {
            run_id,
            scope_instance_id,
            expected_projection_version,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }
    pub fn expected_projection_version(&self) -> u64 {
        self.expected_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmitControlTokenCommand {
    token_id: ControlTokenId,
    provenance: ControlTokenProvenance,
    expected_source_projection_version: u64,
}

impl EmitControlTokenCommand {
    pub fn new(
        token_id: ControlTokenId,
        provenance: ControlTokenProvenance,
        expected_source_projection_version: u64,
    ) -> Self {
        Self {
            token_id,
            provenance,
            expected_source_projection_version,
        }
    }

    pub fn run_id(&self) -> &RunId {
        self.provenance.run_id()
    }
    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }
    pub fn provenance(&self) -> &ControlTokenProvenance {
        &self.provenance
    }
    pub fn expected_source_projection_version(&self) -> u64 {
        self.expected_source_projection_version
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenConsumerKind {
    Activation,
    Branch,
    Merge,
    Fork,
    Join,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumeControlTokenCommand {
    run_id: RunId,
    token_id: ControlTokenId,
    consumer_activation_id: ActivationId,
    consumer_kind: TokenConsumerKind,
    expected_token_projection_version: u64,
}

impl ConsumeControlTokenCommand {
    pub fn new(
        run_id: RunId,
        token_id: ControlTokenId,
        consumer_activation_id: ActivationId,
        consumer_kind: TokenConsumerKind,
        expected_token_projection_version: u64,
    ) -> Self {
        Self {
            run_id,
            token_id,
            consumer_activation_id,
            consumer_kind,
            expected_token_projection_version,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }
    pub fn consumer_activation_id(&self) -> &ActivationId {
        &self.consumer_activation_id
    }
    pub fn consumer_kind(&self) -> TokenConsumerKind {
        self.consumer_kind
    }
    pub fn expected_token_projection_version(&self) -> u64 {
        self.expected_token_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeControlTokenCommand {
    run_id: RunId,
    token_id: ControlTokenId,
    expected_token_projection_version: u64,
}

impl RevokeControlTokenCommand {
    pub fn new(
        run_id: RunId,
        token_id: ControlTokenId,
        expected_token_projection_version: u64,
    ) -> Self {
        Self {
            run_id,
            token_id,
            expected_token_projection_version,
        }
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }
    pub fn expected_token_projection_version(&self) -> u64 {
        self.expected_token_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkLegAdmission {
    leg: ForkLeg,
    scope: ScopeInstance,
    child_node_id: NodeId,
    stable_activation_key: DurableLabel,
    execution_kind: ExecutionKind,
    token_id: ControlTokenId,
}

impl ForkLegAdmission {
    pub fn new(
        leg: ForkLeg,
        scope: ScopeInstance,
        child_node_id: NodeId,
        stable_activation_key: impl Into<String>,
        execution_kind: ExecutionKind,
        token_id: ControlTokenId,
    ) -> Result<Self, RepositoryError> {
        scope.validate().map_err(|_| invalid_command())?;
        if scope.id() != leg.scope_instance_id() || scope.parent().is_none() {
            return Err(invalid_command());
        }
        Ok(Self {
            leg,
            scope,
            child_node_id,
            stable_activation_key: DurableLabel::new(stable_activation_key)?,
            execution_kind,
            token_id,
        })
    }

    pub fn leg(&self) -> &ForkLeg {
        &self.leg
    }
    pub fn scope(&self) -> &ScopeInstance {
        &self.scope
    }
    pub fn child_node_id(&self) -> &NodeId {
        &self.child_node_id
    }
    pub fn stable_activation_key(&self) -> &str {
        self.stable_activation_key.as_str()
    }
    pub fn execution_kind(&self) -> &ExecutionKind {
        &self.execution_kind
    }
    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateForkCommand {
    run_id: RunId,
    fork_group_id: ForkGroupId,
    fork_activation_id: ActivationId,
    parent_scope_instance_id: ScopeInstanceId,
    expected_fork_activation_projection_version: u64,
    expected_parent_scope_projection_version: u64,
    inherited_token_id: Option<ControlTokenId>,
    expected_inherited_token_projection_version: Option<u64>,
    legs: Vec<ForkLegAdmission>,
}

impl CreateForkCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: RunId,
        fork_group_id: ForkGroupId,
        fork_activation_id: ActivationId,
        parent_scope_instance_id: ScopeInstanceId,
        expected_fork_activation_projection_version: u64,
        expected_parent_scope_projection_version: u64,
        inherited_token: Option<(ControlTokenId, u64)>,
        legs: Vec<ForkLegAdmission>,
    ) -> Result<Self, RepositoryError> {
        if legs.is_empty() || legs.len() > 1024 {
            return Err(invalid_command());
        }
        let mut leg_ids = std::collections::BTreeSet::new();
        let mut scopes = std::collections::BTreeSet::new();
        let mut activations = std::collections::BTreeSet::new();
        let mut tokens = std::collections::BTreeSet::new();
        for admission in &legs {
            if admission.leg().run_id() != &run_id
                || admission.scope().parent() != Some(&parent_scope_instance_id)
                || !leg_ids.insert(admission.leg().leg_id().clone())
                || !scopes.insert(admission.scope().id().clone())
                || !activations.insert(admission.leg().child_activation_id().clone())
                || !tokens.insert(admission.token_id().clone())
            {
                return Err(invalid_command());
            }
        }
        let (inherited_token_id, expected_inherited_token_projection_version) =
            inherited_token.unzip();
        Ok(Self {
            run_id,
            fork_group_id,
            fork_activation_id,
            parent_scope_instance_id,
            expected_fork_activation_projection_version,
            expected_parent_scope_projection_version,
            inherited_token_id,
            expected_inherited_token_projection_version,
            legs,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn fork_group_id(&self) -> &ForkGroupId {
        &self.fork_group_id
    }
    pub fn fork_activation_id(&self) -> &ActivationId {
        &self.fork_activation_id
    }
    pub fn parent_scope_instance_id(&self) -> &ScopeInstanceId {
        &self.parent_scope_instance_id
    }
    pub fn expected_fork_activation_projection_version(&self) -> u64 {
        self.expected_fork_activation_projection_version
    }
    pub fn expected_parent_scope_projection_version(&self) -> u64 {
        self.expected_parent_scope_projection_version
    }
    pub fn inherited_token_id(&self) -> Option<&ControlTokenId> {
        self.inherited_token_id.as_ref()
    }
    pub fn expected_inherited_token_projection_version(&self) -> Option<u64> {
        self.expected_inherited_token_projection_version
    }
    pub fn legs(&self) -> &[ForkLegAdmission] {
        &self.legs
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordJoinArrivalCommand {
    run_id: RunId,
    join_activation_id: ActivationId,
    fork_group_id: ForkGroupId,
    leg_id: LegId,
    token_id: ControlTokenId,
    mode: JoinMode,
    expected_group_projection_version: u64,
    expected_token_projection_version: u64,
}

impl RecordJoinArrivalCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: RunId,
        join_activation_id: ActivationId,
        fork_group_id: ForkGroupId,
        leg_id: LegId,
        token_id: ControlTokenId,
        mode: JoinMode,
        expected_group_projection_version: u64,
        expected_token_projection_version: u64,
    ) -> Self {
        Self {
            run_id,
            join_activation_id,
            fork_group_id,
            leg_id,
            token_id,
            mode,
            expected_group_projection_version,
            expected_token_projection_version,
        }
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn join_activation_id(&self) -> &ActivationId {
        &self.join_activation_id
    }
    pub fn fork_group_id(&self) -> &ForkGroupId {
        &self.fork_group_id
    }
    pub fn leg_id(&self) -> &LegId {
        &self.leg_id
    }
    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }
    pub fn mode(&self) -> JoinMode {
        self.mode
    }
    pub fn expected_group_projection_version(&self) -> u64 {
        self.expected_group_projection_version
    }
    pub fn expected_token_projection_version(&self) -> u64 {
        self.expected_token_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum JoinBarrierAuthority {
    Pending {
        settled_legs: u32,
        expected_legs: u32,
    },
    Draining {
        failed_leg_id: LegId,
        settled_legs: u32,
        expected_legs: u32,
    },
    Ready {
        mode: JoinMode,
        settled_legs: u32,
    },
    Failed {
        failed_leg_id: LegId,
        settlement_class: ExecutionLegSettlementClass,
        settled_legs: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinArrivalReceipt {
    commit: ControlCommitReceipt,
    authority: JoinBarrierAuthority,
}

impl JoinArrivalReceipt {
    pub(crate) fn new(commit: ControlCommitReceipt, authority: JoinBarrierAuthority) -> Self {
        Self { commit, authority }
    }
    pub fn commit(&self) -> &ControlCommitReceipt {
        &self.commit
    }
    pub fn authority(&self) -> &JoinBarrierAuthority {
        &self.authority
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReuseCompatibility {
    node_config_hash: ContentHash,
    descriptor_hash: ContentHash,
    input_value_hash: ContentHash,
    output_schema_hash: ContentHash,
    effect_policy_hash: ContentHash,
    data_dependencies_hash: ContentHash,
}

impl ReuseCompatibility {
    pub fn new(
        node_config_hash: ContentHash,
        descriptor_hash: ContentHash,
        input_value_hash: ContentHash,
        output_schema_hash: ContentHash,
        effect_policy_hash: ContentHash,
        data_dependencies_hash: ContentHash,
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
    pub fn node_config_hash(&self) -> &ContentHash {
        &self.node_config_hash
    }
    pub fn descriptor_hash(&self) -> &ContentHash {
        &self.descriptor_hash
    }
    pub fn input_value_hash(&self) -> &ContentHash {
        &self.input_value_hash
    }
    pub fn output_schema_hash(&self) -> &ContentHash {
        &self.output_schema_hash
    }
    pub fn effect_policy_hash(&self) -> &ContentHash {
        &self.effect_policy_hash
    }
    pub fn data_dependencies_hash(&self) -> &ContentHash {
        &self.data_dependencies_hash
    }

    pub fn from_admission_contract(
        contract: &insight_engine::scheduler::ReuseAdmissionContract,
    ) -> Self {
        Self::new(
            contract.node_config_hash().clone(),
            contract.descriptor_hash().clone(),
            contract.input_value_hash().clone(),
            contract.output_schema_hash().clone(),
            contract.effect_policy_hash().clone(),
            contract.data_dependencies_hash().clone(),
        )
    }

    pub(crate) fn matches_admission_contract(
        &self,
        contract: &insight_engine::scheduler::ReuseAdmissionContract,
    ) -> bool {
        self.node_config_hash == *contract.node_config_hash()
            && self.descriptor_hash == *contract.descriptor_hash()
            && self.input_value_hash == *contract.input_value_hash()
            && self.output_schema_hash == *contract.output_schema_hash()
            && self.effect_policy_hash == *contract.effect_policy_hash()
            && self.data_dependencies_hash == *contract.data_dependencies_hash()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateReuseCandidateCommand {
    run_id: RunId,
    candidate_id: DurableLabel,
    target_scope_instance_id: ScopeInstanceId,
    target_node_id: NodeId,
    stable_activation_key: StableActivationKey,
    source_run_id: RunId,
    source_activation_id: ActivationId,
    source_control_provenance: ControlTokenProvenance,
    definition_revision_id: DefinitionRevisionId,
    deployment_revision_id: DeploymentRevisionId,
    plan_hash: ContentHash,
    binding_hash: ContentHash,
    output_value_hash: ContentHash,
    inherited_effect_id: EffectId,
    compatibility: ReuseCompatibility,
    #[serde(default)]
    source_data_dependencies: BTreeSet<ActivationId>,
}

impl CreateReuseCandidateCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: RunId,
        candidate_id: impl Into<String>,
        target_scope_instance_id: ScopeInstanceId,
        target_node_id: NodeId,
        stable_activation_key: impl Into<String>,
        source_run_id: RunId,
        source_activation_id: ActivationId,
        source_control_provenance: ControlTokenProvenance,
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        plan_hash: ContentHash,
        binding_hash: ContentHash,
        output_value_hash: ContentHash,
        inherited_effect_id: EffectId,
        compatibility: ReuseCompatibility,
    ) -> Result<Self, RepositoryError> {
        if run_id == source_run_id
            || source_control_provenance.run_id() != &source_run_id
            || source_control_provenance.source_activation_id() != &source_activation_id
        {
            return Err(invalid_command());
        }
        Ok(Self {
            run_id,
            candidate_id: DurableLabel::new(candidate_id)?,
            target_scope_instance_id,
            target_node_id,
            stable_activation_key: StableActivationKey::new(stable_activation_key)?,
            source_run_id,
            source_activation_id,
            source_control_provenance,
            definition_revision_id,
            deployment_revision_id,
            plan_hash,
            binding_hash,
            output_value_hash,
            inherited_effect_id,
            compatibility,
            source_data_dependencies: BTreeSet::new(),
        })
    }

    /// Attach the exact source Activation identities that supplied this
    /// candidate's worker inputs.  The set is repository-derived from the
    /// frozen source task envelope; callers cannot use it to make a candidate
    /// eligible because exact materialization revalidates every dependency.
    pub fn with_source_data_dependencies(
        mut self,
        dependencies: BTreeSet<ActivationId>,
    ) -> Result<Self, RepositoryError> {
        if dependencies.contains(&self.source_activation_id) {
            return Err(invalid_command());
        }
        self.source_data_dependencies = dependencies;
        Ok(self)
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn candidate_id(&self) -> &str {
        self.candidate_id.as_str()
    }
    pub fn target_scope_instance_id(&self) -> &ScopeInstanceId {
        &self.target_scope_instance_id
    }
    pub fn target_node_id(&self) -> &NodeId {
        &self.target_node_id
    }
    pub fn stable_activation_key(&self) -> &str {
        self.stable_activation_key.as_str()
    }
    pub fn source_run_id(&self) -> &RunId {
        &self.source_run_id
    }
    pub fn source_activation_id(&self) -> &ActivationId {
        &self.source_activation_id
    }
    pub fn source_control_provenance(&self) -> &ControlTokenProvenance {
        &self.source_control_provenance
    }
    pub fn definition_revision_id(&self) -> &DefinitionRevisionId {
        &self.definition_revision_id
    }
    pub fn deployment_revision_id(&self) -> &DeploymentRevisionId {
        &self.deployment_revision_id
    }
    pub fn plan_hash(&self) -> &ContentHash {
        &self.plan_hash
    }
    pub fn binding_hash(&self) -> &ContentHash {
        &self.binding_hash
    }
    pub fn output_value_hash(&self) -> &ContentHash {
        &self.output_value_hash
    }
    pub fn inherited_effect_id(&self) -> &EffectId {
        &self.inherited_effect_id
    }
    pub fn compatibility(&self) -> &ReuseCompatibility {
        &self.compatibility
    }

    pub fn source_data_dependencies(&self) -> &BTreeSet<ActivationId> {
        &self.source_data_dependencies
    }
}

/// Audited candidate provenance stored in the existing
/// `source_control_provenance` JSON authority. Keeping control and data
/// provenance in one canonical value avoids an unaudited side table while
/// still making dependency-closure decisions durable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableReuseProvenance {
    control: ControlTokenProvenance,
    #[serde(default)]
    source_data_dependencies: BTreeSet<ActivationId>,
}

impl DurableReuseProvenance {
    pub(crate) fn from_command(command: &CreateReuseCandidateCommand) -> Self {
        Self {
            control: command.source_control_provenance.clone(),
            source_data_dependencies: command.source_data_dependencies.clone(),
        }
    }

    pub(crate) fn control(&self) -> &ControlTokenProvenance {
        &self.control
    }

    pub(crate) fn source_data_dependencies(&self) -> &BTreeSet<ActivationId> {
        &self.source_data_dependencies
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectReuseCandidateCommand {
    run_id: RunId,
    candidate_id: DurableLabel,
    expected_projection_version: u64,
}

impl RejectReuseCandidateCommand {
    pub fn new(
        run_id: RunId,
        candidate_id: impl Into<String>,
        expected_projection_version: u64,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            run_id,
            candidate_id: DurableLabel::new(candidate_id)?,
            expected_projection_version,
        })
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn candidate_id(&self) -> &str {
        self.candidate_id.as_str()
    }
    pub fn expected_projection_version(&self) -> u64 {
        self.expected_projection_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializeReuseCandidateCommand {
    run_id: RunId,
    candidate_id: DurableLabel,
    activation_id: ActivationId,
    expected_candidate_projection_version: u64,
    expected_scope_projection_version: u64,
    compatibility: ReuseCompatibility,
}

impl MaterializeReuseCandidateCommand {
    pub fn new(
        run_id: RunId,
        candidate_id: impl Into<String>,
        activation_id: ActivationId,
        expected_candidate_projection_version: u64,
        expected_scope_projection_version: u64,
        compatibility: ReuseCompatibility,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            run_id,
            candidate_id: DurableLabel::new(candidate_id)?,
            activation_id,
            expected_candidate_projection_version,
            expected_scope_projection_version,
            compatibility,
        })
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn candidate_id(&self) -> &str {
        self.candidate_id.as_str()
    }
    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }
    pub fn expected_candidate_projection_version(&self) -> u64 {
        self.expected_candidate_projection_version
    }
    pub fn expected_scope_projection_version(&self) -> u64 {
        self.expected_scope_projection_version
    }
    pub fn compatibility(&self) -> &ReuseCompatibility {
        &self.compatibility
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimSchedulerRunCommand {
    run_id: RunId,
    owner: DurableLabel,
    lease_seconds: u32,
}

impl ClaimSchedulerRunCommand {
    pub fn new(
        run_id: RunId,
        owner: impl Into<String>,
        lease_seconds: u32,
    ) -> Result<Self, RepositoryError> {
        if lease_seconds == 0 || lease_seconds > 86_400 {
            return Err(invalid_command());
        }
        Ok(Self {
            run_id,
            owner: DurableLabel::new(owner)?,
            lease_seconds,
        })
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn owner(&self) -> &str {
        self.owner.as_str()
    }
    pub fn lease_seconds(&self) -> u32 {
        self.lease_seconds
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencedSchedulerRunCommand {
    run_id: RunId,
    owner: DurableLabel,
    lease_epoch: u64,
    fencing_token: DurableLabel,
}

impl FencedSchedulerRunCommand {
    pub fn new(
        run_id: RunId,
        owner: impl Into<String>,
        lease_epoch: u64,
        fencing_token: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        if lease_epoch == 0 {
            return Err(invalid_command());
        }
        Ok(Self {
            run_id,
            owner: DurableLabel::new(owner)?,
            lease_epoch,
            fencing_token: DurableLabel::new(fencing_token)?,
        })
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn owner(&self) -> &str {
        self.owner.as_str()
    }
    pub fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }
    pub fn fencing_token(&self) -> &str {
        self.fencing_token.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatSchedulerRunCommand {
    fence: FencedSchedulerRunCommand,
    lease_seconds: u32,
}

impl HeartbeatSchedulerRunCommand {
    pub fn new(
        fence: FencedSchedulerRunCommand,
        lease_seconds: u32,
    ) -> Result<Self, RepositoryError> {
        if lease_seconds == 0 || lease_seconds > 86_400 {
            return Err(invalid_command());
        }
        Ok(Self {
            fence,
            lease_seconds,
        })
    }
    pub fn fence(&self) -> &FencedSchedulerRunCommand {
        &self.fence
    }
    pub fn lease_seconds(&self) -> u32 {
        self.lease_seconds
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerRunLease {
    run_id: RunId,
    owner: DurableLabel,
    lease_epoch: u64,
    fencing_token: DurableLabel,
    expires_at: DateTime<Utc>,
}

impl SchedulerRunLease {
    pub(crate) fn new(
        run_id: RunId,
        owner: &str,
        lease_epoch: u64,
        fencing_token: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            run_id,
            owner: DurableLabel::new(owner)?,
            lease_epoch,
            fencing_token: DurableLabel::new(fencing_token)?,
            expires_at,
        })
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn owner(&self) -> &str {
        self.owner.as_str()
    }
    pub fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }
    pub fn fencing_token(&self) -> &str {
        self.fencing_token.as_str()
    }
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    pub fn fence(&self) -> Result<FencedSchedulerRunCommand, RepositoryError> {
        FencedSchedulerRunCommand::new(
            self.run_id.clone(),
            self.owner(),
            self.lease_epoch,
            self.fencing_token(),
        )
    }
}

#[async_trait]
pub trait ControlDurableRepository:
    super::DurableRepository + super::ProjectionDurableRepository
{
    async fn create_child_scope(
        &self,
        transition_key: TransitionKey,
        command: CreateChildScopeCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
    async fn close_scope_admission(
        &self,
        transition_key: TransitionKey,
        command: CloseScopeAdmissionCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
    async fn settle_scope(
        &self,
        transition_key: TransitionKey,
        command: SettleScopeCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
    async fn emit_control_token(
        &self,
        transition_key: TransitionKey,
        command: EmitControlTokenCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
    async fn consume_control_token(
        &self,
        transition_key: TransitionKey,
        command: ConsumeControlTokenCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
    async fn revoke_control_token(
        &self,
        transition_key: TransitionKey,
        command: RevokeControlTokenCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
    async fn create_fork(
        &self,
        transition_key: TransitionKey,
        command: CreateForkCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
    async fn record_join_arrival(
        &self,
        transition_key: TransitionKey,
        command: RecordJoinArrivalCommand,
    ) -> Result<TransitionOutcome<JoinArrivalReceipt>, RepositoryError>;
    async fn create_reuse_candidate(
        &self,
        transition_key: TransitionKey,
        command: CreateReuseCandidateCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
    async fn reject_reuse_candidate(
        &self,
        transition_key: TransitionKey,
        command: RejectReuseCandidateCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
    async fn materialize_reuse_candidate(
        &self,
        transition_key: TransitionKey,
        command: MaterializeReuseCandidateCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
}

#[async_trait]
pub trait SchedulerLeaseRepository: super::DurableRepository {
    async fn claim_scheduler_run(
        &self,
        transition_key: TransitionKey,
        command: ClaimSchedulerRunCommand,
    ) -> Result<TransitionOutcome<SchedulerRunLease>, RepositoryError>;
    async fn heartbeat_scheduler_run(
        &self,
        transition_key: TransitionKey,
        command: HeartbeatSchedulerRunCommand,
    ) -> Result<TransitionOutcome<SchedulerRunLease>, RepositoryError>;
    async fn release_scheduler_run(
        &self,
        transition_key: TransitionKey,
        command: FencedSchedulerRunCommand,
    ) -> Result<TransitionOutcome<ControlCommitReceipt>, RepositoryError>;
}

/// Cross-crate construction and codec hooks for storage adapters.
#[doc(hidden)]
pub mod adapter {
    use super::*;

    pub fn scope_storage(
        scope: &ScopeInstance,
    ) -> Result<(&str, String, &'static str, ExecutionScopeKind), RepositoryError> {
        let storage = super::scope_storage(scope)?;
        Ok((
            storage.static_scope_id,
            storage.stable_dynamic_key,
            storage.scope_kind,
            storage.event_kind,
        ))
    }

    pub fn event_control_frames(provenance: &ControlTokenProvenance) -> Vec<ExecutionControlFrame> {
        super::event_control_frames(provenance)
    }

    pub const fn join_mode_str(mode: JoinMode) -> &'static str {
        super::join_mode_str(mode)
    }

    pub const fn settlement_str(value: ExecutionLegSettlementClass) -> &'static str {
        super::settlement_str(value)
    }

    pub fn parse_settlement(value: &str) -> Result<ExecutionLegSettlementClass, RepositoryError> {
        super::parse_settlement(value)
    }

    pub fn control_commit_receipt(
        event_seq: u64,
        event_id: String,
        projection_version: u64,
    ) -> ControlCommitReceipt {
        ControlCommitReceipt::new(event_seq, event_id, projection_version)
    }

    pub fn join_arrival_receipt(
        commit: ControlCommitReceipt,
        authority: JoinBarrierAuthority,
    ) -> JoinArrivalReceipt {
        JoinArrivalReceipt::new(commit, authority)
    }

    pub fn reuse_matches_admission_contract(
        compatibility: &ReuseCompatibility,
        contract: &insight_engine::scheduler::ReuseAdmissionContract,
    ) -> bool {
        compatibility.matches_admission_contract(contract)
    }

    pub fn durable_reuse_provenance(
        command: &CreateReuseCandidateCommand,
    ) -> Result<serde_json::Value, RepositoryError> {
        let provenance = super::DurableReuseProvenance::from_command(command);
        serde_json::to_value(provenance).map_err(|_| RepositoryError::invalid_data())
    }

    pub fn decode_durable_reuse_provenance(
        value: &serde_json::Value,
    ) -> Result<(ControlTokenProvenance, BTreeSet<ActivationId>), RepositoryError> {
        let provenance = serde_json::from_value::<super::DurableReuseProvenance>(value.clone())
            .map_err(|_| RepositoryError::invalid_data())?;
        Ok((
            provenance.control().clone(),
            provenance.source_data_dependencies().clone(),
        ))
    }

    pub fn scheduler_run_lease(
        run_id: RunId,
        owner: &str,
        lease_epoch: u64,
        fencing_token: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<SchedulerRunLease, RepositoryError> {
        SchedulerRunLease::new(run_id, owner, lease_epoch, fencing_token, expires_at)
    }
}
