use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

#[cfg(test)]
use super::aggregate::TERMINAL_PROOF_ATTEMPT_LIVE;
use super::{
    aggregate::{TerminalActivationProof, TerminalActivationResult, TERMINAL_PROOF_NOT_TERMINAL},
    ActivationId, ActivationLifecycle, ActivationTerminationReason, ContentHash, ControlTokenId,
    ForkGroupId, IntentHash, InternalFailureKind, InternalFailureSummary, LegId, ModelError,
    PortId, RunId, ScopeInstanceId, TransitionKey, TransitionOutcome, ValueRef,
};

pub const CONTROL_RUN_MISMATCH: &str = "ENGINE_CONTROL_RUN_MISMATCH";
pub const CONTROL_SCOPE_MISMATCH: &str = "ENGINE_CONTROL_SCOPE_MISMATCH";
pub const CONTROL_HANDLE_FOREIGN: &str = "ENGINE_CONTROL_HANDLE_FOREIGN";
pub const CONTROL_TOKEN_UNKNOWN: &str = "ENGINE_CONTROL_TOKEN_UNKNOWN";
pub const CONTROL_PROVENANCE_INVALID: &str = "ENGINE_CONTROL_PROVENANCE_INVALID";
pub const CONTROL_SNAPSHOT_INVALID: &str = "ENGINE_CONTROL_SNAPSHOT_INVALID";
pub const CONTROL_INTENT_CONFLICT: &str = "ENGINE_CONTROL_INTENT_CONFLICT";
pub const ACTIVATION_CONTROL_MODE_INVALID: &str = "ENGINE_ACTIVATION_CONTROL_MODE_INVALID";
pub const ACTIVATION_CONTROL_PORT_INVALID: &str = "ENGINE_ACTIVATION_CONTROL_PORT_INVALID";
pub const ACTIVATION_CONTROL_NOT_ADMITTED: &str = "ENGINE_ACTIVATION_CONTROL_NOT_ADMITTED";
pub const ACTIVATION_CONTROL_PROOF_MISMATCH: &str = "ENGINE_ACTIVATION_CONTROL_PROOF_MISMATCH";
pub const BRANCH_DECISION_CONFLICT: &str = "ENGINE_BRANCH_DECISION_CONFLICT";
pub const BRANCH_NOT_DECIDED: &str = "ENGINE_BRANCH_NOT_DECIDED";
pub const MERGE_CONFIGURATION_INVALID: &str = "ENGINE_MERGE_CONFIGURATION_INVALID";
pub const MERGE_CORRELATION_MISMATCH: &str = "ENGINE_MERGE_CORRELATION_MISMATCH";
pub const MERGE_PORT_INVALID: &str = "ENGINE_MERGE_PORT_INVALID";
pub const MERGE_ARRIVAL_CONFLICT: &str = "ENGINE_MERGE_ARRIVAL_CONFLICT";
pub const MERGE_OUTPUT_NOT_READY: &str = "ENGINE_MERGE_OUTPUT_NOT_READY";
pub const FORK_MEMBERS_INVALID: &str = "ENGINE_FORK_MEMBERS_INVALID";
pub const FORK_LEG_DUPLICATE: &str = "ENGINE_FORK_LEG_DUPLICATE";
pub const FORK_LEG_UNKNOWN: &str = "ENGINE_FORK_LEG_UNKNOWN";
pub const FORK_ATOMIC_ADMISSION_CONFLICT: &str = "ENGINE_FORK_ATOMIC_ADMISSION_CONFLICT";
pub const JOIN_GROUP_MISMATCH: &str = "ENGINE_JOIN_GROUP_MISMATCH";
pub const JOIN_LEG_UNKNOWN: &str = "ENGINE_JOIN_LEG_UNKNOWN";
pub const JOIN_SCOPE_MISMATCH: &str = "ENGINE_JOIN_SCOPE_MISMATCH";
pub const JOIN_PROOF_MISMATCH: &str = "ENGINE_JOIN_PROOF_MISMATCH";
pub const JOIN_ARRIVAL_CONFLICT: &str = "ENGINE_JOIN_ARRIVAL_CONFLICT";
pub const JOIN_TOKEN_REUSED: &str = "ENGINE_JOIN_TOKEN_REUSED";
pub const JOIN_OUTER_CORRELATION_MISMATCH: &str = "ENGINE_JOIN_OUTER_CORRELATION_MISMATCH";
pub const JOIN_OUTPUT_NOT_READY: &str = "ENGINE_JOIN_OUTPUT_NOT_READY";
pub const SCOPE_ADMISSION_CLOSED: &str = "ENGINE_SCOPE_ADMISSION_CLOSED";
pub const SCOPE_CHILD_CONFLICT: &str = "ENGINE_SCOPE_CHILD_CONFLICT";
pub const SCOPE_CHILD_UNKNOWN: &str = "ENGINE_SCOPE_CHILD_UNKNOWN";
pub const SCOPE_COMPLETION_BLOCKED: &str = "ENGINE_SCOPE_COMPLETION_BLOCKED";
pub const SCOPE_STATE_INVALID: &str = "ENGINE_SCOPE_STATE_INVALID";
pub const SETTLEMENT_PROOF_INVALID: &str = "ENGINE_SETTLEMENT_PROOF_INVALID";

/// Result of a small aggregate transition that does not mint or consume a
/// durable token. Token transitions use `TransitionOutcome` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    Duplicate,
}

/// One unresolved exclusive-branch frame. Its fields are deliberately
/// read-only: only control aggregates can add or remove frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchCorrelation {
    run_id: RunId,
    branch_activation_id: ActivationId,
    selected_port: PortId,
    scope_instance_id: ScopeInstanceId,
}

impl BranchCorrelation {
    pub fn new(
        run_id: RunId,
        branch_activation_id: ActivationId,
        selected_port: PortId,
        scope_instance_id: ScopeInstanceId,
    ) -> Self {
        Self {
            run_id,
            branch_activation_id,
            selected_port,
            scope_instance_id,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn branch_activation_id(&self) -> &ActivationId {
        &self.branch_activation_id
    }

    pub fn selected_port(&self) -> &PortId {
        &self.selected_port
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }
}

/// One unresolved structured-concurrency leg. Parent and child scope are both
/// frozen so a Join can restore the parent without trusting a caller value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkLegCorrelation {
    run_id: RunId,
    fork_activation_id: ActivationId,
    fork_group_id: ForkGroupId,
    leg_id: LegId,
    parent_scope_instance_id: ScopeInstanceId,
    scope_instance_id: ScopeInstanceId,
    child_activation_id: ActivationId,
}

impl ForkLegCorrelation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: RunId,
        fork_activation_id: ActivationId,
        fork_group_id: ForkGroupId,
        leg_id: LegId,
        parent_scope_instance_id: ScopeInstanceId,
        scope_instance_id: ScopeInstanceId,
        child_activation_id: ActivationId,
    ) -> Result<Self, ModelError> {
        if parent_scope_instance_id == scope_instance_id {
            return Err(ModelError::new(
                CONTROL_PROVENANCE_INVALID,
                "fork leg parent and child scope must be distinct",
            ));
        }
        Ok(Self {
            run_id,
            fork_activation_id,
            fork_group_id,
            leg_id,
            parent_scope_instance_id,
            scope_instance_id,
            child_activation_id,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn fork_activation_id(&self) -> &ActivationId {
        &self.fork_activation_id
    }

    pub fn fork_group_id(&self) -> &ForkGroupId {
        &self.fork_group_id
    }

    pub fn leg_id(&self) -> &LegId {
        &self.leg_id
    }

    pub fn parent_scope_instance_id(&self) -> &ScopeInstanceId {
        &self.parent_scope_instance_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn child_activation_id(&self) -> &ActivationId {
        &self.child_activation_id
    }
}

/// A single LIFO provenance stack. Branch and Fork frames can no longer be
/// searched or popped independently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "frame", rename_all = "snake_case")]
pub enum ControlFrame {
    Branch(BranchCorrelation),
    ForkLeg(ForkLegCorrelation),
}

/// Durable first-winner identity for one logical token emission.
///
/// The slot is deliberately independent from a caller-supplied retry key. A
/// reconstructed aggregate may therefore retry with another request key, but
/// it still cannot mint a second token for the same logical output. Fork legs
/// carry their frozen group and leg identities, so every declared leg owns a
/// stable, distinct slot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ControlEmissionSlot {
    ActivationOutput,
    BranchDecision,
    MergeOutput,
    JoinOutput,
    ForkLeg {
        fork_group_id: ForkGroupId,
        leg_id: LegId,
    },
}

impl ControlEmissionSlot {
    /// Stable text persisted by SQL projections. Length prefixes avoid an
    /// ambiguous encoding even when opaque IDs themselves contain `:`.
    pub fn storage_key(&self) -> String {
        match self {
            Self::ActivationOutput => "activation_output".to_owned(),
            Self::BranchDecision => "branch_decision".to_owned(),
            Self::MergeOutput => "merge_output".to_owned(),
            Self::JoinOutput => "join_output".to_owned(),
            Self::ForkLeg {
                fork_group_id,
                leg_id,
            } => format!(
                "fork_leg:{}:{}:{}:{}",
                fork_group_id.as_str().len(),
                fork_group_id,
                leg_id.as_str().len(),
                leg_id
            ),
        }
    }

    /// Canonical transition identity available to repositories/schedulers.
    /// Correctness does not rely on callers using it: the ledger separately
    /// enforces the logical slot as the durable first-winner key.
    pub fn transition_key(
        &self,
        run_id: &RunId,
        source_activation_id: &ActivationId,
    ) -> Result<TransitionKey, ModelError> {
        let storage_key = self.storage_key();
        TransitionKey::derive(
            "control.emit.v1",
            &[
                run_id.as_str(),
                source_activation_id.as_str(),
                storage_key.as_str(),
            ],
        )
    }
}

impl ControlFrame {
    pub fn as_branch(&self) -> Option<&BranchCorrelation> {
        match self {
            Self::Branch(frame) => Some(frame),
            Self::ForkLeg(_) => None,
        }
    }

    pub fn as_fork_leg(&self) -> Option<&ForkLegCorrelation> {
        match self {
            Self::ForkLeg(frame) => Some(frame),
            Self::Branch(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ControlTokenProvenance {
    run_id: RunId,
    source_activation_id: ActivationId,
    source_port: PortId,
    emission_slot: ControlEmissionSlot,
    scope_instance_id: ScopeInstanceId,
    frames: Vec<ControlFrame>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlTokenProvenanceWire {
    run_id: RunId,
    source_activation_id: ActivationId,
    source_port: PortId,
    emission_slot: ControlEmissionSlot,
    scope_instance_id: ScopeInstanceId,
    #[serde(default)]
    frames: Vec<ControlFrame>,
}

impl<'de> Deserialize<'de> for ControlTokenProvenance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ControlTokenProvenanceWire::deserialize(deserializer)?;
        Self::new(
            wire.run_id,
            wire.source_activation_id,
            wire.source_port,
            wire.emission_slot,
            wire.scope_instance_id,
            wire.frames,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl ControlTokenProvenance {
    pub fn new(
        run_id: RunId,
        source_activation_id: ActivationId,
        source_port: PortId,
        emission_slot: ControlEmissionSlot,
        scope_instance_id: ScopeInstanceId,
        frames: Vec<ControlFrame>,
    ) -> Result<Self, ModelError> {
        validate_frame_scopes(&scope_instance_id, &frames)?;
        if frames.iter().any(|frame| match frame {
            ControlFrame::Branch(frame) => frame.run_id != run_id,
            ControlFrame::ForkLeg(frame) => frame.run_id != run_id,
        }) {
            return Err(run_mismatch(
                "control provenance contains a frame from another run",
            ));
        }
        if let ControlEmissionSlot::ForkLeg {
            fork_group_id,
            leg_id,
        } = &emission_slot
        {
            let fork = frames
                .last()
                .and_then(ControlFrame::as_fork_leg)
                .ok_or_else(|| {
                    ModelError::new(
                        CONTROL_PROVENANCE_INVALID,
                        "fork-leg emission slot requires its fork frame at the true top",
                    )
                })?;
            if &fork.fork_group_id != fork_group_id
                || &fork.leg_id != leg_id
                || fork.fork_activation_id != source_activation_id
            {
                return Err(ModelError::new(
                    CONTROL_PROVENANCE_INVALID,
                    "fork-leg emission slot does not match its authoritative fork frame",
                ));
            }
        }
        Ok(Self {
            run_id,
            source_activation_id,
            source_port,
            emission_slot,
            scope_instance_id,
            frames,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn source_activation_id(&self) -> &ActivationId {
        &self.source_activation_id
    }

    pub fn source_port(&self) -> &PortId {
        &self.source_port
    }

    pub fn emission_slot(&self) -> &ControlEmissionSlot {
        &self.emission_slot
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn frames(&self) -> &[ControlFrame] {
        &self.frames
    }

    pub fn top_frame(&self) -> Option<&ControlFrame> {
        self.frames.last()
    }
}

fn validate_frame_scopes(
    final_scope: &ScopeInstanceId,
    frames: &[ControlFrame],
) -> Result<(), ModelError> {
    let mut current_scope: Option<&ScopeInstanceId> = None;
    for frame in frames {
        match frame {
            ControlFrame::Branch(branch) => match current_scope {
                None => current_scope = Some(&branch.scope_instance_id),
                Some(current) if current == &branch.scope_instance_id => {}
                Some(_) => {
                    return Err(ModelError::new(
                        CONTROL_PROVENANCE_INVALID,
                        "branch frame scope does not match the active provenance scope",
                    ));
                }
            },
            ControlFrame::ForkLeg(fork) => {
                match current_scope {
                    None => {}
                    Some(current) if current == &fork.parent_scope_instance_id => {}
                    Some(_) => {
                        return Err(ModelError::new(
                            CONTROL_PROVENANCE_INVALID,
                            "fork frame parent does not match the active provenance scope",
                        ));
                    }
                }
                current_scope = Some(&fork.scope_instance_id);
            }
        }
    }
    if current_scope.is_some_and(|current| current != final_scope) {
        return Err(ModelError::new(
            CONTROL_PROVENANCE_INVALID,
            "control provenance frames do not end in the recorded current scope",
        ));
    }
    Ok(())
}

/// Serializable storage row. It is inert data and cannot be passed to Merge or
/// Join; execution requires an `OwnedControlToken` minted or loaded by a
/// `ControlLedger`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersistedControlTokenRow {
    run_id: RunId,
    token_id: ControlTokenId,
    provenance: ControlTokenProvenance,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedControlTokenRowWire {
    run_id: RunId,
    token_id: ControlTokenId,
    provenance: ControlTokenProvenance,
}

impl<'de> Deserialize<'de> for PersistedControlTokenRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PersistedControlTokenRowWire::deserialize(deserializer)?;
        if wire.run_id != wire.provenance.run_id {
            return Err(serde::de::Error::custom(run_mismatch(
                "persisted token row and provenance belong to different runs",
            )));
        }
        Ok(Self {
            run_id: wire.run_id,
            token_id: wire.token_id,
            provenance: wire.provenance,
        })
    }
}

impl PersistedControlTokenRow {
    fn minted(run_id: RunId, token_id: ControlTokenId, provenance: ControlTokenProvenance) -> Self {
        debug_assert_eq!(run_id, provenance.run_id);
        Self {
            run_id,
            token_id,
            provenance,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }

    pub fn provenance(&self) -> &ControlTokenProvenance {
        &self.provenance
    }
}

/// Capability handle for a row owned by one live ledger. It intentionally has
/// no Serialize or Deserialize implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedControlToken {
    ledger_authority: Uuid,
    run_id: RunId,
    token_id: ControlTokenId,
}

impl OwnedControlToken {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }
}

/// Temporary compatibility name for the newly owned handle. The old mutable
/// token API and constructors are intentionally gone.
pub type ControlToken = OwnedControlToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ControlConsumerKind {
    Activation,
    Branch,
    Merge,
    Fork,
    Join,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlConsumer {
    kind: ControlConsumerKind,
    activation_id: ActivationId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenConsumption {
    run_id: RunId,
    token_id: ControlTokenId,
    consumer: ControlConsumer,
    transition_key: TransitionKey,
    intent_hash: IntentHash,
}

impl TokenConsumption {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn token_id(&self) -> &ControlTokenId {
        &self.token_id
    }

    pub fn consumer_kind(&self) -> &'static str {
        match self.consumer.kind {
            ControlConsumerKind::Activation => "activation",
            ControlConsumerKind::Branch => "branch",
            ControlConsumerKind::Merge => "merge",
            ControlConsumerKind::Fork => "fork",
            ControlConsumerKind::Join => "join",
        }
    }

    pub fn consumer_activation_id(&self) -> &ActivationId {
        &self.consumer.activation_id
    }

    pub fn transition_key(&self) -> &TransitionKey {
        &self.transition_key
    }

    pub fn intent_hash(&self) -> &IntentHash {
        &self.intent_hash
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
enum ControlTransitionResult {
    Token {
        emitter_activation_id: ActivationId,
        emission_slot: ControlEmissionSlot,
        row: Box<PersistedControlTokenRow>,
    },
    ActivationAdmitted(Box<ActivationAdmission>),
    ForkCreated {
        fork_activation_id: ActivationId,
        fork_group_id: ForkGroupId,
        tokens: Vec<(LegId, PersistedControlTokenRow)>,
    },
    JoinArrived(Box<JoinArrival>),
}

impl ControlTransitionResult {
    fn emits(&self, token_id: &ControlTokenId) -> bool {
        match self {
            Self::Token { row, .. } => row.token_id() == token_id,
            Self::ForkCreated { tokens, .. } => {
                tokens.iter().any(|(_, row)| row.token_id() == token_id)
            }
            Self::ActivationAdmitted(_) | Self::JoinArrived(_) => false,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
enum ControlTransitionResultWire {
    Token {
        emitter_activation_id: ActivationId,
        emission_slot: ControlEmissionSlot,
        row: Box<PersistedControlTokenRow>,
    },
    ActivationAdmitted(Box<ActivationAdmission>),
    ForkCreated {
        fork_activation_id: ActivationId,
        fork_group_id: ForkGroupId,
        tokens: Vec<(LegId, PersistedControlTokenRow)>,
    },
    JoinArrived(Box<JoinArrivalWire>),
}

impl<'de> Deserialize<'de> for ControlTransitionResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match ControlTransitionResultWire::deserialize(deserializer)? {
            ControlTransitionResultWire::Token {
                emitter_activation_id,
                emission_slot,
                row,
            } => Ok(Self::Token {
                emitter_activation_id,
                emission_slot,
                row,
            }),
            ControlTransitionResultWire::ActivationAdmitted(admission) => {
                Ok(Self::ActivationAdmitted(admission))
            }
            ControlTransitionResultWire::ForkCreated {
                fork_activation_id,
                fork_group_id,
                tokens,
            } => Ok(Self::ForkCreated {
                fork_activation_id,
                fork_group_id,
                tokens,
            }),
            ControlTransitionResultWire::JoinArrived(arrival) => {
                Ok(Self::JoinArrived(Box::new((*arrival).into_trusted())))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedControlTransitionRow {
    run_id: RunId,
    transition_key: TransitionKey,
    intent_hash: IntentHash,
    consumed_token_ids: Vec<ControlTokenId>,
    result: ControlTransitionResult,
}

impl PersistedControlTransitionRow {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn transition_key(&self) -> &TransitionKey {
        &self.transition_key
    }

    pub fn intent_hash(&self) -> &IntentHash {
        &self.intent_hash
    }

    pub fn consumed_token_ids(&self) -> &[ControlTokenId] {
        &self.consumed_token_ids
    }
}

/// Complete durable control authority snapshot. Deserializing it yields inert
/// data; only the crate-private validated restore path can mint new handles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedControlLedgerSnapshot {
    run_id: RunId,
    token_rows: Vec<PersistedControlTokenRow>,
    consumptions: Vec<TokenConsumption>,
    transitions: Vec<PersistedControlTransitionRow>,
}

impl PersistedControlLedgerSnapshot {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn token_rows(&self) -> &[PersistedControlTokenRow] {
        &self.token_rows
    }

    pub fn consumptions(&self) -> &[TokenConsumption] {
        &self.consumptions
    }

    pub fn transitions(&self) -> &[PersistedControlTransitionRow] {
        &self.transitions
    }
}

enum Replay {
    Missing,
    Exact(ControlTransitionResult),
}

/// Run-scoped authority for token rows, idempotency records and the
/// single-consumer first-winner index.
#[derive(Debug)]
pub struct ControlLedger {
    run_id: RunId,
    authority: Uuid,
    rows: BTreeMap<ControlTokenId, PersistedControlTokenRow>,
    emission_slots: BTreeMap<(ActivationId, ControlEmissionSlot), ControlTokenId>,
    consumptions: BTreeMap<ControlTokenId, TokenConsumption>,
    transitions: BTreeMap<TransitionKey, PersistedControlTransitionRow>,
}

impl ControlLedger {
    pub fn new(run_id: RunId) -> Self {
        Self {
            run_id,
            authority: Uuid::new_v4(),
            rows: BTreeMap::new(),
            emission_slots: BTreeMap::new(),
            consumptions: BTreeMap::new(),
            transitions: BTreeMap::new(),
        }
    }

    pub fn snapshot(&self) -> PersistedControlLedgerSnapshot {
        PersistedControlLedgerSnapshot {
            run_id: self.run_id.clone(),
            token_rows: self.rows.values().cloned().collect(),
            consumptions: self.consumptions.values().cloned().collect(),
            transitions: self.transitions.values().cloned().collect(),
        }
    }

    /// Internal transactional staging copy. `ControlLedger` deliberately does
    /// not implement `Clone`, because duplicating a live authority would allow
    /// two divergent ledgers to accept the same owned handle.
    fn staged(&self) -> Self {
        Self {
            run_id: self.run_id.clone(),
            authority: self.authority,
            rows: self.rows.clone(),
            emission_slots: self.emission_slots.clone(),
            consumptions: self.consumptions.clone(),
            transitions: self.transitions.clone(),
        }
    }

    /// Restores only a complete, validated durable snapshot. This is
    /// crate-private so arbitrary deserialized rows cannot be upgraded into an
    /// execution capability by external callers.
    #[allow(dead_code)] // repository restore wiring lands after the model contract
    pub(crate) fn from_snapshot(
        snapshot: PersistedControlLedgerSnapshot,
    ) -> Result<Self, ModelError> {
        let mut ledger = Self::new(snapshot.run_id.clone());
        for row in snapshot.token_rows {
            require_run(
                &ledger.run_id,
                &row.run_id,
                "persisted token row belongs to another run",
            )?;
            let emission_identity = (
                row.provenance.source_activation_id.clone(),
                row.provenance.emission_slot.clone(),
            );
            if ledger
                .emission_slots
                .insert(emission_identity, row.token_id.clone())
                .is_some()
            {
                return Err(snapshot_invalid(
                    "control snapshot contains two tokens for one logical emission slot",
                ));
            }
            if ledger.rows.insert(row.token_id.clone(), row).is_some() {
                return Err(snapshot_invalid(
                    "control snapshot contains a duplicate token identifier",
                ));
            }
        }

        for transition in snapshot.transitions {
            require_run(
                &ledger.run_id,
                &transition.run_id,
                "persisted control transition belongs to another run",
            )?;
            if ledger
                .transitions
                .insert(transition.transition_key.clone(), transition)
                .is_some()
            {
                return Err(snapshot_invalid(
                    "control snapshot contains a duplicate transition key",
                ));
            }
        }

        for consumption in snapshot.consumptions {
            require_run(
                &ledger.run_id,
                &consumption.run_id,
                "persisted token consumption belongs to another run",
            )?;
            if !ledger.rows.contains_key(&consumption.token_id) {
                return Err(snapshot_invalid(
                    "token consumption references a missing token row",
                ));
            }
            let transition = ledger
                .transitions
                .get(&consumption.transition_key)
                .ok_or_else(|| {
                    snapshot_invalid("token consumption references a missing transition")
                })?;
            if transition.intent_hash != consumption.intent_hash {
                return Err(snapshot_invalid(
                    "token consumption intent differs from its transition record",
                ));
            }
            if ledger
                .consumptions
                .insert(consumption.token_id.clone(), consumption)
                .is_some()
            {
                return Err(snapshot_invalid(
                    "control snapshot contains two consumers for one token",
                ));
            }
        }

        ledger.validate_snapshot_references()?;
        Ok(ledger)
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn persisted_rows(&self) -> impl ExactSizeIterator<Item = &PersistedControlTokenRow> {
        self.rows.values()
    }

    pub fn load(
        &self,
        run_id: &RunId,
        token_id: &ControlTokenId,
    ) -> Result<OwnedControlToken, ModelError> {
        if run_id != &self.run_id {
            return Err(run_mismatch("token load requested another run"));
        }
        if !self.rows.contains_key(token_id) {
            return Err(ModelError::new(
                CONTROL_TOKEN_UNKNOWN,
                "control token does not exist in the authoritative ledger",
            ));
        }
        Ok(self.handle(token_id.clone()))
    }

    pub fn row(&self, token: &OwnedControlToken) -> Result<&PersistedControlTokenRow, ModelError> {
        self.resolve(token)
    }

    pub fn consumption(
        &self,
        token: &OwnedControlToken,
    ) -> Result<Option<&TokenConsumption>, ModelError> {
        let row = self.resolve(token)?;
        Ok(self.consumptions.get(&row.token_id))
    }

    fn handle(&self, token_id: ControlTokenId) -> OwnedControlToken {
        OwnedControlToken {
            ledger_authority: self.authority,
            run_id: self.run_id.clone(),
            token_id,
        }
    }

    fn resolve(&self, token: &OwnedControlToken) -> Result<&PersistedControlTokenRow, ModelError> {
        if token.run_id != self.run_id {
            return Err(run_mismatch("owned control token belongs to another run"));
        }
        if token.ledger_authority != self.authority {
            return Err(ModelError::new(
                CONTROL_HANDLE_FOREIGN,
                "owned control token was minted by another ledger authority",
            ));
        }
        self.rows.get(&token.token_id).ok_or_else(|| {
            ModelError::new(
                CONTROL_TOKEN_UNKNOWN,
                "owned control token has no authoritative ledger row",
            )
        })
    }

    fn replay(&self, key: &TransitionKey, intent_hash: &IntentHash) -> Result<Replay, ModelError> {
        match self.transitions.get(key) {
            None => Ok(Replay::Missing),
            Some(record) if &record.intent_hash == intent_hash => {
                Ok(Replay::Exact(record.result.clone()))
            }
            Some(_) => Err(ModelError::new(
                CONTROL_INTENT_CONFLICT,
                "transition key is already bound to a different canonical intent",
            )),
        }
    }

    fn can_consume(&self, token_id: &ControlTokenId) -> bool {
        !self.consumptions.contains_key(token_id)
    }

    fn can_emit(&self, provenance: &ControlTokenProvenance) -> bool {
        !self.emission_slots.contains_key(&(
            provenance.source_activation_id.clone(),
            provenance.emission_slot.clone(),
        ))
    }

    fn prior_emission(
        &self,
        provenance: &ControlTokenProvenance,
    ) -> Option<(&PersistedControlTokenRow, &PersistedControlTransitionRow)> {
        let token_id = self.emission_slots.get(&(
            provenance.source_activation_id.clone(),
            provenance.emission_slot.clone(),
        ))?;
        let row = self.rows.get(token_id)?;
        let transition = self
            .transitions
            .values()
            .find(|transition| transition.result.emits(token_id))?;
        Some((row, transition))
    }

    fn prior_admission(
        &self,
        activation_id: &ActivationId,
    ) -> Option<(&ActivationAdmission, &PersistedControlTransitionRow)> {
        self.transitions.values().find_map(|transition| {
            let ControlTransitionResult::ActivationAdmitted(admission) = &transition.result else {
                return None;
            };
            (&admission.activation_id == activation_id).then_some((admission.as_ref(), transition))
        })
    }

    fn prior_join_arrival(
        &self,
        join_activation_id: &ActivationId,
        fork_group_id: &ForkGroupId,
        leg_id: &LegId,
    ) -> Option<(&JoinArrival, &PersistedControlTransitionRow)> {
        self.transitions.values().find_map(|transition| {
            let ControlTransitionResult::JoinArrived(arrival) = &transition.result else {
                return None;
            };
            (&arrival.join_activation_id == join_activation_id
                && &arrival.fork_group_id == fork_group_id
                && &arrival.leg_id == leg_id)
                .then_some((arrival.as_ref(), transition))
        })
    }

    fn consume(
        &mut self,
        token_id: ControlTokenId,
        consumer: ControlConsumer,
        transition_key: TransitionKey,
        intent_hash: IntentHash,
    ) {
        let previous = self.consumptions.insert(
            token_id.clone(),
            TokenConsumption {
                run_id: self.run_id.clone(),
                token_id,
                consumer,
                transition_key,
                intent_hash,
            },
        );
        debug_assert!(previous.is_none(), "consumption was preflighted");
    }

    fn mint(&mut self, provenance: ControlTokenProvenance) -> OwnedControlToken {
        debug_assert!(self.can_emit(&provenance), "emission slot was preflighted");
        let token_id = loop {
            let candidate = ControlTokenId::random();
            if !self.rows.contains_key(&candidate) {
                break candidate;
            }
        };
        let emission_identity = (
            provenance.source_activation_id.clone(),
            provenance.emission_slot.clone(),
        );
        self.rows.insert(
            token_id.clone(),
            PersistedControlTokenRow::minted(self.run_id.clone(), token_id.clone(), provenance),
        );
        let previous = self
            .emission_slots
            .insert(emission_identity, token_id.clone());
        debug_assert!(previous.is_none(), "emission slot was preflighted");
        self.handle(token_id)
    }

    fn emitted_row(&self, token: &OwnedControlToken) -> PersistedControlTokenRow {
        self.rows
            .get(&token.token_id)
            .expect("newly minted control token has an authoritative row")
            .clone()
    }

    fn record(
        &mut self,
        key: TransitionKey,
        intent_hash: IntentHash,
        result: ControlTransitionResult,
    ) {
        let consumed_token_ids = self
            .consumptions
            .values()
            .filter(|consumption| consumption.transition_key == key)
            .map(|consumption| consumption.token_id.clone())
            .collect();
        let previous = self.transitions.insert(
            key.clone(),
            PersistedControlTransitionRow {
                run_id: self.run_id.clone(),
                transition_key: key,
                intent_hash,
                consumed_token_ids,
                result,
            },
        );
        debug_assert!(previous.is_none(), "transition replay was preflighted");
    }

    #[allow(dead_code)] // reached by the crate-private restore entrypoint
    fn validate_snapshot_references(&self) -> Result<(), ModelError> {
        let mut emitted_token_ids = BTreeSet::new();
        let mut admitted_activation_ids = BTreeSet::new();
        let mut join_arrival_slots = BTreeSet::new();
        for transition in self.transitions.values() {
            for token_id in &transition.consumed_token_ids {
                let consumption = self.consumptions.get(token_id).ok_or_else(|| {
                    snapshot_invalid("transition lost its durable token consumption")
                })?;
                if consumption.transition_key != transition.transition_key
                    || consumption.intent_hash != transition.intent_hash
                {
                    return Err(snapshot_invalid(
                        "transition consumption does not match its canonical intent",
                    ));
                }
            }
            match &transition.result {
                ControlTransitionResult::ActivationAdmitted(admission) => {
                    if !admitted_activation_ids.insert(admission.activation_id.clone()) {
                        return Err(snapshot_invalid(
                            "control snapshot contains two admissions for one Activation",
                        ));
                    }
                    require_run(
                        &self.run_id,
                        &admission.run_id,
                        "Activation admission belongs to another run",
                    )?;
                    match &admission.input {
                        None if !transition.consumed_token_ids.is_empty() => {
                            return Err(snapshot_invalid(
                                "root Activation admission unexpectedly consumed a token",
                            ));
                        }
                        None => {
                            if admission.kind != ActivationAdmissionKind::RootEntry
                                || admission.input_port.is_some()
                                || !admission.inherited_frames.is_empty()
                            {
                                return Err(snapshot_invalid(
                                    "root Activation admission has invalid inherited control",
                                ));
                            }
                        }
                        Some(input) => {
                            if admission.kind != ActivationAdmissionKind::TokenGated
                                || admission.input_port.is_none()
                                || admission.inherited_frames != input.provenance.frames
                                || admission.scope_instance_id != input.provenance.scope_instance_id
                                || transition.consumed_token_ids.as_slice()
                                    != [input.token_id.clone()]
                            {
                                return Err(snapshot_invalid(
                                    "token-gated Activation admission has invalid inherited control",
                                ));
                            }
                            let authoritative =
                                self.rows.get(input.token_id()).ok_or_else(|| {
                                    snapshot_invalid(
                                        "Activation admission references a missing input token",
                                    )
                                })?;
                            if authoritative != input {
                                return Err(snapshot_invalid(
                                    "Activation admission input differs from its token row",
                                ));
                            }
                            let consumption =
                                self.consumptions.get(input.token_id()).ok_or_else(|| {
                                    snapshot_invalid(
                                        "Activation admission input has no durable consumption",
                                    )
                                })?;
                            if consumption.transition_key != transition.transition_key
                                || consumption.intent_hash != transition.intent_hash
                                || consumption.consumer.kind != ControlConsumerKind::Activation
                                || consumption.consumer.activation_id != admission.activation_id
                            {
                                return Err(snapshot_invalid(
                                    "Activation admission consumption does not match its transition",
                                ));
                            }
                        }
                    }
                }
                ControlTransitionResult::Token {
                    emitter_activation_id,
                    emission_slot,
                    row,
                } => {
                    if self.rows.get(row.token_id()) != Some(row.as_ref())
                        || !emitted_token_ids.insert(row.token_id.clone())
                        || &row.provenance.source_activation_id != emitter_activation_id
                        || &row.provenance.emission_slot != emission_slot
                    {
                        return Err(snapshot_invalid(
                            "token transition result differs from its unique authoritative token row",
                        ));
                    }
                }
                ControlTransitionResult::ForkCreated {
                    fork_activation_id,
                    fork_group_id,
                    tokens,
                } => {
                    if tokens.is_empty() {
                        return Err(snapshot_invalid(
                            "fork transition result contains no leg tokens",
                        ));
                    }
                    let mut legs = BTreeSet::new();
                    let mut token_ids = BTreeSet::new();
                    for (leg_id, row) in tokens {
                        let token_id = row.token_id();
                        if !legs.insert(leg_id.clone())
                            || !token_ids.insert(token_id.clone())
                            || !emitted_token_ids.insert(token_id.clone())
                            || self.rows.get(token_id) != Some(row)
                        {
                            return Err(snapshot_invalid(
                                "fork transition result has duplicate, missing, or altered leg tokens",
                            ));
                        }
                        let ControlEmissionSlot::ForkLeg {
                            fork_group_id: slot_group_id,
                            leg_id: slot_leg_id,
                        } = &row.provenance.emission_slot
                        else {
                            return Err(snapshot_invalid(
                                "fork transition result references a non-fork emission slot",
                            ));
                        };
                        let Some(frame) = row
                            .provenance
                            .top_frame()
                            .and_then(ControlFrame::as_fork_leg)
                        else {
                            return Err(snapshot_invalid(
                                "fork transition result lost its top fork frame",
                            ));
                        };
                        if slot_leg_id != leg_id
                            || &frame.leg_id != leg_id
                            || slot_group_id != fork_group_id
                            || &frame.fork_group_id != fork_group_id
                            || &row.provenance.source_activation_id != fork_activation_id
                            || frame.fork_activation_id != row.provenance.source_activation_id
                            || frame.scope_instance_id != row.provenance.scope_instance_id
                            || frame.run_id != self.run_id
                        {
                            return Err(snapshot_invalid(
                                "fork transition leg, slot, frame, and token provenance disagree",
                            ));
                        }
                    }
                }
                ControlTransitionResult::JoinArrived(arrival) => {
                    if !join_arrival_slots.insert((
                        arrival.join_activation_id.clone(),
                        arrival.fork_group_id.clone(),
                        arrival.leg_id.clone(),
                    )) {
                        return Err(snapshot_invalid(
                            "control snapshot contains two arrivals for one Join leg",
                        ));
                    }
                    require_run(
                        &self.run_id,
                        &arrival.run_id,
                        "join arrival transition belongs to another run",
                    )?;
                    let authoritative =
                        self.rows.get(arrival.input.token_id()).ok_or_else(|| {
                            snapshot_invalid("join arrival references a missing input token")
                        })?;
                    if authoritative != &arrival.input {
                        return Err(snapshot_invalid(
                            "join arrival input differs from the authoritative token row",
                        ));
                    }
                    let consumption =
                        self.consumptions
                            .get(arrival.input.token_id())
                            .ok_or_else(|| {
                                snapshot_invalid("join arrival input has no durable consumption")
                            })?;
                    if consumption.transition_key != transition.transition_key
                        || consumption.intent_hash != transition.intent_hash
                        || consumption.consumer.kind != ControlConsumerKind::Join
                        || consumption.consumer.activation_id != arrival.join_activation_id
                    {
                        return Err(snapshot_invalid(
                            "join arrival consumption does not match its transition",
                        ));
                    }
                    let Some(fork) = arrival
                        .input
                        .provenance
                        .top_frame()
                        .and_then(ControlFrame::as_fork_leg)
                    else {
                        return Err(snapshot_invalid(
                            "join arrival input lost its true-top fork correlation",
                        ));
                    };
                    let mut expected_outer_frames = arrival.input.provenance.frames.clone();
                    expected_outer_frames.pop();
                    if arrival.member.run_id != self.run_id
                        || arrival.member.leg_id != arrival.leg_id
                        || arrival.member.child_activation_id != arrival.child_activation_id
                        || arrival.member.scope_instance_id
                            != arrival.input.provenance.scope_instance_id
                        || arrival.outer_frames != expected_outer_frames
                        || arrival.fork_group_id != fork.fork_group_id
                        || arrival.fork_activation_id != fork.fork_activation_id
                        || arrival.parent_scope_instance_id != fork.parent_scope_instance_id
                        || arrival.leg_id != fork.leg_id
                        || arrival.child_activation_id != fork.child_activation_id
                        || arrival.member.scope_instance_id != fork.scope_instance_id
                        || !arrival.attempts_drained
                        || !arrival.terminal.lifecycle().is_terminal()
                        || !arrival.terminal.matches_settlement(&arrival.settlement)
                    {
                        return Err(snapshot_invalid(
                            "join arrival member, settlement, child, and fork provenance disagree",
                        ));
                    }
                }
            }
        }
        if emitted_token_ids != self.rows.keys().cloned().collect::<BTreeSet<_>>() {
            return Err(snapshot_invalid(
                "control snapshot contains a token row with no unique emission transition",
            ));
        }
        for consumption in self.consumptions.values() {
            let transition = self
                .transitions
                .get(&consumption.transition_key)
                .expect("consumption transition existence was validated during restore");
            if !transition
                .consumed_token_ids
                .contains(&consumption.token_id)
            {
                return Err(snapshot_invalid(
                    "token consumption is absent from its transition record",
                ));
            }
        }
        Ok(())
    }
}

fn run_mismatch(message: &'static str) -> ModelError {
    ModelError::new(CONTROL_RUN_MISMATCH, message)
}

#[allow(dead_code)] // reached by the crate-private restore entrypoint
fn snapshot_invalid(message: &'static str) -> ModelError {
    ModelError::new(CONTROL_SNAPSHOT_INVALID, message)
}

fn require_run(expected: &RunId, actual: &RunId, message: &'static str) -> Result<(), ModelError> {
    if expected != actual {
        return Err(run_mismatch(message));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ActivationAdmissionKind {
    TokenGated,
    RootEntry,
}

/// Durable ordinary-node admission. It freezes the authoritative input row and
/// inherited frame stack before the LLM/Action/HTTP/Tool Attempt starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationAdmission {
    run_id: RunId,
    activation_id: ActivationId,
    scope_instance_id: ScopeInstanceId,
    kind: ActivationAdmissionKind,
    input_port: Option<PortId>,
    input: Option<PersistedControlTokenRow>,
    inherited_frames: Vec<ControlFrame>,
}

impl ActivationAdmission {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn is_root_entry(&self) -> bool {
        self.kind == ActivationAdmissionKind::RootEntry
    }

    pub fn input_port(&self) -> Option<&PortId> {
        self.input_port.as_ref()
    }

    pub fn input(&self) -> Option<&PersistedControlTokenRow> {
        self.input.as_ref()
    }

    pub fn inherited_frames(&self) -> &[ControlFrame] {
        &self.inherited_frames
    }
}

#[derive(Debug, Clone)]
struct ActivationControlEmission {
    output_port: PortId,
    token_id: ControlTokenId,
}

/// Control gate for an ordinary executable Activation. Scheduler-native
/// Branch/Merge/Fork/Join/Return nodes keep their dedicated aggregates and do
/// not use this type as a generic token mint escape hatch.
#[derive(Debug)]
pub struct ActivationControlState {
    run_id: RunId,
    activation_id: ActivationId,
    scope_instance_id: ScopeInstanceId,
    admission_kind: ActivationAdmissionKind,
    input_ports: Vec<PortId>,
    input_port_set: BTreeSet<PortId>,
    output_ports: Vec<PortId>,
    output_port_set: BTreeSet<PortId>,
    admission: Option<ActivationAdmission>,
    emission: Option<ActivationControlEmission>,
}

#[derive(Serialize)]
struct ActivationAdmitIntent<'a> {
    operation: &'static str,
    run_id: &'a RunId,
    activation_id: &'a ActivationId,
    scope_instance_id: &'a ScopeInstanceId,
    admission_kind: ActivationAdmissionKind,
    input_ports: &'a [PortId],
    output_ports: &'a [PortId],
    input_port: Option<&'a PortId>,
    input: Option<&'a PersistedControlTokenRow>,
    inherited_frames: &'a [ControlFrame],
}

#[derive(Serialize)]
struct TerminalProofIntent<'a> {
    run_id: &'a RunId,
    scope_instance_id: &'a ScopeInstanceId,
    activation_id: &'a ActivationId,
    terminal: ActivationLifecycle,
    attempts_drained: bool,
}

#[derive(Serialize)]
struct ActivationEmitIntent<'a> {
    operation: &'static str,
    run_id: &'a RunId,
    activation_id: &'a ActivationId,
    scope_instance_id: &'a ScopeInstanceId,
    admission_kind: ActivationAdmissionKind,
    input_ports: &'a [PortId],
    output_ports: &'a [PortId],
    admission: &'a ActivationAdmission,
    proof: TerminalProofIntent<'a>,
    selected_output_port: &'a PortId,
    output: &'a ControlTokenProvenance,
}

impl ActivationControlState {
    pub fn new(
        run_id: RunId,
        activation_id: ActivationId,
        scope_instance_id: ScopeInstanceId,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
    ) -> Result<Self, ModelError> {
        Self::build(
            run_id,
            activation_id,
            scope_instance_id,
            ActivationAdmissionKind::TokenGated,
            input_ports,
            output_ports,
        )
    }

    pub fn new_entry(
        run_id: RunId,
        activation_id: ActivationId,
        scope_instance_id: ScopeInstanceId,
        output_ports: Vec<PortId>,
    ) -> Result<Self, ModelError> {
        Self::build(
            run_id,
            activation_id,
            scope_instance_id,
            ActivationAdmissionKind::RootEntry,
            Vec::new(),
            output_ports,
        )
    }

    fn build(
        run_id: RunId,
        activation_id: ActivationId,
        scope_instance_id: ScopeInstanceId,
        admission_kind: ActivationAdmissionKind,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
    ) -> Result<Self, ModelError> {
        let input_port_set = input_ports.iter().cloned().collect::<BTreeSet<_>>();
        let output_port_set = output_ports.iter().cloned().collect::<BTreeSet<_>>();
        let input_valid = match admission_kind {
            ActivationAdmissionKind::TokenGated => {
                !input_ports.is_empty() && input_port_set.len() == input_ports.len()
            }
            ActivationAdmissionKind::RootEntry => input_ports.is_empty(),
        };
        if !input_valid || output_ports.is_empty() || output_port_set.len() != output_ports.len() {
            return Err(ModelError::new(
                ACTIVATION_CONTROL_PORT_INVALID,
                "Activation control ports must be non-empty where required and unique",
            ));
        }
        Ok(Self {
            run_id,
            activation_id,
            scope_instance_id,
            admission_kind,
            input_ports,
            input_port_set,
            output_ports,
            output_port_set,
            admission: None,
            emission: None,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn input_ports(&self) -> &[PortId] {
        &self.input_ports
    }

    pub fn output_ports(&self) -> &[PortId] {
        &self.output_ports
    }

    pub fn admission(&self) -> Option<&ActivationAdmission> {
        self.admission.as_ref()
    }

    pub fn admit(
        &mut self,
        ledger: &mut ControlLedger,
        transition_key: TransitionKey,
        input_port: PortId,
        input_token: &OwnedControlToken,
    ) -> Result<TransitionOutcome<ActivationAdmission>, ModelError> {
        if self.admission_kind != ActivationAdmissionKind::TokenGated {
            return Err(ModelError::new(
                ACTIVATION_CONTROL_MODE_INVALID,
                "root entry Activation cannot consume an inbound control token",
            ));
        }
        require_run(
            &self.run_id,
            ledger.run_id(),
            "Activation control aggregate and ledger belong to different runs",
        )?;
        if !self.input_port_set.contains(&input_port) {
            return Err(ModelError::new(
                ACTIVATION_CONTROL_PORT_INVALID,
                "Activation admission selected an undeclared input port",
            ));
        }
        let input = ledger.resolve(input_token)?.clone();
        if input.provenance.scope_instance_id != self.scope_instance_id {
            return Err(ModelError::new(
                CONTROL_SCOPE_MISMATCH,
                "Activation input token belongs to another scope instance",
            ));
        }
        let admission = ActivationAdmission {
            run_id: self.run_id.clone(),
            activation_id: self.activation_id.clone(),
            scope_instance_id: self.scope_instance_id.clone(),
            kind: self.admission_kind,
            input_port: Some(input_port),
            input: Some(input.clone()),
            inherited_frames: input.provenance.frames.clone(),
        };
        self.apply_admission(ledger, transition_key, admission)
    }

    pub fn admit_root(
        &mut self,
        ledger: &mut ControlLedger,
        transition_key: TransitionKey,
    ) -> Result<TransitionOutcome<ActivationAdmission>, ModelError> {
        if self.admission_kind != ActivationAdmissionKind::RootEntry {
            return Err(ModelError::new(
                ACTIVATION_CONTROL_MODE_INVALID,
                "token-gated Activation cannot use root admission",
            ));
        }
        require_run(
            &self.run_id,
            ledger.run_id(),
            "root Activation control aggregate and ledger belong to different runs",
        )?;
        let admission = ActivationAdmission {
            run_id: self.run_id.clone(),
            activation_id: self.activation_id.clone(),
            scope_instance_id: self.scope_instance_id.clone(),
            kind: self.admission_kind,
            input_port: None,
            input: None,
            inherited_frames: Vec::new(),
        };
        self.apply_admission(ledger, transition_key, admission)
    }

    fn apply_admission(
        &mut self,
        ledger: &mut ControlLedger,
        transition_key: TransitionKey,
        admission: ActivationAdmission,
    ) -> Result<TransitionOutcome<ActivationAdmission>, ModelError> {
        let intent = ActivationAdmitIntent {
            operation: "activation.admit",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            scope_instance_id: &self.scope_instance_id,
            admission_kind: self.admission_kind,
            input_ports: &self.input_ports,
            output_ports: &self.output_ports,
            input_port: admission.input_port.as_ref(),
            input: admission.input.as_ref(),
            inherited_frames: &admission.inherited_frames,
        };
        let intent_hash = IntentHash::from_serializable(&intent)?;
        match ledger.replay(&transition_key, &intent_hash)? {
            Replay::Exact(ControlTransitionResult::ActivationAdmitted(authoritative)) => {
                let authoritative = *authoritative;
                if self
                    .admission
                    .as_ref()
                    .is_some_and(|stored| stored != &authoritative)
                {
                    return Err(snapshot_invalid(
                        "Activation admission state diverges from its exact replay record",
                    ));
                }
                self.admission = Some(authoritative.clone());
                return Ok(TransitionOutcome::ExactReplay { authoritative });
            }
            Replay::Exact(_) => {
                return Err(snapshot_invalid(
                    "transition replay result kind does not match the canonical command",
                ));
            }
            Replay::Missing => {}
        }
        if let Some((authoritative, authoritative_transition)) =
            ledger.prior_admission(&self.activation_id)
        {
            if authoritative != &admission || authoritative_transition.intent_hash != intent_hash {
                return Ok(TransitionOutcome::StateConflict);
            }
            self.admission = Some(authoritative.clone());
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: authoritative.clone(),
            });
        }
        if self.admission.is_some() {
            return Ok(TransitionOutcome::StateConflict);
        }
        if admission
            .input
            .as_ref()
            .is_some_and(|input| !ledger.can_consume(&input.token_id))
        {
            return Ok(TransitionOutcome::StateConflict);
        }

        let mut staged_ledger = ledger.staged();
        if let Some(input) = &admission.input {
            staged_ledger.consume(
                input.token_id.clone(),
                ControlConsumer {
                    kind: ControlConsumerKind::Activation,
                    activation_id: self.activation_id.clone(),
                },
                transition_key.clone(),
                intent_hash.clone(),
            );
        }
        staged_ledger.record(
            transition_key,
            intent_hash,
            ControlTransitionResult::ActivationAdmitted(Box::new(admission.clone())),
        );
        self.admission = Some(admission.clone());
        *ledger = staged_ledger;
        Ok(TransitionOutcome::Committed { result: admission })
    }

    pub fn emit(
        &mut self,
        ledger: &mut ControlLedger,
        transition_key: TransitionKey,
        output_port: PortId,
        proof: &TerminalActivationProof,
    ) -> Result<TransitionOutcome<OwnedControlToken>, ModelError> {
        require_run(
            &self.run_id,
            ledger.run_id(),
            "Activation control aggregate and ledger belong to different runs",
        )?;
        let admission = self.admission.as_ref().ok_or_else(|| {
            ModelError::new(
                ACTIVATION_CONTROL_NOT_ADMITTED,
                "Activation cannot emit before durable control admission",
            )
        })?;
        if !self.output_port_set.contains(&output_port) {
            return Err(ModelError::new(
                ACTIVATION_CONTROL_PORT_INVALID,
                "Activation emission selected an undeclared output port",
            ));
        }
        if proof.run_id() != &self.run_id
            || proof.scope_instance_id() != &self.scope_instance_id
            || proof.activation_id() != &self.activation_id
            || proof.terminal() != ActivationLifecycle::Succeeded
            || !proof.attempts_drained()
        {
            return Err(ModelError::new(
                ACTIVATION_CONTROL_PROOF_MISMATCH,
                "Activation emission requires its matching drained success proof",
            ));
        }
        let output = ControlTokenProvenance::new(
            self.run_id.clone(),
            self.activation_id.clone(),
            output_port.clone(),
            ControlEmissionSlot::ActivationOutput,
            self.scope_instance_id.clone(),
            admission.inherited_frames.clone(),
        )?;
        let intent = ActivationEmitIntent {
            operation: "activation.emit",
            run_id: &self.run_id,
            activation_id: &self.activation_id,
            scope_instance_id: &self.scope_instance_id,
            admission_kind: self.admission_kind,
            input_ports: &self.input_ports,
            output_ports: &self.output_ports,
            admission,
            proof: TerminalProofIntent {
                run_id: proof.run_id(),
                scope_instance_id: proof.scope_instance_id(),
                activation_id: proof.activation_id(),
                terminal: proof.terminal(),
                attempts_drained: proof.attempts_drained(),
            },
            selected_output_port: &output_port,
            output: &output,
        };
        let intent_hash = IntentHash::from_serializable(&intent)?;
        match ledger.replay(&transition_key, &intent_hash)? {
            Replay::Exact(ControlTransitionResult::Token {
                emitter_activation_id,
                emission_slot,
                row,
            }) => {
                let row = *row;
                let token_id = row.token_id.clone();
                if emitter_activation_id != self.activation_id
                    || emission_slot != ControlEmissionSlot::ActivationOutput
                    || ledger.rows.get(&token_id) != Some(&row)
                {
                    return Err(snapshot_invalid(
                        "Activation emission replay does not match its durable token row",
                    ));
                }
                if self.emission.as_ref().is_some_and(|emission| {
                    emission.output_port != output_port || emission.token_id != token_id
                }) {
                    return Err(snapshot_invalid(
                        "Activation emission state diverges from its exact replay record",
                    ));
                }
                self.emission = Some(ActivationControlEmission {
                    output_port,
                    token_id: token_id.clone(),
                });
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: ledger.handle(token_id),
                });
            }
            Replay::Exact(_) => {
                return Err(snapshot_invalid(
                    "transition replay result kind does not match the canonical command",
                ));
            }
            Replay::Missing => {}
        }
        if let Some((row, authoritative_transition)) = ledger.prior_emission(&output) {
            if row.provenance != output || authoritative_transition.intent_hash != intent_hash {
                return Ok(TransitionOutcome::StateConflict);
            }
            self.emission = Some(ActivationControlEmission {
                output_port,
                token_id: row.token_id.clone(),
            });
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: ledger.handle(row.token_id.clone()),
            });
        }
        if self.emission.is_some() || !ledger.can_emit(&output) {
            return Ok(TransitionOutcome::StateConflict);
        }

        let mut staged_ledger = ledger.staged();
        let token = staged_ledger.mint(output);
        let token_row = staged_ledger.emitted_row(&token);
        staged_ledger.record(
            transition_key.clone(),
            intent_hash.clone(),
            ControlTransitionResult::Token {
                emitter_activation_id: self.activation_id.clone(),
                emission_slot: ControlEmissionSlot::ActivationOutput,
                row: Box::new(token_row),
            },
        );
        self.emission = Some(ActivationControlEmission {
            output_port,
            token_id: token.token_id.clone(),
        });
        *ledger = staged_ledger;
        Ok(TransitionOutcome::Committed { result: token })
    }
}

#[derive(Debug, Clone)]
struct BranchEmission {
    token_id: ControlTokenId,
}

/// First-winner Branch aggregate. Selection and emission are one staged
/// ledger transition; there is no standalone mutable `select` operation.
#[derive(Debug)]
pub struct BranchDecision {
    run_id: RunId,
    branch_activation_id: ActivationId,
    scope_instance_id: ScopeInstanceId,
    outgoing_ports: Vec<PortId>,
    outgoing_port_set: BTreeSet<PortId>,
    selected_port: Option<PortId>,
    emission: Option<BranchEmission>,
}

#[derive(Serialize)]
struct BranchEmitIntent<'a> {
    operation: &'static str,
    run_id: &'a RunId,
    branch_activation_id: &'a ActivationId,
    scope_instance_id: &'a ScopeInstanceId,
    outgoing_ports: &'a [PortId],
    selected_port: &'a PortId,
    input: Option<&'a PersistedControlTokenRow>,
    output: &'a ControlTokenProvenance,
}

impl BranchDecision {
    pub fn new(
        run_id: RunId,
        branch_activation_id: ActivationId,
        scope_instance_id: ScopeInstanceId,
        outgoing_ports: Vec<PortId>,
    ) -> Result<Self, ModelError> {
        let outgoing_port_set = outgoing_ports.iter().cloned().collect::<BTreeSet<_>>();
        if outgoing_ports.is_empty() || outgoing_port_set.len() != outgoing_ports.len() {
            return Err(ModelError::new(
                MERGE_CONFIGURATION_INVALID,
                "branch must declare a non-empty unique outgoing port set",
            ));
        }
        Ok(Self {
            run_id,
            branch_activation_id,
            scope_instance_id,
            outgoing_ports,
            outgoing_port_set,
            selected_port: None,
            emission: None,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn branch_activation_id(&self) -> &ActivationId {
        &self.branch_activation_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn outgoing_ports(&self) -> &[PortId] {
        &self.outgoing_ports
    }

    pub fn selected_port(&self) -> Option<&PortId> {
        self.selected_port.as_ref()
    }

    pub fn select_and_emit(
        &mut self,
        ledger: &mut ControlLedger,
        transition_key: TransitionKey,
        selected_port: PortId,
        inherited: Option<&OwnedControlToken>,
    ) -> Result<TransitionOutcome<OwnedControlToken>, ModelError> {
        require_run(
            &self.run_id,
            ledger.run_id(),
            "branch aggregate and control ledger belong to different runs",
        )?;
        if !self.outgoing_port_set.contains(&selected_port) {
            return Err(ModelError::new(
                MERGE_PORT_INVALID,
                "branch selected a port outside its frozen outgoing set",
            ));
        }

        let input = inherited
            .map(|token| ledger.resolve(token).cloned())
            .transpose()?;
        if input
            .as_ref()
            .is_some_and(|row| row.provenance.scope_instance_id != self.scope_instance_id)
        {
            return Err(ModelError::new(
                CONTROL_SCOPE_MISMATCH,
                "branch input token belongs to another scope instance",
            ));
        }

        let mut frames = input
            .as_ref()
            .map(|row| row.provenance.frames.clone())
            .unwrap_or_default();
        frames.push(ControlFrame::Branch(BranchCorrelation {
            run_id: self.run_id.clone(),
            branch_activation_id: self.branch_activation_id.clone(),
            selected_port: selected_port.clone(),
            scope_instance_id: self.scope_instance_id.clone(),
        }));
        let output = ControlTokenProvenance::new(
            self.run_id.clone(),
            self.branch_activation_id.clone(),
            selected_port.clone(),
            ControlEmissionSlot::BranchDecision,
            self.scope_instance_id.clone(),
            frames,
        )?;
        let intent = BranchEmitIntent {
            operation: "branch.select_emit",
            run_id: &self.run_id,
            branch_activation_id: &self.branch_activation_id,
            scope_instance_id: &self.scope_instance_id,
            outgoing_ports: &self.outgoing_ports,
            selected_port: &selected_port,
            input: input.as_ref(),
            output: &output,
        };
        let intent_hash = IntentHash::from_serializable(&intent)?;

        match ledger.replay(&transition_key, &intent_hash)? {
            Replay::Exact(ControlTransitionResult::Token {
                emitter_activation_id,
                emission_slot,
                row,
            }) => {
                let row = *row;
                let token_id = row.token_id.clone();
                if emitter_activation_id != self.branch_activation_id
                    || emission_slot != ControlEmissionSlot::BranchDecision
                    || ledger.rows.get(&token_id) != Some(&row)
                {
                    return Err(snapshot_invalid(
                        "branch replay does not match its durable token row",
                    ));
                }
                if self
                    .selected_port
                    .as_ref()
                    .is_some_and(|port| port != &selected_port)
                    || self
                        .emission
                        .as_ref()
                        .is_some_and(|emission| emission.token_id != token_id)
                {
                    return Err(snapshot_invalid(
                        "branch decision state diverges from its exact replay record",
                    ));
                }
                self.selected_port = Some(selected_port);
                self.emission = Some(BranchEmission {
                    token_id: token_id.clone(),
                });
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: ledger.handle(token_id),
                });
            }
            Replay::Exact(_) => {
                return Err(snapshot_invalid(
                    "transition replay result kind does not match the canonical command",
                ));
            }
            Replay::Missing => {}
        }

        if let Some((row, authoritative_transition)) = ledger.prior_emission(&output) {
            if row.provenance != output || authoritative_transition.intent_hash != intent_hash {
                return Ok(TransitionOutcome::StateConflict);
            }
            self.selected_port = Some(selected_port);
            self.emission = Some(BranchEmission {
                token_id: row.token_id.clone(),
            });
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: ledger.handle(row.token_id.clone()),
            });
        }

        if self.selected_port.is_some() || self.emission.is_some() || !ledger.can_emit(&output) {
            return Ok(TransitionOutcome::StateConflict);
        }
        if input
            .as_ref()
            .is_some_and(|row| !ledger.can_consume(&row.token_id))
        {
            return Ok(TransitionOutcome::StateConflict);
        }

        let mut staged_ledger = ledger.staged();
        if let Some(row) = &input {
            staged_ledger.consume(
                row.token_id.clone(),
                ControlConsumer {
                    kind: ControlConsumerKind::Branch,
                    activation_id: self.branch_activation_id.clone(),
                },
                transition_key.clone(),
                intent_hash.clone(),
            );
        }
        let token = staged_ledger.mint(output);
        let token_row = staged_ledger.emitted_row(&token);
        staged_ledger.record(
            transition_key.clone(),
            intent_hash.clone(),
            ControlTransitionResult::Token {
                emitter_activation_id: self.branch_activation_id.clone(),
                emission_slot: ControlEmissionSlot::BranchDecision,
                row: Box::new(token_row),
            },
        );

        self.selected_port = Some(selected_port);
        self.emission = Some(BranchEmission {
            token_id: token.token_id.clone(),
        });
        *ledger = staged_ledger;
        Ok(TransitionOutcome::Committed { result: token })
    }
}

/// Persisted Merge acceptance summary. It carries the complete authoritative
/// input row so replay comparisons include source and outer frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeArrival {
    run_id: RunId,
    input: PersistedControlTokenRow,
    selected_port: PortId,
    output_token_id: ControlTokenId,
}

impl MergeArrival {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn input(&self) -> &PersistedControlTokenRow {
        &self.input
    }

    pub fn selected_port(&self) -> &PortId {
        &self.selected_port
    }

    pub fn output_token_id(&self) -> &ControlTokenId {
        &self.output_token_id
    }
}

/// Exclusive Merge aggregate. Its producer Activation and output port are
/// frozen at construction and never supplied by an emission call.
#[derive(Debug)]
pub struct MergeState {
    run_id: RunId,
    merge_activation_id: ActivationId,
    output_port: PortId,
    expected_branch_activation_id: ActivationId,
    expected_scope_instance_id: ScopeInstanceId,
    incoming_ports: Vec<PortId>,
    incoming_port_set: BTreeSet<PortId>,
    arrival: Option<MergeArrival>,
}

#[derive(Serialize)]
struct MergeIntent<'a> {
    operation: &'static str,
    run_id: &'a RunId,
    merge_activation_id: &'a ActivationId,
    output_port: &'a PortId,
    expected_branch_activation_id: &'a ActivationId,
    expected_scope_instance_id: &'a ScopeInstanceId,
    incoming_ports: &'a [PortId],
    input: &'a PersistedControlTokenRow,
    output: &'a ControlTokenProvenance,
}

impl MergeState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: RunId,
        merge_activation_id: ActivationId,
        output_port: PortId,
        expected_branch_activation_id: ActivationId,
        expected_scope_instance_id: ScopeInstanceId,
        incoming_ports: Vec<PortId>,
    ) -> Result<Self, ModelError> {
        let incoming_port_set = incoming_ports.iter().cloned().collect::<BTreeSet<_>>();
        if incoming_ports.is_empty() || incoming_port_set.len() != incoming_ports.len() {
            return Err(ModelError::new(
                MERGE_CONFIGURATION_INVALID,
                "merge must declare a non-empty unique incoming port set",
            ));
        }
        Ok(Self {
            run_id,
            merge_activation_id,
            output_port,
            expected_branch_activation_id,
            expected_scope_instance_id,
            incoming_ports,
            incoming_port_set,
            arrival: None,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn merge_activation_id(&self) -> &ActivationId {
        &self.merge_activation_id
    }

    pub fn output_port(&self) -> &PortId {
        &self.output_port
    }

    pub fn incoming_ports(&self) -> &[PortId] {
        &self.incoming_ports
    }

    pub fn arrival(&self) -> Option<&MergeArrival> {
        self.arrival.as_ref()
    }

    pub fn arrive_and_emit(
        &mut self,
        ledger: &mut ControlLedger,
        transition_key: TransitionKey,
        token: &OwnedControlToken,
    ) -> Result<TransitionOutcome<OwnedControlToken>, ModelError> {
        require_run(
            &self.run_id,
            ledger.run_id(),
            "merge aggregate and control ledger belong to different runs",
        )?;
        let input = ledger.resolve(token)?.clone();
        let branch = input
            .provenance
            .top_frame()
            .and_then(ControlFrame::as_branch)
            .ok_or_else(|| {
                ModelError::new(
                    MERGE_CORRELATION_MISMATCH,
                    "merge requires its Branch correlation at the true top frame",
                )
            })?;
        if input.provenance.scope_instance_id != self.expected_scope_instance_id {
            return Err(ModelError::new(
                CONTROL_SCOPE_MISMATCH,
                "merge token belongs to another scope instance",
            ));
        }
        if branch.branch_activation_id != self.expected_branch_activation_id {
            return Err(ModelError::new(
                MERGE_CORRELATION_MISMATCH,
                "merge token belongs to another branch activation",
            ));
        }
        if branch.scope_instance_id != self.expected_scope_instance_id {
            return Err(ModelError::new(
                CONTROL_SCOPE_MISMATCH,
                "merge branch correlation belongs to another scope instance",
            ));
        }
        if !self.incoming_port_set.contains(&branch.selected_port) {
            return Err(ModelError::new(
                MERGE_PORT_INVALID,
                "merge token selected a port outside the frozen incoming set",
            ));
        }

        let selected_port = branch.selected_port.clone();
        let mut outer_frames = input.provenance.frames.clone();
        let popped = outer_frames.pop();
        debug_assert!(matches!(popped, Some(ControlFrame::Branch(_))));
        let output = ControlTokenProvenance::new(
            self.run_id.clone(),
            self.merge_activation_id.clone(),
            self.output_port.clone(),
            ControlEmissionSlot::MergeOutput,
            self.expected_scope_instance_id.clone(),
            outer_frames,
        )?;
        let intent = MergeIntent {
            operation: "merge.consume_emit",
            run_id: &self.run_id,
            merge_activation_id: &self.merge_activation_id,
            output_port: &self.output_port,
            expected_branch_activation_id: &self.expected_branch_activation_id,
            expected_scope_instance_id: &self.expected_scope_instance_id,
            incoming_ports: &self.incoming_ports,
            input: &input,
            output: &output,
        };
        let intent_hash = IntentHash::from_serializable(&intent)?;

        match ledger.replay(&transition_key, &intent_hash)? {
            Replay::Exact(ControlTransitionResult::Token {
                emitter_activation_id,
                emission_slot,
                row,
            }) => {
                let row = *row;
                let token_id = row.token_id.clone();
                if emitter_activation_id != self.merge_activation_id
                    || emission_slot != ControlEmissionSlot::MergeOutput
                    || ledger.rows.get(&token_id) != Some(&row)
                {
                    return Err(snapshot_invalid(
                        "merge replay does not match its durable token row",
                    ));
                }
                let arrival = MergeArrival {
                    run_id: self.run_id.clone(),
                    input,
                    selected_port,
                    output_token_id: token_id.clone(),
                };
                if self
                    .arrival
                    .as_ref()
                    .is_some_and(|stored| stored != &arrival)
                {
                    return Err(snapshot_invalid(
                        "merge state diverges from its exact replay record",
                    ));
                }
                self.arrival = Some(arrival);
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: ledger.handle(token_id),
                });
            }
            Replay::Exact(_) => {
                return Err(snapshot_invalid(
                    "transition replay result kind does not match the canonical command",
                ));
            }
            Replay::Missing => {}
        }
        if let Some((row, authoritative_transition)) = ledger.prior_emission(&output) {
            if row.provenance != output || authoritative_transition.intent_hash != intent_hash {
                return Ok(TransitionOutcome::StateConflict);
            }
            let arrival = MergeArrival {
                run_id: self.run_id.clone(),
                input,
                selected_port,
                output_token_id: row.token_id.clone(),
            };
            self.arrival = Some(arrival);
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: ledger.handle(row.token_id.clone()),
            });
        }
        if self.arrival.is_some()
            || !ledger.can_consume(&input.token_id)
            || !ledger.can_emit(&output)
        {
            return Ok(TransitionOutcome::StateConflict);
        }

        let mut staged_ledger = ledger.staged();
        staged_ledger.consume(
            input.token_id.clone(),
            ControlConsumer {
                kind: ControlConsumerKind::Merge,
                activation_id: self.merge_activation_id.clone(),
            },
            transition_key.clone(),
            intent_hash.clone(),
        );
        let output_token = staged_ledger.mint(output);
        let output_row = staged_ledger.emitted_row(&output_token);
        staged_ledger.record(
            transition_key,
            intent_hash,
            ControlTransitionResult::Token {
                emitter_activation_id: self.merge_activation_id.clone(),
                emission_slot: ControlEmissionSlot::MergeOutput,
                row: Box::new(output_row),
            },
        );
        self.arrival = Some(MergeArrival {
            run_id: self.run_id.clone(),
            input,
            selected_port,
            output_token_id: output_token.token_id.clone(),
        });
        *ledger = staged_ledger;
        Ok(TransitionOutcome::Committed {
            result: output_token,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildRequirement {
    Required,
    Optional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildSettlement {
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl ChildSettlement {
    fn from_terminal(terminal: ActivationLifecycle) -> Option<Self> {
        match terminal {
            ActivationLifecycle::Succeeded => Some(Self::Succeeded),
            ActivationLifecycle::Failed => Some(Self::Failed),
            ActivationLifecycle::Cancelled => Some(Self::Cancelled),
            ActivationLifecycle::TimedOut => Some(Self::TimedOut),
            ActivationLifecycle::Created
            | ActivationLifecycle::Ready
            | ActivationLifecycle::Leased
            | ActivationLifecycle::Running
            | ActivationLifecycle::RetryWait
            | ActivationLifecycle::Waiting
            | ActivationLifecycle::Terminating => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkLeg {
    run_id: RunId,
    leg_id: LegId,
    output_port: PortId,
    scope_instance_id: ScopeInstanceId,
    child_activation_id: ActivationId,
    requirement: ChildRequirement,
}

impl ForkLeg {
    pub fn new(
        run_id: RunId,
        leg_id: LegId,
        output_port: PortId,
        scope_instance_id: ScopeInstanceId,
        child_activation_id: ActivationId,
        requirement: ChildRequirement,
    ) -> Self {
        Self {
            run_id,
            leg_id,
            output_port,
            scope_instance_id,
            child_activation_id,
            requirement,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn leg_id(&self) -> &LegId {
        &self.leg_id
    }

    pub fn output_port(&self) -> &PortId {
        &self.output_port
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn child_activation_id(&self) -> &ActivationId {
        &self.child_activation_id
    }

    pub fn requirement(&self) -> ChildRequirement {
        self.requirement
    }
}

/// Frozen member set produced only by `ForkGroup::create`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ForkGroup {
    run_id: RunId,
    id: ForkGroupId,
    fork_activation_id: ActivationId,
    parent_scope_instance_id: ScopeInstanceId,
    members: Vec<ForkLeg>,
}

impl ForkGroup {
    fn validate_members(run_id: &RunId, members: &[ForkLeg]) -> Result<(), ModelError> {
        if members.is_empty() {
            return Err(ModelError::new(
                FORK_MEMBERS_INVALID,
                "fork group must contain at least one declared leg",
            ));
        }
        let mut leg_ids = BTreeSet::new();
        let mut child_ids = BTreeSet::new();
        let mut child_scopes = BTreeSet::new();
        for member in members {
            require_run(run_id, &member.run_id, "fork member belongs to another run")?;
            if !leg_ids.insert(member.leg_id.clone()) {
                return Err(ModelError::new(
                    FORK_LEG_DUPLICATE,
                    "fork group contains a duplicate leg identifier",
                ));
            }
            if !child_ids.insert(member.child_activation_id.clone())
                || !child_scopes.insert(member.scope_instance_id.clone())
            {
                return Err(ModelError::new(
                    FORK_MEMBERS_INVALID,
                    "fork legs must have unique child activations and scopes",
                ));
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        run_id: RunId,
        id: ForkGroupId,
        fork_activation_id: ActivationId,
        parent_scope_instance_id: ScopeInstanceId,
        members: Vec<ForkLeg>,
        ledger: &mut ControlLedger,
        parent_scope: &mut ScopeTracker,
        transition_key: TransitionKey,
        inherited: Option<&OwnedControlToken>,
    ) -> Result<TransitionOutcome<ForkCreation>, ModelError> {
        Self::validate_members(&run_id, &members)?;
        require_run(
            &run_id,
            ledger.run_id(),
            "fork aggregate and control ledger belong to different runs",
        )?;
        require_run(
            &run_id,
            parent_scope.run_id(),
            "fork aggregate and parent scope belong to different runs",
        )?;
        if parent_scope.scope_instance_id() != &parent_scope_instance_id {
            return Err(ModelError::new(
                CONTROL_SCOPE_MISMATCH,
                "fork parent scope does not match its frozen parent scope",
            ));
        }

        let group = Self {
            run_id: run_id.clone(),
            id,
            fork_activation_id,
            parent_scope_instance_id,
            members,
        };
        let input = inherited
            .map(|token| ledger.resolve(token).cloned())
            .transpose()?;
        if input
            .as_ref()
            .is_some_and(|row| row.provenance.scope_instance_id != group.parent_scope_instance_id)
        {
            return Err(ModelError::new(
                CONTROL_SCOPE_MISMATCH,
                "fork input token does not belong to its parent scope",
            ));
        }

        let inherited_frames = input
            .as_ref()
            .map(|row| row.provenance.frames.clone())
            .unwrap_or_default();
        let mut outputs = Vec::with_capacity(group.members.len());
        for member in &group.members {
            let mut frames = inherited_frames.clone();
            frames.push(ControlFrame::ForkLeg(ForkLegCorrelation {
                run_id: group.run_id.clone(),
                fork_activation_id: group.fork_activation_id.clone(),
                fork_group_id: group.id.clone(),
                leg_id: member.leg_id.clone(),
                parent_scope_instance_id: group.parent_scope_instance_id.clone(),
                scope_instance_id: member.scope_instance_id.clone(),
                child_activation_id: member.child_activation_id.clone(),
            }));
            outputs.push(ControlTokenProvenance::new(
                group.run_id.clone(),
                group.fork_activation_id.clone(),
                member.output_port.clone(),
                ControlEmissionSlot::ForkLeg {
                    fork_group_id: group.id.clone(),
                    leg_id: member.leg_id.clone(),
                },
                member.scope_instance_id.clone(),
                frames,
            )?);
        }

        let intent = ForkCreateIntent {
            operation: "fork.create_admit_emit",
            run_id: &group.run_id,
            fork_group_id: &group.id,
            fork_activation_id: &group.fork_activation_id,
            parent_scope_instance_id: &group.parent_scope_instance_id,
            members: &group.members,
            input: input.as_ref(),
            outputs: &outputs,
        };
        let intent_hash = IntentHash::from_serializable(&intent)?;
        match ledger.replay(&transition_key, &intent_hash)? {
            Replay::Exact(ControlTransitionResult::ForkCreated {
                fork_activation_id,
                fork_group_id,
                tokens,
            }) => {
                if fork_activation_id != group.fork_activation_id || fork_group_id != group.id {
                    return Err(snapshot_invalid(
                        "fork replay identity diverges from its exact replay record",
                    ));
                }
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: ForkCreation::from_record(ledger, group, tokens)?,
                });
            }
            Replay::Exact(_) => {
                return Err(snapshot_invalid(
                    "transition replay result kind does not match the canonical command",
                ));
            }
            Replay::Missing => {}
        }
        let mut prior_rows = Vec::with_capacity(group.members.len());
        let mut prior_transition_key: Option<TransitionKey> = None;
        for (member, output) in group.members.iter().zip(&outputs) {
            let Some((row, authoritative_transition)) = ledger.prior_emission(output) else {
                continue;
            };
            if row.provenance != *output
                || authoritative_transition.intent_hash != intent_hash
                || prior_transition_key
                    .as_ref()
                    .is_some_and(|key| key != &authoritative_transition.transition_key)
            {
                return Ok(TransitionOutcome::StateConflict);
            }
            prior_transition_key = Some(authoritative_transition.transition_key.clone());
            prior_rows.push((member.leg_id.clone(), row.clone()));
        }
        if !prior_rows.is_empty() {
            if prior_rows.len() != group.members.len()
                || !parent_scope.matches_fork_admission(&group.members)
            {
                return Ok(TransitionOutcome::StateConflict);
            }
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: ForkCreation::from_record(ledger, group, prior_rows)?,
            });
        }
        if input
            .as_ref()
            .is_some_and(|row| !ledger.can_consume(&row.token_id))
            || outputs.iter().any(|output| !ledger.can_emit(output))
        {
            return Ok(TransitionOutcome::StateConflict);
        }
        parent_scope.require_atomic_fork_admission(&group.members)?;

        let mut staged_ledger = ledger.staged();
        let mut staged_scope = parent_scope.staged();
        if let Some(row) = &input {
            staged_ledger.consume(
                row.token_id.clone(),
                ControlConsumer {
                    kind: ControlConsumerKind::Fork,
                    activation_id: group.fork_activation_id.clone(),
                },
                transition_key.clone(),
                intent_hash.clone(),
            );
        }

        let mut leg_tokens = Vec::with_capacity(group.members.len());
        let mut token_record = Vec::with_capacity(group.members.len());
        for (member, output) in group.members.iter().zip(outputs) {
            staged_scope.admit_fork_child(member)?;
            let token = staged_ledger.mint(output);
            token_record.push((member.leg_id.clone(), staged_ledger.emitted_row(&token)));
            leg_tokens.push(ForkLegToken {
                run_id: run_id.clone(),
                leg_id: member.leg_id.clone(),
                token,
            });
        }
        staged_ledger.record(
            transition_key,
            intent_hash,
            ControlTransitionResult::ForkCreated {
                fork_activation_id: group.fork_activation_id.clone(),
                fork_group_id: group.id.clone(),
                tokens: token_record,
            },
        );
        let creation = ForkCreation {
            run_id,
            group,
            tokens: leg_tokens,
        };
        *ledger = staged_ledger;
        *parent_scope = staged_scope;
        Ok(TransitionOutcome::Committed { result: creation })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn id(&self) -> &ForkGroupId {
        &self.id
    }

    pub fn fork_activation_id(&self) -> &ActivationId {
        &self.fork_activation_id
    }

    pub fn parent_scope_instance_id(&self) -> &ScopeInstanceId {
        &self.parent_scope_instance_id
    }

    pub fn members(&self) -> &[ForkLeg] {
        &self.members
    }

    pub fn member(&self, leg_id: &LegId) -> Option<&ForkLeg> {
        self.members.iter().find(|member| &member.leg_id == leg_id)
    }
}

#[derive(Serialize)]
struct ForkCreateIntent<'a> {
    operation: &'static str,
    run_id: &'a RunId,
    fork_group_id: &'a ForkGroupId,
    fork_activation_id: &'a ActivationId,
    parent_scope_instance_id: &'a ScopeInstanceId,
    members: &'a [ForkLeg],
    input: Option<&'a PersistedControlTokenRow>,
    outputs: &'a [ControlTokenProvenance],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkLegToken {
    run_id: RunId,
    leg_id: LegId,
    token: OwnedControlToken,
}

impl ForkLegToken {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn leg_id(&self) -> &LegId {
        &self.leg_id
    }

    pub fn token(&self) -> &OwnedControlToken {
        &self.token
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkCreation {
    run_id: RunId,
    group: ForkGroup,
    tokens: Vec<ForkLegToken>,
}

impl ForkCreation {
    fn from_record(
        ledger: &ControlLedger,
        group: ForkGroup,
        tokens: Vec<(LegId, PersistedControlTokenRow)>,
    ) -> Result<Self, ModelError> {
        let mut handles = Vec::with_capacity(tokens.len());
        for (leg_id, row) in tokens {
            if ledger.rows.get(row.token_id()) != Some(&row) {
                return Err(snapshot_invalid(
                    "fork replay token differs from its authoritative ledger row",
                ));
            }
            let expected_slot = ControlEmissionSlot::ForkLeg {
                fork_group_id: group.id.clone(),
                leg_id: leg_id.clone(),
            };
            if row.provenance.source_activation_id != group.fork_activation_id
                || row.provenance.emission_slot != expected_slot
            {
                return Err(snapshot_invalid(
                    "fork replay token does not match its frozen logical leg slot",
                ));
            }
            let token = ledger.load(&group.run_id, row.token_id())?;
            handles.push(ForkLegToken {
                run_id: group.run_id.clone(),
                leg_id,
                token,
            });
        }
        Ok(Self {
            run_id: group.run_id.clone(),
            group,
            tokens: handles,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn group(&self) -> &ForkGroup {
        &self.group
    }

    pub fn tokens(&self) -> &[ForkLegToken] {
        &self.tokens
    }

    pub fn token(&self, leg_id: &LegId) -> Option<&OwnedControlToken> {
        self.tokens
            .iter()
            .find(|entry| &entry.leg_id == leg_id)
            .map(ForkLegToken::token)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinMode {
    AllSuccess,
    AllSettled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegSettlementClass {
    Succeeded,
    InfrastructureFailure,
    Panic,
    Cancelled,
    DeadlineExceeded,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LegSettlement {
    Succeeded { output: ValueRef },
    InfrastructureFailure,
    Panic,
    Cancelled,
    DeadlineExceeded,
}

impl LegSettlement {
    pub fn class(&self) -> LegSettlementClass {
        match self {
            Self::Succeeded { .. } => LegSettlementClass::Succeeded,
            Self::InfrastructureFailure => LegSettlementClass::InfrastructureFailure,
            Self::Panic => LegSettlementClass::Panic,
            Self::Cancelled => LegSettlementClass::Cancelled,
            Self::DeadlineExceeded => LegSettlementClass::DeadlineExceeded,
        }
    }

    pub fn handling(&self, mode: JoinMode) -> SettlementHandling {
        match (mode, self) {
            (_, Self::Succeeded { .. }) => SettlementHandling::CollectValue,
            (
                _,
                Self::InfrastructureFailure
                | Self::Panic
                | Self::Cancelled
                | Self::DeadlineExceeded,
            ) => SettlementHandling::FailJoin,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementHandling {
    CollectValue,
    FailJoin,
}

/// Settlement coupled to an authoritative terminal proof. It has no serde
/// implementation and no public constructor.
#[derive(Debug, Clone, PartialEq)]
pub struct LegSettlementProof {
    terminal: TerminalActivationProof,
    settlement: LegSettlement,
}

impl LegSettlementProof {
    pub(crate) fn mint(terminal: TerminalActivationProof) -> Result<Self, ModelError> {
        let settlement = match terminal.result() {
            TerminalActivationResult::Succeeded {
                output,
                content_hash,
            } if output.content_hash() == content_hash => LegSettlement::Succeeded {
                output: output.clone(),
            },
            TerminalActivationResult::Succeeded { .. } => {
                return Err(ModelError::new(
                    SETTLEMENT_PROOF_INVALID,
                    "successful terminal result carries a mismatched output hash",
                ));
            }
            TerminalActivationResult::Failed { reason, failure } => {
                settlement_from_failure(*reason, failure.as_ref())?
            }
            TerminalActivationResult::Cancelled => LegSettlement::Cancelled,
            TerminalActivationResult::TimedOut => LegSettlement::DeadlineExceeded,
        };
        Ok(Self {
            terminal,
            settlement,
        })
    }

    pub fn terminal(&self) -> &TerminalActivationProof {
        &self.terminal
    }

    pub fn settlement(&self) -> &LegSettlement {
        &self.settlement
    }
}

fn settlement_from_failure(
    reason: ActivationTerminationReason,
    failure: Option<&InternalFailureSummary>,
) -> Result<LegSettlement, ModelError> {
    if reason == ActivationTerminationReason::EffectOutcomeUnknown {
        return Ok(LegSettlement::InfrastructureFailure);
    }
    if reason != ActivationTerminationReason::Failure {
        return Err(ModelError::new(
            SETTLEMENT_PROOF_INVALID,
            "failed terminal result carries a non-failure termination reason",
        ));
    }
    let Some(failure) = failure else {
        // Native failures such as exhausted lease expiry have no worker
        // summary. They are never upgraded to a collectable business error.
        return Ok(LegSettlement::InfrastructureFailure);
    };
    match failure.kind() {
        // This superseded control aggregate has no typed public SafeError
        // payload. A symbolic internal summary alone must therefore fail
        // closed instead of being upgraded into collectable business data.
        InternalFailureKind::Business => Ok(LegSettlement::InfrastructureFailure),
        InternalFailureKind::Invariant => Ok(LegSettlement::Panic),
        InternalFailureKind::Cancelled => Err(ModelError::new(
            SETTLEMENT_PROOF_INVALID,
            "failed terminal result cannot carry a cancelled failure summary",
        )),
        InternalFailureKind::Workflow
        | InternalFailureKind::Timeout
        | InternalFailureKind::Infrastructure
        | InternalFailureKind::EffectOutcomeUnknown => Ok(LegSettlement::InfrastructureFailure),
    }
}

#[derive(Serialize)]
struct LegSettlementProofIntent<'a> {
    run_id: &'a RunId,
    scope_instance_id: &'a ScopeInstanceId,
    activation_id: &'a ActivationId,
    terminal: ActivationLifecycle,
    attempts_drained: bool,
    result: TerminalActivationResultIntent<'a>,
    settlement: &'a LegSettlement,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TerminalActivationResultIntent<'a> {
    Succeeded {
        output: &'a ValueRef,
        content_hash: &'a ContentHash,
    },
    Failed {
        reason: ActivationTerminationReason,
        failure: Option<&'a InternalFailureSummary>,
    },
    Cancelled,
    TimedOut,
}

impl<'a> From<&'a TerminalActivationResult> for TerminalActivationResultIntent<'a> {
    fn from(result: &'a TerminalActivationResult) -> Self {
        match result {
            TerminalActivationResult::Succeeded {
                output,
                content_hash,
            } => Self::Succeeded {
                output,
                content_hash,
            },
            TerminalActivationResult::Failed { reason, failure } => Self::Failed {
                reason: *reason,
                failure: failure.as_ref(),
            },
            TerminalActivationResult::Cancelled => Self::Cancelled,
            TerminalActivationResult::TimedOut => Self::TimedOut,
        }
    }
}

impl<'a> From<&'a LegSettlementProof> for LegSettlementProofIntent<'a> {
    fn from(proof: &'a LegSettlementProof) -> Self {
        Self {
            run_id: proof.terminal.run_id(),
            scope_instance_id: proof.terminal.scope_instance_id(),
            activation_id: proof.terminal.activation_id(),
            terminal: proof.terminal.terminal(),
            attempts_drained: proof.terminal.attempts_drained(),
            result: proof.terminal.result().into(),
            settlement: &proof.settlement,
        }
    }
}

/// Durable Join arrival row. The full input row is retained so a replay hash
/// cannot ignore a changed source or outer frame stack.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TerminalSettlementEvidence {
    Succeeded {
        output: ValueRef,
        content_hash: ContentHash,
    },
    Failed {
        reason: ActivationTerminationReason,
        failure: Option<InternalFailureSummary>,
    },
    Cancelled,
    TimedOut,
}

impl TerminalSettlementEvidence {
    fn from_proof(proof: &TerminalActivationProof) -> Self {
        match proof.result() {
            TerminalActivationResult::Succeeded {
                output,
                content_hash,
            } => Self::Succeeded {
                output: output.clone(),
                content_hash: content_hash.clone(),
            },
            TerminalActivationResult::Failed { reason, failure } => Self::Failed {
                reason: *reason,
                failure: failure.clone(),
            },
            TerminalActivationResult::Cancelled => Self::Cancelled,
            TerminalActivationResult::TimedOut => Self::TimedOut,
        }
    }

    fn lifecycle(&self) -> ActivationLifecycle {
        match self {
            Self::Succeeded { .. } => ActivationLifecycle::Succeeded,
            Self::Failed { .. } => ActivationLifecycle::Failed,
            Self::Cancelled => ActivationLifecycle::Cancelled,
            Self::TimedOut => ActivationLifecycle::TimedOut,
        }
    }

    fn matches_settlement(&self, settlement: &LegSettlement) -> bool {
        match self {
            Self::Succeeded {
                output,
                content_hash,
            } => {
                output.content_hash() == content_hash
                    && matches!(
                        settlement,
                        LegSettlement::Succeeded {
                            output: settled_output
                        } if settled_output == output
                    )
            }
            Self::Failed { reason, failure } => settlement_from_failure(*reason, failure.as_ref())
                .is_ok_and(|expected| &expected == settlement),
            Self::Cancelled => matches!(settlement, LegSettlement::Cancelled),
            Self::TimedOut => matches!(settlement, LegSettlement::DeadlineExceeded),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinArrival {
    run_id: RunId,
    join_activation_id: ActivationId,
    fork_group_id: ForkGroupId,
    fork_activation_id: ActivationId,
    parent_scope_instance_id: ScopeInstanceId,
    member: ForkLeg,
    input: PersistedControlTokenRow,
    leg_id: LegId,
    child_activation_id: ActivationId,
    terminal: TerminalSettlementEvidence,
    attempts_drained: bool,
    settlement: LegSettlement,
    outer_frames: Vec<ControlFrame>,
}

/// Inert wire forms used only while deserializing a complete ledger snapshot.
/// They are private and are upgraded to trusted control values only inside the
/// crate-private `ControlLedger::from_snapshot` path, which then performs the
/// full cross-reference validation.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum LegSettlementWire {
    Succeeded { output: ValueRef },
    InfrastructureFailure,
    Panic,
    Cancelled,
    DeadlineExceeded,
}

impl LegSettlementWire {
    fn into_trusted(self) -> LegSettlement {
        match self {
            Self::Succeeded { output } => LegSettlement::Succeeded { output },
            Self::InfrastructureFailure => LegSettlement::InfrastructureFailure,
            Self::Panic => LegSettlement::Panic,
            Self::Cancelled => LegSettlement::Cancelled,
            Self::DeadlineExceeded => LegSettlement::DeadlineExceeded,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinArrivalWire {
    run_id: RunId,
    join_activation_id: ActivationId,
    fork_group_id: ForkGroupId,
    fork_activation_id: ActivationId,
    parent_scope_instance_id: ScopeInstanceId,
    member: ForkLeg,
    input: PersistedControlTokenRow,
    leg_id: LegId,
    child_activation_id: ActivationId,
    terminal: TerminalSettlementEvidence,
    attempts_drained: bool,
    settlement: LegSettlementWire,
    outer_frames: Vec<ControlFrame>,
}

impl JoinArrivalWire {
    fn into_trusted(self) -> JoinArrival {
        JoinArrival {
            run_id: self.run_id,
            join_activation_id: self.join_activation_id,
            fork_group_id: self.fork_group_id,
            fork_activation_id: self.fork_activation_id,
            parent_scope_instance_id: self.parent_scope_instance_id,
            member: self.member,
            input: self.input,
            leg_id: self.leg_id,
            child_activation_id: self.child_activation_id,
            terminal: self.terminal,
            attempts_drained: self.attempts_drained,
            settlement: self.settlement.into_trusted(),
            outer_frames: self.outer_frames,
        }
    }
}

impl JoinArrival {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn input(&self) -> &PersistedControlTokenRow {
        &self.input
    }

    pub fn join_activation_id(&self) -> &ActivationId {
        &self.join_activation_id
    }

    pub fn fork_group_id(&self) -> &ForkGroupId {
        &self.fork_group_id
    }

    pub fn member(&self) -> &ForkLeg {
        &self.member
    }

    pub fn token_id(&self) -> &ControlTokenId {
        self.input.token_id()
    }

    pub fn leg_id(&self) -> &LegId {
        &self.leg_id
    }

    pub fn child_activation_id(&self) -> &ActivationId {
        &self.child_activation_id
    }

    pub fn settlement(&self) -> &LegSettlement {
        &self.settlement
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinFailure {
    run_id: RunId,
    leg_id: LegId,
    class: LegSettlementClass,
}

impl JoinFailure {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn leg_id(&self) -> &LegId {
        &self.leg_id
    }

    pub fn class(&self) -> LegSettlementClass {
        self.class
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinStatus {
    Pending {
        pending_legs: Vec<LegId>,
    },
    DrainingAfterFailure {
        failure: JoinFailure,
        pending_legs: Vec<LegId>,
    },
    Ready {
        mode: JoinMode,
    },
    Failed {
        failure: JoinFailure,
    },
}

#[derive(Debug)]
pub struct JoinState {
    run_id: RunId,
    join_activation_id: ActivationId,
    output_port: PortId,
    mode: JoinMode,
    fork_group_id: ForkGroupId,
    fork_activation_id: ActivationId,
    parent_scope_instance_id: ScopeInstanceId,
    members: Vec<ForkLeg>,
    arrivals: BTreeMap<LegId, JoinArrival>,
    outer_frames: Option<Vec<ControlFrame>>,
    output_token_id: Option<ControlTokenId>,
}

#[derive(Serialize)]
struct JoinArrivalIntent<'a> {
    operation: &'static str,
    run_id: &'a RunId,
    join_activation_id: &'a ActivationId,
    output_port: &'a PortId,
    mode: JoinMode,
    fork_group_id: &'a ForkGroupId,
    fork_activation_id: &'a ActivationId,
    parent_scope_instance_id: &'a ScopeInstanceId,
    members: &'a [ForkLeg],
    input: &'a PersistedControlTokenRow,
    proof: LegSettlementProofIntent<'a>,
}

#[derive(Serialize)]
struct JoinEmitIntent<'a> {
    operation: &'static str,
    run_id: &'a RunId,
    join_activation_id: &'a ActivationId,
    output_port: &'a PortId,
    mode: JoinMode,
    fork_group_id: &'a ForkGroupId,
    members: &'a [ForkLeg],
    arrivals: &'a [JoinArrival],
    output: &'a ControlTokenProvenance,
}

impl JoinState {
    fn staged(&self) -> Self {
        Self {
            run_id: self.run_id.clone(),
            join_activation_id: self.join_activation_id.clone(),
            output_port: self.output_port.clone(),
            mode: self.mode,
            fork_group_id: self.fork_group_id.clone(),
            fork_activation_id: self.fork_activation_id.clone(),
            parent_scope_instance_id: self.parent_scope_instance_id.clone(),
            members: self.members.clone(),
            arrivals: self.arrivals.clone(),
            outer_frames: self.outer_frames.clone(),
            output_token_id: self.output_token_id.clone(),
        }
    }

    pub fn new(
        run_id: RunId,
        join_activation_id: ActivationId,
        output_port: PortId,
        mode: JoinMode,
        fork_group: &ForkGroup,
    ) -> Result<Self, ModelError> {
        require_run(
            &run_id,
            &fork_group.run_id,
            "join aggregate and fork group belong to different runs",
        )?;
        Ok(Self {
            run_id,
            join_activation_id,
            output_port,
            mode,
            fork_group_id: fork_group.id.clone(),
            fork_activation_id: fork_group.fork_activation_id.clone(),
            parent_scope_instance_id: fork_group.parent_scope_instance_id.clone(),
            members: fork_group.members.clone(),
            arrivals: BTreeMap::new(),
            outer_frames: None,
            output_token_id: None,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn join_activation_id(&self) -> &ActivationId {
        &self.join_activation_id
    }

    pub fn output_port(&self) -> &PortId {
        &self.output_port
    }

    pub fn mode(&self) -> JoinMode {
        self.mode
    }

    pub fn members(&self) -> &[ForkLeg] {
        &self.members
    }

    pub fn arrival(&self, leg_id: &LegId) -> Option<&JoinArrival> {
        self.arrivals.get(leg_id)
    }

    pub fn pending_legs(&self) -> Vec<LegId> {
        self.members
            .iter()
            .filter(|member| !self.arrivals.contains_key(&member.leg_id))
            .map(|member| member.leg_id.clone())
            .collect()
    }

    pub fn is_complete(&self) -> bool {
        self.arrivals.len() == self.members.len()
    }

    pub fn arrive(
        &mut self,
        ledger: &mut ControlLedger,
        transition_key: TransitionKey,
        token: &OwnedControlToken,
        terminal: &TerminalActivationProof,
    ) -> Result<TransitionOutcome<JoinArrival>, ModelError> {
        let proof = LegSettlementProof::mint(terminal.clone())?;
        require_run(
            &self.run_id,
            ledger.run_id(),
            "join aggregate and control ledger belong to different runs",
        )?;
        let input = ledger.resolve(token)?.clone();
        let fork = input
            .provenance
            .top_frame()
            .and_then(ControlFrame::as_fork_leg)
            .ok_or_else(|| {
                ModelError::new(
                    JOIN_GROUP_MISMATCH,
                    "join requires its Fork leg correlation at the true top frame",
                )
            })?;
        if fork.fork_group_id != self.fork_group_id
            || fork.fork_activation_id != self.fork_activation_id
        {
            return Err(ModelError::new(
                JOIN_GROUP_MISMATCH,
                "join token belongs to another frozen fork group",
            ));
        }
        let member = self
            .members
            .iter()
            .find(|member| member.leg_id == fork.leg_id)
            .ok_or_else(|| {
                ModelError::new(
                    JOIN_LEG_UNKNOWN,
                    "join token names a leg outside the frozen member set",
                )
            })?;
        if input.provenance.scope_instance_id != member.scope_instance_id
            || fork.scope_instance_id != member.scope_instance_id
            || fork.parent_scope_instance_id != self.parent_scope_instance_id
        {
            return Err(ModelError::new(
                JOIN_SCOPE_MISMATCH,
                "join token does not belong to the frozen leg scope",
            ));
        }
        if proof.terminal.run_id() != &self.run_id
            || proof.terminal.scope_instance_id() != &member.scope_instance_id
            || proof.terminal.activation_id() != &member.child_activation_id
            || fork.child_activation_id != member.child_activation_id
        {
            return Err(ModelError::new(
                JOIN_PROOF_MISMATCH,
                "join settlement proof does not match the frozen child activation",
            ));
        }

        let mut outer_frames = input.provenance.frames.clone();
        let popped = outer_frames.pop();
        debug_assert!(matches!(popped, Some(ControlFrame::ForkLeg(_))));
        if self
            .outer_frames
            .as_ref()
            .is_some_and(|expected| expected != &outer_frames)
        {
            return Err(ModelError::new(
                JOIN_OUTER_CORRELATION_MISMATCH,
                "fork legs do not carry the same outer frame stack",
            ));
        }

        let intent = JoinArrivalIntent {
            operation: "join.consume_arrive",
            run_id: &self.run_id,
            join_activation_id: &self.join_activation_id,
            output_port: &self.output_port,
            mode: self.mode,
            fork_group_id: &self.fork_group_id,
            fork_activation_id: &self.fork_activation_id,
            parent_scope_instance_id: &self.parent_scope_instance_id,
            members: &self.members,
            input: &input,
            proof: (&proof).into(),
        };
        let intent_hash = IntentHash::from_serializable(&intent)?;
        match ledger.replay(&transition_key, &intent_hash)? {
            Replay::Exact(ControlTransitionResult::JoinArrived(arrival)) => {
                let arrival = *arrival;
                if self
                    .arrivals
                    .get(&arrival.leg_id)
                    .is_some_and(|stored| stored != &arrival)
                {
                    return Err(snapshot_invalid(
                        "join arrival state diverges from its exact replay record",
                    ));
                }
                self.install_arrival(arrival.clone())?;
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: arrival,
                });
            }
            Replay::Exact(_) => {
                return Err(snapshot_invalid(
                    "transition replay result kind does not match the canonical command",
                ));
            }
            Replay::Missing => {}
        }
        if let Some((authoritative, authoritative_transition)) = ledger.prior_join_arrival(
            &self.join_activation_id,
            &self.fork_group_id,
            &member.leg_id,
        ) {
            if authoritative_transition.intent_hash != intent_hash {
                return Ok(TransitionOutcome::StateConflict);
            }
            let authoritative = authoritative.clone();
            self.install_arrival(authoritative.clone())?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        if self.arrivals.contains_key(&member.leg_id) || !ledger.can_consume(&input.token_id) {
            return Ok(TransitionOutcome::StateConflict);
        }

        let arrival = JoinArrival {
            run_id: self.run_id.clone(),
            join_activation_id: self.join_activation_id.clone(),
            fork_group_id: self.fork_group_id.clone(),
            fork_activation_id: self.fork_activation_id.clone(),
            parent_scope_instance_id: self.parent_scope_instance_id.clone(),
            member: member.clone(),
            input,
            leg_id: member.leg_id.clone(),
            child_activation_id: member.child_activation_id.clone(),
            terminal: TerminalSettlementEvidence::from_proof(&proof.terminal),
            attempts_drained: proof.terminal.attempts_drained(),
            settlement: proof.settlement.clone(),
            outer_frames,
        };
        let mut staged_ledger = ledger.staged();
        let mut staged_join = self.staged();
        staged_ledger.consume(
            arrival.input.token_id.clone(),
            ControlConsumer {
                kind: ControlConsumerKind::Join,
                activation_id: self.join_activation_id.clone(),
            },
            transition_key.clone(),
            intent_hash.clone(),
        );
        staged_join.install_arrival(arrival.clone())?;
        staged_ledger.record(
            transition_key,
            intent_hash,
            ControlTransitionResult::JoinArrived(Box::new(arrival.clone())),
        );
        *ledger = staged_ledger;
        *self = staged_join;
        Ok(TransitionOutcome::Committed { result: arrival })
    }

    fn install_arrival(&mut self, arrival: JoinArrival) -> Result<(), ModelError> {
        match &self.outer_frames {
            None => self.outer_frames = Some(arrival.outer_frames.clone()),
            Some(expected) if expected == &arrival.outer_frames => {}
            Some(_) => {
                return Err(ModelError::new(
                    JOIN_OUTER_CORRELATION_MISMATCH,
                    "join replay carries a different outer frame stack",
                ));
            }
        }
        self.arrivals
            .entry(arrival.leg_id.clone())
            .or_insert(arrival);
        Ok(())
    }

    pub fn arrivals_in_declaration_order(&self) -> Option<Vec<&JoinArrival>> {
        self.is_complete().then(|| {
            self.members
                .iter()
                .map(|member| {
                    self.arrivals
                        .get(&member.leg_id)
                        .expect("complete Join has one arrival per frozen member")
                })
                .collect()
        })
    }

    pub fn status(&self) -> JoinStatus {
        let pending_legs = self.pending_legs();
        let failure = self.members.iter().find_map(|member| {
            let arrival = self.arrivals.get(&member.leg_id)?;
            (arrival.settlement.handling(self.mode) == SettlementHandling::FailJoin).then(|| {
                JoinFailure {
                    run_id: self.run_id.clone(),
                    leg_id: member.leg_id.clone(),
                    class: arrival.settlement.class(),
                }
            })
        });
        match (failure, pending_legs.is_empty()) {
            (Some(failure), false) => JoinStatus::DrainingAfterFailure {
                failure,
                pending_legs,
            },
            (Some(failure), true) => JoinStatus::Failed { failure },
            (None, false) => JoinStatus::Pending { pending_legs },
            (None, true) => JoinStatus::Ready { mode: self.mode },
        }
    }

    pub fn emit(
        &mut self,
        ledger: &mut ControlLedger,
        transition_key: TransitionKey,
    ) -> Result<TransitionOutcome<OwnedControlToken>, ModelError> {
        require_run(
            &self.run_id,
            ledger.run_id(),
            "join aggregate and control ledger belong to different runs",
        )?;
        if !matches!(self.status(), JoinStatus::Ready { .. }) {
            return Err(ModelError::new(
                JOIN_OUTPUT_NOT_READY,
                "join cannot emit before every member reaches a collectable settlement",
            ));
        }
        let outer_frames = self
            .outer_frames
            .clone()
            .expect("ready Join records an outer frame stack");
        let output = ControlTokenProvenance::new(
            self.run_id.clone(),
            self.join_activation_id.clone(),
            self.output_port.clone(),
            ControlEmissionSlot::JoinOutput,
            self.parent_scope_instance_id.clone(),
            outer_frames,
        )?;
        let ordered_arrivals = self
            .arrivals_in_declaration_order()
            .expect("ready Join is complete")
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let intent = JoinEmitIntent {
            operation: "join.emit",
            run_id: &self.run_id,
            join_activation_id: &self.join_activation_id,
            output_port: &self.output_port,
            mode: self.mode,
            fork_group_id: &self.fork_group_id,
            members: &self.members,
            arrivals: &ordered_arrivals,
            output: &output,
        };
        let intent_hash = IntentHash::from_serializable(&intent)?;
        match ledger.replay(&transition_key, &intent_hash)? {
            Replay::Exact(ControlTransitionResult::Token {
                emitter_activation_id,
                emission_slot,
                row,
            }) => {
                let row = *row;
                let token_id = row.token_id.clone();
                if emitter_activation_id != self.join_activation_id
                    || emission_slot != ControlEmissionSlot::JoinOutput
                    || ledger.rows.get(&token_id) != Some(&row)
                {
                    return Err(snapshot_invalid(
                        "join emission replay does not match its durable token row",
                    ));
                }
                if self
                    .output_token_id
                    .as_ref()
                    .is_some_and(|stored| stored != &token_id)
                {
                    return Err(snapshot_invalid(
                        "join emission state diverges from its exact replay record",
                    ));
                }
                self.output_token_id = Some(token_id.clone());
                return Ok(TransitionOutcome::ExactReplay {
                    authoritative: ledger.handle(token_id),
                });
            }
            Replay::Exact(_) => {
                return Err(snapshot_invalid(
                    "transition replay result kind does not match the canonical command",
                ));
            }
            Replay::Missing => {}
        }
        if let Some((row, authoritative_transition)) = ledger.prior_emission(&output) {
            if row.provenance != output || authoritative_transition.intent_hash != intent_hash {
                return Ok(TransitionOutcome::StateConflict);
            }
            self.output_token_id = Some(row.token_id.clone());
            return Ok(TransitionOutcome::ExactReplay {
                authoritative: ledger.handle(row.token_id.clone()),
            });
        }
        if self.output_token_id.is_some() || !ledger.can_emit(&output) {
            return Ok(TransitionOutcome::StateConflict);
        }
        let mut staged_ledger = ledger.staged();
        let token = staged_ledger.mint(output);
        let token_row = staged_ledger.emitted_row(&token);
        staged_ledger.record(
            transition_key,
            intent_hash,
            ControlTransitionResult::Token {
                emitter_activation_id: self.join_activation_id.clone(),
                emission_slot: ControlEmissionSlot::JoinOutput,
                row: Box::new(token_row),
            },
        );
        self.output_token_id = Some(token.token_id.clone());
        *ledger = staged_ledger;
        Ok(TransitionOutcome::Committed { result: token })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeTrackerState {
    Open,
    Closing,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedChild {
    run_id: RunId,
    scope_instance_id: ScopeInstanceId,
    activation_id: ActivationId,
    requirement: ChildRequirement,
    settlement: Option<ChildSettlement>,
}

impl TrackedChild {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    pub fn requirement(&self) -> ChildRequirement {
        self.requirement
    }

    pub fn settlement(&self) -> Option<ChildSettlement> {
        self.settlement
    }
}

#[derive(Debug)]
pub struct ScopeTracker {
    run_id: RunId,
    scope_instance_id: ScopeInstanceId,
    state: ScopeTrackerState,
    children: BTreeMap<ActivationId, TrackedChild>,
}

impl ScopeTracker {
    pub(crate) fn staged(&self) -> Self {
        Self {
            run_id: self.run_id.clone(),
            scope_instance_id: self.scope_instance_id.clone(),
            state: self.state,
            children: self.children.clone(),
        }
    }

    pub fn new(run_id: RunId, scope_instance_id: ScopeInstanceId) -> Self {
        Self {
            run_id,
            scope_instance_id,
            state: ScopeTrackerState::Open,
            children: BTreeMap::new(),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn scope_instance_id(&self) -> &ScopeInstanceId {
        &self.scope_instance_id
    }

    pub fn state(&self) -> ScopeTrackerState {
        self.state
    }

    pub fn child(&self, activation_id: &ActivationId) -> Option<&TrackedChild> {
        self.children.get(activation_id)
    }

    pub fn child_count(&self) -> usize {
        self.children.len()
    }

    pub fn admit_child(
        &mut self,
        activation_id: ActivationId,
        child_scope_instance_id: ScopeInstanceId,
        requirement: ChildRequirement,
    ) -> Result<ApplyOutcome, ModelError> {
        if let Some(existing) = self.children.get(&activation_id) {
            return if existing.requirement == requirement
                && existing.scope_instance_id == child_scope_instance_id
            {
                Ok(ApplyOutcome::Duplicate)
            } else {
                Err(ModelError::new(
                    SCOPE_CHILD_CONFLICT,
                    "scope child was already admitted with another requirement",
                ))
            };
        }
        if self.state != ScopeTrackerState::Open {
            return Err(ModelError::new(
                SCOPE_ADMISSION_CLOSED,
                "scope cannot admit a new child after closing begins",
            ));
        }
        self.children.insert(
            activation_id.clone(),
            TrackedChild {
                run_id: self.run_id.clone(),
                scope_instance_id: child_scope_instance_id,
                activation_id,
                requirement,
                settlement: None,
            },
        );
        Ok(ApplyOutcome::Applied)
    }

    fn require_atomic_fork_admission(&self, members: &[ForkLeg]) -> Result<(), ModelError> {
        if self.state != ScopeTrackerState::Open {
            return Err(ModelError::new(
                SCOPE_ADMISSION_CLOSED,
                "fork cannot admit children after parent scope closing begins",
            ));
        }
        if members
            .iter()
            .any(|member| self.children.contains_key(&member.child_activation_id))
        {
            return Err(ModelError::new(
                FORK_ATOMIC_ADMISSION_CONFLICT,
                "fork creation found a child admitted outside its atomic transition",
            ));
        }
        Ok(())
    }

    fn matches_fork_admission(&self, members: &[ForkLeg]) -> bool {
        members.iter().all(|member| {
            self.children
                .get(&member.child_activation_id)
                .is_some_and(|child| {
                    child.run_id == member.run_id
                        && child.scope_instance_id == member.scope_instance_id
                        && child.requirement == member.requirement
                })
        })
    }

    fn admit_fork_child(&mut self, member: &ForkLeg) -> Result<(), ModelError> {
        require_run(
            &self.run_id,
            &member.run_id,
            "fork member and parent scope belong to different runs",
        )?;
        match self.admit_child(
            member.child_activation_id.clone(),
            member.scope_instance_id.clone(),
            member.requirement,
        )? {
            ApplyOutcome::Applied => Ok(()),
            ApplyOutcome::Duplicate => Err(ModelError::new(
                FORK_ATOMIC_ADMISSION_CONFLICT,
                "fork child admission was not created by the atomic transition",
            )),
        }
    }

    pub fn begin_closing(&mut self) -> Result<ApplyOutcome, ModelError> {
        match self.state {
            ScopeTrackerState::Open => {
                self.state = ScopeTrackerState::Closing;
                Ok(ApplyOutcome::Applied)
            }
            ScopeTrackerState::Closing | ScopeTrackerState::Completed => {
                Ok(ApplyOutcome::Duplicate)
            }
        }
    }

    pub fn settle_child(
        &mut self,
        proof: &TerminalActivationProof,
    ) -> Result<ApplyOutcome, ModelError> {
        if proof.run_id() != &self.run_id {
            return Err(ModelError::new(
                CONTROL_RUN_MISMATCH,
                "terminal proof belongs to another run",
            ));
        }
        let settlement = ChildSettlement::from_terminal(proof.terminal()).ok_or_else(|| {
            ModelError::new(
                TERMINAL_PROOF_NOT_TERMINAL,
                "scope settlement requires a terminal Activation proof",
            )
        })?;
        let child = self
            .children
            .get_mut(proof.activation_id())
            .ok_or_else(|| {
                ModelError::new(
                    SCOPE_CHILD_UNKNOWN,
                    "scope cannot settle an Activation it did not admit",
                )
            })?;
        if proof.scope_instance_id() != &child.scope_instance_id
            || proof.activation_id() != &child.activation_id
        {
            return Err(ModelError::new(
                CONTROL_SCOPE_MISMATCH,
                "terminal proof does not match the admitted child scope",
            ));
        }
        match child.settlement {
            Some(previous) if previous == settlement => Ok(ApplyOutcome::Duplicate),
            Some(_) => Err(ModelError::new(
                SCOPE_CHILD_CONFLICT,
                "scope child already has a different terminal settlement",
            )),
            None if self.state == ScopeTrackerState::Completed => Err(ModelError::new(
                SCOPE_STATE_INVALID,
                "completed scope cannot accept a new child settlement",
            )),
            None => {
                child.settlement = Some(settlement);
                Ok(ApplyOutcome::Applied)
            }
        }
    }

    pub fn completion_blockers(&self) -> Vec<ActivationId> {
        self.children
            .iter()
            .filter(|(_, child)| child.settlement.is_none())
            .map(|(activation_id, _)| activation_id.clone())
            .collect()
    }

    pub fn complete(&mut self) -> Result<ApplyOutcome, ModelError> {
        match self.state {
            ScopeTrackerState::Open => Err(ModelError::new(
                SCOPE_STATE_INVALID,
                "scope must enter closing before completion",
            )),
            ScopeTrackerState::Completed => Ok(ApplyOutcome::Duplicate),
            ScopeTrackerState::Closing if !self.completion_blockers().is_empty() => {
                Err(ModelError::new(
                    SCOPE_COMPLETION_BLOCKED,
                    "scope has admitted children that have not settled",
                ))
            }
            ScopeTrackerState::Closing => {
                self.state = ScopeTrackerState::Completed;
                Ok(ApplyOutcome::Applied)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn run(value: &str) -> RunId {
        RunId::new(value).unwrap()
    }

    fn activation(value: &str) -> ActivationId {
        ActivationId::new(value).unwrap()
    }

    fn scope(value: &str) -> ScopeInstanceId {
        ScopeInstanceId::new(value).unwrap()
    }

    fn port(value: &str) -> PortId {
        PortId::new(value).unwrap()
    }

    fn leg(value: &str) -> LegId {
        LegId::new(value).unwrap()
    }

    fn key(domain: &str, part: &str) -> TransitionKey {
        TransitionKey::derive(domain, &[part]).unwrap()
    }

    fn value(value: serde_json::Value) -> ValueRef {
        ValueRef::inline(value).unwrap()
    }

    fn terminal_result_for(lifecycle: ActivationLifecycle) -> TerminalActivationResult {
        match lifecycle {
            ActivationLifecycle::Succeeded | ActivationLifecycle::Running => {
                let output = value(json!(null));
                TerminalActivationResult::Succeeded {
                    content_hash: output.content_hash().clone(),
                    output,
                }
            }
            ActivationLifecycle::Failed => TerminalActivationResult::Failed {
                reason: ActivationTerminationReason::Failure,
                failure: None,
            },
            ActivationLifecycle::Cancelled => TerminalActivationResult::Cancelled,
            ActivationLifecycle::TimedOut => TerminalActivationResult::TimedOut,
            ActivationLifecycle::Created
            | ActivationLifecycle::Ready
            | ActivationLifecycle::Leased
            | ActivationLifecycle::RetryWait
            | ActivationLifecycle::Waiting
            | ActivationLifecycle::Terminating => TerminalActivationResult::Cancelled,
        }
    }

    fn proof(
        run_id: &RunId,
        scope_id: &ScopeInstanceId,
        activation_id: &ActivationId,
        settlement: LegSettlement,
    ) -> TerminalActivationProof {
        let result = match &settlement {
            LegSettlement::Succeeded { output } => TerminalActivationResult::Succeeded {
                output: output.clone(),
                content_hash: output.content_hash().clone(),
            },
            LegSettlement::InfrastructureFailure => TerminalActivationResult::Failed {
                reason: ActivationTerminationReason::Failure,
                failure: Some(InternalFailureSummary::new(
                    InternalFailureKind::Infrastructure,
                    crate::InternalFailureCode::new("INFRASTRUCTURE_FAILURE").unwrap(),
                )),
            },
            LegSettlement::Panic => TerminalActivationResult::Failed {
                reason: ActivationTerminationReason::Failure,
                failure: Some(InternalFailureSummary::new(
                    InternalFailureKind::Invariant,
                    crate::InternalFailureCode::new("INVARIANT_FAILURE").unwrap(),
                )),
            },
            LegSettlement::Cancelled => TerminalActivationResult::Cancelled,
            LegSettlement::DeadlineExceeded => TerminalActivationResult::TimedOut,
        };
        let terminal = result.lifecycle();
        let proof = LegSettlementProof::mint(
            TerminalActivationProof::mint(
                run_id.clone(),
                scope_id.clone(),
                activation_id.clone(),
                terminal,
                true,
                result,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(proof.settlement(), &settlement);
        proof.terminal
    }

    fn members(run_id: &RunId) -> Vec<ForkLeg> {
        vec![
            ForkLeg::new(
                run_id.clone(),
                leg("beta"),
                port("beta"),
                scope("scope_beta"),
                activation("activation_beta"),
                ChildRequirement::Required,
            ),
            ForkLeg::new(
                run_id.clone(),
                leg("alpha"),
                port("alpha"),
                scope("scope_alpha"),
                activation("activation_alpha"),
                ChildRequirement::Optional,
            ),
        ]
    }

    fn create_fork(
        run_id: &RunId,
        ledger: &mut ControlLedger,
        parent: &mut ScopeTracker,
        inherited: Option<&OwnedControlToken>,
        suffix: &str,
    ) -> ForkCreation {
        ForkGroup::create(
            run_id.clone(),
            ForkGroupId::new(format!("fork_{suffix}")).unwrap(),
            activation(&format!("activation_fork_{suffix}")),
            parent.scope_instance_id().clone(),
            members(run_id),
            ledger,
            parent,
            key("fork.create", suffix),
            inherited,
        )
        .unwrap()
        .committed_result()
        .unwrap()
        .clone()
    }

    #[test]
    fn durable_snapshot_preserves_logical_replay_and_single_consumer_after_restart() {
        let run_id = run("run_snapshot");
        let mut ledger = ControlLedger::new(run_id.clone());
        let branch_key = key("branch.emit", "snapshot");
        let mut branch = BranchDecision::new(
            run_id.clone(),
            activation("activation_branch_snapshot"),
            scope("scope_root"),
            vec![port("then"), port("else")],
        )
        .unwrap();
        let branch_token = branch
            .select_and_emit(&mut ledger, branch_key.clone(), port("then"), None)
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();

        let merge_key = key("merge.consume", "snapshot");
        let mut merge = MergeState::new(
            run_id.clone(),
            activation("activation_merge_snapshot"),
            port("merged"),
            activation("activation_branch_snapshot"),
            scope("scope_root"),
            vec![port("then"), port("else")],
        )
        .unwrap();
        let output_token = merge
            .arrive_and_emit(&mut ledger, merge_key.clone(), &branch_token)
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();

        let encoded = serde_json::to_value(ledger.snapshot()).unwrap();
        let snapshot: PersistedControlLedgerSnapshot = serde_json::from_value(encoded).unwrap();
        let mut restored = ControlLedger::from_snapshot(snapshot).unwrap();
        let restored_input = restored.load(&run_id, branch_token.token_id()).unwrap();

        let mut replay_branch = BranchDecision::new(
            run_id.clone(),
            activation("activation_branch_snapshot"),
            scope("scope_root"),
            vec![port("then"), port("else")],
        )
        .unwrap();
        let replay = replay_branch
            .select_and_emit(
                &mut restored,
                key("branch.emit", "snapshot_after_restart"),
                port("then"),
                None,
            )
            .unwrap();
        assert!(matches!(replay, TransitionOutcome::ExactReplay { .. }));
        assert_eq!(
            replay.committed_result().unwrap().token_id(),
            branch_token.token_id()
        );

        let mut changed_intent = BranchDecision::new(
            run_id.clone(),
            activation("activation_branch_snapshot"),
            scope("scope_root"),
            vec![port("then"), port("else")],
        )
        .unwrap();
        assert!(matches!(
            changed_intent
                .select_and_emit(
                    &mut restored,
                    key("branch.emit", "snapshot_changed_after_restart"),
                    port("else"),
                    None,
                )
                .unwrap(),
            TransitionOutcome::StateConflict
        ));

        let mut replay_merge = MergeState::new(
            run_id.clone(),
            activation("activation_merge_snapshot"),
            port("merged"),
            activation("activation_branch_snapshot"),
            scope("scope_root"),
            vec![port("then"), port("else")],
        )
        .unwrap();
        let replay = replay_merge
            .arrive_and_emit(
                &mut restored,
                key("merge.consume", "snapshot_after_restart"),
                &restored_input,
            )
            .unwrap();
        assert!(matches!(replay, TransitionOutcome::ExactReplay { .. }));
        assert_eq!(
            replay.committed_result().unwrap().token_id(),
            output_token.token_id()
        );

        let mut competing_consumer = MergeState::new(
            run_id,
            activation("activation_competing_merge"),
            port("competing"),
            activation("activation_branch_snapshot"),
            scope("scope_root"),
            vec![port("then"), port("else")],
        )
        .unwrap();
        assert!(matches!(
            competing_consumer
                .arrive_and_emit(
                    &mut restored,
                    key("merge.consume", "competing"),
                    &restored_input,
                )
                .unwrap(),
            TransitionOutcome::StateConflict
        ));
    }

    #[test]
    fn snapshot_restore_fails_closed_if_consumption_authority_is_missing() {
        let run_id = run("run_snapshot_tamper");
        let mut ledger = ControlLedger::new(run_id.clone());
        let mut branch = BranchDecision::new(
            run_id.clone(),
            activation("activation_branch_tamper"),
            scope("scope_root"),
            vec![port("then")],
        )
        .unwrap();
        let input = branch
            .select_and_emit(
                &mut ledger,
                key("branch.emit", "tamper"),
                port("then"),
                None,
            )
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();
        let mut merge = MergeState::new(
            run_id,
            activation("activation_merge_tamper"),
            port("merged"),
            activation("activation_branch_tamper"),
            scope("scope_root"),
            vec![port("then")],
        )
        .unwrap();
        merge
            .arrive_and_emit(&mut ledger, key("merge.consume", "tamper"), &input)
            .unwrap();

        let mut snapshot = ledger.snapshot();
        snapshot.consumptions.clear();
        assert_eq!(
            ControlLedger::from_snapshot(snapshot).unwrap_err().code(),
            CONTROL_SNAPSHOT_INVALID
        );
    }

    #[test]
    fn snapshot_restore_rejects_swapped_emission_results_and_duplicate_logical_slots() {
        let run_id = run("run_snapshot_emission_tamper");
        let mut ledger = ControlLedger::new(run_id.clone());
        for (suffix, selected_port) in [("a", "a"), ("b", "b")] {
            let mut branch = BranchDecision::new(
                run_id.clone(),
                activation(&format!("activation_branch_{suffix}")),
                scope("scope_root"),
                vec![port(selected_port)],
            )
            .unwrap();
            branch
                .select_and_emit(
                    &mut ledger,
                    key("branch.emit", suffix),
                    port(selected_port),
                    None,
                )
                .unwrap();
        }

        let mut swapped = ledger.snapshot();
        let token_results = swapped
            .transitions
            .iter()
            .enumerate()
            .filter_map(|(index, transition)| {
                matches!(transition.result, ControlTransitionResult::Token { .. }).then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(token_results.len(), 2);
        let (left, right) = swapped.transitions.split_at_mut(token_results[1]);
        let ControlTransitionResult::Token { row: left_row, .. } =
            &mut left[token_results[0]].result
        else {
            unreachable!()
        };
        let ControlTransitionResult::Token { row: right_row, .. } = &mut right[0].result else {
            unreachable!()
        };
        std::mem::swap(left_row, right_row);
        assert_eq!(
            ControlLedger::from_snapshot(swapped).unwrap_err().code(),
            CONTROL_SNAPSHOT_INVALID
        );

        let mut duplicated_slot = ledger.snapshot();
        let first_source = duplicated_slot.token_rows[0]
            .provenance
            .source_activation_id
            .clone();
        let first_slot = duplicated_slot.token_rows[0]
            .provenance
            .emission_slot
            .clone();
        duplicated_slot.token_rows[1]
            .provenance
            .source_activation_id = first_source;
        duplicated_slot.token_rows[1].provenance.emission_slot = first_slot;
        assert_eq!(
            ControlLedger::from_snapshot(duplicated_slot)
                .unwrap_err()
                .code(),
            CONTROL_SNAPSHOT_INVALID
        );
    }

    #[test]
    fn fork_leg_slots_are_stable_distinct_and_snapshot_join_tampering_fails_closed() {
        let run_id = run("run_fork_slot_snapshot");
        let mut ledger = ControlLedger::new(run_id.clone());
        let mut parent = ScopeTracker::new(run_id.clone(), scope("scope_parent"));
        let creation = create_fork(&run_id, &mut ledger, &mut parent, None, "slot_snapshot");

        let slot_keys = creation
            .tokens()
            .iter()
            .map(|leg_token| {
                ledger
                    .row(leg_token.token())
                    .unwrap()
                    .provenance()
                    .emission_slot()
                    .storage_key()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(slot_keys.len(), creation.tokens().len());
        assert!(slot_keys.iter().all(|slot| slot.starts_with("fork_leg:")));

        let beta = creation.group().member(&leg("beta")).unwrap();
        let beta_proof = proof(
            &run_id,
            beta.scope_instance_id(),
            beta.child_activation_id(),
            LegSettlement::Succeeded {
                output: value(json!({"answer": 42})),
            },
        );
        let mut join = JoinState::new(
            run_id,
            activation("activation_join_slot_snapshot"),
            port("joined"),
            JoinMode::AllSuccess,
            creation.group(),
        )
        .unwrap();
        join.arrive(
            &mut ledger,
            key("join.arrive", "slot_snapshot_beta"),
            creation.token(&leg("beta")).unwrap(),
            &beta_proof,
        )
        .unwrap();

        let mut snapshot = ledger.snapshot();
        let arrival = snapshot
            .transitions
            .iter_mut()
            .find_map(|transition| match &mut transition.result {
                ControlTransitionResult::JoinArrived(arrival) => Some(arrival.as_mut()),
                _ => None,
            })
            .unwrap();
        arrival.child_activation_id = activation("activation_tampered_child");
        assert_eq!(
            ControlLedger::from_snapshot(snapshot).unwrap_err().code(),
            CONTROL_SNAPSHOT_INVALID
        );
    }

    #[test]
    fn ordinary_activation_consumes_then_emits_its_own_provenance_and_replays_after_restart() {
        let run_id = run("run_ordinary_gate");
        let mut ledger = ControlLedger::new(run_id.clone());
        let mut branch = BranchDecision::new(
            run_id.clone(),
            activation("activation_gate_branch"),
            scope("scope_root"),
            vec![port("then")],
        )
        .unwrap();
        let branch_token = branch
            .select_and_emit(&mut ledger, key("branch.emit", "gate"), port("then"), None)
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();

        let admit_key = key("activation.admit", "ordinary");
        let emit_key = key("activation.emit", "ordinary");
        let mut gate = ActivationControlState::new(
            run_id.clone(),
            activation("activation_llm"),
            scope("scope_root"),
            vec![port("input")],
            vec![port("done")],
        )
        .unwrap();
        assert!(matches!(
            gate.admit(&mut ledger, admit_key.clone(), port("input"), &branch_token,)
                .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            gate.admit(&mut ledger, admit_key.clone(), port("input"), &branch_token,)
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));

        let mut competing_gate = ActivationControlState::new(
            run_id.clone(),
            activation("activation_other_llm"),
            scope("scope_root"),
            vec![port("input")],
            vec![port("done")],
        )
        .unwrap();
        assert!(matches!(
            competing_gate
                .admit(
                    &mut ledger,
                    key("activation.admit", "competing"),
                    port("input"),
                    &branch_token,
                )
                .unwrap(),
            TransitionOutcome::StateConflict
        ));

        let wrong_success = TerminalActivationProof::mint(
            run_id.clone(),
            scope("scope_root"),
            activation("activation_other_llm"),
            ActivationLifecycle::Succeeded,
            true,
            terminal_result_for(ActivationLifecycle::Succeeded),
        )
        .unwrap();
        assert_eq!(
            gate.emit(&mut ledger, emit_key.clone(), port("done"), &wrong_success,)
                .unwrap_err()
                .code(),
            ACTIVATION_CONTROL_PROOF_MISMATCH
        );

        let success = TerminalActivationProof::mint(
            run_id.clone(),
            scope("scope_root"),
            activation("activation_llm"),
            ActivationLifecycle::Succeeded,
            true,
            terminal_result_for(ActivationLifecycle::Succeeded),
        )
        .unwrap();
        let ordinary_token = gate
            .emit(&mut ledger, emit_key.clone(), port("done"), &success)
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();
        let ordinary_row = ledger.row(&ordinary_token).unwrap();
        assert_eq!(
            ordinary_row.provenance.source_activation_id,
            activation("activation_llm")
        );
        assert_eq!(ordinary_row.provenance.source_port, port("done"));
        assert!(matches!(
            ordinary_row.provenance.frames.as_slice(),
            [ControlFrame::Branch(_)]
        ));

        let snapshot: PersistedControlLedgerSnapshot =
            serde_json::from_value(serde_json::to_value(ledger.snapshot()).unwrap()).unwrap();
        let mut restored = ControlLedger::from_snapshot(snapshot).unwrap();
        let restored_input = restored.load(&run_id, branch_token.token_id()).unwrap();
        let mut replay_gate = ActivationControlState::new(
            run_id.clone(),
            activation("activation_llm"),
            scope("scope_root"),
            vec![port("input")],
            vec![port("done")],
        )
        .unwrap();
        assert!(matches!(
            replay_gate
                .admit(&mut restored, admit_key, port("input"), &restored_input,)
                .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));
        let replay_output = replay_gate
            .emit(
                &mut restored,
                key("activation.emit", "ordinary_after_restart"),
                port("done"),
                &success,
            )
            .unwrap();
        assert!(matches!(
            replay_output,
            TransitionOutcome::ExactReplay { .. }
        ));
        assert_eq!(
            replay_output.committed_result().unwrap().token_id(),
            ordinary_token.token_id()
        );

        let mut merge = MergeState::new(
            run_id,
            activation("activation_gate_merge"),
            port("merged"),
            activation("activation_gate_branch"),
            scope("scope_root"),
            vec![port("then")],
        )
        .unwrap();
        let restored_output = restored
            .load(replay_gate.run_id(), ordinary_token.token_id())
            .unwrap();
        merge
            .arrive_and_emit(
                &mut restored,
                key("merge.consume", "after_ordinary"),
                &restored_output,
            )
            .unwrap();
        assert_eq!(
            merge
                .arrival()
                .unwrap()
                .input()
                .provenance()
                .source_activation_id(),
            &activation("activation_llm")
        );
    }

    #[test]
    fn root_entry_admission_is_separate_and_does_not_mint_before_success() {
        let run_id = run("run_root_gate");
        let mut ledger = ControlLedger::new(run_id.clone());
        let mut entry = ActivationControlState::new_entry(
            run_id.clone(),
            activation("activation_entry"),
            scope("scope_root"),
            vec![port("done")],
        )
        .unwrap();
        let admission = entry
            .admit_root(&mut ledger, key("activation.admit", "root"))
            .unwrap();
        assert!(admission.committed_result().unwrap().is_root_entry());
        assert!(ledger.is_empty(), "root admission does not mint a token");

        let token_gated = ActivationControlState::new(
            run_id,
            activation("activation_non_entry"),
            scope("scope_root"),
            vec![port("input")],
            vec![port("done")],
        )
        .unwrap();
        let mut token_gated = token_gated;
        assert_eq!(
            token_gated
                .admit_root(&mut ledger, key("activation.admit", "invalid_root"))
                .unwrap_err()
                .code(),
            ACTIVATION_CONTROL_MODE_INVALID
        );
    }

    #[test]
    fn proof_mint_rejects_live_activation_and_undrained_attempts() {
        let run_id = run("run_proof");
        assert_eq!(
            TerminalActivationProof::mint(
                run_id.clone(),
                scope("scope_root"),
                activation("activation_live"),
                ActivationLifecycle::Running,
                true,
                terminal_result_for(ActivationLifecycle::Running),
            )
            .unwrap_err()
            .code(),
            TERMINAL_PROOF_NOT_TERMINAL
        );
        assert_eq!(
            TerminalActivationProof::mint(
                run_id,
                scope("scope_root"),
                activation("activation_terminal"),
                ActivationLifecycle::Succeeded,
                false,
                terminal_result_for(ActivationLifecycle::Succeeded),
            )
            .unwrap_err()
            .code(),
            TERMINAL_PROOF_ATTEMPT_LIVE
        );
    }

    #[test]
    fn scope_rejects_wrong_and_unregistered_proofs_and_waits_for_optional_children() {
        let run_id = run("run_scope");
        let root = scope("scope_root");
        let child = activation("activation_child");
        let optional = activation("activation_optional");
        let mut tracker = ScopeTracker::new(run_id.clone(), root.clone());
        tracker
            .admit_child(child.clone(), root.clone(), ChildRequirement::Required)
            .unwrap();
        tracker
            .admit_child(optional.clone(), root.clone(), ChildRequirement::Optional)
            .unwrap();

        let wrong_run = TerminalActivationProof::mint(
            run("run_other"),
            root.clone(),
            child.clone(),
            ActivationLifecycle::Succeeded,
            true,
            terminal_result_for(ActivationLifecycle::Succeeded),
        )
        .unwrap();
        assert_eq!(
            tracker.settle_child(&wrong_run).unwrap_err().code(),
            CONTROL_RUN_MISMATCH
        );
        let unknown = TerminalActivationProof::mint(
            run_id.clone(),
            root.clone(),
            activation("activation_unknown"),
            ActivationLifecycle::Succeeded,
            true,
            terminal_result_for(ActivationLifecycle::Succeeded),
        )
        .unwrap();
        assert_eq!(
            tracker.settle_child(&unknown).unwrap_err().code(),
            SCOPE_CHILD_UNKNOWN
        );

        tracker.begin_closing().unwrap();
        let child_proof = TerminalActivationProof::mint(
            run_id.clone(),
            root.clone(),
            child,
            ActivationLifecycle::Succeeded,
            true,
            terminal_result_for(ActivationLifecycle::Succeeded),
        )
        .unwrap();
        tracker.settle_child(&child_proof).unwrap();
        assert_eq!(
            tracker.complete().unwrap_err().code(),
            SCOPE_COMPLETION_BLOCKED
        );
        let optional_proof = TerminalActivationProof::mint(
            run_id,
            root,
            optional,
            ActivationLifecycle::Cancelled,
            true,
            terminal_result_for(ActivationLifecycle::Cancelled),
        )
        .unwrap();
        tracker.settle_child(&optional_proof).unwrap();
        assert_eq!(tracker.complete().unwrap(), ApplyOutcome::Applied);
    }

    #[test]
    fn parent_scope_settles_a_child_in_its_real_leg_scope_only() {
        let run_id = run("run_child_scope");
        let child = activation("activation_leg_child");
        let child_scope = scope("scope_leg");
        let mut parent = ScopeTracker::new(run_id.clone(), scope("scope_root"));
        parent
            .admit_child(
                child.clone(),
                child_scope.clone(),
                ChildRequirement::Required,
            )
            .unwrap();

        let wrong_scope = TerminalActivationProof::mint(
            run_id.clone(),
            scope("scope_root"),
            child.clone(),
            ActivationLifecycle::Succeeded,
            true,
            terminal_result_for(ActivationLifecycle::Succeeded),
        )
        .unwrap();
        assert_eq!(
            parent.settle_child(&wrong_scope).unwrap_err().code(),
            CONTROL_SCOPE_MISMATCH
        );

        let real_scope = TerminalActivationProof::mint(
            run_id,
            child_scope,
            child,
            ActivationLifecycle::Succeeded,
            true,
            terminal_result_for(ActivationLifecycle::Succeeded),
        )
        .unwrap();
        assert_eq!(
            parent.settle_child(&real_scope).unwrap(),
            ApplyOutcome::Applied
        );
    }

    #[test]
    fn join_requires_matching_proof_and_orders_arrivals_by_declaration() {
        let run_id = run("run_join_order");
        let mut ledger = ControlLedger::new(run_id.clone());
        let mut parent = ScopeTracker::new(run_id.clone(), scope("scope_parent"));
        let creation = create_fork(&run_id, &mut ledger, &mut parent, None, "ordered");
        let mut join = JoinState::new(
            run_id.clone(),
            activation("activation_join"),
            port("joined"),
            JoinMode::AllSuccess,
            creation.group(),
        )
        .unwrap();

        let alpha = creation.group().member(&leg("alpha")).unwrap();
        let wrong = proof(
            &run_id,
            alpha.scope_instance_id(),
            &activation("activation_wrong"),
            LegSettlement::Succeeded {
                output: value(json!("wrong")),
            },
        );
        assert_eq!(
            join.arrive(
                &mut ledger,
                key("join.arrive", "wrong"),
                creation.token(&leg("alpha")).unwrap(),
                &wrong,
            )
            .unwrap_err()
            .code(),
            JOIN_PROOF_MISMATCH
        );

        for leg_id in [leg("alpha"), leg("beta")] {
            let member = creation.group().member(&leg_id).unwrap();
            let proof = proof(
                &run_id,
                member.scope_instance_id(),
                member.child_activation_id(),
                LegSettlement::Succeeded {
                    output: value(json!(leg_id.as_str())),
                },
            );
            assert!(matches!(
                join.arrive(
                    &mut ledger,
                    key("join.arrive", leg_id.as_str()),
                    creation.token(&leg_id).unwrap(),
                    &proof,
                )
                .unwrap(),
                TransitionOutcome::Committed { .. }
            ));
        }
        assert_eq!(
            join.arrivals_in_declaration_order()
                .unwrap()
                .iter()
                .map(|arrival| arrival.leg_id().as_str())
                .collect::<Vec<_>>(),
            vec!["beta", "alpha"]
        );
    }

    #[test]
    fn branch_and_fork_frames_unwind_in_true_lifo_order_both_directions() {
        // Branch -> Fork: Join must pop Fork first, then Merge may pop Branch.
        let run_id = run("run_branch_then_fork");
        let mut ledger = ControlLedger::new(run_id.clone());
        let mut outer_branch = BranchDecision::new(
            run_id.clone(),
            activation("activation_outer_branch"),
            scope("scope_parent"),
            vec![port("then")],
        )
        .unwrap();
        let branch_token = outer_branch
            .select_and_emit(
                &mut ledger,
                key("branch.emit", "outer_lifo"),
                port("then"),
                None,
            )
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();
        let mut parent = ScopeTracker::new(run_id.clone(), scope("scope_parent"));
        let creation = create_fork(
            &run_id,
            &mut ledger,
            &mut parent,
            Some(&branch_token),
            "branch_then_fork",
        );
        let mut join = JoinState::new(
            run_id.clone(),
            activation("activation_join_outer"),
            port("joined"),
            JoinMode::AllSuccess,
            creation.group(),
        )
        .unwrap();
        for member in creation.group().members() {
            let settlement = proof(
                &run_id,
                member.scope_instance_id(),
                member.child_activation_id(),
                LegSettlement::Succeeded {
                    output: value(json!(member.leg_id().as_str())),
                },
            );
            join.arrive(
                &mut ledger,
                key("join.arrive", &format!("outer_{}", member.leg_id())),
                creation.token(member.leg_id()).unwrap(),
                &settlement,
            )
            .unwrap();
        }
        let after_join = join
            .emit(&mut ledger, key("join.emit", "outer"))
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();
        assert!(matches!(
            ledger
                .row(&after_join)
                .unwrap()
                .provenance
                .frames
                .as_slice(),
            [ControlFrame::Branch(_)]
        ));
        let mut outer_merge = MergeState::new(
            run_id.clone(),
            activation("activation_outer_merge"),
            port("merged"),
            activation("activation_outer_branch"),
            scope("scope_parent"),
            vec![port("then")],
        )
        .unwrap();
        let final_token = outer_merge
            .arrive_and_emit(&mut ledger, key("merge.consume", "outer_lifo"), &after_join)
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();
        assert!(ledger
            .row(&final_token)
            .unwrap()
            .provenance
            .frames
            .is_empty());

        // Fork -> Branch: Join must reject while Branch is top. Merge pops the
        // Branch, after which Join may consume the exposed Fork frame.
        let run_id = run("run_fork_then_branch");
        let mut ledger = ControlLedger::new(run_id.clone());
        let mut parent = ScopeTracker::new(run_id.clone(), scope("scope_parent"));
        let creation = create_fork(&run_id, &mut ledger, &mut parent, None, "fork_then_branch");
        let beta = creation.group().member(&leg("beta")).unwrap();
        let mut inner_branch = BranchDecision::new(
            run_id.clone(),
            activation("activation_inner_branch"),
            beta.scope_instance_id().clone(),
            vec![port("inner")],
        )
        .unwrap();
        let inner_token = inner_branch
            .select_and_emit(
                &mut ledger,
                key("branch.emit", "inner_lifo"),
                port("inner"),
                Some(creation.token(beta.leg_id()).unwrap()),
            )
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();
        let beta_proof = proof(
            &run_id,
            beta.scope_instance_id(),
            beta.child_activation_id(),
            LegSettlement::Succeeded {
                output: value(json!("beta")),
            },
        );
        let mut join = JoinState::new(
            run_id.clone(),
            activation("activation_join_inner"),
            port("joined"),
            JoinMode::AllSuccess,
            creation.group(),
        )
        .unwrap();
        assert_eq!(
            join.arrive(
                &mut ledger,
                key("join.arrive", "too_early"),
                &inner_token,
                &beta_proof,
            )
            .unwrap_err()
            .code(),
            JOIN_GROUP_MISMATCH
        );
        let mut inner_merge = MergeState::new(
            run_id.clone(),
            activation("activation_inner_merge"),
            port("inner_merged"),
            activation("activation_inner_branch"),
            beta.scope_instance_id().clone(),
            vec![port("inner")],
        )
        .unwrap();
        let exposed_fork = inner_merge
            .arrive_and_emit(
                &mut ledger,
                key("merge.consume", "inner_lifo"),
                &inner_token,
            )
            .unwrap()
            .committed_result()
            .unwrap()
            .clone();
        assert!(matches!(
            ledger.row(&exposed_fork).unwrap().provenance.frames.last(),
            Some(ControlFrame::ForkLeg(_))
        ));
        assert!(matches!(
            join.arrive(
                &mut ledger,
                key("join.arrive", "beta_after_merge"),
                &exposed_fork,
                &beta_proof,
            )
            .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
    }

    #[test]
    fn untyped_business_summary_cannot_become_collectable_all_settled_data() {
        let terminal = TerminalActivationProof::mint(
            run("run_untyped_business"),
            scope("scope_untyped_business"),
            activation("activation_untyped_business"),
            ActivationLifecycle::Failed,
            true,
            TerminalActivationResult::Failed {
                reason: ActivationTerminationReason::Failure,
                failure: Some(InternalFailureSummary::new(
                    InternalFailureKind::Business,
                    crate::InternalFailureCode::new("ANALYSIS_FAILED").unwrap(),
                )),
            },
        )
        .unwrap();
        let proof = LegSettlementProof::mint(terminal).unwrap();
        assert_eq!(proof.settlement(), &LegSettlement::InfrastructureFailure);
        for settlement in [
            LegSettlement::InfrastructureFailure,
            LegSettlement::Panic,
            LegSettlement::Cancelled,
            LegSettlement::DeadlineExceeded,
        ] {
            assert_eq!(
                settlement.handling(JoinMode::AllSettled),
                SettlementHandling::FailJoin
            );
        }
    }

    #[test]
    fn join_exact_replay_is_idempotent_but_changed_settlement_conflicts() {
        let run_id = run("run_join_replay");
        let mut ledger = ControlLedger::new(run_id.clone());
        let mut parent = ScopeTracker::new(run_id.clone(), scope("scope_parent"));
        let creation = create_fork(&run_id, &mut ledger, &mut parent, None, "replay");
        let mut join = JoinState::new(
            run_id.clone(),
            activation("activation_join_replay"),
            port("joined"),
            JoinMode::AllSettled,
            creation.group(),
        )
        .unwrap();
        let beta = creation.group().member(&leg("beta")).unwrap();
        let success = proof(
            &run_id,
            beta.scope_instance_id(),
            beta.child_activation_id(),
            LegSettlement::Succeeded {
                output: value(json!(1)),
            },
        );
        let arrival_key = key("join.arrive", "beta");
        assert!(matches!(
            join.arrive(
                &mut ledger,
                key("join.arrive", "beta_new_request_key"),
                creation.token(&leg("beta")).unwrap(),
                &success,
            )
            .unwrap(),
            TransitionOutcome::Committed { .. }
        ));
        assert!(matches!(
            join.arrive(
                &mut ledger,
                arrival_key.clone(),
                creation.token(&leg("beta")).unwrap(),
                &success,
            )
            .unwrap(),
            TransitionOutcome::ExactReplay { .. }
        ));

        let failure = proof(
            &run_id,
            beta.scope_instance_id(),
            beta.child_activation_id(),
            LegSettlement::InfrastructureFailure,
        );
        assert!(matches!(
            join.arrive(
                &mut ledger,
                key("join.arrive", "beta_changed_settlement"),
                creation.token(&leg("beta")).unwrap(),
                &failure,
            )
            .unwrap(),
            TransitionOutcome::StateConflict
        ));
    }
}
