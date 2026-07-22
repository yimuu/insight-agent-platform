use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use super::{
    state::{
        ActivationTerminationReason, AdmissionState, EffectEvidence, RunLifecycle,
        TerminationReason,
    },
    ActivationId, AttemptNo, ContentHash, ControlTokenId, DefinitionRevisionId,
    DeploymentRevisionId, EffectId, ForkGroupId, LeaseEpoch, LegId, ModelError, NodeId, PortId,
    RunId, ScopeInstanceId,
};

pub const EXECUTION_EVENT_SCHEMA_VERSION: u32 = 2;
pub const PUBLIC_EVENT_SCHEMA_VERSION: u32 = 1;
pub const EXECUTION_TRANSITION_INTENT_SCHEMA_VERSION: u32 = 1;

const EVENT_SEQ_INVALID: &str = "ENGINE_EVENT_SEQ_INVALID";
const EVENT_SEQ_OVERFLOW: &str = "ENGINE_EVENT_SEQ_OVERFLOW";
const TRANSITION_KEY_INVALID: &str = "ENGINE_TRANSITION_KEY_INVALID";
const INTENT_HASH_INVALID: &str = "ENGINE_INTENT_HASH_INVALID";
const INTENT_CANONICALIZATION_FAILED: &str = "ENGINE_INTENT_CANONICALIZATION_FAILED";
pub const TRANSITION_INTENT_CONFLICT: &str = "ENGINE_TRANSITION_INTENT_CONFLICT";
const EVENT_ID_INVALID: &str = "ENGINE_EVENT_ID_INVALID";
const EVENT_CONTEXT_INVALID: &str = "ENGINE_EVENT_CONTEXT_INVALID";
const EVENT_PAYLOAD_INVALID: &str = "ENGINE_EVENT_PAYLOAD_INVALID";
const INTERNAL_FAILURE_CODE_INVALID: &str = "ENGINE_INTERNAL_FAILURE_CODE_INVALID";
const DURABLE_SUBJECT_ID_INVALID: &str = "ENGINE_DURABLE_SUBJECT_ID_INVALID";
const PUBLIC_ERROR_CODE_INVALID: &str = "ENGINE_PUBLIC_ERROR_CODE_INVALID";

const TRANSITION_KEY_PREFIX: &str = "transition_";
const TRANSITION_KEY_DOMAIN: &[u8] = b"insight-agent/transition-key/v1";
const INTENT_HASH_DOMAIN: &[u8] = b"insight-agent/transition-intent/v1";
const MAX_TRANSITION_DOMAIN_BYTES: usize = 128;
const MAX_TRANSITION_PART_BYTES: usize = 1024;
const MAX_TRANSITION_PARTS: usize = 64;
const MAX_TRANSITION_INTENT_BYTES: usize = 1024 * 1024;
const MAX_PUBLIC_ERROR_CODE_BYTES: usize = 128;
const MAX_INTERNAL_FAILURE_CODE_BYTES: usize = 128;
const MAX_DURABLE_SUBJECT_ID_BYTES: usize = 256;
const MAX_CONTROL_FRAMES: usize = 64;
const MAX_FORK_LEGS: usize = 1024;

/// Strictly positive sequence allocated independently inside each Run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventSeq(u64);

impl EventSeq {
    pub const FIRST: Self = Self(1);

    pub fn new(value: u64) -> Result<Self, ModelError> {
        if value == 0 {
            return Err(ModelError::new(
                EVENT_SEQ_INVALID,
                "event sequence must start at one",
            ));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Result<Self, ModelError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| ModelError::new(EVENT_SEQ_OVERFLOW, "event sequence overflowed"))
    }
}

impl fmt::Display for EventSeq {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for EventSeq {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for EventSeq {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

macro_rules! strict_event_id {
    ($name:ident, $prefix:literal, $label:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn random() -> Self {
                Self(format!(concat!($prefix, "{}"), Uuid::new_v4().simple()))
            }

            pub fn parse(value: impl Into<String>) -> Result<Self, ModelError> {
                let value = value.into();
                let Some(hex) = value.strip_prefix($prefix) else {
                    return Err(ModelError::new(
                        EVENT_ID_INVALID,
                        concat!($label, " has an invalid prefix"),
                    ));
                };
                if hex.len() != 32
                    || !hex
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                {
                    return Err(ModelError::new(
                        EVENT_ID_INVALID,
                        concat!($label, " must contain 32 lowercase hexadecimal digits"),
                    ));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(value).map_err(D::Error::custom)
            }
        }
    };
}

strict_event_id!(ExecutionEventId, "event_", "execution event ID");
strict_event_id!(PublicEventId, "public_event_", "public event ID");

macro_rules! durable_subject_id {
    ($name:ident, $label:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > MAX_DURABLE_SUBJECT_ID_BYTES
                    || value.chars().any(|character| {
                        character.is_control()
                            || character.is_whitespace()
                            || matches!(character, '/' | '\\')
                    })
                {
                    return Err(ModelError::new(
                        DURABLE_SUBJECT_ID_INVALID,
                        concat!($label, " must be a bounded body-free identifier"),
                    ));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(D::Error::custom)
            }
        }
    };
}

durable_subject_id!(SignalId, "signal ID");
durable_subject_id!(TimerId, "timer ID");

/// Hierarchical identity for a pending internal fact.
///
/// Authoritative event identity is deliberately absent. `event_id`, sequence,
/// and commit time are assigned only by the durable store when this context and
/// its closed payload are committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionEventContext {
    run_id: RunId,
    node_id: Option<NodeId>,
    scope_instance_id: Option<ScopeInstanceId>,
    activation_id: Option<ActivationId>,
    attempt_no: Option<AttemptNo>,
    causation_event_id: Option<ExecutionEventId>,
}

impl ExecutionEventContext {
    pub fn for_run(run_id: RunId) -> Self {
        Self {
            run_id,
            node_id: None,
            scope_instance_id: None,
            activation_id: None,
            attempt_no: None,
            causation_event_id: None,
        }
    }

    pub fn in_scope(mut self, scope_instance_id: ScopeInstanceId) -> Self {
        self.scope_instance_id = Some(scope_instance_id);
        self
    }

    pub fn at_node(mut self, scope_instance_id: ScopeInstanceId, node_id: NodeId) -> Self {
        self.scope_instance_id = Some(scope_instance_id);
        self.node_id = Some(node_id);
        self
    }

    pub fn for_activation(
        mut self,
        scope_instance_id: ScopeInstanceId,
        node_id: NodeId,
        activation_id: ActivationId,
    ) -> Self {
        self.scope_instance_id = Some(scope_instance_id);
        self.node_id = Some(node_id);
        self.activation_id = Some(activation_id);
        self
    }

    pub fn for_attempt(
        mut self,
        scope_instance_id: ScopeInstanceId,
        node_id: NodeId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
    ) -> Self {
        self.scope_instance_id = Some(scope_instance_id);
        self.node_id = Some(node_id);
        self.activation_id = Some(activation_id);
        self.attempt_no = Some(attempt_no);
        self
    }

    pub fn caused_by(mut self, event_id: ExecutionEventId) -> Self {
        self.causation_event_id = Some(event_id);
        self
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    pub fn scope_instance_id(&self) -> Option<&ScopeInstanceId> {
        self.scope_instance_id.as_ref()
    }

    pub fn activation_id(&self) -> Option<&ActivationId> {
        self.activation_id.as_ref()
    }

    pub fn attempt_no(&self) -> Option<AttemptNo> {
        self.attempt_no
    }

    pub fn causation_event_id(&self) -> Option<&ExecutionEventId> {
        self.causation_event_id.as_ref()
    }

    fn validate_hierarchy(&self) -> Result<(), ModelError> {
        if self.attempt_no.is_some() && self.activation_id.is_none() {
            return Err(ModelError::new(
                EVENT_CONTEXT_INVALID,
                "attempt event identity requires an activation ID",
            ));
        }
        if self.activation_id.is_some()
            && (self.node_id.is_none() || self.scope_instance_id.is_none())
        {
            return Err(ModelError::new(
                EVENT_CONTEXT_INVALID,
                "activation event identity requires node and scope IDs",
            ));
        }
        if self.node_id.is_some() && self.scope_instance_id.is_none() {
            return Err(ModelError::new(
                EVENT_CONTEXT_INVALID,
                "node event identity requires a scope instance ID",
            ));
        }
        Ok(())
    }

    fn validate_committed_event_id(&self, event_id: &ExecutionEventId) -> Result<(), ModelError> {
        if self.causation_event_id.as_ref() == Some(event_id) {
            return Err(ModelError::new(
                EVENT_CONTEXT_INVALID,
                "execution event cannot cause itself",
            ));
        }
        Ok(())
    }
}

/// Identity and internal causation for a public projection event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicEventContext {
    public_event_id: PublicEventId,
    run_id: RunId,
    causation_event_id: ExecutionEventId,
}

impl PublicEventContext {
    pub fn new(
        public_event_id: PublicEventId,
        run_id: RunId,
        causation_event_id: ExecutionEventId,
    ) -> Self {
        Self {
            public_event_id,
            run_id,
            causation_event_id,
        }
    }

    pub fn random(run_id: RunId, causation_event_id: ExecutionEventId) -> Self {
        Self::new(PublicEventId::random(), run_id, causation_event_id)
    }

    pub fn public_event_id(&self) -> &PublicEventId {
        &self.public_event_id
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn causation_event_id(&self) -> &ExecutionEventId {
        &self.causation_event_id
    }
}

/// Stable idempotency identity for one logical state transition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransitionKey(String);

impl TransitionKey {
    /// Derives a key with length-prefixed framing so different component
    /// boundaries can never produce the same preimage.
    pub fn derive(domain: &str, parts: &[&str]) -> Result<Self, ModelError> {
        validate_transition_components(domain, parts)?;

        let mut framed = Vec::new();
        append_framed(&mut framed, TRANSITION_KEY_DOMAIN)?;
        append_framed(&mut framed, domain.as_bytes())?;
        let part_count = u64::try_from(parts.len()).map_err(|_| {
            ModelError::new(
                TRANSITION_KEY_INVALID,
                "transition key has too many identity parts",
            )
        })?;
        framed.extend_from_slice(&part_count.to_be_bytes());
        for part in parts {
            append_framed(&mut framed, part.as_bytes())?;
        }

        let hash = ContentHash::from_bytes(&framed);
        Ok(Self(format!(
            "{TRANSITION_KEY_PREFIX}{}",
            hash.as_str().trim_start_matches("sha256:")
        )))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        let Some(hex) = value.strip_prefix(TRANSITION_KEY_PREFIX) else {
            return Err(ModelError::new(
                TRANSITION_KEY_INVALID,
                "transition key must use the transition_<sha256> form",
            ));
        };
        if !is_lower_sha256(hex) {
            return Err(ModelError::new(
                TRANSITION_KEY_INVALID,
                "transition key must contain 64 lowercase hexadecimal digits",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TransitionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for TransitionKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for TransitionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(D::Error::custom)
    }
}

/// Canonical hash of the complete transition intent associated with a key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntentHash(ContentHash);

impl IntentHash {
    pub fn from_serializable<T>(intent: &T) -> Result<Self, ModelError>
    where
        T: Serialize + ?Sized,
    {
        let canonical = serde_jcs::to_vec(intent).map_err(|_| {
            ModelError::new(
                INTENT_CANONICALIZATION_FAILED,
                "transition intent could not be canonicalized",
            )
        })?;
        if canonical.len() > MAX_TRANSITION_INTENT_BYTES {
            return Err(ModelError::new(
                INTENT_CANONICALIZATION_FAILED,
                "transition intent exceeds the canonical size limit",
            ));
        }
        let mut framed = Vec::with_capacity(
            INTENT_HASH_DOMAIN.len() + std::mem::size_of::<u64>() + canonical.len(),
        );
        framed.extend_from_slice(INTENT_HASH_DOMAIN);
        framed.extend_from_slice(
            &u64::try_from(canonical.len())
                .expect("bounded transition intent length fits u64")
                .to_be_bytes(),
        );
        framed.extend_from_slice(&canonical);
        Ok(Self(ContentHash::from_bytes(&framed)))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, ModelError> {
        ContentHash::parse(value).map(Self).map_err(|_| {
            ModelError::new(
                INTENT_HASH_INVALID,
                "intent hash must use the sha256:<lowercase hex> form",
            )
        })
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.0
    }
}

impl fmt::Display for IntentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for IntentHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for IntentHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(D::Error::custom)
    }
}

/// The indivisible idempotency identity of one logical state transition.
///
/// The constructor is intentionally private: callers obtain an identity only
/// from `TransitionRequest::new`, which computes the intent hash from the
/// canonical serialized intent rather than accepting a caller-supplied hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransitionIdentity {
    run_id: RunId,
    key: TransitionKey,
    intent_hash: IntentHash,
}

impl TransitionIdentity {
    fn new(run_id: RunId, key: TransitionKey, intent_hash: IntentHash) -> Self {
        Self {
            run_id,
            key,
            intent_hash,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn key(&self) -> &TransitionKey {
        &self.key
    }

    pub fn intent_hash(&self) -> &IntentHash {
        &self.intent_hash
    }
}

/// A typed transition request whose identity cannot be separated from its
/// canonical intent hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransitionRequest<I> {
    identity: TransitionIdentity,
    intent: I,
}

impl<I> TransitionRequest<I>
where
    I: Serialize,
{
    pub fn new(run_id: RunId, key: TransitionKey, intent: I) -> Result<Self, ModelError> {
        let intent_hash = IntentHash::from_serializable(&intent)?;
        Ok(Self {
            identity: TransitionIdentity::new(run_id, key, intent_hash),
            intent,
        })
    }
}

impl<I> TransitionRequest<I> {
    pub fn identity(&self) -> &TransitionIdentity {
        &self.identity
    }

    pub fn intent(&self) -> &I {
        &self.intent
    }

    pub fn into_intent(self) -> I {
        self.intent
    }
}

/// Result of authoritative fence and expected-state checks for a transition
/// that has no previously committed idempotency record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionPrecondition {
    Current,
    StaleLease,
    StateConflict,
}

#[derive(Debug, Clone)]
struct TransitionRecord<R> {
    intent_hash: IntentHash,
    result: R,
}

/// Pure reference model for the repository's idempotency unique-key contract.
///
/// Existing records are checked before lease status so an exact replay returns
/// the authoritative result even if its worker lease has since become stale.
#[derive(Debug, Clone)]
pub struct TransitionLedger<R> {
    records: BTreeMap<(RunId, TransitionKey), TransitionRecord<R>>,
}

impl<R> TransitionLedger<R> {
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl<R> TransitionLedger<R>
where
    R: Clone,
{
    pub fn apply<I>(
        &mut self,
        request: &TransitionRequest<I>,
        precondition: TransitionPrecondition,
        apply: impl FnOnce() -> R,
    ) -> Result<TransitionOutcome<R>, ModelError> {
        let identity = request.identity();
        let record_key = (identity.run_id().clone(), identity.key().clone());
        if let Some(record) = self.records.get(&record_key) {
            return if record.intent_hash == *identity.intent_hash() {
                Ok(TransitionOutcome::ExactReplay {
                    authoritative: record.result.clone(),
                })
            } else {
                Err(ModelError::new(
                    TRANSITION_INTENT_CONFLICT,
                    "transition key is already bound to a different canonical intent",
                ))
            };
        }

        match precondition {
            TransitionPrecondition::Current => {}
            TransitionPrecondition::StaleLease => return Ok(TransitionOutcome::StaleLease),
            TransitionPrecondition::StateConflict => return Ok(TransitionOutcome::StateConflict),
        }

        let result = apply();
        self.records.insert(
            record_key,
            TransitionRecord {
                intent_hash: identity.intent_hash().clone(),
                result: result.clone(),
            },
        );
        Ok(TransitionOutcome::Committed { result })
    }
}

impl<R> Default for TransitionLedger<R> {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! string_wire_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($variant:ident => $wire:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            const WIRE_VALUES: &'static [&'static str] = &[$($wire),+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire),+
                }
            }

            pub fn parse(value: &str) -> Option<Self> {
                match value {
                    $($wire => Some(Self::$variant)),+,
                    _ => None,
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(&value)
                    .ok_or_else(|| D::Error::unknown_variant(&value, Self::WIRE_VALUES))
            }
        }
    };
}

string_wire_enum! {
    /// Stable names for authoritative execution facts. Payload schemas are
    /// versioned separately from the public event protocol.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ExecutionEventKind {
        RunCreated => "run.created",
        RunLifecycleChanged => "run.lifecycle_changed",
        RunAdmissionChanged => "run.admission_changed",
        RunTerminationClaimed => "run.termination_claimed",
        ScopeCreated => "scope.created",
        ScopeDraining => "scope.draining",
        ScopeSettled => "scope.settled",
        ActivationCreated => "activation.created",
        ActivationReady => "activation.ready",
        ActivationLeased => "activation.leased",
        ActivationRunning => "activation.running",
        ActivationRetryWait => "activation.retry_wait",
        ActivationWaiting => "activation.waiting",
        ActivationTerminating => "activation.terminating",
        ActivationSucceeded => "activation.succeeded",
        ActivationFailed => "activation.failed",
        ActivationCancelled => "activation.cancelled",
        ActivationTimedOut => "activation.timed_out",
        AttemptCreated => "attempt.created",
        AttemptLeased => "attempt.leased",
        AttemptRunning => "attempt.running",
        AttemptSucceeded => "attempt.succeeded",
        AttemptFailed => "attempt.failed",
        AttemptTimedOut => "attempt.timed_out",
        AttemptAbandoned => "attempt.abandoned",
        AttemptCancelled => "attempt.cancelled",
        EffectEvidenceRecorded => "effect.evidence_recorded",
        ControlTokenEmitted => "control_token.emitted",
        ControlTokenConsumed => "control_token.consumed",
        ControlTokenRevoked => "control_token.revoked",
        ForkCreated => "fork.created",
        JoinArrived => "join.arrived",
        JoinCompleted => "join.completed",
        SignalReceived => "signal.received",
        SignalLate => "signal.late",
        TimerScheduled => "timer.scheduled",
        TimerFired => "timer.fired",
        TimerLate => "timer.late",
        ProjectionMutated => "projection.mutated"
    }
}

string_wire_enum! {
    /// Repository-owned mutation anchors for projections whose delivery/claim
    /// state changes independently of a scheduler transition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ProjectionMutationKind {
        SignalReceived => "signal_received",
        TimerCancelled => "timer_cancelled",
        TaskClaimed => "task_claimed",
        TaskPublished => "task_published",
        TaskAcknowledged => "task_acknowledged",
        SchedulerValuesCommitted => "scheduler_values_committed",
        SchedulerControlCommitted => "scheduler_control_committed",
        ReuseCandidateCreated => "reuse_candidate_created",
        ReuseCandidateRejected => "reuse_candidate_rejected",
        RecoveryMigrationIntent => "recovery_migration_intent",
        SchedulerLeaseClaimed => "scheduler_lease_claimed",
        SchedulerLeaseHeartbeat => "scheduler_lease_heartbeat",
        SchedulerLeaseReleased => "scheduler_lease_released",
        HumanWorkItemMutated => "human_work_item_mutated",
        ArtifactRetentionReleased => "artifact_retention_released"
    }
}

/// Body-free summary of a durable value. Full outputs remain in the payload or
/// artifact store and cannot be embedded in an execution event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionValueSummary {
    content_hash: ContentHash,
    size_bytes: u64,
}

impl ExecutionValueSummary {
    pub fn new(content_hash: ContentHash, size_bytes: u64) -> Self {
        Self {
            content_hash,
            size_bytes,
        }
    }

    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

/// Stable event-local settlement taxonomy. It intentionally does not depend on
/// scheduler/control implementation types, which may evolve independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionLegSettlementClass {
    Succeeded,
    SafeFailure,
    InfrastructureFailure,
    Panic,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InternalFailureKind {
    Business,
    Workflow,
    Timeout,
    Infrastructure,
    Cancelled,
    EffectOutcomeUnknown,
    Invariant,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InternalFailureCode(String);

impl InternalFailureCode {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        let mut bytes = value.bytes();
        let valid_first = bytes.next().is_some_and(|byte| byte.is_ascii_uppercase());
        if value.len() > MAX_INTERNAL_FAILURE_CODE_BYTES
            || !valid_first
            || !bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(ModelError::new(
                INTERNAL_FAILURE_CODE_INVALID,
                "internal failure code must be a bounded uppercase identifier",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InternalFailureCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for InternalFailureCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for InternalFailureCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InternalFailureSummary {
    kind: InternalFailureKind,
    code: InternalFailureCode,
}

impl InternalFailureSummary {
    pub fn new(kind: InternalFailureKind, code: InternalFailureCode) -> Self {
        Self { kind, code }
    }

    pub fn kind(&self) -> InternalFailureKind {
        self.kind
    }

    pub fn code(&self) -> &InternalFailureCode {
        &self.code
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionScopeKind {
    Root,
    MapItem,
    LoopIteration,
    SubflowInvocation,
    AgentLoopTurn,
    ParallelLeg,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionControlFrame {
    Branch {
        branch_activation_id: ActivationId,
        selected_port: PortId,
        scope_instance_id: ScopeInstanceId,
    },
    ForkLeg {
        fork_activation_id: ActivationId,
        fork_group_id: ForkGroupId,
        leg_id: LegId,
        scope_instance_id: ScopeInstanceId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionForkLeg {
    leg_id: LegId,
    output_port: PortId,
    scope_instance_id: ScopeInstanceId,
}

impl ExecutionForkLeg {
    pub fn new(leg_id: LegId, output_port: PortId, scope_instance_id: ScopeInstanceId) -> Self {
        Self {
            leg_id,
            output_port,
            scope_instance_id,
        }
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionJoinMode {
    AllSuccess,
    AllSettled,
}

/// Closed internal event payload. It deliberately has no arbitrary JSON,
/// string error body, provider response, prompt/message, or full output slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionEventPayload {
    RunCreated {
        definition_revision_id: DefinitionRevisionId,
        deployment_revision_id: DeploymentRevisionId,
        /// Root Run deadline fixed by the database clock in the same
        /// transaction that creates the Run. `None` means no root deadline.
        run_deadline_at: Option<DateTime<Utc>>,
    },
    RunLifecycleChanged {
        lifecycle: RunLifecycle,
    },
    RunAdmissionChanged {
        admission: AdmissionState,
    },
    RunTerminationClaimed {
        reason: TerminationReason,
    },
    ScopeCreated {
        scope_kind: ExecutionScopeKind,
        parent_scope_instance_id: Option<ScopeInstanceId>,
    },
    ScopeDraining {
        admitted_children: u32,
        settled_children: u32,
        live_attempts: u32,
    },
    ScopeSettled {
        admitted_children: u32,
        settled_children: u32,
        live_attempts: u32,
    },
    ActivationCreated {
        effect_id: Option<EffectId>,
    },
    ActivationReady,
    ActivationLeased {
        attempt_no: AttemptNo,
        lease_epoch: LeaseEpoch,
    },
    ActivationRunning {
        attempt_no: AttemptNo,
    },
    ActivationRetryWait {
        attempt_no: AttemptNo,
    },
    ActivationWaiting,
    ActivationTerminating {
        reason: ActivationTerminationReason,
    },
    ActivationSucceeded {
        attempt_no: Option<AttemptNo>,
        output: Option<ExecutionValueSummary>,
    },
    ActivationFailed {
        attempt_no: Option<AttemptNo>,
        reason: ActivationTerminationReason,
        /// Some terminal causes (for example an exhausted lease-loss path)
        /// have no truthful worker/provider failure summary. Absence is
        /// authoritative and must never be filled with a synthetic error.
        failure: Option<InternalFailureSummary>,
    },
    ActivationCancelled,
    ActivationTimedOut,
    AttemptCreated {
        lease_epoch: LeaseEpoch,
    },
    AttemptLeased {
        lease_epoch: LeaseEpoch,
    },
    AttemptRunning {
        lease_epoch: LeaseEpoch,
    },
    AttemptSucceeded {
        output: Option<ExecutionValueSummary>,
    },
    AttemptFailed {
        failure: Option<InternalFailureSummary>,
    },
    AttemptTimedOut,
    AttemptAbandoned {
        effect_evidence: EffectEvidence,
    },
    AttemptCancelled,
    EffectEvidenceRecorded {
        evidence: EffectEvidence,
    },
    ControlTokenEmitted {
        token_id: ControlTokenId,
        source_port: PortId,
        token_scope_instance_id: ScopeInstanceId,
        frames: Vec<ExecutionControlFrame>,
    },
    ControlTokenConsumed {
        token_id: ControlTokenId,
    },
    ControlTokenRevoked {
        token_id: ControlTokenId,
    },
    ForkCreated {
        fork_group_id: ForkGroupId,
        legs: Vec<ExecutionForkLeg>,
    },
    JoinArrived {
        fork_group_id: ForkGroupId,
        leg_id: LegId,
        token_id: ControlTokenId,
        settlement: ExecutionLegSettlementClass,
        value: Option<ExecutionValueSummary>,
    },
    JoinCompleted {
        fork_group_id: ForkGroupId,
        mode: ExecutionJoinMode,
        settled_leg_count: u32,
    },
    SignalReceived {
        signal_id: SignalId,
        value: Option<ExecutionValueSummary>,
    },
    SignalLate {
        signal_id: SignalId,
        value: Option<ExecutionValueSummary>,
    },
    TimerScheduled {
        timer_id: TimerId,
        fire_at: DateTime<Utc>,
    },
    TimerFired {
        timer_id: TimerId,
    },
    TimerLate {
        timer_id: TimerId,
    },
    ProjectionMutated {
        mutation: ProjectionMutationKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionContextLevel {
    Run,
    Scope,
    Activation,
    Attempt,
}

impl ExecutionEventPayload {
    pub const fn kind(&self) -> ExecutionEventKind {
        match self {
            Self::RunCreated { .. } => ExecutionEventKind::RunCreated,
            Self::RunLifecycleChanged { .. } => ExecutionEventKind::RunLifecycleChanged,
            Self::RunAdmissionChanged { .. } => ExecutionEventKind::RunAdmissionChanged,
            Self::RunTerminationClaimed { .. } => ExecutionEventKind::RunTerminationClaimed,
            Self::ScopeCreated { .. } => ExecutionEventKind::ScopeCreated,
            Self::ScopeDraining { .. } => ExecutionEventKind::ScopeDraining,
            Self::ScopeSettled { .. } => ExecutionEventKind::ScopeSettled,
            Self::ActivationCreated { .. } => ExecutionEventKind::ActivationCreated,
            Self::ActivationReady => ExecutionEventKind::ActivationReady,
            Self::ActivationLeased { .. } => ExecutionEventKind::ActivationLeased,
            Self::ActivationRunning { .. } => ExecutionEventKind::ActivationRunning,
            Self::ActivationRetryWait { .. } => ExecutionEventKind::ActivationRetryWait,
            Self::ActivationWaiting => ExecutionEventKind::ActivationWaiting,
            Self::ActivationTerminating { .. } => ExecutionEventKind::ActivationTerminating,
            Self::ActivationSucceeded { .. } => ExecutionEventKind::ActivationSucceeded,
            Self::ActivationFailed { .. } => ExecutionEventKind::ActivationFailed,
            Self::ActivationCancelled => ExecutionEventKind::ActivationCancelled,
            Self::ActivationTimedOut => ExecutionEventKind::ActivationTimedOut,
            Self::AttemptCreated { .. } => ExecutionEventKind::AttemptCreated,
            Self::AttemptLeased { .. } => ExecutionEventKind::AttemptLeased,
            Self::AttemptRunning { .. } => ExecutionEventKind::AttemptRunning,
            Self::AttemptSucceeded { .. } => ExecutionEventKind::AttemptSucceeded,
            Self::AttemptFailed { .. } => ExecutionEventKind::AttemptFailed,
            Self::AttemptTimedOut => ExecutionEventKind::AttemptTimedOut,
            Self::AttemptAbandoned { .. } => ExecutionEventKind::AttemptAbandoned,
            Self::AttemptCancelled => ExecutionEventKind::AttemptCancelled,
            Self::EffectEvidenceRecorded { .. } => ExecutionEventKind::EffectEvidenceRecorded,
            Self::ControlTokenEmitted { .. } => ExecutionEventKind::ControlTokenEmitted,
            Self::ControlTokenConsumed { .. } => ExecutionEventKind::ControlTokenConsumed,
            Self::ControlTokenRevoked { .. } => ExecutionEventKind::ControlTokenRevoked,
            Self::ForkCreated { .. } => ExecutionEventKind::ForkCreated,
            Self::JoinArrived { .. } => ExecutionEventKind::JoinArrived,
            Self::JoinCompleted { .. } => ExecutionEventKind::JoinCompleted,
            Self::SignalReceived { .. } => ExecutionEventKind::SignalReceived,
            Self::SignalLate { .. } => ExecutionEventKind::SignalLate,
            Self::TimerScheduled { .. } => ExecutionEventKind::TimerScheduled,
            Self::TimerFired { .. } => ExecutionEventKind::TimerFired,
            Self::TimerLate { .. } => ExecutionEventKind::TimerLate,
            Self::ProjectionMutated { .. } => ExecutionEventKind::ProjectionMutated,
        }
    }

    const fn context_level(&self) -> ExecutionContextLevel {
        match self {
            Self::RunCreated { .. }
            | Self::RunLifecycleChanged { .. }
            | Self::RunAdmissionChanged { .. }
            | Self::RunTerminationClaimed { .. }
            | Self::ProjectionMutated { .. } => ExecutionContextLevel::Run,
            Self::ScopeCreated { .. } | Self::ScopeDraining { .. } | Self::ScopeSettled { .. } => {
                ExecutionContextLevel::Scope
            }
            Self::ActivationCreated { .. }
            | Self::ActivationReady
            | Self::ActivationLeased { .. }
            | Self::ActivationRunning { .. }
            | Self::ActivationRetryWait { .. }
            | Self::ActivationWaiting
            | Self::ActivationTerminating { .. }
            | Self::ActivationSucceeded { .. }
            | Self::ActivationFailed { .. }
            | Self::ActivationCancelled
            | Self::ActivationTimedOut
            | Self::ControlTokenEmitted { .. }
            | Self::ControlTokenConsumed { .. }
            | Self::ControlTokenRevoked { .. }
            | Self::ForkCreated { .. }
            | Self::JoinArrived { .. }
            | Self::JoinCompleted { .. }
            | Self::SignalReceived { .. }
            | Self::SignalLate { .. }
            | Self::TimerScheduled { .. }
            | Self::TimerFired { .. }
            | Self::TimerLate { .. } => ExecutionContextLevel::Activation,
            Self::AttemptCreated { .. }
            | Self::AttemptLeased { .. }
            | Self::AttemptRunning { .. }
            | Self::AttemptSucceeded { .. }
            | Self::AttemptFailed { .. }
            | Self::AttemptTimedOut
            | Self::AttemptAbandoned { .. }
            | Self::AttemptCancelled
            | Self::EffectEvidenceRecorded { .. } => ExecutionContextLevel::Attempt,
        }
    }

    fn validate_context(&self, context: &ExecutionEventContext) -> Result<(), ModelError> {
        context.validate_hierarchy()?;
        let actual = match (
            context.scope_instance_id.is_some(),
            context.node_id.is_some(),
            context.activation_id.is_some(),
            context.attempt_no.is_some(),
        ) {
            (false, false, false, false) => ExecutionContextLevel::Run,
            (true, false, false, false) => ExecutionContextLevel::Scope,
            (true, true, true, false) => ExecutionContextLevel::Activation,
            (true, true, true, true) => ExecutionContextLevel::Attempt,
            _ => {
                return Err(ModelError::new(
                    EVENT_CONTEXT_INVALID,
                    "execution event context has an incomplete identity level",
                ));
            }
        };
        if actual != self.context_level() {
            return Err(ModelError::new(
                EVENT_CONTEXT_INVALID,
                format!(
                    "execution payload {} requires {:?} context, got {:?}",
                    self.kind(),
                    self.context_level(),
                    actual
                ),
            ));
        }

        match self {
            Self::ScopeCreated {
                scope_kind,
                parent_scope_instance_id,
            } => {
                let hierarchy_is_valid = matches!(
                    (scope_kind, parent_scope_instance_id),
                    (ExecutionScopeKind::Root, None)
                        | (
                            ExecutionScopeKind::MapItem
                                | ExecutionScopeKind::LoopIteration
                                | ExecutionScopeKind::SubflowInvocation
                                | ExecutionScopeKind::AgentLoopTurn
                                | ExecutionScopeKind::ParallelLeg,
                            Some(_)
                        )
                );
                if !hierarchy_is_valid
                    || parent_scope_instance_id.as_ref() == context.scope_instance_id.as_ref()
                {
                    return Err(ModelError::new(
                        EVENT_PAYLOAD_INVALID,
                        "scope creation carries an inconsistent kind or parent",
                    ));
                }
            }
            Self::ScopeDraining {
                admitted_children,
                settled_children,
                ..
            } if settled_children > admitted_children => {
                return Err(ModelError::new(
                    EVENT_PAYLOAD_INVALID,
                    "scope draining settled count exceeds admitted count",
                ));
            }
            Self::ScopeSettled {
                admitted_children,
                settled_children,
                live_attempts,
            } if admitted_children != settled_children || *live_attempts != 0 => {
                return Err(ModelError::new(
                    EVENT_PAYLOAD_INVALID,
                    "settled scope must have all children settled and no live attempt",
                ));
            }
            Self::ControlTokenEmitted {
                token_scope_instance_id,
                frames,
                ..
            } => {
                if frames.len() > MAX_CONTROL_FRAMES {
                    return Err(ModelError::new(
                        EVENT_PAYLOAD_INVALID,
                        "control token carries too many structured frames",
                    ));
                }
                let top_scope = frames.last().map(|frame| match frame {
                    ExecutionControlFrame::Branch {
                        scope_instance_id, ..
                    }
                    | ExecutionControlFrame::ForkLeg {
                        scope_instance_id, ..
                    } => scope_instance_id,
                });
                if top_scope.is_some_and(|scope| scope != token_scope_instance_id) {
                    return Err(ModelError::new(
                        EVENT_PAYLOAD_INVALID,
                        "control token scope does not match its top structured frame",
                    ));
                }
            }
            Self::ForkCreated { legs, .. } => {
                if legs.is_empty() || legs.len() > MAX_FORK_LEGS {
                    return Err(ModelError::new(
                        EVENT_PAYLOAD_INVALID,
                        "fork creation requires a bounded non-empty member snapshot",
                    ));
                }
                let leg_ids = legs
                    .iter()
                    .map(|leg| leg.leg_id.clone())
                    .collect::<BTreeSet<_>>();
                let scopes = legs
                    .iter()
                    .map(|leg| leg.scope_instance_id.clone())
                    .collect::<BTreeSet<_>>();
                let output_ports = legs
                    .iter()
                    .map(|leg| leg.output_port.clone())
                    .collect::<BTreeSet<_>>();
                if leg_ids.len() != legs.len()
                    || scopes.len() != legs.len()
                    || output_ports.len() != legs.len()
                {
                    return Err(ModelError::new(
                        EVENT_PAYLOAD_INVALID,
                        "fork member snapshot contains a duplicate leg, scope, or output port",
                    ));
                }
            }
            Self::JoinArrived {
                settlement, value, ..
            } => {
                let value_is_required = matches!(
                    settlement,
                    ExecutionLegSettlementClass::Succeeded
                        | ExecutionLegSettlementClass::SafeFailure
                );
                if value_is_required != value.is_some() {
                    return Err(ModelError::new(
                        EVENT_PAYLOAD_INVALID,
                        "join settlement value does not match its closed settlement class",
                    ));
                }
            }
            Self::JoinCompleted {
                settled_leg_count: 0,
                ..
            } => {
                return Err(ModelError::new(
                    EVENT_PAYLOAD_INVALID,
                    "join completion must cover at least one settled leg",
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

/// A validated internal fact that has not yet received authoritative store
/// commit metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingExecutionEvent {
    context: ExecutionEventContext,
    payload: ExecutionEventPayload,
}

/// Canonical execution intent whose hash includes the command and the complete
/// pending event context, causation, and closed payload. Store-assigned event
/// identity, sequence, and commit time are intentionally excluded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionTransitionIntent<C> {
    schema_version: u32,
    command: C,
    event: PendingExecutionEvent,
}

impl<C> ExecutionTransitionIntent<C> {
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn command(&self) -> &C {
        &self.command
    }

    pub fn event(&self) -> &PendingExecutionEvent {
        &self.event
    }
}

impl<C> TransitionRequest<ExecutionTransitionIntent<C>>
where
    C: Serialize,
{
    pub fn for_execution(
        key: TransitionKey,
        command: C,
        event: PendingExecutionEvent,
    ) -> Result<Self, ModelError> {
        let run_id = event.context.run_id.clone();
        Self::new(
            run_id,
            key,
            ExecutionTransitionIntent {
                schema_version: EXECUTION_TRANSITION_INTENT_SCHEMA_VERSION,
                command,
                event,
            },
        )
    }

    #[allow(dead_code)]
    pub(crate) fn commit_event(
        &self,
        metadata: ExecutionEventCommitMetadata,
    ) -> Result<ExecutionEventEnvelope, ModelError> {
        self.intent.event.clone().commit(metadata, &self.identity)
    }
}

impl PendingExecutionEvent {
    pub fn new(
        context: ExecutionEventContext,
        payload: ExecutionEventPayload,
    ) -> Result<Self, ModelError> {
        payload.validate_context(&context)?;
        Ok(Self { context, payload })
    }

    pub fn context(&self) -> &ExecutionEventContext {
        &self.context
    }

    pub fn kind(&self) -> ExecutionEventKind {
        self.payload.kind()
    }

    pub fn payload(&self) -> &ExecutionEventPayload {
        &self.payload
    }

    fn commit(
        self,
        metadata: ExecutionEventCommitMetadata,
        transition: &TransitionIdentity,
    ) -> Result<ExecutionEventEnvelope, ModelError> {
        self.payload.validate_context(&self.context)?;
        self.context
            .validate_committed_event_id(&metadata.event_id)?;
        if transition.run_id != self.context.run_id {
            return Err(ModelError::new(
                EVENT_CONTEXT_INVALID,
                "transition and pending event belong to different runs",
            ));
        }
        Ok(ExecutionEventEnvelope {
            schema_version: EXECUTION_EVENT_SCHEMA_VERSION,
            event_id: metadata.event_id,
            run_id: self.context.run_id,
            transition_key: transition.key.clone(),
            intent_hash: transition.intent_hash.clone(),
            seq: metadata.seq,
            occurred_at: metadata.occurred_at,
            kind: self.payload.kind(),
            node_id: self.context.node_id,
            scope_instance_id: self.context.scope_instance_id,
            activation_id: self.context.activation_id,
            attempt_no: self.context.attempt_no,
            causation_event_id: self.context.causation_event_id,
            payload: self.payload,
        })
    }
}

/// Store-owned metadata allocated in the same transaction as the ledger row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct ExecutionEventCommitMetadata {
    event_id: ExecutionEventId,
    seq: EventSeq,
    occurred_at: DateTime<Utc>,
}

impl ExecutionEventCommitMetadata {
    #[allow(dead_code)]
    pub(crate) fn new(
        event_id: ExecutionEventId,
        seq: EventSeq,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            event_id,
            seq,
            occurred_at,
        }
    }
}

/// Versioned authoritative execution fact. There is no public constructor;
/// stores commit a validated `PendingExecutionEvent` with transaction-owned
/// metadata, while recovery may deserialize a fail-closed stored envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionEventEnvelope {
    schema_version: u32,
    event_id: ExecutionEventId,
    run_id: RunId,
    transition_key: TransitionKey,
    intent_hash: IntentHash,
    seq: EventSeq,
    occurred_at: DateTime<Utc>,
    kind: ExecutionEventKind,
    node_id: Option<NodeId>,
    scope_instance_id: Option<ScopeInstanceId>,
    activation_id: Option<ActivationId>,
    attempt_no: Option<AttemptNo>,
    causation_event_id: Option<ExecutionEventId>,
    payload: ExecutionEventPayload,
}

impl ExecutionEventEnvelope {
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn event_id(&self) -> &ExecutionEventId {
        &self.event_id
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn transition_key(&self) -> &TransitionKey {
        &self.transition_key
    }

    pub fn intent_hash(&self) -> &IntentHash {
        &self.intent_hash
    }

    pub fn seq(&self) -> EventSeq {
        self.seq
    }

    pub fn occurred_at(&self) -> &DateTime<Utc> {
        &self.occurred_at
    }

    pub fn kind(&self) -> ExecutionEventKind {
        self.kind
    }

    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    pub fn scope_instance_id(&self) -> Option<&ScopeInstanceId> {
        self.scope_instance_id.as_ref()
    }

    pub fn activation_id(&self) -> Option<&ActivationId> {
        self.activation_id.as_ref()
    }

    pub fn attempt_no(&self) -> Option<AttemptNo> {
        self.attempt_no
    }

    pub fn causation_event_id(&self) -> Option<&ExecutionEventId> {
        self.causation_event_id.as_ref()
    }

    pub fn payload(&self) -> &ExecutionEventPayload {
        &self.payload
    }

    pub fn into_payload(self) -> ExecutionEventPayload {
        self.payload
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionEventEnvelopeWire {
    schema_version: u32,
    event_id: ExecutionEventId,
    run_id: RunId,
    transition_key: TransitionKey,
    intent_hash: IntentHash,
    seq: EventSeq,
    occurred_at: DateTime<Utc>,
    kind: ExecutionEventKind,
    node_id: Option<NodeId>,
    scope_instance_id: Option<ScopeInstanceId>,
    activation_id: Option<ActivationId>,
    attempt_no: Option<AttemptNo>,
    causation_event_id: Option<ExecutionEventId>,
    payload: ExecutionEventPayload,
}

impl<'de> Deserialize<'de> for ExecutionEventEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExecutionEventEnvelopeWire::deserialize(deserializer)?;
        if wire.schema_version != EXECUTION_EVENT_SCHEMA_VERSION {
            return Err(D::Error::custom(
                "unsupported execution event schema version",
            ));
        }
        if wire.kind != wire.payload.kind() {
            return Err(D::Error::custom(
                "execution event kind does not match its closed payload",
            ));
        }
        let context = ExecutionEventContext {
            run_id: wire.run_id,
            node_id: wire.node_id,
            scope_instance_id: wire.scope_instance_id,
            activation_id: wire.activation_id,
            attempt_no: wire.attempt_no,
            causation_event_id: wire.causation_event_id,
        };
        wire.payload
            .validate_context(&context)
            .map_err(D::Error::custom)?;
        context
            .validate_committed_event_id(&wire.event_id)
            .map_err(D::Error::custom)?;
        Ok(Self {
            schema_version: wire.schema_version,
            event_id: wire.event_id,
            run_id: context.run_id,
            transition_key: wire.transition_key,
            intent_hash: wire.intent_hash,
            seq: wire.seq,
            occurred_at: wire.occurred_at,
            kind: wire.kind,
            node_id: context.node_id,
            scope_instance_id: context.scope_instance_id,
            activation_id: context.activation_id,
            attempt_no: context.attempt_no,
            causation_event_id: context.causation_event_id,
            payload: wire.payload,
        })
    }
}

/// Repository-level result: idempotent replay is a successful authoritative
/// resolution, while stale leases and state conflicts are non-store outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransitionOutcome<T> {
    Committed { result: T },
    ExactReplay { authoritative: T },
    StaleLease,
    StateConflict,
}

impl<T> TransitionOutcome<T> {
    pub fn committed_result(&self) -> Option<&T> {
        match self {
            Self::Committed { result } => Some(result),
            Self::ExactReplay { authoritative } => Some(authoritative),
            Self::StaleLease | Self::StateConflict => None,
        }
    }

    pub fn map<U>(self, mapper: impl FnOnce(T) -> U) -> TransitionOutcome<U> {
        match self {
            Self::Committed { result } => TransitionOutcome::Committed {
                result: mapper(result),
            },
            Self::ExactReplay { authoritative } => TransitionOutcome::ExactReplay {
                authoritative: mapper(authoritative),
            },
            Self::StaleLease => TransitionOutcome::StaleLease,
            Self::StateConflict => TransitionOutcome::StateConflict,
        }
    }
}

string_wire_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum PublicEventKind {
        RunCreated => "run.created",
        RunStarted => "run.started",
        OperationStarted => "operation.started",
        OperationCompleted => "operation.completed",
        OperationFailed => "operation.failed",
        RunCompleted => "run.completed",
        RunFailed => "run.failed",
        RunCancelled => "run.cancelled",
        RunInterrupted => "run.interrupted"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicFailureKind {
    Workflow,
    Operation,
    Timeout,
    Infrastructure,
    Stop,
}

/// Body-free public code. Internal error objects, sources, provider bodies and
/// stack traces have no representation in the public event model.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicErrorCode(String);

impl PublicErrorCode {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        let mut bytes = value.bytes();
        let valid_first = bytes.next().is_some_and(|byte| byte.is_ascii_uppercase());
        if value.len() > MAX_PUBLIC_ERROR_CODE_BYTES
            || !valid_first
            || !bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(ModelError::new(
                PUBLIC_ERROR_CODE_INVALID,
                "public error code must be a bounded uppercase identifier",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PublicErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for PublicErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for PublicErrorCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicFailureSummary {
    pub kind: PublicFailureKind,
    pub code: PublicErrorCode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicEventPayload {
    RunCreated,
    RunStarted,
    OperationStarted {
        node_id: NodeId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
    },
    OperationCompleted {
        node_id: NodeId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        elapsed_ms: u64,
        output_bytes: u64,
    },
    OperationFailed {
        node_id: NodeId,
        activation_id: ActivationId,
        attempt_no: AttemptNo,
        elapsed_ms: u64,
        failure: PublicFailureSummary,
    },
    RunCompleted,
    RunFailed {
        failure: PublicFailureSummary,
    },
    RunCancelled {
        failure: PublicFailureSummary,
    },
    RunInterrupted {
        failure: PublicFailureSummary,
    },
}

impl PublicEventPayload {
    pub const fn kind(&self) -> PublicEventKind {
        match self {
            Self::RunCreated => PublicEventKind::RunCreated,
            Self::RunStarted => PublicEventKind::RunStarted,
            Self::OperationStarted { .. } => PublicEventKind::OperationStarted,
            Self::OperationCompleted { .. } => PublicEventKind::OperationCompleted,
            Self::OperationFailed { .. } => PublicEventKind::OperationFailed,
            Self::RunCompleted => PublicEventKind::RunCompleted,
            Self::RunFailed { .. } => PublicEventKind::RunFailed,
            Self::RunCancelled { .. } => PublicEventKind::RunCancelled,
            Self::RunInterrupted { .. } => PublicEventKind::RunInterrupted,
        }
    }
}

/// Safe projection envelope. Its closed payload type deliberately cannot carry
/// an arbitrary internal error, JSON value, provider response, or output body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicEventEnvelope {
    schema_version: u32,
    public_event_id: PublicEventId,
    run_id: RunId,
    causation_event_id: ExecutionEventId,
    seq: EventSeq,
    occurred_at: DateTime<Utc>,
    kind: PublicEventKind,
    payload: PublicEventPayload,
}

impl PublicEventEnvelope {
    #[allow(dead_code)]
    pub(crate) fn new(
        context: PublicEventContext,
        seq: EventSeq,
        occurred_at: DateTime<Utc>,
        payload: PublicEventPayload,
    ) -> Self {
        let kind = payload.kind();
        Self {
            schema_version: PUBLIC_EVENT_SCHEMA_VERSION,
            public_event_id: context.public_event_id,
            run_id: context.run_id,
            causation_event_id: context.causation_event_id,
            seq,
            occurred_at,
            kind,
            payload,
        }
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn public_event_id(&self) -> &PublicEventId {
        &self.public_event_id
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn causation_event_id(&self) -> &ExecutionEventId {
        &self.causation_event_id
    }

    pub fn seq(&self) -> EventSeq {
        self.seq
    }

    pub fn occurred_at(&self) -> &DateTime<Utc> {
        &self.occurred_at
    }

    pub fn kind(&self) -> PublicEventKind {
        self.kind
    }

    pub fn payload(&self) -> &PublicEventPayload {
        &self.payload
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicEventEnvelopeWire {
    schema_version: u32,
    public_event_id: PublicEventId,
    run_id: RunId,
    causation_event_id: ExecutionEventId,
    seq: EventSeq,
    occurred_at: DateTime<Utc>,
    kind: PublicEventKind,
    payload: PublicEventPayload,
}

impl<'de> Deserialize<'de> for PublicEventEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PublicEventEnvelopeWire::deserialize(deserializer)?;
        if wire.schema_version != PUBLIC_EVENT_SCHEMA_VERSION {
            return Err(D::Error::custom("unsupported public event schema version"));
        }
        if wire.kind != wire.payload.kind() {
            return Err(D::Error::custom(
                "public event kind does not match its safe payload",
            ));
        }
        Ok(Self {
            schema_version: wire.schema_version,
            public_event_id: wire.public_event_id,
            run_id: wire.run_id,
            causation_event_id: wire.causation_event_id,
            seq: wire.seq,
            occurred_at: wire.occurred_at,
            kind: wire.kind,
            payload: wire.payload,
        })
    }
}

fn validate_transition_components(domain: &str, parts: &[&str]) -> Result<(), ModelError> {
    let domain_is_valid = !domain.is_empty()
        && domain.len() <= MAX_TRANSITION_DOMAIN_BYTES
        && domain.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        });
    let parts_are_valid = !parts.is_empty()
        && parts.len() <= MAX_TRANSITION_PARTS
        && parts.iter().all(|part| {
            !part.is_empty()
                && part.len() <= MAX_TRANSITION_PART_BYTES
                && !part.chars().any(char::is_control)
        });
    if !domain_is_valid || !parts_are_valid {
        return Err(ModelError::new(
            TRANSITION_KEY_INVALID,
            "transition key identity components are invalid",
        ));
    }
    Ok(())
}

fn append_framed(target: &mut Vec<u8>, value: &[u8]) -> Result<(), ModelError> {
    let length = u64::try_from(value.len()).map_err(|_| {
        ModelError::new(
            TRANSITION_KEY_INVALID,
            "transition key identity component is too large",
        )
    })?;
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(value);
    Ok(())
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use chrono::TimeZone;
    use serde_json::{json, Value};

    use super::*;

    fn occurred_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 18, 1, 2, 3).unwrap()
    }

    fn run_id() -> RunId {
        RunId::new("run_event_contract").unwrap()
    }

    fn execution_event_id() -> ExecutionEventId {
        ExecutionEventId::parse("event_00000000000000000000000000000001").unwrap()
    }

    fn causation_event_id() -> ExecutionEventId {
        ExecutionEventId::parse("event_00000000000000000000000000000002").unwrap()
    }

    fn public_event_id() -> PublicEventId {
        PublicEventId::parse("public_event_00000000000000000000000000000001").unwrap()
    }

    fn failure() -> PublicFailureSummary {
        PublicFailureSummary {
            kind: PublicFailureKind::Infrastructure,
            code: PublicErrorCode::new("INFRASTRUCTURE_FAILURE").unwrap(),
        }
    }

    fn internal_failure() -> InternalFailureSummary {
        InternalFailureSummary::new(
            InternalFailureKind::Infrastructure,
            InternalFailureCode::new("WORKER_LOST").unwrap(),
        )
    }

    fn value_summary() -> ExecutionValueSummary {
        ExecutionValueSummary::new(ContentHash::from_bytes(b"durable-output"), 14)
    }

    fn activation_context() -> ExecutionEventContext {
        ExecutionEventContext::for_run(run_id()).for_activation(
            ScopeInstanceId::root(),
            NodeId::new("analyze").unwrap(),
            ActivationId::new("activation_analyze").unwrap(),
        )
    }

    fn attempt_context() -> ExecutionEventContext {
        ExecutionEventContext::for_run(run_id()).for_attempt(
            ScopeInstanceId::root(),
            NodeId::new("analyze").unwrap(),
            ActivationId::new("activation_analyze").unwrap(),
            AttemptNo::FIRST,
        )
    }

    fn run_created_payload() -> ExecutionEventPayload {
        ExecutionEventPayload::RunCreated {
            definition_revision_id: DefinitionRevisionId::new("definition_v1").unwrap(),
            deployment_revision_id: DeploymentRevisionId::new("deployment_v1").unwrap(),
            run_deadline_at: None,
        }
    }

    fn fork_legs() -> Vec<ExecutionForkLeg> {
        vec![
            ExecutionForkLeg::new(
                LegId::new("technical").unwrap(),
                PortId::new("technical").unwrap(),
                ScopeInstanceId::new("scope_technical").unwrap(),
            ),
            ExecutionForkLeg::new(
                LegId::new("risk").unwrap(),
                PortId::new("risk").unwrap(),
                ScopeInstanceId::new("scope_risk").unwrap(),
            ),
        ]
    }

    fn representative_execution_payloads() -> Vec<ExecutionEventPayload> {
        let summary = value_summary();
        let internal_failure = internal_failure();
        let fork_group_id = ForkGroupId::new("fork_group").unwrap();
        let token_id = ControlTokenId::new("token_one").unwrap();
        vec![
            run_created_payload(),
            ExecutionEventPayload::RunLifecycleChanged {
                lifecycle: RunLifecycle::Active,
            },
            ExecutionEventPayload::RunAdmissionChanged {
                admission: AdmissionState::Draining,
            },
            ExecutionEventPayload::RunTerminationClaimed {
                reason: TerminationReason::Cancelled,
            },
            ExecutionEventPayload::ScopeCreated {
                scope_kind: ExecutionScopeKind::Root,
                parent_scope_instance_id: None,
            },
            ExecutionEventPayload::ScopeDraining {
                admitted_children: 2,
                settled_children: 1,
                live_attempts: 1,
            },
            ExecutionEventPayload::ScopeSettled {
                admitted_children: 2,
                settled_children: 2,
                live_attempts: 0,
            },
            ExecutionEventPayload::ActivationCreated {
                effect_id: Some(EffectId::new("effect_analyze").unwrap()),
            },
            ExecutionEventPayload::ActivationReady,
            ExecutionEventPayload::ActivationLeased {
                attempt_no: AttemptNo::FIRST,
                lease_epoch: LeaseEpoch::FIRST,
            },
            ExecutionEventPayload::ActivationRunning {
                attempt_no: AttemptNo::FIRST,
            },
            ExecutionEventPayload::ActivationRetryWait {
                attempt_no: AttemptNo::FIRST,
            },
            ExecutionEventPayload::ActivationWaiting,
            ExecutionEventPayload::ActivationTerminating {
                reason: ActivationTerminationReason::Cancelled,
            },
            ExecutionEventPayload::ActivationSucceeded {
                attempt_no: Some(AttemptNo::FIRST),
                output: Some(summary.clone()),
            },
            ExecutionEventPayload::ActivationFailed {
                attempt_no: Some(AttemptNo::FIRST),
                reason: ActivationTerminationReason::Failure,
                failure: Some(internal_failure.clone()),
            },
            ExecutionEventPayload::ActivationCancelled,
            ExecutionEventPayload::ActivationTimedOut,
            ExecutionEventPayload::AttemptCreated {
                lease_epoch: LeaseEpoch::FIRST,
            },
            ExecutionEventPayload::AttemptLeased {
                lease_epoch: LeaseEpoch::FIRST,
            },
            ExecutionEventPayload::AttemptRunning {
                lease_epoch: LeaseEpoch::FIRST,
            },
            ExecutionEventPayload::AttemptSucceeded {
                output: Some(summary.clone()),
            },
            ExecutionEventPayload::AttemptFailed {
                failure: Some(internal_failure),
            },
            ExecutionEventPayload::AttemptTimedOut,
            ExecutionEventPayload::AttemptAbandoned {
                effect_evidence: EffectEvidence::Unknown,
            },
            ExecutionEventPayload::AttemptCancelled,
            ExecutionEventPayload::EffectEvidenceRecorded {
                evidence: EffectEvidence::Started,
            },
            ExecutionEventPayload::ControlTokenEmitted {
                token_id: token_id.clone(),
                source_port: PortId::new("done").unwrap(),
                token_scope_instance_id: ScopeInstanceId::root(),
                frames: Vec::new(),
            },
            ExecutionEventPayload::ControlTokenConsumed {
                token_id: token_id.clone(),
            },
            ExecutionEventPayload::ControlTokenRevoked {
                token_id: token_id.clone(),
            },
            ExecutionEventPayload::ForkCreated {
                fork_group_id: fork_group_id.clone(),
                legs: fork_legs(),
            },
            ExecutionEventPayload::JoinArrived {
                fork_group_id: fork_group_id.clone(),
                leg_id: LegId::new("technical").unwrap(),
                token_id,
                settlement: ExecutionLegSettlementClass::Succeeded,
                value: Some(summary.clone()),
            },
            ExecutionEventPayload::JoinCompleted {
                fork_group_id,
                mode: ExecutionJoinMode::AllSuccess,
                settled_leg_count: 2,
            },
            ExecutionEventPayload::SignalReceived {
                signal_id: SignalId::new("signal_review").unwrap(),
                value: Some(summary.clone()),
            },
            ExecutionEventPayload::SignalLate {
                signal_id: SignalId::new("signal_late").unwrap(),
                value: Some(summary),
            },
            ExecutionEventPayload::TimerScheduled {
                timer_id: TimerId::new("timer_deadline").unwrap(),
                fire_at: occurred_at(),
            },
            ExecutionEventPayload::TimerFired {
                timer_id: TimerId::new("timer_deadline").unwrap(),
            },
            ExecutionEventPayload::TimerLate {
                timer_id: TimerId::new("timer_deadline").unwrap(),
            },
            ExecutionEventPayload::ProjectionMutated {
                mutation: ProjectionMutationKind::TaskAcknowledged,
            },
        ]
    }

    #[test]
    fn event_sequence_starts_at_one_round_trips_and_rejects_invalid_values() {
        assert_eq!(EventSeq::FIRST.get(), 1);
        assert_eq!(EventSeq::FIRST.next().unwrap().get(), 2);
        assert_eq!(EventSeq::new(0).unwrap_err().code(), EVENT_SEQ_INVALID);
        assert_eq!(
            EventSeq::new(u64::MAX).unwrap().next().unwrap_err().code(),
            EVENT_SEQ_OVERFLOW
        );
        assert_eq!(
            serde_json::from_value::<EventSeq>(json!(7)).unwrap(),
            EventSeq::new(7).unwrap()
        );
        assert!(serde_json::from_value::<EventSeq>(json!(0)).is_err());
        assert!(serde_json::from_value::<EventSeq>(json!(-1)).is_err());
        assert!(serde_json::from_value::<EventSeq>(json!("1")).is_err());
    }

    #[test]
    fn event_ids_are_generated_strict_and_validated_during_deserialization() {
        let generated_execution = ExecutionEventId::random();
        let generated_public = PublicEventId::random();
        assert!(generated_execution.as_str().starts_with("event_"));
        assert!(generated_public.as_str().starts_with("public_event_"));
        assert_eq!(
            ExecutionEventId::parse(generated_execution.as_str()).unwrap(),
            generated_execution
        );
        assert_eq!(
            PublicEventId::parse(generated_public.as_str()).unwrap(),
            generated_public
        );
        assert_eq!(
            serde_json::from_value::<ExecutionEventId>(
                serde_json::to_value(execution_event_id()).unwrap()
            )
            .unwrap(),
            execution_event_id()
        );
        assert!(ExecutionEventId::parse("event_ABC").is_err());
        assert!(PublicEventId::parse("event_00000000000000000000000000000001").is_err());
        assert!(serde_json::from_value::<PublicEventId>(json!(
            "public_event_0000000000000000000000000000000Z"
        ))
        .is_err());
    }

    #[test]
    fn transition_key_is_stable_boundary_safe_and_strictly_deserialized() {
        let first = TransitionKey::derive("attempt.complete", &["run-a", "ab", "c"]).unwrap();
        let repeated = TransitionKey::derive("attempt.complete", &["run-a", "ab", "c"]).unwrap();
        let different_boundary =
            TransitionKey::derive("attempt.complete", &["run-a", "a", "bc"]).unwrap();
        let different_domain =
            TransitionKey::derive("attempt.fail", &["run-a", "ab", "c"]).unwrap();

        assert_eq!(first, repeated);
        assert_ne!(first, different_boundary);
        assert_ne!(first, different_domain);
        assert_eq!(first.as_str().len(), TRANSITION_KEY_PREFIX.len() + 64);
        assert_eq!(
            serde_json::from_value::<TransitionKey>(serde_json::to_value(&first).unwrap()).unwrap(),
            first
        );
        assert!(TransitionKey::derive("Bad Domain", &["run-a"]).is_err());
        assert!(TransitionKey::derive("attempt.complete", &[]).is_err());
        assert!(serde_json::from_value::<TransitionKey>(json!("transition_ABC")).is_err());
    }

    #[test]
    fn intent_hash_uses_canonical_json_and_has_a_strict_wire_form() {
        let first = IntentHash::from_serializable(&json!({"a": 1, "b": 2})).unwrap();
        let reordered = IntentHash::from_serializable(&json!({"b": 2, "a": 1})).unwrap();
        let changed = IntentHash::from_serializable(&json!({"a": 1, "b": 3})).unwrap();

        assert_eq!(first, reordered);
        assert_ne!(first, changed);
        assert_eq!(
            serde_json::from_value::<IntentHash>(serde_json::to_value(&first).unwrap()).unwrap(),
            first
        );
        assert!(IntentHash::parse("sha256:ABC").is_err());
    }

    #[test]
    fn transition_request_hash_binds_run_command_context_causation_and_payload() {
        let key = TransitionKey::derive("attempt.complete", &["shared-logical-key"]).unwrap();
        let base_event = PendingExecutionEvent::new(
            attempt_context().caused_by(causation_event_id()),
            ExecutionEventPayload::AttemptSucceeded {
                output: Some(value_summary()),
            },
        )
        .unwrap();
        let base = TransitionRequest::for_execution(
            key.clone(),
            json!({"expected": "running", "output_slot": "result"}),
            base_event,
        )
        .unwrap();

        let changed_event = PendingExecutionEvent::new(
            attempt_context().caused_by(execution_event_id()),
            ExecutionEventPayload::AttemptSucceeded { output: None },
        )
        .unwrap();
        let changed = TransitionRequest::for_execution(
            key.clone(),
            json!({"expected": "running", "output_slot": "result"}),
            changed_event,
        )
        .unwrap();
        assert_ne!(
            base.identity().intent_hash(),
            changed.identity().intent_hash()
        );

        let other_run = RunId::new("run_other").unwrap();
        let other_event = PendingExecutionEvent::new(
            ExecutionEventContext::for_run(other_run.clone()).for_attempt(
                ScopeInstanceId::root(),
                NodeId::new("analyze").unwrap(),
                ActivationId::new("activation_analyze").unwrap(),
                AttemptNo::FIRST,
            ),
            ExecutionEventPayload::AttemptSucceeded {
                output: Some(value_summary()),
            },
        )
        .unwrap();
        let other = TransitionRequest::for_execution(
            key,
            json!({"expected": "running", "output_slot": "result"}),
            other_event,
        )
        .unwrap();
        assert_eq!(other.identity().run_id(), &other_run);
        assert_ne!(base.identity().run_id(), other.identity().run_id());
        assert_ne!(
            base.identity().intent_hash(),
            other.identity().intent_hash(),
            "the pending event run is part of the canonical execution intent"
        );
    }

    #[test]
    fn transition_ledger_freezes_replay_conflict_stale_and_state_conflict_semantics() {
        let key = TransitionKey::derive("attempt.complete", &["same-key-across-runs"]).unwrap();
        let event = PendingExecutionEvent::new(
            attempt_context(),
            ExecutionEventPayload::AttemptSucceeded {
                output: Some(value_summary()),
            },
        )
        .unwrap();
        let request = TransitionRequest::for_execution(
            key.clone(),
            json!({"expected": "running", "fence": [1, 1]}),
            event,
        )
        .unwrap();
        let calls = Cell::new(0_u32);
        let mut ledger = TransitionLedger::new();

        assert_eq!(
            ledger
                .apply(&request, TransitionPrecondition::Current, || {
                    calls.set(calls.get() + 1);
                    "first-authoritative".to_string()
                })
                .unwrap(),
            TransitionOutcome::Committed {
                result: "first-authoritative".to_string()
            }
        );
        assert_eq!(ledger.len(), 1);

        assert_eq!(
            ledger
                .apply(&request, TransitionPrecondition::StaleLease, || {
                    calls.set(calls.get() + 1);
                    "must-not-run".to_string()
                })
                .unwrap(),
            TransitionOutcome::ExactReplay {
                authoritative: "first-authoritative".to_string()
            },
            "exact replay resolves before the now-stale lease"
        );

        let conflicting = TransitionRequest::for_execution(
            key.clone(),
            json!({"expected": "running", "fence": [1, 1]}),
            PendingExecutionEvent::new(
                attempt_context(),
                ExecutionEventPayload::AttemptSucceeded { output: None },
            )
            .unwrap(),
        )
        .unwrap();
        let error = ledger
            .apply(&conflicting, TransitionPrecondition::StaleLease, || {
                calls.set(calls.get() + 1);
                "must-not-run".to_string()
            })
            .unwrap_err();
        assert_eq!(error.code(), TRANSITION_INTENT_CONFLICT);

        let stale = TransitionRequest::new(
            run_id(),
            TransitionKey::derive("attempt.complete", &["vacant-stale"]).unwrap(),
            json!({"fence": [2, 2]}),
        )
        .unwrap();
        assert_eq!(
            ledger
                .apply(&stale, TransitionPrecondition::StaleLease, || {
                    calls.set(calls.get() + 1);
                    "must-not-run".to_string()
                })
                .unwrap(),
            TransitionOutcome::StaleLease
        );

        let state_conflict = TransitionRequest::new(
            run_id(),
            TransitionKey::derive("attempt.complete", &["vacant-conflict"]).unwrap(),
            json!({"expected": "running"}),
        )
        .unwrap();
        assert_eq!(
            ledger
                .apply(
                    &state_conflict,
                    TransitionPrecondition::StateConflict,
                    || {
                        calls.set(calls.get() + 1);
                        "must-not-run".to_string()
                    }
                )
                .unwrap(),
            TransitionOutcome::StateConflict
        );

        let other_run = RunId::new("run_other_ledger").unwrap();
        let other_run_request = TransitionRequest::new(
            other_run,
            key,
            json!({"expected": "running", "fence": [1, 1]}),
        )
        .unwrap();
        assert_eq!(
            ledger
                .apply(&other_run_request, TransitionPrecondition::Current, || {
                    calls.set(calls.get() + 1);
                    "other-run".to_string()
                })
                .unwrap(),
            TransitionOutcome::Committed {
                result: "other-run".to_string()
            }
        );
        assert_eq!(ledger.len(), 2);
        assert_eq!(calls.get(), 2, "rejected and replayed intents never apply");
    }

    #[test]
    fn every_internal_event_kind_has_a_stable_round_trip() {
        for kind in ExecutionEventKind::ALL {
            let encoded = serde_json::to_value(kind).unwrap();
            assert_eq!(encoded, Value::String(kind.as_str().to_string()));
            assert_eq!(
                serde_json::from_value::<ExecutionEventKind>(encoded).unwrap(),
                *kind
            );
        }
        assert!(serde_json::from_value::<ExecutionEventKind>(json!("internal.debug")).is_err());
    }

    #[test]
    fn every_closed_execution_payload_maps_to_one_kind_and_exact_context_level() {
        let payloads = representative_execution_payloads();
        assert_eq!(payloads.len(), ExecutionEventKind::ALL.len());

        for (payload, expected_kind) in payloads.into_iter().zip(ExecutionEventKind::ALL) {
            assert_eq!(payload.kind(), *expected_kind);
            let encoded = serde_json::to_value(&payload).unwrap();
            assert_eq!(
                serde_json::from_value::<ExecutionEventPayload>(encoded).unwrap(),
                payload
            );

            let correct_context = match payload.context_level() {
                ExecutionContextLevel::Run => ExecutionEventContext::for_run(run_id()),
                ExecutionContextLevel::Scope => {
                    ExecutionEventContext::for_run(run_id()).in_scope(ScopeInstanceId::root())
                }
                ExecutionContextLevel::Activation => activation_context(),
                ExecutionContextLevel::Attempt => attempt_context(),
            };
            PendingExecutionEvent::new(correct_context, payload.clone()).unwrap();

            let wrong_context = match payload.context_level() {
                ExecutionContextLevel::Run => attempt_context(),
                ExecutionContextLevel::Scope
                | ExecutionContextLevel::Activation
                | ExecutionContextLevel::Attempt => ExecutionEventContext::for_run(run_id()),
            };
            assert!(PendingExecutionEvent::new(wrong_context, payload).is_err());
        }
    }

    #[test]
    fn failure_events_preserve_truthful_absence_without_changing_event_kind() {
        let activation = ExecutionEventPayload::ActivationFailed {
            attempt_no: Some(AttemptNo::FIRST),
            reason: ActivationTerminationReason::EffectOutcomeUnknown,
            failure: None,
        };
        let attempt = ExecutionEventPayload::AttemptFailed { failure: None };
        assert_eq!(activation.kind(), ExecutionEventKind::ActivationFailed);
        assert_eq!(attempt.kind(), ExecutionEventKind::AttemptFailed);
        assert_eq!(
            serde_json::from_value::<ExecutionEventPayload>(
                serde_json::to_value(&activation).unwrap()
            )
            .unwrap(),
            activation
        );
        assert_eq!(
            serde_json::from_value::<ExecutionEventPayload>(
                serde_json::to_value(&attempt).unwrap()
            )
            .unwrap(),
            attempt
        );
    }

    #[test]
    fn structured_execution_payloads_reject_inconsistent_safe_metadata() {
        let scope_context =
            ExecutionEventContext::for_run(run_id()).in_scope(ScopeInstanceId::root());
        let parallel_scope = ScopeInstanceId::derive(
            &ScopeInstanceId::root(),
            &NodeId::new("parallel").unwrap(),
            "parallel_leg:risk",
        )
        .unwrap();
        PendingExecutionEvent::new(
            ExecutionEventContext::for_run(run_id()).in_scope(parallel_scope),
            ExecutionEventPayload::ScopeCreated {
                scope_kind: ExecutionScopeKind::ParallelLeg,
                parent_scope_instance_id: Some(ScopeInstanceId::root()),
            },
        )
        .unwrap();
        assert!(PendingExecutionEvent::new(
            scope_context.clone(),
            ExecutionEventPayload::ScopeCreated {
                scope_kind: ExecutionScopeKind::MapItem,
                parent_scope_instance_id: None,
            },
        )
        .is_err());
        assert!(PendingExecutionEvent::new(
            scope_context.clone(),
            ExecutionEventPayload::ScopeDraining {
                admitted_children: 1,
                settled_children: 2,
                live_attempts: 0,
            },
        )
        .is_err());
        assert!(PendingExecutionEvent::new(
            scope_context,
            ExecutionEventPayload::ScopeSettled {
                admitted_children: 1,
                settled_children: 1,
                live_attempts: 1,
            },
        )
        .is_err());

        assert!(PendingExecutionEvent::new(
            activation_context(),
            ExecutionEventPayload::ForkCreated {
                fork_group_id: ForkGroupId::new("fork_empty").unwrap(),
                legs: Vec::new(),
            },
        )
        .is_err());

        let duplicate_scope = ScopeInstanceId::new("scope_duplicate").unwrap();
        assert!(PendingExecutionEvent::new(
            activation_context(),
            ExecutionEventPayload::ForkCreated {
                fork_group_id: ForkGroupId::new("fork_duplicate").unwrap(),
                legs: vec![
                    ExecutionForkLeg::new(
                        LegId::new("first").unwrap(),
                        PortId::new("first").unwrap(),
                        duplicate_scope.clone(),
                    ),
                    ExecutionForkLeg::new(
                        LegId::new("second").unwrap(),
                        PortId::new("second").unwrap(),
                        duplicate_scope,
                    ),
                ],
            },
        )
        .is_err());

        assert!(PendingExecutionEvent::new(
            activation_context(),
            ExecutionEventPayload::ControlTokenEmitted {
                token_id: ControlTokenId::new("token_wrong_scope").unwrap(),
                source_port: PortId::new("done").unwrap(),
                token_scope_instance_id: ScopeInstanceId::new("scope_current").unwrap(),
                frames: vec![ExecutionControlFrame::Branch {
                    branch_activation_id: ActivationId::new("activation_branch").unwrap(),
                    selected_port: PortId::new("then").unwrap(),
                    scope_instance_id: ScopeInstanceId::new("scope_other").unwrap(),
                }],
            },
        )
        .is_err());

        assert!(PendingExecutionEvent::new(
            activation_context(),
            ExecutionEventPayload::JoinArrived {
                fork_group_id: ForkGroupId::new("fork_join").unwrap(),
                leg_id: LegId::new("technical").unwrap(),
                token_id: ControlTokenId::new("token_join").unwrap(),
                settlement: ExecutionLegSettlementClass::Succeeded,
                value: None,
            },
        )
        .is_err());

        assert!(SignalId::new("signal\nsecret").is_err());
        assert!(TimerId::new("timer/../../escape").is_err());
        assert!(InternalFailureCode::new("provider said: secret").is_err());
    }

    #[test]
    fn internal_event_envelope_round_trips_full_identity_and_fails_closed() {
        let pending = PendingExecutionEvent::new(
            attempt_context().caused_by(causation_event_id()),
            ExecutionEventPayload::AttemptRunning {
                lease_epoch: LeaseEpoch::FIRST,
            },
        )
        .unwrap();
        let request = TransitionRequest::for_execution(
            TransitionKey::derive(
                "attempt.running",
                &[run_id().as_str(), "activation_analyze", "1"],
            )
            .unwrap(),
            json!({"expected": "leased", "lease_epoch": 1}),
            pending,
        )
        .unwrap();
        let request_json = serde_json::to_value(&request).unwrap();
        let pending_json = &request_json["intent"]["event"];
        assert!(pending_json.get("event_id").is_none());
        assert!(pending_json.get("seq").is_none());
        assert!(pending_json.get("occurred_at").is_none());

        let event = request
            .commit_event(ExecutionEventCommitMetadata::new(
                execution_event_id(),
                EventSeq::FIRST,
                occurred_at(),
            ))
            .unwrap();
        assert_eq!(event.event_id(), &execution_event_id());
        assert_eq!(event.run_id(), request.identity().run_id());
        assert_eq!(event.transition_key(), request.identity().key());
        assert_eq!(event.intent_hash(), request.identity().intent_hash());
        assert_eq!(event.node_id().unwrap().as_str(), "analyze");
        assert_eq!(event.scope_instance_id(), Some(&ScopeInstanceId::root()));
        assert_eq!(
            event.activation_id().unwrap().as_str(),
            "activation_analyze"
        );
        assert_eq!(event.attempt_no(), Some(AttemptNo::FIRST));
        assert_eq!(event.causation_event_id(), Some(&causation_event_id()));
        let encoded = serde_json::to_value(&event).unwrap();
        assert_eq!(
            serde_json::from_value::<ExecutionEventEnvelope>(encoded.clone()).unwrap(),
            event
        );

        let mut unsupported = encoded.clone();
        unsupported["schema_version"] = json!(EXECUTION_EVENT_SCHEMA_VERSION + 1);
        assert!(serde_json::from_value::<ExecutionEventEnvelope>(unsupported).is_err());

        let mut zero_sequence = encoded.clone();
        zero_sequence["seq"] = json!(0);
        assert!(serde_json::from_value::<ExecutionEventEnvelope>(zero_sequence).is_err());

        let mut invalid_hierarchy = encoded.clone();
        invalid_hierarchy["activation_id"] = Value::Null;
        assert!(serde_json::from_value::<ExecutionEventEnvelope>(invalid_hierarchy).is_err());

        let mut mismatched_kind = encoded.clone();
        mismatched_kind["kind"] = json!("attempt.succeeded");
        assert!(serde_json::from_value::<ExecutionEventEnvelope>(mismatched_kind).is_err());

        let mut wrong_level = encoded.clone();
        wrong_level["kind"] = json!("run.created");
        wrong_level["payload"] = serde_json::to_value(run_created_payload()).unwrap();
        assert!(serde_json::from_value::<ExecutionEventEnvelope>(wrong_level).is_err());

        let mut self_caused = encoded.clone();
        self_caused["causation_event_id"] = self_caused["event_id"].clone();
        assert!(serde_json::from_value::<ExecutionEventEnvelope>(self_caused).is_err());

        let mut provider_body = encoded.clone();
        provider_body["payload"]["provider_body"] = json!("secret response");
        assert!(serde_json::from_value::<ExecutionEventEnvelope>(provider_body).is_err());

        let mut unknown = encoded;
        unknown["unexpected"] = json!(true);
        assert!(serde_json::from_value::<ExecutionEventEnvelope>(unknown).is_err());

        let self_caused_pending = PendingExecutionEvent::new(
            attempt_context().caused_by(execution_event_id()),
            ExecutionEventPayload::AttemptRunning {
                lease_epoch: LeaseEpoch::FIRST,
            },
        )
        .unwrap();
        let self_caused_request = TransitionRequest::for_execution(
            TransitionKey::derive("attempt.running", &[run_id().as_str(), "self-caused"]).unwrap(),
            json!({"expected": "leased"}),
            self_caused_pending,
        )
        .unwrap();
        assert!(self_caused_request
            .commit_event(ExecutionEventCommitMetadata::new(
                execution_event_id(),
                EventSeq::FIRST,
                occurred_at(),
            ))
            .is_err());
    }

    #[test]
    fn transition_outcomes_preserve_authoritative_results_and_all_variants_round_trip() {
        let outcomes = [
            TransitionOutcome::Committed { result: 7_u64 },
            TransitionOutcome::ExactReplay {
                authoritative: 11_u64,
            },
            TransitionOutcome::StaleLease,
            TransitionOutcome::StateConflict,
        ];

        assert_eq!(outcomes[0].committed_result(), Some(&7));
        assert_eq!(outcomes[1].committed_result(), Some(&11));
        assert_eq!(outcomes[2].committed_result(), None);
        assert_eq!(outcomes[3].committed_result(), None);

        for outcome in outcomes {
            let encoded = serde_json::to_value(&outcome).unwrap();
            assert_eq!(
                serde_json::from_value::<TransitionOutcome<u64>>(encoded).unwrap(),
                outcome
            );
        }

        assert_eq!(
            serde_json::to_value(TransitionOutcome::Committed { result: 7_u64 }).unwrap(),
            json!({"outcome": "committed", "result": 7})
        );
        assert_eq!(
            serde_json::to_value(TransitionOutcome::ExactReplay {
                authoritative: 11_u64
            })
            .unwrap(),
            json!({"outcome": "exact_replay", "authoritative": 11})
        );
        for legacy in [
            json!({"outcome": "applied", "result": 7}),
            json!({"outcome": "already_applied", "authoritative": 11}),
            json!({"outcome": "committed", "result": 7, "unexpected": true}),
        ] {
            assert!(serde_json::from_value::<TransitionOutcome<u64>>(legacy).is_err());
        }

        assert_eq!(
            TransitionOutcome::ExactReplay { authoritative: 3 }.map(|value| value.to_string()),
            TransitionOutcome::ExactReplay {
                authoritative: "3".to_string()
            }
        );
    }

    #[test]
    fn public_envelope_uses_only_closed_safe_payloads_and_validates_kind() {
        let payload = PublicEventPayload::OperationFailed {
            node_id: NodeId::new("analyze").unwrap(),
            activation_id: ActivationId::new("activation_analyze").unwrap(),
            attempt_no: AttemptNo::FIRST,
            elapsed_ms: 12,
            failure: failure(),
        };
        let context = PublicEventContext::new(public_event_id(), run_id(), causation_event_id());
        let event = PublicEventEnvelope::new(context, EventSeq::FIRST, occurred_at(), payload);
        assert_eq!(event.kind(), PublicEventKind::OperationFailed);
        assert_eq!(event.public_event_id(), &public_event_id());
        assert_eq!(event.causation_event_id(), &causation_event_id());

        let encoded = serde_json::to_value(&event).unwrap();
        assert_eq!(
            serde_json::from_value::<PublicEventEnvelope>(encoded.clone()).unwrap(),
            event
        );

        let mut mismatched = encoded.clone();
        mismatched["kind"] = json!("operation.completed");
        assert!(serde_json::from_value::<PublicEventEnvelope>(mismatched).is_err());

        let mut arbitrary_internal_error = encoded;
        arbitrary_internal_error["payload"]["failure"]["source"] = json!({
            "provider_body": "secret",
            "stack": ["internal frame"]
        });
        assert!(serde_json::from_value::<PublicEventEnvelope>(arbitrary_internal_error).is_err());
        assert!(PublicErrorCode::new("provider said: secret").is_err());
    }

    #[test]
    fn every_public_event_payload_maps_to_one_stable_kind() {
        let node_id = NodeId::new("answer").unwrap();
        let activation_id = ActivationId::new("activation_answer").unwrap();
        let attempt_no = AttemptNo::FIRST;
        let payloads = [
            PublicEventPayload::RunCreated,
            PublicEventPayload::RunStarted,
            PublicEventPayload::OperationStarted {
                node_id: node_id.clone(),
                activation_id: activation_id.clone(),
                attempt_no,
            },
            PublicEventPayload::OperationCompleted {
                node_id: node_id.clone(),
                activation_id: activation_id.clone(),
                attempt_no,
                elapsed_ms: 1,
                output_bytes: 2,
            },
            PublicEventPayload::OperationFailed {
                node_id,
                activation_id,
                attempt_no,
                elapsed_ms: 1,
                failure: failure(),
            },
            PublicEventPayload::RunCompleted,
            PublicEventPayload::RunFailed { failure: failure() },
            PublicEventPayload::RunCancelled { failure: failure() },
            PublicEventPayload::RunInterrupted { failure: failure() },
        ];

        assert_eq!(payloads.len(), PublicEventKind::ALL.len());
        for (payload, expected) in payloads.into_iter().zip(PublicEventKind::ALL) {
            assert_eq!(payload.kind(), *expected);
            let round_trip = serde_json::from_value::<PublicEventPayload>(
                serde_json::to_value(&payload).unwrap(),
            )
            .unwrap();
            assert_eq!(round_trip, payload);
        }
    }
}
