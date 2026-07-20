use std::collections::{BTreeMap, BTreeSet};

use serde::{
    de::{Error as _, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;

use super::{
    semantic::semantic_hash_for_plan, verify::verify_plan, BranchCaseId, ControlEdgeId,
    ControlPortId, DataBindingId, DataPortId, PhiBindingId, PlanError, PlanType, PolicyId,
    PortName, ScopeId, SecretRef, SourceDocumentId, VersionTag, PLAN_HASH_MISMATCH,
    PLAN_WIRE_INVALID,
};
use crate::engine::{ContentHash, DefinitionRevisionId, LegId, NodeId};

pub const PLAN_WIRE_VERSION: u32 = 4;
pub const PLAN_SEMANTIC_PROJECTION_VERSION: u32 = 4;
pub const DSL_MAJOR_VERSION: u32 = 3;
/// The only CEL implementation whose semantics this Plan version publishes.
/// Unknown expression engines are rejected instead of being run by whatever
/// parser happens to be linked into the current process.
pub const CEL_EXPRESSION_ENGINE_VERSION: &str = "cel-rs-0.14.0";
/// Canonical RFC 8785 JSON literal expressions.
pub const LITERAL_EXPRESSION_ENGINE_VERSION: &str = "json-jcs-rfc8785-v1";
/// Canonical RFC 8785 JSON contract for lazy, pure value selection.
pub const MATCH_EXPRESSION_ENGINE_VERSION: &str = "match-jcs-v1";
/// Canonical RFC 8785 JSON typed construction/project/template programs.
pub const VALUE_EXPRESSION_ENGINE_VERSION: &str = "value-jcs-v1";
pub const MAX_PLAN_JSON_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorFormat {
    Structured,
    Graph,
    Programmatic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanInputErrorKind {
    InvalidShape,
    UnknownField,
    MissingRequired,
    TypeMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanInputError {
    kind: PlanInputErrorKind,
}

impl PlanInputError {
    fn new(kind: PlanInputErrorKind) -> Self {
        Self { kind }
    }

    pub fn kind(&self) -> PlanInputErrorKind {
        self.kind
    }
}

impl std::fmt::Display for PlanInputError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("workflow input does not satisfy the frozen Plan input contract")
    }
}

impl std::error::Error for PlanInputError {}

/// One immutable public-input contract. Object-property `required` flags
/// encode accepted presence; `defaults` upgrades those fields to required in
/// the normalized Run input. An optional field without a default remains
/// absent when omitted. JSON `null` is always an ordinary present value and
/// is accepted only when its declared type includes Null.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanInputContract {
    accepted_type: PlanType,
    defaults: BTreeMap<String, Value>,
}

impl PlanInputContract {
    pub fn new(accepted_type: PlanType) -> Self {
        Self {
            accepted_type,
            defaults: BTreeMap::new(),
        }
    }

    pub fn accepted_type(&self) -> &PlanType {
        &self.accepted_type
    }

    pub fn defaults(&self) -> &BTreeMap<String, Value> {
        &self.defaults
    }

    pub fn with_defaults(mut self, defaults: BTreeMap<String, Value>) -> Self {
        self.defaults = defaults;
        self
    }

    pub fn run_type(&self) -> Result<PlanType, super::PlanTypeError> {
        let mut value = self.accepted_type.clone();
        if let PlanType::Object { properties, .. } = &mut value {
            for name in self.defaults.keys() {
                if let Some(property) = properties.get_mut(name) {
                    property.required = true;
                }
            }
        }
        value.normalized()
    }

    pub fn normalize(&self, input: Value) -> Result<Value, PlanInputError> {
        let PlanType::Object {
            properties,
            additional_properties,
        } = &self.accepted_type
        else {
            return self
                .accepted_type
                .accepts_literal(&input)
                .ok()
                .filter(|accepted| *accepted)
                .map(|_| input)
                .ok_or_else(|| PlanInputError::new(PlanInputErrorKind::TypeMismatch));
        };
        let Value::Object(mut object) = input else {
            return Err(PlanInputError::new(PlanInputErrorKind::InvalidShape));
        };
        if additional_properties.is_none()
            && object.keys().any(|name| !properties.contains_key(name))
        {
            return Err(PlanInputError::new(PlanInputErrorKind::UnknownField));
        }
        for (name, property) in properties {
            if object.contains_key(name) {
                continue;
            }
            if let Some(value) = self.defaults.get(name) {
                object.insert(name.clone(), value.clone());
            } else if property.required {
                return Err(PlanInputError::new(PlanInputErrorKind::MissingRequired));
            }
        }
        let normalized = Value::Object(object);
        self.run_type()
            .ok()
            .and_then(|value_type| value_type.accepts_literal(&normalized).ok())
            .filter(|accepted| *accepted)
            .map(|_| normalized)
            .ok_or_else(|| PlanInputError::new(PlanInputErrorKind::TypeMismatch))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanMetadata {
    pub(super) wire_version: u32,
    pub(super) dsl_version: u32,
    pub(super) definition_revision_id: DefinitionRevisionId,
    pub(super) compiler_version: VersionTag,
    pub(super) author_format: AuthorFormat,
    pub(super) entry_node_id: NodeId,
    pub(super) input_contract: PlanInputContract,
    pub(super) output_type: PlanType,
    pub(super) error_type: PlanType,
}

impl PlanMetadata {
    pub fn new(
        definition_revision_id: DefinitionRevisionId,
        compiler_version: VersionTag,
        author_format: AuthorFormat,
        entry_node_id: NodeId,
        input_contract: PlanInputContract,
        output_type: PlanType,
        error_type: PlanType,
    ) -> Self {
        Self {
            wire_version: PLAN_WIRE_VERSION,
            dsl_version: DSL_MAJOR_VERSION,
            definition_revision_id,
            compiler_version,
            author_format,
            entry_node_id,
            input_contract,
            output_type,
            error_type,
        }
    }

    pub fn wire_version(&self) -> u32 {
        self.wire_version
    }

    pub fn dsl_version(&self) -> u32 {
        self.dsl_version
    }

    pub fn definition_revision_id(&self) -> &DefinitionRevisionId {
        &self.definition_revision_id
    }

    /// Returns the same closed workflow contract retargeted to a new immutable
    /// Definition Revision. Graph semantic editing uses this before rebuilding
    /// and verifying the complete candidate Plan; mutating a published Plan in
    /// place remains impossible.
    pub fn with_definition_revision_id(
        mut self,
        definition_revision_id: DefinitionRevisionId,
    ) -> Self {
        self.definition_revision_id = definition_revision_id;
        self
    }

    pub fn compiler_version(&self) -> &VersionTag {
        &self.compiler_version
    }

    pub fn author_format(&self) -> AuthorFormat {
        self.author_format
    }

    pub fn entry_node_id(&self) -> &NodeId {
        &self.entry_node_id
    }

    pub fn output_type(&self) -> &PlanType {
        &self.output_type
    }

    pub fn input_contract(&self) -> &PlanInputContract {
        &self.input_contract
    }

    pub fn error_type(&self) -> &PlanType {
        &self.error_type
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortDirection {
    Input,
    Output,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPort {
    pub(super) id: ControlPortId,
    pub(super) owner: NodeId,
    pub(super) name: PortName,
    pub(super) direction: PortDirection,
}

impl ControlPort {
    pub fn new(id: ControlPortId, owner: NodeId, name: PortName, direction: PortDirection) -> Self {
        Self {
            id,
            owner,
            name,
            direction,
        }
    }

    pub fn id(&self) -> &ControlPortId {
        &self.id
    }

    pub fn owner(&self) -> &NodeId {
        &self.owner
    }

    pub fn name(&self) -> &PortName {
        &self.name
    }

    pub fn direction(&self) -> PortDirection {
        self.direction
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataPort {
    pub(super) id: DataPortId,
    pub(super) owner: NodeId,
    pub(super) name: PortName,
    pub(super) direction: PortDirection,
    pub(super) value_type: PlanType,
    pub(super) required: bool,
}

impl DataPort {
    pub fn new(
        id: DataPortId,
        owner: NodeId,
        name: PortName,
        direction: PortDirection,
        value_type: PlanType,
        required: bool,
    ) -> Self {
        Self {
            id,
            owner,
            name,
            direction,
            value_type,
            required,
        }
    }

    pub fn id(&self) -> &DataPortId {
        &self.id
    }

    pub fn owner(&self) -> &NodeId {
        &self.owner
    }

    pub fn name(&self) -> &PortName {
        &self.name
    }

    pub fn direction(&self) -> PortDirection {
        self.direction
    }

    pub fn value_type(&self) -> &PlanType {
        &self.value_type
    }

    pub fn required(&self) -> bool {
        self.required
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlEdge {
    pub(super) id: ControlEdgeId,
    pub(super) from: ControlPortId,
    pub(super) to: ControlPortId,
}

impl ControlEdge {
    pub fn new(id: ControlEdgeId, from: ControlPortId, to: ControlPortId) -> Self {
        Self { id, from, to }
    }

    pub fn id(&self) -> &ControlEdgeId {
        &self.id
    }

    pub fn from(&self) -> &ControlPortId {
        &self.from
    }

    pub fn to(&self) -> &ControlPortId {
        &self.to
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataBinding {
    pub(super) id: DataBindingId,
    pub(super) source: ValueSource,
    pub(super) to: DataPortId,
}

impl DataBinding {
    pub fn new(id: DataBindingId, source: ValueSource, to: DataPortId) -> Self {
        Self { id, source, to }
    }

    pub fn from_port(id: DataBindingId, from: DataPortId, to: DataPortId) -> Self {
        Self::new(id, ValueSource::Port { port_id: from }, to)
    }

    pub fn id(&self) -> &DataBindingId {
        &self.id
    }

    pub fn source(&self) -> &ValueSource {
        &self.source
    }

    pub fn to(&self) -> &DataPortId {
        &self.to
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhiBinding {
    pub(super) id: PhiBindingId,
    pub(super) merge_node_id: NodeId,
    pub(super) output: DataPortId,
    pub(super) sources: BTreeMap<BranchCaseId, ValueSource>,
}

impl PhiBinding {
    pub fn new(
        id: PhiBindingId,
        merge_node_id: NodeId,
        output: DataPortId,
        sources: BTreeMap<BranchCaseId, ValueSource>,
    ) -> Self {
        Self {
            id,
            merge_node_id,
            output,
            sources,
        }
    }

    pub fn id(&self) -> &PhiBindingId {
        &self.id
    }

    pub fn merge_node_id(&self) -> &NodeId {
        &self.merge_node_id
    }

    pub fn output(&self) -> &DataPortId {
        &self.output
    }

    pub fn sources(&self) -> &BTreeMap<BranchCaseId, ValueSource> {
        &self.sources
    }
}

/// Closed typed source algebra for immutable values. Run input and pure
/// expressions are explicit graph dependencies; there is no implicit global
/// dictionary available to a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ValueSource {
    RunInput {
        /// Empty means the complete typed workflow input. Non-empty paths are
        /// traversed and type-checked by the verifier.
        path: Vec<String>,
    },
    /// A top-level optional input read. A missing field is represented as an
    /// absent bound input, never as a JSON value. The target DataPort must be
    /// optional and the consuming node must define how absence is handled.
    OptionalRunInput {
        path: Vec<String>,
    },
    Port {
        port_id: DataPortId,
    },
    Literal {
        value: Value,
    },
    Expression {
        expression: PureExpression,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpressionLanguage {
    Cel,
    Template,
    Match,
    Value,
    Literal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PureExpression {
    pub language: ExpressionLanguage,
    pub engine_version: VersionTag,
    pub source: String,
    pub result_type: PlanType,
    /// Symbol name -> explicit data dependency. Dependencies may reference an
    /// input owned by the evaluating node or a dominating output/capture.
    pub dependencies: BTreeMap<String, DataPortId>,
}

impl PureExpression {
    pub fn new(
        language: ExpressionLanguage,
        engine_version: VersionTag,
        source: impl Into<String>,
        result_type: PlanType,
    ) -> Self {
        Self {
            language,
            engine_version,
            source: source.into(),
            result_type,
            dependencies: BTreeMap::new(),
        }
    }

    pub fn with_dependency(mut self, name: impl Into<String>, port: DataPortId) -> Self {
        self.dependencies.insert(name.into(), port);
        self
    }
}

/// Closed public-configuration value algebra. Secret material has no variant;
/// descriptor secret fields use the separate opaque `secret_configuration`
/// map of validated references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DescriptorValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Number(serde_json::Number),
    String(String),
    Array(Vec<DescriptorValue>),
    Object(BTreeMap<String, DescriptorValue>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeafTaskDescriptor {
    pub implementation: String,
    pub descriptor_version: VersionTag,
    /// Non-secret descriptor values. Secret-bearing fields have a separate,
    /// structurally opaque channel below; verifier logic never guesses secrecy
    /// from a field name.
    pub public_configuration: BTreeMap<String, DescriptorValue>,
    pub secret_configuration: BTreeMap<String, SecretRef>,
}

impl LeafTaskDescriptor {
    pub fn new(
        implementation: impl Into<String>,
        descriptor_version: VersionTag,
        public_configuration: BTreeMap<String, DescriptorValue>,
    ) -> Self {
        Self {
            implementation: implementation.into(),
            descriptor_version,
            public_configuration,
            secret_configuration: BTreeMap::new(),
        }
    }

    pub fn with_secret(mut self, field: impl Into<String>, secret_ref: SecretRef) -> Self {
        self.secret_configuration.insert(field.into(), secret_ref);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchCase {
    pub case_id: BranchCaseId,
    pub condition: Option<PureExpression>,
    pub output_port: ControlPortId,
}

impl BranchCase {
    pub fn when(
        case_id: BranchCaseId,
        condition: PureExpression,
        output_port: ControlPortId,
    ) -> Self {
        Self {
            case_id,
            condition: Some(condition),
            output_port,
        }
    }

    pub fn otherwise(case_id: BranchCaseId, output_port: ControlPortId) -> Self {
        Self {
            case_id,
            condition: None,
            output_port,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchDescriptor {
    /// Declaration order is semantic and is retained by canonical hashing.
    pub cases: Vec<BranchCase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeDescriptor {
    pub branch_node_id: NodeId,
    /// Correlation is keyed by the originating named Branch case.
    pub arms: BTreeMap<BranchCaseId, ControlPortId>,
    pub output_port: ControlPortId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkLegDescriptor {
    pub leg_id: LegId,
    pub scope_id: ScopeId,
    pub output_port: ControlPortId,
    /// Exactly one typed value yielded by this static leg. Collect derives its
    /// closed result record from these ports in declaration order.
    pub yield_port: DataPortId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkDescriptor {
    /// Declaration order is semantic and fixes Join/Collect result ordering.
    pub legs: Vec<ForkLegDescriptor>,
    pub join_mode: PlanJoinMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinDescriptor {
    pub fork_node_id: NodeId,
    pub mode: PlanJoinMode,
    /// Must exactly match the correlated Fork member set.
    pub legs: BTreeMap<LegId, ControlPortId>,
    pub output_port: ControlPortId,
}

/// Canonical Plan barrier semantics, deliberately independent from runtime
/// projection/state APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanJoinMode {
    AllSuccess,
    AllSettled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapDescriptor {
    pub items: PureExpression,
    pub body_scope_id: ScopeId,
    /// Per-activation item value exposed by the Map node to its body scope.
    pub item_port: DataPortId,
    /// Typed body value collected in persisted input order.
    pub yield_port: DataPortId,
    pub max_concurrency: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CollectSource {
    StaticFork {
        fork_node_id: NodeId,
        join_node_id: NodeId,
        mode: PlanJoinMode,
    },
    Map {
        map_node_id: NodeId,
    },
    /// A dynamically-instantiated Map. `key_field=Some` uses a stable business
    /// key; `None` uses the canonical input ordinal. The correlated inputs
    /// distinguish an empty input without fabricating a body Activation.
    DynamicMap {
        map_node_id: NodeId,
        key_field: Option<String>,
        empty_output: ControlPortId,
        body_input: ControlPortId,
        empty_input: ControlPortId,
    },
    /// The final typed state of a bounded Loop. `initial_input` is the
    /// zero-occurrence result, `state_port` is exposed to each dynamic body
    /// occurrence, and `yield_port` is the next/final state produced by the
    /// body. A break path is explicit rather than encoded as a business error.
    Loop {
        loop_node_id: NodeId,
        initial_input: DataPortId,
        state_port: DataPortId,
        yield_port: DataPortId,
        completed_input: ControlPortId,
        break_input: Option<ControlPortId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectDescriptor {
    pub source: CollectSource,
    pub output_port: DataPortId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopDescriptor {
    pub flavor: LoopFlavor,
    pub continue_input: ControlPortId,
    pub body_output: ControlPortId,
    pub completed_output: ControlPortId,
    pub exit_condition: PureExpression,
    pub max_iterations: Option<u32>,
    pub deadline_ms: Option<u64>,
}

/// Closed execution semantics for a bounded loop.
///
/// `Agent` is not an authoring alias for an ordinary workflow loop: runtime
/// scope identity, event projection, and recovery all depend on preserving
/// the distinction in the immutable Plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopFlavor {
    Workflow,
    Agent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorBoundaryDescriptor {
    pub protected_scope_id: ScopeId,
    pub handler_scope_id: ScopeId,
    /// A finalizer is a durable child control path, not an ordinary successor.
    /// It is entered by the scheduler while unwinding both success and failure
    /// paths and may therefore execute while the Run has a termination intent.
    pub finalizer_scope_id: Option<ScopeId>,
    pub catch_kind: CatchFailureKind,
    pub protected_output: ControlPortId,
    pub handler_output: ControlPortId,
    pub finalizer_output: Option<ControlPortId>,
    /// Present only when the protected block has a normal completion path.
    /// Authored Return/Raise paths unwind through scheduler state instead of
    /// manufacturing a control edge back into the boundary.
    pub protected_completed_input: Option<ControlPortId>,
    /// Present only when the handler has a normal completion path.
    pub handler_completed_input: Option<ControlPortId>,
    /// Present only when the finalizer has a normal completion path. The
    /// finalizer scope/output may still exist when every authored path exits.
    pub finalizer_completed_input: Option<ControlPortId>,
    /// Present only when at least one complete boundary path can continue.
    pub completed_output: Option<ControlPortId>,
    pub error_port: DataPortId,
}

/// The only failure class an authored catch may consume. Control termination,
/// infrastructure faults, invariant failures, cancellation and panics have no
/// variant and therefore cannot be silently converted into business data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatchFailureKind {
    SafeBusinessFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubflowCallDescriptor {
    pub definition_revision_id: DefinitionRevisionId,
    pub interface_version: VersionTag,
    /// Static scope contract from which every durable invocation scope is
    /// instantiated. It is owned by this call node and has kind=Subflow.
    pub invocation_scope_id: ScopeId,
    /// Authored child input names mapped to their typed Plan input ports.
    ///
    /// Expression dependency ports owned by this node are intentionally not
    /// part of this map and therefore cannot leak into the child RunInput.
    pub inputs: BTreeMap<PortName, DataPortId>,
    /// Durable child deadline policy. The repository evaluates it against its
    /// own clock and caps it at the parent Run deadline.
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitSignalDescriptor {
    pub signal_name: String,
    pub payload_type: PlanType,
}

/// Static routing and lease contract for a durable human work item.
///
/// Completion deliberately retains a scheduler-owned signal identity so that
/// the existing signal/timer first-winner protocol remains the sole wait
/// resolution authority. The work item is nevertheless a first-class Plan
/// node and durable projection, not a syntax alias for `WaitSignal`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskDescriptor {
    pub completion_signal: String,
    pub request_input: DataPortId,
    pub request_type: PlanType,
    pub response_type: PlanType,
    #[serde(default)]
    pub assignees: Vec<String>,
    #[serde(default)]
    pub candidate_groups: Vec<String>,
    pub claim_lease_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerDescriptor {
    pub delay_ms: PureExpression,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReturnDescriptor {
    pub value_input: DataPortId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaiseDescriptor {
    pub error_input: DataPortId,
}

/// Closed Plan node algebra. Adding a new executable node is a wire-contract
/// change, not an arbitrary descriptor string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "descriptor",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum NodeKind {
    LlmTask(LeafTaskDescriptor),
    ActionTask(LeafTaskDescriptor),
    RetrievalTask(LeafTaskDescriptor),
    HttpTask(LeafTaskDescriptor),
    ToolTask(LeafTaskDescriptor),
    Branch(BranchDescriptor),
    Merge(MergeDescriptor),
    Fork(ForkDescriptor),
    Join(JoinDescriptor),
    Map(MapDescriptor),
    Collect(CollectDescriptor),
    Loop(LoopDescriptor),
    ErrorBoundary(ErrorBoundaryDescriptor),
    SubflowCall(SubflowCallDescriptor),
    HumanTask(HumanTaskDescriptor),
    WaitSignal(WaitSignalDescriptor),
    Timer(TimerDescriptor),
    Return(ReturnDescriptor),
    Raise(RaiseDescriptor),
}

impl NodeKind {
    pub fn name(&self) -> &'static str {
        match self {
            Self::LlmTask(_) => "llm_task",
            Self::ActionTask(_) => "action_task",
            Self::RetrievalTask(_) => "retrieval_task",
            Self::HttpTask(_) => "http_task",
            Self::ToolTask(_) => "tool_task",
            Self::Branch(_) => "branch",
            Self::Merge(_) => "merge",
            Self::Fork(_) => "fork",
            Self::Join(_) => "join",
            Self::Map(_) => "map",
            Self::Collect(_) => "collect",
            Self::Loop(_) => "loop",
            Self::ErrorBoundary(_) => "error_boundary",
            Self::SubflowCall(_) => "subflow_call",
            Self::HumanTask(_) => "human_task",
            Self::WaitSignal(_) => "wait_signal",
            Self::Timer(_) => "timer",
            Self::Return(_) => "return",
            Self::Raise(_) => "raise",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Return(_) | Self::Raise(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub(super) id: NodeId,
    pub(super) scope_id: ScopeId,
    pub(super) kind: NodeKind,
}

impl Node {
    pub fn new(id: NodeId, scope_id: ScopeId, kind: NodeKind) -> Self {
        Self { id, scope_id, kind }
    }

    pub fn id(&self) -> &NodeId {
        &self.id
    }

    pub fn scope_id(&self) -> &ScopeId {
        &self.scope_id
    }

    pub fn kind(&self) -> &NodeKind {
        &self.kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScopeKind {
    Root,
    Lexical,
    BranchArm {
        branch_node_id: NodeId,
        case_id: BranchCaseId,
    },
    ForkLeg {
        fork_node_id: NodeId,
        leg_id: LegId,
    },
    MapBody {
        map_node_id: NodeId,
    },
    LoopBody {
        loop_node_id: NodeId,
    },
    ErrorProtected {
        boundary_node_id: NodeId,
    },
    ErrorHandler {
        boundary_node_id: NodeId,
    },
    ErrorFinalizer {
        boundary_node_id: NodeId,
    },
    Subflow {
        call_node_id: NodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeMetadata {
    pub(super) id: ScopeId,
    pub(super) parent: Option<ScopeId>,
    pub(super) owner_node: Option<NodeId>,
    pub(super) kind: ScopeKind,
    pub(super) captures: BTreeSet<DataPortId>,
}

impl ScopeMetadata {
    pub fn root(id: ScopeId) -> Self {
        Self {
            id,
            parent: None,
            owner_node: None,
            kind: ScopeKind::Root,
            captures: BTreeSet::new(),
        }
    }

    pub fn child(
        id: ScopeId,
        parent: ScopeId,
        owner_node: NodeId,
        kind: ScopeKind,
        captures: BTreeSet<DataPortId>,
    ) -> Self {
        Self {
            id,
            parent: Some(parent),
            owner_node: Some(owner_node),
            kind,
            captures,
        }
    }

    pub fn id(&self) -> &ScopeId {
        &self.id
    }

    pub fn parent(&self) -> Option<&ScopeId> {
        self.parent.as_ref()
    }

    pub fn owner_node(&self) -> Option<&NodeId> {
        self.owner_node.as_ref()
    }

    pub fn kind(&self) -> &ScopeKind {
        &self.kind
    }

    pub fn captures(&self) -> &BTreeSet<DataPortId> {
        &self.captures
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeoutPolicy {
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetPolicy {
    pub max_tokens: Option<u64>,
    pub max_cost_microunits: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "descriptor",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PolicyKind {
    Retry(RetryPolicy),
    Timeout(TimeoutPolicy),
    Budget(BudgetPolicy),
}

impl PolicyKind {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Retry(_) => "retry",
            Self::Timeout(_) => "timeout",
            Self::Budget(_) => "budget",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub(super) id: PolicyId,
    pub(super) node_id: NodeId,
    pub(super) kind: PolicyKind,
}

impl Policy {
    pub fn new(id: PolicyId, node_id: NodeId, kind: PolicyKind) -> Self {
        Self { id, node_id, kind }
    }

    pub fn id(&self) -> &PolicyId {
        &self.id
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn kind(&self) -> &PolicyKind {
        &self.kind
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePosition {
    pub offset: u64,
    pub line: u32,
    pub column: u32,
}

impl SourcePosition {
    pub fn new(offset: u64, line: u32, column: u32) -> Self {
        Self {
            offset,
            line,
            column,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpan {
    pub source_id: SourceDocumentId,
    pub start: SourcePosition,
    pub end: SourcePosition,
}

impl SourceSpan {
    pub fn new(source_id: SourceDocumentId, start: SourcePosition, end: SourcePosition) -> Self {
        Self {
            source_id,
            start,
            end,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceMapPolicy {
    /// Explicit fixture/compiler-owned exemption. Only Programmatic Plans may
    /// use it; authored revisions must publish complete hashed provenance.
    #[default]
    ProgrammaticExempt,
    AuthoredComplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceMap {
    policy: SourceMapPolicy,
    documents: BTreeMap<SourceDocumentId, ContentHash>,
    nodes: BTreeMap<NodeId, SourceSpan>,
    control_ports: BTreeMap<ControlPortId, SourceSpan>,
    data_ports: BTreeMap<DataPortId, SourceSpan>,
    control_edges: BTreeMap<ControlEdgeId, SourceSpan>,
    data_bindings: BTreeMap<DataBindingId, SourceSpan>,
    phi_bindings: BTreeMap<PhiBindingId, SourceSpan>,
    scopes: BTreeMap<ScopeId, SourceSpan>,
    policies: BTreeMap<PolicyId, SourceSpan>,
}

impl Default for SourceMap {
    fn default() -> Self {
        Self {
            policy: SourceMapPolicy::ProgrammaticExempt,
            documents: BTreeMap::new(),
            nodes: BTreeMap::new(),
            control_ports: BTreeMap::new(),
            data_ports: BTreeMap::new(),
            control_edges: BTreeMap::new(),
            data_bindings: BTreeMap::new(),
            phi_bindings: BTreeMap::new(),
            scopes: BTreeMap::new(),
            policies: BTreeMap::new(),
        }
    }
}

impl SourceMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn authored(source_id: SourceDocumentId, content_hash: ContentHash) -> Self {
        let mut value = Self {
            policy: SourceMapPolicy::AuthoredComplete,
            ..Self::default()
        };
        value.documents.insert(source_id, content_hash);
        value
    }

    pub fn coverage_policy(&self) -> SourceMapPolicy {
        self.policy
    }

    pub fn documents(&self) -> &BTreeMap<SourceDocumentId, ContentHash> {
        &self.documents
    }

    pub fn insert_document(
        &mut self,
        id: SourceDocumentId,
        content_hash: ContentHash,
    ) -> Option<ContentHash> {
        self.documents.insert(id, content_hash)
    }

    pub fn insert_node(&mut self, id: NodeId, span: SourceSpan) -> Option<SourceSpan> {
        self.nodes.insert(id, span)
    }

    pub fn insert_control_port(
        &mut self,
        id: ControlPortId,
        span: SourceSpan,
    ) -> Option<SourceSpan> {
        self.control_ports.insert(id, span)
    }

    pub fn insert_data_port(&mut self, id: DataPortId, span: SourceSpan) -> Option<SourceSpan> {
        self.data_ports.insert(id, span)
    }

    pub fn insert_control_edge(
        &mut self,
        id: ControlEdgeId,
        span: SourceSpan,
    ) -> Option<SourceSpan> {
        self.control_edges.insert(id, span)
    }

    pub fn insert_data_binding(
        &mut self,
        id: DataBindingId,
        span: SourceSpan,
    ) -> Option<SourceSpan> {
        self.data_bindings.insert(id, span)
    }

    pub fn insert_phi_binding(&mut self, id: PhiBindingId, span: SourceSpan) -> Option<SourceSpan> {
        self.phi_bindings.insert(id, span)
    }

    pub fn insert_scope(&mut self, id: ScopeId, span: SourceSpan) -> Option<SourceSpan> {
        self.scopes.insert(id, span)
    }

    pub fn insert_policy(&mut self, id: PolicyId, span: SourceSpan) -> Option<SourceSpan> {
        self.policies.insert(id, span)
    }

    pub fn node(&self, id: &NodeId) -> Option<&SourceSpan> {
        self.nodes.get(id)
    }

    pub fn control_port(&self, id: &ControlPortId) -> Option<&SourceSpan> {
        self.control_ports.get(id)
    }

    pub fn data_port(&self, id: &DataPortId) -> Option<&SourceSpan> {
        self.data_ports.get(id)
    }

    pub fn control_edge(&self, id: &ControlEdgeId) -> Option<&SourceSpan> {
        self.control_edges.get(id)
    }

    pub fn data_binding(&self, id: &DataBindingId) -> Option<&SourceSpan> {
        self.data_bindings.get(id)
    }

    pub fn phi_binding(&self, id: &PhiBindingId) -> Option<&SourceSpan> {
        self.phi_bindings.get(id)
    }

    pub fn scope(&self, id: &ScopeId) -> Option<&SourceSpan> {
        self.scopes.get(id)
    }

    pub fn policy(&self, id: &PolicyId) -> Option<&SourceSpan> {
        self.policies.get(id)
    }

    pub(super) fn nodes(&self) -> &BTreeMap<NodeId, SourceSpan> {
        &self.nodes
    }

    pub(super) fn control_ports(&self) -> &BTreeMap<ControlPortId, SourceSpan> {
        &self.control_ports
    }

    pub(super) fn data_ports(&self) -> &BTreeMap<DataPortId, SourceSpan> {
        &self.data_ports
    }

    pub(super) fn control_edges(&self) -> &BTreeMap<ControlEdgeId, SourceSpan> {
        &self.control_edges
    }

    pub(super) fn data_bindings(&self) -> &BTreeMap<DataBindingId, SourceSpan> {
        &self.data_bindings
    }

    pub(super) fn phi_bindings(&self) -> &BTreeMap<PhiBindingId, SourceSpan> {
        &self.phi_bindings
    }

    pub(super) fn scopes(&self) -> &BTreeMap<ScopeId, SourceSpan> {
        &self.scopes
    }

    pub(super) fn policies(&self) -> &BTreeMap<PolicyId, SourceSpan> {
        &self.policies
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SemanticHash(String);

impl SemanticHash {
    pub(super) fn from_digest(value: String) -> Self {
        debug_assert!(is_valid_sha256(&value));
        Self(value)
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, PlanError> {
        let value = value.into();
        if !is_valid_sha256(&value) {
            return Err(PlanError::new(
                super::PLAN_WIRE_INVALID,
                "semantic hash must use sha256:<64 lowercase hexadecimal digits>",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn is_valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

impl std::fmt::Display for SemanticHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for SemanticHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SemanticHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(D::Error::custom)
    }
}

/// Published, immutable Canonical Typed Plan. Its fields are intentionally not
/// publicly mutable; construction and deserialization both pass the verifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Plan {
    pub(super) metadata: PlanMetadata,
    pub(super) nodes: Vec<Node>,
    pub(super) control_ports: Vec<ControlPort>,
    pub(super) data_ports: Vec<DataPort>,
    pub(super) control_edges: Vec<ControlEdge>,
    pub(super) data_bindings: Vec<DataBinding>,
    pub(super) phi_bindings: Vec<PhiBinding>,
    pub(super) scopes: Vec<ScopeMetadata>,
    pub(super) policies: Vec<Policy>,
    pub(super) source_map: SourceMap,
    pub(super) semantic_hash: SemanticHash,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanWire {
    metadata: PlanMetadata,
    nodes: Vec<Node>,
    control_ports: Vec<ControlPort>,
    data_ports: Vec<DataPort>,
    control_edges: Vec<ControlEdge>,
    data_bindings: Vec<DataBinding>,
    phi_bindings: Vec<PhiBinding>,
    scopes: Vec<ScopeMetadata>,
    policies: Vec<Policy>,
    source_map: SourceMap,
    semantic_hash: SemanticHash,
}

/// Intermediate JSON tree that rejects duplicate object members at every
/// nesting level before typed maps can collapse them with last-value-wins.
struct UniqueJsonValue(Value);

impl<'de> Deserialize<'de> for UniqueJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = UniqueJsonValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value without duplicate object members")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueJsonValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueJsonValue(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueJsonValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
        while let Some(value) = sequence.next_element::<UniqueJsonValue>()? {
            values.push(value.0);
        }
        Ok(UniqueJsonValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::with_capacity(object.size_hint().unwrap_or(0));
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(A::Error::custom(format!(
                    "{PLAN_WIRE_INVALID}: duplicate JSON object member '{key}'"
                )));
            }
            let value = object.next_value::<UniqueJsonValue>()?;
            values.insert(key, value.0);
        }
        Ok(UniqueJsonValue(Value::Object(values)))
    }
}

impl<'de> Deserialize<'de> for Plan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let unique = UniqueJsonValue::deserialize(deserializer)?;
        let wire = PlanWire::deserialize(unique.0).map_err(D::Error::custom)?;
        let plan = Self::from_parts(
            wire.metadata,
            wire.nodes,
            wire.control_ports,
            wire.data_ports,
            wire.control_edges,
            wire.data_bindings,
            wire.phi_bindings,
            wire.scopes,
            wire.policies,
            wire.source_map,
            wire.semantic_hash,
        );
        verify_plan(&plan).map_err(D::Error::custom)?;
        let expected = semantic_hash_for_plan(&plan).map_err(D::Error::custom)?;
        if expected != plan.semantic_hash {
            return Err(D::Error::custom(PlanError::new(
                PLAN_HASH_MISMATCH,
                format!(
                    "semantic hash mismatch: wire has {}, canonical projection computes {}",
                    plan.semantic_hash, expected
                ),
            )));
        }
        Ok(plan)
    }
}

impl Plan {
    /// Authoritative JSON publication boundary. It rejects oversized payloads,
    /// duplicate keys, invalid wire values/structure, and stale hashes before
    /// yielding an immutable Plan.
    pub fn decode_json(input: &[u8]) -> Result<Self, PlanError> {
        if input.len() > MAX_PLAN_JSON_BYTES {
            return Err(PlanError::new(
                PLAN_WIRE_INVALID,
                format!("Plan JSON exceeds the {MAX_PLAN_JSON_BYTES}-byte publication limit"),
            ));
        }
        serde_json::from_slice(input).map_err(|error| {
            PlanError::new(
                PLAN_WIRE_INVALID,
                format!("Plan JSON failed authoritative decoding: {error}"),
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_parts(
        metadata: PlanMetadata,
        mut nodes: Vec<Node>,
        mut control_ports: Vec<ControlPort>,
        mut data_ports: Vec<DataPort>,
        mut control_edges: Vec<ControlEdge>,
        mut data_bindings: Vec<DataBinding>,
        mut phi_bindings: Vec<PhiBinding>,
        mut scopes: Vec<ScopeMetadata>,
        mut policies: Vec<Policy>,
        source_map: SourceMap,
        semantic_hash: SemanticHash,
    ) -> Self {
        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        control_ports.sort_by(|left, right| left.id.cmp(&right.id));
        data_ports.sort_by(|left, right| left.id.cmp(&right.id));
        control_edges.sort_by(|left, right| left.id.cmp(&right.id));
        data_bindings.sort_by(|left, right| left.id.cmp(&right.id));
        phi_bindings.sort_by(|left, right| left.id.cmp(&right.id));
        scopes.sort_by(|left, right| left.id.cmp(&right.id));
        policies.sort_by(|left, right| left.id.cmp(&right.id));
        Self {
            metadata,
            nodes,
            control_ports,
            data_ports,
            control_edges,
            data_bindings,
            phi_bindings,
            scopes,
            policies,
            source_map,
            semantic_hash,
        }
    }

    pub fn metadata(&self) -> &PlanMetadata {
        &self.metadata
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn control_ports(&self) -> &[ControlPort] {
        &self.control_ports
    }

    pub fn data_ports(&self) -> &[DataPort] {
        &self.data_ports
    }

    pub fn control_edges(&self) -> &[ControlEdge] {
        &self.control_edges
    }

    pub fn data_bindings(&self) -> &[DataBinding] {
        &self.data_bindings
    }

    pub fn phi_bindings(&self) -> &[PhiBinding] {
        &self.phi_bindings
    }

    pub fn scopes(&self) -> &[ScopeMetadata] {
        &self.scopes
    }

    pub fn policies(&self) -> &[Policy] {
        &self.policies
    }

    pub fn source_map(&self) -> &SourceMap {
        &self.source_map
    }

    pub fn semantic_hash(&self) -> &SemanticHash {
        &self.semantic_hash
    }

    pub fn verify(&self) -> Result<(), PlanError> {
        verify_plan(self)?;
        let expected = semantic_hash_for_plan(self)?;
        if expected != self.semantic_hash {
            return Err(PlanError::new(
                PLAN_HASH_MISMATCH,
                "stored semantic hash does not match canonical semantic projection",
            ));
        }
        Ok(())
    }

    pub fn canonical_semantic_bytes(&self) -> Result<Vec<u8>, PlanError> {
        super::semantic::canonical_semantic_bytes(self)
    }
}
