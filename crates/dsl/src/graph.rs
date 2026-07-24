//! Canvas authoring contracts for the current DSL.
//!
//! Graph authoring is a first-class source format, not a serialized [`Plan`].
//! [`GraphAuthorDocument`] publishes explicit nodes, ports, edges, bindings,
//! scopes and policies. A verified Plan is rebuilt at every decode/edit
//! boundary and only cached in memory. Layout and live execution state are
//! separate documents linked only by stable IDs.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use serde::{
    de::{Error as _, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;

use crate::{CompileError, DslPath, SourceSpan as DslSourceSpan};
use insight_engine::{
    plan::{
        AuthorFormat, BranchDescriptor, ControlEdge, ControlEdgeId, ControlPort, ControlPortId,
        DataBinding, DataBindingId, DataPort, DataPortId, ForkDescriptor, JoinDescriptor,
        LoopDescriptor, MapDescriptor, Node, NodeKind, PhiBinding, PhiBindingId, Plan, PlanBuilder,
        PlanDiagnosticTarget, PlanError, PlanMetadata, Policy, PolicyId, ScopeId, ScopeMetadata,
        SemanticHash, SourceDocumentId, SourceMap, SourceMapPolicy, SourcePosition,
        SourceSpan as PlanSourceSpan, PLAN_HASH_MISMATCH,
    },
    ActivationId, ContentHash, DefinitionRevisionId, NodeId, RunId,
};

use super::{
    compile, compile_source, raw, validate, CompileOptions, StructuredAuthorDocument,
    COMPILER_VERSION,
};

pub const GRAPH_AUTHOR_SCHEMA_VERSION: u32 = 2;
pub const VIEW_SCHEMA_VERSION: u32 = 1;
pub const TRACE_OVERLAY_SCHEMA_VERSION: u32 = 1;
pub const MAX_GRAPH_DOCUMENT_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_STRUCTURED_REDUCTION_SOURCE_BYTES: usize = 16 * 1024 * 1024;
pub const GRAPH_SEMANTIC_EDIT_SCHEMA_VERSION: u32 = 1;
pub const MAX_GRAPH_SEMANTIC_EDITS: usize = 4_096;

pub const GRAPH_DOCUMENT_INVALID: &str = "DSL_GRAPH_DOCUMENT_INVALID";
pub const GRAPH_PLAN_INVALID: &str = "DSL_GRAPH_PLAN_INVALID";
pub const GRAPH_PLAN_HASH_MISMATCH: &str = "DSL_GRAPH_PLAN_HASH_MISMATCH";
pub const GRAPH_STRUCTURED_COMPILE_FAILED: &str = "DSL_GRAPH_STRUCTURED_COMPILE_FAILED";
pub const GRAPH_IRREDUCIBLE: &str = "DSL_GRAPH_IRREDUCIBLE";
pub const GRAPH_REDUCTION_UNSUPPORTED: &str = "DSL_GRAPH_REDUCTION_UNSUPPORTED";
pub const GRAPH_REDUCTION_MISMATCH: &str = "DSL_GRAPH_REDUCTION_MISMATCH";
pub const GRAPH_EDIT_KIND_MISMATCH: &str = "DSL_GRAPH_EDIT_KIND_MISMATCH";
pub const GRAPH_EDIT_INVALID: &str = "DSL_GRAPH_EDIT_INVALID";
pub const GRAPH_EDIT_CONFLICT: &str = "DSL_GRAPH_EDIT_CONFLICT";
pub const VIEW_DOCUMENT_INVALID: &str = "DSL_VIEW_DOCUMENT_INVALID";
pub const TRACE_OVERLAY_INVALID: &str = "DSL_TRACE_OVERLAY_INVALID";

/// Stable identity shared by a graph author document, its view, and trace
/// overlays.  It is intentionally independent from a Definition Revision so a
/// new draft/revision can retain canvas identity without changing Plan IDs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct GraphDocumentId(String);

impl GraphDocumentId {
    pub fn new(value: impl Into<String>) -> Result<Self, GraphAuthorError> {
        let value = value.into();
        let mut bytes = value.bytes();
        let valid = value.len() <= 128
            && bytes
                .next()
                .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
            && bytes.all(|byte| {
                byte == b'_'
                    || byte == b'-'
                    || byte == b'.'
                    || byte == b':'
                    || byte.is_ascii_alphanumeric()
            });
        if !valid {
            return Err(GraphAuthorError::new(
                GRAPH_DOCUMENT_INVALID,
                "graph document ID must be a non-empty stable ASCII identifier of at most 128 bytes",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GraphDocumentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for GraphDocumentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphAuthoringMode {
    Graph,
}

/// A non-authoritative proof that this graph came from a structured source.
///
/// It is private and never consulted to execute or hash the graph.  The
/// reverse converter recompiles it and compares semantic hashes before
/// returning the original source.  Consequently it cannot be used to invent a
/// structured document for an arbitrary or irreducible graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StructuredReductionCertificate {
    compiler_version: String,
    source: String,
    source_id: String,
    definition_revision_id: DefinitionRevisionId,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    prompt_files: BTreeMap<String, String>,
}

impl StructuredReductionCertificate {
    fn options(&self) -> CompileOptions {
        let mut options = CompileOptions::new(
            self.definition_revision_id.clone(),
            self.source_id.clone(),
            &self.source,
        );
        options.prompt_files.clone_from(&self.prompt_files);
        options
    }

    fn validate_bounds(&self) -> Result<(), GraphAuthorError> {
        let prompt_bytes = self
            .prompt_files
            .iter()
            .try_fold(0usize, |total, (path, content)| {
                total
                    .checked_add(path.len())
                    .and_then(|value| value.checked_add(content.len()))
            })
            .ok_or_else(|| {
                GraphAuthorError::new(
                    GRAPH_DOCUMENT_INVALID,
                    "structured reduction resources exceed supported size",
                )
            })?;
        if self.source.len() > MAX_STRUCTURED_REDUCTION_SOURCE_BYTES
            || prompt_bytes > MAX_STRUCTURED_REDUCTION_SOURCE_BYTES
            || self.source_id.is_empty()
            || self.source_id.len() > 512
        {
            return Err(GraphAuthorError::new(
                GRAPH_DOCUMENT_INVALID,
                "structured reduction certificate is empty or exceeds supported bounds",
            ));
        }
        Ok(())
    }
}

/// Graph author semantics are stored as explicit top-level graph parts.
///
/// `compiled_plan` is a private, non-serialized publication cache rebuilt from
/// those parts at every decode/edit boundary. The author document therefore
/// cannot smuggle in a precomputed semantic hash or become an editable copy of
/// the Canonical Plan. Coordinates, viewport, colors, annotations and runtime
/// state remain in separate documents below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphAuthorDocument {
    schema_version: u32,
    document_id: GraphDocumentId,
    authoring_mode: GraphAuthoringMode,
    metadata: PlanMetadata,
    nodes: Vec<Node>,
    ports: GraphPorts,
    edges: GraphEdges,
    bindings: GraphBindings,
    scopes: Vec<ScopeMetadata>,
    policies: Vec<Policy>,
    source_map: SourceMap,
    #[serde(skip_serializing)]
    compiled_plan: Plan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    structured_reduction: Option<StructuredReductionCertificate>,
}

/// Typed graph ports. The split remains explicit because control tokens and
/// data values have different verifier and runtime contracts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPorts {
    control: Vec<ControlPort>,
    data: Vec<DataPort>,
}

impl GraphPorts {
    pub fn control(&self) -> &[ControlPort] {
        &self.control
    }

    pub fn data(&self) -> &[DataPort] {
        &self.data
    }
}

/// Typed graph edges. Data flow is represented by bindings rather than by an
/// untyped second edge kind, preserving the Canonical Plan value-source model.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphEdges {
    control: Vec<ControlEdge>,
}

impl GraphEdges {
    pub fn control(&self) -> &[ControlEdge] {
        &self.control
    }
}

/// Typed value bindings, including Phi contracts at control-flow merges.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphBindings {
    data: Vec<DataBinding>,
    phi: Vec<PhiBinding>,
}

impl GraphBindings {
    pub fn data(&self) -> &[DataBinding] {
        &self.data
    }

    pub fn phi(&self) -> &[PhiBinding] {
        &self.phi
    }
}

/// Type-directed canvas semantic edits. The editor never exposes a mutable
/// canonical Plan: all requested changes are staged together and pass the full
/// verifier/hash boundary atomically before replacing the document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphSemanticEdit {
    UpsertNode {
        node: Node,
    },
    DeleteNode {
        node_id: NodeId,
    },
    UpsertControlPort {
        port: ControlPort,
    },
    DeleteControlPort {
        port_id: ControlPortId,
    },
    UpsertDataPort {
        port: DataPort,
    },
    DeleteDataPort {
        port_id: DataPortId,
    },
    UpsertControlEdge {
        edge: ControlEdge,
    },
    DeleteControlEdge {
        edge_id: ControlEdgeId,
    },
    UpsertDataBinding {
        binding: DataBinding,
    },
    DeleteDataBinding {
        binding_id: DataBindingId,
    },
    UpsertPhiBinding {
        binding: PhiBinding,
    },
    DeletePhiBinding {
        binding_id: PhiBindingId,
    },
    UpsertScope {
        scope: ScopeMetadata,
    },
    DeleteScope {
        scope_id: ScopeId,
    },
    UpsertPolicy {
        policy: Policy,
    },
    DeletePolicy {
        policy_id: PolicyId,
    },
    Branch {
        node_id: NodeId,
        descriptor: BranchDescriptor,
    },
    Parallel {
        fork_node_id: NodeId,
        fork: ForkDescriptor,
        join_node_id: NodeId,
        join: JoinDescriptor,
    },
    Map {
        node_id: NodeId,
        descriptor: MapDescriptor,
    },
    Loop {
        node_id: NodeId,
        descriptor: LoopDescriptor,
    },
}

/// Closed wire transaction for one immutable Graph revision edit.
///
/// `expected_semantic_hash` is the compare side of the CAS. The candidate is
/// rebuilt under `target_definition_revision_id`, fully verified, and only
/// then may the service publish it. Individual edits are never persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSemanticEditBatch {
    schema_version: u32,
    expected_semantic_hash: SemanticHash,
    target_definition_revision_id: DefinitionRevisionId,
    edits: Vec<GraphSemanticEdit>,
}

impl GraphSemanticEditBatch {
    pub fn new(
        expected_semantic_hash: SemanticHash,
        target_definition_revision_id: DefinitionRevisionId,
        edits: Vec<GraphSemanticEdit>,
    ) -> Result<Self, GraphAuthorError> {
        let value = Self {
            schema_version: GRAPH_SEMANTIC_EDIT_SCHEMA_VERSION,
            expected_semantic_hash,
            target_definition_revision_id,
            edits,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn decode_json(bytes: &[u8]) -> Result<Self, GraphAuthorError> {
        if bytes.len() > MAX_GRAPH_DOCUMENT_BYTES {
            return Err(GraphAuthorError::new(
                GRAPH_EDIT_INVALID,
                "graph semantic edit document exceeds the supported size",
            ));
        }
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let unique = UniqueGraphJson::deserialize(&mut deserializer)
            .map_err(|error| GraphAuthorError::new(GRAPH_EDIT_INVALID, error.to_string()))?;
        deserializer
            .end()
            .map_err(|error| GraphAuthorError::new(GRAPH_EDIT_INVALID, error.to_string()))?;
        let value = Self::deserialize(unique.0)
            .map_err(|error| GraphAuthorError::new(GRAPH_EDIT_INVALID, error.to_string()))?;
        value.validate()?;
        Ok(value)
    }

    pub fn encode_json(&self) -> Result<Vec<u8>, GraphAuthorError> {
        self.validate()?;
        serde_json::to_vec(self)
            .map_err(|error| GraphAuthorError::new(GRAPH_EDIT_INVALID, error.to_string()))
    }

    pub fn expected_semantic_hash(&self) -> &SemanticHash {
        &self.expected_semantic_hash
    }

    pub fn target_definition_revision_id(&self) -> &DefinitionRevisionId {
        &self.target_definition_revision_id
    }

    pub fn edits(&self) -> &[GraphSemanticEdit] {
        &self.edits
    }

    fn validate(&self) -> Result<(), GraphAuthorError> {
        if self.schema_version != GRAPH_SEMANTIC_EDIT_SCHEMA_VERSION
            || self.edits.is_empty()
            || self.edits.len() > MAX_GRAPH_SEMANTIC_EDITS
        {
            return Err(GraphAuthorError::new(
                GRAPH_EDIT_INVALID,
                "graph semantic edit schema version or batch size is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphAuthorWire {
    schema_version: u32,
    document_id: GraphDocumentId,
    authoring_mode: GraphAuthoringMode,
    metadata: PlanMetadata,
    nodes: Vec<Node>,
    ports: GraphPorts,
    edges: GraphEdges,
    bindings: GraphBindings,
    scopes: Vec<ScopeMetadata>,
    policies: Vec<Policy>,
    source_map: SourceMap,
    #[serde(default)]
    structured_reduction: Option<StructuredReductionCertificate>,
}

/// Intermediate JSON tree that rejects duplicate members at every nesting
/// level. In particular, typed maps must never silently apply last-write-wins
/// before graph verification sees the authored document.
struct UniqueGraphJson(Value);

impl<'de> Deserialize<'de> for UniqueGraphJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueGraphJsonVisitor)
    }
}

struct UniqueGraphJsonVisitor;

impl<'de> Visitor<'de> for UniqueGraphJsonVisitor {
    type Value = UniqueGraphJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a graph JSON value without duplicate object members")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueGraphJson(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueGraphJson(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueGraphJson(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueGraphJson)
            .ok_or_else(|| E::custom("non-finite graph JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueGraphJson(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueGraphJson(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueGraphJson(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueGraphJson(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueGraphJson::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
        while let Some(value) = sequence.next_element::<UniqueGraphJson>()? {
            values.push(value.0);
        }
        Ok(UniqueGraphJson(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::with_capacity(object.size_hint().unwrap_or(0));
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(A::Error::custom(format!(
                    "{GRAPH_DOCUMENT_INVALID}: duplicate JSON object member '{key}'"
                )));
            }
            let value = object.next_value::<UniqueGraphJson>()?;
            values.insert(key, value.0);
        }
        Ok(UniqueGraphJson(Value::Object(values)))
    }
}

impl<'de> Deserialize<'de> for GraphAuthorDocument {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let unique = UniqueGraphJson::deserialize(deserializer)?;
        let wire = GraphAuthorWire::deserialize(unique.0).map_err(D::Error::custom)?;
        GraphAuthorDocument::from_wire(wire).map_err(D::Error::custom)
    }
}

fn graph_metadata(metadata: &PlanMetadata) -> PlanMetadata {
    PlanMetadata::new(
        metadata.definition_revision_id().clone(),
        metadata.compiler_version().clone(),
        AuthorFormat::Graph,
        metadata.entry_node_id().clone(),
        metadata.input_contract().clone(),
        metadata.output_type().clone(),
        metadata.error_type().clone(),
    )
}

fn graph_source_map(
    document_id: &GraphDocumentId,
    plan: &Plan,
    preserve_authored_source: bool,
) -> Result<SourceMap, GraphAuthorError> {
    if preserve_authored_source
        && plan.source_map().coverage_policy() == SourceMapPolicy::AuthoredComplete
    {
        return Ok(plan.source_map().clone());
    }

    // A programmatic Plan has no author coordinates. Explicit conversion to a
    // graph creates a virtual canvas source whose identity is derived from the
    // semantic projection. Stable graph IDs remain the primary canvas
    // locations; the non-empty span satisfies the authored provenance contract
    // without pretending that source-code line numbers existed.
    let source_id = SourceDocumentId::new(format!("graph:{}", document_id.as_str()))
        .map_err(GraphAuthorError::from_plan)?;
    let semantic_bytes = plan
        .canonical_semantic_bytes()
        .map_err(GraphAuthorError::from_plan)?;
    let mut canvas_source = VirtualCanvasSource::new(source_id.clone(), &semantic_bytes);
    let node_spans = plan
        .nodes()
        .iter()
        .map(|node| {
            (
                node.id().clone(),
                canvas_source.allocate("node", node.id().as_str()),
            )
        })
        .collect::<Vec<_>>();
    let control_port_spans = plan
        .control_ports()
        .iter()
        .map(|port| {
            (
                port.id().clone(),
                canvas_source.allocate("control_port", port.id().as_str()),
            )
        })
        .collect::<Vec<_>>();
    let data_port_spans = plan
        .data_ports()
        .iter()
        .map(|port| {
            (
                port.id().clone(),
                canvas_source.allocate("data_port", port.id().as_str()),
            )
        })
        .collect::<Vec<_>>();
    let control_edge_spans = plan
        .control_edges()
        .iter()
        .map(|edge| {
            (
                edge.id().clone(),
                canvas_source.allocate("control_edge", edge.id().as_str()),
            )
        })
        .collect::<Vec<_>>();
    let data_binding_spans = plan
        .data_bindings()
        .iter()
        .map(|binding| {
            (
                binding.id().clone(),
                canvas_source.allocate("data_binding", binding.id().as_str()),
            )
        })
        .collect::<Vec<_>>();
    let phi_binding_spans = plan
        .phi_bindings()
        .iter()
        .map(|binding| {
            (
                binding.id().clone(),
                canvas_source.allocate("phi_binding", binding.id().as_str()),
            )
        })
        .collect::<Vec<_>>();
    let scope_spans = plan
        .scopes()
        .iter()
        .map(|scope| {
            (
                scope.id().clone(),
                canvas_source.allocate("scope", scope.id().as_str()),
            )
        })
        .collect::<Vec<_>>();
    let policy_spans = plan
        .policies()
        .iter()
        .map(|policy| {
            (
                policy.id().clone(),
                canvas_source.allocate("policy", policy.id().as_str()),
            )
        })
        .collect::<Vec<_>>();
    let mut source_map = SourceMap::authored(source_id, canvas_source.content_hash());
    for (id, span) in node_spans {
        source_map.insert_node(id, span);
    }
    for (id, span) in control_port_spans {
        source_map.insert_control_port(id, span);
    }
    for (id, span) in data_port_spans {
        source_map.insert_data_port(id, span);
    }
    for (id, span) in control_edge_spans {
        source_map.insert_control_edge(id, span);
    }
    for (id, span) in data_binding_spans {
        source_map.insert_data_binding(id, span);
    }
    for (id, span) in phi_binding_spans {
        source_map.insert_phi_binding(id, span);
    }
    for (id, span) in scope_spans {
        source_map.insert_scope(id, span);
    }
    for (id, span) in policy_spans {
        source_map.insert_policy(id, span);
    }
    Ok(source_map)
}

/// Builds complete provisional provenance for an edited set of graph parts.
/// The candidate must carry authored-complete provenance in order to cross the
/// Plan verifier. `replace_verified_plan` replaces this provisional map with
/// the canonical semantic Canvas source before the transaction commits.
#[allow(clippy::too_many_arguments)]
fn semantic_edit_source_map(
    document_id: &GraphDocumentId,
    metadata: &PlanMetadata,
    nodes: &[Node],
    control_ports: &[ControlPort],
    data_ports: &[DataPort],
    control_edges: &[ControlEdge],
    data_bindings: &[DataBinding],
    phi_bindings: &[PhiBinding],
    scopes: &[ScopeMetadata],
    policies: &[Policy],
) -> Result<SourceMap, GraphAuthorError> {
    let semantic_seed = serde_json::to_vec(&(
        metadata,
        nodes,
        control_ports,
        data_ports,
        control_edges,
        data_bindings,
        phi_bindings,
        scopes,
        policies,
    ))
    .map_err(|error| GraphAuthorError::new(GRAPH_EDIT_INVALID, error.to_string()))?;
    let source_id = SourceDocumentId::new(format!("graph:{}", document_id.as_str()))
        .map_err(GraphAuthorError::from_plan)?;
    let mut canvas_source = VirtualCanvasSource::new(source_id.clone(), &semantic_seed);

    let mut source_map = SourceMap::authored(source_id, ContentHash::from_bytes(b"pending"));
    for node in nodes {
        source_map.insert_node(
            node.id().clone(),
            canvas_source.allocate("node", node.id().as_str()),
        );
    }
    for port in control_ports {
        source_map.insert_control_port(
            port.id().clone(),
            canvas_source.allocate("control_port", port.id().as_str()),
        );
    }
    for port in data_ports {
        source_map.insert_data_port(
            port.id().clone(),
            canvas_source.allocate("data_port", port.id().as_str()),
        );
    }
    for edge in control_edges {
        source_map.insert_control_edge(
            edge.id().clone(),
            canvas_source.allocate("control_edge", edge.id().as_str()),
        );
    }
    for binding in data_bindings {
        source_map.insert_data_binding(
            binding.id().clone(),
            canvas_source.allocate("data_binding", binding.id().as_str()),
        );
    }
    for binding in phi_bindings {
        source_map.insert_phi_binding(
            binding.id().clone(),
            canvas_source.allocate("phi_binding", binding.id().as_str()),
        );
    }
    for scope in scopes {
        source_map.insert_scope(
            scope.id().clone(),
            canvas_source.allocate("scope", scope.id().as_str()),
        );
    }
    for policy in policies {
        source_map.insert_policy(
            policy.id().clone(),
            canvas_source.allocate("policy", policy.id().as_str()),
        );
    }
    // The content hash is computed only after all deterministic lines exist.
    source_map.insert_document(
        SourceDocumentId::new(format!("graph:{}", document_id.as_str()))
            .map_err(GraphAuthorError::from_plan)?,
        canvas_source.content_hash(),
    );
    Ok(source_map)
}

/// Deterministic virtual document used only after a semantic Canvas edit or
/// conversion from a programmatic Plan. Every semantic element owns a unique
/// line, while the header binds the complete canonical semantic projection.
struct VirtualCanvasSource {
    source_id: SourceDocumentId,
    content: String,
    next_line: u32,
}

impl VirtualCanvasSource {
    fn new(source_id: SourceDocumentId, semantic_bytes: &[u8]) -> Self {
        let semantic_hash = ContentHash::from_bytes(semantic_bytes);
        Self {
            source_id,
            content: format!("canvas_semantic {}\n", semantic_hash.as_str()),
            next_line: 2,
        }
    }

    fn allocate(&mut self, kind: &str, id: &str) -> PlanSourceSpan {
        let line = format!("{kind} {id}");
        let start_offset = self.content.len() as u64;
        let end_offset = start_offset + line.len() as u64;
        let source_line = self.next_line;
        self.content.push_str(&line);
        self.content.push('\n');
        self.next_line += 1;
        PlanSourceSpan::new(
            self.source_id.clone(),
            SourcePosition::new(start_offset, source_line, 1),
            SourcePosition::new(end_offset, source_line, line.chars().count() as u32 + 1),
        )
    }

    fn content_hash(&self) -> ContentHash {
        ContentHash::from_bytes(self.content.as_bytes())
    }
}

#[allow(clippy::too_many_arguments)]
fn compile_graph_parts(
    metadata: PlanMetadata,
    nodes: &[Node],
    control_ports: &[ControlPort],
    data_ports: &[DataPort],
    control_edges: &[ControlEdge],
    data_bindings: &[DataBinding],
    phi_bindings: &[PhiBinding],
    scopes: &[ScopeMetadata],
    policies: &[Policy],
    source_map: &SourceMap,
) -> Result<Plan, GraphAuthorError> {
    let mut builder = PlanBuilder::new(metadata);
    for node in nodes {
        builder.add_node(node.clone());
    }
    for port in control_ports {
        builder.add_control_port(port.clone());
    }
    for port in data_ports {
        builder.add_data_port(port.clone());
    }
    for edge in control_edges {
        builder.add_control_edge(edge.clone());
    }
    for binding in data_bindings {
        builder.add_data_binding(binding.clone());
    }
    for binding in phi_bindings {
        builder.add_phi_binding(binding.clone());
    }
    for scope in scopes {
        builder.add_scope(scope.clone());
    }
    for policy in policies {
        builder.add_policy(policy.clone());
    }
    builder.set_source_map(source_map.clone());
    builder.build().map_err(GraphAuthorError::from_plan)
}

fn upsert_graph_part<T>(values: &mut Vec<T>, value: T, same_identity: impl Fn(&T, &T) -> bool) {
    if let Some(existing) = values
        .iter_mut()
        .find(|existing| same_identity(existing, &value))
    {
        *existing = value;
    } else {
        values.push(value);
    }
}

fn delete_graph_part<T>(
    values: &mut Vec<T>,
    matches: impl Fn(&T) -> bool,
    kind: &str,
    id: &impl fmt::Display,
) -> Result<(), GraphAuthorError> {
    let Some(index) = values.iter().position(matches) else {
        return Err(GraphAuthorError::new(
            GRAPH_EDIT_INVALID,
            format!("graph edit references missing {kind} '{id}'"),
        ));
    };
    values.remove(index);
    Ok(())
}

fn replace_graph_node_kind(
    nodes: &mut [Node],
    node_id: &NodeId,
    expected: impl FnOnce(&NodeKind) -> bool,
    kind: NodeKind,
) -> Result<(), GraphAuthorError> {
    let node = nodes
        .iter_mut()
        .find(|node| node.id() == node_id)
        .ok_or_else(|| {
            GraphAuthorError::new(
                GRAPH_EDIT_KIND_MISMATCH,
                format!("graph edit references missing node '{node_id}'"),
            )
            .with_target(PlanDiagnosticTarget::Node {
                node_id: node_id.clone(),
            })
        })?;
    if !expected(node.kind()) {
        return Err(GraphAuthorError::new(
            GRAPH_EDIT_KIND_MISMATCH,
            format!(
                "graph edit kind does not match node '{}' ({})",
                node_id,
                node.kind().name()
            ),
        )
        .with_target(PlanDiagnosticTarget::Node {
            node_id: node_id.clone(),
        }));
    }
    *node = Node::new(node.id().clone(), node.scope_id().clone(), kind);
    Ok(())
}

impl GraphAuthorDocument {
    fn from_wire(wire: GraphAuthorWire) -> Result<Self, GraphAuthorError> {
        let plan = compile_graph_parts(
            wire.metadata,
            &wire.nodes,
            &wire.ports.control,
            &wire.ports.data,
            &wire.edges.control,
            &wire.bindings.data,
            &wire.bindings.phi,
            &wire.scopes,
            &wire.policies,
            &wire.source_map,
        )?;
        let mut document = Self::from_compiled_plan(wire.document_id, plan);
        document.schema_version = wire.schema_version;
        document.authoring_mode = wire.authoring_mode;
        document.structured_reduction = wire.structured_reduction;
        document.validate()?;
        Ok(document)
    }

    /// Explicit Plan -> graph conversion.  `Plan::verify` rechecks both the
    /// closed graph invariants and the stored semantic hash.
    pub fn from_verified_plan(
        document_id: GraphDocumentId,
        plan: Plan,
    ) -> Result<Self, GraphAuthorError> {
        plan.verify().map_err(GraphAuthorError::from_plan)?;
        let source_map = graph_source_map(&document_id, &plan, true)?;
        let normalized = compile_graph_parts(
            graph_metadata(plan.metadata()),
            plan.nodes(),
            plan.control_ports(),
            plan.data_ports(),
            plan.control_edges(),
            plan.data_bindings(),
            plan.phi_bindings(),
            plan.scopes(),
            plan.policies(),
            &source_map,
        )?;
        Ok(Self::from_compiled_plan(document_id, normalized))
    }

    /// Stable read-only execution graph derived from a pinned Canonical Plan.
    /// This does not create Graph Author edit authority or a View document.
    pub fn from_execution_plan(plan: Plan) -> Result<Self, GraphAuthorError> {
        let identity =
            ContentHash::from_bytes(plan.metadata().definition_revision_id().as_str().as_bytes());
        let document_id = GraphDocumentId::new(format!(
            "execution_{}",
            identity.as_str().trim_start_matches("sha256:")
        ))?;
        Self::from_verified_plan(document_id, plan)
    }

    fn from_compiled_plan(document_id: GraphDocumentId, plan: Plan) -> Self {
        Self {
            schema_version: GRAPH_AUTHOR_SCHEMA_VERSION,
            document_id,
            authoring_mode: GraphAuthoringMode::Graph,
            metadata: plan.metadata().clone(),
            nodes: plan.nodes().to_vec(),
            ports: GraphPorts {
                control: plan.control_ports().to_vec(),
                data: plan.data_ports().to_vec(),
            },
            edges: GraphEdges {
                control: plan.control_edges().to_vec(),
            },
            bindings: GraphBindings {
                data: plan.data_bindings().to_vec(),
                phi: plan.phi_bindings().to_vec(),
            },
            scopes: plan.scopes().to_vec(),
            policies: plan.policies().to_vec(),
            source_map: plan.source_map().clone(),
            compiled_plan: plan,
            structured_reduction: None,
        }
    }

    /// Explicit structured AST -> graph conversion through the current
    /// compiler. Since an AST has no lossless source spelling, a later reverse
    /// conversion can only return a generated canonical document after the
    /// native structural reducer proves complete semantic equivalence.
    pub fn from_structured(
        document_id: GraphDocumentId,
        document: StructuredAuthorDocument,
        options: CompileOptions,
    ) -> Result<Self, GraphAuthorError> {
        let plan = compile(document, options).map_err(GraphAuthorError::from_compile)?;
        Self::from_verified_plan(document_id, plan)
    }

    /// Lossless explicit structured source -> graph conversion.  The source
    /// and prompt resources form a reduction certificate; they remain
    /// non-authoritative until `to_structured` recompiles and hash-checks them.
    pub fn from_structured_source(
        document_id: GraphDocumentId,
        source: impl Into<String>,
        options: CompileOptions,
    ) -> Result<Self, GraphAuthorError> {
        let source = source.into();
        if source.len() > MAX_STRUCTURED_REDUCTION_SOURCE_BYTES {
            return Err(GraphAuthorError::new(
                GRAPH_DOCUMENT_INVALID,
                "structured source exceeds the graph conversion limit",
            ));
        }
        // Source hash/end coordinates are recomputed so the retained source
        // and SourceMap provenance cannot disagree.
        let mut canonical_options = CompileOptions::new(
            options.definition_revision_id.clone(),
            options.source_id.clone(),
            &source,
        );
        canonical_options
            .prompt_files
            .clone_from(&options.prompt_files);
        let plan = compile_source(&source, canonical_options.clone())
            .map_err(GraphAuthorError::from_compile)?;
        let mut graph = Self::from_verified_plan(document_id, plan)?;
        graph.structured_reduction = Some(StructuredReductionCertificate {
            compiler_version: COMPILER_VERSION.to_owned(),
            source,
            source_id: canonical_options.source_id,
            definition_revision_id: canonical_options.definition_revision_id,
            prompt_files: canonical_options.prompt_files,
        });
        Ok(graph)
    }

    /// Authoritative graph publication boundary. The wire format has explicit
    /// graph parts and no stored Plan/hash; decoding recompiles and verifies a
    /// fresh Canonical Plan.
    pub fn decode_json(input: &[u8]) -> Result<Self, GraphAuthorError> {
        if input.len() > MAX_GRAPH_DOCUMENT_BYTES {
            return Err(GraphAuthorError::new(
                GRAPH_DOCUMENT_INVALID,
                "graph author document exceeds the publication limit",
            ));
        }
        let unique = serde_json::from_slice::<UniqueGraphJson>(input).map_err(|error| {
            let message = error.to_string();
            GraphAuthorError::new(
                GRAPH_DOCUMENT_INVALID,
                format!("graph author decoding failed: {message}"),
            )
        })?;
        let wire = GraphAuthorWire::deserialize(unique.0).map_err(|error| {
            GraphAuthorError::new(
                GRAPH_DOCUMENT_INVALID,
                format!("graph author decoding failed: {error}"),
            )
        })?;
        Self::from_wire(wire)
    }

    pub fn encode_json(&self) -> Result<Vec<u8>, GraphAuthorError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| {
            GraphAuthorError::new(
                GRAPH_DOCUMENT_INVALID,
                format!("graph author encoding failed: {error}"),
            )
        })
    }

    pub fn validate(&self) -> Result<(), GraphAuthorError> {
        if self.schema_version != GRAPH_AUTHOR_SCHEMA_VERSION
            || self.authoring_mode != GraphAuthoringMode::Graph
            || self.metadata.author_format() != AuthorFormat::Graph
        {
            return Err(GraphAuthorError::new(
                GRAPH_DOCUMENT_INVALID,
                "unsupported graph author schema or authoring mode",
            ));
        }
        self.compiled_plan
            .verify()
            .map_err(GraphAuthorError::from_plan)?;
        let rebuilt = compile_graph_parts(
            self.metadata.clone(),
            &self.nodes,
            &self.ports.control,
            &self.ports.data,
            &self.edges.control,
            &self.bindings.data,
            &self.bindings.phi,
            &self.scopes,
            &self.policies,
            &self.source_map,
        )?;
        if rebuilt != self.compiled_plan {
            return Err(GraphAuthorError::new(
                GRAPH_DOCUMENT_INVALID,
                "graph author parts disagree with the compiled publication cache",
            ));
        }
        if let Some(certificate) = &self.structured_reduction {
            certificate.validate_bounds()?;
        }
        Ok(())
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn document_id(&self) -> &GraphDocumentId {
        &self.document_id
    }

    pub fn metadata(&self) -> &PlanMetadata {
        &self.metadata
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn ports(&self) -> &GraphPorts {
        &self.ports
    }

    pub fn edges(&self) -> &GraphEdges {
        &self.edges
    }

    pub fn bindings(&self) -> &GraphBindings {
        &self.bindings
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

    pub fn plan(&self) -> &Plan {
        &self.compiled_plan
    }

    pub fn semantic_hash(&self) -> &SemanticHash {
        self.compiled_plan.semantic_hash()
    }

    /// Graph -> verified Plan.  Verification is intentionally repeated at the
    /// editing boundary rather than trusting an in-memory caller.
    pub fn into_verified_plan(self) -> Result<Plan, GraphAuthorError> {
        self.compiled_plan
            .verify()
            .map_err(GraphAuthorError::from_plan)?;
        Ok(self.compiled_plan)
    }

    /// Minimum safe editing primitive.  The complete candidate must pass the
    /// Canonical Plan verifier; arbitrary partial mutation cannot bypass it.
    /// Replacing semantics invalidates any structured reduction certificate.
    pub fn replace_verified_plan(&mut self, candidate: Plan) -> Result<(), GraphAuthorError> {
        candidate.verify().map_err(GraphAuthorError::from_plan)?;
        // A semantic canvas edit is no longer located in the structured source
        // from which the graph may originally have been converted. Rebind all
        // spans to the graph-native virtual source instead of publishing stale
        // YAML provenance.
        let source_map = graph_source_map(&self.document_id, &candidate, false)?;
        let normalized = compile_graph_parts(
            graph_metadata(candidate.metadata()),
            candidate.nodes(),
            candidate.control_ports(),
            candidate.data_ports(),
            candidate.control_edges(),
            candidate.data_bindings(),
            candidate.phi_bindings(),
            candidate.scopes(),
            candidate.policies(),
            &source_map,
        )?;
        self.metadata = normalized.metadata().clone();
        self.nodes = normalized.nodes().to_vec();
        self.ports = GraphPorts {
            control: normalized.control_ports().to_vec(),
            data: normalized.data_ports().to_vec(),
        };
        self.edges = GraphEdges {
            control: normalized.control_edges().to_vec(),
        };
        self.bindings = GraphBindings {
            data: normalized.data_bindings().to_vec(),
            phi: normalized.phi_bindings().to_vec(),
        };
        self.scopes = normalized.scopes().to_vec();
        self.policies = normalized.policies().to_vec();
        self.source_map = normalized.source_map().clone();
        self.compiled_plan = normalized;
        self.structured_reduction = None;
        Ok(())
    }

    pub fn replace_plan_json(&mut self, candidate: &[u8]) -> Result<(), GraphAuthorError> {
        let plan = Plan::decode_json(candidate).map_err(GraphAuthorError::from_plan)?;
        self.replace_verified_plan(plan)
    }

    /// Applies an in-process semantic edit transaction without changing the
    /// Definition Revision identity. Service/API callers use
    /// [`Self::apply_semantic_edit_batch`] to cross an explicit CAS boundary.
    pub fn apply_semantic_edits(
        &mut self,
        edits: impl IntoIterator<Item = GraphSemanticEdit>,
    ) -> Result<(), GraphAuthorError> {
        let edits = edits.into_iter().collect::<Vec<_>>();
        self.apply_semantic_edits_with_metadata(self.metadata.clone(), edits)
    }

    /// Applies one closed wire transaction to a new immutable Definition
    /// Revision. Hash mismatch or any invalid candidate leaves `self`
    /// byte-for-byte unchanged.
    pub fn apply_semantic_edit_batch(
        &mut self,
        batch: GraphSemanticEditBatch,
    ) -> Result<(), GraphAuthorError> {
        batch.validate()?;
        if batch.expected_semantic_hash() != self.semantic_hash() {
            return Err(GraphAuthorError::new(
                GRAPH_EDIT_CONFLICT,
                "graph semantic edit base hash no longer matches",
            ));
        }
        if batch.target_definition_revision_id() == self.metadata.definition_revision_id() {
            return Err(GraphAuthorError::new(
                GRAPH_EDIT_INVALID,
                "graph semantic edit target revision must differ from its base revision",
            ));
        }
        let metadata = self
            .metadata
            .clone()
            .with_definition_revision_id(batch.target_definition_revision_id().clone());
        self.apply_semantic_edits_with_metadata(metadata, batch.edits)
    }

    fn apply_semantic_edits_with_metadata(
        &mut self,
        metadata: PlanMetadata,
        edits: Vec<GraphSemanticEdit>,
    ) -> Result<(), GraphAuthorError> {
        let mut nodes = self.nodes.clone();
        let mut control_ports = self.ports.control.clone();
        let mut data_ports = self.ports.data.clone();
        let mut control_edges = self.edges.control.clone();
        let mut data_bindings = self.bindings.data.clone();
        let mut phi_bindings = self.bindings.phi.clone();
        let mut scopes = self.scopes.clone();
        let mut policies = self.policies.clone();

        for edit in edits {
            match edit {
                GraphSemanticEdit::UpsertNode { node } => {
                    upsert_graph_part(&mut nodes, node, |left, right| left.id() == right.id());
                }
                GraphSemanticEdit::DeleteNode { node_id } => {
                    delete_graph_part(&mut nodes, |node| node.id() == &node_id, "node", &node_id)?
                }
                GraphSemanticEdit::UpsertControlPort { port } => {
                    upsert_graph_part(&mut control_ports, port, |left, right| {
                        left.id() == right.id()
                    });
                }
                GraphSemanticEdit::DeleteControlPort { port_id } => delete_graph_part(
                    &mut control_ports,
                    |port| port.id() == &port_id,
                    "control port",
                    &port_id,
                )?,
                GraphSemanticEdit::UpsertDataPort { port } => {
                    upsert_graph_part(&mut data_ports, port, |left, right| left.id() == right.id());
                }
                GraphSemanticEdit::DeleteDataPort { port_id } => delete_graph_part(
                    &mut data_ports,
                    |port| port.id() == &port_id,
                    "data port",
                    &port_id,
                )?,
                GraphSemanticEdit::UpsertControlEdge { edge } => {
                    upsert_graph_part(&mut control_edges, edge, |left, right| {
                        left.id() == right.id()
                    });
                }
                GraphSemanticEdit::DeleteControlEdge { edge_id } => delete_graph_part(
                    &mut control_edges,
                    |edge| edge.id() == &edge_id,
                    "control edge",
                    &edge_id,
                )?,
                GraphSemanticEdit::UpsertDataBinding { binding } => {
                    upsert_graph_part(&mut data_bindings, binding, |left, right| {
                        left.id() == right.id()
                    });
                }
                GraphSemanticEdit::DeleteDataBinding { binding_id } => delete_graph_part(
                    &mut data_bindings,
                    |binding| binding.id() == &binding_id,
                    "data binding",
                    &binding_id,
                )?,
                GraphSemanticEdit::UpsertPhiBinding { binding } => {
                    upsert_graph_part(&mut phi_bindings, binding, |left, right| {
                        left.id() == right.id()
                    });
                }
                GraphSemanticEdit::DeletePhiBinding { binding_id } => delete_graph_part(
                    &mut phi_bindings,
                    |binding| binding.id() == &binding_id,
                    "Phi binding",
                    &binding_id,
                )?,
                GraphSemanticEdit::UpsertScope { scope } => {
                    upsert_graph_part(&mut scopes, scope, |left, right| left.id() == right.id());
                }
                GraphSemanticEdit::DeleteScope { scope_id } => delete_graph_part(
                    &mut scopes,
                    |scope| scope.id() == &scope_id,
                    "scope",
                    &scope_id,
                )?,
                GraphSemanticEdit::UpsertPolicy { policy } => {
                    upsert_graph_part(&mut policies, policy, |left, right| left.id() == right.id());
                }
                GraphSemanticEdit::DeletePolicy { policy_id } => delete_graph_part(
                    &mut policies,
                    |policy| policy.id() == &policy_id,
                    "policy",
                    &policy_id,
                )?,
                GraphSemanticEdit::Branch {
                    node_id,
                    descriptor,
                } => replace_graph_node_kind(
                    &mut nodes,
                    &node_id,
                    |kind| matches!(kind, NodeKind::Branch(_)),
                    NodeKind::Branch(descriptor),
                )?,
                GraphSemanticEdit::Parallel {
                    fork_node_id,
                    fork,
                    join_node_id,
                    join,
                } => {
                    replace_graph_node_kind(
                        &mut nodes,
                        &fork_node_id,
                        |kind| matches!(kind, NodeKind::Fork(_)),
                        NodeKind::Fork(fork),
                    )?;
                    replace_graph_node_kind(
                        &mut nodes,
                        &join_node_id,
                        |kind| matches!(kind, NodeKind::Join(_)),
                        NodeKind::Join(join),
                    )?;
                }
                GraphSemanticEdit::Map {
                    node_id,
                    descriptor,
                } => replace_graph_node_kind(
                    &mut nodes,
                    &node_id,
                    |kind| matches!(kind, NodeKind::Map(_)),
                    NodeKind::Map(descriptor),
                )?,
                GraphSemanticEdit::Loop {
                    node_id,
                    descriptor,
                } => replace_graph_node_kind(
                    &mut nodes,
                    &node_id,
                    |kind| matches!(kind, NodeKind::Loop(_)),
                    NodeKind::Loop(descriptor),
                )?,
            }
        }

        let source_map = semantic_edit_source_map(
            &self.document_id,
            &metadata,
            &nodes,
            &control_ports,
            &data_ports,
            &control_edges,
            &data_bindings,
            &phi_bindings,
            &scopes,
            &policies,
        )?;
        let candidate = compile_graph_parts(
            metadata,
            &nodes,
            &control_ports,
            &data_ports,
            &control_edges,
            &data_bindings,
            &phi_bindings,
            &scopes,
            &policies,
            &source_map,
        )?;
        self.replace_verified_plan(candidate)
    }

    /// Closed graph -> structured conversion. A retained source certificate
    /// remains the lossless fast path. Native graphs are structurally reduced,
    /// recompiled, and accepted only when the complete semantic hash matches.
    /// Crossing/overlapping regions and non-representable contracts remain in
    /// graph mode with a stable irreducibility diagnostic.
    pub fn to_structured(&self) -> Result<ReducedStructuredDocument, GraphReductionDiagnostic> {
        let Some(certificate) = self.structured_reduction.as_ref() else {
            return self.reduce_native_graph();
        };
        if certificate.compiler_version != COMPILER_VERSION {
            return Err(GraphReductionDiagnostic::new(
                GRAPH_REDUCTION_UNSUPPORTED,
                "structured reduction was produced by an unsupported compiler version",
            ));
        }
        let raw = raw::parse(&certificate.source).map_err(|error| {
            GraphReductionDiagnostic::new(
                GRAPH_REDUCTION_MISMATCH,
                format!(
                    "retained structured source no longer parses: {}",
                    error.code()
                ),
            )
        })?;
        let document = validate(raw).map_err(|error| {
            GraphReductionDiagnostic::new(
                GRAPH_REDUCTION_MISMATCH,
                format!(
                    "retained structured source no longer validates: {}",
                    error.code()
                ),
            )
        })?;
        let candidate =
            compile_source(&certificate.source, certificate.options()).map_err(|error| {
                GraphReductionDiagnostic::new(
                    GRAPH_REDUCTION_MISMATCH,
                    format!(
                        "retained structured source no longer compiles: {}",
                        error.code()
                    ),
                )
            })?;
        if candidate.semantic_hash() != self.compiled_plan.semantic_hash() {
            return Err(GraphReductionDiagnostic::new(
                GRAPH_REDUCTION_MISMATCH,
                "structured reduction semantic hash differs from the graph Plan",
            ));
        }
        Ok(ReducedStructuredDocument {
            document,
            source: certificate.source.clone(),
        })
    }

    fn reduce_native_graph(&self) -> Result<ReducedStructuredDocument, GraphReductionDiagnostic> {
        let source = super::reducer::reduce_plan(&self.compiled_plan).map_err(|reason| {
            GraphReductionDiagnostic::new(
                GRAPH_IRREDUCIBLE,
                format!(
                    "graph is not structurally reducible ({reason}); retain graph authoring mode"
                ),
            )
        })?;
        if source.len() > MAX_STRUCTURED_REDUCTION_SOURCE_BYTES {
            return Err(GraphReductionDiagnostic::new(
                GRAPH_IRREDUCIBLE,
                "reduced structured source exceeds the supported bound; retain graph authoring mode",
            ));
        }
        let raw = raw::parse(&source).map_err(|error| {
            GraphReductionDiagnostic::new(
                GRAPH_IRREDUCIBLE,
                format!(
                    "native graph reduction did not produce valid source ({}); retain graph authoring mode",
                    error.code()
                ),
            )
        })?;
        let document = validate(raw).map_err(|error| {
            GraphReductionDiagnostic::new(
                GRAPH_IRREDUCIBLE,
                format!(
                    "native graph reduction is outside the structured author contract ({}); retain graph authoring mode",
                    error.code()
                ),
            )
        })?;
        let options = CompileOptions::new(
            self.compiled_plan
                .metadata()
                .definition_revision_id()
                .clone(),
            format!("graph-reduction:{}.json", self.document_id),
            &source,
        );
        let candidate = compile_source(&source, options).map_err(|error| {
            GraphReductionDiagnostic::new(
                GRAPH_IRREDUCIBLE,
                format!(
                    "native graph reduction cannot be compiled losslessly ({}); retain graph authoring mode",
                    error.code()
                ),
            )
        })?;
        if candidate.semantic_hash() != self.compiled_plan.semantic_hash() {
            return Err(GraphReductionDiagnostic::new(
                GRAPH_IRREDUCIBLE,
                "native graph reduction changes Plan semantics; retain graph authoring mode",
            ));
        }
        Ok(ReducedStructuredDocument { document, source })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReducedStructuredDocument {
    document: StructuredAuthorDocument,
    source: String,
}

impl ReducedStructuredDocument {
    pub fn document(&self) -> &StructuredAuthorDocument {
        &self.document
    }

    /// Certificate-backed conversion returns the exact retained source;
    /// native conversion returns a deterministic generated JSON/YAML document.
    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn into_document(self) -> StructuredAuthorDocument {
        self.document
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphReductionDiagnostic {
    code: &'static str,
    message: String,
}

impl GraphReductionDiagnostic {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for GraphReductionDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for GraphReductionDiagnostic {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphAuthorError {
    code: &'static str,
    message: String,
    diagnostic: Option<Box<GraphAuthorDiagnostic>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct GraphAuthorDiagnostic {
    target: Option<PlanDiagnosticTarget>,
    plan_source_span: Option<PlanSourceSpan>,
    source_path: Option<DslPath>,
    structured_source_span: Option<DslSourceSpan>,
    decoded_template_span: Option<DslSourceSpan>,
    agent_id: Option<String>,
    step_id: Option<String>,
}

impl GraphAuthorError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            diagnostic: None,
        }
    }

    fn from_plan(error: PlanError) -> Self {
        let code = if error.code() == PLAN_HASH_MISMATCH {
            GRAPH_PLAN_HASH_MISMATCH
        } else {
            GRAPH_PLAN_INVALID
        };
        let mut value = Self::new(code, format!("{}: {}", error.code(), error.message()));
        if error.target().is_some() || error.source_span().is_some() {
            let diagnostic = value.diagnostic_mut();
            diagnostic.target = error.target().cloned();
            diagnostic.plan_source_span = error.source_span().cloned();
        }
        value
    }

    fn from_compile(error: CompileError) -> Self {
        let mut value = Self::new(
            GRAPH_STRUCTURED_COMPILE_FAILED,
            format!("{}: {}", error.code(), error.message()),
        );
        if error.path().is_some()
            || error.span().is_some()
            || error.decoded_template_span().is_some()
            || error.agent_id().is_some()
            || error.step_id().is_some()
        {
            let diagnostic = value.diagnostic_mut();
            diagnostic.source_path = error.path().cloned();
            diagnostic.structured_source_span = error.span();
            diagnostic.decoded_template_span = error.decoded_template_span();
            diagnostic.agent_id = error.agent_id().map(str::to_owned);
            diagnostic.step_id = error.step_id().map(str::to_owned);
        }
        value
    }

    fn with_target(mut self, target: PlanDiagnosticTarget) -> Self {
        self.diagnostic_mut().target = Some(target);
        self
    }

    fn diagnostic_mut(&mut self) -> &mut GraphAuthorDiagnostic {
        self.diagnostic
            .get_or_insert_with(|| Box::new(GraphAuthorDiagnostic::default()))
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn target(&self) -> Option<&PlanDiagnosticTarget> {
        self.diagnostic
            .as_deref()
            .and_then(|diagnostic| diagnostic.target.as_ref())
    }

    /// SourceMap location associated with a Plan target. For native Canvas
    /// edits this is a unique virtual graph line; structured conversions keep
    /// their original authored source document location.
    pub fn plan_source_span(&self) -> Option<&PlanSourceSpan> {
        self.diagnostic
            .as_deref()
            .and_then(|diagnostic| diagnostic.plan_source_span.as_ref())
    }

    pub fn source_path(&self) -> Option<&DslPath> {
        self.diagnostic
            .as_deref()
            .and_then(|diagnostic| diagnostic.source_path.as_ref())
    }

    pub fn structured_source_span(&self) -> Option<DslSourceSpan> {
        self.diagnostic
            .as_deref()
            .and_then(|diagnostic| diagnostic.structured_source_span)
    }

    pub fn decoded_template_span(&self) -> Option<DslSourceSpan> {
        self.diagnostic
            .as_deref()
            .and_then(|diagnostic| diagnostic.decoded_template_span)
    }

    pub fn agent_id(&self) -> Option<&str> {
        self.diagnostic
            .as_deref()
            .and_then(|diagnostic| diagnostic.agent_id.as_deref())
    }

    pub fn step_id(&self) -> Option<&str> {
        self.diagnostic
            .as_deref()
            .and_then(|diagnostic| diagnostic.step_id.as_deref())
    }
}

impl fmt::Display for GraphAuthorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for GraphAuthorError {}

// -------------------------------------------------------------------------
// View document: visual state only, linked by stable graph/node IDs.

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanvasPoint {
    pub x: f64,
    pub y: f64,
}

impl CanvasPoint {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    fn is_finite(self) -> bool {
        self.x.is_finite() && self.y.is_finite()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanvasSize {
    pub width: f64,
    pub height: f64,
}

impl CanvasSize {
    pub fn new(width: f64, height: f64) -> Self {
        Self { width, height }
    }

    fn is_valid(self) -> bool {
        self.width.is_finite() && self.height.is_finite() && self.width > 0.0 && self.height > 0.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeView {
    pub position: CanvasPoint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<CanvasSize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotation: Option<String>,
    #[serde(default)]
    pub collapsed: bool,
}

impl NodeView {
    pub fn at(x: f64, y: f64) -> Self {
        Self {
            position: CanvasPoint::new(x, y),
            size: None,
            color: None,
            annotation: None,
            collapsed: false,
        }
    }

    fn validate(&self) -> Result<(), GraphAuthorError> {
        if !self.position.is_finite() || self.size.is_some_and(|size| !size.is_valid()) {
            return Err(GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                "node position and optional size must be finite; dimensions must be positive",
            ));
        }
        if self.color.as_ref().is_some_and(|value| value.len() > 128)
            || self
                .annotation
                .as_ref()
                .is_some_and(|value| value.len() > 64 * 1024)
        {
            return Err(GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                "node color or annotation exceeds supported bounds",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Viewport {
    pub origin: CanvasPoint,
    pub zoom: f64,
}

impl Default for Viewport {
    fn default() -> Self {
        Self {
            origin: CanvasPoint::new(0.0, 0.0),
            zoom: 1.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewDocument {
    schema_version: u32,
    graph_document_id: GraphDocumentId,
    viewport: Viewport,
    #[serde(default)]
    nodes: BTreeMap<NodeId, NodeView>,
}

impl ViewDocument {
    pub fn new(graph_document_id: GraphDocumentId) -> Self {
        Self {
            schema_version: VIEW_SCHEMA_VERSION,
            graph_document_id,
            viewport: Viewport::default(),
            nodes: BTreeMap::new(),
        }
    }

    pub fn decode_json(input: &[u8]) -> Result<Self, GraphAuthorError> {
        if input.len() > MAX_GRAPH_DOCUMENT_BYTES {
            return Err(GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                "view document exceeds the publication limit",
            ));
        }
        let unique: UniqueGraphJson = serde_json::from_slice(input).map_err(|error| {
            GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                format!("view document decoding failed: {error}"),
            )
        })?;
        let value: Self = serde_json::from_value(unique.0).map_err(|error| {
            GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                format!("view document decoding failed: {error}"),
            )
        })?;
        value.validate_intrinsic()?;
        Ok(value)
    }

    pub fn encode_json(&self) -> Result<Vec<u8>, GraphAuthorError> {
        self.validate_intrinsic()?;
        serde_json::to_vec(self).map_err(|error| {
            GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                format!("view document encoding failed: {error}"),
            )
        })
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn graph_document_id(&self) -> &GraphDocumentId {
        &self.graph_document_id
    }

    pub fn viewport(&self) -> Viewport {
        self.viewport
    }

    pub fn set_viewport(&mut self, viewport: Viewport) -> Result<(), GraphAuthorError> {
        if !viewport.origin.is_finite() || !viewport.zoom.is_finite() || viewport.zoom <= 0.0 {
            return Err(GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                "viewport origin and zoom must be finite and zoom must be positive",
            ));
        }
        self.viewport = viewport;
        Ok(())
    }

    pub fn node(&self, node_id: &NodeId) -> Option<&NodeView> {
        self.nodes.get(node_id)
    }

    pub fn nodes(&self) -> &BTreeMap<NodeId, NodeView> {
        &self.nodes
    }

    pub fn set_node(&mut self, node_id: NodeId, view: NodeView) -> Result<(), GraphAuthorError> {
        view.validate()?;
        self.nodes.insert(node_id, view);
        Ok(())
    }

    pub fn validate_against(&self, graph: &GraphAuthorDocument) -> Result<(), GraphAuthorError> {
        self.validate_intrinsic()?;
        if self.graph_document_id != graph.document_id {
            return Err(GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                "view references a different graph document",
            ));
        }
        let nodes = graph
            .compiled_plan
            .nodes()
            .iter()
            .map(|node| node.id())
            .collect::<BTreeSet<_>>();
        if let Some(unknown) = self.nodes.keys().find(|node_id| !nodes.contains(node_id)) {
            return Err(GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                format!("view references unknown node '{unknown}'"),
            )
            .with_target(PlanDiagnosticTarget::Node {
                node_id: (*unknown).clone(),
            }));
        }
        Ok(())
    }

    fn validate_intrinsic(&self) -> Result<(), GraphAuthorError> {
        if self.schema_version != VIEW_SCHEMA_VERSION
            || !self.viewport.origin.is_finite()
            || !self.viewport.zoom.is_finite()
            || self.viewport.zoom <= 0.0
        {
            return Err(GraphAuthorError::new(
                VIEW_DOCUMENT_INVALID,
                "unsupported view schema or invalid viewport",
            ));
        }
        for view in self.nodes.values() {
            view.validate()?;
        }
        Ok(())
    }
}

// -------------------------------------------------------------------------
// Runtime trace overlay: operational projection only, never author semantics.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceActivationState {
    Created,
    Ready,
    Leased,
    Running,
    RetryWait,
    Waiting,
    Terminating,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationTrace {
    pub activation_id: ActivationId,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    pub state: TraceActivationState,
}

impl ActivationTrace {
    pub fn new(
        activation_id: ActivationId,
        node_id: NodeId,
        attempt: Option<u32>,
        state: TraceActivationState,
    ) -> Self {
        Self {
            activation_id,
            node_id,
            attempt,
            state,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceOverlay {
    schema_version: u32,
    graph_document_id: GraphDocumentId,
    run_id: RunId,
    #[serde(default)]
    activations: Vec<ActivationTrace>,
}

impl TraceOverlay {
    pub fn new(graph_document_id: GraphDocumentId, run_id: RunId) -> Self {
        Self {
            schema_version: TRACE_OVERLAY_SCHEMA_VERSION,
            graph_document_id,
            run_id,
            activations: Vec::new(),
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn graph_document_id(&self) -> &GraphDocumentId {
        &self.graph_document_id
    }

    pub fn activations(&self) -> &[ActivationTrace] {
        &self.activations
    }

    pub fn decode_json(input: &[u8]) -> Result<Self, GraphAuthorError> {
        if input.len() > MAX_GRAPH_DOCUMENT_BYTES {
            return Err(GraphAuthorError::new(
                TRACE_OVERLAY_INVALID,
                "trace overlay exceeds the publication limit",
            ));
        }
        let unique: UniqueGraphJson = serde_json::from_slice(input).map_err(|error| {
            GraphAuthorError::new(
                TRACE_OVERLAY_INVALID,
                format!("trace overlay decoding failed: {error}"),
            )
        })?;
        let value: Self = serde_json::from_value(unique.0).map_err(|error| {
            GraphAuthorError::new(
                TRACE_OVERLAY_INVALID,
                format!("trace overlay decoding failed: {error}"),
            )
        })?;
        value.validate_intrinsic()?;
        Ok(value)
    }

    pub fn encode_json(&self) -> Result<Vec<u8>, GraphAuthorError> {
        self.validate_intrinsic()?;
        serde_json::to_vec(self).map_err(|error| {
            GraphAuthorError::new(
                TRACE_OVERLAY_INVALID,
                format!("trace overlay encoding failed: {error}"),
            )
        })
    }

    pub fn add_activation(&mut self, trace: ActivationTrace) -> Result<(), GraphAuthorError> {
        if self
            .activations
            .iter()
            .any(|existing| existing.activation_id == trace.activation_id)
        {
            return Err(GraphAuthorError::new(
                TRACE_OVERLAY_INVALID,
                "trace overlay contains a duplicate activation ID",
            ));
        }
        self.activations.push(trace);
        self.activations
            .sort_by(|left, right| left.activation_id.cmp(&right.activation_id));
        Ok(())
    }

    pub fn validate_against(&self, graph: &GraphAuthorDocument) -> Result<(), GraphAuthorError> {
        self.validate_intrinsic()?;
        if self.graph_document_id != graph.document_id {
            return Err(GraphAuthorError::new(
                TRACE_OVERLAY_INVALID,
                "unsupported trace schema or trace references a different graph document",
            ));
        }
        let nodes = graph
            .compiled_plan
            .nodes()
            .iter()
            .map(|node| node.id())
            .collect::<BTreeSet<_>>();
        for trace in &self.activations {
            if !nodes.contains(&trace.node_id) {
                return Err(GraphAuthorError::new(
                    TRACE_OVERLAY_INVALID,
                    format!("trace references unknown node '{}'", trace.node_id),
                )
                .with_target(PlanDiagnosticTarget::Node {
                    node_id: trace.node_id.clone(),
                }));
            }
        }
        Ok(())
    }

    fn validate_intrinsic(&self) -> Result<(), GraphAuthorError> {
        if self.schema_version != TRACE_OVERLAY_SCHEMA_VERSION {
            return Err(GraphAuthorError::new(
                TRACE_OVERLAY_INVALID,
                "unsupported trace overlay schema",
            ));
        }
        let mut activations = BTreeSet::new();
        for trace in &self.activations {
            if !activations.insert(&trace.activation_id) {
                return Err(GraphAuthorError::new(
                    TRACE_OVERLAY_INVALID,
                    "trace overlay contains a duplicate activation ID",
                ));
            }
            if trace.attempt == Some(0) {
                return Err(GraphAuthorError::new(
                    TRACE_OVERLAY_INVALID,
                    "trace attempt numbers are one-based when present",
                ));
            }
        }
        Ok(())
    }
}

impl insight_engine::internal::VerifiedAuthorPlanView for GraphAuthorDocument {
    fn encode_author_document(
        &self,
    ) -> Result<Vec<u8>, insight_engine::repository::RepositoryError> {
        self.encode_json()
            .map_err(|_| insight_engine::repository::adapter::invalid_data())
    }

    fn verified_plan(&self) -> &Plan {
        self.plan()
    }
}
