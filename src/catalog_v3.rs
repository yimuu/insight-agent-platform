//! Immutable catalog for published DSL v3 author documents.
//!
//! This boundary deliberately performs filesystem resolution, author parsing,
//! prompt pinning, and normalized-input construction before a Run can be
//! admitted. The durable scheduler only ever receives a verified Canonical
//! Plan and a complete normalized input value.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    dsl::{
        v3::{
            self,
            ast::{Metadata, PromptDeclaration},
            graph::GraphAuthorDocument,
            CompileOptions,
        },
        CompileError,
    },
    engine::{
        plan::{
            DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
            DescriptorFieldContract, DescriptorValue, DescriptorValueSchema, LeafTaskDescriptor,
            LeafTaskKind, LinkedPlan, NodeKind, PlanIndex, PlanInputErrorKind, PortName,
            SubflowContractRegistry, SubflowInterfaceContract, VersionTag, WorkerContract,
            WorkerInputPortContract,
        },
        repository::VersionedPlan,
        ContentHash, DefinitionRevisionId, DeploymentRevisionId, EffectIdempotency,
        ExecutionRevisionPin, LeafTaskExecutor, Plan, RuntimeValue, SchedulerTaskKind,
        WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy, WorkerExecutorRegistry,
    },
    resources::{
        actions::{ActionRegistry, CancellationClass, EffectClass, IdempotencyClass},
        models::{ModelCapability, ModelRegistry},
    },
};

const AGENT_FILE: &str = "agent.yaml";
const DEFINITION_REVISION_DOMAIN: &[u8] = b"insight-agent/definition-revision/v3";
const DEPLOYMENT_REVISION_DOMAIN: &str = "insight-agent/deployment-revision/v3";

/// Closed, machine-readable deployment risk taxonomy. Diagnostics are review
/// evidence only and never mutate the frozen worker policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeploymentRiskCode {
    NonIdempotentEffect,
}

impl DeploymentRiskCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NonIdempotentEffect => "NON_IDEMPOTENT_EFFECT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentRiskSeverity {
    Warning,
}

impl DeploymentRiskSeverity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warning => "warning",
        }
    }
}

/// One closed diagnostic attached to an immutable Deployment Revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentRiskDiagnostic {
    code: DeploymentRiskCode,
    severity: DeploymentRiskSeverity,
    node: crate::engine::NodeId,
    implementation: String,
}

impl DeploymentRiskDiagnostic {
    fn non_idempotent(node: crate::engine::NodeId, implementation: String) -> Self {
        Self {
            code: DeploymentRiskCode::NonIdempotentEffect,
            severity: DeploymentRiskSeverity::Warning,
            node,
            implementation,
        }
    }

    pub fn code(&self) -> DeploymentRiskCode {
        self.code
    }

    pub fn severity(&self) -> DeploymentRiskSeverity {
        self.severity
    }

    pub fn node(&self) -> &crate::engine::NodeId {
        &self.node
    }

    pub fn implementation(&self) -> &str {
        &self.implementation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentInputError {
    code: &'static str,
    message: &'static str,
}

impl AgentInputError {
    fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl std::fmt::Display for AgentInputError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for AgentInputError {}

/// One immutable, verified author revision ready for deployment linking.
#[derive(Debug, Clone)]
pub struct PublishedV3Agent {
    metadata: Metadata,
    definition_revision_id: DefinitionRevisionId,
    author_source: String,
    prompt_files: BTreeMap<String, String>,
    plan: Plan,
    graph_author_document: Option<GraphAuthorDocument>,
}

impl PublishedV3Agent {
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn definition_revision_id(&self) -> &DefinitionRevisionId {
        &self.definition_revision_id
    }

    pub fn author_source(&self) -> &str {
        &self.author_source
    }

    pub fn prompt_files(&self) -> &BTreeMap<String, String> {
        &self.prompt_files
    }

    pub fn plan(&self) -> &Plan {
        &self.plan
    }

    /// Publishes the accepted input shape rather than the stricter normalized
    /// Run shape. Frozen defaults remain annotations on their optional public
    /// properties and are applied authoritatively by [`Self::normalize_input`].
    pub fn public_input_schema(&self) -> Value {
        let contract = self.plan.metadata().input_contract();
        let schema = contract
            .accepted_type()
            .json_schema_document()
            .expect("a verified Plan input contract always has a JSON Schema projection");
        public_input_schema(schema, contract.defaults())
    }

    pub fn graph_author_document(&self) -> Option<&GraphAuthorDocument> {
        self.graph_author_document.as_ref()
    }

    /// Builds a publication candidate from the explicit graph wire. The graph
    /// is reverified here even if it already crossed the JSON decode boundary;
    /// no structured reduction certificate participates in execution truth.
    pub fn from_verified_graph(
        agent_id: impl Into<String>,
        name: impl Into<String>,
        description: impl Into<String>,
        graph: GraphAuthorDocument,
    ) -> Result<Self, CompileError> {
        graph
            .validate()
            .map_err(|error| CompileError::new("GRAPH_DOCUMENT_INVALID", error.to_string()))?;
        let agent_id = agent_id.into();
        let definition_revision_id = graph.metadata().definition_revision_id().clone();
        let plan = graph.plan().clone();
        Ok(Self {
            metadata: Metadata {
                id: agent_id,
                name: name.into(),
                description: description.into(),
            },
            definition_revision_id,
            author_source: String::new(),
            prompt_files: BTreeMap::new(),
            plan,
            graph_author_document: Some(graph),
        })
    }

    /// Rebuilds the publication-side graph object from a fully verified
    /// durable VersionedPlan. This path never recompiles structured source and
    /// therefore cannot silently substitute a current alias binding.
    pub(crate) fn from_stored_graph(plan: &VersionedPlan) -> Result<Self, CompileError> {
        if !plan.is_graph_authored() {
            return Err(CompileError::new(
                "STORED_GRAPH_REQUIRED",
                "only graph-authored deployments can be reconstructed from durable author JSON",
            ));
        }
        let author_bytes = serde_json::to_vec(plan.author_document()).map_err(|_| {
            CompileError::new(
                "STORED_GRAPH_INVALID",
                "stored graph author document cannot be decoded",
            )
        })?;
        let graph = GraphAuthorDocument::decode_json(&author_bytes).map_err(|_| {
            CompileError::new(
                "STORED_GRAPH_INVALID",
                "stored graph author document failed authoritative verification",
            )
        })?;
        let canonical_bytes = serde_json::to_vec(plan.canonical_plan()).map_err(|_| {
            CompileError::new(
                "STORED_GRAPH_INVALID",
                "stored Canonical Plan cannot be decoded",
            )
        })?;
        let canonical = Plan::decode_json(&canonical_bytes).map_err(|_| {
            CompileError::new(
                "STORED_GRAPH_INVALID",
                "stored Canonical Plan failed authoritative verification",
            )
        })?;
        if graph.plan() != &canonical
            || graph.metadata().definition_revision_id() != plan.definition_revision_id()
            || graph.semantic_hash().as_str() != plan.plan_hash().as_str()
        {
            return Err(CompileError::new(
                "STORED_GRAPH_INVALID",
                "stored graph, Plan and immutable revision identity disagree",
            ));
        }
        Ok(Self {
            metadata: Metadata {
                id: plan.agent_id().to_owned(),
                name: plan.display_name().to_owned(),
                description: plan.public_description().to_owned(),
            },
            definition_revision_id: plan.definition_revision_id().clone(),
            author_source: String::new(),
            prompt_files: BTreeMap::new(),
            plan: canonical,
            graph_author_document: Some(graph),
        })
    }

    pub(crate) fn from_stored_versioned_plan(plan: &VersionedPlan) -> Result<Self, CompileError> {
        if plan.is_graph_authored() {
            return Self::from_stored_graph(plan);
        }
        if plan
            .author_document()
            .get("authoring_mode")
            .and_then(Value::as_str)
            != Some("structured")
        {
            return Err(stored_deployment_invalid(
                "stored deployment has an unknown authoring mode",
            ));
        }
        let canonical_bytes = serde_json::to_vec(plan.canonical_plan()).map_err(|_| {
            stored_deployment_invalid("stored structured Canonical Plan cannot be decoded")
        })?;
        let canonical = Plan::decode_json(&canonical_bytes).map_err(|_| {
            stored_deployment_invalid(
                "stored structured Canonical Plan failed authoritative verification",
            )
        })?;
        if canonical.metadata().definition_revision_id() != plan.definition_revision_id()
            || canonical.semantic_hash().as_str() != plan.plan_hash().as_str()
        {
            return Err(stored_deployment_invalid(
                "stored structured Plan and immutable revision identity disagree",
            ));
        }
        let author_source = plan
            .author_document()
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| stored_deployment_invalid("stored structured source is missing"))?
            .to_owned();
        Ok(Self {
            metadata: Metadata {
                id: plan.agent_id().to_owned(),
                name: plan.display_name().to_owned(),
                description: plan.public_description().to_owned(),
            },
            definition_revision_id: plan.definition_revision_id().clone(),
            author_source,
            prompt_files: BTreeMap::new(),
            plan: canonical,
            graph_author_document: None,
        })
    }

    /// Converts accepted public input into the frozen Run input. Optional
    /// fields remain absent, defaulted fields receive their frozen Plan value,
    /// and JSON null remains an ordinary present value subject to its type.
    pub fn normalize_input(&self, input: Value) -> Result<RuntimeValue, AgentInputError> {
        let normalized = self
            .plan
            .metadata()
            .input_contract()
            .normalize(input)
            .map_err(|error| match error.kind() {
                PlanInputErrorKind::InvalidShape => {
                    AgentInputError::new("AGENT_INPUT_INVALID", "agent input must be an object")
                }
                PlanInputErrorKind::UnknownField => AgentInputError::new(
                    "AGENT_INPUT_UNKNOWN_FIELD",
                    "agent input contains an unknown field",
                ),
                PlanInputErrorKind::MissingRequired => AgentInputError::new(
                    "AGENT_INPUT_REQUIRED",
                    "agent input is missing a required field",
                ),
                PlanInputErrorKind::TypeMismatch => AgentInputError::new(
                    "AGENT_INPUT_CONTRACT_INVALID",
                    "agent input does not satisfy the published input contract",
                ),
            })?;
        RuntimeValue::new(normalized).map_err(|_| {
            AgentInputError::new(
                "AGENT_INPUT_INVALID",
                "agent input is outside the canonical value domain",
            )
        })
    }
}

fn public_input_schema(mut schema: Value, defaults: &BTreeMap<String, Value>) -> Value {
    if let Value::Object(object) = &mut schema {
        object.remove("$schema");
        if let Some(Value::Object(properties)) = object.get_mut("properties") {
            for (name, default) in defaults {
                if let Some(property_schema) = properties.get_mut(name) {
                    let schema = std::mem::take(property_schema);
                    *property_schema = match schema {
                        Value::Object(mut schema) => {
                            schema.insert("default".to_owned(), default.clone());
                            Value::Object(schema)
                        }
                        schema => serde_json::json!({"allOf": [schema], "default": default}),
                    };
                }
            }
        }
        object
            .entry("$defs".to_owned())
            .or_insert_with(|| serde_json::json!({}));
    }
    schema
}

/// Immutable lookup table. Duplicate IDs are rejected rather than resolved by
/// directory or iteration order.
#[derive(Debug, Clone, Default)]
pub struct V3AgentCatalog {
    agents: BTreeMap<String, Arc<PublishedV3Agent>>,
}

/// Deployment-specific result of resolving one immutable leaf descriptor.
/// `binding_evidence` must be non-secret and identify the exact provider,
/// plugin, adapter, and worker implementation selected for this deployment.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedLeafDeployment {
    worker_version: VersionTag,
    effect_policy: WorkerEffectPolicy,
    binding_evidence: Value,
}

impl ResolvedLeafDeployment {
    pub fn new(worker_version: VersionTag, binding_evidence: Value) -> Result<Self, CompileError> {
        if !binding_evidence.is_object() {
            return Err(CompileError::new(
                "DEPLOYMENT_BINDING_INVALID",
                "leaf deployment evidence must be a non-secret object",
            ));
        }
        let effect_policy = WorkerEffectPolicy::frozen(
            WorkerEffectClass::Mutating,
            EffectIdempotency::NonIdempotent,
            1,
            0,
            0,
            60_000,
            WorkerCancellation::LeaseOnly,
        )
        .map_err(plan_compile_error)?;
        Ok(Self {
            worker_version,
            effect_policy,
            binding_evidence,
        })
    }

    pub fn with_effect_policy(mut self, effect_policy: WorkerEffectPolicy) -> Self {
        self.effect_policy = effect_policy;
        self
    }

    pub fn worker_version(&self) -> &VersionTag {
        &self.worker_version
    }

    pub fn binding_evidence(&self) -> &Value {
        &self.binding_evidence
    }

    pub fn effect_policy(&self) -> &WorkerEffectPolicy {
        &self.effect_policy
    }
}

/// Trust boundary used at publication time. Runtime model aliases and plugin
/// lookups are resolved here, never lazily by the scheduler.
pub trait LeafDeploymentResolver: Send + Sync {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError>;
}

/// Publication-time validation owned by one exact external leaf adapter.
///
/// The validator receives the compiler-produced immutable descriptor before a
/// Deployment Revision is minted.  It must reject unsupported methods, tool
/// names, configuration shapes, and secret slots here rather than deferring
/// those checks until an Attempt is already leased.
pub trait LeafDeploymentDescriptorValidator: Send + Sync {
    fn validate_descriptor(&self, descriptor: &LeafTaskDescriptor) -> Result<(), CompileError>;
}

impl<F> LeafDeploymentDescriptorValidator for F
where
    F: Fn(&LeafTaskDescriptor) -> Result<(), CompileError> + Send + Sync,
{
    fn validate_descriptor(&self, descriptor: &LeafTaskDescriptor) -> Result<(), CompileError> {
        self(descriptor)
    }
}

/// Complete immutable identity of a production HTTP or Tool adapter.
///
/// Descriptor and worker versions are intentionally independent: the former
/// identifies the request contract while the latter identifies executable
/// code. `adapter_version` identifies the glue between those two contracts and
/// therefore also participates in the Deployment Revision binding hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedLeafAdapterIdentity {
    kind: LeafTaskKind,
    implementation: String,
    descriptor_version: VersionTag,
    adapter_version: VersionTag,
    worker_version: VersionTag,
}

impl VersionedLeafAdapterIdentity {
    pub fn new(
        kind: LeafTaskKind,
        implementation: impl Into<String>,
        descriptor_version: VersionTag,
        adapter_version: VersionTag,
        worker_version: VersionTag,
    ) -> Result<Self, CompileError> {
        let implementation = implementation.into();
        let expected = match kind {
            LeafTaskKind::Http => "core.http",
            LeafTaskKind::Tool => "core.tool",
            LeafTaskKind::Llm | LeafTaskKind::Action => {
                return Err(CompileError::new(
                    "LEAF_ADAPTER_IDENTITY_INVALID",
                    "external leaf registrations are restricted to HTTP and Tool nodes",
                ));
            }
        };
        if implementation != expected {
            return Err(CompileError::new(
                "LEAF_ADAPTER_IDENTITY_INVALID",
                format!(
                    "{} leaf adapter must implement the frozen '{expected}' author adapter",
                    kind.name()
                ),
            ));
        }
        Ok(Self {
            kind,
            implementation,
            descriptor_version,
            adapter_version,
            worker_version,
        })
    }

    pub fn kind(&self) -> LeafTaskKind {
        self.kind
    }

    pub fn implementation(&self) -> &str {
        &self.implementation
    }

    pub fn descriptor_version(&self) -> &VersionTag {
        &self.descriptor_version
    }

    pub fn adapter_version(&self) -> &VersionTag {
        &self.adapter_version
    }

    pub fn worker_version(&self) -> &VersionTag {
        &self.worker_version
    }
}

/// One publication validator and one executable worker bound to an exact
/// version tuple.  Registration evidence is non-secret and becomes part of the
/// immutable deployment binding.
pub struct VersionedLeafAdapterRegistration {
    identity: VersionedLeafAdapterIdentity,
    effect_policy: WorkerEffectPolicy,
    binding_evidence: Value,
    validator: Arc<dyn LeafDeploymentDescriptorValidator>,
    executor: Arc<dyn LeafTaskExecutor>,
}

impl std::fmt::Debug for VersionedLeafAdapterRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VersionedLeafAdapterRegistration")
            .field("identity", &self.identity)
            .field("effect_policy", &self.effect_policy)
            .field("binding_evidence", &self.binding_evidence)
            .finish_non_exhaustive()
    }
}

impl VersionedLeafAdapterRegistration {
    pub fn new(
        identity: VersionedLeafAdapterIdentity,
        effect_policy: WorkerEffectPolicy,
        binding_evidence: Value,
        validator: Arc<dyn LeafDeploymentDescriptorValidator>,
        executor: Arc<dyn LeafTaskExecutor>,
    ) -> Result<Self, CompileError> {
        if !binding_evidence.is_object() || serde_jcs::to_vec(&binding_evidence).is_err() {
            return Err(CompileError::new(
                "LEAF_ADAPTER_BINDING_INVALID",
                "external leaf adapter evidence must be a canonicalizable non-secret object",
            ));
        }
        Ok(Self {
            identity,
            effect_policy,
            binding_evidence,
            validator,
            executor,
        })
    }

    pub fn identity(&self) -> &VersionedLeafAdapterIdentity {
        &self.identity
    }

    pub fn effect_policy(&self) -> &WorkerEffectPolicy {
        &self.effect_policy
    }

    pub fn binding_evidence(&self) -> &Value {
        &self.binding_evidence
    }
}

type LeafAdapterKey = (LeafTaskKind, String, VersionTag);
type LeafAdapterArchiveKey = (LeafTaskKind, String, VersionTag, VersionTag, VersionTag);

/// Closed production registry for independently versioned HTTP and Tool
/// descriptor adapters. Each descriptor has one current publication route,
/// while any number of distinct historical registration revisions can remain
/// executable for Runs pinned to them.
#[derive(Clone, Default)]
pub struct VersionedLeafAdapterRegistry {
    current_by_descriptor: BTreeMap<LeafAdapterKey, Arc<VersionedLeafAdapterRegistration>>,
    registration_archive: BTreeMap<LeafAdapterArchiveKey, Arc<VersionedLeafAdapterRegistration>>,
}

impl std::fmt::Debug for VersionedLeafAdapterRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VersionedLeafAdapterRegistry")
            .field(
                "current_by_descriptor",
                &self.current_by_descriptor.keys().collect::<Vec<_>>(),
            )
            .field(
                "registration_archive",
                &self.registration_archive.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl VersionedLeafAdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        registration: VersionedLeafAdapterRegistration,
    ) -> Result<(), CompileError> {
        self.register_current(registration)
    }

    /// Selects one current publication adapter for a descriptor and retains
    /// that exact registration in the worker archive.
    pub fn register_current(
        &mut self,
        registration: VersionedLeafAdapterRegistration,
    ) -> Result<(), CompileError> {
        let identity = registration.identity();
        let descriptor_key = (
            identity.kind(),
            identity.implementation().to_owned(),
            identity.descriptor_version().clone(),
        );
        if self.current_by_descriptor.contains_key(&descriptor_key) {
            return Err(CompileError::new(
                "DUPLICATE_LEAF_ADAPTER",
                "an external leaf descriptor already has a current production adapter",
            ));
        }
        let registration = self.insert_archived(registration)?;
        self.current_by_descriptor
            .insert(descriptor_key, registration);
        Ok(())
    }

    /// Retains an older executable registration without routing new
    /// Deployment Revisions to it.
    pub fn register_archived(
        &mut self,
        registration: VersionedLeafAdapterRegistration,
    ) -> Result<(), CompileError> {
        self.insert_archived(registration).map(|_| ())
    }

    fn insert_archived(
        &mut self,
        registration: VersionedLeafAdapterRegistration,
    ) -> Result<Arc<VersionedLeafAdapterRegistration>, CompileError> {
        let identity = registration.identity();
        let archive_key = (
            identity.kind(),
            identity.implementation().to_owned(),
            identity.descriptor_version().clone(),
            identity.adapter_version().clone(),
            identity.worker_version().clone(),
        );
        if self.registration_archive.contains_key(&archive_key) {
            return Err(CompileError::new(
                "DUPLICATE_LEAF_ADAPTER",
                "the exact external leaf adapter revision is already archived",
            ));
        }
        if self.registration_archive.values().any(|existing| {
            let existing = existing.identity();
            existing.kind() == identity.kind()
                && existing.implementation() == identity.implementation()
                && existing.descriptor_version() == identity.descriptor_version()
                && existing.worker_version() == identity.worker_version()
        }) {
            return Err(CompileError::new(
                "LEAF_ADAPTER_WORKER_IDENTITY_CONFLICT",
                "distinct adapter revisions cannot share one durable worker dispatch identity",
            ));
        }
        let registration = Arc::new(registration);
        self.registration_archive
            .insert(archive_key, Arc::clone(&registration));
        Ok(registration)
    }

    fn resolve(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<Arc<VersionedLeafAdapterRegistration>, CompileError> {
        let key = (
            kind,
            descriptor.implementation.clone(),
            descriptor.descriptor_version.clone(),
        );
        let registration = self
            .current_by_descriptor
            .get(&key)
            .cloned()
            .ok_or_else(|| {
                CompileError::new(
                    "LEAF_DEPLOYMENT_NOT_REGISTERED",
                    "http/tool Plan node has no exact versioned production adapter",
                )
            })?;
        registration.validator.validate_descriptor(descriptor)?;
        Ok(registration)
    }

    /// Installs every registered executable under its exact durable dispatch
    /// tuple. This is the only supported bridge from publication registrations
    /// to the production worker registry.
    pub fn install_workers(
        &self,
        workers: &mut WorkerExecutorRegistry,
    ) -> Result<(), CompileError> {
        for registration in self.registration_archive.values() {
            let identity = registration.identity();
            workers
                .register(
                    match identity.kind() {
                        LeafTaskKind::Http => SchedulerTaskKind::Http,
                        LeafTaskKind::Tool => SchedulerTaskKind::Tool,
                        LeafTaskKind::Llm | LeafTaskKind::Action => {
                            unreachable!("registration constructor rejects built-in leaf kinds")
                        }
                    },
                    identity.implementation().to_owned(),
                    identity.descriptor_version().clone(),
                    identity.worker_version().clone(),
                    Arc::clone(&registration.executor),
                )
                .map_err(|code| {
                    CompileError::new(
                        code,
                        "failed to install an exact versioned external leaf worker",
                    )
                })?;
        }
        Ok(())
    }
}

/// Production resolver for the reusable model/action registries. The author
/// alias is resolved once at publication and its non-secret adapter evidence
/// enters the immutable Deployment Revision.
pub struct ProductionLeafDeploymentResolver<'a> {
    models: &'a ModelRegistry,
    actions: &'a ActionRegistry,
    external_leaf_adapters: Option<&'a VersionedLeafAdapterRegistry>,
    operation_timeout_ms: u64,
}

impl<'a> ProductionLeafDeploymentResolver<'a> {
    pub fn new(models: &'a ModelRegistry, actions: &'a ActionRegistry) -> Self {
        Self {
            models,
            actions,
            external_leaf_adapters: None,
            operation_timeout_ms: 60_000,
        }
    }

    pub fn with_external_leaf_adapters(
        mut self,
        adapters: &'a VersionedLeafAdapterRegistry,
    ) -> Self {
        self.external_leaf_adapters = Some(adapters);
        self
    }

    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self, CompileError> {
        self.operation_timeout_ms = u64::try_from(timeout.as_millis())
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                CompileError::new(
                    "WORKER_EFFECT_POLICY_INVALID",
                    "runtime operation timeout is outside the durable millisecond domain",
                )
            })?;
        Ok(self)
    }
}

impl LeafDeploymentResolver for ProductionLeafDeploymentResolver<'_> {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        match kind {
            LeafTaskKind::Llm => {
                if descriptor.implementation != "core.llm" {
                    return Err(CompileError::new(
                        "LLM_DESCRIPTOR_INVALID",
                        "llm Plan node must use the frozen core.llm adapter",
                    ));
                }
                let alias = descriptor
                    .public_configuration
                    .get("model")
                    .and_then(descriptor_string)
                    .ok_or_else(|| {
                        CompileError::new(
                            "LLM_MODEL_BINDING_INVALID",
                            "llm descriptor must contain one model alias",
                        )
                    })?;
                let model = self.models.resolve(alias)?;
                let identity = self.models.deployment_identity(alias)?;
                let parameters = descriptor
                    .public_configuration
                    .get("parameters")
                    .map(descriptor_json)
                    .transpose()?
                    .unwrap_or_else(|| serde_json::json!({}));
                model.validate_parameters(&parameters)?;
                if descriptor_contains_key(
                    &DescriptorValue::Object(descriptor.public_configuration.clone()),
                    "image_url",
                ) && !model.capabilities().contains(&ModelCapability::Vision)
                {
                    return Err(CompileError::new(
                        "LLM_VISION_BINDING_REQUIRED",
                        "llm descriptor contains image content but its frozen model is not vision-capable",
                    ));
                }
                let resolved = ResolvedLeafDeployment::new(
                    VersionTag::new(identity.worker_version()).map_err(plan_compile_error)?,
                    serde_json::json!({
                        "adapter": "core.llm",
                        "model_alias": alias,
                        "model_binding_hash": identity.binding_hash(),
                        "model_binding": identity.evidence(),
                    }),
                )?;
                Ok(resolved.with_effect_policy(
                    WorkerEffectPolicy::frozen(
                        WorkerEffectClass::Pure,
                        EffectIdempotency::Idempotent,
                        1,
                        0,
                        0,
                        self.operation_timeout_ms,
                        WorkerCancellation::Cooperative,
                    )
                    .map_err(plan_compile_error)?,
                ))
            }
            LeafTaskKind::Action => {
                let action = self.actions.resolve(&descriptor.implementation)?;
                let identity = action.identity();
                let action_descriptor = action.descriptor();
                let resolved = ResolvedLeafDeployment::new(
                    VersionTag::new(identity.version.to_string()).map_err(plan_compile_error)?,
                    serde_json::json!({
                        "adapter": "native_action",
                        "action_id": identity.id,
                        "action_version": identity.version.to_string(),
                        "descriptor_hash": identity.descriptor_hash,
                        "effect": action_descriptor.effect,
                        "idempotency": action_descriptor.idempotency,
                        "cancellation": action_descriptor.cancellation,
                        "required_capabilities": action_descriptor.required_capabilities.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
                    }),
                )?;
                Ok(resolved.with_effect_policy(
                    WorkerEffectPolicy::frozen(
                        match action_descriptor.effect {
                            EffectClass::Pure => WorkerEffectClass::Pure,
                            EffectClass::ReadOnly => WorkerEffectClass::ReadOnly,
                            EffectClass::Mutating => WorkerEffectClass::Mutating,
                        },
                        match action_descriptor.idempotency {
                            IdempotencyClass::Idempotent => EffectIdempotency::Idempotent,
                            IdempotencyClass::NonIdempotent => EffectIdempotency::NonIdempotent,
                        },
                        1,
                        0,
                        0,
                        self.operation_timeout_ms,
                        match action_descriptor.cancellation {
                            CancellationClass::Cooperative => WorkerCancellation::Cooperative,
                            CancellationClass::NotSupported => WorkerCancellation::LeaseOnly,
                        },
                    )
                    .map_err(plan_compile_error)?,
                ))
            }
            LeafTaskKind::Http | LeafTaskKind::Tool => {
                let registration = self
                    .external_leaf_adapters
                    .ok_or_else(|| {
                        CompileError::new(
                            "LEAF_DEPLOYMENT_NOT_REGISTERED",
                            "http/tool Plan nodes require an explicitly versioned production adapter",
                        )
                    })?
                    .resolve(kind, descriptor)?;
                let identity = registration.identity();
                let resolved = ResolvedLeafDeployment::new(
                    identity.worker_version().clone(),
                    serde_json::json!({
                        "adapter": "versioned_external_leaf",
                        "task_kind": identity.kind().name(),
                        "implementation": identity.implementation(),
                        "descriptor_version": identity.descriptor_version(),
                        "adapter_version": identity.adapter_version(),
                        "worker_version": identity.worker_version(),
                        "registration": registration.binding_evidence(),
                    }),
                )?;
                Ok(resolved.with_effect_policy(registration.effect_policy().clone()))
            }
        }
    }
}

/// Owned publication resolver used by the long-lived Graph Author API. It
/// freezes the same model/action/adapter registries used during process
/// startup, but does not borrow stack-local bootstrap state.
#[derive(Clone)]
pub struct OwnedProductionLeafDeploymentResolver {
    models: ModelRegistry,
    actions: ActionRegistry,
    external_leaf_adapters: Option<VersionedLeafAdapterRegistry>,
    operation_timeout_ms: u64,
}

impl OwnedProductionLeafDeploymentResolver {
    pub fn new(models: &ModelRegistry, actions: &ActionRegistry) -> Self {
        Self {
            models: models.clone(),
            actions: actions.clone(),
            external_leaf_adapters: None,
            operation_timeout_ms: 60_000,
        }
    }

    pub fn with_external_leaf_adapters(mut self, adapters: &VersionedLeafAdapterRegistry) -> Self {
        self.external_leaf_adapters = Some(adapters.clone());
        self
    }

    pub fn with_operation_timeout(mut self, timeout: Duration) -> Result<Self, CompileError> {
        self.operation_timeout_ms = u64::try_from(timeout.as_millis())
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                CompileError::new(
                    "WORKER_EFFECT_POLICY_INVALID",
                    "runtime operation timeout is outside the durable millisecond domain",
                )
            })?;
        Ok(self)
    }
}

impl LeafDeploymentResolver for OwnedProductionLeafDeploymentResolver {
    fn resolve_leaf(
        &self,
        kind: LeafTaskKind,
        descriptor: &LeafTaskDescriptor,
    ) -> Result<ResolvedLeafDeployment, CompileError> {
        let mut resolver = ProductionLeafDeploymentResolver::new(&self.models, &self.actions);
        if let Some(adapters) = &self.external_leaf_adapters {
            resolver = resolver.with_external_leaf_adapters(adapters);
        }
        resolver.operation_timeout_ms = self.operation_timeout_ms;
        resolver.resolve_leaf(kind, descriptor)
    }
}

/// A published Plan paired with one exact, immutable deployment binding.
#[derive(Debug)]
pub struct DeployedV3Agent {
    published: Arc<PublishedV3Agent>,
    deployment_revision_id: DeploymentRevisionId,
    descriptors: DescriptorContractRegistry,
    subflows: SubflowContractRegistry,
    versioned_plan: VersionedPlan,
    risk_diagnostics: Vec<DeploymentRiskDiagnostic>,
}

impl DeployedV3Agent {
    pub fn publish(
        published: Arc<PublishedV3Agent>,
        resolver: &dyn LeafDeploymentResolver,
        subflows: SubflowContractRegistry,
    ) -> Result<Self, CompileError> {
        let plan = published.plan();
        let index = PlanIndex::new(plan).map_err(plan_compile_error)?;
        let mut descriptors = DescriptorContractRegistry::new();
        let mut descriptor_documents = Vec::new();
        let mut resolved_bindings = Vec::new();
        let mut worker_documents = Vec::new();
        let mut risk_diagnostics = Vec::new();

        for node in plan.nodes() {
            let Some(leaf) = index.leaf_descriptor(node.id()) else {
                continue;
            };
            let descriptor = leaf.descriptor();
            let resolved = resolver.resolve_leaf(leaf.kind(), descriptor)?;
            if resolved.effect_policy.effect_idempotency() == EffectIdempotency::NonIdempotent {
                risk_diagnostics.push(DeploymentRiskDiagnostic::non_idempotent(
                    node.id().clone(),
                    descriptor.implementation.clone(),
                ));
            }
            let configuration = exact_configuration_contract(descriptor);
            let inputs = index
                .data_inputs(node.id())
                .iter()
                .map(|port_id| {
                    let port = index.data_port(port_id).ok_or_else(|| {
                        CompileError::new(
                            "DEPLOYMENT_LINK_INVALID",
                            "verified leaf input disappeared from PlanIndex",
                        )
                    })?;
                    Ok((
                        port.name().clone(),
                        WorkerInputPortContract::new(port.value_type().clone(), port.required()),
                    ))
                })
                .collect::<Result<BTreeMap<PortName, WorkerInputPortContract>, CompileError>>()?;
            let outputs = index
                .data_outputs(node.id())
                .iter()
                .map(|port_id| {
                    let port = index.data_port(port_id).ok_or_else(|| {
                        CompileError::new(
                            "DEPLOYMENT_LINK_INVALID",
                            "verified leaf output disappeared from PlanIndex",
                        )
                    })?;
                    Ok((port.name().clone(), port.value_type().clone()))
                })
                .collect::<Result<BTreeMap<PortName, _>, CompileError>>()?;
            let worker = WorkerContract::new(
                leaf.kind(),
                resolved.worker_version.clone(),
                inputs,
                outputs,
            )
            .with_effect_policy(resolved.effect_policy.clone());
            descriptors
                .register_for_node(
                    node.id().clone(),
                    DescriptorContract::new(
                        descriptor.implementation.clone(),
                        descriptor.descriptor_version.clone(),
                        configuration,
                        worker,
                    ),
                )
                .map_err(plan_compile_error)?;

            descriptor_documents.push(serde_json::json!({
                "node_id": node.id(),
                "task_kind": leaf.kind().name(),
                "implementation": descriptor.implementation,
                "descriptor_version": descriptor.descriptor_version,
                "public_configuration": descriptor.public_configuration,
                "secret_slots": descriptor.secret_configuration.keys().collect::<Vec<_>>(),
            }));
            resolved_bindings.push(serde_json::json!({
                "node_id": node.id(),
                "binding": resolved.binding_evidence,
            }));
            worker_documents.push(serde_json::json!({
                "node_id": node.id(),
                "worker_version": resolved.worker_version,
                "effect_policy": resolved.effect_policy,
            }));
        }

        // This is both a pre-publication proof and the object the scheduler
        // will reconstruct for every planning pass.
        let linked = LinkedPlan::link(plan, &descriptors, &subflows).map_err(plan_compile_error)?;

        // A parent deployment is not immutable unless every child execution
        // revision is part of its binding identity.  The author document only
        // names a definition/interface; publication freezes the exact child
        // deployment, Plan and binding hashes here.
        for node in plan.nodes() {
            let Some(contract) = linked.subflow(node.id()) else {
                continue;
            };
            descriptor_documents.push(serde_json::json!({
                "node_id": node.id(),
                "task_kind": "subflow",
                "definition_revision_id": contract.definition_revision_id(),
                "interface_version": contract.interface_version(),
                "input_contract": contract.input_contract(),
                "outputs": contract.outputs(),
                "error_type": contract.error_type(),
            }));
            resolved_bindings.push(serde_json::json!({
                "node_id": node.id(),
                "binding": {
                    "adapter": "durable_subflow",
                    "definition_revision_id": contract.execution_revision().definition_revision_id(),
                    "deployment_revision_id": contract.execution_revision().deployment_revision_id(),
                    "plan_hash": contract.execution_revision().plan_hash(),
                    "binding_hash": contract.execution_revision().binding_hash(),
                    "interface_version": contract.interface_version(),
                },
            }));
        }

        let descriptor_contracts = Value::Array(descriptor_documents);
        let resolved_bindings = Value::Array(resolved_bindings);
        let worker_contracts = Value::Array(worker_documents);
        let binding_projection = serde_json::json!({
            "schema_version": 1,
            "resolved_bindings": &resolved_bindings,
            "worker_contracts": &worker_contracts,
        });
        let binding_bytes = serde_jcs::to_vec(&binding_projection).map_err(|_| {
            CompileError::new(
                "DEPLOYMENT_BINDING_INVALID",
                "deployment binding cannot be canonicalized",
            )
        })?;
        let binding_hash = ContentHash::from_bytes(&binding_bytes);
        // Binding equality is not deployment equality: two unrelated Plans
        // with no external dependencies both have the same empty binding
        // projection. A deployment revision freezes the complete executable
        // identity tuple, while `binding_hash` remains independently useful
        // for compatibility/reuse checks.
        let deployment_identity = serde_json::json!({
            "domain": DEPLOYMENT_REVISION_DOMAIN,
            "definition_revision_id": published.definition_revision_id(),
            "plan_hash": plan.semantic_hash(),
            "binding_hash": &binding_hash,
        });
        let deployment_bytes = serde_jcs::to_vec(&deployment_identity).map_err(|_| {
            CompileError::new(
                "DEPLOYMENT_REVISION_INVALID",
                "deployment identity cannot be canonicalized",
            )
        })?;
        let deployment_hash = ContentHash::from_bytes(&deployment_bytes);
        let deployment_revision_id = DeploymentRevisionId::new(format!(
            "deployrev_{}",
            deployment_hash.as_str().trim_start_matches("sha256:")
        ))
        .map_err(|error| CompileError::new("DEPLOYMENT_REVISION_INVALID", error.to_string()))?;
        let versioned_plan = match published.graph_author_document() {
            Some(graph) => VersionedPlan::from_verified_graph(
                published.metadata.id.clone(),
                published.metadata.id.clone(),
                published.metadata.name.clone(),
                deployment_revision_id.clone(),
                "cel-0.14+match-jcs-v1+value-jcs-v1",
                graph,
                descriptor_contracts,
                resolved_bindings,
                worker_contracts,
            ),
            None => {
                let author_document = serde_json::json!({
                    "authoring_mode": "structured",
                    "source": published.author_source(),
                    "prompt_hashes": published.prompt_files().iter().map(|(path, content)| {
                        (path.clone(), ContentHash::from_bytes(content.as_bytes()).to_string())
                    }).collect::<BTreeMap<_, _>>(),
                });
                VersionedPlan::from_verified_plan(
                    published.metadata.id.clone(),
                    published.metadata.id.clone(),
                    published.metadata.name.clone(),
                    deployment_revision_id.clone(),
                    "cel-0.14+match-jcs-v1+value-jcs-v1",
                    author_document,
                    plan,
                    descriptor_contracts,
                    resolved_bindings,
                    worker_contracts,
                )
            }
        }
        .and_then(|plan| plan.with_public_description(published.metadata.description.clone()))
        .map_err(|error| CompileError::new("DEPLOYMENT_PUBLICATION_FAILED", error.to_string()))?;

        Ok(Self {
            published,
            deployment_revision_id,
            descriptors,
            subflows,
            versioned_plan,
            risk_diagnostics,
        })
    }

    /// Rehydrates an exact graph deployment from frozen durable contracts.
    /// No current alias/model/action resolver participates in this path.
    pub(crate) fn restore_frozen(
        versioned_plan: VersionedPlan,
        subflows: SubflowContractRegistry,
    ) -> Result<Self, CompileError> {
        let deployment_identity = serde_json::json!({
            "domain": DEPLOYMENT_REVISION_DOMAIN,
            "definition_revision_id": versioned_plan.definition_revision_id(),
            "plan_hash": versioned_plan.plan_hash(),
            "binding_hash": versioned_plan.binding_hash(),
        });
        let deployment_bytes = serde_jcs::to_vec(&deployment_identity).map_err(|_| {
            stored_deployment_invalid("stored graph deployment identity is not canonical")
        })?;
        let expected_hash = ContentHash::from_bytes(&deployment_bytes);
        let expected_deployment = DeploymentRevisionId::new(format!(
            "deployrev_{}",
            expected_hash.as_str().trim_start_matches("sha256:")
        ))
        .map_err(|_| stored_deployment_invalid("stored graph deployment identity is invalid"))?;
        if versioned_plan.deployment_revision_id() != &expected_deployment {
            return Err(stored_deployment_invalid(
                "stored graph deployment revision is not content-addressed by its frozen binding",
            ));
        }
        let published = Arc::new(PublishedV3Agent::from_stored_versioned_plan(
            &versioned_plan,
        )?);
        let plan = published.plan();
        let index = PlanIndex::new(plan).map_err(plan_compile_error)?;
        let descriptor_documents = stored_document_array(
            versioned_plan.descriptor_contracts(),
            "stored descriptor contracts",
        )?;
        let resolved_bindings = stored_document_array(
            versioned_plan.resolved_bindings(),
            "stored resolved bindings",
        )?;
        let worker_documents =
            stored_document_array(versioned_plan.worker_contracts(), "stored worker contracts")?;
        let mut descriptors = DescriptorContractRegistry::new();
        let mut risk_diagnostics = Vec::new();
        let mut leaf_count = 0usize;
        let mut subflow_count = 0usize;

        for node in plan.nodes() {
            if let Some(leaf) = index.leaf_descriptor(node.id()) {
                leaf_count += 1;
                let descriptor = leaf.descriptor();
                let descriptor_document = unique_stored_node_document(
                    descriptor_documents,
                    node.id(),
                    "descriptor contract",
                )?;
                let expected_descriptor = serde_json::json!({
                    "node_id": node.id(),
                    "task_kind": leaf.kind().name(),
                    "implementation": descriptor.implementation,
                    "descriptor_version": descriptor.descriptor_version,
                    "public_configuration": descriptor.public_configuration,
                    "secret_slots": descriptor.secret_configuration.keys().collect::<Vec<_>>(),
                });
                if descriptor_document != &expected_descriptor {
                    return Err(stored_deployment_invalid(
                        "stored leaf descriptor contract disagrees with Canonical Plan",
                    ));
                }
                let worker_document =
                    unique_stored_node_document(worker_documents, node.id(), "worker contract")?;
                let worker_version = serde_json::from_value::<VersionTag>(
                    worker_document
                        .get("worker_version")
                        .cloned()
                        .ok_or_else(|| {
                            stored_deployment_invalid("stored worker version is missing")
                        })?,
                )
                .map_err(|_| stored_deployment_invalid("stored worker version is invalid"))?;
                let effect_policy = serde_json::from_value::<WorkerEffectPolicy>(
                    worker_document
                        .get("effect_policy")
                        .cloned()
                        .ok_or_else(|| {
                            stored_deployment_invalid("stored worker policy is missing")
                        })?,
                )
                .map_err(|_| stored_deployment_invalid("stored worker policy is invalid"))?;
                let expected_worker = serde_json::json!({
                    "node_id": node.id(),
                    "worker_version": worker_version,
                    "effect_policy": effect_policy,
                });
                if worker_document != &expected_worker {
                    return Err(stored_deployment_invalid(
                        "stored worker contract contains unsupported fields",
                    ));
                }
                let binding =
                    unique_stored_node_document(resolved_bindings, node.id(), "resolved binding")?;
                if !binding.get("binding").is_some_and(Value::is_object)
                    || binding.as_object().is_none_or(|object| object.len() != 2)
                {
                    return Err(stored_deployment_invalid(
                        "stored leaf binding must be one closed evidence object",
                    ));
                }

                let inputs = index
                    .data_inputs(node.id())
                    .iter()
                    .map(|port_id| {
                        let port = index.data_port(port_id).ok_or_else(|| {
                            stored_deployment_invalid("stored leaf input port disappeared")
                        })?;
                        Ok((
                            port.name().clone(),
                            WorkerInputPortContract::new(
                                port.value_type().clone(),
                                port.required(),
                            ),
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, CompileError>>()?;
                let outputs = index
                    .data_outputs(node.id())
                    .iter()
                    .map(|port_id| {
                        let port = index.data_port(port_id).ok_or_else(|| {
                            stored_deployment_invalid("stored leaf output port disappeared")
                        })?;
                        Ok((port.name().clone(), port.value_type().clone()))
                    })
                    .collect::<Result<BTreeMap<_, _>, CompileError>>()?;
                if effect_policy.effect_idempotency() == EffectIdempotency::NonIdempotent {
                    risk_diagnostics.push(DeploymentRiskDiagnostic::non_idempotent(
                        node.id().clone(),
                        descriptor.implementation.clone(),
                    ));
                }
                let worker = WorkerContract::new(leaf.kind(), worker_version, inputs, outputs)
                    .with_effect_policy(effect_policy);
                descriptors
                    .register_for_node(
                        node.id().clone(),
                        DescriptorContract::new(
                            descriptor.implementation.clone(),
                            descriptor.descriptor_version.clone(),
                            exact_configuration_contract(descriptor),
                            worker,
                        ),
                    )
                    .map_err(plan_compile_error)?;
            }
            if matches!(node.kind(), NodeKind::SubflowCall(_)) {
                subflow_count += 1;
            }
        }

        let linked = LinkedPlan::link(plan, &descriptors, &subflows).map_err(plan_compile_error)?;
        for node in plan.nodes() {
            let Some(contract) = linked.subflow(node.id()) else {
                continue;
            };
            let descriptor_document = unique_stored_node_document(
                descriptor_documents,
                node.id(),
                "subflow descriptor contract",
            )?;
            let expected_descriptor = serde_json::json!({
                "node_id": node.id(),
                "task_kind": "subflow",
                "definition_revision_id": contract.definition_revision_id(),
                "interface_version": contract.interface_version(),
                "input_contract": contract.input_contract(),
                "outputs": contract.outputs(),
                "error_type": contract.error_type(),
            });
            if descriptor_document != &expected_descriptor {
                return Err(stored_deployment_invalid(
                    "stored subflow descriptor contract disagrees with pinned child",
                ));
            }
            let binding_document =
                unique_stored_node_document(resolved_bindings, node.id(), "subflow binding")?;
            let expected_binding = serde_json::json!({
                "node_id": node.id(),
                "binding": {
                    "adapter": "durable_subflow",
                    "definition_revision_id": contract.execution_revision().definition_revision_id(),
                    "deployment_revision_id": contract.execution_revision().deployment_revision_id(),
                    "plan_hash": contract.execution_revision().plan_hash(),
                    "binding_hash": contract.execution_revision().binding_hash(),
                    "interface_version": contract.interface_version(),
                },
            });
            if binding_document != &expected_binding {
                return Err(stored_deployment_invalid(
                    "stored subflow binding disagrees with pinned child deployment",
                ));
            }
        }
        if descriptor_documents.len() != leaf_count + subflow_count
            || resolved_bindings.len() != leaf_count + subflow_count
            || worker_documents.len() != leaf_count
        {
            return Err(stored_deployment_invalid(
                "stored deployment contract arrays contain missing or extra node entries",
            ));
        }

        Ok(Self {
            deployment_revision_id: versioned_plan.deployment_revision_id().clone(),
            published,
            descriptors,
            subflows,
            versioned_plan,
            risk_diagnostics,
        })
    }

    pub fn published(&self) -> &PublishedV3Agent {
        &self.published
    }

    pub fn deployment_revision_id(&self) -> &DeploymentRevisionId {
        &self.deployment_revision_id
    }

    pub fn versioned_plan(&self) -> &VersionedPlan {
        &self.versioned_plan
    }

    /// Non-blocking risks discovered from exact frozen deployment contracts.
    pub fn risk_diagnostics(&self) -> &[DeploymentRiskDiagnostic] {
        &self.risk_diagnostics
    }

    pub fn linked_plan(&self) -> Result<LinkedPlan<'_>, CompileError> {
        LinkedPlan::link(self.published.plan(), &self.descriptors, &self.subflows)
            .map_err(plan_compile_error)
    }

    pub(crate) fn workers_available(&self, workers: &WorkerExecutorRegistry) -> bool {
        let Ok(linked) = self.linked_plan() else {
            return false;
        };
        self.published.plan().nodes().iter().all(|node| {
            let Some(contract) = linked.descriptor(node.id()) else {
                return true;
            };
            let kind = match contract.worker().task_kind() {
                LeafTaskKind::Llm => SchedulerTaskKind::Llm,
                LeafTaskKind::Action => SchedulerTaskKind::Action,
                LeafTaskKind::Http => SchedulerTaskKind::Http,
                LeafTaskKind::Tool => SchedulerTaskKind::Tool,
            };
            workers.contains(
                kind,
                contract.implementation(),
                contract.descriptor_version(),
                contract.worker().worker_version(),
            )
        })
    }
}

fn stored_deployment_invalid(message: impl Into<String>) -> CompileError {
    CompileError::new("STORED_DEPLOYMENT_INVALID", message)
}

fn stored_document_array<'a>(value: &'a Value, label: &str) -> Result<&'a [Value], CompileError> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| stored_deployment_invalid(format!("{label} must be an array")))
}

fn unique_stored_node_document<'a>(
    documents: &'a [Value],
    node_id: &crate::engine::NodeId,
    label: &str,
) -> Result<&'a Value, CompileError> {
    let mut matches = documents.iter().filter(|document| {
        document.get("node_id").and_then(Value::as_str) == Some(node_id.as_str())
    });
    let document = matches
        .next()
        .ok_or_else(|| stored_deployment_invalid(format!("{label} is missing")))?;
    if matches.next().is_some() {
        return Err(stored_deployment_invalid(format!("{label} is duplicated")));
    }
    Ok(document)
}

impl V3AgentCatalog {
    pub fn new(agents: Vec<Arc<PublishedV3Agent>>) -> Result<Self, CompileError> {
        let mut catalog = Self::default();
        for agent in agents {
            let id = agent.metadata.id.clone();
            if catalog.agents.insert(id.clone(), agent).is_some() {
                return Err(CompileError::new(
                    "DUPLICATE_AGENT",
                    format!("published v3 agent '{id}' is already registered"),
                ));
            }
        }
        Ok(catalog)
    }

    pub fn get(&self, agent_id: &str) -> Option<Arc<PublishedV3Agent>> {
        self.agents.get(agent_id).cloned()
    }

    pub fn list(&self) -> impl Iterator<Item = &Arc<PublishedV3Agent>> {
        self.agents.values()
    }

    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.agents.keys().map(String::as_str)
    }
}

/// Publishes a complete enabled catalog in dependency order and freezes every
/// subflow call to one already-published child execution revision.  Missing
/// children and recursive dependency cycles fail before any Run can start.
pub fn deploy_v3_agents(
    catalog: &V3AgentCatalog,
    resolver: &dyn LeafDeploymentResolver,
) -> Result<Vec<Arc<DeployedV3Agent>>, CompileError> {
    let mut revision_owner = BTreeMap::<DefinitionRevisionId, String>::new();
    for agent in catalog.list() {
        if revision_owner
            .insert(
                agent.definition_revision_id().clone(),
                agent.metadata().id.clone(),
            )
            .is_some()
        {
            return Err(CompileError::new(
                "DUPLICATE_DEFINITION_REVISION",
                "enabled agents must have distinct immutable definition revisions",
            ));
        }
    }

    let mut dependencies = BTreeMap::<String, BTreeSet<String>>::new();
    for agent in catalog.list() {
        let mut children = BTreeSet::new();
        for node in agent.plan().nodes() {
            let NodeKind::SubflowCall(call) = node.kind() else {
                continue;
            };
            let child = revision_owner
                .get(&call.definition_revision_id)
                .ok_or_else(|| {
                    CompileError::new(
                        "SUBFLOW_DEFINITION_NOT_ENABLED",
                        format!(
                            "agent '{}' references unavailable subflow revision '{}'",
                            agent.metadata().id,
                            call.definition_revision_id
                        ),
                    )
                })?;
            children.insert(child.clone());
        }
        dependencies.insert(agent.metadata().id.clone(), children);
    }

    let mut deployed = BTreeMap::<String, Arc<DeployedV3Agent>>::new();
    while deployed.len() < dependencies.len() {
        let ready = dependencies
            .iter()
            .filter(|(id, children)| {
                !deployed.contains_key(*id)
                    && children.iter().all(|child| deployed.contains_key(child))
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if ready.is_empty() {
            let unresolved = dependencies
                .keys()
                .filter(|id| !deployed.contains_key(*id))
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            return Err(CompileError::new(
                "SUBFLOW_DEPENDENCY_CYCLE",
                format!("subflow dependency graph contains a cycle: {unresolved}"),
            ));
        }

        for id in ready {
            let published = catalog.get(&id).ok_or_else(|| {
                CompileError::new(
                    "DEPLOYMENT_LINK_INVALID",
                    "deployment dependency disappeared from the immutable catalog",
                )
            })?;
            let subflows = subflow_registry_for(&published, &revision_owner, &deployed)?;
            let deployment = Arc::new(DeployedV3Agent::publish(published, resolver, subflows)?);
            deployed.insert(id, deployment);
        }
    }

    Ok(deployed.into_values().collect())
}

fn subflow_registry_for(
    parent: &PublishedV3Agent,
    revision_owner: &BTreeMap<DefinitionRevisionId, String>,
    deployed: &BTreeMap<String, Arc<DeployedV3Agent>>,
) -> Result<SubflowContractRegistry, CompileError> {
    let mut registry = SubflowContractRegistry::new();
    let mut registered = BTreeSet::new();
    for node in parent.plan().nodes() {
        let NodeKind::SubflowCall(call) = node.kind() else {
            continue;
        };
        let key = (
            call.definition_revision_id.clone(),
            call.interface_version.clone(),
        );
        if !registered.insert(key) {
            continue;
        }
        let child_id = revision_owner
            .get(&call.definition_revision_id)
            .ok_or_else(|| {
                CompileError::new(
                    "SUBFLOW_DEFINITION_NOT_ENABLED",
                    "subflow definition is not present in the enabled catalog",
                )
            })?;
        let child = deployed.get(child_id).ok_or_else(|| {
            CompileError::new(
                "SUBFLOW_DEPENDENCY_NOT_PUBLISHED",
                "subflow dependency was not published before its parent",
            )
        })?;
        let outputs = BTreeMap::from([(
            PortName::new("result").map_err(plan_compile_error)?,
            child.published().plan().metadata().output_type().clone(),
        )]);
        let pin = ExecutionRevisionPin::new(
            child.versioned_plan().definition_revision_id().clone(),
            child.versioned_plan().deployment_revision_id().clone(),
            child.versioned_plan().plan_hash().clone(),
            child.versioned_plan().binding_hash().clone(),
        );
        registry
            .register(SubflowInterfaceContract::new(
                pin,
                call.interface_version.clone(),
                child.published().plan().metadata().input_contract().clone(),
                outputs,
                child.published().plan().metadata().error_type().clone(),
            ))
            .map_err(plan_compile_error)?;
    }
    Ok(registry)
}

/// Links a newly submitted Graph Author revision against the currently routed
/// child deployments. A graph names an immutable child Definition Revision;
/// this boundary freezes the corresponding current Deployment Revision into
/// the parent's own deployment binding.
pub fn subflow_registry_for_available(
    parent: &PublishedV3Agent,
    available: impl IntoIterator<Item = Arc<DeployedV3Agent>>,
) -> Result<SubflowContractRegistry, CompileError> {
    let mut by_revision = BTreeMap::new();
    for child in available {
        let revision = child.published().definition_revision_id().clone();
        if by_revision.insert(revision, child).is_some() {
            return Err(CompileError::new(
                "SUBFLOW_DEFINITION_AMBIGUOUS",
                "more than one current deployment owns a subflow Definition Revision",
            ));
        }
    }

    let mut registry = SubflowContractRegistry::new();
    let mut registered = BTreeSet::new();
    for node in parent.plan().nodes() {
        let NodeKind::SubflowCall(call) = node.kind() else {
            continue;
        };
        let key = (
            call.definition_revision_id.clone(),
            call.interface_version.clone(),
        );
        if !registered.insert(key) {
            continue;
        }
        let child = by_revision
            .get(&call.definition_revision_id)
            .ok_or_else(|| {
                CompileError::new(
                    "SUBFLOW_DEFINITION_NOT_ENABLED",
                    "graph subflow definition is not present in the current deployment catalog",
                )
            })?;
        let outputs = BTreeMap::from([(
            PortName::new("result").map_err(plan_compile_error)?,
            child.published().plan().metadata().output_type().clone(),
        )]);
        let pin = ExecutionRevisionPin::new(
            child.versioned_plan().definition_revision_id().clone(),
            child.versioned_plan().deployment_revision_id().clone(),
            child.versioned_plan().plan_hash().clone(),
            child.versioned_plan().binding_hash().clone(),
        );
        registry
            .register(SubflowInterfaceContract::new(
                pin,
                call.interface_version.clone(),
                child.published().plan().metadata().input_contract().clone(),
                outputs,
                child.published().plan().metadata().error_type().clone(),
            ))
            .map_err(plan_compile_error)?;
    }
    Ok(registry)
}

/// Rebuilds graph subflow contracts from the exact child deployment pins
/// frozen in the parent's durable binding. Current child routes are never
/// consulted, so publishing a newer child cannot retarget an old parent Run.
pub(crate) fn subflow_registry_for_stored(
    parent: &PublishedV3Agent,
    stored: &VersionedPlan,
    available: impl IntoIterator<Item = Arc<DeployedV3Agent>>,
) -> Result<SubflowContractRegistry, CompileError> {
    let mut by_deployment = BTreeMap::new();
    for child in available {
        let deployment = child.deployment_revision_id().as_str().to_owned();
        if by_deployment.insert(deployment, child).is_some() {
            return Err(stored_deployment_invalid(
                "stored deployment archive contains a duplicate deployment revision",
            ));
        }
    }
    let bindings = stored_document_array(stored.resolved_bindings(), "stored resolved bindings")?;
    let mut registry = SubflowContractRegistry::new();
    let mut registered = BTreeSet::new();
    for node in parent.plan().nodes() {
        let NodeKind::SubflowCall(call) = node.kind() else {
            continue;
        };
        let key = (
            call.definition_revision_id.clone(),
            call.interface_version.clone(),
        );
        if !registered.insert(key) {
            continue;
        }
        let document = unique_stored_node_document(bindings, node.id(), "stored subflow binding")?;
        let binding = document
            .get("binding")
            .and_then(Value::as_object)
            .ok_or_else(|| stored_deployment_invalid("stored subflow binding is invalid"))?;
        let deployment_revision = binding
            .get("deployment_revision_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                stored_deployment_invalid("stored subflow deployment revision is missing")
            })?;
        let child = by_deployment.get(deployment_revision).ok_or_else(|| {
            CompileError::new(
                "STORED_SUBFLOW_DEPENDENCY_UNAVAILABLE",
                "pinned child deployment is not yet available in the restored archive",
            )
        })?;
        let child_plan = child.versioned_plan();
        if binding.get("adapter").and_then(Value::as_str) != Some("durable_subflow")
            || binding
                .get("definition_revision_id")
                .and_then(Value::as_str)
                != Some(call.definition_revision_id.as_str())
            || binding.get("interface_version").and_then(Value::as_str)
                != Some(call.interface_version.as_str())
            || binding.get("plan_hash").and_then(Value::as_str)
                != Some(child_plan.plan_hash().as_str())
            || binding.get("binding_hash").and_then(Value::as_str)
                != Some(child_plan.binding_hash().as_str())
            || child_plan.definition_revision_id() != &call.definition_revision_id
        {
            return Err(stored_deployment_invalid(
                "stored subflow pin disagrees with the exact child deployment",
            ));
        }
        let outputs = BTreeMap::from([(
            PortName::new("result").map_err(plan_compile_error)?,
            child.published().plan().metadata().output_type().clone(),
        )]);
        let pin = ExecutionRevisionPin::new(
            child_plan.definition_revision_id().clone(),
            child_plan.deployment_revision_id().clone(),
            child_plan.plan_hash().clone(),
            child_plan.binding_hash().clone(),
        );
        registry
            .register(SubflowInterfaceContract::new(
                pin,
                call.interface_version.clone(),
                child.published().plan().metadata().input_contract().clone(),
                outputs,
                child.published().plan().metadata().error_type().clone(),
            ))
            .map_err(plan_compile_error)?;
    }
    Ok(registry)
}

pub fn compile_enabled_v3_agents(
    directory: &Path,
    enabled: &BTreeSet<String>,
) -> Result<V3AgentCatalog, CompileError> {
    let root = canonical_directory(directory)?;
    let mut agents = Vec::with_capacity(enabled.len());
    for agent_id in enabled {
        validate_agent_directory_name(agent_id)?;
        let agent = compile_v3_agent_dir(&root.join(agent_id))?;
        if agent.metadata.id != *agent_id {
            return Err(CompileError::new(
                "AGENT_ID_MISMATCH",
                format!(
                    "enabled agent directory '{agent_id}' declares id '{}'",
                    agent.metadata.id
                ),
            ));
        }
        agents.push(Arc::new(agent));
    }
    V3AgentCatalog::new(agents)
}

pub fn compile_v3_agent_dir(directory: &Path) -> Result<PublishedV3Agent, CompileError> {
    let root = canonical_directory(directory)?;
    let source_path = root.join(AGENT_FILE);
    let source = fs::read_to_string(&source_path).map_err(|_| {
        CompileError::new(
            "AGENT_READ_FAILED",
            format!("failed to read '{}'", source_path.display()),
        )
    })?;
    let raw = v3::parse(&source).map_err(CompileError::from)?;
    let document = v3::validate(raw)?;
    let metadata = document.metadata.clone().ok_or_else(|| {
        CompileError::new(
            "AGENT_METADATA_REQUIRED",
            "published agents must declare metadata",
        )
    })?;
    let prompt_files = resolve_prompt_files(&root, &document.prompts)?;
    let definition_revision_id = definition_revision(&source, &prompt_files)?;
    let source_id = format!("agents/{}/agent.yaml", metadata.id);
    let mut options = CompileOptions::new(definition_revision_id.clone(), source_id, &source);
    for (path, content) in &prompt_files {
        options = options.with_prompt_file(path.clone(), content.clone());
    }
    let plan = v3::compile(document, options)?;
    if plan.metadata().definition_revision_id() != &definition_revision_id {
        return Err(CompileError::new(
            "DEFINITION_REVISION_MISMATCH",
            "compiled Plan did not retain its frozen definition revision",
        ));
    }
    Ok(PublishedV3Agent {
        metadata,
        definition_revision_id,
        author_source: source,
        prompt_files,
        plan,
        graph_author_document: None,
    })
}

fn resolve_prompt_files(
    root: &Path,
    prompts: &BTreeMap<String, PromptDeclaration>,
) -> Result<BTreeMap<String, String>, CompileError> {
    let mut resolved = BTreeMap::new();
    for prompt in prompts.values() {
        let PromptDeclaration::File(relative) = prompt else {
            continue;
        };
        let relative_path = Path::new(relative);
        if relative_path.is_absolute()
            || relative_path
                .components()
                .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
        {
            return Err(CompileError::new(
                "PROMPT_PATH_INVALID",
                "prompt path must remain inside the agent directory",
            ));
        }
        let candidate = root.join(relative_path);
        let canonical = candidate.canonicalize().map_err(|_| {
            CompileError::new(
                "PROMPT_READ_FAILED",
                format!("failed to resolve prompt file '{relative}'"),
            )
        })?;
        if !canonical.starts_with(root) || !canonical.is_file() {
            return Err(CompileError::new(
                "PROMPT_PATH_INVALID",
                "prompt path must resolve to a regular file inside the agent directory",
            ));
        }
        let content = fs::read_to_string(&canonical).map_err(|_| {
            CompileError::new(
                "PROMPT_READ_FAILED",
                format!("failed to read prompt file '{relative}'"),
            )
        })?;
        resolved.insert(relative.clone(), content);
    }
    Ok(resolved)
}

fn definition_revision(
    source: &str,
    prompt_files: &BTreeMap<String, String>,
) -> Result<DefinitionRevisionId, CompileError> {
    let mut framed = Vec::new();
    append_frame(&mut framed, DEFINITION_REVISION_DOMAIN);
    append_frame(&mut framed, source.as_bytes());
    append_frame(&mut framed, &(prompt_files.len() as u64).to_be_bytes());
    for (path, content) in prompt_files {
        append_frame(&mut framed, path.as_bytes());
        append_frame(&mut framed, content.as_bytes());
    }
    let hash = ContentHash::from_bytes(&framed);
    DefinitionRevisionId::new(format!(
        "defrev_{}",
        hash.as_str().trim_start_matches("sha256:")
    ))
    .map_err(|error| CompileError::new("DEFINITION_REVISION_INVALID", error.to_string()))
}

fn exact_configuration_contract(
    descriptor: &LeafTaskDescriptor,
) -> DescriptorConfigurationContract {
    DescriptorConfigurationContract::closed(
        descriptor
            .public_configuration
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    DescriptorFieldContract::required(descriptor_schema(value)),
                )
            })
            .collect(),
        descriptor
            .secret_configuration
            .keys()
            .map(|name| (name.clone(), true))
            .collect(),
    )
}

fn descriptor_schema(value: &DescriptorValue) -> DescriptorValueSchema {
    match value {
        DescriptorValue::Null => DescriptorValueSchema::Null,
        DescriptorValue::Boolean(_) => DescriptorValueSchema::Boolean,
        DescriptorValue::Integer(_) => DescriptorValueSchema::Integer,
        DescriptorValue::Number(_) => DescriptorValueSchema::Number,
        DescriptorValue::String(_) => DescriptorValueSchema::String,
        DescriptorValue::Array(values) => {
            let mut schemas = values.iter().map(descriptor_schema);
            let first = schemas.next().unwrap_or(DescriptorValueSchema::Any);
            let item = if schemas.all(|candidate| candidate == first) {
                first
            } else {
                DescriptorValueSchema::Any
            };
            DescriptorValueSchema::Array(Box::new(item))
        }
        DescriptorValue::Object(values) => DescriptorValueSchema::Object(
            values
                .iter()
                .map(|(name, value)| {
                    (
                        name.clone(),
                        DescriptorFieldContract::required(descriptor_schema(value)),
                    )
                })
                .collect(),
        ),
    }
}

fn descriptor_string(value: &DescriptorValue) -> Option<&str> {
    match value {
        DescriptorValue::String(value) => Some(value),
        _ => None,
    }
}

fn descriptor_json(value: &DescriptorValue) -> Result<Value, CompileError> {
    Ok(match value {
        DescriptorValue::Null => Value::Null,
        DescriptorValue::Boolean(value) => Value::Bool(*value),
        DescriptorValue::Integer(value) => Value::Number((*value).into()),
        DescriptorValue::Number(value) => Value::Number(value.clone()),
        DescriptorValue::String(value) => Value::String(value.clone()),
        DescriptorValue::Array(values) => Value::Array(
            values
                .iter()
                .map(descriptor_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        DescriptorValue::Object(values) => Value::Object(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), descriptor_json(value)?)))
                .collect::<Result<Map<_, _>, CompileError>>()?,
        ),
    })
}

fn descriptor_contains_key(value: &DescriptorValue, expected: &str) -> bool {
    match value {
        DescriptorValue::Array(values) => values
            .iter()
            .any(|value| descriptor_contains_key(value, expected)),
        DescriptorValue::Object(values) => {
            values.contains_key(expected)
                || values
                    .values()
                    .any(|value| descriptor_contains_key(value, expected))
        }
        DescriptorValue::Null
        | DescriptorValue::Boolean(_)
        | DescriptorValue::Integer(_)
        | DescriptorValue::Number(_)
        | DescriptorValue::String(_) => false,
    }
}

fn plan_compile_error(error: impl std::fmt::Display) -> CompileError {
    CompileError::new("DEPLOYMENT_LINK_INVALID", error.to_string())
}

fn append_frame(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

fn canonical_directory(directory: &Path) -> Result<PathBuf, CompileError> {
    let root = directory.canonicalize().map_err(|_| {
        CompileError::new(
            "AGENTS_DIRECTORY_INVALID",
            format!("agents directory '{}' does not exist", directory.display()),
        )
    })?;
    if !root.is_dir() {
        return Err(CompileError::new(
            "AGENTS_DIRECTORY_INVALID",
            format!(
                "agents directory '{}' is not a directory",
                directory.display()
            ),
        ));
    }
    Ok(root)
}

fn validate_agent_directory_name(agent_id: &str) -> Result<(), CompileError> {
    let path = Path::new(agent_id);
    let mut components = path.components();
    let valid = matches!(components.next(), Some(Component::Normal(value)) if value == agent_id)
        && components.next().is_none();
    if !valid {
        return Err(CompileError::new(
            "AGENT_ID_INVALID",
            format!("enabled agent id '{agent_id}' must be one directory name"),
        ));
    }
    Ok(())
}
