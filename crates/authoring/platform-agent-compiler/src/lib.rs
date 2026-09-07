//! Deterministic compiler for the product Agent manifest.
//!
//! [`compile_agent`] accepts bounded bytes and exact, already-resolved bindings.
//! Project loading, credentials, publication and durable state belong to callers.

mod boundary;
pub use boundary::*;
pub mod editor;
pub mod evaluation;
pub mod evaluation_schema;
mod inspection;
pub use inspection::*;
mod features;
pub mod framework;
pub use features::feature_deployments;
mod source_map;
pub use source_map::*;
mod validation;
pub use validation::*;

use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, pinned_nominal_reference,
    AgentRequiredFeature, AgentResourceSpec, AgentSlotBindingInputV1, AgentSlotTargetInputV1,
    ArtifactPurpose, ArtifactRef, ArtifactState, AuthoringPackage, ClosedJsonSchema,
    DataClassification, ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef, JsonLimits,
    PlanNodeKind, ResourceDocument, ResourceKind, Sha256Digest, MAX_AGENT_AUTHOR_INSTRUCTION_BYTES,
    MAX_CLOSED_SCHEMA_BYTES,
};
use insight_platform_plan::{
    DataPortKey, ExactDataPortRef, PlanLimits, PlanNodeKey, RuntimeDependencyKind,
    RuntimeDependencySlot, RuntimeNode, RuntimePlan,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};
use yaml_rust2::{
    parser::{Event, MarkedEventReceiver, Parser},
    scanner::{Marker, TScalarStyle},
};

pub const AGENT_MANIFEST_API_VERSION: &str = "insight.platform/v1";
pub const AGENT_MANIFEST_KIND: &str = "Agent";
pub const MAX_AGENT_MANIFEST_BYTES: usize = 1_048_576;
pub const MAX_AGENT_DISPLAY_NAME_CHARS: usize = 255;
pub const MAX_AGENT_DEADLINE_SECONDS: u32 = 3_600;
pub const PRIMARY_MODEL_SLOT_ID: &str = "primary_model";
const AUTHORING_MEDIA_TYPE: &str = "application/json";
const TYPED_PLAN_MEDIA_TYPE: &str = "application/json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentCompilerErrorCode {
    AgentManifestInvalid,
    AgentReferenceMissing,
    AgentBindingNotReady,
    AgentCompileFailed,
}

#[derive(Debug)]
pub struct AgentCompilerError {
    code: AgentCompilerErrorCode,
    detail: String,
}

impl AgentCompilerError {
    fn manifest(detail: impl Into<String>) -> Self {
        Self {
            code: AgentCompilerErrorCode::AgentManifestInvalid,
            detail: detail.into(),
        }
    }

    pub fn reference(detail: impl Into<String>) -> Self {
        Self {
            code: AgentCompilerErrorCode::AgentReferenceMissing,
            detail: detail.into(),
        }
    }

    fn binding(detail: impl Into<String>) -> Self {
        Self {
            code: AgentCompilerErrorCode::AgentBindingNotReady,
            detail: detail.into(),
        }
    }

    fn compile(detail: impl Into<String>) -> Self {
        Self {
            code: AgentCompilerErrorCode::AgentCompileFailed,
            detail: detail.into(),
        }
    }

    pub const fn code(&self) -> AgentCompilerErrorCode {
        self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for AgentCompilerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl Error for AgentCompilerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentExecutionKind {
    Deterministic,
    ModelChat,
    FullPlan,
    FrameworkGraph,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentManifestV1 {
    api_version: String,
    kind: String,
    metadata: AgentManifestMetadata,
    spec: AgentManifestSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentManifestMetadata {
    name: String,
    #[serde(default)]
    display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentManifestSpec {
    execution: AgentManifestExecution,
    #[serde(default)]
    instructions: Option<String>,
    #[serde(default)]
    model: Option<AgentManifestModel>,
    input: AgentManifestInput,
    output: AgentManifestOutput,
    #[serde(default)]
    limits: Option<AgentManifestLimits>,
    #[serde(default)]
    publish: Option<AgentManifestPublish>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AgentManifestExecution {
    kind: AgentExecutionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plan: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AgentManifestModel {
    r#ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AgentManifestInput {
    schema: String,
    classification: DataClassification,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AgentManifestOutput {
    schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentManifestLimits {
    deadline_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AgentManifestPublish {
    environment: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCompilerProfile {
    pub default_deadline_seconds: u32,
    pub default_environment: String,
    pub policy_versions: Vec<ExactVersionRef>,
    pub deployment_policies: Vec<ExactPolicyBinding>,
    pub execution_profile: ExactPolicyBinding,
    pub model_loop: ModelLoopCompilerLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelLoopCompilerLimits {
    pub maximum_rounds: u16,
    pub maximum_capability_calls: u32,
    pub maximum_parallel_calls_per_round: u16,
    pub token_budget: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedModelBinding {
    pub manifest_ref: String,
    pub deployment: ExactDeploymentRef,
    pub selection_policy: ExactPolicyBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAgentBindings {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub slots: Vec<AgentSlotBindingInputV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deployment_features: Vec<insight_platform_contracts::AgentDeploymentFeaturesV1>,
    pub model: Option<ResolvedModelBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCompilerInput {
    pub plan_bytes: Option<Vec<u8>>,
    pub manifest_bytes: Vec<u8>,
    pub input_schema_bytes: Vec<u8>,
    pub output_schema_bytes: Vec<u8>,
    pub profile: AgentCompilerProfile,
    pub bindings: ResolvedAgentBindings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestResolution {
    pub plan_path: Option<String>,
    pub input_schema_path: String,
    pub output_schema_path: String,
    pub execution_kind: AgentExecutionKind,
    pub model_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResourceIntent {
    pub authoring_name: String,
    pub required_features: Vec<RequiredAgentFeature>,
    pub input_classification: DataClassification,
    pub default_deadline_seconds: u32,
    pub display_name: String,
    pub authoring_artifact: ArtifactIntent,
    pub contract_digest: Sha256Digest,
    pub dependency_versions: Vec<ExactVersionRef>,
    pub policy_versions: Vec<ExactVersionRef>,
    pub author_instructions: Option<String>,
    pub input_schema: ClosedJsonSchema,
    pub output_schema: ClosedJsonSchema,
    pub error_schema: ClosedJsonSchema,
    pub typed_plan_artifact: ArtifactIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactIntent {
    pub purpose: ArtifactPurpose,
    pub content_digest: Sha256Digest,
    pub byte_length: u64,
    pub media_type: String,
    pub classification: DataClassification,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactAuthority {
    pub purpose: ArtifactPurpose,
    pub state: ArtifactState,
    pub artifact: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentBindingIntent {
    pub environment: String,
    pub default_deadline_seconds: u32,
    pub entry_node_id: String,
    pub entry_node_kind: PlanNodeKind,
    pub slots: Vec<AgentSlotBindingInputV1>,
    pub policies: Vec<ExactPolicyBinding>,
    pub execution_profile: ExactPolicyBinding,
}

pub type RequiredAgentFeature = AgentRequiredFeature;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecyclePlan {
    pub schema_version: u32,
    pub steps: Vec<LifecycleStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleStep {
    pub ordinal: u8,
    pub kind: LifecycleStepKind,
    pub requires: Vec<LogicalOutputRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleStepKind {
    UploadAuthoringArtifact,
    UploadTypedPlanArtifact,
    MaterializeAgentDocument,
    UpsertDraft,
    ValidateDraft,
    PublishRevisions,
    CreateDeployment,
    ActivateDeployment,
    VerifyActiveBinding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalOutputRef {
    AuthoringArtifact,
    TypedPlanArtifact,
    AgentDocument,
    AgentResource,
    ValidationOperation,
    PublishedRevisions,
    AgentDeployment,
    ActiveBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledAgent {
    pub name: String,
    pub execution_kind: AgentExecutionKind,
    pub canonical_manifest_bytes: Vec<u8>,
    pub manifest_digest: Sha256Digest,
    pub resource_intent: AgentResourceIntent,
    pub typed_plan_bytes: Vec<u8>,
    pub typed_plan_digest: Sha256Digest,
    pub deployment_intent: DeploymentBindingIntent,
    pub required_features: Vec<RequiredAgentFeature>,
    pub lifecycle_plan: LifecyclePlan,
}

impl AgentResourceIntent {
    pub fn materialize(
        &self,
        authoring: &ArtifactAuthority,
        typed_plan: &ArtifactAuthority,
    ) -> Result<ResourceDocument, AgentCompilerError> {
        validate_artifact_authority(authoring, &self.authoring_artifact)?;
        validate_artifact_authority(typed_plan, &self.typed_plan_artifact)?;
        if authoring.artifact.artifact_id() == typed_plan.artifact.artifact_id() {
            return Err(AgentCompilerError::binding(
                "authoring and Typed Plan artifacts must have distinct authority IDs",
            ));
        }
        let document = ResourceDocument::Agent(AgentResourceSpec {
            authoring_name: self.authoring_name.clone(),
            required_features: self.required_features.clone(),
            input_classification: self.input_classification,
            default_deadline_seconds: self.default_deadline_seconds,
            authoring_package: AuthoringPackage {
                artifact: authoring.artifact.clone(),
                manifest_digest: self.authoring_artifact.content_digest.clone(),
            },
            contract_digest: self.contract_digest.clone(),
            dependency_versions: self.dependency_versions.clone(),
            policy_versions: self.policy_versions.clone(),
            author_instructions: self.author_instructions.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            error_schema: self.error_schema.clone(),
            typed_plan_artifact_id: typed_plan.artifact.artifact_id().clone(),
            typed_plan_digest: self.typed_plan_artifact.content_digest.clone(),
        });
        document
            .validate()
            .map_err(|error| AgentCompilerError::binding(error.to_string()))?;
        Ok(document)
    }
}

pub fn inspect_manifest(
    manifest_bytes: &[u8],
) -> Result<AgentManifestResolution, AgentCompilerError> {
    let manifest = parse_manifest(manifest_bytes)?;
    validate_manifest(&manifest)?;
    Ok(AgentManifestResolution {
        plan_path: manifest.spec.execution.plan.clone(),
        input_schema_path: manifest.spec.input.schema.clone(),
        output_schema_path: manifest.spec.output.schema.clone(),
        execution_kind: manifest.spec.execution.kind,
        model_ref: manifest.spec.model.map(|model| model.r#ref),
    })
}

/// The shared public object interface identity used by authoring adapters.
pub fn agent_interface_contract_digest(
    input: &ClosedJsonSchema,
    output: &ClosedJsonSchema,
) -> Result<Sha256Digest, AgentCompilerError> {
    input
        .validate()
        .map_err(|_| AgentCompilerError::compile("invalid input schema"))?;
    output
        .validate()
        .map_err(|_| AgentCompilerError::compile("invalid output schema"))?;
    let error=ClosedJsonSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":pinned_nominal_reference("Failure").ok_or_else(||AgentCompilerError::compile("Failure nominal schema unavailable"))?})).map_err(|_|AgentCompilerError::compile("invalid error schema"))?;
    digest_value(
        &json!({"error_schema_digest":error.canonical_digest,"input_schema_digest":input.canonical_digest,"output_schema_digest":output.canonical_digest,"schema_version":1}),
    )
}

fn compiler_error_schema() -> Result<ClosedJsonSchema, AgentCompilerError> {
    ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": pinned_nominal_reference("Failure")
            .ok_or_else(|| AgentCompilerError::compile("Failure nominal schema is unavailable"))?
    }))
    .map_err(|error| AgentCompilerError::compile(format!("error schema: {error}")))
}

fn parse_authored_plan(
    bytes: &[u8],
    kind: AgentExecutionKind,
    contract_digest: &Sha256Digest,
) -> Result<RuntimePlan, AgentCompilerError> {
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: boundary::MAX_AGENT_SOURCE_FILE_BYTES,
            max_depth: 32,
            max_properties_per_object: 1024,
            max_items_per_array: 4096,
            max_string_bytes: MAX_CLOSED_SCHEMA_BYTES,
        },
    )
    .map_err(|_| AgentCompilerError::compile("full Plan is not bounded strict JSON"))?;
    let plan: RuntimePlan = if kind == AgentExecutionKind::FrameworkGraph {
        serde_json::from_value::<framework::StaticFrameworkExportV1>(value)
            .map_err(|_| {
                AgentCompilerError::compile(
                    "framework export is not the closed static typed-port format",
                )
            })?
            .lower(contract_digest.clone())
            .map_err(|_| {
                AgentCompilerError::compile("framework export semantics or limits are unsupported")
            })?
    } else {
        serde_json::from_value(value)
            .map_err(|_| AgentCompilerError::compile("full Plan does not match the owning IR"))?
    };
    if plan.plan_version != 6 || plan.interface_contract_digest != *contract_digest {
        return Err(AgentCompilerError::compile(
            "full Plan ABI or interface contract differs",
        ));
    }
    Ok(plan)
}

fn validate_authored_plan(
    typed_plan: &mut RuntimePlan,
    input_schema: &ClosedJsonSchema,
    output_schema: &ClosedJsonSchema,
    error_schema: &ClosedJsonSchema,
) -> Result<(), AgentCompilerError> {
    typed_plan.plan_version = 6;
    for schema in [input_schema, output_schema, error_schema] {
        let value_schema = insight_platform_contracts::ClosedValueSchema::try_from(schema.clone())
            .map_err(|_| AgentCompilerError::compile("invalid object port schema"))?;
        if let Some(existing) = typed_plan
            .schema_documents
            .insert(schema.canonical_digest.clone(), value_schema.clone())
        {
            if existing != value_schema {
                return Err(AgentCompilerError::compile("schema identity conflict"));
            }
        }
    }
    let plan_limits =
        PlanLimits::from_profile(&insight_platform_contracts::checked_in_hard_limit_profile())
            .map_err(|error| AgentCompilerError::compile(error.to_string()))?;
    typed_plan
        .validate(plan_limits)
        .map_err(|error| AgentCompilerError::compile(error.to_string()))?;
    typed_plan
        .validate_terminal_schema_digests(
            &output_schema.canonical_digest,
            &error_schema.canonical_digest,
        )
        .map_err(|error| AgentCompilerError::compile(error.to_string()))?;
    Ok(())
}

pub fn compile_agent(input: AgentCompilerInput) -> Result<CompiledAgent, AgentCompilerError> {
    let mut manifest = parse_manifest(&input.manifest_bytes)?;
    validate_profile(&input.profile)?;
    validate_manifest(&manifest)?;

    let input_schema = compile_schema(&input.input_schema_bytes, "input schema")?;
    let output_schema = compile_schema(&input.output_schema_bytes, "output schema")?;
    let error_schema = compiler_error_schema()?;

    let display_name = manifest
        .metadata
        .display_name
        .clone()
        .unwrap_or_else(|| default_display_name(&manifest.metadata.name));
    validate_display_name(&display_name)?;
    manifest.metadata.display_name = Some(display_name.clone());
    let deadline_seconds = manifest
        .spec
        .limits
        .as_ref()
        .map_or(input.profile.default_deadline_seconds, |limits| {
            limits.deadline_seconds
        });
    let environment = manifest.spec.publish.as_ref().map_or_else(
        || input.profile.default_environment.clone(),
        |publish| publish.environment.clone(),
    );
    validate_deadline(deadline_seconds)?;
    validate_stable_name(&environment, "publish environment")?;
    manifest.spec.limits = Some(AgentManifestLimits { deadline_seconds });
    manifest.spec.publish = Some(AgentManifestPublish {
        environment: environment.clone(),
    });

    if manifest.spec.execution.kind == AgentExecutionKind::Deterministic
        && input_schema.canonical_digest != output_schema.canonical_digest
    {
        return Err(AgentCompilerError::compile(
            "deterministic execution requires identical input and output schema digests",
        ));
    }

    let contract_digest = agent_interface_contract_digest(&input_schema, &output_schema)?;
    let canonical_manifest_bytes = canonical_json(
        &serde_json::to_value(&manifest)
            .map_err(|error| AgentCompilerError::compile(error.to_string()))?,
    )
    .map_err(|error| AgentCompilerError::compile(error.to_string()))?;
    let manifest_digest = digest_bytes(&canonical_manifest_bytes)?;

    let (mut typed_plan, slots, _) = match manifest.spec.execution.kind {
        AgentExecutionKind::Deterministic => {
            if input.bindings.model.is_some()
                || !input.bindings.slots.is_empty()
                || input.plan_bytes.is_some()
            {
                return Err(AgentCompilerError::binding(
                    "deterministic execution cannot bind a model",
                ));
            }
            (
                deterministic_plan(&contract_digest, &output_schema.canonical_digest),
                Vec::new(),
                Vec::new(),
            )
        }
        AgentExecutionKind::ModelChat => {
            if !input.bindings.slots.is_empty() || input.plan_bytes.is_some() {
                return Err(AgentCompilerError::binding(
                    "model template has unexpected full Plan input",
                ));
            }
            let model =
                manifest.spec.model.as_ref().ok_or_else(|| {
                    AgentCompilerError::manifest("model_chat requires spec.model")
                })?;
            let resolved = input.bindings.model.as_ref().ok_or_else(|| {
                AgentCompilerError::reference(format!(
                    "model reference {:?} is unresolved",
                    model.r#ref
                ))
            })?;
            validate_resolved_model(&model.r#ref, resolved)?;
            let requirement_digest = model_requirement_digest(&model.r#ref)?;
            let slot = AgentSlotBindingInputV1 {
                slot_id: PRIMARY_MODEL_SLOT_ID.to_owned(),
                requirement_digest: requirement_digest.clone(),
                target: AgentSlotTargetInputV1::Model {
                    candidates: vec![resolved.deployment.clone()],
                    selection_policy: resolved.selection_policy.clone(),
                },
            };
            (
                model_chat_plan(
                    &contract_digest,
                    &input_schema.canonical_digest,
                    &output_schema.canonical_digest,
                    &requirement_digest,
                    input.profile.model_loop,
                ),
                vec![slot],
                vec![RequiredAgentFeature::Model],
            )
        }
        AgentExecutionKind::FullPlan | AgentExecutionKind::FrameworkGraph => {
            if input.bindings.model.is_some() {
                return Err(AgentCompilerError::binding(
                    "full Plan uses exact slot bindings",
                ));
            }
            let bytes = input
                .plan_bytes
                .as_deref()
                .ok_or_else(|| AgentCompilerError::reference("full Plan source is missing"))?;
            let plan = parse_authored_plan(bytes, manifest.spec.execution.kind, &contract_digest)?;
            let mut slots = Vec::new();
            let mut observed = BTreeSet::new();
            for binding in &input.bindings.slots {
                binding
                    .validate()
                    .map_err(|_| AgentCompilerError::binding("exact slot binding is invalid"))?;
                let required = plan.dependency_slots.get(&binding.slot_id).ok_or_else(|| {
                    AgentCompilerError::binding("binding does not name a Plan slot")
                })?;
                let kind = match binding.target {
                    AgentSlotTargetInputV1::Model { .. } => RuntimeDependencyKind::Model,
                    AgentSlotTargetInputV1::Capability { .. } => RuntimeDependencyKind::Capability,
                    AgentSlotTargetInputV1::Context { .. } => RuntimeDependencyKind::Context,
                    AgentSlotTargetInputV1::ChildAgent { .. } => RuntimeDependencyKind::ChildAgent,
                    AgentSlotTargetInputV1::Skill { .. } => RuntimeDependencyKind::Skill,
                };
                if required.kind != kind
                    || required.requirement_digest != binding.requirement_digest
                    || !observed.insert(binding.slot_id.clone())
                {
                    return Err(AgentCompilerError::binding(
                        "binding does not satisfy its exact Plan slot",
                    ));
                }
                slots.push(AgentSlotBindingInputV1 {
                    slot_id: binding.slot_id.clone(),
                    requirement_digest: binding.requirement_digest.clone(),
                    target: binding.target.clone(),
                });
            }
            if observed.len() != plan.dependency_slots.len() {
                return Err(AgentCompilerError::binding(
                    "full Plan slot binding is missing",
                ));
            }
            slots.sort_by(|left, right| left.slot_id.cmp(&right.slot_id));
            let features = if plan
                .nodes
                .values()
                .any(|node| matches!(node, RuntimeNode::ModelLoop { .. }))
            {
                vec![RequiredAgentFeature::Model]
            } else {
                Vec::new()
            };
            (plan, slots, features)
        }
    };
    let required_features = crate::features::required_features(&typed_plan, &input.bindings)?;
    // Every authoring path emits the sole current IR.
    validate_authored_plan(
        &mut typed_plan,
        &input_schema,
        &output_schema,
        &error_schema,
    )?;
    let typed_plan_bytes = canonical_json(
        &serde_json::to_value(&typed_plan)
            .map_err(|error| AgentCompilerError::compile(error.to_string()))?,
    )
    .map_err(|error| AgentCompilerError::compile(error.to_string()))?;
    let typed_plan_digest = digest_bytes(&typed_plan_bytes)?;

    let mut policy_versions = input.profile.policy_versions.clone();
    normalize_versions(&mut policy_versions)?;
    let mut deployment_policies = input.profile.deployment_policies.clone();
    normalize_policy_bindings(&mut deployment_policies)?;

    let authoring_length = u64::try_from(canonical_manifest_bytes.len())
        .map_err(|_| AgentCompilerError::compile("canonical manifest length overflow"))?;
    let typed_plan_length = u64::try_from(typed_plan_bytes.len())
        .map_err(|_| AgentCompilerError::compile("Typed Plan length overflow"))?;
    let resource_intent = AgentResourceIntent {
        authoring_name: manifest.metadata.name.clone(),
        required_features: required_features.clone(),
        input_classification: manifest.spec.input.classification,
        default_deadline_seconds: deadline_seconds,
        display_name,
        authoring_artifact: ArtifactIntent {
            purpose: ArtifactPurpose::AuthoringDocument,
            content_digest: manifest_digest.clone(),
            byte_length: authoring_length,
            media_type: AUTHORING_MEDIA_TYPE.to_owned(),
            classification: DataClassification::Internal,
            display_name: Some(format!("{}.agent.json", manifest.metadata.name)),
        },
        contract_digest: contract_digest.clone(),
        dependency_versions: Vec::new(),
        policy_versions,
        author_instructions: manifest.spec.instructions.clone(),
        input_schema,
        output_schema,
        error_schema,
        typed_plan_artifact: ArtifactIntent {
            purpose: ArtifactPurpose::TypedPlan,
            content_digest: typed_plan_digest.clone(),
            byte_length: typed_plan_length,
            media_type: TYPED_PLAN_MEDIA_TYPE.to_owned(),
            classification: DataClassification::Internal,
            display_name: Some(format!("{}.plan.json", manifest.metadata.name)),
        },
    };
    Ok(CompiledAgent {
        name: manifest.metadata.name,
        execution_kind: manifest.spec.execution.kind,
        canonical_manifest_bytes,
        manifest_digest,
        resource_intent,
        typed_plan_bytes,
        typed_plan_digest,
        deployment_intent: DeploymentBindingIntent {
            environment,
            default_deadline_seconds: deadline_seconds,
            entry_node_id: typed_plan.entry_node_id.as_str().to_owned(),
            entry_node_kind: typed_plan.nodes[&typed_plan.entry_node_id].kind(),
            slots,
            policies: deployment_policies,
            execution_profile: input.profile.execution_profile,
        },
        required_features,
        lifecycle_plan: lifecycle_plan(),
    })
}

fn parse_manifest(bytes: &[u8]) -> Result<AgentManifestV1, AgentCompilerError> {
    if bytes.len() > MAX_AGENT_MANIFEST_BYTES {
        return Err(AgentCompilerError::manifest(format!(
            "manifest exceeds {MAX_AGENT_MANIFEST_BYTES} bytes"
        )));
    }
    let source = std::str::from_utf8(bytes)
        .map_err(|_| AgentCompilerError::manifest("manifest is not valid UTF-8"))?;
    if source.is_empty() || source.contains('\r') {
        return Err(AgentCompilerError::manifest(
            "manifest must be non-empty UTF-8 with LF newlines",
        ));
    }
    validate_yaml_subset(source)?;
    yaml_serde::from_str(source)
        .map_err(|_| AgentCompilerError::manifest("manifest does not match the closed schema"))
}

fn validate_manifest(manifest: &AgentManifestV1) -> Result<(), AgentCompilerError> {
    if manifest.api_version != AGENT_MANIFEST_API_VERSION || manifest.kind != AGENT_MANIFEST_KIND {
        return Err(AgentCompilerError::manifest(
            "apiVersion or kind is not the exact Agent v1 contract",
        ));
    }
    validate_stable_name(&manifest.metadata.name, "metadata.name")?;
    if let Some(display_name) = &manifest.metadata.display_name {
        validate_display_name(display_name)?;
    }
    validate_relative_reference(&manifest.spec.input.schema, "input schema")?;
    validate_relative_reference(&manifest.spec.output.schema, "output schema")?;
    if let Some(limits) = &manifest.spec.limits {
        validate_deadline(limits.deadline_seconds)?;
    }
    if let Some(publish) = &manifest.spec.publish {
        validate_stable_name(&publish.environment, "publish environment")?;
    }
    if matches!(
        manifest.spec.execution.kind,
        AgentExecutionKind::FullPlan | AgentExecutionKind::FrameworkGraph
    ) {
        let path = manifest
            .spec
            .execution
            .plan
            .as_deref()
            .ok_or_else(|| AgentCompilerError::manifest("full_plan requires execution.plan"))?;
        validate_relative_reference(path, "full Plan")?;
    } else if manifest.spec.execution.plan.is_some() {
        return Err(AgentCompilerError::manifest(
            "template execution cannot reference a full Plan",
        ));
    }
    match manifest.spec.execution.kind {
        AgentExecutionKind::FullPlan | AgentExecutionKind::FrameworkGraph => {
            if manifest.spec.model.is_some() {
                return Err(AgentCompilerError::manifest(
                    "full Plan binds models through exact slots",
                ));
            }
            if let Some(instructions) = &manifest.spec.instructions {
                if instructions.is_empty()
                    || instructions.len() > MAX_AGENT_AUTHOR_INSTRUCTION_BYTES
                    || instructions.contains('\0')
                {
                    return Err(AgentCompilerError::manifest(
                        "author instructions are invalid",
                    ));
                }
                reject_sensitive_literal(instructions)?;
            }
        }
        AgentExecutionKind::Deterministic => {
            if manifest.spec.instructions.is_some() || manifest.spec.model.is_some() {
                return Err(AgentCompilerError::manifest(
                    "deterministic execution forbids instructions and model",
                ));
            }
        }
        AgentExecutionKind::ModelChat => {
            let instructions = manifest.spec.instructions.as_ref().ok_or_else(|| {
                AgentCompilerError::manifest("model_chat requires spec.instructions")
            })?;
            if instructions.is_empty()
                || instructions.len() > MAX_AGENT_AUTHOR_INSTRUCTION_BYTES
                || instructions.contains('\0')
            {
                return Err(AgentCompilerError::manifest(
                    "spec.instructions is empty, contains NUL, or exceeds its byte limit",
                ));
            }
            reject_sensitive_literal(instructions)?;
            let model =
                manifest.spec.model.as_ref().ok_or_else(|| {
                    AgentCompilerError::manifest("model_chat requires spec.model")
                })?;
            validate_model_ref(&model.r#ref)?;
        }
    }
    Ok(())
}

fn compile_schema(bytes: &[u8], name: &str) -> Result<ClosedJsonSchema, AgentCompilerError> {
    let schema = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: MAX_CLOSED_SCHEMA_BYTES,
            max_depth: 32,
            max_properties_per_object: 1_024,
            max_items_per_array: 4_096,
            max_string_bytes: MAX_CLOSED_SCHEMA_BYTES,
        },
    )
    .map_err(|error| AgentCompilerError::compile(format!("{name}: {error}")))?;
    ClosedJsonSchema::build(schema)
        .map_err(|error| AgentCompilerError::compile(format!("{name}: {error}")))
}

fn validate_profile(profile: &AgentCompilerProfile) -> Result<(), AgentCompilerError> {
    validate_deadline(profile.default_deadline_seconds)?;
    validate_stable_name(&profile.default_environment, "profile environment")?;
    profile
        .execution_profile
        .validate()
        .map_err(|error| AgentCompilerError::binding(error.to_string()))?;
    if profile.model_loop.maximum_rounds == 0
        || profile.model_loop.maximum_capability_calls == 0
        || profile.model_loop.maximum_parallel_calls_per_round == 0
        || profile.model_loop.token_budget == 0
    {
        return Err(AgentCompilerError::binding(
            "model loop profile limits must be positive",
        ));
    }
    for policy in &profile.policy_versions {
        policy
            .validate()
            .map_err(|error| AgentCompilerError::binding(error.to_string()))?;
        if policy.resource_kind != ResourceKind::PolicyRevision {
            return Err(AgentCompilerError::binding(
                "profile policy_versions contains a non-Policy revision",
            ));
        }
    }
    for binding in &profile.deployment_policies {
        binding
            .validate()
            .map_err(|error| AgentCompilerError::binding(error.to_string()))?;
    }
    Ok(())
}

fn validate_resolved_model(
    manifest_ref: &str,
    resolved: &ResolvedModelBinding,
) -> Result<(), AgentCompilerError> {
    if resolved.manifest_ref != manifest_ref
        || resolved.deployment.resource_kind != ResourceKind::ModelDeployment
        || resolved.deployment.validate().is_err()
        || resolved.selection_policy.validate().is_err()
    {
        return Err(AgentCompilerError::binding(
            "resolved model binding does not match the manifest or exact kind",
        ));
    }
    Ok(())
}

fn normalize_versions(versions: &mut [ExactVersionRef]) -> Result<(), AgentCompilerError> {
    versions.sort_by(|left, right| left.revision_id.cmp(&right.revision_id));
    if versions
        .windows(2)
        .any(|pair| pair[0].revision_id == pair[1].revision_id)
    {
        return Err(AgentCompilerError::binding(
            "policy_versions contains duplicate revision IDs",
        ));
    }
    Ok(())
}

fn normalize_policy_bindings(
    bindings: &mut [ExactPolicyBinding],
) -> Result<(), AgentCompilerError> {
    bindings.sort_by(|left, right| {
        left.deployment
            .deployment_id
            .cmp(&right.deployment.deployment_id)
    });
    if bindings
        .windows(2)
        .any(|pair| pair[0].deployment.deployment_id == pair[1].deployment.deployment_id)
    {
        return Err(AgentCompilerError::binding(
            "deployment policies contain duplicate Deployment IDs",
        ));
    }
    Ok(())
}

fn validate_artifact_authority(
    authority: &ArtifactAuthority,
    intent: &ArtifactIntent,
) -> Result<(), AgentCompilerError> {
    if authority.state != ArtifactState::Ready
        || authority.purpose != intent.purpose
        || authority.artifact.content_digest() != &intent.content_digest
        || authority.artifact.byte_length() != intent.byte_length
        || authority.artifact.media_type() != intent.media_type
        || authority.artifact.classification() != intent.classification
        || authority.artifact.display_name() != intent.display_name.as_deref()
    {
        return Err(AgentCompilerError::binding(
            "Artifact authority does not match the frozen compiler intent",
        ));
    }
    Ok(())
}

fn node_key(value: &str) -> PlanNodeKey {
    PlanNodeKey::new(value.to_owned()).expect("static compiler node key is valid")
}

fn deterministic_plan(contract: &Sha256Digest, schema: &Sha256Digest) -> RuntimePlan {
    RuntimePlan {
        schema_documents: Default::default(),
        plan_version: 6,
        interface_contract_digest: contract.clone(),
        entry_node_id: node_key("start"),
        dependency_slots: BTreeMap::new(),
        nodes: BTreeMap::from([
            (
                node_key("start"),
                RuntimeNode::Start {
                    next: node_key("finish"),
                },
            ),
            (
                node_key("finish"),
                RuntimeNode::Return {
                    value: ExactDataPortRef::RunInput {
                        schema_digest: schema.clone(),
                    },
                },
            ),
        ]),
    }
}

fn model_chat_plan(
    contract: &Sha256Digest,
    input_schema: &Sha256Digest,
    output_schema: &Sha256Digest,
    requirement: &Sha256Digest,
    limits: ModelLoopCompilerLimits,
) -> RuntimePlan {
    let response = ExactDataPortRef::NodeOutput {
        producer_node_id: node_key("model"),
        port_id: DataPortKey::new("response".to_owned()).expect("static compiler port is valid"),
        schema_digest: output_schema.clone(),
    };
    RuntimePlan {
        schema_documents: Default::default(),
        plan_version: 6,
        interface_contract_digest: contract.clone(),
        entry_node_id: node_key("start"),
        dependency_slots: BTreeMap::from([(
            PRIMARY_MODEL_SLOT_ID.to_owned(),
            RuntimeDependencySlot {
                kind: RuntimeDependencyKind::Model,
                requirement_digest: requirement.clone(),
            },
        )]),
        nodes: BTreeMap::from([
            (
                node_key("start"),
                RuntimeNode::Start {
                    next: node_key("model"),
                },
            ),
            (
                node_key("model"),
                RuntimeNode::ModelLoop {
                    model_slot_id: PRIMARY_MODEL_SLOT_ID.to_owned(),
                    skill_slot_ids: vec![],
                    capability_slot_ids: vec![],
                    input: ExactDataPortRef::RunInput {
                        schema_digest: input_schema.clone(),
                    },
                    model_route: None,
                    output: response.clone(),
                    maximum_rounds: limits.maximum_rounds,
                    maximum_capability_calls: limits.maximum_capability_calls,
                    maximum_parallel_calls_per_round: limits.maximum_parallel_calls_per_round,
                    token_budget: limits.token_budget,
                    resume: node_key("finish"),
                },
            ),
            (node_key("finish"), RuntimeNode::Return { value: response }),
        ]),
    }
}

fn model_requirement_digest(manifest_ref: &str) -> Result<Sha256Digest, AgentCompilerError> {
    digest_value(&json!({
        "kind": "model",
        "manifest_ref": manifest_ref,
        "schema_version": 1
    }))
}

fn lifecycle_plan() -> LifecyclePlan {
    use LifecycleStepKind as Step;
    use LogicalOutputRef as Output;
    let steps = [
        (Step::UploadAuthoringArtifact, vec![]),
        (Step::UploadTypedPlanArtifact, vec![]),
        (
            Step::MaterializeAgentDocument,
            vec![Output::AuthoringArtifact, Output::TypedPlanArtifact],
        ),
        (Step::UpsertDraft, vec![Output::AgentDocument]),
        (Step::ValidateDraft, vec![Output::AgentResource]),
        (Step::PublishRevisions, vec![Output::ValidationOperation]),
        (Step::CreateDeployment, vec![Output::PublishedRevisions]),
        (Step::ActivateDeployment, vec![Output::AgentDeployment]),
        (Step::VerifyActiveBinding, vec![Output::ActiveBinding]),
    ];
    LifecyclePlan {
        schema_version: 1,
        steps: steps
            .into_iter()
            .enumerate()
            .map(|(index, (kind, requires))| LifecycleStep {
                ordinal: u8::try_from(index + 1)
                    .expect("closed lifecycle has fewer than 256 steps"),
                kind,
                requires,
            })
            .collect(),
    }
}

fn validate_deadline(value: u32) -> Result<(), AgentCompilerError> {
    if value == 0 || value > MAX_AGENT_DEADLINE_SECONDS {
        Err(AgentCompilerError::manifest(
            "deadlineSeconds must be between 1 and 3600",
        ))
    } else {
        Ok(())
    }
}

fn validate_stable_name(value: &str, field: &str) -> Result<(), AgentCompilerError> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 63
        || !bytes[0].is_ascii_lowercase()
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
    {
        return Err(AgentCompilerError::manifest(format!(
            "{field} is not a stable lowercase name"
        )));
    }
    Ok(())
}

fn validate_display_name(value: &str) -> Result<(), AgentCompilerError> {
    if value.is_empty()
        || value.chars().count() > MAX_AGENT_DISPLAY_NAME_CHARS
        || value.chars().any(char::is_control)
    {
        return Err(AgentCompilerError::manifest(
            "metadata.displayName is empty, too long, or contains control characters",
        ));
    }
    Ok(())
}

fn default_display_name(name: &str) -> String {
    name.split('-')
        .map(|part| {
            let mut characters = part.chars();
            characters.next().map_or_else(String::new, |first| {
                first.to_ascii_uppercase().to_string() + characters.as_str()
            })
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn validate_model_ref(value: &str) -> Result<(), AgentCompilerError> {
    if let Some(alias) = value.strip_prefix("project/") {
        return validate_stable_name(alias, "model ref");
    }
    let id = value
        .parse::<insight_platform_contracts::ResourceId>()
        .map_err(|_| {
            AgentCompilerError::manifest("model ref is not a project alias or exact ID")
        })?;
    if id.kind() != ResourceKind::ModelDeployment {
        return Err(AgentCompilerError::manifest(
            "advanced model ref must be a Model Deployment ID",
        ));
    }
    Ok(())
}

pub fn validate_relative_reference(value: &str, name: &str) -> Result<(), AgentCompilerError> {
    if value.is_empty()
        || value.len() > 256
        || value.contains('\\')
        || value.contains('\0')
        || value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(AgentCompilerError::manifest(format!(
            "{name} path must remain relative to the project root"
        )));
    }
    Ok(())
}

fn reject_sensitive_literal(value: &str) -> Result<(), AgentCompilerError> {
    let lowercase = value.to_ascii_lowercase();
    if lowercase.contains("http://")
        || lowercase.contains("https://")
        || lowercase.contains("postgres://")
        || lowercase.contains("postgresql://")
        || lowercase.contains("mysql://")
        || lowercase.contains("mongodb://")
        || lowercase.contains("sh -c")
        || lowercase.contains("bash -c")
        || value.contains("$(")
    {
        return Err(AgentCompilerError::manifest(
            "instructions contain an endpoint, database URL, or shell command",
        ));
    }
    Ok(())
}

#[derive(Default)]
struct YamlEventCollector {
    events: Vec<(Event, Marker)>,
}

impl MarkedEventReceiver for YamlEventCollector {
    fn on_event(&mut self, event: Event, marker: Marker) {
        self.events.push((event, marker));
    }
}

fn validate_yaml_subset(source: &str) -> Result<(), AgentCompilerError> {
    let mut collector = YamlEventCollector::default();
    Parser::new_from_str(source)
        .load(&mut collector, true)
        .map_err(|_| AgentCompilerError::manifest("manifest is not valid YAML"))?;
    let mut cursor = YamlEventCursor {
        events: &collector.events,
        position: 0,
    };
    cursor.expect(|event| matches!(event, Event::StreamStart))?;
    cursor.expect(|event| matches!(event, Event::DocumentStart))?;
    cursor.read_node(false, 0)?;
    cursor.expect(|event| matches!(event, Event::DocumentEnd))?;
    cursor.expect(|event| matches!(event, Event::StreamEnd))?;
    if cursor.position != cursor.events.len() {
        return Err(AgentCompilerError::manifest(
            "manifest must contain exactly one YAML document",
        ));
    }
    Ok(())
}

struct YamlEventCursor<'a> {
    events: &'a [(Event, Marker)],
    position: usize,
}

impl YamlEventCursor<'_> {
    fn expect(&mut self, predicate: impl FnOnce(&Event) -> bool) -> Result<(), AgentCompilerError> {
        let Some((event, _)) = self.events.get(self.position) else {
            return Err(AgentCompilerError::manifest("manifest YAML ended early"));
        };
        if !predicate(event) {
            return Err(AgentCompilerError::manifest(
                "manifest uses a non-JSON-compatible YAML construct",
            ));
        }
        self.position += 1;
        Ok(())
    }

    fn read_node(
        &mut self,
        mapping_key: bool,
        depth: usize,
    ) -> Result<Option<String>, AgentCompilerError> {
        if depth > 32 {
            return Err(AgentCompilerError::manifest("manifest nesting is too deep"));
        }
        let Some((event, _)) = self.events.get(self.position).cloned() else {
            return Err(AgentCompilerError::manifest("manifest YAML ended early"));
        };
        self.position += 1;
        match event {
            Event::Scalar(value, style, anchor, tag) => {
                if anchor != 0 || tag.is_some() || invalid_plain_scalar(&value, style) {
                    return Err(AgentCompilerError::manifest(
                        "manifest scalar uses an anchor, tag, or non-JSON implicit value",
                    ));
                }
                if mapping_key && value == "<<" {
                    return Err(AgentCompilerError::manifest(
                        "YAML merge keys are forbidden",
                    ));
                }
                Ok(Some(value))
            }
            Event::SequenceStart(anchor, tag) => {
                if anchor != 0 || tag.is_some() || mapping_key {
                    return Err(AgentCompilerError::manifest(
                        "YAML anchors, tags, and non-scalar keys are forbidden",
                    ));
                }
                while !matches!(
                    self.events.get(self.position),
                    Some((Event::SequenceEnd, _))
                ) {
                    self.read_node(false, depth + 1)?;
                }
                self.position += 1;
                Ok(None)
            }
            Event::MappingStart(anchor, tag) => {
                if anchor != 0 || tag.is_some() || mapping_key {
                    return Err(AgentCompilerError::manifest(
                        "YAML anchors, tags, and non-scalar keys are forbidden",
                    ));
                }
                let mut keys = BTreeSet::new();
                while !matches!(self.events.get(self.position), Some((Event::MappingEnd, _))) {
                    let Some(key) = self.read_node(true, depth + 1)? else {
                        return Err(AgentCompilerError::manifest(
                            "manifest mapping keys must be strings",
                        ));
                    };
                    if !keys.insert(key) {
                        return Err(AgentCompilerError::manifest(
                            "manifest contains a duplicate mapping key",
                        ));
                    }
                    self.read_node(false, depth + 1)?;
                }
                self.position += 1;
                Ok(None)
            }
            Event::Alias(_) => Err(AgentCompilerError::manifest("YAML aliases are forbidden")),
            _ => Err(AgentCompilerError::manifest(
                "manifest uses a non-JSON-compatible YAML construct",
            )),
        }
    }
}

fn invalid_plain_scalar(value: &str, style: TScalarStyle) -> bool {
    if style != TScalarStyle::Plain {
        return false;
    }
    let lowercase = value.to_ascii_lowercase();
    if matches!(
        lowercase.as_str(),
        "~" | ".nan" | ".inf" | "+.inf" | "-.inf"
    ) || looks_like_timestamp(value)
    {
        return true;
    }
    let numeric_prefix = value
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_digit() || matches!(*byte, b'+' | b'-' | b'.'));
    numeric_prefix
        && (value.starts_with('+')
            || value.contains('_')
            || lowercase.starts_with("0x")
            || lowercase.starts_with("0o")
            || lowercase.starts_with("0b")
            || value.ends_with('.')
            || invalid_leading_zero(value))
}

fn invalid_leading_zero(value: &str) -> bool {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    unsigned.len() > 1 && unsigned.starts_with('0') && unsigned.as_bytes()[1].is_ascii_digit()
}

fn looks_like_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 10
        && bytes.get(4) == Some(&b'-')
        && bytes.get(7) == Some(&b'-')
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

fn digest_value(value: &Value) -> Result<Sha256Digest, AgentCompilerError> {
    canonical_digest(value)
        .map_err(|error| AgentCompilerError::compile(error.to_string()))?
        .parse()
        .map_err(|error| AgentCompilerError::compile(format!("digest: {error}")))
}

fn digest_bytes(bytes: &[u8]) -> Result<Sha256Digest, AgentCompilerError> {
    use sha2::{Digest as _, Sha256};
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}")
            .map_err(|_| AgentCompilerError::compile("digest encoding failed"))?;
    }
    encoded
        .parse()
        .map_err(|error| AgentCompilerError::compile(format!("digest: {error}")))
}

#[cfg(test)]
mod tests {
    fn workspace_root() -> &'static std::path::Path {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .find(|candidate| {
                candidate.join("Cargo.toml").is_file()
                    && candidate
                        .join("contracts/platform-v1/manifest.json")
                        .is_file()
            })
            .expect("compiler test is inside the marked workspace")
    }

    use super::*;
    use insight_platform_contracts::ResourceId;
    use std::fs;

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ConformanceCorpus {
        schema_version: u32,
        profile: AgentCompilerProfile,
        cases: Vec<ConformanceCase>,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ConformanceCase {
        case_id: String,
        manifest: String,
        input_schema: String,
        output_schema: String,
        bindings: ResolvedAgentBindings,
        expected: Value,
    }

    const SCHEMA: &str = r#"{
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "type":"object",
        "properties":{"message":{"type":"string","minLength":1,"maxLength":128,"x-platform-max-bytes":512}},
        "required":["message"],
        "additionalProperties":false
    }"#;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn policy_binding(suffix: u16) -> ExactPolicyBinding {
        ExactPolicyBinding {
            deployment: ExactDeploymentRef::new(
                id(ResourceKind::PolicyDeployment, suffix),
                digest('a'),
            )
            .unwrap(),
            revision: ExactVersionRef::new(
                id(ResourceKind::PolicyRevision, suffix + 1),
                digest('b'),
            )
            .unwrap(),
        }
    }

    fn profile() -> AgentCompilerProfile {
        AgentCompilerProfile {
            default_deadline_seconds: 120,
            default_environment: "development".to_owned(),
            policy_versions: vec![policy_binding(10).revision],
            deployment_policies: vec![policy_binding(20)],
            execution_profile: policy_binding(30),
            model_loop: ModelLoopCompilerLimits {
                maximum_rounds: 1,
                maximum_capability_calls: 1,
                maximum_parallel_calls_per_round: 1,
                token_budget: 2_304,
            },
        }
    }

    fn deterministic_yaml() -> Vec<u8> {
        br#"apiVersion: insight.platform/v1
kind: Agent
metadata:
  name: echo-agent
spec:
  execution:
    kind: deterministic
  input:
    schema: schemas/io.json
    classification: internal
  output:
    schema: schemas/io.json
"#
        .to_vec()
    }

    fn model_yaml() -> Vec<u8> {
        br#"apiVersion: insight.platform/v1
kind: Agent
metadata:
  name: support-agent
  displayName: Support Agent
spec:
  execution:
    kind: model_chat
  instructions: |
    Answer using only the current user input.
  model:
    ref: project/default-model
  input:
    schema: schemas/input.json
    classification: internal
  output:
    schema: schemas/output.json
  limits:
    deadlineSeconds: 120
  publish:
    environment: development
"#
        .to_vec()
    }

    fn model_binding() -> ResolvedAgentBindings {
        ResolvedAgentBindings {
            deployment_features: Vec::new(),
            slots: Vec::new(),
            model: Some(ResolvedModelBinding {
                manifest_ref: "project/default-model".to_owned(),
                deployment: ExactDeploymentRef::new(
                    id(ResourceKind::ModelDeployment, 40),
                    digest('c'),
                )
                .unwrap(),
                selection_policy: policy_binding(50),
            }),
        }
    }

    fn conformance_projection(compiled: &CompiledAgent) -> Value {
        json!({
            "canonical_manifest": std::str::from_utf8(&compiled.canonical_manifest_bytes).unwrap(),
            "contract_digest": compiled.resource_intent.contract_digest,
            "deployment_intent_digest": digest_value(&serde_json::to_value(&compiled.deployment_intent).unwrap()).unwrap(),
            "execution_kind": compiled.execution_kind,
            "lifecycle_plan_digest": digest_value(&serde_json::to_value(&compiled.lifecycle_plan).unwrap()).unwrap(),
            "manifest_digest": compiled.manifest_digest,
            "name": compiled.name,
            "required_features": compiled.required_features,
            "resource_intent_digest": digest_value(&serde_json::to_value(&compiled.resource_intent).unwrap()).unwrap(),
            "typed_plan": std::str::from_utf8(&compiled.typed_plan_bytes).unwrap(),
            "typed_plan_digest": compiled.typed_plan_digest
        })
    }

    #[test]
    fn repository_conformance_corpus_matches_rust_owner_output() {
        let root = workspace_root().join("contracts/product-experience/agent-compiler/v2");
        let corpus: ConformanceCorpus = serde_json::from_slice(
            &fs::read(root.join("corpus.json")).expect("read compiler corpus"),
        )
        .expect("closed compiler corpus");
        assert_eq!(corpus.schema_version, 2);
        for case in corpus.cases {
            let compiled = compile_agent(AgentCompilerInput {
                plan_bytes: None,
                manifest_bytes: fs::read(root.join(&case.manifest)).unwrap(),
                input_schema_bytes: fs::read(root.join(&case.input_schema)).unwrap(),
                output_schema_bytes: fs::read(root.join(&case.output_schema)).unwrap(),
                profile: corpus.profile.clone(),
                bindings: case.bindings,
            })
            .unwrap_or_else(|error| panic!("case {} failed: {error}", case.case_id));
            let actual = conformance_projection(&compiled);
            assert_eq!(actual, case.expected, "case {} drifted", case.case_id);
        }
    }

    #[test]
    fn deterministic_compilation_is_canonical_and_side_effect_free() {
        let first = compile_agent(AgentCompilerInput {
            plan_bytes: None,
            manifest_bytes: deterministic_yaml(),
            input_schema_bytes: SCHEMA.as_bytes().to_vec(),
            output_schema_bytes: SCHEMA.as_bytes().to_vec(),
            profile: profile(),
            bindings: ResolvedAgentBindings::default(),
        })
        .unwrap();
        let json_manifest = json!({
            "apiVersion": AGENT_MANIFEST_API_VERSION,
            "kind": AGENT_MANIFEST_KIND,
            "metadata": {"name": "echo-agent"},
            "spec": {
                "execution": {"kind": "deterministic"},
                "input": {"schema": "schemas/io.json", "classification": "internal"},
                "output": {"schema": "schemas/io.json"}
            }
        });
        let second = compile_agent(AgentCompilerInput {
            plan_bytes: None,
            manifest_bytes: serde_json::to_vec_pretty(&json_manifest).unwrap(),
            input_schema_bytes: SCHEMA.as_bytes().to_vec(),
            output_schema_bytes: SCHEMA.as_bytes().to_vec(),
            profile: profile(),
            bindings: ResolvedAgentBindings::default(),
        })
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.name, "echo-agent");
        assert_eq!(first.resource_intent.display_name, "Echo Agent");
        assert!(first.required_features.is_empty());
        assert_eq!(
            first.resource_intent.input_classification,
            DataClassification::Internal
        );
        assert_eq!(first.resource_intent.default_deadline_seconds, 120);
        let plan: Value = serde_json::from_slice(&first.typed_plan_bytes).unwrap();
        assert_eq!(plan["plan_version"], 6);
        assert_eq!(plan["nodes"]["finish"]["value"]["source"], "run_input");
        assert_eq!(first.lifecycle_plan.steps.len(), 9);
    }

    #[test]
    fn model_chat_compiles_exact_requirement_and_author_instruction() {
        let compiled = compile_agent(AgentCompilerInput {
            plan_bytes: None,
            manifest_bytes: model_yaml(),
            input_schema_bytes: SCHEMA.as_bytes().to_vec(),
            output_schema_bytes: SCHEMA.as_bytes().to_vec(),
            profile: profile(),
            bindings: model_binding(),
        })
        .unwrap();
        assert_eq!(
            compiled.required_features,
            vec![RequiredAgentFeature::Model]
        );
        assert_eq!(
            compiled.resource_intent.author_instructions.as_deref(),
            Some("Answer using only the current user input.\n")
        );
        assert_eq!(
            compiled.resource_intent.input_classification,
            DataClassification::Internal
        );
        assert_eq!(compiled.resource_intent.default_deadline_seconds, 120);
        assert_eq!(compiled.deployment_intent.slots.len(), 1);
        let plan: Value = serde_json::from_slice(&compiled.typed_plan_bytes).unwrap();
        assert_eq!(plan["nodes"]["model"]["kind"], "model_loop");
        assert_eq!(
            plan["dependency_slots"][PRIMARY_MODEL_SLOT_ID]["requirement_digest"],
            serde_json::to_value(&compiled.deployment_intent.slots[0].requirement_digest).unwrap()
        );
        assert_eq!(
            plan["interface_contract_digest"],
            serde_json::to_value(&compiled.resource_intent.contract_digest).unwrap()
        );
    }

    #[test]
    fn deterministic_rejects_a_schema_transform_it_cannot_execute() {
        let other = SCHEMA.replace("message", "answer");
        let error = compile_agent(AgentCompilerInput {
            plan_bytes: None,
            manifest_bytes: deterministic_yaml(),
            input_schema_bytes: SCHEMA.as_bytes().to_vec(),
            output_schema_bytes: other.into_bytes(),
            profile: profile(),
            bindings: ResolvedAgentBindings::default(),
        })
        .unwrap_err();
        assert_eq!(error.code(), AgentCompilerErrorCode::AgentCompileFailed);
    }

    #[test]
    fn dangerous_yaml_and_closed_shape_fail_before_materialization() {
        for source in [
            String::from_utf8(deterministic_yaml())
                .unwrap()
                .replace("kind: Agent", "kind: Agent\nkind: Agent"),
            String::from_utf8(deterministic_yaml()).unwrap().replace(
                "metadata:\n",
                "defaults: &defaults {name: echo-agent}\nmetadata:\n  <<: *defaults\n",
            ),
            String::from_utf8(deterministic_yaml())
                .unwrap()
                .replace("name: echo-agent", "name: !tenant echo-agent"),
            String::from_utf8(deterministic_yaml())
                .unwrap()
                .replace("name: echo-agent", "name: 2026-08-31"),
            String::from_utf8(deterministic_yaml())
                .unwrap()
                .replace("classification: internal", "classification: .NaN"),
            String::from_utf8(deterministic_yaml())
                .unwrap()
                .replace("  output:", "  unknown: true\n  output:"),
        ] {
            let error = compile_agent(AgentCompilerInput {
                plan_bytes: None,
                manifest_bytes: source.into_bytes(),
                input_schema_bytes: SCHEMA.as_bytes().to_vec(),
                output_schema_bytes: SCHEMA.as_bytes().to_vec(),
                profile: profile(),
                bindings: ResolvedAgentBindings::default(),
            })
            .unwrap_err();
            assert_eq!(error.code(), AgentCompilerErrorCode::AgentManifestInvalid);
        }
    }

    #[test]
    fn model_chat_requires_exact_matching_binding_and_safe_instructions() {
        let mut missing = model_binding();
        missing.model = None;
        let error = compile_agent(AgentCompilerInput {
            plan_bytes: None,
            manifest_bytes: model_yaml(),
            input_schema_bytes: SCHEMA.as_bytes().to_vec(),
            output_schema_bytes: SCHEMA.as_bytes().to_vec(),
            profile: profile(),
            bindings: missing,
        })
        .unwrap_err();
        assert_eq!(error.code(), AgentCompilerErrorCode::AgentReferenceMissing);

        let unsafe_manifest = String::from_utf8(model_yaml()).unwrap().replace(
            "Answer using only the current user input.",
            "Use https://example.test",
        );
        let error = compile_agent(AgentCompilerInput {
            plan_bytes: None,
            manifest_bytes: unsafe_manifest.into_bytes(),
            input_schema_bytes: SCHEMA.as_bytes().to_vec(),
            output_schema_bytes: SCHEMA.as_bytes().to_vec(),
            profile: profile(),
            bindings: model_binding(),
        })
        .unwrap_err();
        assert_eq!(error.code(), AgentCompilerErrorCode::AgentManifestInvalid);
    }

    #[test]
    fn materializer_requires_ready_exact_artifact_authority() {
        let compiled = compile_agent(AgentCompilerInput {
            plan_bytes: None,
            manifest_bytes: deterministic_yaml(),
            input_schema_bytes: SCHEMA.as_bytes().to_vec(),
            output_schema_bytes: SCHEMA.as_bytes().to_vec(),
            profile: profile(),
            bindings: ResolvedAgentBindings::default(),
        })
        .unwrap();
        let authority = |intent: &ArtifactIntent, suffix| ArtifactAuthority {
            purpose: intent.purpose,
            state: ArtifactState::Ready,
            artifact: ArtifactRef::new(
                id(ResourceKind::Artifact, suffix),
                intent.content_digest.clone(),
                intent.byte_length,
                intent.media_type.clone(),
                intent.classification,
                intent.display_name.clone(),
            )
            .unwrap(),
        };
        let authoring = authority(&compiled.resource_intent.authoring_artifact, 60);
        let plan = authority(&compiled.resource_intent.typed_plan_artifact, 61);
        let document = compiled
            .resource_intent
            .materialize(&authoring, &plan)
            .unwrap();
        let ResourceDocument::Agent(agent) = document else {
            unreachable!()
        };
        assert_eq!(agent.typed_plan_digest, compiled.typed_plan_digest);

        let mut not_ready = plan;
        not_ready.state = ArtifactState::Verified;
        assert_eq!(
            compiled
                .resource_intent
                .materialize(&authoring, &not_ready)
                .unwrap_err()
                .code(),
            AgentCompilerErrorCode::AgentBindingNotReady
        );
    }
    fn source_bundle() -> AgentSourceBundleV1 {
        let profile = profile();
        AgentSourceBundleV1 {
            schema_version: AGENT_SOURCE_BUNDLE_VERSION,
            compiler_semantic_identity: compiler_semantic_identity(),
            compile_policy_inputs_digest: AgentSourceBundleV1::policy_digest(&profile),
            sources: AgentSourceFilesV1 {
                manifest_path: "agent.yaml".to_owned(),
                files: BTreeMap::from([
                    (
                        "agent.yaml".to_owned(),
                        String::from_utf8(deterministic_yaml()).unwrap(),
                    ),
                    ("schemas/io.json".to_owned(), SCHEMA.to_owned()),
                ]),
            },
            profile,
            bindings: ResolvedAgentBindings::default(),
        }
    }

    #[test]
    fn diagnostic_source_locations_come_from_actual_parser_tokens() {
        let mut bundle = source_bundle();
        bundle.sources.files.insert("schemas/io.json".into(), "{\n  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n  \"type\": \"object\",\n  \"additionalProperties\": false,\n  \"properties\": {},\n  \"required\": [],\n  \"unknownKeyword\": true\n}".into());
        let AgentCompileResponseV1::Rejected { diagnostics } = compile_source_bundle(bundle) else {
            panic!("invalid schema accepted")
        };
        let location = diagnostics[0]
            .location
            .as_ref()
            .expect("actual invalid schema token");
        assert_eq!(location.file, "schemas/io.json");
        assert_eq!(location.source_pointer, "/unknownKeyword");
        assert_eq!((location.line, location.column), (7, 21));
        assert!(!diagnostics[0].safe_detail.contains("unknownKeyword"));
    }

    #[test]
    fn persisted_source_bundle_recompiles_and_binds_every_input() {
        let bundle = source_bundle();
        let native = compile_source_bundle(bundle.clone());
        let encoded = serde_json::to_vec(&bundle).unwrap();
        let wire: AgentCompileResponseV1 =
            serde_json::from_slice(&compile_request_bytes(&encoded)).unwrap();
        assert_eq!(native, wire);
        let AgentCompileResponseV1::Compiled { compilation } = wire else {
            panic!("valid bundle rejected")
        };
        assert_eq!(
            compilation.source_bundle_digest,
            compilation
                .compiled
                .resource_intent
                .authoring_artifact
                .content_digest
        );
        assert_eq!(
            compilation.source_bundle_digest,
            digest_bytes(&compilation.source_bundle_bytes).unwrap()
        );
        let recovered: AgentSourceBundleV1 =
            serde_json::from_slice(&compilation.source_bundle_bytes).unwrap();
        assert_eq!(compile_source_bundle(recovered), native);
        assert_ne!(
            compilation.source_bundle_digest,
            compilation.compiled.manifest_digest
        );
        let source_map: AgentSourceMapV1 =
            serde_json::from_slice(&compilation.source_map_bytes).unwrap();
        assert_eq!(
            source_map.canonical_bytes().unwrap(),
            compilation.source_map_bytes
        );
        assert_eq!(
            digest_bytes(&compilation.source_map_bytes).unwrap(),
            compilation.source_map_digest
        );
        source_map
            .validate_for(&bundle, &compilation.compiled)
            .unwrap();
        assert!(source_map.entries.iter().all(|entry| entry.source.line > 1));
        let mut forged = source_map.clone();
        forged.entries[0].source.column += 1;
        assert!(forged.validate_for(&bundle, &compilation.compiled).is_err());
        let mut forged = source_map.clone();
        forged.entries[0].source.file = "/private/source.yaml".into();
        assert!(forged.canonical_bytes().is_err());
        let mut forged = source_map.clone();
        forged.entries.push(forged.entries[0].clone());
        assert!(forged.canonical_bytes().is_err());
        let mut oversized = (*compilation).clone();
        // The map byte vector is JSON-encoded as decimal bytes. Its aggregate transport
        // limit must include that expansion together with the IR and source bundle.
        oversized.source_map_bytes = vec![255; MAX_AGENT_SOURCE_MAP_BYTES];
        let bounded = crate::boundary::bounded_response(AgentCompileResponseV1::Compiled {
            compilation: Box::new(oversized),
        });
        assert!(
            matches!(bounded, AgentCompileResponseV1::Rejected { diagnostics } if diagnostics[0].code == AgentBoundaryErrorCode::CompilerLimitExceeded)
        );
        let mut changed = bundle;
        changed.profile.default_deadline_seconds += 1;
        let AgentCompileResponseV1::Rejected { diagnostics } = compile_source_bundle(changed)
        else {
            panic!("unfrozen policy accepted")
        };
        assert_eq!(
            diagnostics[0].code,
            AgentBoundaryErrorCode::CompilePolicyMismatch
        );
    }

    #[test]
    fn source_bundle_rejects_duplicate_unknown_unreferenced_and_oversized_inputs() {
        let bundle = source_bundle();
        let valid = String::from_utf8(serde_json::to_vec(&bundle).unwrap()).unwrap();
        let duplicate = valid.replacen(
            "\"schema_version\":1",
            "\"schema_version\":1,\"schema_version\":1",
            1,
        );
        let unknown = valid.replacen('{', "{\"unregistered\":true,", 1);
        for input in [duplicate.as_bytes(), unknown.as_bytes()] {
            let response: AgentCompileResponseV1 =
                serde_json::from_slice(&compile_request_bytes(input)).unwrap();
            assert!(matches!(response, AgentCompileResponseV1::Rejected { .. }));
        }
        let mut extra = bundle;
        extra
            .sources
            .files
            .insert("credentials.json".to_owned(), "{}".to_owned());
        assert!(matches!(
            compile_source_bundle(extra),
            AgentCompileResponseV1::Rejected { .. }
        ));
        let oversized = vec![b' '; MAX_AGENT_COMPILER_REQUEST_BYTES + 1];
        let response: AgentCompileResponseV1 =
            serde_json::from_slice(&compile_request_bytes(&oversized)).unwrap();
        let AgentCompileResponseV1::Rejected { diagnostics } = response else {
            panic!("oversized request accepted")
        };
        assert_eq!(
            diagnostics[0].code,
            AgentBoundaryErrorCode::CompilerLimitExceeded
        );
    }

    #[test]
    fn full_plan_compiles_human_timer_and_signal_with_schema_closure() {
        let AgentCompileResponseV1::Compiled {
            compilation: template,
        } = compile_source_bundle(source_bundle())
        else {
            panic!("template rejected")
        };
        let mut plan: RuntimePlan =
            serde_json::from_slice(&template.compiled.typed_plan_bytes).unwrap();
        let key = |name: &str| PlanNodeKey::new(name.to_owned()).unwrap();
        let output = ExactDataPortRef::NodeOutput {
            producer_node_id: key("human"),
            port_id: DataPortKey::new("response".to_owned()).unwrap(),
            schema_digest: template
                .compiled
                .resource_intent
                .output_schema
                .canonical_digest
                .clone(),
        };
        plan.nodes = BTreeMap::from([
            (key("start"), RuntimeNode::Start { next: key("timer") }),
            (
                key("timer"),
                RuntimeNode::TimerWait {
                    delay_milliseconds: 1000,
                    resume: key("signal"),
                },
            ),
            (
                key("signal"),
                RuntimeNode::SignalWait {
                    signal_key: "ready".to_owned(),
                    payload: None,
                    timeout_milliseconds: 5000,
                    resume: key("human"),
                },
            ),
            (
                key("human"),
                RuntimeNode::HumanTask {
                    definition: insight_platform_plan::HumanTaskDefinition::HumanWork {
                        eligibility_rule: Some(
                            insight_platform_contracts::TaskEligibilityRule::AnyAuthorized,
                        ),
                        eligible_principal_rule_digest:
                            insight_platform_contracts::TaskEligibilityRule::AnyAuthorized
                                .canonical_digest()
                                .unwrap(),
                        safe_prompt_key: "review_input".to_owned(),
                    },
                    response: output.clone(),
                    timeout_milliseconds: 10000,
                    resume: key("finish"),
                },
            ),
            (key("finish"), RuntimeNode::Return { value: output }),
        ]);
        let mut source = source_bundle();
        source
            .sources
            .files
            .get_mut("agent.yaml")
            .unwrap()
            .replace_range(
                ..,
                &String::from_utf8(deterministic_yaml()).unwrap().replace(
                    "kind: deterministic",
                    "kind: full_plan\n    plan: plan.json",
                ),
            );
        source.sources.files.insert(
            "plan.json".to_owned(),
            String::from_utf8(canonical_json(&serde_json::to_value(&plan).unwrap()).unwrap())
                .unwrap(),
        );
        let direct = compile_agent(AgentCompilerInput {
            manifest_bytes: source.sources.files["agent.yaml"].as_bytes().to_vec(),
            input_schema_bytes: SCHEMA.as_bytes().to_vec(),
            output_schema_bytes: SCHEMA.as_bytes().to_vec(),
            plan_bytes: Some(source.sources.files["plan.json"].as_bytes().to_vec()),
            profile: source.profile.clone(),
            bindings: source.bindings.clone(),
        });
        assert!(direct.is_ok(), "full Plan compile: {direct:?}");
        let AgentCompileResponseV1::Compiled { compilation } =
            compile_source_bundle(source.clone())
        else {
            panic!("full Plan rejected")
        };
        let actual: RuntimePlan =
            serde_json::from_slice(&compilation.compiled.typed_plan_bytes).unwrap();
        assert_eq!(actual, plan);
        assert_eq!(
            compilation.compiled.execution_kind,
            AgentExecutionKind::FullPlan
        );
        assert_ne!(
            compilation.source_bundle_digest,
            template.source_bundle_digest
        );
        let mut wrong = plan.clone();
        wrong.schema_documents.remove(
            &template
                .compiled
                .resource_intent
                .output_schema
                .canonical_digest,
        );
        // The compiler supplies interface schemas, while the standalone v6 IR requires its closure.
        assert!(wrong
            .validate(
                PlanLimits::from_profile(
                    &insight_platform_contracts::checked_in_hard_limit_profile()
                )
                .unwrap()
            )
            .is_err());
        wrong.plan_version = 5;
        source.sources.files.insert(
            "plan.json".to_owned(),
            serde_json::to_string(&wrong).unwrap(),
        );
        assert!(matches!(
            compile_source_bundle(source),
            AgentCompileResponseV1::Rejected { .. }
        ));
    }

    #[test]
    fn registry_evidence_requires_exact_source_plan_and_materialized_document() {
        let AgentCompileResponseV1::Compiled { compilation } =
            compile_source_bundle(source_bundle())
        else {
            panic!("template rejected")
        };
        let authority = |intent: &ArtifactIntent, suffix| ArtifactAuthority {
            purpose: intent.purpose,
            state: ArtifactState::Ready,
            artifact: ArtifactRef::new(
                id(ResourceKind::Artifact, suffix),
                intent.content_digest.clone(),
                intent.byte_length,
                intent.media_type.clone(),
                intent.classification,
                intent.display_name.clone(),
            )
            .unwrap(),
        };
        let source = authority(&compilation.compiled.resource_intent.authoring_artifact, 60);
        let plan = authority(
            &compilation.compiled.resource_intent.typed_plan_artifact,
            61,
        );
        let document = compilation.materialize(&source, &plan).unwrap();
        let evidence = validate_frozen_agent_artifacts(
            &document,
            &source,
            &compilation.source_bundle_bytes,
            &plan,
            &compilation.compiled.typed_plan_bytes,
        )
        .unwrap();
        evidence.validate().unwrap();
        assert!(matches!(
            evidence.program_requirement,
            insight_platform_contracts::ExecutionRequirement::Program {
                ir_abi_version: 6,
                ..
            }
        ));
        let mut forged = document.clone();
        let ResourceDocument::Agent(agent) = &mut forged else {
            unreachable!()
        };
        agent.default_deadline_seconds += 1;
        assert!(validate_frozen_agent_artifacts(
            &forged,
            &source,
            &compilation.source_bundle_bytes,
            &plan,
            &compilation.compiled.typed_plan_bytes
        )
        .is_err());
        let mut altered_bytes = compilation.compiled.typed_plan_bytes.clone();
        altered_bytes.push(b' ');
        assert!(validate_frozen_agent_artifacts(
            &document,
            &source,
            &compilation.source_bundle_bytes,
            &plan,
            &altered_bytes
        )
        .is_err());
        let mut noncanonical = compilation.source_bundle_bytes.clone();
        noncanonical.push(b' ');
        assert!(inspect_frozen_source_bundle(&noncanonical).is_err());
    }

    #[test]
    fn source_schema_changes_invalidate_publication_even_with_same_manifest() {
        let bundle = source_bundle();
        let AgentCompileResponseV1::Compiled { compilation: first } =
            compile_source_bundle(bundle.clone())
        else {
            panic!("template rejected")
        };
        let mut modified = bundle;
        *modified.sources.files.get_mut("schemas/io.json").unwrap() =
            SCHEMA.replace("\"minLength\":1", "\"minLength\":2");
        let AgentCompileResponseV1::Compiled {
            compilation: second,
        } = compile_source_bundle(modified)
        else {
            panic!("modified schema rejected")
        };
        assert_eq!(
            first.compiled.manifest_digest,
            second.compiled.manifest_digest
        );
        assert_ne!(first.source_bundle_digest, second.source_bundle_digest);
        assert_ne!(
            first.compiled.typed_plan_digest,
            second.compiled.typed_plan_digest
        );
    }

    #[test]
    fn convenience_and_inspection_use_the_same_closed_transport() {
        let bundle = source_bundle();
        let request = AgentAuthoringRequestV1 {
            schema_version: 1,
            sources: bundle.sources.clone(),
            profile: bundle.profile.clone(),
            bindings: bundle.bindings.clone(),
        };
        let response: AgentCompileResponseV1 = serde_json::from_slice(
            &compile_authoring_request_bytes(&serde_json::to_vec(&request).unwrap()),
        )
        .unwrap();
        assert_eq!(response, compile_source_bundle(bundle.clone()));
        let inspect = AgentManifestInspectionRequestV1 {
            schema_version: 1,
            manifest: bundle.sources.files["agent.yaml"].clone(),
        };
        let valid = serde_json::to_vec(&inspect).unwrap();
        let inspected: AgentManifestInspectionResponseV1 =
            serde_json::from_slice(&inspect_manifest_request_bytes(&valid)).unwrap();
        assert!(matches!(
            inspected,
            AgentManifestInspectionResponseV1::Inspected { .. }
        ));
        let mut unknown = serde_json::to_value(inspect).unwrap();
        unknown["filesystem_root"] = json!("/");
        let rejected: AgentManifestInspectionResponseV1 = serde_json::from_slice(
            &inspect_manifest_request_bytes(&serde_json::to_vec(&unknown).unwrap()),
        )
        .unwrap();
        assert!(matches!(
            rejected,
            AgentManifestInspectionResponseV1::Rejected { .. }
        ));
    }

    #[test]
    fn framework_export_keeps_source_and_recompiles_every_typed_agent_effect() {
        use framework::*;
        let AgentCompileResponseV1::Compiled {
            compilation: template,
        } = compile_source_bundle(source_bundle())
        else {
            panic!("template")
        };
        let template_plan: RuntimePlan =
            serde_json::from_slice(&template.compiled.typed_plan_bytes).unwrap();
        let schema = template.compiled.resource_intent.output_schema.clone();
        let key = |name: &str| PlanNodeKey::new(name.to_owned()).unwrap();
        let port = |name: &str| ExactDataPortRef::NodeOutput {
            producer_node_id: key(name),
            port_id: DataPortKey::new("out".to_owned()).unwrap(),
            schema_digest: schema.canonical_digest.clone(),
        };
        let input = ExactDataPortRef::RunInput {
            schema_digest: schema.canonical_digest.clone(),
        };
        let mut slots = BTreeMap::new();
        let mut bindings = Vec::new();
        for (i, (name, kind, target_kind)) in [
            (
                "model",
                RuntimeDependencyKind::Model,
                ResourceKind::ModelDeployment,
            ),
            (
                "capability",
                RuntimeDependencyKind::Capability,
                ResourceKind::CapabilityDeployment,
            ),
            (
                "context",
                RuntimeDependencyKind::Context,
                ResourceKind::ContextDeployment,
            ),
            (
                "child",
                RuntimeDependencyKind::ChildAgent,
                ResourceKind::AgentDeployment,
            ),
            (
                "skill",
                RuntimeDependencyKind::Skill,
                ResourceKind::SkillDeployment,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let requirement = digest(char::from(b'a' + i as u8));
            let exact =
                ExactDeploymentRef::new(id(target_kind, 100 + i as u16), digest('f')).unwrap();
            let selection_policy = policy_binding(200 + i as u16 * 2);
            let target=match kind {RuntimeDependencyKind::Model=>AgentSlotTargetInputV1::Model{candidates:vec![exact],selection_policy},RuntimeDependencyKind::Capability=>AgentSlotTargetInputV1::Capability{candidates:vec![exact],selection_policy,tool_alias:Some("lookup".into())},RuntimeDependencyKind::Context=>AgentSlotTargetInputV1::Context{binding:Box::new(insight_platform_contracts::ContextBindingInputV1{context_deployment:exact,consistency:insight_platform_contracts::ContextConsistencyPolicy::ExternalObservation,allowed_projection:vec!["message".into()],authorization_policy:policy_binding(300).revision,ranking_policy:policy_binding(302).revision})},RuntimeDependencyKind::ChildAgent=>AgentSlotTargetInputV1::ChildAgent{candidates:vec![exact],selection_policy},RuntimeDependencyKind::Skill=>AgentSlotTargetInputV1::Skill{candidates:vec![exact],selection_policy}};
            slots.insert(
                name.to_owned(),
                RuntimeDependencySlot {
                    kind,
                    requirement_digest: requirement.clone(),
                },
            );
            bindings.push(AgentSlotBindingInputV1 {
                slot_id: name.into(),
                requirement_digest: requirement,
                target,
            });
        }
        let graph = StaticFrameworkExportV1 {
            schema_version: 1,
            dialect: FrameworkExportDialect::LangGraphStaticTypedPortsV1,
            adapter_semantic_identity: framework_adapter_semantic_identity(),
            entry_node_id: key("start"),
            dependency_slots: slots,
            schema_documents: template_plan.schema_documents,
            nodes: BTreeMap::from([
                (key("start"), RuntimeNode::Start { next: key("model") }),
                (
                    key("model"),
                    RuntimeNode::ModelLoop {
                        model_slot_id: "model".into(),
                        skill_slot_ids: vec!["skill".into()],
                        capability_slot_ids: vec!["capability".into()],
                        input,
                        model_route: None,
                        output: port("model"),
                        maximum_rounds: 2,
                        maximum_capability_calls: 2,
                        maximum_parallel_calls_per_round: 1,
                        token_budget: 2048,
                        resume: key("capability"),
                    },
                ),
                (
                    key("capability"),
                    RuntimeNode::CapabilityCall {
                        capability_slot_id: "capability".into(),
                        input: port("model"),
                        candidate_route: None,
                        output: port("capability"),
                        attempt_limit: 2,
                        retry_backoff_milliseconds: 100,
                        resume: key("context"),
                    },
                ),
                (
                    key("context"),
                    RuntimeNode::ContextQuery {
                        context_slot_id: "context".into(),
                        request: port("capability"),
                        result: port("context"),
                        maximum_items: 4,
                        resume: key("child"),
                    },
                ),
                (
                    key("child"),
                    RuntimeNode::ChildAgentCall {
                        child_agent_slot_id: "child".into(),
                        input: port("context"),
                        candidate_route: None,
                        output: port("child"),
                        budget: insight_platform_plan::ChildBudgetLimit {
                            maximum_duration_milliseconds: 30000,
                            maximum_model_tokens: 1024,
                            maximum_capability_calls: 2,
                            maximum_artifact_bytes: 65536,
                            maximum_descendant_runs: 1,
                        },
                        cancellation_policy:
                            insight_platform_plan::ChildCancellationPolicy::CascadeAndWait,
                        attempt_limit: 1,
                        retry_backoff_milliseconds: 100,
                        resume: key("finish"),
                    },
                ),
                (
                    key("finish"),
                    RuntimeNode::Return {
                        value: port("child"),
                    },
                ),
            ]),
        };
        // Explicit synthetic deployment evidence for this pure compiler corpus.
        let mut fixture_bindings = ResolvedAgentBindings {
            model: None,
            slots: bindings.clone(),
            deployment_features: Vec::new(),
        };
        fixture_bindings.deployment_features = feature_deployments(&fixture_bindings)
            .unwrap()
            .into_iter()
            .map(
                |deployment| insight_platform_contracts::AgentDeploymentFeaturesV1 {
                    schema_version: 1,
                    deployment: deployment.clone(),
                    interface_contract_digest: schema.canonical_digest.clone(),
                    required_features: match deployment.resource_kind {
                        ResourceKind::CapabilityDeployment => vec![
                            RequiredAgentFeature::RemoteCapability,
                            RequiredAgentFeature::Mcp,
                        ],
                        ResourceKind::ContextDeployment => {
                            vec![RequiredAgentFeature::Model, RequiredAgentFeature::Context]
                        }
                        ResourceKind::AgentDeployment => vec![RequiredAgentFeature::Sandbox],
                        _ => unreachable!(),
                    },
                },
            )
            .collect();
        let request = FrameworkImportRequestV1 {
            schema_version: 1,
            name: "typed-export".into(),
            display_name: "Typed export".into(),
            input_schema: schema.clone(),
            output_schema: schema,
            input_classification: DataClassification::Internal,
            profile: profile(),
            bindings: fixture_bindings,
            graph: graph.clone(),
        };
        let mut absent = request.clone();
        absent.bindings.deployment_features.clear();
        assert!(
            import_framework(absent).is_err(),
            "ambiguous exact candidates require evidence"
        );
        let mut extra = request.clone();
        extra
            .bindings
            .deployment_features
            .push(extra.bindings.deployment_features[0].clone());
        assert!(
            import_framework(extra).is_err(),
            "extra or duplicate evidence is not ignored"
        );
        let mut reordered = request.clone();
        reordered.bindings.deployment_features.reverse();
        assert!(
            import_framework(reordered).is_err(),
            "source evidence has one canonical target order"
        );
        let imported =
            import_framework(request).expect("typed graph imports through the actual compiler");
        assert_eq!(
            imported.recovery_granularity,
            IntegrationRecoveryGranularity::PlatformNodes
        );
        assert!(imported.source_bundle.sources.files["framework.json"]
            .contains("adapter_semantic_identity"));
        assert!(!serde_json::to_string(&imported.source_bundle.bindings)
            .unwrap()
            .contains("binding_id"));
        let AgentCompileResponseV1::Compiled { compilation } =
            compile_source_bundle(imported.source_bundle.clone())
        else {
            panic!("imported graph")
        };
        assert_eq!(
            compilation.compiled.required_features,
            vec![
                RequiredAgentFeature::Model,
                RequiredAgentFeature::Context,
                RequiredAgentFeature::RemoteCapability,
                RequiredAgentFeature::Mcp,
                RequiredAgentFeature::Sandbox
            ]
        );
        let actual: RuntimePlan =
            serde_json::from_slice(&compilation.compiled.typed_plan_bytes).unwrap();
        assert_eq!(actual.nodes, graph.nodes);
        assert_eq!(actual.plan_version, 6);
        assert_eq!(
            compilation.compiled.execution_kind,
            AgentExecutionKind::FrameworkGraph
        );
        let mut bad = imported.source_bundle;
        let mut value = serde_json::to_value(graph).unwrap();
        value["reducers"] = json!({"state":"arbitrary_host_closure"});
        bad.sources.files.insert(
            "framework.json".into(),
            serde_json::to_string(&value).unwrap(),
        );
        assert!(matches!(
            compile_source_bundle(bad),
            AgentCompileResponseV1::Rejected { .. }
        ));
    }

    #[test]
    fn full_plan_boolean_port_uses_real_internal_schema_and_shared_compilation() {
        use insight_platform_plan::{
            BranchArm, ExpressionLimits, PortAssignment, TypedExpressionProgram, TypedInstruction,
        };
        let mut source = source_bundle();
        let AgentCompileResponseV1::Compiled { compilation } =
            compile_source_bundle(source.clone())
        else {
            panic!("template")
        };
        let mut plan: RuntimePlan =
            serde_json::from_slice(&compilation.compiled.typed_plan_bytes).unwrap();
        let boolean = insight_platform_contracts::ClosedValueSchema::build(
            json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"boolean"}),
        )
        .unwrap();
        assert!(ClosedJsonSchema::try_from(boolean.clone()).is_err());
        let key = |name: &str| PlanNodeKey::new(name.into()).unwrap();
        let port = ExactDataPortRef::NodeOutput {
            producer_node_id: key("compute"),
            port_id: DataPortKey::new("decision".into()).unwrap(),
            schema_digest: boolean.canonical_digest.clone(),
        };
        let literal = insight_platform_contracts::ClosedJsonValue::build(
            boolean.canonical_digest.clone(),
            json!(true),
        )
        .unwrap();
        let expression = TypedExpressionProgram::build(
            vec![],
            vec![TypedInstruction::Literal { value: literal }],
            boolean.canonical_digest.clone(),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        let condition = TypedExpressionProgram::build(
            vec![port.clone()],
            vec![TypedInstruction::LoadPort { port: port.clone() }],
            boolean.canonical_digest.clone(),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        plan.schema_documents
            .insert(boolean.canonical_digest.clone(), boolean);
        plan.nodes.insert(
            key("start"),
            RuntimeNode::Start {
                next: key("compute"),
            },
        );
        plan.nodes.insert(
            key("compute"),
            RuntimeNode::Compute {
                assignments: vec![PortAssignment {
                    output_port: port,
                    expression,
                }],
                next: key("branch"),
            },
        );
        plan.nodes.insert(
            key("branch"),
            RuntimeNode::Branch {
                ordered_arms: vec![BranchArm {
                    when: condition,
                    target: key("finish"),
                }],
                otherwise: key("other"),
            },
        );
        plan.nodes
            .insert(key("other"), plan.nodes[&key("finish")].clone());
        source.sources.files.insert(
            "agent.yaml".into(),
            String::from_utf8(deterministic_yaml()).unwrap().replace(
                "kind: deterministic",
                "kind: full_plan\n    plan: plan.json",
            ),
        );
        source
            .sources
            .files
            .insert("plan.json".into(), serde_json::to_string(&plan).unwrap());
        let response = compile_source_bundle(source.clone());
        assert!(
            matches!(response, AgentCompileResponseV1::Compiled { .. }),
            "{response:?}"
        );
        let wire: AgentCompileResponseV1 = serde_json::from_slice(&compile_request_bytes(
            &serde_json::to_vec(&source).unwrap(),
        ))
        .unwrap();
        assert_eq!(wire, response);
        let AgentCompileResponseV1::Compiled { compilation } = response else {
            unreachable!()
        };
        let map: AgentSourceMapV1 = serde_json::from_slice(&compilation.source_map_bytes).unwrap();
        map.validate_for(&source, &compilation.compiled).unwrap();
        assert!(map
            .entries
            .iter()
            .any(|entry| matches!(entry.target, AgentSourceTargetV1::Expression { .. })));
        assert!(map
            .entries
            .iter()
            .any(|entry| matches!(entry.target, AgentSourceTargetV1::Port { .. })));
        assert!(map
            .entries
            .iter()
            .all(|entry| entry.source.file == "plan.json"));
        let plan_text = &source.sources.files["plan.json"];
        for entry in &map.entries {
            let actual: Value = serde_json::from_str(plan_text).unwrap();
            assert!(actual.pointer(&entry.source.source_pointer).is_some());
            assert_eq!(entry.source.line, 1);
            assert!(entry.source.column > 1);
        }
        let mut wrong_node = map.clone();
        wrong_node.entries[0].target = AgentSourceTargetV1::Node {
            node_id: key("other"),
            ir_pointer: "/nodes/missing".into(),
        };
        assert!(wrong_node
            .validate_for(&source, &compilation.compiled)
            .is_err());
    }

    #[test]
    fn full_plan_preserves_bounded_map_loop_and_typed_error_regions() {
        use insight_platform_contracts::{
            ClosedJsonValue, ClosedValueSchema, Failure, FailureClass, FailureCode, FailureSource,
            PlatformFailureCode, Retryability,
        };
        use insight_platform_plan::{
            ExpressionLimits, LoopCarriedPort, MapFailurePolicy, PortAssignment,
            TypedExpressionProgram, TypedInstruction,
        };
        let mut source = source_bundle();
        let AgentCompileResponseV1::Compiled { compilation } =
            compile_source_bundle(source.clone())
        else {
            panic!("current template rejected")
        };
        let mut plan: RuntimePlan =
            serde_json::from_slice(&compilation.compiled.typed_plan_bytes).unwrap();
        let object =
            ClosedValueSchema::try_from(compilation.compiled.resource_intent.input_schema.clone())
                .unwrap();
        let failure =
            ClosedValueSchema::try_from(compilation.compiled.resource_intent.error_schema.clone())
                .unwrap();
        let boolean = ClosedValueSchema::build(
            json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"boolean"}),
        )
        .unwrap();
        let array = ClosedValueSchema::build(json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array","items":object.schema,"minItems":0,"maxItems":4})).unwrap();
        for schema in [&boolean, &array] {
            plan.schema_documents
                .insert(schema.canonical_digest.clone(), schema.clone());
        }
        let key = |name: &str| PlanNodeKey::new(name.into()).unwrap();
        let port =
            |node: &str, name: &str, schema: &ClosedValueSchema| ExactDataPortRef::NodeOutput {
                producer_node_id: key(node),
                port_id: DataPortKey::new(name.into()).unwrap(),
                schema_digest: schema.canonical_digest.clone(),
            };
        let literal = |schema: &ClosedValueSchema, value: Value| {
            TypedExpressionProgram::build(
                vec![],
                vec![TypedInstruction::Literal {
                    value: ClosedJsonValue::build(schema.canonical_digest.clone(), value).unwrap(),
                }],
                schema.canonical_digest.clone(),
                ExpressionLimits::ABSOLUTE,
            )
            .unwrap()
        };
        let carried = LoopCarriedPort {
            body_output_port: port("loop_body", "out", &object),
            next_iteration_port: port("loop", "carried", &object),
        };
        let failure_value = serde_json::to_value(Failure {
            code: FailureCode::Platform {
                code: PlatformFailureCode::PlanInvariantFailed,
            },
            class: FailureClass::Platform,
            retryability: Retryability::Never,
            safe_message: None,
            details_ref: None,
            source: FailureSource::Plan,
        })
        .unwrap();
        let failure_port = port("failure_value", "failure", &failure);
        plan.nodes
            .insert(key("start"), RuntimeNode::Start { next: key("loop") });
        plan.nodes.extend([
            (
                key("loop"),
                RuntimeNode::Loop {
                    condition: literal(&boolean, json!(false)),
                    carried_ports: vec![carried.clone()],
                    body: key("loop_body"),
                    exit: key("map"),
                    maximum_iterations: 2,
                },
            ),
            (
                key("loop_body"),
                RuntimeNode::Compute {
                    assignments: vec![PortAssignment {
                        output_port: carried.body_output_port,
                        expression: literal(&object, json!({"message":"carried"})),
                    }],
                    next: key("loop"),
                },
            ),
            (
                key("map"),
                RuntimeNode::Map {
                    items: literal(&array, json!([{"message":"item"}])),
                    item_port: port("map", "item", &object),
                    body: key("map_body"),
                    next: key("boundary"),
                    maximum_items: 4,
                    failure_policy: MapFailurePolicy::AllSettled,
                },
            ),
            (
                key("map_body"),
                RuntimeNode::Compute {
                    assignments: vec![PortAssignment {
                        output_port: port("map_body", "out", &object),
                        expression: literal(&object, json!({"message":"mapped"})),
                    }],
                    next: key("boundary"),
                },
            ),
            (
                key("boundary"),
                RuntimeNode::ErrorBoundary {
                    body: key("failure_value"),
                    handlers: BTreeMap::from([("plan_invariant_failed".into(), key("finish"))]),
                },
            ),
            (
                key("failure_value"),
                RuntimeNode::Compute {
                    assignments: vec![PortAssignment {
                        output_port: failure_port.clone(),
                        expression: literal(&failure, failure_value),
                    }],
                    next: key("raise"),
                },
            ),
            (
                key("raise"),
                RuntimeNode::Raise {
                    failure: failure_port,
                },
            ),
        ]);
        source.sources.files.insert(
            "agent.yaml".into(),
            String::from_utf8(deterministic_yaml()).unwrap().replace(
                "kind: deterministic",
                "kind: full_plan\n    plan: plan.json",
            ),
        );
        let compile = |plan: &RuntimePlan| {
            let mut source = source.clone();
            source
                .sources
                .files
                .insert("plan.json".into(), serde_json::to_string(plan).unwrap());
            let response = compile_source_bundle(source.clone());
            let wire: AgentCompileResponseV1 = serde_json::from_slice(&compile_request_bytes(
                &serde_json::to_vec(&source).unwrap(),
            ))
            .unwrap();
            assert_eq!(
                wire, response,
                "native and bounded WASM transport use identical semantics"
            );
            response
        };
        let actual = compile(&plan);
        let AgentCompileResponseV1::Compiled {
            compilation: actual,
        } = actual
        else {
            panic!("bounded control-flow authoring rejected: {actual:?}")
        };
        let decoded: RuntimePlan =
            serde_json::from_slice(&actual.compiled.typed_plan_bytes).unwrap();
        assert_eq!(decoded.nodes, plan.nodes);
        for name in ["loop", "map", "boundary", "raise"] {
            assert!(String::from_utf8_lossy(&actual.source_map_bytes)
                .contains(&format!("\"node_id\":\"{name}\"")));
        }
        for field in [
            "map_budget",
            "loop_budget",
            "loop_scope",
            "handler_code",
            "failure_schema",
        ] {
            let mut invalid = plan.clone();
            match field {
                "map_budget" => {
                    if let RuntimeNode::Map { maximum_items, .. } =
                        invalid.nodes.get_mut(&key("map")).unwrap()
                    {
                        *maximum_items = 0
                    }
                }
                "loop_budget" => {
                    if let RuntimeNode::Loop {
                        maximum_iterations, ..
                    } = invalid.nodes.get_mut(&key("loop")).unwrap()
                    {
                        *maximum_iterations = 0
                    }
                }
                "loop_scope" => {
                    if let RuntimeNode::Loop { carried_ports, .. } =
                        invalid.nodes.get_mut(&key("loop")).unwrap()
                    {
                        carried_ports[0].body_output_port = port("map_body", "out", &object)
                    }
                }
                "handler_code" => {
                    if let RuntimeNode::ErrorBoundary { handlers, .. } =
                        invalid.nodes.get_mut(&key("boundary")).unwrap()
                    {
                        *handlers = BTreeMap::from([("Invalid Handler".into(), key("finish"))])
                    }
                }
                "failure_schema" => {
                    invalid.nodes.insert(
                        key("raise"),
                        RuntimeNode::Raise {
                            failure: ExactDataPortRef::RunInput {
                                schema_digest: object.canonical_digest.clone(),
                            },
                        },
                    );
                }
                _ => unreachable!(),
            }
            assert!(
                matches!(compile(&invalid), AgentCompileResponseV1::Rejected { .. }),
                "{field} must reject"
            );
        }
    }

    #[test]
    fn evaluation_compiles_actual_all_settled_children_with_stable_trial_identity() {
        use evaluation::*;
        let schema = ClosedJsonSchema::build(serde_json::from_str(SCHEMA).unwrap()).unwrap();
        let artifact = |suffix, digest| {
            ArtifactRef::new(
                id(ResourceKind::Artifact, suffix),
                digest,
                64,
                "application/json",
                DataClassification::Restricted,
                Some("evaluation.json".into()),
            )
            .unwrap()
        };
        let manifest = EvaluationManifestV1 {
            schema_version: 1,
            dataset_id: "samples".into(),
            samples: vec![EvaluationSampleV1 {
                sample_id: "one".into(),
                input: artifact(301, digest('b')),
                expected: Some(artifact(302, digest('c'))),
                input_schema_digest: schema.canonical_digest.clone(),
                expected_schema_digest: Some(schema.canonical_digest.clone()),
            }],
            repetitions: 2,
            subject: ExactDeploymentRef::new(id(ResourceKind::AgentDeployment, 303), digest('d'))
                .unwrap(),
            evaluator: ExactDeploymentRef::new(id(ResourceKind::AgentDeployment, 304), digest('e'))
                .unwrap(),
            metric_schema: schema.clone(),
        };
        let trials = manifest.trials().unwrap();
        assert_eq!(trials, manifest.trials().unwrap());
        assert_ne!(trials[0].trial_digest, trials[1].trial_digest);
        let manifest_artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, 305),
            manifest.canonical_digest().unwrap(),
            insight_platform_contracts::canonical_json(&serde_json::to_value(&manifest).unwrap())
                .unwrap()
                .len() as u64,
            "application/json",
            DataClassification::Restricted,
            None,
        )
        .unwrap();
        let mut request = EvaluationPlanRequestV1 {
            deployment_features: Vec::new(),
            schema_version: 1,
            name: "sample-evaluation".into(),
            display_name: "Sample evaluation".into(),
            manifest_artifact: manifest_artifact.clone(),
            manifest: manifest.clone(),
            subject_input_schema: schema.clone(),
            subject_output_schema: schema.clone(),
            expected_schema: Some(schema),
            subject_selection_policy: policy_binding(310),
            evaluator_selection_policy: policy_binding(312),
            child_budget: insight_platform_plan::ChildBudgetLimit {
                maximum_duration_milliseconds: 30000,
                maximum_model_tokens: 1024,
                maximum_capability_calls: 2,
                maximum_artifact_bytes: 65536,
                maximum_descendant_runs: 1,
            },
            profile: profile(),
        };
        // Explicit synthetic exact references for pure authoring/protocol fixtures, not Registry authority.
        let evaluator_schema = crate::evaluation::evaluation_evaluator_input_schema(
            &request.subject_input_schema,
            &request.subject_output_schema,
            request.expected_schema.as_ref(),
        )
        .unwrap();
        request.deployment_features = [
            (
                &request.manifest.subject,
                &request.subject_input_schema,
                &request.subject_output_schema,
            ),
            (
                &request.manifest.evaluator,
                &evaluator_schema,
                &request.manifest.metric_schema,
            ),
        ]
        .into_iter()
        .map(
            |(target, input, output)| insight_platform_contracts::AgentDeploymentFeaturesV1 {
                schema_version: 1,
                deployment: target.clone(),
                interface_contract_digest: crate::agent_interface_contract_digest(input, output)
                    .unwrap(),
                required_features: Vec::new(),
            },
        )
        .collect();
        request
            .deployment_features
            .sort_by(|a, b| a.deployment.deployment_id.cmp(&b.deployment.deployment_id));
        let output = compile_evaluation_plan(request.clone())
            .expect("evaluation is an actual ordinary Plan");
        let AgentCompileResponseV1::Compiled { compilation } =
            compile_source_bundle(output.source_bundle.clone())
        else {
            panic!("compiled evaluation source")
        };
        let plan: RuntimePlan =
            serde_json::from_slice(&compilation.compiled.typed_plan_bytes).unwrap();
        assert!(plan.nodes.values().any(|node| matches!(
            node,
            RuntimeNode::Join {
                policy: insight_platform_plan::JoinPolicy::AllSettled,
                ..
            }
        )));
        for trial in &trials {
            assert!(matches!(
                plan.nodes[&trial_subject_node(trial)],
                RuntimeNode::ChildAgentCall { .. }
            ));
            assert!(matches!(
                plan.nodes[&trial_evaluator_node(trial)],
                RuntimeNode::ChildAgentCall { .. }
            ));
        }
        assert_eq!(compile_evaluation_plan(request).unwrap(), output);
        let mut invalid = manifest.clone();
        invalid.samples.push(invalid.samples[0].clone());
        assert!(invalid.validate().is_err());
        let report = EvaluationReportV1 {
            schema_version: 1,
            manifest: manifest_artifact,
            parent_run_id: id(ResourceKind::Run, 320),
            trials: trials
                .into_iter()
                .map(|trial| EvaluationTrialResultV1 {
                    trial,
                    input: manifest.samples[0].input.clone(),
                    expected: manifest.samples[0].expected.clone(),
                    evidence: EvaluationTrialEvidenceV1::Missing {
                        reason: EvaluationMissingReason::NotStarted,
                    },
                })
                .collect(),
            scored_trials: 0,
            failed_trials: 0,
            missing_trials: 2,
        };
        report.validate_for(&manifest).unwrap();
        let mut failed = report.clone();
        failed.trials[0].evidence = EvaluationTrialEvidenceV1::Failed {
            stage: EvaluationFailureStage::Subject,
            failed_run_id: id(ResourceKind::Run, 321),
            subject_run_id: Some(id(ResourceKind::Run, 321)),
            terminal_state: insight_platform_contracts::RunState::TimedOut,
            terminal_version: 3,
            failure_value: None,
        };
        failed.failed_trials = 1;
        failed.missing_trials = 1;
        failed
            .validate_for(&manifest)
            .expect("known terminal failure needs no fabricated output");
        let mut wrong = failed.clone();
        if let EvaluationTrialEvidenceV1::Failed { terminal_state, .. } =
            &mut wrong.trials[0].evidence
        {
            *terminal_state = insight_platform_contracts::RunState::Succeeded;
        }
        assert!(wrong.validate_for(&manifest).is_err());
        let mut wrong = failed;
        if let EvaluationTrialEvidenceV1::Failed { failure_value, .. } =
            &mut wrong.trials[0].evidence
        {
            *failure_value = Some(Box::new(EvaluationRunValueEvidenceV1 {
                run_id: id(ResourceKind::Run, 322),
                value_id: id(ResourceKind::RunValue, 323),
                schema_digest: digest('a'),
                content_digest: digest('b'),
                artifact: None,
            }));
        }
        assert!(wrong.validate_for(&manifest).is_err());
        let mut lie = report;
        lie.scored_trials = 2;
        lie.missing_trials = 0;
        assert!(lie.validate_for(&manifest).is_err());
    }
    fn inspected_sources(model: bool) -> AgentSourceFilesV1 {
        let manifest = if model {
            model_yaml()
        } else {
            deterministic_yaml()
        };
        let mut files =
            BTreeMap::from([("agent.yaml".into(), String::from_utf8(manifest).unwrap())]);
        for path in if model {
            vec!["schemas/input.json", "schemas/output.json"]
        } else {
            vec!["schemas/io.json"]
        } {
            files.insert(path.into(), SCHEMA.into());
        }
        AgentSourceFilesV1 {
            manifest_path: "agent.yaml".into(),
            files,
        }
    }
    #[test]
    fn source_inspection_validates_sources_without_any_dependency_or_profile() {
        for model in [false, true] {
            let request = AgentSourceInspectionRequestV1 {
                schema_version: 1,
                sources: inspected_sources(model),
            };
            let response = inspect_agent_sources(request.clone());
            assert!(matches!(
                response,
                AgentManifestInspectionResponseV1::Inspected { .. }
            ));
            assert_eq!(
                serde_json::to_vec(&response).unwrap(),
                inspect_source_request_bytes(&serde_json::to_vec(&request).unwrap())
            );
        }
        let compiled = compile_agent(AgentCompilerInput {
            plan_bytes: None,
            manifest_bytes: deterministic_yaml(),
            input_schema_bytes: SCHEMA.as_bytes().to_vec(),
            output_schema_bytes: SCHEMA.as_bytes().to_vec(),
            profile: profile(),
            bindings: Default::default(),
        })
        .unwrap();
        let mut sources = inspected_sources(false);
        let mut manifest: Value =
            serde_json::from_slice(&compiled.canonical_manifest_bytes).unwrap();
        manifest["spec"]["execution"] = json!({"kind":"full_plan","plan":"graph.json"});
        sources.files.insert(
            "agent.yaml".into(),
            serde_json::to_string(&manifest).unwrap(),
        );
        sources.files.insert(
            "graph.json".into(),
            String::from_utf8(compiled.typed_plan_bytes).unwrap(),
        );
        assert!(inspect_source_files(&sources).is_ok());
        let mut plan: Value = serde_json::from_str(&sources.files["graph.json"]).unwrap();
        plan["entry_node_id"] = json!("missing-node");
        sources
            .files
            .insert("graph.json".into(), serde_json::to_string(&plan).unwrap());
        assert!(
            inspect_source_files(&sources).is_err(),
            "invalid local Plan graph must fail before exact resolution"
        );
    }
    #[test]
    fn source_inspection_rejects_local_schema_files_and_strict_boundary_errors() {
        for mutation in 0..6 {
            let mut sources = inspected_sources(true);
            match mutation {
                0 => {
                    sources.files.remove("schemas/output.json");
                }
                1 => {
                    sources.files.insert("unused.json".into(), "{}".into());
                }
                2 => {
                    sources.files.insert(
                        "schemas/input.json".into(),
                        "{\"type\":\"object\",\"type\":\"object\"}".into(),
                    );
                }
                3 => {
                    sources.files.insert(
                        "schemas/input.json".into(),
                        SCHEMA.replace("\"type\":\"string\"", "\"$ref\":\"#/$defs/missing\""),
                    );
                }
                4 => {
                    sources.files.insert(
                        "agent.yaml".into(),
                        String::from_utf8(model_yaml())
                            .unwrap()
                            .replace("kind: model_chat", "kind: unknown"),
                    );
                }
                _ => {
                    sources.files.insert("../escape".into(), "{}".into());
                }
            }
            assert!(
                matches!(
                    inspect_agent_sources(AgentSourceInspectionRequestV1 {
                        schema_version: 1,
                        sources
                    }),
                    AgentManifestInspectionResponseV1::Rejected { .. }
                ),
                "mutation {mutation}"
            );
        }
        let valid = serde_json::to_string(&AgentSourceInspectionRequestV1 {
            schema_version: 1,
            sources: inspected_sources(false),
        })
        .unwrap();
        for bytes in [
            valid
                .replacen(
                    "\"schema_version\":1",
                    "\"schema_version\":1,\"schema_version\":1",
                    1,
                )
                .into_bytes(),
            valid
                .replacen("\"schema_version\":1", "\"schema_version\":2", 1)
                .into_bytes(),
            valid
                .replacen(
                    "\"schema_version\":1",
                    "\"schema_version\":1,\"unexpected\":true",
                    1,
                )
                .into_bytes(),
            vec![b' '; MAX_AGENT_COMPILER_REQUEST_BYTES + 1],
        ] {
            let response: AgentManifestInspectionResponseV1 =
                serde_json::from_slice(&inspect_source_request_bytes(&bytes)).unwrap();
            assert!(matches!(
                response,
                AgentManifestInspectionResponseV1::Rejected { .. }
            ));
        }
    }
}
