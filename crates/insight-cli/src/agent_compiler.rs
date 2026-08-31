//! Deterministic compiler for the product Agent manifest.
//!
//! Compilation is deliberately split from project file loading. [`compile_agent`] accepts only
//! bounded bytes and exact, already-resolved bindings; it cannot use the network, database,
//! process environment, clock, random source, or an execution backend. [`load_project_sources`]
//! is the sole filesystem adapter and rejects links and paths outside the project root.

use insight_platform_contracts::{
    canonical_digest, canonical_json, parse_strict_json, pinned_nominal_reference,
    AgentRequiredFeature, AgentResourceSpec, ArtifactPurpose, ArtifactRef, ArtifactState,
    AuthoringPackage, ClosedJsonSchema, DataClassification, ExactDeploymentRef, ExactPolicyBinding,
    ExactVersionRef, JsonLimits, PlanNodeKind, ResourceDocument, ResourceKind, Sha256Digest,
    MAX_AGENT_AUTHOR_INSTRUCTION_BYTES, MAX_CLOSED_SCHEMA_BYTES,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    error::Error,
    fmt, fs,
    path::{Component, Path, PathBuf},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

    fn reference(detail: impl Into<String>) -> Self {
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
    pub model: Option<ResolvedModelBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCompilerInput {
    pub manifest_bytes: Vec<u8>,
    pub input_schema_bytes: Vec<u8>,
    pub output_schema_bytes: Vec<u8>,
    pub profile: AgentCompilerProfile,
    pub bindings: ResolvedAgentBindings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedAgentProject {
    pub manifest_path: PathBuf,
    pub manifest_bytes: Vec<u8>,
    pub input_schema_bytes: Vec<u8>,
    pub output_schema_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentManifestResolution {
    pub execution_kind: AgentExecutionKind,
    pub model_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentBindingIntent {
    pub environment: String,
    pub default_deadline_seconds: u32,
    pub entry_node_id: String,
    pub entry_node_kind: PlanNodeKind,
    pub slots: Vec<SlotBindingIntent>,
    pub policies: Vec<ExactPolicyBinding>,
    pub execution_profile: ExactPolicyBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SlotBindingIntent {
    pub slot_id: String,
    pub requirement_digest: Sha256Digest,
    pub target: SlotTargetIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SlotTargetIntent {
    Model {
        candidates: Vec<ExactDeploymentRef>,
        selection_policy: ExactPolicyBinding,
    },
}

pub type RequiredAgentFeature = AgentRequiredFeature;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecyclePlan {
    pub schema_version: u32,
    pub steps: Vec<LifecycleStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleStep {
    pub ordinal: u8,
    pub kind: LifecycleStepKind,
    pub requires: Vec<LogicalOutputRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

pub fn load_project_sources(
    project_root: &Path,
    manifest_path: &Path,
) -> Result<LoadedAgentProject, AgentCompilerError> {
    let canonical_root = fs::canonicalize(project_root).map_err(|error| {
        AgentCompilerError::reference(format!("cannot resolve project root: {error}"))
    })?;
    if !canonical_root.is_dir() {
        return Err(AgentCompilerError::reference(
            "project root is not a directory",
        ));
    }
    let canonical_manifest = safe_project_file(
        &canonical_root,
        manifest_path,
        MAX_AGENT_MANIFEST_BYTES,
        "manifest",
    )?;
    let manifest_bytes =
        read_bounded_file(&canonical_manifest, MAX_AGENT_MANIFEST_BYTES, "manifest")?;
    let manifest = parse_manifest(&manifest_bytes)?;
    let input_path = Path::new(&manifest.spec.input.schema);
    let output_path = Path::new(&manifest.spec.output.schema);
    let canonical_input = safe_project_file(
        &canonical_root,
        input_path,
        MAX_CLOSED_SCHEMA_BYTES,
        "input schema",
    )?;
    let canonical_output = safe_project_file(
        &canonical_root,
        output_path,
        MAX_CLOSED_SCHEMA_BYTES,
        "output schema",
    )?;
    Ok(LoadedAgentProject {
        manifest_path: canonical_manifest,
        manifest_bytes,
        input_schema_bytes: read_bounded_file(
            &canonical_input,
            MAX_CLOSED_SCHEMA_BYTES,
            "input schema",
        )?,
        output_schema_bytes: read_bounded_file(
            &canonical_output,
            MAX_CLOSED_SCHEMA_BYTES,
            "output schema",
        )?,
    })
}

pub fn inspect_manifest(
    manifest_bytes: &[u8],
) -> Result<AgentManifestResolution, AgentCompilerError> {
    let manifest = parse_manifest(manifest_bytes)?;
    validate_manifest(&manifest)?;
    Ok(AgentManifestResolution {
        execution_kind: manifest.spec.execution.kind,
        model_ref: manifest.spec.model.map(|model| model.r#ref),
    })
}

pub fn compile_agent(input: AgentCompilerInput) -> Result<CompiledAgent, AgentCompilerError> {
    let mut manifest = parse_manifest(&input.manifest_bytes)?;
    validate_profile(&input.profile)?;
    validate_manifest(&manifest)?;

    let input_schema = compile_schema(&input.input_schema_bytes, "input schema")?;
    let output_schema = compile_schema(&input.output_schema_bytes, "output schema")?;
    let error_schema = ClosedJsonSchema::build(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": pinned_nominal_reference("Failure")
            .ok_or_else(|| AgentCompilerError::compile("Failure nominal schema is unavailable"))?
    }))
    .map_err(|error| AgentCompilerError::compile(format!("error schema: {error}")))?;

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

    let contract_digest: Sha256Digest = digest_value(&json!({
        "error_schema_digest": error_schema.canonical_digest,
        "input_schema_digest": input_schema.canonical_digest,
        "output_schema_digest": output_schema.canonical_digest,
        "schema_version": 1
    }))?;
    let canonical_manifest_bytes = canonical_json(
        &serde_json::to_value(&manifest)
            .map_err(|error| AgentCompilerError::compile(error.to_string()))?,
    )
    .map_err(|error| AgentCompilerError::compile(error.to_string()))?;
    let manifest_digest = digest_bytes(&canonical_manifest_bytes)?;

    let (typed_plan, slots, required_features) = match manifest.spec.execution.kind {
        AgentExecutionKind::Deterministic => {
            if input.bindings.model.is_some() {
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
            let slot = SlotBindingIntent {
                slot_id: PRIMARY_MODEL_SLOT_ID.to_owned(),
                requirement_digest: requirement_digest.clone(),
                target: SlotTargetIntent::Model {
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
    };
    let typed_plan_bytes = canonical_json(&typed_plan)
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
            entry_node_id: "start".to_owned(),
            entry_node_kind: PlanNodeKind::Start,
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
    match manifest.spec.execution.kind {
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

fn deterministic_plan(contract: &Sha256Digest, schema: &Sha256Digest) -> Value {
    json!({
        "dependency_slots": {},
        "entry_node_id": "start",
        "interface_contract_digest": contract,
        "nodes": {
            "finish": {
                "kind": "return",
                "value": {"schema_digest": schema, "source": "run_input"}
            },
            "start": {"kind": "start", "next": "finish"}
        },
        "plan_version": 5
    })
}

fn model_chat_plan(
    contract: &Sha256Digest,
    input_schema: &Sha256Digest,
    output_schema: &Sha256Digest,
    requirement: &Sha256Digest,
    limits: ModelLoopCompilerLimits,
) -> Value {
    json!({
        "dependency_slots": {
            "primary_model": {"kind": "model", "requirement_digest": requirement}
        },
        "entry_node_id": "start",
        "interface_contract_digest": contract,
        "nodes": {
            "finish": {
                "kind": "return",
                "value": {
                    "port_id": "response",
                    "producer_node_id": "model",
                    "schema_digest": output_schema,
                    "source": "node_output"
                }
            },
            "model": {
                "capability_slot_ids": [],
                "input": {"schema_digest": input_schema, "source": "run_input"},
                "kind": "model_loop",
                "maximum_capability_calls": limits.maximum_capability_calls,
                "maximum_parallel_calls_per_round": limits.maximum_parallel_calls_per_round,
                "maximum_rounds": limits.maximum_rounds,
                "model_route": null,
                "model_slot_id": PRIMARY_MODEL_SLOT_ID,
                "output": {
                    "port_id": "response",
                    "producer_node_id": "model",
                    "schema_digest": output_schema,
                    "source": "node_output"
                },
                "resume": "finish",
                "skill_slot_ids": [],
                "token_budget": limits.token_budget
            },
            "start": {"kind": "start", "next": "model"}
        },
        "plan_version": 5
    })
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

fn validate_relative_reference(value: &str, name: &str) -> Result<(), AgentCompilerError> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains('\\')
        || value.contains('\0')
        || value
            .split('/')
            .any(|component| component.is_empty() || component == ".")
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::CurDir
                    | Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
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

fn safe_project_file(
    canonical_root: &Path,
    relative: &Path,
    maximum_bytes: usize,
    name: &str,
) -> Result<PathBuf, AgentCompilerError> {
    let rendered = relative.to_string_lossy();
    validate_relative_reference(&rendered, name)?;
    let mut candidate = canonical_root.to_path_buf();
    for component in relative.components() {
        match component {
            Component::Normal(part) => candidate.push(part),
            Component::CurDir => continue,
            _ => {
                return Err(AgentCompilerError::reference(format!(
                    "{name} path escapes the project root"
                )))
            }
        }
        let metadata = fs::symlink_metadata(&candidate).map_err(|error| {
            AgentCompilerError::reference(format!("cannot inspect {name}: {error}"))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(AgentCompilerError::reference(format!(
                "{name} path contains a symbolic link"
            )));
        }
    }
    let canonical = fs::canonicalize(&candidate).map_err(|error| {
        AgentCompilerError::reference(format!("cannot resolve {name}: {error}"))
    })?;
    let metadata = fs::metadata(&canonical).map_err(|error| {
        AgentCompilerError::reference(format!("cannot inspect {name}: {error}"))
    })?;
    if !canonical.starts_with(canonical_root)
        || !metadata.is_file()
        || metadata.len() > u64::try_from(maximum_bytes).unwrap_or(u64::MAX)
    {
        return Err(AgentCompilerError::reference(format!(
            "{name} is outside the project, not a file, or exceeds its byte limit"
        )));
    }
    Ok(canonical)
}

fn read_bounded_file(
    path: &Path,
    maximum_bytes: usize,
    name: &str,
) -> Result<Vec<u8>, AgentCompilerError> {
    let bytes = fs::read(path)
        .map_err(|error| AgentCompilerError::reference(format!("cannot read {name}: {error}")))?;
    if bytes.len() > maximum_bytes {
        return Err(AgentCompilerError::reference(format!(
            "{name} exceeds its byte limit"
        )));
    }
    Ok(bytes)
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
    use super::*;
    use insight_platform_contracts::ResourceId;
    use tempfile::tempdir;

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
        let root = crate::workspace_assets::workspace_path(
            "contracts/product-experience/agent-compiler/v1",
        );
        let corpus: ConformanceCorpus = serde_json::from_slice(
            &fs::read(root.join("corpus.json")).expect("read compiler corpus"),
        )
        .expect("closed compiler corpus");
        assert_eq!(corpus.schema_version, 1);
        for case in corpus.cases {
            let compiled = compile_agent(AgentCompilerInput {
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
        assert_eq!(plan["plan_version"], 5);
        assert_eq!(plan["nodes"]["finish"]["value"]["source"], "run_input");
        assert_eq!(first.lifecycle_plan.steps.len(), 9);
    }

    #[test]
    fn model_chat_compiles_exact_requirement_and_author_instruction() {
        let compiled = compile_agent(AgentCompilerInput {
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
    fn project_loader_rejects_parent_absolute_and_symlink_paths() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join("schemas")).unwrap();
        fs::write(directory.path().join("agent.yaml"), deterministic_yaml()).unwrap();
        fs::write(directory.path().join("schemas/io.json"), SCHEMA).unwrap();
        let loaded = load_project_sources(directory.path(), Path::new("agent.yaml")).unwrap();
        assert_eq!(loaded.input_schema_bytes, SCHEMA.as_bytes());

        assert_eq!(
            load_project_sources(directory.path(), Path::new("../agent.yaml"))
                .unwrap_err()
                .code(),
            AgentCompilerErrorCode::AgentManifestInvalid
        );
        assert_eq!(
            load_project_sources(directory.path(), &directory.path().join("agent.yaml"))
                .unwrap_err()
                .code(),
            AgentCompilerErrorCode::AgentManifestInvalid
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            fs::remove_file(directory.path().join("schemas/io.json")).unwrap();
            let outside = tempdir().unwrap();
            fs::write(outside.path().join("io.json"), SCHEMA).unwrap();
            symlink(
                outside.path().join("io.json"),
                directory.path().join("schemas/io.json"),
            )
            .unwrap();
            assert_eq!(
                load_project_sources(directory.path(), Path::new("agent.yaml"))
                    .unwrap_err()
                    .code(),
                AgentCompilerErrorCode::AgentReferenceMissing
            );
        }
    }

    #[test]
    fn materializer_requires_ready_exact_artifact_authority() {
        let compiled = compile_agent(AgentCompilerInput {
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
}
