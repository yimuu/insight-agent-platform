//! Product-facing Agent authoring and project state built exclusively on public `/v1` clients.

use crate::{
    apply, artifact,
    public_client::{PublicClientError, PublicHttpClient, PublicJsonResponse},
};
use chrono::Utc;
use insight_platform_agent_compiler::{
    self as agent_compiler, AgentCompilationV1, AgentCompileResponseV1, AgentCompilerProfile,
    AgentExecutionKind, AgentSourceBundleV1, AgentSourceFilesV1, ArtifactAuthority, CompiledAgent,
    ModelLoopCompilerLimits, ResolvedAgentBindings, ResolvedModelBinding,
};
use insight_platform_contracts::{
    AdministrativeGate, AgentProductState, DeploymentClosure, EntityLifecycle, ExactDeploymentRef,
    ExactVersionRef, OperationViewV1, PublicJobState, RegistryResourceKind, ResourceDocument,
    ResourceDraftPayload, ResourceId, ResourceKind, Sha256Digest, UtcTimestamp,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

const LOCK_KIND: &str = "insight.platform.agent-project-lock/v2";
const MODEL_BINDINGS_KIND: &str = "insight.platform.agent-model-bindings/v1";
const MAX_LOCK_BYTES: u64 = 1_048_576;

#[derive(Debug)]
pub enum AgentCommandError {
    Compiler(agent_compiler::AgentCompilerError),
    Public(PublicClientError),
    Artifact(artifact::ArtifactClientError),
    Apply(apply::ApplyError),
    InvalidLocalState(String),
    InvalidAuthority(String),
    Io { path: String, detail: String },
}

impl fmt::Display for AgentCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compiler(error) => write!(formatter, "{error}"),
            Self::Public(error) => write!(formatter, "{error}"),
            Self::Artifact(error) => write!(formatter, "{error}"),
            Self::Apply(error) => write!(formatter, "{error}"),
            Self::InvalidLocalState(detail) => {
                write!(formatter, "Agent project state is invalid: {detail}")
            }
            Self::InvalidAuthority(detail) => {
                write!(formatter, "Agent authority response is invalid: {detail}")
            }
            Self::Io { path, detail } => write!(formatter, "cannot access {path}: {detail}"),
        }
    }
}

impl std::error::Error for AgentCommandError {}

impl From<agent_compiler::AgentCompilerError> for AgentCommandError {
    fn from(value: agent_compiler::AgentCompilerError) -> Self {
        Self::Compiler(value)
    }
}

impl From<PublicClientError> for AgentCommandError {
    fn from(value: PublicClientError) -> Self {
        Self::Public(value)
    }
}

impl From<artifact::ArtifactClientError> for AgentCommandError {
    fn from(value: artifact::ArtifactClientError) -> Self {
        Self::Artifact(value)
    }
}

impl From<apply::ApplyError> for AgentCommandError {
    fn from(value: apply::ApplyError) -> Self {
        Self::Apply(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentOutputMode {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentOutputOptions {
    pub mode: AgentOutputMode,
    pub verbose: bool,
    pub debug_authority: bool,
}

impl Default for AgentOutputOptions {
    fn default() -> Self {
        Self {
            mode: AgentOutputMode::Text,
            verbose: false,
            debug_authority: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentValidationReportV1 {
    pub schema_version: u16,
    pub source_map_digest: Sha256Digest,
    pub agent_name: String,
    pub manifest_digest: Sha256Digest,
    pub execution_kind: AgentExecutionKind,
    pub required_features: Vec<agent_compiler::RequiredAgentFeature>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentLockEntryV2 {
    pub source_bundle_digest: Sha256Digest,
    pub manifest_digest: Sha256Digest,
    pub agent_id: ResourceId,
    pub interface_revision_id: Option<ResourceId>,
    pub plan_revision_id: Option<ResourceId>,
    pub active_deployment_id: Option<ResourceId>,
    pub environment: Option<String>,
    pub last_success_at: UtcTimestamp,
    pub latest_run_id: Option<ResourceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentProjectLockV2 {
    pub schema_version: u16,
    pub kind: String,
    pub agents: BTreeMap<String, AgentLockEntryV2>,
}

impl Default for AgentProjectLockV2 {
    fn default() -> Self {
        Self {
            schema_version: 2,
            kind: LOCK_KIND.to_owned(),
            agents: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSummaryV1 {
    pub schema_version: u32,
    pub name: String,
    pub display_name: String,
    pub agent_id: ResourceId,
    pub active_deployment: Option<ExactDeploymentRef>,
    pub state: AgentProductState,
    pub environment: Option<String>,
    pub updated_at: UtcTimestamp,
    pub published_at: Option<UtcTimestamp>,
    pub required_features: Vec<agent_compiler::RequiredAgentFeature>,
    pub latest_run_state: Option<insight_platform_contracts::RunState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ListPageV1<T> {
    schema_version: u32,
    items: Vec<T>,
    next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceViewV1 {
    pub schema_version: u32,
    pub resource_id: ResourceId,
    pub active_deployment_id: Option<ResourceId>,
    pub resource_kind: RegistryResourceKind,
    pub lifecycle_state: EntityLifecycle,
    pub gate_state: AdministrativeGate,
    pub draft_generation: u64,
    pub version: u64,
    pub draft: ResourceDraftPayload,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentViewV1 {
    schema_version: u32,
    deployment_id: ResourceId,
    resource_id: ResourceId,
    resource_kind: RegistryResourceKind,
    resource_version_id: ResourceId,
    environment: String,
    closure_digest: Sha256Digest,
    closure: DeploymentClosure,
    created_at: UtcTimestamp,
    etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PublishedResourceVersionSummaryV1 {
    resource_version_id: ResourceId,
    revision_no: u64,
    content_digest: Sha256Digest,
    artifact_id: Option<ResourceId>,
    etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishResourceDraftResponseV1 {
    schema_version: u32,
    resource_id: ResourceId,
    resource_kind: RegistryResourceKind,
    draft_generation: u64,
    version: u64,
    published_versions: Vec<PublishedResourceVersionSummaryV1>,
    etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AgentUpdateJournalV3 {
    attempt_id: ResourceId,
    base_resource_version: u64,
    base_active_deployment_id: Option<ResourceId>,
    source_bundle_digest: Sha256Digest,
    schema_version: u16,
    kind: String,
    manifest_digest: Sha256Digest,
    agent_id: ResourceId,
    updated_resource_etag: Option<String>,
    validation_operation_id: Option<ResourceId>,
    validated_resource_etag: Option<String>,
    draft_generation: Option<u64>,
    published_versions: Option<Vec<PublishedResourceVersionSummaryV1>>,
    published_resource_etag: Option<String>,
    deployment_id: Option<ResourceId>,
    deployed_resource_etag: Option<String>,
    final_resource_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalModelBindingsV1 {
    schema_version: u16,
    kind: String,
    models: BTreeMap<String, ResolvedModelBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPublicationReportV1 {
    pub schema_version: u16,
    pub agent_name: String,
    pub agent_id: ResourceId,
    pub state: String,
    pub environment: String,
    pub manifest_digest: Sha256Digest,
    pub unchanged: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_operation_id: Option<ResourceId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deployment_id: Option<ResourceId>,
}

#[derive(Debug)]
pub(crate) struct CapturedAgentSources {
    sources: AgentSourceFilesV1,
    resolution: agent_compiler::AgentManifestResolution,
    manifest_path: PathBuf,
}

pub(crate) fn capture_project_sources(
    project_root: &Path,
    manifest_path: &Path,
) -> Result<CapturedAgentSources, AgentCommandError> {
    let loaded = crate::agent_sources::load_project_sources(project_root, manifest_path)?;
    let resolution = agent_compiler::inspect_manifest(&loaded.manifest_bytes)?;
    let manifest_key = manifest_path
        .to_str()
        .ok_or_else(|| {
            AgentCommandError::InvalidLocalState("manifest path must be UTF-8".to_owned())
        })?
        .to_owned();
    let text = |bytes: Vec<u8>| {
        String::from_utf8(bytes).map_err(|_| {
            AgentCommandError::InvalidLocalState("authoring source must be UTF-8".to_owned())
        })
    };
    let mut files = BTreeMap::from([
        (manifest_key.clone(), text(loaded.manifest_bytes)?),
        (
            resolution.input_schema_path.clone(),
            text(loaded.input_schema_bytes)?,
        ),
        (
            resolution.output_schema_path.clone(),
            text(loaded.output_schema_bytes)?,
        ),
    ]);
    if let (Some(path), Some(bytes)) = (resolution.plan_path.clone(), loaded.plan_bytes) {
        files.insert(path, text(bytes)?);
    }
    let sources = AgentSourceFilesV1 {
        manifest_path: manifest_key,
        files,
    };
    match agent_compiler::inspect_agent_sources(agent_compiler::AgentSourceInspectionRequestV1 {
        schema_version: 1,
        sources: sources.clone(),
    }) {
        agent_compiler::AgentManifestInspectionResponseV1::Inspected { resolution, .. } => {
            Ok(CapturedAgentSources {
                sources,
                resolution,
                manifest_path: manifest_path.to_owned(),
            })
        }
        agent_compiler::AgentManifestInspectionResponseV1::Rejected { diagnostics } => {
            Err(AgentCommandError::InvalidLocalState(format!(
                "authoring source inspection rejected: {}",
                serde_json::to_string(&diagnostics).unwrap_or_else(|_| "invalid diagnostic".into())
            )))
        }
    }
}

pub(crate) fn compile_project(
    project_root: &Path,
    captured: CapturedAgentSources,
    profile: AgentCompilerProfile,
) -> Result<AgentCompilationV1, AgentCommandError> {
    let CapturedAgentSources {
        sources,
        resolution,
        manifest_path,
    } = captured;
    compile_project_with_bindings(
        project_root,
        &manifest_path,
        profile,
        None,
        None,
        sources,
        resolution,
    )
}
pub(crate) fn compile_project_online(
    project_root: &Path,
    captured: CapturedAgentSources,
    profile: AgentCompilerProfile,
    client: &PublicHttpClient,
) -> Result<AgentCompilationV1, AgentCommandError> {
    use insight_platform_registry::authoring::{
        AuthoringResolutionV1, ResolveAgentBindingsRequestV1, MAX_AUTHORING_QUERY_BYTES,
    };
    let CapturedAgentSources {
        sources,
        resolution,
        manifest_path,
    } = captured;
    let path = project_root
        .join(manifest_path.parent().unwrap_or_else(|| Path::new("")))
        .join(".insight/agent-binding-selections.json");
    let resolved = if path.exists() {
        let bytes = read_bounded_file(&path, MAX_AUTHORING_QUERY_BYTES as u64)?;
        let value = insight_platform_contracts::parse_strict_json(
            &bytes,
            insight_platform_contracts::JsonLimits {
                max_bytes: MAX_AUTHORING_QUERY_BYTES,
                max_depth: 32,
                max_properties_per_object: 128,
                max_items_per_array: 64,
                max_string_bytes: 4096,
            },
        )
        .map_err(|_| {
            AgentCommandError::InvalidLocalState(
                "authoring selections are not bounded strict JSON".into(),
            )
        })?;
        let request: ResolveAgentBindingsRequestV1 =
            serde_json::from_value(value).map_err(|_| {
                AgentCommandError::InvalidLocalState(
                    "authoring selections violate the query contract".into(),
                )
            })?;
        let response = client.resolve_agent_bindings(&request)?;
        let mut slots = Vec::new();
        for slot in response.slots {
            match slot.resolution {
                AuthoringResolutionV1::Resolved { binding, .. } => slots.push(*binding),
                AuthoringResolutionV1::Rejected { code } => {
                    return Err(AgentCommandError::InvalidAuthority(format!(
                        "slot {} resolution rejected: {:?}",
                        slot.slot_id, code
                    )))
                }
            }
        }
        Some(ResolvedAgentBindings {
            slots,
            model: None,
            deployment_features: Vec::new(),
        })
    } else if let Some(model_ref) = &resolution.model_ref {
        let authoring = read_authoring_profile(client)?;
        if compiler_profile(&authoring) != profile {
            return Err(AgentCommandError::InvalidAuthority(
                "authoring policies changed during compilation; capture a new attempt".to_owned(),
            ));
        }
        let model = authoring
            .models
            .into_iter()
            .find(|model| &model.alias == model_ref)
            .ok_or_else(|| {
                AgentCommandError::InvalidAuthority(
                    "requested model is not available in the current authoring profile".to_owned(),
                )
            })?;
        Some(ResolvedAgentBindings {
            model: Some(ResolvedModelBinding {
                manifest_ref: model_ref.clone(),
                deployment: model.deployment,
                selection_policy: model.selection_policy,
            }),
            slots: Vec::new(),
            deployment_features: Vec::new(),
        })
    } else {
        None
    };
    compile_project_with_bindings(
        project_root,
        &manifest_path,
        profile,
        resolved,
        Some(client),
        sources,
        resolution,
    )
}
fn compile_project_with_bindings(
    project_root: &Path,
    manifest_path: &Path,
    profile: AgentCompilerProfile,
    resolved: Option<ResolvedAgentBindings>,
    client: Option<&PublicHttpClient>,
    sources: AgentSourceFilesV1,
    resolution: agent_compiler::AgentManifestResolution,
) -> Result<AgentCompilationV1, AgentCommandError> {
    let mut bindings = if let Some(bindings) = resolved {
        if !matches!(
            resolution.execution_kind,
            agent_compiler::AgentExecutionKind::FullPlan
                | agent_compiler::AgentExecutionKind::FrameworkGraph
        ) && (resolution.execution_kind != agent_compiler::AgentExecutionKind::ModelChat
            || bindings.model.is_none()
            || !bindings.slots.is_empty())
        {
            return Err(AgentCommandError::InvalidLocalState(
                "slot selections require full_plan authoring".into(),
            ));
        }
        bindings
    } else if matches!(
        resolution.execution_kind,
        agent_compiler::AgentExecutionKind::FullPlan
            | agent_compiler::AgentExecutionKind::FrameworkGraph
    ) {
        let path = project_root
            .join(manifest_path.parent().unwrap_or_else(|| Path::new("")))
            .join(".insight")
            .join("agent-exact-bindings.json");
        if path.exists() {
            let bytes = read_bounded_file(&path, MAX_LOCK_BYTES)?;
            let value = insight_platform_contracts::parse_strict_json(
                &bytes,
                insight_platform_contracts::JsonLimits {
                    max_bytes: MAX_LOCK_BYTES as usize,
                    max_depth: 32,
                    max_properties_per_object: 1024,
                    max_items_per_array: 1024,
                    max_string_bytes: 4096,
                },
            )
            .map_err(|_| {
                AgentCommandError::InvalidLocalState(
                    "exact bindings are not bounded strict JSON".into(),
                )
            })?;
            serde_json::from_value::<ResolvedAgentBindings>(value).map_err(|_| {
                AgentCommandError::InvalidLocalState(
                    "exact bindings do not match the compiler contract".into(),
                )
            })?
        } else {
            ResolvedAgentBindings::default()
        }
    } else {
        resolve_model_bindings(
            &project_root.join(manifest_path.parent().unwrap_or_else(|| Path::new(""))),
            resolution.model_ref.as_deref(),
        )?
    };
    if let Some(client) = client {
        if let Some(request) = insight_platform_registry::authoring::exact_feature_request(
            &bindings.slots,
        )
        .map_err(|_| {
            AgentCommandError::InvalidLocalState("exact feature bindings are invalid".into())
        })? {
            let response = client.resolve_agent_bindings(&request)?;
            bindings.deployment_features =
                insight_platform_registry::authoring::resolved_feature_evidence(
                    &response, &request,
                )
                .map_err(|_| {
                    AgentCommandError::InvalidAuthority(
                        "deployment feature evidence is unavailable".into(),
                    )
                })?;
        } else if !bindings.deployment_features.is_empty() {
            return Err(AgentCommandError::InvalidLocalState(
                "unrelated deployment feature evidence".into(),
            ));
        }
    }
    let bundle = AgentSourceBundleV1 {
        schema_version: agent_compiler::AGENT_SOURCE_BUNDLE_VERSION,
        compiler_semantic_identity: agent_compiler::compiler_semantic_identity(),
        compile_policy_inputs_digest: AgentSourceBundleV1::policy_digest(&profile),
        sources,
        profile,
        bindings,
    };
    match agent_compiler::compile_source_bundle(bundle) {
        AgentCompileResponseV1::Compiled { compilation } => Ok(*compilation),
        AgentCompileResponseV1::Rejected { diagnostics } => {
            Err(AgentCommandError::InvalidLocalState(format!(
                "authoring compilation rejected: {}",
                serde_json::to_string(&diagnostics).unwrap_or_else(|_| "invalid diagnostic".into())
            )))
        }
    }
}

pub fn validation_report(compilation: &AgentCompilationV1) -> AgentValidationReportV1 {
    let compiled = &compilation.compiled;
    AgentValidationReportV1 {
        schema_version: 1,
        source_map_digest: compilation.source_map_digest.clone(),
        agent_name: compiled.name.clone(),
        manifest_digest: compiled.manifest_digest.clone(),
        execution_kind: compiled.execution_kind,
        required_features: compiled.required_features.clone(),
    }
}

/// A restored source package carries its exact, non-secret compilation profile.
/// This affects offline validation only; publication rechecks server authority.
pub fn restored_compiler_profile(
    project_root: &Path,
    manifest_path: &Path,
) -> Result<Option<AgentCompilerProfile>, AgentCommandError> {
    let manifest = if manifest_path.is_absolute() {
        manifest_path.to_path_buf()
    } else {
        project_root.join(manifest_path)
    };
    let path = manifest
        .parent()
        .ok_or_else(|| AgentCommandError::InvalidLocalState("manifest parent missing".into()))?
        .join(".insight/agent-compiler-profile.json");
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(&path, error)),
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(AgentCommandError::InvalidLocalState(
                "compiler profile must be a regular file".into(),
            ))
        }
        Ok(_) => {}
    }
    let root = fs::canonicalize(project_root).map_err(|error| io_error(project_root, error))?;
    if !fs::canonicalize(&path)
        .map_err(|error| io_error(&path, error))?
        .starts_with(root)
    {
        return Err(AgentCommandError::InvalidLocalState(
            "compiler profile escapes project".into(),
        ));
    }
    require_private_file(&path)?;
    let bytes = read_bounded_file(&path, agent_compiler::MAX_AGENT_SOURCE_FILE_BYTES as u64)?;
    let value = insight_platform_contracts::parse_strict_json(
        &bytes,
        insight_platform_contracts::JsonLimits {
            max_bytes: agent_compiler::MAX_AGENT_SOURCE_FILE_BYTES,
            max_depth: 16,
            max_properties_per_object: 64,
            max_items_per_array: 64,
            max_string_bytes: 4096,
        },
    )
    .map_err(|_| {
        AgentCommandError::InvalidLocalState("compiler profile must be bounded strict JSON".into())
    })?;
    let profile = serde_json::from_value(value).map_err(|_| {
        AgentCommandError::InvalidLocalState("compiler profile violates its closed contract".into())
    })?;
    Ok(Some(profile))
}

pub fn offline_compiler_profile(
    project_root: &Path,
) -> Result<AgentCompilerProfile, AgentCommandError> {
    restored_compiler_profile(project_root, Path::new("agent.yaml"))?.ok_or_else(||
        AgentCommandError::InvalidLocalState("offline validation requires an explicit exact .insight/agent-compiler-profile.json; capture the server profile or restore a published source package".to_owned()))
}

fn read_authoring_profile(
    client: &PublicHttpClient,
) -> Result<insight_platform_api::product::AgentAuthoringProfileV1, AgentCommandError> {
    let profile = client
        .get_body_json::<insight_platform_api::product::AgentAuthoringProfileV1>(
            "/v1/agent-authoring-profile",
            StatusCode::OK,
        )?
        .body;
    profile.validate().map_err(|_| {
        AgentCommandError::InvalidAuthority(
            "authoring profile violates its exact contract".to_owned(),
        )
    })?;
    Ok(profile)
}
fn compiler_profile(
    profile: &insight_platform_api::product::AgentAuthoringProfileV1,
) -> AgentCompilerProfile {
    AgentCompilerProfile {
        default_deadline_seconds: profile.default_deadline_seconds,
        default_environment: profile.default_environment.clone(),
        policy_versions: profile.policy_versions.clone(),
        deployment_policies: profile.deployment_policies.clone(),
        execution_profile: profile.execution_profile.clone(),
        model_loop: ModelLoopCompilerLimits {
            maximum_rounds: profile.model_loop.maximum_rounds,
            maximum_capability_calls: profile.model_loop.maximum_capability_calls,
            maximum_parallel_calls_per_round: profile.model_loop.maximum_parallel_calls_per_round,
            token_budget: profile.model_loop.token_budget,
        },
    }
}
pub fn online_compiler_profile(
    client: &PublicHttpClient,
    _project_root: &Path,
) -> Result<AgentCompilerProfile, AgentCommandError> {
    Ok(compiler_profile(&read_authoring_profile(client)?))
}

fn resolve_model_bindings(
    project_root: &Path,
    model_ref: Option<&str>,
) -> Result<ResolvedAgentBindings, AgentCommandError> {
    let Some(model_ref) = model_ref else {
        return Ok(ResolvedAgentBindings::default());
    };
    let path = project_root
        .join(".insight")
        .join("agent-model-bindings.json");
    let bytes = read_bounded_file(&path, MAX_LOCK_BYTES)?;
    require_private_file(&path)?;
    let bindings = serde_json::from_slice::<LocalModelBindingsV1>(&bytes).map_err(|_| {
        AgentCommandError::InvalidLocalState(
            "agent-model-bindings.json is not closed JSON".to_owned(),
        )
    })?;
    if bindings.schema_version != 1 || bindings.kind != MODEL_BINDINGS_KIND {
        return Err(AgentCommandError::InvalidLocalState(
            "agent model binding kind or schema version is invalid".to_owned(),
        ));
    }
    let model = bindings.models.get(model_ref).cloned().ok_or_else(|| {
        AgentCommandError::InvalidLocalState(format!(
            "model alias {model_ref:?} is unresolved; enable the model feature or provide an exact project binding"
        ))
    })?;
    Ok(ResolvedAgentBindings {
        deployment_features: Vec::new(),
        slots: Vec::new(),
        model: Some(model),
    })
}

pub fn read_resource(
    client: &PublicHttpClient,
    agent_id: &ResourceId,
) -> Result<ResourceViewV1, AgentCommandError> {
    require_agent_id(agent_id)?;
    let response: PublicJsonResponse<ResourceViewV1> =
        client.get_json(&format!("/v1/agents/{agent_id}"), StatusCode::OK)?;
    validate_resource_response(&response, agent_id)?;
    Ok(response.body)
}

/// Resolve only this Resource's active ID and exact immutable Plan; draft content never supplies Run defaults.
pub fn published_run_defaults(
    client: &PublicHttpClient,
    agent_id: &ResourceId,
) -> Result<
    (
        ExactDeploymentRef,
        insight_platform_contracts::AgentResourceSpec,
    ),
    AgentCommandError,
> {
    use insight_platform_api::resource::{DeploymentViewV1, ResourceVersionViewV1};
    let resource = read_resource(client, agent_id)?;
    let active = resource
        .active_deployment_id
        .filter(|id| id.kind() == ResourceKind::AgentDeployment)
        .ok_or_else(|| {
            AgentCommandError::InvalidAuthority("Agent has no active published deployment".into())
        })?;
    let deployment: PublicJsonResponse<DeploymentViewV1> = client.get_json(
        &format!("/v1/agents/{agent_id}/deployments/{active}"),
        StatusCode::OK,
    )?;
    deployment.body.validate().map_err(|_| {
        AgentCommandError::InvalidAuthority("Agent Deployment violates its contract".into())
    })?;
    if deployment.body.resource_id != *agent_id
        || deployment.body.deployment_id != active
        || deployment.body.etag != deployment.etag
    {
        return Err(AgentCommandError::InvalidAuthority(
            "Agent Deployment identity differs".into(),
        ));
    }
    let insight_platform_contracts::DeploymentClosure::Agent(closure) = deployment.body.closure
    else {
        return Err(AgentCommandError::InvalidAuthority(
            "Expected Agent Deployment".into(),
        ));
    };
    let exact = ExactDeploymentRef::new(active, deployment.body.closure_digest)
        .map_err(|e| AgentCommandError::InvalidAuthority(e.to_string()))?;
    let published: PublicJsonResponse<ResourceVersionViewV1> = client.get_json(
        &format!(
            "/v1/agents/{agent_id}/versions/{}",
            closure.plan.revision_id
        ),
        StatusCode::OK,
    )?;
    published.body.validate().map_err(|_| {
        AgentCommandError::InvalidAuthority("Agent Plan version violates its contract".into())
    })?;
    if published.body.resource_id != *agent_id
        || published.body.resource_version_id != closure.plan.revision_id
        || published.body.content_digest != closure.plan.semantic_digest
        || published.body.etag != published.etag
    {
        return Err(AgentCommandError::InvalidAuthority(
            "Agent Plan identity differs".into(),
        ));
    }
    let ResourceDocument::Agent(spec) = published.body.payload.document else {
        return Err(AgentCommandError::InvalidAuthority(
            "Expected Agent Plan document".into(),
        ));
    };
    if spec.typed_plan_digest != closure.plan.semantic_digest {
        return Err(AgentCommandError::InvalidAuthority(
            "Agent Plan digest differs".into(),
        ));
    }
    Ok((exact, spec))
}

pub fn list_remote_agents(
    client: &PublicHttpClient,
) -> Result<Vec<AgentSummaryV1>, AgentCommandError> {
    let mut cursor = None;
    let mut agents = Vec::new();
    loop {
        let path = cursor.as_ref().map_or_else(
            || "/v1/agents?page_size=50".to_owned(),
            |cursor: &String| format!("/v1/agents?page_size=50&cursor={cursor}"),
        );
        let page = client
            .get_body_json::<ListPageV1<AgentSummaryV1>>(&path, StatusCode::OK)?
            .body;
        if page.schema_version != 1 || page.items.len() > 50 {
            return Err(AgentCommandError::InvalidAuthority(
                "Agent list page violates its bound".to_owned(),
            ));
        }
        for agent in &page.items {
            validate_agent_summary(agent)?;
        }
        agents.extend(page.items);
        match page.next_cursor {
            Some(next) if !next.is_empty() && next.len() <= 4_096 => cursor = Some(next),
            Some(_) => {
                return Err(AgentCommandError::InvalidAuthority(
                    "Agent list cursor is invalid".to_owned(),
                ))
            }
            None => break,
        }
        if agents.len() > 10_000 {
            return Err(AgentCommandError::InvalidAuthority(
                "Agent list exceeds the CLI safety bound".to_owned(),
            ));
        }
    }
    Ok(agents)
}

pub fn resolve_agent_id(
    project_root: &Path,
    selector: &str,
) -> Result<ResourceId, AgentCommandError> {
    if let Ok(agent_id) = ResourceId::parse_expected(selector, ResourceKind::Agent) {
        return Ok(agent_id);
    }
    let lock = load_lock(project_root)?;
    lock.agents
        .get(selector)
        .map(|entry| entry.agent_id.clone())
        .ok_or_else(|| {
            AgentCommandError::InvalidLocalState(format!(
                "Agent name {selector:?} is not in insight.lock; publish or adopt it first"
            ))
        })
}

pub fn adopt_agent(
    project_root: &Path,
    client: &PublicHttpClient,
    name: &str,
    agent_id: ResourceId,
) -> Result<AgentLockEntryV2, AgentCommandError> {
    let resource = read_resource(client, &agent_id)?;
    let ResourceDocument::Agent(spec) = &resource.draft.document else {
        return Err(AgentCommandError::InvalidAuthority(
            "the exact Resource is not an Agent".to_owned(),
        ));
    };
    if spec.authoring_name != name {
        return Err(AgentCommandError::InvalidAuthority(format!(
            "server-owned Agent name {:?} does not match requested name {name:?}",
            spec.authoring_name
        )));
    }
    let remote = list_remote_agents(client)?
        .into_iter()
        .find(|summary| summary.agent_id == agent_id);
    let entry = AgentLockEntryV2 {
        source_bundle_digest: spec.authoring_package.artifact.content_digest().clone(),
        manifest_digest: spec.authoring_package.manifest_digest.clone(),
        agent_id,
        interface_revision_id: None,
        plan_revision_id: None,
        active_deployment_id: None,
        environment: remote.and_then(|summary| summary.environment),
        last_success_at: UtcTimestamp::from_datetime(Utc::now()),
        latest_run_id: None,
    };
    let mut lock = load_lock(project_root)?;
    if lock
        .agents
        .get(name)
        .is_some_and(|current| current.agent_id != entry.agent_id)
    {
        return Err(AgentCommandError::InvalidLocalState(format!(
            "Agent name {name:?} is already mapped to a different Resource"
        )));
    }
    lock.agents.insert(name.to_owned(), entry.clone());
    save_lock(project_root, &lock)?;
    Ok(entry)
}

pub fn publish_agent(
    project_root: &Path,
    management_client: &PublicHttpClient,
    runtime_client: &PublicHttpClient,
    expected_tenant_id: &ResourceId,
    compilation: &AgentCompilationV1,
    operation_timeout: Duration,
) -> Result<AgentPublicationReportV1, AgentCommandError> {
    let compiled = &compilation.compiled;
    let source_bundle_digest = &compilation.source_bundle_digest;
    let existing = load_lock(project_root)?.agents.get(&compiled.name).cloned();
    let cache = project_root
        .join(".insight")
        .join("agent-publication")
        .join(digest_suffix(source_bundle_digest)?);
    let journal = existing
        .as_ref()
        .map(|entry| load_update_journal(&cache.join("update.json"), compilation, &entry.agent_id))
        .transpose()?
        .flatten();
    if let Some(existing) = &existing {
        // An incomplete attempt retains its Receipt/CAS identity even when its outcome is unknown.
        if journal
            .as_ref()
            .is_none_or(|journal| journal.final_resource_etag.is_some())
        {
            let current = read_resource(management_client, &existing.agent_id)?;
            if let Some(mut observed) =
                matching_active_publication(management_client, &current, compilation)?
            {
                observed.latest_run_id = existing.latest_run_id.clone();
                let active = observed.active_deployment_id.clone();
                let mut lock = load_lock(project_root)?;
                lock.agents.insert(compiled.name.clone(), observed);
                save_lock(project_root, &lock)?;
                return Ok(AgentPublicationReportV1 {
                    schema_version: 1,
                    agent_name: compiled.name.clone(),
                    agent_id: existing.agent_id.clone(),
                    state: "ready".into(),
                    environment: compiled.deployment_intent.environment.clone(),
                    manifest_digest: compiled.manifest_digest.clone(),
                    unchanged: true,
                    validation_operation_id: None,
                    active_deployment_id: active,
                });
            }
        }
    }
    crate::private_state::ensure_durable_directory(&cache)
        .map_err(|error| io_error(&cache, error))?;
    set_private_directory(&cache)?;
    let manifest_path = cache.join("authoring.json");
    let plan_path = cache.join("typed-plan.json");
    write_private_file(&manifest_path, &compilation.source_bundle_bytes)?;
    write_private_file(&plan_path, &compiled.typed_plan_bytes)?;
    write_private_file(
        &cache.join("source-map.json"),
        &compilation.source_map_bytes,
    )?;

    let uploader = artifact::HttpsArtifactObjectUploader::with_additional_roots(
        runtime_client.additional_roots(),
    )?;
    let upload = |path: &Path, intent: &agent_compiler::ArtifactIntent| {
        artifact::upload_artifact(
            runtime_client,
            &uploader,
            expected_tenant_id,
            path,
            artifact::ArtifactUploadOptions {
                purpose: intent.purpose,
                classification: intent.classification,
                declared_media_type: Some(intent.media_type.clone()),
                display_name: intent.display_name.clone(),
                operation_timeout,
            },
            &project_root.join(".insight/artifact-upload"),
        )
    };
    let authoring_upload = upload(&manifest_path, &compiled.resource_intent.authoring_artifact)?;
    let plan_upload = upload(&plan_path, &compiled.resource_intent.typed_plan_artifact)?;
    let authoring_id =
        ResourceId::parse_expected(&authoring_upload.artifact_id, ResourceKind::Artifact)
            .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))?;
    let plan_id = ResourceId::parse_expected(&plan_upload.artifact_id, ResourceKind::Artifact)
        .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))?;
    let authoring = artifact::read_artifact(runtime_client, &authoring_id)?;
    let plan = artifact::read_artifact(runtime_client, &plan_id)?;
    let authoring_authority = artifact_authority(authoring)?;
    let plan_authority = artifact_authority(plan)?;
    let document = compilation.materialize(&authoring_authority, &plan_authority)?;
    if let Some(existing) = existing {
        return update_agent(
            project_root,
            management_client,
            expected_tenant_id,
            compilation,
            document,
            &plan_id,
            existing,
            operation_timeout,
            &cache,
            journal,
        );
    }
    let manifest = build_apply_manifest(compiled, document, &plan_id)?;
    let bytes = serde_json::to_vec(&manifest)
        .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?;
    let applied = apply::apply_manifest(
        management_client,
        expected_tenant_id,
        &bytes,
        operation_timeout,
        &cache.join("lifecycle"),
    )?;
    let agent_id = ResourceId::parse_expected(&applied.resource_id, ResourceKind::Agent)
        .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))?;
    let validation_operation_id =
        ResourceId::parse_expected(&applied.validation_operation_id, ResourceKind::Job)
            .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))?;
    let active_deployment_id = applied
        .active_deployment_id
        .as_deref()
        .map(|value| ResourceId::parse_expected(value, ResourceKind::AgentDeployment))
        .transpose()
        .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))?;
    let current = read_resource(management_client, &agent_id)?;
    let entry = matching_active_publication(management_client, &current, compilation)?
        .filter(|entry| entry.active_deployment_id == active_deployment_id)
        .ok_or_else(|| {
            AgentCommandError::InvalidAuthority(
                "completed initial publication no longer matches active authority".into(),
            )
        })?;
    let mut lock = load_lock(project_root)?;
    lock.agents.insert(compiled.name.clone(), entry);
    save_lock(project_root, &lock)?;
    Ok(AgentPublicationReportV1 {
        schema_version: 1,
        agent_name: compiled.name.clone(),
        agent_id,
        state: "ready".to_owned(),
        environment: compiled.deployment_intent.environment.clone(),
        manifest_digest: compiled.manifest_digest.clone(),
        unchanged: false,
        validation_operation_id: Some(validation_operation_id),
        active_deployment_id,
    })
}

#[allow(clippy::too_many_arguments)]
fn update_agent(
    project_root: &Path,
    client: &PublicHttpClient,
    expected_tenant_id: &ResourceId,
    compilation: &AgentCompilationV1,
    document: ResourceDocument,
    typed_plan_artifact_id: &ResourceId,
    existing: AgentLockEntryV2,
    operation_timeout: Duration,
    cache: &Path,
    previous_journal: Option<AgentUpdateJournalV3>,
) -> Result<AgentPublicationReportV1, AgentCommandError> {
    let compiled = &compilation.compiled;
    let source_bundle_digest = &compilation.source_bundle_digest;
    let journal_path = cache.join("update.json");
    let mut journal = match previous_journal {
        Some(journal) if journal.final_resource_etag.is_none() => journal,
        _ => {
            let current = read_resource(client, &existing.agent_id)?;
            let ResourceDocument::Agent(spec) = &current.draft.document else {
                return Err(AgentCommandError::InvalidAuthority(
                    "existing Resource is not an Agent".into(),
                ));
            };
            if spec.authoring_name != compiled.name {
                return Err(AgentCommandError::InvalidAuthority(
                    "server-owned Agent name cannot be changed".into(),
                ));
            }
            AgentUpdateJournalV3 {
                attempt_id: ResourceId::from_uuid_v7(ResourceKind::ServerRequest, Uuid::now_v7())
                    .map_err(|_| {
                    AgentCommandError::InvalidLocalState(
                        "publication attempt identity generation failed".into(),
                    )
                })?,
                base_resource_version: current.version,
                base_active_deployment_id: current.active_deployment_id,
                source_bundle_digest: source_bundle_digest.clone(),
                schema_version: 3,
                kind: "insight.platform.agent-update-journal/v3".into(),
                manifest_digest: compiled.manifest_digest.clone(),
                agent_id: existing.agent_id.clone(),
                updated_resource_etag: None,
                validation_operation_id: None,
                validated_resource_etag: None,
                draft_generation: None,
                published_versions: None,
                published_resource_etag: None,
                deployment_id: None,
                deployed_resource_etag: None,
                final_resource_etag: None,
            }
        }
    };
    validate_update_journal(&journal, compilation, &existing.agent_id)?;
    save_update_journal(&journal_path, &journal)?;
    if journal.updated_resource_etag.is_none() {
        let request = serde_json::json!({"display_name": compiled.resource_intent.display_name, "document": document});
        let response: PublicJsonResponse<ResourceViewV1> = client.put_json(
            &format!("/v1/agents/{}/draft", existing.agent_id),
            &request,
            StatusCode::OK,
            &publication_receipt(&journal.attempt_id, "update"),
            &insight_platform_api::resource::resource_etag(
                &existing.agent_id,
                journal.base_resource_version,
            ),
        )?;
        validate_resource_response(&response, &existing.agent_id)?;
        if response.body.version
            != journal
                .base_resource_version
                .checked_add(1)
                .ok_or_else(|| {
                    AgentCommandError::InvalidLocalState("Resource version overflow".into())
                })?
            || response.body.draft.document != document
            || response.body.draft.display_name != compiled.resource_intent.display_name
        {
            return Err(AgentCommandError::InvalidAuthority(
                "updated Draft differs from frozen intent".into(),
            ));
        }
        journal.updated_resource_etag = Some(response.etag);
        save_update_journal(&journal_path, &journal)?;
    }

    if journal.validation_operation_id.is_none() {
        let response: PublicJsonResponse<OperationViewV1> = client.post_empty(
            &format!("/v1/agents/{}/draft:validate", existing.agent_id),
            StatusCode::ACCEPTED,
            &publication_receipt(&journal.attempt_id, "validate"),
            journal.updated_resource_etag.as_deref().ok_or_else(|| {
                AgentCommandError::InvalidLocalState(
                    "update journal omitted the post-update ETag".to_owned(),
                )
            })?,
        )?;
        response.body.validate().map_err(|_| {
            AgentCommandError::InvalidAuthority(
                "validation Operation violates its public contract".to_owned(),
            )
        })?;
        if response.body.tenant_id != *expected_tenant_id
            || response.body.etag != response.etag
            || response.body.kind != insight_platform_contracts::PublicJobKind::ResourceValidation
            || !matches!(&response.body.target, insight_platform_contracts::PublicJobTarget::ResourceVersion { resource_id, resource_version }
                if resource_id == &existing.agent_id && Some(*resource_version) == journal.updated_resource_etag.as_ref().and_then(|etag| update_resource_version(&existing.agent_id, etag).ok()))
        {
            return Err(AgentCommandError::InvalidAuthority(
                "validation Operation belongs to another tenant".to_owned(),
            ));
        }
        journal.validation_operation_id = Some(response.body.operation_id);
        save_update_journal(&journal_path, &journal)?;
    }
    let validation_operation_id = journal.validation_operation_id.clone().ok_or_else(|| {
        AgentCommandError::InvalidLocalState(
            "update journal omitted validation Operation".to_owned(),
        )
    })?;
    if journal.validated_resource_etag.is_none() {
        let operation = client.wait_operation(
            &validation_operation_id,
            expected_tenant_id,
            operation_timeout,
        )?;
        if operation.state != PublicJobState::Succeeded {
            return Err(AgentCommandError::InvalidAuthority(format!(
                "validation Operation reached terminal state {:?}",
                operation.state
            )));
        }
        let validated = read_resource(client, &existing.agent_id)?;
        if validated.draft.validation.is_none()
            || validated.draft.document != document
            || validated.draft.display_name != compiled.resource_intent.display_name
            || Some(validated.version)
                != journal
                    .updated_resource_etag
                    .as_ref()
                    .and_then(|etag| update_resource_version(&existing.agent_id, etag).ok())
                    .and_then(|version| version.checked_add(1))
        {
            return Err(AgentCommandError::InvalidAuthority(
                "successful validation did not materialize a validation summary".to_owned(),
            ));
        }
        journal.validated_resource_etag = Some(validated.etag);
        journal.draft_generation = Some(validated.draft_generation);
        save_update_journal(&journal_path, &journal)?;
    }

    if journal.published_versions.is_none() {
        let request = serde_json::json!({
            "kind": "agent",
            "revision_no": journal.draft_generation.ok_or_else(|| AgentCommandError::InvalidLocalState("update journal omitted draft generation".to_owned()))?,
            "interface_content_digest": compiled.resource_intent.contract_digest,
            "plan_content_digest": compiled.typed_plan_digest,
            "artifact_id": typed_plan_artifact_id
        });
        let response: PublicJsonResponse<PublishResourceDraftResponseV1> = client.post_json(
            &format!("/v1/agents/{}/draft:publish", existing.agent_id),
            &request,
            StatusCode::OK,
            &publication_receipt(&journal.attempt_id, "publish"),
            journal.validated_resource_etag.as_deref(),
        )?;
        if response.body.schema_version != 1
            || response.body.resource_id != existing.agent_id
            || response.body.resource_kind != RegistryResourceKind::Agent
            || response.body.draft_generation != journal.draft_generation.unwrap_or_default()
            || response.body.version == 0
            || response.body.etag != response.etag
            || response.body.published_versions.len() != 2
            || response.body.published_versions.iter().any(|version| {
                version.revision_no != journal.draft_generation.unwrap_or_default()
                    || version.artifact_id.as_ref() != Some(typed_plan_artifact_id)
                    || version.etag
                        != insight_platform_api::resource::resource_version_etag(
                            &version.resource_version_id,
                            &version.content_digest,
                        )
            })
        {
            return Err(AgentCommandError::InvalidAuthority(
                "publish response identity or version closure is invalid".to_owned(),
            ));
        }
        journal.published_versions = Some(response.body.published_versions);
        journal.published_resource_etag = Some(response.etag);
        save_update_journal(&journal_path, &journal)?;
    }
    let published = journal.published_versions.as_ref().ok_or_else(|| {
        AgentCommandError::InvalidLocalState("update journal omitted published versions".to_owned())
    })?;
    if published.iter().any(|version| {
        version.revision_no != journal.draft_generation.unwrap_or_default()
            || version.artifact_id.as_ref() != Some(typed_plan_artifact_id)
            || version.etag
                != insight_platform_api::resource::resource_version_etag(
                    &version.resource_version_id,
                    &version.content_digest,
                )
    }) {
        return Err(AgentCommandError::InvalidLocalState(
            "published journal Artifact/version evidence differs".into(),
        ));
    }
    let interface = exact_published_version(
        published,
        ResourceKind::AgentInterfaceRevision,
        &compiled.resource_intent.contract_digest,
    )?;
    let plan = exact_published_version(
        published,
        ResourceKind::AgentPlanRevision,
        &compiled.typed_plan_digest,
    )?;
    if journal.deployment_id.is_none() {
        let slots = serde_json::to_value(&compiled.deployment_intent.slots)
            .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?;
        let request = serde_json::json!({
            "resource_version_id": plan.revision_id,
            "environment": compiled.deployment_intent.environment,
            "closure": {
                "resource_kind": "agent",
                "bindings": {
                    "interface": interface,
                    "plan": plan,
                    "entry_node_id": compiled.deployment_intent.entry_node_id,
                    "entry_node_kind": compiled.deployment_intent.entry_node_kind,
                    "slots": slots,
                    "policies": compiled.deployment_intent.policies,
                    "execution_profile": compiled.deployment_intent.execution_profile
                }
            }
        });
        let response: PublicJsonResponse<DeploymentViewV1> = client.post_json(
            &format!("/v1/agents/{}/deployments", existing.agent_id),
            &request,
            StatusCode::CREATED,
            &publication_receipt(&journal.attempt_id, "deploy"),
            journal.published_resource_etag.as_deref(),
        )?;
        if response.body.schema_version != 1
            || response.body.resource_id != existing.agent_id
            || response.body.resource_kind != RegistryResourceKind::Agent
            || response.body.deployment_id.kind() != ResourceKind::AgentDeployment
            || response.body.resource_version_id != plan.revision_id
            || response.body.environment != compiled.deployment_intent.environment
            || response.body.etag != response.etag
        {
            return Err(AgentCommandError::InvalidAuthority(
                "Agent Deployment response identity is invalid".to_owned(),
            ));
        }
        journal.deployment_id = Some(response.body.deployment_id);
        save_update_journal(&journal_path, &journal)?;
    }
    if journal.deployed_resource_etag.is_none() {
        let deployed = read_resource(client, &existing.agent_id)?;
        if Some(deployed.version)
            != journal
                .published_resource_etag
                .as_ref()
                .and_then(|etag| update_resource_version(&existing.agent_id, etag).ok())
                .and_then(|version| version.checked_add(1))
        {
            return Err(AgentCommandError::InvalidAuthority(
                "post-Deployment Resource is not the exact successor".into(),
            ));
        }
        journal.deployed_resource_etag = Some(deployed.etag);
        save_update_journal(&journal_path, &journal)?;
    }
    let deployment_id = journal.deployment_id.clone().ok_or_else(|| {
        AgentCommandError::InvalidLocalState("update journal omitted Deployment".to_owned())
    })?;
    if journal.final_resource_etag.is_none() {
        let response: PublicJsonResponse<ResourceViewV1> = client.post_empty(
            &format!(
                "/v1/agents/{}/deployments/{deployment_id}:activate",
                existing.agent_id
            ),
            StatusCode::OK,
            &publication_receipt(&journal.attempt_id, "activate"),
            journal.deployed_resource_etag.as_deref().ok_or_else(|| {
                AgentCommandError::InvalidLocalState(
                    "update journal omitted post-Deployment Resource ETag".to_owned(),
                )
            })?,
        )?;
        validate_resource_response(&response, &existing.agent_id)?;
        if response.body.gate_state != AdministrativeGate::Enabled {
            return Err(AgentCommandError::InvalidAuthority(
                "activation did not enable the Agent".to_owned(),
            ));
        }
        journal.final_resource_etag = Some(response.etag);
        save_update_journal(&journal_path, &journal)?;
    }

    let current = read_resource(client, &existing.agent_id)?;
    let mut entry = matching_active_publication(client, &current, compilation)?
        .filter(|entry| entry.active_deployment_id.as_ref() == Some(&deployment_id))
        .ok_or_else(|| {
            AgentCommandError::InvalidAuthority(
                "completed publication no longer matches active authority".into(),
            )
        })?;
    entry.latest_run_id = existing.latest_run_id;
    let mut lock = load_lock(project_root)?;
    lock.agents.insert(compiled.name.clone(), entry);
    save_lock(project_root, &lock)?;
    Ok(AgentPublicationReportV1 {
        schema_version: 1,
        agent_name: compiled.name.clone(),
        agent_id: existing.agent_id,
        state: "ready".to_owned(),
        environment: compiled.deployment_intent.environment.clone(),
        manifest_digest: compiled.manifest_digest.clone(),
        unchanged: false,
        validation_operation_id: Some(validation_operation_id),
        active_deployment_id: Some(deployment_id),
    })
}

fn exact_published_version(
    published: &[PublishedResourceVersionSummaryV1],
    kind: ResourceKind,
    expected_digest: &Sha256Digest,
) -> Result<ExactVersionRef, AgentCommandError> {
    let version = published
        .iter()
        .find(|version| version.resource_version_id.kind() == kind)
        .ok_or_else(|| {
            AgentCommandError::InvalidAuthority(format!("publish response omitted {kind}"))
        })?;
    if &version.content_digest != expected_digest || version.revision_no == 0 {
        return Err(AgentCommandError::InvalidAuthority(format!(
            "published {kind} digest or revision is invalid"
        )));
    }
    ExactVersionRef::new(
        version.resource_version_id.clone(),
        version.content_digest.clone(),
    )
    .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))
}

fn matching_active_publication(
    client: &PublicHttpClient,
    current: &ResourceViewV1,
    compilation: &AgentCompilationV1,
) -> Result<Option<AgentLockEntryV2>, AgentCommandError> {
    use insight_platform_api::resource::{
        AgentDeploymentClosureInputV1, CreateDeploymentClosureV1, DeploymentViewV1,
        ResourceVersionViewV1,
    };
    use insight_platform_contracts::{AgentSlotTargetInputV1, FrozenSlotTarget};
    let invalid = || {
        AgentCommandError::InvalidAuthority(
            "active publication violates its exact authority".into(),
        )
    };
    if current.gate_state != AdministrativeGate::Enabled
        || current.lifecycle_state != EntityLifecycle::Active
    {
        return Ok(None);
    }
    let Some(active) = &current.active_deployment_id else {
        return Ok(None);
    };
    let deployment: PublicJsonResponse<DeploymentViewV1> = client.get_json(
        &format!("/v1/agents/{}/deployments/{active}", current.resource_id),
        StatusCode::OK,
    )?;
    deployment.body.validate().map_err(|_| invalid())?;
    if deployment.body.resource_id != current.resource_id
        || deployment.body.deployment_id != *active
        || deployment.etag != deployment.body.etag
    {
        return Err(invalid());
    }
    let DeploymentClosure::Agent(actual) = &deployment.body.closure else {
        return Err(invalid());
    };
    let plan: PublicJsonResponse<ResourceVersionViewV1> = client.get_json(
        &format!(
            "/v1/agents/{}/versions/{}",
            current.resource_id, actual.plan.revision_id
        ),
        StatusCode::OK,
    )?;
    plan.body.validate().map_err(|_| invalid())?;
    if plan.body.resource_id != current.resource_id
        || plan.body.resource_version_id != actual.plan.revision_id
        || plan.body.content_digest != actual.plan.semantic_digest
        || plan.etag != plan.body.etag
        || deployment.body.resource_version_id != plan.body.resource_version_id
    {
        return Err(invalid());
    }
    let ResourceDocument::Agent(spec) = &plan.body.payload.document else {
        return Err(invalid());
    };
    let compiled = &compilation.compiled;
    if spec.authoring_name != compiled.name
        || spec.authoring_package.manifest_digest != compiled.manifest_digest
        || spec.authoring_package.artifact.content_digest() != &compilation.source_bundle_digest
        || spec.typed_plan_digest != compiled.typed_plan_digest
        || plan.body.artifact_id.as_ref() != Some(&spec.typed_plan_artifact_id)
        || actual.interface.semantic_digest != compiled.resource_intent.contract_digest
        || actual.plan.semantic_digest != compiled.typed_plan_digest
        || deployment.body.environment != compiled.deployment_intent.environment
    {
        return Ok(None);
    }
    // Reuse the server-owned Context identities solely to compare the intended logical closure.
    let mut context_ids = Vec::new();
    for desired in &compiled.deployment_intent.slots {
        if matches!(desired.target, AgentSlotTargetInputV1::Context { .. }) {
            let Some(observed) = actual
                .slots
                .iter()
                .find(|slot| slot.slot_id == desired.slot_id)
            else {
                return Ok(None);
            };
            let FrozenSlotTarget::Context { binding } = &observed.target else {
                return Ok(None);
            };
            context_ids.push(binding.context_binding_id.clone());
        }
    }
    let expected = CreateDeploymentClosureV1::Agent(AgentDeploymentClosureInputV1 {
        interface: actual.interface.clone(),
        plan: actual.plan.clone(),
        entry_node_id: compiled.deployment_intent.entry_node_id.clone(),
        entry_node_kind: compiled.deployment_intent.entry_node_kind,
        slots: compiled.deployment_intent.slots.clone(),
        policies: compiled.deployment_intent.policies.clone(),
        execution_profile: compiled.deployment_intent.execution_profile.clone(),
    })
    .materialize(active, context_ids)
    .map_err(|_| invalid())?;
    if expected != deployment.body.closure {
        return Ok(None);
    }
    let after = read_resource(client, &current.resource_id)?;
    if after.etag != current.etag || after.active_deployment_id.as_ref() != Some(active) {
        return Err(invalid());
    }
    Ok(Some(AgentLockEntryV2 {
        source_bundle_digest: compilation.source_bundle_digest.clone(),
        manifest_digest: compiled.manifest_digest.clone(),
        agent_id: current.resource_id.clone(),
        interface_revision_id: Some(actual.interface.revision_id.clone()),
        plan_revision_id: Some(actual.plan.revision_id.clone()),
        active_deployment_id: Some(active.clone()),
        environment: Some(deployment.body.environment),
        last_success_at: UtcTimestamp::from_datetime(Utc::now()),
        latest_run_id: None,
    }))
}

fn update_resource_version(agent_id: &ResourceId, etag: &str) -> Result<u64, AgentCommandError> {
    let prefix = format!("\"{agent_id}-");
    etag.strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix('"'))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|version| *version > 0 && *version <= i64::MAX as u64)
        .filter(|version| insight_platform_api::resource::resource_etag(agent_id, *version) == etag)
        .ok_or_else(|| {
            AgentCommandError::InvalidLocalState("update journal Resource ETag is invalid".into())
        })
}

fn publication_receipt(attempt: &ResourceId, action: &str) -> String {
    format!("insight-agent-v3-{attempt}-{action}")
}
fn load_update_journal(
    path: &Path,
    compilation: &AgentCompilationV1,
    agent: &ResourceId,
) -> Result<Option<AgentUpdateJournalV3>, AgentCommandError> {
    if !path.exists() {
        return Ok(None);
    }
    require_private_file(path)?;
    let bytes = read_bounded_file(path, MAX_LOCK_BYTES)?;
    let value = insight_platform_contracts::parse_strict_json(
        &bytes,
        insight_platform_contracts::JsonLimits {
            max_bytes: MAX_LOCK_BYTES as usize,
            max_depth: 12,
            max_properties_per_object: 32,
            max_items_per_array: 2,
            max_string_bytes: 4096,
        },
    )
    .map_err(|_| {
        AgentCommandError::InvalidLocalState("update journal is not bounded strict JSON".into())
    })?;
    let journal = serde_json::from_value(value).map_err(|_| {
        AgentCommandError::InvalidLocalState(
            "update journal violates current closed contract".into(),
        )
    })?;
    validate_update_journal(&journal, compilation, agent)?;
    Ok(Some(journal))
}

fn validate_update_journal(
    journal: &AgentUpdateJournalV3,
    compilation: &AgentCompilationV1,
    agent_id: &ResourceId,
) -> Result<(), AgentCommandError> {
    if journal.schema_version != 3
        || journal.attempt_id.kind() != ResourceKind::ServerRequest
        || journal.base_resource_version == 0
        || journal.base_resource_version > i64::MAX as u64
        || journal
            .base_active_deployment_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::AgentDeployment)
        || journal.source_bundle_digest != compilation.source_bundle_digest
        || journal.kind != "insight.platform.agent-update-journal/v3"
        || journal.manifest_digest != compilation.compiled.manifest_digest
        || journal.agent_id != *agent_id
        || journal
            .validation_operation_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::Job)
        || journal
            .deployment_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::AgentDeployment)
    {
        return Err(AgentCommandError::InvalidLocalState(
            "Agent update journal identity does not match this publication".to_owned(),
        ));
    }
    let phases = [
        journal.updated_resource_etag.is_some(),
        journal.validation_operation_id.is_some(),
        journal.validated_resource_etag.is_some(),
        journal.published_versions.is_some(),
        journal.deployment_id.is_some(),
        journal.deployed_resource_etag.is_some(),
        journal.final_resource_etag.is_some(),
    ];
    if phases.windows(2).any(|pair| !pair[0] && pair[1])
        || journal.draft_generation.is_some() != journal.validated_resource_etag.is_some()
        || journal
            .draft_generation
            .is_some_and(|value| value == 0 || value > i64::MAX as u64)
        || journal.published_resource_etag.is_some() != journal.published_versions.is_some()
        || [
            &journal.updated_resource_etag,
            &journal.validated_resource_etag,
            &journal.published_resource_etag,
            &journal.deployed_resource_etag,
            &journal.final_resource_etag,
        ]
        .into_iter()
        .flatten()
        .any(|etag| etag.len() > 256 || update_resource_version(agent_id, etag).is_err())
        || journal
            .published_versions
            .as_ref()
            .is_some_and(|versions| versions.len() != 2)
    {
        return Err(AgentCommandError::InvalidLocalState(
            "update journal phase dependencies are invalid".into(),
        ));
    }
    Ok(())
}

fn save_update_journal(
    path: &Path,
    journal: &AgentUpdateJournalV3,
) -> Result<(), AgentCommandError> {
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?;
    if bytes.len() > MAX_LOCK_BYTES as usize {
        return Err(AgentCommandError::InvalidLocalState(
            "update journal exceeds bound".into(),
        ));
    }
    write_private_file(path, &bytes)
}

pub fn remember_run(
    project_root: &Path,
    agent_name: &str,
    run_id: ResourceId,
) -> Result<(), AgentCommandError> {
    if run_id.kind() != ResourceKind::Run {
        return Err(AgentCommandError::InvalidLocalState(
            "latest Run ID has the wrong kind".to_owned(),
        ));
    }
    let mut lock = load_lock(project_root)?;
    let entry = lock.agents.get_mut(agent_name).ok_or_else(|| {
        AgentCommandError::InvalidLocalState(format!(
            "Agent name {agent_name:?} is not in insight.lock"
        ))
    })?;
    entry.latest_run_id = Some(run_id);
    save_lock(project_root, &lock)
}

pub fn latest_run_for_agent(
    project_root: &Path,
    name: &str,
) -> Result<ResourceId, AgentCommandError> {
    load_lock(project_root)?
        .agents
        .get(name)
        .and_then(|entry| entry.latest_run_id.clone())
        .ok_or_else(|| {
            AgentCommandError::InvalidLocalState(format!(
                "Agent {name:?} has no Run recorded in this project"
            ))
        })
}

pub fn load_lock(project_root: &Path) -> Result<AgentProjectLockV2, AgentCommandError> {
    let path = lock_path(project_root);
    if !path.exists() {
        return Ok(AgentProjectLockV2::default());
    }
    require_private_file(&path)?;
    let bytes = read_bounded_file(&path, MAX_LOCK_BYTES)?;
    let lock = serde_json::from_slice::<AgentProjectLockV2>(&bytes).map_err(|_| {
        AgentCommandError::InvalidLocalState("insight.lock is not the supported v2 source-bundle format; preserve the old file and adopt the existing Agent from server authority".to_owned())
    })?;
    validate_lock(&lock)?;
    Ok(lock)
}

pub fn save_lock(project_root: &Path, lock: &AgentProjectLockV2) -> Result<(), AgentCommandError> {
    validate_lock(lock)?;
    let bytes = serde_json::to_vec_pretty(lock)
        .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?;
    write_private_file(&lock_path(project_root), &bytes)
}

fn validate_lock(lock: &AgentProjectLockV2) -> Result<(), AgentCommandError> {
    if lock.schema_version != 2 || lock.kind != LOCK_KIND || lock.agents.len() > 1_024 {
        return Err(AgentCommandError::InvalidLocalState(
            "insight.lock kind, version, or entry bound is invalid".to_owned(),
        ));
    }
    for (name, entry) in &lock.agents {
        if name.is_empty()
            || name.len() > 128
            || entry.agent_id.kind() != ResourceKind::Agent
            || entry
                .interface_revision_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::AgentInterfaceRevision)
            || entry
                .plan_revision_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::AgentPlanRevision)
            || entry
                .active_deployment_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::AgentDeployment)
            || entry
                .latest_run_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Run)
        {
            return Err(AgentCommandError::InvalidLocalState(format!(
                "insight.lock entry {name:?} is invalid"
            )));
        }
    }
    Ok(())
}

fn build_apply_manifest(
    compiled: &CompiledAgent,
    document: ResourceDocument,
    typed_plan_artifact_id: &ResourceId,
) -> Result<serde_json::Value, AgentCommandError> {
    let slots = serde_json::to_value(&compiled.deployment_intent.slots)
        .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?;
    Ok(serde_json::json!({
        "schema_version": 1,
        "kind": "insight.platform.apply/v1",
        "resource_noun": "agents",
        "create": {
            "display_name": compiled.resource_intent.display_name,
            "document": document
        },
        "publish": {
            "kind": "agent",
            "revision_no": 1,
            "interface_content_digest": compiled.resource_intent.contract_digest,
            "plan_content_digest": compiled.typed_plan_digest,
            "artifact_id": typed_plan_artifact_id
        },
        "deployment": {
            "environment": compiled.deployment_intent.environment,
            "closure": {
                "resource_kind": "agent",
                "bindings": {
                    "entry_node_id": compiled.deployment_intent.entry_node_id,
                    "entry_node_kind": compiled.deployment_intent.entry_node_kind,
                    "slots": slots,
                    "policies": compiled.deployment_intent.policies,
                    "execution_profile": compiled.deployment_intent.execution_profile
                }
            }
        }
    }))
}

fn artifact_authority(
    view: artifact::ArtifactViewV1,
) -> Result<ArtifactAuthority, AgentCommandError> {
    let artifact = view.content.ok_or_else(|| {
        AgentCommandError::InvalidAuthority("uploaded Artifact is not Ready".to_owned())
    })?;
    Ok(ArtifactAuthority {
        purpose: view.purpose,
        state: view.state,
        artifact,
    })
}

fn validate_resource_response(
    response: &PublicJsonResponse<ResourceViewV1>,
    agent_id: &ResourceId,
) -> Result<(), AgentCommandError> {
    if response.body.schema_version != 1
        || response.body.resource_id != *agent_id
        || response.body.resource_kind != RegistryResourceKind::Agent
        || response.body.version == 0
        || response.body.draft_generation == 0
        || response.body.etag != response.etag
        || response.body.draft.document.kind() != RegistryResourceKind::Agent
    {
        return Err(AgentCommandError::InvalidAuthority(
            "Agent Resource identity, kind, version, or ETag is inconsistent".to_owned(),
        ));
    }
    Ok(())
}

fn validate_agent_summary(summary: &AgentSummaryV1) -> Result<(), AgentCommandError> {
    if summary.schema_version != 1
        || summary.name.is_empty()
        || summary.name.len() > 128
        || summary.agent_id.kind() != ResourceKind::Agent
        || summary.active_deployment.as_ref().is_some_and(|value| {
            value.resource_kind != ResourceKind::AgentDeployment || value.validate().is_err()
        })
        || summary.display_name.is_empty()
        || summary
            .required_features
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(AgentCommandError::InvalidAuthority(
            "Agent summary is inconsistent".to_owned(),
        ));
    }
    Ok(())
}

fn require_agent_id(agent_id: &ResourceId) -> Result<(), AgentCommandError> {
    if agent_id.kind() != ResourceKind::Agent {
        return Err(AgentCommandError::InvalidLocalState(
            "Agent ID has the wrong kind".to_owned(),
        ));
    }
    Ok(())
}

fn digest_suffix(digest: &Sha256Digest) -> Result<&str, AgentCommandError> {
    digest
        .as_str()
        .strip_prefix("sha256:")
        .ok_or_else(|| AgentCommandError::InvalidLocalState("digest prefix is invalid".to_owned()))
}

fn lock_path(project_root: &Path) -> PathBuf {
    project_root.join("insight.lock")
}

fn read_bounded_file(path: &Path, maximum_bytes: u64) -> Result<Vec<u8>, AgentCommandError> {
    let metadata = fs::metadata(path).map_err(|error| io_error(path, error))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum_bytes {
        return Err(AgentCommandError::InvalidLocalState(format!(
            "{} is not a bounded regular file",
            path.display()
        )));
    }
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|error| io_error(path, error))?
        .take(maximum_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io_error(path, error))?;
    if bytes.is_empty() || bytes.len() as u64 > maximum_bytes {
        return Err(AgentCommandError::InvalidLocalState(
            "file changed beyond its byte bound".into(),
        ));
    }
    Ok(bytes)
}

fn require_private_file(path: &Path) -> Result<(), AgentCommandError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = fs::metadata(path)
            .map_err(|error| io_error(path, error))?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(AgentCommandError::InvalidLocalState(format!(
                "{} permissions must not grant group or other access",
                path.display()
            )));
        }
    }
    Ok(())
}

fn set_private_directory(path: &Path) -> Result<(), AgentCommandError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error(path, error))?;
    }
    Ok(())
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), AgentCommandError> {
    if bytes.is_empty() || bytes.len() as u64 > MAX_LOCK_BYTES {
        return Err(AgentCommandError::InvalidLocalState(
            "private state is empty or exceeds its bound".to_owned(),
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        AgentCommandError::InvalidLocalState("private state path has no parent".to_owned())
    })?;
    crate::private_state::ensure_durable_directory(parent)
        .map_err(|error| io_error(parent, error))?;
    let temporary = path.with_extension(format!("{}.tmp", Uuid::now_v7()));
    let result = (|| {
        #[cfg(unix)]
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&temporary)
            .map_err(|error| io_error(&temporary, error))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| io_error(&temporary, error))?;
        fs::rename(&temporary, path).map_err(|error| io_error(path, error))?;
        require_private_file(path)?;
        crate::private_state::sync_directory(parent).map_err(|error| io_error(parent, error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn io_error(path: &Path, error: std::io::Error) -> AgentCommandError {
    AgentCommandError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn lock_round_trips_privately_and_rejects_wrong_id_kinds() {
        let root = tempdir().unwrap();
        let mut lock = AgentProjectLockV2::default();
        lock.agents.insert(
            "echo-agent".to_owned(),
            AgentLockEntryV2 {
                source_bundle_digest:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .parse()
                        .unwrap(),
                manifest_digest:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .parse()
                        .unwrap(),
                agent_id: ResourceId::from_uuid_v7(ResourceKind::Agent, Uuid::now_v7()).unwrap(),
                interface_revision_id: None,
                plan_revision_id: None,
                active_deployment_id: None,
                environment: Some("development".to_owned()),
                last_success_at: UtcTimestamp::from_datetime(Utc::now()),
                latest_run_id: None,
            },
        );
        save_lock(root.path(), &lock).unwrap();
        assert_eq!(load_lock(root.path()).unwrap(), lock);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(lock_path(root.path()))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn corrupt_lock_fails_closed() {
        let root = tempdir().unwrap();
        write_private_file(&lock_path(root.path()), b"{}").unwrap();
        assert!(matches!(
            load_lock(root.path()),
            Err(AgentCommandError::InvalidLocalState(_))
        ));
    }
}

#[cfg(test)]
#[path = "agent_publication_tests.rs"]
mod publication_tests;
