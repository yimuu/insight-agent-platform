//! Product-facing Agent authoring and project state built exclusively on public `/v1` clients.

use crate::{
    agent_compiler::{
        self, AgentCompilerInput, AgentCompilerProfile, AgentExecutionKind, ArtifactAuthority,
        CompiledAgent, ModelLoopCompilerLimits, ResolvedAgentBindings, ResolvedModelBinding,
    },
    apply, artifact,
    public_client::{PublicClientError, PublicHttpClient, PublicJsonResponse},
};
use chrono::Utc;
use insight_platform_contracts::{
    canonical_digest, AdministrativeGate, AgentProductState, DeploymentClosure, EntityLifecycle,
    ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef, OperationViewV1, PublicJobState,
    PublishedVersionPayload, RegistryResourceKind, ResourceDocument, ResourceDraftPayload,
    ResourceId, ResourceKind, Sha256Digest, UtcTimestamp,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

const LOCK_KIND: &str = "insight.platform.agent-project-lock/v1";
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
    pub agent_name: String,
    pub manifest_digest: Sha256Digest,
    pub execution_kind: AgentExecutionKind,
    pub required_features: Vec<agent_compiler::RequiredAgentFeature>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentLockEntryV1 {
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
pub struct AgentProjectLockV1 {
    pub schema_version: u16,
    pub kind: String,
    pub agents: BTreeMap<String, AgentLockEntryV1>,
}

impl Default for AgentProjectLockV1 {
    fn default() -> Self {
        Self {
            schema_version: 1,
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
struct ResourceVersionViewV1 {
    schema_version: u32,
    resource_id: ResourceId,
    resource_kind: RegistryResourceKind,
    resource_version_id: ResourceId,
    revision_no: u64,
    content_digest: Sha256Digest,
    artifact_id: Option<ResourceId>,
    payload: PublishedVersionPayload,
    created_at: UtcTimestamp,
    etag: String,
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
struct AgentUpdateJournalV1 {
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
struct LocalArtifactBootstrapProfile {
    schema_version: u32,
    scheduling_policy_id: ResourceId,
    scheduling_policy_revision_id: ResourceId,
    scheduling_policy_deployment_id: ResourceId,
    #[serde(flatten)]
    ignored: BTreeMap<String, serde_json::Value>,
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

pub fn compile_project(
    project_root: &Path,
    manifest_path: &Path,
    profile: AgentCompilerProfile,
) -> Result<CompiledAgent, AgentCommandError> {
    let loaded = agent_compiler::load_project_sources(project_root, manifest_path)?;
    let resolution = agent_compiler::inspect_manifest(&loaded.manifest_bytes)?;
    let bindings = resolve_model_bindings(project_root, resolution.model_ref.as_deref())?;
    Ok(agent_compiler::compile_agent(AgentCompilerInput {
        manifest_bytes: loaded.manifest_bytes,
        input_schema_bytes: loaded.input_schema_bytes,
        output_schema_bytes: loaded.output_schema_bytes,
        profile,
        bindings,
    })?)
}

pub fn validation_report(compiled: &CompiledAgent) -> AgentValidationReportV1 {
    AgentValidationReportV1 {
        schema_version: 1,
        agent_name: compiled.name.clone(),
        manifest_digest: compiled.manifest_digest.clone(),
        execution_kind: compiled.execution_kind,
        required_features: compiled.required_features.clone(),
    }
}

pub fn offline_compiler_profile(
    project_root: &Path,
    runtime_configuration_directory: &Path,
) -> Result<AgentCompilerProfile, AgentCommandError> {
    let bootstrap = load_bootstrap_profile(runtime_configuration_directory)?;
    let revision_digest = local_identity_digest(
        project_root,
        "offline-scheduling-policy-revision",
        &bootstrap.scheduling_policy_revision_id,
    )?;
    let deployment_digest = local_identity_digest(
        project_root,
        "offline-scheduling-policy-deployment",
        &bootstrap.scheduling_policy_deployment_id,
    )?;
    let binding = ExactPolicyBinding {
        revision: ExactVersionRef::new(bootstrap.scheduling_policy_revision_id, revision_digest)
            .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?,
        deployment: ExactDeploymentRef::new(
            bootstrap.scheduling_policy_deployment_id,
            deployment_digest,
        )
        .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?,
    };
    Ok(default_profile(binding))
}

pub fn online_compiler_profile(
    client: &PublicHttpClient,
    runtime_configuration_directory: &Path,
) -> Result<AgentCompilerProfile, AgentCommandError> {
    let bootstrap = load_bootstrap_profile(runtime_configuration_directory)?;
    let revision: PublicJsonResponse<ResourceVersionViewV1> = client.get_json(
        &format!(
            "/v1/policies/{}/versions/{}",
            bootstrap.scheduling_policy_id, bootstrap.scheduling_policy_revision_id
        ),
        StatusCode::OK,
    )?;
    let deployment: PublicJsonResponse<DeploymentViewV1> = client.get_json(
        &format!(
            "/v1/policies/{}/deployments/{}",
            bootstrap.scheduling_policy_id, bootstrap.scheduling_policy_deployment_id
        ),
        StatusCode::OK,
    )?;
    if revision.body.schema_version != 1
        || revision.body.resource_id != bootstrap.scheduling_policy_id
        || revision.body.resource_kind != RegistryResourceKind::Policy
        || revision.body.resource_version_id != bootstrap.scheduling_policy_revision_id
        || revision.body.resource_version_id.kind() != ResourceKind::PolicyRevision
        || revision.body.revision_no == 0
        || revision.body.payload.document.kind() != RegistryResourceKind::Policy
        || revision.body.etag != revision.etag
        || deployment.body.schema_version != 1
        || deployment.body.resource_id != bootstrap.scheduling_policy_id
        || deployment.body.resource_kind != RegistryResourceKind::Policy
        || deployment.body.deployment_id != bootstrap.scheduling_policy_deployment_id
        || deployment.body.resource_version_id != bootstrap.scheduling_policy_revision_id
        || deployment.body.environment != "development"
        || deployment.body.etag != deployment.etag
    {
        return Err(AgentCommandError::InvalidAuthority(
            "built-in Scheduling policy identity is inconsistent".to_owned(),
        ));
    }
    let binding = ExactPolicyBinding {
        revision: ExactVersionRef::new(
            revision.body.resource_version_id,
            revision.body.content_digest,
        )
        .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))?,
        deployment: ExactDeploymentRef::new(
            deployment.body.deployment_id,
            deployment.body.closure_digest,
        )
        .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))?,
    };
    Ok(default_profile(binding))
}

fn default_profile(binding: ExactPolicyBinding) -> AgentCompilerProfile {
    AgentCompilerProfile {
        default_deadline_seconds: 120,
        default_environment: "development".to_owned(),
        policy_versions: vec![binding.revision.clone()],
        deployment_policies: vec![binding.clone()],
        execution_profile: binding,
        model_loop: ModelLoopCompilerLimits {
            maximum_rounds: 1,
            maximum_capability_calls: 1,
            maximum_parallel_calls_per_round: 1,
            token_budget: 2_304,
        },
    }
}

fn load_bootstrap_profile(
    runtime_configuration_directory: &Path,
) -> Result<LocalArtifactBootstrapProfile, AgentCommandError> {
    let path = runtime_configuration_directory.join("artifact-bootstrap.json");
    let bytes = read_bounded_file(&path, MAX_LOCK_BYTES)?;
    let profile =
        serde_json::from_slice::<LocalArtifactBootstrapProfile>(&bytes).map_err(|_| {
            AgentCommandError::InvalidLocalState(
                "artifact-bootstrap.json does not match the closed development profile".to_owned(),
            )
        })?;
    if profile.schema_version != 1
        || profile.scheduling_policy_id.kind() != ResourceKind::Policy
        || profile.scheduling_policy_revision_id.kind() != ResourceKind::PolicyRevision
        || profile.scheduling_policy_deployment_id.kind() != ResourceKind::PolicyDeployment
    {
        return Err(AgentCommandError::InvalidLocalState(
            "built-in Scheduling policy IDs are invalid".to_owned(),
        ));
    }
    Ok(profile)
}

fn local_identity_digest(
    project_root: &Path,
    purpose: &str,
    id: &ResourceId,
) -> Result<Sha256Digest, AgentCommandError> {
    canonical_digest(&serde_json::json!({
        "project_root": project_root.file_name().and_then(|name| name.to_str()).unwrap_or("project"),
        "purpose": purpose,
        "resource_id": id,
        "schema_version": 1
    }))
    .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?
    .parse()
    .map_err(|error| AgentCommandError::InvalidLocalState(format!("digest: {error}")))
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
    Ok(ResolvedAgentBindings { model: Some(model) })
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
) -> Result<AgentLockEntryV1, AgentCommandError> {
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
    let entry = AgentLockEntryV1 {
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
    compiled: &CompiledAgent,
    operation_timeout: Duration,
) -> Result<AgentPublicationReportV1, AgentCommandError> {
    let existing = load_lock(project_root)?.agents.get(&compiled.name).cloned();
    if let Some(existing) = &existing {
        if existing.manifest_digest == compiled.manifest_digest {
            let resource = read_resource(management_client, &existing.agent_id)?;
            let exact_manifest = matches!(
                &resource.draft.document,
                ResourceDocument::Agent(spec)
                    if spec.authoring_name == compiled.name
                        && spec.authoring_package.manifest_digest == compiled.manifest_digest
            );
            if resource.gate_state == AdministrativeGate::Enabled && exact_manifest {
                return Ok(AgentPublicationReportV1 {
                    schema_version: 1,
                    agent_name: compiled.name.clone(),
                    agent_id: existing.agent_id.clone(),
                    state: "ready".to_owned(),
                    environment: existing
                        .environment
                        .clone()
                        .unwrap_or_else(|| compiled.deployment_intent.environment.clone()),
                    manifest_digest: compiled.manifest_digest.clone(),
                    unchanged: true,
                    validation_operation_id: None,
                    active_deployment_id: existing.active_deployment_id.clone(),
                });
            }
            if !exact_manifest {
                return Err(AgentCommandError::InvalidAuthority(
                    "insight.lock manifest does not match the exact Agent authority; adopt or resolve the concurrent update before publishing"
                        .to_owned(),
                ));
            }
        }
    }

    let cache = project_root
        .join(".insight")
        .join("agent-publication")
        .join(digest_suffix(&compiled.manifest_digest)?);
    fs::create_dir_all(&cache).map_err(|error| io_error(&cache, error))?;
    set_private_directory(&cache)?;
    let manifest_path = cache.join("authoring.json");
    let plan_path = cache.join("typed-plan.json");
    write_private_file(&manifest_path, &compiled.canonical_manifest_bytes)?;
    write_private_file(&plan_path, &compiled.typed_plan_bytes)?;

    let uploader = artifact::HttpsArtifactObjectUploader::new()?;
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
            &cache.join("artifacts"),
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
    let document = compiled
        .resource_intent
        .materialize(&authoring_authority, &plan_authority)?;
    if let Some(existing) = existing {
        return update_agent(
            project_root,
            management_client,
            expected_tenant_id,
            compiled,
            document,
            &authoring_id,
            existing,
            operation_timeout,
            &cache,
        );
    }
    let manifest = build_apply_manifest(compiled, document, &authoring_id)?;
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
    let interface_revision_id = applied
        .published_versions
        .iter()
        .find(|version| version.resource_kind == ResourceKind::AgentInterfaceRevision.to_string())
        .map(|version| {
            ResourceId::parse_expected(
                &version.resource_version_id,
                ResourceKind::AgentInterfaceRevision,
            )
        })
        .transpose()
        .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))?;
    let plan_revision_id = applied
        .published_versions
        .iter()
        .find(|version| version.resource_kind == ResourceKind::AgentPlanRevision.to_string())
        .map(|version| {
            ResourceId::parse_expected(
                &version.resource_version_id,
                ResourceKind::AgentPlanRevision,
            )
        })
        .transpose()
        .map_err(|error| AgentCommandError::InvalidAuthority(error.to_string()))?;
    let entry = AgentLockEntryV1 {
        manifest_digest: compiled.manifest_digest.clone(),
        agent_id: agent_id.clone(),
        interface_revision_id,
        plan_revision_id,
        active_deployment_id: active_deployment_id.clone(),
        environment: Some(compiled.deployment_intent.environment.clone()),
        last_success_at: UtcTimestamp::from_datetime(Utc::now()),
        latest_run_id: None,
    };
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
    compiled: &CompiledAgent,
    document: ResourceDocument,
    authoring_artifact_id: &ResourceId,
    existing: AgentLockEntryV1,
    operation_timeout: Duration,
    cache: &Path,
) -> Result<AgentPublicationReportV1, AgentCommandError> {
    let journal_path = cache.join("update.json");
    let mut journal = if journal_path.exists() {
        require_private_file(&journal_path)?;
        serde_json::from_slice::<AgentUpdateJournalV1>(&read_bounded_file(
            &journal_path,
            MAX_LOCK_BYTES,
        )?)
        .map_err(|_| {
            AgentCommandError::InvalidLocalState(
                "Agent update journal is not closed JSON".to_owned(),
            )
        })?
    } else {
        AgentUpdateJournalV1 {
            schema_version: 1,
            kind: "insight.platform.agent-update-journal/v1".to_owned(),
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
    };
    validate_update_journal(&journal, compiled, &existing.agent_id)?;
    save_update_journal(&journal_path, &journal)?;

    if journal.updated_resource_etag.is_none() {
        let current = read_resource(client, &existing.agent_id)?;
        let current_spec = match &current.draft.document {
            ResourceDocument::Agent(spec) => spec,
            _ => {
                return Err(AgentCommandError::InvalidAuthority(
                    "existing Resource is not an Agent".to_owned(),
                ))
            }
        };
        if current_spec.authoring_name != compiled.name {
            return Err(AgentCommandError::InvalidAuthority(
                "server-owned Agent name cannot be changed".to_owned(),
            ));
        }
        let updated = if current.draft.document == document
            && current.draft.display_name == compiled.resource_intent.display_name
        {
            current
        } else {
            let request = serde_json::json!({
                "display_name": compiled.resource_intent.display_name,
                "document": document
            });
            let response: PublicJsonResponse<ResourceViewV1> = client.put_json(
                &format!("/v1/agents/{}/draft", existing.agent_id),
                &request,
                StatusCode::OK,
                &publication_receipt(&compiled.manifest_digest, "update"),
                &current.etag,
            )?;
            validate_resource_response(&response, &existing.agent_id)?;
            response.body
        };
        journal.updated_resource_etag = Some(updated.etag);
        save_update_journal(&journal_path, &journal)?;
    }

    if journal.validation_operation_id.is_none() {
        let response: PublicJsonResponse<OperationViewV1> = client.post_empty(
            &format!("/v1/agents/{}/draft:validate", existing.agent_id),
            StatusCode::ACCEPTED,
            &publication_receipt(&compiled.manifest_digest, "validate"),
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
        if response.body.tenant_id != *expected_tenant_id {
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
        if validated.draft.validation.is_none() {
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
            "artifact_id": authoring_artifact_id
        });
        let response: PublicJsonResponse<PublishResourceDraftResponseV1> = client.post_json(
            &format!("/v1/agents/{}/draft:publish", existing.agent_id),
            &request,
            StatusCode::OK,
            &publication_receipt(&compiled.manifest_digest, "publish"),
            journal.validated_resource_etag.as_deref(),
        )?;
        if response.body.schema_version != 1
            || response.body.resource_id != existing.agent_id
            || response.body.resource_kind != RegistryResourceKind::Agent
            || response.body.draft_generation != journal.draft_generation.unwrap_or_default()
            || response.body.version == 0
            || response.body.etag != response.etag
            || response.body.published_versions.len() != 2
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
            &publication_receipt(&compiled.manifest_digest, "deploy"),
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
        journal.deployed_resource_etag = Some(read_resource(client, &existing.agent_id)?.etag);
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
            &publication_receipt(&compiled.manifest_digest, "activate"),
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

    let entry = AgentLockEntryV1 {
        manifest_digest: compiled.manifest_digest.clone(),
        agent_id: existing.agent_id.clone(),
        interface_revision_id: Some(interface.revision_id.clone()),
        plan_revision_id: Some(plan.revision_id.clone()),
        active_deployment_id: Some(deployment_id.clone()),
        environment: Some(compiled.deployment_intent.environment.clone()),
        last_success_at: UtcTimestamp::from_datetime(Utc::now()),
        latest_run_id: existing.latest_run_id,
    };
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

fn publication_receipt(manifest_digest: &Sha256Digest, action: &str) -> String {
    format!(
        "insight-agent-v1-{}-{action}",
        manifest_digest
            .as_str()
            .strip_prefix("sha256:")
            .unwrap_or("invalid")
    )
}

fn validate_update_journal(
    journal: &AgentUpdateJournalV1,
    compiled: &CompiledAgent,
    agent_id: &ResourceId,
) -> Result<(), AgentCommandError> {
    if journal.schema_version != 1
        || journal.kind != "insight.platform.agent-update-journal/v1"
        || journal.manifest_digest != compiled.manifest_digest
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
    Ok(())
}

fn save_update_journal(
    path: &Path,
    journal: &AgentUpdateJournalV1,
) -> Result<(), AgentCommandError> {
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?;
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

pub fn load_lock(project_root: &Path) -> Result<AgentProjectLockV1, AgentCommandError> {
    let path = lock_path(project_root);
    if !path.exists() {
        return Ok(AgentProjectLockV1::default());
    }
    require_private_file(&path)?;
    let bytes = read_bounded_file(&path, MAX_LOCK_BYTES)?;
    let lock = serde_json::from_slice::<AgentProjectLockV1>(&bytes).map_err(|_| {
        AgentCommandError::InvalidLocalState("insight.lock is not closed JSON".to_owned())
    })?;
    validate_lock(&lock)?;
    Ok(lock)
}

pub fn save_lock(project_root: &Path, lock: &AgentProjectLockV1) -> Result<(), AgentCommandError> {
    validate_lock(lock)?;
    let bytes = serde_json::to_vec_pretty(lock)
        .map_err(|error| AgentCommandError::InvalidLocalState(error.to_string()))?;
    write_private_file(&lock_path(project_root), &bytes)
}

fn validate_lock(lock: &AgentProjectLockV1) -> Result<(), AgentCommandError> {
    if lock.schema_version != 1 || lock.kind != LOCK_KIND || lock.agents.len() > 1_024 {
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
    authoring_artifact_id: &ResourceId,
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
            "artifact_id": authoring_artifact_id
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
    fs::read(path).map_err(|error| io_error(path, error))
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
    fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
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
        require_private_file(path)
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
        let mut lock = AgentProjectLockV1::default();
        lock.agents.insert(
            "echo-agent".to_owned(),
            AgentLockEntryV1 {
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
